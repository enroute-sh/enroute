//! Failure detection and failover: which nodes are alive, and, on the root
//! log's leader, the controller that hands each repository's log to a new
//! sequencer when its sequencer fails (and to new replicas when one is
//! removed), as Kafka's controller does partitions' leaders.
//!
//! **Liveness.** Every node pings every member every [`PING`]; the loop
//! itself answers, so a node whose loop is stuck counts as down, not just
//! one whose process is gone. A node unheard of for [`DOWN`] is down.
//! Pongs carry the node's boot (random per start): a node that restarted
//! lost its sequencers' unsynced entries, so their terms end even if it
//! was never seen down.
//!
//! **Failover** (see `shardlog`'s model and [`super::repo`]): seal the log
//! at term + 1 on its replicas (and, during a replica change, the replicas
//! it changes from), and once a quorum (of each) answered, appoint a member
//! as up to date as such a quorum by a compare-and-swap on the root
//! (`Appointment`), batched for every log ready in a tick. A seal refused
//! by a higher term, or an appointment that no longer applies, means
//! another took over: the log is looked at again.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::proto::{Appointment, Incarnation, Sealed, frame};

use super::log_id;
use super::repo::Msg;
use super::root::{self, Directory};
use super::shardlog::{self, Config, Limits, Request, Response};
use super::transport::{Frame, frame as framed};

/// How often each node pings every other member.
pub(super) const PING: Duration = Duration::from_millis(250);
/// A node unheard of this long is down.
pub(super) const DOWN: Duration = Duration::from_secs(1);
/// A failover not done in this long starts again.
const RETRY: Duration = Duration::from_secs(3);
/// Logs the controller looks at per tick, at most.
const CHECKS_PER_TICK: usize = 20_000;
/// Failovers in flight, at most.
const IN_FLIGHT: usize = 50_000;

/// What this node knows of the others' liveness.
pub(super) struct Liveness {
    me: u64,
    boot: u64,
    pinged: Instant,
    started: Instant,
    /// When each node was last heard of, and its boot then.
    seen: HashMap<u64, (Instant, u64)>,
    /// When each node last answered a ping, which goes to its address in
    /// the directory: it is alive *there*.
    answered: HashMap<u64, Instant>,
}

impl Liveness {
    pub(super) fn new(me: u64, boot: u64) -> Liveness {
        let now = Instant::now();
        Liveness {
            me,
            boot,
            pinged: now.checked_sub(PING).unwrap_or(now),
            started: now,
            seen: HashMap::new(),
            answered: HashMap::new(),
        }
    }

    /// The pings due to `members`.
    pub(super) fn tick(&mut self, members: &[u64]) -> Vec<(u64, Frame)> {
        if self.pinged.elapsed() < PING {
            return Vec::new();
        }
        self.pinged = Instant::now();
        let ping = crate::proto::Ping { boot: self.boot };
        (members.iter())
            .filter(|&&n| n != self.me)
            .map(|&n| (n, framed(0, frame::Kind::Ping(ping))))
            .collect()
    }

    /// `from` pinged: it is alive. The pong, and whether its boot changed
    /// (it restarted).
    pub(super) fn on_ping(&mut self, from: u64, ping: crate::proto::Ping) -> (Frame, bool) {
        let restarted = self.heard(from, ping.boot);
        let answer = crate::proto::Ping { boot: self.boot };
        (framed(0, frame::Kind::Pong(answer)), restarted)
    }

    /// `from` answered a ping. Whether its boot changed (it restarted).
    pub(super) fn on_pong(&mut self, from: u64, pong: crate::proto::Ping) -> bool {
        self.answered.insert(from, Instant::now());
        self.heard(from, pong.boot)
    }

    /// `from` is alive with `boot`. Whether that is a new boot: a restarted
    /// node's ping can come before its pong, so both must tell.
    fn heard(&mut self, from: u64, boot: u64) -> bool {
        let old = self.seen.insert(from, (Instant::now(), boot));
        old.is_some_and(|(_, b)| b != boot)
    }

    /// Whether this node has been up long enough to tell who is down.
    pub(super) fn settled(&self) -> bool {
        self.started.elapsed() >= DOWN
    }

    pub(super) fn alive(&self, node: u64) -> bool {
        self.boot(node).is_some()
    }

    /// `node`'s boot, while it is alive.
    pub(super) fn boot(&self, node: u64) -> Option<u64> {
        if node == self.me {
            return Some(self.boot);
        }
        self.seen
            .get(&node)
            .filter(|(t, _)| t.elapsed() < DOWN)
            .map(|&(_, b)| b)
    }

    /// The nodes that answer at their address in the directory now.
    pub(super) fn answering(&self) -> HashSet<u64> {
        (self.answered.iter())
            .filter(|(_, t)| t.elapsed() < DOWN)
            .map(|(&n, _)| n)
            .chain([self.me])
            .collect()
    }

    /// The nodes alive now.
    pub(super) fn up(&self) -> HashSet<u64> {
        (self.seen.iter())
            .filter(|(_, (t, _))| t.elapsed() < DOWN)
            .map(|(&n, _)| n)
            .chain([self.me])
            .collect()
    }
}

/// `d` in milliseconds.
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The cluster as the root's leader sees it, for the controller.
struct View<'a> {
    dir: &'a Directory,
    live: &'a Liveness,
}

impl shardlog::Cluster<u64, u64> for View<'_> {
    fn config(&self, log: &u64) -> Option<Config<u64>> {
        let repo = self.dir.logs.get(log)?;
        self.dir.live(repo).map(root::config)
    }

    fn member(&self, r: u64) -> bool {
        self.dir.meta.members.contains(&r)
    }

    fn boot(&self, r: u64) -> Option<u64> {
        self.live.boot(r)
    }

    /// A removed replica is replaced, missing ones are added (logs made
    /// while there were fewer members), and one sharing a zone moves to a
    /// zone the log lacks.
    fn moved(&self, log: &u64, config: &Config<u64>) -> Option<Vec<u64>> {
        let (dir, live) = (self.dir, self.live);
        let repo = dir.logs.get(log)?;
        let members = &dir.meta.members;
        let want = root::REPLICAS.min(members.len());
        let short = short(dir, &config.replicas);
        if !(short || dir.crowded(&config.replicas)) {
            return None;
        }
        let others: Vec<u64> = (members.iter())
            .copied()
            .filter(|&n| live.alive(n) && !config.replicas.contains(&n))
            .collect();
        if short {
            let mut replicas = dir.choose(repo, &config.replicas, &others);
            replicas.truncate(want);
            // (Not while too few members are up to place them.)
            (replicas != config.replicas && replicas.len() == want).then_some(replicas)
        } else {
            dir.respread(repo, &config.replicas, config.sequencer, &others, |n| {
                live.alive(n)
            })
        }
    }
}

/// Whether `replicas` are fewer than a log wants (placed while fewer members
/// were up) or hold a removed node.
fn short(dir: &Directory, replicas: &[u64]) -> bool {
    let members = &dir.meta.members;
    replicas.len() < root::REPLICAS.min(members.len())
        || replicas.iter().any(|n| !members.contains(n))
}

/// The root leader's controller ([`shardlog::Root`]) over log ids, on this
/// node's clock.
pub(super) struct Controller {
    root: shardlog::Root<u64, u64>,
    started: Instant,
}

impl Controller {
    pub(super) fn new() -> Controller {
        Controller {
            root: shardlog::Root::new(Limits {
                retry: millis(RETRY),
                per_tick: CHECKS_PER_TICK,
                in_flight: IN_FLIGHT,
            }),
            started: Instant::now(),
        }
    }

    /// The controller's clock.
    fn now(&self) -> u64 {
        millis(self.started.elapsed())
    }

    /// Look at `repo`'s live log soon.
    fn note(&mut self, dir: &Directory, repo: &str) {
        if let Some(inc) = dir.live(repo) {
            self.root.note(log_id(repo, inc.epoch));
        }
    }

    /// Look at the repositories `node` sequences.
    pub(super) fn note_node(&mut self, dir: &Directory, node: u64) {
        if !self.root.leading() {
            return;
        }
        if let Some(repos) = dir.sequencing.get(&node) {
            for r in repos {
                self.note(dir, r);
            }
        }
    }

    /// Look at the repositories short of replicas (placed while fewer
    /// members were up, or with a removed one) or with two in one zone: a
    /// node came up to take them.
    pub(super) fn note_short(&mut self, dir: &Directory) {
        if !self.root.leading() {
            return;
        }
        // A log with two replicas in one zone, if a member is in a zone it
        // lacks.
        let zones: BTreeSet<&str> = (dir.meta.members.iter())
            .filter_map(|&n| dir.zone(n))
            .collect();
        let lacks = |inc: &Incarnation| {
            zones
                .iter()
                .any(|&z| inc.replicas.iter().all(|&n| dir.zone(n) != Some(z)))
        };
        let moves = |inc: &Incarnation| {
            short(dir, &inc.replicas)
                || !zones.is_empty() && dir.crowded(&inc.replicas) && lacks(inc)
        };
        for (r, inc) in &dir.repos {
            if !inc.deleted && moves(inc) {
                self.root.note(log_id(r, inc.epoch));
            }
        }
    }

    /// Look at every repository.
    pub(super) fn note_all(&mut self, dir: &Directory) {
        if !self.root.leading() {
            return;
        }
        for &log in dir.logs.keys() {
            self.root.note(log);
        }
    }

    /// A tick: on the root's leader, start the failovers due and return
    /// the seals to send (by node, with the log); and the appointments
    /// ready to propose.
    pub(super) fn tick(
        &mut self,
        leading: bool,
        dir: &Directory,
        live: &Liveness,
        down: &[u64],
    ) -> (Vec<(u64, Frame)>, Vec<Appointment>) {
        if self.root.lead(leading) {
            self.note_all(dir);
        }
        if !leading || !live.settled() {
            return (Vec::new(), Vec::new());
        }
        for &n in down {
            self.note_node(dir, n);
        }
        let view = View { dir, live };
        let now = self.now();
        let mut frames = Vec::new();
        for seal in self.root.tick(&view, now) {
            let request = Msg::Request(Request::Seal { term: seal.term });
            let f = super::repo::frame(seal.log, request);
            frames.extend(seal.to.iter().map(|&n| (n, f.clone())));
            tracing::info!(
                repo = dir.logs.get(&seal.log).map(String::as_str),
                term = seal.term,
                "sealing a log"
            );
        }
        (frames, self.appointments(&view, now))
    }

    /// The appointments of the failovers whose seals reached their quorums:
    /// of the old sequencer if it can (it likely restarted), else of the
    /// least loaded candidate that is up.
    fn appointments(&mut self, view: &View<'_>, now: u64) -> Vec<Appointment> {
        let ready: Vec<(u64, Vec<u64>)> = (self.root.ready())
            .map(|(&log, c)| (log, c.to_vec()))
            .collect();
        let mut loads: HashMap<u64, usize> = HashMap::new();
        let mut out = Vec::new();
        for (log, candidates) in ready {
            let Some(repo) = view.dir.logs.get(&log) else {
                continue;
            };
            let Some(inc) = view.dir.live(repo) else {
                continue;
            };
            let up = candidates.into_iter().filter(|&n| view.live.alive(n));
            let Some(sequencer) = up.min_by_key(|&n| {
                let load = view.dir.sequenced(n) + loads.get(&n).copied().unwrap_or(0);
                (n != inc.sequencer, load)
            }) else {
                continue;
            };
            let Some(a) = self.root.appoint(view, &log, sequencer, now) else {
                continue;
            };
            *loads.entry(sequencer).or_default() += 1;
            out.push(Appointment {
                repo: repo.clone(),
                epoch: inc.epoch,
                base_term: a.base,
                sequencer,
                replicas: a.replicas.unwrap_or_default(),
                boot: a.boot,
            });
        }
        out
    }

    /// A replica answered a seal.
    pub(super) fn on_sealed(
        &mut self,
        dir: &Directory,
        live: &Liveness,
        log: u64,
        from: u64,
        s: Sealed,
    ) {
        let response = Response::sealed(s.term, s.last, s.last_term, s.fence);
        let view = View { dir, live };
        self.root.on_sealed(&view, &log, from, s.term, response);
    }

    /// `repo`'s sequencer of `term` was deposed (see `proto::Deposed`): if
    /// the root still names that term, the log needs a new one.
    pub(super) fn on_deposed(&mut self, dir: &Directory, live: &Liveness, repo: &str, term: u64) {
        if let Some(inc) = dir.live(repo).filter(|_| self.root.leading()) {
            let view = View { dir, live };
            self.root.on_deposed(&view, log_id(repo, inc.epoch), term);
        }
    }

    /// The root applied a change to `repo`'s log: its failover is over.
    pub(super) fn applied(&mut self, repo: &str, epoch: u64) {
        self.root.applied(log_id(repo, epoch));
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::Ping;

    use super::*;

    #[test]
    fn a_restart_is_seen_by_ping_or_pong() {
        let mut live = Liveness::new(1, 10);
        assert!(!live.on_pong(2, Ping { boot: 20 }));
        // Restarted, and pinged first: its pong must not hide the restart.
        assert!(live.on_ping(2, Ping { boot: 21 }).1);
        assert!(!live.on_pong(2, Ping { boot: 21 }));
        assert!(live.on_pong(2, Ping { boot: 22 }));
        assert!(!live.on_ping(2, Ping { boot: 22 }).1);
    }

    /// A directory with members `zones` (id and zone) and repository `r`'s
    /// log on 1, 2 and 3, sequenced by 1 at boot 10.
    fn directory(zones: &[(u64, &str)]) -> (Directory, u64) {
        use crate::proto::{Create, Member, root_entry::Change};
        let mut dir = Directory::bootstrap(1);
        for &(id, zone) in zones {
            let m = Member {
                id,
                zone: zone.into(),
                ..Member::default()
            };
            dir.apply(&Change::AddMember(m).into());
        }
        let create = Create {
            repo: "r".into(),
            sequencer: 1,
            boot: 10,
            replicas: vec![1, 2, 3],
            ..Create::default()
        };
        dir.apply(&Change::Create(create).into());
        let log = log_id("r", dir.live("r").unwrap().epoch);
        (dir, log)
    }

    /// The replicas of the term the controller gives `log` next, if any,
    /// once every replica answers its seal.
    fn sealed_for(dir: &Directory, live: &Liveness, log: u64) -> Option<Vec<u64>> {
        let view = View { dir, live };
        let mut c = Controller::new();
        c.root.lead(true);
        c.root.note(log);
        let seal = c.root.tick(&view, 0).pop()?;
        for &r in &seal.to {
            let response = Response::sealed(seal.term, 0, 0, seal.term);
            c.root.on_sealed(&view, &log, r, seal.term, response);
        }
        let (_, candidates) = c.root.ready().next()?;
        let sequencer = candidates[0];
        let a = c.root.appoint(&view, &log, sequencer, 0)?;
        Some(
            a.replicas
                .unwrap_or_else(|| root::config(dir.live("r").unwrap()).replicas),
        )
    }

    #[test]
    fn a_log_with_two_replicas_in_one_zone_moves_one_once_a_zone_is_up() {
        let (dir, log) = directory(&[(1, "a"), (2, "a"), (3, "b"), (4, "c")]);
        let mut live = Liveness::new(1, 10);
        live.on_pong(2, Ping { boot: 20 });
        live.on_pong(3, Ping { boot: 30 });
        assert_eq!(sealed_for(&dir, &live, log), None);
        live.on_pong(4, Ping { boot: 40 });
        assert_eq!(sealed_for(&dir, &live, log), Some(vec![1, 3, 4]));
    }

    #[test]
    fn a_removed_sequencer_hands_its_log_off_before_its_replica_moves() {
        use crate::proto::root_entry::Change;
        let (mut dir, log) = directory(&[(1, ""), (2, ""), (3, ""), (4, "")]);
        let mut live = Liveness::new(2, 20);
        for (n, boot) in [(1, 10), (3, 30), (4, 40)] {
            live.on_pong(n, Ping { boot });
        }
        dir.apply(&Change::RemoveMember(1).into());
        // A new term on the same replicas first.
        assert_eq!(sealed_for(&dir, &live, log), Some(vec![1, 2, 3]));
        // Then the removed replica moves.
        let config = Config {
            sequencer: 2,
            boot: 20,
            ..root::config(dir.live("r").unwrap())
        };
        let moved = shardlog::Cluster::moved(
            &View {
                dir: &dir,
                live: &live,
            },
            &log,
            &config,
        );
        let moved = moved.unwrap();
        assert_eq!(moved.len(), 3);
        assert!(!moved.contains(&1) && moved.contains(&4), "{moved:?}");
    }
}
