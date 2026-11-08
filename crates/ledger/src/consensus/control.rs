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
//! it changes from), and once a quorum (of each) answered, appoint one of
//! the most up-to-date answering members by a compare-and-swap on the root
//! (`Appointment`), batched for every log ready in a tick. A seal refused
//! by a higher term, or an appointment that no longer applies, means
//! another took over: the log is looked at again.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::proto::{Appointment, Incarnation, Record, Sealed, frame};

use super::log_id;
use super::repo::Msg;
use super::root::Directory;
use super::shardlog::{self, Next, Response};
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

/// A failover in flight: the seal of the next term, as the protocol runs
/// it ([`shardlog::Reconfigure`]), and once it reached its quorums, the
/// replicas that may be appointed.
struct Failover {
    repo: String,
    epoch: u64,
    seal: shardlog::Reconfigure<u64>,
    candidates: Option<Vec<u64>>,
    /// Whether the appointment was proposed.
    proposed: bool,
    at: Instant,
}

/// The root leader's controller.
#[derive(Default)]
pub(super) struct Controller {
    /// Failovers in flight, by log id.
    inflight: HashMap<u64, Failover>,
    /// Repositories to look at.
    todo: VecDeque<String>,
    queued: HashSet<String>,
    /// Whether this node led the root at the last tick.
    leading: bool,
    /// Repositories whose sequencer was deposed by a seal whose
    /// appointment never came: they need a new term.
    deposed: HashSet<String>,
}

/// Why a log needs a new term, if it does.
fn needs(
    repo: &str,
    inc: &Incarnation,
    dir: &Directory,
    live: &Liveness,
    deposed: bool,
) -> Option<Vec<u64>> {
    let members = &dir.meta.members;
    let seq_gone =
        deposed || !members.contains(&inc.sequencer) || live.boot(inc.sequencer) != Some(inc.boot);
    // A removed replica is replaced, missing ones are added (logs made
    // while there were fewer members), and one sharing a zone moves to a
    // zone the log lacks, one change at a time.
    let want = super::root::REPLICAS.min(members.len());
    let removed = inc.replicas.iter().any(|n| !members.contains(n));
    let short = removed || inc.replicas.len() < want;
    if !inc.joint.is_empty() || !(short || dir.crowded(&inc.replicas)) {
        return seq_gone.then(|| inc.replicas.clone());
    }
    let others: Vec<u64> = (members.iter())
        .copied()
        .filter(|&n| live.alive(n) && !inc.replicas.contains(&n))
        .collect();
    let moved = if short {
        let mut replicas = dir.choose(repo, &inc.replicas, &others);
        replicas.truncate(want);
        // (Not while too few members are up to place them.)
        (replicas != inc.replicas && replicas.len() == want).then_some(replicas)
    } else {
        dir.respread(repo, &inc.replicas, inc.sequencer, &others, |n| {
            live.alive(n)
        })
    };
    match moved {
        // A move never drops the sitting sequencer: it hands the log to
        // another replica first, in a term on the same replicas, as a lease
        // moves before its replica does. It stays a replica of that term, so
        // it learns the outcome of what it appended. Its replica moves next.
        Some(replicas) if !replicas.contains(&inc.sequencer) => Some(inc.replicas.clone()),
        moved => moved.or_else(|| seq_gone.then(|| inc.replicas.clone())),
    }
}

impl Controller {
    /// Look at `repo` again soon.
    pub(super) fn note(&mut self, repo: &str) {
        if self.leading && self.queued.insert(repo.to_string()) {
            self.todo.push_back(repo.to_string());
        }
    }

    /// Look at the repositories `node` sequences.
    pub(super) fn note_node(&mut self, dir: &Directory, node: u64) {
        if let Some(repos) = dir.sequencing.get(&node) {
            for r in repos {
                self.note(r);
            }
        }
    }

    /// Look at the repositories short of replicas (placed while fewer
    /// members were up, or with a removed one) or with two in one zone: a
    /// node came up to take them.
    pub(super) fn note_short(&mut self, dir: &Directory) {
        if !self.leading {
            return;
        }
        let members = &dir.meta.members;
        let want = super::root::REPLICAS.min(members.len());
        // A log with two replicas in one zone, if a member is in a zone it
        // lacks.
        let zones: BTreeSet<&str> = members.iter().filter_map(|&n| dir.zone(n)).collect();
        let lacks = |inc: &Incarnation| {
            zones
                .iter()
                .any(|&z| inc.replicas.iter().all(|&n| dir.zone(n) != Some(z)))
        };
        let short = |inc: &Incarnation| {
            inc.replicas.len() < want
                || inc.replicas.iter().any(|n| !members.contains(n))
                || !zones.is_empty() && dir.crowded(&inc.replicas) && lacks(inc)
        };
        for (r, inc) in &dir.repos {
            if !inc.deleted && short(inc) {
                self.note(r);
            }
        }
    }

    /// Look at every repository.
    pub(super) fn note_all(&mut self, dir: &Directory) {
        if !self.leading {
            return;
        }
        for r in dir.repos.keys() {
            self.note(r);
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
        if leading != self.leading {
            self.leading = leading;
            self.inflight.clear();
            self.todo.clear();
            self.queued.clear();
            self.deposed.clear();
            if leading {
                self.note_all(dir);
            }
        }
        if !leading || !live.settled() {
            return (Vec::new(), Vec::new());
        }
        for &n in down {
            self.note_node(dir, n);
        }
        let mut seals = Vec::new();
        // Failovers that stalled start again.
        let stalled: Vec<u64> = (self.inflight.iter())
            .filter(|(_, f)| f.at.elapsed() > RETRY)
            .map(|(&l, _)| l)
            .collect();
        for log in stalled {
            if let Some(f) = self.inflight.remove(&log) {
                self.note(&f.repo);
            }
        }
        for _ in 0..CHECKS_PER_TICK {
            if self.inflight.len() >= IN_FLIGHT {
                break;
            }
            let Some(repo) = self.todo.pop_front() else {
                break;
            };
            self.queued.remove(&repo);
            let Some(inc) = dir.live(&repo) else { continue };
            let log = log_id(&repo, inc.epoch);
            if self.inflight.contains_key(&log) {
                continue;
            }
            let deposed = self.deposed.contains(&repo);
            let Some(replicas) = needs(&repo, inc, dir, live, deposed) else {
                continue;
            };
            let (seal, sends) = shardlog::Reconfigure::start::<Bytes, Record>(
                inc.term,
                &inc.replicas,
                &inc.joint,
                replicas,
            );
            let msgs = sends.into_iter().map(|(n, q)| (n, Msg::Request(q)));
            seals.extend(msgs.map(|(n, m)| (n, super::repo::frame(log, m))));
            tracing::info!(
                repo,
                term = inc.term + 1,
                sequencer = inc.sequencer,
                "sealing a log"
            );
            self.inflight.insert(
                log,
                Failover {
                    repo,
                    epoch: inc.epoch,
                    seal,
                    candidates: None,
                    proposed: false,
                    at: Instant::now(),
                },
            );
        }
        let ready = self.ready(dir, live);
        (seals, ready)
    }

    /// A replica answered a seal. Only members' answers count: an
    /// appointee must be one, and a quorum of them alone holds everything
    /// committed.
    pub(super) fn on_sealed(&mut self, dir: &Directory, log: u64, from: u64, s: Sealed) {
        let Some(f) = self.inflight.get_mut(&log) else {
            return;
        };
        if s.term != f.seal.base() + 1 {
            return;
        }
        let response = if s.fence > s.term {
            Response::Fenced { fence: s.fence }
        } else {
            Response::Sealed {
                last: s.last,
                last_term: s.last_term,
            }
        };
        let members = &dir.meta.members;
        match f.seal.on_seal(from, response, |n| members.contains(&n)) {
            Next::Wait | Next::Done(_) => {}
            Next::Appoint { candidates, .. } => f.candidates = Some(candidates),
            Next::Failed => {
                // A higher term came first: look again once the root says.
                if let Some(f) = self.inflight.remove(&log) {
                    self.note(&f.repo);
                }
            }
        }
    }

    /// The failovers whose seals reached their quorums: their appointments.
    fn ready(&mut self, dir: &Directory, live: &Liveness) -> Vec<Appointment> {
        let mut out = Vec::new();
        let mut loads: HashMap<u64, usize> = HashMap::new();
        for f in self.inflight.values_mut().filter(|f| !f.proposed) {
            let Some(candidates) = &f.candidates else {
                continue;
            };
            let Some(inc) = dir.live(&f.repo).filter(|i| i.epoch == f.epoch) else {
                continue;
            };
            let members = &dir.meta.members;
            let candidates =
                (candidates.iter().copied()).filter(|&n| members.contains(&n) && live.alive(n));
            // The old sequencer if it can (it likely restarted), else the
            // least loaded.
            let Some(sequencer) = candidates.min_by_key(|&n| {
                let load = dir.sequenced(n) + loads.get(&n).copied().unwrap_or(0);
                (n != inc.sequencer, load)
            }) else {
                continue;
            };
            let Some(boot) = live.boot(sequencer) else {
                continue;
            };
            *loads.entry(sequencer).or_default() += 1;
            f.proposed = true;
            f.at = Instant::now();
            out.push(Appointment {
                repo: f.repo.clone(),
                epoch: f.epoch,
                base_term: f.seal.base(),
                sequencer,
                replicas: if f.seal.replicas() == inc.replicas {
                    Vec::new()
                } else {
                    f.seal.replicas().to_vec()
                },
                boot,
            });
        }
        out
    }

    /// `repo`'s sequencer of `term` was deposed (see `proto::Deposed`): if
    /// the root still names that term, the log needs a new one.
    pub(super) fn on_deposed(&mut self, dir: &Directory, repo: &str, term: u64) {
        if self.leading && dir.live(repo).is_some_and(|i| i.term == term) {
            self.deposed.insert(repo.to_string());
            self.note(repo);
        }
    }

    /// The root applied a change to `repo`'s log: its failover is over.
    pub(super) fn applied(&mut self, repo: &str, epoch: u64) {
        self.deposed.remove(repo);
        self.inflight.remove(&log_id(repo, epoch));
        self.note(repo);
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

    #[test]
    fn a_log_with_two_replicas_in_one_zone_moves_one_once_a_zone_is_up() {
        use crate::proto::{Member, root_entry::Change::AddMember};
        let mut dir = Directory::bootstrap(1);
        for (id, zone) in [(1, "a"), (2, "a"), (3, "b"), (4, "c")] {
            let m = Member {
                id,
                zone: zone.into(),
                ..Member::default()
            };
            dir.apply(&AddMember(m).into());
        }
        // Placed while c was down.
        let inc = Incarnation {
            replicas: vec![1, 2, 3],
            sequencer: 1,
            boot: 10,
            ..Incarnation::default()
        };
        let mut live = Liveness::new(1, 10);
        live.on_pong(2, Ping { boot: 20 });
        live.on_pong(3, Ping { boot: 30 });
        assert_eq!(needs("r", &inc, &dir, &live, false), None);
        live.on_pong(4, Ping { boot: 40 });
        assert_eq!(needs("r", &inc, &dir, &live, false), Some(vec![1, 3, 4]));
        // Not during a replica change.
        let joint = Incarnation {
            joint: vec![1, 2, 4],
            ..inc
        };
        assert_eq!(needs("r", &joint, &dir, &live, false), None);
    }

    #[test]
    fn a_removed_sequencer_hands_its_log_off_before_its_replica_moves() {
        use crate::proto::{Member, root_entry::Change};
        let mut dir = Directory::bootstrap(1);
        for id in 1..=4 {
            let m = Member {
                id,
                ..Member::default()
            };
            dir.apply(&Change::AddMember(m).into());
        }
        let mut live = Liveness::new(2, 20);
        for (n, boot) in [(1, 10), (3, 30), (4, 40)] {
            live.on_pong(n, Ping { boot });
        }
        dir.apply(&Change::RemoveMember(1).into());
        let inc = Incarnation {
            replicas: vec![1, 2, 3],
            sequencer: 1,
            boot: 10,
            ..Incarnation::default()
        };
        // A new term on the same replicas first.
        assert_eq!(needs("r", &inc, &dir, &live, false), Some(vec![1, 2, 3]));
        // Then the removed replica moves.
        let handed = Incarnation {
            sequencer: 2,
            boot: 20,
            ..inc
        };
        let moved = needs("r", &handed, &dir, &live, false).unwrap();
        assert_eq!(moved.len(), 3);
        assert!(!moved.contains(&1) && moved.contains(&4), "{moved:?}");
    }
}
