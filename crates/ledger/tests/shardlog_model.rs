//! Checks the replicated log with Stateright over every interleaving of
//! sequencers, appointments, replica changes, reads, trims, syncs and
//! crashes, driving the ledger's own machines: a [`Node`] per replica and
//! a [`Reconfigure`] per failover.
//!
//! Every message is in flight until the model delivers it, in any order,
//! so messages land late and after their machine is gone. A fault drops a
//! message or the answers to it. A node's writes are durable only once it
//! syncs, and a crash puts its store back as it was then.
//!
//! Each planted bug breaks one rule of the protocol in the model layer; its
//! test asserts that the checker finds a violation.

#![cfg(test)]

use std::collections::BTreeMap;

use stateright::{Checker, Expectation, HasDiscoveries, Model, Property, UniformChooser};

use enroute_ledger::consensus::shardlog::{
    Cell, Effects, Msg, Next, Node, Pos, Reconfigure, Request, Response, Store, Synced, Term,
    Terms, count, majority, rename_all,
};

type V = u8;
type ReplicaId = u8;
/// The applied values an install carries.
type Applied = Vec<Option<V>>;
type M = Msg<V, Applied>;

/// The replicas the model has: three in a log's config, and one spare that
/// a replica change can bring in.
const REPLICAS: ReplicaId = 4;

/// Failover `id`'s controller, as an address.
fn controller(id: usize) -> ReplicaId {
    100 + u8::try_from(id).unwrap()
}

/// Every replica the model has.
fn replicas() -> impl Iterator<Item = ReplicaId> {
    0..REPLICAS
}

/// Breaks one rule of the protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Bug {
    /// Replicas take appends from any term.
    NoFence,
    /// Replicas take appends without checking the cell before them.
    NoPrevCheck,
    /// An appointment takes one seal answer as a quorum.
    SealOne,
    /// An appointment picks the least up-to-date answer.
    NotUpToDate,
    /// A sequencer takes one answer as a quorum.
    AckEarly,
    /// A read trusts its sequencer without checking that its term is open.
    NoCheck,
    /// Appends carry the sequencer's last position as the commit point.
    EarlyCommit,
    /// A replica change seals only the new replicas.
    SealNewOnly,
    /// A replica change drops the old replicas as soon as it is appointed.
    ActivateEarly,
    /// A node answers before its writes are durable.
    SyncEarly,
}

/// A log: cells from a start, after a prefix kept only as applied values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Log<V> {
    /// The values of positions 1 to `start - 1`, applied and trimmed.
    applied: Vec<Option<V>>,
    /// The term of position `start - 1` (0 for position 0).
    base_term: Term,
    /// The cells from `start`.
    cells: Vec<Cell<V>>,
}

impl<V> Default for Log<V> {
    fn default() -> Self {
        Log {
            applied: Vec::new(),
            base_term: 0,
            cells: Vec::new(),
        }
    }
}

impl<V> Terms for Log<V> {
    fn start(&self) -> Pos {
        count(self.applied.len()) + 1
    }

    fn last(&self) -> Pos {
        count(self.applied.len() + self.cells.len())
    }

    fn term(&self, pos: Pos) -> Option<Term> {
        let start = self.start();
        if pos + 1 == start {
            return Some(self.base_term);
        }
        let i = usize::try_from(pos.checked_sub(start)?).ok()?;
        self.cells.get(i).map(|c| c.term)
    }
}

impl<V: Clone> Log<V> {
    /// The cells from `pos`, if not trimmed.
    #[must_use]
    fn from(&self, pos: Pos) -> Option<Vec<Cell<V>>> {
        let i = usize::try_from(pos.checked_sub(self.start())?).ok()?;
        Some(self.cells.get(i..).map(<[_]>::to_vec).unwrap_or_default())
    }

    /// The value at `pos` if it is held (the outer option); none for a
    /// no-op.
    #[must_use]
    #[expect(clippy::option_option, reason = "held or not, then a value or a no-op")]
    fn value(&self, pos: Pos) -> Option<Option<V>> {
        let i = usize::try_from(pos.checked_sub(1)?).ok()?;
        match self.applied.get(i) {
            Some(v) => Some(v.clone()),
            None => self
                .cells
                .get(i - self.applied.len())
                .map(|c| c.value.clone()),
        }
    }

    /// The values of positions 1 to `upto`, if held.
    #[must_use]
    fn values(&self, upto: Pos) -> Option<Vec<Option<V>>> {
        (1..=upto).map(|pos| self.value(pos)).collect()
    }

    /// Drops the cells from `from` and appends `cells` there.
    fn write(&mut self, from: Pos, cells: Vec<Cell<V>>) {
        let keep = usize::try_from(from.saturating_sub(self.start())).unwrap_or(usize::MAX);
        self.cells.truncate(keep);
        self.cells.extend(cells);
    }

    /// Moves the positions below `below` into the applied prefix.
    fn trim(&mut self, below: Pos) {
        let n = usize::try_from(below.saturating_sub(self.start()))
            .unwrap_or(usize::MAX)
            .min(self.cells.len());
        let Some(last) = n.checked_sub(1).and_then(|i| self.cells.get(i)) else {
            return;
        };
        self.base_term = last.term;
        self.applied.extend(self.cells.drain(..n).map(|c| c.value));
    }
}

/// A node's log and hard state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Disk {
    log: Log<V>,
    fence: Term,
    commit: Pos,
}

/// A node's store: as written, and as durable (at the last sync).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct Mem {
    now: Disk,
    disk: Disk,
}

impl Mem {
    fn dirty(&self) -> bool {
        self.now != self.disk
    }

    /// The last position durable and still in the log.
    fn written(&self) -> Pos {
        let (a, b) = (&self.now.log, &self.disk.log);
        (1..=a.last().min(b.last()))
            .take_while(|&p| a.term(p) == b.term(p))
            .last()
            .unwrap_or(0)
    }
}

impl Terms for Mem {
    fn start(&self) -> Pos {
        self.now.log.start()
    }
    fn last(&self) -> Pos {
        self.now.log.last()
    }
    fn term(&self, pos: Pos) -> Option<Term> {
        self.now.log.term(pos)
    }
}

impl Store<V> for Mem {
    type State = Applied;

    fn fence(&self) -> Term {
        self.now.fence
    }
    fn commit(&self) -> Pos {
        self.now.commit
    }
    fn set_fence(&mut self, term: Term) {
        self.now.fence = term;
    }
    fn set_commit(&mut self, commit: Pos) {
        self.now.commit = commit;
    }
    fn cells(&self, from: Pos) -> Option<Vec<Cell<V>>> {
        self.now.log.from(from)
    }
    fn write(&mut self, from: Pos, cells: Vec<Cell<V>>) {
        self.now.log.write(from, cells);
    }
    fn snapshot(&self) -> Option<(Pos, Term, Applied)> {
        let log = &self.now.log;
        Some((log.start() - 1, log.base_term, log.applied.clone()))
    }
    fn install(&mut self, index: Pos, last_term: Term, state: Applied, keep: bool) {
        let log = &self.now.log;
        let cells = if keep {
            log.from(index + 1).unwrap_or_default()
        } else {
            Vec::new()
        };
        self.now.log = Log {
            applied: state,
            base_term: last_term,
            cells,
        };
    }
    fn trim(&mut self, below: Pos) {
        self.now.log.trim(below);
    }
}

type N = Node<ReplicaId, Mem>;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct Envelope {
    from: ReplicaId,
    to: ReplicaId,
    msg: M,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ReadSlot {
    /// At a node's round, with the values acknowledged when it began.
    Running(ReplicaId, u64, Vec<V>),
    Done(Vec<V>, Vec<V>),
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Slot {
    /// Whether it changes the replicas.
    Sealing(Reconfigure<ReplicaId>, bool),
    /// Sealed a quorum: any candidate may be appointed, from `base`.
    Appointing(Appointing),
    Gone,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Appointing {
    base: Term,
    term: Term,
    candidates: Vec<ReplicaId>,
    /// The new term's replicas, if they change.
    change: Option<Vec<ReplicaId>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    nodes: Vec<N>,
    /// The config: the open term, its replicas, and while a replica change
    /// runs, the ones it changes from (all sorted).
    term: Term,
    config: Vec<ReplicaId>,
    joint: Vec<ReplicaId>,
    reconfigs: Vec<Slot>,
    reads: Vec<ReadSlot>,
    /// Messages in flight, sorted so that equal states hash equally.
    net: Vec<Envelope>,
    /// Values not yet appended.
    pool: Vec<V>,
    /// Acknowledged values by position, sorted.
    acks: Vec<(Pos, V)>,
    changes: u8,
    trims: u8,
    faults: u8,
    crashes: u8,
    /// The node that may crash: the others sync at once. Choosing it up
    /// front leaves one node with unsynced writes instead of every one.
    doomed: Option<ReplicaId>,
    bad: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Action {
    Append(ReplicaId),
    Deliver(usize),
    /// Deliver a message and lose its answers.
    Lose(usize),
    Drop(usize),
    Retry(ReplicaId, ReplicaId),
    Sync(ReplicaId),
    Reconfigure,
    /// Replace a replica with the spare.
    Change(ReplicaId),
    /// Appoint a candidate of a reconfiguration.
    Appoint(usize, ReplicaId),
    /// Drop the old replicas once the term has committed on the new.
    Activate(ReplicaId),
    Read(ReplicaId),
    Trim(ReplicaId),
    Crash(ReplicaId),
}

#[derive(Clone, Debug)]
struct Bounds {
    values: Vec<V>,
    window: Pos,
    reconfigs: usize,
    /// Whether a reconfiguration starts only while the earlier ones run,
    /// so all of them race from one term.
    race: bool,
    reads: usize,
    /// Replica changes, each one of the reconfigurations.
    changes: u8,
    trims: u8,
    faults: u8,
    crashes: u8,
    bug: Option<Bug>,
}

impl Default for Bounds {
    fn default() -> Self {
        Bounds {
            values: vec![10],
            window: 1,
            reconfigs: 1,
            race: false,
            reads: 0,
            changes: 0,
            trims: 0,
            faults: 0,
            crashes: 0,
            bug: None,
        }
    }
}

impl State {
    fn node(&mut self, r: ReplicaId) -> &mut N {
        &mut self.nodes[usize::from(r)]
    }

    fn ack(&mut self, r: ReplicaId, (from, to): (Pos, Pos)) {
        let log = &self.nodes[usize::from(r)].store().now.log;
        let acked: Vec<(Pos, V)> = (from..=to)
            .filter_map(|pos| Some((pos, log.value(pos)??)))
            .collect();
        self.acks.extend(acked);
        self.acks.sort_unstable();
        self.acks.dedup();
    }

    fn acked_values(&self) -> Vec<V> {
        self.acks.iter().map(|a| a.1).collect()
    }

    /// The replica no set of the config names.
    fn spare(&self) -> Option<ReplicaId> {
        replicas().find(|r| !self.config.contains(r) && !self.joint.contains(r))
    }
}

impl Bounds {
    /// On a node that won't crash, a sync only delays answers, as the
    /// network can: the node syncs at once, which keeps the state space
    /// small. On the doomed node, syncs are actions of their own.
    fn settle(&self, s: &mut State, r: ReplicaId) {
        if (s.crashes > 0 && s.doomed == Some(r)) || r >= REPLICAS {
            return;
        }
        while s.nodes[usize::from(r)].store().dirty() || s.nodes[usize::from(r)].holds() {
            self.sync(s, r);
        }
    }

    /// Carries out node `r`'s effects.
    fn effects(&self, s: &mut State, r: ReplicaId, fx: Effects<ReplicaId, V, Applied>) {
        if let Some(c) = fx.committed {
            s.ack(r, c);
        }
        for (round, index) in fx.reads {
            let Some(id) = (s.reads.iter())
                .position(|x| matches!(x, ReadSlot::Running(n, k, _) if *n == r && *k == round))
            else {
                continue;
            };
            let ReadSlot::Running(_, _, acks) = &s.reads[id] else {
                continue;
            };
            s.reads[id] = match index {
                Some(index) => {
                    let log = &s.nodes[usize::from(r)].store().now.log;
                    let values = log.values(index).unwrap_or_default();
                    ReadSlot::Done(values.into_iter().flatten().collect(), acks.clone())
                }
                None => ReadSlot::Failed,
            };
        }
        // Answers land at once, as part of the step that sent them (a lost
        // answer is a fault): only requests wait in the network.
        let mut answers = Vec::new();
        for (to, msg) in fx.sends {
            let e = Envelope { from: r, to, msg };
            match e.msg {
                Msg::Request(_) | Msg::Check { .. } => s.net.push(e),
                _ => answers.push(e),
            }
        }
        s.net.sort();
        for e in answers {
            self.deliver(s, e, false);
        }
    }

    /// Whom `r`'s answer counts as from: `r`, or every replica for a
    /// planted bug that takes one answer as a quorum.
    fn answering(&self, r: ReplicaId, msg: &M) -> Vec<ReplicaId> {
        let one_is_quorum = match msg {
            Msg::Ack {
                response: Response::Appended { .. },
                ..
            } => self.bug == Some(Bug::AckEarly),
            Msg::Sealed { term, fence, .. } if fence == term => self.bug == Some(Bug::SealOne),
            _ => false,
        };
        let others = replicas().filter(|&o| one_is_quorum && o != r);
        std::iter::once(r).chain(others).collect()
    }

    /// Applies the planted bugs that alter a request on its way to `to`.
    fn plant(&self, s: &mut State, to: ReplicaId, request: &mut Request<V, Applied>) {
        let write = matches!(request, Request::Append(_) | Request::Install { .. });
        if write && self.bug == Some(Bug::NoFence) {
            s.node(to).store_mut().now.fence = 0;
        }
        if let Request::Append(a) = request {
            let held = s.nodes[usize::from(to)].store().term(a.prev);
            match (self.bug, held) {
                (Some(Bug::NoPrevCheck), Some(t)) => a.prev_term = t,
                (Some(Bug::EarlyCommit), _) => a.commit = a.prev + count(a.cells.len()),
                _ => {}
            }
        }
    }

    fn deliver(&self, s: &mut State, e: Envelope, lose: bool) {
        if e.to >= 100 {
            self.to_controller(s, usize::from(e.to - 100), e.from, &e.msg);
            return;
        }
        let mut msg = e.msg;
        let to = e.to;
        if let Msg::Request(request) = &mut msg {
            self.plant(s, to, request);
        }
        let fake: Vec<ReplicaId> = self.answering(e.from, &msg).into_iter().skip(1).collect();
        let mut fx = match s.node(to).step(e.from, msg.clone()) {
            Ok(fx) => fx,
            Err(broken) => {
                s.bad = Some(format!("replica {to} dropped committed {}", broken.0));
                return;
            }
        };
        for other in fake {
            if let Ok(more) = s.node(to).step(other, msg.clone()) {
                fx.committed = fx.committed.or(more.committed);
            }
        }
        if self.bug == Some(Bug::SyncEarly) {
            let early = s.node(to).synced(&Synced::after_sync(), 0);
            fx.sends.extend(early.sends);
        }
        if lose {
            fx.sends.retain(|(t, _)| *t != e.from);
        }
        self.after(s, to, fx);
    }

    fn to_controller(&self, s: &mut State, id: usize, from: ReplicaId, msg: &M) {
        let Msg::Sealed {
            term,
            last,
            last_term,
            fence,
        } = *msg
        else {
            return;
        };
        let Some(Slot::Sealing(m, change)) = s.reconfigs.get_mut(id) else {
            return;
        };
        if term != m.base() + 1 {
            return;
        }
        let change = change.then(|| m.replicas().to_vec());
        let response = if fence > term {
            Response::Fenced { fence }
        } else if self.bug == Some(Bug::NotUpToDate) {
            // A planted bug hides how up to date the answer is.
            Response::Sealed {
                last: 0,
                last_term: 0,
            }
        } else {
            Response::Sealed { last, last_term }
        };
        let base = m.base();
        let fake = self.answering(
            from,
            &Msg::Sealed {
                term,
                last,
                last_term,
                fence,
            },
        );
        let next = (fake.into_iter())
            .map(|r| m.on_seal(r, response, |_| true))
            .find(|next| !matches!(next, Next::Wait))
            .unwrap_or(Next::Wait);
        s.reconfigs[id] = match next {
            Next::Wait => return,
            Next::Appoint { term, candidates } => Slot::Appointing(Appointing {
                base,
                term,
                candidates,
                change,
            }),
            Next::Done(_) | Next::Failed => Slot::Gone,
        };
    }

    fn appoint(&self, s: &mut State, id: usize, sequencer: ReplicaId) {
        let slot = std::mem::replace(&mut s.reconfigs[id], Slot::Gone);
        let Slot::Appointing(a) = slot else { return };
        // The compare-and-swap on the config (and one replica change at a
        // time).
        if a.base != s.term || (a.change.is_some() && !s.joint.is_empty()) {
            return;
        }
        s.term = a.term;
        if let Some(new) = a.change {
            let old = std::mem::replace(&mut s.config, new);
            // (With the bug, the old replicas are dropped at once.)
            s.joint = if self.bug == Some(Bug::ActivateEarly) {
                Vec::new()
            } else {
                old
            };
        }
        let config = s.config.clone();
        let written = s.nodes[usize::from(sequencer)].store().written();
        let fx = s.node(sequencer).appoint(a.term, config, written);
        self.after(s, sequencer, fx);
    }

    /// The planted bug that takes the sequencer's own copy as a quorum.
    fn fake_quorum(
        &self,
        s: &mut State,
        r: ReplicaId,
        written: Pos,
        fx: &mut Effects<ReplicaId, V, Applied>,
    ) {
        if self.bug != Some(Bug::AckEarly) {
            return;
        }
        let Some(term) = s.nodes[usize::from(r)].sequencing::<V>() else {
            return;
        };
        let ack = Msg::Ack {
            term,
            response: Response::Appended { last: written },
        };
        for other in replicas().filter(|&o| o != r) {
            if let Ok(more) = s.node(r).step(other, ack.clone()) {
                fx.committed = fx.committed.or(more.committed);
                fx.sends.extend(more.sends);
            }
        }
    }

    fn sync(&self, s: &mut State, r: ReplicaId) {
        let store = s.node(r).store_mut();
        store.disk = store.now.clone();
        let written = store.written();
        let mut fx = s.node(r).synced(&Synced::after_sync(), written);
        self.fake_quorum(s, r, written, &mut fx);
        self.effects(s, r, fx);
    }

    /// Carries out node `r`'s effects, then settles it.
    fn after(&self, s: &mut State, r: ReplicaId, fx: Effects<ReplicaId, V, Applied>) {
        self.effects(s, r, fx);
        self.settle(s, r);
    }

    fn reconfigure(s: &mut State, current: &[ReplicaId], replicas: Vec<ReplicaId>, change: bool) {
        let joint = if change { Vec::new() } else { s.joint.clone() };
        let (m, sends) = Reconfigure::start::<V, Applied>(s.term, current, &joint, replicas);
        let id = s.reconfigs.len();
        s.reconfigs.push(Slot::Sealing(m, change));
        for (to, request) in sends {
            s.net.push(Envelope {
                from: controller(id),
                to,
                msg: Msg::Request(request),
            });
        }
        s.net.sort();
    }
}

/// Drops the checks and seal answers no machine needs. They change no
/// replica, so delivering them only adds states.
fn forget(s: &mut State) {
    let (reads, nodes, reconfigs) = (&s.reads, &s.nodes, &s.reconfigs);
    s.net.retain(|e| match &e.msg {
        Msg::Check { round, .. } => nodes[usize::from(e.from)]
            .waiting(e.to)
            .any(|k| k == *round),
        Msg::Open { round, .. } => {
            reads
                .iter()
                .any(|x| matches!(x, ReadSlot::Running(n, k, _) if *n == e.to && k == round))
                && nodes[usize::from(e.to)]
                    .waiting(e.from)
                    .any(|k| k == *round)
        }
        Msg::Sealed { .. } if e.to >= 100 => matches!(
            reconfigs.get(usize::from(e.to - 100)),
            Some(Slot::Sealing(m, _)) if m.pending(e.from)
        ),
        _ => true,
    });
}

/// Whether a message between `a` and `b` is in flight, either way.
fn in_flight(s: &State, a: ReplicaId, b: ReplicaId) -> bool {
    s.net
        .iter()
        .any(|e| (e.from, e.to) == (a, b) || (e.from, e.to) == (b, a))
}

/// Whether node `r` holds cells below its commit point.
fn trimmable(s: &State, r: ReplicaId) -> bool {
    let store = s.nodes[usize::from(r)].store();
    store.commit().min(store.last()) >= store.start()
}

impl Model for Bounds {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let s = State {
            nodes: replicas()
                .map(|r| Node::new(r, Mem::default(), self.window))
                .collect(),
            term: 1,
            config: vec![0, 1, 2],
            joint: Vec::new(),
            reconfigs: Vec::new(),
            reads: Vec::new(),
            net: Vec::new(),
            pool: self.values.clone(),
            acks: Vec::new(),
            changes: self.changes,
            trims: self.trims,
            faults: self.faults,
            crashes: self.crashes,
            doomed: None,
            bad: None,
        };
        let doomed: Vec<Option<ReplicaId>> = if self.crashes > 0 {
            replicas().map(Some).collect()
        } else {
            vec![None]
        };
        doomed
            .into_iter()
            .map(|doomed| {
                let mut s = State {
                    doomed,
                    ..s.clone()
                };
                let fx = s.node(0).appoint(1, vec![0, 1, 2], 0);
                self.after(&mut s, 0, fx);
                s
            })
            .collect()
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.bad.is_some() {
            return;
        }
        for (i, n) in state.nodes.iter().enumerate() {
            let r = u8::try_from(i).unwrap();
            if !state.pool.is_empty() && n.open() {
                actions.push(Action::Append(r));
            }
            // A retry resends appends to a lagging replica and the checks
            // that still wait for it.
            let lagging = n.lagging();
            let idle = replicas()
                .filter(|to| lagging.contains(to) || n.waiting(*to).next().is_some())
                .filter(|&to| !in_flight(state, r, to));
            actions.extend(idle.map(|to| Action::Retry(r, to)));
            let doomed = state.crashes > 0 && state.doomed == Some(r);
            if doomed && (n.store().dirty() || n.holds()) {
                actions.push(Action::Sync(r));
            }
            if state.reads.len() < self.reads && n.leading() {
                actions.push(Action::Read(r));
            }
            if n.sequencing() == Some(state.term) && !state.joint.is_empty() && n.leading() {
                actions.push(Action::Activate(r));
            }
            if doomed && (n.sequencing().is_some() || n.store().dirty()) {
                actions.push(Action::Crash(r));
            }
            if state.trims > 0 && trimmable(state, r) {
                actions.push(Action::Trim(r));
            }
        }
        for i in 0..state.net.len() {
            // Equal messages reach equal states.
            if i > 0 && state.net[i] == state.net[i - 1] {
                continue;
            }
            actions.push(Action::Deliver(i));
            if state.faults > 0 {
                actions.push(Action::Lose(i));
                actions.push(Action::Drop(i));
            }
        }
        let running = state.reconfigs.iter().all(|r| *r != Slot::Gone);
        for (id, slot) in state.reconfigs.iter().enumerate() {
            if let Slot::Appointing(a) = slot {
                actions.extend(a.candidates.iter().map(|&r| Action::Appoint(id, r)));
            }
        }
        if state.reconfigs.len() < self.reconfigs && (running || !self.race) {
            actions.push(Action::Reconfigure);
            if state.changes > 0 && state.joint.is_empty() {
                actions.extend(state.config.iter().map(|&r| Action::Change(r)));
            }
        }
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut s = state.clone();
        match action {
            Action::Append(r) => {
                let value = s.pool[0];
                let (_, fx) = s.node(r).propose(value)?;
                s.pool.remove(0);
                self.after(&mut s, r, fx);
            }
            Action::Deliver(i) => {
                let e = s.net.remove(i);
                self.deliver(&mut s, e, false);
            }
            Action::Lose(i) => {
                let e = s.net.remove(i);
                s.faults -= 1;
                self.deliver(&mut s, e, true);
            }
            Action::Drop(i) => {
                s.net.remove(i);
                s.faults -= 1;
            }
            Action::Retry(r, to) => {
                let mut fx = s.node(r).retry(to);
                let checks = s.node(r).retry_checks(to);
                fx.sends.extend(checks.sends);
                fx.reads.extend(checks.reads);
                self.after(&mut s, r, fx);
            }
            Action::Sync(r) => self.sync(&mut s, r),
            Action::Reconfigure => {
                let config = s.config.clone();
                Self::reconfigure(&mut s, &config, config.clone(), false);
            }
            Action::Change(out) => {
                let spare = s.spare()?;
                let mut new: Vec<ReplicaId> =
                    s.config.iter().copied().filter(|&r| r != out).collect();
                new.push(spare);
                new.sort_unstable();
                // The replicas it changes from, as the ledger seals (the
                // new one has nothing to lose).
                let current = if self.bug == Some(Bug::SealNewOnly) {
                    new.clone()
                } else {
                    s.config.clone()
                };
                s.changes -= 1;
                Self::reconfigure(&mut s, &current, new, true);
            }
            Action::Appoint(id, r) => self.appoint(&mut s, id, r),
            Action::Activate(r) => {
                if s.nodes[usize::from(r)].sequencing() == Some(s.term) {
                    s.joint.clear();
                }
            }
            Action::Read(r) => {
                if self.bug == Some(Bug::NoCheck) {
                    let n = &s.nodes[usize::from(r)];
                    let values = n.store().now.log.values(n.commit()).unwrap_or_default();
                    let acks = s.acked_values();
                    s.reads
                        .push(ReadSlot::Done(values.into_iter().flatten().collect(), acks));
                } else {
                    let round = s.node(r).read()?;
                    let acks = s.acked_values();
                    s.reads.push(ReadSlot::Running(r, round, acks));
                    let fx = s.node(r).send_checks();
                    self.after(&mut s, r, fx);
                }
            }
            Action::Trim(r) => {
                let n = s.node(r);
                let below = n.store().commit() + 1;
                n.trim(below);
                s.trims -= 1;
            }
            Action::Crash(r) => {
                let n = s.node(r);
                let store = n.store_mut();
                // At most what was committed and durable here may have been
                // applied, and its application may be durable when the
                // commit point is not.
                let applied = store.now.commit.min(store.written());
                store.now = store.disk.clone();
                n.restart(applied, false);
                s.crashes -= 1;
                for r in replicas() {
                    self.settle(&mut s, r);
                }
            }
        }
        forget(&mut s);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no committed cell dropped", |_, s: &State| s.bad.is_none()),
            Property::always("committed prefixes agree", |_, s: &State| {
                disagreement(s).is_none()
            }),
            Property::always("acks are durable", |_, s: &State| lost_ack(s).is_none()),
            Property::always("reads see earlier acks", |_, s: &State| {
                missed_ack(s).is_none()
            }),
            Property::always("reads agree", |_, s: &State| reads_disagree(s).is_none()),
            Property::sometimes("an ack", |_, s: &State| !s.acks.is_empty()),
            Property::sometimes("an appointment", |_, s: &State| s.term > 1),
            Property::sometimes("a replica change", |_, s: &State| {
                s.config != [0, 1, 2] && s.joint.is_empty()
            }),
            Property::sometimes("a read", |_, s: &State| {
                s.reads
                    .iter()
                    .any(|r| matches!(r, ReadSlot::Done(values, _) if !values.is_empty()))
            }),
        ]
    }
}

/// A position where two committed prefixes, or one and an
/// acknowledgement, differ.
fn disagreement(s: &State) -> Option<Pos> {
    let mut seen: BTreeMap<Pos, Option<V>> = BTreeMap::new();
    for (pos, v) in &s.acks {
        seen.insert(*pos, Some(*v));
    }
    let disks = (s.nodes.iter()).flat_map(|n| [&n.store().now, &n.store().disk]);
    for d in disks {
        for pos in 1..=d.commit {
            // A replica may know a commit point past what it holds.
            let Some(value) = d.log.value(pos) else {
                continue;
            };
            if *seen.entry(pos).or_insert(value) != value {
                return Some(pos);
            }
        }
    }
    None
}

/// The sets a seal of the config needs a quorum of each of.
fn quorums(s: &State) -> Vec<Vec<ReplicaId>> {
    let mut q = vec![s.config.clone()];
    if !s.joint.is_empty() {
        q.push(s.joint.clone());
    }
    q
}

/// An acknowledged value that no set of the config has a quorum holding
/// durably at its position: the next seal could miss it.
fn lost_ack(s: &State) -> Option<(Pos, V)> {
    s.acks.iter().copied().find(|&(pos, v)| {
        let holds =
            |r: &ReplicaId| s.nodes[usize::from(*r)].store().disk.log.value(pos) == Some(Some(v));
        !(quorums(s).iter()).any(|q| q.iter().filter(|r| holds(r)).count() >= majority(q.len()))
    })
}

/// A read that missed a value acknowledged before it began.
fn missed_ack(s: &State) -> Option<(usize, V)> {
    s.reads.iter().enumerate().find_map(|(id, slot)| {
        let ReadSlot::Done(values, acks) = slot else {
            return None;
        };
        acks.iter().find(|v| !values.contains(v)).map(|v| (id, *v))
    })
}

/// Two reads, neither a prefix of the other.
fn reads_disagree(s: &State) -> Option<(usize, usize)> {
    let done: Vec<(usize, &Vec<V>)> = s
        .reads
        .iter()
        .enumerate()
        .filter_map(|(id, slot)| match slot {
            ReadSlot::Done(values, _) => Some((id, values)),
            _ => None,
        })
        .collect();
    for (i, a) in &done {
        for (j, b) in &done {
            let n = a.len().min(b.len());
            if a[..n] != b[..n] {
                return Some((*i, *j));
            }
        }
    }
    None
}

/// `s` with replica `r` renamed `perm[r]`.
fn relabel(s: &State, perm: &[ReplicaId]) -> State {
    let to = |r: ReplicaId| perm.get(usize::from(r)).copied().unwrap_or(r);
    let mut t = s.clone();
    for (r, node) in s.nodes.iter().enumerate() {
        let mut node = node.clone();
        node.relabel(to);
        t.nodes[usize::from(perm[r])] = node;
    }
    rename_all(to, &mut t.config);
    rename_all(to, &mut t.joint);
    t.doomed = s.doomed.map(to);
    for slot in &mut t.reconfigs {
        match slot {
            Slot::Sealing(m, _) => m.relabel(to),
            Slot::Appointing(a) => {
                rename_all(to, &mut a.candidates);
                if let Some(c) = &mut a.change {
                    rename_all(to, c);
                }
            }
            Slot::Gone => {}
        }
    }
    for slot in &mut t.reads {
        if let ReadSlot::Running(n, _, _) = slot {
            *n = to(*n);
        }
    }
    for e in &mut t.net {
        e.from = to(e.from);
        e.to = to(e.to);
    }
    t.net.sort();
    t
}

/// The state among `s`'s renamings with the least hash: replicas are
/// interchangeable, so the checker visits one state of each set.
fn representative(s: &State) -> State {
    use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
    let hasher = BuildHasherDefault::<DefaultHasher>::default();
    // While the spare is untouched, renaming it gains nothing: only the
    // config's replicas are renamed (a coarser reduction, still sound).
    let spare_untouched = s.config == [0, 1, 2]
        && s.joint.is_empty()
        && *s.nodes[3].store() == Mem::default()
        && s.net.iter().all(|e| e.to != 3);
    let perms: Vec<Vec<ReplicaId>> = if spare_untouched {
        perms().into_iter().filter(|p| p[3] == 3).collect()
    } else {
        perms()
    };
    perms
        .iter()
        .map(|perm| relabel(s, perm))
        .min_by_key(|t| hasher.hash_one(t))
        .unwrap()
}

/// Every renaming of the replicas.
fn perms() -> Vec<Vec<ReplicaId>> {
    let mut out: Vec<Vec<ReplicaId>> = vec![Vec::new()];
    for _ in replicas() {
        let mut next = Vec::new();
        for p in &out {
            for r in replicas().filter(|r| !p.contains(r)) {
                next.push([p.clone(), vec![r]].concat());
            }
        }
        out = next;
    }
    out
}

fn num_threads() -> usize {
    std::thread::available_parallelism().map_or(1, usize::from)
}

fn run(bounds: Bounds, finish: HasDiscoveries) -> impl Checker<Bounds> {
    let mut builder = bounds.checker().threads(num_threads()).finish_when(finish);
    if let Ok(secs) = std::env::var("SHARDLOG_TIMEOUT") {
        builder = builder.timeout(std::time::Duration::from_secs(secs.parse().unwrap()));
    }
    let checker = builder.spawn_bfs().join();
    report(&checker);
    checker
}

#[expect(
    clippy::print_stderr,
    reason = "the state count shows what a run covered"
)]
fn report(checker: &impl Checker<Bounds>) {
    eprintln!("states: {}", checker.unique_state_count());
}

/// Checks that no always property breaks within `bounds` and that the run
/// reaches each of `reaches`.
fn holds(bounds: Bounds, reaches: &[&'static str]) {
    let mut builder = bounds
        .checker()
        .threads(num_threads())
        .symmetry_fn(representative);
    if let Ok(secs) = std::env::var("SHARDLOG_TIMEOUT") {
        builder = builder.timeout(std::time::Duration::from_secs(secs.parse().unwrap()));
    }
    let checker = builder.spawn_dfs().join();
    report(&checker);
    for property in checker.model().properties() {
        let found = checker.discovery(property.name);
        match property.expectation {
            Expectation::Always => {
                if let Some(path) = found {
                    let last = path.last_state().clone();
                    panic!("{:?}\n{:?}\n{last:#?}", property.name, path.into_actions());
                }
            }
            _ if reaches.contains(&property.name) => {
                assert!(found.is_some(), "{:?} not reached", property.name);
            }
            _ => {}
        }
    }
}

/// Checks that the checker finds a violation of `property` with `bug`.
/// Returns the steps that break it.
#[expect(clippy::print_stderr, reason = "shows the counterexample")]
fn breaks(bug: Bug, property: &'static str, bounds: Bounds) -> Vec<Action> {
    let bounds = Bounds {
        bug: Some(bug),
        ..bounds
    };
    let checker = run(bounds, HasDiscoveries::AnyOf([property].into()));
    let found: Vec<_> = checker.discoveries().into_keys().collect();
    let Some(path) = checker.discovery(property) else {
        panic!("{bug:?} did not break {property:?}, found {found:?}");
    };
    let actions = path.into_actions();
    if std::env::var_os("SHARDLOG_PATHS").is_some() {
        eprintln!("{bug:?}: {actions:#?}");
    }
    actions
}

/// Bounds from `SHARDLOG_BOUNDS`: values, window, reconfigurations, reads,
/// trims, faults, crashes, and optionally replica changes.
#[test]
#[ignore = "explores the bounds in SHARDLOG_BOUNDS"]
fn sized() {
    let spec = std::env::var("SHARDLOG_BOUNDS").unwrap_or_else(|_| "1,1,1,1,0,0,0".into());
    let n: Vec<u8> = spec.split(',').map(|x| x.parse().unwrap()).collect();
    holds(
        Bounds {
            values: (1..=n[0]).map(|v| v * 10).collect(),
            window: n[1].into(),
            reconfigs: n[2].into(),
            race: std::env::var_os("SHARDLOG_RACE").is_some(),
            reads: n[3].into(),
            changes: n.get(7).copied().unwrap_or(0),
            trims: n[4],
            faults: n[5],
            crashes: n[6],
            bug: None,
        },
        &[],
    );
}

/// One value, an appointment and a read.
#[test]
fn takeover() {
    holds(
        Bounds {
            reads: 1,
            ..Bounds::default()
        },
        &["an ack", "an appointment", "a read"],
    );
}

/// A request or an answer is lost.
#[test]
fn faults() {
    holds(
        Bounds {
            faults: 1,
            ..Bounds::default()
        },
        &["an ack", "an appointment"],
    );
}

/// Two appointments race for the config.
#[test]
#[ignore = "takes half a minute in a release build"]
fn racing_appointments() {
    holds(
        Bounds {
            reconfigs: 2,
            race: true,
            ..Bounds::default()
        },
        &["an ack", "an appointment"],
    );
}

/// A node crashes, losing what it had not synced, and an appointment
/// hands the log on.
#[test]
fn crash() {
    holds(
        Bounds {
            crashes: 1,
            ..Bounds::default()
        },
        &["an ack", "an appointment"],
    );
}

/// A replica is replaced by the spare.
#[test]
fn replica_change() {
    holds(
        Bounds {
            changes: 1,
            ..Bounds::default()
        },
        &["an ack", "an appointment", "a replica change"],
    );
}

/// A replica change, then the sequencer crashes and an appointment hands
/// the log on, in the joint config or after it.
#[test]
#[ignore = "takes minutes in a release build"]
fn replica_change_failover() {
    holds(
        Bounds {
            reconfigs: 2,
            changes: 1,
            crashes: 1,
            ..Bounds::default()
        },
        &["an ack", "an appointment", "a replica change"],
    );
}

/// A read during and after a replica change.
#[test]
#[ignore = "takes minutes in a release build"]
fn replica_change_read() {
    holds(
        Bounds {
            changes: 1,
            reads: 1,
            ..Bounds::default()
        },
        &["an ack", "a replica change", "a read"],
    );
}

#[test]
fn seal_new_only_loses_acks() {
    breaks(
        Bug::SealNewOnly,
        "acks are durable",
        Bounds {
            changes: 1,
            ..Bounds::default()
        },
    );
}

#[test]
fn early_activation_loses_acks() {
    breaks(
        Bug::ActivateEarly,
        "acks are durable",
        Bounds {
            reconfigs: 2,
            changes: 1,
            crashes: 1,
            ..Bounds::default()
        },
    );
}

/// Replicas and sequencers trim below their commit points, so a lagging
/// replica catches up from a snapshot.
#[test]
fn trim() {
    holds(
        Bounds {
            trims: 1,
            ..Bounds::default()
        },
        &["an ack", "an appointment"],
    );
}

/// As `trim`, with a second value behind the first.
#[test]
#[ignore = "takes minutes in a release build"]
fn trim_two() {
    holds(
        Bounds {
            values: vec![10, 20],
            trims: 1,
            ..Bounds::default()
        },
        &["an ack", "an appointment"],
    );
}

#[test]
fn no_fence_breaks_logs() {
    breaks(Bug::NoFence, "committed prefixes agree", Bounds::default());
}

#[test]
fn no_prev_check_breaks_logs() {
    breaks(
        Bug::NoPrevCheck,
        "committed prefixes agree",
        Bounds {
            values: vec![10, 20],
            faults: 1,
            ..Bounds::default()
        },
    );
}

#[test]
fn one_seal_loses_acks() {
    breaks(Bug::SealOne, "acks are durable", Bounds::default());
}

#[test]
fn stale_appointee_loses_acks() {
    breaks(Bug::NotUpToDate, "acks are durable", Bounds::default());
}

#[test]
fn early_ack_loses_acks() {
    breaks(Bug::AckEarly, "acks are durable", Bounds::default());
}

#[test]
fn answering_before_the_sync_loses_acks() {
    breaks(
        Bug::SyncEarly,
        "acks are durable",
        Bounds {
            crashes: 1,
            ..Bounds::default()
        },
    );
}

#[test]
fn no_check_misses_acks() {
    breaks(
        Bug::NoCheck,
        "reads see earlier acks",
        Bounds {
            reads: 1,
            ..Bounds::default()
        },
    );
}

/// The sequencer writes its own copy before it sends, so an early commit
/// point on one other replica is unsafe only once the sequencer's node
/// loses its unsynced copy in a crash.
#[test]
fn early_commit_breaks_logs() {
    breaks(
        Bug::EarlyCommit,
        "committed prefixes agree",
        Bounds {
            crashes: 1,
            ..Bounds::default()
        },
    );
}

/// More of everything than exhaustive checks reach.
fn walk_bounds() -> Bounds {
    Bounds {
        values: vec![10, 20, 30, 40],
        window: 2,
        reconfigs: 4,
        race: false,
        reads: 2,
        changes: 2,
        trims: 2,
        faults: 3,
        crashes: 2,
        bug: None,
    }
}

/// Every planted bug, by name.
const BUGS: [(&str, Bug); 10] = [
    ("NoFence", Bug::NoFence),
    ("NoPrevCheck", Bug::NoPrevCheck),
    ("SealOne", Bug::SealOne),
    ("NotUpToDate", Bug::NotUpToDate),
    ("AckEarly", Bug::AckEarly),
    ("NoCheck", Bug::NoCheck),
    ("EarlyCommit", Bug::EarlyCommit),
    ("SealNewOnly", Bug::SealNewOnly),
    ("ActivateEarly", Bug::ActivateEarly),
    ("SyncEarly", Bug::SyncEarly),
];

/// The seed in `SHARDLOG_SIM_SEED`, or one from the clock: each thread
/// starts from its own seed after it, so a fixed default would repeat the
/// same walks on every run.
fn walk_seed() -> u64 {
    std::env::var("SHARDLOG_SIM_SEED").map_or_else(
        |_| {
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
            now.map_or(1, |d| d.as_secs())
        },
        |s| s.parse().unwrap(),
    )
}

/// Random walks over `bounds` from `seed` for up to `secs`: the first
/// property they break and the actions that break it, and the states
/// they visited.
fn walk(bounds: Bounds, seed: u64, secs: u64) -> (Option<(&'static str, Vec<Action>)>, usize) {
    let checker = bounds
        .checker()
        .threads(num_threads())
        .timeout(std::time::Duration::from_secs(secs))
        .finish_when(HasDiscoveries::AnyFailures)
        .spawn_simulation(seed, UniformChooser)
        .join();
    let states = checker.unique_state_count();
    let broken = (checker.model().properties().into_iter())
        .filter(|p| p.expectation == Expectation::Always)
        .find_map(|p| Some((p.name, checker.discovery(p.name)?.into_actions())));
    (broken, states)
}

/// Random walks with more of everything than exhaustive checks reach.
/// `SHARDLOG_SIM_BUG` plants a bug, to see that the walks find it.
#[test]
#[ignore = "runs for the time in SHARDLOG_SIM_SECS, 60 by default"]
#[expect(
    clippy::print_stderr,
    reason = "the seed and state count show what a run covered"
)]
fn simulate() {
    let secs = std::env::var("SHARDLOG_SIM_SECS").map_or(60, |s| s.parse().unwrap());
    let seed = walk_seed();
    let bug = std::env::var("SHARDLOG_SIM_BUG").ok().map(|b| {
        let found = BUGS.iter().find(|(name, _)| *name == b);
        found.unwrap_or_else(|| panic!("no bug {b}")).1
    });
    eprintln!("seed: {seed}");
    let (broken, states) = walk(
        Bounds {
            bug,
            ..walk_bounds()
        },
        seed,
        secs,
    );
    eprintln!("states: {states}");
    if let Some((property, actions)) = broken {
        panic!("{property:?} (seed {seed})\n{actions:#?}");
    }
}

/// The walks find every planted bug within `SHARDLOG_SIM_SECS` (60 by
/// default) each: a clean run of [`simulate`] means something only if
/// they would have found these.
#[test]
#[ignore = "runs for up to SHARDLOG_SIM_SECS per planted bug"]
#[expect(clippy::print_stderr, reason = "how fast each bug is found")]
fn simulate_finds_planted_bugs() {
    let secs = std::env::var("SHARDLOG_SIM_SECS").map_or(60, |s| s.parse().unwrap());
    let seed = walk_seed();
    eprintln!("seed: {seed}");
    let mut missed = Vec::new();
    for (name, bug) in BUGS {
        let start = std::time::Instant::now();
        let bounds = Bounds {
            bug: Some(bug),
            ..walk_bounds()
        };
        match walk(bounds, seed, secs) {
            (Some((property, _)), states) => eprintln!(
                "{name}: {property:?} after {:.1?}, {states} states",
                start.elapsed()
            ),
            (None, _) => missed.push(name),
        }
    }
    assert!(
        missed.is_empty(),
        "the walks missed {missed:?} (seed {seed})"
    );
}

/// An install keeps the cells after it when they follow from it, but they
/// may be an older term's: found by `simulate`, where a replica holding an
/// older term's no-op after the installed position took the request's
/// commit point and later had to drop a cell it counted as committed.
#[test]
fn install_commits_only_what_it_installs() {
    let mut node: N = Node::new(2, Mem::default(), 2);
    let log = Log {
        applied: Vec::new(),
        base_term: 0,
        cells: vec![
            Cell {
                term: 1,
                value: None,
            },
            Cell {
                term: 1,
                value: Some(10),
            },
            Cell {
                term: 2,
                value: None,
            },
        ],
    };
    let disk = Disk {
        log,
        fence: 3,
        commit: 0,
    };
    *node.store_mut() = Mem {
        now: disk.clone(),
        disk,
    };
    let install = Request::Install {
        term: 3,
        index: 2,
        last_term: 1,
        state: vec![None, Some(10)],
        commit: 5,
    };
    node.step(0, Msg::Request(install)).unwrap();
    assert_eq!(node.store().last(), 3, "the cell after the install is kept");
    assert_eq!(node.store().commit(), 2);
}
