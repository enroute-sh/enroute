//! A replicated log on a few passive replicas, whose sequencer a
//! controller appoints: Raft's log replication with its elections moved
//! into a configuration master, as in Vertical Paxos (Lamport, Malkhi and
//! Zhou, PODC 2009), `PacificA` (Lin et al., 2008) and Kafka's controller.
//!
//! A *term* is one sequencer's tenure. The sequencer appends values to its
//! log and replicates them to every replica, as a Raft leader does: each
//! append names the position and term of the cell before it, and a replica
//! takes it only if its own log has that cell (the log matching property),
//! truncating any cells after it that differ. A position is committed once
//! a quorum holds it and it is of the sequencer's own term (Raft's commit
//! rule; a sequencer starts its term with a no-op, so the cells before it
//! commit with it). Appends carry the commit point, so replicas apply up to
//! it and may trim below it.
//!
//! Replicas are passive: they never time out, elect or send on their own. A
//! replica's *fence* is the highest term it has heard of; it refuses
//! appends of lower terms.
//!
//! The controller changes the sequencer. It seals the next term on a
//! quorum, which raises each replica's fence and returns the term and
//! position of its last cell; it appoints the replica whose log is the most
//! up to date of those answers (Raft's election restriction), by
//! compare-and-swap on the log's config. A seal quorum meets every write
//! quorum, so the appointee holds every committed cell. Nothing else is
//! decided by quorum.
//!
//! The controller changes the replicas the same way, one at a time, as
//! Vertical Paxos does: the seal of the new term goes to the old replicas
//! and needs a quorum of them, and the appointee is one of the most up to
//! date answers that stays a replica. Until the new term's no-op commits,
//! the config is *joint*: it keeps the old replicas, and every seal needs a
//! quorum of them as well as of the new, since cells committed before the
//! change may be on the old alone. Once the no-op
//! commits on a quorum of the new replicas, everything before it is there,
//! and the controller drops the old ones.
//!
//! A read takes the sequencer's commit point and then checks on a quorum
//! that its term is not sealed. That order matters (see the 2025 erratum to
//! the Delos paper): a check that ran first could pass before a seal and
//! miss values committed in the next term.
//!
//! The protocol is sans-IO: a [`Node`] per log per node, which hides that
//! node's replica, its sequencer and its read rounds behind one input and
//! effects interface over the node's [`Store`], and [`Reconfigure`], one
//! failover of the controller. The tests run them under a model checker,
//! and the ledger drives the same ones: it carries their messages, keeps
//! their stores, and syncs them. They own every protocol decision and the
//! hard state's changes; answers that rest on writes go only once the
//! writes are durable ([`Synced`]).

use std::collections::BTreeMap;

/// The replicas of `n` that a write, a seal and a check need.
#[must_use]
pub fn majority(n: usize) -> usize {
    n / 2 + 1
}

/// A sequencer's tenure.
pub type Term = u64;

/// A position in the log, from 1; 0 is the log's initial state.
pub type Pos = u64;

/// A count as a position.
#[must_use]
pub fn count(n: usize) -> Pos {
    Pos::try_from(n).unwrap_or(Pos::MAX)
}

/// A position's cell: the term that wrote it, and its value.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Cell<V> {
    /// The term whose sequencer appended it.
    pub term: Term,
    /// The value; none for the no-op that starts a term.
    pub value: Option<V>,
}

/// Cells to append after the position `prev`, whose cell is of `prev_term`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Append<V> {
    /// The sender's term.
    pub term: Term,
    /// The position before the first cell.
    pub prev: Pos,
    /// The term of the cell at `prev` (0 at position 0).
    pub prev_term: Term,
    /// The cells from `prev + 1`.
    pub cells: Vec<Cell<V>>,
    /// The sender's commit point.
    pub commit: Pos,
}

/// A request to a replica.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Request<V, T> {
    /// Append cells from the sequencer.
    Append(Append<V>),
    /// Take the sequencer's applied state up to a position it has trimmed.
    Install {
        /// The sender's term.
        term: Term,
        /// The last position the state covers.
        index: Pos,
        /// The term of that position.
        last_term: Term,
        /// The applied state up to `index`.
        state: T,
        /// The sender's commit point.
        commit: Pos,
    },
    /// Refuse appends below `term`, and say where the log ends.
    Seal {
        /// The term about to start.
        term: Term,
    },
}

/// A replica's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Response {
    /// The replica's log matches the sender's up to `last`.
    Appended {
        /// The last position it matches.
        last: Pos,
    },
    /// The replica's log does not reach or match the append's `prev`; it
    /// may match up to `last`.
    Behind {
        /// Where to append from, less one.
        last: Pos,
    },
    /// A higher term is open or sealed.
    Fenced {
        /// The replica's fence.
        fence: Term,
    },
    /// Sealed; its log ends at `last`, in a cell of `last_term`.
    Sealed {
        /// The last position it holds.
        last: Pos,
        /// The term of that cell.
        last_term: Term,
    },
    /// Whether the term is open.
    Open(bool),
}

/// What a replica knows of its log's positions: enough to decide an
/// append. The ledger's log store answers it from its index of entries.
pub trait Terms {
    /// The first position held as a cell; those before it are applied and
    /// trimmed.
    fn start(&self) -> Pos;
    /// The last position held.
    fn last(&self) -> Pos;
    /// The term of the cell at `pos`, from `start() - 1` to `last()`.
    fn term(&self, pos: Pos) -> Option<Term>;
}

/// How a replica takes an append (see [`plan`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Plan {
    /// Answer with this, and change nothing.
    Refuse(Response),
    /// Raise the fence to the append's term, drop the cells from
    /// `truncate` (if any), append the append's cells from index `skip`,
    /// take `commit` as the commit point, and answer `Appended { last }`.
    Take {
        /// The first position to drop.
        truncate: Option<Pos>,
        /// How many of the append's cells the log already holds.
        skip: usize,
        /// The last position the append covers.
        last: Pos,
        /// The commit point after it.
        commit: Pos,
    },
    /// The append would drop a committed cell: the protocol is broken.
    Conflict(Pos),
}

/// The head of an append: its term, `prev` and `prev_term`.
type Head = (Term, Pos, Term);

/// How a replica whose log is `log`, with fence `fence` and commit point
/// `commit`, takes an append with `head` of cells of `terms`, carrying the
/// commit point `sent_commit`.
fn plan(
    log: &impl Terms,
    (fence, commit): (Term, Pos),
    head: Head,
    terms: &[Term],
    sent_commit: Pos,
) -> Plan {
    let (term, prev, prev_term) = head;
    if term < fence {
        return Plan::Refuse(Response::Fenced { fence });
    }
    let (start, last) = (log.start(), log.last());
    if prev > last {
        return Plan::Refuse(Response::Behind { last });
    }
    // Positions before the start are committed, so every log has them.
    if prev + 1 >= start && log.term(prev) != Some(prev_term) {
        return Plan::Refuse(Response::Behind {
            last: prev.saturating_sub(1),
        });
    }
    let mut truncate = None;
    let mut skip = 0;
    for (i, &t) in terms.iter().enumerate() {
        let pos = prev + 1 + count(i);
        if pos < start {
            skip = i + 1;
            continue;
        }
        match log.term(pos) {
            Some(have) if have == t => skip = i + 1,
            Some(_) => {
                truncate = Some(pos);
                break;
            }
            None => break,
        }
    }
    if let Some(pos) = truncate
        && pos <= commit
    {
        return Plan::Conflict(pos);
    }
    let covered = prev + count(terms.len());
    Plan::Take {
        truncate,
        skip,
        last: covered,
        commit: commit.max(sent_commit.min(covered)),
    }
}

/// What a replica id must be: the model's are small numbers it can
/// permute, the ledger's are node ids.
pub trait Id: Copy + Ord + std::hash::Hash + std::fmt::Debug {}

impl<T: Copy + Ord + std::hash::Hash + std::fmt::Debug> Id for T {}

/// One node's copy of a log and its hard state (the fence and the commit
/// point): what the machines read and write. A node's replica and its
/// sequencer share it. Writes are durable only once the owner syncs the
/// store; until then a crash may undo them, so answers that rest on them
/// wait.
pub trait Store<V>: Terms {
    /// The applied state an install carries.
    type State: Clone;

    /// The highest term this replica has heard of.
    fn fence(&self) -> Term;
    /// The highest position it knows committed.
    fn commit(&self) -> Pos;
    /// Sets the fence (the machines only raise it).
    fn set_fence(&mut self, term: Term);
    /// Sets the commit point (the machines only raise it, to a held
    /// position).
    fn set_commit(&mut self, commit: Pos);
    /// The cells from `from` on, as many as one append should carry (at
    /// least one if any are held); none if `from` is trimmed.
    fn cells(&self, from: Pos) -> Option<Vec<Cell<V>>>;
    /// Drops the cells from `from` (none committed) and appends `cells`
    /// there.
    fn write(&mut self, from: Pos, cells: Vec<Cell<V>>);
    /// The applied state to install on a replica behind the start: its
    /// position, the term of that position, and the state.
    fn snapshot(&self) -> Option<(Pos, Term, Self::State)>;
    /// Takes an installed state at `index`, of `last_term`, keeping the
    /// cells after it if `keep` (they follow from it).
    fn install(&mut self, index: Pos, last_term: Term, state: Self::State, keep: bool);
    /// Drops the cells below `below` (all committed): the log then starts
    /// there.
    fn trim(&mut self, below: Pos);
}

/// Proof that a store's writes so far are durable: the owner makes one
/// only right after syncing them, and only it releases held answers.
#[derive(Debug)]
pub struct Synced(());

impl Synced {
    /// Says the store's writes up to now are durable. Call only right
    /// after the sync returned.
    #[must_use]
    pub fn after_sync() -> Synced {
        Synced(())
    }
}

/// An answer that rests on writes not yet durable: it may go only after
/// the store syncs them.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[must_use = "a held answer goes once the store is synced"]
struct Held(Response);

impl Held {
    /// The answer, now that the writes it rests on are durable.
    #[must_use]
    fn release(self, _: &Synced) -> Response {
        self.0
    }
}

/// A replica's answer to a request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Answer {
    /// Changes nothing: may go at once.
    Now(Response),
    /// Changed the store: goes once synced.
    Held(Held),
}

/// The protocol is broken: a request would drop a committed position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Broken(pub Pos);

/// Runs one request on a replica's store.
///
/// # Errors
///
/// [`Broken`] if the request would drop a committed position: the replica
/// must stop.
fn replica<V, S: Store<V>>(store: &mut S, request: Request<V, S::State>) -> Result<Answer, Broken> {
    let fence = store.fence();
    match request {
        Request::Append(a) => {
            let terms: Vec<Term> = a.cells.iter().map(|c| c.term).collect();
            let head = (a.term, a.prev, a.prev_term);
            let (truncate, skip, last, commit) =
                match plan(store, (fence, store.commit()), head, &terms, a.commit) {
                    Plan::Refuse(response) => return Ok(Answer::Now(response)),
                    Plan::Conflict(pos) => return Err(Broken(pos)),
                    Plan::Take {
                        truncate,
                        skip,
                        last,
                        commit,
                    } => (truncate, skip, last, commit),
                };
            store.set_fence(a.term);
            let from = a.prev + 1 + count(skip);
            let cells: Vec<Cell<V>> = a.cells.into_iter().skip(skip).collect();
            if truncate.is_some() || !cells.is_empty() {
                store.write(from, cells);
            }
            store.set_commit(commit.max(store.commit()));
            Ok(Answer::Held(Held(Response::Appended { last })))
        }
        Request::Install {
            term,
            index,
            last_term,
            state,
            commit: _,
        } => {
            if term < fence {
                return Ok(Answer::Now(Response::Fenced { fence }));
            }
            store.set_fence(term);
            let appended = Answer::Held(Held(Response::Appended { last: index }));
            if index <= store.commit() {
                return Ok(appended);
            }
            let keep = index + 1 >= store.start() && store.term(index) == Some(last_term);
            store.install(index, last_term, state, keep);
            // Only the installed state is known to be the sequencer's: the
            // cells kept after it may be an older term's, so the commit
            // point the request carries moves on with the next append.
            store.set_commit(index);
            Ok(appended)
        }
        Request::Seal { term } => {
            if term < fence {
                return Ok(Answer::Now(Response::Fenced { fence }));
            }
            store.set_fence(term);
            let last = store.last();
            let last_term = store.term(last).unwrap_or_default();
            Ok(Answer::Held(Held(Response::Sealed { last, last_term })))
        }
    }
}

/// Requests and where they go.
pub type Sends<R, V, T> = Vec<(R, Request<V, T>)>;

/// A started sequencer and its first sends.
type Started<R, V, S> = (Sequencer<R>, Sends<R, V, <S as Store<V>>::State>);

/// A proposal's position and what it sends.
type Appended<R, V, S> = (Pos, Sends<R, V, <S as Store<V>>::State>);

/// One replica's progress, as its sequencer sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
struct Progress {
    /// The next position to send it.
    next: Pos,
    /// The last position it matches.
    matched: Pos,
    /// The commit point last sent to it.
    told: Pos,
    /// The first position of the last append sent to it.
    sent: Pos,
}

/// What a sequencer does after a replica's answer to an append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Acked {
    /// The replica matches more: the commit point may move.
    Advance,
    /// Send the replica more, from its next position.
    Resend,
    /// A higher term is open or sealed.
    Deposed,
    /// Nothing to do.
    Ignore,
}

impl Progress {
    /// Takes the replica's answer to an append or install of this term.
    fn on_response(&mut self, response: Response) -> Acked {
        match response {
            Response::Appended { last } => {
                self.matched = self.matched.max(last);
                self.next = self.next.max(last + 1);
                Acked::Advance
            }
            Response::Behind { last } => {
                // Sent from an earlier position than this already: it
                // was a reply to an append overtaken since.
                let next = (last + 1).max(self.matched + 1);
                if next >= self.sent {
                    return Acked::Ignore;
                }
                self.next = next;
                Acked::Resend
            }
            Response::Fenced { .. } => Acked::Deposed,
            Response::Sealed { .. } | Response::Open(_) => Acked::Ignore,
        }
    }
}

/// The new commit point of a sequencer of `term` at `commit`, whose
/// replicas match `matched`: the highest position a majority holds, if it
/// is past `commit` and of `term` (`term_at` gives a position's term).
fn commit_point(
    matched: impl IntoIterator<Item = Pos>,
    commit: Pos,
    term: Term,
    term_at: impl Fn(Pos) -> Option<Term>,
) -> Option<Pos> {
    let mut matched: Vec<Pos> = matched.into_iter().collect();
    matched.sort_unstable_by(|a, b| b.cmp(a));
    let n = *matched.get(majority(matched.len()).checked_sub(1)?)?;
    (n > commit && term_at(n) == Some(term)).then_some(n)
}

/// Positions newly committed: `from..=to`.
pub type Committed = Option<(Pos, Pos)>;

/// The sequencer of one term, on the node `me`, writing to that node's
/// store (the methods take it). It sends nothing to `me`: its own copy
/// counts through [`written`](Self::written), once synced.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Sequencer<R> {
    term: Term,
    me: R,
    /// The replicas it writes to and counts, sorted.
    replicas: Vec<R>,
    commit: Pos,
    window: Pos,
    /// Each replica's progress, by replica.
    progress: BTreeMap<R, Progress>,
    deposed: bool,
}

impl<R: Id> Sequencer<R> {
    /// The sequencer of `term` on `replicas`, on `me` (one of them), whose
    /// store is durable up to `written`. It appends at most `window`
    /// positions past the commit point, and starts the term with a no-op.
    /// None if the store's fence is past `term`.
    fn new<V, S: Store<V>>(
        term: Term,
        me: R,
        replicas: Vec<R>,
        store: &mut S,
        written: Pos,
        window: Pos,
    ) -> Option<Started<R, V, S>> {
        if store.fence() > term {
            return None;
        }
        store.set_fence(term);
        let next = store.last() + 1;
        let start = Progress {
            next,
            ..Progress::default()
        };
        let progress = replicas.iter().map(|&r| (r, start)).collect();
        let mut seq = Sequencer {
            term,
            me,
            replicas,
            commit: store.commit(),
            window,
            progress,
            deposed: false,
        };
        store.write(next, vec![Cell { term, value: None }]);
        if let Some(p) = seq.progress.get_mut(&me) {
            p.matched = written.min(next - 1);
            p.next = next + 1;
        }
        let sends = seq.send_all(store, |_| true);
        Some((seq, sends))
    }

    /// The replicas it writes to.
    #[must_use]
    fn replicas(&self) -> &[R] {
        &self.replicas
    }

    /// Its term.
    #[must_use]
    fn term(&self) -> Term {
        self.term
    }

    /// The highest position it knows committed.
    #[must_use]
    fn commit(&self) -> Pos {
        self.commit
    }

    /// Whether it still holds its term on its store: no replica refused it
    /// and no higher term reached its node.
    #[must_use]
    fn live<V>(&self, store: &impl Store<V>) -> bool {
        !self.deposed && store.fence() == self.term
    }

    /// Whether it has committed a cell of its own term, so its commit point
    /// covers everything committed before the term (Raft's rule for read
    /// indexes). Until then it serves no reads.
    #[must_use]
    fn ready<V>(&self, store: &impl Store<V>) -> bool {
        self.live(store) && store.term(self.commit) == Some(self.term)
    }

    /// Whether a replica refused it for a higher term.
    #[must_use]
    fn deposed(&self) -> bool {
        self.deposed
    }

    /// A replica's progress.
    #[must_use]
    fn progress(&self, r: R) -> Progress {
        self.progress.get(&r).copied().unwrap_or_default()
    }

    /// Whether `r`, another of its replicas, lacks cells or the commit
    /// point.
    #[must_use]
    fn lagging<V>(&self, store: &impl Store<V>, r: R) -> bool {
        let p = self.progress(r);
        r != self.me
            && self.replicas.contains(&r)
            && (p.matched < store.last() || p.told < self.commit)
    }

    fn send_all<V, S: Store<V>>(
        &mut self,
        store: &S,
        which: impl Fn(&Progress) -> bool,
    ) -> Sends<R, V, S::State> {
        let to: Vec<R> = (self.replicas.iter().copied())
            .filter(|&r| r != self.me && which(&self.progress(r)))
            .collect();
        to.into_iter()
            .filter_map(|r| Some((r, self.send(store, r)?)))
            .collect()
    }

    /// What to send `r` from its next position on.
    fn send<V, S: Store<V>>(&mut self, store: &S, r: R) -> Option<Request<V, S::State>> {
        if r == self.me || !self.live(store) {
            return None;
        }
        let (term, commit) = (self.term, self.commit);
        let p = self.progress.get_mut(&r)?;
        p.told = commit;
        if p.next < store.start() {
            let (index, last_term, state) = store.snapshot()?;
            p.sent = p.next;
            p.next = index + 1;
            return Some(Request::Install {
                term,
                index,
                last_term,
                state,
                commit,
            });
        }
        let prev = p.next - 1;
        let prev_term = store.term(prev)?;
        let cells = store.cells(p.next)?;
        p.sent = p.next;
        p.next = prev + count(cells.len()) + 1;
        Some(Request::Append(Append {
            term,
            prev,
            prev_term,
            cells,
            commit,
        }))
    }

    /// Whether [`append`](Self::append) would take a value.
    #[must_use]
    fn open<V>(&self, store: &impl Store<V>) -> bool {
        self.live(store) && store.last() < self.commit + self.window
    }

    /// Appends `value` and sends it to the replicas that have been sent
    /// every cell before it: its position, and the sends. None if the
    /// window is full or the term is over.
    fn append<V, S: Store<V>>(&mut self, store: &mut S, value: V) -> Option<Appended<R, V, S>> {
        if !self.open(store) {
            return None;
        }
        let pos = store.last() + 1;
        store.write(
            pos,
            vec![Cell {
                term: self.term,
                value: Some(value),
            }],
        );
        if let Some(p) = self.progress.get_mut(&self.me) {
            p.next = pos + 1;
        }
        let sends = self.send_all(store, |p| p.next == pos);
        Some((pos, sends))
    }

    /// Takes `r`'s answer to an append or install: the positions newly
    /// committed and what to send `r` next.
    fn on_response<V, S: Store<V>>(
        &mut self,
        store: &mut S,
        r: R,
        response: Response,
    ) -> (Committed, Sends<R, V, S::State>) {
        if r == self.me {
            return (None, Vec::new());
        }
        let Some(p) = self.progress.get_mut(&r) else {
            return (None, Vec::new());
        };
        match p.on_response(response) {
            Acked::Advance => (self.advance(store), Vec::new()),
            Acked::Resend => {
                let sends = self.send(store, r).map(|q| (r, q)).into_iter().collect();
                (None, sends)
            }
            Acked::Deposed => {
                self.deposed = true;
                (None, Vec::new())
            }
            Acked::Ignore => (None, Vec::new()),
        }
    }

    /// Its own node's store is durable up to `upto` (see [`Synced`]).
    fn written<V>(&mut self, store: &mut impl Store<V>, upto: Pos, _: &Synced) -> Committed {
        if !self.live(store) {
            return None;
        }
        let me = self.me;
        let p = self.progress.get_mut(&me)?;
        p.matched = p.matched.max(upto.min(store.last()));
        self.advance(store)
    }

    /// Moves the commit point to the highest position a quorum matches, if
    /// it is of this term, on its node's store too.
    fn advance<V>(&mut self, store: &mut impl Store<V>) -> Committed {
        if !self.live(store) {
            return None;
        }
        let matched = (self.replicas.iter()).map(|&r| self.progress(r).matched);
        let n = commit_point(matched, self.commit, self.term, |p| store.term(p))?;
        let from = self.commit + 1;
        self.commit = n;
        if n > store.commit() {
            store.set_commit(n);
        }
        Some((from, n))
    }

    /// Sends `r` everything after the last position it matched, and the
    /// commit point: after a lost message, or to tell it of commits.
    fn retry<V, S: Store<V>>(&mut self, store: &S, r: R) -> Sends<R, V, S::State> {
        if let Some(p) = self.progress.get_mut(&r) {
            p.next = p.matched + 1;
        }
        self.send(store, r).map(|q| (r, q)).into_iter().collect()
    }

    /// Sends `r`, if it was not sent the commit point, what follows what
    /// it was sent, with the commit point.
    fn tell<V, S: Store<V>>(&mut self, store: &S, r: R) -> Sends<R, V, S::State> {
        if r == self.me || !self.replicas.contains(&r) || self.progress(r).told >= self.commit {
            return Vec::new();
        }
        self.send(store, r).map(|q| (r, q)).into_iter().collect()
    }

    /// Renames the replicas by `f`, for symmetry reduction in model
    /// checking.
    fn relabel(&mut self, f: impl Fn(R) -> R) {
        self.progress = (self.progress.iter()).map(|(&r, &p)| (f(r), p)).collect();
        self.me = f(self.me);
        rename_all(&f, &mut self.replicas);
    }
}

/// The replicas of `set` renamed by `f`, sorted.
pub fn rename_all<R: Id>(f: impl Fn(R) -> R, set: &mut [R]) {
    for r in set.iter_mut() {
        *r = f(*r);
    }
    set.sort_unstable();
}

/// What a machine does next.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Next<R> {
    /// Waits for more answers.
    Wait,
    /// Appoint one of `candidates` for `term`, by compare-and-swap on the
    /// config.
    Appoint {
        /// The new term.
        term: Term,
        /// The replicas whose logs are the most up to date: any of them
        /// may sequence it.
        candidates: Vec<R>,
    },
    /// A read: everything up to this position is what it reads.
    Done(Pos),
    /// A higher term came first.
    Failed,
}

/// A seal answer: the term and position of a replica's last cell, and the
/// replica. Sorted, the last is the most up to date.
pub type Answered<R> = (Term, Pos, R);

/// The controller's change of sequencer, from `term` to the next.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Reconfigure<R> {
    term: Term,
    /// The sets of replicas it needs a quorum of each of, sorted.
    quorums: Vec<Vec<R>>,
    /// The replicas of the new term: the appointee is one of them.
    replicas: Vec<R>,
    answers: Vec<Answered<R>>,
    done: bool,
}

impl<R: Id> Reconfigure<R> {
    /// Seals `term + 1` for a term on `replicas`, on the config's
    /// replicas, and during a replica change (`joint`, the replicas it
    /// changes from) on those too. For a new replica change, `replicas`
    /// are the new ones and `joint` the current: the change seal goes to
    /// the replicas it changes from.
    #[must_use]
    pub fn start<V, T>(
        term: Term,
        current: &[R],
        joint: &[R],
        replicas: Vec<R>,
    ) -> (Self, Sends<R, V, T>) {
        let mut quorums = vec![current.to_vec()];
        if !joint.is_empty() {
            quorums.push(joint.to_vec());
        }
        let mut to: Vec<R> = quorums.iter().flatten().copied().collect();
        to.sort_unstable();
        to.dedup();
        let m = Reconfigure {
            term,
            quorums,
            replicas,
            answers: Vec::new(),
            done: false,
        };
        let sends = (to.into_iter())
            .map(|r| (r, Request::Seal { term: term + 1 }))
            .collect();
        (m, sends)
    }

    /// The replicas of the new term.
    #[must_use]
    pub fn replicas(&self) -> &[R] {
        &self.replicas
    }

    /// The term it replaces.
    #[must_use]
    pub fn base(&self) -> Term {
        self.term
    }

    /// Whether it still waits for `r`'s answer.
    #[must_use]
    pub fn pending(&self, r: R) -> bool {
        !self.done && self.answers.iter().all(|a| a.2 != r)
    }

    /// Renames the replicas by `f` (see [`Node::relabel`]).
    pub fn relabel(&mut self, f: impl Fn(R) -> R) {
        for a in &mut self.answers {
            a.2 = f(a.2);
        }
        if !self.done {
            self.answers.sort_unstable();
        }
        for q in &mut self.quorums {
            rename_all(&f, q);
        }
        rename_all(&f, &mut self.replicas);
    }

    /// Takes `r`'s answer to the seal. Once a majority of every sealed set
    /// answered, the candidates are the most up-to-date answers among the
    /// new term's replicas; `eligible` leaves out replicas that may not
    /// sequence (the ledger's: not members). Their answers count toward
    /// nothing.
    pub fn on_seal(&mut self, r: R, response: Response, eligible: impl Fn(R) -> bool) -> Next<R> {
        if self.done {
            return Next::Wait;
        }
        let Response::Sealed { last, last_term } = response else {
            self.done = true;
            return Next::Failed;
        };
        if eligible(r) && self.pending(r) {
            self.answers.push((last_term, last, r));
            self.answers.sort_unstable();
        }
        let answered: Vec<R> = self.answers.iter().map(|a| a.2).collect();
        if !reached(&self.quorums, &answered) {
            return Next::Wait;
        }
        self.done = true;
        let mut candidates = most_up_to_date(&self.answers);
        candidates.retain(|r| self.replicas.contains(r));
        Next::Appoint {
            term: self.term + 1,
            candidates,
        }
    }
}

/// Whether the replicas in `answered` hold a majority of every set in
/// `quorums`.
fn reached<R: PartialEq>(quorums: &[Vec<R>], answered: &[R]) -> bool {
    quorums.iter().all(|q| {
        let n = answered.iter().filter(|r| q.contains(r)).count();
        n >= majority(q.len())
    })
}

/// The replicas whose seal answers, by (last term, last position), are the
/// most up to date: Raft's election restriction.
fn most_up_to_date<R: Copy>(answers: &[Answered<R>]) -> Vec<R> {
    let Some(best) = answers.iter().map(|a| (a.0, a.1)).max() else {
        return Vec::new();
    };
    (answers.iter())
        .filter(|a| (a.0, a.1) == best)
        .map(|a| a.2)
        .collect()
}

/// A read round at a node's sequencer: a linearizable read of everything
/// up to the sequencer's commit point, once a quorum says its term is
/// open. The sequencer's own node answers too (through [`replica`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Round<R> {
    id: u64,
    index: Pos,
    /// The sequencer's replicas, which it checks with.
    replicas: Vec<R>,
    open: Vec<R>,
    /// Whether its checks went out: no reader joins it after.
    sent: bool,
}

impl<R: Id> Round<R> {
    /// Takes `seq`'s commit point; its term is then checked on every
    /// replica. None until the sequencer is [ready](Sequencer::ready).
    #[must_use]
    fn start<V, S: Store<V>>(id: u64, seq: &Sequencer<R>, store: &S) -> Option<Self> {
        if !seq.ready(store) {
            return None;
        }
        Some(Round {
            id,
            index: seq.commit(),
            replicas: seq.replicas().to_vec(),
            open: Vec::new(),
            sent: false,
        })
    }

    /// Renames the replicas by `f`.
    fn relabel(&mut self, f: impl Fn(R) -> R) {
        rename_all(&f, &mut self.open);
        rename_all(&f, &mut self.replicas);
    }

    /// Whether its checks went out and it still waits for `r`'s.
    #[must_use]
    fn pending(&self, r: R) -> bool {
        self.sent && !self.open.contains(&r)
    }

    /// Takes `r`'s answer to the check: whether `r` says the term is open.
    /// Done once a quorum does; failed once any does not.
    fn on_check(&mut self, r: R, open: bool) -> Next<R> {
        if !open {
            return Next::Failed;
        }
        if !self.open.contains(&r) && self.replicas.contains(&r) {
            self.open.push(r);
        }
        if self.open.len() < majority(self.replicas.len()) {
            return Next::Wait;
        }
        Next::Done(self.index)
    }
}

/// A message between nodes of a log, and to and from its controller.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg<V, T> {
    /// To a replica: a sequencer's request.
    Request(Request<V, T>),
    /// To a sequencer: a replica's answer to an append or install of
    /// `term`.
    Ack {
        /// The term it answers.
        term: Term,
        /// The answer.
        response: Response,
    },
    /// To the controller: a replica's answer to a seal of `term`. Refused
    /// if `fence` is past `term`.
    Sealed {
        /// The term sealed.
        term: Term,
        /// The replica's last position.
        last: Pos,
        /// Its term.
        last_term: Term,
        /// The replica's fence.
        fence: Term,
    },
    /// To a replica: whether `term` is still open, for read round `round`.
    Check {
        /// The reader's term.
        term: Term,
        /// The round.
        round: u64,
    },
    /// To a reader: the answer to a check.
    Open {
        /// The round.
        round: u64,
        /// Whether the term is open.
        open: bool,
    },
}

/// What a node's caller does after an input.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Effects<R, V, T> {
    /// Messages to send now.
    pub sends: Vec<(R, Msg<V, T>)>,
    /// Positions newly committed by this node's sequencer.
    pub committed: Committed,
    /// Read rounds that ended: everything up to the position is what they
    /// read, or none if they failed (a higher term came).
    pub reads: Vec<(u64, Option<Pos>)>,
    /// The sequencer here was refused for a higher term the root may not
    /// know of yet: tell the root.
    pub deposed: Option<Term>,
}

/// A proposal's position and the node's effects.
pub type Proposed<R, V, S> = (Pos, Effects<R, V, <S as Store<V>>::State>);

impl<R, V, T> Default for Effects<R, V, T> {
    fn default() -> Self {
        Effects {
            sends: Vec::new(),
            committed: None,
            reads: Vec::new(),
            deposed: None,
        }
    }
}

impl<R, V, T> Effects<R, V, T> {
    fn commit(&mut self, c: Committed) {
        self.committed = match (self.committed, c) {
            (Some((a, _)), Some((_, b))) => Some((a, b)),
            (a, b) => a.or(b),
        };
    }

    fn requests(&mut self, sends: Sends<R, V, T>) {
        self.sends
            .extend(sends.into_iter().map(|(r, q)| (r, Msg::Request(q))));
    }
}

/// One node's part in one log: its replica, its sequencer while it holds
/// the term, and its read rounds, over its store. The roles are hidden:
/// a caller feeds in what arrives and carries out the [`Effects`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node<R, S> {
    me: R,
    store: S,
    window: Pos,
    seq: Option<Sequencer<R>>,
    rounds: Vec<Round<R>>,
    next_round: u64,
    /// Answers waiting for the store's next sync.
    held: Vec<(R, Held, Reply)>,
}

impl<R: Id, S> Node<R, S> {
    /// Node `me`, on `store` as it is on disk (after a restart, too).
    /// Its sequencer appends at most `window` positions past the commit
    /// point.
    pub fn new(me: R, store: S, window: Pos) -> Self {
        Node {
            me,
            store,
            window,
            seq: None,
            rounds: Vec::new(),
            next_round: 0,
            held: Vec::new(),
        }
    }

    /// Its store.
    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Its store, for writes that are not the protocol's: the caller's
    /// own bookkeeping, and syncing it. The fence, the commit point and
    /// the cells are the node's to change.
    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    /// Renames the replicas by `f`, for symmetry reduction in model
    /// checking.
    pub fn relabel(&mut self, f: impl Fn(R) -> R) {
        self.me = f(self.me);
        if let Some(s) = &mut self.seq {
            s.relabel(&f);
        }
        for round in &mut self.rounds {
            round.relabel(&f);
        }
        for h in &mut self.held {
            h.0 = f(h.0);
        }
        self.held.sort_unstable();
    }
}

impl<R: Id, S> Node<R, S> {
    /// The term it sequences, if it still holds it.
    #[must_use]
    pub fn sequencing<V>(&self) -> Option<Term>
    where
        S: Store<V>,
    {
        self.seq
            .as_ref()
            .filter(|s| s.live(&self.store))
            .map(Sequencer::term)
    }

    /// Whether it sequences the log and its commit point covers every
    /// earlier term: it may take proposals and reads.
    #[must_use]
    pub fn leading<V>(&self) -> bool
    where
        S: Store<V>,
    {
        self.seq.as_ref().is_some_and(|s| s.ready(&self.store))
    }

    /// Whether its sequencer may take a proposal now (its window has
    /// room).
    #[must_use]
    pub fn open<V>(&self) -> bool
    where
        S: Store<V>,
    {
        self.seq.as_ref().is_some_and(|s| s.open(&self.store))
    }

    /// Its sequencer's commit point, or its replica's.
    #[must_use]
    pub fn commit<V>(&self) -> Pos
    where
        S: Store<V>,
    {
        self.seq
            .as_ref()
            .map_or(self.store.commit(), Sequencer::commit)
            .max(self.store.commit())
    }

    /// The other replicas its sequencer has not brought up to date.
    #[must_use]
    pub fn lagging<V>(&self) -> Vec<R>
    where
        S: Store<V>,
    {
        let Some(s) = &self.seq else {
            return Vec::new();
        };
        (s.replicas().iter().copied())
            .filter(|&r| s.lagging(&self.store, r))
            .collect()
    }

    /// The least position every replica matches, while it sequences:
    /// cells from there may still be sent.
    #[must_use]
    pub(crate) fn slowest(&self) -> Option<Pos> {
        let s = self.seq.as_ref()?;
        (s.replicas.iter()).map(|&r| s.progress(r).matched).min()
    }

    /// The replicas its sequencer writes to.
    #[must_use]
    pub fn replicas(&self) -> Option<&[R]> {
        self.seq.as_ref().map(Sequencer::replicas)
    }

    /// The term of a sequencer here that lost it: a replica refused it, a
    /// check found it closed, or a higher term reached this node.
    #[must_use]
    pub fn deposed<V>(&self) -> Option<Term>
    where
        S: Store<V>,
    {
        (self.seq.as_ref())
            .filter(|s| !s.live(&self.store))
            .map(Sequencer::term)
    }

    /// Whether it holds answers for the next sync.
    #[must_use]
    pub fn holds(&self) -> bool {
        !self.held.is_empty()
    }

    /// Read rounds whose checks still wait for `r`.
    pub fn waiting(&self, r: R) -> impl Iterator<Item = u64> + '_ {
        (self.rounds.iter())
            .filter(move |x| x.pending(r))
            .map(|x| x.id)
    }

    /// A restart: the store is back as it was at its last sync (the caller
    /// does that), and nothing in memory survives. Its owner applied the
    /// positions up to `applied`, so they are committed, whether or not
    /// the commit point saying so was synced. If `sole`, this node is the
    /// log's only replica (its config not joint), so every cell it holds is
    /// committed too.
    pub fn restart<V>(&mut self, applied: Pos, sole: bool)
    where
        S: Store<V>,
    {
        self.seq = None;
        self.rounds.clear();
        self.held.clear();
        let held = if sole { self.store.last() } else { applied };
        let floor = held.max(applied).min(self.store.last());
        if floor > self.store.commit() {
            self.store.set_commit(floor);
        }
    }

    /// Takes a message from `from`.
    ///
    /// # Errors
    ///
    /// [`Broken`] if a request would drop a committed position: the node
    /// must stop.
    pub fn step<V>(
        &mut self,
        from: R,
        msg: Msg<V, S::State>,
    ) -> Result<Effects<R, V, S::State>, Broken>
    where
        S: Store<V>,
    {
        let mut fx = Effects::default();
        match msg {
            Msg::Request(request) => {
                let reply = match &request {
                    Request::Append(a) => Reply::Ack(a.term),
                    Request::Install { term, .. } => Reply::Ack(*term),
                    Request::Seal { term } => Reply::Seal(*term),
                };
                let answer = replica(&mut self.store, request)?;
                self.after_fence(&mut fx);
                match answer {
                    Answer::Now(response) => fx.sends.push((from, reply.msg(response))),
                    Answer::Held(h) => {
                        // The newer answer covers the older one: both wait for
                        // the same sync, and it reports the store as it is now.
                        self.held.retain(|(to, _, r)| (*to, *r) != (from, reply));
                        self.held.push((from, h, reply));
                    }
                }
            }
            Msg::Ack { term, response } => {
                if let Some(s) = self.seq.as_mut().filter(|s| s.term() == term) {
                    let (c, sends) = s.on_response(&mut self.store, from, response);
                    fx.deposed = s.deposed().then_some(term);
                    fx.commit(c);
                    fx.requests(sends);
                }
            }
            Msg::Check { term, round } => {
                let open = self.store.fence() <= term;
                fx.sends.push((from, Msg::Open { round, open }));
            }
            Msg::Open { round, open } => self.on_open(from, round, open, &mut fx),
            Msg::Sealed { .. } => {}
        }
        Ok(fx)
    }

    /// The store's writes so far are durable, up to position `written`:
    /// releases the answers that waited for them.
    pub fn synced<V>(&mut self, synced: &Synced, written: Pos) -> Effects<R, V, S::State>
    where
        S: Store<V>,
    {
        let mut fx = Effects::default();
        for (to, h, reply) in std::mem::take(&mut self.held) {
            fx.sends.push((to, reply.msg(h.release(synced))));
        }
        if let Some(s) = &mut self.seq {
            fx.commit(s.written(&mut self.store, written, synced));
        }
        fx
    }

    /// The root appointed this node to sequence `term` on `replicas`; its
    /// store is durable up to `written`. It starts the term with a no-op.
    pub fn appoint<V>(
        &mut self,
        term: Term,
        replicas: Vec<R>,
        written: Pos,
    ) -> Effects<R, V, S::State>
    where
        S: Store<V>,
    {
        let mut fx = Effects::default();
        if self.seq.as_ref().is_some_and(|s| s.term() >= term) {
            return fx;
        }
        self.fail_reads(&mut fx);
        let me = self.me;
        let Some((seq, sends)) =
            Sequencer::new(term, me, replicas, &mut self.store, written, self.window)
        else {
            return fx;
        };
        self.seq = Some(seq);
        fx.requests(sends);
        fx
    }

    /// Appends `value` at its sequencer: its position, if it holds its
    /// term and its window has room.
    pub fn propose<V>(&mut self, value: V) -> Option<Proposed<R, V, S>>
    where
        S: Store<V>,
    {
        let s = self.seq.as_mut()?;
        let (pos, sends) = s.append(&mut self.store, value)?;
        let mut fx = Effects::default();
        fx.requests(sends);
        Some((pos, fx))
    }

    /// Resends to replica `r` what it lacks (after a lost message, or to
    /// tell it of commits).
    pub fn retry<V>(&mut self, r: R) -> Effects<R, V, S::State>
    where
        S: Store<V>,
    {
        let mut fx = Effects::default();
        if let Some(s) = &mut self.seq {
            fx.requests(s.retry(&self.store, r));
        }
        fx
    }

    /// Tells replica `r` the commit point, if it was not sent it, with
    /// what follows what it was sent: as a resend would, without going
    /// back.
    pub(crate) fn tell<V>(&mut self, r: R) -> Effects<R, V, S::State>
    where
        S: Store<V>,
    {
        let mut fx = Effects::default();
        if let Some(s) = &mut self.seq {
            fx.requests(s.tell(&self.store, r));
        }
        fx
    }

    /// A read: the round it waits on, which ends in
    /// [`Effects::reads`]. None unless this node [leads](Self::leading).
    /// It joins a round whose checks have not gone out if the commit
    /// point has not moved since the round took it.
    pub fn read<V>(&mut self) -> Option<u64>
    where
        S: Store<V>,
    {
        let s = self.seq.as_ref()?;
        if let Some(r) = (self.rounds.iter()).find(|r| !r.sent && r.index == s.commit()) {
            return Some(r.id);
        }
        let id = self.next_round;
        let round = Round::start::<V, S>(id, s, &self.store)?;
        self.next_round += 1;
        self.rounds.push(round);
        Some(id)
    }

    /// Sends the checks of the rounds that have not sent them: no reader
    /// joins those after. Its own replica answers at once.
    pub fn send_checks<V>(&mut self) -> Effects<R, V, S::State>
    where
        S: Store<V>,
    {
        let mut fx = Effects::default();
        let Some(term) = self.sequencing() else {
            self.fail_reads(&mut fx);
            return fx;
        };
        let replicas = self.replicas().map(<[R]>::to_vec).unwrap_or_default();
        let ids: Vec<u64> = (self.rounds.iter().filter(|r| !r.sent))
            .map(|r| r.id)
            .collect();
        for id in ids {
            if let Some(r) = self.rounds.iter_mut().find(|r| r.id == id) {
                r.sent = true;
            }
            let others = replicas.iter().filter(|&&to| to != self.me);
            fx.sends
                .extend(others.map(|&to| (to, Msg::Check { term, round: id })));
            if replicas.contains(&self.me) {
                let open = self.store.fence() <= term;
                self.on_open(self.me, id, open, &mut fx);
            }
        }
        fx
    }

    /// Resends the checks that still wait for `r`.
    pub fn retry_checks<V>(&mut self, r: R) -> Effects<R, V, S::State>
    where
        S: Store<V>,
    {
        let mut fx = Effects::default();
        let Some(term) = self.sequencing() else {
            self.fail_reads(&mut fx);
            return fx;
        };
        let rounds: Vec<u64> = self.waiting(r).collect();
        for round in rounds {
            fx.sends.push((r, Msg::Check { term, round }));
        }
        fx
    }

    fn on_open<V, T>(&mut self, from: R, round: u64, open: bool, fx: &mut Effects<R, V, T>) {
        let Some(i) = self.rounds.iter().position(|r| r.id == round && r.sent) else {
            return;
        };
        let Some(slot) = self.rounds.get_mut(i) else {
            return;
        };
        match slot.on_check(from, open) {
            Next::Done(index) => {
                self.rounds.remove(i);
                fx.reads.push((round, Some(index)));
            }
            Next::Failed => {
                // A replica's fence is past the term: it is over here too.
                if let Some(s) = &mut self.seq {
                    s.deposed = true;
                    fx.deposed = Some(s.term);
                }
                self.fail_reads(fx);
            }
            Next::Wait | Next::Appoint { .. } => {}
        }
    }

    fn fail_reads<V, T>(&mut self, fx: &mut Effects<R, V, T>) {
        fx.reads.extend(self.rounds.drain(..).map(|r| (r.id, None)));
    }

    /// After a request: a higher fence ends this node's term here.
    fn after_fence<V, T>(&mut self, fx: &mut Effects<R, V, T>)
    where
        S: Store<V>,
    {
        if self.seq.as_ref().is_some_and(|s| !s.live(&self.store)) {
            self.fail_reads(fx);
        }
    }

    /// Trims its log below `below`, but not past its commit point: whether
    /// the start moved.
    pub fn trim<V>(&mut self, below: Pos) -> bool
    where
        S: Store<V>,
    {
        let below = below
            .min(self.store.commit() + 1)
            .min(self.store.last() + 1);
        if below <= self.store.start() {
            return false;
        }
        self.store.trim(below);
        true
    }
}

/// Whom a replica's answer goes to: the sequencer of a term, or the
/// controller for a seal of a term.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Reply {
    Ack(Term),
    Seal(Term),
}

impl Reply {
    fn msg<V, T>(self, response: Response) -> Msg<V, T> {
        match (self, response) {
            (Reply::Seal(term), Response::Sealed { last, last_term }) => Msg::Sealed {
                term,
                last,
                last_term,
                fence: term,
            },
            (Reply::Seal(term), Response::Fenced { fence }) => Msg::Sealed {
                term,
                last: 0,
                last_term: 0,
                fence,
            },
            (Reply::Ack(term) | Reply::Seal(term), response) => Msg::Ack { term, response },
        }
    }
}
