//! The apply thread: every log's committed entries are applied to the
//! state store here, off the event loop, so a round's consensus work and
//! its fsync never wait for the state machine (as `TiKV` splits its raft and
//! apply pools).
//!
//! The loop hands over jobs in order through one queue, so a log's entries,
//! a snapshot that replaces its state, and its deletion apply in log order.
//! Everything that depends on the applied state happens here: answering
//! proposers, notifying subscribers (a new one is registered here, between
//! two jobs, so it sees the state and then every change after it), and
//! publishing each log's [`Progress`] to readers. The loop hears back which
//! logs applied entries ([`Input::Applied`], for read barriers and the
//! ledger's events) and what is durable ([`Input::Durable`], for log
//! truncation).
//!
//! **Durability.** Writes are not durable as they are applied: the store
//! has no write-ahead log, and a crash loses what it did not flush (the
//! log replays it). Every [`SYNC_EVERY`] the thread lets the store
//! flush if it holds enough, and on a [`Job::Sync`] (the loop asks when it
//! started logs from states just written: new logs, or ones installed from
//! a snapshot, whose log writes wait for them) it flushes now. The store
//! writes a flush on a thread of its own, so this thread never waits for
//! one. Once a flush is durable, what it covers is reported: for
//! every log written since the previous flush, the index the store has
//! durably applied and its oldest pinned inline layer *as of the flush*: a
//! log may be truncated up to the index, but not past that pin. (Pins as of
//! the store's current state would not do: a flush applied but not yet
//! durable has dropped a pin that a crash would bring back.)
//!
//! **Evaluation.** Proposals are evaluated here too, so they read the
//! applied state in log order with the entries: a run of them in one round
//! that is never committed, over the store and the changes this node
//! evaluated that are still in flight for the log (its *chain*), so a
//! repository's proposals pipeline without waiting for each other to
//! apply. An effect that applies ends its proposal; one that is skipped
//! (the state moved: another node's change landed first) is evaluated
//! again, from a new chain.
//!
//! **Read pipelining.** Before evaluating a run of proposals, the thread
//! reads every ref and counter they will read in one go, from several
//! threads ([`crate::state::store::Round::prefetch`]), so they then run
//! against the block cache instead of waiting on the disk one read at a
//! time. Applying reads only counters (and the layers a change removes).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use parking_lot::Mutex;
use prost::Message as _;
use tokio::sync::{mpsc, oneshot};

use enroute_shared::proto::{Update, update};
use enroute_shared::roles::NotHosted;
use enroute_shared::state::{
    Change, Command, LayerEdit, LayerMeta, Outcome, RemovedLayer, RepoMeta, apply_layer_edit,
    command,
};

use crate::logdb::LogDb;
use crate::proto::{Effect, StoredCell};
use crate::state::machine::{self, State as _, count};
use crate::state::store::{Flushed, Root, Round, StateStore};
use crate::state::stored::{self, Stored};

use super::engine::Input;
use super::repo::effect;
use super::repo::load;
use super::stats::{Window, pct, per, rate};
use super::transport::{Addresses, url_of};

/// How often the store is synced (when it was written), and what is
/// durable reported: how far behind a log's truncation may lag.
const SYNC_EVERY: Duration = Duration::from_millis(100);

/// Jobs taken in one batch, at most.
const BATCH: usize = 4096;

/// How soon to look again at subscribers that fell behind, while some do.
const RESYNC_POLL: Duration = Duration::from_millis(20);

/// Evaluations of one proposal before it gives up: each is a log entry
/// that another node's change overtook.
const TRIES: u32 = 8;

/// A command on its way through the log: what its proposer is told once
/// its effect applies, or what is evaluated again if it is skipped.
pub(super) struct Proposal {
    cmd: Command,
    pub(super) resp: oneshot::Sender<Result<Outcome>>,
    /// Its outcome, as evaluated.
    pub(super) outcome: Outcome,
    /// Evaluations so far.
    pub(super) tries: u32,
}

impl Proposal {
    pub(super) fn new(cmd: Command, resp: oneshot::Sender<Result<Outcome>>) -> Proposal {
        Proposal {
            cmd,
            resp,
            outcome: Outcome::default(),
            tries: 0,
        }
    }

    /// The proposal, if it may be evaluated (again): else its proposer
    /// is told it was tried enough.
    pub(super) fn again(self) -> Option<Proposal> {
        if self.tries < TRIES {
            return Some(self);
        }
        let e = anyhow::anyhow!("the repository kept changing under the command: retry");
        drop(self.resp.send(Err(e)));
        None
    }
}

/// The effects this node evaluated for a log that are not applied or
/// skipped yet, in order: what its next proposal is evaluated after.
struct Chain {
    id: u64,
    inflight: VecDeque<Effect>,
}

/// How far a log's state is applied. The apply thread publishes it before
/// it answers proposers, so a proposer sees its own change.
#[derive(Clone, Copy, Default)]
pub(super) struct Progress {
    pub(super) version: u64,
    pub(super) applied: u64,
    pub(super) deleted: bool,
}

/// What the loop asks of the apply thread, in log order.
pub(super) enum Job {
    /// Apply committed entries; answer the proposers of those whose index
    /// is in `replies`.
    Apply {
        log: u64,
        cells: Vec<StoredCell>,
        replies: HashMap<u64, Proposal>,
    },
    /// Evaluate a proposal, and hand its entry to the loop.
    Evaluate { log: u64, proposal: Proposal },
    /// A new log, from its initial state; its progress is published in
    /// `progress`.
    Host {
        log: u64,
        root: Root,
        meta: RepoMeta,
        progress: Arc<Mutex<Progress>>,
    },
    /// Replace a hosted log's state with one installed from the sequencer,
    /// which subscribers then start from.
    Install {
        log: u64,
        root: Root,
        meta: RepoMeta,
    },
    /// An entry evaluated for the log never reached it (a new term
    /// truncated it): forget its in-flight effects, so the next evaluation
    /// starts from the applied state.
    Forget(u64),
    /// The log is no longer hosted: delete its state, end its subscriptions.
    Drop(u64),
    /// Subscribe to a log: its state, then every change.
    Follow {
        log: u64,
        tx: mpsc::Sender<Update>,
        resp: oneshot::Sender<Result<u64>>,
    },
    /// A subscriber of the log went away: forget the closed ones.
    Unfollow(u64),
    /// The log's sequencer is now `sequencer` (0: none): tell its
    /// subscribers.
    Sequencer { log: u64, sequencer: u64 },
    /// Sync the store now (everything queued before), then report these
    /// logs settled ([`Input::Settled`]).
    Sync(Vec<u64>),
}

/// An update naming the log's sequencer by its internal URL (empty: none
/// known, or no address).
fn sequencer_update(url: String) -> Update {
    Update {
        kind: Some(update::Kind::Sequencer(url)),
    }
}

fn is_evaluate(job: &Job) -> bool {
    matches!(job, Job::Evaluate { .. })
}

/// An `Evaluate` job's log and proposal.
fn evaluation(job: Job) -> Option<(u64, Proposal)> {
    match job {
        Job::Evaluate { log, proposal } => Some((log, proposal)),
        _ => None,
    }
}

/// What a batch's applied entries leave to publish.
#[derive(Default)]
struct Done {
    deleted: HashSet<u64>,
    replies: Vec<(oneshot::Sender<Result<Outcome>>, Result<Outcome>)>,
}

/// What the thread knows of a hosted log.
struct Log {
    /// Only this thread writes it.
    progress: Arc<Mutex<Progress>>,
    subscribers: Vec<Subscriber>,
    /// Its sequencer, as the loop last reported it (0: none known).
    sequencer: u64,
    /// Its layers as last sent to the subscribers, while it has some: what
    /// a subscriber behind catches up to (see [`Missed`]).
    layers: Option<Vec<LayerMeta>>,
}

/// A subscriber of a log, and what it missed while behind (its queue
/// filled).
struct Subscriber {
    tx: mpsc::Sender<Update>,
    behind: Option<Missed>,
}

/// What a subscriber behind missed. Like an unsynced watcher in etcd, it
/// is caught up later instead of being dropped, and catching up costs what
/// it missed rather than the whole state: its changes merged into one
/// (each ref's latest value, the last of the other fields), with one layer
/// edit from the layers it holds to the log's current ones, and the last
/// sequencer. Only a deletion or a new state takes a whole state.
#[derive(Default)]
struct Missed {
    /// None: it needs a whole state.
    change: Option<Change>,
    /// Where in `change.refs` each ref is.
    refs: HashMap<String, usize>,
    /// The layers it holds (None: not known), and whether it missed a
    /// layer edit.
    held: Option<Vec<RemovedLayer>>,
    layers_changed: bool,
    sequencer: Option<Update>,
}

impl Missed {
    /// Missing changes from `layers` on.
    fn new(layers: Option<&Vec<LayerMeta>>) -> Missed {
        let held = layers.map(|ls| {
            ls.iter()
                .map(|l| RemovedLayer {
                    id: l.id.clone(),
                    first_id: l.first_id,
                })
                .collect()
        });
        Missed {
            change: Some(Change::default()),
            held,
            ..Missed::default()
        }
    }

    fn add(&mut self, u: Update) {
        match u.kind {
            Some(update::Kind::Change(mut c)) => {
                if c.layers.take().is_some() {
                    self.layers_changed = true;
                }
                if self.layers_changed && self.held.is_none() {
                    self.change = None;
                }
                if let Some(into) = &mut self.change
                    && !merge(into, &mut self.refs, c)
                {
                    self.change = None;
                }
            }
            Some(update::Kind::Sequencer(_)) => self.sequencer = Some(u),
            Some(update::Kind::State(_)) | None => self.change = None,
        }
    }

    /// The change that catches it up to `layers`, unless it needs a whole
    /// state.
    fn catch_up(&mut self, layers: Option<&Vec<LayerMeta>>) -> Option<Change> {
        let mut c = self.change.take()?;
        if self.layers_changed {
            c.layers = Some(LayerEdit {
                remove: self.held.take()?,
                insert: layers?.clone(),
            });
        }
        Some(c)
    }
}

/// Merge `c` (with no layer edit) into `into`, the changes before it, with
/// `refs` indexing its refs: applying the result is applying both, in
/// order. False if `c` is a deletion, which a change after it cannot undo.
fn merge(into: &mut Change, refs: &mut HashMap<String, usize>, c: Change) -> bool {
    let Change {
        version,
        refs: changed,
        head,
        layers: _,
        next_id,
        upload_gen,
        deleted,
        last_push_ms,
        chain,
    } = c;
    if deleted {
        return false;
    }
    for r in changed {
        if let Some(slot) = refs.get(&r.name).and_then(|&i| into.refs.get_mut(i)) {
            *slot = r;
        } else {
            refs.insert(r.name.clone(), into.refs.len());
            into.refs.push(r);
        }
    }
    into.head = head.or(into.head.take());
    into.next_id = next_id.or(into.next_id);
    into.upload_gen = upload_gen.or(into.upload_gen);
    into.last_push_ms = last_push_ms.or(into.last_push_ms);
    (into.version, into.chain) = (version, chain);
    true
}

impl Subscriber {
    /// Whether it is behind and its queue has room again (half of it).
    fn due(&self) -> bool {
        self.behind.is_some() && self.tx.capacity() * 2 >= self.tx.max_capacity()
    }
}

impl Log {
    /// Send `update` to the subscribers that are not behind; one whose
    /// queue is full falls behind, and keeps what it misses (see
    /// [`Missed`]). Whether any subscriber is behind.
    fn notify(&mut self, update: &Update) -> bool {
        let layers = self.layers.as_ref();
        self.subscribers.retain_mut(|s| {
            if let Some(m) = &mut s.behind {
                m.add(update.clone());
                return !s.tx.is_closed();
            }
            match s.tx.try_send(update.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(u)) => {
                    let mut m = Missed::new(layers);
                    m.add(u);
                    s.behind = Some(m);
                    true
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });
        self.track_layers(update);
        self.subscribers.iter().any(|s| s.behind.is_some())
    }

    /// Keep [`layers`](Self::layers) in step with `update`, sent to the
    /// subscribers (none left: forget them).
    fn track_layers(&mut self, update: &Update) {
        if self.subscribers.is_empty() {
            self.layers = None;
        }
        let Some(layers) = &mut self.layers else {
            return;
        };
        match &update.kind {
            Some(update::Kind::Change(Change {
                layers: Some(e), ..
            })) => apply_layer_edit(layers, e),
            Some(update::Kind::State(m)) => layers.clone_from(&m.layers),
            _ => {}
        }
    }

    /// Catch up the subscribers behind that have room again: with what
    /// they missed, or with `state` (if given) when that needs one.
    /// Whether any is still behind.
    fn resync(&mut self, state: Option<&Update>) -> bool {
        let layers = self.layers.as_ref();
        self.subscribers.retain_mut(|s| {
            if !s.due() {
                return !s.tx.is_closed();
            }
            let Some(mut m) = s.behind.take() else {
                return true;
            };
            let first = match (m.catch_up(layers), state) {
                (Some(c), _) => Update {
                    kind: Some(update::Kind::Change(c)),
                },
                (None, Some(state)) => state.clone(),
                (None, None) => {
                    s.behind = Some(Missed {
                        sequencer: m.sequencer,
                        ..Missed::default()
                    });
                    return true;
                }
            };
            // Room for both (the queue is at least half empty), unless it
            // closed since.
            s.tx.try_send(first).is_ok() && m.sequencer.is_none_or(|l| s.tx.try_send(l).is_ok())
        });
        self.subscribers.iter().any(|s| s.behind.is_some())
    }

    /// Whether a subscriber behind has room again, and whether one of
    /// those needs a whole state.
    fn due(&self) -> (bool, bool) {
        let due = || self.subscribers.iter().filter(|s| s.due());
        let state = due().any(|s| {
            s.behind.as_ref().is_some_and(|m| {
                m.change.is_none()
                    || (m.layers_changed && (m.held.is_none() || self.layers.is_none()))
            })
        });
        (due().next().is_some(), state)
    }
}

pub(super) struct Applier {
    state: Arc<StateStore>,
    db: LogDb,
    logs: HashMap<u64, Log>,
    /// Each log's chain, while it has effects in flight.
    chains: HashMap<u64, Chain>,
    /// Logs with a subscriber behind (see [`Log::notify`]).
    behind: HashSet<u64>,
    /// The loop's input, for reports.
    report: mpsc::UnboundedSender<Input>,
    /// The ledger nodes' internal URLs, to name sequencers by (None: a
    /// standalone node).
    addresses: Option<Addresses>,
    /// Logs written since the last sync.
    unsynced: HashSet<u64>,
    last_sync: Instant,
    /// Batches since they were last logged (`enroute::ledger_timing`).
    stats: Window<Batches>,
}

/// What the apply thread did over a window.
#[derive(Default)]
struct Batches {
    count: u64,
    jobs: u64,
    entries: u64,
    busy: Duration,
    prefetch: Duration,
    sync: Duration,
    syncs: u64,
    /// Subscriptions started, logs whose subscribers behind were caught
    /// up, and the whole states that took.
    follows: u64,
    catchups: u64,
    resyncs: u64,
}

/// Log the apply thread's statistics, as the loop does its rounds'.
fn log_batches(stats: &mut Window<Batches>) {
    let Some((span, b)) = stats.due() else { return };
    tracing::debug!(
        target: "enroute::ledger_timing",
        batches_per_s = rate(b.count, span),
        busy_pct = pct(b.busy, span),
        batch_us = per(b.busy.as_micros(), b.count),
        prefetch_us = per(b.prefetch.as_micros(), b.count),
        jobs = per(u128::from(b.jobs), b.count),
        entries = per(u128::from(b.entries), b.count),
        syncs = b.syncs,
        sync_us = per(b.sync.as_micros(), b.syncs),
        follows = b.follows,
        catchups = b.catchups,
        resyncs = b.resyncs,
        "apply batches"
    );
}

impl Applier {
    /// The thread for `logs` (log id → its published progress) as
    /// recovered.
    #[expect(
        clippy::unwrap_used,
        reason = "invariant: the monotonic clock is older than `SYNC_EVERY`"
    )]
    pub(super) fn new(
        state: Arc<StateStore>,
        db: LogDb,
        logs: impl IntoIterator<Item = (u64, Arc<Mutex<Progress>>)>,
        report: mpsc::UnboundedSender<Input>,
        addresses: Option<Addresses>,
    ) -> Applier {
        let logs: HashMap<u64, Log> = logs
            .into_iter()
            .map(|(id, progress)| {
                (
                    id,
                    Log {
                        progress,
                        subscribers: Vec::new(),
                        sequencer: 0,
                        layers: None,
                    },
                )
            })
            .collect();
        // Recovery replayed entries without syncing: the first sync reports
        // every log.
        let unsynced = logs.keys().copied().collect();
        Applier {
            state,
            db,
            logs,
            chains: HashMap::new(),
            behind: HashSet::new(),
            report,
            addresses,
            unsynced,
            last_sync: Instant::now().checked_sub(SYNC_EVERY).unwrap(),
            stats: Window::new(),
        }
    }

    /// Run until the loop drops its end of the queue. A failure to write the
    /// store stops the node, as a failed log write does.
    ///
    /// # Panics
    ///
    /// Panics if the store cannot be written or synced.
    #[expect(
        clippy::panic,
        reason = "a node that cannot persist its state must stop: fail-stop is what replication's safety assumes"
    )]
    pub(super) fn run(mut self, jobs: &Receiver<Job>) {
        if let Err(e) = self.serve(jobs) {
            panic!("apply thread: {e:#}");
        }
    }

    fn serve(&mut self, jobs: &Receiver<Job>) -> Result<()> {
        if let Some(flushed) = self.sync(true, Vec::new())? {
            flushed.wait().context("syncing the state store")?;
        }
        loop {
            let mut wait = if self.unsynced.is_empty() {
                Duration::from_secs(3600)
            } else {
                SYNC_EVERY.saturating_sub(self.last_sync.elapsed())
            };
            if !self.behind.is_empty() {
                wait = wait.min(RESYNC_POLL);
            }
            let mut batch = Vec::new();
            match jobs.recv_timeout(wait) {
                Ok(j) => batch.push(j),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            while batch.len() < BATCH
                && let Ok(j) = jobs.try_recv()
            {
                batch.push(j);
            }
            let t = Instant::now();
            if !batch.is_empty() {
                self.stats.count += 1;
                self.stats.jobs += count(batch.len());
            }
            self.batch(batch)?;
            self.resync();
            self.stats.busy += t.elapsed();
            if !self.unsynced.is_empty() && self.last_sync.elapsed() >= SYNC_EVERY {
                self.sync(false, Vec::new())
                    .context("syncing the state store")?;
            }
            log_batches(&mut self.stats);
        }
        // The store flushes its memtables as it closes (no WAL to replay
        // at the next open).
        Ok(())
    }

    /// Run a batch of jobs: apply, evaluate, publish, answer, report. Each
    /// run of consecutive `Apply` jobs is one round of the store (one
    /// write), each run of `Evaluate` jobs one round never written; any
    /// other job is handled between rounds.
    fn batch(&mut self, batch: Vec<Job>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut touched = HashSet::new();
        let mut done = Done::default();
        let mut jobs = batch.into_iter().peekable();
        while let Some(job) = jobs.next() {
            match job {
                Job::Apply { .. } => {
                    let more =
                        std::iter::from_fn(|| jobs.next_if(|j| matches!(j, Job::Apply { .. })));
                    let run = std::iter::once(job).chain(more).collect();
                    self.apply_run(run, &mut touched, &mut done)?;
                }
                Job::Evaluate { log, proposal } => {
                    let more =
                        std::iter::from_fn(|| jobs.next_if(is_evaluate).and_then(evaluation));
                    let run = std::iter::once((log, proposal)).chain(more).collect();
                    self.evaluate_run(run, &mut done)?;
                }
                job => self.other(job, &mut touched)?,
            }
        }
        // Publish, then answer proposers, then tell the loop.
        let mut applied = Vec::with_capacity(touched.len());
        for log in touched {
            let (Some(root), Some(l)) = (self.state.root(log), self.logs.get(&log)) else {
                continue;
            };
            let mut p = l.progress.lock();
            (p.version, p.applied) = (root.version, root.applied_index);
            p.deleted |= done.deleted.contains(&log);
            applied.push(log);
        }
        for (resp, out) in done.replies {
            // A proposer that gave up needs no answer.
            drop(resp.send(out));
        }
        if !applied.is_empty() {
            drop(self.report.send(Input::Applied(applied)));
        }
        Ok(())
    }

    /// Send a new state to the subscribers behind that have room for it
    /// again (one read of the state per log).
    fn resync(&mut self) {
        let behind: Vec<u64> = self.behind.iter().copied().collect();
        for log in behind {
            if !self.resync_log(log) {
                self.behind.remove(&log);
            }
        }
    }

    /// [`resync`](Self::resync) one log: whether a subscriber is still
    /// behind.
    fn resync_log(&mut self, log: u64) -> bool {
        let Some(l) = self.logs.get_mut(&log) else {
            return false;
        };
        let (due, needs_state) = l.due();
        if !due {
            return l.subscribers.iter().any(|s| s.behind.is_some());
        }
        self.stats.catchups += 1;
        if !needs_state {
            return l.resync(None);
        }
        let meta = match load(&self.state, &self.db, log) {
            Ok(Some((_, meta))) => meta,
            Ok(None) => {
                // Dropped: its subscribers' streams end with it.
                l.subscribers.clear();
                return false;
            }
            Err(e) => {
                tracing::warn!(log, "reading a state to catch up a subscriber: {e:#}");
                return true;
            }
        };
        self.stats.resyncs += 1;
        l.resync(Some(&Update {
            kind: Some(update::Kind::State(meta)),
        }))
    }

    /// Apply a run of `Apply` jobs in one round, read ahead first.
    fn apply_run(
        &mut self,
        run: Vec<Job>,
        touched: &mut HashSet<u64>,
        done: &mut Done,
    ) -> Result<()> {
        let state = self.state.clone();
        let t = Instant::now();
        let mut r = state.round();
        let logs: Vec<u64> = run
            .iter()
            .filter_map(|j| match j {
                Job::Apply { log, .. } => Some(*log),
                _ => None,
            })
            .filter(|log| state.contains(*log))
            .collect();
        r.prefetch(&[], &logs)?;
        self.stats.prefetch += t.elapsed();
        for job in run {
            let Job::Apply {
                log,
                cells,
                replies: mut waiting,
            } = job
            else {
                continue;
            };
            if !state.contains(log) {
                continue; // dropped meanwhile
            }
            self.stats.entries += count(cells.len());
            self.apply_entries(&mut r, log, &cells, &mut waiting, done)?;
            touched.insert(log);
            self.unsynced.insert(log);
        }
        r.commit()
    }

    /// Apply one log's entries in `r`, collecting the replies and
    /// deletions; the loop has the sequencer evaluate again the proposals
    /// whose effect was skipped.
    fn apply_entries(
        &mut self,
        r: &mut Round<'_>,
        log: u64,
        cells: &[StoredCell],
        waiting: &mut HashMap<u64, Proposal>,
        done: &mut Done,
    ) -> Result<()> {
        for c in cells {
            let effect = effect(log, c);
            let applied = stored::apply(r, log, c.index, c.term, effect.as_ref())?;
            let (Some(effect), Some(applied)) = (effect, applied) else {
                continue;
            };
            self.settle(log, &effect, applied);
            if applied && let Some(c) = &effect.change {
                self.notify(log, c, done);
            }
            let Some(p) = waiting.remove(&c.index) else {
                continue;
            };
            if applied {
                done.replies.push((p.resp, Ok(p.outcome)));
            } else {
                drop(self.report.send(Input::Reevaluate(log, p)));
            }
        }
        Ok(())
    }

    /// Tell `log`'s subscribers about a change that applied.
    fn notify(&mut self, log: u64, c: &Change, done: &mut Done) {
        if c.deleted {
            done.deleted.insert(log);
        }
        // (A log without subscribers tracks no layers, so it skips the copy.)
        if let Some(l) = self.logs.get_mut(&log)
            && !l.subscribers.is_empty()
            && l.notify(&Update {
                kind: Some(update::Kind::Change(c.clone())),
            })
        {
            self.behind.insert(log);
        }
    }

    /// Keep `log`'s chain in step with an effect that applied (or was
    /// skipped).
    fn settle(&mut self, log: u64, effect: &Effect, applied: bool) {
        let Some(ch) = self.chains.get_mut(&log) else {
            return;
        };
        let ours = effect.chain == ch.id;
        if !ours {
            // Another node's change: ours was evaluated against an older
            // state.
            if applied && effect.change.is_some() {
                self.chains.remove(&log);
            }
            return;
        }
        if !applied {
            self.chains.remove(&log);
            return;
        }
        // Gone too: effects before it (dropped, or they could not have
        // let it apply), and confirmations of the same state.
        while ch.inflight.front().is_some_and(|f| {
            f.base < effect.base
                || (f.base == effect.base && (f.change.is_none() || effect.change.is_some()))
        }) {
            ch.inflight.pop_front();
        }
        if ch.inflight.is_empty() {
            self.chains.remove(&log);
        }
    }

    /// Evaluate a run of proposals in one round that is never written,
    /// read ahead first, and hand each one's entry to the loop.
    fn evaluate_run(&mut self, run: Vec<(u64, Proposal)>, done: &mut Done) -> Result<()> {
        let state = self.state.clone();
        let mut r = state.round();
        {
            let hosted = |log: &u64| state.contains(*log);
            let keys: Vec<(u64, &[u8])> = run
                .iter()
                .filter(|(log, _)| hosted(log))
                .flat_map(|(log, p)| refs(&p.cmd).map(move |name| (*log, name)))
                .collect();
            let logs: Vec<u64> = run.iter().map(|(log, _)| *log).filter(hosted).collect();
            r.prefetch(&keys, &logs)?;
        }
        let mut replayed = HashSet::new();
        for (log, mut p) in run {
            if !state.contains(log) {
                done.replies.push((p.resp, Err(NotHosted.into())));
                continue;
            }
            if replayed.insert(log) {
                self.replay(&mut r, log)?;
            }
            p.tries += 1;
            match self.evaluate(&mut r, log, &p.cmd) {
                Ok(Evaluated::Settled(out)) => done.replies.push((p.resp, Ok(out))),
                Ok(Evaluated::Entry(data, out)) => {
                    p.outcome = out;
                    drop(self.report.send(Input::Evaluated(log, data, p)));
                }
                Ok(Evaluated::Confirm(base, out)) => {
                    p.outcome = out;
                    drop(self.report.send(Input::Confirm(log, base, p)));
                }
                Err(e) => done.replies.push((p.resp, Err(e))),
            }
        }
        // Never committed: the log carries the changes.
        drop(r);
        Ok(())
    }

    /// Write `log`'s chain over the store in `r`, so proposals are
    /// evaluated after it. A chain that no longer fits the store is
    /// dropped (its effects will be skipped).
    fn replay(&mut self, r: &mut Round<'_>, log: u64) -> Result<()> {
        let Some(ch) = self.chains.get(&log) else {
            return Ok(());
        };
        let mut s = Stored::new(r, log, 0);
        let mut c = s.counters()?;
        // Checked first, so nothing of a chain that does not fit is written.
        for e in &ch.inflight {
            if !machine::applies(&c, e) {
                self.chains.remove(&log);
                return Ok(());
            }
            if let Some(change) = &e.change {
                (c.version, c.chain) = (change.version, change.chain);
            }
        }
        for e in &ch.inflight {
            machine::apply_effect(&mut s, e)?;
        }
        Ok(())
    }

    /// Evaluate `cmd` against `log`'s state in `r` (its chain written
    /// there), and add its effect to the chain.
    fn evaluate(&mut self, r: &mut Round<'_>, log: u64, cmd: &Command) -> Result<Evaluated> {
        let mut s = Stored::new(r, log, 0);
        let c = s.counters()?;
        if let Some(out) = machine::settled(&c, cmd) {
            return Ok(Evaluated::Settled(out));
        }
        let ch = self.chains.entry(log).or_insert_with(|| Chain {
            id: rand::random(),
            inflight: VecDeque::new(),
        });
        let (out, change) = machine::evaluate(&mut s, cmd, ch.id)?;
        // Nothing of the chain in flight changes the state: the outcome
        // holds as long as the applied state does.
        if change.is_none() && ch.inflight.iter().all(|e| e.change.is_none()) {
            return Ok(Evaluated::Confirm(c.version, out));
        }
        let effect = Effect {
            base: c.version,
            base_chain: c.chain,
            chain: ch.id,
            change,
        };
        let data = effect.encode_to_vec();
        ch.inflight.push_back(effect);
        Ok(Evaluated::Entry(data, out))
    }

    /// A job other than `Apply` (between rounds).
    fn other(&mut self, job: Job, touched: &mut HashSet<u64>) -> Result<()> {
        match job {
            Job::Apply { .. } | Job::Evaluate { .. } => {
                bail!("an Apply or Evaluate job reached the other-job path")
            }
            Job::Host {
                log,
                root,
                meta,
                progress,
            } => {
                self.chains.remove(&log);
                self.state.install(log, root, stored::entries(&meta)?)?;
                let subscribers = Vec::new();
                self.logs.insert(
                    log,
                    Log {
                        progress,
                        subscribers,
                        sequencer: 0,
                        layers: None,
                    },
                );
                touched.insert(log);
                self.unsynced.insert(log);
            }
            Job::Forget(log) => {
                self.chains.remove(&log);
            }
            Job::Install { log, root, meta } => {
                self.chains.remove(&log);
                self.state.install(log, root, stored::entries(&meta)?)?;
                // The loop hands a snapshot only for a log it hosts.
                let l = self
                    .logs
                    .get_mut(&log)
                    .context("a snapshot for a log that is not hosted")?;
                l.progress.lock().deleted = meta.deleted;
                if l.notify(&Update {
                    kind: Some(update::Kind::State(meta.clone())),
                }) {
                    self.behind.insert(log);
                }
                touched.insert(log);
                self.unsynced.insert(log);
            }
            Job::Drop(log) => {
                self.chains.remove(&log);
                self.state.delete(log)?;
                self.logs.remove(&log);
                touched.remove(&log);
                self.unsynced.remove(&log);
            }
            Job::Follow { log, tx, resp } => self.follow(log, tx, resp),
            Job::Unfollow(log) => {
                if let Some(l) = self.logs.get_mut(&log) {
                    l.subscribers.retain(|s| !s.tx.is_closed());
                    l.layers.take_if(|_| l.subscribers.is_empty());
                }
            }
            Job::Sequencer { log, sequencer } => {
                if let Some(l) = self.logs.get_mut(&log) {
                    l.sequencer = sequencer;
                    let url = url_of(self.addresses.as_ref(), sequencer);
                    let behind = l.notify(&sequencer_update(url));
                    behind.then(|| self.behind.insert(log));
                }
            }
            Job::Sync(settled) => {
                self.sync(true, settled)?;
            }
        }
        Ok(())
    }

    /// Subscribe `tx` to `log`, and answer `resp`.
    fn follow(&mut self, log: u64, tx: mpsc::Sender<Update>, resp: oneshot::Sender<Result<u64>>) {
        // A requester that gave up needs no answer (so each `send` is dropped).
        let Some(l) = self.logs.get_mut(&log) else {
            drop(resp.send(Err(NotHosted.into())));
            return;
        };
        self.stats.follows += 1;
        // The state and the sequencer first, then (registered in the same
        // step, so nothing is missed) every change after them.
        match load(&self.state, &self.db, log) {
            Ok(Some((_, meta))) => {
                let layers = meta.layers.clone();
                let state = Update {
                    kind: Some(update::Kind::State(meta)),
                };
                if tx.try_send(state).is_ok()
                    && tx
                        .try_send(sequencer_update(url_of(
                            self.addresses.as_ref(),
                            l.sequencer,
                        )))
                        .is_ok()
                {
                    l.subscribers.push(Subscriber { tx, behind: None });
                    l.layers = Some(layers);
                }
                drop(resp.send(Ok(log)));
            }
            Ok(None) => drop(resp.send(Err(NotHosted.into()))),
            Err(e) => drop(resp.send(Err(e.context("reading the state")))),
        }
    }

    /// Flush the store (if it holds enough, unless `now`), and once the
    /// flush is durable report, for every log written since the last one,
    /// its applied index and oldest pin as of the flush (the only writer is
    /// this thread), and the `settled` logs. The flush runs without this
    /// thread waiting for it.
    fn sync(&mut self, now: bool, settled: Vec<u64>) -> Result<Option<Flushed>> {
        let t = Instant::now();
        self.last_sync = t;
        if !now && !self.state.due() {
            return Ok(None);
        }
        let mut durable = Vec::with_capacity(self.unsynced.len());
        for log in self.unsynced.drain() {
            let Some(root) = self.state.root(log) else {
                continue;
            };
            durable.push((log, root.applied_index, self.state.oldest_pin(log)?));
        }
        let report = self.report.clone();
        let flushed = self.state.flush(move || {
            if !durable.is_empty() {
                drop(report.send(Input::Durable(durable)));
            }
            if !settled.is_empty() {
                drop(report.send(Input::Settled(settled)));
            }
        })?;
        (self.stats.sync, self.stats.syncs) = (self.stats.sync + t.elapsed(), self.stats.syncs + 1);
        Ok(Some(flushed))
    }
}

/// What a proposal's evaluation came to.
enum Evaluated {
    /// An outcome that needs no log entry.
    Settled(Outcome),
    /// The entry to propose (an encoded [`Effect`]), and the outcome once
    /// it applies.
    Entry(Vec<u8>, Outcome),
    /// An outcome that changes nothing, evaluated against the applied
    /// state at this version: it holds if the version still is the
    /// log's once a read barrier shows this node still leads.
    Confirm(u64, Outcome),
}

/// The refs `cmd` compare-and-swaps: what its evaluation reads.
fn refs(cmd: &Command) -> impl Iterator<Item = &[u8]> {
    let updates = match &cmd.kind {
        Some(command::Kind::Commit(c)) => c.updates.as_slice(),
        _ => &[],
    };
    updates.iter().map(|u| u.name.as_bytes())
}

#[cfg(test)]
mod tests {
    use enroute_shared::oid::Oid;
    use enroute_shared::proto::{LayerEdit, LayerMeta, RefChange};

    use super::*;

    fn set(name: &str, byte: u8, version: u64) -> Change {
        Change {
            refs: vec![RefChange {
                name: name.into(),
                value: Some(Oid::sha1([byte; 20])),
                updated_at_ms: version,
            }],
            version,
            chain: version,
            ..Change::default()
        }
    }

    fn layer(id: &str, version: u64) -> Change {
        Change {
            layers: Some(LayerEdit {
                insert: vec![LayerMeta {
                    id: id.into(),
                    ..LayerMeta::default()
                }],
                remove: vec![],
            }),
            next_id: Some(version),
            version,
            ..Change::default()
        }
    }

    fn change(c: Change) -> Update {
        Update {
            kind: Some(update::Kind::Change(c)),
        }
    }

    /// Catching up with what was missed (refs merged, layers edited to the
    /// log's current ones) is applying every change in order; a deletion
    /// takes a whole state.
    #[test]
    fn catching_up_applies_the_missed_changes() {
        let mut base = RepoMeta::new();
        base.apply_change(&layer("l0", 1));
        let mut log_layers = base.layers.clone();
        let mut missed = Missed::new(Some(&log_layers));
        let replace = Change {
            layers: Some(LayerEdit {
                remove: vec![RemovedLayer {
                    id: "l0".into(),
                    first_id: 0,
                }],
                insert: vec![LayerMeta {
                    id: "m".into(),
                    ..LayerMeta::default()
                }],
            }),
            version: 4,
            ..Change::default()
        };
        let changes = [
            set("refs/heads/a", 1, 2),
            layer("l1", 3),
            replace,
            set("refs/heads/b", 2, 5),
            layer("l2", 6),
            set("refs/heads/a", 3, 7),
            Change {
                refs: vec![RefChange {
                    name: "refs/heads/b".into(),
                    value: None,
                    updated_at_ms: 0,
                }],
                head: Some("refs/heads/a".into()),
                version: 8,
                ..Change::default()
            },
        ];
        let mut one = base.clone();
        for c in &changes {
            one.apply_change(c);
            if let Some(e) = &c.layers {
                apply_layer_edit(&mut log_layers, e);
            }
            missed.add(change(c.clone()));
        }
        let caught = missed.catch_up(Some(&log_layers)).unwrap();
        assert_eq!(caught.refs.len(), 2);
        base.apply_change(&caught);
        assert_eq!(base, one);

        let mut missed = Missed::new(Some(&log_layers));
        missed.add(change(Change {
            deleted: true,
            ..Change::default()
        }));
        assert!(missed.catch_up(Some(&log_layers)).is_none());
    }
}
