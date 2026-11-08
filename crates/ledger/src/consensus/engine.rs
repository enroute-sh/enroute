//! The consensus engine: one replicated log per repository, plus the root
//! log, all driven by one event loop on top of the node's log store
//! ([`crate::logdb`]).
//!
//! **Repository logs** replicate as [`super::repo`] describes: each has
//! (up to) three replicas, one of them its sequencer, which the root log's
//! directory names along with the log's term. A node hosts the logs it is
//! a replica of. Nothing runs for an idle log: no ticks, no heartbeats; a
//! sequencer resends only to replicas that lag, and only while they are
//! alive. The root's leader hands a log to a new sequencer when its
//! sequencer fails (see [`super::control`]).
//!
//! **Incarnations.** Incarnation `epoch` of a repository's log starts when
//! the root log ([`root`]) applies its creation: its replicas host it from
//! the initial state the directory implies, and its first sequencer (the
//! proposer) opens term 1. Its log id is derived from (repo, epoch), and its
//! identity is part of its state, so a recovering node knows which log is
//! which. As the root applies an incarnation's end (or a newer one, or new
//! replicas without this node), nodes drop its log; a restarting node drops
//! those its directory says ended or are not its own.
//!
//! **The root log** is a raft-rs group under a reserved id, hosted by every
//! node; its entries are applied by the loop itself, into the directory in
//! memory and in the log store (see [`root`]).
//!
//! Each loop iteration takes whatever inputs are pending (proposals, peer
//! frames, read requests, ticks), makes every change they made durable
//! with one log store write (group commit), then sends the answers that
//! required it, applies committed entries, and answers proposers. Entries
//! committed and written before the round apply before its write, which
//! holds at most their commit point. Inputs that arrive during the fsync
//! are batched into the next iteration.
//!
//! **State machines on disk.** Every log's state machine is in the node's
//! state store ([`crate::state::store`], laid out by [`crate::state::stored`]):
//! committed entries are applied in atomic batches that also record them
//! as applied, unsynced, on the apply thread ([`super::apply`]), which
//! also answers proposers. Nothing of a repository's state stays in
//! memory, so an idle or huge repository costs the engine no more than a
//! small one. Three rules keep the two stores in step:
//! - a log is truncated (compacted) only up to an entry the state store has
//!   durably applied, as the apply thread reports after each sync;
//! - nor past an inline layer's entry until the layer is flushed (its bytes
//!   are only there; see `stored`), as of that same durable point;
//! - on restart, each log replays its committed entries after the state's
//!   applied index. A state installed from a sequencer (or a new log's
//!   initial state) is durable in the state store before the log is reset
//!   to it (or created): a crash in between leaves the state ahead of the
//!   log, which recovery resets (or a state without a log, dropped).
//!
//! A round that needs the latter waits, before its log write, for the
//! apply thread to have applied and synced everything handed to it.
//!
//! **Proposals** are evaluated by the log's sequencer, which appends their
//! effect (see [`super::apply`]); another node answers [`NotSequencer`],
//! which names the sequencer for the compute to ask. A proposal is
//! answered when its entry applies, on whichever node it waits; one whose
//! position another term's entry took is evaluated again (and so told the
//! new sequencer).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use prost::Message as _;
use raft::prelude::*;
use raft::{Config as RaftCfg, RawNode, StateRole};
use raft_engine::LogBatch;
use tokio::sync::{mpsc, oneshot};

use enroute_shared::auth::Bearer;
use enroute_shared::proto::{RepoSummary, Update};
use enroute_shared::roles::NotHosted;
use enroute_shared::state::{Command, LayerMeta, Outcome, RepoMeta};

use crate::logdb::{LogDb, WeakLogDb, is_log};
use crate::proto::{Member, Record, RootEntry, Start, frame, root_entry};
use crate::state::machine::{self, count};
use crate::state::store::StateStore;
use crate::state::stored;

use super::apply::{Applier, Job, Progress, Proposal};
use super::codec::{Cells, Entries, Prost, Proto};
use super::control::{Controller, Liveness};
use super::log_id;
use super::repo::load;
use super::repo::{self, Config, LedgerStore, RepoLog};
use super::root::{self, Change, Directory, ROOT, ROOT_START, SharedRoot, hosts};
use super::root_store::{LogMeta, LogStore, fetch};
use super::shardlog::{Request, Store as _, Synced, Terms};
use super::stats::{Window, pct, per, rate};
use super::transport::{
    Addresses, Frame, Transport, frame as framed, root_frame, root_message, url_of,
};

const TICK: Duration = Duration::from_millis(100);
const ELECTION_TICKS: usize = 10;
const HEARTBEAT_TICKS: usize = 2;
const PROPOSAL_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a root proposal or read waits: its callers try again.
const ROOT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a caller waits for the loop to answer a root call (which it
/// does within `ROOT_TIMEOUT`).
const ROOT_CALL_TIMEOUT: Duration = Duration::from_secs(6);
/// Re-issue an unanswered root read index request after this long.
const READ_RETRY: Duration = Duration::from_millis(500);
/// Resend to a lagging replica, or a read's checks, after this long.
const RESEND: Duration = Duration::from_millis(300);
/// Ask the root again to end a replica change after this long.
const ACTIVATE_RETRY: Duration = Duration::from_secs(3);
/// How often a deposed sequencer the root still names tells its leader.
const DEPOSED_RETRY: Duration = Duration::from_secs(1);
/// A root learner this close to the commit index is caught up.
const CATCH_UP: u64 = 100;
/// Keys of a log's state in the log store.
const SNAPSHOT_KEY: &[u8] = b"snapshot";
const HARD_STATE_KEY: &[u8] = b"hard_state";
/// How often old log store files are purged.
const PURGE_EVERY: Duration = Duration::from_secs(10);
/// Updates a subscriber may fall behind before its changes are merged
/// until it catches up.
const FOLLOW_QUEUE: usize = 256;

/// The error of a request to a node that does not sequence the
/// repository's log: the sequencer evaluates proposals and serves reads.
/// It names the sequencer, by id and by internal URL (empty: none known,
/// or no address).
#[derive(Debug, Clone)]
pub struct NotSequencer {
    /// The sequencer's id.
    pub sequencer: u64,
    /// The sequencer's internal URL.
    pub url: String,
}

impl std::fmt::Display for NotSequencer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "not the sequencer of the repository's log (node {:x} is)",
            self.sequencer
        )
    }
}

impl std::error::Error for NotSequencer {}

pub(super) enum Input {
    Propose(Command, oneshot::Sender<Result<Outcome>>),
    /// From the apply thread: a proposal evaluated, and its log entry.
    Evaluated(u64, Vec<u8>, Proposal),
    /// From the apply thread: a proposal evaluated to an outcome that
    /// changes nothing, against the log's state at this version.
    Confirm(u64, u64, Proposal),
    /// From the apply thread: a proposal whose effect was skipped.
    Reevaluate(u64, Proposal),
    ReadIndex(u64, oneshot::Sender<Result<u64>>),
    /// Frames from a peer.
    Frames(u64, Vec<Frame>),
    /// A peer could not be sent root log messages.
    Unreachable(u64),
    /// Where `repo`'s live log is (see [`Engine::locate`]).
    Locate(String, oneshot::Sender<Result<Located>>),
    /// Propose an entry to the root log (see [`Engine::propose_root`]).
    ProposeRoot(Vec<u8>, oneshot::Sender<Result<()>>),
    /// A peer refused this node as removed, its logs moved off it: drop
    /// every repository's log it still hosts.
    Retire,
    /// Subscribe to a hosted repository's log (see [`Engine::follow`]).
    Follow(String, mpsc::Sender<Update>, oneshot::Sender<Result<u64>>),
    /// A subscription to this log ended (see [`Subscription`]).
    Unfollow(u64),
    /// Purging old log store files is held back by these logs: compact them
    /// (or drop them, if not hosted).
    Purge(Vec<u64>),
    /// From the apply thread: these logs applied entries (see [`Progress`]).
    Applied(Vec<u64>),
    /// From the apply thread, after a sync: logs durably applied up to
    /// these indexes, and their oldest pinned entry then.
    Durable(Vec<(u64, u64, Option<u64>)>),
    /// From the apply thread: the states these logs start from (see
    /// `Loop::settling`) are durable.
    Settled(Vec<u64>),
    Stop,
}

/// Where a repository's live log is (see [`Engine::locate`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Located {
    /// Hosted here (now, durably).
    Here,
    /// Hosted by other nodes: its sequencer.
    Elsewhere(u64),
    /// The repository has no live incarnation.
    Absent,
}

/// What happened to this node's logs since the ledger last looked (see
/// [`Engine::take_events`]): the repositories whose state changed, or whose
/// log this node started sequencing. Maintenance tracking acts on them,
/// instead of visiting every hosted repository.
pub(crate) type Events = HashSet<String>;

/// A hosted repository, as readers outside the loop see it (see
/// [`Engine::hosted`]). Its state is in the state store.
#[derive(Clone, Debug)]
pub struct Hosted {
    /// Its log's id.
    pub log: u64,
    /// The incarnation of the repository this log serves.
    pub epoch: u64,
    /// Its state's version.
    pub version: u64,
    /// The log position its state is at.
    pub applied: u64,
    /// Whether this node sequences its log (and committed its term's
    /// first entry).
    pub leading: bool,
    /// The sequencer the directory here names (0: none), and its term.
    pub sequencer: u64,
    /// The term of that sequencer.
    pub term: u64,
    /// Whether its state is a tombstone (the repository was deleted).
    pub deleted: bool,
}

/// A hosted log's published entry. Only the loop writes the map, `leading`,
/// `sequencer` and `term`, and only the apply thread writes `progress`, so
/// neither checks that a log is still its repository's.
struct Published {
    log: u64,
    epoch: u64,
    leading: bool,
    sequencer: u64,
    term: u64,
    progress: Arc<Mutex<Progress>>,
}

impl Published {
    fn hosted(&self) -> Hosted {
        let p = *self.progress.lock();
        Hosted {
            log: self.log,
            epoch: self.epoch,
            version: p.version,
            applied: p.applied,
            leading: self.leading,
            sequencer: self.sequencer,
            term: self.term,
            deleted: p.deleted,
        }
    }
}

/// Counters of the consensus engine, for the status endpoint.
#[derive(Default, Clone, Debug, serde::Serialize)]
pub struct Stats {
    /// Hosted logs.
    pub logs: usize,
    /// Logs whose sequencer here resends to a lagging replica.
    pub lagging_logs: usize,
    /// Frames sent to peers.
    pub messages_sent: u64,
    /// Of those, repository logs' (not the root's, nor pings).
    pub log_messages: u64,
    /// Writes to the log store.
    pub log_writes: u64,
    /// Bytes written to the log store.
    pub log_bytes: u64,
    /// States installed from a sequencer (a replica behind its log's start).
    pub snapshots_installed: u64,
    /// Root log snapshots installed from the root's leader.
    pub root_snapshots: u64,
    /// Repository log truncations (compactions).
    pub compactions: u64,
    /// Root log snapshots taken, which truncate it.
    pub root_compactions: u64,
    /// Logs this node sequences.
    pub leading: usize,
    /// Logs the controller sealed for a new term (on the root's leader).
    pub seals: u64,
    /// Appointments it proposed.
    pub appointments: u64,
}

/// This node's consensus engine.
pub struct Engine {
    tx: mpsc::UnboundedSender<Input>,
    /// The stores, owned by the loop (so they close once it has stopped).
    state: std::sync::Weak<StateStore>,
    db: WeakLogDb,
    /// True once the loop and the apply thread have stopped, closing the
    /// stores.
    stopped: tokio::sync::watch::Receiver<bool>,
    /// True once asked to stop.
    stopping: tokio::sync::watch::Sender<bool>,
    hosted: Arc<RwLock<BTreeMap<String, Published>>>,
    root: SharedRoot,
    events: Arc<Mutex<Events>>,
    stats: Arc<RwLock<Stats>>,
    addresses: Option<Addresses>,
    /// Nodes learned of outside the directory (see [`Engine::introduce`]).
    introduced: RwLock<HashMap<u64, Member>>,
    /// This start's boot (see [`Liveness`]).
    boot: u64,
    /// This node's id (its store id).
    pub id: u64,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// A proposal, or a read barrier, waiting for its log's sequencer to be
/// ready (or the root's leader to be known).
struct Waiting {
    log: u64,
    deadline: Instant,
    kind: Wait,
}

enum Wait {
    /// A proposal to evaluate, once this node sequences the log.
    Evaluate(Proposal),
    Read(Reader),
    /// A root entry, proposed by any node (followers forward it).
    Root {
        pid: u64,
        data: Vec<u8>,
        resp: oneshot::Sender<Result<()>>,
    },
}

/// Who waits for a read.
pub(super) enum Reader {
    /// A read barrier, told the index.
    Barrier(oneshot::Sender<Result<u64>>),
    /// A proposal evaluated to an outcome that changes nothing, at this
    /// version (see [`Loop::on_confirm`]).
    Confirm(u64, Box<Proposal>),
}

impl Reader {
    fn fail(self, e: anyhow::Error) {
        match self {
            Reader::Barrier(resp) => drop(resp.send(Err(e))),
            Reader::Confirm(_, proposal) => drop(proposal.resp.send(Err(e))),
        }
    }
}

struct Loop {
    node_id: u64,
    /// This start's boot (see [`Liveness`]).
    boot: u64,
    db: LogDb,
    state: Arc<StateStore>,
    snapshot_every: u64,
    /// Writes for the next round beyond the logs' own (compactions, dropped
    /// logs, installed states, new logs' starts).
    batch: LogBatch,
    /// The apply thread's queue.
    applier: std::sync::mpsc::Sender<Job>,
    /// Logs started (hosted, or reset to a snapshot) from states handed to
    /// the apply thread since the last round: one sync makes those durable.
    settling: Vec<u64>,
    /// Logs whose writes wait for the state they start from to be durable,
    /// with the syncs they wait for. A log store write must not start a log
    /// from a state that a crash could lose, and waiting only these logs
    /// keeps the others' rounds from waiting for a full state flush.
    unsettled: HashMap<u64, u32>,
    transport: Option<Transport>,
    /// Peers' internal URLs, to name a log's sequencer by.
    addresses: Option<Addresses>,
    root_log: RootLog,
    repos: HashMap<u64, RepoLog>,
    /// What readers outside the loop see; also the hosted logs by repository.
    hosted: Arc<RwLock<BTreeMap<String, Published>>>,
    /// For the ledger's lifecycle work.
    events: Arc<Mutex<Events>>,
    /// The root log's state.
    root: SharedRoot,
    /// The ledger's members and the nodes removed from it, as of the root's
    /// last entry applied.
    members: Vec<u64>,
    removed: Vec<u64>,
    liveness: Liveness,
    /// Nodes alive at the last tick.
    up: HashSet<u64>,
    controller: Controller,
    /// Callers to answer once this round's changes are durable (logs
    /// hosted, root entries applied).
    replies: Vec<oneshot::Sender<Result<()>>>,
    /// Callers to answer once these logs' starts are durable.
    located: Vec<(u64, oneshot::Sender<Result<Located>>)>,
    waiting: Vec<Waiting>,
    /// Reads with their index, until applied here.
    read_applied: Vec<(u64, u64, Reader)>,
    /// Logs with changes to write or answers to send this round.
    dirty: HashSet<u64>,
    /// Logs whose sequencer here has replicas to catch up.
    lagging: HashSet<u64>,
    /// Logs this node sequences (or did: an entry whose log no longer has
    /// a sequencer here is dropped when next looked at), so a tick looks at
    /// them, not at every log.
    sequencing: HashSet<u64>,
    /// Frames to this node itself, handled as if received.
    inbox: VecDeque<Frame>,
    next_id: u64,
    stats: Stats,
    shared_stats: Arc<RwLock<Stats>>,
    /// Round statistics since they were last logged (`enroute::ledger_timing`).
    rounds: Window<Rounds>,
}

/// What the loop's rounds did over a window: how many, how long they took
/// (and their durable write), how many inputs and logs each saw.
#[derive(Default)]
struct Rounds {
    count: u64,
    busy: Duration,
    write: Duration,
    inputs: u64,
    logs: u64,
}

fn stopped<T>(_: T) -> anyhow::Error {
    anyhow::anyhow!("consensus engine stopped")
}

/// The logs recovered at start.
struct RecoveredLogs {
    repos: HashMap<u64, RepoLog>,
    hosted: BTreeMap<String, Published>,
    batch: LogBatch,
}

impl RecoveredLogs {
    fn drop_log(&mut self, state: &StateStore, log: u64) -> Result<()> {
        self.batch.add_command(log, raft_engine::Command::Clean);
        state.delete(log)
    }

    /// Recover log `log` from what was `read` of it, or drop it (also if
    /// the directory `dir` says its incarnation ended, or that this node
    /// `me` is not one of its replicas).
    fn add(
        &mut self,
        (db, state): (&LogDb, &Arc<StateStore>),
        dir: &Directory,
        me: u64,
        log: u64,
        read: repo::StoredLog,
    ) -> Result<()> {
        let Some(root) = state.root(log) else {
            return self.drop_log(state, log);
        };
        let applied = (root.applied_index, root.applied_term);
        let Some((store, cells)) =
            LedgerStore::recover(log, (db, state), read, applied, &mut self.batch)
                .with_context(|| format!("recovering log {log:x}"))?
        else {
            return self.drop_log(state, log);
        };
        let inc = dir.live(&root.repo).filter(|i| i.epoch == root.epoch);
        let Some(inc) = inc.filter(|i| hosts(i, me)) else {
            return self.drop_log(state, log);
        };
        let config = Config::of(inc);
        let sole = inc.replicas == [me] && inc.joint.is_empty();
        let progress = Arc::new(Mutex::new(Progress {
            version: root.version,
            applied: 0,
            deleted: state.round().counters(log)?.deleted,
        }));
        let mut repo = RepoLog::new(
            root.repo.clone(),
            root.epoch,
            store,
            progress.clone(),
            config.clone(),
            me,
        );
        repo.node.restart::<Bytes>(root.applied_index, sole);
        // Bring the state machine up to the commit point.
        let commit = repo.store().commit();
        let mut round = state.round();
        repo.handed = root.applied_index;
        for c in cells.iter().filter(|c| c.index <= commit) {
            stored::apply(
                &mut round,
                log,
                c.index,
                c.term,
                repo::effect(log, c).as_ref(),
            )?;
            repo.handed = c.index;
        }
        round.commit()?;
        {
            let mut p = progress.lock();
            p.applied = repo.handed;
            p.version = state.root(log).map_or(root.version, |r| r.version);
        }
        let published = Published {
            log,
            epoch: root.epoch,
            leading: false,
            sequencer: config.sequencer,
            term: config.term,
            progress,
        };
        if let Some(old) = self.hosted.insert(root.repo.clone(), published) {
            self.repos.remove(&old.log);
            self.drop_log(state, old.log)?;
        }
        self.repos.insert(log, repo);
        Ok(())
    }
}

/// Recover every repository log in the log store, applying its committed
/// entries synchronously. Logs whose incarnation ended, or that are not
/// this node's (`me`), as far as the directory `dir` knows, are dropped.
fn recover_all(
    db: &LogDb,
    state: &Arc<StateStore>,
    me: u64,
    dir: &Directory,
) -> Result<RecoveredLogs> {
    let mut rec = RecoveredLogs {
        repos: HashMap::new(),
        hosted: BTreeMap::new(),
        batch: LogBatch::default(),
    };
    let groups: Vec<u64> = db
        .raft_groups()
        .into_iter()
        .filter(|&l| is_log(l))
        .collect();
    // Entries are read on the store's reader thread, many logs at a time.
    for chunk in groups.chunks(RECOVER_CHUNK) {
        let ids = chunk.to_vec();
        let mut read: HashMap<u64, repo::StoredLog> = db.read(move |db| {
            ids.into_iter()
                .map(|log| Ok((log, repo::read_log(db, log)?)))
                .collect::<raft_engine::Result<_>>()
        })?;
        for &log in chunk {
            let entries = read.remove(&log).unwrap_or_default();
            rec.add((db, state), dir, me, log, entries)?;
        }
    }
    // States without a log: hosting or unhosting was interrupted.
    for log in state.logs() {
        if !rec.repos.contains_key(&log) {
            state.delete(log)?;
        }
    }
    // What recovery wrote, durable before any log write relies on it.
    state.persist()?;
    Ok(rec)
}

/// Purge old log store files; logs holding them back get compacted.
fn spawn_purge(db: WeakLogDb, tx: mpsc::UnboundedSender<Input>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(PURGE_EVERY).await;
            if !purge_pass(&db, &tx).await {
                return;
            }
        }
    });
}

/// One purge pass; false once the engine has stopped.
async fn purge_pass(db: &WeakLogDb, tx: &mpsc::UnboundedSender<Input>) -> bool {
    // The loop owns the store: once it has stopped, so do we.
    let Some(db) = db.upgrade() else { return false };
    match tokio::task::spawn_blocking(move || db.read(raft_engine::Engine::purge_expired_files))
        .await
    {
        Ok(Ok(ids)) if !ids.is_empty() => tx.send(Input::Purge(ids)).is_ok(),
        Ok(Ok(_)) => true,
        Ok(Err(e)) => {
            tracing::warn!("purging the log store: {e:#}");
            true
        }
        Err(_) => false,
    }
}

impl Engine {
    /// Start the engine on `db` (the log store) and `state` (the state
    /// store) as node `id`, snapshotting each log every `snapshot_every`
    /// applied entries; `peers`: peers' addresses and this node's token for
    /// them (None for a standalone node, the only replica of its logs, with
    /// no transport). Without a root log yet, it starts a new ledger's if
    /// `bootstrap` (see [`root`]).
    pub(crate) fn start(
        db: LogDb,
        state: Arc<StateStore>,
        snapshot_every: u64,
        id: u64,
        peers: Option<(Addresses, Bearer)>,
        bootstrap: bool,
    ) -> Result<Arc<Engine>> {
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let root: SharedRoot = Arc::default();
        let root_log = recover_root(&db, id, &logger, &root, bootstrap)?;
        let RecoveredLogs {
            repos,
            hosted,
            batch,
        } = recover_all(&db, &state, id, &root.read().dir)?;
        let (members, removed) = root.read().dir.members();
        let hosted = Arc::new(RwLock::new(hosted));
        let events: Arc<Mutex<Events>> = Arc::default();
        let counters: Arc<RwLock<Stats>> = Arc::default();

        let (tx, rx) = mpsc::unbounded_channel();
        let addresses = peers.as_ref().map(|(a, _)| a.clone());
        if let Some(a) = &addresses {
            learn(a, &root.read().dir);
        }
        let transport = peers.map(|(a, bearer)| Transport::new(id, a, bearer, tx.clone()));
        let purge_db = db.downgrade();
        let (weak_state, weak_db) = (Arc::downgrade(&state), db.downgrade());
        let (jobs, queue) = std::sync::mpsc::channel();
        let applier = Applier::new(
            state.clone(),
            db.clone(),
            repos.iter().map(|(&id, r)| (id, r.progress.clone())),
            tx.clone(),
            addresses.clone(),
        );
        let apply_thread = std::thread::Builder::new()
            .name("ledger-apply".into())
            .spawn(move || applier.run(&queue))?;
        let boot = rand::random::<u64>() | 1;
        // Replay the entries recovery did not (it stops at none, but a
        // log's committed suffix past its state's is handed over here).
        let dirty = repos.keys().copied().collect();
        let mut lp = Loop {
            next_id: id << 48,
            node_id: id,
            boot,
            db,
            state,
            snapshot_every,
            batch,
            applier: jobs,
            settling: Vec::new(),
            unsettled: HashMap::new(),
            transport,
            addresses: addresses.clone(),
            root_log,
            repos,
            hosted: hosted.clone(),
            events: events.clone(),
            root: root.clone(),
            members,
            removed,
            liveness: Liveness::new(id, boot),
            up: HashSet::new(),
            controller: Controller::default(),
            replies: Vec::new(),
            located: Vec::new(),
            waiting: Vec::new(),
            read_applied: Vec::new(),
            dirty,
            lagging: HashSet::new(),
            sequencing: HashSet::new(),
            inbox: VecDeque::new(),
            stats: Stats::default(),
            shared_stats: counters.clone(),
            rounds: Window::new(),
        };
        // Sequence the logs the directory still names this node for: only
        // with a boot of this start, so the root appoints them again.
        let ids: Vec<u64> = lp.repos.keys().copied().collect();
        for log in ids {
            lp.publish(log);
        }
        let (done, stopped) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            // The loop, and with it the stores, are dropped when it returns;
            // the apply thread then finishes its queue and drops its own.
            lp.run(rx).await;
            drop(tokio::task::spawn_blocking(move || apply_thread.join()).await);
            if done.send(true).is_err() {
                tracing::debug!("nobody waits for the engine to stop");
            }
        });
        spawn_purge(purge_db, tx.clone());
        Ok(Arc::new(Engine {
            tx,
            state: weak_state,
            db: weak_db,
            stopped,
            stopping: tokio::sync::watch::Sender::new(false),
            hosted,
            root,
            events,
            stats: counters,
            addresses,
            introduced: RwLock::default(),
            boot,
            id,
        }))
    }

    /// Order and apply a command; resolves once applied on this node.
    pub(crate) async fn propose(&self, cmd: Command) -> Result<Outcome> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Input::Propose(cmd, tx)).map_err(stopped)?;
        rx.await.map_err(stopped)?
    }

    /// Linearizable read barrier: returns once this node has applied
    /// everything committed to `repo`'s log before the call (this node
    /// must sequence it: else [`NotSequencer`]).
    pub(crate) async fn read_barrier(&self, repo: &str) -> Result<()> {
        let Some(log) = log_of(&self.hosted, repo) else {
            return Err(self.elsewhere(repo));
        };
        self.read_index(log, PROPOSAL_TIMEOUT).await
    }

    /// The error for a request about `repo`, which this node does not
    /// host: its sequencer, if it is live.
    fn elsewhere(&self, repo: &str) -> anyhow::Error {
        match self.root.read().dir.live(repo) {
            Some(inc) => NotSequencer {
                sequencer: inc.sequencer,
                url: url_of(self.addresses.as_ref(), inc.sequencer),
            }
            .into(),
            None => NotHosted.into(),
        }
    }

    async fn read_index(&self, log: u64, timeout: Duration) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Input::ReadIndex(log, tx)).map_err(stopped)?;
        tokio::time::timeout(timeout, rx)
            .await
            .context("read barrier timed out")?
            .map_err(stopped)??;
        Ok(())
    }

    /// Follow `repo`'s log (it must be hosted here): its state, then every
    /// change after it. The stream ends when the log is dropped. A
    /// subscriber that falls `FOLLOW_QUEUE` updates behind gets the changes
    /// it missed as one, once it has room again.
    pub(crate) async fn follow(&self, repo: &str) -> Result<Subscription> {
        let (tx, rx) = mpsc::channel(FOLLOW_QUEUE);
        let (resp, done) = oneshot::channel();
        self.tx
            .send(Input::Follow(repo.to_string(), tx, resp))
            .map_err(stopped)?;
        // (Any replica serves it: another node is not hosting it.)
        let log = done.await.map_err(stopped)??;
        Ok(Subscription {
            rx,
            log,
            input: self.tx.clone(),
        })
    }

    /// Propose `entry` to the root log; resolves once applied here (and
    /// durable), whether or not it still changed anything. Fails if it is
    /// not applied within a few seconds (it may still be): proposing it
    /// again is safe.
    pub(crate) async fn propose_root(&self, entry: RootEntry) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let data = entry.encode_to_vec();
        self.tx
            .send(Input::ProposeRoot(data, tx))
            .map_err(stopped)?;
        tokio::time::timeout(ROOT_CALL_TIMEOUT, rx)
            .await
            .context("root proposal timed out")?
            .map_err(stopped)?
    }

    /// Linearizable read barrier on the root log: returns once this node
    /// has applied everything the root committed before the call.
    pub(crate) async fn root_barrier(&self) -> Result<()> {
        self.read_index(ROOT, ROOT_CALL_TIMEOUT).await
    }

    /// Where `repo`'s live incarnation is, as the directory has it here:
    /// hosted here (resolves once its log is durable), or by other nodes.
    pub(crate) async fn locate(&self, repo: &str) -> Result<Located> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Input::Locate(repo.to_string(), tx))
            .map_err(stopped)?;
        rx.await.map_err(stopped)?
    }

    /// A new incarnation of `repo` (in `object_format`), placed by this
    /// node: its replicas (this node and the least loaded live members, in
    /// distinct zones first) and this node as its sequencer.
    pub(crate) fn create(
        &self,
        repo: &str,
        head: Option<String>,
        object_format: i32,
    ) -> crate::proto::Create {
        let root = self.root.read();
        let replicas = root.dir.choose(repo, &[self.id], &root.alive);
        crate::proto::Create {
            repo: repo.to_string(),
            head,
            object_format,
            replicas,
            sequencer: self.id,
            boot: self.boot,
        }
    }

    /// The URL of node `id`, as the registry has it (empty: none).
    pub(crate) fn url(&self, id: u64) -> String {
        url_of(self.addresses.as_ref(), id)
    }

    /// The replicas of `repo`'s live log, and those it is changing from,
    /// as the directory has them here.
    #[must_use]
    pub fn replicas(&self, repo: &str) -> Option<(Vec<u64>, Vec<u64>)> {
        let root = self.root.read();
        let i = root.dir.live(repo)?;
        Some((i.replicas.clone(), i.joint.clone()))
    }

    /// `repo`'s latest incarnation, as the directory has it here.
    pub(crate) fn incarnation(&self, repo: &str) -> Option<crate::proto::Incarnation> {
        self.root.read().dir.repos.get(repo).cloned()
    }

    /// The ledger's members, and the nodes removed from it, as the
    /// directory has them here.
    #[must_use]
    pub fn members(&self) -> (Vec<u64>, Vec<u64>) {
        self.root.read().dir.members()
    }

    /// Where each member is, and the key that checks its tokens, as the
    /// directory has them here.
    #[must_use]
    pub fn peers(&self) -> Vec<Member> {
        self.root.read().dir.peers().cloned().collect()
    }

    /// Member `id` as the directory has it here.
    #[must_use]
    pub fn peer(&self, id: u64) -> Option<Member> {
        self.root.read().dir.peers().find(|p| p.id == id).cloned()
    }

    /// Node `id` as the directory has it (removed since, maybe), else as
    /// it was introduced.
    #[must_use]
    pub fn known(&self, id: u64) -> Option<Member> {
        let node = self.root.read().dir.node(id).cloned();
        node.or_else(|| self.introduced.read().get(&id).cloned())
    }

    /// Whether node `id` was removed from the ledger and holds no log any
    /// more, as applied here (see [`root::Directory::departed`]).
    pub(crate) fn departed(&self, id: u64) -> bool {
        self.root.read().dir.departed(id)
    }

    /// Learn of `peers` that the directory does not have yet (a node that
    /// is joining talks to the members before it has the directory).
    pub(crate) fn introduce(&self, peers: &[Member]) {
        let Some(a) = &self.addresses else { return };
        let new: Vec<&Member> = peers.iter().filter(|p| self.peer(p.id).is_none()).collect();
        let mut introduced = self.introduced.write();
        let mut a = a.write();
        for p in new {
            a.entry(p.id).or_insert_with(|| p.url.clone());
            introduced.insert(p.id, p.clone());
        }
    }

    /// The nodes alive, as this node last saw, sorted.
    #[must_use]
    pub fn alive(&self) -> Vec<u64> {
        let mut alive = self.root.read().alive.clone();
        alive.sort_unstable();
        alive
    }

    /// Whether node `id` answers pings at its address in the directory.
    #[must_use]
    pub fn answers(&self, id: u64) -> bool {
        self.root.read().answering.contains(&id)
    }

    /// The root log's voters, as applied here.
    #[must_use]
    pub fn root_voters(&self) -> Vec<u64> {
        self.root.read().dir.meta.voters.clone()
    }

    /// Whether this node leads the root log.
    pub(crate) fn leads_root(&self) -> bool {
        self.root.read().leading
    }

    /// Deliver frames received from peer `from`.
    pub(crate) fn receive(&self, from: u64, frames: Vec<Frame>) {
        drop(self.tx.send(Input::Frames(from, frames)));
    }

    /// What happened to this node's logs since the last call.
    pub(crate) fn take_events(&self) -> Events {
        std::mem::take(&mut *self.events.lock())
    }

    /// The hosted incarnation of `repo`: its log and epoch, how far its
    /// state is applied, and whether this node sequences it.
    pub fn hosted(&self, repo: &str) -> Option<Hosted> {
        self.hosted.read().get(repo).map(Published::hosted)
    }

    /// The identity and whole state of the hosted incarnation of `repo`,
    /// at the last entry applied, read from the state store (and the log:
    /// inline layers' bytes). Blocking. None if not hosted here.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine has stopped, or the state cannot be
    /// read.
    pub fn record(&self, repo: &str) -> Result<Option<Record>> {
        let Some(log) = log_of(&self.hosted, repo) else {
            return Ok(None);
        };
        let (Some(state), Some(db)) = (self.state.upgrade(), self.db.upgrade()) else {
            return Err(stopped(()));
        };
        Ok(load(&state, &db, log)?.map(|(root, meta)| root.record(meta)))
    }

    /// The repository's current state on this node, read from the state
    /// store. Blocking.
    pub fn repo_meta(&self, repo: &str) -> Option<RepoMeta> {
        match self.record(repo) {
            Ok(r) => r.and_then(|r| r.meta),
            Err(e) => {
                tracing::warn!(repo, "reading the state: {e:#}");
                None
            }
        }
    }

    /// Repositories active (hosted) on this node.
    #[must_use]
    pub fn repos(&self) -> Vec<String> {
        self.hosted.read().keys().cloned().collect()
    }

    /// The summaries of up to `limit` repositories in the directory
    /// (deleted ones too) named with `prefix` and after `after`, in name
    /// order; and whether more follow. A live one hosted here is described
    /// from its state, the others from the directory alone. Blocking.
    pub(crate) fn list(
        &self,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<(Vec<RepoSummary>, bool)> {
        use std::ops::Bound;
        let from = if after >= prefix {
            Bound::Excluded(after)
        } else {
            Bound::Included(prefix)
        };
        let (page, more) = {
            let root = self.root.read();
            let hosted = self.hosted.read();
            let mut names = root
                .dir
                .repos
                .range::<str, _>((from, Bound::Unbounded))
                .take_while(|(n, _)| n.starts_with(prefix));
            let page: Vec<_> = names
                .by_ref()
                .take(limit)
                .map(|(n, i)| {
                    let log = log_id(n, i.epoch);
                    let here = hosted.get(n).is_some_and(|p| p.log == log);
                    (n.clone(), i.epoch, i.deleted, here.then_some(log))
                })
                .collect();
            (page, names.next().is_some())
        };
        let state = self.state.upgrade().ok_or_else(|| stopped(()))?;
        let mut round = state.round();
        let page = page
            .into_iter()
            .map(|(name, epoch, deleted, log)| {
                match log {
                    // (Not if dropped meanwhile.)
                    Some(log) if !deleted && state.contains(log) => {
                        stored::summary(&mut round, log, &name, epoch)
                    }
                    _ => Ok(RepoSummary {
                        name,
                        epoch,
                        deleted,
                        ..Default::default()
                    }),
                }
            })
            .collect::<Result<_>>()?;
        Ok((page, more))
    }

    /// The summary of the hosted incarnation of `repo`: its identity and
    /// what describes it (no refs or layers). Blocking.
    pub(crate) fn summary(&self, repo: &str) -> Result<Option<RepoSummary>> {
        let Some(h) = self.hosted(repo) else {
            return Ok(None);
        };
        let state = self.state.upgrade().ok_or_else(|| stopped(()))?;
        Ok(Some(stored::summary(
            &mut state.round(),
            h.log,
            repo,
            h.epoch,
        )?))
    }

    /// `repo`'s manifest, without inline layers' bytes, if this node hosts
    /// it.
    pub(crate) fn layers(&self, repo: &str) -> Result<Option<Vec<LayerMeta>>> {
        let Some(h) = self.hosted(repo) else {
            return Ok(None);
        };
        let state = self.state.upgrade().ok_or_else(|| stopped(()))?;
        Ok(Some(stored::layers(&state.round(), h.log)?))
    }

    /// A snapshot of the engine's counters.
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.stats.read().clone()
    }

    /// The `Peer` gRPC service for this engine (the transport between
    /// ledger nodes), to mount on the node's server.
    #[must_use]
    pub fn peer_server(
        self: &Arc<Self>,
    ) -> crate::proto::peer_server::PeerServer<super::transport::PeerService> {
        super::transport::server(Arc::downgrade(self))
    }

    /// What this node knows of `repo`'s log, or `None` if it does not host
    /// it.
    #[must_use]
    pub fn repo_status(&self, repo: &str) -> Option<serde_json::Value> {
        let h = self.hosted(repo)?;
        Some(serde_json::json!({
            "node": self.id,
            "repo": repo,
            "epoch": h.epoch,
            "term": h.term,
            "sequencer": (h.sequencer != 0).then_some(h.sequencer),
            "leading": h.leading,
            "version": h.version,
            "applied": h.applied,
        }))
    }

    /// This node's view of its hosted repositories, for the status endpoint.
    #[must_use]
    pub fn status(&self) -> serde_json::Value {
        let hosted = self.hosted.read();
        serde_json::json!({
            "node": self.id,
            "repos": hosted.len(),
            "leading": hosted.values().filter(|h| h.leading).count(),
            "engine": self.stats(),
        })
    }

    /// Ask the loop to stop.
    pub fn shutdown(&self) {
        self.stopping.send_replace(true);
        drop(self.tx.send(Input::Stop));
    }

    /// Becomes true once the engine is asked to stop.
    #[must_use]
    pub fn stopping(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stopping.subscribe()
    }

    /// Wait until the engine has stopped (see [`shutdown`](Self::shutdown))
    /// and closed its stores, so their files may be copied.
    pub async fn stopped(&self) {
        drop(self.stopped.clone().wait_for(|&s| s).await);
    }
}

/// A subscription to a log's updates (see [`Engine::follow`]).
///
/// Dropping it tells the apply thread, which otherwise would keep the closed
/// queue until the log next changes, and an idle log may never change.
pub(crate) struct Subscription {
    rx: mpsc::Receiver<Update>,
    log: u64,
    input: mpsc::UnboundedSender<Input>,
}

impl Subscription {
    /// The next update, or `None` once the subscription has ended.
    pub(crate) async fn recv(&mut self) -> Option<Update> {
        self.rx.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Closed first, so the apply thread sees it closed.
        self.rx.close();
        drop(self.input.send(Input::Unfollow(self.log)));
    }
}

/// The proposal or read id an entry's context carries (0 if none).
fn ctx_id(ctx: &[u8]) -> u64 {
    u64::from_le_bytes(ctx.try_into().unwrap_or([0; 8]))
}

/// The log hosted for `repo`.
fn log_of(hosted: &RwLock<BTreeMap<String, Published>>, repo: &str) -> Option<u64> {
    hosted.read().get(repo).map(|p| p.log)
}

/// Logs whose cells recovery reads in one go.
const RECOVER_CHUNK: usize = 1000;

/// What a round leaves for after its write.
#[derive(Default)]
struct RoundWork {
    /// The root log's `Ready` number, if it had one.
    root_round: Option<u64>,
    /// What the root's storage view takes once durable.
    root_persisted: Option<RootPersisted>,
    /// Root messages that may only be sent once durable.
    root_held: Vec<Message>,
    /// Repository logs written, and the last position each one wrote.
    written: Vec<(u64, u64)>,
}

/// Fail a waiting proposal or read barrier with `e`.
fn reject(kind: Wait, e: anyhow::Error) {
    match kind {
        Wait::Evaluate(proposal) => drop(proposal.resp.send(Err(e))),
        Wait::Read(reader) => reader.fail(e),
        Wait::Root { resp, .. } => drop(resp.send(Err(e))),
    }
}

/// `addresses` are the members' in `dir`. It takes `addresses` only to
/// swap the map, so a caller may hold the root's lock.
fn learn(addresses: &Addresses, dir: &Directory) {
    let peers: HashMap<u64, String> = dir.reachable().map(|p| (p.id, p.url.clone())).collect();
    if *addresses.read() != peers {
        *addresses.write() = peers;
    }
}

/// Stop: this node cannot keep its state. Fail-stop is what
/// replication's safety assumes.
#[expect(
    clippy::panic,
    reason = "a node that cannot persist its state must stop: fail-stop is what replication's safety assumes"
)]
fn fail_stop(e: &anyhow::Error) -> ! {
    panic!("consensus engine: {e:#}");
}

/// Hand the apply thread a job. It stops only with the loop or by
/// failing, and a node whose apply thread failed stops.
fn hand(applier: &std::sync::mpsc::Sender<Job>, job: Job) {
    if let Err(e) = applier.send(job) {
        fail_stop(&anyhow::anyhow!("the apply thread stopped: {e}"));
    }
}

impl Loop {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Input>) {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                input = rx.recv() => if !self.handle(input.unwrap_or(Input::Stop)) {
                    return;
                },
                _ = ticker.tick() => self.tick(),
            }
            // Batch everything else that is pending.
            if !self.drain(&mut rx) {
                return;
            }
            self.retry_waiting();
            if !self.settle(&mut rx).await {
                return;
            }
        }
    }

    /// Process rounds while logs have work (e.g. entries that just became
    /// durable are now committed), taking new inputs in between; false if
    /// the loop must stop.
    async fn settle(&mut self, rx: &mut mpsc::UnboundedReceiver<Input>) -> bool {
        for _ in 0..16 {
            if let Err(e) = self.process().await {
                // Durability failed: we must not continue.
                fail_stop(&e);
            }
            let idle = (self.dirty.iter()).all(|id| self.unsettled.contains_key(id));
            if idle && !self.root_log.dirty && self.inbox.is_empty() {
                break;
            }
            if !self.drain(rx) {
                return false;
            }
            // Issue reads and proposals that arrived meanwhile, or they
            // would wait for the next tick.
            self.retry_waiting();
        }
        true
    }

    /// Handle the inputs pending (and frames to this node itself); false
    /// once told to stop.
    fn drain(&mut self, rx: &mut mpsc::UnboundedReceiver<Input>) -> bool {
        for _ in 0..10_000 {
            let Ok(i) = rx.try_recv() else { break };
            if !self.handle(i) {
                return false;
            }
        }
        self.drain_inbox();
        true
    }

    fn drain_inbox(&mut self) {
        while let Some(f) = self.inbox.pop_front() {
            self.on_frame(self.node_id, f);
        }
    }

    fn tick(&mut self) {
        let now = Instant::now();
        self.root_log.raw.tick();
        self.root_log.dirty = true;
        // Liveness: pings, and who went down or restarted.
        for (to, f) in self.liveness.tick(&self.members) {
            self.send(to, f);
        }
        let up = self.liveness.up();
        let down: Vec<u64> = self.up.difference(&up).copied().collect();
        let came: Vec<u64> = up.difference(&self.up).copied().collect();
        self.up = up;
        if !down.is_empty() || !came.is_empty() {
            self.root.write().alive = self.up.iter().copied().collect();
        }
        let mut answering: Vec<u64> = self.liveness.answering().into_iter().collect();
        answering.sort_unstable();
        if self.root.read().answering != answering {
            self.root.write().answering = answering;
        }
        // Replicas back up are caught up at once, and logs short of
        // replicas may grow onto them now.
        if !came.is_empty() {
            self.lagging.extend(self.sequencing.iter().copied());
            let root = self.root.read();
            self.controller.note_short(&root.dir);
        }
        self.resend(now);
        self.control(&down);
        // Re-issue root read index requests that got no answer: a leader
        // drops them (silently) while it hasn't committed in its term, and
        // forwarded ones can be lost with a leadership change.
        let stale: Vec<u64> = (self.root_log.reads.iter())
            .filter(|(_, r)| now.duration_since(r.issued) > READ_RETRY)
            .map(|(r, _)| *r)
            .collect();
        for rid in stale {
            if let Some(r) = self.root_log.reads.remove(&rid) {
                self.waiting.push(Waiting {
                    log: ROOT,
                    deadline: r.deadline,
                    kind: Wait::Read(r.reader),
                });
            }
        }
        for (_, (resp, _)) in self.root_log.pending.extract_if(|_, (_, d)| *d < now) {
            drop(resp.send(Err(anyhow::anyhow!("root proposal timed out"))));
        }
        self.stats.logs = self.repos.len();
        self.stats.lagging_logs = self.lagging.len();
        let repos = &self.repos;
        self.sequencing
            .retain(|log| repos.get(log).is_some_and(|r| r.seq_term().is_some()));
        self.stats.leading = (self.sequencing.iter())
            .filter(|log| repos.get(log).is_some_and(RepoLog::leading))
            .count();
        *self.shared_stats.write() = self.stats.clone();
    }

    /// The logs this node sequences.
    fn sequenced(&self) -> impl Iterator<Item = (u64, &RepoLog)> {
        (self.sequencing.iter()).filter_map(|&log| Some((log, self.repos.get(&log)?)))
    }

    /// Resend to lagging replicas that are alive, and reads' checks, that
    /// got no answer for a while; ask the root again to end replica
    /// changes.
    fn resend(&mut self, now: Instant) {
        let me = self.node_id;
        let ids: Vec<u64> = self.lagging.iter().copied().collect();
        for log in ids {
            let Some(r) = self.repos.get(&log) else {
                self.lagging.remove(&log);
                continue;
            };
            let lagging = r.node.lagging::<Bytes>();
            if r.node.sequencing::<Bytes>().is_none() || lagging.is_empty() {
                self.lagging.remove(&log);
                continue;
            }
            let stale: Vec<u64> = (lagging.into_iter())
                .filter(|to| {
                    r.sent
                        .get(to)
                        .is_none_or(|t| now.duration_since(*t) > RESEND)
                })
                .filter(|to| self.up.contains(to))
                .collect();
            for to in stale {
                self.retry(log, to);
            }
        }
        // Reads' checks.
        let mut retry = Vec::new();
        for (log, r) in self.sequenced() {
            if r.checked.is_none_or(|t| now.duration_since(t) <= RESEND) {
                continue;
            }
            let replicas = r.node.replicas().unwrap_or_default();
            let waiting = replicas
                .iter()
                .filter(|&&n| r.node.waiting(n).next().is_some());
            retry.extend(waiting.map(|&n| (log, n)));
        }
        for (log, to) in retry {
            if let Some(r) = self.repos.get_mut(&log) {
                r.checked = Some(now);
                let fx = r.node.retry_checks(to);
                self.effects(log, fx);
            }
        }
        // Deposed sequencers the root still names: a seal whose appointment
        // never came. Only the root's leader can give the log a new term.
        let root_leader = self.root_log.raw.raft.leader_id;
        let mut deposed = Vec::new();
        for &log in &self.sequencing {
            let Some(r) = self.repos.get_mut(&log) else {
                continue;
            };
            let Some(term) = r.node.deposed::<Bytes>() else {
                continue;
            };
            let due = r
                .reported
                .is_none_or(|t| now.duration_since(t) > DEPOSED_RETRY);
            if root_leader != 0 && due && r.config.sequencer == me && r.config.term == term {
                r.reported = Some(now);
                let d = crate::proto::Deposed {
                    repo: r.repo.clone(),
                    term,
                };
                deposed.push(framed(log, frame::Kind::Deposed(d)));
            }
        }
        for f in deposed {
            self.send(root_leader, f);
        }
        // Replica changes to end.
        let activate: Vec<crate::proto::Activation> = (self.sequencing.iter())
            .filter_map(|log| {
                let r = self.repos.get_mut(log)?;
                let term = r.node.sequencing::<Bytes>().filter(|_| r.leading())?;
                let due = r
                    .activated
                    .is_none_or(|t| now.duration_since(t) > ACTIVATE_RETRY);
                (!r.config.joint.is_empty() && due).then(|| {
                    r.activated = Some(now);
                    crate::proto::Activation {
                        repo: r.repo.clone(),
                        epoch: r.epoch,
                        term,
                    }
                })
            })
            .collect();
        if !activate.is_empty() {
            let entry: RootEntry = root_entry::Change::Activate(crate::proto::Activate {
                activations: activate,
            })
            .into();
            self.propose_root_quietly(&entry);
        }
    }

    /// On the root's leader: start failovers, and propose the appointments
    /// ready.
    fn control(&mut self, down: &[u64]) {
        // (Not a node removed from the ledger.)
        let leading = self.root.read().leading && self.members.contains(&self.node_id);
        let (seals, appointments) = {
            let root = self.root.read();
            self.controller
                .tick(leading, &root.dir, &self.liveness, down)
        };
        self.stats.seals += count(seals.len());
        for (to, f) in seals {
            self.send(to, f);
        }
        if !appointments.is_empty() {
            self.stats.appointments += count(appointments.len());
            let entry: RootEntry =
                root_entry::Change::Appoint(crate::proto::Appoint { appointments }).into();
            self.propose_root_quietly(&entry);
        }
    }

    /// Handle one input; false if it says to stop.
    fn handle(&mut self, input: Input) -> bool {
        self.rounds.inputs += 1;
        match input {
            Input::Propose(cmd, resp) => self.on_propose(cmd, resp),
            Input::Evaluated(log, data, proposal) => self.on_evaluated(log, data, proposal),
            Input::Confirm(log, version, proposal) => self.on_confirm(log, version, proposal),
            Input::Reevaluate(log, proposal) => self.evaluate(log, proposal),
            Input::Follow(repo, tx, resp) => self.on_follow(&repo, tx, resp),
            Input::Unfollow(log) => self.on_unfollow(log),
            Input::ReadIndex(log, resp) => self.read(log, Reader::Barrier(resp)),
            Input::Frames(from, frames) => {
                for f in frames {
                    self.on_frame(from, f);
                }
            }
            Input::Locate(repo, resp) => self.on_locate(&repo, resp),
            Input::ProposeRoot(data, resp) => {
                self.next_id += 1;
                let pid = self.next_id;
                self.wait(ROOT, Wait::Root { pid, data, resp });
            }
            Input::Retire => self.retire(),
            Input::Purge(ids) => self.on_purge(ids),
            Input::Applied(logs) => {
                let repos = logs.iter().filter_map(|id| self.repos.get(id));
                self.events.lock().extend(repos.map(|r| r.repo.clone()));
                self.answer_reads();
            }
            Input::Durable(durable) => self.on_durable(durable),
            Input::Settled(logs) => self.on_settled(logs),
            Input::Unreachable(peer) => {
                self.root_log.raw.report_unreachable(peer);
                self.root_log.dirty = true;
            }
            Input::Stop => return false,
        }
        true
    }

    fn on_propose(&mut self, cmd: Command, resp: oneshot::Sender<Result<Outcome>>) {
        let Some(log) = log_of(&self.hosted, &cmd.repo) else {
            drop(resp.send(Err(self.not_here(&cmd.repo))));
            return;
        };
        let Some(progress) = self.repos.get(&log).map(|r| *r.progress.lock()) else {
            drop(resp.send(Err(self.not_here(&cmd.repo))));
            return;
        };
        // Refused anyway: keep it out of the log.
        if progress.deleted {
            drop(resp.send(Ok(machine::on_deleted(&cmd, progress.version))));
            return;
        }
        self.evaluate(log, Proposal::new(cmd, resp));
    }

    /// The error for a request about `repo`, not hosted here.
    fn not_here(&self, repo: &str) -> anyhow::Error {
        match self.root.read().dir.live(repo) {
            Some(inc) => self.not_sequencer(inc.sequencer),
            None => NotHosted.into(),
        }
    }

    fn not_sequencer(&self, sequencer: u64) -> anyhow::Error {
        NotSequencer {
            sequencer,
            url: url_of(self.addresses.as_ref(), sequencer),
        }
        .into()
    }

    /// Answer an outcome evaluated without a change at `version`, once a
    /// read shows the repository still at that version: a linearizable
    /// read, so a refusal needs no log entry. A change in the meantime
    /// evaluates the command again.
    fn on_confirm(&mut self, log: u64, version: u64, proposal: Proposal) {
        self.read(log, Reader::Confirm(version, Box::new(proposal)));
    }

    /// Have the sequencer evaluate a proposal (again, after a change
    /// overtook it), unless it has been tried enough.
    fn evaluate(&mut self, log: u64, proposal: Proposal) {
        if let Some(proposal) = proposal.again() {
            self.wait(log, Wait::Evaluate(proposal));
        }
    }

    /// The apply thread evaluated a proposal: append its entry, if this
    /// node still sequences the log.
    fn on_evaluated(&mut self, log: u64, data: Vec<u8>, proposal: Proposal) {
        let proposed = (self.repos.get_mut(&log).filter(|r| r.leading()))
            .and_then(|r| Some((r.node.sequencing::<Bytes>()?, r)))
            .and_then(|(term, r)| Some((term, r.node.propose(Bytes::from(data))?, r)));
        let Some((term, (index, fx), r)) = proposed else {
            hand(&self.applier, Job::Forget(log));
            self.evaluate(log, proposal);
            return;
        };
        r.replies.insert(index, (term, proposal));
        self.dirty.insert(log);
        self.lagging.insert(log);
        self.effects(log, fx);
    }

    fn on_follow(
        &mut self,
        repo: &str,
        tx: mpsc::Sender<Update>,
        resp: oneshot::Sender<Result<u64>>,
    ) {
        let Some(log) = log_of(&self.hosted, repo) else {
            drop(resp.send(Err(NotHosted.into())));
            return;
        };
        hand(&self.applier, Job::Follow { log, tx, resp });
    }

    fn on_unfollow(&mut self, log: u64) {
        hand(&self.applier, Job::Unfollow(log));
    }

    fn read(&mut self, log: u64, reader: Reader) {
        if log != ROOT && !self.repos.contains_key(&log) {
            reader.fail(NotHosted.into());
            return;
        }
        self.wait(log, Wait::Read(reader));
    }

    /// Hold `kind` until the log can take it (see `retry_one`).
    fn wait(&mut self, log: u64, kind: Wait) {
        let timeout = if log == ROOT {
            ROOT_TIMEOUT
        } else {
            PROPOSAL_TIMEOUT
        };
        self.waiting.push(Waiting {
            log,
            deadline: Instant::now() + timeout,
            kind,
        });
        if log == ROOT {
            self.root_log.dirty = true;
        }
    }

    /// Send a frame to a peer (or to this node itself).
    fn send(&mut self, to: u64, f: Frame) {
        if to == self.node_id {
            self.inbox.push_back(f);
            return;
        }
        let Some(t) = &self.transport else { return };
        self.stats.messages_sent += 1;
        if f.log != ROOT && f.log != 0 {
            self.stats.log_messages += 1;
        }
        t.send(to, f);
    }

    /// Resend replica `to` of log `log` what it lacks.
    fn retry(&mut self, log: u64, to: u64) {
        if let Some(r) = self.repos.get_mut(&log) {
            let fx = r.node.retry(to);
            self.effects(log, fx);
        }
    }

    /// Hand log `log`'s node a message from `from`, and carry out what it
    /// does.
    ///
    /// # Panics
    ///
    /// Panics if a request would drop a committed entry: the protocol is
    /// broken, and this node must stop before it spreads.
    fn step(&mut self, log: u64, from: u64, msg: repo::Msg) {
        let Some(r) = self.repos.get_mut(&log) else {
            // Not hosted here (yet: the root's entry hosting it may not
            // have applied): the sequencer sends it again.
            return;
        };
        let request = matches!(msg, repo::Msg::Request(_));
        let fx = match r.node.step(from, msg) {
            Ok(fx) => fx,
            Err(broken) => fail_stop(&anyhow::anyhow!(
                "{} (log {log:x}): a request would drop committed position {}",
                r.repo,
                broken.0
            )),
        };
        if request {
            self.dirty.insert(log);
        }
        self.effects(log, fx);
    }

    /// Carry out what log `log`'s node did: send its messages, write the
    /// states it installed, evaluate again the proposals it lost, and
    /// answer the reads that ended.
    fn effects(&mut self, log: u64, fx: repo::Effects) {
        let now = Instant::now();
        let Some(r) = self.repos.get_mut(&log) else {
            return;
        };
        let mut lost = r.lost();
        let installed = r.node.store_mut().take_installed();
        for i in &installed {
            lost.extend(r.covered(i.index, i.kept));
            r.handed = r.handed.max(i.index);
        }
        let mut read = Vec::new();
        let mut failed = Vec::new();
        for (round, index) in fx.reads {
            let readers = r.readers.remove(&round).unwrap_or_default();
            match index {
                Some(index) => read.extend(readers.into_iter().map(|x| (log, index, x))),
                None => failed.extend(readers),
            }
        }
        let mut requests = false;
        let mut sends = Vec::with_capacity(fx.sends.len());
        for (to, msg) in fx.sends {
            if matches!(msg, repo::Msg::Request(_)) {
                r.sent.insert(to, now);
                requests = true;
            }
            sends.push((to, repo::frame(log, msg)));
        }
        let (repo_name, epoch, sequencer) = (r.repo.clone(), r.epoch, r.config.sequencer);
        if let Some(i) = installed.last() {
            let start = Start {
                index: i.index,
                term: i.last_term,
            };
            let drop_to = (installed.iter().map(|i| i.drop_to))
                .chain(r.start_due.map(|(_, d)| d))
                .max()
                .unwrap_or(0);
            r.start_due = Some((start, drop_to));
        }
        for i in installed {
            let root = stored::root(&repo_name, epoch, i.index, i.last_term);
            let job = Job::Install {
                log,
                root,
                meta: i.meta,
            };
            hand(&self.applier, job);
            self.hold(log);
            self.stats.snapshots_installed += 1;
        }
        self.after_truncate(log, lost);
        if requests {
            self.lagging.insert(log);
        }
        for (to, f) in sends {
            self.send(to, f);
        }
        if fx.committed.is_some() {
            self.dirty.insert(log);
        }
        if !failed.is_empty() || fx.deposed.is_some() {
            for reader in failed {
                reader.fail(self.not_sequencer(sequencer));
            }
            self.publish(log);
        }
        if !read.is_empty() {
            self.read_applied.extend(read);
            self.answer_reads();
        }
    }

    fn on_frame(&mut self, from: u64, f: Frame) {
        let log = f.log;
        let Some(kind) = f.kind else { return };
        match kind {
            frame::Kind::Root(data) => match root_message(&data) {
                // (Not for a node that had this address before.)
                Ok(m) if m.to == self.node_id => {
                    drop(self.root_log.raw.step(m));
                    self.root_log.dirty = true;
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(from, "an undecodable root message: {e:#}"),
            },
            kind @ (frame::Kind::Append(_)
            | frame::Kind::Install(_)
            | frame::Kind::Ack(_)
            | frame::Kind::Seal(_)
            | frame::Kind::Check(_)
            | frame::Kind::Open(_)) => {
                if let Some(msg) = repo::msg(kind) {
                    self.step(log, from, msg);
                }
            }
            frame::Kind::Sealed(s) => {
                let root = self.root.read();
                self.controller.on_sealed(&root.dir, log, from, s);
            }
            frame::Kind::Deposed(d) => {
                let root = self.root.read();
                self.controller.on_deposed(&root.dir, &d.repo, d.term);
            }
            frame::Kind::Ping(p) => {
                let (pong, restarted) = self.liveness.on_ping(from, p);
                self.send(from, pong);
                if restarted {
                    self.restarted(from);
                }
            }
            frame::Kind::Pong(p) => {
                if self.liveness.on_pong(from, p) {
                    self.restarted(from);
                }
            }
        }
    }

    /// `node` restarted: the logs it sequenced need new terms.
    fn restarted(&mut self, node: u64) {
        let root = self.root.read();
        self.controller.note_node(&root.dir, node);
    }

    /// Proposals whose positions were truncated: evaluated again (by the
    /// new sequencer).
    fn after_truncate(&mut self, log: u64, lost: Vec<Proposal>) {
        if lost.is_empty() {
            return;
        }
        hand(&self.applier, Job::Forget(log));
        for p in lost {
            self.evaluate(log, p);
        }
    }

    fn on_locate(&mut self, repo: &str, resp: oneshot::Sender<Result<Located>>) {
        let located = {
            let root = self.root.read();
            match root.dir.live(repo) {
                None => Located::Absent,
                Some(inc) if hosts(inc, self.node_id) => Located::Here,
                Some(inc) => Located::Elsewhere(inc.sequencer),
            }
        };
        if located != Located::Here {
            drop(resp.send(Ok(located)));
            return;
        }
        // Answered once its log's start is durable.
        let hosted = log_of(&self.hosted, repo).is_some();
        if !hosted {
            self.sync_repo(repo);
        }
        let due = log_of(&self.hosted, repo)
            .filter(|l| self.repos.get(l).is_some_and(|r| r.start_due.is_some()));
        match due {
            None if hosted => drop(resp.send(Ok(Located::Here))),
            log => self.located.push((log.unwrap_or_default(), resp)),
        }
    }

    /// This node was removed from the ledger and its logs moved off it (or
    /// a peer says so): drop every repository's log it still hosts.
    fn retire(&mut self) {
        let ids: Vec<u64> = self.repos.keys().copied().collect();
        if !ids.is_empty() {
            tracing::warn!(
                logs = ids.len(),
                "this node was removed from the ledger: dropping its logs"
            );
        }
        for id in ids {
            self.unhost(id);
        }
    }

    /// Compact the logs `ids` hold back, or drop them if not hosted.
    fn on_purge(&mut self, ids: Vec<u64>) {
        for id in ids {
            if id == ROOT || self.repos.contains_key(&id) {
                self.compact(id);
                continue;
            }
            if !is_log(id) {
                continue;
            }
            hand(&self.applier, Job::Drop(id));
            self.batch.add_command(id, raft_engine::Command::Clean);
        }
    }

    fn on_durable(&mut self, durable: Vec<(u64, u64, Option<u64>)>) {
        for (id, index, pin) in durable {
            if let Some(r) = self.repos.get_mut(&id) {
                r.durable = (index, pin);
                self.maybe_snapshot(id);
            }
        }
    }

    /// Issue waiting proposals and reads whose log can take them; keep the
    /// others waiting, or refuse them.
    fn retry_waiting(&mut self) {
        let now = Instant::now();
        for w in std::mem::take(&mut self.waiting) {
            if w.log == ROOT {
                self.retry_root(w, now);
            } else {
                self.retry_one(w, now);
            }
        }
    }

    fn retry_one(&mut self, w: Waiting, now: Instant) {
        let Some(r) = self.repos.get_mut(&w.log) else {
            reject(w.kind, NotHosted.into());
            return;
        };
        if w.deadline < now {
            reject(
                w.kind,
                anyhow::anyhow!("the log's sequencer is not ready (timed out)"),
            );
            return;
        }
        if !r.leading() {
            // A sequencer that has yet to commit its term's no-op serves
            // nothing yet; any other node names the sequencer.
            // (Nor does one that the directory names but has yet to be
            // appointed again: after a restart, or once deposed.)
            if r.node.sequencing::<Bytes>().is_some() || r.config.sequencer == self.node_id {
                self.waiting.push(w);
                return;
            }
            let sequencer = r.config.sequencer;
            reject(w.kind, self.not_sequencer(sequencer));
            return;
        }
        match w.kind {
            Wait::Evaluate(proposal) => {
                // After every entry handed to the apply thread so far.
                let job = Job::Evaluate {
                    log: w.log,
                    proposal,
                };
                hand(&self.applier, job);
            }
            Wait::Read(reader) => {
                // (It leads, so it has a round to wait on.)
                let Some(round) = r.node.read::<Bytes>() else {
                    self.waiting.push(Waiting {
                        kind: Wait::Read(reader),
                        ..w
                    });
                    return;
                };
                r.readers.entry(round).or_default().push(reader);
                self.dirty.insert(w.log);
            }
            Wait::Root { resp, .. } => drop(resp.send(Err(anyhow::anyhow!("not a root log")))),
        }
    }

    /// One round: collect the root log's `Ready` and every repository log's
    /// writes, make them durable with one log store write (group commit
    /// across all logs), then send what required it (acks, seal answers,
    /// raft's votes), advance commit points and apply what committed.
    ///
    /// Synchronous: overlapping the fsync with the next round gains nothing
    /// once the loop is not CPU-bound.
    async fn process(&mut self) -> Result<()> {
        let started = Instant::now();
        let settling = std::mem::take(&mut self.settling);
        if !settling.is_empty() {
            hand(&self.applier, Job::Sync(settling));
        }
        // Logs waiting for their state are left out until it is durable.
        let dirty: Vec<u64> = (std::mem::take(&mut self.dirty).into_iter())
            .filter(|id| !self.unsettled.contains_key(id))
            .collect();
        self.rounds.logs += count(dirty.len());
        let mut batch = std::mem::take(&mut self.batch);
        let (waiting, located) = (std::mem::take(&mut self.located).into_iter())
            .partition(|(log, _)| self.unsettled.contains_key(log));
        self.located = waiting;
        let mut work = RoundWork::default();
        self.reconcile_root();
        if std::mem::take(&mut self.root_log.dirty) {
            self.collect_root(&mut batch, &mut work)?;
        }
        let replies = std::mem::take(&mut self.replies);
        for &id in &dirty {
            self.collect(id, &mut batch, &mut work)?;
        }
        // What committed and was already durable here applies now, not after
        // this round's write: that write holds at most the commit point for
        // it, which a restart recovers from the applied state.
        for &id in &dirty {
            self.apply(id);
        }
        if !batch.is_empty() {
            self.write_batch(batch).await?;
        }
        for resp in replies {
            drop(resp.send(Ok(())));
        }
        for (_, resp) in located {
            drop(resp.send(Ok(Located::Here)));
        }
        self.release(work, &dirty);
        self.answer_reads();
        *self.shared_stats.write() = self.stats.clone();
        self.end_round(started);
        Ok(())
    }

    fn collect(&mut self, id: u64, batch: &mut LogBatch, work: &mut RoundWork) -> Result<()> {
        let Some(r) = self.repos.get_mut(&id) else {
            return Ok(());
        };
        if let Some((start, drop_to)) = r.start_due.take() {
            repo::put_start(batch, id, &start, drop_to)?;
        }
        let (cells, hard) = r.node.store_mut().take_writes();
        if !cells.is_empty() {
            batch.add_entries_with::<Cells, Proto>(id, &cells)?;
            work.written.push((id, Terms::last(r.store())));
        }
        if let Some(hard) = hard {
            repo::put_hard(batch, id, &hard)?;
        }
        // Checks for reads, now that they have their readers.
        let fx = r.node.send_checks::<Bytes>();
        if !fx.sends.is_empty() {
            r.checked = Some(Instant::now());
        }
        self.effects(id, fx);
        Ok(())
    }

    /// Write the round's `batch` to the log store (group commit).
    async fn write_batch(&mut self, mut batch: LogBatch) -> Result<()> {
        let db = self.db.clone();
        let w = Instant::now();
        let n = tokio::task::spawn_blocking(move || db.write(&mut batch, true)).await??;
        self.rounds.write += w.elapsed();
        self.stats.log_writes += 1;
        self.stats.log_bytes += count(n);
        Ok(())
    }

    /// Everything from this round is durable: update the root's storage
    /// view and tell raft, send what required durability, advance the
    /// sequencers' commit points (this node's replica counts now), and hand
    /// what committed to the apply thread.
    fn release(&mut self, work: RoundWork, dirty: &[u64]) {
        if let Some((snap, terms, hs)) = work.root_persisted {
            let mut m = self.root_log.raw.store().meta.write();
            if let Some(snap) = snap {
                m.apply_snapshot(snap);
            }
            m.append(terms);
            if let Some(hs) = hs {
                m.hard_state = hs;
            }
            // raft-rs reads every entry after its applied index (an election
            // scans them for configuration changes): none may be compacted.
            debug_assert!(
                self.root_log.raw.raft.raft_log.applied >= m.snapshot.get_metadata().index,
                "the root log starts after raft's applied index {}",
                self.root_log.raw.raft.raft_log.applied
            );
        }
        if let Some(number) = work.root_round {
            self.root_log.raw.on_persist_ready(number);
            self.root_log.dirty = true;
        }
        for m in work.root_held {
            let to = m.to;
            self.send(to, root_frame(&m));
        }
        for (id, last) in work.written {
            if let Some(r) = self.repos.get_mut(&id) {
                // (Truncated meanwhile: only what is still there.)
                r.node.store_mut().wrote(last);
            }
        }
        // Every write of these logs is durable now: their answers go, and
        // their sequencers count their own copies.
        for &id in dirty {
            let Some(r) = self.repos.get_mut(&id) else {
                continue;
            };
            let written = r.store().written();
            let fx = r.node.synced(&Synced::after_sync(), written);
            self.effects(id, fx);
            self.apply(id);
        }
    }

    /// Hand log `id`'s entries committed and durable here to the apply
    /// thread, with the proposers they answer; tell lagging replicas the
    /// commit point.
    fn apply(&mut self, id: u64) {
        let Some(r) = self.repos.get_mut(&id) else {
            return;
        };
        let upto = r.store().commit().min(r.store().written());
        let mut again = Vec::new();
        while r.handed < upto {
            let cells = r.store().cells_between(r.handed + 1, upto);
            let Some(last) = cells.last().map(|c| c.index) else {
                tracing::error!(
                    repo = r.repo,
                    at = r.handed + 1,
                    "committed cells are missing"
                );
                break;
            };
            let (replies, evaluate) = repo::answered(&mut r.replies, &cells);
            again.extend(evaluate);
            let job = Job::Apply {
                log: id,
                cells,
                replies,
            };
            hand(&self.applier, job);
            r.handed = last;
        }
        r.trim_cache();
        let became = r.leading() && !self.hosted.read().get(&r.repo).is_some_and(|p| p.leading);
        // Replicas not told the commit point yet (and alive), told now.
        let solo = self.transport.is_none();
        let tell: Vec<u64> = (r.node.lagging::<Bytes>().into_iter())
            .filter(|to| solo || self.up.contains(to))
            .collect();
        if became {
            self.publish(id);
        }
        for p in again {
            self.evaluate(id, p);
        }
        for to in tell {
            if let Some(r) = self.repos.get_mut(&id) {
                let fx = r.node.tell(to);
                self.effects(id, fx);
            }
        }
    }

    /// Count a round, and log the statistics once a window is due.
    fn end_round(&mut self, started: Instant) {
        self.rounds.count += 1;
        self.rounds.busy += started.elapsed();
        let Some((span, r)) = self.rounds.due() else {
            return;
        };
        tracing::debug!(
            target: "enroute::ledger_timing",
            rounds_per_s = rate(r.count, span),
            busy_pct = pct(r.busy, span),
            round_us = per(r.busy.as_micros(), r.count),
            write_us = per(r.write.as_micros(), r.count),
            inputs = per(u128::from(r.inputs), r.count),
            logs = per(u128::from(r.logs), r.count),
            "engine rounds"
        );
    }

    /// Answer the readers whose read index this node has applied.
    fn answer_reads(&mut self) {
        for (log, index, reader) in std::mem::take(&mut self.read_applied) {
            let progress = if log == ROOT {
                Some(Progress {
                    applied: self.root.read().dir.meta.applied_index,
                    ..Progress::default()
                })
            } else {
                self.repos.get(&log).map(|r| *r.progress.lock())
            };
            match (progress, reader) {
                (Some(p), reader) if p.applied < index => {
                    self.read_applied.push((log, index, reader));
                }
                (_, Reader::Barrier(resp)) => drop(resp.send(Ok(index))),
                (Some(p), Reader::Confirm(version, proposal)) if p.version == version => {
                    drop(proposal.resp.send(Ok(proposal.outcome)));
                }
                // Changed since: evaluated again.
                (_, Reader::Confirm(_, proposal)) => self.evaluate(log, *proposal),
            }
        }
    }

    /// Act on a change to the directory.
    fn on_root_change(&mut self, change: Change) {
        match change {
            Change::None => {}
            Change::Created(repo, epoch) | Change::Appointed(repo, epoch) => {
                self.controller.applied(&repo, epoch);
                self.sync_repo(&repo);
            }
            Change::Deleted(repo, epoch) => self.unhost(log_id(&repo, epoch)),
            Change::Members => {
                self.members_changed();
                let root = self.root.read();
                self.controller.note_all(&root.dir);
            }
        }
    }

    /// Bring this node's hosting of `repo`'s log in line with the
    /// directory: host it (from its initial state) if it is a replica, drop
    /// it if not, take its new configuration, and start or end its
    /// sequencer here.
    fn sync_repo(&mut self, repo: &str) {
        let inc = self.root.read().dir.live(repo).cloned();
        let current = log_of(&self.hosted, repo);
        let Some(inc) = inc.filter(|i| hosts(i, self.node_id)) else {
            if let Some(log) = current {
                self.unhost(log);
            }
            return;
        };
        let log = log_id(repo, inc.epoch);
        if current != Some(log) {
            if let Some(old) = current {
                self.unhost(old);
            }
            self.host(repo, &inc);
        }
        let Some(r) = self.repos.get_mut(&log) else {
            return;
        };
        let config = Config::of(&inc);
        if r.config != config {
            let sequencer_changed = r.config.sequencer != config.sequencer;
            r.config = config;
            if sequencer_changed {
                let job = Job::Sequencer {
                    log,
                    sequencer: r.config.sequencer,
                };
                hand(&self.applier, job);
            }
        }
        let term = r.config.term;
        let ours = r.config.sequencer == self.node_id && r.config.boot == self.boot;
        if !ours && r.node.sequencing::<Bytes>().is_some_and(|t| t < term) {
            // A term of an older configuration is over: the root sealed it,
            // as if the seal reached this replica too.
            self.step(
                log,
                self.node_id,
                repo::Msg::Request(Request::Seal { term }),
            );
        }
        let Some(r) = self.repos.get_mut(&log) else {
            return;
        };
        if ours && r.seq_term() != Some(term) && r.fence() <= term {
            tracing::info!(repo, term, "sequencing a log");
            let replicas = r.config.replicas.clone();
            let written = r.store().written();
            let fx = r.node.appoint(term, replicas, written);
            self.lagging.insert(log);
            self.sequencing.insert(log);
            self.dirty.insert(log);
            self.effects(log, fx);
        }
        self.publish(log);
    }

    /// Publish log `log`'s sequencer and term to `hosted`.
    fn publish(&mut self, log: u64) {
        let Some(r) = self.repos.get(&log) else {
            return;
        };
        let mut hosted = self.hosted.write();
        let Some(h) = hosted.get_mut(&r.repo) else {
            return;
        };
        let leading = r.leading();
        // A new sequencer tracks what earlier ones may have left to
        // maintain.
        if leading && !h.leading {
            self.events.lock().insert(r.repo.clone());
        }
        (h.leading, h.sequencer, h.term) = (leading, r.config.sequencer, r.config.term);
    }

    /// The directory's members may have changed: a node removed drops its
    /// repositories' logs.
    fn members_changed(&mut self) {
        let root = self.root.read();
        if let Some(a) = &self.addresses {
            learn(a, &root.dir);
        }
        let (members, removed) = root.dir.members();
        // A removed node keeps its logs until they move off it, each as the
        // directory says (see `sync_repo`).
        let departed = root.dir.departed(self.node_id);
        drop(root);
        if (&members, &removed) == (&self.members, &self.removed) {
            return;
        }
        if departed {
            self.retire();
        }
        (self.members, self.removed) = (members, removed);
    }

    /// Host incarnation `inc` of `repo`'s log from its initial state.
    fn host(&mut self, repo: &str, inc: &crate::proto::Incarnation) {
        let id = log_id(repo, inc.epoch);
        let meta = root::initial(inc);
        // Its initial state, durable before (with the round's write) its log.
        let root = stored::root(repo, inc.epoch, 0, 0);
        let progress = Arc::new(Mutex::new(Progress {
            version: meta.version,
            applied: 0,
            deleted: meta.deleted,
        }));
        hand(
            &self.applier,
            Job::Host {
                log: id,
                root,
                meta,
                progress: progress.clone(),
            },
        );
        self.hold(id);
        let config = Config::of(inc);
        let store = LedgerStore::new(id, self.db.clone(), self.state.clone());
        let mut r = RepoLog::new(
            repo.to_string(),
            inc.epoch,
            store,
            progress.clone(),
            config.clone(),
            self.node_id,
        );
        r.start_due = Some((*r.store().start(), 0));
        self.repos.insert(id, r);
        self.hosted.write().insert(
            repo.to_string(),
            Published {
                log: id,
                epoch: inc.epoch,
                leading: false,
                sequencer: config.sequencer,
                term: config.term,
                progress,
            },
        );
        self.dirty.insert(id);
    }

    /// Hold `log`'s writes until the state just handed for it is durable.
    fn hold(&mut self, log: u64) {
        self.settling.push(log);
        *self.unsettled.entry(log).or_default() += 1;
    }

    /// The states `logs` start from are durable: their writes may go.
    fn on_settled(&mut self, logs: Vec<u64>) {
        for log in logs {
            let Some(n) = self.unsettled.get_mut(&log) else {
                continue;
            };
            *n -= 1;
            if *n == 0 {
                self.unsettled.remove(&log);
                self.dirty.insert(log);
            }
        }
    }

    /// Stop hosting a log (its incarnation ended, or it moved off this
    /// node; its subscriptions end with it), and drop it from the log
    /// store.
    fn unhost(&mut self, id: u64) {
        let Some(r) = self.repos.remove(&id) else {
            return;
        };
        self.lagging.remove(&id);
        // A proposal in the log may still commit on the other replicas: its
        // outcome is unknown, so a retry could apply it twice. Reads go to
        // the new sequencer.
        let sequencer = r.config.sequencer;
        for (_, (_, p)) in r.replies {
            drop(p.resp.send(Err(anyhow::anyhow!(
                "this node left the log before the proposal's outcome was known"
            ))));
        }
        for reader in r.readers.into_values().flatten() {
            reader.fail(self.not_sequencer(sequencer));
        }
        // A state without a log is dropped at restart, as is a log without
        // a state: the two deletions need no order.
        hand(&self.applier, Job::Drop(id));
        self.batch.add_command(id, raft_engine::Command::Clean);
        // A hosted log is always its repository's entry.
        self.hosted.write().remove(&r.repo);
    }

    /// Compact a log every `snapshot_every` durably applied entries, and a
    /// repository log as soon as it is quiet: everything in it durably
    /// applied, and (if this node sequences it) every replica caught up.
    /// Otherwise most logs, which take a few entries now and then, would
    /// never compact, and the log store's purges would rewrite them again
    /// and again.
    fn maybe_snapshot(&mut self, id: u64) {
        let (durable, start, quiet) = if id == ROOT {
            let m = self.root_log.raw.store().meta.read();
            (
                self.root_log.durable,
                m.snapshot.get_metadata().index,
                false,
            )
        } else {
            let Some(r) = self.repos.get(&id) else { return };
            let store = r.store();
            let quiet = r.durable.0 >= Terms::last(store) && r.node.lagging::<Bytes>().is_empty();
            (r.durable.0, store.start().index, quiet)
        };
        if durable >= start + self.snapshot_every || (quiet && durable > start) {
            self.compact(id);
        }
    }

    /// Log compaction: drop the entries up to the index the state store has
    /// durably applied (in the round's batch; and at once in memory: those
    /// entries are applied, so nothing reads them), but not the entry of the
    /// oldest inline layer not flushed then, nor any after it.
    fn compact(&mut self, id: u64) {
        let written = if id == ROOT {
            self.compact_root().map(|snap| {
                let index = snap.get_metadata().index;
                put_snapshot(&mut self.batch, ROOT, &snap);
                self.batch
                    .add_command(ROOT, raft_engine::Command::Compact { index: index + 1 });
            })
        } else {
            // (Not before its start is written: that one would replace it.)
            let start = (self.repos.get_mut(&id))
                .filter(|r| r.start_due.is_none())
                .and_then(RepoLog::compact);
            start.map(|start| {
                repo::put_start(&mut self.batch, id, &start, start.index)
                    .unwrap_or_else(|e| fail_stop(&e));
            })
        };
        match (written, id == ROOT) {
            (None, _) => {}
            (Some(()), true) => self.stats.root_compactions += 1,
            (Some(()), false) => self.stats.compactions += 1,
        }
    }
}

mod root_log;

use root_log::{RootLog, RootPersisted, put_snapshot, recover_root};

#[cfg(test)]
mod tests;
