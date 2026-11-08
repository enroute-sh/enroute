//! A repository's replicated log, as one node hosts it: a
//! [`shardlog::Node`] over the ledger's log store.
//!
//! The protocol is [`super::shardlog`]'s, and so is every decision in it:
//! the node takes appends, seals, installs and checks, and on the node the
//! root appointed, sequences the log and runs read rounds. This module only
//! gives it a store ([`LedgerStore`]) and carries its messages as frames;
//! the engine syncs the store and hands what committed to the apply thread.
//!
//! In the log store, a log is its cells ([`StoredCell`]: an empty value is
//! a no-op), where it starts ([`Start`]) and its replica's hard state
//! ([`Hard`]): the ledger's own records, not raft-rs's.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::Mutex;
use prost::Message as _;
use raft_engine::LogBatch;

use enroute_shared::state::RepoMeta;

use crate::logdb::LogDb;
use crate::proto::{Effect, Hard, Incarnation, Record, Start, StoredCell, frame};
use crate::state::machine::count;
use crate::state::store::{Root, StateStore};
use crate::state::stored;

use super::apply::{Progress, Proposal};
use super::codec::{Cells, Proto};
use super::engine::Reader;
use super::shardlog::{self, Cell, Pos, Response, Store, Term, Terms};
use super::transport::{Frame, frame as framed};

/// The keys of a repository log's start and hard state in the log store.
const START_KEY: &[u8] = b"start";
const HARD_KEY: &[u8] = b"hard";

/// Entries sent in one append, at most (and the bytes, roughly).
const APPEND_ENTRIES: usize = 256;
const APPEND_BYTES: usize = 4 << 20;

/// Entries kept in memory past those applied, for sending to lagging
/// replicas without reading the log store.
const KEEP: usize = 1024;

/// Positions a sequencer appends past its commit point, at most: in
/// effect none (the engine's round bounds what is in flight).
const WINDOW: Pos = 1 << 40;

/// A log's node, as the ledger runs it.
pub(super) type Node = shardlog::Node<u64, LedgerStore>;
/// A message between nodes of a log, as the protocol has it.
pub(super) type Msg = shardlog::Msg<Bytes, Record>;
/// What the engine does after a node's input.
pub(super) type Effects = shardlog::Effects<u64, Bytes, Record>;

/// Saturating: positions and lengths never reach the limits.
fn to_usize(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

/// A state installed from the sequencer, for the engine to carry out: the
/// apply thread takes it, and the log store drops the cells it covers.
pub(super) struct Installed {
    pub index: u64,
    pub last_term: u64,
    pub meta: RepoMeta,
    /// The cells up to here go from the log store.
    pub drop_to: u64,
    /// Whether the cells after `index` stayed.
    pub kept: bool,
}

/// A log's positions on this node, as accepted: where it starts, the terms
/// of its cells after that, and its replica's hard state. Only the node
/// changes them, through [`Store`].
#[derive(Clone, Debug, Default)]
pub(super) struct Positions {
    start: Start,
    terms: VecDeque<u64>,
    hard: Hard,
}

impl Positions {
    fn first(&self) -> u64 {
        self.start.index + 1
    }

    fn last(&self) -> u64 {
        self.start.index + count(self.terms.len())
    }

    fn term(&self, pos: u64) -> Option<u64> {
        if pos == self.start.index {
            return Some(self.start.term);
        }
        let i = pos.checked_sub(self.first())?;
        self.terms.get(to_usize(i)).copied()
    }

    /// Cells of `terms` from `from` on, in place of any there.
    fn put(&mut self, from: u64, terms: impl IntoIterator<Item = u64>) {
        self.terms
            .truncate(to_usize(from.saturating_sub(self.first())));
        self.terms.extend(terms);
    }

    /// The log starts after `index`, of `term`; the cells after it stay
    /// if `keep`.
    fn start_at(&mut self, index: u64, term: u64, keep: bool) {
        if keep {
            let n = to_usize(index.saturating_sub(self.start.index)).min(self.terms.len());
            self.terms.drain(..n);
        } else {
            self.terms.clear();
        }
        self.start = Start { index, term };
    }
}

/// A log as the log store holds it: where it starts, its cells after that,
/// and its hard state (all absent for a log it does not hold).
pub(super) type StoredLog = (Option<Start>, Vec<StoredCell>, Option<Hard>);

/// Read log `log` from the log store, on its reader thread.
pub(super) fn read_log(db: &raft_engine::Engine, log: u64) -> raft_engine::Result<StoredLog> {
    let start = db.get_value::<Start, Proto>(log, START_KEY)?;
    let hard = db.get_value::<Hard, Proto>(log, HARD_KEY)?;
    let from = start.as_ref().map_or(0, |s| s.index) + 1;
    let cells = match db.last_index(log).filter(|&l| l >= from) {
        Some(last) => fetch(db, log, from, last + 1, None)?,
        None => Vec::new(),
    };
    Ok((start, cells, hard))
}

/// Log `log`'s cells `low..high` (at most `max_size` bytes, but at least
/// one), read on the store's reader thread (see [`LogDb::read`]).
pub(super) fn fetch(
    db: &raft_engine::Engine,
    log: u64,
    low: u64,
    high: u64,
    max_size: Option<usize>,
) -> raft_engine::Result<Vec<StoredCell>> {
    let mut cells = Vec::with_capacity(to_usize(high.saturating_sub(low).min(1024)));
    db.fetch_entries_to_with::<Cells, Proto>(log, low, high, max_size, &mut cells)?;
    Ok(cells)
}

/// A cell's effect, if it is not a no-op (of log `log`, for the error).
pub(super) fn effect(log: u64, c: &StoredCell) -> Option<Effect> {
    if c.value.is_empty() {
        return None;
    }
    match Effect::decode(c.value.as_ref()) {
        Ok(e) => Some(e),
        Err(err) => {
            tracing::error!(log, index = c.index, "undecodable cell: {err}");
            None
        }
    }
}

/// Put log `log`'s hard state in a write.
pub(super) fn put_hard(batch: &mut LogBatch, log: u64, hard: &Hard) -> Result<()> {
    batch.put_value::<Proto, _>(log, HARD_KEY.to_vec(), hard)?;
    Ok(())
}

/// Put log `log`'s start in a write, and drop its cells up to `drop_to`.
pub(super) fn put_start(batch: &mut LogBatch, log: u64, start: &Start, drop_to: u64) -> Result<()> {
    batch.put_value::<Proto, _>(log, START_KEY.to_vec(), start)?;
    if drop_to > 0 {
        batch.add_command(log, raft_engine::Command::Compact { index: drop_to + 1 });
    }
    Ok(())
}

/// The effect at position `index` of repository log `log`.
fn effect_at(db: &LogDb, log: u64, index: u64) -> Result<Effect> {
    let cells = db.read(move |db| fetch(db, log, index, index + 1, None))?;
    let c = cells.first().context("no such cell")?;
    Ok(Effect::decode(c.value.as_ref())?)
}

/// `log`'s root and whole state at the last entry applied, inline layers'
/// bytes read from the log. None if it has no state.
pub(super) fn load(store: &StateStore, db: &LogDb, log: u64) -> Result<Option<(Root, RepoMeta)>> {
    stored::load(store, log, |index| effect_at(db, log, index))
}

/// A log's store on this node: the log as accepted (cells in it may not
/// be durable yet, see `written`), the hard state (fence and commit point),
/// and the writes for the engine's next round. Its fields are its own: the
/// engine reads them and takes its writes, and only the node changes the
/// log and the hard state (through [`Store`]).
pub(super) struct LedgerStore {
    log: u64,
    db: LogDb,
    state: Arc<StateStore>,
    pos: Positions,
    /// Cells accepted and not yet applied, or kept for lagging replicas.
    cache: BTreeMap<u64, StoredCell>,
    /// Cells accepted since the last write, for the next one: contiguous.
    unwritten: Vec<StoredCell>,
    /// Whether the fence or commit point changed since the last write.
    hard_dirty: bool,
    /// The last position durable in the log store.
    written: u64,
    /// The last cell the log store holds once the writes taken land: a
    /// truncation waiting in `unwritten` has not dropped the cells after
    /// it there yet.
    stored: u64,
    /// The first position dropped since the engine last looked (its
    /// proposals there are answered by no cell).
    truncated: Option<u64>,
    /// States installed since the engine last looked.
    installed: Vec<Installed>,
    /// The start a trim moved the log to since the engine last looked.
    trimmed: Option<Start>,
}

impl LedgerStore {
    /// A new log's store: empty, from position 1.
    pub(super) fn new(log: u64, db: LogDb, state: Arc<StateStore>) -> Self {
        Self::of(log, db, state, Positions::default())
    }

    fn of(log: u64, db: LogDb, state: Arc<StateStore>, pos: Positions) -> Self {
        let written = pos.last();
        LedgerStore {
            log,
            db,
            state,
            pos,
            cache: BTreeMap::new(),
            unwritten: Vec::new(),
            hard_dirty: false,
            written,
            stored: written,
            truncated: None,
            installed: Vec::new(),
            trimmed: None,
        }
    }

    /// A log's store as the log store holds it (`read`), with its state
    /// machine `applied` up to that position of that term: the cells after
    /// that are returned, for the caller to apply up to the commit point.
    /// A state installed in the state store before a crash took the log
    /// with it is carried out here: the log is reset to it (in `batch`).
    /// None if the log store does not hold the log (torn).
    pub(super) fn recover(
        log: u64,
        (db, state): (&LogDb, &Arc<StateStore>),
        read: StoredLog,
        (applied, applied_term): (u64, u64),
        batch: &mut LogBatch,
    ) -> Result<Option<(Self, Vec<StoredCell>)>> {
        let (Some(start), mut cells, hard) = read else {
            return Ok(None);
        };
        // The log was trimmed past what the state store kept: the cells in
        // between are gone (the sync before trimming failed us).
        anyhow::ensure!(
            applied >= start.index,
            "its state is at {applied}, before the log's start ({})",
            start.index
        );
        let mut pos = Positions {
            start,
            terms: VecDeque::new(),
            hard: hard.unwrap_or_default(),
        };
        pos.put(pos.first(), cells.iter().map(|c| c.term));
        if applied > pos.last() || pos.term(applied) != Some(applied_term) {
            let drop_to = pos.last().max(applied);
            pos.start_at(applied, applied_term, false);
            put_start(batch, log, &pos.start, drop_to)?;
            cells.clear();
        }
        cells.retain(|c| c.index > applied);
        Ok(Some((Self::of(log, db.clone(), state.clone(), pos), cells)))
    }

    /// Where the log starts.
    pub(super) fn start(&self) -> &Start {
        &self.pos.start
    }

    /// The last position durable in the log store.
    pub(super) fn written(&self) -> u64 {
        self.written
    }

    /// The writes since the last round's: the cells accepted, and the
    /// hard state if it changed.
    pub(super) fn take_writes(&mut self) -> (Vec<StoredCell>, Option<Hard>) {
        let cells = std::mem::take(&mut self.unwritten);
        if let Some(c) = cells.last() {
            // An append drops every cell after the ones it carries.
            self.stored = c.index;
        }
        let hard = std::mem::take(&mut self.hard_dirty).then_some(self.pos.hard);
        (cells, hard)
    }

    /// The log store holds the log durably up to `last` (or what is still
    /// there of it, if it was truncated since).
    pub(super) fn wrote(&mut self, last: u64) {
        self.written = self.written.max(last.min(self.pos.last()));
    }

    /// The states installed since the last look.
    pub(super) fn take_installed(&mut self) -> Vec<Installed> {
        std::mem::take(&mut self.installed)
    }

    /// The start a trim moved the log to since the last look.
    pub(super) fn take_trimmed(&mut self) -> Option<Start> {
        self.trimmed.take()
    }

    /// The cells `from..=to`, as many as one append carries, from the
    /// cache or else the log store.
    pub(super) fn cells_between(&self, from: u64, to: u64) -> Vec<StoredCell> {
        if from > to {
            return Vec::new();
        }
        if self.cache.contains_key(&from) {
            let mut bytes = 0;
            return (self.cache.range(from..=to).zip(from..))
                .take_while(|((i, _), want)| **i == *want)
                .take(APPEND_ENTRIES)
                .map(|((_, c), _)| c)
                .take_while(|c| {
                    let fits = bytes <= APPEND_BYTES;
                    bytes += c.value.len();
                    fits
                })
                .cloned()
                .collect();
        }
        let high = to.min(from + count(APPEND_ENTRIES) - 1) + 1;
        let log = self.log;
        let mut out = match self
            .db
            .read(move |db| fetch(db, log, from, high, Some(APPEND_BYTES)))
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(log, from, high, "reading cells: {e:#}");
                Vec::new()
            }
        };
        // Then the cache, for what the store does not have yet.
        let next = from + count(out.len());
        if next <= to && self.cache.contains_key(&next) {
            out.extend(self.cells_between(next, to));
        }
        out
    }

    fn truncate_at(&mut self, from: u64) {
        self.truncated = Some(self.truncated.map_or(from, |t| t.min(from)));
    }
}

impl Terms for LedgerStore {
    fn start(&self) -> u64 {
        self.pos.first()
    }

    fn last(&self) -> u64 {
        self.pos.last()
    }

    fn term(&self, pos: u64) -> Option<u64> {
        self.pos.term(pos)
    }
}

impl Store<Bytes> for LedgerStore {
    type State = Record;

    fn fence(&self) -> Term {
        self.pos.hard.fence
    }

    fn commit(&self) -> Pos {
        self.pos.hard.commit
    }

    fn set_fence(&mut self, term: Term) {
        if term != self.fence() {
            self.pos.hard.fence = term;
            self.hard_dirty = true;
        }
    }

    fn set_commit(&mut self, commit: Pos) {
        if commit != self.commit() {
            self.pos.hard.commit = commit;
            self.hard_dirty = true;
        }
    }

    fn cells(&self, from: Pos) -> Option<Vec<Cell<Bytes>>> {
        if from < Terms::start(self) {
            return None;
        }
        let cells = self.cells_between(from, self.last());
        Some(
            (cells.into_iter())
                .map(|c| Cell {
                    term: c.term,
                    value: (!c.value.is_empty()).then_some(c.value),
                })
                .collect(),
        )
    }

    fn write(&mut self, from: Pos, cells: Vec<Cell<Bytes>>) {
        if from <= self.last() {
            self.truncate_at(from);
            self.cache.split_off(&from);
            self.written = self.written.min(from - 1);
        }
        match self.unwritten.first().map(|c| c.index) {
            Some(u) if from >= u => self.unwritten.truncate(to_usize(from - u)),
            _ => self.unwritten.clear(),
        }
        let cells: Vec<StoredCell> = (cells.into_iter().zip(from..))
            .map(|(c, index)| StoredCell {
                index,
                term: c.term,
                value: c.value.unwrap_or_default(),
            })
            .collect();
        self.pos.put(from, cells.iter().map(|c| c.term));
        for c in &cells {
            self.cache.insert(c.index, c.clone());
        }
        self.unwritten.extend(cells);
    }

    fn snapshot(&self) -> Option<(Pos, Term, Record)> {
        match load(&self.state, &self.db, self.log) {
            Ok(Some((root, meta))) => {
                Some((root.applied_index, root.applied_term, root.record(meta)))
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(log = self.log, "reading a state to install: {e:#}");
                None
            }
        }
    }

    fn install(&mut self, index: Pos, last_term: Term, state: Record, keep: bool) {
        // The cells after it stay if they follow from it; else every one
        // goes, the log store's too. The log store must not drop a
        // position a write still to come starts at (that write truncates
        // what it replaces), nor keep one it does not hold any more.
        let drop_to = if keep {
            self.unwritten
                .first()
                .map_or(index, |c| index.min(c.index - 1))
        } else {
            self.unwritten.clear();
            self.cache.clear();
            let drop_to = index.max(self.stored);
            self.stored = index;
            drop_to
        };
        self.pos.start_at(index, last_term, keep);
        self.written = self.written.max(index);
        self.cache = self.cache.split_off(&(index + 1));
        self.installed.push(Installed {
            index,
            last_term,
            meta: state.meta.unwrap_or_default(),
            drop_to,
            kept: keep,
        });
    }

    fn trim(&mut self, below: Pos) {
        let index = below - 1;
        let Some(term) = self.pos.term(index) else {
            return;
        };
        self.pos.start_at(index, term, true);
        self.trimmed = Some(self.pos.start);
    }
}

/// A log's configuration, as the root's directory has it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Config {
    pub term: u64,
    pub sequencer: u64,
    pub boot: u64,
    pub replicas: Vec<u64>,
    pub joint: Vec<u64>,
}

impl Config {
    pub(super) fn of(inc: &Incarnation) -> Config {
        Config {
            term: inc.term,
            sequencer: inc.sequencer,
            boot: inc.boot,
            replicas: inc.replicas.clone(),
            joint: inc.joint.clone(),
        }
    }
}

/// A hosted repository's log.
pub(super) struct RepoLog {
    pub repo: String,
    pub epoch: u64,
    pub node: Node,
    /// As the apply thread publishes it.
    pub progress: Arc<Mutex<Progress>>,
    /// The last entry durably applied, and the oldest entry pinned by an
    /// inline layer then: how far the log may be compacted.
    pub durable: (u64, Option<u64>),
    pub config: Config,
    /// The last position handed to the apply thread.
    pub handed: u64,
    /// Proposals waiting for their entry (by position, with its term).
    pub replies: BTreeMap<u64, (u64, Proposal)>,
    /// Readers waiting on the node's read rounds, by round.
    pub readers: BTreeMap<u64, Vec<Reader>>,
    /// When requests last went to each replica.
    pub sent: HashMap<u64, Instant>,
    /// When the read rounds' checks last went out.
    pub checked: Option<Instant>,
    /// When it last asked the root to end a replica change.
    pub activated: Option<Instant>,
    /// When it last told the root's leader it was deposed.
    pub reported: Option<Instant>,
    /// Where the log starts, and the index it drops entries up to: to
    /// write once the state it starts from is durable.
    pub start_due: Option<(Start, u64)>,
}

impl RepoLog {
    pub(super) fn new(
        repo: String,
        epoch: u64,
        store: LedgerStore,
        progress: Arc<Mutex<Progress>>,
        config: Config,
        me: u64,
    ) -> RepoLog {
        RepoLog {
            repo,
            epoch,
            node: Node::new(me, store, WINDOW),
            progress,
            // Nothing replayed counts until the apply thread's first sync.
            durable: (0, None),
            config,
            handed: 0,
            replies: BTreeMap::new(),
            readers: BTreeMap::new(),
            sent: HashMap::new(),
            checked: None,
            activated: None,
            reported: None,
            start_due: None,
        }
    }

    pub(super) fn store(&self) -> &LedgerStore {
        self.node.store()
    }

    pub(super) fn fence(&self) -> u64 {
        self.store().fence()
    }

    /// Whether this node sequences the log and may take proposals and
    /// reads.
    pub(super) fn leading(&self) -> bool {
        self.node.leading::<Bytes>()
    }

    /// The term of this node's sequencer, live or deposed.
    pub(super) fn seq_term(&self) -> Option<u64> {
        let live = self.node.sequencing::<Bytes>();
        live.or_else(|| self.node.deposed::<Bytes>())
    }

    /// The proposals answered by no entry since the last look: those
    /// truncated.
    pub(super) fn lost(&mut self) -> Vec<Proposal> {
        let Some(from) = self.node.store_mut().truncated.take() else {
            return Vec::new();
        };
        let lost = self.replies.split_off(&from);
        lost.into_values().map(|(_, p)| p).collect()
    }

    /// The proposals an installed state covers, at `index` and before
    /// (every one if the entries after it went too).
    pub(super) fn covered(&mut self, index: u64, kept: bool) -> Vec<Proposal> {
        let rest = if kept {
            self.replies.split_off(&(index + 1))
        } else {
            BTreeMap::new()
        };
        let lost = std::mem::replace(&mut self.replies, rest);
        lost.into_values().map(|(_, p)| p).collect()
    }

    /// Drop cached entries no longer needed: applied, and (on a sequencer)
    /// matched by every replica, or beyond [`KEEP`].
    pub(super) fn trim_cache(&mut self) {
        let mut keep_from = self.handed + 1;
        if let Some(slowest) = self.node.slowest() {
            keep_from = keep_from
                .min(slowest + 1)
                .max(self.handed.saturating_sub(count(KEEP)));
        }
        let store = self.node.store_mut();
        let rest = store.cache.split_off(&keep_from);
        store.cache = rest;
    }

    /// Trim the log up to what the state store durably applied, but not
    /// past the oldest unflushed inline layer, nor what is not committed and
    /// written: the new start, if it moved (in memory now; the caller
    /// writes it).
    pub(super) fn compact(&mut self) -> Option<Start> {
        let (durable, pin) = self.durable;
        let below = pin
            .map_or(durable, |p| durable.min(p - 1))
            .min(self.store().written())
            + 1;
        self.node.trim::<Bytes>(below);
        self.node.store_mut().take_trimmed()
    }
}

/// A frame for log `log` carrying `msg`.
pub(super) fn frame(log: u64, msg: Msg) -> Frame {
    use crate::proto::ack::Kind;
    use shardlog::Request;
    let kind = match msg {
        Msg::Request(Request::Append(a)) => frame::Kind::Append(crate::proto::Append {
            term: a.term,
            prev: a.prev,
            prev_term: a.prev_term,
            cells: (a.cells.into_iter())
                .map(|c| crate::proto::Cell {
                    term: c.term,
                    data: c.value.unwrap_or_default(),
                })
                .collect(),
            commit: a.commit,
        }),
        Msg::Request(Request::Install {
            term,
            index,
            last_term,
            state,
            commit,
        }) => frame::Kind::Install(crate::proto::Install {
            term,
            index,
            last_term,
            commit,
            record: state.encode_to_vec().into(),
        }),
        Msg::Request(Request::Seal { term }) => frame::Kind::Seal(crate::proto::Seal { term }),
        Msg::Ack { term, response } => {
            let kind = match response {
                Response::Appended { last } => Kind::Appended(last),
                Response::Behind { last } => Kind::Behind(last),
                Response::Fenced { fence } => Kind::Fenced(fence),
                // Not answers to appends.
                Response::Sealed { .. } | Response::Open(_) => Kind::Fenced(term),
            };
            frame::Kind::Ack(crate::proto::Ack {
                term,
                kind: Some(kind),
            })
        }
        Msg::Sealed {
            term,
            last,
            last_term,
            fence,
        } => frame::Kind::Sealed(crate::proto::Sealed {
            term,
            last,
            last_term,
            fence,
        }),
        Msg::Check { term, round } => frame::Kind::Check(crate::proto::Check { term, round }),
        Msg::Open { round, open } => frame::Kind::Open(crate::proto::Open { round, open }),
    };
    framed(log, kind)
}

/// The message a frame for a log's node carries, if it is one (seal
/// answers go to the controller instead), and decodes.
pub(super) fn msg(kind: frame::Kind) -> Option<Msg> {
    use crate::proto::ack::Kind;
    use shardlog::{Append, Request};
    Some(match kind {
        frame::Kind::Append(a) => Msg::Request(Request::Append(Append {
            term: a.term,
            prev: a.prev,
            prev_term: a.prev_term,
            cells: (a.cells.into_iter())
                .map(|c| Cell {
                    term: c.term,
                    value: (!c.data.is_empty()).then_some(c.data),
                })
                .collect(),
            commit: a.commit,
        })),
        frame::Kind::Install(i) => {
            let state = match Record::decode(i.record.as_ref()) {
                Ok(rec) => rec,
                Err(e) => {
                    tracing::warn!("an undecodable state from the sequencer: {e:#}");
                    return None;
                }
            };
            Msg::Request(Request::Install {
                term: i.term,
                index: i.index,
                last_term: i.last_term,
                state,
                commit: i.commit,
            })
        }
        frame::Kind::Seal(s) => Msg::Request(Request::Seal { term: s.term }),
        frame::Kind::Ack(a) => Msg::Ack {
            term: a.term,
            response: match a.kind? {
                Kind::Appended(last) => Response::Appended { last },
                Kind::Behind(last) => Response::Behind { last },
                Kind::Fenced(fence) => Response::Fenced { fence },
            },
        },
        frame::Kind::Check(c) => Msg::Check {
            term: c.term,
            round: c.round,
        },
        frame::Kind::Open(o) => Msg::Open {
            round: o.round,
            open: o.open,
        },
        _ => return None,
    })
}

/// The proposals of `replies` whose cell is in `cells` with their term,
/// by position; the others (another term's cell took their position) are
/// returned to evaluate again.
pub(super) fn answered(
    replies: &mut BTreeMap<u64, (u64, Proposal)>,
    cells: &[StoredCell],
) -> (HashMap<u64, Proposal>, Vec<Proposal>) {
    let mut ok = HashMap::new();
    let mut again = Vec::new();
    for c in cells {
        if let Some((term, p)) = replies.remove(&c.index) {
            if term == c.term {
                ok.insert(c.index, p);
            } else {
                again.push(p);
            }
        }
    }
    (ok, again)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells(terms: &[u64]) -> Vec<Cell<Bytes>> {
        (terms.iter())
            .map(|&term| Cell {
                term,
                value: Some(Bytes::from_static(b"x")),
            })
            .collect()
    }

    /// A store on a fresh log store, holding cells 1..=10 of term 1.
    fn written(dir: &std::path::Path) -> (LogDb, LedgerStore) {
        let db = crate::logdb::open(&dir.join("log")).unwrap();
        let state = Arc::new(StateStore::open(&dir.join("state"), 16 << 20).unwrap());
        let mut store = LedgerStore::new(7, db.clone(), state);
        store.write(1, cells(&[1; 10]));
        write(&db, &mut store, None);
        (db, store)
    }

    /// One round's write: `before` (an install's start), then the cells.
    fn write(db: &LogDb, store: &mut LedgerStore, before: Option<&Installed>) {
        let mut batch = LogBatch::default();
        if let Some(i) = before {
            let start = Start {
                index: i.index,
                term: i.last_term,
            };
            put_start(&mut batch, 7, &start, i.drop_to).unwrap();
        }
        let (cells, _) = store.take_writes();
        batch.add_entries_with::<Cells, Proto>(7, &cells).unwrap();
        db.write(&mut batch, true).unwrap();
        store.wrote(store.last());
    }

    fn terms(db: &LogDb, from: u64) -> Vec<u64> {
        let last = db.last_index(7).unwrap();
        let read = fetch(db, 7, from, last + 1, None).unwrap();
        read.iter().map(|c| c.term).collect()
    }

    fn record() -> Record {
        Record::default()
    }

    #[test]
    fn an_install_drops_cells_a_pending_truncation_would_have() {
        let dir = tempfile::tempdir().unwrap();
        let (db, mut store) = written(dir.path());
        // A new sequencer rewrites 5..: not written yet.
        store.write(5, cells(&[2, 2]));
        // Then a state at 5 of another term: every cell goes.
        store.install(5, 3, record(), false);
        let installed = store.take_installed();
        store.write(6, cells(&[3]));
        write(&db, &mut store, installed.first());
        assert_eq!(terms(&db, 6), [3]);
    }

    #[test]
    fn a_kept_install_keeps_a_pending_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let (db, mut store) = written(dir.path());
        store.write(5, cells(&[2, 2, 2]));
        // A state at 6 that the rewritten cells follow from.
        store.install(6, 2, record(), true);
        let installed = store.take_installed();
        write(&db, &mut store, installed.first());
        assert_eq!(terms(&db, 7), [2]);
    }
}
