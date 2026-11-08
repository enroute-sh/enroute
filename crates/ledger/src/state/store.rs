//! The ledger's state machines on disk: every repository's refs, HEAD,
//! layer manifest, counters and where its inline layers are, in one
//! LSM tree per node (how a repository maps onto it: [`crate::state::stored`]).
//!
//! **Layout.** Three key spaces in one tree, each under a prefix byte, then
//! keyed by log id:
//!
//! ```text
//! meta    log (8 B)                Root: applied index and term, version, identity
//! data    log ‖ 01                 Counters (version, upload_gen, next_id, layers, ...)
//!         log ‖ 02                 HEAD: the branch it names
//!         log ‖ 03 ‖ first_id      one layer of the manifest (opaque bytes), in id order
//!         log ‖ 04 ‖ ref name      an object id (and when it was set; see `stored`)
//!         log ‖ 05 ‖ index ‖ id    a pin: inline layer `id`'s bytes are in log entry `index`
//! inline  log ‖ layer id           an inline layer's bytes, from a snapshot
//! ```
//!
//! **Inline layers stay in the log.** A push's inline layer travels in its
//! log entry, and the store records only a *pin*: the entry's index. The
//! log may be truncated up to the lower of what a flush covers and the
//! oldest pin ([`StateStore::oldest_pin`]); a flush of the layer drops the
//! pin. While the object store is down pins hold the log back (the inline
//! cap bounds by how much). A snapshot cannot point into the sender's log,
//! so [`StateStore::export`] leaves pins out and the exporter sends the
//! bytes from its log instead ([`Entry::Inline`]); the receiver keeps those
//! in `inline` until their flush.
//!
//! **No write-ahead log.** The log already holds every applied entry,
//! so the store keeps writes in memory until a flush writes them to tables,
//! and a crash loses what was not flushed: recovery replays it from the
//! log. Writes go through a [`Round`]: a run of applied entries, with
//! every touched repository's applied index, in one atomic batch.
//! Installing a whole state (or deleting one) is one batch too. One tree
//! holds all three key spaces, so a flush is atomic across them: a root
//! never gets to disk ahead of the data it names. [`StateStore::flush`]
//! seals the memtable and writes it on a thread of its own, so writers do
//! not wait for the disk; [`StateStore::persist`] waits. The log may
//! forget entries only up to what a flush covers.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use lsm_tree::compaction::Leveled;
use lsm_tree::{AbstractTree, AnyTree, Cache, Config, Guard, SeqNo, SequenceNumberCounter};
use parking_lot::{Mutex, RwLock};
use prost::Message as _;

use enroute_shared::oid::ObjectFormat;

use crate::proto::Record;

/// A repository's log (the id the consensus engine gives it).
pub(crate) type LogId = u64;

/// The 8 bytes of `b` from `at`.
fn word(b: &[u8], at: usize) -> Result<[u8; 8]> {
    b.get(at..)
        .and_then(|t| t.first_chunk::<8>())
        .copied()
        .context("short key or value")
}

/// A stored key without its key space byte.
#[expect(
    clippy::indexing_slicing,
    reason = "every stored key starts with its key space byte; a panic flags corruption"
)]
fn unspaced(k: &[u8]) -> &[u8] {
    &k[1..]
}

/// `b` from `at`.
fn tail(b: &[u8], at: usize) -> Result<&[u8]> {
    b.get(at..).context("short key")
}

/// The prefix byte of each key space.
const META: u8 = 0x00;
const DATA: u8 = 0x01;
const INLINE: u8 = 0x02;

/// Flush at least this often while there are writes in memory: it bounds
/// how far the log's truncation lags, and what recovery replays. The
/// engine's tests wait for truncations.
const FLUSH_EVERY: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(10)
};

/// Compactions run after each flush, at most (each call does one merge).
const COMPACTIONS_PER_FLUSH: usize = 8;

/// The kinds of `data` keys (the byte after the log id; see the layout).
const COUNTERS: u8 = 0x01;
const HEAD: u8 = 0x02;
const LAYER: u8 = 0x03;
const REF: u8 = 0x04;
const PIN: u8 = 0x05;

/// Threads a round's prefetch reads from.
const PREFETCH_THREADS: usize = 8;

/// Where a repository's state is at, and who it is: incarnation `epoch`
/// of `repo`, with its membership as of `applied_index`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Root {
    pub applied_index: u64,
    pub applied_term: u64,
    /// Its counters' version (kept here too, as every root is in memory).
    pub version: u64,
    pub repo: String,
    pub epoch: u64,
}

impl Root {
    /// Who the log is: a `Record` without state, as stored.
    fn identity(&self) -> Record {
        Record {
            repo: self.repo.clone(),
            epoch: self.epoch,
            ..Default::default()
        }
    }

    fn encode(&self) -> Vec<u8> {
        let identity = self.identity().encode_to_vec();
        let mut v = Vec::with_capacity(24 + identity.len());
        for u in [self.applied_index, self.applied_term, self.version] {
            v.extend_from_slice(&u.to_le_bytes());
        }
        v.extend_from_slice(&identity);
        v
    }

    fn decode(v: &[u8]) -> Result<Root> {
        let u = |i: usize| word(v, i).map(u64::from_le_bytes);
        let identity = Record::decode(v.get(24..).context("short root")?)?;
        Ok(Root {
            applied_index: u(0)?,
            applied_term: u(8)?,
            version: u(16)?,
            repo: identity.repo,
            epoch: identity.epoch,
        })
    }
}

/// A repository's counters, how many layers its manifest has, and the
/// other small fields every command reads (see `RepoMeta`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counters {
    pub version: u64,
    pub upload_gen: u64,
    pub next_id: u64,
    pub layers: u64,
    pub last_push_ms: u64,
    pub epoch: u64,
    pub deleted: bool,
    pub keep_head: bool,
    /// The evaluation chain of the last change applied (`RepoMeta.chain`).
    pub chain: u64,
    /// How the incarnation names its objects (`RepoMeta.object_format`).
    pub format: ObjectFormat,
}

const DELETED: u64 = 1;
const KEEP_HEAD: u64 = 2;
/// The incarnation's ids are SHA-256. Unset in every state written before
/// SHA-256, which are SHA-1.
const SHA256: u64 = 4;

impl Counters {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let flags = (u64::from(self.deleted) * DELETED)
            | (u64::from(self.keep_head) * KEEP_HEAD)
            | (u64::from(self.format == ObjectFormat::Sha256) * SHA256);
        [
            self.version,
            self.upload_gen,
            self.next_id,
            self.layers,
            self.last_push_ms,
            self.epoch,
            flags,
            self.chain,
        ]
        .iter()
        .flat_map(|u| u.to_le_bytes())
        .collect()
    }

    pub(crate) fn decode(v: &[u8]) -> Result<Counters> {
        ensure!(v.len() == 64, "bad counters");
        let u = |i: usize| word(v, i * 8).map(u64::from_le_bytes);
        let flags = u(6)?;
        Ok(Counters {
            version: u(0)?,
            upload_gen: u(1)?,
            next_id: u(2)?,
            layers: u(3)?,
            last_push_ms: u(4)?,
            epoch: u(5)?,
            deleted: flags & DELETED != 0,
            keep_head: flags & KEEP_HEAD != 0,
            chain: u(7)?,
            format: if flags & SHA256 == 0 {
                ObjectFormat::Sha1
            } else {
                ObjectFormat::Sha256
            },
        })
    }
}

fn data_key(log: LogId, kind: u8, rest: &[u8]) -> Vec<u8> {
    [&log.to_be_bytes()[..], &[kind], rest].concat()
}

fn inline_key(log: LogId, id: &[u8]) -> Vec<u8> {
    [&log.to_be_bytes()[..], id].concat()
}

fn pin_key(log: LogId, index: u64, id: &[u8]) -> Vec<u8> {
    data_key(log, PIN, &[&index.to_be_bytes()[..], id].concat())
}

/// Every key of `log` in `data` or `inline`: those start with its id, and
/// the next byte (a kind, or a layer id's) is never 0xff.
fn range(log: LogId) -> (Vec<u8>, Vec<u8>) {
    (
        log.to_be_bytes().to_vec(),
        [&log.to_be_bytes()[..], &[0xff]].concat(),
    )
}

/// One entry of a repository's state, as exported for a snapshot and
/// installed from one. Values are opaque here (see `stored`), but for the
/// counters. There are no pins: they name the sender's log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Entry {
    Counters(Counters),
    Head(Vec<u8>),
    Layer {
        first_id: u32,
        value: Vec<u8>,
    },
    Ref {
        name: Vec<u8>,
        value: Vec<u8>,
    },
    /// An inline layer's bytes, by layer id.
    Inline {
        id: Vec<u8>,
        value: Vec<u8>,
    },
}

impl Entry {
    /// The entry of a `data` key (without the log id) and its value; None
    /// for a pin.
    fn from_data(key: &[u8], value: Vec<u8>) -> Result<Option<Entry>> {
        let (&kind, rest) = key.split_first().context("empty key")?;
        Ok(Some(match kind {
            COUNTERS => Entry::Counters(Counters::decode(&value)?),
            HEAD => Entry::Head(value),
            LAYER => Entry::Layer {
                first_id: u32::from_be_bytes(rest.try_into().context("bad layer key")?),
                value,
            },
            REF => Entry::Ref {
                name: rest.to_vec(),
                value,
            },
            PIN => return Ok(None),
            _ => anyhow::bail!("unknown key kind {kind}"),
        }))
    }

    /// The entry's key (with its key space) in `log`'s state, and value.
    fn into_op(self, log: LogId) -> (Vec<u8>, Vec<u8>) {
        let data = |kind, rest: &[u8], value| (spaced(DATA, &data_key(log, kind, rest)), value);
        match self {
            Entry::Counters(c) => data(COUNTERS, &[], c.encode()),
            Entry::Head(v) => data(HEAD, &[], v),
            Entry::Layer { first_id, value } => data(LAYER, &first_id.to_be_bytes(), value),
            Entry::Ref { name, value } => data(REF, &name, value),
            Entry::Inline { id, value } => (spaced(INLINE, &inline_key(log, &id)), value),
        }
    }
}

/// `k` in key space `space`.
fn spaced(space: u8, k: &[u8]) -> Vec<u8> {
    [&[space][..], k].concat()
}

/// The next pair of `it`.
fn next(it: &mut impl Iterator<Item = impl Guard>) -> Result<Option<lsm_tree::KvPair>> {
    Ok(it.next().map(Guard::into_inner).transpose()?)
}

/// The sequence numbers readers read at, and what a new reader sees.
/// Flushes and compactions keep every version a reader can still read.
struct Snapshots {
    open: Mutex<BTreeMap<SeqNo, usize>>,
    /// Readers see every batch below it: raising it publishes a batch. Not
    /// the tree's own visible counter, which a flush raises too.
    visible: SequenceNumberCounter,
}

impl Snapshots {
    fn open(&self) -> SeqNo {
        let mut open = self.open.lock();
        let seqno = self.visible.get();
        *open.entry(seqno).or_default() += 1;
        seqno
    }

    fn close(&self, seqno: SeqNo) {
        let mut open = self.open.lock();
        if let Some(n) = open.get_mut(&seqno) {
            *n -= 1;
            if *n == 0 {
                open.remove(&seqno);
            }
        }
    }

    /// Versions below this may go: no reader reads below it. A read below
    /// what a flush or compaction was given panics in the tree.
    fn watermark(&self) -> SeqNo {
        let open = self.open.lock();
        let visible = self.visible.get();
        open.keys()
            .next()
            .map_or(visible, |&s| s.min(visible))
            .saturating_sub(1)
    }
}

/// A reader's view: every batch published when it was taken.
struct Snapshot<'s> {
    snaps: &'s Snapshots,
    seqno: SeqNo,
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        self.snaps.close(self.seqno);
    }
}

/// A flush for the flusher thread: what to run once it is durable, and
/// where its outcome goes.
struct Flush {
    on_durable: Box<dyn FnOnce() + Send>,
    done: mpsc::Sender<Result<()>>,
}

/// Why a flush has no outcome: an earlier one failed.
const FLUSHER_STOPPED: &str = "state store: the flusher stopped after a failed flush";

/// A flush on its way, from [`StateStore::flush`].
pub(crate) struct Flushed(mpsc::Receiver<Result<()>>);

impl Flushed {
    /// Wait until the flush is durable.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush, or an earlier one, failed.
    pub(crate) fn wait(self) -> Result<()> {
        self.0
            .recv()
            .map_err(|mpsc::RecvError| anyhow::anyhow!(FLUSHER_STOPPED))?
    }
}

pub(crate) struct StateStore {
    tree: AnyTree,
    seqno: SequenceNumberCounter,
    snaps: Arc<Snapshots>,
    /// Serializes batches, and a rotation against them: a sealed memtable
    /// holds whole batches only.
    write: Mutex<()>,
    /// Bytes in the active memtable.
    memtable: AtomicU64,
    flush_bytes: u64,
    last_flush: Mutex<Instant>,
    /// Writes sealed memtables to tables, in order, off the caller's thread.
    flusher: Option<(mpsc::SyncSender<Flush>, JoinHandle<()>)>,
    compactor: Option<(mpsc::Sender<()>, JoinHandle<()>)>,
    roots: RwLock<HashMap<LogId, Root>>,
    /// Each log's pins (index ‖ id, so the first is the oldest), kept with
    /// every write: a sync asks for the oldest pin of every log it covers,
    /// and a range read of the tree for each cost the apply thread time
    /// that grew with the store.
    pins: Mutex<HashMap<LogId, BTreeSet<Vec<u8>>>>,
    /// Held while open: two stores on one directory would lose writes.
    _lock: File,
}

/// Compacts after each flush signal, until the store drops its sender.
fn compact_loop(tree: &AnyTree, snaps: &Snapshots, rx: &mpsc::Receiver<()>) {
    let strategy: Arc<dyn lsm_tree::compaction::CompactionStrategy> = Arc::new(Leveled::default());
    while rx.recv().is_ok() {
        for _ in 0..COMPACTIONS_PER_FLUSH {
            if let Err(e) = tree.compact(strategy.clone(), snaps.watermark()) {
                tracing::warn!("state store: compacting: {e:#}");
                break;
            }
        }
    }
}

impl StateStore {
    /// Open (or create) the store at `path`, within `memory_bytes` of block
    /// cache and memtable.
    pub(crate) fn open(path: &Path, memory_bytes: usize) -> Result<StateStore> {
        let lock = lock(path)?;
        let seqno = SequenceNumberCounter::default();
        let internal = SequenceNumberCounter::default();
        let tree = Config::new(path.join("tree"), seqno.clone(), internal.clone())
            .use_cache(Arc::new(Cache::with_capacity_bytes(
                // Saturating: a size past 64 bits is "all of it".
                u64::try_from(memory_bytes / 2).unwrap_or(u64::MAX),
            )))
            // No key-value separation: kept inline layers are few and live
            // only until their flush, and blob files are not crash-safe (a
            // new one's directory entry is not synced).
            .open()
            .with_context(|| format!("opening the state store at {}", path.display()))?;
        // A read at 0 sees nothing, so batches start at 1.
        let start = tree.get_highest_seqno().map_or(1, |s| s + 1);
        seqno.fetch_max(start);
        internal.fetch_max(start);
        let snaps = Arc::new(Snapshots {
            open: Mutex::default(),
            visible: SequenceNumberCounter::new(start),
        });
        let roots = load_roots(&tree, start)?;
        let pins = load_pins(&tree, roots.keys().copied(), start)?;
        let compactor = spawn_compactor(tree.clone(), snaps.clone())?;
        let flusher = spawn_flusher(tree.clone(), snaps.clone(), compactor.0.clone())?;
        Ok(StateStore {
            tree,
            seqno,
            snaps,
            write: Mutex::new(()),
            memtable: AtomicU64::new(0),
            // At most 64 MiB, so it always fits.
            flush_bytes: u64::try_from((memory_bytes / 8).min(64 << 20)).unwrap_or(64 << 20),
            last_flush: Mutex::new(Instant::now()),
            flusher: Some(flusher),
            compactor: Some(compactor),
            roots: RwLock::new(roots),
            pins: Mutex::new(pins),
            _lock: lock,
        })
    }

    fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            snaps: &self.snaps,
            seqno: self.snaps.open(),
        }
    }

    /// Write `ops` (None: delete) as one batch, and publish it.
    fn write(&self, ops: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>)>) {
        let _w = self.write.lock();
        let s = self.seqno.next();
        let mut memtable = None;
        let mut pins = self.pins.lock();
        for (k, v) in ops {
            if let Some((log, pin)) = pin_of(&k) {
                track_pin(&mut pins, log, pin, v.is_some());
            }
            let (_, size) = match v {
                Some(v) => self.tree.insert(k, v, s),
                None => self.tree.remove(k, s),
            };
            memtable = Some(size);
        }
        if let Some(size) = memtable {
            self.memtable.store(size, Ordering::Relaxed);
        }
        self.snaps.visible.fetch_max(s + 1);
    }

    /// The keys (with their key space) in `space` from `from` to `to`.
    fn keys(&self, space: u8, from: &[u8], to: &[u8], seqno: SeqNo) -> Result<Vec<Vec<u8>>> {
        self.tree
            .range(spaced(space, from)..spaced(space, to), seqno, None)
            .map(|g| Ok(g.key()?.to_vec()))
            .collect()
    }

    /// Whether a flush is due: much is in memory, or has been for long.
    pub(crate) fn due(&self) -> bool {
        let bytes = self.memtable.load(Ordering::Relaxed);
        bytes > 0 && (bytes >= self.flush_bytes || self.last_flush.lock().elapsed() >= FLUSH_EVERY)
    }

    /// Seal everything written so far, and write it to a table on the
    /// flusher thread, which then runs `on_durable`. A flush costs as much
    /// as the memtable holds, so the caller waits only if the flusher is a
    /// flush behind. Flushes run in order, and fail from the first that
    /// fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the store is closed, or the flusher has stopped
    /// after a failed flush.
    pub(crate) fn flush(&self, on_durable: impl FnOnce() + Send + 'static) -> Result<Flushed> {
        {
            let _w = self.write.lock();
            self.tree.rotate_memtable();
            self.memtable.store(0, Ordering::Relaxed);
        }
        *self.last_flush.lock() = Instant::now();
        let (done, flushed) = mpsc::channel();
        let (tx, _) = self.flusher.as_ref().context("state store: closed")?;
        tx.send(Flush {
            on_durable: Box::new(on_durable),
            done,
        })
        .map_err(|mpsc::SendError(_)| anyhow::anyhow!(FLUSHER_STOPPED))?;
        Ok(Flushed(flushed))
    }

    /// Flush now: everything written so far is durable when this returns.
    pub(crate) fn persist(&self) -> Result<()> {
        self.flush(|| ())?.wait()
    }

    /// The root of `log`'s state, if it has one.
    pub(crate) fn root(&self, log: LogId) -> Option<Root> {
        self.roots.read().get(&log).cloned()
    }

    /// Whether `log` has a state here.
    pub(crate) fn contains(&self, log: LogId) -> bool {
        self.roots.read().contains_key(&log)
    }

    /// Every repository with a state here.
    pub(crate) fn logs(&self) -> Vec<LogId> {
        self.roots.read().keys().copied().collect()
    }

    /// Start a round of writes.
    pub(crate) fn round(&self) -> Round<'_> {
        Round {
            store: self,
            snap: self.snapshot(),
            roots: HashMap::new(),
            counters: HashMap::new(),
            overlay: BTreeMap::new(),
            written: BTreeSet::new(),
            inline_deletes: BTreeSet::new(),
        }
    }

    /// The oldest log entry `log`'s state still reads inline bytes from:
    /// its log may be truncated below it (and up to what a flush covers),
    /// not past it. None if no inline layer is pinned.
    pub(crate) fn oldest_pin(&self, log: LogId) -> Result<Option<u64>> {
        let pins = self.pins.lock();
        let Some(first) = pins.get(&log).and_then(BTreeSet::first) else {
            return Ok(None);
        };
        Ok(Some(u64::from_be_bytes(word(first, 0)?)))
    }

    /// `log`'s whole state at one point: its root, and every entry passed to
    /// `f` (refs in order, then inline layers' bytes), until `f` fails.
    /// None if it has no state. Pins are not exported: they name this
    /// node's log (the exporter sends the bytes they pin instead).
    pub(crate) fn export(
        &self,
        log: LogId,
        mut f: impl FnMut(Entry) -> Result<()>,
    ) -> Result<Option<Root>> {
        let snap = self.snapshot();
        let Some(v) = self
            .tree
            .get(spaced(META, &log.to_be_bytes()), snap.seqno)?
        else {
            return Ok(None);
        };
        let root = Root::decode(&v)?;
        let (from, to) = range(log);
        let mut it = self
            .tree
            .range(spaced(DATA, &from)..spaced(DATA, &to), snap.seqno, None);
        while let Some((k, v)) = next(&mut it)? {
            if let Some(e) = Entry::from_data(tail(&k, 9)?, v.to_vec())? {
                f(e)?;
            }
        }
        let mut it = self
            .tree
            .range(spaced(INLINE, &from)..spaced(INLINE, &to), snap.seqno, None);
        while let Some((k, v)) = next(&mut it)? {
            f(Entry::Inline {
                id: tail(&k, 9)?.to_vec(),
                value: v.to_vec(),
            })?;
        }
        Ok(Some(root))
    }

    /// Replace `log`'s state with `entries` at `root` (its version is the
    /// counters' among them), in one batch, not flushed (see
    /// [`persist`](Self::persist)).
    pub(crate) fn install(
        &self,
        log: LogId,
        mut root: Root,
        entries: impl IntoIterator<Item = Entry>,
    ) -> Result<()> {
        // No range deletes: delete what is there, then put the new entries.
        // A batch can hold a key only once (its writes share a sequence
        // number), so a new entry replaces its delete.
        let mut ops: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            self.clear(log)?.into_iter().map(|k| (k, None)).collect();
        for e in entries {
            if let Entry::Counters(c) = &e {
                root.version = c.version;
            }
            let (k, v) = e.into_op(log);
            ops.insert(k, Some(v));
        }
        ops.insert(spaced(META, &log.to_be_bytes()), Some(root.encode()));
        self.write(ops);
        self.roots.write().insert(log, root);
        Ok(())
    }

    /// Delete `log`'s state (not flushed).
    pub(crate) fn delete(&self, log: LogId) -> Result<()> {
        let mut ops: Vec<_> = self.clear(log)?.into_iter().map(|k| (k, None)).collect();
        ops.push((spaced(META, &log.to_be_bytes()), None));
        self.write(ops);
        self.roots.write().remove(&log);
        Ok(())
    }

    /// Every `data` and `inline` key of `log`.
    fn clear(&self, log: LogId) -> Result<Vec<Vec<u8>>> {
        let snap = self.snapshot();
        let (from, to) = range(log);
        let mut keys = self.keys(DATA, &from, &to, snap.seqno)?;
        keys.extend(self.keys(INLINE, &from, &to, snap.seqno)?);
        Ok(keys)
    }
}

/// Lock the store's directory at `path`, creating it if needed.
fn lock(path: &Path) -> Result<File> {
    std::fs::create_dir_all(path)?;
    let lock = File::options()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path.join("lock"))?;
    // A node stopping in this process (tests restart nodes in place) may
    // still hold the lock for a moment.
    let mut tries = 0;
    loop {
        match lock.try_lock() {
            Ok(()) => return Ok(lock),
            Err(std::fs::TryLockError::WouldBlock) if tries < 100 => {
                tries += 1;
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => anyhow::bail!("locking the state store at {}: {e}", path.display()),
        }
    }
}

/// The log and pin (index ‖ id) of `k`, a stored key, if it is a pin.
fn pin_of(k: &[u8]) -> Option<(LogId, &[u8])> {
    let (&[DATA], rest) = k.split_first_chunk::<1>()? else {
        return None;
    };
    let (log, rest) = rest.split_first_chunk::<8>()?;
    let (&[PIN], pin) = rest.split_first_chunk::<1>()? else {
        return None;
    };
    Some((u64::from_be_bytes(*log), pin))
}

/// Add (or drop) `log`'s `pin` in `pins`.
fn track_pin(pins: &mut HashMap<LogId, BTreeSet<Vec<u8>>>, log: LogId, pin: &[u8], add: bool) {
    let of = pins.entry(log).or_default();
    if add {
        of.insert(pin.to_vec());
    } else {
        of.remove(pin);
    }
    if of.is_empty() {
        pins.remove(&log);
    }
}

/// The pins of `logs`, as [`StateStore::pins`] keeps them.
fn load_pins(
    tree: &AnyTree,
    logs: impl IntoIterator<Item = LogId>,
    seqno: SeqNo,
) -> Result<HashMap<LogId, BTreeSet<Vec<u8>>>> {
    let mut pins = HashMap::new();
    for log in logs {
        let from = spaced(DATA, &data_key(log, PIN, &[]));
        let to = spaced(DATA, &data_key(log, PIN + 1, &[]));
        let mut it = tree.range(from..to, seqno, None);
        let mut of = BTreeSet::new();
        while let Some((k, _)) = next(&mut it)? {
            if let Some((_, pin)) = pin_of(&k) {
                of.insert(pin.to_vec());
            }
        }
        if !of.is_empty() {
            pins.insert(log, of);
        }
    }
    Ok(pins)
}

/// Every repository's root, as of `seqno`.
fn load_roots(tree: &AnyTree, seqno: SeqNo) -> Result<HashMap<LogId, Root>> {
    let mut roots = HashMap::new();
    let mut it = tree.prefix([META], seqno, None);
    while let Some((k, v)) = next(&mut it)? {
        let key = <[u8; 8]>::try_from(tail(&k, 1)?).context("bad root key")?;
        roots.insert(u64::from_be_bytes(key), Root::decode(&v)?);
    }
    Ok(roots)
}

/// The thread that compacts the tree after each flush it is told of.
fn spawn_compactor(
    tree: AnyTree,
    snaps: Arc<Snapshots>,
) -> Result<(mpsc::Sender<()>, JoinHandle<()>)> {
    let (tx, rx) = mpsc::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("state-compact".into())
        .spawn(move || compact_loop(&tree, &snaps, &rx))?;
    Ok((tx, thread))
}

/// The thread that writes sealed memtables to tables, one flush at a time
/// in the order sent, then tells `compact`. It stops at the first failure,
/// so that nothing after it is reported durable.
fn spawn_flusher(
    tree: AnyTree,
    snaps: Arc<Snapshots>,
    compact: mpsc::Sender<()>,
) -> Result<(mpsc::SyncSender<Flush>, JoinHandle<()>)> {
    // One flush waiting at most: past that, sealed memtables would pile up
    // in memory while the disk is behind.
    let (tx, rx) = mpsc::sync_channel::<Flush>(1);
    let thread = std::thread::Builder::new()
        .name("state-flush".into())
        .spawn(move || {
            while let Ok(Flush { on_durable, done }) = rx.recv() {
                let r = tree.flush(&tree.get_flush_lock(), snaps.watermark());
                if let Err(e) = r {
                    let e = anyhow::Error::from(e).context("state store: flushing");
                    tracing::error!("{e:#}");
                    // The waiter may be gone.
                    drop(done.send(Err(e)));
                    return;
                }
                if compact.send(()).is_err() {
                    tracing::warn!("state store: the compactor has stopped");
                }
                on_durable();
                drop(done.send(Ok(())));
            }
        })?;
    Ok((tx, thread))
}

impl Drop for StateStore {
    /// Flush, so the next open has nothing to replay from the log.
    fn drop(&mut self) {
        if let Err(e) = self.persist() {
            tracing::warn!("state store: flushing at shutdown: {e:#}");
        }
        // The flusher holds a sender to the compactor: it stops first.
        if let Some((tx, flusher)) = self.flusher.take() {
            drop(tx);
            // A flusher panic is already reported by the panic hook.
            drop(flusher.join());
        }
        if let Some((tx, compactor)) = self.compactor.take() {
            drop(tx);
            // A compactor panic is already reported by the panic hook.
            drop(compactor.join());
        }
    }
}

/// The writes of a run of applied entries, as one atomic batch by
/// [`Round::commit`]. Reads and scans through a round see its own writes,
/// over the store as it was when the round started.
pub(crate) struct Round<'s> {
    store: &'s StateStore,
    snap: Snapshot<'s>,
    /// Roots changed in this round (applied indexes, versions).
    roots: HashMap<LogId, Root>,
    counters: HashMap<LogId, Counters>,
    /// Data keys written in this round (None: deleted), and keys read: what
    /// the round's reads and scans see over the store.
    overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// The keys of `overlay` to write at commit. A batch can hold a key
    /// only once, so the round keeps the last write of each.
    written: BTreeSet<Vec<u8>>,
    /// `inline` keys (with their key space) to delete.
    inline_deletes: BTreeSet<Vec<u8>>,
}

impl Round<'_> {
    /// Write (or, with None, delete) a `data` key.
    fn write(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.written.insert(key.clone());
        self.overlay.insert(key, value);
    }

    /// Visit the `data` keys in `from..to` in order, this round's writes
    /// included, until `f` returns false.
    #[expect(
        clippy::expect_used,
        reason = "the merge just peeked at these entries; absence is a broken invariant"
    )]
    fn scan(&self, from: &[u8], to: &[u8], mut f: impl FnMut(&[u8], &[u8]) -> bool) -> Result<()> {
        let mut ours = self
            .overlay
            .range::<[u8], _>((Bound::Included(from), Bound::Excluded(to)))
            .peekable();
        let mut it =
            self.store
                .tree
                .range(spaced(DATA, from)..spaced(DATA, to), self.snap.seqno, None);
        let mut stored = next(&mut it)?;
        loop {
            // The smaller key next; ours wins a tie.
            let take_ours = match (ours.peek(), &stored) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((k, _)), Some((s, _))) => k.as_slice() <= unspaced(s),
            };
            let theirs = (!take_ours).then(|| stored.take().expect("some"));
            if let Some((k, v)) = &theirs
                && !f(unspaced(k), v)
            {
                break;
            }
            if !take_ours {
                stored = next(&mut it)?;
                continue;
            }
            let (k, v) = ours.next().expect("peeked");
            if stored
                .as_ref()
                .is_some_and(|(s, _)| unspaced(s) == k.as_slice())
            {
                stored = next(&mut it)?;
            }
            if v.as_ref().is_some_and(|v| !f(k, v)) {
                break;
            }
        }
        Ok(())
    }

    /// Read the refs `keys` names and the counters of `logs`, in one go,
    /// from several threads: the round's compare-and-swaps then wait for
    /// no reads, and the reads overlap (a cache miss is a disk read).
    pub(crate) fn prefetch(&mut self, keys: &[(LogId, &[u8])], logs: &[LogId]) -> Result<()> {
        let mut want: Vec<Vec<u8>> = keys
            .iter()
            .map(|(log, name)| data_key(*log, REF, name))
            .filter(|k| !self.overlay.contains_key(k))
            .collect();
        want.extend(
            logs.iter()
                .filter(|log| !self.counters.contains_key(log))
                .map(|log| data_key(*log, COUNTERS, &[])),
        );
        want.sort_unstable();
        want.dedup();
        let (tree, seqno) = (&self.store.tree, self.snap.seqno);
        let chunk = want.len().div_ceil(PREFETCH_THREADS).max(256);
        // The tree has no batched get.
        let read = |part: &[Vec<u8>]| {
            part.iter()
                .map(|k| Ok(tree.get(spaced(DATA, k), seqno)?.map(|v| v.to_vec())))
                .collect::<Vec<_>>()
        };
        // A small round reads here: a thread per part costs more than it saves.
        let got: Vec<Result<Option<Vec<u8>>>> = if want.len() <= chunk {
            read(&want)
        } else {
            std::thread::scope(|s| {
                let parts: Vec<_> = want
                    .chunks(chunk)
                    .map(|part| s.spawn(move || read(part)))
                    .collect();
                parts
                    .into_iter()
                    .flat_map(|p| p.join().unwrap_or_else(|e| std::panic::resume_unwind(e)))
                    .collect()
            })
        };
        for (k, v) in want.into_iter().zip(got) {
            let v = v?;
            if k.get(8) == Some(&COUNTERS) {
                let log = u64::from_be_bytes(word(&k, 0)?);
                self.counters.insert(
                    log,
                    v.as_deref()
                        .map(Counters::decode)
                        .transpose()?
                        .unwrap_or_default(),
                );
            } else {
                self.overlay.insert(k, v);
            }
        }
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.overlay.get(&key) {
            return Ok(v.clone());
        }
        let v = self
            .store
            .tree
            .get(spaced(DATA, &key), self.snap.seqno)?
            .map(|v| v.to_vec());
        self.overlay.insert(key, v.clone());
        Ok(v)
    }

    /// A ref's object id.
    pub(crate) fn get_ref(&mut self, log: LogId, name: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(data_key(log, REF, name))
    }

    /// Set (or, with None, delete) a ref.
    pub(crate) fn set_ref(&mut self, log: LogId, name: &[u8], oid: Option<&[u8]>) {
        self.write(data_key(log, REF, name), oid.map(<[u8]>::to_vec));
    }

    /// The first ref (in name order) starting with `prefix`, this round's
    /// writes included.
    pub(crate) fn first_ref(&mut self, log: LogId, prefix: &[u8]) -> Result<Option<Vec<u8>>> {
        let from = data_key(log, REF, prefix);
        let mut first = None;
        self.scan(&from, &data_key(log, REF + 1, &[]), |k, _| {
            first = k
                .starts_with(&from)
                .then(|| k.get(9..).map(<[u8]>::to_vec))
                .flatten();
            false
        })?;
        Ok(first)
    }

    /// The branch HEAD names.
    pub(crate) fn head(&mut self, log: LogId) -> Result<Option<Vec<u8>>> {
        self.get(data_key(log, HEAD, &[]))
    }

    pub(crate) fn set_head(&mut self, log: LogId, branch: &[u8]) {
        self.write(data_key(log, HEAD, &[]), Some(branch.to_vec()));
    }

    /// The value of the first layer of the manifest whose first id is at
    /// least `first_id`, this round's writes included.
    pub(crate) fn next_layer(&self, log: LogId, first_id: u32) -> Result<Option<Vec<u8>>> {
        let mut out = None;
        self.scan(
            &data_key(log, LAYER, &first_id.to_be_bytes()),
            &data_key(log, LAYER + 1, &[]),
            |_, v| {
                out = Some(v.to_vec());
                false
            },
        )?;
        Ok(out)
    }

    /// The values of every layer of the manifest, in id order, this round's
    /// writes included.
    pub(crate) fn layers(&self, log: LogId) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        self.scan(
            &data_key(log, LAYER, &[]),
            &data_key(log, LAYER + 1, &[]),
            |_, v| {
                out.push(v.to_vec());
                true
            },
        )?;
        Ok(out)
    }

    /// One layer of the manifest, by its first id.
    pub(crate) fn layer(&mut self, log: LogId, first_id: u32) -> Result<Option<Vec<u8>>> {
        self.get(data_key(log, LAYER, &first_id.to_be_bytes()))
    }

    pub(crate) fn put_layer(&mut self, log: LogId, first_id: u32, layer: &[u8]) {
        self.write(
            data_key(log, LAYER, &first_id.to_be_bytes()),
            Some(layer.to_vec()),
        );
    }

    pub(crate) fn delete_layer(&mut self, log: LogId, first_id: u32) {
        self.write(data_key(log, LAYER, &first_id.to_be_bytes()), None);
    }

    /// Inline layer `id`'s bytes are in log entry `index` (the entry being
    /// applied): the log keeps it until [`Round::drop_inline`].
    pub(crate) fn pin_inline(&mut self, log: LogId, index: u64, id: &[u8]) {
        self.write(pin_key(log, index, id), Some(Vec::new()));
    }

    /// Inline layer `id` (pinned at `index`) is flushed: drop its pin, or
    /// its bytes if a snapshot brought them.
    pub(crate) fn drop_inline(&mut self, log: LogId, index: u64, id: &[u8]) {
        self.write(pin_key(log, index, id), None);
        self.inline_deletes
            .insert(spaced(INLINE, &inline_key(log, id)));
    }

    /// Drop `log`'s refs, manifest, pins and inline bytes (a deletion).
    /// There are no range deletes, so this reads every key it drops.
    pub(crate) fn clear_state(&mut self, log: LogId) -> Result<()> {
        let (from, to) = (data_key(log, LAYER, &[]), data_key(log, PIN + 1, &[]));
        let seqno = self.snap.seqno;
        let mut drop: Vec<Vec<u8>> = self
            .store
            .keys(DATA, &from, &to, seqno)?
            .into_iter()
            .map(|k| unspaced(&k).to_vec())
            .collect();
        drop.extend(self.overlay.range(from..to).map(|(k, _)| k.clone()));
        for k in drop {
            self.write(k, None);
        }
        let (from, to) = range(log);
        self.inline_deletes
            .extend(self.store.keys(INLINE, &from, &to, seqno)?);
        Ok(())
    }

    pub(crate) fn counters(&mut self, log: LogId) -> Result<Counters> {
        if let Some(c) = self.counters.get(&log) {
            return Ok(*c);
        }
        let c = self
            .get(data_key(log, COUNTERS, &[]))?
            .map(|v| Counters::decode(&v))
            .transpose()?
            .unwrap_or_default();
        self.counters.insert(log, c);
        Ok(c)
    }

    pub(crate) fn set_counters(&mut self, log: LogId, c: Counters) {
        self.counters.insert(log, c);
    }

    /// `log`'s root, to change in this round.
    pub(crate) fn root_mut(&mut self, log: LogId) -> Result<&mut Root> {
        // In place once in the round: this runs for every entry applied.
        Ok(match self.roots.entry(log) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => e.insert(
                self.store
                    .root(log)
                    .with_context(|| format!("no state for log {log:x}"))?,
            ),
        })
    }

    /// Write the round, not flushed.
    #[expect(
        clippy::expect_used,
        reason = "every written key is in the overlay; absence is a broken invariant"
    )]
    pub(crate) fn commit(mut self) -> Result<()> {
        let store = self.store;
        for (log, c) in std::mem::take(&mut self.counters) {
            self.root_mut(log)?.version = c.version;
            self.overlay
                .insert(data_key(log, COUNTERS, &[]), Some(c.encode()));
            self.written.insert(data_key(log, COUNTERS, &[]));
        }
        let mut ops =
            Vec::with_capacity(self.written.len() + self.inline_deletes.len() + self.roots.len());
        for k in &self.written {
            let v = self.overlay.remove(k).expect("written");
            ops.push((spaced(DATA, k), v));
        }
        ops.extend(self.inline_deletes.iter().map(|k| (k.clone(), None)));
        for (log, root) in &self.roots {
            ops.push((spaced(META, &log.to_be_bytes()), Some(root.encode())));
        }
        drop(self.snap);
        store.write(ops);
        store.roots.write().extend(self.roots);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
