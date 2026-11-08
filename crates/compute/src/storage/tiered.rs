//! Tiered reads of immutable files: pool pages in memory, the file on local
//! disk, and the object store, where the file is durable. Whatever the file
//! is (a layer, an index), the mechanism is the same; local disk is a cache
//! tier.
//!
//! Every local copy is one file and a bitmap of the chunks present in it: a
//! copy written here has every chunk, and a file opened from the object
//! store starts with none. Local disk does not outlive the process, so the
//! bitmap is the only record of the chunks.
//!
//! **Nothing synchronous ever waits on a remote tier.** Synchronous reads (the pool's page loads) only ever read the local file:
//! bytes in a chunk that is not present yet fail with [`NotResident`],
//! which carries the handle to load them. Readers load what they need
//! first, or are resumable (see `repo::machine`); there is no fallback. Remote bytes arrive only through
//! the asynchronous [`TieredSource::load`], which coalesces missing chunks
//! into large ranged requests, runs them concurrently and fetches each
//! chunk once however many readers ask for it. Without a disk budget, the
//! rest of a file opened remotely is loaded in the background.

use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use foldhash::{HashMap, HashMapExt};
use futures::StreamExt;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt};
use parking_lot::Mutex;
use sha2::Digest;
use tokio::sync::watch;

use super::handles::{Handle, Handles};
use super::pool::{Access, PageSource, to_u64, to_usize};

/// Ranged reads of one remote object.
#[async_trait::async_trait]
pub(super) trait RangeFetch: Send + Sync {
    async fn get(&self, range: Range<u64>) -> Result<Bytes>;
}

/// A file in the object store.
pub(super) struct StoreFetch {
    pub store: Arc<dyn ObjectStore>,
    pub key: StorePath,
}

#[async_trait::async_trait]
impl RangeFetch for StoreFetch {
    async fn get(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(self.store.get_range(&self.key, range).await?)
    }
}

/// Unit of presence in a sparse local file: a load fetches whole chunks.
pub(super) const CHUNK: u64 = 1 << 20;

/// The chunk size in use: [`CHUNK`], or `ENROUTE_TIER_CHUNK` bytes in tests.
pub(super) fn chunk_bytes() -> u64 {
    static N: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *N.get_or_init(|| super::test_size("ENROUTE_TIER_CHUNK").unwrap_or(CHUNK))
}
/// Largest remote request: adjacent missing chunks are fetched together up
/// to this size (object stores serve 8-16 MiB ranges most efficiently).
const MAX_REQUEST: u64 = 16 << 20;
/// Requests in flight per load.
const LOAD_CONCURRENCY: usize = 8;

/// Bytes of a file that are not on local disk yet. Synchronous readers
/// return it (wrapped in their error) instead of waiting; an asynchronous
/// caller loads the bytes ([`load_pinned`]) and resumes.
#[derive(Clone)]
pub struct NotResident {
    source: Arc<TieredSource>,
    range: Range<u64>,
}

impl NotResident {
    /// Fetch the missing bytes to local disk.
    async fn load(&self, access: Access) -> Result<()> {
        self.source.load(self.range.clone(), access).await
    }

    /// How many bytes.
    pub fn len(&self) -> u64 {
        self.range.end - self.range.start
    }

    /// Merge waits for nearby bytes of the same file into larger loads
    /// (a gap of up to a chunk is cheaper to fetch than another request).
    fn coalesce(mut v: Vec<NotResident>) -> Vec<NotResident> {
        if v.len() < 2 {
            return v;
        }
        v.sort_by_key(|n| (Arc::as_ptr(&n.source).addr(), n.range.start));
        let mut out: Vec<NotResident> = Vec::with_capacity(v.len());
        for n in v {
            match out.last_mut() {
                Some(l)
                    if Arc::ptr_eq(&l.source, &n.source)
                        && n.range.start <= l.range.end + l.source.chunk =>
                {
                    l.range.end = l.range.end.max(n.range.end);
                }
                _ => out.push(n),
            }
        }
        out
    }

    /// Find a `NotResident` among the causes of `e`.
    pub fn find(e: &anyhow::Error) -> Option<&NotResident> {
        e.chain().find_map(|c| {
            c.downcast_ref::<NotResident>().or_else(|| {
                c.downcast_ref::<std::io::Error>()?
                    .get_ref()?
                    .downcast_ref::<NotResident>()
            })
        })
    }
}

impl std::fmt::Debug for NotResident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bytes {:?} not resident", self.range)
    }
}

impl std::fmt::Display for NotResident {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for NotResident {}

/// Fetch the bytes of `need` to local disk, telling the disk cache how they
/// are used, and keep them there until the returned pins are dropped (for
/// readers that load first and read later, synchronously). Loads of the
/// same chunks share one fetch.
pub(crate) async fn load_pinned(need: Vec<NotResident>, access: Access) -> Result<Vec<Pinned>> {
    let all = futures::future::try_join_all(NotResident::coalesce(need).into_iter().map(
        |n| async move {
            // Pinned first: the loaded bytes stay until the reader is done.
            let pin = n.source.pin(n.range.clone());
            n.load(access).await?;
            Ok::<_, anyhow::Error>(pin)
        },
    ));
    all.await
}

/// Chunks kept on local disk while this is alive: the disk cache does not
/// evict them.
#[must_use]
pub(crate) struct Pinned(Option<(Arc<TieredSource>, Range<u64>)>);

impl Pinned {
    /// Nothing pinned (a source that is entirely local).
    pub(crate) fn none() -> Pinned {
        Pinned(None)
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        if let Some((src, chunks)) = self.0.take() {
            src.unpin(chunks);
        }
    }
}

/// A node-wide budget for the bytes of sparse local files. Over budget,
/// chunks are evicted by CLOCK: a chunk read for a point access (or read
/// again from disk) is marked referenced and survives one sweep; chunks
/// loaded by scans (full clones, merges) are not, so a scan cycles through
/// without displacing the working set. Pinned chunks are never evicted.
/// Eviction punches a hole in the file, so the space is freed.
pub(super) struct DiskCache {
    budget: u64,
    used: AtomicU64,
    evictions: AtomicU64,
    clock: Mutex<Clock>,
}

impl DiskCache {
    pub(super) fn new(budget: u64) -> Arc<DiskCache> {
        Arc::new(DiskCache {
            budget,
            used: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            clock: Mutex::default(),
        })
    }

    pub(super) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "budget": self.budget,
            "used": self.used.load(Ordering::Relaxed),
            "evictions": self.evictions.load(Ordering::Relaxed),
        })
    }

    /// Account for chunks just written to `src`, evicting others if over
    /// budget.
    fn admit(&self, src: &Arc<TieredSource>, chunks: Range<u64>) {
        let bytes: u64 = chunks.clone().map(|c| src.chunk_len(c)).sum();
        let victims = {
            let mut clock = self.clock.lock();
            for c in chunks {
                clock.push(Arc::downgrade(src), c);
            }
            if self.used.fetch_add(bytes, Ordering::Relaxed) + bytes <= self.budget {
                return;
            }
            self.select(&mut clock)
        };
        if !victims.is_empty() {
            // Punch outside the lock; each victim stays claimed (refetches
            // wait) until its hole is punched.
            tokio::task::spawn_blocking(move || punch_holes(victims));
        }
    }

    /// Run the hand until usage is within budget (or everything left is
    /// pinned, referenced twice over, or being loaded). Returns the claimed
    /// victims, already unmarked.
    fn select(&self, clock: &mut Clock) -> Vec<Claim> {
        let mut victims = Vec::new();
        let over = || self.used.load(Ordering::Relaxed) > self.budget;
        clock.run(over, |src, c| {
            if !src.has(c) {
                return true;
            }
            let Some(claim) = src.try_claim(c) else {
                return false;
            };
            src.drop_chunk(c);
            self.evictions.fetch_add(1, Ordering::Relaxed);
            victims.push(claim);
            true
        });
        victims
    }
}

/// The disk cache's CLOCK: a ring of chunks and a hand.
#[derive(Default)]
struct Clock {
    ring: Vec<(Weak<TieredSource>, u64)>,
    hand: usize,
}

impl Clock {
    fn push(&mut self, src: Weak<TieredSource>, c: u64) {
        self.ring.push((src, c));
    }

    /// Run the hand while `over()` is true, at most twice round the ring.
    /// Chunks not used since the hand last passed go to `evict`, which
    /// returns true when the chunk has left, or was gone already (the entry
    /// then goes too).
    fn run(
        &mut self,
        mut over: impl FnMut() -> bool,
        mut evict: impl FnMut(&Arc<TieredSource>, u64) -> bool,
    ) {
        let mut steps = 2 * self.ring.len();
        while over() && steps > 0 && !self.ring.is_empty() {
            steps -= 1;
            if self.hand >= self.ring.len() {
                self.hand = 0;
            }
            #[expect(clippy::indexing_slicing, reason = "the hand was reset into the ring")]
            let (src, c) = &self.ring[self.hand];
            let c = *c;
            let Some(src) = src.upgrade() else {
                self.ring.swap_remove(self.hand);
                continue;
            };
            if src.referenced.clear(c, Ordering::Relaxed) || !evict(&src, c) {
                self.hand += 1;
            } else {
                self.ring.swap_remove(self.hand);
            }
        }
    }
}

/// Free the disk space of the evicted `victims`.
fn punch_holes(victims: Vec<Claim>) {
    for v in victims {
        let src = &v.source;
        let start = v.chunks.start * src.chunk;
        let end = (v.chunks.end * src.chunk).min(src.len);
        // Filesystems free whole blocks only.
        let (s, e) = (start.next_multiple_of(4096), end / 4096 * 4096);
        if s < e
            && let Err(err) = src
                .file
                .get()
                .and_then(|f| crate::sys::punch_hole(&f, s, e - s))
        {
            tracing::debug!("punching a hole in a cached file: {err}");
        }
    }
}

/// The SHA-256 of every [`chunk_bytes`] bytes of a file up to `covered`
/// (the last one may be shorter), from the file itself (see the layer
/// format).
pub(crate) struct ChunkHashes {
    pub covered: u64,
    pub hashes: Arc<Vec<[u8; 32]>>,
}

impl ChunkHashes {
    /// Whether `bytes`, the start of chunk `c`, match its hash (only the
    /// covered part is hashed).
    pub(super) fn check(&self, c: u64, bytes: &[u8]) -> bool {
        let start = c * chunk_bytes();
        if start >= self.covered {
            return true;
        }
        let n = to_usize((self.covered - start).min(chunk_bytes())).min(bytes.len());
        let Some(want) = self.hashes.get(to_usize(c)) else {
            return false;
        };
        sha2::Sha256::digest(bytes.split_at(n).0).as_slice() == want
    }
}

/// A local copy as it is when opened.
#[derive(Default, Clone, Copy)]
pub(super) struct LocalCopy {
    pub len: u64,
    /// Every chunk is present (else none: a new copy, created empty).
    pub complete: bool,
    /// A hard link to the file in the object store: it has every chunk.
    /// It shares that file's inode, so it is opened read-only and kept out
    /// of the disk budget: an eviction would punch the store's file, and a
    /// repair would write into it.
    pub linked: bool,
    /// Written and checked by this process. The chunks of a linked copy
    /// are checked against their hashes before first use.
    pub trusted: bool,
}

/// One atomic bit per chunk.
struct AtomicBits(Box<[AtomicU64]>);

impl AtomicBits {
    fn new(n: u64) -> AtomicBits {
        AtomicBits((0..n.div_ceil(64)).map(|_| AtomicU64::new(0)).collect())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "chunk indexes are below the chunk count the bitmap was sized for; a larger one is a bug"
    )]
    fn word(&self, i: u64) -> &AtomicU64 {
        &self.0[to_usize(i / 64)]
    }

    fn get(&self, i: u64, order: Ordering) -> bool {
        self.word(i).load(order) & (1 << (i % 64)) != 0
    }

    fn set(&self, i: u64, order: Ordering) {
        self.word(i).fetch_or(1 << (i % 64), order);
    }

    /// Clear bit `i`, and return whether it was set.
    fn clear(&self, i: u64, order: Ordering) -> bool {
        self.word(i).fetch_and(!(1 << (i % 64)), order) & (1 << (i % 64)) != 0
    }
}

/// A file served from a local copy that remote loads fill in.
pub(super) struct TieredSource {
    me: Weak<TieredSource>,
    len: u64,
    chunk: u64,
    path: PathBuf,
    /// The local file, opened on demand (see [`Handles`]).
    file: Handle,
    /// One bit per chunk: present in `file`.
    present: AtomicBits,
    remote: Arc<dyn RangeFetch>,
    /// Chunks being fetched (or evicted); waiters see the sender dropped
    /// when done.
    loading: Mutex<HashMap<u64, watch::Receiver<()>>>,
    /// The node's disk budget, if any.
    cache: Option<Arc<DiskCache>>,
    /// One bit per chunk: used since the eviction hand last passed.
    referenced: AtomicBits,
    /// Pin counts by chunk.
    pins: Mutex<HashMap<u64, u32>>,
    /// Bumped by every eviction from this file.
    epoch: AtomicU64,
    /// Chunk hashes, once known (they are in the file: a layer's index
    /// region).
    hashes: std::sync::OnceLock<ChunkHashes>,
    /// One bit per chunk: present and checked against its hash.
    verified: AtomicBits,
}

impl TieredSource {
    /// A source for the file held by `remote`, backed by the local `copy`
    /// at `path` (created if it has no chunks), charging its chunks to
    /// `cache` (None: outside the disk budget, never evicted).
    pub(super) fn open(
        handles: &Arc<Handles>,
        remote: Arc<dyn RangeFetch>,
        path: PathBuf,
        cache: Option<Arc<DiskCache>>,
        copy: LocalCopy,
    ) -> Result<Arc<TieredSource>> {
        let len = copy.len;
        let file = if copy.complete {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(!copy.linked)
                .open(&path)?;
            ensure!(
                f.metadata()?.len() == len,
                "{} has the wrong length",
                path.display()
            );
            f
        } else {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            f.set_len(len)?;
            f
        };
        let file = handles.register(&path, copy.linked, file)?;
        let cache = cache.filter(|_| !copy.linked);
        let chunk = chunk_bytes();
        let n = len.div_ceil(chunk);
        let src = Arc::new_cyclic(|me| TieredSource {
            me: me.clone(),
            len,
            chunk,
            path,
            file,
            present: AtomicBits::new(n),
            remote,
            loading: Mutex::new(HashMap::new()),
            cache,
            referenced: AtomicBits::new(n),
            pins: Mutex::new(HashMap::new()),
            epoch: AtomicU64::new(0),
            hashes: std::sync::OnceLock::new(),
            verified: AtomicBits::new(n),
        });
        if copy.complete {
            src.mark(0..n);
            if let Some(cache) = &src.cache {
                cache.admit(&src, 0..n);
            }
        }
        if copy.complete && copy.trusted {
            for c in 0..n {
                src.verified.set(c, Ordering::Release);
            }
        }
        Ok(src)
    }

    /// The local file's path.
    #[cfg(test)]
    pub(super) fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Check present chunk `c` against its hash; a chunk that fails is
    /// dropped (loaded again when next needed).
    fn verify_present(&self, c: u64) -> std::io::Result<bool> {
        let Some(h) = self.hashes.get() else {
            return Ok(true);
        };
        if self.verified.get(c, Ordering::Acquire) {
            return Ok(true);
        }
        let mut buf = vec![0u8; to_usize(self.chunk_len(c))];
        self.file.get()?.read_exact_at(&mut buf, c * self.chunk)?;
        if h.check(c, &buf) {
            self.verified.set(c, Ordering::Release);
            return Ok(true);
        }
        tracing::warn!(path = %self.path.display(), chunk = c, "local chunk does not match its hash; dropping it");
        self.drop_chunk(c);
        Ok(false)
    }

    /// Forget chunk `c` (evicted or damaged). Readers see its bit cleared,
    /// or the epoch moved if they read across a punch (see `read_at`).
    fn drop_chunk(&self, c: u64) {
        self.present.clear(c, Ordering::SeqCst);
        self.verified.clear(c, Ordering::SeqCst);
        self.epoch.fetch_add(1, Ordering::SeqCst);
        if let Some(cache) = &self.cache {
            cache.used.fetch_sub(self.chunk_len(c), Ordering::Relaxed);
        }
    }

    /// Learn the file's chunk hashes. Chunks present but not checked yet
    /// (loaded before the hashes could be read, or linked) are checked
    /// before they are first read.
    pub(super) fn set_chunk_hashes(&self, hashes: ChunkHashes) {
        // Already known: the first set wins.
        drop(self.hashes.set(hashes));
    }

    fn chunk_len(&self, c: u64) -> u64 {
        (self.len - c * self.chunk).min(self.chunk)
    }

    /// Unpin `chunks`, evictable again once no pin holds them.
    fn unpin(&self, chunks: Range<u64>) {
        let mut pins = self.pins.lock();
        for c in chunks {
            let std::collections::hash_map::Entry::Occupied(mut e) = pins.entry(c) else {
                continue;
            };
            *e.get_mut() -= 1;
            if *e.get() == 0 {
                e.remove();
            }
        }
    }

    fn set_referenced(&self, chunks: Range<u64>) {
        if self.cache.is_some() {
            for c in chunks {
                self.referenced.set(c, Ordering::Relaxed);
            }
        }
    }

    /// Claim chunk `c` (for eviction) unless it is pinned or being loaded.
    fn try_claim(self: &Arc<Self>, c: u64) -> Option<Claim> {
        if self.pins.lock().contains_key(&c) {
            return None;
        }
        let mut loading = self.loading.lock();
        if loading.contains_key(&c) {
            return None;
        }
        let (tx, rx) = watch::channel(());
        loading.insert(c, rx);
        Some(Claim {
            source: self.clone(),
            chunks: c..c + 1,
            access: Access::Scan,
            _done: tx,
        })
    }

    /// Keep the chunks of `range` on local disk until the pin is dropped.
    pub(super) fn pin(&self, range: Range<u64>) -> Pinned {
        let Some(me) = self.me.upgrade() else {
            return Pinned::none();
        };
        let chunks = self.chunks(&range);
        let mut pins = self.pins.lock();
        for c in chunks.clone() {
            *pins.entry(c).or_default() += 1;
        }
        Pinned(Some((me, chunks)))
    }

    fn has(&self, c: u64) -> bool {
        self.present.get(c, Ordering::Acquire)
    }

    fn mark(&self, chunks: Range<u64>) {
        for c in chunks {
            self.present.set(c, Ordering::Release);
        }
    }

    fn chunks(&self, range: &Range<u64>) -> Range<u64> {
        range.start / self.chunk..range.end.min(self.len).div_ceil(self.chunk)
    }

    /// Whether all of `range` is on local disk.
    pub(super) fn resident(&self, range: Range<u64>) -> bool {
        self.chunks(&range).all(|c| self.has(c))
    }

    /// Whether every chunk is on local disk.
    pub(super) fn complete(&self) -> bool {
        self.resident(0..self.len)
    }

    /// Mark `range` referenced for the disk cache, if a point access read it.
    fn touch(&self, range: &Range<u64>, access: Access) {
        if access == Access::Hot {
            self.set_referenced(self.chunks(range));
        }
    }

    /// Claim the missing chunks of `range` nobody is fetching, as runs of
    /// adjacent chunks; note the ones in flight.
    fn claim_missing(
        &self,
        me: &Arc<TieredSource>,
        range: &Range<u64>,
        access: Access,
    ) -> (Vec<Claim>, Vec<watch::Receiver<()>>) {
        let (mut runs, mut waits) = (Vec::new(), Vec::new());
        let mut loading = self.loading.lock();
        let mut run: Option<(u64, u64, watch::Sender<()>)> = None;
        for c in self.chunks(range) {
            let claim = !self.has(c) && !loading.contains_key(&c);
            if let Some(rx) = loading.get(&c).filter(|_| !self.has(c)) {
                waits.push(rx.clone());
            }
            let extend = claim
                && run
                    .as_ref()
                    .is_some_and(|(s, e, _)| *e == c && (e - s) * self.chunk < MAX_REQUEST);
            if !extend && let Some((s, e, tx)) = run.take() {
                runs.push(Claim {
                    source: me.clone(),
                    chunks: s..e,
                    access,
                    _done: tx,
                });
            }
            if claim {
                let (s, tx) = match run.take() {
                    Some((s, _, tx)) => (s, tx),
                    None => (c, watch::channel(()).0),
                };
                loading.insert(c, tx.subscribe());
                run = Some((s, c + 1, tx));
            }
        }
        if let Some((s, e, tx)) = run {
            runs.push(Claim {
                source: me.clone(),
                chunks: s..e,
                access,
                _done: tx,
            });
        }
        (runs, waits)
    }

    /// Fetch every chunk of `range` that is not on local disk yet. Chunks
    /// another load is fetching are waited for, not fetched twice. For the
    /// disk cache, a point access marks them referenced, a scan does not.
    pub(super) async fn load(&self, range: Range<u64>, access: Access) -> Result<()> {
        let me = self.me.upgrade().context("file closed")?;
        loop {
            if self.resident(range.clone()) {
                self.touch(&range, access);
                return Ok(());
            }
            let (runs, waits) = self.claim_missing(&me, &range, access);
            let mut fetches = futures::stream::iter(runs)
                .map(Claim::run)
                .buffer_unordered(LOAD_CONCURRENCY);
            while let Some(r) = fetches.next().await {
                r?;
            }
            for mut w in waits {
                // Err: the fetcher finished (or failed; the next round claims
                // what is still missing).
                drop(w.changed().await);
            }
        }
    }
}

/// Closing a file keeps its local copy (a later open may reuse it, or it
/// is removed when the file is released); its chunks leave the disk
/// budget's accounting.
impl Drop for TieredSource {
    fn drop(&mut self) {
        if let Some(cache) = &self.cache {
            let n = (0..self.len.div_ceil(self.chunk))
                .filter(|&c| self.has(c))
                .map(|c| self.chunk_len(c))
                .sum();
            cache.used.fetch_sub(n, Ordering::Relaxed);
        }
    }
}

/// Chunks this load fetches; others wait until it is dropped.
struct Claim {
    source: Arc<TieredSource>,
    chunks: Range<u64>,
    access: Access,
    _done: watch::Sender<()>,
}

impl Claim {
    /// Fetch a claimed run of chunks into the local file.
    async fn run(self) -> Result<()> {
        let src = self.source.clone();
        let range = self.chunks.start * src.chunk..(self.chunks.end * src.chunk).min(src.len);
        let bytes = src.remote.get(range.clone()).await?;
        ensure!(
            to_u64(bytes.len()) == range.end - range.start,
            "short read: {} of {} bytes",
            bytes.len(),
            range.end - range.start
        );
        let checked = match src.hashes.get() {
            None => false,
            Some(h) => {
                // The length check above makes the parts line up with the chunks.
                for (c, part) in self.chunks.clone().zip(bytes.chunks(to_usize(src.chunk))) {
                    anyhow::ensure!(
                        h.check(c, part),
                        "chunk {c} of {} does not match its hash",
                        src.path.display()
                    );
                }
                true
            }
        };
        let (off, writer) = (range.start, src.clone());
        tokio::task::spawn_blocking(move || writer.file.get()?.write_all_at(&bytes, off)).await??;
        if self.access == Access::Hot {
            src.set_referenced(self.chunks.clone());
        }
        if checked {
            for c in self.chunks.clone() {
                src.verified.set(c, Ordering::Release);
            }
        }
        src.mark(self.chunks.clone());
        if let Some(cache) = &src.cache {
            cache.admit(&src, self.chunks.clone());
        }
        Ok(())
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        let mut loading = self.source.loading.lock();
        for c in self.chunks.clone() {
            loading.remove(&c);
        }
    }
}

impl TieredSource {
    /// Run `read` on the local file once every chunk of `range` is local
    /// and checked, again if an eviction raced it.
    fn read_local(
        &self,
        range: Range<u64>,
        mut read: impl FnMut(&std::fs::File) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        loop {
            // An eviction clears the chunk's bit and bumps the epoch before
            // punching its hole: a read that may have seen the hole sees the
            // epoch move, and tries again.
            let epoch = self.epoch.load(Ordering::SeqCst);
            // Chunks not checked yet (loaded before the hashes were known, or
            // linked) are checked on first use.
            let ok = self.chunks(&range).try_fold(true, |ok, c| {
                Ok::<_, std::io::Error>(ok && self.has(c) && self.verify_present(c)?)
            })?;
            if !ok {
                let source = self
                    .me
                    .upgrade()
                    .ok_or_else(|| std::io::Error::other("file closed"))?;
                return Err(std::io::Error::other(NotResident { source, range }));
            }
            read(&*self.file.get()?)?;
            if self.epoch.load(Ordering::SeqCst) == epoch {
                return Ok(());
            }
        }
    }
}

impl PageSource for TieredSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
        let range = off..off + to_u64(buf.len());
        self.read_local(range, |f| f.read_exact_at(buf, off))
    }

    fn read_pages_at(&self, off: u64, bufs: &mut [&mut [u8]]) -> std::io::Result<()> {
        let len: usize = bufs.iter().map(|b| b.len()).sum();
        let range = off..off + to_u64(len);
        self.read_local(range, |f| crate::sys::read_exact_vectored_at(f, bufs, off))
    }

    fn used(&self, range: Range<u64>) {
        self.set_referenced(self.chunks(&range));
    }

    fn pin(&self, range: Range<u64>) -> Pinned {
        TieredSource::pin(self, range)
    }

    fn set_chunk_hashes(&self, hashes: ChunkHashes) -> Result<()> {
        TieredSource::set_chunk_hashes(self, hashes);
        Ok(())
    }

    fn missing(&self, range: Range<u64>) -> Option<NotResident> {
        if self.resident(range.clone()) {
            return None;
        }
        Some(NotResident {
            source: self.me.upgrade()?,
            range,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_handles() -> Arc<Handles> {
        Handles::new(1 << 10, tempfile::tempdir().unwrap().keep()).unwrap()
    }

    fn us(n: u64) -> usize {
        usize::try_from(n).unwrap()
    }

    fn u(n: usize) -> u64 {
        u64::try_from(n).unwrap()
    }
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;

    struct Remote {
        data: Vec<u8>,
        gets: AtomicUsize,
        bytes: AtomicU64,
        /// Serve chunk `n` with a flipped byte.
        corrupt: Option<u64>,
    }

    #[async_trait::async_trait]
    impl RangeFetch for Remote {
        async fn get(&self, r: Range<u64>) -> Result<Bytes> {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            self.gets.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(r.end - r.start, Ordering::Relaxed);
            let mut b = self.data[us(r.start)..us(r.end)].to_vec();
            if let Some(c) = self
                .corrupt
                .filter(|c| (r.start..r.end).contains(&(c * CHUNK)))
            {
                b[us(c * CHUNK - r.start)] ^= 1;
            }
            Ok(Bytes::from(b))
        }
    }

    fn setup(n: usize) -> (tempfile::TempDir, Arc<Remote>, Arc<TieredSource>) {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..n)
            .map(|i| u8::try_from(i * 31 % 251).unwrap())
            .collect();
        let remote = Arc::new(Remote {
            data,
            gets: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            corrupt: None,
        });
        let src = TieredSource::open(
            &test_handles(),
            remote.clone(),
            dir.path().join("f.part"),
            None,
            LocalCopy {
                len: u(n),
                ..Default::default()
            },
        )
        .unwrap();
        (dir, remote, src)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_reads_never_fetch() {
        let (_d, remote, src) = setup(5 * us(CHUNK) + 100);
        let mut buf = [0u8; 10];
        let e = src.read_at(CHUNK + 5, &mut buf).unwrap_err();
        let e = anyhow::Error::from(e).context("reading a page");
        let nr = NotResident::find(&e).expect("not resident");
        assert_eq!(remote.gets.load(Ordering::Relaxed), 0);
        nr.load(Access::Hot).await.unwrap();
        src.read_at(CHUNK + 5, &mut buf).unwrap();
        assert_eq!(&buf[..], &remote.data[us(CHUNK) + 5..us(CHUNK) + 15]);
        // Only the chunk holding the range was fetched.
        assert_eq!(remote.bytes.load(Ordering::Relaxed), CHUNK);
        assert!(src.read_at(0, &mut buf).is_err());
    }

    async fn load_span(s: Arc<TieredSource>, i: u64) -> Result<()> {
        s.load(i * CHUNK..(30 + i) * CHUNK, Access::Hot).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_loads_fetch_each_chunk_once() {
        let (_d, remote, src) = setup(40 * us(CHUNK) + 7);
        let loads: Vec<_> = (0..8)
            .map(|i| tokio::spawn(load_span(src.clone(), i)))
            .collect();
        for l in loads {
            l.await.unwrap().unwrap();
        }
        assert!(src.resident(0..37 * CHUNK));
        assert_eq!(remote.bytes.load(Ordering::Relaxed), 37 * CHUNK);
        // Large runs are coalesced into requests of up to MAX_REQUEST.
        assert!(
            remote.gets.load(Ordering::Relaxed) < 37,
            "{} requests",
            remote.gets.load(Ordering::Relaxed)
        );
    }

    fn cached(
        n: usize,
        budget_chunks: u64,
    ) -> (
        tempfile::TempDir,
        Arc<Remote>,
        Arc<TieredSource>,
        Arc<DiskCache>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..n)
            .map(|i| u8::try_from(i * 31 % 251).unwrap())
            .collect();
        let remote = Arc::new(Remote {
            data,
            gets: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            corrupt: None,
        });
        let cache = DiskCache::new(budget_chunks * CHUNK);
        let src = TieredSource::open(
            &test_handles(),
            remote.clone(),
            dir.path().join("f.part"),
            Some(cache.clone()),
            LocalCopy {
                len: u(n),
                ..Default::default()
            },
        )
        .unwrap();
        (dir, remote, src, cache)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disk_budget_evicts_scans_before_hot_and_never_pinned() {
        let (_d, remote, src, cache) = cached(20 * us(CHUNK), 6);
        let pin = src.pin(0..CHUNK);
        src.load(0..CHUNK, Access::Hot).await.unwrap();
        src.load(CHUNK..2 * CHUNK, Access::Hot).await.unwrap();
        for c in 2..20 {
            src.load(c * CHUNK..(c + 1) * CHUNK, Access::Scan)
                .await
                .unwrap();
            assert!(
                cache.used.load(Ordering::Relaxed) <= 6 * CHUNK,
                "over budget after chunk {c}"
            );
        }
        assert!(cache.evictions.load(Ordering::Relaxed) >= 14);
        // Pinned: kept. Hot: survived the first sweeps. Early scans: gone.
        assert!(src.resident(0..CHUNK));
        assert!(!src.resident(2 * CHUNK..3 * CHUNK));
        let mut buf = [0u8; 4];
        let e = anyhow::Error::from(src.read_at(2 * CHUNK, &mut buf).unwrap_err());
        assert!(NotResident::find(&e).is_some());
        // Evicted bytes load again, correctly.
        NotResident::find(&e)
            .unwrap()
            .load(Access::Hot)
            .await
            .unwrap();
        src.read_at(2 * CHUNK, &mut buf).unwrap();
        assert_eq!(&buf, &remote.data[2 * us(CHUNK)..2 * us(CHUNK) + 4]);
        // Closing the file returns its bytes (once pending hole punches,
        // which hold it, are done).
        drop((pin, src, e));
        let mut tries = 0;
        while cache.used.load(Ordering::Relaxed) != 0 && tries < 100 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tries += 1;
        }
        assert_eq!(cache.used.load(Ordering::Relaxed), 0);
    }

    /// Read one spot in every chunk; returns how many reads succeeded.
    fn read_round(src: &TieredSource, data: &[u8], buf: &mut [u8]) -> u32 {
        let mut ok = 0;
        for c in 0..16u64 {
            let off = c * CHUNK + 100;
            if src.read_at(off, buf).is_ok() {
                assert_eq!(&buf[..], &data[us(off)..us(off) + buf.len()], "read a hole");
                ok += 1;
            }
        }
        ok
    }

    fn read_until(stop: &AtomicBool, src: &TieredSource, data: &[u8]) -> u32 {
        let mut buf = vec![0u8; 4096];
        let mut ok = 0;
        while !stop.load(Ordering::Relaxed) {
            ok += read_round(src, data, &mut buf);
        }
        ok
    }

    async fn load_all(src: &TieredSource, access: Access) {
        for c in 0..16 {
            src.load(c * CHUNK..(c + 1) * CHUNK, access).await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reads_racing_evictions_never_see_holes() {
        let (_d, remote, src, _cache) = cached(16 * us(CHUNK), 3);
        let stop = Arc::new(AtomicBool::new(false));
        let (s2, r2, st2) = (src.clone(), remote.clone(), stop.clone());
        let reader = std::thread::spawn(move || read_until(&st2, &s2, &r2.data));
        for access in [Access::Scan, Access::Hot].into_iter().cycle().take(20) {
            load_all(&src, access).await;
        }
        stop.store(true, Ordering::Relaxed);
        assert!(reader.join().unwrap() > 0);
    }

    fn hashes_of(data: &[u8], covered: u64) -> ChunkHashes {
        let hashes = data[..us(covered)]
            .chunks(us(CHUNK))
            .map(|c| sha2::Sha256::digest(c).into())
            .collect();
        ChunkHashes {
            covered,
            hashes: Arc::new(hashes),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn chunks_are_checked_against_their_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let n = 6 * us(CHUNK) + 100;
        let data: Vec<u8> = (0..n)
            .map(|i| u8::try_from(i * 31 % 251).unwrap())
            .collect();
        let remote = Arc::new(Remote {
            data: data.clone(),
            gets: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
            corrupt: Some(2),
        });
        let src = TieredSource::open(
            &test_handles(),
            remote,
            dir.path().join("f.part"),
            None,
            LocalCopy {
                len: u(n),
                ..Default::default()
            },
        )
        .unwrap();
        // Loaded before the hashes are known (like a layer's metadata):
        // checked once they are, and the corrupt chunk dropped.
        src.load(0..3 * CHUNK, Access::Hot).await.unwrap();
        src.set_chunk_hashes(hashes_of(&data, 5 * CHUNK + 50));
        let mut b = [0u8; 8];
        src.read_at(CHUNK, &mut b).unwrap();
        assert!(src.read_at(2 * CHUNK, &mut b).is_err());
        assert!(src.resident(0..2 * CHUNK) && !src.resident(2 * CHUNK..3 * CHUNK));
        // Loaded after: a corrupt chunk is refused.
        assert!(src.load(2 * CHUNK..3 * CHUNK, Access::Hot).await.is_err());
        src.load(3 * CHUNK..u(n), Access::Hot).await.unwrap();
        let mut buf = vec![0u8; 100];
        src.read_at(u(n) - 100, &mut buf).unwrap();
        assert_eq!(buf, data[n - 100..]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_all_completes_the_copy() {
        let (_d, remote, src) = setup(3 * us(CHUNK) + 1);
        src.load(CHUNK..CHUNK + 1, Access::Hot).await.unwrap();
        assert!(!src.complete());
        src.load(0..src.len, Access::Scan).await.unwrap();
        assert!(src.complete());
        let mut buf = vec![0u8; remote.data.len()];
        src.read_at(0, &mut buf).unwrap();
        assert_eq!(buf, remote.data);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn written_copies_are_complete_and_trusted() {
        let (dir, remote, src) = setup(2 * us(CHUNK));
        src.load(0..src.len, Access::Scan).await.unwrap();
        let path = src.path().to_path_buf();
        drop(src);
        let src = TieredSource::open(
            &test_handles(),
            remote.clone(),
            path,
            None,
            LocalCopy {
                len: 2 * CHUNK,
                complete: true,
                linked: false,
                trusted: true,
            },
        )
        .unwrap();
        assert!(src.complete());
        let gets = remote.gets.load(Ordering::Relaxed);
        let mut buf = vec![0u8; 2 * us(CHUNK)];
        src.read_at(0, &mut buf).unwrap();
        assert_eq!(buf, remote.data);
        assert_eq!(remote.gets.load(Ordering::Relaxed), gets, "nothing fetched");
        drop(dir);
    }

    /// Ranged reads of a local file, as a `file://` store serves them.
    struct FileFetch(PathBuf);

    #[async_trait::async_trait]
    impl RangeFetch for FileFetch {
        async fn get(&self, r: Range<u64>) -> Result<Bytes> {
            let mut b = vec![0u8; us(r.end - r.start)];
            std::fs::File::open(&self.0)?.read_exact_at(&mut b, r.start)?;
            Ok(b.into())
        }
    }

    /// A linked copy shares the stored file: it is never evicted, and a
    /// damaged chunk fails to load rather than being written into it.
    #[tokio::test(flavor = "multi_thread")]
    async fn linked_copies_are_never_written() {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..4 * CHUNK)
            .map(|i| u8::try_from(i * 31 % 251).unwrap())
            .collect();
        let path = dir.path().join("f.layer");
        std::fs::write(&path, &data).unwrap();
        // The stored file itself is damaged.
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all_at(&[0xff; 16], CHUNK + 10).unwrap();
        let damaged = std::fs::read(&path).unwrap();
        let cache = DiskCache::new(CHUNK);
        let src = TieredSource::open(
            &test_handles(),
            Arc::new(FileFetch(path.clone())),
            path.clone(),
            Some(cache.clone()),
            LocalCopy {
                len: 4 * CHUNK,
                complete: true,
                linked: true,
                trusted: false,
            },
        )
        .unwrap();
        assert!(src.complete());
        assert_eq!(cache.used.load(Ordering::Relaxed), 0);
        src.set_chunk_hashes(hashes_of(&data, 4 * CHUNK));
        let mut b = vec![0u8; 32];
        src.read_at(0, &mut b).unwrap();
        let e = anyhow::Error::from(src.read_at(CHUNK, &mut b).unwrap_err());
        let e = NotResident::find(&e)
            .unwrap()
            .load(Access::Hot)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("does not match its hash"), "{e:#}");
        assert!(
            src.file.get().unwrap().write_all_at(b"x", 0).is_err(),
            "opened for writing"
        );
        assert_eq!(std::fs::read(&path).unwrap(), damaged);
    }
}
