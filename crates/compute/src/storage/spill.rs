//! Temp tables: bounded-memory working storage for one operation.
//!
//! Indexing a pack needs a handful of per-object tables. Held in hash maps
//! they cost ~1.5 KB per object, which puts a Linux-sized import (~10M
//! objects) at ~15 GB. Here they are arrays of fixed-size records that stay
//! in memory while small and then move into the [buffer pool](super::pool)
//! as writable pages: the pool charges them to the same budget as layer
//! pages and, under pressure, writes dirty ones back to a scratch file
//! (created only then) and evicts them, like `DuckDB`'s buffer manager does
//! for intermediates. Lookups by object id use a sorted table instead of a
//! hash map.
//!
//! Small pushes never touch the pool or the filesystem.

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::git::oid::Oid;

use super::files::{ScratchBudget, ScratchHandle};
use super::pool::{BufferPool, Evict, PAGE_SIZE, PageOwner, Queue, Slot, fail, to_u64, to_usize};

/// A fixed-size record that can live in a page: any bit pattern is a valid
/// value and there is no padding. Implemented by deriving zerocopy's
/// `FromBytes`, `IntoBytes`, `Immutable` and `KnownLayout`, which check
/// this at compile time.
pub trait Record:
    FromBytes + IntoBytes + Immutable + KnownLayout + Copy + Send + Sync + 'static
{
}
impl<T: FromBytes + IntoBytes + Immutable + KnownLayout + Copy + Send + Sync + 'static> Record
    for T
{
}

/// Scratch space for one operation: temp tables and builder spill files.
/// Handed out by [`crate::storage::store::Storage::workspace`]; callers
/// never see where it lives.
#[derive(Clone, Debug)]
pub struct Workspace {
    dir: PathBuf,
    pool: Arc<BufferPool>,
    budget: ScratchBudget,
}

impl Workspace {
    pub(super) fn new(dir: &Path, pool: Arc<BufferPool>, budget: ScratchBudget) -> Workspace {
        Workspace {
            dir: dir.to_path_buf(),
            pool,
            budget,
        }
    }

    /// A workspace in `dir` with a pool of its own.
    #[cfg(test)]
    pub(crate) fn at(dir: &Path) -> Workspace {
        Workspace::new(dir, BufferPool::new(256 << 20), ScratchBudget::generous())
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    pub(super) fn budget(&self) -> &ScratchBudget {
        &self.budget
    }

    /// An empty temp table.
    pub fn table<T: Record>(&self) -> TempTable<T> {
        TempTable::new(self)
    }
}

/// Default bytes a table holds as a plain vector before moving into the
/// pool.
pub(super) const SPILL_BYTES: usize = 16 << 20;

/// Bytes sorted in memory per run when sorting a paged table.
const SORT_RUN_BYTES: usize = 64 << 20;

/// A growable array of `T`: a plain vector while small, then pages in the
/// buffer pool (see the module docs). Reads take `&self` and may run
/// concurrently; writes take `&mut self`.
///
/// I/O errors on a scratch file (only possible once pages were evicted)
/// panic: the operation fails, not the process.
pub struct TempTable<T: Record> {
    ws: Workspace,
    /// Everything while small; once paged, the records after the last full
    /// page.
    tail: Vec<T>,
    paged: Option<Arc<TempPages>>,
    /// The table's own list of its pages. Only the owner (with `&mut`)
    /// adds or drops pages, so readers index it without synchronization;
    /// the pool's evictor uses the locked list in `TempPages`.
    pages: Vec<Arc<TempPage>>,
    /// The page being written: checked out of the pool (marked busy) so a
    /// run of `set`s on it costs no republishing; put back when another
    /// page is written or before anything that needs the pool's view.
    staged: Option<(usize, Box<[u8]>)>,
    len: usize,
    limit: usize,
}

impl<T: Record> std::fmt::Debug for TempTable<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempTable")
            .field("len", &self.len)
            .field("limit", &self.limit)
            .field("paged", &self.paged.is_some())
            .finish_non_exhaustive()
    }
}

impl<T: Record> Drop for TempTable<T> {
    fn drop(&mut self) {
        self.unstage();
    }
}

/// The paged part of a table, registered with the pool.
struct TempPages {
    pool: Arc<BufferPool>,
    dir: PathBuf,
    budget: ScratchBudget,
    /// Bytes per page (whole records only).
    page_bytes: usize,
    pages: parking_lot::RwLock<Vec<Arc<TempPage>>>,
    /// Scratch file for evicted pages, created on first write-back.
    file: Mutex<Option<ScratchHandle>>,
    me: Weak<TempPages>,
}

struct TempPage {
    data: Slot,
    /// Modified since last written back.
    dirty: AtomicBool,
    /// Being modified by the table's writer: not evictable.
    busy: AtomicBool,
    /// Evicted (its contents are in the scratch file).
    out: AtomicBool,
    referenced: AtomicBool,
}

impl TempPages {
    fn offset(&self, p: usize) -> u64 {
        to_u64(p) * to_u64(self.page_bytes)
    }

    fn owner(&self) -> Weak<dyn PageOwner> {
        self.me.clone()
    }

    /// Bring page `p` back from the scratch file (it was evicted).
    fn fault(&self, p: usize, page: &TempPage) -> Arc<Box<[u8]>> {
        let mut buf = self.pool.reserve(self.page_bytes);
        {
            let f = self.file.lock();
            let Some(f) = f.as_ref() else {
                fail(format_args!("evicted page without scratch file"));
            };
            if let Err(e) = f.read_exact_at(&mut buf, self.offset(p)) {
                fail(format_args!("temp table read failed: {e}"));
            }
        }
        self.pool.stats.missed(1, self.page_bytes);
        let buf = match self.pool.install(&page.data, buf) {
            Ok(buf) => buf,
            Err(held) => return held,
        };
        page.out.store(false, Ordering::SeqCst);
        page.dirty.store(false, Ordering::SeqCst);
        self.pool.admit(self.owner(), to_u64(p), Queue::WriteBack);
        buf
    }

    /// Call `f` with page `p`'s bytes. The hit path borrows through an
    /// arc-swap guard and touches no shared reference count.
    fn read<R>(&self, p: usize, page: &TempPage, f: impl FnOnce(&[u8]) -> R) -> R {
        if !page.referenced.load(Ordering::Relaxed) {
            page.referenced.store(true, Ordering::Relaxed);
        }
        {
            let g = page.data.load();
            if let Some(b) = &*g {
                return f(b);
            }
        }
        let b = self.fault(p, page);
        f(&b)
    }

    /// Check page `p` out for writing: the evictor skips it until
    /// [`check_in`](Self::check_in). The caller has exclusive access to the
    /// table, so only the evictor may race with this.
    fn check_out(&self, p: usize, page: &TempPage) -> Box<[u8]> {
        page.busy.store(true, Ordering::SeqCst);
        let cur = if let Some(b) = page.data.swap(None) {
            b
        } else {
            // Evicted: fault it in, then take it.
            page.busy.store(false, Ordering::SeqCst);
            self.fault(p, page);
            page.busy.store(true, Ordering::SeqCst);
            page.data
                .swap(None)
                .unwrap_or_else(|| fail(format_args!("faulted page vanished")))
        };
        Arc::try_unwrap(cur).unwrap_or_else(|shared| {
            // The evictor holds it for a moment: copy.
            let mut b = self.pool.buffer(shared.len());
            b.copy_from_slice(&shared);
            b
        })
    }

    fn check_in(page: &TempPage, buf: Box<[u8]>) {
        page.data.store(Some(Arc::new(buf)));
        page.dirty.store(true, Ordering::SeqCst);
        page.referenced.store(true, Ordering::Relaxed);
        page.busy.store(false, Ordering::SeqCst);
    }

    /// Add a full page (dirty, in memory); returns it for the table's list.
    fn add(&self, bytes: &[u8]) -> Arc<TempPage> {
        let mut buf = self.pool.reserve(self.page_bytes);
        buf.copy_from_slice(bytes);
        let page = Arc::new(TempPage {
            data: Slot::new(Some(Arc::new(buf))),
            dirty: AtomicBool::new(true),
            busy: AtomicBool::new(false),
            out: AtomicBool::new(false),
            referenced: AtomicBool::new(true),
        });
        let p = {
            let mut pages = self.pages.write();
            pages.push(page.clone());
            pages.len() - 1
        };
        self.pool.admit(self.owner(), to_u64(p), Queue::WriteBack);
        page
    }

    /// Drop pages from `from` on.
    fn truncate_pages(&self, from: usize) {
        let mut pages = self.pages.write();
        if from >= pages.len() {
            return;
        }
        let (mut bytes, mut n) = (0, 0);
        for page in pages.iter().skip(from) {
            if page.data.swap(None).is_some() {
                bytes += self.page_bytes;
                n += 1;
            }
        }
        self.pool.released(bytes, n);
        pages.truncate(from);
    }

    fn write_back(&self, p: usize, data: &[u8]) -> std::io::Result<()> {
        let mut f = self.file.lock();
        let file = match &mut *f {
            Some(file) => file,
            // Eviction runs on whichever thread needs memory: never wait
            // here. The page stays in memory and a later pass retries.
            empty => empty.insert(self.budget.try_create(&self.dir)?),
        };
        file.write_all_at(data, self.offset(p))
    }
}

impl PageOwner for TempPages {
    fn clear_referenced(&self, page: u64) -> bool {
        let pages = self.pages.read();
        pages
            .get(to_usize(page))
            .is_some_and(|p| p.referenced.swap(false, Ordering::Relaxed))
    }

    fn resident(&self, page: u64) -> bool {
        self.pages
            .read()
            .get(to_usize(page))
            .is_some_and(|p| !p.out.load(Ordering::SeqCst))
    }

    fn try_evict(&self, page: u64) -> Evict {
        let Some(pg) = self.pages.read().get(to_usize(page)).cloned() else {
            return Evict::Gone;
        };
        if pg.out.load(Ordering::SeqCst) {
            return Evict::Gone;
        }
        if pg.busy.load(Ordering::SeqCst) {
            return Evict::Pinned;
        }
        let Some(buf) = pg.data.load_full() else {
            return Evict::Pinned;
        };
        if Arc::strong_count(&buf) > 2 {
            return Evict::Pinned;
        }
        if pg.dirty.swap(false, Ordering::SeqCst)
            && let Err(e) = self.write_back(to_usize(page), &buf)
        {
            tracing::warn!("temp table write-back failed: {e:#}");
            pg.dirty.store(true, Ordering::SeqCst);
            return Evict::Pinned;
        }
        // Lost a race with a writer: it keeps the page (and re-dirtied it).
        if !pg.data.take_if(&buf) {
            return Evict::Pinned;
        }
        pg.out.store(true, Ordering::SeqCst);
        let bytes = buf.len();
        Evict::Freed {
            bytes,
            buf: Arc::try_unwrap(buf).ok(),
        }
    }
}

impl Drop for TempPages {
    fn drop(&mut self) {
        let (mut bytes, mut n) = (0, 0);
        for page in self.pages.get_mut().iter() {
            if page.data.swap(None).is_some() {
                bytes += self.page_bytes;
                n += 1;
            }
        }
        self.pool.released(bytes, n);
    }
}

impl<T: Record> TempTable<T> {
    pub fn new(ws: &Workspace) -> Self {
        Self::with_limit(ws, SPILL_BYTES)
    }

    pub(crate) fn with_limit(ws: &Workspace, limit: usize) -> Self {
        TempTable {
            ws: ws.clone(),
            tail: Vec::new(),
            paged: None,
            pages: Vec::new(),
            staged: None,
            len: 0,
            limit,
        }
    }

    /// `n` copies of `v`.
    pub fn filled(ws: &Workspace, n: usize, v: T) -> Self {
        let mut s = Self::new(ws);
        for _ in 0..n {
            s.push(v);
        }
        s
    }

    const fn per_page() -> usize {
        PAGE_SIZE / size_of::<T>()
    }

    const fn page_bytes() -> usize {
        Self::per_page() * size_of::<T>()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether the table has moved into the pool.
    #[cfg(test)]
    pub(crate) fn spilled(&self) -> bool {
        self.paged.is_some()
    }

    fn paged_records(&self) -> usize {
        self.pages.len() * Self::per_page()
    }

    /// Put the staged page (if any) back into the pool.
    fn unstage(&mut self) {
        let Some((p, buf)) = self.staged.take() else {
            return;
        };
        let Some(page) = self.pages.get(p) else {
            fail(format_args!("staged page {p} is not in the table"));
        };
        TempPages::check_in(page, buf);
    }

    /// Call `f` with page `p`'s bytes (the staged copy if it is staged).
    /// `None` if the table has no such page.
    fn read_page<R>(&self, p: usize, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        if let Some((sp, buf)) = &self.staged
            && *sp == p
        {
            return Some(f(buf));
        }
        Some(self.paged.as_ref()?.read(p, self.pages.get(p)?, f))
    }

    /// The whole records of a page.
    fn records(page: &[u8]) -> Option<&[T]> {
        <[T]>::ref_from_bytes(page.get(..Self::page_bytes())?).ok()
    }

    /// Call `f` with records `range` of page `p`.
    fn with_page_records<R>(
        &self,
        p: usize,
        range: std::ops::Range<usize>,
        f: impl FnOnce(&[T]) -> R,
    ) -> R {
        self.read_page(p, |b| Some(f(Self::records(b)?.get(range)?)))
            .flatten()
            .unwrap_or_else(|| fail(format_args!("temp table page {p} is missing or short")))
    }

    /// Move into the pool: from here on, full pages live there.
    fn page_out(&mut self) {
        let pages = Arc::new_cyclic(|me| TempPages {
            pool: self.ws.pool.clone(),
            dir: self.ws.dir.clone(),
            budget: self.ws.budget.clone(),
            page_bytes: Self::page_bytes(),
            pages: parking_lot::RwLock::new(Vec::new()),
            file: Mutex::new(None),
            me: me.clone(),
        });
        self.paged = Some(pages);
        self.flush_tail();
    }

    /// Move full pages from the tail into the pool.
    fn flush_tail(&mut self) {
        let Some(p) = &self.paged else { return };
        let per = Self::per_page();
        if self.tail.len() < per {
            return;
        }
        let full = self.tail.len() / per * per;
        for chunk in self.tail.chunks(per).take(full / per) {
            self.pages.push(p.add(chunk.as_bytes()));
        }
        self.tail.drain(..full);
        // The tail never holds more than a page from here on; don't keep
        // the capacity it grew to before paging out.
        if self.tail.capacity() > 2 * per {
            self.tail.shrink_to(per);
        }
    }

    pub fn push(&mut self, v: T) {
        self.tail.push(v);
        self.len += 1;
        if self.paged.is_some() {
            if self.tail.len() == Self::per_page() {
                self.flush_tail();
            }
        } else if self.len * size_of::<T>() > self.limit {
            self.page_out();
        }
    }

    pub fn extend_from_slice(&mut self, vs: &[T]) {
        self.tail.extend_from_slice(vs);
        self.len += vs.len();
        if self.paged.is_some() {
            self.flush_tail();
        } else if self.len * size_of::<T>() > self.limit {
            self.page_out();
        }
    }

    pub fn get(&self, i: usize) -> T {
        assert!(
            i < self.len,
            "temp table index {i} out of range ({})",
            self.len
        );
        let base = self.paged_records();
        let v = if i >= base {
            self.tail.get(i - base).copied()
        } else {
            let per = Self::per_page();
            let off = (i % per) * size_of::<T>();
            self.read_page(i / per, |b| {
                T::read_from_bytes(b.get(off..off + size_of::<T>())?).ok()
            })
            .flatten()
        };
        v.unwrap_or_else(|| fail(format_args!("temp table index {i} is missing")))
    }

    pub fn set(&mut self, i: usize, v: T) {
        assert!(
            i < self.len,
            "temp table index {i} out of range ({})",
            self.len
        );
        let base = self.paged_records();
        let done = if i >= base {
            self.tail.get_mut(i - base).map(|slot| *slot = v)
        } else {
            self.set_paged(i, v)
        };
        if done.is_none() {
            fail(format_args!("temp table index {i} is missing"));
        }
    }

    /// Writes record `i`, which lies in a page, through the staged copy.
    fn set_paged(&mut self, i: usize, v: T) -> Option<()> {
        let per = Self::per_page();
        let off = (i % per) * size_of::<T>();
        let p = i / per;
        if self.staged.as_ref().is_none_or(|(sp, _)| *sp != p) {
            self.unstage();
            let buf = self.paged.as_ref()?.check_out(p, self.pages.get(p)?);
            self.staged = Some((p, buf));
        }
        let (_, buf) = self.staged.as_mut()?;
        buf.get_mut(off..off + size_of::<T>())?
            .copy_from_slice(v.as_bytes());
        Some(())
    }

    /// Shorten to `n` elements.
    pub fn truncate(&mut self, n: usize) {
        if n >= self.len {
            return;
        }
        self.unstage();
        let base = self.paged_records();
        if n >= base {
            self.tail.truncate(n - base);
        } else {
            let per = Self::per_page();
            let keep_pages = n / per;
            // The partial page becomes the new tail.
            let tail = self.range(keep_pages * per..n);
            if let Some(paged) = &self.paged {
                paged.truncate_pages(keep_pages);
            }
            self.pages.truncate(keep_pages);
            self.tail = tail;
        }
        self.len = n;
    }

    /// Records `range` (copied).
    pub fn range(&self, range: std::ops::Range<usize>) -> Vec<T> {
        let mut out = Vec::with_capacity(range.len());
        let mut i = range.start;
        while i < range.end {
            i += self.with_run(i, range.end - i, |run| out.extend_from_slice(run));
        }
        out
    }

    /// Call `f` with the records from `i` on that are contiguous in memory
    /// (up to `max` of them, and at most to the end of `i`'s page); returns
    /// how many were passed.
    fn with_run(&self, i: usize, max: usize, f: impl FnOnce(&[T])) -> usize {
        let base = self.paged_records();
        if i >= base {
            let Some(run) = self
                .tail
                .get(i - base..(i - base + max).min(self.tail.len()))
            else {
                fail(format_args!("temp table index {i} is missing"));
            };
            f(run);
            return run.len();
        }
        let per = Self::per_page();
        let (p, at) = (i / per, i % per);
        let n = max.min(per - at);
        self.with_page_records(p, at..at + n, f);
        n
    }

    /// Call `f` with records `range` as one slice: borrowed when they lie in
    /// one page (or the tail), else copied.
    pub(crate) fn with_slice<R>(
        &self,
        range: std::ops::Range<usize>,
        f: impl FnOnce(&[T]) -> R,
    ) -> R {
        let base = self.paged_records();
        if range.start >= base {
            let Some(run) = self.tail.get(range.start - base..range.end - base) else {
                fail(format_args!("temp table range {range:?} is missing"));
            };
            return f(run);
        }
        let per = Self::per_page();
        if range.is_empty() || (range.start / per == (range.end - 1) / per && range.end <= base) {
            if range.is_empty() {
                return f(&[]);
            }
            let (p, at) = (range.start / per, range.start % per);
            let n = range.len();
            return self.with_page_records(p, at..at + n, f);
        }
        f(&self.range(range))
    }

    /// All records, in order.
    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        let (mut i, mut at) = (0, 0);
        let mut buf: Vec<T> = Vec::new();
        std::iter::from_fn(move || {
            if !self.advance(&mut i, &mut at, &mut buf) {
                return None;
            }
            at += 1;
            Some(
                buf.get(at - 1)
                    .copied()
                    .unwrap_or_else(|| fail(format_args!("run buffer is empty"))),
            )
        })
    }

    /// Refills `buf` from record `i` when it is used up. Returns false at the
    /// end of the table.
    fn advance(&self, i: &mut usize, at: &mut usize, buf: &mut Vec<T>) -> bool {
        if *at < buf.len() {
            return true;
        }
        if *i >= self.len {
            return false;
        }
        buf.clear();
        *i += self.with_run(*i, self.len - *i, |run| buf.extend_from_slice(run));
        *at = 0;
        true
    }

    /// Consecutive chunks of up to `n` records (copied).
    pub fn chunks(&self, n: usize) -> impl Iterator<Item = Vec<T>> + '_ {
        let n = n.max(1);
        (0..self.len)
            .step_by(n)
            .map(move |s| self.range(s..(s + n).min(self.len)))
    }

    /// First index for which `pred` is false (`pred` must hold for a prefix
    /// of the table), like `slice::partition_point`.
    pub fn partition_point(&self, mut pred: impl FnMut(&T) -> bool) -> usize {
        self.partition_point_in(0..self.len, &mut pred)
    }

    fn partition_point_in(
        &self,
        range: std::ops::Range<usize>,
        pred: &mut impl FnMut(&T) -> bool,
    ) -> usize {
        let (mut lo, mut hi) = (range.start, range.end);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if pred(&self.get(mid)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Sort by `key`: in memory while the table is a plain vector or fits
    /// one run, otherwise as an external merge sort (sorted runs, then a
    /// k-way merge).
    pub fn sort_by_key<K: Ord>(&mut self, key: impl Fn(&T) -> K) {
        self.unstage();
        if self.paged.is_none() {
            self.tail.sort_unstable_by_key(|v| key(v));
            return;
        }
        let run = (SORT_RUN_BYTES / size_of::<T>()).max(1);
        let mut runs: Vec<TempTable<T>> = Vec::new();
        for start in (0..self.len).step_by(run) {
            let mut v = self.range(start..(start + run).min(self.len));
            v.sort_unstable_by_key(|v| key(v));
            let mut t = TempTable::with_limit(&self.ws, self.limit);
            t.extend_from_slice(&v);
            runs.push(t);
        }
        *self = match <[TempTable<T>; 1]>::try_from(runs) {
            Ok([only]) => only,
            Err(runs) => self.merge_runs(&runs, &key),
        };
    }

    /// K-way merge of sorted `runs` into a new table.
    fn merge_runs<K: Ord>(&self, runs: &[TempTable<T>], key: &impl Fn(&T) -> K) -> TempTable<T> {
        let mut out = TempTable::with_limit(&self.ws, self.limit);
        let mut iters: Vec<_> = runs.iter().map(|r| r.iter().peekable()).collect();
        let mut heap = std::collections::BinaryHeap::new();
        for (r, it) in iters.iter_mut().enumerate() {
            if let Some(v) = it.peek() {
                heap.push(std::cmp::Reverse((key(v), r)));
            }
        }
        while let Some(std::cmp::Reverse((_, r))) = heap.pop() {
            let Some(it) = iters.get_mut(r) else {
                fail(format_args!("run {r} is missing"));
            };
            let Some(v) = it.next() else {
                fail(format_args!("run {r} ended early"));
            };
            out.push(v);
            if let Some(v) = it.peek() {
                heap.push(std::cmp::Reverse((key(v), r)));
            }
        }
        out
    }
}

impl TempTable<u8> {
    /// A reader over the bytes (for variable-length records).
    pub fn reader(&self) -> TableReader<'_> {
        TableReader {
            table: self,
            pos: 0,
        }
    }
}

/// Sequential reads of a byte table.
#[derive(Debug)]
pub struct TableReader<'a> {
    table: &'a TempTable<u8>,
    pos: usize,
}

impl std::io::Read for TableReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.table.len || out.is_empty() {
            return Ok(0);
        }
        let n = self.table.with_run(self.pos, out.len(), |run| {
            // `with_run` passes at most `out.len()` records.
            let Some(dst) = out.get_mut(..run.len()) else {
                fail(format_args!("run is longer than the read buffer"));
            };
            dst.copy_from_slice(run);
        });
        self.pos += n;
        Ok(n)
    }
}

/// A set of small integers (pack entry indices), one bit each.
#[derive(Default, Clone)]
pub(crate) struct BitSet(Vec<u64>);

impl BitSet {
    pub(crate) fn new(n: usize) -> Self {
        BitSet(vec![0; n.div_ceil(64)])
    }

    pub(crate) fn contains(&self, i: usize) -> bool {
        self.0.get(i / 64).is_some_and(|w| w & (1 << (i % 64)) != 0)
    }

    /// Returns true if `i` was not yet present.
    pub(crate) fn insert(&mut self, i: usize) -> bool {
        let Some(w) = self.0.get_mut(i / 64) else {
            fail(format_args!("bit {i} is out of range"));
        };
        let bit = 1 << (i % 64);
        let new = *w & bit == 0;
        *w |= bit;
        new
    }

    pub(crate) fn clear(&mut self) {
        self.0.fill(0);
    }
}

#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct OidEntry {
    oid: Oid,
    idx: [u8; 4],
}

/// Object id -> index, as a sorted table with a fan-out on the leading
/// bits (like a pack .idx file), sized to the table. For duplicate ids the
/// smallest index wins.
pub(crate) struct OidIndex {
    entries: TempTable<OidEntry>,
    fanout: Vec<u32>,
    bits: u32,
}

impl OidIndex {
    pub(crate) fn build(ws: &Workspace, items: impl Iterator<Item = (Oid, u32)>) -> Self {
        let mut entries = TempTable::new(ws);
        for (oid, idx) in items {
            entries.push(OidEntry {
                oid,
                idx: idx.to_be_bytes(),
            });
        }
        // Big-endian index bytes sort numerically, so (oid, idx) order puts
        // the smallest index of a duplicate first.
        entries.sort_by_key(|a| (a.oid, a.idx));
        // About one entry per bucket, at most 16 bits.
        let bits = (usize::BITS - entries.len().leading_zeros()).clamp(1, 16);
        let mut fanout = vec![0u32; (1 << bits) + 1];
        for e in entries.iter() {
            let Some(n) = fanout.get_mut(prefix(&e.oid, bits) + 1) else {
                fail(format_args!("fan-out slot is missing"));
            };
            *n += 1;
        }
        let mut total = 0;
        for n in &mut fanout {
            total += *n;
            *n = total;
        }
        OidIndex {
            entries,
            fanout,
            bits,
        }
    }

    pub(crate) fn get(&self, oid: &Oid) -> Option<u32> {
        let p = prefix(oid, self.bits);
        // A bucket holds about one entry: one slice read, searched in place.
        let (Some(&lo), Some(&hi)) = (self.fanout.get(p), self.fanout.get(p + 1)) else {
            fail(format_args!("fan-out slot {p} is missing"));
        };
        let range = to_usize(lo.into())..to_usize(hi.into());
        self.entries.with_slice(range, |bucket| {
            let i = bucket.partition_point(|e| e.oid < *oid);
            bucket
                .get(i)
                .filter(|e| e.oid == *oid)
                .map(|e| u32::from_be_bytes(e.idx))
        })
    }
}

/// The leading `bits` bits of `oid`.
fn prefix(oid: &Oid, bits: u32) -> usize {
    let [a, b, c, d, ..] = *oid.prefix();
    to_usize((u32::from_be_bytes([a, b, c, d]) >> (32 - bits)).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(dir: &Path, pool_bytes: usize) -> Workspace {
        Workspace::new(dir, BufferPool::new(pool_bytes), ScratchBudget::generous())
    }

    #[test]
    fn small_stays_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let mut v: TempTable<u32> = TempTable::new(&ws(dir.path(), 1 << 20));
        v.extend_from_slice(&[1, 2, 3]);
        assert!(!v.spilled());
        assert_eq!(v.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "slow under Miri")]
    fn pages_out_writes_back_and_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        // A pool smaller than the table forces write-back and faults.
        let ws = ws(dir.path(), 4 * PAGE_SIZE);
        let mut v: TempTable<u64> = TempTable::with_limit(&ws, 64);
        let n = 200_000u64; // ~1.6 MB, 25 pages
        for i in 0..n {
            v.push(i * 3);
        }
        assert!(v.spilled());
        assert!(ws.pool.used() <= 5 * PAGE_SIZE, "used {}", ws.pool.used());
        assert!(ws.pool.stats.evictions.load(Ordering::Relaxed) > 0);
        assert_eq!(v.get(0), 0);
        assert_eq!(v.get(to_usize(n) - 1), (n - 1) * 3);
        for i in (0..to_usize(n)).step_by(997) {
            v.set(i, 7);
        }
        for (i, x) in v.iter().enumerate() {
            assert_eq!(x, if i % 997 == 0 { 7 } else { to_u64(i) * 3 }, "at {i}");
        }
        v.truncate(12_345);
        assert_eq!(v.len(), 12_345);
        assert_eq!(v.get(12_344), 12_344 * 3);
        v.push(1);
        assert_eq!(v.get(12_345), 1);
        drop(v);
        assert_eq!(ws.pool.used(), 0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "slow under Miri")]
    fn external_sort() {
        let dir = tempfile::tempdir().unwrap();
        let ws = ws(dir.path(), 8 * PAGE_SIZE);
        let mut v: TempTable<u64> = TempTable::with_limit(&ws, 64);
        let n = 3 * (SORT_RUN_BYTES / 8) + 1234; // four runs
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            v.push(x % 1_000_000);
        }
        v.sort_by_key(|a| *a);
        assert_eq!(v.len(), n);
        let mut prev = 0;
        for x in v.iter() {
            assert!(x >= prev);
            prev = x;
        }
    }

    #[test]
    fn oid_index_lookup() {
        let dir = tempfile::tempdir().unwrap();
        // Pseudo-random ids (xorshift), so the test also runs under Miri,
        // which cannot execute the SHA-1 intrinsics.
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let oids: Vec<Oid> = (0..5000)
            .map(|_| {
                let mut o = [0u8; 20];
                for chunk in o.chunks_mut(8) {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    chunk.copy_from_slice(&x.to_le_bytes()[..chunk.len()]);
                }
                Oid::sha1(o)
            })
            .collect();
        let mut items: Vec<(Oid, u32)> = oids
            .iter()
            .enumerate()
            .map(|(i, o)| (*o, u32::try_from(i).unwrap()))
            .collect();
        items.push((oids[42], 9999)); // duplicate: smallest index wins
        let idx = OidIndex::build(&Workspace::at(dir.path()), items.into_iter());
        for (i, o) in oids.iter().enumerate() {
            assert_eq!(idx.get(o), Some(u32::try_from(i).unwrap()));
        }
        assert_eq!(idx.get(&Oid::ZERO), None);
    }
}
