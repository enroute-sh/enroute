//! The node's file I/O outside the log store.
//!
//! Files are immutable once written. New files are written once,
//! sequentially, through [`FileWriter`] (or [`AsyncFileWriter`]) under a
//! temporary name (`tempfile`'s, removed unless kept) and moved into place.
//! Reads go through the [buffer pool](super::pool).

use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use tempfile::{NamedTempFile, TempPath};
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Handle, RuntimeFlavor};
use tokio::sync::{OwnedSemaphorePermit as Permit, Semaphore};

use super::pool::{BufferPool, PagedFile, to_u64};

/// How long a wait for a scratch slot lasts. An operation may hold some
/// slots while it waits for more, so the wait must end.
const WAIT: Duration = Duration::from_secs(30);

/// Bounds the scratch files (and spool files) open at once, so many
/// concurrent operations cannot use all descriptors. Every counted file is
/// made through the budget and holds its slot until it is dropped.
#[derive(Clone, Debug)]
pub(super) struct ScratchBudget {
    slots: Arc<Semaphore>,
    capacity: usize,
    wait: Duration,
    /// Runs the timed wait for threads that have no runtime of their own
    /// (rayon). None outside a multi-thread runtime: a full budget then
    /// fails at once. A current-thread runtime's timer only runs while its
    /// own thread polls, so a wait from another thread could miss the limit.
    rt: Option<Handle>,
}

impl ScratchBudget {
    pub(super) fn new(capacity: usize) -> Self {
        ScratchBudget {
            slots: Arc::new(Semaphore::new(capacity)),
            capacity,
            wait: WAIT,
            rt: Handle::try_current()
                .ok()
                .filter(|h| h.runtime_flavor() == RuntimeFlavor::MultiThread),
        }
    }

    #[cfg(test)]
    fn with_wait(capacity: usize, wait: Duration) -> Self {
        ScratchBudget {
            wait,
            ..Self::new(capacity)
        }
    }

    /// A budget that never limits in practice.
    #[cfg(test)]
    pub(crate) fn generous() -> Self {
        Self::new(1 << 20)
    }

    fn timed_out(&self) -> anyhow::Error {
        anyhow::anyhow!(
            "too many scratch files open (budget {}): retry later",
            self.capacity
        )
    }

    fn try_acquire(&self) -> Option<Permit> {
        self.slots.clone().try_acquire_owned().ok()
    }

    async fn acquire(&self) -> Result<Permit> {
        match tokio::time::timeout(self.wait, self.slots.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) | Err(_) => Err(self.timed_out()),
        }
    }

    /// Like [`acquire`](Self::acquire) for threads that may block.
    fn acquire_blocking(&self) -> Result<Permit> {
        if let Some(permit) = self.try_acquire() {
            return Ok(permit);
        }
        let Some(rt) = &self.rt else {
            return Err(self.timed_out());
        };
        match Handle::try_current() {
            // Blocking the only thread would stall the runtime.
            Ok(h) if h.runtime_flavor() == RuntimeFlavor::CurrentThread => Err(self.timed_out()),
            // A worker hands its tasks to another thread first.
            Ok(_) => tokio::task::block_in_place(|| rt.block_on(self.acquire())),
            Err(_) => rt.block_on(self.acquire()),
        }
    }

    /// A new unnamed scratch file in `dir` (removed by the OS once closed).
    /// Blocks while the budget is full: call from a blocking thread.
    pub(super) fn create_blocking(&self, dir: &Path) -> Result<ScratchHandle> {
        let permit = self.acquire_blocking()?;
        Ok(ScratchHandle::new(dir, permit)?)
    }

    /// Like [`create_blocking`](Self::create_blocking) but never waits.
    pub(super) fn try_create(&self, dir: &Path) -> std::io::Result<ScratchHandle> {
        let permit = self
            .try_acquire()
            .ok_or_else(|| std::io::Error::other("too many scratch files open: retry later"))?;
        ScratchHandle::new(dir, permit)
    }
}

/// A temporary path and the budget slot of its open file.
#[derive(Debug)]
pub(super) struct ScratchPath {
    path: TempPath,
    permit: Permit,
}

impl ScratchPath {
    /// Move the file to `dst`. Returns the slot so the caller can keep it
    /// while its own descriptor stays open.
    fn persist(self, dst: &Path) -> Result<Permit> {
        self.path
            .persist(dst)
            .with_context(|| format!("move {} into place", dst.display()))?;
        Ok(self.permit)
    }
}

/// A new file being written sequentially under a temporary name in `dir`
/// (removed if dropped before it is moved into place).
pub(super) struct FileWriter {
    file: std::io::BufWriter<std::fs::File>,
    temp: ScratchPath,
}

impl FileWriter {
    /// Blocks while the budget is full: call from a blocking thread.
    pub(super) fn create(dir: &Path, budget: &ScratchBudget) -> Result<FileWriter> {
        let permit = budget.acquire_blocking()?;
        std::fs::create_dir_all(dir)?;
        let (file, path) = NamedTempFile::new_in(dir)
            .with_context(|| format!("create a file in {}", dir.display()))?
            .into_parts();
        Ok(FileWriter {
            file: std::io::BufWriter::with_capacity(1 << 20, file),
            temp: ScratchPath { path, permit },
        })
    }

    /// Flush and close; the file stays at its temporary path while the
    /// returned path lives.
    pub(super) fn finish(self) -> Result<ScratchPath> {
        self.file
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)?;
        Ok(self.temp)
    }
}

impl Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// [`FileWriter`] for async callers (request bodies).
pub(super) struct AsyncFileWriter {
    file: tokio::fs::File,
    temp: ScratchPath,
}

impl AsyncFileWriter {
    pub(super) async fn create(dir: &Path, budget: &ScratchBudget) -> Result<AsyncFileWriter> {
        let permit = budget.acquire().await?;
        tokio::fs::create_dir_all(dir).await?;
        let (file, path) = NamedTempFile::new_in(dir)
            .with_context(|| format!("create a file in {}", dir.display()))?
            .into_parts();
        Ok(AsyncFileWriter {
            file: tokio::fs::File::from_std(file),
            temp: ScratchPath { path, permit },
        })
    }

    pub(super) async fn write(&mut self, b: &[u8]) -> Result<()> {
        Ok(self.file.write_all(b).await?)
    }

    pub(super) async fn finish(mut self) -> Result<ScratchPath> {
        self.file.flush().await?;
        Ok(self.temp)
    }
}

/// Open an existing file for reads through `pool`.
pub(super) fn open(pool: &Arc<BufferPool>, path: &Path) -> Result<Arc<PagedFile>> {
    let f = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    Ok(pool.open(Arc::new(f)))
}

/// An unnamed scratch file that holds its budget slot until dropped.
pub(super) struct ScratchHandle {
    file: std::fs::File,
    _permit: Permit,
}

impl ScratchHandle {
    fn new(dir: &Path, permit: Permit) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(ScratchHandle {
            file: tempfile::tempfile_in(dir)?,
            _permit: permit,
        })
    }
}

impl Deref for ScratchHandle {
    type Target = std::fs::File;

    fn deref(&self) -> &std::fs::File {
        &self.file
    }
}

impl Write for ScratchHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl std::io::Read for ScratchHandle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        std::io::Read::read(&mut self.file, buf)
    }
}

impl std::io::Seek for ScratchHandle {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.file, pos)
    }
}

/// A temporary file read through the pool, removed when dropped unless it
/// was moved into place ([`ScratchFile::persist`]).
#[derive(Debug)]
pub(crate) struct ScratchFile {
    file: Arc<PagedFile>,
    /// None once persisted. Its slot is released with the file.
    temp: parking_lot::Mutex<Option<ScratchPath>>,
}

impl ScratchFile {
    pub(super) fn open(pool: &Arc<BufferPool>, temp: ScratchPath) -> Result<ScratchFile> {
        Ok(ScratchFile {
            file: open(pool, &temp.path)?,
            temp: parking_lot::Mutex::new(Some(temp)),
        })
    }

    pub(super) fn path(&self) -> Result<PathBuf> {
        self.temp
            .lock()
            .as_ref()
            .map(|t| t.path.to_path_buf())
            .context("scratch file already moved into place")
    }

    /// Move the file to `dst` (atomically replacing it).
    pub(crate) async fn persist(self, dst: &Path) -> Result<()> {
        let temp = self
            .temp
            .lock()
            .take()
            .context("scratch file already moved into place")?;
        let dst = dst.to_path_buf();
        // The descriptor stays open until `self` drops, so keep the slot.
        let _permit = tokio::task::spawn_blocking(move || temp.persist(&dst)).await??;
        Ok(())
    }
}

/// Bytes for one operation (a request body, a freshly built file): in
/// memory when small, else in a scratch file.
#[derive(Clone)]
pub(crate) enum Scratch {
    Mem(Bytes),
    File(Arc<ScratchFile>),
}

// Not derived: the bytes would be printed.
impl std::fmt::Debug for Scratch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scratch")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl Scratch {
    pub(crate) fn len(&self) -> u64 {
        match self {
            Scratch::Mem(b) => to_u64(b.len()),
            Scratch::File(f) => f.file.len(),
        }
    }

    /// All the bytes, in memory.
    pub(crate) async fn into_bytes(self) -> Result<Bytes> {
        match self {
            Scratch::Mem(b) => Ok(b),
            Scratch::File(f) => Ok(tokio::fs::read(f.path()?).await?.into()),
        }
    }

    /// Store the bytes at `dst` (atomically replacing it): a scratch file
    /// is moved there, bytes in memory are written under a temporary name
    /// in `tmp` first.
    pub(super) async fn persist(
        self,
        tmp: &Path,
        dst: &Path,
        budget: &ScratchBudget,
    ) -> Result<()> {
        match self {
            Scratch::Mem(b) => {
                let mut w = AsyncFileWriter::create(tmp, budget).await?;
                w.write(&b).await?;
                let done = w.finish().await?;
                let dst = dst.to_path_buf();
                tokio::task::spawn_blocking(move || done.persist(&dst).map(drop)).await?
            }
            Scratch::File(f) => {
                Arc::try_unwrap(f)
                    .ok()
                    .context("scratch file still in use")?
                    .persist(dst)
                    .await
            }
        }
    }
}

/// A request body is a byte source for the codecs.
impl crate::git::io::ReadAt for Scratch {
    fn len(&self) -> u64 {
        Scratch::len(self)
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Scratch::Mem(b) => crate::git::io::Mem(b).read_at(off, buf),
            // Read as a scan: cached while it helps (delta resolution reads
            // entries again), first to go under pressure.
            Scratch::File(f) => f
                .file
                .read_into(off, buf, super::pool::Access::Scan)
                .map_err(std::io::Error::other),
        }
    }

    fn as_slice(&self) -> Option<&[u8]> {
        match self {
            Scratch::Mem(b) => Some(b),
            Scratch::File(_) => None,
        }
    }
}

/// Receive `body`, gunzipping it if `gzip`, in memory up to `mem_limit`
/// bytes and spooled to a scratch file in `dir` beyond. Each chunk also
/// goes to `tap` as it arrives, until its receiver is gone.
///
/// Gunzipped as it arrives, so `mem_limit` bounds what the body inflates to.
pub(super) async fn spool<S, E>(
    pool: &Arc<BufferPool>,
    budget: &ScratchBudget,
    dir: &Path,
    body: S,
    gzip: bool,
    mem_limit: usize,
    tap: tokio::sync::mpsc::Sender<Bytes>,
) -> Result<Scratch>
where
    S: futures::Stream<Item = std::result::Result<Bytes, E>> + Unpin,
    E: Into<anyhow::Error>,
{
    use futures::TryStreamExt as _;
    use futures::future::Either;

    let body = body.map_err(|error| std::io::Error::other(error.into()));
    let mut body = if gzip {
        Either::Left(tokio_util::io::ReaderStream::with_capacity(
            async_compression::tokio::bufread::GzipDecoder::new(tokio_util::io::StreamReader::new(
                body,
            )),
            1 << 20,
        ))
    } else {
        Either::Right(body)
    };
    let mut tap = Some(tap);
    let mut mem = Vec::new();
    let mut file: Option<AsyncFileWriter> = None;
    while let Some(chunk) = body.try_next().await? {
        match &mut file {
            Some(f) => f.write(&chunk).await?,
            None if mem.len() + chunk.len() > mem_limit => {
                let mut f = AsyncFileWriter::create(dir, budget).await?;
                f.write(&mem).await?;
                f.write(&chunk).await?;
                mem = Vec::new();
                file = Some(f);
            }
            None => mem.extend_from_slice(&chunk),
        }
        if let Some(t) = &tap
            && t.send(chunk).await.is_err()
        {
            tap = None;
        }
    }
    Ok(match file {
        None => Scratch::Mem(mem.into()),
        Some(f) => Scratch::File(Arc::new(ScratchFile::open(pool, f.finish().await?)?)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn drain(mut rx: tokio::sync::mpsc::Receiver<Bytes>) -> Vec<u8> {
        let mut all = Vec::new();
        while rx.recv_many(&mut all, 4).await > 0 {}
        all.concat()
    }

    /// The memory limit applies to the inflated body, not to what was sent,
    /// and the tap sees the inflated body.
    #[tokio::test]
    async fn a_gzip_body_inflates_into_the_spool() {
        use crate::git::io::ReadAt as _;

        let dir = tempfile::tempdir().unwrap();
        let pool = BufferPool::new(1 << 20);
        for (size, in_memory) in [(1000, true), (3 << 20, false)] {
            let data: Vec<u8> = (0..size).map(|i| u8::try_from(i % 7).unwrap()).collect();
            let gz = Bytes::from(sirocco::compress(
                sirocco::Format::Gzip,
                sirocco::deflate::Level::FASTEST,
                &data,
            ));
            let body = futures::stream::iter(
                gz.chunks(4096)
                    .map(|chunk| Ok::<_, std::io::Error>(gz.slice_ref(chunk)))
                    .collect::<Vec<_>>(),
            );

            let budget = ScratchBudget::generous();
            let (tap, rx) = tokio::sync::mpsc::channel::<Bytes>(4);
            let tapped = tokio::spawn(drain(rx));
            let spooled = spool(&pool, &budget, dir.path(), body, true, 1 << 20, tap)
                .await
                .unwrap();
            assert!(tapped.await.unwrap() == data, "{size}");
            assert_eq!(matches!(spooled, Scratch::Mem(_)), in_memory, "{size}");
            let mut read = vec![0u8; data.len()];
            spooled.read_at(0, &mut read).unwrap();
            assert_eq!(read, data);
        }
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let pool = BufferPool::new(1 << 20);
        let data: Vec<u8> = (0..300_000u32)
            .map(|i| u8::try_from(i % 253).unwrap())
            .collect();
        let mut w = FileWriter::create(dir.path(), &ScratchBudget::generous()).unwrap();
        w.write_all(&data).unwrap();
        let tmp = w.finish().unwrap();
        let dst = dir.path().join("final");
        ScratchFile::open(&pool, tmp)
            .unwrap()
            .persist(&dst)
            .await
            .unwrap();
        let f = open(&pool, &dst).unwrap();
        let mut read = vec![0u8; data.len()];
        f.read_into(0, &mut read, super::super::pool::Access::Scan)
            .unwrap();
        assert_eq!(read, data);
        // Scratch files not moved into place are removed.
        let w = FileWriter::create(dir.path(), &ScratchBudget::generous()).unwrap();
        let path = w.finish().unwrap().path.to_path_buf();
        assert!(!path.exists());
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn a_full_budget_blocks_until_a_file_drops() {
        let (rt, dir) = (runtime(), tempfile::tempdir().unwrap());
        let _guard = rt.enter();
        let budget = ScratchBudget::with_wait(2, Duration::from_secs(10));
        let a = FileWriter::create(dir.path(), &budget).unwrap();
        let _b = FileWriter::create(dir.path(), &budget).unwrap();
        assert!(budget.try_acquire().is_none());
        let (b2, d2) = (budget.clone(), dir.path().to_path_buf());
        let waiter = std::thread::spawn(move || FileWriter::create(&d2, &b2).map(drop));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!waiter.is_finished());
        drop(a);
        waiter.join().unwrap().unwrap();
    }

    #[test]
    fn a_wait_past_the_limit_is_an_error() {
        let (rt, dir) = (runtime(), tempfile::tempdir().unwrap());
        let _guard = rt.enter();
        let budget = ScratchBudget::with_wait(1, Duration::from_millis(50));
        let _a = FileWriter::create(dir.path(), &budget).unwrap();
        let (b2, d2) = (budget.clone(), dir.path().to_path_buf());
        let err = std::thread::spawn(move || FileWriter::create(&d2, &b2).err())
            .join()
            .unwrap()
            .unwrap();
        assert!(
            err.to_string()
                .contains("too many scratch files open (budget 1)")
        );
    }

    #[test]
    fn a_blocking_pool_thread_waits_for_a_slot() {
        let (rt, dir) = (runtime(), tempfile::tempdir().unwrap());
        let budget = {
            let _guard = rt.enter();
            ScratchBudget::with_wait(1, Duration::from_secs(10))
        };
        let a = budget.create_blocking(dir.path()).unwrap();
        let (b2, d2) = (budget.clone(), dir.path().to_path_buf());
        let waiter = rt.spawn_blocking(move || b2.create_blocking(&d2).map(drop));
        std::thread::sleep(Duration::from_millis(100));
        drop(a);
        rt.block_on(waiter).unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_current_thread_worker_does_not_wait() {
        let dir = tempfile::tempdir().unwrap();
        let budget = ScratchBudget::with_wait(1, Duration::from_secs(10));
        let _a = budget.create_blocking(dir.path()).unwrap();
        assert!(budget.create_blocking(dir.path()).is_err());
    }

    #[tokio::test]
    async fn an_async_wait_ends_on_drop_or_on_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let budget = ScratchBudget::with_wait(1, Duration::from_millis(100));
        let a = AsyncFileWriter::create(dir.path(), &budget).await.unwrap();
        AsyncFileWriter::create(dir.path(), &budget)
            .await
            .err()
            .unwrap();
        let roomy = ScratchBudget::with_wait(1, Duration::from_secs(10));
        let a2 = AsyncFileWriter::create(dir.path(), &roomy).await.unwrap();
        let (r2, d2) = (roomy.clone(), dir.path().to_path_buf());
        let waiter = tokio::spawn(async move { AsyncFileWriter::create(&d2, &r2).await.map(drop) });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        drop(a2);
        waiter.await.unwrap().unwrap();
        drop(a);
    }

    #[test]
    fn a_finished_file_keeps_its_slot_until_the_scratch_file_drops() {
        let dir = tempfile::tempdir().unwrap();
        let pool = BufferPool::new(1 << 20);
        let budget = ScratchBudget::with_wait(1, Duration::from_millis(20));
        let w = FileWriter::create(dir.path(), &budget).unwrap();
        let file = ScratchFile::open(&pool, w.finish().unwrap()).unwrap();
        assert!(budget.try_acquire().is_none());
        drop(file);
        assert!(budget.try_acquire().is_some());
    }
}
