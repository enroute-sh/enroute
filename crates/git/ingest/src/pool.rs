//! The worker pool a push's CPU runs on.
//!
//! Resolution and delta encoding both spend nearly all of a push's CPU —
//! inflating, applying deltas, hashing, encoding, recompressing — so both run
//! here rather than on the task that owns the request. All I/O stays on the
//! async side: workers are handed everything they need and hand back
//! everything they produced, which is what lets them be plain CPU with no
//! runtime of their own.

use std::cell::RefCell;

use enroute_git_core::Error;
use enroute_git_store::ObjectDeflater;

/// The pool, or `None` if it could not be built.
///
/// Sized to the machine's full parallelism: ingest owns its process, and
/// the driver competing with it spends nearly all its time stalled waiting.
pub(crate) fn pool() -> Option<&'static rayon::ThreadPool> {
    static POOL: std::sync::OnceLock<Option<rayon::ThreadPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let threads = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        match rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("enroute-git-resolve-{i}"))
            .build()
        {
            Ok(pool) => Some(pool),
            Err(e) => {
                tracing::error!(error = %e, "could not start the resolve pool");
                None
            }
        }
    })
    .as_ref()
}

thread_local! {
    /// One deflate state per pool thread.
    ///
    /// Rebuilding it per object costs more than compressing does — see
    /// [`ObjectDeflater`].
    static DEFLATER: RefCell<ObjectDeflater> = RefCell::new(ObjectDeflater::new());
}

/// Compress `content` at the commit-pack's level, reusing this thread's
/// deflate state.
///
/// Only callable from a pool thread's job.
///
/// # Errors
/// Returns an error if compression fails.
pub(crate) fn deflate_into(content: &[u8], out: &mut Vec<u8>) -> Result<usize, Error> {
    DEFLATER
        .with(|d| d.borrow_mut().deflate_into(content, out))
        .map_err(Error::from)
}

/// Run `job` on the pool, without blocking the caller's thread.
///
/// # Errors
/// Returns an error if the pool is unavailable or the worker stopped before
/// answering.
pub(crate) async fn on_pool<T, F>(job: F) -> Result<T, Error>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    // Running inline instead would put a push's whole CPU cost on the runtime
    // thread the driver's own reads depend on — a path nothing exercises and
    // that stalls every other request on the machine. Failing is honest.
    let pool = pool().ok_or_else(|| anyhow::anyhow!("resolve pool unavailable"))?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    pool.spawn(move || {
        // A dropped receiver means the driver already gave up; nothing to do.
        drop(tx.send(job()));
    });
    rx.await
        .map_err(|_recv| anyhow::anyhow!("resolve worker stopped before answering"))
        .map_err(Error::from)
}
