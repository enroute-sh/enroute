use std::sync::Arc;

pub(crate) mod coalesce;

pub(crate) use crate::git::cast::{ix, to_u64, to_usize};

/// The number at the start of `s` as C's `strtol` family reads it with base
/// 0, which is how git reads numbers off the wire.
///
/// That is leading whitespace, an optional sign, then hex after `0x`, octal
/// after `0`, else decimal. `None` when no digit is read; otherwise whether
/// it is negative, its magnitude (`None` past `u64::MAX`), and the rest of
/// `s`.
pub(crate) fn c_number(s: &str) -> Option<(bool, Option<u64>, &str)> {
    let s = s.trim_start_matches([' ', '\t', '\n', '\x0b', '\x0c', '\r']);
    let (negative, s) = match s.strip_prefix('-') {
        Some(s) => (true, s),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .filter(|h| h.starts_with(|c: char| c.is_ascii_hexdigit()));
    let (radix, digits) = match hex {
        Some(h) => (16, h),
        None if s.starts_with('0') => (8, s),
        None => (10, s),
    };
    let len = digits
        .find(|c: char| !c.is_digit(radix))
        .unwrap_or(digits.len());
    let (num, rest) = digits.split_at(len);
    if num.is_empty() {
        return None;
    }
    Some((negative, u64::from_str_radix(num, radix).ok(), rest))
}

/// Run `f` on the blocking pool.
pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> anyhow::Result<T> + Send + 'static,
) -> anyhow::Result<T> {
    tokio::task::spawn_blocking(f).await?
}

/// The items `rx` delivers, as a stream.
pub(crate) fn received<T: Send + 'static>(
    rx: tokio::sync::mpsc::Receiver<T>,
) -> impl futures::Stream<Item = T> + Send + 'static {
    futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|x| (x, rx)) })
}

/// A pack sink that sends each chunk, as `frame` makes it, into `tx`. The
/// pack writer runs on blocking threads, so the sink sends synchronously.
pub(crate) fn channel_sink<E: Send + 'static>(
    tx: tokio::sync::mpsc::Sender<Result<bytes::Bytes, E>>,
    mut frame: impl FnMut(&[u8]) -> bytes::Bytes + Send + 'static,
) -> crate::repo::serve::Sink {
    Box::new(move |chunk| {
        tx.blocking_send(Ok(frame(chunk)))
            .map_err(|_gone| anyhow::anyhow!("the pack's reader went away"))
    })
}

/// A cache of shared byte buffers (decoded objects, pack entries) bounded
/// by their total size: `quick_cache`'s sharded, size-weighted cache.
pub(crate) type ByteCache<K> = quick_cache::sync::Cache<K, Arc<Vec<u8>>, ByteWeight>;

/// Make a [`ByteCache`] of at most `bytes`.
pub(crate) fn byte_cache<K: std::hash::Hash + Eq>(bytes: usize) -> ByteCache<K> {
    // Sized for objects of a few KiB (trees, source files).
    let weight = u64::try_from(bytes).unwrap_or(u64::MAX);
    quick_cache::sync::Cache::with_weighter((bytes / 4096).max(64), weight, ByteWeight)
}

/// A cached buffer's weight: its bytes plus a fixed per-entry overhead
/// (allocation headers and rounding, the `Arc`, the cache's slot), which
/// dominates for the small objects typical of trees and source files.
#[derive(Clone, Debug)]
pub(crate) struct ByteWeight;

impl<K> quick_cache::Weighter<K, Arc<Vec<u8>>> for ByteWeight {
    fn weight(&self, _: &K, v: &Arc<Vec<u8>>) -> u64 {
        u64::try_from(v.len())
            .unwrap_or(u64::MAX)
            .saturating_add(128)
    }
}

/// A duration in whole microseconds, saturated at `u64::MAX`.
pub(crate) fn micros(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// A duration in whole milliseconds, saturated at `u64::MAX`.
pub(crate) fn millis(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// `t` in Unix milliseconds (0 before 1970).
pub(crate) fn unix_ms(t: std::time::SystemTime) -> u64 {
    t.duration_since(std::time::UNIX_EPOCH).map_or(0, millis)
}

/// Now, in Unix milliseconds.
pub(crate) fn now_ms() -> u64 {
    unix_ms(std::time::SystemTime::now())
}

/// Start the process-wide pool for CPU-bound work (encoding, compression,
/// replay): `threads` workers (0: one per core). Every parallel step runs
/// on it, so concurrent requests share the cores instead of each spawning
/// its own threads. Work on the pool must not block on I/O from other
/// requests or on clients. Later calls (e.g. several nodes in one test
/// process) keep the first pool.
pub fn init_cpu_pool(threads: usize) {
    // Fails when a pool already exists; the first pool stays.
    drop(
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("enroute-cpu-{i}"))
            .build_global(),
    );
}

/// Run `work(0..blocks)` on the CPU pool; results in block order.
pub(crate) fn par_blocks<T: Send>(blocks: usize, work: impl Fn(usize) -> T + Sync) -> Vec<T> {
    use rayon::prelude::*;
    if blocks <= 1 {
        return (0..blocks).map(work).collect();
    }
    (0..blocks).into_par_iter().map(&work).collect()
}
