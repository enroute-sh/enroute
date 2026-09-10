//! Phase timers and the narrowings the counters around them need.
//!
//! Field encoding lives in `enroute_git_cost`, beside the other numbers that
//! reach a span.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use enroute_git_cost::millis;

/// A duration accumulated by tasks that share no lifetime.
///
/// Nanoseconds in a relaxed atomic: independent tallies, read once at the
/// end, with no ordering between them worth paying for.
#[derive(Debug, Default)]
pub(crate) struct AtomicDuration(AtomicU64);

impl AtomicDuration {
    pub(crate) fn add(&self, elapsed: Duration) {
        self.0
            .fetch_add(as_u64(elapsed.as_nanos()), Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::Relaxed))
    }
}

/// Record each `(field, duration)` on the current span, in milliseconds.
///
/// `Span::record` on an undeclared name is a silent no-op, so every name here
/// must appear in the phase's `#[instrument]` attribute.
pub(crate) fn record_ms(fields: &[(&str, Duration)]) {
    let span = tracing::Span::current();
    for (name, elapsed) in fields {
        span.record(*name, millis(*elapsed));
    }
}

/// A wider integer as `u64`, saturating.
///
/// For the counts and progress denominators where an overflow is impossible
/// and a bespoke error message would say nothing.
pub(crate) fn as_u64<T: TryInto<u64>>(n: T) -> u64 {
    n.try_into().unwrap_or(u64::MAX)
}

/// [`as_u64`] for the other direction.
pub(crate) fn as_usize<T: TryInto<usize>>(n: T) -> usize {
    n.try_into().unwrap_or(usize::MAX)
}

/// Time `f`, adding what it took to `into`.
pub(crate) fn timed<T>(into: &mut Duration, f: impl FnOnce() -> T) -> T {
    let at = Instant::now();
    let out = f();
    *into += at.elapsed();
    out
}

/// [`timed`] for work that awaits.
pub(crate) async fn timed_async<T>(into: &mut Duration, f: impl Future<Output = T>) -> T {
    let at = Instant::now();
    let out = f.await;
    *into += at.elapsed();
    out
}
