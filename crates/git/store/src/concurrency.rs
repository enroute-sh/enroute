//! Bounded fan-out over store operations, shared by the read and write paths.

use std::future::Future;

use tokio::sync::Semaphore;

/// [`futures::future::try_join_all`] with at most `cap` tasks in flight,
/// order-preserving so callers can zip results against inputs.
///
/// A permit gate, not `StreamExt::buffered`: buffered's higher-ranked
/// bounds defeat the `Send` check of the boxed future both call sites await.
///
/// # Errors
/// Returns the first error any task yields, as `try_join_all` does.
#[expect(
    clippy::disallowed_methods,
    reason = "the bounded wrapper the lint's replacement is built from"
)]
pub async fn bounded<F, T, E>(cap: usize, tasks: impl IntoIterator<Item = F>) -> Result<Vec<T>, E>
where
    F: Future<Output = Result<T, E>>,
{
    let permits = &Semaphore::new(cap);
    futures::future::try_join_all(tasks.into_iter().map(|task| async move {
        // Holding the `Result` holds the permit. The semaphore is local and
        // never closed, so the `Err` arm is unreachable, not a case to handle.
        let _permit = permits.acquire().await;
        task.await
    }))
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::bounded;

    #[tokio::test]
    async fn never_exceeds_the_cap_and_preserves_order() {
        let cap = 64;
        let in_flight = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let in_flight = &in_flight;
        let peak = &peak;

        let total = cap * 4;
        let results: Vec<usize> = bounded(
            cap,
            (0..total).map(|i| async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, std::io::Error>(i)
            }),
        )
        .await
        .unwrap();

        assert_eq!(results, (0..total).collect::<Vec<_>>());
        assert!(
            peak.load(Ordering::SeqCst) <= cap,
            "peak concurrency {} exceeded the cap",
            peak.load(Ordering::SeqCst)
        );
    }
}
