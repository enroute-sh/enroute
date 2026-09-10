//! Bounded fan-out for the push path's per-commit and per-ref store work.

use std::future::Future;

/// How many store operations a push keeps in flight at once.
///
/// Each holds a staging file descriptor and a primary-store connection, so
/// unbounded fan-out exhausts descriptors before it saturates the network.
pub(crate) const MAX_CONCURRENT_STORE_OPS: usize = 64;

/// [`enroute_git_store::bounded`] at the push path's cap, so callers name the policy
/// rather than repeating the number.
pub(crate) async fn try_join_bounded<F, T, E>(
    tasks: impl IntoIterator<Item = F>,
) -> Result<Vec<T>, E>
where
    F: Future<Output = Result<T, E>>,
{
    enroute_git_store::bounded(MAX_CONCURRENT_STORE_OPS, tasks).await
}
