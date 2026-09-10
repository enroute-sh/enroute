//! Staging chunk storage for in-flight pushes, and cleanup of finished or
//! abandoned sessions.
//!
//! A session owns the prefix `{storage_key}/sessions/{session_id}` and
//! writes `/chunks/{chunk_id}` under it. All reads/writes go through
//! [`StagingSession`] rather than [`StagingStore`] directly — the store's own
//! `put`/`get`/`delete` are private, so nothing can touch a session's
//! storage without first proving it's registered. Dropping a
//! [`StagingSession`] deregisters it and spawns a delete of its chunks; a
//! process that dies before that runs leaves an orphan, which
//! [`StagingStore::sweep_orphaned_sessions`] cleans up once at startup (see
//! [`StagingStore::spawn_startup_sweep`]) by deleting anything not owned by a
//! currently-registered session, unparseable keys included. That sweep is
//! sound only against staging one process owns exclusively — a backend
//! shared with another Machine or workload would get swept too — which is
//! why the store lives on the worker that ingests, not on the repository's
//! `Store`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, path::Path};

use enroute_git_core::StorageKey;
use enroute_git_retrieve::RepoMetadata;

/// One ingesting process's staging store: where resolved loose objects live
/// for the duration of a push, distinct from `enroute_git_store::Store`.
///
/// Reachable only via [`crate::LocalIngestWorker`], since its cleanup is
/// sound only for a backend one process owns alone.
#[derive(Debug)]
pub(crate) struct StagingStore {
    inner: Arc<dyn ObjectStore>,
    /// Session ids registered via [`StagingStore::start_session`]; the sweep
    /// treats anything else as abandoned.
    active_sessions: Mutex<HashSet<u128>>,
}

impl StagingStore {
    /// Stage into `inner`, which this process must own exclusively — see the
    /// module docs.
    #[must_use]
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            active_sessions: Mutex::new(HashSet::new()),
        }
    }

    /// Delete every staging chunk written for `session_id`, by prefix rather
    /// than tracked chunk ids.
    ///
    /// Works even for a push that errors before it knows what it wrote. Safe
    /// to call more than once; logs errors per key rather than failing.
    async fn delete_session_prefix(&self, storage_key: StorageKey, session_id: u128) {
        let prefix = Path::from(format!("{storage_key}/sessions/{session_id:032x}"));
        let mut listing = self.inner.list(Some(&prefix));
        while let Some(entry) = listing.next().await {
            let location = match entry {
                Ok(meta) => meta.location,
                Err(e) => {
                    tracing::warn!(error = %e, %session_id, "failed to list staging chunks for cleanup");
                    continue;
                }
            };
            if let Err(e) = self.inner.delete(&location).await {
                tracing::warn!(error = %e, %location, "failed to delete staging chunk");
            }
        }
    }

    /// Begin a fresh staging session scoped to `repo`.
    ///
    /// Dropping the guard deletes its chunks. Takes `self` by `Arc` so it
    /// owns its handle rather than borrowing, which would be self-referential.
    #[must_use]
    pub(crate) fn start_session(self: Arc<Self>, repo: &RepoMetadata) -> StagingSession {
        self.register_session_id(new_session_id(), repo.storage_key)
    }

    /// Registers an already-chosen id rather than generating one, so tests
    /// can register a session under a specific id.
    fn register_session_id(
        self: Arc<Self>,
        session_id: u128,
        storage_key: StorageKey,
    ) -> StagingSession {
        self.active_sessions_lock().insert(session_id);
        StagingSession {
            store: self,
            session_id,
            storage_key,
        }
    }

    /// A `HashSet` mutation can't panic, so a poisoned lock can't have left
    /// the set corrupt — safe to recover rather than propagate.
    fn active_sessions_lock(&self) -> std::sync::MutexGuard<'_, HashSet<u128>> {
        self.active_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Delete every on-disk staging session that isn't currently registered.
    ///
    /// Returns the number swept. Non-fatal: logs errors per key rather than
    /// failing the caller.
    async fn sweep_orphaned_sessions(&self) -> usize {
        let mut swept = 0usize;
        for (session_id, locations) in self.list_sessions().await {
            // One rule, so nothing can slip between two: keep what a live
            // session owns, delete the rest. An unparseable key is by
            // definition owned by no live session.
            let live = session_id.is_some_and(|id| self.active_sessions_lock().contains(&id));
            if !live {
                self.delete_swept(session_id, locations).await;
                swept += 1;
            }
        }
        swept
    }

    /// Runs [`StagingStore::sweep_orphaned_sessions`] once, in the
    /// background — at startup only, see the module docs.
    pub(crate) fn spawn_startup_sweep(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let swept = self.sweep_orphaned_sessions().await;
            if swept > 0 {
                tracing::warn!(
                    swept,
                    "swept orphaned staging sessions left by a previous process"
                );
            }
        })
    }

    /// Everything in the staging store, grouped by the session that owns it,
    /// in one listing pass.
    ///
    /// A key that doesn't parse is grouped under `None` rather than dropped:
    /// dropping it let stale-layout keys accumulate forever, invisibly.
    async fn list_sessions(&self) -> HashMap<Option<u128>, Vec<Path>> {
        let mut sessions: HashMap<Option<u128>, Vec<Path>> = HashMap::new();
        let mut listing = self.inner.list(None);
        while let Some(entry) = listing.next().await {
            let location = match entry {
                Ok(meta) => meta.location,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to list staging store during sweep");
                    continue;
                }
            };
            sessions
                .entry(parse_session_id(&location))
                .or_default()
                .push(location);
        }
        sessions
    }

    async fn delete_swept(&self, session_id: Option<u128>, locations: Vec<Path>) {
        // The `None` wording is distinct on purpose: the store is supposed to
        // hold only what this code writes, so unrecognized keys mean either a
        // layout that changed under us or a store shared with something else.
        tracing::warn!(
            ?session_id,
            key_count = locations.len(),
            "{}",
            if session_id.is_some() {
                "sweeping orphaned staging session"
            } else {
                "sweeping staging keys belonging to no session"
            }
        );
        for location in locations {
            if let Err(e) = self.inner.delete(&location).await {
                tracing::warn!(error = %e, %location, "failed to delete staging key");
            }
        }
    }
}

/// Session ID: Unix seconds in the high 64 bits, random in the low 64 —
/// sortable by age, and collision-resistant across concurrent pushes.
fn new_session_id() -> u128 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let rand = rand::random::<u64>();
    (u128::from(secs) << 64) | u128::from(rand)
}

/// Ranged GET behind the chunk readers.
///
/// `Ok(None)` for an empty range: an HTTP `Range` header can't express one,
/// so a zero-length `GetRange::Bounded` risks being read as non-empty.
async fn get_range(
    store: &Arc<dyn ObjectStore>,
    key: &Path,
    range: std::ops::Range<usize>,
) -> Result<Option<object_store::GetResult>> {
    if range.is_empty() {
        return Ok(None);
    }
    let start = u64::try_from(range.start).context("range start")?;
    let end = u64::try_from(range.end).context("range end")?;
    let result = store
        .get_opts(
            key,
            GetOptions {
                range: Some(GetRange::Bounded(start..end)),
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("getting range of {key}"))?;
    Ok(Some(result))
}

/// Where one of a session's chunks lives.
fn chunk_key(repo: &RepoMetadata, session_id: u128, chunk_id: u32) -> Path {
    Path::from(format!(
        "{}/sessions/{session_id:032x}/chunks/{chunk_id:08}",
        repo.storage_key
    ))
}

/// Parses the session id out of any key a session writes, or `None`.
///
/// Indifferent to what follows the session id, so a session writing
/// something new needs no change here.
fn parse_session_id(path: &Path) -> Option<u128> {
    let key: &str = path.as_ref();
    let mut parts = key.splitn(4, '/');
    parts.next()?;
    if parts.next()? != "sessions" {
        return None;
    }
    u128::from_str_radix(parts.next()?, 16).ok()
}

/// RAII marker that `session_id` is an in-flight staging session — see
/// [`StagingStore::start_session`].
///
/// Dropping it deregisters the session and deletes its chunks; nothing
/// outside this module can hold one without a session registered.
#[derive(Debug)]
pub(crate) struct StagingSession {
    store: Arc<StagingStore>,
    session_id: u128,
    storage_key: StorageKey,
}

impl Drop for StagingSession {
    fn drop(&mut self) {
        self.store.active_sessions_lock().remove(&self.session_id);
        let store = self.store.clone();
        let storage_key = self.storage_key;
        let session_id = self.session_id;
        tokio::spawn(async move {
            store.delete_session_prefix(storage_key, session_id).await;
        });
    }
}

impl StagingSession {
    /// Write a chunk of concatenated loose objects to this session's staging
    /// area.
    ///
    /// # Errors
    /// Returns an error if the staging store is unavailable.
    pub(crate) async fn put_chunk(
        &self,
        repo: &RepoMetadata,
        chunk_id: u32,
        data: Bytes,
    ) -> Result<()> {
        self.store
            .inner
            .put(&chunk_key(repo, self.session_id, chunk_id), data.into())
            .await
            .map(|_| ())
            .context("putting staging chunk")
    }

    /// Fetch a byte range from one of this session's staging chunks.
    ///
    /// An empty range skips the store entirely — see [`get_range`].
    ///
    /// # Errors
    /// The staging store is unavailable, or the chunk does not exist.
    pub(crate) async fn get_chunk_range(
        &self,
        repo: &RepoMetadata,
        chunk_id: u32,
        range: std::ops::Range<usize>,
    ) -> Result<Bytes> {
        let mut chunks: Vec<Bytes> = self
            .stream_chunk_range(repo, chunk_id, range)
            .await?
            .map_err(|e| anyhow::anyhow!(e))
            .try_collect()
            .await
            .context("reading staging chunk range")?;
        // Small ranged GETs usually arrive as one chunk — skip the concat copy.
        if chunks.len() == 1 {
            return Ok(chunks.pop().unwrap_or_default());
        }
        Ok(chunks.concat().into())
    }

    /// Streaming half of the pair above, also used directly for bodies too
    /// large to buffer.
    ///
    /// An empty range yields an empty stream.
    ///
    /// # Errors
    /// The staging store is unavailable, or the chunk does not exist.
    pub(crate) async fn stream_chunk_range(
        &self,
        repo: &RepoMetadata,
        chunk_id: u32,
        range: std::ops::Range<usize>,
    ) -> Result<BoxStream<'static, Result<Bytes, std::io::Error>>> {
        let Some(result) = get_range(
            &self.store.inner,
            &chunk_key(repo, self.session_id, chunk_id),
            range,
        )
        .await?
        else {
            return Ok(futures::stream::empty().boxed());
        };
        Ok(result.into_stream().map_err(std::io::Error::other).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::staging_store as make_store;
    use enroute_git_core::{RepoId, StorageKey};

    /// A [`RepoMetadata`] fixture for tests that only need *some* repo
    /// identity; only `storage_key` is read here.
    fn test_repo() -> RepoMetadata {
        RepoMetadata {
            id: RepoId::new(1),
            storage_key: StorageKey::new_v4(),
            default_branch: "refs/heads/main".to_string(),
        }
    }

    /// Write a chunk under `session_id` with no session registered for it —
    /// what a process that exited without cleaning up leaves behind.
    ///
    /// Straight at the backend, since only a live session can write one.
    async fn put_orphan(
        store: &StagingStore,
        repo: &RepoMetadata,
        session_id: u128,
        chunk_id: u32,
        data: &'static [u8],
    ) {
        store
            .inner
            .put(
                &chunk_key(repo, session_id, chunk_id),
                Bytes::from_static(data).into(),
            )
            .await
            .unwrap();
    }

    async fn chunk_exists(
        store: &StagingStore,
        repo: &RepoMetadata,
        session_id: u128,
        chunk_id: u32,
    ) -> bool {
        store
            .inner
            .get(&chunk_key(repo, session_id, chunk_id))
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn delete_session_prefix_removes_only_that_sessions_chunks() {
        let store = make_store();
        let repo = test_repo();
        put_orphan(&store, &repo, 1, 0, b"chunk a").await;
        put_orphan(&store, &repo, 1, 1, b"chunk b").await;
        put_orphan(&store, &repo, 2, 0, b"other session").await;

        store.delete_session_prefix(repo.storage_key, 1).await;

        assert!(
            !chunk_exists(&store, &repo, 1, 0).await,
            "chunk 0 of the deleted session should be gone"
        );
        assert!(
            !chunk_exists(&store, &repo, 1, 1).await,
            "chunk 1 of the deleted session should be gone"
        );
        let survivor = store
            .inner
            .get(&chunk_key(&repo, 2, 0))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            survivor,
            Bytes::from_static(b"other session"),
            "a different session's chunk must survive"
        );
    }

    #[tokio::test]
    async fn delete_session_prefix_on_empty_session_is_noop() {
        let store = make_store();
        let repo = test_repo();
        // Nothing was ever staged under this session id — must not panic or error.
        store.delete_session_prefix(repo.storage_key, 99).await;
    }

    /// The sweep keys off "no live session owns this", not "parses as a
    /// dead session" — else a no-longer-written layout sits forever.
    #[tokio::test]
    async fn sweep_removes_keys_from_a_layout_we_no_longer_write() {
        let store = make_store();
        let repo = test_repo();
        let stale = Path::from(format!("{}/chunks/{:032x}/00000000", repo.storage_key, 7));
        store
            .inner
            .put(&stale, Bytes::from_static(b"old layout").into())
            .await
            .unwrap();

        store.sweep_orphaned_sessions().await;

        assert!(
            store.inner.get(&stale).await.is_err(),
            "a key no live session owns should be swept, parseable or not"
        );
    }

    #[tokio::test]
    async fn sweep_ignores_active_session() {
        let store = make_store();
        let repo = test_repo();
        let session_id = new_session_id();
        put_orphan(&store, &repo, session_id, 0, b"x").await;
        let guard = store
            .clone()
            .register_session_id(session_id, repo.storage_key);

        store.sweep_orphaned_sessions().await;

        assert!(
            chunk_exists(&store, &repo, session_id, 0).await,
            "an actively-registered session must survive the sweep"
        );
        drop(guard);
    }

    #[tokio::test]
    async fn sweep_deletes_unregistered_session() {
        let store = make_store();
        let repo = test_repo();
        let session_id = new_session_id();
        // No guard is ever registered for this id — as if left on disk by a
        // process that already exited.
        put_orphan(&store, &repo, session_id, 0, b"x").await;

        let swept = store.sweep_orphaned_sessions().await;

        assert_eq!(swept, 1);
        assert!(!chunk_exists(&store, &repo, session_id, 0).await);
    }

    /// `Drop` only spawns the delete, it doesn't await it — poll rather than
    /// assert immediately.
    async fn eventually_chunk_gone(
        store: &StagingStore,
        repo: &RepoMetadata,
        session_id: u128,
    ) -> bool {
        for _ in 0..1000 {
            if !chunk_exists(store, repo, session_id, 0).await {
                return true;
            }
            tokio::task::yield_now().await;
        }
        false
    }

    #[tokio::test]
    async fn dropping_the_guard_deletes_its_chunks() {
        let store = make_store();
        let repo = test_repo();
        let session_id = new_session_id();
        put_orphan(&store, &repo, session_id, 0, b"x").await;
        let guard = store
            .clone()
            .register_session_id(session_id, repo.storage_key);

        drop(guard);

        assert!(
            eventually_chunk_gone(&store, &repo, session_id).await,
            "staging chunk was not deleted after its guard dropped"
        );
    }
}
