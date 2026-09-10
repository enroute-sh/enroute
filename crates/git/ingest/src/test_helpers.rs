//! The staging fixtures and stage vocabulary this crate's tests share.
//!
//! Object content comes from `enroute_git_test_support`'s builders, so a
//! fixture here and one in another crate cannot disagree about an oid.

use std::sync::Arc;

use object_store::memory::InMemory;

use crate::staging::StagingStore;

/// A staging store of its own per test.
///
/// A sweep deletes everything no live session owns, so sharing one across
/// concurrent tests would have them delete each other's chunks.
pub(crate) fn staging_store() -> Arc<StagingStore> {
    staging_store_on(Arc::new(InMemory::new()))
}

/// Who a test pushes as, where a hook is shown an actor at all.
pub(crate) const TEST_USER: &str = "alice";

/// A staging store over `inner`, for tests that need to fault its reads.
pub(crate) fn staging_store_on(inner: Arc<dyn object_store::ObjectStore>) -> Arc<StagingStore> {
    Arc::new(StagingStore::new(inner))
}

/// A stage's short name, for tests asserting which stages a push reports.
///
/// One vocabulary, so two such tests can't disagree about what to call it.
pub(crate) fn stage_name(stage: &crate::progress::IngestProgress) -> &'static str {
    use crate::progress::IngestProgress as P;
    match stage {
        P::Dispatching => "dispatching",
        P::ResolvingObjects { .. } => "resolving",
        P::PreparingPacks { .. } => "preparing",
        P::CompressingObjects { .. } => "compressing",
        P::CheckingConnectivity => "checking-connectivity",
        P::UpdatingRepository { .. } => "updating-repository",
        P::RecordingCommits => "recording-commits",
        P::UpdatingReferences => "updating-references",
    }
}
