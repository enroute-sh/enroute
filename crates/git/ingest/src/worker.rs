//! Where a push's ingestion runs: `receive_pack` decides *that* a push
//! should be ingested, and an [`IngestWorker`] decides *where*.
//!
//! The pack crosses as a reader rather than a location: naming a staged
//! location would force every local run and test through a round trip.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use object_store::ObjectStore;
use tokio::io::AsyncBufRead;

use enroute_git_core::Error;
use enroute_git_cost::Meter;
use enroute_git_retrieve::{RefUpdate, RefsMap, RepoMetadata, Storage};

use crate::progress::ProgressSink;
use crate::ref_updates::Ingested;
use crate::session::IngestSession;
use crate::staging::StagingStore;

/// Everything one push needs ingested except its pack.
///
/// `existing` rides along rather than being fetched twice — the caller
/// reads it anyway, for the non-fast-forward pre-screen.
#[derive(Debug, Clone)]
pub struct IngestRequest {
    /// The repository being pushed to.
    pub repo: RepoMetadata,
    /// Current values of only the refs this push touches.
    pub existing: RefsMap,
    /// The ref updates the client asked for, in its order — which is the
    /// order the outcomes come back in.
    pub updates: Vec<RefUpdate>,
}

/// A pack, owned, as handed to a worker.
///
/// Boxed rather than generic so [`IngestWorker`] stays object-safe: it
/// reaches `receive_pack` as a trait object, not a type parameter.
pub type PackReader = Box<dyn AsyncBufRead + Send + Unpin>;

/// A pack, plus what its sender already knows about it.
pub struct IncomingPack {
    /// The pack's bytes, still arriving.
    pub reader: PackReader,
    /// What the pack runs to; `None` for a body still arriving.
    ///
    /// Reserves the scan's copy up front, so a wrong hint costs a
    /// reallocation, not an error.
    pub len_hint: Option<u64>,
}

/// The reader is a boxed stream with no `Debug`, so only the hint is left.
impl std::fmt::Debug for IncomingPack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingPack")
            .field("len_hint", &self.len_hint)
            .finish_non_exhaustive()
    }
}

/// Ingests one push, wherever that happens to run.
///
/// Object-safe with a `Send` future so the response path can spawn it
/// beside a keepalive ticker.
pub trait IngestWorker: Send + Sync {
    /// Store what `pack` brings and record the commit graph, reporting
    /// stages through `progress` and charging what it spends to `meter`.
    ///
    /// Ends with the objects durable and no ref moved; what lands is decided
    /// next by [`apply_ref_updates`], which only the front door can run.
    ///
    /// [`apply_ref_updates`]: crate::apply_ref_updates
    ///
    /// # Errors
    /// The pack is malformed or fails its checksum, or a store or index
    /// operation fails. A per-ref rejection is not an error — see [`Ingested`].
    ///
    /// # Cost
    /// `meter` is passed down rather than returned with the outcome, so a
    /// failed push still accounts for what it spent.
    fn ingest<'a>(
        &'a self,
        request: IngestRequest,
        pack: IncomingPack,
        progress: ProgressSink<'a>,
        meter: &'a Arc<Meter>,
    ) -> Pin<Box<dyn Future<Output = Result<Ingested, Error>> + Send + 'a>>;
}

/// Whether a packfile follows this push's commands.
///
/// `send-pack` skips `pack_objects` when every command is a delete, so
/// reading one anyway meets EOF at the header.
fn carries_pack(updates: &[RefUpdate]) -> bool {
    updates.iter().any(|u| !u.new_id.is_null())
}

/// Ingests in this process, against this process's stores.
#[derive(Debug, Clone)]
pub struct LocalIngestWorker {
    state: Storage,
    staging: Arc<StagingStore>,
}

impl LocalIngestWorker {
    /// Ingest into `state`'s stores, staging each push in `staging` — which
    /// this process must own exclusively, per [`StagingStore`].
    ///
    /// Takes the backend rather than a built [`StagingStore`], so holding one
    /// is inseparable from being the thing that ingests.
    ///
    #[must_use]
    pub fn new(state: Storage, staging: Arc<dyn ObjectStore>) -> Self {
        Self {
            state,
            staging: Arc::new(StagingStore::new(staging)),
        }
    }

    /// The same worker as the handle callers actually hold — `Arc::new`
    /// alone can't infer the trait object.
    #[must_use]
    pub fn shared(state: Storage, staging: Arc<dyn ObjectStore>) -> Arc<dyn IngestWorker> {
        Arc::new(Self::new(state, staging))
    }

    /// Reclaim staging left behind by a previous process, once, in the
    /// background.
    ///
    /// Call at startup only, where this process is the backend's sole
    /// writer — it deletes everything no live session owns.
    pub fn spawn_startup_staging_sweep(&self) {
        drop(self.staging.clone().spawn_startup_sweep());
    }
}

impl IngestWorker for LocalIngestWorker {
    fn ingest<'a>(
        &'a self,
        request: IngestRequest,
        pack: IncomingPack,
        progress: ProgressSink<'a>,
        meter: &'a Arc<Meter>,
    ) -> Pin<Box<dyn Future<Output = Result<Ingested, Error>> + Send + 'a>> {
        Box::pin(async move {
            // The caller's meter, seen through the primary store, so everything
            // this push reads and writes there is charged to it and no other.
            // Session scratch is deliberately left out — see
            // `enroute_git_cost::StoreRole::Handoff`.
            let mut session = IngestSession::new(
                self.state.metered(Arc::clone(meter)),
                self.staging.clone(),
                request.repo,
            );
            // `finalize` cleans up its own staging on every path; dropping the
            // session does the same for a pack that never parsed.
            if carries_pack(&request.updates)
                && let Err(e) = session
                    .ingest_pack(pack.reader, pack.len_hint, progress)
                    .await
            {
                drop(session);
                return Err(e);
            }
            session
                .finalize(&request.existing, &request.updates, progress)
                .await
        })
    }
}
