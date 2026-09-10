//! The five stores a repository is kept in, assembled.
//!
//! One flat value rather than a store holding a store: `AppState` grouped a
//! bucket with a "metadata store" that was itself three, which meant a
//! caller reached what it wanted through two names instead of one. What is
//! here is only the assembling — each layer answers its own questions.

use std::sync::Arc;

use anyhow::Result;
use object_store::ObjectStore;

use enroute_git_core::RepoId;
use enroute_git_cost::Meter;
use enroute_git_graph_store::CommitGraph;
use enroute_git_journal::{Index, Ledger, MemoryLedger};
use enroute_git_metadata::Rows;
use enroute_git_objects::Objects;
use enroute_git_store::Store;
use enroute_lattice_core::Policy;
use enroute_lattice_store::{Report, Sweep, Swept};

/// What a deployment starts the segmented indexes with.
///
/// A starting point rather than a measurement: compaction is where these
/// want tuning, against real pushes.
pub const STARTING_POLICY: Policy = Policy {
    fanout: 8,
    max_inputs: 32,
    max_input_bytes: 64 << 20,
    graduation_bytes: 1 << 20,
    inline_ceiling: 64,
};

/// Every layer a repository is kept in, over one bucket.
///
/// Nothing here names a database: whether one is underneath is the ledger's
/// and the row store's to know, and neither says so from up here.
#[derive(Debug, Clone)]
pub struct Storage {
    /// What exists, where refs point, and what every object is called.
    pub rows: Rows,
    /// What a commit points at, and where its pack image is.
    pub graph: CommitGraph,
    /// Where a tree or blob is stored, and what a tree contains.
    pub objects: Objects,
    /// Where a write's index rows land, all of them or none.
    ///
    /// Behind a pointer, since which one it is is what says whether this
    /// storage has a database underneath it at all.
    pub ledger: Arc<dyn Ledger>,
    /// The bytes themselves.
    pub store: Arc<Store>,
}

impl Storage {
    /// Every layer in a map, with segments and bytes in `bucket`.
    ///
    /// No database at all: the rows are a map, and so is every catalog the
    /// ledger writes through.
    #[must_use]
    pub fn in_memory(bucket: Arc<dyn ObjectStore>, store: Arc<Store>) -> Self {
        let memory = Arc::new(enroute_git_metadata::Memory::new());
        let rows = Rows::over_memory(Arc::clone(&memory));
        Self::assemble(
            rows,
            Arc::new(MemoryLedger::in_memory(memory)),
            bucket,
            store,
        )
    }

    /// The four indexes over one ledger's catalogs, whichever kind it is.
    ///
    /// The catalogs come from the ledger that writes them, so a read and a
    /// write cannot disagree about which list is which.
    #[must_use]
    pub fn assemble(
        rows: Rows,
        ledger: Arc<dyn Ledger>,
        bucket: Arc<dyn ObjectStore>,
        store: Arc<Store>,
    ) -> Self {
        let graph = CommitGraph::new(
            ledger.catalog(Index::CommitGraph),
            ledger.catalog(Index::CommitPacks),
            rows.clone(),
            Arc::clone(&bucket),
            STARTING_POLICY,
        );
        let objects = Objects::new(
            ledger.catalog(Index::Trees),
            ledger.catalog(Index::Blobs),
            rows.clone(),
            bucket,
            STARTING_POLICY,
        );
        Self {
            rows,
            graph,
            objects,
            ledger,
            store,
        }
    }

    /// This same storage, with everything it reads or writes charged to
    /// `meter`.
    ///
    /// The meter travels inside the bucket handle, since a push's work
    /// spreads across spawned tasks where nothing ambient reaches them.
    #[must_use]
    pub fn metered(&self, meter: Arc<Meter>) -> Self {
        // Every layer that reaches the bucket, not just the bytes: a walk
        // reads segments, and an operation charged for half of what it spent
        // is what `enroute-git-cost` exists to prevent.
        Self {
            graph: self.graph.metered(Arc::clone(&meter)),
            objects: self.objects.metered(Arc::clone(&meter)),
            store: Arc::new(self.store.metered(meter)),
            ..self.clone()
        }
    }

    /// Merge one repository's index segments down a tier where the policy
    /// says to.
    ///
    /// Here rather than in whatever schedules it, so a fifth segmented value
    /// is one edit in the engine instead of one in a binary above it.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said. Safe to
    /// repeat and safe to interrupt, so the next pass resumes.
    pub async fn compact_indexes_of(&self, repo: RepoId) -> Result<Report> {
        Ok(self
            .graph
            .repo(repo)
            .compact()
            .await?
            .plus(self.objects.repo(repo).compact().await?))
    }

    /// Delete one repository's index objects that no segment row names.
    ///
    /// The other half of graduation: an object is put before the row naming
    /// it, so a rollback and a failed delete both leave one behind.
    ///
    /// # Errors
    /// Whatever the catalog or the bucket said. Safe to repeat.
    pub async fn sweep_index_segments_of(
        &self,
        repo: RepoId,
        now_ms: u64,
        grace_secs: u64,
        dry_run: bool,
    ) -> Result<Swept> {
        let sweep = Sweep {
            now_ms,
            grace_secs,
            dry_run,
        };
        Ok(self
            .graph
            .repo(repo)
            .sweep_bucket(sweep)
            .await?
            .plus(self.objects.repo(repo).sweep_bucket(sweep).await?))
    }
}
