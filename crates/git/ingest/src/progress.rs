//! Coarse, user-facing progress reporting for a push's long stages.

/// Structural stages of [`crate::IngestSession::ingest_pack`] and
/// [`crate::IngestSession::finalize`].
///
/// Reporting a stage retires the one before it, so a counted stage may
/// report `total: 0` to claim the stage before its denominator exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestProgress {
    /// Handing the push to a worker that runs elsewhere.
    ///
    /// Never reported by an in-process worker, which has no such gap.
    Dispatching,
    /// Materializing the pack's entries into objects.
    ///
    /// `done`/`total` count every entry in the pack, not only its deltas.
    ResolvingObjects {
        /// Entries resolved so far.
        done: u64,
        /// Entries in the pack.
        total: u64,
    },
    /// Deciding what each pushed commit's pack stores.
    ///
    /// The walk advances many commits at once, so this moves in jumps.
    PreparingPacks {
        /// Commits whose pack contents are settled.
        done: u64,
        /// Commits in the push.
        total: u64,
    },
    /// Encoding and staging every version the plan calls for.
    ///
    /// Separate from [`Self::ResolvingObjects`]: it counts encodings, of
    /// which one object can need several.
    CompressingObjects {
        /// Encodings staged so far.
        done: u64,
        /// Encodings the plan calls for.
        total: u64,
    },
    /// Walking the pushed refs' ancestry to confirm every object they need
    /// is either staged by this push or already recorded.
    CheckingConnectivity,
    /// Promoting staged objects to the primary store.
    ///
    /// `done`/`total` count connected commits whose pack finished uploading.
    UpdatingRepository {
        /// Commit packs uploaded so far.
        done: u64,
        /// Total connected commits to promote.
        total: u64,
    },
    /// Everything between promotion and the ref update: writing the commit
    /// graph, then the `pre-receive` hook.
    RecordingCommits,
    /// Applying the push's ref updates to the ref store.
    UpdatingReferences,
}

/// A sink for [`IngestProgress`] reports.
///
/// Invoked synchronously, never across an `.await` — a caller can back it
/// with something as simple as an atomic store.
pub type ProgressSink<'a> = &'a (dyn Fn(IngestProgress) + Send + Sync);

/// A [`ProgressSink`] that discards every report, for callers that don't
/// need one.
pub fn noop_progress(_progress: IngestProgress) {}
