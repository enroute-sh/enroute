//! One background pass over everything a deployment has to keep tidy, and
//! the two places it runs from.
//!
//! Erasing deleted repositories, gathering their pack images, merging index
//! segments down a tier and sweeping both kinds of orphan are one pass
//! rather than five jobs: each makes work for the next, so a run that does
//! them in order finishes what it starts. Every step is idempotent, and only
//! the gather takes a lock — a per-repository one it skips rather than waits
//! on — so running this in the serving process on a timer, in a scheduled
//! invocation beside it, or both at once is safe. See [`run`] for the order.

use core::fmt;
use core::time::Duration;
use std::collections::HashSet;

use anyhow::{Context, Result};
use enroute_git_core::Ulid;
use enroute_git_retrieve::{Coalesced, RepoMetadata, STARTING_COALESCE, Storage, coalesce};
use enroute_git_store::Store;

/// How patient one pass is, and whether it deletes at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Maintenance {
    /// Minimum age of an unreferenced object before it counts as an orphan.
    pub grace_secs: u64,
    /// Minimum age of a repository's deletion before it is erased.
    pub deleted_grace_secs: u64,
    /// Report what a pass would take, and take nothing.
    pub dry_run: bool,
}

impl Maintenance {
    /// The pass `configured` describes, taking nothing else from it.
    ///
    /// One place, because both structs are all-`u64` and a window copied at
    /// one call site and not the other is a thing no compiler would catch.
    #[must_use]
    pub fn from_config(configured: &enroute_config::Maintenance, dry_run: bool) -> Self {
        Self {
            grace_secs: configured.grace_secs,
            deleted_grace_secs: configured.deleted_grace_secs,
            dry_run,
        }
    }
}

impl Default for Maintenance {
    /// The configuration's own defaults, so the pass and the file it is
    /// usually built from cannot drift apart.
    fn default() -> Self {
        Self::from_config(&enroute_config::Maintenance::default(), false)
    }
}

/// Sums a summary's counters, saturating.
///
/// Written once per struct rather than field by field: a counter nobody
/// remembered to add reads zero across a deployment and says nothing.
macro_rules! sum_counters {
    ($summary:ident { $($field:ident),+ $(,)? }) => {
        impl core::ops::AddAssign for $summary {
            fn add_assign(&mut self, other: Self) {
                // Destructured, so a new counter stops this compiling.
                let Self { $($field),+ } = other;
                $(self.$field = self.$field.saturating_add($field);)+
            }
        }
    };
}

/// What one purge pass reclaimed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PurgeSummary {
    /// Repositories erased, rows and all.
    pub repos: u64,
    /// Stored objects deleted across them.
    pub objects: u64,
    /// Their summed sizes.
    pub reclaimed_bytes: u64,
    /// Bucket objects a delete failed on, left for the index sweep to find.
    pub orphaned: usize,
    /// Repositories held back because one of those deletes failed.
    pub deferred: u64,
}

/// What merging index segments down a tier did.
///
/// The substrate's own report, converted here, so nothing above the engine
/// names a `lattice` type to read a number out of it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MergeSummary {
    /// Segments consumed by a merge.
    pub merged: u64,
    /// Bytes the merged segments were written back out as.
    pub written: u64,
    /// Merged segments large enough to graduate to the bucket.
    pub graduated: u64,
    /// Superseded objects a delete failed on, left for the sweep below.
    pub orphaned: u64,
}

sum_counters!(MergeSummary {
    merged,
    written,
    graduated,
    orphaned,
});

/// What sweeping the index segments' bucket objects found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct IndexSweepSummary {
    /// Objects under an index prefix that are named as a segment is.
    pub scanned: u64,
    /// Ones no catalog row named, past the grace window.
    pub orphans: u64,
    /// What deleting them freed.
    pub reclaimed_bytes: u64,
}

sum_counters!(IndexSweepSummary {
    scanned,
    orphans,
    reclaimed_bytes,
});

/// Totals across a sweep, printed by the binary.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepSummary {
    /// Segment objects listed across all repos.
    pub scanned: u64,
    /// Orphans past the grace window (deleted, or would-be under `--dry-run`).
    pub orphans: u64,
    /// Their summed sizes.
    pub reclaimed_bytes: u64,
}

sum_counters!(SweepSummary {
    scanned,
    orphans,
    reclaimed_bytes,
});

/// What one pass did, step by step.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Pass {
    /// Deleted repositories erased.
    pub purged: PurgeSummary,
    /// Index segments merged down a tier.
    pub merged: MergeSummary,
    /// Pack images copied into fewer objects.
    pub coalesced: Coalesced,
    /// Index objects no catalog row named.
    pub indexes: IndexSweepSummary,
    /// Pack images no row referenced.
    pub packs: SweepSummary,
    /// Whether this pass deleted anything or only looked.
    pub dry_run: bool,
}

impl Pass {
    /// Two repositories' shares of one pass, summed.
    ///
    /// `purged` and `dry_run` belong to the whole pass rather than to a
    /// repository, so they are kept rather than added.
    #[must_use]
    fn plus(mut self, other: Self) -> Self {
        self.merged += other.merged;
        self.coalesced = self.coalesced.plus(other.coalesced);
        self.indexes += other.indexes;
        self.packs += other.packs;
        self
    }
}

impl fmt::Display for Pass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reclaimed = self
            .purged
            .reclaimed_bytes
            .saturating_add(self.indexes.reclaimed_bytes)
            .saturating_add(self.packs.reclaimed_bytes);
        write!(
            f,
            "{} repositories erased, {} pack images moved into {} fewer objects, \
             {} segments merged into {} bytes ({} graduated), \
             {} index orphans and {} pack orphans taken, {reclaimed} bytes reclaimed{}",
            self.purged.repos,
            self.coalesced.images,
            self.coalesced.segments,
            self.merged.merged,
            self.merged.written,
            self.merged.graduated,
            self.indexes.orphans,
            self.packs.orphans,
            if self.dry_run { " (dry run)" } else { "" }
        )
    }
}

/// Run one pass: erase, merge, then sweep what those two left behind.
///
/// The order is what makes one run enough: erasing drops repositories the
/// rest would scan, and gathering and merging make what the sweeps take.
///
/// # Errors
/// Whatever a store said. Every step is safe to repeat and safe to
/// interrupt, so a failed pass loses nothing a later one cannot redo.
pub async fn run(storage: &Storage, config: Maintenance) -> Result<Pass> {
    let mut pass = Pass {
        dry_run: config.dry_run,
        ..Pass::default()
    };
    pass.purged = purge_deleted_repos(storage, config.deleted_grace_secs, config.dry_run).await?;

    for repo in storage.rows.all().await? {
        // Per repository, since the steps below make work for each other but
        // not across repositories, and one in a bad state must not stop the
        // rest of a deployment being kept tidy.
        match run_repo(storage, &repo, config).await {
            Ok(one) => pass = pass.plus(one),
            Err(error) => {
                tracing::warn!(repo_id = %repo.id, %error, "maintaining a repository failed");
            }
        }
    }
    Ok(pass)
}

/// One repository's share of a pass: gather, merge, then sweep both kinds of
/// orphan it holds.
///
/// Erasing deleted repositories is not here, since a repository being erased
/// is not one being maintained. Everything else a pass does is.
///
/// # Errors
/// Whatever a store said.
pub async fn run_repo(storage: &Storage, repo: &RepoMetadata, config: Maintenance) -> Result<Pass> {
    let mut pass = Pass {
        dry_run: config.dry_run,
        ..Pass::default()
    };

    // Before the merge, so the index segments it writes are merged by this
    // pass rather than left for the next one.
    if !config.dry_run {
        pass.coalesced = coalesce(storage, repo, STARTING_COALESCE).await?;

        // Nothing to say for a dry run: a merge writes a segment, and one
        // that wrote nothing is not what the next one would do.
        let merged = storage.compact_indexes_of(repo.id).await?;
        pass.merged = MergeSummary {
            merged: as_u64(merged.merged),
            written: merged.written,
            graduated: as_u64(merged.graduated),
            orphaned: as_u64(merged.orphaned),
        };
    }

    let now_ms = now_ms()?;
    let swept = storage
        .sweep_index_segments_of(repo.id, now_ms, config.grace_secs, config.dry_run)
        .await?;
    pass.indexes = IndexSweepSummary {
        scanned: swept.scanned,
        orphans: swept.orphans,
        reclaimed_bytes: swept.bytes,
    };

    pass.packs = sweep_pack_images(
        storage,
        repo,
        SweepConfig {
            now_ms,
            grace_secs: config.grace_secs,
            dry_run: config.dry_run,
        },
    )
    .await?;
    Ok(pass)
}

/// Run a pass every `interval`, for as long as this process serves.
///
/// The first one waits, since a starting process has better things to do,
/// and a failed one is logged rather than fatal.
#[must_use]
pub fn spawn(
    storage: Storage,
    config: Maintenance,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match run(&storage, config).await {
                Ok(pass) => tracing::info!(%pass, "maintenance pass"),
                Err(error) => tracing::warn!(%error, "maintenance pass failed"),
            }
        }
    })
}

/// Erase every repository deleted longer than `grace_secs` ago: its stored
/// objects first, then its rows.
///
/// Dropping the rows first would leave bytes nothing can ever name again, so
/// one whose objects did not all go waits for the next run.
///
/// # Errors
///
/// Returns an error if a store is unavailable; the next run resumes.
pub async fn purge_deleted_repos(
    metadata: &Storage,
    grace_secs: u64,
    dry_run: bool,
) -> Result<PurgeSummary> {
    let store = &metadata.store;
    let mut summary = PurgeSummary::default();
    for repo in metadata.rows.deleted(grace_secs).await? {
        if dry_run {
            summary.repos += 1;
            continue;
        }
        let (objects, bytes) = store
            .delete_repo_objects(&repo)
            .await
            .with_context(|| format!("deleting objects of repo {}", repo.id))?;

        // The index segments are the store's own objects, under keys of their
        // own — and the index sweep anti-joins against the catalog rows this
        // purge is about to drop, so they must go before it, not after.
        let segments = metadata
            .graph
            .repo(repo.id)
            .purge_bucket()
            .await?
            .plus(metadata.objects.repo(repo.id).purge_bucket().await?);

        summary.objects += objects + segments.objects;
        summary.reclaimed_bytes += bytes + segments.bytes;
        summary.orphaned += segments.orphaned;

        // A delete that failed leaves an object only these rows still name.
        // Dropping them now would strand it for good, so the repository
        // waits for the next run instead.
        if segments.orphaned > 0 {
            summary.deferred += 1;
            continue;
        }
        metadata
            .ledger
            .erase(repo.id)
            .await
            .with_context(|| format!("purging rows of repo {}", repo.id))?;
        summary.repos += 1;
    }
    Ok(summary)
}

/// Sweep every repository's pack images against the rows that name them.
///
/// Here rather than in the engine, because a pack image is named by a row
/// while an index segment is named by a catalog.
async fn sweep_pack_images(
    storage: &Storage,
    repo: &RepoMetadata,
    config: SweepConfig,
) -> Result<SweepSummary> {
    // What hands a gathered-away object to the anti-join below, and not
    // before its grace window has passed: until this row goes, the object is
    // referenced and a reader part way through it is safe.
    if !config.dry_run {
        storage
            .rows
            .repo(repo.id)
            .drop_retired_segments(config.grace_secs)
            .await?;
    }

    let listed = storage.store.list_segments(repo).await?;
    // LIST first, then read Postgres: a push committing between the two shows
    // up as referenced. The reverse order would let a segment uploaded after
    // the query look like an orphan.
    let listed_ids: Vec<Ulid> = listed.iter().map(|(id, _)| *id).collect();
    let referenced = storage
        .rows
        .repo(repo.id)
        .referenced_segments(&listed_ids)
        .await?;
    let orphans = sweep_repo(&storage.store, repo, &listed, &referenced, config).await?;

    let mut summary = SweepSummary {
        scanned: as_u64(listed.len()),
        orphans: as_u64(orphans.len()),
        reclaimed_bytes: 0,
    };
    for orphan in orphans {
        summary.reclaimed_bytes += orphan.size;
    }
    Ok(summary)
}

/// One deletable orphan: its segment id and object size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Orphan {
    /// The segment object's ULID.
    id: Ulid,
    /// Object size in bytes, for the summary's reclaimed total.
    size: u64,
}

/// The sweep's shared knobs, fixed for a whole run.
#[derive(Debug, Clone, Copy)]
struct SweepConfig {
    /// "Now", as milliseconds since the epoch — compared against segment
    /// ULID timestamps.
    now_ms: u64,
    /// Minimum age before an unreferenced segment counts as an orphan.
    grace_secs: u64,
    /// List orphans without deleting.
    dry_run: bool,
}

/// Decide which of `listed` to delete: unreferenced, and stamped older than
/// `grace_secs` before `now_ms` by the ULID's own timestamp.
///
/// The window is what tells an orphan from a push still running: an orphan
/// left behind is wasted bytes, a live segment deleted is a corrupt commit.
#[must_use]
fn plan_repo_sweep(
    listed: &[(Ulid, u64)],
    referenced: &HashSet<Ulid>,
    now_ms: u64,
    grace_secs: u64,
) -> Vec<Orphan> {
    listed
        .iter()
        .filter(|(id, _)| !referenced.contains(id))
        .filter(|(id, _)| {
            now_ms.saturating_sub(id.timestamp_ms()) > grace_secs.saturating_mul(1000)
        })
        .map(|(id, size)| Orphan {
            id: *id,
            size: *size,
        })
        .collect()
}

/// Delete `listed`'s orphans past the grace window — unless `config.dry_run`
/// — and return them, which is all the caller needs to tally a summary.
///
/// # Errors
/// Returns an error if a delete fails; ones already done stay done, and the
/// next run picks up the rest.
async fn sweep_repo(
    store: &Store,
    repo: &RepoMetadata,
    listed: &[(Ulid, u64)],
    referenced: &HashSet<Ulid>,
    config: SweepConfig,
) -> Result<Vec<Orphan>> {
    let orphans = plan_repo_sweep(listed, referenced, config.now_ms, config.grace_secs);
    if config.dry_run {
        return Ok(orphans);
    }
    for orphan in &orphans {
        store
            .delete_segment(repo, orphan.id)
            .await
            .with_context(|| format!("deleting orphan {}", orphan.id))?;
    }
    Ok(orphans)
}

/// "Now" as the sweeps compare it, against a segment ULID's own timestamp.
fn now_ms() -> Result<u64> {
    use std::time::{SystemTime, UNIX_EPOCH};
    Ok(u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| anyhow::anyhow!("the system clock is before the epoch: {error}"))?
            .as_millis(),
    )
    .unwrap_or(u64::MAX))
}

/// A count in the width a summary holds, saturating rather than wrapping.
fn as_u64(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ulid_at(ms: u64) -> Ulid {
        Ulid::from_parts(ms, 42)
    }

    #[test]
    fn plan_deletes_only_unreferenced_past_grace() {
        let now: u64 = 10_000_000;
        let old_orphan = ulid_at(now - 7200 * 1000);
        let fresh_orphan = ulid_at(now - 60 * 1000);
        let old_referenced = ulid_at(now - 9000 * 1000);
        let listed = vec![
            (old_orphan, 100),
            (fresh_orphan, 200),
            (old_referenced, 300),
        ];
        let referenced: HashSet<Ulid> = [old_referenced].into_iter().collect();

        let orphans = plan_repo_sweep(&listed, &referenced, now, 3600);
        assert_eq!(
            orphans,
            vec![Orphan {
                id: old_orphan,
                size: 100
            }]
        );
    }

    async fn remaining_ids(store: &Store, repo: &RepoMetadata) -> Vec<Ulid> {
        let mut ids: Vec<Ulid> = store
            .list_segments(repo)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn sweep_repo_deletes_orphans_and_respects_dry_run() {
        use bytes::Bytes;
        use enroute_git_core::{RepoId, StorageKey};
        use object_store::memory::InMemory;
        use std::sync::Arc;

        let store = Arc::new(Store::new(Arc::new(InMemory::new())));
        let repo = RepoMetadata {
            id: RepoId::new(1),
            storage_key: StorageKey::new_v4(),
            default_branch: "refs/heads/main".to_string(),
        };

        let now: u64 = 10_000_000_000;
        let orphan_id = ulid_at(now - 7200 * 1000);
        let live_id = ulid_at(now - 7200 * 1000 + 1);
        for id in [orphan_id, live_id] {
            let mut w = store.clone().segment_writer(&repo, id);
            w.write(Bytes::from_static(b"bytes")).await.unwrap();
            w.finish().await.unwrap();
        }
        let referenced: HashSet<Ulid> = [live_id].into_iter().collect();

        let listed = store.list_segments(&repo).await.unwrap();
        let config = |dry_run| SweepConfig {
            now_ms: now,
            grace_secs: 3600,
            dry_run,
        };

        let planned = sweep_repo(&store, &repo, &listed, &referenced, config(true))
            .await
            .unwrap();
        assert_eq!(
            planned,
            vec![Orphan {
                id: orphan_id,
                size: 5
            }]
        );
        assert_eq!(
            remaining_ids(&store, &repo).await.len(),
            2,
            "dry run must not delete"
        );

        let swept = sweep_repo(&store, &repo, &listed, &referenced, config(false))
            .await
            .unwrap();
        assert_eq!(
            swept, planned,
            "dry run reports exactly what the real sweep deletes"
        );
        assert_eq!(remaining_ids(&store, &repo).await, vec![live_id]);
    }

    /// A repository's counters must reach the pass's totals, which a summary
    /// summed on the wrong side would silently keep at zero.
    #[test]
    fn a_pass_sums_every_counter_of_a_repository() {
        let one = Pass {
            merged: MergeSummary {
                merged: 1,
                written: 2,
                graduated: 3,
                orphaned: 4,
            },
            indexes: IndexSweepSummary {
                scanned: 5,
                orphans: 6,
                reclaimed_bytes: 7,
            },
            packs: SweepSummary {
                scanned: 8,
                orphans: 9,
                reclaimed_bytes: 10,
            },
            ..Pass::default()
        };

        let both = one.plus(one);

        assert_eq!(
            both.merged,
            MergeSummary {
                merged: 2,
                written: 4,
                graduated: 6,
                orphaned: 8,
            }
        );
        assert_eq!(
            both.indexes,
            IndexSweepSummary {
                scanned: 10,
                orphans: 12,
                reclaimed_bytes: 14,
            }
        );
        assert_eq!(
            both.packs,
            SweepSummary {
                scanned: 16,
                orphans: 18,
                reclaimed_bytes: 20,
            }
        );
    }
}
