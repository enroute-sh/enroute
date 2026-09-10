//! Reading an object back out, across the layers that hold it.
//!
//! The mirror of `enroute-git-ingest`'s `append`: that writes identity, both
//! indexes and the bytes; this reads them. An object id names a thing
//! without saying what kind, so a read starts at the dictionary, learns
//! where the bytes are from whichever index holds that kind, and ends at the
//! bucket. Refs are not here — reading one is a single-layer read straight
//! off the rows handle and needs nothing composed.

mod coalesce;
mod fetch;
mod meta;
mod storage;
mod trees;

pub use coalesce::{CoalescePolicy, Coalesced, STARTING_COALESCE, coalesce};
pub use fetch::{RUN_GAP_BYTES, known, object, objects, runs_of};
pub use meta::{meta, metas};
pub use storage::{STARTING_POLICY, Storage};
pub use trees::{TreeError, TreeItem, TreeWalk, diff_trees, tree};

/// Re-exported so a caller reaches storage through one crate: these are the
/// rows layer's, and it is below this one.
pub use enroute_git_metadata::{
    RefEntry, RefUpdate, RefUpdateRejection, RefUpdateResult, RefsMap, RepoMetadata,
    is_branch_refname, is_direct_ref,
};

/// Re-exported from `enroute-git-graph-store`: names one row of a needed set
/// for a fetch's callers to destructure.
pub use enroute_git_graph_store::NeededCommit;

use anyhow::{Context as _, Result};
use gix_hash::ObjectId;

use enroute_git_core::RepoId;

/// Move refs without a push: no pack, so nothing can be introduced.
///
/// [`MetadataStore::update_refs`] with every tip first checked against what
/// this repository holds. A function, so no implementor can waive that.
///
/// # Errors
/// Returns an error if the backing store is unavailable.
pub async fn move_refs(
    storage: &Storage,
    repo_id: RepoId,
    updates: &[RefUpdate],
) -> Result<Vec<RefUpdateResult>> {
    let null = ObjectId::null(gix_hash::Kind::Sha1);
    let tips: Vec<ObjectId> = updates
        .iter()
        .map(|u| u.new_id)
        .filter(|new_id| *new_id != null)
        .collect();
    let stored = metas(storage, repo_id, &tips).await?;
    // Recorded, not merely numbered: a ref this repository cannot serve from
    // is the one thing this function exists to refuse.
    let is_missing = |u: &RefUpdate| {
        u.new_id != null
            && !stored
                .get(&u.new_id)
                .is_some_and(enroute_git_core::ObjectMeta::is_stored)
    };

    let present: Vec<RefUpdate> = updates.iter().filter(|u| !is_missing(u)).cloned().collect();
    let mut applied = storage
        .rows
        .repo(repo_id)
        .update_refs(&present)
        .await?
        .into_iter();

    // Zipped back in the caller's order rather than keyed by refname: an
    // outcome list is read positionally, and a map would collapse a refname
    // asked about twice into one answer. A store that answered fewer is a
    // broken store, and says so rather than inventing a plausible refusal.
    let mut results = Vec::with_capacity(updates.len());
    for update in updates {
        results.push(if is_missing(update) {
            RefUpdateResult {
                refname: update.refname.clone(),
                result: Err(RefUpdateRejection::UnknownCommit),
            }
        } else {
            applied
                .next()
                .context("the ref store skipped an update it was given")?
        });
    }
    Ok(results)
}
