//! Verifies that a pushed ref's history is fully connected.
//!
//! Every object reachable from the tip is either present in the just-pushed
//! pack or already recorded in the index.

use std::collections::VecDeque;

use gix_hash::ObjectId;

use enroute_git_core::{Error, ObjectHashMap, ObjectHashSet};
use enroute_git_graph::ObjectRefs;
use enroute_git_retrieve::{RepoMetadata, Storage};

/// Check that `tip` and everything reachable from it is either present in
/// `pack` or already in the index for `repo`.
///
/// Returns every visited oid alongside the pass/fail result, so a caller
/// checking several refs needs no second walk.
///
/// # Errors
/// Returns an error if the index is unavailable.
pub(crate) async fn check_connectivity(
    state: &Storage,
    repo: &RepoMetadata,
    tip: ObjectId,
    pack: &ObjectHashMap<ObjectRefs>,
) -> Result<(bool, ObjectHashSet), Error> {
    let mut queue: VecDeque<ObjectId> = VecDeque::new();
    let mut visited: ObjectHashSet = ObjectHashSet::default();
    let mut store_checks: Vec<ObjectId> = Vec::new();
    queue.push_back(tip);

    while let Some(sha) = queue.pop_front() {
        if !visited.insert(sha) {
            continue;
        }
        if let Some(refs) = pack.get(&sha) {
            queue.extend(refs.deps());
        } else {
            store_checks.push(sha);
        }
    }

    let found = enroute_git_retrieve::metas(state, repo.id, &store_checks)
        .await
        .map_err(Error::from)?;

    // Recorded, not merely numbered: this is the gate that decides a push
    // brought a complete history, and a number produces no bytes.
    let connected = store_checks.iter().all(|sha| {
        found
            .get(sha)
            .is_some_and(enroute_git_core::ObjectMeta::is_stored)
    });

    Ok((connected, visited))
}
