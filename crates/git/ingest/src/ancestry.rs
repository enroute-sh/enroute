//! Whether a stored entry's base sits where its readers will look: in the
//! pack of an ancestor of the entry's own commit, or that commit itself.
//!
//! The proof itself is [`enroute_git_graph::ancestry`]; what is here binds it
//! to the store. A home this push introduces needs nothing but the push's own
//! commits; an older home needs a walk back from each *boundary* — a parent
//! this push names but did not send — and those walks are what cost.

use gix_hash::ObjectId;

use enroute_git_core::{Error, ObjectHashMap, ObjectHashSet};
use enroute_git_graph::{Ancestry, Boundaries, Push, Query};
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::concurrency::try_join_bounded;

/// How far back a boundary walk reaches.
///
/// Measured distances run past 100 commits for a sixth of a large push's
/// deltas, so a small cap would forfeit them; the walk costs milliseconds.
const WALK_CAP: u64 = 5_000;

/// How many boundary parents are worth one walk each.
///
/// Past this the push touches so many separate lines of history that
/// proving pre-existing bases stops paying for itself.
const MAX_BOUNDARY_WALKS: usize = 8;

/// Prove what can be proven of `queries`.
///
/// `parents_of` is this push's own commit graph and nothing else: a parent
/// missing from it is a boundary, and each boundary costs one store walk.
///
/// # Errors
/// Returns an error if the push's commits can't be topologically ordered, or
/// if a boundary walk fails.
pub(crate) async fn prove(
    queries: &[Query],
    parents_of: &ObjectHashMap<Vec<ObjectId>>,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<Ancestry, Error> {
    if queries.is_empty() {
        return Ok(Ancestry::default());
    }
    let push = Push::new(parents_of)?;
    let boundaries = push.boundaries(MAX_BOUNDARY_WALKS);

    // One walk per boundary, answering every pre-existing home at once.
    let stored_homes: Vec<ObjectId> = queries
        .iter()
        .filter(|q| !parents_of.contains_key(&q.home))
        .map(|q| q.home)
        .collect::<ObjectHashSet>()
        .into_iter()
        .collect();
    let reachable: Vec<ObjectHashSet> = try_join_bounded(boundaries.iter().map(|&boundary| {
        let stored_homes = &stored_homes;
        async move {
            state
                .graph
                .repo(repo.id)
                .ancestors_among(boundary, stored_homes, WALK_CAP)
                .await
                .map_err(Error::from)
        }
    }))
    .await?;

    Ok(push.prove(
        &Boundaries {
            oids: &boundaries,
            reachable: &reachable,
        },
        queries,
    ))
}
