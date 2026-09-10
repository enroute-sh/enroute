//! What a fetch needs, walked across the two stores that hold it.
//!
//! A walk is two halves with two owners: the commit graph answers what walks
//! commits, the object index what reads trees and locations, and neither can
//! answer the other's. Each store answers for itself, so what is left here is
//! the seeds, in oids, and the two stores named side by side.

use anyhow::{Context, Result};
use gix_hash::ObjectId;

use enroute_git_core::{ObjectMeta, ObjectSeq, ObjectSeqs};
use enroute_git_graph_store::{BandPolicy, NeededCommits, RepoGraph, WalkSeeds};
use enroute_git_objects::RepoObjects;

/// Result of a needed-set walk, shallow (`deepen <depth>`) or not.
///
/// A plain fetch leaves `backfill`/`shallow`/`unshallow` empty; otherwise
/// `backfill` carries what a boundary snapshot references that `needed` misses.
#[derive(Debug, Default, Clone)]
pub struct ShallowNeeded {
    /// Commits within the depth cut, streamed as full packs.
    pub needed: NeededCommits,
    /// Boundary-snapshot trees/blobs not covered by `needed`, resolved to
    /// pack locations for loose streaming.
    pub backfill: Vec<(ObjectId, ObjectMeta)>,
    /// Every object in the boundary snapshots' tree closure.
    ///
    /// So [`Self::contains`] is an O(1) bitmap check.
    pub backfill_seqs: ObjectSeqs,
    /// New shallow-boundary commits, for the response's `shallow` lines
    /// (boundary commits already reported shallow are omitted).
    pub shallow: Vec<ObjectId>,
    /// Client-shallow commits whose parents this fetch sends, for the
    /// response's `unshallow` lines.
    pub unshallow: Vec<ObjectId>,
}

impl ShallowNeeded {
    /// Whether `seq` is in any pack this fetch streams (needed or backfill).
    #[must_use]
    pub fn contains(&self, seq: ObjectSeq) -> bool {
        self.needed.contains(seq) || self.backfill_seqs.contains(seq)
    }
}

/// Every commit reachable from `wants` but not `haves`, with the union of
/// their pack-content bitmaps.
///
/// See [`shallow`] for the `deepen <depth>` form.
pub(crate) async fn plain(
    graph: RepoGraph<'_>,
    wants: &[ObjectId],
    haves: &[ObjectId],
    client_shallow: &[ObjectId],
) -> Result<NeededCommits> {
    if wants.is_empty() {
        return Ok(NeededCommits::default());
    }
    let seeds = resolve_seeds(graph, wants, haves, client_shallow).await?;
    graph.needed_commits(&seeds, BandPolicy::production()).await
}

/// The needed set for a shallow (`deepen <depth>`) fetch.
///
/// The graph cuts the history and names the boundary's root trees; closing
/// over those trees is the object index's answer, so the two meet here.
pub(crate) async fn shallow(
    graph: RepoGraph<'_>,
    objects: RepoObjects<'_>,
    wants: &[ObjectId],
    haves: &[ObjectId],
    client_shallow: &[ObjectId],
    depth: u64,
) -> Result<ShallowNeeded> {
    let depth = i64::try_from(depth).context("deepen depth out of range")?;
    let seeds = resolve_seeds(graph, wants, haves, client_shallow).await?;
    let walked = graph
        .needed_commits_shallow(&seeds, depth, BandPolicy::production())
        .await?;

    let (backfill, backfill_seqs) = if walked.boundary_roots.is_empty() {
        (Vec::new(), ObjectSeqs::default())
    } else {
        let closure = objects.tree_closure(&walked.boundary_roots).await?;
        // Drop cross-lineage duplicates `needed` already covers before
        // resolving locations.
        let seqs: Vec<ObjectSeq> = closure
            .iter()
            .filter(|seq| !walked.needed.contains(*seq))
            .collect();
        (objects.locations_by_seq(&seqs).await?, closure)
    };

    Ok(ShallowNeeded {
        needed: walked.needed,
        backfill,
        backfill_seqs,
        shallow: walked.shallow,
        unshallow: walked.unshallow,
    })
}

/// Resolve want/have/client-shallow seed oids to commit seqs, deduplicated.
///
/// The one place oid space meets the walk. An oid this repository never
/// numbered is dropped, and one numbered but not recorded walks to nothing.
async fn resolve_seeds(
    graph: RepoGraph<'_>,
    wants: &[ObjectId],
    haves: &[ObjectId],
    client_shallow: &[ObjectId],
) -> Result<WalkSeeds> {
    let all: Vec<ObjectId> = wants
        .iter()
        .chain(haves.iter())
        .chain(client_shallow.iter())
        .copied()
        .collect();
    let seq_of = graph.seqs_of(&all).await?;

    let resolve = |oids: &[ObjectId]| -> Vec<i64> {
        let mut seen = std::collections::HashSet::new();
        oids.iter()
            .filter_map(|oid| seq_of.get(oid).copied())
            .filter(|seq| seen.insert(*seq))
            .collect()
    };
    Ok(WalkSeeds {
        wants: resolve(wants),
        haves: resolve(haves),
        shallow: resolve(client_shallow),
    })
}

/// What a fetch must send: every commit reachable from `wants` but not
/// `haves`, cut at `deepen`'s depth when there is one.
///
/// # Errors
/// Whatever the two stores said.
pub async fn needed(
    graph: RepoGraph<'_>,
    objects: RepoObjects<'_>,
    wants: &[ObjectId],
    haves: &[ObjectId],
    client_shallow: &[ObjectId],
    deepen: Option<u64>,
) -> Result<ShallowNeeded> {
    if let Some(depth) = deepen {
        shallow(graph, objects, wants, haves, client_shallow, depth).await
    } else {
        Ok(ShallowNeeded {
            needed: plain(graph, wants, haves, client_shallow).await?,
            ..Default::default()
        })
    }
}
