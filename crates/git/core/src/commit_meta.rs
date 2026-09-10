//! Commit metadata and ordering shared with `enroute-git-metadata`'s
//! `Rows`.
//!
//! Builds on these without `enroute-git-graph` needing to depend on it.

use std::collections::VecDeque;

use gix_hash::ObjectId;

use crate::{Error, ObjectHashMap, SegmentLocation};

/// Metadata for a single commit being appended to a commit graph.
///
/// A commit is always the first object in its own pack, so no separate
/// object-index row is needed, unlike trees/blobs ([`crate::NewObject`]).
#[derive(Debug, Clone)]
pub struct NewCommit {
    /// The commit's root tree.
    pub root_tree: ObjectId,
    /// OIDs of the parent commits, in the order they appear in the object.
    pub parents: Vec<ObjectId>,
    /// Byte span of the commit's self-describing inline header plus its
    /// compressed body together, within its own pack.
    pub entry_len: u64,
    /// Absolute byte offset within this commit's pack where the blob section
    /// begins.
    ///
    /// Bounds a `blob:none` fetch's single pack GET; see
    /// `enroute-git-proto`'s `fetch_commit_run`.
    pub blob_offset: u64,
    /// When the commit was last applied, in seconds since the epoch.
    ///
    /// What the commit index ranks a commit by: a generation number bounds a
    /// walk by shape, where seq bounds it by the repository's push volume.
    pub committer_date: i64,
    /// The segment object this commit's pack image was appended to.
    ///
    /// Recorded by the upload and registered in `commit_segments` by
    /// `append`.
    pub segment: SegmentLocation,
}

/// Byte length of the commit-pack file header.
///
/// Packed-object data always starts immediately after this fixed header —
/// there's no variable-size manifest to skip over.
pub const COMMIT_PACK_HEADER_SIZE: u64 = 12;

/// Orders `new_commits` so every commit appears after its in-batch parents.
///
/// Parents outside the batch are assumed resolvable independently of order.
///
/// # Errors
///
/// Returns an error if `new_commits` contains a cycle.
pub fn topo_order(new_commits: &ObjectHashMap<Vec<ObjectId>>) -> Result<Vec<ObjectId>, Error> {
    let mut in_degree: ObjectHashMap<usize> = new_commits.keys().map(|&oid| (oid, 0)).collect();
    let mut rev_deps: ObjectHashMap<Vec<ObjectId>> = ObjectHashMap::default();

    for (&oid, parents) in new_commits {
        for parent in parents {
            if new_commits.contains_key(parent) {
                *in_degree.entry(oid).or_insert(0) += 1;
                rev_deps.entry(*parent).or_default().push(oid);
            }
        }
    }

    let mut ready: Vec<ObjectId> = in_degree
        .iter()
        .filter_map(|(&oid, &deg)| (deg == 0).then_some(oid))
        .collect();
    ready.sort_unstable();

    let mut order = Vec::with_capacity(new_commits.len());
    let mut queue = VecDeque::from(ready);
    while let Some(oid) = queue.pop_front() {
        order.push(oid);
        let mut next_ready: Vec<ObjectId> = Vec::new();
        for &dep in rev_deps.get(&oid).into_iter().flatten() {
            let deg = in_degree.entry(dep).or_insert(0);
            *deg = deg.saturating_sub(1);
            if *deg == 0 {
                next_ready.push(dep);
            }
        }
        next_ready.sort_unstable();
        queue.extend(next_ready);
    }

    if order.len() != new_commits.len() {
        return Err(anyhow::anyhow!("commit graph: cycle detected in pushed commits").into());
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::topo_order;
    use crate::{ObjectHashMap, oid};
    use gix_hash::ObjectId;

    #[test]
    fn topo_order_respects_in_batch_dependencies() {
        let (c0, c1, c2) = (oid(1), oid(2), oid(3));
        let batch = ObjectHashMap::from_iter([(c2, vec![c1]), (c0, vec![]), (c1, vec![c0])]);
        let order = topo_order(&batch).unwrap();
        let pos = |oid: ObjectId| order.iter().position(|&o| o == oid).unwrap();
        assert!(pos(c0) < pos(c1));
        assert!(pos(c1) < pos(c2));
    }

    #[test]
    fn topo_order_detects_cycle() {
        let (a, b) = (oid(1), oid(2));
        let batch = ObjectHashMap::from_iter([(a, vec![b]), (b, vec![a])]);
        let err = topo_order(&batch).unwrap_err();
        assert!(err.to_string().contains("cycle"));
    }

    #[test]
    fn topo_order_ignores_out_of_batch_parents() {
        let (known_parent, child) = (oid(9), oid(2));
        let batch = ObjectHashMap::from_iter([(child, vec![known_parent])]);
        let order = topo_order(&batch).unwrap();
        assert_eq!(order, vec![child]);
    }
}
