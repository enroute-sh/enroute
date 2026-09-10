//! Whole trees, read as one composed walk.
//!
//! Listing every path under a commit and feeding a diff its trees are
//! storage walks: the graph says which trees exist, the bucket says what
//! each holds, and a caller doing either pays a round trip per directory.
//! How much work one walk may spend stays the caller's, so every entry
//! point takes its budget as an argument.

use std::collections::{HashMap, HashSet};

use anyhow::{Result, anyhow};
use futures::StreamExt as _;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::ObjectSeq;
use enroute_git_graph::{
    DiffOptions, ObjectRefs, TreeChild, TreeDiff, TreeReader, diff_trees_with, object_refs,
};
use enroute_git_metadata::RepoMetadata;

use crate::Storage;

/// How many tree objects are read at once.
///
/// A level's reads are independent and go out together, bounded so a wide
/// directory does not open one request per child.
const TREE_READ_CONCURRENCY: usize = 32;

/// One entry a tree walk found, its path in git's own bytes.
///
/// Bytes rather than a string because git requires no encoding of a name;
/// how to show an invalid one is the caller's decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeItem {
    /// The path from the walk's root, `/`-joined.
    pub path: Vec<u8>,
    /// What the entry names.
    pub oid: ObjectId,
    /// Whether the entry is a directory rather than a file.
    pub is_tree: bool,
}

/// Every path under one starting point, one level per depth.
///
/// Levels rather than a flat list so a caller answering in pieces need not
/// re-derive where a depth ends.
#[derive(Debug, Default)]
pub struct TreeWalk {
    /// The entries, breadth first, one `Vec` per depth.
    pub levels: Vec<Vec<TreeItem>>,
    /// Whether the walk hit its entry budget with more left to name.
    pub truncated: bool,
}

/// Why a tree walk gave no listing.
///
/// Typed rather than an `anyhow` chain so a caller can tell its own fault
/// from this side's without reading message strings.
#[derive(Debug, thiserror::Error)]
pub enum TreeError {
    /// The repository holds no object by this id.
    #[error("no object {0}")]
    Missing(ObjectId),
    /// The id names an object no tree walk can start from.
    #[error("{oid} is a {kind}")]
    NotTreeish {
        /// What the caller named.
        oid: ObjectId,
        /// What it turned out to be.
        kind: Kind,
    },
    /// The store or an index would not answer, or disagreed with itself.
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

/// Every path under `start`, breadth first, at most `entry_limit` of them.
///
/// A commit is resolved to its root tree, so a caller holding a branch tip
/// needs no round trip of its own to turn it into one.
///
/// # Errors
/// [`TreeError::Missing`] and [`TreeError::NotTreeish`] name the caller's
/// argument; everything else is the storage's.
pub async fn tree(
    storage: &Storage,
    repo: &RepoMetadata,
    start: ObjectId,
    entry_limit: usize,
) -> Result<TreeWalk, TreeError> {
    let root = root_tree(storage, repo, start).await?;
    let oids = trees_under(storage, repo, root, entry_limit).await?;
    let trees = read_trees(storage, repo, oids).await?;
    Ok(levels(&trees, root, entry_limit))
}

/// Diff two trees, fetching what each pass says it was missing.
///
/// Every round names a tree the last one had not read, so the trees in hand
/// only grow and the loop ends — at `read_limit` trees if not before.
///
/// # Errors
/// Whatever reading a tree said. Roots that name nothing are not an error:
/// the diff reports them unreadable, and the answer says it is partial.
pub async fn diff_trees(
    storage: &Storage,
    repo: &RepoMetadata,
    old_root: Option<ObjectId>,
    new_root: ObjectId,
    options: DiffOptions,
    read_limit: usize,
) -> Result<TreeDiff> {
    let mut read = ReadTrees::default();
    loop {
        let diff = diff_trees_with(&mut read, old_root, new_root, options);
        // A diff that stopped short is still reported: `is_complete` says the
        // paths are a part of the answer, which is what a listing does too.
        if diff.is_complete() || read.0.len() >= read_limit {
            return Ok(diff);
        }
        read.0
            .extend(read_trees(storage, repo, diff.unreadable).await?);
    }
}

/// The trees a diff has already read.
///
/// The diff itself holds no storage and is not async, so it names what it
/// could not read and is run again — this is what it reads from.
#[derive(Default)]
struct ReadTrees(HashMap<ObjectId, Vec<TreeChild>>);

impl TreeReader for ReadTrees {
    fn children(&mut self, oid: ObjectId) -> Option<Vec<TreeChild>> {
        self.0.get(&oid).cloned()
    }
}

/// The tree a walk starts from, with a commit resolved to its root.
///
/// Identity answers what `start` is before any bytes move, so a walk
/// starting at a tree reads no object to find that out.
async fn root_tree(
    storage: &Storage,
    repo: &RepoMetadata,
    start: ObjectId,
) -> Result<ObjectId, TreeError> {
    // Numbered without being recorded reads as absent: a walk from it would
    // otherwise return the root and none of the tree under it.
    let Some(found) = crate::meta(storage, repo.id, start)
        .await?
        .filter(enroute_git_core::ObjectMeta::is_stored)
    else {
        return Err(TreeError::Missing(start));
    };
    match found.kind {
        Kind::Tree => Ok(start),
        Kind::Commit => {
            let (kind, bytes) = crate::object(storage, repo, start)
                .await
                .map_err(|error| anyhow!(error).context(format!("reading commit {start}")))?;
            if let Ok(ObjectRefs::Commit { root_tree, .. }) = object_refs(kind, &bytes) {
                return Ok(root_tree);
            }
            Err(anyhow!("commit {start} did not parse as one").into())
        }
        kind => Err(TreeError::NotTreeish { oid: start, kind }),
    }
}

/// Every tree under `root`, found without reading an object.
///
/// The graph records each tree's direct entries, so the shape of one is
/// answerable from Postgres.
///
/// # Errors
/// When identity holds no tree at `root`. Reading the objects instead
/// starts at that same row, so it could not answer either.
async fn trees_under(
    storage: &Storage,
    repo: &RepoMetadata,
    root: ObjectId,
    entry_limit: usize,
) -> Result<Vec<ObjectId>> {
    let seqs = storage.rows.repo(repo.id).identify(&[root]).await?;
    // A root tree is a tree, and its seq means nothing in another space.
    let root_seq = seqs
        .get(&root)
        .copied()
        .filter(|held| held.kind == Kind::Tree)
        .and_then(|held| u64::try_from(held.seq).ok())
        .ok_or_else(|| anyhow!("identity holds no tree at {root}"))?;

    let mut found = vec![root];
    let mut frontier = vec![root_seq];
    let mut seen: HashSet<u64> = HashSet::from([root_seq]);

    while !frontier.is_empty() {
        let children = storage.objects.repo(repo.id).children_of(&frontier).await?;

        // A tree two directories share is one tree, and expanding it twice
        // costs its whole subtree again. Blobs never enter this: the entries
        // arrive already split, so a blob is not resolved only to be dropped.
        frontier = children
            .values()
            .flat_map(|entries| entries.trees.iter())
            .filter(|seq| seen.insert(*seq))
            .collect();
        if frontier.is_empty() {
            break;
        }

        let wanted: Vec<ObjectSeq> = frontier.iter().copied().map(ObjectSeq::Tree).collect();
        let metas = storage
            .objects
            .repo(repo.id)
            .locations_by_seq(&wanted)
            .await?;
        found.extend(metas.into_iter().map(|(oid, _)| oid));

        if found.len() > entry_limit {
            break;
        }
    }

    Ok(found)
}

/// Read every named tree at once, as a map from oid to its entries.
///
/// The one place a walk touches object storage, and it happens in a single
/// bounded fan-out rather than a round of reads per depth.
async fn read_trees(
    storage: &Storage,
    repo: &RepoMetadata,
    oids: Vec<ObjectId>,
) -> Result<HashMap<ObjectId, Vec<TreeChild>>> {
    let read = futures::stream::iter(oids.into_iter().map(|oid| async move {
        // A miss here is an index naming a tree the store does not hold,
        // which is this side's inconsistency and never the caller's.
        let (kind, bytes) = crate::object(storage, repo, oid)
            .await
            .map_err(|error| anyhow!(error).context(format!("reading tree {oid}")))?;
        let children = object_refs(kind, &bytes)
            .ok()
            .and_then(ObjectRefs::into_tree_children)
            .unwrap_or_default();
        Ok::<_, anyhow::Error>((oid, children))
    }))
    .buffer_unordered(TREE_READ_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    read.into_iter().collect()
}

/// Walk the trees already in memory, breadth first, naming every path.
fn levels(
    trees: &HashMap<ObjectId, Vec<TreeChild>>,
    root: ObjectId,
    entry_limit: usize,
) -> TreeWalk {
    let mut walk = TreeWalk::default();
    let mut level = vec![(Vec::new(), root)];
    let mut named = 0usize;

    while !level.is_empty() {
        let mut entries = Vec::new();
        let mut next = Vec::new();
        for (prefix, oid) in level {
            let Some(children) = trees.get(&oid) else {
                continue;
            };
            let found: Vec<TreeItem> = children
                .iter()
                .filter_map(|child| tree_item(&prefix, child))
                .collect();
            next.extend(
                found
                    .iter()
                    .filter(|item| item.is_tree)
                    .map(|item| (item.path.clone(), item.oid)),
            );
            entries.extend(found);
        }

        if named + entries.len() > entry_limit {
            entries.truncate(entry_limit.saturating_sub(named));
            walk.levels.push(entries);
            walk.truncated = true;
            return walk;
        }

        named += entries.len();
        if !entries.is_empty() {
            walk.levels.push(entries);
        }
        level = next;
    }

    walk
}

/// One entry under its walk-rooted path.
///
/// `None` for a gitlink, skipped for the reason `deps` skips them: it names
/// a commit in a repository this one does not have.
fn tree_item(prefix: &[u8], child: &TreeChild) -> Option<TreeItem> {
    if child.is_commit {
        return None;
    }

    let mut path = Vec::with_capacity(prefix.len() + 1 + child.name.len());
    if !prefix.is_empty() {
        path.extend_from_slice(prefix);
        path.push(b'/');
    }
    path.extend_from_slice(&child.name);

    Some(TreeItem {
        path,
        oid: child.oid,
        is_tree: child.is_tree,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use gix_hash::ObjectId;

    use enroute_git_core::oid;
    use enroute_git_graph::TreeChild;

    use super::{TreeItem, levels};

    fn dir(name: &str, to: ObjectId) -> TreeChild {
        TreeChild {
            oid: to,
            is_tree: true,
            is_commit: false,
            name: name.as_bytes().to_vec(),
        }
    }

    fn file(name: &str, to: ObjectId) -> TreeChild {
        TreeChild {
            oid: to,
            is_tree: false,
            is_commit: false,
            name: name.as_bytes().to_vec(),
        }
    }

    fn paths(walk: &super::TreeWalk) -> Vec<String> {
        walk.levels
            .iter()
            .flatten()
            .map(|item| String::from_utf8_lossy(&item.path).into_owned())
            .collect()
    }

    #[test]
    fn a_gitlink_is_skipped() {
        let root = oid(1);
        let trees = HashMap::from([(
            root,
            vec![
                file("a.txt", oid(2)),
                TreeChild {
                    oid: oid(3),
                    is_tree: false,
                    is_commit: true,
                    name: b"vendored".to_vec(),
                },
            ],
        )]);

        let walk = levels(&trees, root, 100);
        assert_eq!(paths(&walk), ["a.txt"]);
        assert!(!walk.truncated);
    }

    #[test]
    fn a_shared_directory_lists_under_both_names() {
        let (root, shared) = (oid(1), oid(2));
        let trees = HashMap::from([
            (root, vec![dir("left", shared), dir("right", shared)]),
            (shared, vec![file("kept.txt", oid(3))]),
        ]);

        let walk = levels(&trees, root, 100);
        assert_eq!(
            paths(&walk),
            ["left", "right", "left/kept.txt", "right/kept.txt"]
        );
    }

    #[test]
    fn the_budget_cuts_the_level_it_lands_in() {
        let root = oid(1);
        let trees = HashMap::from([(
            root,
            vec![file("a", oid(2)), file("b", oid(3)), file("c", oid(4))],
        )]);

        let walk = levels(&trees, root, 2);
        assert!(walk.truncated);
        assert_eq!(paths(&walk), ["a", "b"]);
    }

    #[test]
    fn a_budget_already_spent_still_says_truncated() {
        let (root, sub) = (oid(1), oid(2));
        let trees = HashMap::from([
            (root, vec![dir("sub", sub)]),
            (sub, vec![file("more.txt", oid(3))]),
        ]);

        // The first level spends the whole budget, so the next is cut to
        // nothing — and the cut must still be reported.
        let walk = levels(&trees, root, 1);
        assert!(walk.truncated);
        assert_eq!(paths(&walk), ["sub"]);
        assert_eq!(walk.levels.last().map(Vec::len), Some(0));
    }

    #[test]
    fn an_unread_tree_ends_the_descent_quietly() {
        let root = oid(1);
        let trees = HashMap::from([(root, vec![dir("gone", oid(9)), file("kept", oid(3))])]);

        let walk = levels(&trees, root, 100);
        assert_eq!(paths(&walk), ["gone", "kept"]);
    }

    #[test]
    fn items_name_their_kind_and_oid() {
        let root = oid(1);
        let trees = HashMap::from([
            (root, vec![dir("d", oid(2))]),
            (oid(2), vec![file("f", oid(3))]),
        ]);

        let walk = levels(&trees, root, 100);
        let flat: Vec<&TreeItem> = walk.levels.iter().flatten().collect();
        assert!(flat[0].is_tree);
        assert_eq!(flat[1].oid, oid(3));
        assert_eq!(flat[1].path, b"d/f");
    }
}
