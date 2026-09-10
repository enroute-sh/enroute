//! Name-keyed recursive diff between two trees.
//!
//! Separate from attribution, which asks only *which objects are new
//! here*: delta storage needs the before/after pairing that attribution
//! discards. Mirrors `git diff-tree -r -t`, and reports what a path lost only
//! when asked. An unreadable tree is named rather than failing, so the caller
//! fetches it and runs again, keeping this crate free of storage and async.

use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::ObjectHashSet;

use crate::refs::TreeChild;

/// Supplies a tree's entries.
///
/// Implemented over whatever the caller has: staged objects during a push,
/// or the primary store.
pub trait TreeReader {
    /// The entries of tree `oid`, or `None` if it isn't available yet.
    fn children(&mut self, oid: ObjectId) -> Option<Vec<TreeChild>>;
}

/// One path whose content differs between the two trees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// Full path from the root, `/`-joined, raw bytes (git names need not be
    /// UTF-8).
    pub path: Vec<u8>,
    /// Blob or tree; a path can change from one to the other.
    pub kind: Kind,
    /// The version at this path in the old tree, or `None` if it is new here.
    pub old: Option<ObjectId>,
    /// The version at this path in the new tree.
    pub new: ObjectId,
}

/// One path the new tree no longer has.
///
/// Reported only under [`DiffOptions::removals`]. A path that changed type is
/// both a removal of what it was and a change to what it now is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removal {
    /// As [`Change::path`].
    pub path: Vec<u8>,
    /// What stood here, blob or tree.
    pub kind: Kind,
    /// The version at this path in the old tree.
    pub old: ObjectId,
}

/// What one pass of [`diff_trees`] could work out.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TreeDiff {
    /// Paths whose content differs, across every subtree that could be read.
    pub changes: Vec<Change>,
    /// Paths the new tree no longer has, empty unless
    /// [`DiffOptions::removals`] asked for them.
    pub removals: Vec<Removal>,
    /// Trees the reader couldn't supply, so the diff beneath them is
    /// missing.
    ///
    /// Fetch these and diff again. Each tree is named once even when
    /// several paths need it.
    pub unreadable: Vec<ObjectId>,
}

impl TreeDiff {
    /// Whether every subtree was readable, so `changes` is the whole answer.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.unreadable.is_empty()
    }
}

/// What a caller wants beyond the before/after pairing.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DiffOptions {
    /// Also report [`TreeDiff::removals`], descending into deleted trees so
    /// every path under one is named.
    ///
    /// Off by default: delta storage has nothing to delta against a path that
    /// is gone, so the walk of the old side would buy it nothing.
    pub removals: bool,
}

/// Both sides' entries, or `None` with the culprits recorded — a frame is
/// usable only when the whole pairing is in hand.
fn read_frame<S: TreeReader>(
    src: &mut S,
    old: Option<ObjectId>,
    new: Option<ObjectId>,
    unreadable: &mut Vec<ObjectId>,
) -> Option<(Vec<TreeChild>, Vec<TreeChild>)> {
    let mut read = |oid: Option<ObjectId>| match oid {
        None => Some(Vec::new()),
        Some(oid) => src.children(oid).or_else(|| {
            unreadable.push(oid);
            None
        }),
    };
    let old_entries = read(old);
    let new_entries = read(new);
    Some((old_entries?, new_entries?))
}

/// Diffs `new_root` against `old_root`, reporting every path whose content
/// changed.
///
/// Passing `None` for `old_root` treats everything as new. Subtrees that
/// are byte-identical are pruned, so cost scales with the change.
pub fn diff_trees<S: TreeReader>(
    src: &mut S,
    old_root: Option<ObjectId>,
    new_root: ObjectId,
) -> TreeDiff {
    diff_trees_with(src, old_root, new_root, DiffOptions::default())
}

/// [`diff_trees`], with whatever the caller wants beyond the pairing.
///
/// See [`DiffOptions`] for what the extra work is and what it costs.
pub fn diff_trees_with<S: TreeReader>(
    src: &mut S,
    old_root: Option<ObjectId>,
    new_root: ObjectId,
    options: DiffOptions,
) -> TreeDiff {
    let mut out = TreeDiff::default();
    if old_root == Some(new_root) {
        return out;
    }
    // Explicit stack: a deep tree shouldn't be able to blow the thread's stack.
    let mut stack = vec![(Vec::new(), old_root, Some(new_root))];
    while let Some((prefix, old, new)) = stack.pop() {
        let Some((old_entries, new_entries)) = read_frame(src, old, new, &mut out.unreadable)
        else {
            continue; // fetch what's missing, then come back for this subtree
        };
        let mut previous: std::collections::HashMap<&[u8], &TreeChild> = old_entries
            .iter()
            .filter(|e| !e.is_commit)
            .map(|e| (e.name.as_slice(), e))
            .collect();

        for entry in new_entries.iter().filter(|e| !e.is_commit) {
            let was = previous.remove(entry.name.as_slice());
            if was.is_some_and(|w| w.oid == entry.oid && w.is_tree == entry.is_tree) {
                continue; // identical here, so identical all the way down
            }
            let path = join(&prefix, &entry.name);

            // A path that changed type has no usable predecessor to delta
            // against, so it counts as new rather than modified — and what
            // stood there is gone, which is a removal like any other.
            let old_oid = was.filter(|w| w.is_tree == entry.is_tree).map(|w| w.oid);
            if options.removals
                && let Some(gone) = was.filter(|w| w.is_tree != entry.is_tree)
            {
                removed(&mut out, &mut stack, path.clone(), gone);
            }
            // Cloned only to descend: a blob is the common case and its path
            // is wanted in one place.
            if entry.is_tree {
                stack.push((path.clone(), old_oid, Some(entry.oid)));
            }
            out.changes.push(Change {
                path,
                kind: if entry.is_tree {
                    Kind::Tree
                } else {
                    Kind::Blob
                },
                old: old_oid,
                new: entry.oid,
            });
        }

        if options.removals {
            // Whatever the loop above did not claim by name is a path the new
            // side no longer has.
            let mut gone: Vec<&TreeChild> = previous.into_values().collect();
            gone.sort_by(|a, b| a.name.cmp(&b.name));
            for entry in gone {
                let path = join(&prefix, &entry.name);
                removed(&mut out, &mut stack, path, entry);
            }
        }
    }
    // A copied or moved directory sits under two paths, so the walk names its
    // tree twice. Every caller wants the set, so reduce it once here.
    let mut named = ObjectHashSet::default();
    out.unreadable.retain(|oid| named.insert(*oid));
    out
}

/// One entry's path, under the prefix the walk reached it at.
fn join(prefix: &[u8], name: &[u8]) -> Vec<u8> {
    let mut path = prefix.to_vec();
    if !path.is_empty() {
        path.push(b'/');
    }
    path.extend_from_slice(name);
    path
}

/// Records one removed path, and queues a removed tree so the paths under it
/// are named too.
///
/// Only ever called under [`DiffOptions::removals`]; the caller checks, since
/// it is the one holding a path it would otherwise build for nothing.
fn removed(
    out: &mut TreeDiff,
    stack: &mut Vec<(Vec<u8>, Option<ObjectId>, Option<ObjectId>)>,
    path: Vec<u8>,
    entry: &TreeChild,
) {
    if entry.is_tree {
        stack.push((path.clone(), Some(entry.oid), None));
    }
    out.removals.push(Removal {
        path,
        kind: if entry.is_tree {
            Kind::Tree
        } else {
            Kind::Blob
        },
        old: entry.oid,
    });
}

#[cfg(test)]
mod tests {
    use super::{Change, DiffOptions, TreeReader, diff_trees, diff_trees_with};
    use crate::refs::TreeChild;
    use enroute_git_core::oid;
    use gix_hash::ObjectId;
    use std::collections::HashMap;

    fn blob(name: &str, n: u8) -> TreeChild {
        TreeChild {
            oid: oid(n),
            is_tree: false,
            is_commit: false,
            name: name.as_bytes().to_vec(),
        }
    }

    fn tree(name: &str, n: u8) -> TreeChild {
        TreeChild {
            oid: oid(n),
            is_tree: true,
            is_commit: false,
            name: name.as_bytes().to_vec(),
        }
    }

    fn gitlink(name: &str, n: u8) -> TreeChild {
        TreeChild {
            oid: oid(n),
            is_tree: false,
            is_commit: true,
            name: name.as_bytes().to_vec(),
        }
    }

    struct Map(HashMap<ObjectId, Vec<TreeChild>>);

    impl TreeReader for Map {
        fn children(&mut self, o: ObjectId) -> Option<Vec<TreeChild>> {
            self.0.get(&o).cloned()
        }
    }

    fn changed(
        mut src: Map,
        old: Option<ObjectId>,
        new: ObjectId,
    ) -> Vec<(String, Option<u8>, u8)> {
        let diff = diff_trees(&mut src, old, new);
        assert!(diff.is_complete(), "unreadable: {:?}", diff.unreadable);
        let mut got: Vec<Change> = diff.changes;
        got.sort_by(|a, b| a.path.cmp(&b.path));
        got.into_iter()
            .map(|c| {
                (
                    String::from_utf8(c.path).unwrap(),
                    c.old.map(|o| o.as_slice()[0]),
                    c.new.as_slice()[0],
                )
            })
            .collect()
    }

    #[test]
    fn identical_trees_report_nothing() {
        let src = Map(HashMap::from([(oid(1), vec![blob("a", 10)])]));
        assert!(changed(src, Some(oid(1)), oid(1)).is_empty());
    }

    #[test]
    fn modified_blob_pairs_old_with_new() {
        let src = Map(HashMap::from([
            (oid(1), vec![blob("a", 10), blob("b", 20)]),
            (oid(2), vec![blob("a", 11), blob("b", 20)]),
        ]));
        assert_eq!(
            changed(src, Some(oid(1)), oid(2)),
            vec![("a".into(), Some(10), 11)]
        );
    }

    #[test]
    fn added_blob_has_no_predecessor() {
        let src = Map(HashMap::from([
            (oid(1), vec![blob("a", 10)]),
            (oid(2), vec![blob("a", 10), blob("new", 30)]),
        ]));
        assert_eq!(
            changed(src, Some(oid(1)), oid(2)),
            vec![("new".into(), None, 30)]
        );
    }

    #[test]
    fn deleted_paths_are_not_reported() {
        let src = Map(HashMap::from([
            (oid(1), vec![blob("a", 10), blob("gone", 40)]),
            (oid(2), vec![blob("a", 10)]),
        ]));
        assert!(changed(src, Some(oid(1)), oid(2)).is_empty());
    }

    #[test]
    fn subtrees_are_reported_and_descended() {
        let src = Map(HashMap::from([
            (oid(1), vec![tree("dir", 50)]),
            (oid(2), vec![tree("dir", 51)]),
            (oid(50), vec![blob("f", 10), blob("keep", 99)]),
            (oid(51), vec![blob("f", 11), blob("keep", 99)]),
        ]));
        // "keep" is identical on both sides, so it is pruned.
        assert_eq!(
            changed(src, Some(oid(1)), oid(2)),
            vec![("dir".into(), Some(50), 51), ("dir/f".into(), Some(10), 11)]
        );
    }

    #[test]
    fn identical_subtree_is_pruned_without_descending() {
        let mut src = Map(HashMap::from([
            (oid(1), vec![tree("dir", 50), blob("x", 10)]),
            (oid(2), vec![tree("dir", 50), blob("x", 11)]),
        ]));
        // oid(50)'s children are deliberately absent: descending would name it
        // unreadable, and pruning means we never look.
        let diff = diff_trees(&mut src, Some(oid(1)), oid(2));
        assert!(diff.is_complete());
        assert_eq!(diff.changes.len(), 1);
        assert_eq!(diff.changes[0].path, b"x".to_vec());
    }

    #[test]
    fn gitlinks_are_skipped_on_both_sides() {
        let src = Map(HashMap::from([
            (oid(1), vec![gitlink("sub", 60)]),
            (oid(2), vec![gitlink("sub", 61), blob("a", 10)]),
        ]));
        assert_eq!(
            changed(src, Some(oid(1)), oid(2)),
            vec![("a".into(), None, 10)]
        );
    }

    #[test]
    fn type_change_drops_the_predecessor() {
        // Nothing to delta against, so it must be reported as new.
        let src = Map(HashMap::from([
            (oid(1), vec![blob("thing", 10)]),
            (oid(2), vec![tree("thing", 50)]),
            (oid(50), vec![blob("inner", 20)]),
        ]));
        assert_eq!(
            changed(src, Some(oid(1)), oid(2)),
            vec![("thing".into(), None, 50), ("thing/inner".into(), None, 20)]
        );
    }

    /// A directory copied to a second path shares one old tree, and the
    /// caller's retry loop treats a second naming as a tree it already fetched.
    #[test]
    fn a_tree_missing_under_two_paths_is_named_once() {
        let src = HashMap::from([
            (oid(1), vec![tree("a", 50), tree("b", 50)]),
            (oid(2), vec![tree("a", 51), tree("b", 52)]),
            (oid(51), vec![blob("f", 10)]),
            (oid(52), vec![blob("f", 11)]),
        ]);
        let diff = diff_trees(&mut Map(src), Some(oid(1)), oid(2));
        assert_eq!(diff.unreadable, vec![oid(50)]);
    }

    #[test]
    fn absent_old_root_makes_everything_new() {
        let src = Map(HashMap::from([
            (oid(2), vec![blob("a", 10), tree("d", 50)]),
            (oid(50), vec![blob("b", 20)]),
        ]));
        assert_eq!(
            changed(src, None, oid(2)),
            vec![
                ("a".into(), None, 10),
                ("d".into(), None, 50),
                ("d/b".into(), None, 20)
            ]
        );
    }

    /// The caller's retry loop: an unreadable tree is named, and the same diff
    /// completes once it has been fetched.
    #[test]
    fn an_unreadable_tree_is_named_and_resolves_on_retry() {
        let complete = HashMap::from([
            (oid(1), vec![tree("dir", 50)]),
            (oid(2), vec![tree("dir", 51)]),
            (oid(50), vec![blob("f", 10)]),
            (oid(51), vec![blob("f", 11)]),
        ]);
        let mut partial = complete.clone();
        partial.remove(&oid(50));
        let first = diff_trees(&mut Map(partial), Some(oid(1)), oid(2));
        assert!(!first.is_complete());
        assert_eq!(first.unreadable, vec![oid(50)]);
        assert_eq!(first.changes.len(), 1);
        assert_eq!(first.changes[0].path, b"dir".to_vec());

        let second = diff_trees(&mut Map(complete), Some(oid(1)), oid(2));
        assert!(second.is_complete());
        assert_eq!(second.changes.len(), 2);
    }

    #[test]
    fn nested_paths_are_joined_from_the_root() {
        let src = Map(HashMap::from([
            (oid(2), vec![tree("a", 50)]),
            (oid(50), vec![tree("b", 51)]),
            (oid(51), vec![blob("c", 20)]),
        ]));
        assert_eq!(
            changed(src, None, oid(2)),
            vec![
                ("a".into(), None, 50),
                ("a/b".into(), None, 51),
                ("a/b/c".into(), None, 20)
            ]
        );
    }

    fn removed(mut src: Map, old: Option<ObjectId>, new: ObjectId) -> Vec<(String, u8)> {
        let diff = diff_trees_with(&mut src, old, new, DiffOptions { removals: true });
        assert!(diff.is_complete(), "unreadable: {:?}", diff.unreadable);
        let mut got = diff.removals;
        got.sort_by(|a, b| a.path.cmp(&b.path));
        got.into_iter()
            .map(|r| (String::from_utf8(r.path).unwrap(), r.old.as_slice()[0]))
            .collect()
    }

    #[test]
    fn a_deleted_blob_is_reported_when_asked_for() {
        let src = Map(HashMap::from([
            (oid(1), vec![blob("a", 10), blob("gone", 40)]),
            (oid(2), vec![blob("a", 10)]),
        ]));
        assert_eq!(
            removed(src, Some(oid(1)), oid(2)),
            vec![("gone".into(), 40)]
        );
    }

    /// A deleted directory is descended, so every path under it is named
    /// rather than only the tree that held them.
    #[test]
    fn a_deleted_tree_names_the_paths_under_it() {
        let src = Map(HashMap::from([
            (oid(1), vec![tree("dir", 50), blob("a", 10)]),
            (oid(2), vec![blob("a", 10)]),
            (oid(50), vec![blob("f", 20), tree("deep", 51)]),
            (oid(51), vec![blob("g", 30)]),
        ]));
        assert_eq!(
            removed(src, Some(oid(1)), oid(2)),
            vec![
                ("dir".into(), 50),
                ("dir/deep".into(), 51),
                ("dir/deep/g".into(), 30),
                ("dir/f".into(), 20)
            ]
        );
    }

    /// The two halves meet here: what stood at the path is a removal, and
    /// what now stands there is new.
    #[test]
    fn a_type_change_is_both_a_removal_and_a_change() {
        let src = HashMap::from([
            (oid(1), vec![tree("thing", 50)]),
            (oid(2), vec![blob("thing", 10)]),
            (oid(50), vec![blob("inner", 20)]),
        ]);
        assert_eq!(
            removed(Map(src.clone()), Some(oid(1)), oid(2)),
            vec![("thing".into(), 50), ("thing/inner".into(), 20)]
        );
        assert_eq!(
            changed(Map(src), Some(oid(1)), oid(2)),
            vec![("thing".into(), None, 10)]
        );
    }

    #[test]
    fn gitlinks_are_skipped_on_the_deleted_side_too() {
        let src = Map(HashMap::from([
            (oid(1), vec![gitlink("sub", 60), blob("a", 10)]),
            (oid(2), vec![blob("a", 10)]),
        ]));
        assert!(removed(src, Some(oid(1)), oid(2)).is_empty());
    }

    #[test]
    fn removals_are_absent_unless_asked_for() {
        let mut src = Map(HashMap::from([
            (oid(1), vec![blob("gone", 40)]),
            (oid(2), vec![blob("a", 10)]),
        ]));
        assert!(
            diff_trees(&mut src, Some(oid(1)), oid(2))
                .removals
                .is_empty()
        );
    }
}
