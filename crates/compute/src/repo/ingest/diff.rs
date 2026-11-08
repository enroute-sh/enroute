//! One level of a commit's tree diff, and the tree delta it gives.

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::git::delta;
use crate::git::object::{TreeEntry, max_tree_entries, tree_entries_at, tree_order};
use crate::git::oid::{Kind, ObjectFormat, Oid};
use crate::repo::view::must;

use super::encode::delta_size;
use super::{Assign, Ctx};

pub(super) enum Slot {
    Member(Oid),
    /// A delta base for a new object, and for a tree, the delta against it
    /// that the diff found.
    Assign(Oid, Assign, Option<Vec<u8>>),
    Child(usize),
}

/// The result of diffing one frame.
pub(super) enum Diffed {
    Done(Vec<(Oid, Vec<Option<Oid>>)>, Vec<Slot>),
    Missing(Vec<crate::storage::NotResident>),
}

/// The first parent version in `cands`, with its position.
fn pick_base(cands: &[Option<Oid>]) -> Option<(Oid, usize)> {
    cands
        .iter()
        .enumerate()
        .find_map(|(i, c)| c.map(|o| (o, i)))
}

/// Output the member `oid` (`versions`: its parents' versions at its path),
/// and, if it is new and a parent has a version, that version as its
/// delta base.
fn push_member(
    cx: &Ctx,
    out: &mut Vec<Slot>,
    oid: Oid,
    versions: &[Option<Oid>],
    parent_commits: &[Oid],
    delta: impl FnOnce(usize) -> Option<Vec<u8>>,
) {
    out.push(Slot::Member(oid));
    if cx.new_entry(&oid).is_some()
        && let Some((b, i)) = pick_base(versions)
    {
        let ctx = *must(parent_commits.get(i), "a commit per parent");
        out.push(Slot::Assign(oid, Assign { base: b, ctx }, delta(i)));
    }
}

/// A tree's entries and the bytes each takes.
type Entries<'a> = Vec<(TreeEntry<'a>, std::ops::Range<usize>)>;

/// The entries of `data`, a tree of a `format` repository.
fn entries_of(format: ObjectFormat, data: &[u8]) -> Result<Entries<'_>> {
    // Sized up front: a tree's entries are read for every version of it.
    let mut v = Vec::with_capacity(max_tree_entries(format, data.len()));
    for e in tree_entries_at(format, data) {
        v.push(e?);
    }
    Ok(v)
}

/// For each entry of `tree`, the entry of `parent` with its name and kind,
/// found by a merge in git's tree order.
fn matches(tree: &Entries<'_>, parent: &Entries<'_>) -> Vec<Option<usize>> {
    let mut j = 0;
    tree.iter()
        .map(|(e, _)| {
            while parent.get(j).is_some_and(|(p, _)| tree_order(p, e).is_lt()) {
                j += 1;
            }
            parent
                .get(j)
                .filter(|(p, _)| tree_order(p, e).is_eq())
                .map(|_| j)
        })
        .collect()
}

/// `target` as a delta against `base`, from the matches the diff found: an
/// entry that is in the base whole is a copy, and one whose id alone
/// changed copies its mode and name. `None` if the delta is not under half
/// the tree, the same bar as for other objects.
fn tree_delta(
    format: ObjectFormat,
    (base, base_entries): (&[u8], &Entries<'_>),
    (target, entries): (&[u8], &Entries<'_>),
    found: &[Option<usize>],
) -> Option<Vec<u8>> {
    if !delta_size(target.len()) {
        return None;
    }
    let n = format.len();
    let mut w = delta::Writer::new(base.len(), target.len(), target.len() / 2)?;
    for ((_, at), j) in entries.iter().zip(found) {
        let bytes = target.get(at.clone())?;
        let (head, oid) = bytes.split_at(bytes.len() - n);
        match j.and_then(|j| base_entries.get(j)) {
            Some((_, b)) if base.get(b.start..b.end - n) == Some(head) => {
                w.copy(b.start, head.len());
                if base.get(b.end - n..b.end) == Some(oid) {
                    w.copy(b.end - n, n);
                } else {
                    w.insert(oid);
                }
            }
            _ => w.insert(bytes),
        }
        if w.full() {
            return None;
        }
    }
    w.finish()
}

/// One level of the path diff of `tree` against the parents' trees at the
/// same path: the slots it outputs (subtrees that differ become children,
/// listed with their parents' versions), or the bytes it is missing.
/// Every object that differs from all parents is a member (identical
/// subtrees are pruned); the first parent's version at the same path is
/// proposed as its delta base.
pub(super) fn diff_level(
    cx: &Ctx,
    tree: Oid,
    parents: &[Option<Oid>],
    parent_commits: &[Oid],
) -> Result<Diffed> {
    // Everything this level reads; what is not local is loaded first.
    let mut need = Vec::new();
    let mut read = |oid: &Oid| -> Result<Option<Arc<Vec<u8>>>> {
        match cx.content(oid) {
            Ok(c) => Ok(Some(c)),
            Err(e) => {
                let d = cx.snap.lookup(oid).context("object missing")?;
                need.extend(cx.snap.missing_after(d, e)?);
                Ok(None)
            }
        }
    };
    let data = read(&tree)?;
    let parent_data: Vec<Option<Arc<Vec<u8>>>> = parents
        .iter()
        .map(|p| p.map(|p| read(&p)).transpose().map(Option::flatten))
        .collect::<Result<_>>()?;
    if !need.is_empty() {
        return Ok(Diffed::Missing(need));
    }
    let data = must(data, "tree was read");
    let format = cx.snap.format();
    let entries = entries_of(format, &data)?;
    let parent_entries = parent_data
        .iter()
        .map(|pd| {
            pd.as_deref()
                .map_or_else(|| Ok(Vec::new()), |pd| entries_of(format, pd))
        })
        .collect::<Result<Vec<Entries<'_>>>>()?;
    let parent_matches: Vec<Vec<Option<usize>>> = parent_entries
        .iter()
        .map(|p| matches(&entries, p))
        .collect();
    let mut out = Vec::new();
    push_member(cx, &mut out, tree, parents, parent_commits, |p| {
        let base = parent_data.get(p)?.as_deref()?;
        let b = parent_entries.get(p)?;
        tree_delta(format, (base, b), (&data, &entries), parent_matches.get(p)?)
    });
    let mut children = Vec::new();
    for (n, (e, _)) in entries.iter().enumerate() {
        let Some(kind) = e.kind() else { continue };
        let at_parents: Vec<Option<Oid>> = parent_entries
            .iter()
            .zip(&parent_matches)
            .map(|(p, m)| {
                let (pe, _) = p.get((*m.get(n)?)?)?;
                (pe.kind() == Some(kind)).then_some(pe.oid)
            })
            .collect();
        if at_parents.contains(&Some(e.oid)) {
            continue;
        }
        if kind == Kind::Tree {
            out.push(Slot::Child(0));
            children.push((e.oid, at_parents));
        } else {
            push_member(cx, &mut out, e.oid, &at_parents, parent_commits, |_| None);
        }
    }
    Ok(Diffed::Done(children, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MODES: [crate::git::object::EntryMode; 5] = {
        use crate::git::object::EntryMode;
        [
            EntryMode::FILE,
            EntryMode::EXECUTABLE,
            EntryMode::SYMLINK,
            EntryMode::TREE,
            EntryMode::GITLINK,
        ]
    };

    /// A tree of (name, (mode in [`MODES`], id byte)); a mode past the
    /// end leaves the name out.
    fn tree_of(rows: &[(String, (usize, u8))]) -> Vec<u8> {
        crate::git::object::write_tree(rows.iter().filter_map(|(name, (m, n))| {
            Some(TreeEntry {
                mode: *MODES.get(*m)?,
                name: name.as_bytes(),
                oid: Oid::sha1([*n; 20]),
            })
        }))
        .unwrap()
    }

    /// Pairs of trees that share most entries, over names that test git's
    /// order (`a`, `a-`, `a.c` and a directory `a` sort apart).
    fn any_tree_pair() -> impl proptest::strategy::Strategy<Value = (Vec<u8>, Vec<u8>)> {
        use proptest::prelude::*;
        let name = proptest::string::string_regex("[ab][-.ab]{0,3}").unwrap();
        // Per name: the base's entry, and the target's if it differs.
        let row = (
            (0..MODES.len(), 0..3u8),
            proptest::option::weighted(0.3, (0..=MODES.len(), 0..3u8)),
        );
        proptest::collection::btree_map(name, row, 0..40).prop_map(|rows| {
            let (base, target): (Vec<_>, Vec<_>) = rows
                .into_iter()
                .map(|(name, (b, t))| ((name.clone(), b), (name, t.unwrap_or(b))))
                .unzip();
            (tree_of(&base), tree_of(&target))
        })
    }

    proptest::proptest! {
        /// The merge finds the entries a lookup by name and kind finds, and
        /// the delta from the diff rebuilds the tree.
        #[test]
        fn a_tree_diff_gives_its_delta((base, target) in any_tree_pair()) {
            use crate::git::object::EntryMode;
            let entries = |t| {
                tree_entries_at(ObjectFormat::Sha1, t)
                    .collect::<Result<Entries<'_>>>()
                    .unwrap()
            };
            let (b, t) = (entries(&base), entries(&target));
            let found = matches(&t, &b);
            let dir = |e: &TreeEntry<'_>| e.mode == EntryMode::TREE;
            for ((e, _), j) in t.iter().zip(&found) {
                let by_name = b.iter().position(|(p, _)| p.name == e.name && dir(p) == dir(e));
                proptest::prop_assert_eq!(*j, by_name);
            }
            if let Some(delta) = tree_delta(ObjectFormat::Sha1, (&base, &b), (&target, &t), &found) {
                proptest::prop_assert_eq!(delta::target_size(&delta).unwrap(), target.len());
                proptest::prop_assert_eq!(delta::apply(&base, &delta).unwrap(), target);
            } else {
                proptest::prop_assert!(base != target || !delta_size(target.len()));
            }
        }
    }
}
