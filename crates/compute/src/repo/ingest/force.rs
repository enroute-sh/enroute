//! Force flags of ref updates: whether each old value is an ancestor of
//! its new value.

use std::sync::Arc;

use crate::git::object::tag_target;
use crate::git::oid::{Kind, Oid};
use crate::repo::graph::reaches;
use crate::repo::view::{DenseId, Snapshot};
use crate::storage::BitSet;

use super::{Commits, Ctx};

/// Whether each move from `olds[i]` to `targets[i]` is a force; the zero id
/// means none. `None` without `olds`: the history walk is costly and only a
/// hook reads the result.
pub(super) fn forces(
    snap: &Snapshot,
    pushed: Option<(&Ctx, &Commits)>,
    targets: &[Oid],
    olds: Option<&[Oid]>,
) -> Option<Vec<bool>> {
    // A bit per pushed commit, not a set of oids: a large push spills.
    let mut seen = BitSet::new(pushed.map_or(0, |(_, commits)| commits.recs.len()));
    let force = olds?
        .iter()
        .zip(targets)
        .map(|(&old, &new)| match pushed {
            _ if old.is_zero() || new.is_zero() => false,
            Some((cx, commits)) => force_through(cx, commits, &mut seen, old, new),
            None => is_force(snap, old, new),
        })
        .collect();
    Some(force)
}

/// [`is_force`] where `new` may be a pushed commit. Walks back from `new`
/// through the pushed commits to the snapshot commits they build on, then
/// asks the snapshot's graph from there. The first part reads each pushed
/// commit at most once.
fn force_through(cx: &Ctx, commits: &Commits, seen: &mut BitSet, old: Oid, new: Oid) -> bool {
    let snap = &cx.snap;
    if old == new || snap.lookup(&new).is_some() {
        return is_force(snap, old, new);
    }
    let commit = |o| {
        peel_to_commit(
            o,
            |o| match cx.idx.lookup(o) {
                Some(i) => Some(cx.idx.kind(i)),
                None => snap.lookup(o).map(|d| snap.kind(d)),
            },
            |o| cx.content(o).ok(),
        )
    };
    let (Some(old), Some(new)) = (commit(old), commit(new)) else {
        return true;
    };
    let stored = |o: &Oid| snap.lookup_kind(o, Kind::Commit);
    match (walk_pushed(commits, seen, stored, old, new), stored(&old)) {
        (Walk::Reached, _) => false,
        (Walk::Edge(edge), Some(old)) => !reaches(snap, edge, old).unwrap_or(false),
        (Walk::Edge(_), None) => true,
    }
}

/// Whether moving a ref from `old` to `new` is a force (`old` is not an
/// ancestor of `new`), from `snap`'s commit graph alone. Tags are peeled, as
/// `git merge-base --is-ancestor` does. Something it cannot prove a
/// fast-forward is a force, so that a policy against force pushes fails
/// closed.
fn is_force(snap: &Snapshot, old: Oid, new: Oid) -> bool {
    if old == new {
        return false;
    }
    let commit = |o| {
        let c = peel_to_commit(
            o,
            |o| snap.lookup(o).map(|d| snap.kind(d)),
            |o| snap.read_oid(o).ok().flatten().map(|(_, b)| b),
        )?;
        snap.lookup_kind(&c, Kind::Commit)
    };
    let (Some(old), Some(new)) = (commit(old), commit(new)) else {
        return true;
    };
    !reaches(snap, [new], old).unwrap_or(false)
}

/// The commit `oid` names, through annotated tags. `kind` and `read` give an
/// object's kind and content.
fn peel_to_commit(
    mut oid: Oid,
    kind: impl Fn(&Oid) -> Option<Kind>,
    read: impl Fn(&Oid) -> Option<Arc<Vec<u8>>>,
) -> Option<Oid> {
    // A tag chain longer than this is not worth the walk.
    for _ in 0..16 {
        match kind(&oid)? {
            Kind::Commit => return Some(oid),
            Kind::Tag => oid = tag_target(&read(&oid)?).ok()?,
            _ => return None,
        }
    }
    None
}

/// Where a walk back through the pushed commits ends.
#[derive(Debug, PartialEq, Eq)]
enum Walk {
    /// It met `old`.
    Reached,
    /// The stored commits it reached.
    Edge(Vec<DenseId>),
}

/// Walk back from `new` through the pushed `commits` until `old`, stopping at
/// each commit that `stored` finds. `seen` has a bit per pushed commit.
fn walk_pushed(
    commits: &Commits,
    seen: &mut BitSet,
    stored: impl Fn(&Oid) -> Option<DenseId>,
    old: Oid,
    new: Oid,
) -> Walk {
    seen.clear();
    let mut stack = vec![new];
    let mut edge = Vec::new();
    while let Some(o) = stack.pop() {
        if o == old {
            return Walk::Reached;
        }
        if let Some(d) = stored(&o) {
            edge.push(d);
        } else if let Some(p) = commits.find(&o)
            && seen.insert(p)
        {
            commits.with_parents(&commits.rec(p), |ps| stack.extend_from_slice(ps));
        }
    }
    Walk::Edge(edge)
}

#[cfg(test)]
mod tests {
    use super::super::CommitRec;
    use super::*;
    use crate::git::oid::hash_object;
    use crate::storage::{TempTable, Workspace};

    #[test]
    fn walk_reads_spilled_tables() {
        let dir = tempfile::tempdir().unwrap();
        let ws = Workspace::at(dir.path());
        let mut recs = TempTable::with_limit(&ws, 64);
        let mut parents = TempTable::with_limit(&ws, 64);
        let oid = |i: u32| hash_object(Kind::Commit, &i.to_le_bytes());
        // A chain 1..=n on the stored commit 0, with a merge of 1 at the top.
        let n = 5000;
        for i in 1..=n {
            let ps = if i == n {
                vec![oid(i - 1), oid(1)]
            } else {
                vec![oid(i - 1)]
            };
            recs.push(CommitRec {
                oid: oid(i),
                tree: Oid::ZERO,
                time: 0,
                parents_off: u64::try_from(parents.len()).unwrap(),
                nparents: u32::try_from(ps.len()).unwrap(),
                _pad: 0,
            });
            parents.extend_from_slice(&ps);
        }
        recs.sort_by_key(|r| r.oid);
        assert!(recs.spilled() && parents.spilled());
        let commits = Commits { recs, parents };
        let stored = |o: &Oid| (*o == oid(0)).then_some(7);
        // One bitset for both walks, as one push shares it.
        let mut seen = BitSet::new(commits.recs.len());
        assert_eq!(
            walk_pushed(&commits, &mut seen, stored, oid(10), oid(n)),
            Walk::Reached
        );
        let elsewhere = hash_object(Kind::Blob, b"elsewhere");
        assert_eq!(
            walk_pushed(&commits, &mut seen, stored, elsewhere, oid(n)),
            Walk::Edge(vec![7])
        );
    }
}
