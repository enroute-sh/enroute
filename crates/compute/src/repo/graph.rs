//! Walks over a snapshot's commit graph: ancestry, merge bases and
//! first-parent history.

use std::collections::BinaryHeap;

use anyhow::{Context, Result};
use foldhash::{HashMap, HashSet, HashSetExt};

use super::view::{CommitLinks, CommitNum, DenseId, Snapshot};

/// The walk links of a commit the snapshot holds.
fn node(snap: &Snapshot, d: DenseId) -> Result<CommitLinks<'_>> {
    snap.links_of(d)
        .with_context(|| format!("commit {} has no graph node", snap.oid(d)))
}

/// The walk links of commit number `c`.
fn node_num(snap: &Snapshot, c: CommitNum) -> Result<CommitLinks<'_>> {
    snap.links(c)
        .with_context(|| format!("commit number {c} has no graph node"))
}

/// Whether `target` is one of `from` or an ancestor of one.
pub(crate) fn reaches(
    snap: &Snapshot,
    from: impl IntoIterator<Item = DenseId>,
    target: DenseId,
) -> Result<bool> {
    let target = node(snap, target)?;
    let mut seen: HashSet<CommitNum> = HashSet::new();
    let mut stack = Vec::new();
    for d in from {
        let c = node(snap, d)?.num;
        if seen.insert(c) {
            stack.push(c);
        }
    }
    if stack.contains(&target.num) {
        return Ok(true);
    }
    // Generations fall strictly along parent edges, so nothing at or below
    // the target's generation (other than the target) leads to it.
    let floor = target.generation;
    while let Some(c) = stack.pop() {
        for &p in node_num(snap, c)?.parents {
            if p == target.num {
                return Ok(true);
            }
            if seen.insert(p) && node_num(snap, p)?.generation > floor {
                stack.push(p);
            }
        }
    }
    Ok(false)
}

const FROM_A: u8 = 1;
const FROM_B: u8 = 2;
const STALE: u8 = 4;

/// Git's `paint_down_to_common`, highest generation first: every commit is
/// taken after all of its descendants, so its marks are final when it is.
///
/// The result therefore needs no reduction: a common commit below another
/// one is stale when it is taken.
pub(crate) fn paint_down_to_common(
    snap: &Snapshot,
    a: DenseId,
    b: DenseId,
    budget: usize,
) -> Result<(Vec<DenseId>, bool)> {
    if a == b {
        return Ok((vec![a], false));
    }
    let (na, nb) = (node(snap, a)?, node(snap, b)?);
    let mut marks: HashMap<CommitNum, u8> =
        HashMap::from_iter([(na.num, FROM_A), (nb.num, FROM_B)]);
    let mut queue: BinaryHeap<(u32, CommitNum)> = BinaryHeap::new();
    let mut queued: HashSet<CommitNum> = HashSet::from_iter([na.num, nb.num]);
    queue.push((na.generation, na.num));
    queue.push((nb.generation, nb.num));
    // Queued commits not yet marked stale: the walk ends when none are left.
    let mut live = 2usize;
    let mut found = Vec::new();
    let mut taken = 0usize;
    while live > 0 {
        let Some((_, c)) = queue.pop() else { break };
        queued.remove(&c);
        let mut m = marks.get(&c).copied().unwrap_or_default();
        if m & STALE == 0 {
            live -= 1;
        }
        taken += 1;
        if taken > budget {
            return Ok((found, true));
        }
        let node = node_num(snap, c)?;
        if m == FROM_A | FROM_B {
            found.push(node.id);
            m |= STALE;
        }
        for &p in node.parents {
            let pm = marks.entry(p).or_default();
            if *pm & m == m {
                continue;
            }
            let was_stale = *pm & STALE != 0;
            *pm |= m;
            let stale = *pm & STALE != 0;
            if queued.insert(p) {
                queue.push((node_num(snap, p)?.generation, p));
                live += usize::from(!stale);
            } else if stale && !was_stale {
                live -= 1;
            }
        }
    }
    Ok((found, false))
}

/// `tip`, then its first parent, and so on: at most `limit` commits.
pub(crate) fn first_parents(snap: &Snapshot, tip: DenseId, limit: usize) -> Result<Vec<DenseId>> {
    let mut page = Vec::new();
    let mut at = Some(node(snap, tip)?);
    while let Some(c) = at {
        if page.len() >= limit {
            break;
        }
        page.push(c.id);
        at = c.parents.first().map(|&p| node_num(snap, p)).transpose()?;
    }
    Ok(page)
}
