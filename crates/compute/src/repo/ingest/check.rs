//! What indexing collects of each object, and the checks and commit order
//! built from it.

use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use parking_lot::Mutex;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::git::object::references;
use crate::git::oid::{Kind, Oid};
use crate::repo::packindex::{ContentCache, PackIndex};
use crate::repo::view::{Snapshot, must};
use crate::storage::{BitSet, TempTable, Workspace};
use crate::util::{ix, to_u64};

use super::force::forces;
use super::{CommitRec, Commits, Ctx, Ingested};

/// A reference from a pack object: the target and the kind the referrer
/// says it has.
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct RefRec {
    oid: Oid,
    kind: u8,
}

/// Recently seen references, direct-mapped: consecutive versions of a tree
/// repeat most of their entries, so this drops most duplicates before they
/// reach the (sorted, deduplicated later) reference list.
pub(super) const REF_FILTER_SLOTS: usize = 1 << 18;

/// What the pack's visitor collects while indexing.
pub(super) struct Collected {
    refs: TempTable<RefRec>,
    filter: Vec<Option<RefRec>>,
    commits: TempTable<CommitRec>,
    parents: TempTable<Oid>,
}

impl Collected {
    pub(super) fn new(ws: &Workspace, filter_slots: usize) -> Collected {
        Collected {
            refs: TempTable::new(ws),
            filter: vec![None; filter_slots],
            commits: TempTable::new(ws),
            parents: TempTable::new(ws),
        }
    }
}

pub(super) fn visitor(
    collected: &Mutex<Collected>,
) -> impl Fn(&Oid, Kind, &[u8]) -> Result<()> + Sync + '_ {
    move |oid, kind, data| {
        // The id was hashed in the format the object's references use.
        let (refs, commit) = references(oid.format(), kind, data)
            .with_context(|| format!("invalid {} {oid}", kind.name()))?;
        let mut c = collected.lock();
        let c = &mut *c;
        let slots = c.filter.len();
        for (k, o) in refs {
            let r = RefRec {
                oid: o,
                kind: k.code(),
            };
            let slot = must(
                c.filter.get_mut(filter_slot(&o, slots)),
                "reference filter slot",
            );
            if slot.is_some_and(|s| s.oid == o && s.kind == r.kind) {
                continue;
            }
            *slot = Some(r);
            c.refs.push(r);
        }
        if let Some(info) = commit {
            let parents_off = to_u64(c.parents.len());
            c.parents.extend_from_slice(&info.parents);
            c.commits.push(CommitRec {
                oid: *oid,
                tree: info.tree,
                time: info.committer.seconds,
                parents_off,
                nparents: u32::try_from(info.parents.len())
                    .context("commit has too many parents")?,
                _pad: 0,
            });
        }
        Ok(())
    }
}

/// The slot of `oid` in a reference filter of `slots` slots.
fn filter_slot(oid: &Oid, slots: usize) -> usize {
    let [a, b, c, d, ..] = *oid.prefix();
    ix(u32::from_le_bytes([a, b, c, d]))
        .checked_rem(slots)
        .unwrap_or(0)
}

/// An indexed pack with new objects, ready for segments.
pub(super) struct Indexed {
    pub(super) cx: Arc<Ctx>,
    pub(super) commits: Commits,
    /// New commits in topological order (positions in `commits`).
    pub(super) order: Vec<u32>,
    pub(super) ing: Ingested,
}

/// Connectivity checks, the set of new objects and the commits' order; the
/// pack's kinds of the `targets` and their force flags go into the result.
/// `Err` holds the result if nothing is new.
pub(super) fn check_and_order(
    snap: Arc<Snapshot>,
    idx: PackIndex,
    cache: ContentCache,
    collected: Collected,
    targets: &[Oid],
    olds: Option<&[Oid]>,
) -> Result<Result<Indexed, Ingested>> {
    let Collected {
        mut refs,
        filter,
        commits: mut commit_recs,
        parents,
    } = collected;
    drop(filter);
    let mut ing = Ingested::empty();
    for t in targets {
        if let Some(i) = idx.lookup(t) {
            ing.kinds.insert(*t, idx.kind(i));
        }
    }
    // Connectivity: everything referenced exists with the expected kind,
    // and nothing is referenced as two different kinds.
    refs.sort_by_key(|a| (a.oid, a.kind));
    let mut prev: Option<RefRec> = None;
    for r in refs.iter() {
        if let Some(p) = prev
            && p.oid == r.oid
        {
            if p.kind != r.kind {
                bail!(
                    "object {} referenced as both {} and {}",
                    r.oid,
                    kind_name(p.kind),
                    kind_name(r.kind)
                );
            }
            continue;
        }
        prev = Some(r);
        let actual = match idx.lookup(&r.oid) {
            Some(i) => Some(idx.kind(i)),
            None => snap.lookup(&r.oid).map(|d| snap.kind(d)),
        };
        match actual {
            Some(a) if a.code() == r.kind => {}
            Some(a) => bail!(
                "object {} is a {}, expected {}",
                r.oid,
                a.name(),
                kind_name(r.kind)
            ),
            None => bail!("missing object {} ({})", r.oid, kind_name(r.kind)),
        }
    }
    drop(refs);

    let mut new = BitSet::new(idx.len());
    let mut any_new = false;
    for i in 0..idx.len() {
        let o = idx.oid(i);
        if idx.lookup(&o) == Some(i) && snap.lookup(&o).is_none() {
            new.insert(i);
            any_new = true;
        }
    }
    if !any_new {
        ing.force = forces(&snap, None, targets, olds);
        return Ok(Err(ing));
    }
    commit_recs.sort_by_key(|a| a.oid);
    // A commit may appear more than once in a pack: keep one record.
    let mut kept = 0;
    for i in 0..commit_recs.len() {
        let r = commit_recs.get(i);
        if kept == 0 || commit_recs.get(kept - 1).oid != r.oid {
            commit_recs.set(kept, r);
            kept += 1;
        }
    }
    commit_recs.truncate(kept);
    let commits = Commits {
        recs: commit_recs,
        parents,
    };
    let cx = Arc::new(Ctx {
        snap,
        idx,
        cache,
        new,
    });
    let order = topo_order(&cx, &commits)?;
    ing.force = forces(&cx.snap, Some((&cx, &commits)), targets, olds);
    Ok(Ok(Indexed {
        cx,
        commits,
        order,
        ing,
    }))
}

fn kind_name(code: u8) -> &'static str {
    Kind::from_code(code).map_or("?", Kind::name)
}

/// A mutable element of `v`.
fn at(v: &mut [u32], i: usize) -> &mut u32 {
    must(v.get_mut(i), "commit index in range")
}

/// New commits in topological order (parents first), as positions in
/// `commits`. Ties are broken by pack order, as git's own order would.
fn topo_order(cx: &Ctx, commits: &Commits) -> Result<Vec<u32>> {
    // (pack entry, position in `commits`) of each new commit, in pack order.
    let mut new_commits: Vec<(u32, u32)> = Vec::new();
    for p in 0..commits.recs.len() {
        if let Some(i) = cx.new_entry(&commits.rec(p).oid) {
            new_commits.push((
                u32::try_from(i).context("pack has too many objects")?,
                u32::try_from(p).context("pack has too many commits")?,
            ));
        }
    }
    new_commits.sort_unstable();
    // Ordinal (index into new_commits) of each position in `commits`.
    let mut ordinal = vec![u32::MAX; commits.recs.len()];
    for (o, &(_, p)) in new_commits.iter().enumerate() {
        *at(&mut ordinal, ix(p)) = u32::try_from(o).context("pack has too many commits")?;
    }
    let ord_of = |oid: &Oid| {
        commits
            .find(oid)
            .and_then(|p| ordinal.get(p).copied())
            .filter(|&o| o != u32::MAX)
    };
    // (child ordinal, parent ordinal) of each edge among the new commits.
    let edges = || {
        new_commits.iter().enumerate().flat_map(|(o, &(_, p))| {
            let ord_of = &ord_of;
            commits
                .parents(&commits.rec(ix(p)))
                .into_iter()
                .filter_map(move |par| ord_of(&par).map(|po| (o, ix(po))))
        })
    };
    // Children as compressed adjacency lists, in new_commits order.
    let n = new_commits.len();
    let mut indeg = vec![0u32; n];
    let mut starts = vec![0u32; n + 1];
    for (o, po) in edges() {
        *at(&mut indeg, o) += 1;
        *at(&mut starts, po + 1) += 1;
    }
    let mut total = 0u32;
    for c in &mut starts {
        total += *c;
        *c = total;
    }
    let mut fill = starts.clone();
    let mut children = vec![0u32; ix(total)];
    for (o, po) in edges() {
        let next = at(&mut fill, po);
        *at(&mut children, ix(*next)) = u32::try_from(o).context("pack has too many commits")?;
        *next += 1;
    }
    drop(fill);
    let n32 = u32::try_from(n).context("pack has too many commits")?;
    let mut ready: Vec<u32> = (0..n32)
        .rev()
        .filter(|&o| indeg.get(ix(o)) == Some(&0))
        .collect();
    let mut out = Vec::with_capacity(n);
    while let Some(o) = ready.pop() {
        let &(_, pos) = must(new_commits.get(ix(o)), "commit index in range");
        out.push(pos);
        let from = *must(starts.get(ix(o)), "commit index in range");
        let to = *must(starts.get(ix(o) + 1), "commit index in range");
        let kids = must(children.get(ix(from)..ix(to)), "commit index in range");
        for &k in kids.iter().rev() {
            let d = at(&mut indeg, ix(k));
            *d -= 1;
            if *d == 0 {
                ready.push(k);
            }
        }
    }
    ensure!(out.len() == n, "commit graph contains a cycle");
    Ok(out)
}
