//! Push ingestion: validate an incoming pack and turn it into an L0 layer.
//!
//! Validation needs every object resolved and hashed anyway, so ingestion
//! piggybacks on it:
//!
//! 1. index the pack (thin bases come from the current snapshot),
//! 2. fsck-lite + connectivity: every reference resolves, with the right kind,
//! 3. per new commit, path-diff its tree against its parents' trees. The
//!    changed objects form the commit's *segment*; each changed object gets
//!    the parent's version at the same path as its delta base, with that
//!    parent as the *base context* (guaranteeing base availability when
//!    serving),
//! 4. encode objects (`REF_DELTA` vs. the parent version, or full) and write
//!    the layer.

use std::sync::Arc;

use anyhow::{Context, Result};
use foldhash::{HashMap, HashMapExt};
use parking_lot::Mutex;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::git::io::{Incoming, ReadAt};
use crate::git::oid::{Kind, ObjectFormat, Oid};
use crate::git::pack::PackReader;
use crate::storage::Sealed;
use crate::storage::{BitSet, TempTable, Workspace};
use crate::util::{blocking, ix, to_usize};

use super::machine;
use super::packindex::{ContentCache, PackIndex, Scanned};
use super::view::{Snapshot, must};

mod check;
mod diff;
mod encode;
mod force;
mod segments;

use check::{Collected, Indexed, REF_FILTER_SLOTS, check_and_order, visitor};
use encode::{base_entries, encode};
use force::forces;
use segments::Segments;

pub(crate) struct Ingested {
    /// Built layer and its object count, if the pack had new objects.
    pub layer: Option<(Sealed, u32)>,
    /// Kinds of the requested targets (e.g. ref update values) found in the
    /// pack.
    pub kinds: HashMap<Oid, Kind>,
    /// Per target given to [`ingest`]: whether its move is a force, as
    /// [`HookCommand::force`](crate::hooks::HookCommand::force) defines it.
    /// `None` if the caller gave no old values.
    pub force: Option<Vec<bool>>,
}

impl Ingested {
    pub(crate) fn empty() -> Self {
        Ingested {
            layer: None,
            kinds: HashMap::new(),
            force: None,
        }
    }

    /// Nothing ingested: `snap` holds every object `targets` names.
    pub(crate) fn unpacked(snap: &Snapshot, targets: &[Oid], olds: Option<&[Oid]>) -> Self {
        Ingested {
            force: forces(snap, None, targets, olds),
            ..Ingested::empty()
        }
    }
}

/// Kind of `oid` in the pack or the snapshot.
pub(crate) fn kind_of(snap: &Snapshot, ing: &Ingested, oid: &Oid) -> Option<Kind> {
    ing.kinds
        .get(oid)
        .copied()
        .or_else(|| snap.lookup(oid).map(|d| snap.kind(d)))
}

/// What the phases after indexing share.
struct Ctx {
    snap: Arc<Snapshot>,
    idx: PackIndex,
    cache: ContentCache,
    /// Pack entries holding new objects (first copy of an oid the snapshot
    /// lacks).
    new: BitSet,
}

impl Ctx {
    fn external(&self, oid: &Oid) -> Result<Option<(Kind, Arc<Vec<u8>>)>> {
        self.snap.read_oid(oid)
    }

    fn content(&self, oid: &Oid) -> Result<Arc<Vec<u8>>> {
        if let Some(i) = self.idx.lookup(oid) {
            return self.idx.content(i, &|o| self.external(o), &self.cache);
        }
        let d = self
            .snap
            .lookup(oid)
            .with_context(|| format!("object {oid} missing"))?;
        self.snap.read(d)
    }

    /// Pack entry of `oid` if it is a new object.
    fn new_entry(&self, oid: &Oid) -> Option<usize> {
        self.idx.lookup(oid).filter(|&i| self.new.contains(i))
    }

    /// Root tree of commit `oid`: from the pack, else the commit graph.
    fn commit_tree(&self, commits: &Commits, oid: &Oid) -> Result<Oid> {
        if let Some(p) = commits.find(oid) {
            return Ok(commits.rec(p).tree);
        }
        let d = self
            .snap
            .lookup_kind(oid, Kind::Commit)
            .with_context(|| format!("commit {oid} missing"))?;
        let tree = self
            .snap
            .commit(d)
            .context("commit missing from graph")?
            .tree;
        Ok(self.snap.oid(tree))
    }
}

/// Decoded-object cache for resolving the incoming pack's deltas and
/// reading delta bases (git's index-pack uses 96 MiB for the same job).
const INGEST_CACHE: usize = 128 << 20;

/// A commit in the pack, as a fixed-size record; parents live in a
/// separate array.
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct CommitRec {
    oid: Oid,
    tree: Oid,
    time: i64,
    parents_off: u64,
    nparents: u32,
    _pad: u32,
}

/// The pack's commits, sorted by oid.
struct Commits {
    recs: TempTable<CommitRec>,
    parents: TempTable<Oid>,
}

impl Commits {
    fn find(&self, oid: &Oid) -> Option<usize> {
        let i = self.recs.partition_point(|r| r.oid < *oid);
        (i < self.recs.len() && self.recs.get(i).oid == *oid).then_some(i)
    }

    fn rec(&self, i: usize) -> CommitRec {
        self.recs.get(i)
    }

    fn parents(&self, r: &CommitRec) -> Vec<Oid> {
        self.parents.range(Self::parents_range(r))
    }

    /// `f` of `r`'s parents, borrowed when they lie in one page.
    fn with_parents<T>(&self, r: &CommitRec, f: impl FnOnce(&[Oid]) -> T) -> T {
        self.parents.with_slice(Self::parents_range(r), f)
    }

    fn parents_range(r: &CommitRec) -> std::ops::Range<usize> {
        let start = to_usize(r.parents_off);
        start..start.saturating_add(ix(r.nparents))
    }
}

/// Delta base and base context assigned to a new object (zero = none).
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct Assign {
    base: Oid,
    ctx: Oid,
}

/// What ingest needs of a pack's sequential pass.
pub(crate) struct Pass1 {
    scanned: Scanned,
    collected: Collected,
}

/// The sequential pass over the pack `inc` delivers, to a `format`
/// repository. It needs no snapshot, so a push runs it while the pack
/// arrives. An object over `max_object` bytes fails it.
pub(crate) fn pass1(
    inc: &mut Incoming,
    format: ObjectFormat,
    ws: &Workspace,
    max_object: u64,
) -> Result<Pass1> {
    let reader = PackReader::new(inc, format, max_object)?;
    let filter_slots = (ix(reader.count()) * 4)
        .clamp(64, REF_FILTER_SLOTS)
        .next_power_of_two();
    let c = Mutex::new(Collected::new(ws, filter_slots));
    let scanned = PackIndex::scan(reader, &visitor(&c), ws)?;
    Ok(Pass1 {
        scanned,
        collected: c.into_inner(),
    })
}

/// [`ingest`] of a stored pack, which runs its sequential pass first.
pub(crate) async fn ingest_stored(
    snap: Arc<Snapshot>,
    pack: Arc<dyn ReadAt>,
    ws: Workspace,
    targets: Vec<Oid>,
    olds: Option<Vec<Oid>>,
    max_object: u64,
) -> Result<Ingested> {
    let scanned = {
        let (pack, ws) = (pack.clone(), ws.clone());
        let format = snap.format();
        blocking(move || pass1(&mut Incoming::of(pack), format, &ws, max_object)).await?
    };
    ingest(snap, pack, ws, targets, olds, scanned).await
}

/// Ingest `pack` against `snap`. The phases run on the blocking pool and
/// the CPU pool; what they need of `snap` that is not local (thin-pack
/// bases, parent trees, delta bases) is loaded between and during them,
/// asynchronously (see [`machine`](super::machine)): no phase waits on a
/// remote tier. `targets` are the new values of the push's ref updates, and
/// `olds`, if given, their old values, for the force flags. The zero id
/// means none. `scanned` is the pack's sequential pass ([`pass1`]).
pub(crate) async fn ingest(
    snap: Arc<Snapshot>,
    pack: Arc<dyn ReadAt>,
    ws: Workspace,
    targets: Vec<Oid>,
    olds: Option<Vec<Oid>>,
    scanned: Pass1,
) -> Result<Ingested> {
    let t0 = std::time::Instant::now();

    // 1. The pack's REF_DELTA bases in the snapshot (a thin pack's) are
    //    loaded before deltas are resolved.
    let s = snap.clone();
    let (scanned, external, collected) = blocking(move || {
        let Pass1 { scanned, collected } = scanned;
        let mut external: Vec<_> = scanned.ref_bases().filter_map(|o| s.lookup(&o)).collect();
        external.sort_unstable();
        external.dedup();
        Ok((scanned, external, collected))
    })
    .await?;
    // Pinned through resolution, which reads them synchronously.
    let pins = machine::load_objects(&snap, external).await?;

    // 2. Resolve, check connectivity, find what is new, order commits.
    let (s, w) = (snap.clone(), ws.clone());
    let indexed = blocking(move || -> Result<Result<Indexed, Ingested>> {
        let cache = crate::util::byte_cache(INGEST_CACHE);
        let external = |oid: &Oid| s.read_oid(oid);
        let c = Mutex::new(collected);
        let idx = scanned.resolve(pack, &external, &cache, &visitor(&c), &w)?;
        check_and_order(s, idx, cache, c.into_inner(), &targets, olds.as_deref())
    })
    .await?;
    drop(pins);
    let t_index = t0.elapsed();
    let mut st = match indexed {
        Ok(st) => st,
        Err(nothing_new) => return Ok(nothing_new),
    };

    // 3. Segments and delta base assignment, a window of commits at a time.
    let cx = st.cx.clone();
    let order = std::mem::take(&mut st.order);
    let diffs = Segments::new(cx.clone(), st.commits, order, &ws);
    let mut segs = machine::drive(diffs).await?;
    // Objects not introduced by any new commit (tags, stray objects), in
    // pack order.
    let c = cx.clone();
    let segs = blocking(move || {
        for i in 0..c.idx.len() {
            if c.new.contains(i) && !segs.in_order.contains(i) {
                segs.member_order
                    .push(u32::try_from(i).context("pack has too many objects")?);
            }
        }
        Ok(segs)
    })
    .await?;
    let t_segments = t0.elapsed();

    // 4. Delta base entries, and the bases in the snapshot, loaded before
    // encoding.
    let (c, assign, w) = (cx.clone(), segs.assign, ws.clone());
    let (assign, base_of, bases) = blocking(move || {
        let (base_of, bases) = base_entries(&c, &assign, &w)?;
        Ok((assign, base_of, bases))
    })
    .await?;
    let pins = machine::load_objects(&snap, bases).await?;

    // 5. Encode and write the layer.
    let (builder, member_order, tree_deltas) = (segs.builder, segs.member_order, segs.tree_deltas);
    let mut ing = st.ing;
    let w = ws.clone();
    let (data, count, t_encode) = blocking(move || {
        let (data, count) = encode(
            &cx,
            &member_order,
            &assign,
            &tree_deltas,
            &base_of,
            builder,
            &w,
        )?;
        Ok((data, count, t0.elapsed()))
    })
    .await?;
    drop(pins);
    let ms = |d: std::time::Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let since = |later: std::time::Duration, earlier: std::time::Duration| {
        must(later.checked_sub(earlier), "monotonic clock")
    };
    tracing::debug!(
        objects = count,
        index_ms = ms(t_index),
        segments_ms = ms(since(t_segments, t_index)),
        encode_ms = ms(since(t_encode, t_segments)),
        "ingested pack"
    );
    ing.layer = Some((data, count));
    Ok(ing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::io::Owned;
    use crate::git::oid::hash_object;
    use crate::git::pack::full_entry;
    use crate::git::pack::pack_of;
    use crate::repo::Repos;
    use crate::storage::Storage;

    fn repos() -> (tempfile::TempDir, Arc<Repos>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::for_test(dir.path());
        (dir, Repos::new(storage, 16 << 20))
    }

    fn commit(tree: Oid, parents: &[Oid], msg: &str) -> Vec<u8> {
        use std::fmt::Write;
        let mut s = format!("tree {tree}\n");
        for p in parents {
            writeln!(s, "parent {p}").unwrap();
        }
        s.push_str("author A <a@x> 1 +0000\ncommitter A <a@x> 1 +0000\n\n");
        s.push_str(msg);
        s.into_bytes()
    }

    #[tokio::test]
    async fn force_flags_of_pushed_commits() {
        let (_dir, repos) = repos();
        let tree = hash_object(Kind::Tree, b"");
        let mut objects = vec![(Kind::Tree, Vec::new())];
        let mut add = |kind, body: Vec<u8>| {
            let oid = hash_object(kind, &body);
            objects.push((kind, body));
            oid
        };
        // root <- b1 <- b2, root <- side, and merge of b2 and side.
        let root = add(Kind::Commit, commit(tree, &[], "root"));
        let b1 = add(Kind::Commit, commit(tree, &[root], "b1"));
        let b2 = add(Kind::Commit, commit(tree, &[b1], "b2"));
        let side = add(Kind::Commit, commit(tree, &[root], "side"));
        let merge = add(Kind::Commit, commit(tree, &[b2, side], "merge"));
        let tag = add(
            Kind::Tag,
            format!("object {b2}\ntype commit\ntag v1\ntagger A <a@x> 1 +0000\n\nv1\n")
                .into_bytes(),
        );
        let unknown = hash_object(Kind::Blob, b"nowhere");
        let none = Oid::ZERO;
        let cases = [
            ((root, b2), false),
            ((b2, side), true),
            ((side, merge), false),
            ((b2, merge), false),
            ((merge, b2), true),
            ((none, b2), false),
            ((b2, none), false),
            ((merge, merge), false),
            // Peeled, as git does.
            ((root, tag), false),
            ((side, tag), true),
            // Fail closed.
            ((root, tree), true),
            ((unknown, b2), true),
        ];
        let entries: Vec<_> = objects.iter().map(|(k, b)| full_entry(*k, b)).collect();
        let pack: Arc<dyn ReadAt> = Arc::new(Owned(pack_of(ObjectFormat::Sha1, &entries)));
        let empty = repos.handle("empty").snapshot();
        let olds = cases.iter().map(|((old, _), _)| *old).collect();
        let targets = cases.iter().map(|((_, new), _)| *new).collect();
        let ws = repos.storage().workspace();
        let ing = ingest_stored(empty, pack, ws, targets, Some(olds), u64::MAX)
            .await
            .unwrap();
        let want: Vec<bool> = cases.iter().map(|(_, f)| *f).collect();
        assert_eq!(ing.force, Some(want));
    }
}
