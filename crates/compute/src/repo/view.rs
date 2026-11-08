//! Node-local, immutable views of a repository.
//!
//! A [`Snapshot`] is a consistent (refs, layers) pair at one replicated
//! version. Readers grab an `Arc<Snapshot>` and never block writers; a new
//! version is published by swapping in a new snapshot that shares unchanged
//! [`LayerView`]s with the previous one.
//!
//! Dense ids are the objects' ids from the replicated state: each layer
//! covers a range of them (its manifest entry's `first_id` and `id_span`),
//! the same on every node, oldest layers lowest; a pushed layer's ids follow
//! its local indexes, and a merged layer's id runs skip its holes. When an
//! object exists in several layers, every copy maps to the *oldest* one (the
//! smallest id), so each oid has exactly one dense id within a snapshot. A
//! view therefore does not depend on its position: a flush or compaction
//! replacing earlier layers keeps the ids it resolved to valid.

use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use foldhash::HashMap;
use zerocopy::little_endian::{I64, U32};
use zerocopy::{FromBytes, Immutable, IntoBytes, Unaligned};

use enroute_shared::state::LayerMeta;

use crate::git::oid::{Kind, ObjectFormat, Oid};
use crate::git::pack::StoredEntry;
use crate::meta::RepoState;
use crate::storage::Access;
use crate::storage::{Layer, NotResident, Ref, RefTarget, TableRecord};
use crate::util::{ByteCache, byte_cache, ix};

pub(crate) type DenseId = u32;

/// Fails the current request. Layer reads have no error channel in the
/// accessors below, and a panic ends only the request task.
#[expect(
    clippy::panic,
    reason = "accessors without an error channel; the request task catches the panic"
)]
pub(super) fn fatal(msg: std::fmt::Arguments<'_>) -> ! {
    panic!("{msg}")
}

/// `v`, which a broken invariant (`what`) would make `None`.
pub(super) fn must<T>(v: Option<T>, what: &str) -> T {
    v.unwrap_or_else(|| fatal(format_args!("broken invariant: {what}")))
}

/// The number of a commit: its position among the snapshot's commits.
///
/// Merges keep every commit's id and drop only newer copies, so a commit's
/// number never changes. A stored graph can name parents by number.
pub(crate) type CommitNum = u32;

/// A commit in the graph, borrowed from its layer view's arrays.
#[derive(Clone, Copy, Debug)]
pub struct CommitNode<'a> {
    pub num: CommitNum,
    /// Dense id of the commit.
    pub id: DenseId,
    pub parents: &'a [CommitNum],
    /// Dense id of the root tree.
    pub tree: DenseId,
    /// Length of the longest path to a root commit.
    pub generation: u32,
    /// Commit time, in seconds since the epoch.
    pub time: i64,
    /// Objects introduced by this commit relative to its parents, including
    /// the commit itself; sorted, without duplicates.
    pub segment: &'a [DenseId],
}

/// What a walk reads of a commit: fewer arrays than a [`CommitNode`].
/// Ordered by generation, then number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CommitLinks<'a> {
    pub generation: u32,
    pub num: CommitNum,
    pub id: DenseId,
    pub parents: &'a [CommitNum],
}

/// A layer's commit graph as flat arrays, one row per commit in rank order
/// (duplicates of older copies have none). Row `r` is commit `first + r`.
#[derive(Default)]
#[cfg_attr(test, derive(Debug, PartialEq))]
struct CommitGraph {
    /// Not stored: the number of commits in older layers.
    first: CommitNum,
    /// Commit rank -> row (`NO_COMMIT`: a duplicate); `None` if every
    /// commit has a row, as in every stored graph.
    rows: Option<Vec<u32>>,
    /// What a walk reads, together, so that a commit costs one cache line.
    walk: Vec<WalkRow>,
    /// `NO_COMMIT` until the row is filled.
    tree: Vec<DenseId>,
    time: Vec<i64>,
    /// (start, len) into `members`.
    member_range: Vec<(u32, u32)>,
    parents: Vec<CommitNum>,
    members: Vec<DenseId>,
}

const NO_COMMIT: DenseId = DenseId::MAX;

#[derive(Clone, Copy, Default)]
#[cfg_attr(test, derive(Debug, PartialEq))]
struct WalkRow {
    id: DenseId,
    generation: u32,
    /// (start, len) into `parents`.
    parents: (u32, u32),
}

impl CommitGraph {
    /// The number after the layer's last commit.
    fn end(&self) -> CommitNum {
        self.first + must(u32::try_from(self.walk.len()).ok(), "commit graph")
    }

    /// The row of the commit of rank `rank`, if it has one.
    fn row(&self, rank: usize) -> Option<usize> {
        match &self.rows {
            None => Some(rank),
            Some(rows) => rows.get(rank).filter(|&&r| r != NO_COMMIT).map(|&r| ix(r)),
        }
    }

    fn links(&self, row: usize) -> Option<CommitLinks<'_>> {
        let w = self.walk.get(row)?;
        Some(CommitLinks {
            num: self.first + u32::try_from(row).ok()?,
            id: w.id,
            generation: w.generation,
            parents: sub_slice(&self.parents, w.parents)?,
        })
    }

    fn get(&self, row: usize) -> Option<CommitNode<'_>> {
        let tree = *self.tree.get(row)?;
        if tree == NO_COMMIT {
            return None;
        }
        let links = must(self.links(row), "commit graph");
        Some(CommitNode {
            num: links.num,
            id: links.id,
            parents: links.parents,
            tree,
            generation: links.generation,
            time: must(self.time.get(row).copied(), "commit graph"),
            segment: must(
                sub_slice(
                    &self.members,
                    must(self.member_range.get(row).copied(), "commit graph"),
                ),
                "commit graph members",
            ),
        })
    }
}

impl CommitGraph {
    /// The graph of a merge of the layers of `graphs`, in order: a merge
    /// keeps every canonical copy, in storage order, and every id.
    fn concat<'a>(graphs: impl Iterator<Item = &'a CommitGraph>) -> Option<CommitGraph> {
        let mut g = CommitGraph::default();
        for s in graphs {
            let shift = |(start, len): (u32, u32), base: usize| {
                Some((start.checked_add(u32::try_from(base).ok()?)?, len))
            };
            let (pbase, mbase) = (g.parents.len(), g.members.len());
            g.tree.extend_from_slice(&s.tree);
            g.time.extend_from_slice(&s.time);
            for (w, &mr) in s.walk.iter().zip(&s.member_range) {
                g.walk.push(WalkRow {
                    parents: shift(w.parents, pbase)?,
                    ..*w
                });
                g.member_range.push(shift(mr, mbase)?);
            }
            g.parents.extend_from_slice(&s.parents);
            g.members.extend_from_slice(&s.members);
        }
        Some(g)
    }

    /// The graph as a layer stores it: the lengths of its arrays, then the
    /// arrays as fixed-width little-endian values, so a read only copies
    /// them.
    fn to_file(&self) -> Vec<u8> {
        let words = |v: &[u32]| v.iter().map(|&w| U32::new(w)).collect::<Vec<_>>();
        let pairs = |v: &[(u32, u32)]| v.iter().flat_map(|&(a, b)| [a, b]).collect::<Vec<_>>();
        let len = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
        let mut out = Vec::new();
        let head = [self.walk.len(), self.parents.len(), self.members.len()].map(len);
        out.extend_from_slice(words(&head).as_bytes());
        let walk = self
            .walk
            .iter()
            .flat_map(|w| [w.id, w.generation, w.parents.0, w.parents.1]);
        out.extend_from_slice(words(&walk.collect::<Vec<_>>()).as_bytes());
        out.extend_from_slice(words(&self.tree).as_bytes());
        let time: Vec<I64> = self.time.iter().map(|&t| I64::new(t)).collect();
        out.extend_from_slice(time.as_bytes());
        out.extend_from_slice(words(&pairs(&self.member_range)).as_bytes());
        out.extend_from_slice(words(&self.parents).as_bytes());
        out.extend_from_slice(words(&self.members).as_bytes());
        out
    }

    /// Read a file [`to_file`](Self::to_file) wrote; `None` if it does not
    /// hold a whole graph.
    fn from_file(b: &[u8]) -> Option<CommitGraph> {
        fn take<'a, T: FromBytes + Immutable + Unaligned>(
            b: &mut &'a [u8],
            n: usize,
        ) -> Option<&'a [T]> {
            let (v, rest) = <[T]>::ref_from_prefix_with_elems(b, n).ok()?;
            *b = rest;
            Some(v)
        }
        let mut b = b;
        let [n, np, nm] = take::<U32>(&mut b, 3)? else {
            return None;
        };
        let (n, np, nm) = (ix(n.get()), ix(np.get()), ix(nm.get()));
        let words = |v: &[U32]| v.iter().map(|w| w.get()).collect::<Vec<_>>();
        let pairs = |v: &[[U32; 2]]| v.iter().map(|[a, b]| (a.get(), b.get())).collect();
        let walk = take::<[U32; 4]>(&mut b, n)?
            .iter()
            .map(|[id, generation, start, len]| WalkRow {
                id: id.get(),
                generation: generation.get(),
                parents: (start.get(), len.get()),
            })
            .collect();
        let tree = words(take(&mut b, n)?);
        let time = take::<I64>(&mut b, n)?.iter().map(|t| t.get()).collect();
        let g = CommitGraph {
            first: 0,
            rows: None,
            walk,
            tree,
            time,
            member_range: pairs(take(&mut b, n)?),
            parents: words(take(&mut b, np)?),
            members: words(take(&mut b, nm)?),
        };
        b.is_empty().then_some(g)
    }
}

/// The commit graph of a merge of the layers of `views`, as its layer
/// stores it (see [`LayerBuilder::set_graph`](crate::storage::LayerBuilder::set_graph)).
pub(crate) fn merged_graph(views: &[Arc<LayerView>]) -> Option<Vec<u8>> {
    CommitGraph::concat(views.iter().map(|lv| &lv.commits)).map(|g| g.to_file())
}

/// The `len` items of `v` from `start`.
fn sub_slice(v: &[DenseId], (start, len): (u32, u32)) -> Option<&[DenseId]> {
    let start = ix(start);
    v.get(start..start.checked_add(ix(len))?)
}

/// A layer's objects within a snapshot's dense id space.
pub struct LayerView {
    /// The layer.
    pub layer: Arc<Layer>,
    /// Whether the layer is stored (else inline: its bytes are in the log).
    pub stored: bool,
    /// The layer's id range: `base .. base + span`.
    pub base: DenseId,
    /// The number of ids in the range.
    pub span: u32,
    /// local -> canonical dense id; `None` means every copy is canonical.
    canon: Option<Vec<DenseId>>,
    commits: CommitGraph,
}

impl std::fmt::Debug for LayerView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayerView")
            .field("layer", &self.layer.id)
            .field("stored", &self.stored)
            .field("base", &self.base)
            .field("span", &self.span)
            .finish_non_exhaustive()
    }
}

impl LayerView {
    /// The canonical dense id of the object at `local`: its oldest copy's.
    ///
    /// # Panics
    ///
    /// Panics if `local` is outside the layer, or on an I/O error.
    pub(crate) fn dense(&self, local: u32) -> DenseId {
        match &self.canon {
            None => self.own(local),
            Some(c) => c.get(ix(local)).copied().unwrap_or_else(|| {
                fatal(format_args!("layer {}: no object {local}", self.layer.id))
            }),
        }
    }

    /// The id of the object at `local` in this layer (canonical unless an
    /// older layer has a copy).
    pub(crate) fn own(&self, local: u32) -> DenseId {
        self.layer.id_of(self.base, local)
    }

    /// The local index of the object with id `d` in this layer, if any.
    pub fn local(&self, d: DenseId) -> Option<u32> {
        self.layer.local_of(self.base, d)
    }

    /// Sets the delta base link of each delta object of the layer in `links`.
    fn fill_links(&self, links: &mut [u32]) -> Result<()> {
        self.layer.for_each_record(|l, r| {
            if r.base().is_none() {
                return;
            }
            let d = self.own(l);
            let slot = links
                .get_mut(ix(d))
                .unwrap_or_else(|| fatal(format_args!("dense id {d} outside snapshot")));
            *slot = r.base().0;
        })
    }

    /// The commit graph slot of the object at `local`, if it is a commit.
    fn commit_slot(&self, local: u32) -> Option<usize> {
        match self.layer.kind_rank(local) {
            (Kind::Commit, rank) => Some(ix(rank)),
            _ => None,
        }
    }

    /// The first id after the layer's range.
    pub fn end(&self) -> DenseId {
        self.base + self.span
    }

    /// The view of `layer` (stored if `is_stored`, else inline), covering ids `base .. base + span`,
    /// after `prev` (older layers, whose stored layers are `stored`, and
    /// whose inline layers' objects are in `index`; `index` may also name
    /// objects of this layer or later ones, by their canonical ids).
    pub fn build(
        layer: Arc<Layer>,
        is_stored: bool,
        base: DenseId,
        span: u32,
        prev: &[Arc<LayerView>],
        stored: &[Arc<LayerView>],
        index: &InlineIndex,
    ) -> Result<LayerView> {
        ensure!(
            layer.id_span() == span,
            "layer {} does not cover ids {base}..{}",
            layer.id,
            base + span
        );
        let canon = if prev.is_empty() && index.is_empty() {
            None
        } else {
            canonical_ids(&layer, base, stored, index)?
        };
        let mut view = LayerView {
            layer,
            stored: is_stored,
            base,
            span,
            canon,
            commits: CommitGraph::default(),
        };
        view.commits = view.commit_graph(prev, stored, index)?;
        Ok(view)
    }

    /// The view of merge output `layer` (of manifest entry `m`), with the
    /// commit graph it stores: `None` for a pushed layer, which stores
    /// none, or if they do not match.
    /// Its first commit is `first`.
    fn of_merge(layer: Arc<Layer>, m: &LayerMeta, first: CommitNum) -> Result<Option<LayerView>> {
        let matches = |g: &CommitGraph| {
            layer.id_span() == m.id_span && g.walk.len() == ix(layer.count(Kind::Commit))
        };
        let Some(mut g) = CommitGraph::from_file(&layer.graph()?).filter(matches) else {
            return Ok(None);
        };
        g.first = first;
        Ok(Some(LayerView {
            layer,
            stored: m.is_stored(),
            base: m.first_id,
            span: m.id_span,
            canon: None,
            commits: g,
        }))
    }

    fn commit_graph(
        &self,
        prev: &[Arc<LayerView>],
        stored: &[Arc<LayerView>],
        index: &InlineIndex,
    ) -> Result<CommitGraph> {
        // Duplicates of older copies get no row.
        let mut rows = Vec::with_capacity(ix(self.layer.count(Kind::Commit)));
        let mut n = 0u32;
        for local in self.layer.locals_of(Kind::Commit) {
            if self.dense(local) == self.own(local) {
                rows.push(n);
                n += 1;
            } else {
                rows.push(NO_COMMIT);
            }
        }
        let n = ix(n);
        let mut g = CommitGraph {
            first: commit_end(prev),
            rows: (n < rows.len()).then_some(rows),
            walk: vec![WalkRow::default(); n],
            tree: vec![NO_COMMIT; n],
            time: vec![0; n],
            member_range: vec![(0, 0); n],
            parents: Vec::new(),
            members: Vec::new(),
        };
        for rec in self.layer.commits()? {
            let me = self.dense(rec.local);
            if me != self.own(rec.local) {
                continue; // duplicate of an older copy
            }
            let node = NewNode {
                local: rec.local,
                me,
                tree: rec.tree,
                time: rec.time,
                parents: rec.parents,
                members: rec.members,
            };
            self.add_commit(
                &mut g,
                node,
                &Older {
                    prev,
                    stored,
                    index,
                },
            )?;
        }
        g.parents.shrink_to_fit();
        g.members.shrink_to_fit();
        Ok(g)
    }

    /// Adds commit `rec` to `g`.
    fn add_commit(&self, g: &mut CommitGraph, rec: NewNode, older: &Older<'_>) -> Result<()> {
        let Older {
            prev,
            stored,
            index,
        } = *older;
        let me = rec.me;
        let resolve = |r: Ref, kind| self.resolve(r, kind, stored, index);
        let rank = must(self.commit_slot(rec.local), "commit record of a commit");
        let row = must(g.row(rank), "a row for each canonical commit");
        let tree = resolve(rec.tree, Some(Kind::Tree)).context("commit tree missing")?;
        let pstart = offset(&g.parents)?;
        let mut generation = 1;
        for p in rec.parents {
            let pd = resolve(p, Some(Kind::Commit)).context("commit parent missing")?;
            let parent = if let Some(l) = self.local(pd) {
                self.commit_slot(l).and_then(|s| g.get(g.row(s)?))
            } else {
                commit_in(prev, pd)
            }
            .context("parent commit has no graph node")?;
            generation = generation.max(parent.generation + 1);
            g.parents.push(parent.num);
        }
        let mstart = offset(&g.members)?;
        g.members.push(me);
        for m in rec.members {
            if let Some(d) = resolve(m, None) {
                g.members.push(d);
            } else {
                tracing::warn!(layer = %self.layer.id, "segment member missing");
            }
        }
        let seg = must(g.members.get_mut(ix(mstart)..), "segment start in range");
        seg.sort_unstable();
        let kept = dedup_sorted(seg);
        g.members.truncate(ix(mstart) + kept);
        let plen = offset(&g.parents)? - pstart;
        let mlen = u32::try_from(kept).context("commit segment too large")?;
        let (Some(w), Some(t), Some(tm), Some(mr)) = (
            g.walk.get_mut(row),
            g.tree.get_mut(row),
            g.time.get_mut(row),
            g.member_range.get_mut(row),
        ) else {
            fatal(format_args!(
                "commit {row} outside the layer's commit range"
            ));
        };
        *w = WalkRow {
            id: me,
            generation,
            parents: (pstart, plen),
        };
        *t = tree;
        *tm = rec.time;
        *mr = (mstart, mlen);
        Ok(())
    }

    fn resolve(
        &self,
        r: Ref,
        kind: Option<Kind>,
        stored: &[Arc<LayerView>],
        index: &InlineIndex,
    ) -> Option<DenseId> {
        match r.decode() {
            RefTarget::None => None,
            RefTarget::Local(l) => Some(self.dense(l)),
            RefTarget::External(e) => lookup_in(stored, index, &self.layer.ext_oid(e), kind),
        }
    }
}

/// A commit to add to a [`CommitGraph`].
struct NewNode {
    local: u32,
    me: DenseId,
    tree: Ref,
    time: i64,
    parents: Vec<Ref>,
    members: Vec<Ref>,
}

/// What lookups of ids in older layers use.
#[derive(Clone, Copy)]
struct Older<'a> {
    prev: &'a [Arc<LayerView>],
    stored: &'a [Arc<LayerView>],
    index: &'a InlineIndex,
}

/// The length of `v` as a table offset.
fn offset(v: &[DenseId]) -> Result<u32> {
    u32::try_from(v.len()).context("commit graph too large")
}

/// Removes the repeats from sorted `seg` in place; returns the new length.
fn dedup_sorted(seg: &mut [DenseId]) -> usize {
    let mut kept = 0;
    let mut last = None;
    for i in 0..seg.len() {
        let Some(&v) = seg.get(i) else { break };
        if last == Some(v) {
            continue;
        }
        last = Some(v);
        if let Some(slot) = seg.get_mut(kept) {
            *slot = v;
        }
        kept += 1;
    }
    kept
}

/// The canonical id of each object of `layer` (ids from `base`): the oldest
/// copy's, in older layers (`stored`, `index`) if any. `None` if every
/// object is its own canonical copy.
fn canonical_ids(
    layer: &Layer,
    base: DenseId,
    stored: &[Arc<LayerView>],
    index: &InlineIndex,
) -> Result<Option<Vec<DenseId>>> {
    let mut any = false;
    let mut c: Vec<DenseId> = Vec::with_capacity(ix(layer.len()));
    layer.for_each_oid(|l, oid| {
        let own = layer.id_of(base, l);
        let older = lookup_in(stored, index, oid, Some(layer.kind_of(l))).filter(|&d| d < own);
        any |= older.is_some();
        c.push(older.unwrap_or(own));
    })?;
    Ok(any.then_some(c))
}

/// The objects of a snapshot's inline layers, by oid: their canonical dense
/// id (the oldest copy, older stored layers included) and kind. Canonical
/// ids never change, so an entry never goes wrong: entries below a
/// watermark are dropped once their layers are stored (see
/// [`since`](InlineIndex::since)), and lookups find those objects there.
///
/// Under heavy writes a repository has hundreds of inline layers, one per
/// push until a flush merges them, each tiny: walking them (a bloom filter
/// check each, and the false positives' table searches) would dominate
/// object lookups. Stored layers are few and large, and keep their
/// bloom filters and sorted tables. Rebuilt with each snapshot: a snapshot
/// that only adds layers copies its predecessor's and extends it.
#[derive(Clone, Default)]
pub struct InlineIndex {
    map: HashMap<Oid, IndexEntry>,
}

impl std::fmt::Debug for InlineIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InlineIndex")
            .field("objects", &self.map.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
struct IndexEntry {
    dense: DenseId,
    kind: Kind,
}

impl InlineIndex {
    /// The entries for objects with ids from `first` on.
    pub fn since(&self, first: DenseId) -> InlineIndex {
        let mut map = self.map.clone();
        map.retain(|_, e| e.dense >= first);
        InlineIndex { map }
    }

    /// Add the objects of `lv` (an inline layer newer than every layer
    /// added before), keeping older entries for objects already present.
    pub fn add(&mut self, lv: &LayerView) -> Result<()> {
        lv.layer.for_each_oid(|l, oid| {
            self.map.entry(*oid).or_insert(IndexEntry {
                dense: lv.dense(l),
                kind: lv.layer.kind_of(l),
            });
        })
    }

    /// Whether the index has no objects.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Find `oid` (of `kind`, if given) among layers whose stored layers are
/// `stored` (oldest first) and whose inline layers' objects are in
/// `index`: the oldest copy, as walking the layers oldest first would. An
/// index entry already names it (its dense id is canonical, older stored
/// layers included); otherwise the stored layers are checked in order.
/// Compaction keeps them few: about one per size class.
fn lookup_in(
    stored: &[Arc<LayerView>],
    index: &InlineIndex,
    oid: &Oid,
    kind: Option<Kind>,
) -> Option<DenseId> {
    if let Some(e) = index.map.get(oid) {
        return kind.is_none_or(|k| k == e.kind).then_some(e.dense);
    }
    stored.iter().find_map(|lv| {
        if !lv.layer.may_contain(oid) {
            return None;
        }
        let found = match kind {
            Some(k) => lv.layer.find_kind(k, oid),
            None => lv.layer.find(oid),
        };
        found.map(|l| lv.dense(l))
    })
}

/// Where an object's stored entry is in its layer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EntryLoc {
    pub off: u64,
    pub len: u32,
    /// Delta base, if stored as a delta.
    pub base: Option<DenseId>,
}

/// The kind of the delta base of `l` in `lv`: a delta and its base are of
/// one kind.
fn base_kind(lv: &LayerView, l: u32) -> Kind {
    lv.layer.kind_of(l)
}

/// The position of the layer whose range holds `d` (layers are in id
/// order, their ranges disjoint).
fn layer_at(layers: &[Arc<LayerView>], d: DenseId) -> Option<usize> {
    let i = layers.partition_point(|lv| lv.end() <= d);
    layers.get(i).is_some_and(|lv| d >= lv.base).then_some(i)
}

fn locate(layers: &[Arc<LayerView>], d: DenseId) -> Option<(&LayerView, u32)> {
    let lv = layers.get(layer_at(layers, d)?)?;
    lv.local(d).map(|l| (lv.as_ref(), l))
}

fn commit_in(layers: &[Arc<LayerView>], d: DenseId) -> Option<CommitNode<'_>> {
    let (lv, local) = locate(layers, d)?;
    lv.commits.get(lv.commits.row(lv.commit_slot(local)?)?)
}

/// The number after the last commit of `layers`.
fn commit_end(layers: &[Arc<LayerView>]) -> CommitNum {
    layers.last().map_or(0, |lv| lv.commits.end())
}

/// The graph of the layer holding commit `c`, and `c`'s row in it.
fn graph_of(layers: &[Arc<LayerView>], c: CommitNum) -> Option<(&CommitGraph, usize)> {
    let lv = layers.get(layers.partition_point(|lv| lv.commits.end() <= c))?;
    Some((&lv.commits, ix(c.checked_sub(lv.commits.first)?)))
}

/// A consistent view of a repository at one replicated version.
pub struct Snapshot {
    /// The replicated state this snapshot reflects (refs, HEAD, version,
    /// layers).
    pub meta: Arc<RepoState>,
    /// The views of the layers, oldest first.
    pub layers: Vec<Arc<LayerView>>,
    /// The stored layers among `layers`, oldest first.
    pub stored: Vec<Arc<LayerView>>,
    /// The objects of the inline layers among `layers`.
    pub inline: Arc<InlineIndex>,
    /// The node's object caches.
    pub cache: Arc<Caches>,
    /// Every object's raw delta-base link, by dense id (built on first use
    /// by a bulk rebuild, then shared by all requests on this snapshot).
    pub chains: std::sync::OnceLock<Arc<Vec<u32>>>,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("version", &self.meta.version)
            .field("layers", &self.layers.len())
            .finish_non_exhaustive()
    }
}

impl Snapshot {
    /// The view of no state yet (version 0).
    #[must_use]
    pub fn empty(cache: Arc<Caches>) -> Snapshot {
        Snapshot {
            meta: Arc::default(),
            layers: Vec::new(),
            stored: Vec::new(),
            inline: Arc::default(),
            cache,
            chains: std::sync::OnceLock::new(),
        }
    }

    /// Replicated version this snapshot reflects.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.meta.version
    }

    /// How the repository names its objects.
    #[must_use]
    pub fn format(&self) -> ObjectFormat {
        self.meta.format()
    }

    /// What [`build`](Self::build) of `meta` can take from this snapshot.
    ///
    /// Views do not depend on their position: every view whose layer is
    /// still there, with the same id range and still inline or stored, is
    /// reused, wherever it now sits. In practice the rest are new pushes at the end, and flush
    /// and compaction outputs (which hold only canonical copies, and refer
    /// only to older objects). Both lists are in id order (a layer list is
    /// ordered by its ranges), so one walk pairs them: no hashing.
    pub(crate) fn reuse(&self, meta: Arc<RepoState>) -> Reuse {
        let mut views = Vec::with_capacity(meta.layers.len());
        let mut j = 0;
        for m in &meta.layers {
            while self.layers.get(j).is_some_and(|lv| lv.base < m.first_id) {
                j += 1;
            }
            let lv = self
                .layers
                .get(j..)
                .unwrap_or_default()
                .iter()
                .take_while(|lv| lv.base == m.first_id)
                .find(|lv| {
                    lv.span == m.id_span && lv.stored == m.is_stored() && lv.layer.id == m.id
                });
            views.push(lv.cloned());
        }
        Reuse { meta, views }
    }

    /// The snapshot of `reuse`'s state following this one, with new views
    /// over `opened` (the layers of [`Reuse::missing`], in order).
    ///
    /// # Errors
    ///
    /// Returns an error if `opened` has too few layers, or a layer does not
    /// match its manifest entry or cannot be read.
    pub(crate) fn build(&self, reuse: Reuse, opened: Vec<Arc<Layer>>) -> Result<Snapshot> {
        let Reuse { meta, views } = reuse;
        // Inline index entries below the oldest inline layer's ids are for
        // objects now only in stored layers, where lookups find them.
        let watermark = meta
            .layers
            .iter()
            .filter(|m| !m.is_stored())
            .map(|m| m.first_id)
            .min()
            .unwrap_or(u32::MAX);
        let mut index = self.inline.since(watermark);
        let mut opened = opened.into_iter();
        let mut layers: Vec<Arc<LayerView>> = Vec::with_capacity(meta.layers.len());
        // The stored layers so far, for lookups while building.
        let mut stored: Vec<Arc<LayerView>> = Vec::new();
        for (m, lv) in meta.layers.iter().zip(views) {
            let first = commit_end(&layers);
            let lv = if let Some(lv) = lv {
                debug_assert_eq!(
                    lv.commits.first, first,
                    "a reused layer's commit numbers moved"
                );
                lv
            } else {
                let layer = must(opened.next(), "a layer opened for each one without a view");
                // Every request waits for this snapshot, and indexing a big
                // merge output again takes seconds.
                let fast = LayerView::of_merge(layer.clone(), m, first)?;
                let lv = match fast {
                    Some(lv) => lv,
                    None => LayerView::build(
                        layer,
                        m.is_stored(),
                        m.first_id,
                        m.id_span,
                        &layers,
                        &stored,
                        &index,
                    )?,
                };
                add_to_index(&mut index, &lv)?;
                Arc::new(lv)
            };
            if m.is_stored() {
                stored.push(lv.clone());
            }
            layers.push(lv);
        }
        Ok(Snapshot {
            stored,
            meta,
            layers,
            inline: Arc::new(index),
            cache: self.cache.clone(),
            chains: std::sync::OnceLock::new(),
        })
    }
}

/// Adds `lv` to `index` if it is an inline layer.
fn add_to_index(index: &mut InlineIndex, lv: &LayerView) -> Result<()> {
    if !lv.stored {
        index.add(lv)?;
    }
    Ok(())
}

/// What a new snapshot of `meta` can reuse from the one before it (see
/// [`Snapshot::reuse`]).
#[derive(Debug)]
pub(crate) struct Reuse {
    meta: Arc<RepoState>,
    /// For each layer of `meta`, the view to reuse (`None`: one to build).
    views: Vec<Option<Arc<LayerView>>>,
}

impl Reuse {
    /// The layers without a view to reuse, in order.
    pub(crate) fn missing(&self) -> impl Iterator<Item = &LayerMeta> {
        self.meta
            .layers
            .iter()
            .zip(&self.views)
            .filter(|(_, lv)| lv.is_none())
            .map(|(m, _)| m)
    }
}

/// Node-private caches keyed by object id. Content-addressed entries never
/// go stale and survive compaction (which copies entries verbatim into new
/// layers), and duplicate copies across layers share one entry.
#[derive(Debug)]
pub struct Caches {
    /// Decoded object content.
    pub(crate) objects: ByteCache<Oid>,
    /// Self-contained pack entries for objects whose stored form is a delta
    /// the client may lack (shallow boundaries, filters): re-encoding is the
    /// main cost of serving them, and hot tips are cloned over and over.
    pub(crate) entries: ByteCache<Oid>,
}

impl Caches {
    /// Splits `bytes` between decoded objects (3/4) and entries (1/4).
    pub fn new(bytes: usize) -> Caches {
        Caches {
            objects: byte_cache(bytes / 4 * 3),
            entries: byte_cache(bytes / 4),
        }
    }
}

/// Every this many steps of a delta chain, the intermediate result is
/// cached as a checkpoint. Caching every step would let one read of a deep
/// chain evict up to 50 other objects' worth of useful entries.
const CHAIN_CHECKPOINT: usize = 16;

/// The most objects a delta chain may have. Git's own default depth is 50:
/// a longer chain is corrupt (or a cycle).
const MAX_CHAIN: usize = 1000;

impl Snapshot {
    /// The dense id of `oid`, if the snapshot has it.
    pub fn lookup(&self, oid: &Oid) -> Option<DenseId> {
        lookup_in(&self.stored, &self.inline, oid, None)
    }

    pub(crate) fn lookup_kind(&self, oid: &Oid, kind: Kind) -> Option<DenseId> {
        lookup_in(&self.stored, &self.inline, oid, Some(kind))
    }

    /// Check that every object of every layer looks up to the copy a plain
    /// oldest-first walk over all layers finds (for tests of the inline
    /// index and the stored layers).
    ///
    /// # Errors
    ///
    /// Returns an error if a lookup differs, or a layer cannot be read.
    #[cfg(test)]
    pub fn check_lookups(&self) -> Result<()> {
        for lv in &self.layers {
            self.check_layer_lookups(lv)?;
        }
        Ok(())
    }

    /// [`check_lookups`](Self::check_lookups) for the objects of `lv`.
    #[cfg(test)]
    fn check_layer_lookups(&self, lv: &LayerView) -> Result<()> {
        let mut wrong = None;
        lv.layer.for_each_oid(|_, oid| {
            if wrong.is_none() {
                wrong = self.lookup_mismatch(oid);
            }
        })?;
        if let Some(w) = wrong {
            bail!("{w}");
        }
        // Id order is storage order: ids go up with the entries, and a
        // delta's base comes first.
        let mut last = None;
        let mut wrong = None;
        lv.layer.for_each_record(|l, r| {
            let id = lv.own(l);
            let base = match r.base().decode() {
                RefTarget::Local(b) => Some(b),
                _ => None,
            };
            if wrong.is_none()
                && (last.is_some_and(|(id0, off0)| id0 >= id || off0 >= r.entry_off())
                    || base.is_some_and(|b| b >= l))
            {
                wrong = Some(l);
            }
            last = Some((id, r.entry_off()));
        })?;
        if let Some(l) = wrong {
            bail!("layer {}: object {l} out of storage order", lv.layer.id);
        }
        Ok(())
    }

    /// A description of how the lookup of `oid` differs from an oldest-first
    /// walk, if it does.
    #[cfg(test)]
    fn lookup_mismatch(&self, oid: &Oid) -> Option<String> {
        let walk = self
            .layers
            .iter()
            .find_map(|w| w.layer.find(oid).map(|l| w.dense(l)));
        let got = self.lookup(oid);
        (got != walk).then(|| format!("{oid}: lookup {got:?}, oldest-first walk {walk:?}"))
    }

    /// The view and local index of `d`.
    ///
    /// # Panics
    ///
    /// Panics if `d` is outside the snapshot.
    pub(crate) fn locate(&self, d: DenseId) -> (&LayerView, u32) {
        locate(&self.layers, d)
            .unwrap_or_else(|| fatal(format_args!("dense id {d} outside snapshot")))
    }

    /// The position of the layer whose range holds `d` (which may be a
    /// hole).
    ///
    /// # Panics
    ///
    /// Panics if `d` is outside the snapshot.
    pub(crate) fn layer_at(&self, d: DenseId) -> usize {
        layer_at(&self.layers, d)
            .unwrap_or_else(|| fatal(format_args!("dense id {d} outside snapshot")))
    }

    /// Every object's raw delta-base link (`u32::MAX`: none), by dense id:
    /// one sequential scan of the tables, done once per snapshot.
    pub(crate) fn chain_links(&self) -> Result<Arc<Vec<u32>>> {
        if let Some(c) = self.chains.get() {
            return Ok(c.clone());
        }
        let end = self.layers.last().map_or(0, |l| l.end());
        let mut links = vec![u32::MAX; ix(end)];
        for lv in &self.layers {
            lv.fill_links(&mut links)?;
        }
        Ok(self.chains.get_or_init(|| Arc::new(links)).clone())
    }

    /// Where `d`'s stored entry is, with its delta base.
    pub(crate) fn entry_loc(&self, d: DenseId) -> Result<EntryLoc> {
        let (lv, l) = self.locate(d);
        let r = lv.layer.record(l)?;
        let base = if r.base().is_none() {
            None
        } else {
            self.resolve_ref(lv, r.base(), base_kind(lv, l))
        };
        Ok(EntryLoc {
            off: r.entry_off(),
            len: r.entry_len(),
            base,
        })
    }

    /// Call `f` with `d`'s stored entry at `loc` (from
    /// [`entry_loc`](Self::entry_loc)), without reading its record again.
    pub(crate) fn with_entry_at<R>(
        &self,
        d: DenseId,
        loc: &EntryLoc,
        mode: Access,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R> {
        let (lv, _) = self.locate(d);
        lv.layer.with_entry_at(loc.off, loc.len, mode, f)
    }

    /// The kind of object `d`.
    pub fn kind(&self, d: DenseId) -> Kind {
        let (lv, l) = self.locate(d);
        lv.layer.kind_of(l)
    }

    /// The object id of `d`.
    pub fn oid(&self, d: DenseId) -> Oid {
        let (lv, l) = self.locate(d);
        lv.layer.oid(l)
    }

    /// The uncompressed size of object `d`.
    pub fn size(&self, d: DenseId) -> u64 {
        let (lv, l) = self.locate(d);
        lv.layer.size(l)
    }

    /// The table record of `d`. Panics on an I/O error (the request fails,
    /// the process does not).
    fn record(&self, d: DenseId) -> TableRecord {
        let (lv, l) = self.locate(d);
        lv.layer
            .record(l)
            .unwrap_or_else(|e| fatal(format_args!("layer {}: {e:#}", lv.layer.id)))
    }

    /// The commit graph node of `d`, if `d` is a commit with one.
    pub fn commit(&self, d: DenseId) -> Option<CommitNode<'_>> {
        commit_in(&self.layers, d)
    }

    /// The walk links of commit `d`, if `d` is a commit with a graph node.
    pub(crate) fn links_of(&self, d: DenseId) -> Option<CommitLinks<'_>> {
        let (lv, local) = locate(&self.layers, d)?;
        lv.commits.links(lv.commits.row(lv.commit_slot(local)?)?)
    }

    /// The walk links of commit number `c`.
    pub(crate) fn links(&self, c: CommitNum) -> Option<CommitLinks<'_>> {
        let (g, row) = graph_of(&self.layers, c)?;
        g.links(row)
    }

    /// The number after the last commit.
    pub(crate) fn commit_end(&self) -> CommitNum {
        commit_end(&self.layers)
    }

    /// Resolve a reference to an object of `kind`, stored in `lv`.
    fn resolve_ref(&self, lv: &LayerView, r: Ref, kind: Kind) -> Option<DenseId> {
        match r.decode() {
            RefTarget::None => None,
            RefTarget::Local(l) => Some(lv.dense(l)),
            RefTarget::External(e) => self.lookup_kind(&lv.layer.ext_oid(e), kind),
        }
    }

    pub(crate) fn delta_depth(&self, d: DenseId) -> u8 {
        self.record(d).depth()
    }

    /// The delta base that `link`, stored with `d`, names (`None`: no
    /// link). An error if the snapshot does not have it.
    pub(crate) fn base_from(&self, d: DenseId, link: Ref) -> Result<Option<DenseId>> {
        if link.is_none() {
            return Ok(None);
        }
        let (lv, l) = self.locate(d);
        self.resolve_ref(lv, link, base_kind(lv, l))
            .map(Some)
            .with_context(|| {
                let oid = lv.layer.ref_oid(link).unwrap_or(Oid::ZERO);
                format!("delta base {oid} missing")
            })
    }

    /// Walk the delta chain from `d`: `step` takes each object in turn and
    /// gives its base link (see [`base_from`](Self::base_from)), or `None`
    /// to stop there.
    pub(crate) fn walk_chain(
        &self,
        d: DenseId,
        mut step: impl FnMut(DenseId) -> Result<Option<Ref>>,
    ) -> Result<()> {
        let mut cur = d;
        for _ in 0..MAX_CHAIN {
            let Some(link) = step(cur)? else {
                return Ok(());
            };
            match self.base_from(cur, link)? {
                Some(b) => cur = b,
                None => return Ok(()),
            }
        }
        bail!("delta chain too long")
    }

    /// Fully decoded object content (deltas resolved, cached).
    ///
    /// # Errors
    ///
    /// Returns an error if a layer entry is missing, not local, or corrupt, or
    /// the delta chain is too long.
    pub fn read(&self, d: DenseId) -> Result<Arc<Vec<u8>>> {
        // Collect the chain down to a cached or full object.
        let mut chain: Vec<DenseId> = Vec::new();
        let mut content = None;
        self.walk_chain(d, |x| {
            let (lv, l) = self.locate(x);
            let key = lv.layer.oid(l);
            if let Some(c) = self.cache.objects.get(&key) {
                content = Some(c);
                return Ok(None);
            }
            let rec = lv.layer.object(l)?;
            let e = StoredEntry::parse(lv.layer.format(), &rec.entry)?;
            if e.delta {
                chain.push(x);
                return Ok(Some(rec.base));
            }
            let c = Arc::new(e.decode(None)?);
            if chain.is_empty() {
                self.cache.objects.insert(key, c.clone());
            }
            content = Some(c);
            Ok(None)
        })?;
        let mut content = content.context("delta without a base link")?;
        while let Some(x) = chain.pop() {
            let (lv, l) = self.locate(x);
            let rec = lv.layer.object(l)?;
            let out = Arc::new(
                StoredEntry::parse(lv.layer.format(), &rec.entry)?.decode(Some(&content))?,
            );
            if chain.len().is_multiple_of(CHAIN_CHECKPOINT) {
                self.cache.objects.insert(lv.layer.oid(l), out.clone());
            }
            content = out;
        }
        Ok(content)
    }

    /// Bytes [`read`](Self::read) of `d` needs that are not local: its
    /// entry and those of its delta chain, down to a cached or full object
    /// (from table records, which are always local).
    ///
    /// # Errors
    ///
    /// Returns an error if the delta chain cannot be walked.
    pub fn missing(&self, d: DenseId) -> Result<Vec<NotResident>> {
        self.missing_chain(d, true)
    }

    /// Like [`missing`](Self::missing), for the whole delta chain however
    /// much of it is cached (for loads well ahead of the reads: the cache
    /// may drop an object in between).
    ///
    /// # Errors
    ///
    /// Returns an error if the delta chain cannot be walked.
    pub(crate) fn missing_all(&self, d: DenseId) -> Result<Vec<NotResident>> {
        self.missing_chain(d, false)
    }

    /// What to load before reading `d` again, after a read of it failed
    /// with `e`: what its chain lacks, or at least what `e` says was not
    /// local. `e` itself if it is not about bytes that are not local.
    pub(crate) fn missing_after(&self, d: DenseId, e: anyhow::Error) -> Result<Vec<NotResident>> {
        let Some(nr) = NotResident::find(&e) else {
            return Err(e);
        };
        let mut need = self.missing(d)?;
        if need.is_empty() {
            need.push(nr.clone());
        }
        Ok(need)
    }

    fn missing_chain(&self, d: DenseId, use_cache: bool) -> Result<Vec<NotResident>> {
        let mut need = Vec::new();
        self.walk_chain(d, |x| {
            let (lv, l) = self.locate(x);
            if use_cache && self.cache.objects.get(&lv.layer.oid(l)).is_some() {
                return Ok(None);
            }
            let r = lv.layer.record(l)?;
            need.extend(
                lv.layer
                    .entries_missing(r.entry_off(), r.entry_off() + u64::from(r.entry_len())),
            );
            Ok(Some(r.base()))
        })?;
        Ok(need)
    }

    /// The kind and content of the object `oid`, if the snapshot has it.
    ///
    /// # Errors
    ///
    /// Returns an error if [`read`](Self::read) fails.
    pub fn read_oid(&self, oid: &Oid) -> Result<Option<(Kind, Arc<Vec<u8>>)>> {
        match self.lookup(oid) {
            Some(d) => Ok(Some((self.kind(d), self.read(d)?))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merged_commit_graphs_round_trip_through_layers() {
        let g = |tree: Vec<u32>, parents: Vec<u32>, members: Vec<u32>| CommitGraph {
            first: 0,
            rows: None,
            walk: tree
                .iter()
                .map(|t| WalkRow {
                    id: t + 100,
                    generation: 1,
                    parents: (0, u32::try_from(parents.len()).unwrap()),
                })
                .collect(),
            time: vec![-5; tree.len()],
            member_range: vec![(0, u32::try_from(members.len()).unwrap()); tree.len()],
            tree,
            parents,
            members,
        };
        let (a, b) = (g(vec![7], vec![], vec![1]), g(vec![9], vec![0], vec![2, 3]));
        let merged = CommitGraph::concat([&a, &b].into_iter()).unwrap();
        assert_eq!(merged.tree, [7, 9]);
        let ids: Vec<_> = merged.walk.iter().map(|w| w.id).collect();
        assert_eq!(ids, [107, 109]);
        let parents: Vec<_> = merged.walk.iter().map(|w| w.parents).collect();
        assert_eq!(parents, [(0, 0), (0, 1)]);
        assert_eq!(merged.member_range, [(0, 1), (1, 2)]);
        let file = merged.to_file();
        assert_eq!(CommitGraph::from_file(&file), Some(merged));
        assert_eq!(CommitGraph::from_file(&file[..file.len() - 1]), None);
    }
}
