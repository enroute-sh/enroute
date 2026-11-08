//! Fetch serving: plan which objects to send, then stream a pack assembled
//! from stored entries.
//!
//! Planning works on metadata only (commit graph + per-commit segment
//! bitmaps), so the object count is known before the first byte is sent.
//! Streaming copies stored pack entries verbatim. Deltas are kept as long as
//! their base is in the pack; otherwise the object is re-encoded as a full
//! entry, or as a delta against another object of the pack that shared its
//! base (see [`assemble`]).
//!
//! Packs are sent in storage order (layer by layer, each front to back:
//! commits, trees, blobs), so streaming reads every page once instead of
//! jumping between commits' segments. Pack order is free: stored deltas
//! name their base by id, and `OFS_DELTA` is only used for a base already
//! sent.

use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use foldhash::{HashMap, HashMapExt, HashSet};
use roaring::RoaringBitmap;

use crate::access::Hidden;
use crate::git::delta;
use crate::git::oid::{Kind, ObjectFormat, Oid};
use crate::git::pack::{
    OFS_DELTA, PackHasher, StoredEntry, encode_entry_header, encode_ofs, full_entry,
    ref_delta_entry,
};
use crate::storage::{Access, NotResident};
use crate::util::ix;

use super::machine::{self, Ticket, Waits};
use super::view::{CommitNum, DenseId, EntryLoc, Snapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Filter {
    #[default]
    None,
    BlobNone,
    /// `blob:limit=n`: blobs of `n` bytes or more are left out.
    BlobLimit(u64),
    /// tree:0 — commits (and tags) only.
    TreeNone,
}

impl Filter {
    pub(crate) fn parse(s: &str) -> Result<Filter> {
        if s == "blob:none" {
            return Ok(Filter::BlobNone);
        }
        if let Some(n) = s.strip_prefix("blob:limit=") {
            return Ok(Filter::BlobLimit(parse_size(n)?));
        }
        if s == "tree:0" {
            return Ok(Filter::TreeNone);
        }
        bail!("unsupported filter {s:?}")
    }
}

/// A size as git's `git_parse_ulong` reads it: a C number with no sign,
/// then an optional `k`, `m` or `g`.
fn parse_size(s: &str) -> Result<u64> {
    let bad = || anyhow::anyhow!("invalid size {s:?}");
    let (negative, n, unit) = crate::util::c_number(s).ok_or_else(bad)?;
    if negative {
        return Err(bad());
    }
    let mul = match unit.to_ascii_lowercase().as_str() {
        "" => 1u64,
        "k" => 1 << 10,
        "m" => 1 << 20,
        "g" => 1 << 30,
        _ => return Err(bad()),
    };
    n.and_then(|n| n.checked_mul(mul))
        .with_context(|| format!("size {s:?} is too large"))
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FetchArgs {
    pub wants: Vec<Oid>,
    pub haves: Vec<Oid>,
    /// Client accepts `OFS_DELTA` entries.
    pub ofs_delta: bool,
    pub include_tag: bool,
    /// Refs the client may not see: `include_tag` skips these tags.
    pub hidden: Hidden,
    pub filter: Filter,
    pub deepen: Option<u32>,
    /// Depth counts from the client's current shallow boundary.
    pub deepen_relative: bool,
    pub client_shallows: Vec<Oid>,
}

/// An object to send, with where its stored entry is.
pub(crate) struct Planned {
    pub d: DenseId,
    pub loc: EntryLoc,
    pub emit: Emit,
}

/// How a planned object goes into the pack.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Emit {
    /// Its stored entry, as is.
    Stored,
    /// Re-encoded whole.
    Whole,
    /// Re-encoded as a delta against an object sent before it.
    Delta(DenseId),
}

#[derive(Default)]
pub(crate) struct Plan {
    /// In send order (storage order).
    pub objects: Vec<Planned>,
    pub shallow: Vec<Oid>,
    pub unshallow: Vec<Oid>,
    /// Rewrite deltas whose base precedes them in the pack as `OFS_DELTA`.
    pub ofs_delta: bool,
}

const UNINTERESTING: u8 = 1;
const QUEUED: u8 = 2;

/// Commits reachable from `wants` but not from `haves` (generation-ordered
/// walk), with the `shallow` commits taken as parentless.
pub(crate) fn walk(
    snap: &Snapshot,
    wants: &[DenseId],
    haves: &[DenseId],
    shallow: &RoaringBitmap,
) -> Result<Vec<DenseId>> {
    // The walk often reaches most of history, so flags by commit beat a map.
    let mut flags = vec![0u8; ix(snap.commit_end())];
    let mut heap = BinaryHeap::new();
    let mut interesting_queued = 0usize;
    let node = |d: DenseId| snap.links_of(d).context("commit missing from graph");

    for &h in haves {
        let n = node(h)?;
        let f = flag(&mut flags, n.num)?;
        if *f == 0 {
            *f = UNINTERESTING | QUEUED;
            heap.push(n);
        }
    }
    for &w in wants {
        let n = node(w)?;
        let f = flag(&mut flags, n.num)?;
        if *f == 0 {
            *f = QUEUED;
            interesting_queued += 1;
            heap.push(n);
        }
    }
    let mut send = Vec::new();
    while interesting_queued > 0 {
        let Some(n) = heap.pop() else { break };
        let f = flag(&mut flags, n.num)?;
        *f &= !QUEUED;
        let unint = *f & UNINTERESTING != 0;
        if !unint {
            interesting_queued -= 1;
            send.push(n.id);
        }
        // As with git's shallow grafts: the client lacks the history past
        // its boundary, so a have there does not cover it.
        if shallow.contains(n.id) {
            continue;
        }
        for &p in n.parents {
            if mark_parent(flag(&mut flags, p)?, unint, &mut interesting_queued) {
                heap.push(snap.links(p).context("commit missing from graph")?);
            }
        }
    }
    Ok(send)
}

fn flag(flags: &mut [u8], c: CommitNum) -> Result<&mut u8> {
    flags.get_mut(ix(c)).context("commit outside graph")
}

/// Flag `pf`, the flags of a parent of a commit walked as `unint`
/// (uninteresting) or not. Returns whether the parent is new to the walk and
/// so to be queued.
fn mark_parent(pf: &mut u8, unint: bool, interesting_queued: &mut usize) -> bool {
    if *pf == 0 {
        *pf = QUEUED | if unint { UNINTERESTING } else { 0 };
        if !unint {
            *interesting_queued += 1;
        }
        return true;
    }
    if unint && *pf & UNINTERESTING == 0 {
        *pf |= UNINTERESTING;
        if *pf & QUEUED != 0 {
            *interesting_queued -= 1;
        }
    }
    false
}

/// Commits to send, from the commit graph alone.
struct Selected {
    /// (commit, whole tree) in parents-first order; whole-tree commits are
    /// shallow boundaries.
    commits: Vec<(DenseId, bool)>,
    shallow: Vec<Oid>,
    unshallow: Vec<Oid>,
    shallow_mode: bool,
}

fn select_commits(snap: &Snapshot, args: &FetchArgs, want_commits: &[DenseId]) -> Result<Selected> {
    let have_commits: Vec<DenseId> = args
        .haves
        .iter()
        .filter_map(|h| snap.lookup_kind(h, Kind::Commit))
        .collect();

    let shallow_mode = args.deepen.is_some() || !args.client_shallows.is_empty();
    let client_shallow: RoaringBitmap = args
        .client_shallows
        .iter()
        .filter_map(|s| snap.lookup_kind(s, Kind::Commit))
        .collect();

    if let Some(depth) = args.deepen {
        let deepen = Deepen {
            snap,
            args,
            depth,
            haves: have_commits.iter().copied().collect(),
            client_shallow: &client_shallow,
        };
        let (commits, shallow, unshallow) = deepen.select(want_commits, &have_commits)?;
        return Ok(Selected {
            commits,
            shallow,
            unshallow,
            shallow_mode,
        });
    }
    let mut send = walk(snap, want_commits, &have_commits, &client_shallow)?;
    send.reverse(); // walk emits children first
    Ok(Selected {
        commits: send.into_iter().map(|d| (d, false)).collect(),
        shallow: Vec::new(),
        unshallow: Vec::new(),
        shallow_mode,
    })
}

/// Queue `d` at depth `dep` unless the walk has seen it.
fn enqueue(seen: &mut HashMap<DenseId, u32>, q: &mut VecDeque<DenseId>, d: DenseId, dep: u32) {
    if seen.insert(d, dep).is_none() {
        q.push_back(d);
    }
}

/// Commits to send (commit, whole tree), new shallow boundary commits and
/// commits to unshallow.
type DeepenOut = (Vec<(DenseId, bool)>, Vec<Oid>, Vec<Oid>);

/// A depth-limited selection of commits.
struct Deepen<'a> {
    snap: &'a Snapshot,
    args: &'a FetchArgs,
    depth: u32,
    haves: HashSet<DenseId>,
    client_shallow: &'a RoaringBitmap,
}

impl Deepen<'_> {
    /// The commits to send (commit, whole tree) in parents-first order, the
    /// new shallow boundary commits and the commits to unshallow.
    #[expect(clippy::indexing_slicing, reason = "every queued commit was seen")]
    fn select(&self, want_commits: &[DenseId], have_commits: &[DenseId]) -> Result<DeepenOut> {
        let (snap, args, depth) = (self.snap, self.args, self.depth);
        let (mut shallow, mut unshallow) = (Vec::new(), Vec::new());
        // (commit, depth); a commit at depth >= `depth` becomes a shallow
        // boundary. As in git, that holds for a root too: the client then
        // records it as shallow, though nothing lies past it.
        let mut seen: HashMap<DenseId, u32> = HashMap::new();
        let mut q = VecDeque::new();
        self.seed(want_commits, have_commits, &mut seen, &mut q)?;
        let mut visited = Vec::new();
        while let Some(d) = q.pop_front() {
            let dep = seen[&d];
            let node = snap.commit(d).context("commit missing")?;
            let is_client_shallow = self.client_shallow.contains(d);
            let client_has = self.haves.contains(&d) && !is_client_shallow;
            if client_has && args.deepen_relative {
                continue;
            }
            let at_limit = if args.deepen_relative {
                dep >= depth && dep > 0
            } else {
                dep >= depth
            };
            let boundary = at_limit && !node.parents.is_empty();
            if is_client_shallow && !at_limit {
                unshallow.push(snap.oid(d));
            }
            // Absolute depth is measured from the wants, so walk through
            // commits the client already has without sending them.
            if !client_has {
                visited.push((d, boundary));
            }
            if at_limit && !is_client_shallow && !client_has {
                shallow.push(snap.oid(d));
            }
            if boundary || at_limit {
                continue;
            }
            // In relative mode, walked new commits only descend into shallow
            // territory through the client's boundary commits.
            let descend = !args.deepen_relative || dep > 0 || is_client_shallow;
            self.queue_parents(node.parents, dep, descend, &mut seen, &mut q)?;
        }
        visited.sort_by_key(|(d, _)| snap.commit(*d).map_or(0, |c| c.generation));
        Ok((visited, shallow, unshallow))
    }

    /// Queue the commits where the walk starts.
    fn seed(
        &self,
        want_commits: &[DenseId],
        have_commits: &[DenseId],
        seen: &mut HashMap<DenseId, u32>,
        q: &mut VecDeque<DenseId>,
    ) -> Result<()> {
        if !self.args.deepen_relative {
            for &w in want_commits {
                enqueue(seen, q, w, 1);
            }
            return Ok(());
        }
        // New commits down to what the client has, then `depth` more
        // generations below its current shallow boundary.
        // The client has its shallow commits even if it sent no haves
        // (v0 clients don't in their first stateless request).
        let mut known = have_commits.to_vec();
        known.extend(self.client_shallow.iter());
        let send = walk(self.snap, want_commits, &known, self.client_shallow)?;
        for d in send.into_iter().chain(self.client_shallow.iter()) {
            enqueue(seen, q, d, 0);
        }
        Ok(())
    }

    /// Queue the parents of a commit at depth `dep` not seen yet.
    fn queue_parents(
        &self,
        parents: &[CommitNum],
        dep: u32,
        descend: bool,
        seen: &mut HashMap<DenseId, u32>,
        q: &mut VecDeque<DenseId>,
    ) -> Result<()> {
        for &p in parents {
            let p = self.snap.links(p).context("commit missing")?.id;
            if seen.contains_key(&p) {
                continue;
            }
            if !descend && (self.haves.contains(&p) || self.client_shallow.contains(p)) {
                continue;
            }
            let p_dep = if self.args.deepen_relative && !descend {
                0
            } else {
                dep + 1
            };
            seen.insert(p, p_dep);
            q.push_back(p);
        }
        Ok(())
    }
}

/// Plan a fetch. Planning reads objects only to walk trees and peel tags;
/// those reads load what is not local without waiting on it (see
/// [`machine`]). Everything else works on local metadata, on the blocking
/// pool.
pub(crate) async fn plan(snap: &Arc<Snapshot>, args: &FetchArgs) -> Result<Plan> {
    let filter = args.filter;
    let skip_kind = match filter {
        Filter::BlobNone => Some(Kind::Blob),
        Filter::TreeNone => Some(Kind::Tree),
        _ => None,
    };

    // Resolve wants.
    let mut want_commits = Vec::new();
    let mut extra: Vec<DenseId> = Vec::new(); // tags, explicitly wanted trees/blobs
    let wants = args
        .wants
        .iter()
        .map(|w| snap.lookup(w).with_context(|| format!("not our ref {w}")))
        .collect::<Result<_>>()?;
    let mut want_trees = Vec::new();
    for p in machine::peel(snap, wants).await? {
        extra.extend(&p.tags);
        let d = p.end.context("tag target missing")?;
        match snap.kind(d) {
            Kind::Commit => want_commits.push(d),
            Kind::Tree => want_trees.push(d),
            _ => extra.push(d),
        }
    }
    if !want_trees.is_empty() {
        let skip = if filter == Filter::BlobNone {
            Some(Kind::Blob)
        } else {
            None
        };
        extra.extend(machine::tree_closure(snap, want_trees, skip).await?.iter());
    }

    let (s2, a2) = (snap.clone(), args.clone());
    let sel =
        tokio::task::spawn_blocking(move || select_commits(&s2, &a2, &want_commits)).await??;

    // Boundary commits contribute their whole tree: all of them are walked
    // together, so a shallow fetch across merges walks each shared subtree
    // once.
    let roots: Vec<DenseId> = if skip_kind == Some(Kind::Tree) {
        Vec::new()
    } else {
        sel.commits
            .iter()
            .filter(|(_, full)| *full)
            .map(|&(c, _)| snap.commit(c).map(|n| n.tree).context("commit missing"))
            .collect::<Result<_>>()?
    };
    let closure = if roots.is_empty() {
        RoaringBitmap::new()
    } else {
        machine::tree_closure(snap, roots, skip_kind).await?
    };
    let tags = if args.include_tag {
        let tags = snap
            .meta
            .refs
            .iter()
            .filter(|(n, _)| n.starts_with("refs/tags/") && !args.hidden.hides(n));
        let tags = tags
            .filter_map(|(_, oid)| snap.lookup_kind(oid, Kind::Tag))
            .collect();
        machine::peel(snap, tags).await?
    } else {
        Vec::new()
    };
    let (s2, a2) = (snap.clone(), args.clone());
    tokio::task::spawn_blocking(move || assemble(&s2, &a2, sel, &closure, extra, tags)).await?
}

/// The object list of a plan, in storage order, and which objects to
/// re-encode.
fn assemble(
    snap: &Snapshot,
    args: &FetchArgs,
    sel: Selected,
    closure: &RoaringBitmap,
    extra: Vec<DenseId>,
    tags: Vec<machine::Peeled>,
) -> Result<Plan> {
    let filter = args.filter;
    let excluded = |d: DenseId| -> bool {
        match filter {
            Filter::None => false,
            Filter::BlobNone => snap.kind(d) == Kind::Blob,
            // As in git, a blob of exactly `n` bytes is left out too.
            Filter::BlobLimit(n) => snap.kind(d) == Kind::Blob && snap.size(d) >= n,
            Filter::TreeNone => matches!(snap.kind(d), Kind::Blob | Kind::Tree),
        }
    };
    let Selected {
        commits,
        shallow,
        unshallow,
        shallow_mode,
    } = sel;
    let mut in_pack = RoaringBitmap::new();
    let mut add = |d: DenseId| {
        if !excluded(d) {
            in_pack.insert(d);
        }
    };
    for &(c, full_tree) in &commits {
        if full_tree {
            add(c);
        } else {
            for &d in snap.commit(c).context("commit missing")?.segment {
                add(d);
            }
        }
    }
    for d in closure {
        add(d);
    }
    // Explicitly wanted objects are never filtered (promisor fetches).
    in_pack.extend(extra);
    for p in tags {
        // A missing target leaves the last tag reached as the end.
        let end = p.end.or(p.tags.last().copied());
        if end.is_some_and(|d| in_pack.contains(d)) {
            in_pack.extend(p.tags.into_iter().filter(|&t| !excluded(t)));
        }
    }

    // Id order is table order and storage order: one sequential pass over
    // the records, and entries sent with each delta's base before it.
    let mut objects = Vec::with_capacity(usize::try_from(in_pack.len()).unwrap_or_default());
    // No thin deltas: git's `index-pack --fix-thin` resolves every delta on
    // a base it has to add on one thread, which costs the client more than
    // the bytes they save. Of the objects that share a base outside the
    // pack, the first goes whole and the others as deltas against it, so
    // they resolve as one in-pack tree. Shallow and filtered fetches keep
    // many such objects: replay rebuilds them whole in bulk.
    let grouped = |kind: Kind| {
        !shallow_mode
            && (filter == Filter::None || (filter == Filter::BlobNone && kind == Kind::Tree))
    };
    let mut first_on_base: HashMap<DenseId, DenseId> = HashMap::new();
    for d in &in_pack {
        let loc = snap.entry_loc(d)?;
        let emit = match loc.base.filter(|&b| !in_pack.contains(b)) {
            None => Emit::Stored,
            Some(b) if grouped(snap.kind(d)) => match first_on_base.entry(b) {
                Entry::Vacant(e) => {
                    e.insert(d);
                    Emit::Whole
                }
                Entry::Occupied(e) => Emit::Delta(*e.get()),
            },
            Some(_) => Emit::Whole,
        };
        objects.push(Planned { d, loc, emit });
    }
    Ok(Plan {
        objects,
        shallow,
        unshallow,
        ofs_delta: args.ofs_delta,
    })
}

/// Objects per window: re-encoding within a window runs in parallel, and
/// windows are streamed in order.
const WINDOW: usize = 1024;
/// Packs with more objects than this read layers as a scan.
const SCAN_OBJECTS: usize = 10_000;
/// Re-encoded objects per parallel work unit.
const ENCODE_BLOCK: usize = 32;

/// A self-contained pack entry for `d`, from the entry cache or re-encoded.
fn full_entry_cached(snap: &Snapshot, d: DenseId) -> Result<Arc<Vec<u8>>> {
    let oid = snap.oid(d);
    if let Some(e) = snap.cache.entries.get(&oid) {
        return Ok(e);
    }
    let e = Arc::new(full_entry(snap.kind(d), &snap.read(d)?));
    snap.cache.entries.insert(oid, e.clone());
    Ok(e)
}

/// The entry of `d` as a delta against `base` (returned with it), or whole
/// if that is not smaller. Not cached: the base depends on what else the
/// fetch sends.
fn delta_entry(snap: &Snapshot, d: DenseId, base: DenseId) -> Result<Encoded> {
    let (target, from) = (snap.read(d)?, snap.read(base)?);
    Ok(match delta::encode(&from, &target, target.len()) {
        Some(dl) => (Arc::new(ref_delta_entry(&snap.oid(base), &dl)), Some(base)),
        None => (Arc::new(full_entry(snap.kind(d), &target)), None),
    })
}

/// A re-encoded entry, with its delta base if it is a delta.
type Encoded = (Arc<Vec<u8>>, Option<DenseId>);

/// Entry bytes past the one being written whose loads are kept in flight.
const AHEAD_BYTES: u64 = 32 << 20;

/// Where pack bytes go (e.g. the client connection).
pub(crate) type Sink = Box<dyn FnMut(&[u8]) -> Result<()> + Send>;

/// Stream the planned pack to `sink`.
pub(crate) async fn write_pack(snap: Arc<Snapshot>, plan: Plan, sink: Sink) -> Result<()> {
    machine::drive(PackWriter::new(snap, plan, sink)?).await
}

/// The pack being written.
struct Out {
    sink: Sink,
    format: ObjectFormat,
    hasher: PackHasher,
    buf: Vec<u8>,
    pos: u64,
    /// Pack offset of each object written so far (for `OFS_DELTA`). Stored
    /// deltas name their base by object id (`REF_DELTA`, 20 or 32 bytes); a base
    /// earlier in this pack can be named by its distance instead (1-4
    /// bytes), which is what git's own packs do.
    offsets: Option<HashMap<DenseId, u64>>,
}

impl Out {
    fn flush(&mut self) -> Result<()> {
        self.hasher.update(&self.buf);
        (self.sink)(&self.buf)?;
        self.buf.clear();
        Ok(())
    }

    fn written(&mut self, d: DenseId, start: usize) -> Result<()> {
        if let Some(o) = &mut self.offsets {
            o.insert(d, self.pos);
        }
        self.pos += u64::try_from(self.buf.len() - start)?;
        if self.buf.len() >= 1 << 16 {
            self.flush()?;
        }
        Ok(())
    }

    /// A self-contained entry.
    fn full(&mut self, d: DenseId, entry: &[u8]) -> Result<()> {
        let start = self.buf.len();
        self.buf.extend_from_slice(entry);
        self.written(d, start)
    }

    /// A stored entry, its base named by offset if already written.
    fn stored(&mut self, d: DenseId, base: Option<DenseId>, entry: &[u8]) -> Result<()> {
        let start = self.buf.len();
        match base.and_then(|b| self.offsets.as_ref()?.get(&b).copied()) {
            Some(off) => {
                let e = StoredEntry::parse(self.format, entry)?;
                ensure!(e.delta, "delta base on a full entry");
                encode_entry_header(OFS_DELTA, e.size, &mut self.buf);
                encode_ofs(self.pos - off, &mut self.buf);
                self.buf.extend_from_slice(e.zdata);
            }
            None => self.buf.extend_from_slice(entry),
        }
        self.written(d, start)
    }
}

/// Writes a planned pack as a [`Machine`](machine::Machine): entries go out
/// in plan order as soon as their bytes are local, with loads for the next
/// [`AHEAD_BYTES`] of entries in flight; objects to re-encode are rebuilt
/// first in bulk ([`Replay`](super::replay::Replay)) or, a few at a time,
/// once their delta chains are local.
pub(crate) struct PackWriter {
    snap: Arc<Snapshot>,
    plan: Plan,
    out: Out,
    header: Option<Vec<u8>>,
    mode: Access,
    replay: Option<super::replay::Replay>,
    /// Next object to write.
    next: usize,
    /// Objects before this have their loads requested.
    ahead: usize,
    /// Entry bytes in `next..ahead`.
    ahead_bytes: u64,
    /// What each object in `next..ahead` added to `ahead_bytes` (its entry,
    /// or the delta chain it needs loaded).
    charges: VecDeque<u64>,
    /// Loads requested for objects before the given index, by ticket; once
    /// done, their pins are held until the writer gets past that index.
    pending: HashMap<Ticket, usize>,
    held: Vec<(usize, Vec<crate::storage::Pinned>)>,
    /// Re-encoded entries not written yet.
    encoded: HashMap<DenseId, Encoded>,
}

impl PackWriter {
    pub(crate) fn new(snap: Arc<Snapshot>, mut plan: Plan, sink: Sink) -> Result<PackWriter> {
        let format = snap.format();
        let (hasher, header) = PackHasher::header(
            format,
            u32::try_from(plan.objects.len()).context("too many objects for a pack")?,
        );
        // A large pack streams most of the layers: read it as a scan so it
        // does not evict the pool's working set.
        let mode = if plan.objects.len() > SCAN_OBJECTS {
            Access::Scan
        } else {
            Access::Hot
        };
        // Many objects to re-encode (the boundary trees of a shallow fetch):
        // rebuild them all in one replay of their delta chains, and send them
        // first (stored deltas after them can then name them by offset).
        let mut replay = None;
        let whole: RoaringBitmap = plan
            .objects
            .iter()
            .filter(|p| p.emit == Emit::Whole)
            .map(|p| p.d)
            .collect();
        if whole.len() >= super::replay::replay_min() {
            replay = Some(super::replay::Replay::new(&snap, &whole)?);
            plan.objects.retain(|p| p.emit != Emit::Whole);
        }
        let header_len = u64::try_from(header.len())?;
        let offsets = plan
            .ofs_delta
            .then(|| HashMap::with_capacity(plan.objects.len()));
        Ok(PackWriter {
            out: Out {
                sink,
                format,
                hasher,
                buf: Vec::with_capacity(1 << 16),
                pos: header_len,
                offsets,
            },
            header: Some(header),
            snap,
            plan,
            mode,
            replay,
            next: 0,
            ahead: 0,
            ahead_bytes: 0,
            charges: VecDeque::new(),
            pending: HashMap::new(),
            held: Vec::new(),
            encoded: HashMap::new(),
        })
    }

    /// Request loads for the entries ahead (in batches, once half of the
    /// window is used up).
    fn read_ahead(&mut self, waits: &mut Waits) -> Result<()> {
        if self.ahead > self.next && self.ahead_bytes >= AHEAD_BYTES / 2 {
            return Ok(());
        }
        let mut need = Vec::new();
        while let Some(&Planned { d, loc, emit }) = self.plan.objects.get(self.ahead) {
            if self.ahead != self.next && self.ahead_bytes >= AHEAD_BYTES {
                break;
            }
            let charge = if emit == Emit::Stored {
                need.extend(
                    self.snap
                        .locate(d)
                        .0
                        .layer
                        .entries_missing(loc.off, loc.off + u64::from(loc.len)),
                );
                u64::from(loc.len)
            } else {
                let chain = self.chains_missing(d, emit)?;
                let bytes = chain.iter().map(NotResident::len).sum();
                need.extend(chain);
                bytes
            };
            self.ahead_bytes += charge;
            self.charges.push_back(charge);
            self.ahead += 1;
        }
        if !need.is_empty() {
            let t = waits.park_as(need, self.mode);
            self.pending.insert(t, self.ahead);
        }
        Ok(())
    }

    /// Re-encode the objects of the window at `next` whose delta chains are
    /// local, in parallel on the CPU pool.
    fn encode(&mut self) -> Result<()> {
        let end = (self.next + WINDOW).min(self.ahead);
        let mut todo = Vec::new();
        let window = self
            .plan
            .objects
            .iter()
            .skip(self.next)
            .take(end.saturating_sub(self.next));
        for &Planned { d, emit, .. } in window {
            if emit != Emit::Stored
                && !self.encoded.contains_key(&d)
                && self.chains_missing(d, emit)?.is_empty()
            {
                todo.push((d, emit));
            }
        }
        let snap = &self.snap;
        // A chain evicted since it was checked is skipped (and waited for).
        let blocks = crate::util::par_blocks(todo.len().div_ceil(ENCODE_BLOCK), |b| {
            encode_block(snap, todo.chunks(ENCODE_BLOCK).nth(b).unwrap_or_default())
        });
        for b in blocks {
            self.encoded.extend(b?);
        }
        Ok(())
    }

    /// The not-local parts of the chains that re-encoding `d` reads.
    fn chains_missing(&self, d: DenseId, emit: Emit) -> Result<Vec<NotResident>> {
        let mut need = self.snap.missing(d)?;
        if let Emit::Delta(base) = emit {
            need.extend(self.snap.missing(base)?);
        }
        Ok(need)
    }

    fn finish(&mut self) -> Result<()> {
        self.out.flush()?;
        let trailer = self.out.hasher.finish();
        (self.out.sink)(&trailer)
    }
}

/// Re-encode `block`, skipping objects whose chains are not local now.
fn encode_block(snap: &Snapshot, block: &[(DenseId, Emit)]) -> Result<Vec<(DenseId, Encoded)>> {
    let mut out = Vec::new();
    for &(d, emit) in block {
        let entry = match emit {
            Emit::Delta(base) => delta_entry(snap, d, base),
            _ => full_entry_cached(snap, d).map(|e| (e, None)),
        };
        match entry {
            Ok(e) => out.push((d, e)),
            Err(e) if NotResident::find(&e).is_some() => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

/// What became of the object at `next`.
enum Progress {
    Written,
    /// Nothing to wait for: try again.
    Retry,
    /// Waiting for its bytes.
    Parked,
}

impl PackWriter {
    /// Write the re-encoded entry of `d`, once its chains are local.
    fn write_reencoded(&mut self, d: DenseId, emit: Emit, waits: &mut Waits) -> Result<Progress> {
        if !self.encoded.contains_key(&d) {
            self.encode()?;
        }
        let Some((e, base)) = self.encoded.remove(&d) else {
            // Not local (yet, or any more): wait for its chains.
            let need = self.chains_missing(d, emit)?;
            if need.is_empty() {
                return Ok(Progress::Retry);
            }
            let t = waits.park(need);
            self.pending.insert(t, self.next + 1);
            return Ok(Progress::Parked);
        };
        self.out.stored(d, base, &e)?;
        Ok(Progress::Written)
    }

    /// Copy the stored entry of `d`, once its bytes are local.
    fn write_stored(&mut self, d: DenseId, loc: EntryLoc, waits: &mut Waits) -> Result<Progress> {
        let out = &mut self.out;
        let read = self
            .snap
            .with_entry_at(d, &loc, self.mode, |entry| out.stored(d, loc.base, entry));
        match read {
            Ok(r) => {
                r?;
                Ok(Progress::Written)
            }
            Err(e) => {
                let Some(nr) = NotResident::find(&e) else {
                    return Err(e);
                };
                let t = waits.park(vec![nr.clone()]);
                self.pending.insert(t, self.next + 1);
                Ok(Progress::Parked)
            }
        }
    }
}

impl machine::Machine for PackWriter {
    type Output = ();

    fn run(&mut self, waits: &mut Waits) -> Result<Option<()>> {
        if let Some(h) = self.header.take() {
            (self.out.sink)(&h)?;
        }
        if let Some(r) = &mut self.replay {
            let out = &mut self.out;
            if !r.step(waits, &mut |d, entry| out.full(d, entry))? {
                return Ok(None);
            }
            self.replay = None;
        }
        loop {
            self.read_ahead(waits)?;
            let Some(&Planned { d, loc, emit }) = self.plan.objects.get(self.next) else {
                self.finish()?;
                return Ok(Some(()));
            };
            let progress = if emit == Emit::Stored {
                self.write_stored(d, loc, waits)?
            } else {
                self.write_reencoded(d, emit, waits)?
            };
            match progress {
                Progress::Written => {}
                Progress::Retry => continue,
                Progress::Parked => return Ok(None),
            }
            self.ahead_bytes -= self.charges.pop_front().unwrap_or(0);
            self.next += 1;
            if self.held.first().is_some_and(|(end, _)| *end <= self.next) {
                let next = self.next;
                self.held.retain(|(end, _)| *end > next);
            }
        }
    }

    fn resume(&mut self, ticket: Ticket, pins: Vec<crate::storage::Pinned>) {
        if let Some(end) = self.pending.remove(&ticket) {
            self.held.push((end, pins));
            self.held.sort_by_key(|(end, _)| *end);
        } else if let Some(r) = &mut self.replay {
            // Replay groups pin their own bytes.
            r.resume(ticket);
        }
    }
}
