//! The layer file format.
//!
//! A layer is an immutable file holding a set of objects plus derived data
//! (commit graph, per-commit segments). Layers are written once, uploaded to
//! the object store, and read through the buffer pool from a local copy or
//! straight from the object store.
//!
//! ```text
//! data region        concatenated pack entries (full or REF_DELTA), in
//!                    storage order: a delta's base comes before it
//! object table       fixed-size records, one per entry, in storage order
//! oid index          (oid, local index) per object, sorted by (kind, oid),
//!                    in 4 KiB pages that no entry crosses
//! external oids      oids referenced by this layer but stored elsewhere
//! commit section     commits in topological order (parents first) with
//!                    tree, parents, time and segment members
//! index region       every 256th oid of each kind in the oid index, the
//!                    kind runs and id runs (below), then a Bloom filter
//!                    over all oids: what opening a layer needs, in one
//!                    read next to the footer (two reads when remote);
//!                    then the SHA-256 of every chunk of the file before
//!                    the index region
//! footer             section offsets, counts, crc32 of the index region
//!                    and footer, the object format, "ENRLAYR2"
//! ```
//!
//! The chunk hashes let a reader verify any chunk on its own: bytes read by
//! range from the object store, and chunks of a linked local copy.
//!
//! Inside a layer every object has a *local index*: its position in the
//! object table, which is also the order of the entries. Cross-object
//! references are encoded as a [`Ref`]: a local index, an index into the
//! external oid table, or none. *Kind runs* give the kind of each range of
//! local indexes: a pushed layer has one run per kind, a merged layer one
//! per kind of each source.
//!
//! Every object also has an *id*, the same on every node: the layer's
//! manifest entry gives it a range (`first_id`, `id_span`). The file holds
//! ids relative to `first_id`, as *id runs*: a pushed layer's file is
//! written before its range is assigned, and has one run, so its ids are
//! `first_id + local`. A merged layer keeps the ids of the objects it took
//! from its sources, in their storage order, so its ids also go up with
//! the local index, and its runs record where they skip the ids of dropped
//! copies. In both, id order is storage order, and a delta's base has a
//! lower id than the delta.

use std::io::{Seek, Write};
use std::ops::{Index, IndexMut};
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use sha2::Digest;
use zerocopy::little_endian::{I64, U32, U64};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::git::oid::{Kind, ObjectFormat, Oid};
use crate::storage::files::{self, FileWriter};
use crate::storage::pool::{Access, Chunk, PagedFile, must, to_u64, to_usize};
use crate::storage::spill::{OidIndex, TempTable, Workspace};
use crate::storage::store::{Release, Sealed};
use crate::storage::tiered::{ChunkHashes, NotResident, Pinned};

mod lookup;

const CHUNK_HASH: usize = 32;
/// The end of a layer. Version 1 layers, from before SHA-256, are not read.
const END_MAGIC: &[u8; 8] = b"ENRLAYR2";

pub(super) use crate::git::cast::ix as widen;

/// How a footer names an object format.
fn format_code(format: ObjectFormat) -> u32 {
    match format {
        ObjectFormat::Sha1 => 1,
        ObjectFormat::Sha256 => 2,
    }
}

/// Where a layer's fixed-size items are, which depends on how long its ids
/// are. Every id in a layer is of its repository's format.
#[derive(Clone, Copy, Debug)]
pub(super) struct Geometry {
    format: ObjectFormat,
    /// A table record: an id and a [`RecordTail`].
    record: usize,
    /// An oid index entry: an id and a local index.
    pub(super) entry: usize,
    /// Oid index entries on each index page. No entry crosses a page, so a
    /// lookup reads one page for an oid and its local index.
    pub(super) page_entries: u32,
}

impl Geometry {
    const fn of(format: ObjectFormat) -> Geometry {
        let oid = format.len();
        let entry = oid + size_of::<U32>();
        Geometry {
            format,
            record: oid + size_of::<RecordTail>(),
            entry,
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "a page holds a few hundred entries (no From in a const fn)"
            )]
            page_entries: (INDEX_PAGE / entry) as u32,
        }
    }

    pub(super) fn format(&self) -> ObjectFormat {
        self.format
    }

    /// The length of an id.
    fn oid(&self) -> usize {
        self.format.len()
    }

    /// The offset of table record `local`.
    fn record_off(&self, local: u32) -> u64 {
        u64::from(local) * to_u64(self.record)
    }

    /// The offset of oid index entry `i`.
    fn oid_entry_off(&self, i: u32) -> u64 {
        u64::from(i / self.page_entries) * to_u64(INDEX_PAGE)
            + u64::from(i % self.page_entries) * to_u64(self.entry)
    }
}

/// One value per object kind. Indexing by [`Kind`] cannot go out of range.
#[derive(Clone, Copy, Debug, Default)]
struct PerKind<T>([T; 4]);

impl<T> Index<Kind> for PerKind<T> {
    type Output = T;

    fn index(&self, kind: Kind) -> &T {
        let [commit, tree, blob, tag] = &self.0;
        match kind {
            Kind::Commit => commit,
            Kind::Tree => tree,
            Kind::Blob => blob,
            Kind::Tag => tag,
        }
    }
}

impl<T> IndexMut<Kind> for PerKind<T> {
    fn index_mut(&mut self, kind: Kind) -> &mut T {
        let [commit, tree, blob, tag] = &mut self.0;
        match kind {
            Kind::Commit => commit,
            Kind::Tree => tree,
            Kind::Blob => blob,
            Kind::Tag => tag,
        }
    }
}

/// Where each section of a layer file is, and how many items it holds.
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct Footer {
    table_off: U64,
    oids_off: U64,
    ext_off: U64,
    commits_off: U64,
    commits_len: U64,
    index_off: U64,
    counts: [U32; 4],
    ext_count: U32,
    commit_count: U32,
    kind_runs: U32,
    id_runs: U32,
    id_span: U32,
    /// The format of every id in the layer (see [`format_code`]).
    object_format: U32,
}

/// The end of a layer file.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct Tail {
    footer: Footer,
    /// The crc32 of the index region and the footer.
    crc: U32,
    magic: [u8; 8],
}

const TAIL: usize = size_of::<Tail>();

impl Tail {
    /// The tail of `file`, and where it starts. The magic is not checked.
    fn read(file: &PagedFile) -> Result<(u64, Tail)> {
        let at = file
            .len()
            .checked_sub(to_u64(TAIL))
            .context("layer too small")?;
        let mut tail = Tail::new_zeroed();
        file.read_direct(at, tail.as_mut_bytes())?;
        Ok((at, tail))
    }
}

/// A commit as [`LayerBuilder::add_commit`] keeps it until the layer is
/// written, followed by the oids of its parents, then of its members.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct StagedCommit {
    oid: Oid,
    tree: Oid,
    time: I64,
    parents: U32,
    members: U32,
}

/// A commit in the commit section, followed by its parents' refs, then the
/// number of its members and their refs.
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct CommitHead {
    local: U32,
    tree: U32,
    time: I64,
    parents: U32,
}

/// Encoded reference to an object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ref(pub u32);

impl Ref {
    pub(crate) const NONE: Ref = Ref(u32::MAX);
    const EXT: u32 = 1 << 31;

    pub fn local(i: u32) -> Ref {
        debug_assert!(i < Self::EXT);
        Ref(i)
    }
    pub(crate) fn external(i: u32) -> Ref {
        Ref(i | Self::EXT)
    }
    pub fn is_none(self) -> bool {
        self == Self::NONE
    }
    pub fn decode(self) -> RefTarget {
        if self.is_none() {
            RefTarget::None
        } else if self.0 & Self::EXT != 0 {
            RefTarget::External(self.0 & !Self::EXT)
        } else {
            RefTarget::Local(self.0)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RefTarget {
    None,
    Local(u32),
    External(u32),
}

/// An object to be written into a layer.
pub(crate) struct NewObject<'a> {
    pub oid: Oid,
    pub kind: Kind,
    /// Uncompressed size.
    pub size: u64,
    /// Stored pack entry bytes (full or `REF_DELTA`).
    pub entry: &'a [u8],
    /// Delta base, if `entry` is a `REF_DELTA`.
    pub base: Option<Oid>,
    /// Commit whose tree contains `base` (guarantees base availability).
    pub base_ctx: Option<Oid>,
    /// Delta chain depth (0 = full).
    pub depth: u8,
    /// Its id, in a merged layer (see [`LayerBuilder::set_ids`]).
    pub id: Option<u32>,
}

pub(crate) struct NewCommit {
    pub oid: Oid,
    pub tree: Oid,
    pub parents: Vec<Oid>,
    pub time: i64,
    /// Trees and blobs introduced relative to the parents (path diff).
    pub members: Vec<Oid>,
}

/// Builds a layer file. Entry bytes are buffered per lane in memory; once a
/// layer gets large they are spilled to temporary files. Per-object and
/// per-commit metadata live in [`TempTable`]s, and the oid -> local index map
/// is a sorted [`OidIndex`], so building a layer of any size needs bounded
/// memory. Small layers (the common push) never touch the filesystem until
/// written.
pub(crate) struct LayerBuilder {
    geometry: Geometry,
    objects: TempTable<ObjRec>,
    /// Serialized commits (see `add_commit`), in insertion order.
    commits: TempTable<u8>,
    commit_count: u32,
    /// Entry bytes by lane (see [`write_data`](Self::write_data)).
    spill: Vec<Spill>,
    spill_len: Vec<u64>,
    ws: Workspace,
    buffered: usize,
    /// The id range of a merged layer (see [`set_ids`](Self::set_ids)).
    ids: Option<std::ops::Range<u32>>,
    /// The last id added to a merged layer.
    last_id: Option<u32>,
    /// A merged layer's commit graph (see [`set_graph`](Self::set_graph)).
    graph: Vec<u8>,
}

/// Layers smaller than this are built in memory.
const MEMORY_LAYER: u64 = 16 << 20;

/// Bytes buffered in memory before spilling to disk.
const SPILL_THRESHOLD: usize = 32 << 20;

/// A kind's entry bytes: in memory, or in an unnamed scratch file.
enum Spill {
    Mem(Vec<u8>),
    File(std::io::BufWriter<files::ScratchHandle>),
}

#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct ObjRec {
    size: u64,
    /// Offset within the lane's spill file.
    off: u64,
    len: u32,
    kind: u8,
    depth: u8,
    has_base: u8,
    has_ctx: u8,
    oid: Oid,
    base: Oid,
    ctx: Oid,
    /// The object's id in a merged layer (`NO_ID` otherwise).
    id: u32,
}

const NO_ID: u32 = u32::MAX;

/// An oid index entry with its kind, while the index is sorted.
#[derive(Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct KindOid {
    local: u32,
    kind: u8,
    _pad: [u8; 3],
    oid: Oid,
}

/// A run of local indexes, and the kind (as its code) or id of its first.
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct Run {
    local: U32,
    value: U32,
}

impl LayerBuilder {
    /// A builder of a layer of `format` ids; callers add each oid once (it
    /// keeps no per-object state in memory to check).
    pub(crate) fn new(ws: &Workspace, format: ObjectFormat) -> Self {
        LayerBuilder {
            geometry: Geometry::of(format),
            objects: ws.table(),
            commits: ws.table(),
            commit_count: 0,
            spill: Kind::ALL.map(|_| Spill::Mem(Vec::new())).into(),
            spill_len: vec![0; Kind::ALL.len()],
            ws: ws.clone(),
            buffered: 0,
            ids: None,
            last_id: None,
            graph: Vec::new(),
        }
    }

    /// Store `graph`, a merged layer's commit graph in its ids, after the
    /// commit section: opening the layer loads it, so a view of the layer
    /// needs no lookups (see [`Layer::graph`]).
    pub(crate) fn set_graph(&mut self, graph: Vec<u8>) {
        self.graph = graph;
    }

    /// Make this a merged layer covering ids `range`: every entry is added
    /// with its id, in id order, which the file records.
    pub(crate) fn set_ids(&mut self, range: std::ops::Range<u32>) {
        debug_assert!(self.objects.is_empty());
        self.ids = Some(range);
        self.spill = vec![Spill::Mem(Vec::new())];
        self.spill_len = vec![0];
    }

    /// The lane of an entry of `kind`: one per kind in a pushed layer (see
    /// [`write_data`](Self::write_data)), one in all in a merged layer.
    fn lane(&self, kind: Kind) -> usize {
        if self.ids.is_some() {
            0
        } else {
            usize::from(kind.code() - 1)
        }
    }

    /// Move all in-memory buffers to spill files.
    fn spill_to_disk(&mut self) -> Result<()> {
        for lane in &mut self.spill {
            if let Spill::Mem(buf) = lane {
                // Blocks while the budget is full: runs on blocking threads.
                let file = self.ws.budget().create_blocking(self.ws.dir())?;
                let mut w = std::io::BufWriter::with_capacity(1 << 20, file);
                w.write_all(buf)?;
                *lane = Spill::File(w);
            }
        }
        self.buffered = 0;
        Ok(())
    }

    /// Add an object.
    pub(crate) fn add(
        &mut self,
        NewObject {
            oid,
            kind,
            size,
            entry,
            base,
            base_ctx,
            depth,
            id,
        }: NewObject,
    ) -> Result<()> {
        match (&self.ids, id) {
            (Some(r), Some(id)) => {
                ensure!(r.contains(&id), "id {id} outside the layer's range {r:?}");
                ensure!(
                    self.last_id.is_none_or(|last| last < id),
                    "id {id} added after a higher one"
                );
                self.last_id = Some(id);
            }
            (None, None) => {}
            _ => anyhow::bail!("ids are given for a merged layer's entries, and only for those"),
        }
        let lane = self.lane(kind);
        match must(self.spill.get_mut(lane), "lane") {
            Spill::Mem(buf) => {
                buf.extend_from_slice(entry);
                self.buffered += entry.len();
            }
            Spill::File(w) => w.write_all(entry)?,
        }
        if self.buffered > SPILL_THRESHOLD {
            self.spill_to_disk()?;
        }
        let len = must(self.spill_len.get_mut(lane), "lane");
        let off = *len;
        *len += to_u64(entry.len());
        self.objects.push(ObjRec {
            size,
            off,
            len: entry.len().try_into().context("entry too large")?,
            kind: kind.code(),
            depth,
            has_base: u8::from(base.is_some()),
            has_ctx: u8::from(base_ctx.is_some()),
            oid,
            base: base.unwrap_or(Oid::ZERO),
            ctx: base_ctx.unwrap_or(Oid::ZERO),
            id: id.unwrap_or(NO_ID),
        });
        Ok(())
    }

    /// Commits must be added in topological order (parents first).
    pub(crate) fn add_commit(&mut self, c: &NewCommit) -> Result<()> {
        let head = StagedCommit {
            oid: c.oid,
            tree: c.tree,
            time: c.time.into(),
            parents: u32::try_from(c.parents.len())?.into(),
            members: u32::try_from(c.members.len())?.into(),
        };
        self.commits.extend_from_slice(head.as_bytes());
        self.commits.extend_from_slice(c.parents.as_bytes());
        self.commits.extend_from_slice(c.members.as_bytes());
        self.commit_count += 1;
        Ok(())
    }

    pub(crate) fn object_count(&self) -> usize {
        self.objects.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.objects.is_empty() && self.commit_count == 0
    }

    /// Finish the layer: built in memory when small, else written to a
    /// temporary file.
    pub(crate) fn finish(self) -> Result<Sealed> {
        if self.approx_size() < MEMORY_LAYER {
            let mut out = Vec::new();
            let written = self.write_to(&mut out)?;
            return Ok(Sealed::new(written, files::Scratch::Mem(out.into())));
        }
        // No fsync here: local copies are never the durable copy.
        let pool = self.ws.pool().clone();
        let mut f = FileWriter::create(self.ws.dir(), self.ws.budget())?;
        let written = self.write_to(&mut f)?;
        Ok(Sealed::new(
            written,
            files::Scratch::File(Arc::new(files::ScratchFile::open(&pool, f.finish()?)?)),
        ))
    }

    /// Estimated serialized size, to decide between memory and a file.
    pub(crate) fn approx_size(&self) -> u64 {
        self.spill_len.iter().sum::<u64>()
            + to_u64(self.objects.len()) * to_u64(self.geometry.record + self.geometry.entry + 40)
    }

    /// Serialize the layer.
    pub(crate) fn write_to<W: Write>(mut self, out: W) -> Result<Written> {
        // Table order is storage order (see `write_data`); local index =
        // table position. Records carry their data offsets, so every later
        // pass is sequential.
        let mut order = std::mem::replace(&mut self.objects, self.ws.table());
        if self.ids.is_none() {
            order.sort_by_key(|o| (o.kind, o.off));
        }
        let count = u32::try_from(order.len()).context("too many objects for a layer")?;
        let mut refs = RefEncoder {
            local: OidIndex::build(
                &self.ws,
                (0..count).zip(order.iter()).map(|(pos, o)| (o.oid, pos)),
            ),
            ext: Vec::new(),
            ext_of: foldhash::HashMap::default(),
        };

        let mut w = CrcWriter::new(out, super::tiered::chunk_bytes());
        let lane_off = self.write_data(&mut w)?;

        let table_off = w.pos;
        let mut table = self.write_table(&mut w, &order, &mut refs, &lane_off)?;
        drop(order);
        let oids_off = w.pos;
        let samples = write_oids(self.geometry, &mut w, &mut table.by_oid)?;

        // Encode the commit section first so the external table is complete.
        let commit_buf = self.encode_commits(&mut refs)?;

        let ext_off = w.pos;
        for o in &refs.ext {
            w.write_all(o.as_bytes())?;
        }
        let commits_off = w.pos;
        for chunk in commit_buf.chunks(1 << 20) {
            w.write_all(&chunk)?;
        }
        w.write_all(&std::mem::take(&mut self.graph))?;

        let index_off = w.pos;
        let hashes = w.end_chunks();
        w.write_all(&samples)?;
        w.write_all(table.kind_runs.as_bytes())?;
        w.write_all(table.id_runs.as_bytes())?;
        for word in &table.bloom.bits {
            w.write_all(&word.to_le_bytes())?;
        }
        for h in &hashes {
            w.write_all(h)?;
        }

        let id_span = self.ids.as_ref().map_or(count, |r| r.end - r.start);
        let footer = Footer {
            table_off: table_off.into(),
            oids_off: oids_off.into(),
            ext_off: ext_off.into(),
            commits_off: commits_off.into(),
            commits_len: to_u64(commit_buf.len()).into(),
            index_off: index_off.into(),
            counts: table.counts.0.map(U32::new),
            ext_count: u32::try_from(refs.ext.len())?.into(),
            commit_count: self.commit_count.into(),
            kind_runs: u32::try_from(table.kind_runs.len())?.into(),
            id_runs: u32::try_from(table.id_runs.len())?.into(),
            id_span: id_span.into(),
            object_format: format_code(self.geometry.format).into(),
        };
        let mut crc = must(w.tail.take(), "tail crc");
        crc.update(footer.as_bytes());
        let tail = Tail {
            footer,
            crc: crc.finalize().into(),
            magic: *END_MAGIC,
        };
        w.write_all(tail.as_bytes())?;
        w.inner.flush()?;
        Ok(Written {
            root: w.root.finalize().into(),
            bytes: w.pos,
            meta_bytes: w.pos - table_off,
        })
    }

    /// The data region: the lanes in order, each in insertion order. A
    /// pushed layer has a lane per kind (commits, trees, blobs, tags):
    /// ingest adds a commit's new objects after it, so each lane is in
    /// topological order. A merged layer has one lane: its sources' entries
    /// in their storage order. Returns where each lane starts.
    fn write_data<W: Write>(&mut self, w: &mut CrcWriter<W>) -> Result<Vec<u64>> {
        let mut lane_off = Vec::with_capacity(self.spill.len());
        for (lane, &len) in std::mem::take(&mut self.spill)
            .into_iter()
            .zip(&self.spill_len)
        {
            lane_off.push(w.pos);
            match lane {
                Spill::Mem(buf) => w.write_all(&buf)?,
                Spill::File(sp) => {
                    let mut f = sp
                        .into_inner()
                        .map_err(std::io::IntoInnerError::into_error)?;
                    f.seek(std::io::SeekFrom::Start(0))?;
                    let n = std::io::copy(&mut f, w)?;
                    ensure!(n == len, "spill file size mismatch");
                }
            }
        }
        Ok(lane_off)
    }

    /// The object table, in table order, with what the oid index, the
    /// index region and the footer need.
    fn write_table<W: Write>(
        &self,
        w: &mut CrcWriter<W>,
        order: &TempTable<ObjRec>,
        refs: &mut RefEncoder,
        lane_off: &[u64],
    ) -> Result<Table> {
        let mut t = Table {
            counts: PerKind::default(),
            kind_runs: Vec::new(),
            id_runs: Vec::new(),
            bloom: Bloom::new(to_u64(order.len())),
            by_oid: self.ws.table(),
        };
        for (local, o) in (0u32..).zip(order.iter()) {
            let kind = Kind::from_code(o.kind).context("bad kind")?;
            if t.kind_runs
                .last()
                .is_none_or(|r| r.value.get() != u32::from(o.kind))
            {
                t.kind_runs.push(Run {
                    local: local.into(),
                    value: u32::from(o.kind).into(),
                });
            }
            let id = self.ids.as_ref().map_or(local, |r| o.id - r.start);
            if t.id_runs
                .last()
                .is_none_or(|r| r.value.get() + (local - r.local.get()) != id)
            {
                t.id_runs.push(Run {
                    local: local.into(),
                    value: id.into(),
                });
            }
            t.counts[kind] += 1;
            t.bloom.insert(&o.oid);
            t.by_oid.push(KindOid {
                local,
                kind: o.kind,
                _pad: [0; 3],
                oid: o.oid,
            });
            let base = if o.has_base != 0 {
                refs.enc(&o.base)?
            } else {
                Ref::NONE
            };
            // Readers replay and send in storage order, bases first.
            ensure!(
                !matches!(base.decode(), RefTarget::Local(b) if b >= local),
                "delta stored before its base"
            );
            let ctx = if o.has_ctx != 0 {
                refs.enc(&o.ctx)?
            } else {
                Ref::NONE
            };
            ensure!(
                o.oid.format() == self.geometry.format,
                "{} id {} in a {} layer",
                o.oid.format(),
                o.oid,
                self.geometry.format
            );
            let rec = RecordTail {
                off: (must(lane_off.get(self.lane(kind)), "lane") + o.off).into(),
                len: o.len.into(),
                size: o.size.into(),
                base: base.0.into(),
                ctx: ctx.0.into(),
                depth: o.depth,
                _pad: [0; 3],
            };
            write_item(w, o.oid.as_bytes(), rec.as_bytes())?;
        }
        Ok(t)
    }

    /// The commit section: each staged commit with its oids as refs.
    fn encode_commits(&self, refs: &mut RefEncoder) -> Result<TempTable<u8>> {
        let mut commit_buf: TempTable<u8> = self.ws.table();
        let mut rest = std::io::BufReader::with_capacity(1 << 16, self.commits.reader());
        let (mut enc, mut staged, mut oids) = (Vec::new(), StagedCommit::new_zeroed(), Vec::new());
        for _ in 0..self.commit_count {
            std::io::Read::read_exact(&mut rest, staged.as_mut_bytes())?;
            let (np, nm) = (staged.parents.get(), staged.members.get());
            oids.resize(widen(np) + widen(nm), Oid::ZERO);
            std::io::Read::read_exact(&mut rest, oids.as_mut_bytes())?;
            let (parents, members) = must(oids.split_at_checked(widen(np)), "staged commit size");
            let local = refs
                .local
                .get(&staged.oid)
                .context("commit object missing from layer")?;
            let head = CommitHead {
                local: local.into(),
                tree: refs.enc(&staged.tree)?.0.into(),
                time: staged.time,
                parents: np.into(),
            };
            enc.clear();
            enc.extend_from_slice(head.as_bytes());
            for p in parents {
                enc.extend_from_slice(U32::new(refs.enc(p)?.0).as_bytes());
            }
            enc.extend_from_slice(U32::new(nm).as_bytes());
            for m in members {
                enc.extend_from_slice(U32::new(refs.enc(m)?.0).as_bytes());
            }
            commit_buf.extend_from_slice(&enc);
        }
        Ok(commit_buf)
    }
}

/// The oid index: `by_oid` sorted by (kind, oid), with each page padded
/// to [`INDEX_PAGE`]. Returns every `SAMPLE`th oid of each kind.
fn write_oids<W: Write>(
    g: Geometry,
    w: &mut CrcWriter<W>,
    by_oid: &mut TempTable<KindOid>,
) -> Result<Vec<u8>> {
    by_oid.sort_by_key(|e| (e.kind, e.oid));
    let mut samples = Vec::new();
    let (mut kind, mut nth) = (0u8, 0u32);
    for (i, e) in (1u32..).zip(by_oid.iter()) {
        if e.kind != kind {
            (kind, nth) = (e.kind, 0);
        }
        if nth.is_multiple_of(SAMPLE) {
            samples.extend_from_slice(e.oid.as_bytes());
        }
        nth += 1;
        write_item(w, e.oid.as_bytes(), U32::new(e.local).as_bytes())?;
        if i.is_multiple_of(g.page_entries) {
            w.write_all(must(
                [0; INDEX_PAGE].get(..INDEX_PAGE % g.entry),
                "page padding",
            ))?;
        }
    }
    Ok(samples)
}

/// Write an item of `id` then `rest` (a record or an index entry) in one
/// call: each call to `w` hashes what it is given.
fn write_item<W: Write>(w: &mut CrcWriter<W>, id: &[u8], rest: &[u8]) -> Result<()> {
    let mut b = [0u8; enroute_shared::oid::MAX_OID_LEN + size_of::<RecordTail>()];
    let n = id.len() + rest.len();
    let (head, tail) = must(b.split_at_mut_checked(id.len()), "item buffer");
    head.copy_from_slice(id);
    must(tail.get_mut(..rest.len()), "item buffer").copy_from_slice(rest);
    w.write_all(must(b.get(..n), "item buffer"))?;
    Ok(())
}

/// What the index region and footer need from the object table.
struct Table {
    counts: PerKind<u32>,
    kind_runs: Vec<Run>,
    id_runs: Vec<Run>,
    bloom: Bloom,
    /// Every object's oid index entry, in table order.
    by_oid: TempTable<KindOid>,
}

struct RefEncoder {
    local: OidIndex,
    ext: Vec<Oid>,
    ext_of: foldhash::HashMap<Oid, u32>,
}

impl RefEncoder {
    fn enc(&mut self, o: &Oid) -> Result<Ref> {
        if let Some(l) = self.local.get(o) {
            return Ok(Ref::local(l));
        }
        let next = u32::try_from(self.ext.len()).context("too many external oids")?;
        let i = *self.ext_of.entry(*o).or_insert(next);
        if i == next {
            self.ext.push(*o);
        }
        Ok(Ref::external(i))
    }
}

/// Writes a layer, keeping the SHA-256 of each chunk (until
/// [`end_chunks`](Self::end_chunks)), then the crc32 and the SHA-256 (the
/// layer's [`root`]) of the tail.
struct CrcWriter<W: Write> {
    inner: W,
    root: sha2::Sha256,
    pos: u64,
    chunks: Option<ChunkHasher>,
    tail: Option<crc32fast::Hasher>,
}

struct ChunkHasher {
    size: u64,
    filled: u64,
    cur: sha2::Sha256,
    done: Vec<[u8; CHUNK_HASH]>,
}

impl ChunkHasher {
    /// Hash `buf`, closing each chunk as it fills.
    fn update(&mut self, mut buf: &[u8]) {
        while !buf.is_empty() {
            let room = to_usize(self.size - self.filled);
            let (head, rest) = buf.split_at(room.min(buf.len()));
            self.cur.update(head);
            self.filled += to_u64(head.len());
            buf = rest;
            self.close_if_full();
        }
    }

    fn close_if_full(&mut self) {
        if self.filled == self.size {
            let h = std::mem::replace(&mut self.cur, sha2::Sha256::new());
            self.done.push(h.finalize().into());
            self.filled = 0;
        }
    }
}

impl<W: Write> CrcWriter<W> {
    fn new(inner: W, chunk: u64) -> Self {
        let chunks = ChunkHasher {
            size: chunk,
            filled: 0,
            cur: sha2::Sha256::new(),
            done: Vec::new(),
        };
        CrcWriter {
            inner,
            root: sha2::Sha256::new(),
            pos: 0,
            chunks: Some(chunks),
            tail: None,
        }
    }

    /// Stop hashing chunks (the last one may be partial) and start on the
    /// tail. Returns every chunk's hash.
    fn end_chunks(&mut self) -> Vec<[u8; CHUNK_HASH]> {
        let mut c = must(self.chunks.take(), "hashing chunks");
        self.tail = Some(crc32fast::Hasher::new());
        if c.filled > 0 {
            c.done.push(c.cur.finalize().into());
        }
        c.done
    }
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        let buf = must(buf.get(..n), "writer wrote more than it was given");
        if let Some(t) = &mut self.tail {
            t.update(buf);
        }
        if let Some(c) = &mut self.chunks {
            c.update(buf);
        } else {
            self.root.update(buf);
        }
        self.pos += to_u64(n);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// What a manifest records of a layer file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Written {
    /// The SHA-256 of the file from the index region on. The chunk hashes
    /// there cover all bytes before it, so the writer hashes each byte once.
    pub root: [u8; 32],
    pub bytes: u64,
    /// The bytes from the object table to the end, which an open loads:
    /// knowing them lets it do so in one ranged read.
    pub meta_bytes: u64,
}

impl Written {
    /// The facts of layer file `data`.
    pub(super) fn of(data: &[u8]) -> Result<Written> {
        let end = data.len().checked_sub(TAIL).context("layer too small")?;
        let tail = must(
            Tail::read_from_bytes(data.split_at(end).1).ok(),
            "tail size",
        );
        let f = &tail.footer;
        let in_bounds = |off: u64| usize::try_from(off).ok().filter(|&off| off <= end);
        let (Some(index_off), Some(table_off)) =
            (in_bounds(f.index_off.get()), in_bounds(f.table_off.get()))
        else {
            anyhow::bail!("layer sections out of bounds");
        };
        Ok(Written {
            root: sha2::Sha256::digest(data.split_at(index_off).1).into(),
            bytes: to_u64(data.len()),
            meta_bytes: to_u64(data.len() - table_off),
        })
    }
}

/// A read-only layer, read through the buffer pool.
pub struct Layer {
    pub id: String,
    /// The length of its ids, and where its items are.
    pub(super) geometry: Geometry,
    /// The whole file, in large pages: entry data is read in long runs.
    file: Arc<PagedFile>,
    /// The object table, oid index and external oids, in small pages: they
    /// are read a record at a time, at random.
    table: Arc<PagedFile>,
    oids: Arc<PagedFile>,
    ext: Arc<PagedFile>,
    /// The number of ids the layer covers.
    id_span: u32,
    /// The first local index of each id run, and its id relative to the
    /// layer's first.
    id_runs: Runs,
    /// The first local index of each kind run, its kind, and the number of
    /// objects of that kind before it.
    kind_runs: Vec<(u32, Kind, u32)>,
    table_off: u64,
    commits_off: u64,
    commits_len: usize,
    /// The commit graph section, between the commit section and the index
    /// region (empty for a pushed layer).
    graph: std::ops::Range<u64>,
    counts: PerKind<u32>,
    /// Where each kind's entries start in the oid index.
    starts: PerKind<u32>,
    /// Total number of objects.
    total: u32,
    ext_count: u32,
    pub commit_count: u32,
    /// Every `SAMPLE`th object id of the oid index, per kind, so a lookup
    /// binary-searches memory first and then touches a single index page.
    samples: PerKind<Vec<Oid>>,
    /// Bloom filter over the layer's object ids, so lookups across many
    /// layers skip most of them with a few bit probes.
    bloom: Bloom,
    /// Keeps the metadata local while the layer is open.
    pinned: Option<Pinned>,
    /// A stored layer's local copy may go once the layer closes.
    pub(super) release: Option<Release>,
}

impl std::fmt::Debug for Layer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Layer")
            .field("id", &self.id)
            .field("format", &self.geometry.format)
            .field("objects", &self.total)
            .field("commits", &self.commit_count)
            .finish_non_exhaustive()
    }
}

/// Page size of the regions read a record at a time (table, oid index,
/// external oids): a miss there loads a few KiB, not a 64 KiB data page.
const INDEX_PAGE: usize = 4 << 10;

/// Oid index entries between samples.
const SAMPLE: u32 = 256;

#[derive(Debug)]
pub struct ObjectRecord {
    pub oid: Oid,
    pub kind: Kind,
    pub size: u64,
    pub entry: Chunk,
    pub base: Ref,
    pub ctx: Ref,
    pub depth: u8,
}

/// An object table record: the object's id, where its entry is, its
/// uncompressed size and its delta metadata. Stored as the id's bytes and
/// then a [`RecordTail`].
#[derive(Clone, Copy, Debug)]
pub struct TableRecord {
    pub oid: Oid,
    tail: RecordTail,
}

/// A table record after its id, as stored.
#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct RecordTail {
    off: U64,
    len: U32,
    size: U64,
    base: U32,
    ctx: U32,
    depth: u8,
    _pad: [u8; 3],
}

impl TableRecord {
    /// The record stored in `b`, one record long, of a `g` layer.
    fn read(g: Geometry, b: &[u8]) -> TableRecord {
        let (oid, tail) = must(b.split_at_checked(g.oid()), "record size");
        TableRecord {
            oid: Oid::from_slice(g.format, oid),
            tail: must(RecordTail::read_from_bytes(tail).ok(), "record size"),
        }
    }

    /// The entry's offset in the file.
    pub(crate) fn entry_off(&self) -> u64 {
        self.tail.off.get()
    }

    /// The entry's stored length.
    pub(crate) fn entry_len(&self) -> u32 {
        self.tail.len.get()
    }

    /// The object's uncompressed size.
    pub fn size(&self) -> u64 {
        self.tail.size.get()
    }

    /// Delta base.
    pub fn base(&self) -> Ref {
        Ref(self.tail.base.get())
    }

    /// The commit the base was chosen in the context of.
    pub fn ctx(&self) -> Ref {
        Ref(self.tail.ctx.get())
    }

    /// Delta chain depth (0 = full).
    pub fn depth(&self) -> u8 {
        self.tail.depth
    }
}

#[derive(Debug)]
pub struct CommitRecord {
    pub local: u32,
    pub tree: Ref,
    pub time: i64,
    pub parents: Vec<Ref>,
    pub members: Vec<Ref>,
}

impl Layer {
    /// Open a layer from a storage file, whichever tier it is read from.
    /// Everything but the data region (the object table, external oids,
    /// commit section, index region and footer, which follow it) is made
    /// local and kept so while the layer is open: lookups and table scans
    /// then never meet a non-resident page; only entry reads can.
    /// `meta_bytes` (see [`Written`]) makes that one ranged read.
    pub(super) async fn open(id: String, file: Arc<PagedFile>, meta_bytes: u64) -> Result<Layer> {
        let n = file.len();
        let table_off = n
            .checked_sub(meta_bytes)
            .filter(|&off| off + to_u64(TAIL) <= n)
            .context("layer metadata out of bounds")?;
        let pin = file.load_pinned(table_off..n, Access::Hot).await?;
        let mut layer = tokio::task::spawn_blocking(move || Layer::from_file(id, file)).await??;
        ensure!(
            layer.table_off == table_off,
            "layer metadata size does not match"
        );
        layer.pinned = Some(pin);
        Ok(layer)
    }

    /// Parse a layer whose metadata is local: reads the footer and the
    /// index region, which check themselves, and hands the chunk hashes
    /// (which check everything before them) to the file.
    fn from_file(id: String, file: Arc<PagedFile>) -> Result<Layer> {
        let n = file.len();
        let (index_end, tail) = Tail::read(&file)?;
        ensure!(&tail.magic == END_MAGIC, "bad layer magic");
        let code = tail.footer.object_format.get();
        let format = ObjectFormat::ALL
            .into_iter()
            .find(|&f| format_code(f) == code)
            .with_context(|| format!("unknown layer object format {code}"))?;
        let geometry = Geometry::of(format);
        let f = &tail.footer;
        let counts = PerKind(f.counts.map(U32::get));
        let mut starts = PerKind::default();
        let mut total = 0u32;
        for k in Kind::ALL {
            starts[k] = total;
            total = total
                .checked_add(counts[k])
                .context("layer object count overflows")?;
        }
        let (table_off, oids_off, ext_off, commits_off, index_off) = (
            f.table_off.get(),
            f.oids_off.get(),
            f.ext_off.get(),
            f.commits_off.get(),
            f.index_off.get(),
        );
        ensure!(
            table_off <= oids_off
                && oids_off <= ext_off
                && ext_off <= commits_off
                && commits_off <= n,
            "layer sections out of order"
        );
        let commits_len = f.commits_len.get();
        let mut layer = Layer {
            id,
            geometry,
            table_off,
            commits_off,
            counts,
            starts,
            total,
            commits_len: usize::try_from(commits_len).context("commit section too large")?,
            graph: commits_off.saturating_add(commits_len)..index_off,
            ext_count: f.ext_count.get(),
            commit_count: f.commit_count.get(),
            table: file.view(table_off..oids_off, INDEX_PAGE),
            oids: file.view(oids_off..ext_off, INDEX_PAGE),
            ext: file.view(ext_off..commits_off, INDEX_PAGE),
            id_span: f.id_span.get(),
            id_runs: Vec::new(),
            kind_runs: Vec::new(),
            file,
            samples: PerKind::default(),
            bloom: Bloom::new(0),
            pinned: None,
            release: None,
        };
        let nsamples: usize = counts.0.iter().map(|&c| widen(c.div_ceil(SAMPLE))).sum();
        let bloom = Bloom::new(u64::from(total));
        let runs = widen(f.kind_runs.get()) + widen(f.id_runs.get());
        let index_len = nsamples * geometry.oid() + runs * size_of::<Run>() + bloom.bits.len() * 8;
        ensure!(
            table_off + geometry.record_off(total) <= oids_off
                && oids_off + geometry.oid_entry_off(total) <= ext_off
                && commits_off + commits_len <= index_off
                && index_off + to_u64(index_len) <= index_end,
            "layer sections out of bounds"
        );
        layer.read_index(&tail, index_off, index_end, index_len, bloom.mask)?;
        Ok(layer)
    }

    /// Keep the kind runs and id runs from `rest`, checked. Returns what
    /// follows them.
    fn read_runs<'a>(&mut self, f: &Footer, rest: &'a [u8]) -> Result<&'a [u8]> {
        let (kinds, rest) = parse_runs(rest, f.kind_runs.get())?;
        let (ids, rest) = parse_runs(rest, f.id_runs.get())?;
        let total = self.total;
        // Where run `i` of `v` ends, if it holds any objects.
        let end = |v: &[(u32, u32)], i: usize| {
            let end = v.get(i + 1).map_or(total, |n| n.0);
            v.get(i)
                .filter(|r| (i > 0 || r.0 == 0) && r.0 < end)
                .map(|_| end)
        };
        let mut before = PerKind::<u32>::default();
        for (i, &(start, code)) in kinds.iter().enumerate() {
            let end = end(&kinds, i).context("kind runs out of order")?;
            let kind = u8::try_from(code)
                .ok()
                .and_then(Kind::from_code)
                .context("bad kind run")?;
            self.kind_runs.push((start, kind, before[kind]));
            before[kind] += end - start;
        }
        ensure!(
            before.0 == self.counts.0 && (total == 0 || !kinds.is_empty()),
            "kind runs do not match the counts"
        );
        for (i, &(start, id)) in ids.iter().enumerate() {
            let end = end(&ids, i).context("id runs out of order")?;
            let next = ids.get(i + 1).map_or(self.id_span, |n| n.1);
            let last = id.checked_add(end - start);
            ensure!(
                last.is_some_and(|l| l <= next && l <= self.id_span),
                "id runs out of order"
            );
        }
        ensure!(total == 0 || !ids.is_empty(), "no id runs");
        self.id_runs = ids;
        Ok(rest)
    }

    /// Read the index region: check it, hand the chunk hashes to the file
    /// and keep the oid samples and the Bloom filter.
    fn read_index(
        &mut self,
        tail: &Tail,
        index_off: u64,
        index_end: u64,
        index_len: usize,
        bloom_mask: u64,
    ) -> Result<()> {
        let mut index =
            vec![0u8; usize::try_from(index_end - index_off).context("index region too large")?];
        self.file.read_direct(index_off, &mut index)?;
        // The index region and footer check themselves; the chunk hashes
        // check everything before.
        let mut t = crc32fast::Hasher::new();
        t.update(&index);
        t.update(tail.footer.as_bytes());
        ensure!(
            t.finalize() == tail.crc.get(),
            "layer {} index region corrupt",
            self.id
        );
        let (mut rest, hashes) = index
            .split_at_checked(index_len)
            .context("bad chunk hash table")?;
        let (hashes, partial) = hashes.as_chunks::<CHUNK_HASH>();
        ensure!(
            partial.is_empty()
                && to_u64(hashes.len()) == index_off.div_ceil(super::tiered::chunk_bytes()),
            "bad chunk hash table"
        );
        self.file.set_chunk_hashes(ChunkHashes {
            covered: index_off,
            hashes: Arc::new(hashes.to_vec()),
        })?;
        for k in Kind::ALL {
            let g = self.geometry;
            let len = widen(self.counts[k].div_ceil(SAMPLE)) * g.oid();
            let (samples, after) = rest.split_at_checked(len).context("bad oid samples")?;
            self.samples[k] = samples
                .chunks_exact(g.oid())
                .map(|o| Oid::from_slice(g.format, o))
                .collect();
            rest = after;
        }
        let rest = self.read_runs(&tail.footer, rest)?;
        let (words, partial) = rest.as_chunks::<8>();
        ensure!(partial.is_empty(), "bad bloom filter");
        self.bloom = Bloom {
            bits: words.iter().map(|w| u64::from_le_bytes(*w)).collect(),
            mask: bloom_mask,
        };
        Ok(())
    }

    /// The number of ids the layer covers (its manifest entry's
    /// `id_span`).
    pub(crate) fn id_span(&self) -> u32 {
        self.id_span
    }

    /// The id of the object at `local`, in a layer whose range starts at
    /// `first_id` (its manifest entry's).
    pub(crate) fn id_of(&self, first_id: u32, local: u32) -> u32 {
        debug_assert!(local < self.len(), "local index out of range");
        let i = self.id_runs.partition_point(|r| r.0 <= local);
        let (start, id) = *must(i.checked_sub(1).and_then(|i| self.id_runs.get(i)), "id run");
        first_id + id + (local - start)
    }

    /// The local index of the object with id `id`, in a layer whose range
    /// starts at `first_id`: None for a hole, or an id outside the layer.
    pub(crate) fn local_of(&self, first_id: u32, id: u32) -> Option<u32> {
        let id = id.checked_sub(first_id)?;
        let r = self.id_runs.partition_point(|r| r.1 <= id);
        let (start, first) = *self.id_runs.get(r.checked_sub(1)?)?;
        let end = self.id_runs.get(r).map_or(self.total, |n| n.0);
        let local = start + (id - first);
        (local < end).then_some(local)
    }

    /// Whether `oid` may be in this layer (no false negatives).
    pub(crate) fn may_contain(&self, oid: &Oid) -> bool {
        self.bloom.may_contain(oid)
    }

    /// Total number of objects.
    pub fn len(&self) -> u32 {
        self.total
    }

    /// Bytes before the object table: the data region.
    pub(crate) fn data_end(&self) -> u64 {
        self.table_off
    }

    /// Whether the layer has no objects.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of objects of `kind`.
    pub fn count(&self, kind: Kind) -> u32 {
        self.counts[kind]
    }

    /// Range of oid index entries of objects of `kind`.
    pub(crate) fn index_range(&self, kind: Kind) -> std::ops::Range<u32> {
        let start = self.starts[kind];
        start..start + self.counts[kind]
    }

    /// The kind of the object at `local`, and how many objects of that
    /// kind come before it. Panics when `local` is out of range (see
    /// `oid`).
    pub(crate) fn kind_rank(&self, local: u32) -> (Kind, u32) {
        let i = self.kind_runs.partition_point(|r| r.0 <= local);
        let run = i
            .checked_sub(1)
            .and_then(|i| self.kind_runs.get(i))
            .filter(|_| local < self.total)
            .ok_or_else(|| anyhow::anyhow!("local index {local} out of range"));
        let (start, kind, before) = *self.or_panic(run);
        (kind, before + (local - start))
    }

    /// The locals of the objects of `kind`, in rank order.
    pub(crate) fn locals_of(&self, kind: Kind) -> impl Iterator<Item = u32> + '_ {
        let ends = self
            .kind_runs
            .iter()
            .skip(1)
            .map(|r| r.0)
            .chain([self.total]);
        self.kind_runs
            .iter()
            .zip(ends)
            .filter(move |(r, _)| r.1 == kind)
            .flat_map(|(r, end)| r.0..end)
    }

    /// The kind of the object at `local`. Panics when `local` is out of
    /// range (see `oid`).
    pub(crate) fn kind_of(&self, local: u32) -> Kind {
        self.kind_rank(local).0
    }

    /// The table record of `local`.
    pub fn record(&self, local: u32) -> Result<TableRecord> {
        let g = self.geometry;
        self.table
            .with_bytes(g.record_off(local), g.record, Access::Hot, |b| {
                TableRecord::read(g, b)
            })
    }

    /// The format of the layer's ids.
    pub(crate) fn format(&self) -> ObjectFormat {
        self.geometry.format
    }

    /// The value of `r`; panics on its error. The accessors that return no
    /// `Result` use it: a failed read fails the request, not the process.
    #[expect(
        clippy::panic,
        reason = "the infallible accessors document that they panic on an I/O error"
    )]
    fn or_panic<T>(&self, r: Result<T>) -> T {
        r.unwrap_or_else(|e| panic!("layer {}: {e:#}", self.id))
    }

    /// Object id at `local`. Panics on an I/O error (the request fails, the
    /// process does not); use [`try_oid`](Self::try_oid) to handle it.
    pub fn oid(&self, local: u32) -> Oid {
        self.or_panic(self.try_oid(local))
    }

    pub(crate) fn try_oid(&self, local: u32) -> Result<Oid> {
        let g = self.geometry;
        self.table
            .with_bytes(g.record_off(local), g.oid(), Access::Hot, |b| {
                Oid::from_slice(g.format, b)
            })
    }

    /// The object at `local`, with its stored entry.
    pub fn object(&self, local: u32) -> Result<ObjectRecord> {
        self.object_with(local, Access::Hot)
    }

    /// Like [`object`](Self::object), reading the entry with `access` (use
    /// [`Access::Scan`] when streaming many objects, e.g. a full clone).
    pub(crate) fn object_with(&self, local: u32, access: Access) -> Result<ObjectRecord> {
        let r = self.record(local)?;
        Ok(ObjectRecord {
            oid: r.oid,
            kind: self.kind_of(local),
            entry: self
                .file
                .read(r.entry_off(), widen(r.entry_len()), access)?,
            size: r.size(),
            base: r.base(),
            ctx: r.ctx(),
            depth: r.depth(),
        })
    }

    /// Call `f` with the stored entry at `off` (from its record).
    pub(crate) fn with_entry_at<R>(
        &self,
        off: u64,
        len: u32,
        mode: Access,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R> {
        self.file.with_bytes(off, widen(len), mode, f)
    }

    /// Whether reading the entries in `off..end` of the file would need
    /// bytes that are not local; if so, what to load.
    pub(crate) fn entries_missing(&self, off: u64, end: u64) -> Option<NotResident> {
        self.file.missing(off..end)
    }

    /// Keep the entries in `off..end` (of the file) local while the pin
    /// lives.
    pub(crate) fn pin_entries(&self, off: u64, end: u64) -> Pinned {
        self.file.pin_range(off..end)
    }

    /// Uncompressed size of `local`. Panics on an I/O error (see `oid`).
    pub fn size(&self, local: u32) -> u64 {
        self.or_panic(self.record(local)).size()
    }

    pub fn find(&self, oid: &Oid) -> Option<u32> {
        Kind::ALL.into_iter().find_map(|k| self.find_kind(k, oid))
    }

    /// Search for `oid` among objects of `kind`. Panics on an I/O error
    /// (see `oid`).
    pub(crate) fn find_kind(&self, kind: Kind, oid: &Oid) -> Option<u32> {
        self.or_panic(lookup::find(self, kind, oid))
    }

    /// External oid `i`. Panics on an I/O error (see `oid`).
    pub(crate) fn ext_oid(&self, i: u32) -> Oid {
        assert!(i < self.ext_count);
        let g = self.geometry;
        let off = u64::from(i) * to_u64(g.oid());
        self.or_panic(
            self.ext
                .with_bytes(off, g.oid(), Access::Hot, |b| Oid::from_slice(g.format, b)),
        )
    }

    /// Resolve a ref to an oid.
    pub(crate) fn ref_oid(&self, r: Ref) -> Option<Oid> {
        match r.decode() {
            RefTarget::None => None,
            RefTarget::Local(l) => Some(self.oid(l)),
            RefTarget::External(e) => Some(self.ext_oid(e)),
        }
    }

    /// Call `f` with every record, in table order, reading the table
    /// sequentially (outside the pool: a one-off scan).
    pub(crate) fn for_each_record(&self, mut f: impl FnMut(u32, &TableRecord)) -> Result<()> {
        let g = self.geometry;
        self.for_each_raw(|l, r| f(l, &TableRecord::read(g, r)))
    }

    /// Call `f(local, oid)` for every object in table order (see
    /// [`for_each_record`](Self::for_each_record)).
    pub(crate) fn for_each_oid(&self, mut f: impl FnMut(u32, &Oid)) -> Result<()> {
        let g = self.geometry;
        self.for_each_raw(|l, r| f(l, &Oid::from_slice(g.format, r)))
    }

    /// Call `f` with every record's bytes, in table order, reading the
    /// table sequentially (outside the pool: a one-off scan).
    fn for_each_raw(&self, mut f: impl FnMut(u32, &[u8])) -> Result<()> {
        const BATCH: u32 = 4096;
        let g = self.geometry;
        let mut buf = vec![0u8; widen(self.total.min(BATCH)) * g.record];
        let mut l = 0u32;
        while l < self.total {
            let k = (self.total - l).min(BATCH);
            let (bytes, _) = must(
                buf.split_at_mut_checked(widen(k) * g.record),
                "batch buffer",
            );
            self.file
                .read_direct(self.table_off + g.record_off(l), bytes)?;
            for (i, r) in (l..).zip(bytes.chunks_exact(g.record)) {
                f(i, r);
            }
            l += k;
        }
        Ok(())
    }

    /// A merged layer's commit graph (see [`LayerBuilder::set_graph`]):
    /// empty for a pushed layer.
    pub(crate) fn graph(&self) -> Result<Vec<u8>> {
        let mut b = vec![0u8; usize::try_from(self.graph.end - self.graph.start)?];
        self.file.read_direct(self.graph.start, &mut b)?;
        Ok(b)
    }

    /// The layer's commits, in commit section order.
    pub fn commits(&self) -> Result<Vec<CommitRecord>> {
        let mut out = Vec::with_capacity(widen(self.commit_count));
        let mut b = vec![0u8; self.commits_len];
        self.file.read_direct(self.commits_off, &mut b)?;
        let refs = |r: &[U32]| r.iter().map(|v| Ref(v.get())).collect();
        let mut rest = b.as_slice();
        for _ in 0..self.commit_count {
            let (head, r) = CommitHead::ref_from_prefix(rest).map_err(truncated)?;
            let (parents, r) = <[U32]>::ref_from_prefix_with_elems(r, widen(head.parents.get()))
                .map_err(truncated)?;
            let (members, r) = U32::read_from_prefix(r).map_err(truncated)?;
            let (members, r) =
                <[U32]>::ref_from_prefix_with_elems(r, widen(members.get())).map_err(truncated)?;
            rest = r;
            out.push(CommitRecord {
                local: head.local.get(),
                tree: Ref(head.tree.get()),
                time: head.time.get(),
                parents: refs(parents),
                members: refs(members),
            });
        }
        Ok(out)
    }

    /// Make `range` of the file local (for readers of many entries in
    /// order, such as merges), and keep it so until the pin is dropped.
    pub(crate) fn load_range(
        &self,
        range: std::ops::Range<u64>,
    ) -> futures::future::BoxFuture<'static, Result<Pinned>> {
        self.file.load_pinned(range, Access::Scan)
    }
}

/// Runs as (local index, value).
type Runs = Vec<(u32, u32)>;

/// `n` runs from the front of `rest`, as (local index, value), and what
/// follows them.
fn parse_runs(rest: &[u8], n: u32) -> Result<(Runs, &[u8])> {
    let (runs, rest) = <[Run]>::ref_from_prefix_with_elems(rest, widen(n))
        .ok()
        .context("bad runs")?;
    Ok((
        runs.iter()
            .map(|r| (r.local.get(), r.value.get()))
            .collect(),
        rest,
    ))
}

fn truncated<E>(_: E) -> anyhow::Error {
    anyhow::anyhow!("truncated commit section")
}

/// A Bloom filter over object ids (~10 bits per object, 4 probes, ~1%
/// false positives). Object ids are uniformly random, so their bytes serve
/// directly as the hash values.
pub(crate) struct Bloom {
    bits: Vec<u64>,
    mask: u64,
}

impl Bloom {
    /// A filter sized for `n` objects.
    pub(crate) fn new(n: u64) -> Bloom {
        let nbits = (n * 10).max(64).next_power_of_two();
        Bloom {
            bits: vec![0; to_usize(nbits / 64)],
            mask: nbits - 1,
        }
    }

    pub(crate) fn insert(&mut self, oid: &Oid) {
        for h in Self::hashes(oid) {
            let i = h & self.mask;
            // `mask` keeps `i` inside `bits`.
            *must(self.bits.get_mut(to_usize(i / 64)), "bloom word") |= 1 << (i % 64);
        }
    }

    fn hashes(oid: &Oid) -> [u64; 4] {
        let [
            a0,
            a1,
            a2,
            a3,
            b0,
            b1,
            b2,
            b3,
            c0,
            c1,
            c2,
            c3,
            d0,
            d1,
            d2,
            d3,
            ..,
        ] = *oid.prefix();
        [
            [a0, a1, a2, a3],
            [b0, b1, b2, b3],
            [c0, c1, c2, c3],
            [d0, d1, d2, d3],
        ]
        .map(|w| u64::from(u32::from_le_bytes(w)))
    }

    pub(crate) fn may_contain(&self, oid: &Oid) -> bool {
        Self::hashes(oid).iter().all(|h| {
            let i = h & self.mask;
            must(self.bits.get(to_usize(i / 64)), "bloom word") & (1 << (i % 64)) != 0
        })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::git::oid::{hash_object, hash_object_in};
    use crate::git::pack::full_entry;

    #[test]
    fn write_and_read() {
        for format in ObjectFormat::ALL {
            write_and_read_in(format);
        }
    }

    /// A layer of `format` ids, with enough blobs that its oid index takes
    /// several pages, reads back what was written.
    fn write_and_read_in(format: ObjectFormat) {
        let hash_object = |k, d: &[u8]| hash_object_in(format, k, d);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("l");
        let blob = b"hello".to_vec();
        let blob_oid = hash_object(Kind::Blob, &blob);
        let tree = {
            let mut t = b"100644 a\0".to_vec();
            t.extend_from_slice(blob_oid.as_bytes());
            t
        };
        let tree_oid = hash_object(Kind::Tree, &tree);
        let commit =
            format!("tree {tree_oid}\nauthor a <a> 1 +0000\ncommitter a <a> 5 +0000\n\nm\n")
                .into_bytes();
        let commit_oid = hash_object(Kind::Commit, &commit);
        let mut b = LayerBuilder::new(&Workspace::at(dir.path()), format);
        let more: Vec<(Vec<u8>, Oid)> = (0..400u32)
            .map(|i| {
                let d = format!("blob {i}").into_bytes();
                let o = hash_object(Kind::Blob, &d);
                (d, o)
            })
            .collect();
        let objects = [
            (Kind::Blob, &blob, blob_oid),
            (Kind::Tree, &tree, tree_oid),
            (Kind::Commit, &commit, commit_oid),
        ]
        .into_iter()
        .chain(more.iter().map(|(d, o)| (Kind::Blob, d, *o)));
        for (k, d, o) in objects {
            b.add(NewObject {
                oid: o,
                kind: k,
                size: u64::try_from(d.len()).unwrap(),
                entry: &full_entry(k, d),
                base: None,
                base_ctx: None,
                depth: 0,
                id: None,
            })
            .unwrap();
        }
        let ext = Oid::repeat(format, 7);
        b.add_commit(&NewCommit {
            oid: commit_oid,
            tree: tree_oid,
            parents: vec![ext],
            time: 5,
            members: vec![tree_oid, blob_oid],
        })
        .unwrap();
        b.write_to(std::fs::File::create(&path).unwrap()).unwrap();
        let pool = crate::storage::pool::BufferPool::new(1 << 20);
        let l = Layer::from_file("x".into(), files::open(&pool, &path).unwrap()).unwrap();
        assert_eq!(l.format(), format);
        assert_eq!(l.len(), 403);
        assert_eq!(l.find(&blob_oid), Some(2));
        for (_, o) in &more {
            assert_eq!(l.kind_of(l.find(o).unwrap()), Kind::Blob);
        }
        assert_eq!(l.find(&Oid::repeat(format, 9)), None);
        let other = ObjectFormat::ALL
            .into_iter()
            .find(|&f| f != format)
            .unwrap();
        assert_eq!(l.find(&Oid::repeat(other, 9)), None);
        assert_eq!(l.kind_of(l.find(&tree_oid).unwrap()), Kind::Tree);
        let cs = l.commits().unwrap();
        assert_eq!(cs.len(), 1);
        assert_eq!(l.ref_oid(cs[0].parents[0]), Some(ext));
        assert_eq!(l.ref_oid(cs[0].tree), Some(tree_oid));
        let members: Vec<_> = cs[0].members.iter().map(|&m| l.ref_oid(m)).collect();
        assert_eq!(members, [Some(tree_oid), Some(blob_oid)]);
        assert_eq!((cs[0].local, cs[0].time), (0, 5));
        assert_eq!(
            &*l.object(2).unwrap().entry,
            &full_entry(Kind::Blob, &blob)[..]
        );
        // A pushed layer: ids follow local indexes from its range's start.
        assert_eq!(l.id_span(), 403);
        assert_eq!(l.id_of(10, 2), 12);
        assert_eq!(l.local_of(10, 12), Some(2));
        assert_eq!(l.local_of(10, 413), None);
        assert_eq!(l.local_of(10, 9), None);
    }

    /// The root covers the tail directly, and each chunk through the chunk
    /// hashes, which an in-memory layer checks when it opens.
    #[test]
    fn the_root_covers_every_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let mut x = 1u64;
        let data: Vec<u8> = (0..(5 << 19))
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_le_bytes()[0]
            })
            .collect();
        let entry = full_entry(Kind::Blob, &data);
        let mut b = LayerBuilder::new(&Workspace::at(dir.path()), ObjectFormat::Sha1);
        b.add(blob(hash_object(Kind::Blob, &data), &entry, None))
            .unwrap();
        let mut file = Vec::new();
        let written = b.write_to(&mut file).unwrap();
        assert_eq!(Written::of(&file).unwrap(), written);
        assert_eq!(written.bytes, u64::try_from(file.len()).unwrap());
        let root = written.root;
        let pool = crate::storage::pool::BufferPool::new(1 << 20);
        let open = |f: &[u8]| {
            let f = crate::storage::pool::Memory(bytes::Bytes::copy_from_slice(f));
            Layer::from_file("x".into(), pool.open(Arc::new(f)))
        };
        open(&file).unwrap();
        let chunk = usize::try_from(super::super::tiered::chunk_bytes()).unwrap();
        for at in [0, chunk + 1, 2 * chunk + 1] {
            let mut damaged = file.clone();
            damaged[at] ^= 1;
            assert_eq!(Written::of(&damaged).unwrap().root, root);
            assert!(open(&damaged).is_err(), "byte {at}");
        }
        let mut damaged = file.clone();
        *damaged.last_mut().unwrap() ^= 1;
        assert_ne!(Written::of(&damaged).unwrap().root, root);
    }

    pub(in crate::storage) fn blob(oid: Oid, entry: &[u8], id: Option<u32>) -> NewObject<'_> {
        NewObject {
            oid,
            kind: Kind::Blob,
            size: 1,
            entry,
            base: None,
            base_ctx: None,
            depth: 0,
            id,
        }
    }

    /// A merged layer keeps its objects in the order added, which is id
    /// order with holes, and maps ids both ways; kinds come in runs.
    #[test]
    fn merged_layers_map_ids() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("l");
        let mut b = LayerBuilder::new(&Workspace::at(dir.path()), ObjectFormat::Sha1);
        b.set_ids(100..110);
        let mut ids = Vec::new();
        for (i, (kind, id)) in [
            (Kind::Commit, 100u32),
            (Kind::Blob, 101),
            (Kind::Commit, 103),
            (Kind::Tree, 104),
            (Kind::Blob, 107),
            (Kind::Blob, 109),
        ]
        .into_iter()
        .enumerate()
        {
            let oid = hash_object(kind, format!("object {i}\n").as_bytes());
            b.add(NewObject {
                kind,
                ..blob(oid, &[0], Some(id))
            })
            .unwrap();
            ids.push((oid, kind, id));
        }
        let entry = full_entry(Kind::Blob, b"x");
        let oid = hash_object(Kind::Blob, b"x");
        assert!(
            b.add(blob(oid, &entry, Some(110))).is_err(),
            "outside the range"
        );
        assert!(b.add(blob(oid, &entry, None)).is_err(), "without an id");
        assert!(b.add(blob(oid, &entry, Some(108))).is_err(), "out of order");
        assert!(b.add(blob(oid, &entry, Some(109))).is_err(), "taken");
        b.write_to(std::fs::File::create(&path).unwrap()).unwrap();
        let pool = crate::storage::pool::BufferPool::new(1 << 20);
        let l = Layer::from_file("x".into(), files::open(&pool, &path).unwrap()).unwrap();
        let mut commits = 0;
        for (local, &(oid, kind, id)) in (0u32..).zip(&ids) {
            assert_eq!(l.find(&oid), Some(local));
            assert_eq!(l.kind_of(local), kind);
            if kind == Kind::Commit {
                assert_eq!(l.kind_rank(local), (kind, commits));
                commits += 1;
            }
            assert_eq!(l.id_of(100, local), id);
            assert_eq!(l.local_of(100, id), Some(local));
        }
        assert_eq!(l.id_span(), 10);
        for hole in [102, 105, 106, 108, 110, 99] {
            assert_eq!(l.local_of(100, hole), None, "{hole}");
        }
    }
}
