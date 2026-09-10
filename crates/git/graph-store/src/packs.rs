//! Tier two: what an answer needs, once tier one has named the commits.
//!
//! A segment of its own rather than a section of tier one, because the
//! bitmaps are hundreds of bytes a commit against tier one's twenty, and a
//! deep walk must not pay for what it only traverses. Two segmented values,
//! two catalog tables, one join apiece.

use std::collections::BTreeMap;

use roaring::RoaringTreemap;

use enroute_git_core::{SegmentLocation, Ulid};
use enroute_lattice_core::frame::{
    Frame, HEADER, Header, Layout, field32, field64, read_header, write_header,
};
use enroute_lattice_core::tail;
use enroute_lattice_core::{Join, Key, KeyRange, Segment};

use crate::format::{Malformed, first_seq};
use crate::strided::Strided;

/// Bytes one record takes.
const RECORD: usize = 60;

/// How a tier-two segment is framed.
const PACKS: Layout = Layout {
    magic: *b"CIX2",
    record: RECORD,
    unit: 1,
};

/// Bit saying a slot holds a commit.
const PRESENT: u32 = 1;

/// What a fetch needs about one commit's pack.
#[derive(Debug, Clone, PartialEq)]
pub struct Pack {
    /// The commit entry's own byte span within its pack.
    pub entry_len: u64,
    /// Where the pack's blob section begins.
    pub blob_offset: u64,
    /// Which segment object holds the pack image, and its span.
    pub segment: SegmentLocation,
    /// The `object_seq`s of the trees this pack stores.
    pub trees: RoaringTreemap,
    /// The `object_seq`s of the blobs this pack stores.
    pub blobs: RoaringTreemap,
}

/// One commit's pack facts, with the bitmaps kept as written.
///
/// Serialized rather than decoded, so the layout is canonical and the join
/// has a total order to break ties with.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Node {
    entry_len: u64,
    blob_offset: u64,
    segment_id: u128,
    base_offset: u64,
    image_len: u64,
    trees: Vec<u8>,
    blobs: Vec<u8>,
}

/// The newest image of a commit's pack, and a total order under that.
///
/// An image moves only when it is copied into another segment, so the
/// greater segment id is the live one and the rest makes the order total.
impl Ord for Node {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.segment_id.cmp(&other.segment_id).then_with(|| {
            (
                self.entry_len,
                self.blob_offset,
                self.base_offset,
                self.image_len,
            )
                .cmp(&(
                    other.entry_len,
                    other.blob_offset,
                    other.base_offset,
                    other.image_len,
                ))
                .then_with(|| self.trees.cmp(&other.trees))
                .then_with(|| self.blobs.cmp(&other.blobs))
        })
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// The pack facts for a range of commit seqs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackIndex(Strided<Node>);

/// What the index knows about one commit's pack.
#[derive(Debug, Clone)]
pub struct PackEntry<'a> {
    node: &'a Node,
}

impl PackEntry<'_> {
    /// The commit entry's own byte span within its pack.
    #[must_use]
    pub const fn entry_len(&self) -> u64 {
        self.node.entry_len
    }

    /// Where the pack's blob section begins.
    #[must_use]
    pub const fn blob_offset(&self) -> u64 {
        self.node.blob_offset
    }

    /// Which segment object holds the pack image, and its span.
    #[must_use]
    pub const fn segment(&self) -> SegmentLocation {
        SegmentLocation {
            id: Ulid(self.node.segment_id),
            base_offset: self.node.base_offset,
            image_len: self.node.image_len,
        }
    }

    /// The trees this pack stores.
    ///
    /// # Errors
    /// [`Malformed::Bitmap`] when the stored bytes are not a roaring bitmap.
    pub fn trees(&self) -> Result<RoaringTreemap, Malformed> {
        bitmap(&self.node.trees)
    }

    /// The blobs this pack stores.
    ///
    /// # Errors
    /// [`Malformed::Bitmap`] when the stored bytes are not a roaring bitmap.
    pub fn blobs(&self) -> Result<RoaringTreemap, Malformed> {
        bitmap(&self.node.blobs)
    }
}

/// Builds a tier-two segment one commit at a time.
#[derive(Debug, Default)]
pub struct PackBuilder {
    packs: BTreeMap<i64, Node>,
}

impl PackBuilder {
    /// A segment with nothing in it yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `pack` at `seq`, replacing whatever was there.
    ///
    /// # Errors
    /// [`Malformed::Bitmap`] when a bitmap will not serialize.
    pub fn insert(&mut self, seq: i64, pack: &Pack) -> Result<(), Malformed> {
        self.packs.insert(
            seq,
            Node {
                entry_len: pack.entry_len,
                blob_offset: pack.blob_offset,
                segment_id: pack.segment.id.0,
                base_offset: pack.segment.base_offset,
                image_len: pack.segment.image_len,
                trees: serialize(&pack.trees)?,
                blobs: serialize(&pack.blobs)?,
            },
        );
        Ok(())
    }

    /// Lays the packs out as an array over the range they span.
    #[must_use]
    pub fn build(self) -> PackIndex {
        PackIndex(Strided::laid_out(self.packs))
    }
}

impl PackIndex {
    /// What the index knows about `seq`, or `None` for one it does not hold.
    #[must_use]
    pub fn get(&self, seq: i64) -> Option<PackEntry<'_>> {
        Some(PackEntry {
            node: self.0.get(seq)?,
        })
    }

    /// Every commit seq the index holds, with its pack facts.
    ///
    /// In seq order, and holes are absent rather than empty: a segment id is
    /// not a key this can look one up by, so gathering scans.
    pub fn entries(&self) -> impl Iterator<Item = (i64, PackEntry<'_>)> {
        self.0.iter().map(|(seq, node)| (seq, PackEntry { node }))
    }
}

impl Join for PackIndex {
    /// Union, and on a seq both hold, the node in the newest segment.
    ///
    /// A commit's pack facts are written once and move only when the image is
    /// copied, so [`Node`]'s order decides which copy is the live one.
    fn join(&mut self, other: Self) {
        self.0 = core::mem::take(&mut self.0).joined(other.0);
    }
}

impl Segment for PackIndex {
    type Error = Malformed;

    fn range(&self) -> Option<KeyRange> {
        self.0.range()
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let mut heap: Vec<u8> = Vec::new();
        let mut records: Vec<u8> = Vec::new();
        for slot in self.0.slots() {
            let Some(node) = slot else {
                records.extend_from_slice(&[0; RECORD]);
                continue;
            };
            records.extend_from_slice(&node.entry_len.to_le_bytes());
            records.extend_from_slice(&node.blob_offset.to_le_bytes());
            records.extend_from_slice(&node.segment_id.to_le_bytes());
            records.extend_from_slice(&node.base_offset.to_le_bytes());
            records.extend_from_slice(&node.image_len.to_le_bytes());
            records.extend_from_slice(&tail::stow(&node.trees, &mut heap).to_le_bytes());
            records.extend_from_slice(&tail::stow(&node.blobs, &mut heap).to_le_bytes());
            records.extend_from_slice(&PRESENT.to_le_bytes());
        }

        write_header(
            &Header {
                first: Key::new(u64::try_from(self.0.first()).unwrap_or(0)),
                records: self.0.slots().len(),
                tail: heap.len(),
            },
            PACKS,
            out,
        );
        out.extend_from_slice(&records);
        out.extend_from_slice(&heap);
    }

    fn decode_range(bytes: &[u8], range: KeyRange) -> Result<Self, Malformed> {
        let header = read_header(bytes, PACKS)?;
        let body = bytes.get(HEADER..).unwrap_or_default();
        let heap_at = header.records.saturating_mul(RECORD);
        let heap = body.get(heap_at..).unwrap_or_default();
        let first = first_seq(header.first)?;

        // The bitmaps are the bulk here, and each one is copied out of the
        // heap as its record is read — so a slice is what keeps a read of a
        // few commits from paying for every commit merged in beside them.
        let Some((from, upto)) = header.slice_of(range) else {
            return Ok(Self::default());
        };

        let mut records = Vec::with_capacity(upto - from);
        for at in from..upto {
            let at = at.saturating_mul(RECORD);
            let Some(chunk) = body
                .get(at..at + RECORD)
                .and_then(<[u8]>::first_chunk::<RECORD>)
            else {
                return Err(Frame::Truncated {
                    expected: HEADER + header.records * RECORD,
                    actual: bytes.len(),
                }
                .into());
            };
            records.push(read_record(chunk, heap)?);
        }
        Ok(Self(Strided::trimmed(
            first.saturating_add(i64::try_from(from).unwrap_or(0)),
            records,
        )))
    }
}

/// Reads one record, resolving its two heap references.
fn read_record(bytes: &[u8; RECORD], heap: &[u8]) -> Result<Option<Node>, Malformed> {
    if field32(bytes, 56) & PRESENT == 0 {
        return Ok(None);
    }
    Ok(Some(Node {
        entry_len: field64(bytes, 0),
        blob_offset: field64(bytes, 8),
        segment_id: field128(bytes, 16),
        base_offset: field64(bytes, 32),
        image_len: field64(bytes, 40),
        trees: fetch(field32(bytes, 48), heap)?,
        blobs: fetch(field32(bytes, 52), heap)?,
    }))
}

/// One bitmap as it is stored, out of the heap it was stowed in.
fn fetch(at: u32, heap: &[u8]) -> Result<Vec<u8>, Malformed> {
    tail::fetch(at, heap)
        .map(<[u8]>::to_vec)
        .ok_or(Malformed::StrayExtra(at))
}

fn serialize(bitmap: &RoaringTreemap) -> Result<Vec<u8>, Malformed> {
    tail::encode_bitmap(bitmap).ok_or(Malformed::Bitmap)
}

fn bitmap(bytes: &[u8]) -> Result<RoaringTreemap, Malformed> {
    tail::decode_bitmap(bytes).ok_or(Malformed::Bitmap)
}

fn field128(bytes: &[u8; RECORD], at: usize) -> u128 {
    bytes
        .get(at..at + 16)
        .and_then(|slice| slice.first_chunk::<16>())
        .map_or(0, |chunk| u128::from_le_bytes(*chunk))
}
