//! The object index as one joinable value, keyed by seq in one kind's space.

use std::collections::BTreeMap;

use gix_hash::ObjectId;
use roaring::RoaringTreemap;

use enroute_git_core::{ObjectSeqs, SegmentLocation, Ulid};
use enroute_lattice_core::frame::{
    Frame, HEADER, Header, field32, field64, read_header, write_header,
};
use enroute_lattice_core::tail::{self, NONE};
use enroute_lattice_core::{Join, Key, KeyRange, Segment};

use crate::format::{ENTRY, LOCATION, Malformed, OBJECTS};

/// No delta base, in the width a base seq is stored at.
const NO_BASE: u64 = u64::MAX;

/// One place an object's bytes are stored.
///
/// A delta base is a property of the entry rather than of the object: the
/// same content can sit in several packs against different bases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Location {
    /// The commit whose pack holds this entry, by its commit seq.
    ///
    /// First, so the lowest sorts first: that one is the introducing pack,
    /// which is the one a point read resolves through.
    pub pack_seq: i64,
    /// That commit's oid, so a reader names the pack without a second lookup.
    pub pack_oid: ObjectId,
    /// Where that pack's image sits.
    pub segment: SegmentLocation,
    /// Byte offset of this entry's header within the pack image.
    pub offset: u64,
    /// The header and the compressed body together, for one range read.
    pub entry_len: u64,
    /// What this entry deltas against, in this object's own space.
    ///
    /// Git never deltas across kinds, so a base is always numbered where the
    /// entry is — which a chain walk now relies on by type rather than hope.
    pub base_seq: Option<u64>,
}

/// What the index knows about one object.
///
/// The packs it is in only grow; where one of them sits is the one thing
/// here a copy replaces.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Object {
    /// Every pack it is stored in, lowest pack seq first and one each.
    pub locations: Vec<Location>,
    /// A tree's direct entries; empty for a blob.
    pub children: ObjectSeqs,
}

impl Object {
    /// The pack a point read resolves through: the lowest that stores it.
    #[must_use]
    pub fn introducing(&self) -> Option<&Location> {
        self.locations.first()
    }

    /// Whether the object holds nothing worth a slot.
    fn is_empty(&self) -> bool {
        self.locations.is_empty() && self.children.is_empty()
    }

    /// Takes everything `other` holds that this one does not.
    fn absorb(&mut self, other: Self) {
        self.children.absorb(&other.children);
        if other.locations.is_empty() {
            return;
        }
        self.locations.extend(other.locations);
        self.locations.sort_unstable();
        self.locations.dedup();
        keep_newest_per_pack(&mut self.locations);
    }
}

/// Drops every location whose image has already been copied elsewhere.
///
/// A max per pack over a union of packs, so the join stays associative,
/// commutative and idempotent — a copy is written under a fresh ULID.
fn keep_newest_per_pack(locations: &mut Vec<Location>) {
    // Sorted already, and `Location` orders by pack seq before segment id,
    // so the live one of each run is the one at its end.
    locations.dedup_by(|later, earlier| {
        if later.pack_seq != earlier.pack_seq {
            return false;
        }
        *earlier = *later;
        true
    });
}

/// The object index over a range of seqs in one kind's space.
///
/// Sorted pairs rather than a slot per seq: an object gains packs after it
/// is recorded, and a re-inclusion scatters across the whole space.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ObjectIndex {
    entries: Vec<(u64, Object)>,
}

/// Builds an index one object at a time.
#[derive(Debug, Default)]
pub struct Builder {
    objects: BTreeMap<u64, Object>,
}

impl Builder {
    /// An index with nothing in it yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `location` to what the index knows about `seq`.
    ///
    /// A second location for a pack it already holds replaces that one, so
    /// no value the join ever sees is unreduced.
    pub fn locate(&mut self, seq: u64, location: Location) {
        let object = self.objects.entry(seq).or_default();
        if !object.locations.contains(&location) {
            object.locations.push(location);
            object.locations.sort_unstable();
            keep_newest_per_pack(&mut object.locations);
        }
    }

    /// Records a tree's direct entries.
    pub fn children(&mut self, seq: u64, children: &ObjectSeqs) {
        if children.is_empty() {
            return;
        }
        self.objects
            .entry(seq)
            .or_default()
            .children
            .absorb(children);
    }

    /// Lays the objects out in seq order.
    #[must_use]
    pub fn build(self) -> ObjectIndex {
        ObjectIndex {
            entries: self
                .objects
                .into_iter()
                .filter(|(_, object)| !object.is_empty())
                .collect(),
        }
    }
}

impl ObjectIndex {
    /// What the index knows about `seq`, or `None` for one it does not hold.
    #[must_use]
    pub fn get(&self, seq: u64) -> Option<&Object> {
        let at = self
            .entries
            .binary_search_by_key(&seq, |(held, _)| *held)
            .ok()?;
        self.entries.get(at).map(|(_, object)| object)
    }

    /// Whether the index holds nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Join for ObjectIndex {
    /// Union, componentwise, all the way down.
    ///
    /// Two segments holding one object hold two sets of the packs it was in,
    /// and the answer is both.
    fn join(&mut self, other: Self) {
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = other;
            return;
        }
        let mut merged: BTreeMap<u64, Object> =
            std::mem::take(&mut self.entries).into_iter().collect();
        for (seq, object) in other.entries {
            merged.entry(seq).or_default().absorb(object);
        }
        self.entries = merged.into_iter().collect();
    }
}

impl Segment for ObjectIndex {
    type Error = Malformed;

    fn range(&self) -> Option<KeyRange> {
        let first = self.entries.first()?.0;
        let last = self.entries.last()?.0;
        KeyRange::new(Key::new(first), Key::new(last)).ok()
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let mut entries: Vec<u8> = Vec::with_capacity(self.entries.len() * ENTRY);
        let mut heap: Vec<u8> = Vec::new();
        for (seq, object) in &self.entries {
            entries.extend_from_slice(&seq.to_le_bytes());
            entries.extend_from_slice(&stow_locations(&object.locations, &mut heap).to_le_bytes());
            entries
                .extend_from_slice(&stow_bitmap(&object.children.trees, &mut heap).to_le_bytes());
            entries
                .extend_from_slice(&stow_bitmap(&object.children.blobs, &mut heap).to_le_bytes());
        }

        write_header(
            &Header {
                first: self
                    .entries
                    .first()
                    .map_or(Key::ZERO, |(seq, _)| Key::new(*seq)),
                records: self.entries.len(),
                tail: heap.len(),
            },
            OBJECTS,
            out,
        );
        out.extend_from_slice(&entries);
        out.extend_from_slice(&heap);
    }

    fn decode_range(bytes: &[u8], range: KeyRange) -> Result<Self, Malformed> {
        let header = read_header(bytes, OBJECTS)?;
        let body = bytes.get(HEADER..).unwrap_or_default();
        let heap_at = header.records.saturating_mul(ENTRY);
        let heap = body.get(heap_at..).unwrap_or_default();
        let (first, last) = (range.first().get(), range.last().get());

        // Sized for the whole array rather than the window: this is also the
        // path a merge takes, and there the two are the same number.
        let mut entries = Vec::with_capacity(header.records);
        let mut previous: Option<u64> = None;
        for at in 0..header.records {
            let entry = body
                .get(at.saturating_mul(ENTRY)..)
                .and_then(<[u8]>::first_chunk::<ENTRY>)
                .ok_or(Frame::Truncated {
                    expected: HEADER + header.records * ENTRY,
                    actual: bytes.len(),
                })?;

            let seq = field64(entry, 0);
            // Read rather than assumed: a lookup binary-searches this order,
            // so bytes out of order would answer wrongly instead of failing.
            // Checked across the whole array even when only part is wanted,
            // since the order is what makes picking that part sound.
            if previous.is_some_and(|held| held >= seq) {
                return Err(Malformed::Unsorted(at));
            }
            previous = Some(seq);

            // Only the heap reads are skipped, and they are the cost. Not a
            // `break` past `last`: that would leave the tail of the array
            // unchecked, and an unsorted entry there could be one this read
            // wanted, dropped without a word.
            if seq < first || seq > last {
                continue;
            }

            entries.push((
                seq,
                Object {
                    locations: read_locations(field32(entry, 8), heap)?,
                    children: ObjectSeqs {
                        trees: read_bitmap(field32(entry, 12), heap)?,
                        blobs: read_bitmap(field32(entry, 16), heap)?,
                    },
                },
            ));
        }
        Ok(Self { entries })
    }
}

/// Appends a location list to the heap, returning where it went.
fn stow_locations(locations: &[Location], heap: &mut Vec<u8>) -> u32 {
    if locations.is_empty() {
        return NONE;
    }
    let at = u32::try_from(heap.len()).unwrap_or(NONE);
    heap.extend_from_slice(&u32::try_from(locations.len()).unwrap_or(0).to_le_bytes());
    for location in locations {
        heap.extend_from_slice(&location.pack_seq.to_le_bytes());
        heap.extend_from_slice(&location.offset.to_le_bytes());
        heap.extend_from_slice(&location.entry_len.to_le_bytes());
        heap.extend_from_slice(&location.base_seq.unwrap_or(NO_BASE).to_le_bytes());
        heap.extend_from_slice(location.pack_oid.as_bytes());
        heap.extend_from_slice(&location.segment.id.0.to_le_bytes());
        heap.extend_from_slice(&location.segment.base_offset.to_le_bytes());
        heap.extend_from_slice(&location.segment.image_len.to_le_bytes());
    }
    at
}

/// Reads back what [`stow_locations`] wrote.
fn read_locations(at: u32, heap: &[u8]) -> Result<Vec<Location>, Malformed> {
    if at == NONE {
        return Ok(Vec::new());
    }
    // The count of locations, not of bytes: they are a fixed stride, so the
    // heap's own length word counts them and the reader multiplies.
    let count = tail::length(at, heap)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or(Malformed::StrayHeap(at))?;
    let start = usize::try_from(at).map_err(|_wide| Malformed::StrayHeap(at))?;

    let mut locations = Vec::with_capacity(count);
    for index in 0..count {
        let record = start
            .checked_add(4)
            .and_then(|body| body.checked_add(index.checked_mul(LOCATION)?))
            .and_then(|record| heap.get(record..))
            .and_then(<[u8]>::first_chunk::<LOCATION>)
            .ok_or(Malformed::StrayHeap(at))?;

        let base_seq = field64(record, 24);
        locations.push(Location {
            pack_seq: i64::from_le_bytes(
                record
                    .get(0..8)
                    .and_then(|slice| slice.first_chunk::<8>())
                    .copied()
                    .unwrap_or_default(),
            ),
            offset: field64(record, 8),
            entry_len: field64(record, 16),
            base_seq: (base_seq != NO_BASE).then_some(base_seq),
            pack_oid: record
                .get(32..52)
                .and_then(|slice| ObjectId::try_from(slice).ok())
                .ok_or(Malformed::StrayHeap(at))?,
            segment: SegmentLocation {
                id: Ulid(u128::from_le_bytes(
                    record
                        .get(52..68)
                        .and_then(|slice| slice.first_chunk::<16>())
                        .copied()
                        .unwrap_or_default(),
                )),
                base_offset: field64(record, 68),
                image_len: field64(record, 76),
            },
        });
    }
    Ok(locations)
}

/// Appends a bitmap to the heap, returning where it went.
///
/// A bitmap that will not serialize is stored as no children of that kind,
/// which is what an unreadable one would read back as anyway.
fn stow_bitmap(bitmap: &RoaringTreemap, heap: &mut Vec<u8>) -> u32 {
    tail::stow_bitmap(bitmap, heap).unwrap_or(NONE)
}

/// Reads back what [`stow_bitmap`] wrote.
fn read_bitmap(at: u32, heap: &[u8]) -> Result<RoaringTreemap, Malformed> {
    let encoded = tail::fetch(at, heap).ok_or(Malformed::StrayHeap(at))?;
    tail::decode_bitmap(encoded).ok_or(Malformed::Bitmap)
}
