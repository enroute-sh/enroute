//! Tier two: the pack facts, their bytes, and their join.

use proptest::prelude::*;
use roaring::RoaringTreemap;

use enroute_git_core::{SegmentLocation, Ulid};
use enroute_git_graph_store::{Malformed, Pack, PackBuilder, PackIndex};
use enroute_lattice_core::frame::Frame;
use enroute_lattice_core::{Join, Key, KeyRange, Segment, conformance};

fn bitmap(values: &[u64]) -> RoaringTreemap {
    values.iter().copied().collect()
}

fn pack(seed: u64, trees: &[u64], blobs: &[u64]) -> Pack {
    Pack {
        entry_len: seed + 1,
        blob_offset: seed + 2,
        segment: SegmentLocation {
            id: Ulid(u128::from(seed) + 3),
            base_offset: seed + 4,
            image_len: seed + 5,
        },
        trees: bitmap(trees),
        blobs: bitmap(blobs),
    }
}

fn built(entries: &[(i64, Pack)]) -> PackIndex {
    let mut builder = PackBuilder::new();
    for (seq, pack) in entries {
        if builder.insert(*seq, pack).is_err() {
            return PackIndex::default();
        }
    }
    builder.build()
}

fn any_index() -> impl Strategy<Value = PackIndex> {
    prop::collection::vec(
        (0_i64..24, 0_u64..8, prop::collection::vec(0_u64..64, 0..6)),
        0..12,
    )
    .prop_map(|specs| {
        let entries: Vec<(i64, Pack)> = specs
            .into_iter()
            .map(|(seq, seed, trees)| (seq, pack(seed, &trees, &[seed])))
            .collect();
        built(&entries)
    })
}

#[test]
fn a_pack_reads_back_as_it_was_written() {
    let written = pack(10, &[1, 2, 900_000], &[7]);
    let index = built(&[(5, written.clone())]);

    let entry = index.get(5).expect("the pack just inserted");
    assert_eq!(entry.entry_len(), written.entry_len);
    assert_eq!(entry.blob_offset(), written.blob_offset);
    assert_eq!(entry.segment(), written.segment);
    assert_eq!(entry.trees().expect("a bitmap"), written.trees);
    assert_eq!(entry.blobs().expect("a bitmap"), written.blobs);
    assert!(index.get(4).is_none());
}

// NULL means the empty bitmap in the column this replaces, and an empty
// bitmap must cost no heap here either.
#[test]
fn a_pack_with_no_trees_or_blobs_stores_no_heap() {
    let bare = pack(0, &[], &[]);
    let index = built(&[(1, bare.clone())]);

    let entry = index.get(1).expect("the pack");
    assert!(entry.trees().expect("a bitmap").is_empty());
    assert!(entry.blobs().expect("a bitmap").is_empty());

    // Header plus one record, and nothing else.
    assert_eq!(index.encoded().len(), 24 + 60);
    assert_eq!(
        PackIndex::decode(&index.encoded()).expect("round trip"),
        index
    );
}

#[test]
fn a_hole_in_the_range_is_absent_rather_than_empty() {
    let index = built(&[(2, pack(1, &[1], &[])), (6, pack(2, &[], &[2]))]);
    assert_eq!(index.entries().count(), 2);
    assert!(index.get(4).is_none(), "the hole holds nothing");
    assert_eq!(
        Segment::range(&index),
        Some(KeyRange::new(Key::new(2), Key::new(6)).expect("a range"))
    );
}

#[test]
fn an_empty_index_holds_and_says_nothing() {
    let index = PackBuilder::new().build();
    assert_eq!(index.entries().count(), 0);
    assert_eq!(Segment::range(&index), None);
    assert_eq!(
        PackIndex::decode(&index.encoded()).expect("round trip"),
        index
    );
}

#[test]
fn bytes_that_are_not_a_tier_two_segment_are_refused() {
    assert_eq!(
        PackIndex::decode(&[]),
        Err(Malformed::Frame(Frame::Short(0)))
    );
    assert!(matches!(
        PackIndex::decode(&[0; 24]),
        Err(Malformed::Frame(Frame::Magic { .. }))
    ));

    // A tier-one segment must not read as a tier-two one, or a mixed-up
    // catalog row would decode to nonsense rather than to an error.
    let graph = enroute_git_graph_store::Builder::new().build();
    assert!(matches!(
        PackIndex::decode(&graph.encoded()),
        Err(Malformed::Frame(Frame::Short(_) | Frame::Magic { .. }))
    ));
}

#[test]
fn a_bitmap_pointing_outside_the_heap_is_refused() {
    let mut bytes = built(&[(3, pack(1, &[1, 2, 3], &[]))]).encoded();
    // `trees_at` is at byte 48 of the single record.
    let at = 24 + 48;
    bytes[at..at + 4].copy_from_slice(&9_999_u32.to_le_bytes());
    assert_eq!(PackIndex::decode(&bytes), Err(Malformed::StrayExtra(9_999)));
}

proptest! {
    // The bitmaps are the bulk of a pack record, so a narrowed read is the
    // one this layout most needs and a wrong slice would hurt most.
    #[test]
    fn a_pack_index_is_a_segment(
        a in any_index(),
        b in any_index(),
        c in any_index(),
        split in 0_u64..24,
    ) {
        prop_assert_eq!(conformance::check(&a, &b, &c, Key::new(split)), Ok(()));
    }
}

/// A pack image is written once and moves only when it is copied, so the
/// join must answer with the segment it was copied into.
#[test]
fn a_moved_image_wins_over_the_segment_it_moved_from() {
    let written = pack(10, &[1, 2], &[7]);
    let mut moved = written.clone();
    moved.segment.id = Ulid(moved.segment.id.0 + 1000);
    moved.segment.base_offset += 4096;

    let before = built(&[(5, written)]);
    let after = built(&[(5, moved.clone())]);

    let joined = |first: &PackIndex, second: &PackIndex| {
        let mut held = first.clone();
        held.join(second.clone());
        held.get(5).expect("the commit").segment()
    };

    assert_eq!(
        joined(&before, &after),
        moved.segment,
        "the segment it was copied from outlived the copy"
    );
    assert_eq!(
        joined(&after, &before),
        joined(&before, &after),
        "which side the copy arrived on decided the answer"
    );
}
