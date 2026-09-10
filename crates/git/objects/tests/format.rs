//! The bytes, the join, and the three laws.

use proptest::prelude::*;

use enroute_git_core::{ObjectSeqs, SegmentLocation, Ulid, oid};
use enroute_git_objects::{Builder, Location, Malformed, ObjectIndex};
use enroute_lattice_core::frame::Frame;
use enroute_lattice_core::{Join, Key, KeyRange, Segment, conformance};

fn location(pack_seq: i64, base_seq: Option<u64>) -> Location {
    let seed = pack_seq.unsigned_abs();
    Location {
        pack_seq,
        pack_oid: oid(u8::try_from(pack_seq % 251).unwrap_or(0)),
        segment: SegmentLocation {
            id: Ulid(u128::from(seed) + 1),
            base_offset: seed + 2,
            image_len: seed + 3,
        },
        offset: seed + 4,
        entry_len: seed + 5,
        base_seq,
    }
}

/// Children split the way the two numberings hold them: odd seqs are blobs,
/// which is arbitrary but keeps both halves exercised.
fn children(values: &[u64]) -> ObjectSeqs {
    ObjectSeqs {
        trees: values.iter().copied().filter(|v| v % 2 == 0).collect(),
        blobs: values.iter().copied().filter(|v| v % 2 == 1).collect(),
    }
}

fn any_index() -> impl Strategy<Value = ObjectIndex> {
    prop::collection::vec(
        (
            0_u64..40,
            0_i64..8,
            prop::option::of(0_u64..40),
            prop::collection::vec(0_u64..64, 0..4),
        ),
        0..14,
    )
    .prop_map(|specs| {
        let mut builder = Builder::new();
        for (seq, pack_seq, base, children) in specs {
            builder.locate(seq, location(pack_seq, base));
            if !children.is_empty() {
                builder.children(seq, &self::children(&children));
            }
        }
        builder.build()
    })
}

#[test]
fn an_object_reads_back_with_every_pack_it_is_in() {
    let mut builder = Builder::new();
    builder.locate(7, location(3, None));
    builder.locate(7, location(1, Some(4)));
    let index = builder.build();

    let object = index.get(7).expect("the object just inserted");
    assert_eq!(
        object
            .locations
            .iter()
            .map(|l| l.pack_seq)
            .collect::<Vec<_>>(),
        vec![1, 3],
        "lowest pack seq first, whatever order they arrived in"
    );
    assert_eq!(
        object.introducing().map(|l| l.pack_seq),
        Some(1),
        "the introducing pack is the lowest, with no column saying so"
    );
    assert_eq!(object.introducing().and_then(|l| l.base_seq), Some(4));
    assert!(index.get(6).is_none());
}

// The same entry arriving twice is one entry: a re-push stores nothing new.
#[test]
fn the_same_location_twice_is_recorded_once() {
    let mut builder = Builder::new();
    builder.locate(1, location(2, None));
    builder.locate(1, location(2, None));
    let index = builder.build();
    assert_eq!(index.get(1).expect("the object").locations.len(), 1);
}

#[test]
fn a_tree_carries_its_entries_and_a_blob_carries_none() {
    let mut builder = Builder::new();
    builder.locate(1, location(1, None));
    builder.children(1, &children(&[4, 9, 900_000]));
    builder.locate(2, location(1, None));
    let index = builder.build();

    assert_eq!(
        index.get(1).expect("the tree").children,
        children(&[4, 9, 900_000])
    );
    assert!(index.get(2).expect("the blob").children.is_empty());
    assert_eq!(
        ObjectIndex::decode(&index.encoded()).expect("round trip"),
        index
    );
}

#[test]
fn an_empty_index_holds_and_says_nothing() {
    let index = Builder::new().build();
    assert!(index.get(0).is_none());
    assert_eq!(Segment::range(&index), None);
    assert_eq!(
        ObjectIndex::decode(&index.encoded()).expect("round trip"),
        index
    );
}

// Sparse on purpose: a tree segment skips every blob between its trees, and
// must cost nothing for the gap.
#[test]
fn a_wide_gap_costs_two_entries_and_no_more() {
    let mut builder = Builder::new();
    builder.locate(0, location(1, None));
    builder.locate(1_000_000, location(1, None));
    let index = builder.build();

    assert!(index.get(0).is_some() && index.get(1_000_000).is_some());
    assert!(index.get(500_000).is_none(), "the gap holds nothing");
    assert_eq!(
        Segment::range(&index),
        Some(KeyRange::new(Key::ZERO, Key::new(1_000_000)).expect("a range"))
    );
    // A header, two entries, and one location list they share the shape of.
    assert!(
        index.encoded().len() < 300,
        "a million-wide gap must not be a million slots: {}",
        index.encoded().len()
    );
}

#[test]
fn bytes_that_are_not_an_object_index_are_refused() {
    assert_eq!(
        ObjectIndex::decode(&[]),
        Err(Malformed::Frame(Frame::Short(0)))
    );
    assert!(matches!(
        ObjectIndex::decode(&[0; 24]),
        Err(Malformed::Frame(Frame::Magic { .. }))
    ));

    let mut builder = Builder::new();
    builder.locate(1, location(1, None));
    let good = builder.build().encoded();

    let mut wrong_version = good.clone();
    wrong_version[4] = 99;
    assert_eq!(
        ObjectIndex::decode(&wrong_version),
        Err(Malformed::Frame(Frame::Version(99)))
    );

    let truncated = good.get(..good.len() - 1).expect("shorter").to_vec();
    assert!(matches!(
        ObjectIndex::decode(&truncated),
        Err(Malformed::Frame(Frame::Truncated { .. }))
    ));
}

// A lookup binary-searches the order, so bytes out of order would answer
// wrongly rather than fail — which is why decode checks rather than trusts.
#[test]
fn entries_out_of_order_are_refused() {
    let mut builder = Builder::new();
    builder.locate(1, location(1, None));
    builder.locate(2, location(1, None));
    let mut bytes = builder.build().encoded();

    // Swap the two seqs so they descend. An entry is a seq and three heap
    // references, and the header in front of them is twenty-four bytes.
    let (first, second) = (24, 24 + 20);
    for byte in 0..8 {
        bytes.swap(first + byte, second + byte);
    }
    assert_eq!(ObjectIndex::decode(&bytes), Err(Malformed::Unsorted(1)));
}

#[test]
fn a_heap_reference_outside_the_heap_is_refused() {
    let mut builder = Builder::new();
    builder.locate(3, location(1, None));
    let mut bytes = builder.build().encoded();
    // The locations offset is bytes 8..12 of the single entry.
    let at = 24 + 8;
    bytes[at..at + 4].copy_from_slice(&9_999_u32.to_le_bytes());
    assert_eq!(
        ObjectIndex::decode(&bytes),
        Err(Malformed::StrayHeap(9_999))
    );
}

proptest! {
    // Sorted pairs, not a stride — so the narrowing is a filter, and what it
    // must not do is drop an entry the range covers.
    #[test]
    fn an_object_index_is_a_segment(
        a in any_index(),
        b in any_index(),
        c in any_index(),
        split in 0_u64..40,
    ) {
        prop_assert_eq!(conformance::check(&a, &b, &c, Key::new(split)), Ok(()));
    }

    // The property the whole design rests on: a pack learned by one segment
    // and a pack learned by another are both true of the object.
    #[test]
    fn joining_unions_the_packs_an_object_is_in(seq in 0_u64..40) {
        let mut one = Builder::new();
        one.locate(seq, location(1, None));
        one.children(seq, &children(&[10]));
        let mut one = one.build();

        let mut two = Builder::new();
        two.locate(seq, location(5, Some(2)));
        two.children(seq, &children(&[20]));
        let two = two.build();

        one.join(two);
        let object = one.get(seq).expect("the object");
        prop_assert_eq!(
            object.locations.iter().map(|l| l.pack_seq).collect::<Vec<_>>(),
            vec![1, 5]
        );
        prop_assert_eq!(&object.children, &children(&[10, 20]));
    }

}

/// The same entry, in the segment its image was copied into.
fn moved(location: Location, segment: u128) -> Location {
    Location {
        segment: SegmentLocation {
            id: Ulid(segment),
            ..location.segment
        },
        ..location
    }
}

fn one(seq: u64, location: Location) -> ObjectIndex {
    let mut builder = Builder::new();
    builder.locate(seq, location);
    builder.build()
}

/// A pack's image is in one segment at a time, so copying it retires the
/// old location rather than adding a second one beside it.
#[test]
fn a_moved_image_replaces_the_location_it_moved_from() {
    let before = location(3, None);
    let after = moved(before, before.segment.id.0 + 1000);

    let mut joined = one(7, before);
    joined.join(one(7, after));

    assert_eq!(
        joined.get(7).expect("the object").locations,
        vec![after],
        "the older segment's location outlived the copy"
    );
}

/// Which side the copy arrived on must not decide the answer.
///
/// Two readers holding different segments would otherwise disagree about
/// where an object is, and both be right.
#[test]
fn a_moved_image_wins_from_either_side() {
    let before = location(3, None);
    let after = moved(before, before.segment.id.0 + 1000);

    let joined = |first: Location, second: Location| {
        let mut index = one(7, first);
        index.join(one(7, second));
        index.get(7).expect("the object").locations.clone()
    };

    assert_eq!(joined(before, after), joined(after, before));
    assert_eq!(joined(before, after), vec![after]);
}

/// Two packs are two locations; only two copies of one pack collapse.
#[test]
fn separate_packs_keep_a_location_each() {
    let mut builder = Builder::new();
    builder.locate(7, location(3, None));
    builder.locate(7, location(5, None));

    assert_eq!(
        builder.build().get(7).expect("the object").locations.len(),
        2,
        "an object in two packs is in two packs"
    );
}
