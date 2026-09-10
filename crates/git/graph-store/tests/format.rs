//! The bytes, the join, and the three laws.

mod common;

use proptest::prelude::*;

use enroute_lattice_core::frame::Frame;

use enroute_git_graph_store::{
    Builder, Commit, CommitIndex, DatedCommit, Malformed, TooLarge, Unwritable,
};
use enroute_lattice_core::{Join, Key, KeyRange, Segment, conformance};

use crate::common::Graph;

fn commit(parents: &[i64], root_tree: u64, generation: u32) -> Commit<'_> {
    Commit {
        parents,
        root_tree,
        generation,
    }
}

fn graph() -> impl Strategy<Value = Graph> {
    prop::collection::vec((0_u8..4, 0_u64..32), 1..24).prop_map(|salts| Graph::woven(&salts))
}

#[test]
fn a_commit_reads_back_as_it_was_written() {
    let mut builder = Builder::new();
    builder
        .insert(7, commit(&[3, 5], 900, 12))
        .expect("in range");
    let index = builder.build();

    let entry = index.get(7).expect("the commit just inserted");
    assert_eq!(entry.parents().iter().collect::<Vec<_>>(), vec![3, 5]);
    assert_eq!(entry.root_tree(), 900);
    assert_eq!(entry.generation(), 12);
    assert!(index.get(6).is_none(), "nothing below it was inserted");
}

#[test]
fn a_root_commit_has_no_parents() {
    let mut builder = Builder::new();
    builder.insert(0, commit(&[], 1, 1)).expect("in range");
    let index = builder.build();

    let entry = index.get(0).expect("the root");
    assert!(entry.parents().is_empty());
    assert_eq!(entry.parents().len(), 0);
    assert_eq!(entry.parents().iter().count(), 0);
}

// Two parents are inline and the rest are not, so an octopus is the case
// where the stride stays fixed only because the tail exists.
#[test]
fn an_octopus_merge_keeps_every_parent_in_order() {
    let parents = [1, 2, 3, 4, 5, 6, 7];
    let mut builder = Builder::new();
    builder
        .insert(8, commit(&parents, 40, 9))
        .expect("in range");
    let index = builder.build();

    let entry = index.get(8).expect("the merge");
    assert_eq!(entry.parents().iter().collect::<Vec<_>>(), parents.to_vec());
    assert_eq!(entry.parents().len(), 7);
    assert_eq!(
        CommitIndex::decode(&index.encoded()).expect("round trip"),
        index
    );
}

#[test]
fn an_empty_index_holds_and_says_nothing() {
    let index = Builder::new().build();
    assert_eq!(Segment::range(&index), None);
    assert!(index.get(0).is_none());
    assert_eq!(
        CommitIndex::decode(&index.encoded()).expect("round trip"),
        index
    );
}

// An aborted push leaves a permanent hole, and the array has to carry it.
#[test]
fn a_hole_in_the_range_is_covered_but_absent() {
    let mut builder = Builder::new();
    builder.insert(4, commit(&[], 1, 1)).expect("in range");
    builder.insert(9, commit(&[4], 2, 2)).expect("in range");
    let index = builder.build();

    assert!(index.get(4).is_some() && index.get(9).is_some());
    assert!(index.get(6).is_none(), "the hole holds nothing");
    assert_eq!(
        Segment::range(&index),
        Some(KeyRange::new(Key::new(4), Key::new(9)).expect("a range")),
        "the range is trimmed to what is present"
    );
}

#[test]
fn a_seq_too_large_for_a_slot_is_refused() {
    let mut builder = Builder::new();
    let huge = i64::from(u32::MAX);
    assert_eq!(
        builder.insert(huge, commit(&[], 1, 1)),
        Err(TooLarge::Commit(huge))
    );
    assert_eq!(
        builder.insert(-1, commit(&[], 1, 1)),
        Err(TooLarge::Commit(-1))
    );
    assert_eq!(
        builder.insert(1, commit(&[], u64::from(u32::MAX), 1)),
        Err(TooLarge::Object(u64::from(u32::MAX)))
    );
    assert_eq!(
        builder.insert(1, commit(&[huge], 1, 1)),
        Err(TooLarge::Commit(huge))
    );
}

#[test]
fn bytes_that_are_not_an_index_are_refused() {
    assert_eq!(
        CommitIndex::decode(&[]),
        Err(Malformed::Frame(Frame::Short(0)))
    );
    assert!(matches!(
        CommitIndex::decode(&[0; 24]),
        Err(Malformed::Frame(Frame::Magic { .. }))
    ));

    let mut builder = Builder::new();
    builder.insert(1, commit(&[], 5, 1)).expect("in range");
    let good = builder.build().encoded();

    let mut wrong_version = good.clone();
    wrong_version[4] = 99;
    assert_eq!(
        CommitIndex::decode(&wrong_version),
        Err(Malformed::Frame(Frame::Version(99)))
    );

    let truncated = good.get(..good.len() - 1).expect("shorter").to_vec();
    assert!(matches!(
        CommitIndex::decode(&truncated),
        Err(Malformed::Frame(Frame::Truncated { .. }))
    ));
}

// A stray offset would otherwise read as "no parents", which is a commit
// quietly losing its history rather than a file failing to load.
#[test]
fn an_octopus_pointing_outside_the_tail_is_refused() {
    let mut builder = Builder::new();
    builder
        .insert(9, commit(&[1, 2, 3], 5, 1))
        .expect("in range");
    let mut bytes = builder.build().encoded();

    // The `extra` field is the last four bytes of the single record.
    let at = 24 + 16;
    bytes[at..at + 4].copy_from_slice(&99_u32.to_le_bytes());
    assert_eq!(CommitIndex::decode(&bytes), Err(Malformed::StrayExtra(99)));
}

proptest! {
    // Slices that overlap, so the join is checked against segments that
    // disagree about a seq rather than against disjoint ones.
    #[test]
    fn a_commit_index_is_a_segment(
        graph in graph(),
        cuts in (1_i64..8, 1_i64..8),
        split in 0_u64..24,
    ) {
        let (low, high) = cuts;
        prop_assert_eq!(
            conformance::check(
                &graph.slice(0, low),
                &graph.slice(low / 2, low + high),
                &graph.index(),
                Key::new(split),
            ),
            Ok(())
        );
    }

    // Nothing in a hole, and a hole is not a commit.
    #[test]
    fn a_composed_index_holds_exactly_what_the_graph_did(graph in graph()) {
        let index = graph.index();
        let last = graph.commits.keys().copied().max().unwrap_or(0);
        for seq in 0..=last {
            let Some(node) = graph.commits.get(&seq) else {
                prop_assert!(index.get(seq).is_none(), "a seq the graph never had");
                continue;
            };
            let entry = index.get(seq).expect("every commit");
            prop_assert_eq!(entry.parents().iter().collect::<Vec<_>>(), node.parents.clone());
            prop_assert_eq!(entry.root_tree(), node.root_tree);
            prop_assert_eq!(entry.generation(), node.generation);
        }
    }
}

// Joining a segment that disagrees must still be a lattice, or compaction
// under an arbitrary grouping is not safe.
#[test]
fn a_disagreement_resolves_the_same_way_whichever_side_arrives_first() {
    let mut one = Builder::new();
    one.insert(3, commit(&[1], 10, 5)).expect("in range");
    let one = one.build();

    let mut two = Builder::new();
    two.insert(3, commit(&[2], 11, 6)).expect("in range");
    let two = two.build();

    let mut forward = one.clone();
    forward.join(two.clone());
    let mut backward = two;
    backward.join(one);
    assert_eq!(forward, backward);

    let mut twice = forward.clone();
    twice.join(forward.clone());
    assert_eq!(twice, forward, "and joining it again changes nothing");
}

// A generation below a parent's would prune a walk that should have gone on,
// so the builder refuses to guess at one it cannot work out.
#[test]
fn a_generation_is_computed_from_parents_or_refused() {
    let mut builder = Builder::new();

    let root = builder
        .insert_dated(
            1,
            DatedCommit {
                parents: &[],
                root_tree: 10,
                committer_date: 1_700_000_000,
            },
        )
        .expect("a root needs no parent");
    assert_eq!(root, 1_700_000_000, "its own date, clearing nothing");

    // Clock skew: a child dated before its parent still outranks it.
    let child = builder
        .insert_dated(
            2,
            DatedCommit {
                parents: &[1],
                root_tree: 11,
                committer_date: 5,
            },
        )
        .expect("its parent is in the builder");
    assert_eq!(child, root + 1);

    let stranger = builder.insert_dated(
        3,
        DatedCommit {
            parents: &[99],
            root_tree: 12,
            committer_date: 1_700_000_100,
        },
    );
    assert_eq!(
        stranger,
        Err(Unwritable::UnknownParent {
            child: 3,
            parent: 99
        })
    );

    // A parent in an older segment: looked up once, then told to the builder.
    builder.know(99, 4_000);
    let joined = builder
        .insert_dated(
            3,
            DatedCommit {
                parents: &[99],
                root_tree: 12,
                committer_date: 100,
            },
        )
        .expect("now it can be ranked");
    assert_eq!(joined, 4_001);

    let index = builder.build();
    for seq in [1_i64, 2, 3] {
        let entry = index.get(seq).expect("every commit");
        for parent in entry.parents().iter() {
            if let Some(parent) = index.get(parent) {
                assert!(
                    parent.generation() < entry.generation(),
                    "a parent must rank below its child"
                );
            }
        }
    }
}
