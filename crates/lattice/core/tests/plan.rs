//! What compaction is told to do, and the bounds it is told to stay inside.

use proptest::prelude::*;

use enroute_lattice_core::{Key, KeyRange, Placement, Policy, Residence, Tier, plan};

fn policy() -> Policy {
    Policy {
        fanout: 4,
        max_inputs: 8,
        max_input_bytes: 1_000,
        graduation_bytes: 256,
        inline_ceiling: 16,
    }
}

fn placement(first: u64, last: u64, tier: u8, bytes: u64) -> Placement {
    Placement {
        range: KeyRange::new(Key::new(first), Key::new(last)).unwrap_or(KeyRange::EVERYTHING),
        tier: Tier::new(tier),
        bytes,
        residence: if bytes >= 256 {
            Residence::Bucket
        } else {
            Residence::Inline
        },
    }
}

/// `count` tier-zero segments of 10 keys and 10 bytes each, laid end to end.
fn run_of(count: u64) -> Vec<Placement> {
    (0..count)
        .map(|index| placement(index * 10, index * 10 + 9, 0, 10))
        .collect()
}

#[test]
fn a_tier_below_the_fanout_is_left_alone() {
    assert!(plan(&run_of(3), &policy()).is_empty());
}

#[test]
fn a_tier_at_the_fanout_merges_into_the_next() {
    let merges = plan(&run_of(4), &policy());
    assert_eq!(merges.len(), 1);
    let merge = &merges[0];
    assert_eq!(merge.inputs, vec![0, 1, 2, 3]);
    assert_eq!(merge.tier, Tier::new(1));
    assert_eq!(merge.range, KeyRange::new(Key::ZERO, Key::new(39)).unwrap());
}

// Peak memory is one merge's inputs, so the count bound has to hold even
// when the tier holds far more than one merge's worth.
#[test]
fn a_merge_takes_no_more_than_max_inputs() {
    let merges = plan(&run_of(20), &policy());
    assert!(
        merges.iter().all(|merge| merge.inputs.len() <= 8),
        "{merges:?}"
    );
    assert_eq!(
        merges.iter().map(|merge| merge.inputs.len()).sum::<usize>(),
        20
    );
}

#[test]
fn a_merge_takes_no_more_than_max_input_bytes() {
    let placements: Vec<Placement> = (0..6)
        .map(|index| placement(index * 10, index * 10 + 9, 0, 400))
        .collect();
    let merges = plan(&placements, &policy());
    assert!(!merges.is_empty());
    for merge in &merges {
        let bytes: u64 = merge
            .inputs
            .iter()
            .map(|index| placements[*index].bytes)
            .sum();
        // Two inputs are always allowed: a merge of one is not a merge.
        assert!(bytes <= 1_000 || merge.inputs.len() == 2, "{merge:?}");
    }
}

// Rewriting one segment as itself is pure cost, so a leftover is left over.
#[test]
fn a_run_of_one_is_not_a_merge() {
    let merges = plan(&run_of(9), &policy());
    assert_eq!(merges.len(), 1, "{merges:?}");
    assert_eq!(merges[0].inputs.len(), 8);
}

// The inlined tail is what Postgres carries, so it has a ceiling of its own
// that does not wait for the fanout.
#[test]
fn too_many_inlined_segments_force_a_merge_below_the_fanout() {
    let mut policy = policy();
    policy.fanout = 100;
    policy.inline_ceiling = 2;
    let merges = plan(&run_of(3), &policy);
    assert_eq!(merges.len(), 1, "{merges:?}");
    assert_eq!(merges[0].inputs.len(), 3);
}

#[test]
fn inline_pressure_is_relieved_by_the_lowest_tier_that_can() {
    let mut policy = policy();
    policy.fanout = 100;
    policy.inline_ceiling = 1;
    let placements = vec![
        placement(0, 9, 0, 10),
        placement(10, 19, 0, 10),
        placement(20, 29, 1, 10),
        placement(30, 39, 1, 10),
    ];
    let merges = plan(&placements, &policy);
    assert_eq!(
        merges.len(),
        1,
        "one tier relieves it, not every tier: {merges:?}"
    );
    assert_eq!(merges[0].tier, Tier::new(1));
}

#[test]
fn where_a_segment_belongs_follows_its_size() {
    let policy = policy();
    assert_eq!(policy.residence_for(255), Residence::Inline);
    assert_eq!(policy.residence_for(256), Residence::Bucket);
}

fn any_placement() -> impl Strategy<Value = Placement> {
    (0_u64..200, 0_u64..50, 0_u8..3, 1_u64..500)
        .prop_map(|(first, span, tier, bytes)| placement(first, first + span, tier, bytes))
}

proptest! {
    // Whatever the plan says, running it must not lose or duplicate a
    // segment, and must not widen what the result claims to cover.
    #[test]
    fn a_plan_partitions_what_it_touches(
        placements in prop::collection::vec(any_placement(), 0..30),
    ) {
        let merges = plan(&placements, &policy());
        let mut seen = std::collections::HashSet::new();
        for merge in &merges {
            prop_assert!(merge.inputs.len() >= 2, "{merge:?}");
            for index in &merge.inputs {
                prop_assert!(seen.insert(*index), "index {index} merged twice");
                prop_assert!(*index < placements.len());
            }

            let inputs: Vec<&Placement> = merge.inputs.iter().map(|i| &placements[*i]).collect();

            let tier = inputs[0].tier;
            prop_assert!(inputs.iter().all(|p| p.tier == tier), "mixed tiers: {merge:?}");
            prop_assert_eq!(merge.tier, tier.next());

            let hull = inputs.iter().map(|p| p.range).reduce(KeyRange::hull).unwrap();
            prop_assert_eq!(merge.range, hull);
        }
    }

    // Nothing above may assume the ranges are disjoint, and planning is the
    // first thing that would notice if it did.
    #[test]
    fn overlapping_ranges_plan_the_same_as_any_other(
        placements in prop::collection::vec(
            (0_u64..20, 0_u64..100).prop_map(|(first, span)| placement(first, first + span, 0, 10)),
            4..20,
        ),
    ) {
        let merges = plan(&placements, &policy());
        prop_assert!(!merges.is_empty());
        for merge in merges {
            let hull = merge
                .inputs
                .iter()
                .map(|index| placements[*index].range)
                .reduce(KeyRange::hull)
                .unwrap();
            prop_assert_eq!(merge.range, hull);
        }
    }
}
