//! The band walks, against a graph walked the slow obvious way.

mod common;

use std::collections::{BTreeMap, BTreeSet, HashSet};

use proptest::prelude::*;

use enroute_git_graph_store::{Builder, Commit, CommitIndex, FLAG_HAVE, FLAG_WANT, NotCovered};

use crate::common::Graph;

fn graph() -> impl Strategy<Value = Graph> {
    prop::collection::vec((0_u8..4, 0_u64..32), 2..24).prop_map(|salts| Graph::woven(&salts))
}

/// A commit's parents in the reference graph.
fn parents(graph: &Graph, seq: i64) -> &[i64] {
    graph
        .commits
        .get(&seq)
        .map_or(&[][..], |node| node.parents.as_slice())
}

/// Everything reachable from `seeds`, split at `floor`, by fixpoint.
fn ancestors(graph: &Graph, seeds: &[i64], floor: i64) -> (HashSet<i64>, HashSet<i64>) {
    let mut walk: BTreeSet<i64> = seeds.iter().copied().collect();
    loop {
        let mut grown = BTreeSet::new();
        for seq in &walk {
            if *seq < floor {
                continue;
            }
            grown.extend(parents(graph, *seq).iter().copied());
        }
        let before = walk.len();
        walk.extend(grown);
        if walk.len() == before {
            break;
        }
    }
    walk.into_iter().partition(|seq| *seq >= floor)
}

/// The `(parent, flag)` pairs one painted commit hands on.
fn grow(
    graph: &Graph,
    seq: i64,
    flag: u8,
    have_seeds: &HashSet<i64>,
    grown: &mut BTreeSet<(i64, u8)>,
) {
    for parent in parents(graph, seq) {
        if flag == FLAG_WANT && have_seeds.contains(parent) {
            continue;
        }
        grown.insert((*parent, flag));
    }
}

/// Paint flags per seq, by fixpoint over `(seq, flag)` pairs.
fn paint(
    graph: &Graph,
    seeds: &[i64],
    flags: &[u8],
    floor: i64,
    want_block: &[i64],
    have_block: &[i64],
) -> BTreeMap<i64, u8> {
    let have_seeds: HashSet<i64> = seeds
        .iter()
        .zip(flags)
        .filter(|(_, flag)| **flag == FLAG_HAVE)
        .map(|(seq, _)| *seq)
        .collect();

    let mut walk: BTreeSet<(i64, u8)> = seeds.iter().copied().zip(flags.iter().copied()).collect();
    loop {
        let mut grown = BTreeSet::new();
        for (seq, flag) in &walk {
            if *seq < floor
                || (*flag == FLAG_WANT && want_block.contains(seq))
                || (*flag == FLAG_HAVE && have_block.contains(seq))
            {
                continue;
            }
            grow(graph, *seq, *flag, &have_seeds, &mut grown);
        }
        let before = walk.len();
        walk.extend(grown);
        if walk.len() == before {
            break;
        }
    }

    let mut painted: BTreeMap<i64, u8> = BTreeMap::new();
    for (seq, flag) in walk {
        *painted.entry(seq).or_insert(0) |= flag;
    }
    painted
}

/// Minimum depth per seq, by fixpoint over `(seq, depth)` pairs.
fn depths(
    graph: &Graph,
    seeds: &[i64],
    seeded: &[i64],
    floor: i64,
    limit: i64,
) -> BTreeMap<i64, i64> {
    let mut walk: BTreeSet<(i64, i64)> =
        seeds.iter().copied().zip(seeded.iter().copied()).collect();
    loop {
        let mut grown = BTreeSet::new();
        for (seq, depth) in &walk {
            if *seq < floor || *depth >= limit {
                continue;
            }
            for parent in parents(graph, *seq) {
                grown.insert((*parent, depth + 1));
            }
        }
        let before = walk.len();
        walk.extend(grown);
        if walk.len() == before {
            break;
        }
    }

    let mut best: BTreeMap<i64, i64> = BTreeMap::new();
    for (seq, depth) in walk {
        let slot = best.entry(seq).or_insert(depth);
        *slot = (*slot).min(depth);
    }
    best
}

/// A line of commits, each the child of the last.
///
/// An index that cannot hold them comes back empty rather than panicking,
/// which every assertion below fails on anyway.
fn chain(length: u32) -> CommitIndex {
    let mut builder = Builder::new();
    for step in 0..length {
        let seq = i64::from(step);
        let parents = if step == 0 { Vec::new() } else { vec![seq - 1] };
        let commit = Commit {
            parents: &parents,
            root_tree: u64::from(step) + 100,
            generation: step + 1,
        };
        if builder.insert(seq, commit).is_err() {
            return CommitIndex::default();
        }
    }
    builder.build()
}

#[test]
fn a_walk_stops_at_the_floor_and_reports_what_it_found_there() {
    let index = chain(10);
    let (visited, below) = index.expand_ancestors(&[9], 5).expect("covered");

    assert_eq!(visited, HashSet::from([9, 8, 7, 6, 5]));
    assert_eq!(below, HashSet::from([4]), "one boundary parent, unexpanded");
}

#[test]
fn a_seed_below_the_floor_is_reported_rather_than_walked() {
    let index = chain(10);
    let (visited, below) = index.expand_ancestors(&[2], 5).expect("covered");
    assert!(visited.is_empty());
    assert_eq!(below, HashSet::from([2]));
}

// The index is a source of truth, so a band it does not cover has to say so
// rather than hand back a short answer.
#[test]
fn a_walk_into_a_seq_the_index_lacks_is_an_error() {
    let mut builder = Builder::new();
    builder
        .insert(
            9,
            Commit {
                parents: &[4],
                root_tree: 1,
                generation: 2,
            },
        )
        .expect("in range");
    let index = builder.build();

    assert_eq!(
        index.expand_ancestors(&[9], 0),
        Err(NotCovered { seq: 4, floor: 0 })
    );
}

// Want-paint must not travel through a have, which is the whole of what
// "commits needed" means.
#[test]
fn paint_returns_only_what_the_wants_alone_reach() {
    // 0 <- 1 <- 2 <- 3, and a side branch 1 <- 4 <- 5.
    let mut builder = Builder::new();
    for (seq, parents) in [
        (0_i64, vec![]),
        (1, vec![0]),
        (2, vec![1]),
        (3, vec![2]),
        (4, vec![1]),
        (5, vec![4]),
    ] {
        builder
            .insert(
                seq,
                Commit {
                    parents: &parents,
                    root_tree: u64::try_from(seq).expect("positive") + 1,
                    generation: 1,
                },
            )
            .expect("in range");
    }
    let index = builder.build();

    let band = index
        .expand_paint(&[5, 3], &[FLAG_WANT, FLAG_HAVE], 0, &[], &[])
        .expect("covered");
    assert_eq!(
        band.finalized,
        vec![4, 5],
        "the side branch only, since 0 and 1 are under the have"
    );
    assert!(
        band.below.is_empty(),
        "the floor is the bottom of the graph"
    );
}

#[test]
fn depth_paints_the_shortest_distance_from_any_want() {
    let index = chain(8);
    let band = index.expand_depth(&[7], &[0], 0, 3).expect("covered");

    let depths: Vec<(i64, i64)> = band
        .reached
        .iter()
        .map(|reached| (reached.seq, reached.depth))
        .collect();
    assert_eq!(depths, vec![(4, 3), (5, 2), (6, 1), (7, 0)]);
    assert!(
        band.reached
            .iter()
            .all(|r| r.root_tree_seq == u64::try_from(r.seq).expect("positive") + 100),
        "each carries its own root tree"
    );
}

proptest! {
    #[test]
    fn ancestors_agree_with_the_slow_walk(graph in graph(), floor in 0_i64..12, seed in 0_usize..24) {
        let index = graph.index();
        let Some(&top) = graph.commits.keys().next_back() else { return Ok(()) };
        let seeds = [i64::try_from(seed).expect("small") % (top + 1)];

        let (visited, below) = index.expand_ancestors(&seeds, floor).expect("whole graph");
        prop_assert_eq!((visited, below), ancestors(&graph, &seeds, floor));
    }

    #[test]
    fn paint_agrees_with_the_slow_walk(
        graph in graph(),
        floor in 0_i64..10,
        want in 0_usize..24,
        have in 0_usize..24,
    ) {
        let index = graph.index();
        let Some(&top) = graph.commits.keys().next_back() else { return Ok(()) };
        let seeds = [
            i64::try_from(want).expect("small") % (top + 1),
            i64::try_from(have).expect("small") % (top + 1),
        ];
        if seeds[0] == seeds[1] {
            return Ok(());
        }
        let flags = [FLAG_WANT, FLAG_HAVE];

        let band = index.expand_paint(&seeds, &flags, floor, &[], &[]).expect("whole graph");
        let painted = paint(&graph, &seeds, &flags, floor, &[], &[]);

        let expected_finalized: Vec<i64> = painted
            .iter()
            .filter(|(seq, flags)| **seq >= floor && **flags == FLAG_WANT)
            .map(|(seq, _)| *seq)
            .collect();
        let expected_below: BTreeMap<i64, u8> = painted
            .into_iter()
            .filter(|(seq, _)| *seq < floor)
            .collect();
        prop_assert_eq!(band.finalized, expected_finalized);
        prop_assert_eq!(band.below, expected_below);
    }

    #[test]
    fn depth_agrees_with_the_slow_walk(
        graph in graph(),
        floor in 0_i64..10,
        limit in 0_i64..8,
        seed in 0_usize..24,
    ) {
        let index = graph.index();
        let Some(&top) = graph.commits.keys().next_back() else { return Ok(()) };
        let seeds = [i64::try_from(seed).expect("small") % (top + 1)];

        let band = index.expand_depth(&seeds, &[0], floor, limit).expect("whole graph");
        let expected = depths(&graph, &seeds, &[0], floor, limit);

        let mut found: BTreeMap<i64, i64> = band
            .reached
            .iter()
            .map(|reached| (reached.seq, reached.depth))
            .collect();
        found.extend(band.below);
        prop_assert_eq!(found, expected);
    }

    // The walk must not care whether the graph arrived as one segment or
    // several, which is the point of composing before walking.
    #[test]
    fn a_composed_index_walks_the_same_as_a_whole_one(graph in graph(), floor in 0_i64..10) {
        let whole = graph.index();
        let Some(&top) = graph.commits.keys().next_back() else { return Ok(()) };

        let mut composed = graph.slice(top / 2 + 1, top);
        enroute_lattice_core::Join::join(&mut composed, graph.slice(0, top / 2));
        prop_assert_eq!(&composed, &whole);

        let seeds = [top];
        prop_assert_eq!(
            composed.expand_ancestors(&seeds, floor).expect("covered"),
            whole.expand_ancestors(&seeds, floor).expect("covered")
        );
    }
}
