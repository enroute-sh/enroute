//! A commit graph in a map, to build indexes from and walk the slow way.
//!
//! The reference walks live beside the tests that use them, written from the
//! recursive CTEs the index replaces rather than from the index itself, so
//! agreeing with them is evidence rather than a tautology.

use std::collections::BTreeMap;

use enroute_git_graph_store::{Builder, Commit, CommitIndex};

/// One commit, as the reference holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Node {
    pub(crate) parents: Vec<i64>,
    pub(crate) root_tree: u64,
    pub(crate) generation: u32,
}

/// A commit graph, keyed by seq, with every parent below its child.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Graph {
    pub(crate) commits: BTreeMap<i64, Node>,
}

impl Graph {
    /// A graph of one commit per salt, wired so it varies.
    ///
    /// Every parent is below its child, which is the property `commits.seq`
    /// guarantees and every walk relies on.
    pub(crate) fn woven(salts: &[(u8, u64)]) -> Self {
        let mut graph = Self::default();
        let mut seq: i64 = 0;
        for (at, (fanin, salt)) in salts.iter().enumerate() {
            let parents = wire(at, usize::from(*fanin), *salt);
            let generation = parents
                .iter()
                .filter_map(|parent| graph.commits.get(parent))
                .map(|node| node.generation)
                .max()
                .unwrap_or(0)
                .saturating_add(1);
            graph.commits.insert(
                seq,
                Node {
                    parents,
                    root_tree: salt.wrapping_add(1000),
                    generation,
                },
            );
            seq = seq.saturating_add(1);
        }
        graph
    }

    /// The whole graph as one index.
    pub(crate) fn index(&self) -> CommitIndex {
        self.slice(0, i64::MAX)
    }

    /// The same graph, but only the seqs in `[first, last]`.
    ///
    /// A seq the index cannot hold yields an empty one rather than a panic,
    /// which every assertion here fails on anyway.
    pub(crate) fn slice(&self, first: i64, last: i64) -> CommitIndex {
        let mut builder = Builder::new();
        for (seq, node) in self.commits.range(first..=last) {
            if builder.insert(*seq, node.as_commit()).is_err() {
                return CommitIndex::default();
            }
        }
        builder.build()
    }
}

impl Node {
    pub(crate) fn as_commit(&self) -> Commit<'_> {
        Commit {
            parents: &self.parents,
            root_tree: self.root_tree,
            generation: self.generation,
        }
    }
}

/// Parents for the commit at `at`, all of them below it.
fn wire(at: usize, fanin: usize, salt: u64) -> Vec<i64> {
    let salt = usize::try_from(salt).unwrap_or(0);
    let mut parents: Vec<i64> = Vec::new();
    for step in 0..fanin.min(at) {
        let parent = i64::try_from((salt + step * 7) % at).unwrap_or(0);
        if !parents.contains(&parent) {
            parents.push(parent);
        }
    }
    parents
}
