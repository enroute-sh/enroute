//! The write as a value: what to list, what to keep, what to retire.

use enroute_git_core::Ulid;
use enroute_lattice_store::{Scope, Written};

/// One of the four segmented lists the engine keeps.
///
/// Named here rather than in the crate that reads each, because what a write
/// lands is one set spanning both of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Index {
    /// What a commit points at, keyed by commit seq.
    CommitGraph,
    /// Where a commit's pack image is, and what it holds.
    CommitPacks,
    /// Where a tree is stored, and what its entries are.
    Trees,
    /// Where a blob is stored.
    Blobs,
}

impl Index {
    /// Every list, for a caller that has to visit each in turn.
    pub const ALL: [Self; 4] = [
        Self::CommitGraph,
        Self::CommitPacks,
        Self::Trees,
        Self::Blobs,
    ];

    /// This list's one of four, in [`Self::ALL`]'s order.
    ///
    /// What lets a ledger hold one catalog per list rather than a list to
    /// search: naming a list it does not hold is not a thing that exists.
    pub fn of<T>(self, each: &[T; 4]) -> &T {
        let [commit_graph, commit_packs, trees, blobs] = each;
        match self {
            Self::CommitGraph => commit_graph,
            Self::CommitPacks => commit_packs,
            Self::Trees => trees,
            Self::Blobs => blobs,
        }
    }
}

/// One segment to list, and where it belongs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listing {
    /// Which list it goes in.
    pub index: Index,
    /// Which value within that list.
    pub scope: Scope,
    /// The segment, already encoded and already put.
    pub segment: Written,
}

/// Every row one write adds to the indexes, and to what keeps them alive.
///
/// A plain value: appending does no I/O and cannot fail, so a caller may build
/// one across as many bucket puts as it likes with no transaction open.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Journal {
    listed: Vec<Listing>,
    registered: Vec<Ulid>,
    retired: Vec<Ulid>,
}

impl Journal {
    /// A journal with nothing in it.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lists `segment` under `scope` of `index`.
    pub fn list(&mut self, index: Index, scope: &Scope, segment: Written) {
        self.listed.push(Listing {
            index,
            scope: scope.clone(),
            segment,
        });
    }

    /// Keeps these pack images alive against the sweep.
    pub fn register(&mut self, segments: impl IntoIterator<Item = Ulid>) {
        self.registered.extend(segments);
    }

    /// Hands these pack images to the sweep, a grace window from now.
    pub fn retire(&mut self, segments: impl IntoIterator<Item = Ulid>) {
        self.retired.extend(segments);
    }

    /// Whether this write would land no rows at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.listed.is_empty() && self.registered.is_empty() && self.retired.is_empty()
    }

    /// The segments to list.
    #[must_use]
    pub fn listed(&self) -> &[Listing] {
        &self.listed
    }

    /// The pack images to keep.
    #[must_use]
    pub fn registered(&self) -> &[Ulid] {
        &self.registered
    }

    /// The pack images to retire.
    #[must_use]
    pub fn retired(&self) -> &[Ulid] {
        &self.retired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_journal_with_nothing_appended_is_empty() {
        assert!(Journal::new().is_empty());
    }

    #[test]
    fn retiring_alone_is_still_a_write() {
        let mut journal = Journal::new();
        journal.retire([Ulid(1)]);
        assert!(
            !journal.is_empty(),
            "a gather's retirement would be dropped"
        );
    }
}
