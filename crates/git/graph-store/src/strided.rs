//! A slot per seq, which is what both commit tiers are underneath.
//!
//! Commit seqs are handed out in order, so a repository's are dense and a
//! lookup can be arithmetic rather than a search. Both ends are always
//! occupied: a segment whose range claimed a hole would be fetched for a
//! read it has nothing to answer. What one slot holds is each tier's own,
//! and so is how it lays the slots out as bytes; this owns the arithmetic
//! and the join over them.

use std::collections::BTreeMap;

use enroute_lattice_core::{Key, KeyRange};

/// One node per seq from the first, and a hole where a seq has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Strided<N> {
    first: i64,
    slots: Vec<Option<N>>,
}

/// Nothing, at a seq nothing is at.
///
/// Written out because deriving it would ask the same of a node, which is a
/// value each tier only ever has a real one of.
impl<N> Default for Strided<N> {
    fn default() -> Self {
        Self {
            first: 0,
            slots: Vec::new(),
        }
    }
}

impl<N> Strided<N> {
    /// The seq the first slot is for.
    pub(crate) const fn first(&self) -> i64 {
        self.first
    }

    /// Every slot, one per seq from [`Strided::first`].
    pub(crate) fn slots(&self) -> &[Option<N>] {
        &self.slots
    }

    /// What is at `seq`, or `None` for a hole or a seq outside the range.
    pub(crate) fn get(&self, seq: i64) -> Option<&N> {
        if seq < self.first {
            return None;
        }
        self.slots.get(offset(self.first, seq))?.as_ref()
    }

    /// The highest seq present, or `None` when none is.
    pub(crate) fn last(&self) -> Option<i64> {
        let at = self.slots.iter().rposition(Option::is_some)?;
        i64::try_from(at)
            .ok()
            .map(|at| self.first.saturating_add(at))
    }

    /// Whether it holds nothing at all.
    pub(crate) fn is_empty(&self) -> bool {
        self.last().is_none()
    }

    /// Every seq it holds with what is there, ascending.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (i64, &N)> {
        self.slots.iter().enumerate().filter_map(|(at, slot)| {
            let seq = self.first.saturating_add(i64::try_from(at).ok()?);
            Some((seq, slot.as_ref()?))
        })
    }

    /// The keys it covers, or `None` when it holds none.
    pub(crate) fn range(&self) -> Option<KeyRange> {
        let last = self.last()?;
        let first = u64::try_from(self.first).ok()?;
        KeyRange::new(Key::new(first), Key::new(u64::try_from(last).ok()?)).ok()
    }
}

impl<N: Ord> Strided<N> {
    /// Nodes laid out over the seqs they span.
    pub(crate) fn laid_out(nodes: BTreeMap<i64, N>) -> Self {
        let (Some((&first, _)), Some((&last, _))) = (nodes.iter().next(), nodes.iter().next_back())
        else {
            return Self::default();
        };
        let mut slots = holes(span_of(first, last));
        for (seq, node) in nodes {
            if let Some(slot) = slots.get_mut(offset(first, seq)) {
                *slot = Some(node);
            }
        }
        Self::trimmed(first, slots)
    }

    /// `slots` from `first`, less the holes at either end.
    pub(crate) fn trimmed(first: i64, mut slots: Vec<Option<N>>) -> Self {
        let Some(lowest) = slots.iter().position(Option::is_some) else {
            return Self::default();
        };
        let highest = slots.iter().rposition(Option::is_some).unwrap_or(lowest);
        slots.truncate(highest.saturating_add(1));
        slots.drain(..lowest);
        Self {
            first: first.saturating_add(i64::try_from(lowest).unwrap_or(0)),
            slots,
        }
    }

    /// Union, and on a seq both hold, the greater node.
    ///
    /// A total order over the nodes is what makes the join a lattice, so
    /// each tier decides which of two it means to keep by ordering them.
    pub(crate) fn joined(self, other: Self) -> Self {
        if other.is_empty() {
            return self;
        }
        if self.is_empty() {
            return other;
        }
        let (Some(mine), Some(theirs)) = (self.last(), other.last()) else {
            return self;
        };
        let first = self.first.min(other.first);
        let mut slots = holes(span_of(first, mine.max(theirs)));
        absorb(self, first, &mut slots);
        absorb(other, first, &mut slots);
        Self::trimmed(first, slots)
    }
}

/// Merges every node of `source` into `slots`, keeping the greater.
fn absorb<N: Ord>(source: Strided<N>, first: i64, slots: &mut [Option<N>]) {
    let Strided {
        first: from,
        slots: taken,
    } = source;
    for (at, node) in taken.into_iter().enumerate() {
        let Some(node) = node else { continue };
        let Ok(at) = i64::try_from(at) else { continue };
        let Some(slot) = slots.get_mut(offset(first, from.saturating_add(at))) else {
            continue;
        };
        if slot.as_ref().is_none_or(|held| *held < node) {
            *slot = Some(node);
        }
    }
}

/// A span of empty slots, which is what a layout starts as.
fn holes<N>(span: usize) -> Vec<Option<N>> {
    core::iter::repeat_with(|| None).take(span).collect()
}

/// How many slots a range of seqs needs.
fn span_of(first: i64, last: i64) -> usize {
    usize::try_from(last.saturating_sub(first).saturating_add(1)).unwrap_or(0)
}

/// Where `seq` sits in an array starting at `first`.
pub(crate) fn offset(first: i64, seq: i64) -> usize {
    usize::try_from(seq.saturating_sub(first)).unwrap_or(usize::MAX)
}
