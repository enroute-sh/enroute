//! The dense integer key space segments are ranged over.

use std::num::NonZeroUsize;

use thiserror::Error;

/// A position in the key space a segment covers part of.
///
/// Dense is the property that matters: it makes a segment an array, so a
/// lookup is offset arithmetic rather than a search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(u64);

impl Key {
    /// The lowest key there is.
    pub const ZERO: Self = Self(0);

    /// The key at `value`.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// This key as a number, for the arithmetic a reader indexes with.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Key {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// A range with no keys in it, which no segment can cover.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("a key range must not end below where it starts: {first} > {last}")]
pub struct EmptyRange {
    /// Where the range was asked to start.
    pub first: u64,
    /// Where it was asked to end.
    pub last: u64,
}

/// An inclusive range of keys, which is what one segment covers.
///
/// Ranges are neither disjoint nor contiguous across a set of segments, and
/// nothing here may assume they are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRange {
    first: Key,
    last: Key,
}

impl KeyRange {
    /// Every key there is, for a read that wants whatever a scope holds.
    pub const EVERYTHING: Self = Self {
        first: Key::ZERO,
        last: Key(u64::MAX),
    };

    /// The range from `first` to `last`, both included.
    ///
    /// # Errors
    /// [`EmptyRange`] when `last` is below `first`.
    pub const fn new(first: Key, last: Key) -> Result<Self, EmptyRange> {
        if last.0 < first.0 {
            return Err(EmptyRange {
                first: first.0,
                last: last.0,
            });
        }
        Ok(Self { first, last })
    }

    /// The lowest key in the range.
    #[must_use]
    pub const fn first(self) -> Key {
        self.first
    }

    /// The highest key in the range.
    #[must_use]
    pub const fn last(self) -> Key {
        self.last
    }

    /// How many keys the range spans.
    ///
    /// This is what a range read costs, against the count of keys a walk
    /// actually visits — the gap the whole read-amplification question is.
    #[must_use]
    pub const fn span(self) -> u64 {
        self.last.0 - self.first.0 + 1
    }

    /// Whether `key` falls inside the range.
    #[must_use]
    pub const fn contains(self, key: Key) -> bool {
        self.first.0 <= key.0 && key.0 <= self.last.0
    }

    /// Whether the two ranges share any key.
    #[must_use]
    pub const fn overlaps(self, other: Self) -> bool {
        self.first.0 <= other.last.0 && other.first.0 <= self.last.0
    }

    /// The smallest range covering both, which is what a merge produces.
    ///
    /// The hull of two ranges with a hole between them covers the hole,
    /// which is sound because a hole is a key no segment holds.
    #[must_use]
    pub const fn hull(self, other: Self) -> Self {
        Self {
            first: if self.first.0 <= other.first.0 {
                self.first
            } else {
                other.first
            },
            last: if self.last.0 >= other.last.0 {
                self.last
            } else {
                other.last
            },
        }
    }
}

/// Cover `keys` with at most `most` ranges, cutting at the widest gaps.
///
/// A read pays for the span it asks for, so a handful of scattered keys must
/// not be asked for as one range: the hull between them is the cost.
#[must_use]
pub fn cluster(keys: &[Key], most: NonZeroUsize) -> Vec<KeyRange> {
    let mut sorted: Vec<u64> = keys.iter().map(|key| key.get()).collect();
    sorted.sort_unstable();
    sorted.dedup();
    let Some(end) = sorted.len().checked_sub(1) else {
        return Vec::new();
    };

    // Cutting the widest gaps is what buys the most span for a fixed number
    // of reads: the total asked for is the hull less whatever is cut out.
    let mut gaps: Vec<(u64, usize)> = sorted
        .windows(2)
        .enumerate()
        .filter_map(|(at, pair)| match pair {
            [low, high] if high - low > 1 => Some((high - low, at)),
            _ => None,
        })
        .collect();
    let keep = most.get().saturating_sub(1);
    if gaps.len() > keep {
        gaps.select_nth_unstable_by(keep, |a, b| b.cmp(a));
        gaps.truncate(keep);
    }

    let mut cuts: Vec<usize> = gaps.into_iter().map(|(_, at)| at).collect();
    cuts.sort_unstable();
    cuts.push(end);

    let mut ranges = Vec::with_capacity(cuts.len());
    let mut from = 0;
    for cut in cuts {
        if let (Some(&first), Some(&last)) = (sorted.get(from), sorted.get(cut))
            && let Ok(range) = KeyRange::new(Key::new(first), Key::new(last))
        {
            ranges.push(range);
        }
        from = cut + 1;
    }
    if worth_splitting(&ranges, &sorted) {
        return ranges;
    }
    hull_of(&sorted)
}

/// Whether reading `ranges` separately beats reading the one span they sit in.
///
/// Every range is a round trip and a fetch of what it lists, so splitting has
/// to divide the span by at least what it multiplies those by.
fn worth_splitting(ranges: &[KeyRange], sorted: &[u64]) -> bool {
    if ranges.len() < 2 {
        return true;
    }
    let split: u64 = ranges.iter().map(|range| range.span()).sum();
    let whole = hull_span(sorted);
    let reads = u64::try_from(ranges.len()).unwrap_or(u64::MAX);
    split.saturating_mul(reads) <= whole
}

/// The one range covering every key, as the fallback when cutting is not
/// worth it.
fn hull_of(sorted: &[u64]) -> Vec<KeyRange> {
    let Some((&first, &last)) = sorted.first().zip(sorted.last()) else {
        return Vec::new();
    };
    KeyRange::new(Key::new(first), Key::new(last))
        .map(|range| vec![range])
        .unwrap_or_default()
}

/// How wide the one range covering every key would be.
fn hull_span(sorted: &[u64]) -> u64 {
    match (sorted.first(), sorted.last()) {
        (Some(&first), Some(&last)) => last.saturating_sub(first).saturating_add(1),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{Key, KeyRange, cluster};
    use std::num::NonZeroUsize;

    fn most(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("a positive cap")
    }

    fn keys(values: &[u64]) -> Vec<Key> {
        values.iter().copied().map(Key::new).collect()
    }

    fn spans(ranges: &[KeyRange]) -> u64 {
        ranges.iter().map(|range| range.span()).sum()
    }

    #[test]
    fn nothing_covers_nothing() {
        assert!(cluster(&[], most(4)).is_empty());
    }

    #[test]
    fn a_run_stays_one_range() {
        let ranges = cluster(&keys(&[4, 5, 6, 7]), most(4));
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].first().get(), 4);
        assert_eq!(ranges[0].last().get(), 7);
    }

    // The clone case: a cluster at the tip and one old tag far below it.
    #[test]
    fn a_far_outlier_is_cut_away_from_the_tip() {
        let ranges = cluster(&keys(&[1, 999_998, 999_999, 1_000_000]), most(4));
        assert_eq!(ranges.len(), 2);
        assert_eq!(spans(&ranges), 1 + 3, "the empty million is not read");
    }

    #[test]
    fn the_cap_is_never_exceeded() {
        let scattered = keys(&[0, 100, 200, 300, 400, 500, 600]);
        for cap in 1..=8 {
            assert!(cluster(&scattered, most(cap)).len() <= cap);
        }
    }

    // Whatever it cuts, every key asked for must still be covered.
    #[test]
    fn every_key_stays_covered() {
        let values = [3_u64, 4, 90, 91, 92, 5_000, 900_000, 900_001];
        for cap in 1..=6 {
            let ranges = cluster(&keys(&values), most(cap));
            for value in values {
                assert!(
                    ranges.iter().any(|range| range.contains(Key::new(value))),
                    "{value} covered at cap {cap}"
                );
            }
        }
    }

    // The one that matters: every range is a round trip and a fetch of what
    // it lists, so a gap of one absent key must not buy itself a read.
    #[test]
    fn a_sparse_run_is_not_worth_cutting_up() {
        let every_other = keys(&[1, 3, 5, 7, 9, 11, 13, 15]);
        let ranges = cluster(&every_other, most(8));
        assert_eq!(
            ranges.len(),
            1,
            "eight reads to skip seven keys is worse than one read of fifteen"
        );
        assert_eq!(spans(&ranges), 15);
    }

    // Splitting has to divide the span by at least what it multiplies the
    // reads by, and evenly-spread keys never do.
    #[test]
    fn evenly_spread_keys_stay_one_range() {
        let spread = keys(&[0, 100, 200, 300, 400, 500, 600, 700, 800, 900]);
        assert_eq!(cluster(&spread, most(8)).len(), 1);
    }

    // Three tight clusters spread far apart: each one earns its read, and
    // the pairwise "does this cut halve it" test would have missed them.
    #[test]
    fn several_far_apart_clusters_each_earn_a_read() {
        let mut values: Vec<u64> = Vec::new();
        for base in [0_u64, 500_000, 1_000_000] {
            values.extend(base..base + 10);
        }
        let ranges = cluster(&keys(&values), most(8));
        assert_eq!(ranges.len(), 3);
        assert_eq!(spans(&ranges), 30, "and none of the million between them");
    }

    #[test]
    fn a_wider_cap_never_reads_more() {
        let values = keys(&[1, 2, 3, 5_000, 5_001, 900_000]);
        let mut previous = u64::MAX;
        for cap in 1..=6 {
            let total = spans(&cluster(&values, most(cap)));
            assert!(total <= previous, "cap {cap} read more than the cap below");
            previous = total;
        }
    }

    #[test]
    fn one_range_is_the_hull() {
        let ranges = cluster(&keys(&[7, 900_000]), most(1));
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].first().get(), 7);
        assert_eq!(ranges[0].last().get(), 900_000);
    }
}
