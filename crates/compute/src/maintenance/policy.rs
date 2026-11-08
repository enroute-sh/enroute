//! The merge policy for a repository's stored layers: which runs merge,
//! and when a manifest needs no more maintenance. Only the compute applies
//! it: the ledger only says which repositories' layers changed.

use std::ops::Range;

use enroute_shared::state::LayerMeta;

/// The default number of layers of one size class that merge at a time.
pub(crate) const MERGE_FACTOR: usize = 8;

/// Layers below this size are in the smallest size class: merging tiny
/// flushes by exact size buys nothing. Small, because a layer of this class
/// is rewritten each time new layers of the class merge into it.
pub(super) const MIN_CLASS_BYTES: u64 = 64 << 10;

/// The disjoint runs of stored layers, of `sizes`, that are due to merge
/// at `merge_factor`, oldest first: none if compaction is off.
#[must_use]
pub(super) fn due(sizes: &[u64], merge_factor: usize) -> Vec<Range<usize>> {
    // A merge of one layer would only rewrite it, again and again.
    if merge_factor < 2 {
        return Vec::new();
    }
    pick(sizes, merge_factor, MIN_CLASS_BYTES)
}

/// Whether a manifest needs no more maintenance: every layer is stored,
/// and no run is due to merge.
#[must_use]
pub(super) fn settled(layers: &[LayerMeta], merge_factor: usize) -> bool {
    let sizes = stored_sizes(layers);
    sizes.len() == layers.len() && due(&sizes, merge_factor).is_empty()
}

/// Choose which layers to merge: disjoint runs of at least `factor`
/// adjacent layers of about one size, oldest first.
///
/// This is a log-structured merge policy over a list that must stay in id
/// order, as Lucene's `LogMergePolicy` is for segments in document order.
/// From the oldest layer on, a stretch reaches to the last layer within
/// `factor` of the largest one left (sizes under `floor` count as `floor`),
/// and its layers merge `factor` at a time, or more, up to `factor` times
/// the largest one, when a backlog has built up. Smaller layers between them
/// join their merges, so none is left behind. A merge makes its output about
/// `factor` times bigger than its inputs. So each byte is rewritten about
/// once per size class (log base `factor` of the repository size over
/// `floor`), and a repository has fewer than `factor` stored layers per
/// size class. `factor` is at least 2.
#[expect(clippy::indexing_slicing, reason = "indexes are within `sizes`")]
#[must_use]
pub(super) fn pick(sizes: &[u64], factor: usize, floor: u64) -> Vec<Range<usize>> {
    let wide = u64::try_from(factor).unwrap_or(u64::MAX);
    let size = |i: usize| sizes[i].max(floor);
    let n = sizes.len();
    let mut runs = Vec::new();
    let mut start = 0;
    while start < n {
        let largest = (start..n).map(size).max().unwrap_or(floor);
        let end = (start..n)
            .rev()
            .find(|&i| size(i).saturating_mul(wide) >= largest)
            .map_or(n, |i| i + 1);
        // A backlog merges in wide runs: one merge per `factor` layers would
        // drain only `factor - 1` layers per merge. The byte cap keeps each
        // merge short.
        let cap = largest.saturating_mul(wide);
        let mut i = start;
        while i < end {
            let (mut j, mut bytes) = (i, 0u64);
            while j < end && (j - i < factor || bytes.saturating_add(sizes[j]) <= cap) {
                bytes = bytes.saturating_add(sizes[j]);
                j += 1;
            }
            if j - i >= factor {
                runs.push(i..j);
            }
            i = j;
        }
        start = end;
    }
    runs
}

/// Sizes of the stored prefix of a manifest: what compaction may merge
/// (inline layers are flushed first).
#[must_use]
pub(super) fn stored_sizes(layers: &[LayerMeta]) -> Vec<u64> {
    layers
        .iter()
        .take_while(|l| l.is_stored())
        .map(|l| l.bytes)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_classes_merge_factor_at_a_time() {
        // Too few of a size class: nothing.
        assert_eq!(pick(&[100, 1, 1, 1], 4, 1), Vec::<Range<usize>>::new());
        // Four small layers on top of a big one: only they merge.
        assert_eq!(pick(&[1000, 10, 10, 10, 10], 4, 1), vec![1..5]);
        // Under the floor, sizes do not matter, and a backlog merges at
        // once up to `factor` times the class size.
        assert_eq!(pick(&[5, 1, 9, 2, 1, 1, 7, 3], 4, 10), vec![0..8]);
        assert_eq!(pick(&[9; 10], 4, 10), vec![0..4, 4..8]);
        assert_eq!(pick(&[1; 100], 4, 10), vec![0..40, 40..80, 80..100]);
        // A smaller layer between two of a class joins their merge.
        assert_eq!(pick(&[100, 1, 100, 100, 100, 10], 4, 1), vec![0..4]);
        // Classes merge independently, oldest first.
        assert_eq!(
            pick(&[1000, 1000, 1000, 1000, 10, 10, 10, 10], 4, 1),
            vec![0..4, 4..8]
        );
    }

    /// Merge `sizes` until nothing is due; returns the bytes written.
    fn compact(sizes: &mut Vec<u64>, factor: usize, floor: u64) -> u64 {
        let mut written = 0;
        let mut runs = pick(sizes, factor, floor);
        while !runs.is_empty() {
            for r in runs.into_iter().rev() {
                let merged: u64 = sizes[r.clone()].iter().sum();
                written += merged;
                sizes.splice(r, [merged]);
            }
            runs = pick(sizes, factor, floor);
        }
        written
    }

    #[test]
    fn write_amplification_and_layer_count_are_logarithmic() {
        // A hot repository: flushes of 10 bytes, up to 10 million bytes.
        let (factor, floor) = (4, 10);
        let mut sizes: Vec<u64> = Vec::new();
        let (mut written, mut flushed) = (0u64, 0u64);
        for _ in 0..1_000_000u64 {
            sizes.push(10);
            flushed += 10;
            written += compact(&mut sizes, factor, floor);
            // log4(10M / 10) = 10 size classes, fewer than 4 layers each.
            assert!(sizes.len() <= 3 * 11, "{} layers", sizes.len());
        }
        assert!(
            written <= 11 * flushed,
            "write amplification {written}/{flushed}"
        );
    }
}
