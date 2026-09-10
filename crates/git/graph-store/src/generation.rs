//! The cutoff seq cannot give.
//!
//! `seq` already bounds a walk, since `parent.seq < child.seq` — but by the
//! repository's push volume rather than by topological distance, so a branch
//! a hundred commits deep can sit millions of seqs from where it diverged.

/// Git's generation number v2: the corrected commit date.
///
/// The commit's own date, raised to clear every parent, so it prunes by
/// shape and still answers a date-bounded question — which v1 cannot.
pub(crate) fn corrected_date(committer_date: i64, parents: impl IntoIterator<Item = u32>) -> u32 {
    // Seconds in a `u32`, so the ceiling is 2106. A date past it saturates,
    // which costs pruning and never correctness: the number stays monotone.
    let own = u32::try_from(committer_date.max(0)).unwrap_or(u32::MAX);
    let floor = parents
        .into_iter()
        .max()
        .map_or(0, |deepest| deepest.saturating_add(1));
    own.max(floor).max(1)
}

#[cfg(test)]
mod tests {
    use super::corrected_date;

    // The property the pruning rests on, whatever the dates say.
    #[test]
    fn a_child_always_outranks_its_parents() {
        assert!(
            corrected_date(0, [5, 9, 1]) > 9,
            "a date older than a parent"
        );
        assert!(corrected_date(1_700_000_000, [5, 9, 1]) > 9);
        assert_eq!(corrected_date(-1, []), 1, "a date before the epoch");
    }

    #[test]
    fn a_commit_keeps_its_own_date_when_it_clears_its_parents() {
        assert_eq!(corrected_date(1_700_000_000, [3]), 1_700_000_000);
        assert_eq!(corrected_date(i64::MAX, []), u32::MAX, "and saturates");
    }
}
