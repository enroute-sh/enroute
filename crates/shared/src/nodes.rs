//! Rendezvous ranking over nodes, and how long a node counts as alive
//! after it was last heard from.

use std::time::Duration;

/// A node not heard from for this long is not alive. Safety never depends
/// on liveness: it only steers requests and maintenance.
pub const ALIVE: Duration = Duration::from_secs(10);

/// Sort `names` into `repo`'s order of preference (rendezvous hashing): the
/// same on every node, whatever order the names come in, and a node joining
/// or leaving moves only the repositories that rank it among their first.
/// (The sort is stable: sorted names break ties in a 64-bit hash alike.)
pub fn rank(repo: &str, names: &mut [String]) {
    names.sort_by_cached_key(|n| std::cmp::Reverse(weight(repo, n.as_bytes())));
}

/// The first of `ids` in `repo`'s order, as [`rank`] orders names.
#[must_use]
pub fn first(repo: &str, ids: &[u64]) -> Option<u64> {
    ids.iter()
        .copied()
        .max_by_key(|&id| (weight(repo, &id.to_le_bytes()), id))
}

/// A hash of (repository, node) that is stable across builds and platforms
/// (FNV-1a, then `SplitMix64`'s finalizer to spread it).
#[must_use]
pub fn weight(repo: &str, node: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in repo.as_bytes().iter().chain(node) {
        h = (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3);
    }
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rank_is_stable_and_spreads() {
        let ranked = |repo: &str, names: &[&str]| {
            let mut names: Vec<String> = names.iter().map(ToString::to_string).collect();
            rank(repo, &mut names);
            names
        };
        let r = ranked("repo", &["a", "b", "c", "d", "e"]);
        assert_eq!(r, ranked("repo", &["e", "c", "a", "d", "b"]));
        // Removing a node keeps the others' order.
        let without: Vec<&str> = r[1..].iter().map(String::as_str).collect();
        assert_eq!(ranked("repo", &without), r[1..]);
        // Repositories are spread: every node is first for some of them.
        let mut first = std::collections::BTreeSet::new();
        for i in 0..200 {
            first.insert(ranked(&format!("r{i}"), &["a", "b", "c", "d", "e"])[0].clone());
        }
        assert_eq!(first.len(), 5);
    }

    #[test]
    fn first_is_stable_and_spreads() {
        let ids: Vec<u64> = (1..=5).collect();
        assert_eq!(first("repo", &ids), first("repo", &[5, 3, 1, 4, 2]));
        let firsts: std::collections::BTreeSet<_> = (0..200)
            .filter_map(|i| first(&format!("r{i}"), &ids))
            .collect();
        assert_eq!(firsts.len(), 5);
    }
}
