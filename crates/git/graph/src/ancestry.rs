//! Whether one commit of a push can read another's pack: ancestor closures
//! over the push's own commit graph.
//!
//! A commit the push sends is settled in memory, along the push's topological
//! order. A commit it only names — a *boundary* — is settled by whoever calls
//! this, which hands back what each boundary reaches. Every answer is sound
//! and none is complete: a dropped closure or an unwalked boundary reports
//! "not proven", never a wrong "yes".

use std::collections::{HashMap, HashSet, VecDeque};

use gix_hash::ObjectId;
use roaring::RoaringBitmap;

use enroute_git_core::{Error, ObjectHashMap, ObjectHashSet, topo_order};

/// Ceiling on how many ancestor bits are resident at once.
///
/// Reached only by a wide, dense DAG; dropping a closure only loses proofs,
/// never correctness.
const MAX_LIVE_ANCESTOR_BITS: u64 = 16 * 1024 * 1024;

/// One thing to prove: that `home`'s pack is readable from `target`'s.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Query {
    /// The commit whose pack is about to hold the entry.
    pub target: ObjectId,
    /// The commit whose pack holds the base.
    pub home: ObjectId,
}

/// What was proven.
///
/// Anything absent is unproven, which callers must treat as a refusal
/// rather than as a negative.
#[derive(Debug, Default)]
pub struct Ancestry {
    proven: HashSet<(ObjectId, ObjectId)>,
}

impl Ancestry {
    /// Whether `home`'s pack is reachable from `target`'s — trivially so when
    /// they are the same pack.
    #[must_use]
    pub fn holds(&self, query: Query) -> bool {
        query.target == query.home || self.proven.contains(&(query.target, query.home))
    }
}

/// The push's own commits, in the order the closures are built along.
///
/// `parents_of` is that push and nothing else, so a parent missing from it is
/// a boundary — the one thing this cannot settle on its own.
#[derive(Debug)]
pub struct Push<'a> {
    order: Vec<ObjectId>,
    parents_of: &'a ObjectHashMap<Vec<ObjectId>>,
    index_of: ObjectHashMap<u32>,
}

/// The parents a push names but did not send, and what each one reaches.
#[derive(Debug)]
pub struct Boundaries<'a> {
    /// As [`Push::boundaries`] returned them; the order fixes each one's bit.
    pub oids: &'a [ObjectId],
    /// Parallel to `oids`: the queried homes each boundary can reach.
    pub reachable: &'a [ObjectHashSet],
}

impl<'a> Push<'a> {
    /// Order `parents_of` topologically, ready to build closures along.
    ///
    /// # Errors
    /// Returns an error if the push's commits can't be topologically ordered.
    pub fn new(parents_of: &'a ObjectHashMap<Vec<ObjectId>>) -> Result<Self, Error> {
        let order = topo_order(parents_of)?;
        let index_of: ObjectHashMap<u32> = order
            .iter()
            .enumerate()
            .filter_map(|(at, &commit)| u32::try_from(at).ok().map(|at| (commit, at)))
            .collect();
        Ok(Self {
            order,
            parents_of,
            index_of,
        })
    }

    /// Every parent this push names but did not send, in the order its
    /// commits are walked, capped at `max`.
    ///
    /// A caller answers at most `max` of them, and pays for each answer.
    #[must_use]
    pub fn boundaries(&self, max: usize) -> Vec<ObjectId> {
        let mut seen: ObjectHashSet = ObjectHashSet::default();
        self.parents()
            .filter(|parent| !self.parents_of.contains_key(*parent))
            .filter(|parent| seen.insert(**parent))
            .take(max)
            .copied()
            .collect()
    }

    /// Every parent edge of the push, in walk order and with repeats.
    fn parents(&self) -> impl Iterator<Item = &ObjectId> {
        self.order
            .iter()
            .flat_map(|commit| self.parents_of.get(commit).into_iter().flatten())
    }

    /// How many of the push's own commits name each one as a parent, by slot.
    fn children_left(&self) -> Vec<u32> {
        let mut counts: Vec<u32> = vec![0; self.order.len()];
        for parent in self.parents() {
            if let Some(count) = self
                .index_of
                .get(parent)
                .and_then(|&at| counts.get_mut(slot(at)))
            {
                *count += 1;
            }
        }
        counts
    }

    /// `commit`'s ancestors within the push, plus a bit per boundary it
    /// reaches.
    fn closure_of(
        &self,
        commit: &ObjectId,
        closures: &Closures,
        boundaries: &Boundaries<'_>,
        commits: u32,
    ) -> RoaringBitmap {
        let mut closure = RoaringBitmap::new();
        for parent in self.parents_of.get(commit).into_iter().flatten() {
            match self.index_of.get(parent) {
                Some(&parent_at) => closures.inherit(&mut closure, parent_at),
                None => reached(&mut closure, commits, boundaries, parent),
            }
        }
        closure
    }

    /// Build each commit's ancestor closure in topological order, answering
    /// the queries aimed at it as it goes.
    ///
    /// A DAG wide enough to exceed [`MAX_LIVE_ANCESTOR_BITS`] loses its
    /// oldest closures, and with them only the proofs that depended on them.
    #[must_use]
    pub fn prove(&self, boundaries: &Boundaries<'_>, queries: &[Query]) -> Ancestry {
        let mut asked: HashMap<ObjectId, Vec<ObjectId>> = HashMap::new();
        for query in queries {
            asked.entry(query.target).or_default().push(query.home);
        }

        let commits = u32::try_from(self.order.len()).unwrap_or(u32::MAX);
        let mut children_left = self.children_left();
        let mut closures = Closures::default();
        let mut proven: HashSet<(ObjectId, ObjectId)> = HashSet::new();

        for (at, &commit) in self.order.iter().enumerate() {
            let closure = self.closure_of(&commit, &closures, boundaries, commits);

            let holds = |home: &ObjectId| match self.index_of.get(home) {
                Some(&home_at) => closure.contains(home_at),
                None => reaches_older(&closure, commits, boundaries, *home),
            };
            let homes = asked.get(&commit).into_iter().flatten();
            proven.extend(homes.filter(|home| holds(home)).map(|&home| (commit, home)));

            // Parents are done with once their last child has been built.
            let in_push = self
                .parents_of
                .get(&commit)
                .into_iter()
                .flatten()
                .filter_map(|parent| self.index_of.get(parent).copied());
            for parent_at in in_push {
                closures.retire(parent_at, &mut children_left);
            }

            let at = u32::try_from(at).unwrap_or(u32::MAX);
            if children_left.get(slot(at)).is_some_and(|&left| left > 0) {
                closures.keep(at, closure);
            }
        }

        Ancestry { proven }
    }
}

/// A closure index as a slot, saturating — a push never nears `u32::MAX`
/// commits, and an out-of-range slot only loses a proof.
fn slot(at: u32) -> usize {
    usize::try_from(at).unwrap_or(usize::MAX)
}

/// The ancestor closures currently worth holding on to.
#[derive(Default)]
struct Closures {
    live: HashMap<u32, RoaringBitmap>,
    /// Total cardinality held, against [`MAX_LIVE_ANCESTOR_BITS`].
    resident: u64,
    /// Insertion order, so pressure evicts what has been around longest.
    oldest: VecDeque<u32>,
}

impl Closures {
    /// Fold a parent's ancestors in, and the parent itself.
    fn inherit(&self, closure: &mut RoaringBitmap, parent_at: u32) {
        if let Some(inherited) = self.live.get(&parent_at) {
            *closure |= inherited;
        }
        closure.insert(parent_at);
    }

    /// Note one of `parent_at`'s children as built, dropping its closure once
    /// none are left to inherit it.
    fn retire(&mut self, parent_at: u32, children_left: &mut [u32]) {
        let Some(left) = children_left.get_mut(slot(parent_at)) else {
            return;
        };
        *left = left.saturating_sub(1);
        if *left == 0 {
            self.drop_one(parent_at);
        }
    }

    fn keep(&mut self, at: u32, closure: RoaringBitmap) {
        self.resident = self.resident.saturating_add(closure.len());
        self.live.insert(at, closure);
        self.oldest.push_back(at);
        while self.resident > MAX_LIVE_ANCESTOR_BITS {
            let Some(evict) = self.oldest.pop_front() else {
                break;
            };
            self.drop_one(evict);
        }
    }

    fn drop_one(&mut self, at: u32) {
        if let Some(dropped) = self.live.remove(&at) {
            self.resident = self.resident.saturating_sub(dropped.len());
        }
    }
}

/// Mark that this commit reaches `parent`, a commit the push did not send.
///
/// A boundary the caller left unanswered has no bit, so nothing below it is
/// ever proven.
fn reached(
    closure: &mut RoaringBitmap,
    commits: u32,
    boundaries: &Boundaries<'_>,
    parent: &ObjectId,
) {
    if let Some(bit) = boundaries.oids.iter().position(|b| b == parent)
        && let Ok(bit) = u32::try_from(bit)
    {
        closure.insert(commits.saturating_add(bit));
    }
}

/// Whether any boundary this commit reaches can reach `home`.
fn reaches_older(
    closure: &RoaringBitmap,
    commits: u32,
    boundaries: &Boundaries<'_>,
    home: ObjectId,
) -> bool {
    boundaries
        .reachable
        .iter()
        .enumerate()
        .any(|(at, reachable)| {
            u32::try_from(at).is_ok_and(|bit| closure.contains(commits.saturating_add(bit)))
                && reachable.contains(&home)
        })
}

#[cfg(test)]
mod tests {
    use gix_hash::ObjectId;

    use enroute_git_core::{ObjectHashMap, ObjectHashSet};

    use super::{Ancestry, Boundaries, Push, Query};

    fn oid(n: u8) -> ObjectId {
        ObjectId::from_bytes_or_panic(&[n; 20])
    }

    /// Prove with no boundaries answered — everything a push settles from its
    /// own commits.
    fn prove_in_push(graph: &[(u8, Vec<u8>)], queries: &[(u8, u8)]) -> Ancestry {
        let parents_of: ObjectHashMap<Vec<ObjectId>> = graph
            .iter()
            .map(|(commit, parents)| (oid(*commit), parents.iter().copied().map(oid).collect()))
            .collect();
        let queries: Vec<Query> = queries
            .iter()
            .map(|&(target, home)| q(target, home))
            .collect();
        Push::new(&parents_of).unwrap().prove(
            &Boundaries {
                oids: &[],
                reachable: &[],
            },
            &queries,
        )
    }

    /// The case the whole scheme turns on: a base introduced on a side branch
    /// is *not* readable from the other side, however recent it is.
    #[test]
    fn a_side_branch_is_not_an_ancestor_of_the_line_it_forked_from() {
        // 1 → 2 → 3 (main), 1 → 4 (side), 3 + 4 → 5 (merge)
        let graph = [
            (1u8, vec![]),
            (2, vec![1]),
            (3, vec![2]),
            (4, vec![1]),
            (5, vec![3, 4]),
        ];
        let proven = prove_in_push(&graph, &[(3, 4), (4, 3), (5, 4), (5, 3), (3, 1), (2, 3)]);

        assert!(!proven.holds(q(3, 4)), "the side branch is not below main");
        assert!(!proven.holds(q(4, 3)), "nor main below the side branch");
        assert!(proven.holds(q(5, 4)), "the merge sees both sides");
        assert!(proven.holds(q(5, 3)));
        assert!(proven.holds(q(3, 1)), "the fork point is below both");
        assert!(!proven.holds(q(2, 3)), "a descendant is not an ancestor");
    }

    #[test]
    fn a_commit_is_its_own_home() {
        let proven = prove_in_push(&[(1u8, vec![])], &[]);
        assert!(
            proven.holds(q(1, 1)),
            "an entry may delta against a base in its own pack"
        );
    }

    /// Nothing here asserts a particular eviction, only that whatever
    /// survives is true.
    ///
    /// An unproven base costs CPU; a wrongly proven one costs a corrupt pack.
    #[test]
    fn nothing_unreachable_is_ever_proven() {
        let graph: Vec<(u8, Vec<u8>)> = (0..40u8)
            .map(|i| (i, if i == 0 { vec![] } else { vec![i - 1] }))
            .collect();
        let queries: Vec<(u8, u8)> = (0..40u8)
            .flat_map(|a| (0..40u8).map(move |b| (a, b)))
            .collect();
        let proven = prove_in_push(&graph, &queries);

        for (target, home) in queries {
            if proven.holds(q(target, home)) {
                assert!(home <= target, "{home} is not an ancestor of {target}");
            }
        }
    }

    /// A boundary the caller answered puts a commit older than the push
    /// within reach.
    #[test]
    fn an_answered_boundary_reaches_what_it_was_told_it_reaches() {
        let parents_of: ObjectHashMap<Vec<ObjectId>> =
            [(oid(2), vec![oid(1)])].into_iter().collect();
        let older = oid(9);
        let reachable = [ObjectHashSet::from_iter([older])];
        let push = Push::new(&parents_of).unwrap();
        let boundaries = push.boundaries(8);
        assert_eq!(boundaries, vec![oid(1)]);

        let proven = push.prove(
            &Boundaries {
                oids: &boundaries,
                reachable: &reachable,
            },
            &[
                Query {
                    target: oid(2),
                    home: older,
                },
                Query {
                    target: oid(2),
                    home: oid(8),
                },
            ],
        );

        assert!(proven.holds(q(2, 9)), "the boundary reaches it");
        assert!(!proven.holds(q(2, 8)), "and nothing else");
    }

    /// A boundary past the caller's budget is left out, so nothing below it
    /// is proven.
    #[test]
    fn boundaries_stop_at_the_cap() {
        let parents_of: ObjectHashMap<Vec<ObjectId>> = [
            (oid(3), vec![oid(1)]),
            (oid(4), vec![oid(2)]),
            (oid(5), vec![oid(3), oid(4)]),
        ]
        .into_iter()
        .collect();
        let push = Push::new(&parents_of).unwrap();
        assert_eq!(push.boundaries(1).len(), 1);
        assert_eq!(push.boundaries(8).len(), 2);
    }

    fn q(target: u8, home: u8) -> Query {
        Query {
            target: oid(target),
            home: oid(home),
        }
    }

    /// Unused in the in-memory tests but part of the type's contract: an
    /// empty set proves nothing at all.
    #[test]
    fn an_empty_proof_holds_only_reflexively() {
        let empty = Ancestry::default();
        assert!(empty.holds(q(1, 1)));
        assert!(!empty.holds(q(1, 2)));
    }
}
