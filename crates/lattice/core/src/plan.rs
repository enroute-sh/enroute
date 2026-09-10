//! What compaction should merge, decided without reading a single byte.
//!
//! Planning is pure and separate from running for two reasons: the tiering
//! policy is then property-testable against nothing, and a driver is free to
//! run a plan partially, out of order, or twice. It may, because the join is
//! associative, commutative and idempotent — a plan is advice about cost,
//! never a step correctness depends on.

use std::collections::BTreeMap;

use crate::key::KeyRange;

/// Which generation of merge produced a segment.
///
/// Tier 0 is one write and each tier above is a merge of the one below, so
/// the count of segments a read composes stays bounded as writes pile up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tier(u8);

impl Tier {
    /// What a single write produces, before any merge.
    pub const ZERO: Self = Self(0);

    /// The tier numbered `value`.
    #[must_use]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// This tier as a number, for the column it is stored in.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }

    /// The tier a merge of this one produces.
    ///
    /// Saturating: a repository deep enough to reach the top keeps merging
    /// in place rather than overflowing into tier zero.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Which store a segment's bytes are in.
///
/// The medium follows the size, because fragmentation is an object-store
/// property: many small objects is many GETs, and many small rows is one query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Residence {
    /// In the catalog row itself, arriving with the segment list.
    Inline,
    /// In the object store, read by key.
    Bucket,
}

/// What the catalog knows about one segment without reading its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// The keys the segment covers.
    pub range: KeyRange,
    /// Which generation of merge produced it.
    pub tier: Tier,
    /// How large its encoding is.
    pub bytes: u64,
    /// Where those bytes are.
    pub residence: Residence,
}

/// When compaction runs, and how much it takes on at once.
///
/// There is deliberately no default. Every field here trades read cost
/// against write cost, and the balance wants measuring per deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Segments of one tier that make one of the next.
    pub fanout: usize,
    /// The most segments one merge takes, which bounds its peak memory.
    pub max_inputs: usize,
    /// The most input bytes one merge takes, for the same reason.
    pub max_input_bytes: u64,
    /// The encoded size at which a segment is worth a GET, and so graduates.
    pub graduation_bytes: u64,
    /// Inlined segments a scope may hold before a merge is due whatever the
    /// fanout says.
    pub inline_ceiling: usize,
}

impl Policy {
    /// Where a segment of `bytes` belongs.
    ///
    /// Asked of the encoded result rather than of the inputs, since an
    /// idempotent join can return less than it was given.
    #[must_use]
    pub const fn residence_for(&self, bytes: u64) -> Residence {
        if bytes >= self.graduation_bytes {
            Residence::Bucket
        } else {
            Residence::Inline
        }
    }
}

/// One merge: which segments to join, and what the result covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merge {
    /// Indices into the slice [`plan`] was given, in key order.
    pub inputs: Vec<usize>,
    /// The tier the result is written at.
    pub tier: Tier,
    /// The keys the result covers, which is the hull of its inputs.
    pub range: KeyRange,
}

/// What compaction should do, lowest tier first.
///
/// One pass over the placements as they stand: a merge's output is not fed
/// to another merge in the same plan, it is picked up by the next call.
#[must_use]
pub fn plan(placements: &[Placement], policy: &Policy) -> Vec<Merge> {
    let mut by_tier: BTreeMap<Tier, Vec<usize>> = BTreeMap::new();
    let mut inlined = 0_usize;
    for (index, placement) in placements.iter().enumerate() {
        by_tier.entry(placement.tier).or_default().push(index);
        if placement.residence == Residence::Inline {
            inlined += 1;
        }
    }

    // Relieved by the lowest tier that can, since merging the whole
    // repository to clear a tail nobody is reading is the wrong trade.
    let mut pressured = inlined > policy.inline_ceiling;

    let mut merges = Vec::new();
    for (tier, mut indices) in by_tier {
        indices.sort_by_key(|index| placements.get(*index).map(sort_key));
        let due = indices.len() >= policy.fanout || (pressured && indices.len() >= 2);
        if !due {
            continue;
        }
        let before = merges.len();
        for run in runs(&indices, placements, policy) {
            if let Some(range) = hull(&run, placements) {
                merges.push(Merge {
                    inputs: run,
                    tier: tier.next(),
                    range,
                });
            }
        }
        if merges.len() > before {
            pressured = false;
        }
    }
    merges
}

/// Where a placement sorts, so a run is of neighbours and its hull is tight.
fn sort_key(placement: &Placement) -> (u64, u64) {
    (placement.range.first().get(), placement.range.last().get())
}

/// Splits key-ordered `indices` into the runs one merge each takes.
///
/// A trailing run of one is dropped: joining a segment with nothing rewrites
/// it for no gain.
fn runs(indices: &[usize], placements: &[Placement], policy: &Policy) -> Vec<Vec<usize>> {
    let mut runs: Vec<Vec<usize>> = Vec::new();
    let mut run: Vec<usize> = Vec::new();
    let mut bytes = 0_u64;
    for index in indices {
        let size = placements
            .get(*index)
            .map_or(0, |placement| placement.bytes);
        let full = run.len() >= policy.max_inputs.max(2)
            || (run.len() >= 2 && bytes.saturating_add(size) > policy.max_input_bytes);
        if full {
            runs.push(std::mem::take(&mut run));
            bytes = 0;
        }
        run.push(*index);
        bytes = bytes.saturating_add(size);
    }
    if run.len() >= 2 {
        runs.push(run);
    }
    runs
}

/// The range a merge of `run` covers.
fn hull(run: &[usize], placements: &[Placement]) -> Option<KeyRange> {
    run.iter()
        .filter_map(|index| placements.get(*index))
        .map(|placement| placement.range)
        .reduce(KeyRange::hull)
}
