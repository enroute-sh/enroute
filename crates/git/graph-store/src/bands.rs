//! The band walks, in seq space and nothing else.
//!
//! Each of these is the seq-space half of one walk in [`crate::walk`]. The
//! other half — an oid, a pack bitmap, a location — is not in this index, so
//! [`RepoGraph`], which has both, joins them.
//!
//! [`RepoGraph`]: crate::RepoGraph

use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use thiserror::Error;

use crate::index::CommitIndex;

/// Paint flag for "reachable from a want".
pub const FLAG_WANT: u8 = 1;

/// Paint flag for "reachable from a have".
pub const FLAG_HAVE: u8 = 2;

/// A commit a walk had to read that the index does not hold.
///
/// Stricter than the recursive CTE this replaces, which drops it silently.
/// A fetch quietly missing commits is the worst way to learn of a bug.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the commit index does not hold commit {seq}, inside a band floored at {floor}")]
pub struct NotCovered {
    /// The commit that was not there.
    pub seq: i64,
    /// The floor of the band being walked.
    pub floor: i64,
}

/// One round of a paint walk, in seqs.
#[derive(Debug, Clone, Default)]
pub struct PaintBand {
    /// Want-only commits at or above the floor, ascending.
    ///
    /// What the caller answers with, once it has the oids and pack columns
    /// this index does not hold.
    pub finalized: Vec<i64>,
    /// Boundary parents below the floor, with the flags that reached them.
    pub below: BTreeMap<i64, u8>,
}

/// One commit a depth walk reached, in seqs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reached {
    /// Its seq.
    pub seq: i64,
    /// Its pooled minimum depth from any want.
    pub depth: i64,
    /// Its root tree's `object_seq`.
    pub root_tree_seq: u64,
}

/// One round of a depth walk, in seqs.
#[derive(Debug, Clone, Default)]
pub struct DepthBand {
    /// Commits at or above the floor, ascending.
    pub reached: Vec<Reached>,
    /// Below-floor discoveries, min-merged into the next round's frontier.
    pub below: BTreeMap<i64, i64>,
}

impl CommitIndex {
    /// Every commit reachable from `seeds`, split at `floor`.
    ///
    /// Seeds below the floor are reported and not expanded, which is what
    /// makes the next band pick them up.
    ///
    /// # Errors
    /// [`NotCovered`] when the walk reaches into a seq the index lacks.
    pub fn expand_ancestors(
        &self,
        seeds: &[i64],
        floor: i64,
    ) -> Result<(HashSet<i64>, HashSet<i64>), NotCovered> {
        let mut visited = HashSet::new();
        let mut below = HashSet::new();
        let mut seen: HashSet<i64> = HashSet::new();
        let mut stack: Vec<i64> = seeds.to_vec();

        while let Some(seq) = stack.pop() {
            if !seen.insert(seq) {
                continue;
            }
            if seq < floor {
                below.insert(seq);
                continue;
            }
            visited.insert(seq);
            stack.extend(self.must_get(seq, floor)?.parents().iter());
        }
        Ok((visited, below))
    }

    /// Paints down from `seeds`, one colour per distinct flag.
    ///
    /// Colours do not mix while they travel: a seq reached by two of them
    /// ends up with both bits, and only a want-only seq is finalized.
    ///
    /// # Errors
    /// [`NotCovered`] when the walk reaches into a seq the index lacks.
    pub fn expand_paint(
        &self,
        seeds: &[i64],
        flags: &[u8],
        floor: i64,
        want_block: &[i64],
        have_block: &[i64],
    ) -> Result<PaintBand, NotCovered> {
        let paint = Paint {
            seeds,
            flags,
            floor,
            want_block: want_block.iter().copied().collect(),
            have_block: have_block.iter().copied().collect(),
            have_seeds: seeds_flagged(seeds, flags, FLAG_HAVE),
        };

        let mut painted: BTreeMap<i64, u8> = BTreeMap::new();
        for colour in colours(flags) {
            self.paint_colour(colour, &paint, &mut painted)?;
        }

        let mut band = PaintBand::default();
        for (seq, flags) in painted {
            if seq < floor {
                band.below.insert(seq, flags);
            } else if flags == FLAG_WANT {
                band.finalized.push(seq);
            }
        }
        Ok(band)
    }

    /// Walks one colour from its own seeds, recording every seq it reaches.
    fn paint_colour(
        &self,
        colour: u8,
        paint: &Paint<'_>,
        painted: &mut BTreeMap<i64, u8>,
    ) -> Result<(), NotCovered> {
        let mut seen: HashSet<i64> = HashSet::new();
        let mut stack: Vec<i64> = seeds_flagged(paint.seeds, paint.flags, colour)
            .into_iter()
            .collect();

        while let Some(seq) = stack.pop() {
            if !seen.insert(seq) {
                continue;
            }
            *painted.entry(seq).or_insert(0) |= colour;
            let blocked = paint.blocks(colour).is_some_and(|set| set.contains(&seq));
            if seq < paint.floor || blocked {
                continue;
            }
            self.push_parents(seq, colour, paint, &mut stack)?;
        }
        Ok(())
    }

    /// Pushes the parents want-paint is allowed to travel to.
    fn push_parents(
        &self,
        seq: i64,
        colour: u8,
        paint: &Paint<'_>,
        stack: &mut Vec<i64>,
    ) -> Result<(), NotCovered> {
        for parent in self.must_get(seq, paint.floor)?.parents().iter() {
            // Want-paint stops at a have seed rather than colouring through
            // it: that is what makes the answer want-only.
            if colour == FLAG_WANT && paint.have_seeds.contains(&parent) {
                continue;
            }
            stack.push(parent);
        }
        Ok(())
    }

    /// Paints every commit within `depth_limit` of the seeds with its
    /// minimum depth, down to `floor`.
    ///
    /// # Errors
    /// [`NotCovered`] when the walk reaches into a seq the index lacks.
    pub fn expand_depth(
        &self,
        seeds: &[i64],
        depths: &[i64],
        floor: i64,
        depth_limit: i64,
    ) -> Result<DepthBand, NotCovered> {
        let mut best: BTreeMap<i64, i64> = BTreeMap::new();
        let mut stack: Vec<(i64, i64)> =
            seeds.iter().copied().zip(depths.iter().copied()).collect();

        while let Some((seq, depth)) = stack.pop() {
            // Relaxation, not visitation: a shorter path found later has to
            // expand again, or the minimum below it is never corrected.
            if best.get(&seq).is_some_and(|held| *held <= depth) {
                continue;
            }
            best.insert(seq, depth);
            if seq < floor || depth >= depth_limit {
                continue;
            }
            for parent in self.must_get(seq, floor)?.parents().iter() {
                stack.push((parent, depth.saturating_add(1)));
            }
        }

        let mut band = DepthBand::default();
        for (seq, depth) in best {
            if seq < floor {
                band.below.insert(seq, depth);
                continue;
            }
            band.reached.push(Reached {
                seq,
                depth,
                root_tree_seq: self.must_get(seq, floor)?.root_tree(),
            });
        }
        Ok(band)
    }

    /// What the index holds at `seq`, or why the band cannot be walked.
    fn must_get(&self, seq: i64, floor: i64) -> Result<crate::index::Entry<'_>, NotCovered> {
        self.get(seq).ok_or(NotCovered { seq, floor })
    }
}

/// One paint walk's seeds and the seqs each colour may not expand from.
struct Paint<'a> {
    seeds: &'a [i64],
    flags: &'a [u8],
    floor: i64,
    want_block: HashSet<i64>,
    have_block: HashSet<i64>,
    have_seeds: HashSet<i64>,
}

impl Paint<'_> {
    /// What stops `colour`, or nothing when it is a colour neither rule names.
    fn blocks(&self, colour: u8) -> Option<&HashSet<i64>> {
        match colour {
            FLAG_WANT => Some(&self.want_block),
            FLAG_HAVE => Some(&self.have_block),
            _ => None,
        }
    }
}

/// The distinct flag values among `flags`, ascending.
///
/// One colour per value rather than per bit, so a seed carrying both travels
/// as its own colour — blocked by neither rule, as the CTE had it.
fn colours(flags: &[u8]) -> Vec<u8> {
    let mut colours: Vec<u8> = flags.to_vec();
    colours.sort_unstable();
    colours.dedup();
    colours
}

fn seeds_flagged(seeds: &[i64], flags: &[u8], flag: u8) -> HashSet<i64> {
    seeds
        .iter()
        .zip(flags)
        .filter(|(_, held)| **held == flag)
        .map(|(seq, _)| *seq)
        .collect()
}
