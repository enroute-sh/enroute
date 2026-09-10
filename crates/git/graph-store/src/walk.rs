//! Fetch-negotiation commit walks: paint-down-to-common, the depth-limited
//! shallow walk, and the ancestry probes.
//!
//! Only the schedule is here. Each round loads the band it is about to walk,
//! expands it in seq space ([`crate::bands`]), and joins what comes back to
//! the oids and pack facts the tier-one index does not hold.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Result, anyhow};
use gix_hash::ObjectId;
use roaring::RoaringTreemap;

use enroute_git_core::{ObjectSeq, SegmentLocation};

use crate::PackEntry;
use crate::bands::{FLAG_HAVE, FLAG_WANT};
use crate::source::{missing, unnamed, unpacked};
use crate::store::{PackFacts, RepoGraph};

/// Maximum `seq` width of one round of a banded walk.
///
/// Bounds per-round expansion work and the row set held between rounds.
const WALK_BAND_MAX_WIDTH: i64 = 100_000;

/// Width of a walk's *first* band, growing by [`WALK_BAND_GROWTH`] per
/// round up to the ceiling.
///
/// Wasted descent past a want/have meet is bounded by band width, so
/// starting small keeps a shallow fetch cheap.
const WALK_BAND_INITIAL_WIDTH: i64 = 1024;

/// Per-round growth factor for a walk's band width.
const WALK_BAND_GROWTH: i64 = 8;

/// The geometric band-width schedule a banded walk descends through.
///
/// Its ceiling is injectable so tests can force single-seq bands;
/// production fixes it at [`WALK_BAND_MAX_WIDTH`].
#[derive(Debug, Clone, Copy)]
pub struct BandPolicy {
    current: i64,
    max: i64,
}

impl BandPolicy {
    /// A schedule capped at `max_width` (the initial band never exceeds it).
    #[must_use]
    pub fn new(max_width: i64) -> Self {
        Self {
            current: WALK_BAND_INITIAL_WIDTH.min(max_width),
            max: max_width,
        }
    }

    /// The production schedule, capped at [`WALK_BAND_MAX_WIDTH`].
    #[must_use]
    pub fn production() -> Self {
        Self::new(WALK_BAND_MAX_WIDTH)
    }

    /// This round's band width.
    fn width(self) -> i64 {
        self.current
    }

    /// Advance to the next (wider) band.
    fn grow(&mut self) {
        self.current = self.current.saturating_mul(WALK_BAND_GROWTH).min(self.max);
    }
}

/// Pre-resolved, deduplicated `commits.seq` seeds for a walk — oids are kept
/// out of the inner loops.
#[derive(Debug, Default, Clone)]
pub struct WalkSeeds {
    /// Want seed seqs.
    pub wants: Vec<i64>,
    /// Have seed seqs.
    pub haves: Vec<i64>,
    /// Client-shallow seed seqs (graft points).
    pub shallow: Vec<i64>,
}

/// Result of a needed-commit walk: the commits a fetch must send, plus the
/// union of their pack-content bitmaps over `object_seq`, split by kind.
///
/// The unions give an exact deduplicated object count with no storage
/// traffic, and double as a membership oracle for dedup ([`Self::contains`]).
#[derive(Debug, Default, Clone)]
pub struct NeededCommits {
    /// Needed commits, in stream order.
    pub commits: Vec<NeededCommit>,
    /// Union of the needed commits' `pack_trees` bitmaps.
    pub trees: RoaringTreemap,
    /// Union of the needed commits' `pack_blobs` bitmaps.
    pub blobs: RoaringTreemap,
}

/// One commit a fetch must send, with the per-pack metadata the read path
/// needs to fetch its bytes without opening the pack.
#[derive(Debug, Clone, Copy)]
pub struct NeededCommit {
    /// The commit's oid.
    pub oid: ObjectId,
    /// Its pack image's segment and span.
    ///
    /// Carried here so the fetch path issues one bounded range GET with no
    /// extra metadata round trip.
    pub segment: SegmentLocation,
    /// Objects in this commit's pack (commit + trees + blobs).
    ///
    /// Only meaningful on a bitmap-collecting walk (a fetch), not an
    /// ancestry probe.
    pub pack_object_count: u32,
    /// Commit+tree objects in this pack (no blobs).
    ///
    /// Entries a `blob:none` fetch reads off the front, since entries are
    /// kind-segregated (commit → trees → blobs).
    pub pack_tree_count: u32,
    /// Byte offset where the pack's blob section begins.
    ///
    /// Bounds a `blob:none` fetch's single GET.
    pub blob_offset: u64,
}

impl NeededCommits {
    /// Whether `seq` is stored in any needed pack.
    ///
    /// Asked in the space it belongs to: the two numberings overlap, so a
    /// blob seq matching a tree bitmap would be a different object entirely.
    #[must_use]
    pub fn contains(&self, seq: ObjectSeq) -> bool {
        match seq {
            ObjectSeq::Tree(seq) => self.trees.contains(seq),
            ObjectSeq::Blob(seq) => self.blobs.contains(seq),
            // A tag is loose rather than packed, so it is in no pack.
            ObjectSeq::Tag(_) => false,
        }
    }

    /// Exact deduplicated object count of this needed set's packs.
    ///
    /// Callers add what lives outside the packs (tags, loose wants,
    /// backfill).
    #[must_use]
    pub fn object_count(&self, keep_trees: bool, keep_blobs: bool) -> u64 {
        let commits = u64::try_from(self.commits.len()).unwrap_or(u64::MAX);
        let trees = if keep_trees { self.trees.len() } else { 0 };
        let blobs = if keep_blobs { self.blobs.len() } else { 0 };
        commits.saturating_add(trees).saturating_add(blobs)
    }

    /// Records one finalized needed commit, deriving its counts from the
    /// bitmaps and OR-ing them into the running unions.
    ///
    /// A single pack never holds `u32::MAX` objects, so an out-of-range
    /// count saturates rather than erroring.
    fn push(&mut self, oid: ObjectId, pack: &PackEntry<'_>) -> Result<()> {
        let trees = pack.trees()?;
        let blobs = pack.blobs()?;
        let pack_object_count = u32::try_from(1 + trees.len() + blobs.len()).unwrap_or(u32::MAX);
        let pack_tree_count = u32::try_from(1 + trees.len()).unwrap_or(u32::MAX);
        self.commits.push(NeededCommit {
            oid,
            segment: pack.segment(),
            pack_object_count,
            pack_tree_count,
            blob_offset: pack.blob_offset(),
        });
        self.trees |= trees;
        self.blobs |= blobs;
        Ok(())
    }
}

/// The commit-side answer for a shallow (`deepen <depth>`) fetch.
#[derive(Debug, Default, Clone)]
pub struct ShallowCommits {
    /// Commits within the depth cut, streamed as full packs.
    pub needed: NeededCommits,
    /// New shallow-boundary commits, for the response's `shallow` lines
    /// (boundary commits already reported shallow are omitted).
    pub shallow: Vec<ObjectId>,
    /// Client-shallow commits whose parents this fetch sends, for the
    /// response's `unshallow` lines.
    pub unshallow: Vec<ObjectId>,
    /// The boundary commits' root-tree `object_seq`s.
    ///
    /// A client cut at the boundary needs that snapshot whole, and what a
    /// tree names is the object index's to answer rather than this store's.
    pub boundary_roots: Vec<u64>,
}

/// One commit inside a shallow fetch's sent set, keyed elsewhere by its seq.
#[derive(Debug, Clone)]
struct SentCommit {
    /// The commit's oid.
    oid: ObjectId,
    /// Its parent seqs (all of `parent1`/`parent2`/`extra_parents`).
    parent_seqs: Vec<i64>,
    /// Its pooled minimum depth from any want.
    depth: i64,
    /// Its root tree's `object_seq`.
    root_tree_seq: u64,
}

/// Result of [`RepoGraph::depth_classify`]: every commit within `depth` of
/// the wants.
///
/// When bitmaps were collected, also shapes the rows as [`NeededCommits`]
/// so the no-haves fast path needs no second walk.
#[derive(Debug, Default)]
struct DepthClassified {
    /// Every sent commit, keyed by its `seq`.
    sent: HashMap<i64, SentCommit>,
    /// The sent set shaped as a needed set (only when bitmaps were collected).
    needed: NeededCommits,
}

impl RepoGraph<'_> {
    /// Every commit reachable from the want seeds but not the have seeds,
    /// with the union of their pack-content bitmaps.
    ///
    /// The client-shallow seeds graft those commits parentless for the walk.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn needed_commits(
        &self,
        seeds: &WalkSeeds,
        policy: BandPolicy,
    ) -> Result<NeededCommits> {
        if seeds.wants.is_empty() {
            return Ok(NeededCommits::default());
        }
        // Client shallow points block both paints (git graft semantics): the
        // client sees them as parentless.
        self.banded_paint_walk(
            &seeds.wants,
            &seeds.haves,
            &seeds.shallow,
            &seeds.shallow,
            policy,
        )
        .await
    }

    /// The commit side of a shallow (`deepen <depth>`) fetch: classify by
    /// depth, then derive the needed subset.
    ///
    /// With no haves the sent set doubles as the needed one. The boundary's
    /// root trees go back for the object index to close over.
    ///
    /// # Errors
    /// If `depth <= 0`, or whatever the catalog, the bucket or a segment's
    /// bytes said.
    pub async fn needed_commits_shallow(
        &self,
        seeds: &WalkSeeds,
        depth: i64,
        policy: BandPolicy,
    ) -> Result<ShallowCommits> {
        if depth <= 0 {
            return Err(anyhow!("deepen depth must be positive"));
        }
        if seeds.wants.is_empty() {
            return Ok(ShallowCommits::default());
        }

        // Pass 1: with no haves the sent set (already bitmapped) is the needed
        // set; otherwise pass 2 recollects bitmaps for the smaller needed
        // subset, so this pass skips those columns.
        let fast_path = seeds.haves.is_empty();
        let classified = self
            .depth_classify(&seeds.wants, depth, fast_path, policy)
            .await?;

        // Boundary criterion is pooled min-depth == depth, not "has a parent
        // outside the sent set" (which breaks once two wants reach the same
        // commit). A root at that depth is harmlessly grafted parentless too.
        let boundary_seqs: Vec<i64> = classified
            .sent
            .iter()
            .filter(|(_, row)| row.depth == depth)
            .map(|(seq, _)| *seq)
            .collect();
        let client_shallow_set: HashSet<i64> = seeds.shallow.iter().copied().collect();

        // `shallow`: boundary commits the client hasn't already recorded so.
        // `unshallow`: client-shallow commits still sent but no longer on the
        // boundary — their parents are coming, so the client may drop the graft.
        let shallow: Vec<ObjectId> = boundary_seqs
            .iter()
            .filter(|seq| !client_shallow_set.contains(seq))
            .filter_map(|seq| classified.sent.get(seq))
            .map(|row| row.oid)
            .collect();
        let unshallowed_seqs: Vec<i64> = seeds
            .shallow
            .iter()
            .copied()
            .filter(|seq| classified.sent.get(seq).is_some_and(|c| c.depth != depth))
            .collect();
        let unshallow: Vec<ObjectId> = unshallowed_seqs
            .iter()
            .filter_map(|seq| classified.sent.get(seq))
            .map(|row| row.oid)
            .collect();

        // Pass 2: the needed set.
        let needed = if fast_path {
            classified.needed
        } else {
            let mut want_seqs = seeds.wants.clone();
            want_seqs.extend(
                unshallowed_seqs
                    .iter()
                    .filter_map(|seq| classified.sent.get(seq))
                    .flat_map(|commit| commit.parent_seqs.iter().copied()),
            );
            // Want expansion blocked at the new boundary and the client's
            // shallow points; continuation below a shallow point only via the
            // unshallowed-parent seeds above.
            let want_block: Vec<i64> = boundary_seqs
                .iter()
                .chain(seeds.shallow.iter())
                .copied()
                .collect();
            self.banded_paint_walk(
                &want_seqs,
                &seeds.haves,
                &want_block,
                &seeds.shallow,
                policy,
            )
            .await?
        };

        // Root tree seqs already read by pass 1 — no extra round trip.
        let boundary_roots: Vec<u64> = boundary_seqs
            .iter()
            .filter_map(|seq| classified.sent.get(seq))
            .map(|row| row.root_tree_seq)
            .collect();

        Ok(ShallowCommits {
            needed,
            shallow,
            unshallow,
            boundary_roots,
        })
    }

    /// Git-style paint-down-to-common: want and have seeds together, in
    /// descending `seq` bands, keeping what only want reaches.
    ///
    /// `seq` is strictly ancestry-monotone, so a processed band's flags are
    /// final and the walk stops once no want-only frontier remains.
    async fn banded_paint_walk(
        &self,
        want_seqs: &[i64],
        have_seqs: &[i64],
        want_block: &[i64],
        have_block: &[i64],
        mut policy: BandPolicy,
    ) -> Result<NeededCommits> {
        // Frontier: seq → accumulated flags, below the last band's floor.
        let mut frontier: BTreeMap<i64, u8> = BTreeMap::new();
        for &seq in want_seqs {
            *frontier.entry(seq).or_insert(0) |= FLAG_WANT;
        }
        for &seq in have_seqs {
            *frontier.entry(seq).or_insert(0) |= FLAG_HAVE;
        }

        let mut needed = NeededCommits::default();
        // Only a want-only frontier entry can still contribute; have-side
        // expansion exists only to paint explored history common. None left
        // means the walk is done, however much history remains below.
        while frontier.values().any(|&flags| flags == FLAG_WANT) {
            self.paint_band(
                policy.width(),
                want_block,
                have_block,
                &mut frontier,
                &mut needed,
            )
            .await?;
            policy.grow();
        }
        Ok(needed)
    }

    /// One round of [`Self::banded_paint_walk`]: expands the topmost
    /// `band_width` seqs of `frontier`.
    ///
    /// Appends finalized want-only commits to `needed` and merges below-band
    /// boundary parents back into `frontier`.
    async fn paint_band(
        &self,
        band_width: i64,
        want_block: &[i64],
        have_block: &[i64],
        frontier: &mut BTreeMap<i64, u8>,
        needed: &mut NeededCommits,
    ) -> Result<()> {
        let Some((&ceil, _)) = frontier.last_key_value() else {
            return Ok(());
        };
        let floor = ceil.saturating_sub(band_width - 1);
        let (seed_seqs, seed_flags): (Vec<i64>, Vec<u8>) = frontier
            .split_off(&floor)
            .into_iter()
            .map(|(seq, flags)| {
                // A have-reachable commit is common: it keeps painting `have`,
                // but its want flag must not leak downward.
                let flag = if flags & FLAG_HAVE == 0 {
                    FLAG_WANT
                } else {
                    FLAG_HAVE
                };
                (seq, flag)
            })
            .unzip();

        let index = self.index(floor, ceiling(&seed_seqs, floor)).await?;
        let band = index.expand_paint(&seed_seqs, &seed_flags, floor, want_block, have_block)?;
        let (packs, oids) = self.pack_facts(&band.finalized).await?;

        needed.commits.reserve(band.finalized.len());
        for seq in &band.finalized {
            let pack = packs.get(*seq).ok_or_else(|| unpacked(*seq))?;
            needed.push(*oids.get(seq).ok_or_else(|| unnamed(*seq))?, &pack)?;
        }
        for (seq, flags) in band.below {
            *frontier.entry(seq).or_insert(0) |= flags;
        }
        Ok(())
    }

    /// Depth-limited banded walk from the wants alone: paints every commit
    /// with its *minimum* depth from any want, never past `depth_limit`.
    ///
    /// Same monotonicity argument as [`Self::banded_paint_walk`]: once a band
    /// is processed its min-depth is final.
    async fn depth_classify(
        &self,
        want_seqs: &[i64],
        depth_limit: i64,
        collect_bitmaps: bool,
        mut policy: BandPolicy,
    ) -> Result<DepthClassified> {
        // Frontier: seq → minimum depth discovered so far.
        let mut frontier: BTreeMap<i64, i64> = BTreeMap::new();
        for &seq in want_seqs {
            frontier
                .entry(seq)
                .and_modify(|depth| *depth = (*depth).min(1))
                .or_insert(1);
        }

        let mut classified = DepthClassified::default();
        while !frontier.is_empty() {
            self.depth_band(
                policy.width(),
                depth_limit,
                collect_bitmaps,
                &mut frontier,
                &mut classified,
            )
            .await?;
            policy.grow();
        }
        Ok(classified)
    }

    /// One round of [`Self::depth_classify`]: expands the topmost
    /// `band_width` seqs of `frontier`.
    async fn depth_band(
        &self,
        band_width: i64,
        depth_limit: i64,
        collect_bitmaps: bool,
        frontier: &mut BTreeMap<i64, i64>,
        classified: &mut DepthClassified,
    ) -> Result<()> {
        let Some((&ceil, _)) = frontier.last_key_value() else {
            return Ok(());
        };
        let floor = ceil.saturating_sub(band_width - 1);
        let (seed_seqs, seed_depths): (Vec<i64>, Vec<i64>) =
            frontier.split_off(&floor).into_iter().unzip();

        let index = self.index(floor, ceiling(&seed_seqs, floor)).await?;
        let band = index.expand_depth(&seed_seqs, &seed_depths, floor, depth_limit)?;

        let reached: Vec<i64> = band.reached.iter().map(|one| one.seq).collect();
        // A walk that will not answer with the bitmaps does not read them.
        let (packs, oids) = if collect_bitmaps {
            self.pack_facts(&reached).await?
        } else {
            (PackFacts::default(), self.oids_of(&reached).await?)
        };

        for one in &band.reached {
            let oid = *oids.get(&one.seq).ok_or_else(|| unnamed(one.seq))?;
            if collect_bitmaps {
                let pack = packs.get(one.seq).ok_or_else(|| unpacked(one.seq))?;
                classified.needed.push(oid, &pack)?;
            }
            classified.sent.insert(
                one.seq,
                SentCommit {
                    oid,
                    parent_seqs: index
                        .get(one.seq)
                        .ok_or_else(|| missing(one.seq))?
                        .parents()
                        .iter()
                        .collect(),
                    depth: one.depth,
                    root_tree_seq: one.root_tree_seq,
                },
            );
        }
        for (seq, depth) in band.below {
            frontier
                .entry(seq)
                .and_modify(|held| *held = (*held).min(depth))
                .or_insert(depth);
        }
        Ok(())
    }

    /// Whether `ancestor_seq` is `descendant_seq` itself or reachable from it
    /// via parent edges — git's `--is-ancestor`.
    ///
    /// Floored at `ancestor_seq`: since `parent.seq < child.seq` always, a
    /// path dropping below it without hitting it exactly never will.
    pub(crate) async fn banded_is_ancestor(
        &self,
        ancestor_seq: i64,
        descendant_seq: i64,
        mut policy: BandPolicy,
    ) -> Result<bool> {
        if ancestor_seq == descendant_seq {
            return Ok(true);
        }
        if ancestor_seq > descendant_seq {
            return Ok(false);
        }

        let mut frontier: HashSet<i64> = HashSet::from([descendant_seq]);
        while let Some(&ceil) = frontier.iter().max() {
            let floor = ceil.saturating_sub(policy.width() - 1).max(ancestor_seq);
            let seeds: Vec<i64> = frontier.into_iter().collect();
            let (visited, below) = self.expand_ancestor_band(&seeds, floor).await?;
            if visited.contains(&ancestor_seq) {
                return Ok(true);
            }
            // Once `floor` hits its clamp at `ancestor_seq`, this round already
            // searched down to the target — anything still `below` is
            // unreachable by monotonicity. Keep going only while `floor` was
            // width-limited.
            if floor <= ancestor_seq {
                return Ok(false);
            }
            frontier = below;
            policy.grow();
        }
        Ok(false)
    }

    /// The best common ancestors of `a_seq` and `b_seq` — `git merge-base
    /// --all` — and whether the walk stopped at its bound instead.
    ///
    /// Every commit both reach, less those another one of them reaches. One
    /// is ordinary, two is a criss-cross, none is two histories that never met.
    pub(crate) async fn banded_merge_bases(
        &self,
        a_seq: i64,
        b_seq: i64,
        policy: BandPolicy,
        max_commits: u64,
    ) -> Result<(Vec<i64>, bool)> {
        let max_commits = usize::try_from(max_commits).unwrap_or(usize::MAX);
        if a_seq == b_seq {
            return Ok((vec![a_seq], false));
        }

        // What each side reaches, frontier included: a parent below the band
        // floor is reachable whether or not it has been expanded yet, and the
        // walk stops on the frontier being common rather than explored.
        let mut reach_a: HashSet<i64> = HashSet::from([a_seq]);
        let mut reach_b: HashSet<i64> = HashSet::from([b_seq]);
        let mut frontier_a: HashSet<i64> = HashSet::from([a_seq]);
        let mut frontier_b: HashSet<i64> = HashSet::from([b_seq]);

        let mut band = policy;
        let mut exhausted = false;

        while let Some(ceil) = frontier_a.iter().chain(frontier_b.iter()).max().copied() {
            let floor = ceil.saturating_sub(band.width() - 1);

            for (frontier, reach) in [
                (&mut frontier_a, &mut reach_a),
                (&mut frontier_b, &mut reach_b),
            ] {
                self.merge_base_side(floor, frontier, reach).await?;
            }

            // Everything left to explore is reachable from both, so everything
            // below it is common too — and dominated by it. Nothing maximal is
            // left down there.
            if frontier_a.is_subset(&reach_b) && frontier_b.is_subset(&reach_a) {
                break;
            }

            // Counted in commits read rather than in bands, since a band's
            // width is a schedule and this is a budget.
            if reach_a.len() + reach_b.len() > max_commits {
                exhausted = true;
                break;
            }
            band.grow();
        }

        let mut common: Vec<i64> = reach_a.intersection(&reach_b).copied().collect();
        common.sort_unstable_by(|left, right| right.cmp(left));
        Ok((self.maximal(&common).await?, exhausted))
    }

    /// Expands one side of a merge-base walk down to `floor`.
    ///
    /// A frontier wholly below the floor is left alone: the walk would hand
    /// the seeds straight back, so the round trip buys nothing.
    async fn merge_base_side(
        &self,
        floor: i64,
        frontier: &mut HashSet<i64>,
        reach: &mut HashSet<i64>,
    ) -> Result<()> {
        if frontier.iter().all(|&seq| seq < floor) {
            return Ok(());
        }
        let seeds: Vec<i64> = frontier.iter().copied().collect();
        let (visited, below) = self.expand_ancestor_band(&seeds, floor).await?;
        reach.extend(visited);
        reach.extend(below.iter().copied());
        *frontier = below;
        Ok(())
    }

    /// Those of `common` that no other one of them reaches, newest first.
    ///
    /// `common` arrives newest first, so its head is always maximal. Taking
    /// one removes everything it reaches, so this expands once per base.
    async fn maximal(&self, common: &[i64]) -> Result<Vec<i64>> {
        let mut left: HashSet<i64> = common.iter().copied().collect();
        let mut bases = Vec::new();

        for &seq in common {
            if !left.remove(&seq) {
                continue;
            }
            bases.push(seq);
            if left.is_empty() {
                break;
            }
            // Everything this one reaches is the ancestor of a common
            // ancestor, so none of it is maximal. One band down to the lowest
            // candidate still in play covers all of them, so this is one
            // expansion per base.
            let Some(floor) = left.iter().min().copied() else {
                break;
            };
            let (visited, _) = self.expand_ancestor_band(&[seq], floor).await?;
            left.retain(|candidate| !visited.contains(candidate));
        }

        Ok(bases)
    }

    /// Everything `seeds` reach down to `floor`, and what they reached below
    /// it, over one composed read of the band.
    async fn expand_ancestor_band(
        &self,
        seeds: &[i64],
        floor: i64,
    ) -> Result<(HashSet<i64>, HashSet<i64>)> {
        let index = self.index(floor, ceiling(seeds, floor)).await?;
        Ok(index.expand_ancestors(seeds, floor)?)
    }

    /// The tier-two facts and the oids for a set of seqs, in one round.
    async fn pack_facts(&self, seqs: &[i64]) -> Result<(PackFacts, HashMap<i64, ObjectId>)> {
        if seqs.is_empty() {
            return Ok((PackFacts::default(), HashMap::new()));
        }
        tokio::try_join!(self.packs_at(seqs), self.oids_of(seqs))
    }
}

/// The top of a band, which is the highest seed it starts from.
fn ceiling(seeds: &[i64], floor: i64) -> i64 {
    seeds.iter().copied().max().unwrap_or(floor).max(floor)
}
