//! The push path: hand out seqs, rank the commits, write two segments.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow};
use gix_hash::ObjectId;

use enroute_git_core::Ulid;
use enroute_git_journal::{Index, Journal};
use enroute_lattice_core::{Key, KeyRange};
use enroute_lattice_store::{Entry, Reclaimed, Report, Sweep, Swept};

use crate::store::RepoGraph;
use crate::{Builder, DatedCommit, Pack, PackBuilder, PackEntry, PackIndex};

/// The ranges a scan reads, cut only where no segment straddles the cut.
///
/// A read fetches a whole bucket object, so a cut inside a segment would
/// fetch it again for the band on the other side.
fn bands(entries: &[Entry]) -> Vec<KeyRange> {
    let mut ranges: Vec<(u64, u64)> = entries
        .iter()
        .map(|entry| (entry.range.first().get(), entry.range.last().get()))
        .collect();
    ranges.sort_unstable();

    let mut bands: Vec<(u64, u64)> = Vec::new();
    for (first, last) in ranges {
        if let Some(band) = bands.last_mut() {
            // Absorbed when it overlaps — splitting there would fetch it
            // twice — and while the band is still narrower than the target.
            if first <= band.1 || band.1.saturating_sub(band.0) < SCAN_BAND {
                band.1 = band.1.max(last);
                continue;
            }
        }
        bands.push((first, last));
    }

    bands
        .into_iter()
        .filter_map(|(first, last)| KeyRange::new(Key::new(first), Key::new(last)).ok())
        .collect()
}

/// Splits seq-ordered images into runs, cutting where the gap between two of
/// them would cost more in empty slots than another segment costs to read.
fn runs<'a>(ordered: &[&'a (i64, Pack)]) -> Vec<Vec<&'a (i64, Pack)>> {
    let mut runs: Vec<Vec<&(i64, Pack)>> = Vec::new();
    let mut run: Vec<&(i64, Pack)> = Vec::new();
    for image in ordered {
        let far = run
            .last()
            .is_some_and(|(last, _): &&(i64, Pack)| image.0.saturating_sub(*last) > RELOCATION_GAP);
        if far {
            runs.push(std::mem::take(&mut run));
        }
        run.push(image);
    }
    if !run.is_empty() {
        runs.push(run);
    }
    runs
}

/// One entry's pack facts, decoded.
fn image(entry: &PackEntry<'_>) -> Result<Pack> {
    Ok(Pack {
        entry_len: entry.entry_len(),
        blob_offset: entry.blob_offset(),
        segment: entry.segment(),
        trees: entry.trees().context("a pack's tree bitmap")?,
        blobs: entry.blobs().context("a pack's blob bitmap")?,
    })
}

/// The seq gap a relocation segment will span rather than split at.
///
/// A slot costs sixty bytes whether a commit is in it or not, so this is
/// the width of hole worth carrying to keep two segments from becoming three.
const RELOCATION_GAP: i64 = 1024;

/// Commit seqs one band of a gather's scan covers.
///
/// The scan reads a band's pack bitmaps into memory, and this runs beside a
/// front door by default — so it is a memory bound rather than a tuning knob.
const SCAN_BAND: u64 = 25_000;

/// What the parents of a push rank at, read before its transaction opens.
///
/// Its own step because the read is a read: a connection that waits on a
/// second one while holding a transaction is how a pool deadlocks.
#[derive(Debug, Clone, Default)]
pub struct Ranks {
    generations: HashMap<i64, u32>,
}

/// One commit a push is recording.
#[derive(Debug, Clone)]
pub struct Recorded {
    /// The seq it was allocated.
    pub seq: i64,
    /// What it is called.
    pub oid: ObjectId,
    /// Its parents' seqs, in the order git records them.
    pub parents: Vec<i64>,
    /// Its root tree's `object_seq`.
    pub root_tree_seq: u64,
    /// When it was last applied, in seconds since the epoch.
    pub committer_date: i64,
    /// What a fetch needs about its pack.
    pub pack: Pack,
}

impl RepoGraph<'_> {
    /// What every parent in `parents` but not in `mine` ranks at.
    ///
    /// One read of the segments spanning them, which for an ordinary push is
    /// the tip it built on and nothing else.
    ///
    /// # Errors
    /// Whatever the catalog said, or a parent the graph does not hold.
    pub async fn ranks_of(&self, mine: &[i64], parents: &[i64]) -> Result<Ranks> {
        let mine: HashSet<i64> = mine.iter().copied().collect();
        let outside: Vec<i64> = parents
            .iter()
            .copied()
            .filter(|parent| !mine.contains(parent))
            .collect();
        Ok(Ranks {
            generations: self.generations_of(&outside).await?,
        })
    }

    /// Adds `commits` to `journal`, ranked against `ranks`.
    ///
    /// Encoded and put here, listed when the journal lands: a rollback then
    /// leaves an orphan rather than a row pointing at nothing.
    ///
    /// # Errors
    /// Whatever the bucket or the ranking said.
    pub async fn record(
        &self,
        journal: &mut Journal,
        commits: &[Recorded],
        ranks: &Ranks,
    ) -> Result<()> {
        if commits.is_empty() {
            return Ok(());
        }
        let (graph, packs) = Self::build(commits, ranks)?;
        let graph_scope = self.graph_scope();
        let pack_scope = self.pack_scope();

        if let Some(segment) = self.store.graph.prepare(&graph_scope, &graph).await? {
            journal.list(Index::CommitGraph, &graph_scope, segment);
        }
        if let Some(segment) = self.store.packs.prepare(&pack_scope, &packs).await? {
            journal.list(Index::CommitPacks, &pack_scope, segment);
        }

        Ok(())
    }

    /// Merge this repository's commit segments down a tier where the policy
    /// says to.
    ///
    /// Safe to repeat and safe to interrupt: a pass that dies part way leaves
    /// what it already merged merged, and the next one carries on.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn compact(&self) -> Result<Report> {
        let graph = self
            .store
            .graph
            .compact(&self.graph_scope())
            .await
            .context("compacting the commit index")?;
        let packs = self
            .store
            .packs
            .compact(&self.pack_scope())
            .await
            .context("compacting the pack index")?;
        Ok(graph.plus(packs))
    }

    /// Delete every bucket object this repository's segments hold.
    ///
    /// Before the rows go, since the rows are what name the keys — and a
    /// repository being erased has no reader left to see the gap.
    ///
    /// # Errors
    /// Whatever the catalog said.
    pub async fn purge_bucket(&self) -> Result<Reclaimed> {
        let graph = self
            .store
            .graph
            .purge_bucket(&self.graph_scope())
            .await
            .context("purging the commit index's bucket objects")?;
        let packs = self
            .store
            .packs
            .purge_bucket(&self.pack_scope())
            .await
            .context("purging the pack index's bucket objects")?;
        Ok(graph.plus(packs))
    }

    /// Every commit whose pack image sits in one of `segments`.
    ///
    /// A banded scan, since this index is keyed by commit seq and holding a
    /// repository's whole history of bitmaps at once is not a bound.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn images_in(&self, segments: &HashSet<Ulid>) -> Result<Vec<(i64, Pack)>> {
        let scope = self.pack_scope();
        // From the catalog rather than from a counter: the listing carries
        // every segment's range, and no bytes come with it.
        let entries = self
            .store
            .packs
            .catalog()
            .entries(&scope)
            .await
            .map_err(anyhow::Error::from_boxed)
            .context("listing the pack index to gather")?;

        let mut found = Vec::new();
        for band in bands(&entries) {
            let read = self
                .store
                .packs
                .read(&scope, band)
                .await
                .context("reading the pack index to gather")?;

            let held = read
                .iter()
                .flat_map(PackIndex::entries)
                .filter(|(_, entry)| segments.contains(&entry.segment().id));
            for (seq, entry) in held {
                found.push((seq, image(&entry)?));
            }
        }
        Ok(found)
    }

    /// Records where these images were copied to.
    ///
    /// The node naming the newer object wins the join, so this retires what
    /// it replaces. One segment per run of seqs: a hole costs sixty bytes.
    ///
    /// # Errors
    /// Whatever the bucket or the catalog said.
    pub async fn relocate_images(
        &self,
        journal: &mut Journal,
        moved: &[(i64, Pack)],
    ) -> Result<()> {
        if moved.is_empty() {
            return Ok(());
        }
        let mut ordered: Vec<&(i64, Pack)> = moved.iter().collect();
        ordered.sort_by_key(|(seq, _)| *seq);

        let scope = self.pack_scope();
        for run in runs(&ordered) {
            let mut builder = PackBuilder::new();
            for (seq, pack) in run {
                builder.insert(*seq, pack).context("a moved pack image")?;
            }
            if let Some(segment) = self
                .store
                .packs
                .prepare(&scope, &builder.build())
                .await
                .context("writing the moved pack images")?
            {
                journal.list(Index::CommitPacks, &scope, segment);
            }
        }
        Ok(())
    }

    /// Delete this repository's index objects that no segment row names.
    ///
    /// Both tiers, since a segment of either graduates by the same rule and
    /// is orphaned by the same rollback.
    ///
    /// # Errors
    /// Whatever the bucket or the catalog said.
    pub async fn sweep_bucket(&self, sweep: Sweep) -> Result<Swept> {
        let graph = self
            .store
            .graph
            .sweep_bucket(&self.graph_scope(), sweep)
            .await
            .context("sweeping the commit index's bucket objects")?;
        let packs = self
            .store
            .packs
            .sweep_bucket(&self.pack_scope(), sweep)
            .await
            .context("sweeping the pack index's bucket objects")?;
        Ok(graph.plus(packs))
    }

    /// Both tiers for one push, with every commit ranked.
    ///
    /// Pure: everything it needed to read was read by `ranks_for`.
    fn build(commits: &[Recorded], ranks: &Ranks) -> Result<(crate::CommitIndex, PackIndex)> {
        let mut graph = Builder::new();
        for (seq, generation) in &ranks.generations {
            graph.know(*seq, *generation);
        }

        // Seq order is topological order: a parent is always allocated
        // before the child that can name it, so every parent is already in
        // the builder by the time its child is ranked.
        let mut ordered: Vec<&Recorded> = commits.iter().collect();
        ordered.sort_by_key(|commit| commit.seq);

        let mut packs = PackBuilder::new();
        for commit in ordered {
            graph.insert_dated(
                commit.seq,
                DatedCommit {
                    parents: &commit.parents,
                    root_tree: commit.root_tree_seq,
                    committer_date: commit.committer_date,
                },
            )?;
            packs.insert(commit.seq, &commit.pack)?;
        }
        Ok((graph.build(), packs.build()))
    }

    /// The generation of every parent this push does not itself hold.
    ///
    /// One read of the segments spanning them, which for an ordinary push is
    /// the tip it built on and nothing else.
    async fn generations_of(&self, seqs: &[i64]) -> Result<HashMap<i64, u32>> {
        let (Some(floor), Some(ceil)) = (seqs.iter().copied().min(), seqs.iter().copied().max())
        else {
            return Ok(HashMap::new());
        };

        let index = self.index(floor, ceil).await?;
        let mut found = HashMap::with_capacity(seqs.len());
        for seq in seqs {
            let entry = index.get(*seq).ok_or_else(|| {
                anyhow!("commit {seq} is named as a parent but is not in the commit graph")
            })?;
            found.insert(*seq, entry.generation());
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use enroute_lattice_core::{Residence, Tier};

    use super::*;

    fn entry(first: u64, last: u64) -> Entry {
        Entry {
            id: enroute_lattice_store::SegmentId::fresh(),
            range: KeyRange::new(Key::new(first), Key::new(last)).expect("a range"),
            tier: Tier::ZERO,
            bytes: 0,
            residence: Residence::Bucket,
        }
    }

    fn spans(bands: &[KeyRange]) -> Vec<(u64, u64)> {
        bands
            .iter()
            .map(|band| (band.first().get(), band.last().get()))
            .collect()
    }

    /// A read fetches whole objects, so a band ending inside a segment would
    /// fetch that segment for the band on either side of the cut.
    #[test]
    fn a_band_never_ends_inside_a_segment() {
        let wide = SCAN_BAND * 4;
        let bands = bands(&[entry(0, wide), entry(wide + 1, wide + 10)]);

        assert_eq!(
            spans(&bands),
            vec![(0, wide), (wide + 1, wide + 10)],
            "a segment wider than a band was cut in two"
        );
    }

    #[test]
    fn narrow_segments_share_a_band() {
        let bands = bands(&[entry(0, 10), entry(11, 20), entry(21, 30)]);
        assert_eq!(spans(&bands), vec![(0, 30)], "one read would have done");
    }

    /// Overlap is allowed by the substrate, and both sides of one must be
    /// read together or the join sees half of it.
    #[test]
    fn overlapping_segments_share_a_band() {
        let wide = SCAN_BAND * 2;
        let bands = bands(&[entry(0, wide), entry(wide / 2, wide + 5)]);
        assert_eq!(spans(&bands), vec![(0, wide + 5)]);
    }

    fn moved(seq: i64) -> (i64, Pack) {
        (
            seq,
            Pack {
                entry_len: 1,
                blob_offset: 0,
                segment: enroute_git_core::SegmentLocation {
                    id: Ulid(1),
                    base_offset: 0,
                    image_len: 1,
                },
                trees: roaring::RoaringTreemap::new(),
                blobs: roaring::RoaringTreemap::new(),
            },
        )
    }

    /// This tier is a stride array, so one segment spanning a large push
    /// would be sixty bytes of zeros for every commit of it.
    #[test]
    fn a_relocation_splits_rather_than_span_a_large_gap() {
        let images = [moved(1), moved(2), moved(1_000_000)];
        let ordered: Vec<&(i64, Pack)> = images.iter().collect();

        let runs = runs(&ordered);
        assert_eq!(runs.len(), 2, "the hole was carried rather than cut");
        assert_eq!(runs[0].len(), 2);
        assert_eq!(runs[1].len(), 1);
    }

    #[test]
    fn a_relocation_keeps_a_small_gap_in_one_segment() {
        let images = [moved(1), moved(RELOCATION_GAP)];
        let ordered: Vec<&(i64, Pack)> = images.iter().collect();

        assert_eq!(runs(&ordered).len(), 1, "two segments where one would do");
    }
}
