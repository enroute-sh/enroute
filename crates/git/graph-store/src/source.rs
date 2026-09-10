//! The reads that answer in oids.
//!
//! A caller asks in oids, the walks run in seq space, and this is the lookup
//! on each side of one. Nothing recursive reaches the database.

use std::collections::HashMap;

use anyhow::{Result, anyhow};
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{
    COMMIT_PACK_HEADER_SIZE, CommitPackLocation, ObjectHashSet, ObjectMeta, PackImageLocation,
};
use enroute_git_metadata::Identity;

use crate::CommitIndex;
use crate::store::RepoGraph;
use crate::walk::BandPolicy;

/// The best common ancestors of two commits, as a caller named them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bases {
    /// Maximal common ancestors, newest first, empty when the two histories
    /// share nothing.
    pub bases: Vec<ObjectId>,
    /// Set when the walk stopped at its bound, so `bases` is what was found
    /// by then rather than the answer.
    pub exhausted: bool,
}

/// Seqs one round of a first-parent page reads at a time.
///
/// A page follows one edge per commit, so the band only has to be wide
/// enough that an ordinary page is one read.
const PAGE_BAND: i64 = 4096;

impl RepoGraph<'_> {
    /// The parent seqs of one commit, in the order git recorded them.
    ///
    /// # Errors
    /// Whatever the catalog said, or a commit the graph does not hold.
    pub async fn parents_of(&self, seq: i64) -> Result<Vec<i64>> {
        let index = self.index(seq, seq).await?;
        Ok(index
            .get(seq)
            .ok_or_else(|| missing(seq))?
            .parents()
            .iter()
            .collect())
    }

    /// Where each named commit's own pack entry is.
    ///
    /// Takes what identity already said rather than asking again, and reads
    /// past everything in it that is not a commit.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn locations_of(
        &self,
        named: &[(ObjectId, Identity)],
    ) -> Result<Vec<(ObjectId, ObjectMeta)>> {
        let seqs: HashMap<ObjectId, i64> = named
            .iter()
            .filter(|(_, held)| held.kind == Kind::Commit)
            .map(|(oid, held)| (*oid, held.seq))
            .collect();
        if seqs.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: Vec<i64> = seqs.values().copied().collect();
        let packs = self.packs_at(&wanted).await?;
        // A commit numbered but not yet recorded answers `None` rather than
        // failing: identity says what an oid is called, the index says what
        // the repository holds, and one batch must not fail for the other's
        // absence.
        Ok(seqs
            .into_iter()
            .map(|(oid, seq)| {
                let location = packs.get(seq).map(|pack| CommitPackLocation {
                    image: PackImageLocation {
                        pack_sha: oid,
                        offset: COMMIT_PACK_HEADER_SIZE,
                        entry_len: pack.entry_len(),
                        base: None,
                    },
                    segment: pack.segment(),
                });
                (
                    oid,
                    ObjectMeta {
                        kind: Kind::Commit,
                        location,
                        object_seq: None,
                    },
                )
            })
            .collect())
    }

    /// Which of `candidates` the history of `seed` reaches.
    ///
    /// Floored so a walk cannot run the length of history: sound, not
    /// complete, which is the contract.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn ancestors_among(
        &self,
        seed: ObjectId,
        candidates: &[ObjectId],
        max_commits: u64,
    ) -> Result<ObjectHashSet> {
        if candidates.is_empty() {
            return Ok(ObjectHashSet::default());
        }
        let mut oids: Vec<ObjectId> = candidates.to_vec();
        oids.push(seed);
        let seqs = self.seqs_of(&oids).await?;

        let Some(&seed_seq) = seqs.get(&seed) else {
            return Ok(ObjectHashSet::default());
        };
        let wanted: HashMap<i64, ObjectId> = candidates
            .iter()
            .filter_map(|oid| seqs.get(oid).map(|seq| (*seq, *oid)))
            .collect();
        let Some(lowest) = wanted.keys().copied().min() else {
            return Ok(ObjectHashSet::default());
        };

        let reach = i64::try_from(max_commits).unwrap_or(i64::MAX);
        let floor = lowest.max(seed_seq.saturating_sub(reach));
        let index = self.index(floor, seed_seq).await?;
        let (visited, below) = index.expand_ancestors(&[seed_seq], floor)?;

        let mut found = ObjectHashSet::default();
        for seq in visited.iter().chain(below.iter()) {
            if let Some(oid) = wanted.get(seq) {
                found.insert(*oid);
            }
        }
        Ok(found)
    }

    /// The best common ancestors of two commits.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn merge_bases(&self, a: ObjectId, b: ObjectId, max_commits: u64) -> Result<Bases> {
        let seqs = self.seqs_of(&[a, b]).await?;
        let (Some(&a_seq), Some(&b_seq)) = (seqs.get(&a), seqs.get(&b)) else {
            return Ok(Bases::default());
        };
        let (found, exhausted) = self
            .banded_merge_bases(a_seq, b_seq, BandPolicy::production(), max_commits)
            .await?;

        let oids = self.oids_of(&found).await?;
        Ok(Bases {
            bases: found
                .iter()
                .map(|seq| oids.get(seq).copied().ok_or_else(|| unnamed(*seq)))
                .collect::<Result<_>>()?,
            exhausted,
        })
    }

    /// Whether `ancestor` is `descendant` or is reachable from it.
    ///
    /// An oid the graph does not hold has no ancestry, so the answer is
    /// `false` rather than an error.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn is_ancestor(&self, ancestor: ObjectId, descendant: ObjectId) -> Result<bool> {
        let seqs = self.seqs_of(&[ancestor, descendant]).await?;
        let (Some(&ancestor), Some(&descendant)) = (seqs.get(&ancestor), seqs.get(&descendant))
        else {
            return Ok(false);
        };
        self.banded_is_ancestor(ancestor, descendant, BandPolicy::production())
            .await
    }

    /// A history page: `tip`, then its first parent, and so on.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn first_parent_page(&self, tip: ObjectId, limit: u32) -> Result<Vec<ObjectId>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        let Some(&tip) = self.seqs_of(&[tip]).await?.get(&tip) else {
            return Ok(Vec::new());
        };

        let mut page: Vec<i64> = Vec::new();
        let mut at = Some(tip);
        while let Some(seq) = at {
            if page.len() >= limit {
                break;
            }
            let floor = seq.saturating_sub(PAGE_BAND).max(0);
            at = follow(&self.index(floor, seq).await?, seq, floor, limit, &mut page)?;
        }

        let oids = self.oids_of(&page).await?;
        page.iter()
            .map(|seq| oids.get(seq).copied().ok_or_else(|| unnamed(*seq)))
            .collect()
    }
}

pub(crate) fn missing(seq: i64) -> anyhow::Error {
    anyhow!("the commit graph does not hold commit {seq}")
}

pub(crate) fn unnamed(seq: i64) -> anyhow::Error {
    anyhow!("commit {seq} is in the commit graph but has no oid recorded")
}

pub(crate) fn unpacked(seq: i64) -> anyhow::Error {
    anyhow!("commit {seq} is in the commit graph but has no pack recorded")
}

/// Follows first parents from `from` while this band still holds them.
///
/// Returns where it stopped: `Some` for a parent below the band, which the
/// next round reloads around, and `None` for a root or a full page.
fn follow(
    index: &CommitIndex,
    from: i64,
    floor: i64,
    limit: usize,
    page: &mut Vec<i64>,
) -> Result<Option<i64>> {
    let mut at = Some(from);
    while let Some(seq) = at {
        if seq < floor {
            return Ok(Some(seq));
        }
        if page.len() >= limit {
            return Ok(None);
        }
        let entry = index.get(seq).ok_or_else(|| missing(seq))?;
        page.push(seq);
        at = entry.parents().iter().next();
    }
    Ok(None)
}
