//! Gathering a repository's pack images into fewer, larger segment objects.
//!
//! A push writes its own segment objects, so a repository pushed to often
//! holds many small ones and a wide fetch pays a GET for each. This copies a
//! run of them into one object and points every location at the copy. Not a
//! repack, and the difference is the whole design: a pack image is copied
//! byte for byte, so nothing is re-deltified, no pack is rewritten and no
//! object id moves. It buys fewer round trips and no bytes back — reclaiming
//! what an unreachable object holds needs a real repack, which this is not
//! and does not stand in for. Here rather than in a layer below because it
//! reads and writes all of them, as [`Storage`] itself does.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use futures::TryStreamExt as _;

use enroute_git_core::{ObjectSeqs, Ulid};
use enroute_git_journal::Journal;
use enroute_git_metadata::RepoMetadata;

use crate::storage::Storage;

/// When gathering is worth doing, and how much it takes on at once.
///
/// No default, for the reason [`enroute_lattice_core::Policy`] has none:
/// every number here trades read cost against write cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoalescePolicy {
    /// The size under which a segment object counts as small enough to move.
    ///
    /// A large one is already worth its own GET, and copying it would be
    /// paying its bytes again to save nothing.
    pub small_bytes: u64,
    /// The most bytes one merge copies, which bounds what a pass costs.
    pub max_output_bytes: u64,
    /// The most segments one merge takes.
    pub max_inputs: usize,
}

/// What a deployment starts coalescing with.
///
/// A starting point rather than a measurement: eight megabytes is small
/// enough that a fetch reading one is mostly paying for the round trip.
pub const STARTING_COALESCE: CoalescePolicy = CoalescePolicy {
    small_bytes: 8 << 20,
    max_output_bytes: 256 << 20,
    max_inputs: 32,
};

/// What one pass gathered.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Coalesced {
    /// Segment objects copied into a new one.
    pub segments: u64,
    /// Bytes the new object holds.
    pub bytes: u64,
    /// Pack images that now sit somewhere else.
    pub images: u64,
    /// Object locations rewritten to say so.
    pub locations: u64,
}

impl Coalesced {
    /// Two passes' worth, summed.
    #[must_use]
    pub const fn plus(self, other: Self) -> Self {
        Self {
            segments: self.segments.saturating_add(other.segments),
            bytes: self.bytes.saturating_add(other.bytes),
            images: self.images.saturating_add(other.images),
            locations: self.locations.saturating_add(other.locations),
        }
    }
}

/// One segment object to copy, and where its bytes land in the copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Source {
    id: Ulid,
    bytes: u64,
    shift: u64,
}

/// Copy one run of `repo`'s small segment objects into one larger object.
///
/// Nothing happens when the run would be one object or none: copying a
/// segment on its own rewrites it for no gain.
///
/// # Errors
/// Whatever a store said.
pub async fn coalesce(
    storage: &Storage,
    repo: &RepoMetadata,
    policy: CoalescePolicy,
) -> Result<Coalesced> {
    let listed = storage.store.list_segments(repo).await?;
    // Live, not merely referenced: a retired segment keeps its row until the
    // sweep's window passes, and copying one again would carry bytes whose
    // images already moved into the next object, and again on the next pass.
    let live = storage
        .rows
        .repo(repo.id)
        .live_segments(&listed.iter().map(|(id, _)| *id).collect::<Vec<_>>())
        .await?;
    let sources = plan(&listed, &live, policy);
    if sources.len() < 2 {
        return Ok(Coalesced::default());
    }

    // Later than every source, rather than whatever this host's clock says.
    // Both indexes keep the record naming the greater id, so a copy stamped
    // behind what it copies would be dropped by the join while the sources
    // are retired — leaving every location naming bytes on their way out.
    let into = after(&sources);
    let moved: HashMap<Ulid, (Ulid, u64)> = sources
        .iter()
        .map(|source| (source.id, (into, source.shift)))
        .collect();

    // Every read before the transaction opens: a connection waiting on a
    // second one while holding the first is how a pool deadlocks.
    let images = storage
        .graph
        .repo(repo.id)
        .images_in(&moved.keys().copied().collect())
        .await?;
    if images.is_empty() {
        // Nothing is written yet, so there is nothing to undo: the sources
        // hold no image any more, which a concurrent gather explains.
        return Ok(Coalesced::default());
    }
    let objects = objects_in(&images);
    let relocated = relocated(&images, &moved);
    let locations = storage
        .objects
        .repo(repo.id)
        .moved_locations(&objects, &moved)
        .await?;

    // Outside the transaction, because a quarter of a gigabyte streamed
    // through a bucket is a long time to hold a connection idle in one — and
    // with the pass running in the serving process, that connection is one a
    // push wanted.
    let bytes = copy(storage, repo, &sources, into).await?;

    // Built before the lock is asked for, so the index segments this puts are
    // not another round trip a held lock waits on. A pass that then loses the
    // lock has put objects no row will name, which is the orphan the index
    // sweep already collects.
    let mut journal = Journal::new();
    storage
        .objects
        .repo(repo.id)
        .relocate(&mut journal, &locations)
        .await?;
    storage
        .graph
        .repo(repo.id)
        .relocate_images(&mut journal, &relocated)
        .await?;
    journal.register([into]);
    // Retired rather than dropped: the sweep keys off the row, and a reader
    // that composed the index before this commit is still reading ranges out
    // of these objects. The row goes a grace window later.
    journal.retire(sources.iter().map(|source| source.id));

    if !storage.ledger.commit_held(repo, &journal).await? {
        // Somebody else is gathering this repository, so the copy above is
        // this pass's to take back rather than the sweep's to find in six
        // hours. A failed delete leaves an orphan, which is what the sweep
        // is for.
        if let Err(error) = storage.store.delete_segment(repo, into).await {
            tracing::warn!(%error, segment = %into, "a lost gather left its copy behind");
        }
        return Ok(Coalesced::default());
    }

    Ok(Coalesced {
        segments: u64::try_from(sources.len()).unwrap_or(u64::MAX),
        bytes,
        images: u64::try_from(images.len()).unwrap_or(u64::MAX),
        locations: u64::try_from(locations.len()).unwrap_or(u64::MAX),
    })
}

/// A fresh id later than every source's, whatever this host's clock says.
///
/// A ULID orders by its timestamp first, so one stamped past the newest
/// source outranks all of them under any skew between the hosts.
fn after(sources: &[Source]) -> Ulid {
    let newest = sources
        .iter()
        .map(|source| source.id.timestamp_ms())
        .max()
        .unwrap_or(0);
    let fresh = Ulid::generate();
    if fresh.timestamp_ms() > newest {
        return fresh;
    }
    Ulid::from_parts(newest.saturating_add(1), fresh.random())
}

/// Which of `listed` to copy, and where each lands in the copy.
///
/// Only segments a row still names: an unreferenced one is a failed push's,
/// and copying it would make it live again.
fn plan(listed: &[(Ulid, u64)], live: &HashSet<Ulid>, policy: CoalescePolicy) -> Vec<Source> {
    let mut small: Vec<(Ulid, u64)> = listed
        .iter()
        .filter(|(id, _)| live.contains(id))
        .filter(|(_, bytes)| *bytes < policy.small_bytes)
        .copied()
        .collect();
    // Oldest first, so what is copied has settled and a run is of neighbours
    // in time — which is roughly how a fetch reads them.
    small.sort_unstable();

    let mut sources = Vec::new();
    let mut shift: u64 = 0;
    for (id, bytes) in small {
        if sources.len() >= policy.max_inputs
            || shift.saturating_add(bytes) > policy.max_output_bytes
        {
            break;
        }
        sources.push(Source { id, bytes, shift });
        shift = shift.saturating_add(bytes);
    }
    sources
}

/// Stream every source object into one new one, in the planned order.
async fn copy(
    storage: &Storage,
    repo: &RepoMetadata,
    sources: &[Source],
    into: Ulid,
) -> Result<u64> {
    let mut writer = storage.store.segment_writer(repo, into);
    let mut written: u64 = 0;
    for source in sources {
        let Some(mut stream) = storage
            .store
            .stream_segment_slice(repo, source.id, 0, None)
            .await?
        else {
            // Gone from under us, which only a sweep that took a live
            // segment could do. Abandon the copy rather than write a hole.
            writer.abort().await?;
            anyhow::bail!("segment {} is missing", source.id);
        };
        // Where this source's bytes start in the copy, as planned. Every
        // offset after it is measured from here.
        let starts_at = written;
        while let Some(chunk) = stream.try_next().await.context("reading a segment")? {
            written = written.saturating_add(u64::try_from(chunk.len()).unwrap_or(0));
            writer.write(chunk).await?;
        }

        // The plan measured this segment from a listing and the relocation is
        // already computed against it, so a segment that streamed a different
        // length would put every image after it at the wrong offset — and a
        // wrong offset reads as garbage rather than as a failure.
        let streamed = written.saturating_sub(starts_at);
        if starts_at != source.shift || streamed != source.bytes {
            writer.abort().await?;
            anyhow::bail!(
                "segment {} streamed {streamed} bytes at {starts_at}, planned {} at {}",
                source.id,
                source.bytes,
                source.shift
            );
        }
    }
    writer.finish().await?;
    Ok(written)
}

/// The objects every moved image holds, both shelves at once.
fn objects_in(images: &[(i64, enroute_git_graph_store::Pack)]) -> ObjectSeqs {
    let mut objects = ObjectSeqs::default();
    for (_, pack) in images {
        objects.trees |= &pack.trees;
        objects.blobs |= &pack.blobs;
    }
    objects
}

/// Every moved image, with its segment location pointing at the copy.
fn relocated(
    images: &[(i64, enroute_git_graph_store::Pack)],
    moved: &HashMap<Ulid, (Ulid, u64)>,
) -> Vec<(i64, enroute_git_graph_store::Pack)> {
    images
        .iter()
        .filter_map(|(seq, pack)| {
            let (into, shift) = moved.get(&pack.segment.id)?;
            let mut moved = pack.clone();
            moved.segment.id = *into;
            moved.segment.base_offset = pack.segment.base_offset.saturating_add(*shift);
            Some((*seq, moved))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn listed(sizes: &[(u128, u64)]) -> Vec<(Ulid, u64)> {
        sizes
            .iter()
            .map(|(id, bytes)| (Ulid(*id), *bytes))
            .collect()
    }

    fn all(listed: &[(Ulid, u64)]) -> HashSet<Ulid> {
        listed.iter().map(|(id, _)| *id).collect()
    }

    const POLICY: CoalescePolicy = CoalescePolicy {
        small_bytes: 100,
        max_output_bytes: 250,
        max_inputs: 8,
    };

    #[test]
    fn a_large_segment_is_left_where_it_is() {
        let listed = listed(&[(1, 10), (2, 500), (3, 20)]);
        let planned = plan(&listed, &all(&listed), POLICY);

        assert_eq!(
            planned.iter().map(|s| s.id.0).collect::<Vec<_>>(),
            vec![1, 3],
            "a segment already worth its own GET was copied for nothing"
        );
    }

    #[test]
    fn offsets_follow_the_order_the_bytes_are_written_in() {
        let listed = listed(&[(1, 10), (2, 20), (3, 30)]);
        let planned = plan(&listed, &all(&listed), POLICY);

        assert_eq!(
            planned.iter().map(|s| s.shift).collect::<Vec<_>>(),
            vec![0, 10, 30],
            "an image would be read from the wrong offset"
        );
    }

    #[test]
    fn a_run_stops_at_the_output_size() {
        let listed = listed(&[(1, 90), (2, 90), (3, 90), (4, 90)]);
        let planned = plan(&listed, &all(&listed), POLICY);

        assert_eq!(planned.len(), 2, "one merge copied more than it may");
    }

    /// An unreferenced object is a failed push's, and taking it into a copy
    /// would make it live again.
    #[test]
    fn an_unreferenced_segment_is_not_copied() {
        let listed = listed(&[(1, 10), (2, 10)]);
        let live: HashSet<Ulid> = [Ulid(1)].into_iter().collect();

        let planned = plan(&listed, &live, POLICY);
        assert_eq!(planned.iter().map(|s| s.id.0).collect::<Vec<_>>(), vec![1]);
    }
}
