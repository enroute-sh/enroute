//! The bytes: locate an object's entry, then rebuild it.
//!
//! A stored entry may be a delta against another object rather than the
//! object itself, so reading one means rebuilding it from a chain of bases —
//! one metadata walk locates the whole chain, then every entry is fetched
//! concurrently: two round trips whatever the depth.

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{CommitPackLocation, Error, ObjectHashMap, ObjectMeta, Ulid, decode_loose};
use enroute_git_metadata::RepoMetadata;

use enroute_git_store::{
    PackEntryHeader, bounded, decode_commit_pack_object, decode_pack_entry_header,
};

use crate::Storage;

/// Longest delta chain a read will follow before calling the store corrupt.
///
/// Writers cut at 32 deepest; the headroom catches a cycle or a bad pointer.
const MAX_DELTA_CHAIN: u32 = 64;

/// Bytes worth reading past, and discarding, to keep two entries in one GET.
///
/// Set where skipping and re-requesting cost the same — about 29 KB for a
/// gigabit link at [`RUN_FETCH_CONCURRENCY`], which every consumer must match.
pub const RUN_GAP_BYTES: u64 = 32 * 1024;

/// Bytes one coalesced GET may span.
///
/// A cap on one run, not a batch: [`known`] holds all its runs at once, and
/// this cap also bounds bytes in flight on the streaming path.
const RUN_SPAN_BYTES: u64 = 1024 * 1024;

/// Runs read at once.
///
/// Matches `upload_pack`'s `FETCH_CONCURRENCY`, since [`RUN_GAP_BYTES`] is
/// derived against it.
const RUN_FETCH_CONCURRENCY: usize = 64;

/// Chains resolved at once — a burst cap, not a width.
///
/// These are Postgres round trips and the pool meters them.
const CHAIN_RESOLVE_CONCURRENCY: usize = 16;

/// One entry's placement: the segment holding it and its span within.
type Placement = (Ulid, u64, u64);

fn placement(loc: CommitPackLocation) -> Placement {
    (loc.segment.id, loc.segment_offset(), loc.image.entry_len)
}

/// Group items that sit close together into the requests that will read them,
/// as index ranges over `items`.
///
/// `where_it_sits` gives each item its segment, offset and length; `items`
/// must already be in that order, and every run holds at least one item.
#[must_use]
pub fn runs_of<T>(
    items: &[T],
    where_it_sits: impl Fn(&T) -> (Ulid, u64, u64),
) -> Vec<std::ops::Range<usize>> {
    let mut runs: Vec<std::ops::Range<usize>> = Vec::new();
    // The open run's segment, where it starts, and the offset just past the
    // last item admitted — what the next candidate's gap is measured from.
    let mut open: Option<(Ulid, u64, u64)> = None;
    for (at, item) in items.iter().enumerate() {
        let (segment, offset, len) = where_it_sits(item);
        let end = offset.saturating_add(len);
        match open {
            // Splitting a run costs a round trip, so items merge until the
            // gap outweighs one. `offset >= run_end` bars an overlap: a run
            // is read forward once and cannot rewind to the second's start.
            Some((run_segment, start, run_end))
                if run_segment == segment
                    && offset >= run_end
                    && offset - run_end <= RUN_GAP_BYTES
                    && end.saturating_sub(start) <= RUN_SPAN_BYTES =>
            {
                open = Some((segment, start, end));
                if let Some(run) = runs.last_mut() {
                    run.end = at + 1;
                }
            }
            _ => {
                open = Some((segment, offset, end));
                runs.push(at..at + 1);
            }
        }
    }
    runs
}

/// A single request covering one or more entries that sit close together.
struct Run {
    segment: Ulid,
    offset: u64,
    len: u64,
    /// Which of the sorted placements this run covers.
    covers: std::ops::Range<usize>,
}

/// The requests that will read `sorted`, each with the span it must GET.
fn plan_runs(sorted: &[Placement]) -> Vec<Run> {
    runs_of(sorted, |&(segment, offset, len)| (segment, offset, len))
        .into_iter()
        .filter_map(|covers| {
            let &(segment, offset, _) = sorted.get(covers.start)?;
            let &(_, last_offset, last_len) = sorted.get(covers.end - 1)?;
            Some(Run {
                segment,
                offset,
                len: last_offset.saturating_add(last_len).saturating_sub(offset),
                covers,
            })
        })
        .collect()
}

/// Every entry's own bytes, keyed by where it sits.
type Entries = std::collections::HashMap<(Ulid, u64), Bytes>;

/// Read every placement, merging near-adjacent ones into shared requests.
async fn read_entries(
    storage: &Storage,
    repo: &RepoMetadata,
    mut sorted: Vec<Placement>,
) -> Result<Entries, Error> {
    sorted.sort_unstable();
    sorted.dedup();
    let runs = plan_runs(&sorted);
    let sorted = &sorted;
    let read: Vec<Entries> = bounded(
        RUN_FETCH_CONCURRENCY,
        runs.into_iter().map(|run| async move {
            let bytes = storage
                .store
                .get_segment_slice(repo, run.segment, run.offset, Some(run.len))
                .await
                .map_err(Error::from)?
                .ok_or_else(|| anyhow::anyhow!("segment {} not found in store", run.segment))?;
            let mut out = Entries::new();
            for &(segment, offset, len) in sorted.get(run.covers).unwrap_or_default() {
                let at = usize::try_from(offset - run.offset).unwrap_or(usize::MAX);
                let to = at.saturating_add(usize::try_from(len).unwrap_or(usize::MAX));
                let slice = bytes
                    .get(at..to)
                    .ok_or_else(|| anyhow::anyhow!("run for segment {segment} was read short"))?;
                out.insert((segment, offset), bytes.slice_ref(slice));
            }
            Ok::<_, Error>(out)
        }),
    )
    .await?;
    Ok(read.into_iter().flatten().collect())
}

/// Split one already-read entry into its inline header and compressed body.
fn split_entry(oid: ObjectId, entry: &Bytes) -> Result<(PackEntryHeader, Bytes), Error> {
    let (header, consumed) = decode_pack_entry_header(entry)
        .map_err(|e| anyhow::anyhow!("decode pack entry header for {oid}: {e}"))?;
    Ok((header, entry.slice(consumed..)))
}

/// Rebuild one object from entries already in hand.
fn rebuild(oid: ObjectId, chain: &[CommitPackLocation], entries: &Entries) -> Result<Bytes, Error> {
    let mut hops = Vec::with_capacity(chain.len());
    for &loc in chain {
        let key = (loc.segment.id, loc.segment_offset());
        let entry = entries
            .get(&key)
            .ok_or_else(|| anyhow::anyhow!("entry for {oid} was not read"))?;
        hops.push(split_entry(oid, entry)?);
    }
    // Innermost first: the last hop is whole, and each delta above rebuilds
    // the one before it.
    let mut hops = hops.into_iter().rev();
    let Some((header, body)) = hops.next() else {
        return Err(anyhow::anyhow!("delta chain for {oid} is empty").into());
    };
    // A walk that stopped early leaves a delta innermost, and decoding that as
    // whole yields the delta stream under `oid`'s name — wrong bytes, no error.
    if header.base.is_some() {
        return Err(anyhow::anyhow!("delta chain for {oid} has no base").into());
    }
    let mut content = decode_commit_pack_object(&body, header.length)
        .map_err(|e| anyhow::anyhow!("decode delta base for {oid}: {e}"))?;
    for (header, body) in hops {
        let delta = decode_commit_pack_object(&body, header.length)
            .map_err(|e| anyhow::anyhow!("decode delta for {oid}: {e}"))?;
        content = enroute_git_packfile::apply_delta(&content, &delta)
            .map_err(|e| anyhow::anyhow!("apply delta for {oid}: {e}"))?
            .into();
    }
    Ok(content)
}

/// Reconstruct many located objects, reading entries that sit near each other
/// through one request.
///
/// Objects are named by location rather than [`ObjectMeta`], so a tag — which
/// has none, by design — can't be asked for.
///
/// # Errors
/// Returns an error if a chain walk or read fails, if a chain exceeds
/// [`MAX_DELTA_CHAIN`], or if an entry fails to decode.
pub async fn known(
    storage: &Storage,
    repo: &RepoMetadata,
    wanted: &[(ObjectId, CommitPackLocation)],
) -> Result<ObjectHashMap<Bytes>, Error> {
    // A whole entry needs no walk at all — only a delta pays for the chain
    // query, and those queue on Postgres rather than the store.
    let chains: Vec<(ObjectId, Vec<CommitPackLocation>)> = bounded(
        CHAIN_RESOLVE_CONCURRENCY,
        wanted.iter().map(|&(oid, loc)| async move {
            if loc.image.base.is_none() {
                return Ok((oid, vec![loc]));
            }
            let chain = storage
                .objects
                .repo(repo.id)
                .chain(oid, MAX_DELTA_CHAIN)
                .await
                .map_err(Error::from)?;
            if chain.len() > usize::try_from(MAX_DELTA_CHAIN).unwrap_or(usize::MAX) {
                return Err(Error::from(anyhow::anyhow!(
                    "delta chain for {oid} exceeds {MAX_DELTA_CHAIN} hops"
                )));
            }
            // A walk starting anywhere else is some other object's, and would
            // rebuild those bytes under this oid. By placement, not whole:
            // `resolve_chain` leaves each hop's `base` unset.
            if chain.first().map(|first| placement(*first)) != Some(placement(loc)) {
                return Err(Error::from(anyhow::anyhow!(
                    "delta chain for {oid} does not start at its own entry"
                )));
            }
            Ok((oid, chain))
        }),
    )
    .await?;

    let entries = read_entries(
        storage,
        repo,
        chains
            .iter()
            .flat_map(|(_, chain)| chain.iter().copied().map(placement))
            .collect(),
    )
    .await?;

    chains
        .iter()
        .map(|(oid, chain)| Ok((*oid, rebuild(*oid, chain, &entries)?)))
        .collect()
}

/// Fetch an annotated tag's loose bytes.
async fn fetch_tag(storage: &Storage, repo: &RepoMetadata, sha: &str) -> Result<Bytes, Error> {
    storage
        .store
        .get_tag(repo, sha)
        .await
        .map_err(|e| Error::from(anyhow::anyhow!(e)))?
        .ok_or_else(|| Error::from(anyhow::anyhow!("tag {sha} not found in store")))
}

/// Reconstruct an object whose index metadata is already in hand, following
/// its delta chain if it has one.
///
/// A batch of one, so a second walk implementation can't disagree with the
/// first about what a corrupt chain looks like.
///
/// # Errors
/// Returns an error if the object or one of its bases is missing from the
/// store, a read fails, an entry's header or body fails to decode, or the
/// chain exceeds [`MAX_DELTA_CHAIN`].
async fn one_known(
    storage: &Storage,
    repo: &RepoMetadata,
    oid: ObjectId,
    meta: &ObjectMeta,
) -> Result<(Kind, Bytes), Error> {
    if meta.kind == Kind::Tag {
        let raw = fetch_tag(storage, repo, &oid.to_hex().to_string()).await?;
        return decode_loose(&raw);
    }
    // Numbered and never placed reads the same as never pushed: both mean
    // this repository cannot produce the object.
    let loc = meta.location.ok_or(Error::Missing(oid))?;
    let content = known(storage, repo, &[(oid, loc)])
        .await?
        .remove(&oid)
        .ok_or_else(|| anyhow::anyhow!("object {oid} was not read"))?;
    Ok((meta.kind, content))
}

/// The plural of [`object`], which is what lets chains overlap and
/// neighbouring entries coalesce.
///
/// An oid the index doesn't have is omitted.
///
/// # Errors
/// Returns an error if a lookup or read fails, or if an object the index
/// placed comes back unread.
pub async fn objects(
    storage: &Storage,
    repo: &RepoMetadata,
    oids: &[ObjectId],
) -> Result<ObjectHashMap<(Kind, Bytes)>, Error> {
    let metas = crate::metas(storage, repo.id, oids)
        .await
        .map_err(Error::from)?;
    let mut wanted: Vec<(ObjectId, CommitPackLocation)> = Vec::with_capacity(metas.len());
    let mut singular: Vec<(ObjectId, ObjectMeta)> = Vec::new();
    for (&oid, meta) in &metas {
        // Numbered and never placed is omitted, which is what an oid the
        // index does not hold has always done here.
        if !meta.is_stored() {
            continue;
        }
        // A tag has no location by design, and reads by the loose path.
        match meta.location.filter(|_| meta.kind != Kind::Tag) {
            Some(loc) => wanted.push((oid, loc)),
            None => singular.push((oid, meta.clone())),
        }
    }

    let contents = known(storage, repo, &wanted).await?;
    if contents.len() != wanted.len() {
        return Err(anyhow::anyhow!(
            "{} of {} placed objects were not read",
            wanted.len() - contents.len(),
            wanted.len()
        )
        .into());
    }

    let mut found = enroute_git_core::object_hash_map_with_capacity(metas.len());
    for (&oid, meta) in &metas {
        if let Some(content) = contents.get(&oid) {
            found.insert(oid, (meta.kind, content.clone()));
        }
    }
    for (oid, meta) in singular {
        found.insert(oid, one_known(storage, repo, oid, &meta).await?);
    }
    Ok(found)
}

/// Fetch and reconstruct an object by OID, returning its kind and content.
///
/// # Errors
/// Returns an error if the object is not found in the index, or for any
/// reason reading and reconstructing it fails.
pub async fn object(
    storage: &Storage,
    repo: &RepoMetadata,
    oid: ObjectId,
) -> Result<(Kind, Bytes), Error> {
    let meta = crate::meta(storage, repo.id, oid)
        .await
        .map_err(Error::from)?
        .ok_or(Error::Missing(oid))?;
    one_known(storage, repo, oid, &meta).await
}

#[cfg(test)]
mod tests {
    use super::{Placement, RUN_GAP_BYTES, RUN_SPAN_BYTES, plan_runs};
    use enroute_git_core::Ulid;

    fn segment(n: u128) -> Ulid {
        Ulid(n)
    }

    /// Offsets each run covers, so a case reads as the grouping it asserts.
    fn grouped(spans: &[Placement]) -> Vec<Vec<u64>> {
        plan_runs(spans)
            .into_iter()
            .map(|run| {
                spans
                    .get(run.covers)
                    .unwrap_or_default()
                    .iter()
                    .map(|&(_, offset, _)| offset)
                    .collect()
            })
            .collect()
    }

    #[test]
    fn adjacent_entries_share_one_request() {
        let spans = vec![(segment(1), 0, 100), (segment(1), 100, 50)];
        assert_eq!(grouped(&spans), vec![vec![0, 100]]);
        assert_eq!(plan_runs(&spans).first().map(|r| r.len), Some(150));
    }

    #[test]
    fn a_different_segment_always_splits() {
        let spans = vec![(segment(1), 0, 100), (segment(2), 100, 50)];
        assert_eq!(grouped(&spans), vec![vec![0], vec![100]]);
    }

    /// A gap up to the tolerance is cheaper to read past than to pay a round
    /// trip for; one byte more is not.
    #[test]
    fn gaps_merge_up_to_the_break_even() {
        let within = vec![(segment(1), 0, 100), (segment(1), 100 + RUN_GAP_BYTES, 50)];
        assert_eq!(grouped(&within), vec![vec![0, 100 + RUN_GAP_BYTES]]);

        let beyond = vec![(segment(1), 0, 100), (segment(1), 101 + RUN_GAP_BYTES, 50)];
        assert_eq!(grouped(&beyond), vec![vec![0], vec![101 + RUN_GAP_BYTES]]);
    }

    /// The span cap bounds bytes in flight even across perfectly adjacent
    /// entries.
    #[test]
    fn the_span_cap_bounds_a_run() {
        let half = RUN_SPAN_BYTES / 2;
        let spans = vec![
            (segment(1), 0, half),
            (segment(1), half, half),
            (segment(1), RUN_SPAN_BYTES, 1),
        ];
        assert_eq!(grouped(&spans), vec![vec![0, half], vec![RUN_SPAN_BYTES]]);
    }

    #[test]
    fn no_spans_plan_no_requests() {
        assert!(plan_runs(&[]).is_empty());
    }

    /// An entry nested inside the open run gets a request of its own.
    ///
    /// A run is read forward once, so it could never rewind to the nested
    /// entry's start; entries in one segment never really overlap.
    #[test]
    fn a_nested_entry_reads_on_its_own() {
        let spans = vec![(segment(1), 100, 500), (segment(1), 200, 50)];
        let runs = plan_runs(&spans);
        assert_eq!(
            runs.iter().map(|r| (r.offset, r.len)).collect::<Vec<_>>(),
            vec![(100, 500), (200, 50)]
        );
    }
}
