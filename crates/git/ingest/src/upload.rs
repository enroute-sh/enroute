//! Promotes a push's objects to the primary store as commit-pack images
//! appended into segment objects, returning the metadata to record.
//!
//! Images are appended back-to-back into segments cut at
//! [`SEGMENT_TARGET_BYTES`], so a bulk push costs O(bytes / target) S3 PUTs.
//! Promotion returns the metadata rather than writing it, so a crash leaves
//! at worst unreferenced segments, never a graph pointing at unindexed
//! objects.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _};
use gix_hash::ObjectId;
use gix_object::Kind;
use tokio::sync::Semaphore;
use tracing::Instrument as _;

use enroute_git_core::{
    Error, NewCommit, NewObject, ObjectHashMap, ObjectHashSet, PackImageLocation, SegmentLocation,
    Ulid, encode_loose, topo_order,
};
use enroute_git_cost::{count, millis};
use enroute_git_graph::ObjectRefs;
use enroute_git_retrieve::{RepoMetadata, Storage};
use enroute_git_store::{
    MAX_PACK_ENTRY_HEADER_BYTES, ObjectWriter, PackTrailerEntry, Store, blob_section_offset,
    decode_commit_pack_object, decode_pack_entry_header, encode_commit_pack_header,
    encode_pack_entry_header, encode_pack_trailer, has_pack_entry_header,
};

/// Byte size at which the pump cuts to the next segment — a kernel-scale
/// push produces hundreds of segments, not millions of objects.
const SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// How many segments upload at once — the plan fixes each one's layout
/// before any I/O, so they need nothing from each other.
const MAX_CONCURRENT_SEGMENTS: usize = 8;

/// At or above this, an entry streams through the pump chunk-by-chunk
/// instead of being prefetched whole, so no blob is ever fully resident.
const STREAM_THRESHOLD_BYTES: u64 = 1024 * 1024;

/// How many sub-threshold entry payloads a segment prefetches ahead of its
/// own writes — caps resident bytes while overlapping per-entry GET latency.
const ENTRY_PREFETCH: usize = 8;

/// How many images' payloads are queued at a time — well above
/// [`ENTRY_PREFETCH`], keeping queue memory independent of push size.
const PREFETCH_WINDOW_IMAGES: usize = 256;

use crate::concurrency::try_join_bounded;
use crate::delta_plan::{DeltaEntry, DeltaPlan, EntrySource, commit_parents};
use crate::object_io::{
    AttributionCounts, KnownIdentities, ObjectIoCtx, counted, prefetch_identities,
};
use crate::pack::{WirePack, WireRef};
use crate::progress::{IngestProgress, ProgressSink};
use crate::staging::StagingSession;
use crate::timing::{as_u64, record_ms, timed_async};

/// Where an object's content lives within a push's staging store.
///
/// Kind and decompressed length ride along: staged content has no loose
/// header to recover them from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StagedObjectLocation {
    /// Which staging chunk the object's bytes live in.
    pub chunk_id: u32,
    /// Byte offset of the object's compressed content within the chunk.
    pub offset: usize,
    /// Byte length of the object's compressed content.
    pub compressed_len: usize,
    /// Byte length of the object's decompressed content.
    pub decompressed_len: usize,
    /// The object's git type.
    pub kind: Kind,
}

/// One object on its way into staging: what it is, and what its bytes encode
/// against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StagedItem {
    /// The object the bytes reconstruct.
    pub sha: ObjectId,
    /// The object's git type.
    pub kind: Kind,
    /// The version the bytes delta against, or `None` when they are the
    /// object itself.
    pub base: Option<ObjectId>,
    /// Byte length of what the bytes decompress to — the delta stream, for a
    /// delta.
    pub decompressed_len: usize,
}

/// Every encoding of one object that staging holds.
///
/// More than one is normal: a version live across several lattice hops is
/// materialised against each base.
#[derive(Debug, Default, Clone)]
pub(crate) struct StagedEncodings(Vec<(Option<ObjectId>, StagedObjectLocation)>);

impl StagedEncodings {
    /// Record `loc` as this object's encoding against `base`.
    pub(crate) fn insert(&mut self, base: Option<ObjectId>, loc: StagedObjectLocation) {
        match self.0.iter_mut().find(|(b, _)| *b == base) {
            Some(slot) => slot.1 = loc,
            None => self.0.push((base, loc)),
        }
    }

    /// The encoding against `base`, if staging holds it.
    pub(crate) fn against(&self, base: Option<ObjectId>) -> Option<&StagedObjectLocation> {
        self.0.iter().find(|(b, _)| *b == base).map(|(_, loc)| loc)
    }

    /// The object's own bytes, not a particular pack's framing of it.
    ///
    /// Absent for an object this push only ever encoded as a delta.
    pub(crate) fn whole(&self) -> Option<&StagedObjectLocation> {
        self.against(None)
    }
}

/// Staged pack data produced by the streaming pack pass, ready for promotion.
///
/// No `#[derive(Debug)]`: `progress` is a `dyn Fn` reference, which doesn't
/// implement `Debug`.
pub(crate) struct StagedPack<'a> {
    /// Outgoing references for every pack object, keyed by OID.
    pub object_refs: &'a ObjectHashMap<ObjectRefs>,
    /// Commits that passed the connectivity check and should be promoted.
    pub connected_commits: &'a ObjectHashSet,
    /// `oid → location` in the staging store.
    pub chunk_index: &'a ObjectHashMap<StagedEncodings>,
    /// What each commit's pack stores and what each entry deltas against.
    pub plan: &'a DeltaPlan,
    /// The packs this push arrived in — where every entry the client sent in
    /// a form worth keeping is copied from.
    pub wire: &'a [Arc<WirePack>],
    /// This push's registered staging session, proving the reads below are
    /// authorized — shared so the pump's segment tasks can each hold one.
    pub staging_session: Arc<StagingSession>,
    /// Reports `IngestProgress::UpdatingRepository` as commit packs finish
    /// uploading — pass [`crate::noop_progress`] if the caller doesn't care.
    pub progress: ProgressSink<'a>,
}

/// Everything a push's promotion yields to record in the metadata store, for
/// the caller to fold into one atomic `append`.
#[derive(Debug, Default)]
pub(crate) struct PromotedObjects {
    /// Commits to record, keyed by oid — root tree, parents, and pack
    /// location for each commit connected to a real ref update.
    pub new_commits: ObjectHashMap<NewCommit>,
    /// Non-commit objects (trees, blobs, tags) and their pack locations.
    pub new_objects: Vec<NewObject>,
    /// The identities attribution already read, so `append` needn't read
    /// the same oids a second time.
    pub known_seqs: KnownIdentities,
}

/// Build the `NewCommit` map for
/// [`enroute_git_retrieve::MetadataStore::append`].
///
/// # Errors
/// A connected commit was never promoted (a bug).
fn build_new_commits(
    pack: &StagedPack<'_>,
    promoted: &ObjectHashMap<PromotedCommit>,
) -> Result<ObjectHashMap<NewCommit>, Error> {
    let (object_refs, connected_commits) = (pack.object_refs, pack.connected_commits);
    let candidates: Vec<(ObjectId, ObjectId, &Vec<ObjectId>, i64)> = object_refs
        .iter()
        .filter_map(|(&oid, refs)| {
            if !connected_commits.contains(&oid) {
                return None;
            }
            let ObjectRefs::Commit {
                root_tree,
                parents,
                committer_date,
            } = refs
            else {
                return None;
            };
            Some((oid, *root_tree, parents, *committer_date))
        })
        .collect();

    let mut new_commits = ObjectHashMap::default();
    for (oid, root_tree, parents, committer_date) in candidates {
        let Some(&PromotedCommit {
            entry_len,
            blob_offset,
            segment,
        }) = promoted.get(&oid)
        else {
            return Err(anyhow::anyhow!(
                "connected commit {oid} missing its location from promotion"
            )
            .into());
        };
        new_commits.insert(
            oid,
            NewCommit {
                root_tree,
                parents: parents.clone(),
                entry_len,
                blob_offset,
                committer_date,
                segment,
            },
        );
    }
    Ok(new_commits)
}

/// Promote this push's objects to primary S3 and return the object metadata
/// to record.
///
/// Commits and attributed trees/blobs become commit packs; tags go to the tag
/// path. The returned [`PromotedObjects`] is recorded by the caller.
///
/// # Errors
/// Returns an error if staging reads or primary writes fail.
#[tracing::instrument(name = "enroute_git_ingest::upload::upload_staged_ordered", skip(pack, state, repo), fields(repo_id = %repo.id, commit_count = count(pack.connected_commits.len())))]
pub(crate) async fn upload_staged_ordered(
    pack: &StagedPack<'_>,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<PromotedObjects, Error> {
    let built = build_packs(pack, state, repo).await?;

    try_join_bounded(
        built
            .tag_writes
            .iter()
            .map(|(sha, bytes)| state.store.put_tag(repo, sha, bytes.clone())),
    )
    .await
    .map_err(|e| anyhow::anyhow!("put tag: {e:#}"))?;

    let new_commits = build_new_commits(pack, &built.promoted)?;

    Ok(PromotedObjects {
        new_commits,
        new_objects: built.new_objects,
        known_seqs: built.known_seqs,
    })
}

/// Where one commit's pack image ended up — everything `build_new_commits`
/// needs beyond the commit's own graph edges.
#[derive(Debug, Clone, Copy)]
struct PromotedCommit {
    /// Inline header + compressed body of the commit's own entry.
    entry_len: u64,
    /// Blob-section start offset within the image (see [`blob_section_offset`]).
    blob_offset: u64,
    /// The segment object the image was appended to.
    segment: SegmentLocation,
}

struct BuiltPacks {
    promoted: ObjectHashMap<PromotedCommit>,
    tag_writes: Vec<(String, Bytes)>,
    /// Non-commit objects (trees, blobs, tags) to record in the object index.
    new_objects: Vec<NewObject>,
    /// See [`PromotedObjects::known_seqs`].
    known_seqs: KnownIdentities,
}

async fn build_packs(
    pack: &StagedPack<'_>,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<BuiltPacks, Error> {
    let ordered_commits = topo_sort_commits(pack.connected_commits, pack.object_refs)?;
    // One read up front for every seq the walk (and, later, `append`) could
    // ask for — see `prefetch_identities`.
    let counts = AttributionCounts::default();
    let seqs = prefetch_identities(pack.object_refs, state, repo, &counts).await?;
    let io = ObjectIoCtx {
        chunk_index: pack.chunk_index,
        staging_session: pack.staging_session.clone(),
        state,
        repo,
        counts,
    };

    let CommitPackBuilt {
        promoted,
        mut new_objects,
    } = build_commit_packs(&ordered_commits, pack, &io).await?;
    let (tag_writes, tag_objects) = collect_tag_writes(pack, repo).await?;
    new_objects.extend(tag_objects);

    Ok(BuiltPacks {
        promoted,
        tag_writes,
        new_objects,
        known_seqs: seqs,
    })
}

struct CommitPackBuilt {
    promoted: ObjectHashMap<PromotedCommit>,
    /// Trees and blobs to record, deduped and merged across every referencing
    /// pack (see [`record_object_entries_absolute`]).
    new_objects: Vec<NewObject>,
}

/// Build one commit-pack image per entry in `ordered_commits` and pump them,
/// in order, into segment objects cut at [`SEGMENT_TARGET_BYTES`].
///
/// Three passes: diff each tree, [`plan_images`] fixes layout before any
/// upload byte moves, then [`pump_images`] streams the bytes.
#[tracing::instrument(
    name = "enroute_git_ingest::upload::build_commit_packs",
    skip_all,
    fields(
        commits = count(ordered_commits.len()),
        // Promotion's metadata round trips, summed.
        lookup_ms = tracing::field::Empty,
        lookups = tracing::field::Empty,
        plan_ms = tracing::field::Empty,
        fold_ms = tracing::field::Empty,
    )
)]
async fn build_commit_packs(
    ordered_commits: &[ObjectId],
    pack: &StagedPack<'_>,
    io: &ObjectIoCtx<'_>,
) -> Result<CommitPackBuilt, Error> {
    let span = tracing::Span::current();
    let per_commit: Vec<PlannedCommit<'_>> = ordered_commits
        .iter()
        .map(|&commit_oid| PlannedCommit {
            commit_oid,
            // A commit whose bytes this push never received is one an earlier
            // one stored; its own entry comes back out of staging either way.
            source: pack
                .plan
                .commits
                .get(&commit_oid)
                .copied()
                .unwrap_or(EntrySource::Encoded),
            entries: pack
                .plan
                .packs
                .get(&commit_oid)
                .map_or(&[][..], Vec::as_slice),
        })
        .collect();

    // Each pass records as soon as its number is final, so a failure in a
    // later one still reports what the earlier ones cost.
    let at = Instant::now();
    let prefetched = prefetch_preexisting_meta(&per_commit, io).await?;

    let plan = plan_images(
        &per_commit,
        &Sources {
            wire: pack.wire,
            chunk_index: io.chunk_index,
            prefetched: &prefetched,
        },
        SEGMENT_TARGET_BYTES,
    )?;
    span.record("plan_ms", millis(at.elapsed()));
    span.record("lookup_ms", millis(io.counts.elapsed()));
    span.record("lookups", count(io.counts.calls()));

    // The pump yields trailer entries, which carry no base — capture what the
    // layout chose before the plan is consumed.
    let bases: ObjectHashMap<ObjectHashMap<Option<ObjectId>>> = plan
        .iter()
        .map(|image| {
            (
                image.commit_oid,
                image.entries.iter().map(|e| (e.oid, e.base)).collect(),
            )
        })
        .collect();

    let images = pump_images(plan, pack, io).await?;

    let at = Instant::now();
    let mut promoted: ObjectHashMap<PromotedCommit> = ObjectHashMap::default();
    let mut new_objects: ObjectHashMap<NewObject> = ObjectHashMap::default();

    for (commit_oid, entries, segment) in images {
        let blob_offset = blob_section_offset(&entries);

        // Commit is always the first entry (`plan_images` preserves the
        // commit → trees → blobs order).
        let mut entries = entries.into_iter();
        let commit_entry = entries
            .next()
            .ok_or_else(|| anyhow::anyhow!("commit pack {commit_oid} has no entries"))?;
        debug_assert_eq!(
            commit_entry.sha, commit_oid,
            "commit pack's first entry must be the commit itself"
        );
        debug_assert_eq!(
            commit_entry.offset,
            enroute_git_core::COMMIT_PACK_HEADER_SIZE,
            "commit entry must start the data section"
        );
        promoted.insert(
            commit_oid,
            PromotedCommit {
                entry_len: commit_entry.entry_len,
                blob_offset,
                segment,
            },
        );

        record_object_entries_absolute(
            &mut new_objects,
            commit_oid,
            entries,
            bases.get(&commit_oid),
            pack.object_refs,
        );
    }

    let new_objects = new_objects.into_values().collect();
    span.record("fold_ms", millis(at.elapsed()));
    Ok(CommitPackBuilt {
        promoted,
        new_objects,
    })
}

// ── layout plan ───────────────────────────────────────────────────────────────

/// Where one planned entry's bytes come from at pump time.
enum PlannedSource {
    /// Kept from the pack the client sent — a slice of the resident pack,
    /// so this costs no read and no compression.
    Wire {
        header: Bytes,
        body: Bytes,
        /// Decompressed length, for the trailer.
        length: u64,
    },
    /// Staged by this push: a precomputed inline header, then the compressed
    /// body from a staging chunk.
    Staged {
        header: Bytes,
        chunk_id: u32,
        body: std::ops::Range<usize>,
        /// Decompressed length, for the trailer.
        length: u64,
    },
    /// Pre-existing: the whole entry forwarded verbatim from its segment;
    /// the decompressed length is decoded from the bytes in flight.
    Indexed { segment: Ulid, start: u64 },
}

/// One entry of a planned image — byte-exact before any upload I/O.
struct PlannedEntry {
    oid: ObjectId,
    kind: Kind,
    /// The encoding this entry actually got, recorded so the index can name
    /// it without re-reading the written bytes.
    base: Option<ObjectId>,
    /// Intra-image offset of the entry's `header_length` byte.
    offset: u64,
    /// Header + compressed body span.
    entry_len: u64,
    source: PlannedSource,
}

/// One commit-pack image with its assigned position in the push's segment
/// sequence.
///
/// The segment's ULID isn't chosen here: it's minted when the pump opens
/// the writer, so the janitor's grace window measures from bytes landing.
struct PlannedImage {
    commit_oid: ObjectId,
    /// Byte offset within its segment.
    ///
    /// Zero exactly at a cut, which is how the pump knows to start the
    /// next segment object.
    base_offset: u64,
    image_len: u64,
    /// `entries.len()`, in the pack header's own width — the fallible
    /// narrowing happens once, here, rather than again in the pump.
    entry_count: u32,
    entries: Vec<PlannedEntry>,
}

/// One commit's planned image, before any of its byte positions are known.
struct PlannedCommit<'a> {
    commit_oid: ObjectId,
    /// Where the commit's own entry comes from.
    source: EntrySource,
    /// Its pack's other entries, already in the order they are written.
    entries: &'a [DeltaEntry],
}

/// Compute the byte-exact layout of every image and segment.
///
/// Pure, no I/O: staged sizes come from the chunk index and pre-existing
/// ones from the object index, so offsets and cuts follow arithmetically.
fn plan_images(
    per_commit: &[PlannedCommit<'_>],
    index: &Sources<'_>,
    target: u64,
) -> Result<Vec<PlannedImage>, Error> {
    let mut images = Vec::with_capacity(per_commit.len());
    let mut segment_size: u64 = 0;

    for PlannedCommit {
        commit_oid,
        source,
        entries: planned,
    } in per_commit
    {
        // The commit itself always leads and is always whole: it is what
        // names the base every other entry is measured against.
        let oids = std::iter::once(DeltaEntry {
            oid: *commit_oid,
            kind: Kind::Commit,
            base: None,
            source: *source,
        })
        .chain(planned.iter().copied());

        let mut entries: Vec<PlannedEntry> = Vec::with_capacity(1 + planned.len());
        let mut offset = enroute_git_core::COMMIT_PACK_HEADER_SIZE;
        for planned in oids {
            let entry = plan_entry(planned, offset, index)?;
            offset = offset
                .checked_add(entry.entry_len)
                .ok_or_else(|| anyhow::anyhow!("pack offset overflow"))?;
            entries.push(entry);
        }
        let entry_count = u32::try_from(entries.len())
            .map_err(|e| anyhow::anyhow!("commit pack object count overflow: {e}"))?;
        let image_len = offset + enroute_git_store::trailer_suffix_len(entry_count);

        images.push(PlannedImage {
            commit_oid: *commit_oid,
            base_offset: segment_size,
            image_len,
            entry_count,
            entries,
        });
        segment_size += image_len;
        if segment_size >= target {
            segment_size = 0;
        }
    }
    Ok(images)
}

/// The three places an entry's bytes can come from.
///
/// In the order [`plan_entry`] asks: the pack the client sent, this push's
/// staging, or the object index.
struct Sources<'a> {
    wire: &'a [Arc<WirePack>],
    chunk_index: &'a ObjectHashMap<StagedEncodings>,
    prefetched: &'a ObjectHashMap<enroute_git_core::ObjectMeta>,
}

/// Plan an entry kept from the client's own pack: our header, its bytes.
///
/// A delta stream carries no reference to its base; only the entry header
/// does. Re-pointing at our base is a header rewrite, body untouched.
fn plan_wire_entry(
    oid: ObjectId,
    kind: Kind,
    base: Option<ObjectId>,
    offset: u64,
    at: WireRef,
    wire: &[Arc<WirePack>],
) -> Result<PlannedEntry, Error> {
    let pack = wire
        .get(at.pack)
        .ok_or_else(|| anyhow::anyhow!("entry for {oid} names a pack this push never took"))?;
    let body = pack.body(at.at)?;
    // For a delta this is the instruction stream's length, not the object's —
    // the same convention the wire uses, so the scan's figure carries over.
    let length = pack.entry(at.at)?.decompressed_size;
    let compressed_len =
        u64::try_from(body.len()).map_err(|e| anyhow::anyhow!("compressed len: {e}"))?;
    planned_entry(
        Encoded {
            oid,
            kind,
            base,
            offset,
        },
        length,
        compressed_len,
        |header| PlannedSource::Wire {
            header,
            body,
            length,
        },
    )
}

/// What an entry whose header this pass encodes is, and where it lands.
///
/// The four fields [`planned_entry`] copies straight through, kept together
/// so its two callers need no argument list of their own.
#[derive(Clone, Copy)]
struct Encoded {
    oid: ObjectId,
    kind: Kind,
    base: Option<ObjectId>,
    offset: u64,
}

/// Encode `entry`'s inline header and measure the whole entry around it.
///
/// `source` is handed that header, since the two forms that need one differ
/// only in where the body comes from.
fn planned_entry(
    entry: Encoded,
    length: u64,
    compressed_len: u64,
    source: impl FnOnce(Bytes) -> PlannedSource,
) -> Result<PlannedEntry, Error> {
    let Encoded {
        oid,
        kind,
        base,
        offset,
    } = entry;
    let header = encode_pack_entry_header(oid, kind, base, length, compressed_len)
        .map_err(|e| anyhow::anyhow!("encode entry header for {oid}: {e}"))?;
    let header_len =
        u64::try_from(header.len()).map_err(|e| anyhow::anyhow!("entry header size: {e}"))?;
    Ok(PlannedEntry {
        oid,
        kind,
        base,
        offset,
        entry_len: header_len + compressed_len,
        source: source(header),
    })
}

/// Plan one entry from whichever index knows it — staged by this push, or
/// pre-existing in the object index.
fn plan_entry(
    planned: DeltaEntry,
    offset: u64,
    index: &Sources<'_>,
) -> Result<PlannedEntry, Error> {
    let DeltaEntry {
        oid,
        kind,
        base,
        source,
    } = planned;
    if let EntrySource::Wire(at) = source {
        return plan_wire_entry(oid, kind, base, offset, at, index.wire);
    }
    let (chunk_index, prefetched) = (index.chunk_index, index.prefetched);
    // A delta no smaller than its object was stored whole instead, so fall
    // back to those bytes rather than claiming a base nothing stored.
    let staged = chunk_index.get(&oid).and_then(|encodings| {
        encodings
            .against(base)
            .map(|loc| (base, loc))
            .or_else(|| encodings.whole().map(|loc| (None, loc)))
    });
    if let Some((base, loc)) = staged {
        let length = u64::try_from(loc.decompressed_len)
            .map_err(|e| anyhow::anyhow!("object length: {e}"))?;
        let compressed_len = u64::try_from(loc.compressed_len)
            .map_err(|e| anyhow::anyhow!("compressed len: {e}"))?;
        return planned_entry(
            Encoded {
                oid,
                kind,
                base,
                offset,
            },
            length,
            compressed_len,
            |header| PlannedSource::Staged {
                header,
                chunk_id: loc.chunk_id,
                body: loc.offset..loc.offset + loc.compressed_len,
                length,
            },
        );
    }
    let meta = prefetched
        .get(&oid)
        .ok_or_else(|| anyhow::anyhow!("object {oid} not found in staging or prefetched index"))?;
    let loc = meta
        .location
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("object {oid} has no location in index"))?;
    // Forwarding copies the stored entry base and all, and a stored entry's
    // base need not be an ancestor of this commit. `IngestSession::wanted`
    // stages a whole copy of anything deltified, so a base here means that
    // pass missed it and forwarding would leave the pack unreachable.
    if let Some(base) = loc.image.base {
        return Err(anyhow::anyhow!(
            "object {oid} is stored as a delta against {base} and was not \
             materialised before planning"
        )
        .into());
    }
    Ok(PlannedEntry {
        oid,
        kind,
        base: None,
        offset,
        entry_len: loc.image.entry_len,
        source: PlannedSource::Indexed {
            segment: loc.segment.id,
            start: loc.segment_offset(),
        },
    })
}

// ── byte pump ─────────────────────────────────────────────────────────────────

/// A prefetched entry payload: sub-threshold bytes fetched ahead of the
/// pump, or a marker that the pump should stream this entry itself.
enum EntryPayload {
    /// The staged body, or the whole verbatim indexed entry.
    Buffered(Bytes),
    /// At or above [`STREAM_THRESHOLD_BYTES`] — streamed at pump time.
    Inline,
}

/// Promoted images: for each, the commit it belongs to, its trailer, and
/// where its bytes landed.
type PromotedImages = Vec<(ObjectId, Vec<PackTrailerEntry>, SegmentLocation)>;

/// Everything a segment task needs from [`ObjectIoCtx`], owned so it can be
/// spawned.
///
/// Sharing the staging session also keeps its chunks alive for as long as
/// any task can still read them.
#[derive(Clone)]
struct SegmentIo {
    store: Arc<Store>,
    repo: RepoMetadata,
    staging_session: Arc<StagingSession>,
}

/// What a segment task hands back.
struct SegmentOutcome {
    /// Reported on the failure path too: a push that dies part-way through
    /// promotion is the one whose timings are worth having.
    counts: Counts,
    /// `Ok(None)` stood down because another segment had already failed —
    /// distinct from `Err` so the root cause is what gets reported.
    result: Result<Option<PromotedImages>, Error>,
}

/// Upload every planned image, [`MAX_CONCURRENT_SEGMENTS`] segments at a time.
///
/// Segments are independent — see [`split_segments`] — so each is its own
/// task, which is what keeps a push's promotion off a single core.
// Shape comes from the plan, which is byte-exact before any upload I/O, so
// only the timings have to be accumulated as the pump runs.
#[tracing::instrument(
    name = "enroute_git_ingest::upload::pump",
    skip_all,
    fields(
        images = count(plan.len()),
        entries = count(plan.iter().map(|i| i.entries.len()).sum::<usize>()),
        segments = count(plan.iter().filter(|i| i.base_offset == 0).count()),
        bytes = count(plan.iter().map(|i| i.image_len).sum::<u64>()),
        // Summed across concurrent tasks, so unlike the span's own wall time
        // these can exceed it — read them against each other, not against it.
        read_ms = tracing::field::Empty,
        write_ms = tracing::field::Empty,
        cut_ms = tracing::field::Empty,
    )
)]
async fn pump_images(
    plan: Vec<PlannedImage>,
    pack: &StagedPack<'_>,
    io: &ObjectIoCtx<'_>,
) -> Result<PromotedImages, Error> {
    let total = as_u64(plan.len());
    let segments = split_segments(plan);
    let segment_count = segments.len();
    let segment_io = SegmentIo {
        store: io.state.store.clone(),
        repo: io.repo.clone(),
        staging_session: io.staging_session.clone(),
    };
    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_SEGMENTS));
    // Set by whichever segment fails first. Without it every segment still
    // queued would upload in full before the client learns the push is dead.
    let cancel = Arc::new(AtomicBool::new(false));

    // Progress ticks per image, not per segment: most pushes fit in one
    // segment, and a bar that only moves when a segment lands never moves.
    let (image_done, mut image_done_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

    let mut tasks = tokio::task::JoinSet::new();
    for (index, images) in segments.into_iter().enumerate() {
        let segment = Segment {
            io: segment_io.clone(),
            permits: Arc::clone(&permits),
            cancel: Arc::clone(&cancel),
            image_done: image_done.clone(),
            counts: Counts::default(),
        };
        tasks.spawn(
            // Spawned tasks inherit no span, so the store's own spans would
            // otherwise export as orphan traces.
            async move { (index, segment.run(&images).await) }.instrument(tracing::Span::current()),
        );
    }
    // The tasks' clones are now the only senders, so the loop below ends
    // exactly when the last task finishes.
    drop(image_done);

    let mut done = 0u64;
    while image_done_rx.recv().await.is_some() {
        done += 1;
        (pack.progress)(IngestProgress::UpdatingRepository { done, total });
    }

    collect_segments(tasks, segment_count).await
}

/// Split a plan into one run of images per segment object.
///
/// [`plan_images`] resets `base_offset` to zero at each cut, so every zero
/// starts a segment, and the runs share nothing, so they upload concurrently.
fn split_segments(plan: Vec<PlannedImage>) -> Vec<Vec<PlannedImage>> {
    let mut segments: Vec<Vec<PlannedImage>> = Vec::new();
    for image in plan {
        if image.base_offset == 0 || segments.is_empty() {
            segments.push(Vec::new());
        }
        // Non-empty by the push above, so this never falls through.
        if let Some(run) = segments.last_mut() {
            run.push(image);
        }
    }
    segments
}

/// Join every segment task, then report the first failure.
///
/// Returning early would strand a cancelled task's live multipart upload,
/// which [`Store::list_segments`] cannot reclaim.
async fn collect_segments(
    mut tasks: tokio::task::JoinSet<(usize, SegmentOutcome)>,
    segment_count: usize,
) -> Result<PromotedImages, Error> {
    let mut landed: Vec<Option<PromotedImages>> = (0..segment_count).map(|_| None).collect();
    let mut counts = Counts::default();
    let mut failed: Option<Error> = None;

    while let Some(joined) = tasks.join_next().await {
        let (index, outcome) = match joined {
            Ok(joined) => joined,
            Err(e) => {
                if failed.is_none() {
                    failed = Some(anyhow::anyhow!("segment upload task: {e}").into());
                }
                continue;
            }
        };
        counts.add(&outcome.counts);
        match outcome.result {
            Ok(Some(images)) => {
                if let Some(slot) = landed.get_mut(index) {
                    *slot = Some(images);
                }
            }
            // Stood down: no images, and not itself the failure to report.
            Ok(None) => {}
            Err(e) => {
                if failed.is_none() {
                    failed = Some(e);
                }
            }
        }
    }
    counts.record();
    if let Some(e) = failed {
        return Err(e);
    }

    let mut results = Vec::new();
    for slot in landed {
        results.extend(slot.ok_or_else(|| anyhow::anyhow!("a segment task never reported"))?);
    }
    Ok(results)
}

/// Where a segment's time went, split by the stage that spent it.
#[derive(Default)]
struct Counts {
    /// Awaiting the next prefetched payload.
    ///
    /// Polling the queue also drives the other in-flight reads, so their
    /// cost lands here too.
    read: Duration,
    /// In the writer: entry headers, bodies, trailers, and the backpressure
    /// wait for multipart capacity.
    write: Duration,
    /// Waiting for a [`MAX_CONCURRENT_SEGMENTS`] slot, plus this segment's
    /// own completion at the end.
    ///
    /// What the task could not overlap.
    cut: Duration,
}

impl Counts {
    fn add(&mut self, other: &Self) {
        self.read += other.read;
        self.write += other.write;
        self.cut += other.cut;
    }

    fn record(&self) {
        record_ms(&[
            ("read_ms", self.read),
            ("write_ms", self.write),
            ("cut_ms", self.cut),
        ]);
    }
}

/// One segment's upload: the handles its stages share, and where its time
/// went.
///
/// Owned rather than borrowed, since each one is its own spawned task.
struct Segment {
    io: SegmentIo,
    /// The [`MAX_CONCURRENT_SEGMENTS`] cap, taken for the whole upload.
    permits: Arc<Semaphore>,
    /// Set by whichever segment fails first, so the queued ones stand down.
    cancel: Arc<AtomicBool>,
    /// Ticks once per image landed, for the caller's progress meter.
    image_done: tokio::sync::mpsc::UnboundedSender<()>,
    counts: Counts,
}

impl Segment {
    /// Upload `images` as one segment object, reporting the counters
    /// whichever way it goes.
    ///
    /// A push that dies part-way through promotion is the one whose timings
    /// are worth having.
    async fn run(mut self, images: &[PlannedImage]) -> SegmentOutcome {
        let result = self.upload(images).await;
        SegmentOutcome {
            counts: self.counts,
            result,
        }
    }

    /// Take a slot, write the whole segment in it, and stand the rest of the
    /// push down if that fails.
    async fn upload(&mut self, images: &[PlannedImage]) -> Result<Option<PromotedImages>, Error> {
        // Checked here as well as inside the write loop, so a segment that
        // never started costs a branch rather than an upload.
        if self.cancel.load(Ordering::Relaxed) {
            return Ok(None);
        }
        // Held for the whole segment, so the cap is on open writers — and
        // their multipart queues and read windows — not on spawned tasks. The
        // semaphore is local and never closed, so `Err` is unreachable.
        let permits = Arc::clone(&self.permits);
        let _permit = timed_async(&mut self.counts.cut, permits.acquire())
            .await
            .map_err(|e| anyhow::anyhow!("segment upload permit: {e}"))?;
        if self.cancel.load(Ordering::Relaxed) {
            return Ok(None);
        }

        let result = self.write_object(images).await;
        if result.is_err() {
            // Before the permit drops, or a segment queued behind this one
            // takes the freed slot and starts work that is already lost.
            self.cancel.store(true, Ordering::Relaxed);
        }
        result
    }

    /// One segment object's whole life: open a writer, write every image,
    /// then finish it.
    async fn write_object(
        &mut self,
        images: &[PlannedImage],
    ) -> Result<Option<PromotedImages>, Error> {
        // Minted here, not at plan time, so its timestamp — all the janitor's
        // grace window goes on — dates from this segment's own upload.
        let id = Ulid::generate();
        let mut writer = self.io.store.segment_writer(&self.io.repo, id);

        match self.write(images, id, &mut writer).await {
            Ok(Some(promoted)) => {
                // Durable only once this returns: the caller must not record a
                // location naming a segment whose bytes are not all there.
                timed_async(&mut self.counts.cut, writer.finish())
                    .await
                    .map_err(|e| anyhow::anyhow!("finish segment upload: {e:#}"))?;
                Ok(Some(promoted))
            }
            // Incomplete either way, so abort rather than finish — and abort
            // rather than drop, which leaves an upload nothing can reclaim.
            incomplete => {
                if let Err(abort_err) = writer.abort().await {
                    tracing::warn!(
                        error = %abort_err,
                        "failed to abort an unfinished segment upload"
                    );
                }
                incomplete
            }
        }
    }

    /// Stream one segment's images into `writer` in plan order: file header,
    /// each entry, then the trailer.
    ///
    /// Plan order is seq order within the segment, so a recent-history fetch
    /// reads a contiguous tail.
    async fn write(
        &mut self,
        images: &[PlannedImage],
        segment_id: Ulid,
        writer: &mut ObjectWriter,
    ) -> Result<Option<PromotedImages>, Error> {
        let mut results = Vec::with_capacity(images.len());
        // Windowed so the boxed-future queue's allocation tracks prefetch depth
        // rather than the segment's entry count.
        for window in images.chunks(PREFETCH_WINDOW_IMAGES) {
            let written = self
                .write_window(window, segment_id, writer, &mut results)
                .await?;
            if !written {
                return Ok(None);
            }
        }
        Ok(Some(results))
    }

    /// Write one window of images, prefetching their payloads together.
    ///
    /// `false` once the push is cancelled, which leaves `results` holding
    /// only what this segment wrote before it stood down.
    async fn write_window(
        &mut self,
        window: &[PlannedImage],
        segment_id: Ulid,
        writer: &mut ObjectWriter,
        results: &mut PromotedImages,
    ) -> Result<bool, Error> {
        let Self {
            io,
            cancel,
            image_done,
            counts,
            ..
        } = self;
        // Boxed into a `Vec` to give `buffered` one concrete future type:
        // building them lazily in a `map` leaves rustc inferring a
        // higher-ranked lifetime that fails `Send` once the task is spawned.
        let payload_futures: Vec<futures::future::BoxFuture<'_, Result<EntryPayload, Error>>> =
            window
                .iter()
                .flat_map(|image| image.entries.iter())
                .map(|entry| prefetch_payload(entry, io).boxed())
                .collect();
        let mut payloads = futures::stream::iter(payload_futures).buffered(ENTRY_PREFETCH);

        for image in window {
            // Per image, so a segment already under way stops at the next
            // boundary instead of uploading to the end of its run.
            if cancel.load(Ordering::Relaxed) {
                return Ok(false);
            }
            let segment = SegmentLocation {
                id: segment_id,
                base_offset: image.base_offset,
                image_len: image.image_len,
            };

            timed_async(
                &mut counts.write,
                writer.write(Bytes::copy_from_slice(&encode_commit_pack_header(
                    image.entry_count,
                ))),
            )
            .await
            .map_err(|e| anyhow::anyhow!("write commit pack header: {e:#}"))?;

            let mut trailer: Vec<PackTrailerEntry> = Vec::with_capacity(image.entries.len());
            for entry in &image.entries {
                let payload = timed_async(&mut counts.read, payloads.try_next())
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("entry payload stream ended early"))?;
                let length =
                    timed_async(&mut counts.write, pump_entry(writer, entry, payload, io)).await?;
                trailer.push(PackTrailerEntry {
                    sha: entry.oid,
                    kind: entry.kind,
                    length,
                    offset: entry.offset,
                    entry_len: entry.entry_len,
                });
            }
            timed_async(
                &mut counts.write,
                writer.write(encode_pack_trailer(&trailer)),
            )
            .await
            .map_err(|e| anyhow::anyhow!("write commit pack trailer: {e:#}"))?;

            results.push((image.commit_oid, trailer, segment));
            // Ignored: a dropped receiver only means nothing is watching
            // progress any more.
            let _sent = image_done.send(());
        }
        Ok(true)
    }
}

/// Prefetch one entry's payload, or defer it to the pump if it's at or
/// above [`STREAM_THRESHOLD_BYTES`].
async fn prefetch_payload(entry: &PlannedEntry, io: &SegmentIo) -> Result<EntryPayload, Error> {
    match &entry.source {
        // Already resident: the pack it came in is held for the whole push.
        PlannedSource::Wire { body, .. } => Ok(EntryPayload::Buffered(body.clone())),
        PlannedSource::Staged { chunk_id, body, .. } => {
            if as_u64(body.len()) >= STREAM_THRESHOLD_BYTES {
                return Ok(EntryPayload::Inline);
            }
            let bytes = io
                .staging_session
                .get_chunk_range(&io.repo, *chunk_id, body.clone())
                .await
                .map_err(|e| anyhow::anyhow!("staging range read for {}: {e}", entry.oid))?;
            Ok(EntryPayload::Buffered(bytes))
        }
        PlannedSource::Indexed { segment, start } => {
            if entry.entry_len >= STREAM_THRESHOLD_BYTES {
                return Ok(EntryPayload::Inline);
            }
            let bytes = io
                .store
                .get_segment_slice(&io.repo, *segment, *start, Some(entry.entry_len))
                .await
                .map_err(|e| anyhow::anyhow!("segment read for {}: {e}", entry.oid))?
                .ok_or_else(|| anyhow::anyhow!("segment {segment} not found"))?;
            Ok(EntryPayload::Buffered(bytes))
        }
    }
}

/// Write one entry into `writer`, returning its decompressed length for the
/// trailer.
///
/// Precomputed header + body for a staged entry, verbatim bytes for an
/// indexed one.
async fn pump_entry(
    writer: &mut ObjectWriter,
    entry: &PlannedEntry,
    payload: EntryPayload,
    io: &SegmentIo,
) -> Result<u64, Error> {
    let oid = entry.oid;
    match (&entry.source, payload) {
        // Kept from the client's pack: our header, then its own compressed
        // bytes, neither of which needs reading from anywhere.
        (
            PlannedSource::Wire {
                header,
                body,
                length,
            },
            _,
        ) => {
            write_bytes(writer, oid, "entry header", header.clone()).await?;
            write_bytes(writer, oid, "body", body.clone()).await?;
            Ok(*length)
        }
        (
            PlannedSource::Staged {
                header,
                chunk_id,
                body,
                length,
            },
            payload,
        ) => {
            write_bytes(writer, oid, "entry header", header.clone()).await?;
            match payload {
                EntryPayload::Buffered(bytes) => {
                    write_bytes(writer, oid, "body", bytes).await?;
                }
                EntryPayload::Inline => {
                    let mut stream = io
                        .staging_session
                        .stream_chunk_range(&io.repo, *chunk_id, body.clone())
                        .await
                        .map_err(|e| anyhow::anyhow!("staging stream read for {oid}: {e}"))?;
                    while let Some(chunk) = stream
                        .try_next()
                        .await
                        .map_err(|e| anyhow::anyhow!("staging stream for {oid}: {e}"))?
                    {
                        write_bytes(writer, oid, "body", chunk).await?;
                    }
                }
            }
            Ok(*length)
        }
        (PlannedSource::Indexed { segment, start }, payload) => match payload {
            EntryPayload::Buffered(bytes) => {
                let (header, _) = decode_pack_entry_header(&bytes)
                    .map_err(|e| anyhow::anyhow!("decode entry header for {oid}: {e}"))?;
                check_forwarded_sha(entry, &header)?;
                write_bytes(writer, oid, "entry", bytes).await?;
                Ok(header.length)
            }
            EntryPayload::Inline => {
                let mut stream = io
                    .store
                    .stream_segment_slice(&io.repo, *segment, *start, Some(entry.entry_len))
                    .await
                    .map_err(|e| anyhow::anyhow!("segment stream read for {oid}: {e}"))?
                    .ok_or_else(|| anyhow::anyhow!("segment {segment} not found"))?;

                let mut prefix: Vec<u8> = Vec::new();
                let mut decoded: Option<u64> = None;
                while let Some(chunk) = stream
                    .try_next()
                    .await
                    .map_err(|e| anyhow::anyhow!("segment stream for {oid}: {e}"))?
                {
                    if decoded.is_none() {
                        decoded = try_decode_streamed_header(&mut prefix, &chunk, entry)?;
                    }
                    write_bytes(writer, oid, "entry", chunk).await?;
                }
                decoded
                    .ok_or_else(|| {
                        anyhow::anyhow!("entry stream for {oid} ended before its header")
                    })
                    .map_err(Error::from)
            }
        },
    }
}

/// Write one piece of an entry, naming `what` and the object it belongs to
/// if the writer refuses it.
async fn write_bytes(
    writer: &mut ObjectWriter,
    oid: ObjectId,
    what: &str,
    bytes: Bytes,
) -> Result<(), Error> {
    writer
        .write(bytes)
        .await
        .map_err(|e| anyhow::anyhow!("write {what} for {oid}: {e}").into())
}

/// Feed streamed bytes into `prefix` until the entry's inline header can be
/// decoded, returning its decompressed length.
///
/// The header is at the front, so the prefix stays tiny regardless of entry
/// size.
fn try_decode_streamed_header(
    prefix: &mut Vec<u8>,
    chunk: &[u8],
    entry: &PlannedEntry,
) -> Result<Option<u64>, Error> {
    let want = MAX_PACK_ENTRY_HEADER_BYTES.saturating_sub(prefix.len());
    prefix.extend(chunk.iter().take(want));
    if !has_pack_entry_header(prefix) {
        return Ok(None);
    }
    let (header, _) = decode_pack_entry_header(prefix)
        .map_err(|e| anyhow::anyhow!("decode entry header for {}: {e}", entry.oid))?;
    check_forwarded_sha(entry, &header)?;
    Ok(Some(header.length))
}

/// Catches an index/segment desync before the bytes reach a new segment.
fn check_forwarded_sha(
    entry: &PlannedEntry,
    header: &enroute_git_store::PackEntryHeader,
) -> Result<(), Error> {
    if header.sha != entry.oid {
        return Err(anyhow::anyhow!(
            "forwarded entry has sha {}, expected {}",
            header.sha,
            entry.oid
        )
        .into());
    }
    Ok(())
}

/// Fetch each annotated tag and collect its index metadata.
///
/// Tags are stored as loose zlib bytes at a SHA-keyed path, with no
/// recorded location.
async fn collect_tag_writes(
    pack: &StagedPack<'_>,
    repo: &RepoMetadata,
) -> Result<(Vec<(String, Bytes)>, Vec<NewObject>), Error> {
    let tags: Vec<(ObjectId, &StagedObjectLocation)> = pack
        .object_refs
        .iter()
        .filter_map(|(&oid, refs)| {
            matches!(refs, ObjectRefs::Tag(_))
                .then(|| {
                    pack.chunk_index
                        .get(&oid)
                        .and_then(StagedEncodings::whole)
                        .map(|loc| (oid, loc))
                })
                .flatten()
        })
        .collect();

    let mut tag_writes: Vec<(String, Bytes)> = Vec::new();
    let mut tag_objects: Vec<NewObject> = Vec::new();

    for (tag_oid, loc) in tags {
        let tag_sha = tag_oid.to_hex().to_string();
        let (length, compressed) =
            fetch_staged_bytes(tag_oid, loc, &pack.staging_session, repo).await?;
        let content = decode_commit_pack_object(&compressed, length)
            .map_err(|e| anyhow::anyhow!("decode tag {tag_sha}: {e}"))?;
        let (_, loose) = encode_loose(loc.kind, &content)
            .map_err(|e| anyhow::anyhow!("re-encode tag as loose: {e}"))?;
        tag_objects.push(NewObject {
            oid: tag_oid,
            kind: Kind::Tag,
            locations: vec![],
            children: vec![],
        });
        tag_writes.push((tag_sha, Bytes::from(loose)));
    }

    Ok((tag_writes, tag_objects))
}

/// Record index metadata for a commit pack's tree/blob entries; the commit's
/// own entry is handled by [`build_commit_packs`].
///
/// `new_objects` accumulates across the whole push, so a tree/blob
/// attributed to two packs gains a second location on a later call.
fn record_object_entries_absolute(
    new_objects: &mut ObjectHashMap<NewObject>,
    pack_sha: ObjectId,
    entries: impl Iterator<Item = PackTrailerEntry>,
    bases: Option<&ObjectHashMap<Option<ObjectId>>>,
    object_refs: &ObjectHashMap<ObjectRefs>,
) {
    for entry in entries {
        let obj = new_objects.entry(entry.sha).or_insert_with(|| NewObject {
            oid: entry.sha,
            kind: entry.kind,
            locations: Vec::new(),
            children: match (entry.kind, object_refs.get(&entry.sha)) {
                (Kind::Tree, Some(refs @ ObjectRefs::Tree(_))) => refs.deps(),
                _ => Vec::new(),
            },
        });
        debug_assert!(
            !obj.locations.iter().any(|l| l.pack_sha == pack_sha),
            "object {} already has a location in pack {pack_sha}",
            entry.sha
        );
        obj.locations.push(PackImageLocation {
            pack_sha,
            offset: entry.offset,
            entry_len: entry.entry_len,
            base: bases.and_then(|b| b.get(&entry.sha).copied()).flatten(),
        });
    }
}

// ── pack assembly ─────────────────────────────────────────────────────────────

/// Batch-look-up index metadata for every pre-existing tree/blob the packs
/// in `per_commit` reference: those absent from `io.chunk_index`.
///
/// One `batch_lookup` covers them all, deduped.
async fn prefetch_preexisting_meta(
    per_commit: &[PlannedCommit<'_>],
    io: &ObjectIoCtx<'_>,
) -> Result<ObjectHashMap<enroute_git_core::ObjectMeta>, Error> {
    let mut seen: ObjectHashSet = ObjectHashSet::default();
    let preexisting: Vec<ObjectId> = per_commit
        .iter()
        .flat_map(|commit| commit.entries.iter())
        .filter(|planned| {
            !matches!(planned.source, EntrySource::Wire(_))
                && io
                    .chunk_index
                    .get(&planned.oid)
                    .and_then(|e| e.against(planned.base))
                    .is_none()
                && seen.insert(planned.oid)
        })
        .map(|planned| planned.oid)
        .collect();
    if preexisting.is_empty() {
        return Ok(ObjectHashMap::default());
    }

    counted(
        io,
        enroute_git_retrieve::metas(io.state, io.repo.id, &preexisting),
    )
    .await
    .map_err(|e| anyhow::anyhow!("batch index lookup: {e:#}"))
    .map_err(Error::from)
}

/// Fetch a staged object's compressed body plus its decompressed length,
/// already known from [`StagedObjectLocation`].
///
/// Already at the commit-pack zlib level, so relocation pays no
/// decompress/recompress.
async fn fetch_staged_bytes(
    oid: ObjectId,
    loc: &StagedObjectLocation,
    session: &StagingSession,
    repo: &RepoMetadata,
) -> Result<(u64, Bytes), Error> {
    let compressed = session
        .get_chunk_range(
            repo,
            loc.chunk_id,
            loc.offset..loc.offset + loc.compressed_len,
        )
        .await
        // `{e:#}` so the store's own cause survives into the message the
        // pushing client sees, instead of stopping at this layer's context.
        .map_err(|e| anyhow::anyhow!("staging range read for {oid}: {e:#}"))?;
    let length =
        u64::try_from(loc.decompressed_len).map_err(|e| anyhow::anyhow!("object length: {e}"))?;
    Ok((length, compressed))
}

// ── topological sort ──────────────────────────────────────────────────────────

/// Topo-sort `connected_commits` (parents before children) via
/// [`enroute_git_core::topo_order`].
///
/// Parents outside the set are ignored.
///
/// # Errors
/// Returns an error if `connected_commits` contains a cycle.
fn topo_sort_commits(
    connected_commits: &ObjectHashSet,
    object_refs: &ObjectHashMap<ObjectRefs>,
) -> Result<Vec<ObjectId>, Error> {
    let mut parents = commit_parents(object_refs);
    parents.retain(|commit, _| connected_commits.contains(commit));
    topo_order(&parents)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::delta_plan::{DeltaEntry, DeltaPlan, EntrySource};

    use bytes::Bytes;
    use enroute_git_core::{ObjectHashMap, ObjectHashSet, encode_loose};
    use enroute_git_retrieve::{RepoMetadata, Storage};
    use gix_hash::ObjectId;
    use gix_object::Kind;

    use crate::staging::StagingSession;
    use crate::test_helpers::staging_store;
    use enroute_git_test_support::{
        annotated_tag, blob, commit, make_state, put_loose_as_singleton_pack, tree_of,
        tree_with_blob,
    };

    use super::{StagedEncodings, StagedObjectLocation, StagedPack};
    use crate::progress::{IngestProgress, noop_progress};
    use enroute_git_graph::{ObjectRefs, TreeChild};

    // ── helpers ───────────────────────────────────────────────────────────────

    /// One object as these fixtures stage it: id, kind, and raw content.
    type Object = (ObjectId, Kind, Vec<u8>);

    fn tree_refs(blob_oid: ObjectId) -> ObjectRefs {
        ObjectRefs::Tree(vec![TreeChild {
            oid: blob_oid,
            is_tree: false,
            is_commit: false,
            name: b"blob".to_vec(),
        }])
    }

    /// Seed `content` as an object an earlier push already stored.
    async fn preexisting(
        state: &Storage,
        repo: &RepoMetadata,
        oid: ObjectId,
        kind: Kind,
        content: &[u8],
    ) {
        let (_, loose) = encode_loose(kind, content).unwrap();
        put_loose_as_singleton_pack(state, repo, &oid.to_hex().to_string(), loose.into()).await;
    }

    /// Stage `objects` as a single chunk of commit-pack-compressed content,
    /// matching production's staged form.
    async fn stage(
        session: &StagingSession,
        repo: &RepoMetadata,
        objects: &[Object],
    ) -> ObjectHashMap<StagedEncodings> {
        let mut chunk: Vec<u8> = Vec::new();
        let mut index: ObjectHashMap<StagedEncodings> = ObjectHashMap::default();
        for (sha, kind, content) in objects {
            let compressed = enroute_git_store::encode_commit_pack_object(content).unwrap();
            let offset = chunk.len();
            chunk.extend_from_slice(&compressed);
            index.entry(*sha).or_default().insert(
                None,
                StagedObjectLocation {
                    chunk_id: 0,
                    offset,
                    compressed_len: compressed.len(),
                    decompressed_len: content.len(),
                    kind: *kind,
                },
            );
        }
        session
            .put_chunk(repo, 0, Bytes::from(chunk))
            .await
            .unwrap();
        index
    }

    async fn indexed(state: &Storage, repo: &RepoMetadata, sha: ObjectId) -> bool {
        enroute_git_retrieve::meta(state, repo.id, sha)
            .await
            .unwrap()
            .is_some()
    }

    /// Promote a staged pack and record it in the metadata store.
    ///
    /// The same two steps `receive_pack` performs, so tests see the same
    /// state a real push leaves behind.
    async fn promote_and_record(
        pack: &StagedPack<'_>,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> anyhow::Result<super::PromotedObjects> {
        let promoted = super::upload_staged_ordered(pack, state, repo).await?;
        crate::append::append(
            crate::append::Engine {
                ids: state.rows.repo(repo.id),
                graph: state.graph.repo(repo.id),
                objects: state.objects.repo(repo.id),
                ledger: state.ledger.as_ref(),
            },
            &promoted.new_commits,
            &promoted.new_objects,
            &promoted.known_seqs,
        )
        .await?;
        Ok(promoted)
    }

    /// Plan a test's pack the way a push does, so promotion is exercised
    /// against real membership rather than a hand-written one.
    async fn plan_of(
        object_refs: &ObjectHashMap<ObjectRefs>,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> DeltaPlan {
        crate::delta_plan::plan_push(object_refs, state, repo, &noop_progress)
            .await
            .expect("planning a test's pack")
    }

    /// A staged, single new root commit (with its own new blob+tree), the
    /// common case shared by most `upload_staged_ordered` tests.
    struct SimpleCommit {
        blob_oid: ObjectId,
        tree_oid: ObjectId,
        commit_oid: ObjectId,
        chunk_index: ObjectHashMap<StagedEncodings>,
        object_refs: ObjectHashMap<ObjectRefs>,
        connected: ObjectHashSet,
        plan: DeltaPlan,
    }

    impl SimpleCommit {
        fn staged_pack<'a>(&'a self, session: &Arc<StagingSession>) -> StagedPack<'a> {
            StagedPack {
                object_refs: &self.object_refs,
                connected_commits: &self.connected,
                chunk_index: &self.chunk_index,
                plan: &self.plan,
                wire: &[],
                staging_session: session.clone(),
                progress: &noop_progress,
            }
        }
    }

    async fn stage_simple_commit(
        state: &Storage,
        session: &StagingSession,
        repo: &RepoMetadata,
        blob_content: &[u8],
    ) -> SimpleCommit {
        let (blob_oid, blob_bytes) = blob(blob_content);
        let (tree_oid, tree_bytes) = tree_with_blob("f", blob_oid);
        let (commit_oid, commit_bytes) = commit(tree_oid, None);

        let chunk_index = stage(
            session,
            repo,
            &[
                (blob_oid, Kind::Blob, blob_bytes),
                (tree_oid, Kind::Tree, tree_bytes),
                (commit_oid, Kind::Commit, commit_bytes),
            ],
        )
        .await;

        let object_refs = ObjectHashMap::from_iter([
            (blob_oid, ObjectRefs::Blob),
            (tree_oid, tree_refs(blob_oid)),
            (
                commit_oid,
                ObjectRefs::Commit {
                    root_tree: tree_oid,
                    parents: vec![],
                    committer_date: 0,
                },
            ),
        ]);
        let connected = ObjectHashSet::from_iter([commit_oid]);
        let plan = plan_of(&object_refs, state, repo).await;

        SimpleCommit {
            blob_oid,
            tree_oid,
            commit_oid,
            chunk_index,
            object_refs,
            connected,
            plan,
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn upload_empty_is_noop() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));
        let plan = DeltaPlan::default();
        promote_and_record(
            &StagedPack {
                object_refs: &ObjectHashMap::default(),
                connected_commits: &ObjectHashSet::default(),
                chunk_index: &ObjectHashMap::default(),
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();
    }

    /// Tags are the one object kind nothing else in a pack references, so
    /// pushing the same tag twice must keep its original seq.
    ///
    /// Does not pin the prefetch's tag arm: an uncovered oid is read by
    /// `append` anyway.
    #[tokio::test]
    async fn upload_annotated_tag_survives_a_repush() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let (blob_oid, blob_bytes) = blob(b"hello");
        let (tree_oid, tree_bytes) = tree_with_blob("f", blob_oid);
        let (commit_oid, commit_bytes) = commit(tree_oid, None);
        let (tag_oid, tag_bytes) = annotated_tag(commit_oid, "v1");

        let chunk_index = stage(
            &session,
            &repo,
            &[
                (blob_oid, Kind::Blob, blob_bytes),
                (tree_oid, Kind::Tree, tree_bytes),
                (commit_oid, Kind::Commit, commit_bytes),
                (tag_oid, Kind::Tag, tag_bytes),
            ],
        )
        .await;
        let object_refs = ObjectHashMap::from_iter([
            (blob_oid, ObjectRefs::Blob),
            (tree_oid, tree_refs(blob_oid)),
            (
                commit_oid,
                ObjectRefs::Commit {
                    root_tree: tree_oid,
                    parents: vec![],
                    committer_date: 0,
                },
            ),
            (tag_oid, ObjectRefs::Tag(commit_oid)),
        ]);
        let connected = ObjectHashSet::from_iter([commit_oid]);
        let plan = plan_of(&object_refs, &state, &repo).await;
        let pack = StagedPack {
            object_refs: &object_refs,
            connected_commits: &connected,
            chunk_index: &chunk_index,
            plan: &plan,
            staging_session: session.clone(),
            wire: &[],
            progress: &noop_progress,
        };

        promote_and_record(&pack, &state, &repo).await.unwrap();
        assert!(indexed(&state, &repo, tag_oid).await, "tag not indexed");
        let first = state.rows.repo(repo.id).identify(&[tag_oid]).await;
        let first = *first.unwrap().get(&tag_oid).expect("tag has a seq");

        promote_and_record(&pack, &state, &repo).await.unwrap();
        let second = state.rows.repo(repo.id).identify(&[tag_oid]).await;
        assert_eq!(
            second.unwrap().get(&tag_oid),
            Some(&first),
            "re-pushing the tag must keep its original seq"
        );
    }

    #[tokio::test]
    async fn upload_single_commit_with_tree_and_blob() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let sc = stage_simple_commit(&state, &session, &repo, b"hello").await;

        promote_and_record(&sc.staged_pack(&session), &state, &repo)
            .await
            .unwrap();

        assert!(
            indexed(&state, &repo, sc.commit_oid).await,
            "commit not indexed"
        );
        assert!(
            indexed(&state, &repo, sc.tree_oid).await,
            "tree not indexed"
        );
        assert!(
            indexed(&state, &repo, sc.blob_oid).await,
            "blob not indexed"
        );

        let commit_meta = enroute_git_retrieve::meta(&state, repo.id, sc.commit_oid)
            .await
            .unwrap()
            .unwrap();
        let loc = commit_meta.location.as_ref().unwrap();
        assert_eq!(loc.image.pack_sha, sc.commit_oid);
        // Commit is the first entry, right after the file header.
        assert_eq!(loc.image.offset, enroute_git_core::COMMIT_PACK_HEADER_SIZE);
    }

    #[tokio::test]
    #[expect(
        clippy::similar_names,
        reason = "a/b suffix pairs are intentional in two-commit tests"
    )]
    async fn upload_chain_second_commit_shares_no_objects_with_first() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let (blob_a, blob_a_bytes) = blob(b"a");
        let (tree_a, tree_a_bytes) = tree_with_blob("f", blob_a);
        let (commit_a, commit_a_bytes) = commit(tree_a, None);

        let (blob_b, blob_b_bytes) = blob(b"b");
        let (tree_b, tree_b_bytes) = tree_with_blob("f", blob_b);
        let (commit_b, commit_b_bytes) = commit(tree_b, Some(commit_a));

        let chunk_index = stage(
            &session,
            &repo,
            &[
                (blob_a, Kind::Blob, blob_a_bytes),
                (tree_a, Kind::Tree, tree_a_bytes),
                (commit_a, Kind::Commit, commit_a_bytes),
                (blob_b, Kind::Blob, blob_b_bytes),
                (tree_b, Kind::Tree, tree_b_bytes),
                (commit_b, Kind::Commit, commit_b_bytes),
            ],
        )
        .await;

        let object_refs = ObjectHashMap::from_iter([
            (blob_a, ObjectRefs::Blob),
            (tree_a, tree_refs(blob_a)),
            (
                commit_a,
                ObjectRefs::Commit {
                    root_tree: tree_a,
                    parents: vec![],
                    committer_date: 0,
                },
            ),
            (blob_b, ObjectRefs::Blob),
            (tree_b, tree_refs(blob_b)),
            (
                commit_b,
                ObjectRefs::Commit {
                    root_tree: tree_b,
                    parents: vec![commit_a],
                    committer_date: 0,
                },
            ),
        ]);
        let connected = ObjectHashSet::from_iter([commit_a, commit_b]);

        let plan = plan_of(&object_refs, &state, &repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &connected,
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();

        for oid in [blob_a, tree_a, commit_a, blob_b, tree_b, commit_b] {
            assert!(
                indexed(&state, &repo, oid).await,
                "{} not indexed",
                oid.to_hex()
            );
        }

        let commit_b_meta = enroute_git_retrieve::meta(&state, repo.id, commit_b)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(commit_b_meta.location.unwrap().image.pack_sha, commit_b);
    }

    #[tokio::test]
    #[expect(
        clippy::similar_names,
        reason = "a/b suffix pairs are intentional in two-commit tests"
    )]
    async fn upload_shared_subtree_claimed_by_first_commit() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let (blob_oid, blob_bytes) = blob(b"shared");
        let (tree_oid, tree_bytes) = tree_with_blob("f", blob_oid);
        let (commit_a, commit_a_bytes) = commit(tree_oid, None);
        let (commit_b, commit_b_bytes) = commit(tree_oid, Some(commit_a));

        let chunk_index = stage(
            &session,
            &repo,
            &[
                (blob_oid, Kind::Blob, blob_bytes),
                (tree_oid, Kind::Tree, tree_bytes),
                (commit_a, Kind::Commit, commit_a_bytes),
                (commit_b, Kind::Commit, commit_b_bytes),
            ],
        )
        .await;

        let object_refs = ObjectHashMap::from_iter([
            (blob_oid, ObjectRefs::Blob),
            (tree_oid, tree_refs(blob_oid)),
            (
                commit_a,
                ObjectRefs::Commit {
                    root_tree: tree_oid,
                    parents: vec![],
                    committer_date: 0,
                },
            ),
            (
                commit_b,
                ObjectRefs::Commit {
                    root_tree: tree_oid,
                    parents: vec![commit_a],
                    committer_date: 0,
                },
            ),
        ]);
        let connected = ObjectHashSet::from_iter([commit_a, commit_b]);

        let plan = plan_of(&object_refs, &state, &repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &connected,
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();

        let tree_meta = enroute_git_retrieve::meta(&state, repo.id, tree_oid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tree_meta.location.unwrap().image.pack_sha, commit_a);

        let blob_meta = enroute_git_retrieve::meta(&state, repo.id, blob_oid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(blob_meta.location.unwrap().image.pack_sha, commit_a);

        let commit_b_meta = enroute_git_retrieve::meta(&state, repo.id, commit_b)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(commit_b_meta.location.unwrap().image.pack_sha, commit_b);
    }

    #[tokio::test]
    async fn preexisting_objects_gain_new_pack_location() {
        // Pre-existing blob/tree reused by a new root commit must gain an extra-pack location.
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let (blob_oid, blob_bytes) = blob(b"pre-existing");
        let (tree_oid, tree_bytes) = tree_with_blob("f", blob_oid);
        preexisting(&state, &repo, blob_oid, Kind::Blob, &blob_bytes).await;
        preexisting(&state, &repo, tree_oid, Kind::Tree, &tree_bytes).await;

        let (commit_oid, commit_bytes) = commit(tree_oid, None);
        let chunk_index = stage(&session, &repo, &[(commit_oid, Kind::Commit, commit_bytes)]).await;

        let object_refs = ObjectHashMap::from_iter([(
            commit_oid,
            ObjectRefs::Commit {
                root_tree: tree_oid,
                parents: vec![],
                committer_date: 0,
            },
        )]);
        let plan = plan_of(&object_refs, &state, &repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &ObjectHashSet::from_iter([commit_oid]),
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();

        assert!(
            state
                .objects
                .repo(repo.id)
                .packs_of(blob_oid)
                .await
                .unwrap()
                .contains(&commit_oid),
            "pre-existing blob must gain an extra-pack entry for the new commit pack"
        );

        assert!(
            state
                .objects
                .repo(repo.id)
                .packs_of(tree_oid)
                .await
                .unwrap()
                .contains(&commit_oid),
            "pre-existing tree must gain an extra-pack entry for the new commit pack"
        );
    }

    #[tokio::test]
    async fn upload_preexisting_objects_included_in_commit_pack() {
        // Under the per-parent-diff invariant, a root commit (no parents)
        // treats its whole tree as new, even objects already in primary.
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let (blob_oid, blob_bytes) = blob(b"pre-existing");
        let (tree_oid, tree_bytes) = tree_with_blob("f", blob_oid);
        preexisting(&state, &repo, blob_oid, Kind::Blob, &blob_bytes).await;
        preexisting(&state, &repo, tree_oid, Kind::Tree, &tree_bytes).await;

        let (commit_oid, commit_bytes) = commit(tree_oid, None);

        let chunk_index = stage(&session, &repo, &[(commit_oid, Kind::Commit, commit_bytes)]).await;

        let object_refs = ObjectHashMap::from_iter([(
            commit_oid,
            ObjectRefs::Commit {
                root_tree: tree_oid,
                parents: vec![],
                committer_date: 0,
            },
        )]);
        let connected = ObjectHashSet::from_iter([commit_oid]);

        let plan = plan_of(&object_refs, &state, &repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &connected,
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();

        assert!(indexed(&state, &repo, commit_oid).await);
        assert!(
            indexed(&state, &repo, tree_oid).await,
            "pre-existing tree must be pulled into commit pack"
        );
        assert!(
            indexed(&state, &repo, blob_oid).await,
            "pre-existing blob must be pulled into commit pack"
        );
    }

    #[tokio::test]
    async fn upload_large_commit_uses_streaming_multipart_path() {
        // A blob past MULTIPART_CHUNK_BYTES forces real multipart; content must be
        // incompressible so its zlib size stays close to raw.
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let mut seed: u8 = 1;
        let big_content: Vec<u8> = (0..enroute_git_store::MULTIPART_CHUNK_BYTES + 1024)
            .map(|_| {
                seed = seed.wrapping_mul(167).wrapping_add(71);
                seed
            })
            .collect();
        let sc = stage_simple_commit(&state, &session, &repo, &big_content).await;

        promote_and_record(&sc.staged_pack(&session), &state, &repo)
            .await
            .unwrap();

        assert!(indexed(&state, &repo, sc.commit_oid).await);
        assert!(indexed(&state, &repo, sc.tree_oid).await);
        assert!(indexed(&state, &repo, sc.blob_oid).await);

        let blob_meta = enroute_git_retrieve::meta(&state, repo.id, sc.blob_oid)
            .await
            .unwrap()
            .unwrap();
        let loc = blob_meta.location.as_ref().unwrap();
        let raw = state
            .store
            .get_segment_slice(
                &repo,
                loc.segment.id,
                loc.segment_offset(),
                Some(loc.image.entry_len),
            )
            .await
            .unwrap()
            .unwrap();
        let (header, consumed) = enroute_git_store::decode_pack_entry_header(&raw).unwrap();
        let content =
            enroute_git_store::decode_commit_pack_object(&raw[consumed..], header.length).unwrap();
        assert_eq!(&content[..], &big_content[..]);
    }

    #[tokio::test]
    async fn upload_long_chain_attributes_unchanged_tree_only_to_root() {
        // Every descendant reuses the root's tree/blob; ownership must resolve via the
        // forward-propagated ancestor bitmap, not duplicate the pack.
        const CHAIN_LEN: usize = 200;

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let (blob_oid, blob_bytes) = blob(b"unchanged");
        let (tree_oid, tree_bytes) = tree_with_blob("f", blob_oid);

        let mut staged: Vec<Object> = vec![
            (blob_oid, Kind::Blob, blob_bytes),
            (tree_oid, Kind::Tree, tree_bytes),
        ];
        let mut object_refs: ObjectHashMap<ObjectRefs> = ObjectHashMap::from_iter([
            (blob_oid, ObjectRefs::Blob),
            (tree_oid, tree_refs(blob_oid)),
        ]);
        let mut connected: ObjectHashSet = ObjectHashSet::default();
        let mut commit_oids: Vec<ObjectId> = Vec::with_capacity(CHAIN_LEN);

        let mut parent: Option<ObjectId> = None;
        for _ in 0..CHAIN_LEN {
            let (commit_oid, commit_bytes) = commit(tree_oid, parent);
            object_refs.insert(
                commit_oid,
                ObjectRefs::Commit {
                    root_tree: tree_oid,
                    parents: parent.into_iter().collect(),
                    committer_date: 0,
                },
            );
            staged.push((commit_oid, Kind::Commit, commit_bytes));
            connected.insert(commit_oid);
            parent = Some(commit_oid);
            commit_oids.push(commit_oid);
        }

        let chunk_index = stage(&session, &repo, &staged).await;

        let plan = plan_of(&object_refs, &state, &repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &connected,
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();

        let root_commit = commit_oids[0];
        assert_eq!(
            state
                .objects
                .repo(repo.id)
                .packs_of(tree_oid)
                .await
                .unwrap(),
            vec![root_commit],
            "tree must be attributed to exactly one pack"
        );

        assert_eq!(
            state
                .objects
                .repo(repo.id)
                .packs_of(blob_oid)
                .await
                .unwrap(),
            vec![root_commit]
        );

        for &commit_oid in &commit_oids {
            assert!(indexed(&state, &repo, commit_oid).await);
        }
    }

    #[tokio::test]
    async fn build_commit_packs_reports_progress_per_commit() {
        // Distinct trees per commit, so each gets its own pack and `total`
        // below is exactly the number of commits promoted.
        const COMMIT_COUNT: usize = 5;

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let mut staged: Vec<Object> = Vec::new();
        let mut object_refs: ObjectHashMap<ObjectRefs> = ObjectHashMap::default();
        let mut connected: ObjectHashSet = ObjectHashSet::default();

        for i in 0..COMMIT_COUNT {
            let (blob_oid, blob_bytes) = blob(format!("commit {i}").as_bytes());
            let (tree_oid, tree_bytes) = tree_with_blob("f", blob_oid);
            let (commit_oid, commit_bytes) = commit(tree_oid, None);
            staged.push((blob_oid, Kind::Blob, blob_bytes));
            staged.push((tree_oid, Kind::Tree, tree_bytes));
            staged.push((commit_oid, Kind::Commit, commit_bytes));
            object_refs.insert(blob_oid, ObjectRefs::Blob);
            object_refs.insert(tree_oid, tree_refs(blob_oid));
            object_refs.insert(
                commit_oid,
                ObjectRefs::Commit {
                    root_tree: tree_oid,
                    parents: vec![],
                    committer_date: 0,
                },
            );
            connected.insert(commit_oid);
        }

        let chunk_index = stage(&session, &repo, &staged).await;

        let reports: std::sync::Mutex<Vec<IngestProgress>> = std::sync::Mutex::new(Vec::new());
        let progress = |stage: IngestProgress| reports.lock().unwrap().push(stage);

        let plan = plan_of(&object_refs, &state, &repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &connected,
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();

        let reports = reports.into_inner().unwrap();
        assert_eq!(reports.len(), COMMIT_COUNT);
        let total_u64 = u64::try_from(COMMIT_COUNT).unwrap();
        let mut done_values: Vec<u64> = reports
            .iter()
            .map(|stage| match stage {
                IngestProgress::UpdatingRepository { done, total } => {
                    assert_eq!(*total, total_u64);
                    *done
                }
                other => panic!("unexpected stage: {other:?}"),
            })
            .collect();
        done_values.sort_unstable();
        assert_eq!(done_values, (1..=total_u64).collect::<Vec<_>>());
    }

    /// A tree with one `40000` (subtree) entry named "outer" pointing at
    /// `child`.
    fn tree_with_subtree(child: ObjectId) -> (ObjectId, Vec<u8>) {
        tree_of(&[("40000", "outer", child)])
    }

    /// A tree with two entries: subtree "a" and blob "b", sorted correctly
    /// (git requires tree entries sorted by name).
    fn tree_with_subtree_and_blob(subtree: ObjectId, blob_oid: ObjectId) -> (ObjectId, Vec<u8>) {
        tree_of(&[("40000", "a", subtree), ("100644", "b", blob_oid)])
    }

    /// `ObjectRefs` for a `tree_with_subtree_and_blob(subtree, leaf)` tree.
    fn outer_refs(subtree: ObjectId, leaf: ObjectId) -> ObjectRefs {
        ObjectRefs::Tree(vec![
            TreeChild {
                oid: subtree,
                is_tree: true,
                is_commit: false,
                name: b"a".to_vec(),
            },
            TreeChild {
                oid: leaf,
                is_tree: false,
                is_commit: false,
                name: b"b".to_vec(),
            },
        ])
    }

    /// `ObjectRefs` for a `tree_with_subtree(outer)` tree.
    fn root_refs(outer: ObjectId) -> ObjectRefs {
        ObjectRefs::Tree(vec![TreeChild {
            oid: outer,
            is_tree: true,
            is_commit: false,
            name: b"outer".to_vec(),
        }])
    }

    /// Push one commit built from `objects` (staged, in commit-pack order)
    /// and `object_refs`, via [`promote_and_record`].
    async fn push_commit(
        state: &Storage,
        session: &Arc<StagingSession>,
        repo: &RepoMetadata,
        commit_oid: ObjectId,
        objects: &[Object],
        object_refs: ObjectHashMap<ObjectRefs>,
    ) {
        let chunk_index = stage(session, repo, objects).await;
        let plan = plan_of(&object_refs, state, repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &ObjectHashSet::from_iter([commit_oid]),
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            state,
            repo,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn upload_unchanged_subtree_two_levels_deep_pruned_across_pushes() {
        // root -> "outer" -> {"a" unchanged, "b" changed}: only "b" differs
        // across the pushes, but "outer" and root get new oids too, so the
        // frontier must reach two levels deep before "a" prunes.
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let (leaf_blob, leaf_blob_bytes) = blob(b"unchanged leaf");
        let (unchanged_subtree, unchanged_subtree_bytes) = tree_with_blob("f", leaf_blob);
        let (blob_y1, blob_y1_bytes) = blob(b"y1");
        let (blob_y2, blob_y2_bytes) = blob(b"y2");
        let (outer1, outer1_bytes) = tree_with_subtree_and_blob(unchanged_subtree, blob_y1);
        let (outer2, outer2_bytes) = tree_with_subtree_and_blob(unchanged_subtree, blob_y2);
        let (root1, root1_bytes) = tree_with_subtree(outer1);
        let (root2, root2_bytes) = tree_with_subtree(outer2);
        let (commit1, commit1_bytes) = commit(root1, None);
        let (commit2, commit2_bytes) = commit(root2, Some(commit1));

        push_commit(
            &state,
            &session,
            &repo,
            commit1,
            &[
                (leaf_blob, Kind::Blob, leaf_blob_bytes),
                (unchanged_subtree, Kind::Tree, unchanged_subtree_bytes),
                (blob_y1, Kind::Blob, blob_y1_bytes),
                (outer1, Kind::Tree, outer1_bytes),
                (root1, Kind::Tree, root1_bytes),
                (commit1, Kind::Commit, commit1_bytes),
            ],
            ObjectHashMap::from_iter([
                (leaf_blob, ObjectRefs::Blob),
                (unchanged_subtree, tree_refs(leaf_blob)),
                (blob_y1, ObjectRefs::Blob),
                (outer1, outer_refs(unchanged_subtree, blob_y1)),
                (root1, root_refs(outer1)),
                (
                    commit1,
                    ObjectRefs::Commit {
                        root_tree: root1,
                        parents: vec![],
                        committer_date: 0,
                    },
                ),
            ]),
        )
        .await;

        // Push 2, separately: only blob_y1 -> blob_y2 changed, but "outer"
        // and root both get new oids as a result.
        push_commit(
            &state,
            &session,
            &repo,
            commit2,
            &[
                (blob_y2, Kind::Blob, blob_y2_bytes),
                (outer2, Kind::Tree, outer2_bytes),
                (root2, Kind::Tree, root2_bytes),
                (commit2, Kind::Commit, commit2_bytes),
            ],
            ObjectHashMap::from_iter([
                (blob_y2, ObjectRefs::Blob),
                (outer2, outer_refs(unchanged_subtree, blob_y2)),
                (root2, root_refs(outer2)),
                (
                    commit2,
                    ObjectRefs::Commit {
                        root_tree: root2,
                        parents: vec![commit1],
                        committer_date: 0,
                    },
                ),
            ]),
        )
        .await;

        assert_owning_pack(&state, &repo, root2, commit2, "changed root").await;
        assert_owning_pack(&state, &repo, outer2, commit2, "changed outer tree").await;
        assert_owning_pack(&state, &repo, blob_y2, commit2, "changed blob").await;
        assert_owning_pack(
            &state,
            &repo,
            unchanged_subtree,
            commit1,
            "unchanged subtree two levels deep",
        )
        .await;
        assert_owning_pack(
            &state,
            &repo,
            leaf_blob,
            commit1,
            "unchanged leaf blob three levels deep",
        )
        .await;
    }

    /// Assert `oid`'s sole owning pack is `expected_pack` — `what` names the
    /// object in the assertion failure message.
    async fn assert_owning_pack(
        state: &Storage,
        repo: &RepoMetadata,
        oid: ObjectId,
        expected_pack: ObjectId,
        what: &str,
    ) {
        assert_eq!(
            state.objects.repo(repo.id).packs_of(oid).await.unwrap(),
            vec![expected_pack],
            "{what} must be owned by exactly {expected_pack}'s pack"
        );
    }

    // ── segments ──────────────────────────────────────────────────────────────

    /// The `per_commit` shape `plan_images` consumes, owned so tests can
    /// build it before borrowing it into place.
    type PerCommit = Vec<(ObjectId, Vec<DeltaEntry>)>;

    /// The index trio `plan_images` reads, for tests that stage everything
    /// and so keep nothing from a wire pack.
    fn sources<'a>(
        chunk_index: &'a ObjectHashMap<StagedEncodings>,
        prefetched: &'a ObjectHashMap<enroute_git_core::ObjectMeta>,
    ) -> super::Sources<'a> {
        super::Sources {
            wire: &[],
            chunk_index,
            prefetched,
        }
    }

    fn borrowed(per_commit: &PerCommit) -> Vec<super::PlannedCommit<'_>> {
        per_commit
            .iter()
            .map(|(oid, entries)| super::PlannedCommit {
                commit_oid: *oid,
                source: EntrySource::Encoded,
                entries: entries.as_slice(),
            })
            .collect()
    }

    /// Three single-entry commits with staged locations — the smallest input
    /// on which `plan_images`' cut arithmetic is observable.
    fn planned_three_commits() -> (PerCommit, ObjectHashMap<StagedEncodings>) {
        planned_commits(3)
    }

    /// `n` single-entry commits with staged locations, all the same size.
    fn planned_commits(n: u8) -> (PerCommit, ObjectHashMap<StagedEncodings>) {
        let mut per_commit: PerCommit = Vec::new();
        let mut chunk_index: ObjectHashMap<StagedEncodings> = ObjectHashMap::default();
        for i in 0u8..n {
            let oid = ObjectId::from_bytes_or_panic(&[i + 1; 20]);
            per_commit.push((oid, vec![]));
            chunk_index.entry(oid).or_default().insert(
                None,
                StagedObjectLocation {
                    chunk_id: 0,
                    offset: usize::from(i) * 100,
                    compressed_len: 100,
                    decompressed_len: 200,
                    kind: Kind::Commit,
                },
            );
        }
        (per_commit, chunk_index)
    }

    #[test]
    fn plan_images_cuts_at_byte_target_in_order() {
        let (per_commit, chunk_index) = planned_three_commits();
        let prefetched = ObjectHashMap::default();

        // Huge target: one segment, base offsets accumulate by exact image
        // length (header + entry + fixed-width trailer).
        let plan = super::plan_images(
            &borrowed(&per_commit),
            &sources(&chunk_index, &prefetched),
            u64::MAX,
        )
        .unwrap();
        let image_len = enroute_git_core::COMMIT_PACK_HEADER_SIZE
            + plan[0].entries[0].entry_len
            + enroute_git_store::trailer_suffix_len(1);
        assert_eq!(plan[0].base_offset, 0);
        assert_eq!(plan[1].base_offset, image_len);
        assert_eq!(plan[2].base_offset, 2 * image_len);
        assert!(plan.iter().all(|image| image.image_len == image_len));
        // Only the first image starts a segment: the rest append to it.
        assert_eq!(
            plan.iter().filter(|image| image.base_offset == 0).count(),
            1
        );
        assert_eq!(
            plan[0].entries[0].offset,
            enroute_git_core::COMMIT_PACK_HEADER_SIZE
        );

        // Target below one image: every image cuts into its own segment,
        // which a zero base offset on all three is exactly what says.
        let plan = super::plan_images(
            &borrowed(&per_commit),
            &sources(&chunk_index, &prefetched),
            1,
        )
        .unwrap();
        assert!(plan.iter().all(|image| image.base_offset == 0));
    }

    /// Several segments of several images each — the shape the pump's
    /// concurrency rests on.
    ///
    /// A misgrouped run would write another segment's bytes.
    #[test]
    fn split_segments_groups_each_cut_s_images_together() {
        let (per_commit, chunk_index) = planned_commits(6);
        let prefetched = ObjectHashMap::default();

        let one = super::plan_images(
            &borrowed(&per_commit),
            &sources(&chunk_index, &prefetched),
            u64::MAX,
        )
        .unwrap();
        let image_len = one[0].image_len;
        // Cuts after every second image, so none of the three runs is trivial.
        let plan = super::plan_images(
            &borrowed(&per_commit),
            &sources(&chunk_index, &prefetched),
            2 * image_len,
        )
        .unwrap();

        let commits: Vec<ObjectId> = plan.iter().map(|image| image.commit_oid).collect();
        let runs = super::split_segments(plan);

        assert_eq!(runs.len(), 3);
        for run in &runs {
            assert_eq!(run.len(), 2);
            assert_eq!(run[0].base_offset, 0, "a run starts its segment");
            assert_eq!(run[1].base_offset, image_len, "and the next appends to it");
        }
        // A regrouping, never a reordering: the caller folds locations in
        // plan order.
        let regrouped: Vec<ObjectId> = runs
            .iter()
            .flatten()
            .map(|image| image.commit_oid)
            .collect();
        assert_eq!(regrouped, commits);
    }

    #[tokio::test]
    #[expect(
        clippy::similar_names,
        reason = "a/b suffix pairs are intentional in two-commit tests"
    )]
    async fn multi_commit_push_shares_one_segment() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let (blob_a, blob_a_bytes) = blob(b"a");
        let (tree_a, tree_a_bytes) = tree_with_blob("f", blob_a);
        let (commit_a, commit_a_bytes) = commit(tree_a, None);
        let (blob_b, blob_b_bytes) = blob(b"b");
        let (tree_b, tree_b_bytes) = tree_with_blob("f", blob_b);
        let (commit_b, commit_b_bytes) = commit(tree_b, Some(commit_a));

        let chunk_index = stage(
            &session,
            &repo,
            &[
                (blob_a, Kind::Blob, blob_a_bytes),
                (tree_a, Kind::Tree, tree_a_bytes),
                (commit_a, Kind::Commit, commit_a_bytes),
                (blob_b, Kind::Blob, blob_b_bytes),
                (tree_b, Kind::Tree, tree_b_bytes),
                (commit_b, Kind::Commit, commit_b_bytes),
            ],
        )
        .await;
        let object_refs = ObjectHashMap::from_iter([
            (blob_a, ObjectRefs::Blob),
            (tree_a, tree_refs(blob_a)),
            (
                commit_a,
                ObjectRefs::Commit {
                    root_tree: tree_a,
                    parents: vec![],
                    committer_date: 0,
                },
            ),
            (blob_b, ObjectRefs::Blob),
            (tree_b, tree_refs(blob_b)),
            (
                commit_b,
                ObjectRefs::Commit {
                    root_tree: tree_b,
                    parents: vec![commit_a],
                    committer_date: 0,
                },
            ),
        ]);
        let connected = ObjectHashSet::from_iter([commit_a, commit_b]);

        let plan = plan_of(&object_refs, &state, &repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &connected,
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();

        let loc_a = enroute_git_retrieve::meta(&state, repo.id, commit_a)
            .await
            .unwrap()
            .unwrap()
            .location
            .unwrap();
        let loc_b = enroute_git_retrieve::meta(&state, repo.id, commit_b)
            .await
            .unwrap()
            .unwrap()
            .location
            .unwrap();
        assert_eq!(
            loc_a.segment.id, loc_b.segment.id,
            "one small push, one segment"
        );
        assert_eq!(
            loc_a.segment.base_offset, 0,
            "first image starts the segment"
        );
        assert!(
            loc_b.segment.base_offset > 0,
            "second image appended after the first"
        );

        // A mid-segment object round-trips through the offset arithmetic.
        let (kind, content) = enroute_git_retrieve::object(&state, &repo, blob_b)
            .await
            .unwrap();
        assert_eq!(kind, Kind::Blob);
        assert_eq!(&content[..], b"b");
    }

    /// More segments than [`super::MAX_CONCURRENT_SEGMENTS`], so slots get
    /// reused — and every segment must still land, not just the last few.
    #[tokio::test]
    async fn every_segment_lands_when_segments_outnumber_upload_slots() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let mut staged_objects: Vec<Object> = Vec::new();
        let mut per_commit: PerCommit = Vec::new();
        for i in 0..u8::try_from(super::MAX_CONCURRENT_SEGMENTS + 2).unwrap() {
            // Only the commits are staged; the tree just makes them distinct.
            let tree_oid = ObjectId::from_bytes_or_panic(&[i + 1; 20]);
            let (commit_oid, commit_bytes) = commit(tree_oid, None);
            staged_objects.push((commit_oid, Kind::Commit, commit_bytes));
            per_commit.push((commit_oid, vec![]));
        }
        let chunk_index = stage(&session, &repo, &staged_objects).await;

        // Target of 1 puts every image in its own segment.
        let plan = super::plan_images(
            &borrowed(&per_commit),
            &sources(&chunk_index, &ObjectHashMap::default()),
            1,
        )
        .unwrap();
        assert!(
            plan.len() > super::MAX_CONCURRENT_SEGMENTS,
            "the cap has to bind for this to test anything"
        );
        let planned = plan.len();

        let object_refs = ObjectHashMap::default();
        let connected = ObjectHashSet::default();
        let delta_plan = DeltaPlan::default();
        let images = super::pump_images(
            plan,
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &connected,
                chunk_index: &chunk_index,
                plan: &delta_plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &super::ObjectIoCtx {
                chunk_index: &chunk_index,
                staging_session: session.clone(),
                state: &state,
                repo: &repo,
                counts: super::AttributionCounts::default(),
            },
        )
        .await
        .unwrap();

        let distinct: std::collections::HashSet<_> =
            images.iter().map(|(_, _, seg)| seg.id).collect();
        assert_eq!(distinct.len(), planned, "one segment object per cut");

        for (commit_oid, entries, seg) in images {
            let bytes = state
                .store
                .get_segment_slice(&repo, seg.id, seg.base_offset, Some(seg.image_len))
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("segment {} never landed", seg.id));
            assert!(
                image_entry_content(&bytes, commit_oid, entries.len()).is_some(),
                "segment {} landed without its commit entry",
                seg.id
            );
        }
    }

    /// A failure in one segment has to stop the ones queued behind it.
    ///
    /// Nothing else keeps a doomed push from uploading every remaining
    /// segment before the client hears about it.
    #[tokio::test]
    async fn a_failed_segment_stands_the_queued_ones_down() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let faulty = Arc::new(enroute_git_test_support::FaultyObjectStore::new());
        let session =
            Arc::new(crate::test_helpers::staging_store_on(faulty.clone()).start_session(&repo));

        // Many more segments than slots, so most are still queued when the
        // first read fails.
        let segments = 5 * super::MAX_CONCURRENT_SEGMENTS;
        let mut staged_objects: Vec<Object> = Vec::new();
        let mut per_commit: PerCommit = Vec::new();
        for i in 0..u8::try_from(segments).unwrap() {
            let tree_oid = ObjectId::from_bytes_or_panic(&[i + 1; 20]);
            let (commit_oid, commit_bytes) = commit(tree_oid, None);
            staged_objects.push((commit_oid, Kind::Commit, commit_bytes));
            per_commit.push((commit_oid, vec![]));
        }
        let chunk_index = stage(&session, &repo, &staged_objects).await;

        // Every staged read from here on fails, so only the segments holding
        // a slot can even try.
        faulty.fail_gets_after(0);

        // Target of 1 puts every image in its own segment.
        let plan = super::plan_images(
            &borrowed(&per_commit),
            &sources(&chunk_index, &ObjectHashMap::default()),
            1,
        )
        .unwrap();
        assert_eq!(plan.len(), segments);

        let object_refs = ObjectHashMap::default();
        let connected = ObjectHashSet::default();
        let delta_plan = DeltaPlan::default();
        let err = super::pump_images(
            plan,
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &connected,
                chunk_index: &chunk_index,
                plan: &delta_plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &super::ObjectIoCtx {
                chunk_index: &chunk_index,
                staging_session: session.clone(),
                state: &state,
                repo: &repo,
                counts: super::AttributionCounts::default(),
            },
        )
        .await
        .expect_err("every staged read failed, so the push cannot succeed");
        // The read failure itself, not a stand-down message: the root cause
        // is what reaches the pushing client.
        assert!(
            format!("{err:#}").contains("staging range read"),
            "expected the underlying read failure, got: {err:#}"
        );

        // Without the stand-down every segment would have attempted a read.
        let attempted = faulty.recorded_ranges().len();
        assert!(
            attempted <= super::MAX_CONCURRENT_SEGMENTS,
            "{attempted} segments attempted a read; at most \
             {} should have, the rest standing down",
            super::MAX_CONCURRENT_SEGMENTS
        );
    }

    /// Walk an image's first `entry_count` entries and decompress the one
    /// named `oid`, or `None` if absent.
    fn image_entry_content(image: &[u8], oid: ObjectId, entry_count: usize) -> Option<Bytes> {
        let mut cursor = usize::try_from(enroute_git_core::COMMIT_PACK_HEADER_SIZE).unwrap();
        for _ in 0..entry_count {
            let (header, consumed) =
                enroute_git_store::decode_pack_entry_header(&image[cursor..]).unwrap();
            let body_start = cursor + consumed;
            let body_len = usize::try_from(header.compressed_len).unwrap();
            cursor = body_start + body_len;
            if header.sha != oid {
                continue;
            }
            let content = enroute_git_store::decode_commit_pack_object(
                &image[body_start..body_start + body_len],
                header.length,
            )
            .unwrap();
            return Some(content);
        }
        None
    }

    #[tokio::test]
    async fn large_preexisting_entry_streams_verbatim_into_new_segment() {
        // A pre-existing blob past STREAM_THRESHOLD_BYTES pulled into a new
        // root commit's pack exercises the pump's verbatim streaming path,
        // including the in-flight entry-header decode for the trailer.
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = Arc::new(staging_store().start_session(&repo));

        let mut seed: u8 = 1;
        let big_content: Vec<u8> = (0..2 * 1024 * 1024)
            .map(|_| {
                seed = seed.wrapping_mul(167).wrapping_add(71);
                seed
            })
            .collect();
        let (blob_oid, blob_bytes) = blob(&big_content);
        let (tree_oid, tree_bytes) = tree_with_blob("f", blob_oid);
        preexisting(&state, &repo, blob_oid, Kind::Blob, &blob_bytes).await;
        preexisting(&state, &repo, tree_oid, Kind::Tree, &tree_bytes).await;

        let (commit_oid, commit_bytes) = commit(tree_oid, None);
        let chunk_index = stage(&session, &repo, &[(commit_oid, Kind::Commit, commit_bytes)]).await;
        let object_refs = ObjectHashMap::from_iter([(
            commit_oid,
            ObjectRefs::Commit {
                root_tree: tree_oid,
                parents: vec![],
                committer_date: 0,
            },
        )]);
        let plan = plan_of(&object_refs, &state, &repo).await;
        promote_and_record(
            &StagedPack {
                object_refs: &object_refs,
                connected_commits: &ObjectHashSet::from_iter([commit_oid]),
                chunk_index: &chunk_index,
                plan: &plan,
                staging_session: session.clone(),
                wire: &[],
                progress: &noop_progress,
            },
            &state,
            &repo,
        )
        .await
        .unwrap();

        // Walk the new image's entries in the segment and round-trip the
        // forwarded blob's bytes.
        let commit_meta = enroute_git_retrieve::meta(&state, repo.id, commit_oid)
            .await
            .unwrap()
            .unwrap();
        let seg = commit_meta.location.unwrap().segment;
        let image = state
            .store
            .get_segment_slice(&repo, seg.id, seg.base_offset, Some(seg.image_len))
            .await
            .unwrap()
            .unwrap();
        let content = image_entry_content(&image, blob_oid, 3)
            .expect("forwarded blob entry missing from the new image");
        assert_eq!(&content[..], &big_content[..]);
    }

    #[tokio::test]
    async fn duplicate_push_keeps_first_location() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let session = Arc::new(staging_store().start_session(&repo));
        let sc = stage_simple_commit(&state, &session, &repo, b"dup").await;
        promote_and_record(&sc.staged_pack(&session), &state, &repo)
            .await
            .unwrap();
        let first = enroute_git_retrieve::meta(&state, repo.id, sc.commit_oid)
            .await
            .unwrap()
            .unwrap();

        // Same commits again (a racing/replayed push): the loser's segment
        // bytes go dead in place; the recorded location must not move.
        let session2 = Arc::new(staging_store().start_session(&repo));
        let sc2 = stage_simple_commit(&state, &session2, &repo, b"dup").await;
        assert_eq!(sc2.commit_oid, sc.commit_oid);
        promote_and_record(&sc2.staged_pack(&session2), &state, &repo)
            .await
            .unwrap();

        let second = enroute_git_retrieve::meta(&state, repo.id, sc.commit_oid)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second, first, "first location wins — never repoint");
    }
}
