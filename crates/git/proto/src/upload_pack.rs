use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::channel::mpsc;
use futures::{Stream, StreamExt as _, stream};
use gix_hash::ObjectId;
use gix_object::Kind;
use tokio::io::{AsyncRead, AsyncReadExt as _};
use tokio_util::io::StreamReader;

use enroute_git_core::{ObjectHashSet, ObjectMeta, decode_loose};
use enroute_git_cost::Meter;
use enroute_git_graph::{ObjectRefs, object_refs};
use enroute_git_ingest::Actor;
use enroute_git_packfile::{
    write_pack_entry, write_pack_entry_header, write_pack_header, write_ref_delta_header,
};
use enroute_git_retrieve::{NeededCommit, RepoMetadata, Storage};
use enroute_git_store::{decode_commit_pack_header, decode_pack_entry_header};

use crate::Error;
use crate::ls_refs::ls_refs;
use crate::pack::{Framing, PackWriter, SidebandTx, send_raw_error, send_sideband_error};
use crate::pktline::{Body, Packet, Packets, trim_lf};
use crate::visibility::{Access, RefVisibility, advertised_refs};

/// One pack entry ready for the wire, shared by `fetch_commit_run` and
/// `fetch_loose_entry`.
///
/// Lets `fetch_source` merge both into one concurrent pipeline.
struct PackEntry {
    oid: ObjectId,
    kind: Kind,
    body: EntryBody,
}

/// An entry's payload, in whichever form reading it left it.
enum EntryBody {
    /// Compressed body and decompressed length, both passing to the wire
    /// untouched.
    Stored { length: u64, compressed: Bytes },
    /// A delta whose base this fetch also sends, so it goes out as-is.
    ///
    /// `length` is the delta stream's, not the object's.
    Delta {
        base: ObjectId,
        length: u64,
        compressed: Bytes,
    },
    /// Stored as a delta whose base this fetch cannot promise, so it was
    /// rebuilt on read (see `rebuild_entry`) and still needs compressing.
    Rebuilt(Bytes),
}

/// Whether a fetch can hand a stored delta over untouched.
///
/// True only for a full-ancestry fetch. Truncating history breaks it: past
/// a `have` or shallow boundary the base may sit in a commit never sent.
const fn may_pass_deltas(haves: &[ObjectId], shallow: &[ObjectId], deepen: Option<u64>) -> bool {
    haves.is_empty() && shallow.is_empty() && deepen.is_none()
}

/// Rebuild a delta entry into the object it encodes.
///
/// Runs inside the per-source streams, not the emit loop, so base fetches
/// overlap under `FETCH_CONCURRENCY` instead of a serial round trip each.
async fn rebuild_entry(
    state: &Storage,
    repo: &RepoMetadata,
    oid: ObjectId,
    base: ObjectId,
    length: u64,
    compressed: &[u8],
) -> Result<Bytes, Error> {
    let delta = enroute_git_store::decode_commit_pack_object(compressed, length)
        .map_err(|e| anyhow::anyhow!("decode delta for {oid}: {e}"))?;
    let (_, base_content) = enroute_git_retrieve::object(state, repo, base).await?;
    let content = enroute_git_packfile::apply_delta(&base_content, &delta)
        .map_err(|e| anyhow::anyhow!("apply delta for {oid}: {e}"))?;
    Ok(Bytes::from(content))
}

/// Sideband-frame-sized chunks (~256 KiB total) the pack producer may buffer
/// ahead of the HTTP write side.
///
/// A pipelining/memory tradeoff only: the producer blocks once full.
const PACK_CHANNEL_CAPACITY: usize = 4;

/// The response to a `git-upload-pack` request: either a complete pkt-line
/// body, or a streamed packfile (sideband-framed, chunk by chunk).
#[derive(Debug)]
pub enum UploadPackResponse {
    /// A complete pkt-line response body (ls-refs listing, or a negotiation
    /// acknowledgments round).
    Body(Vec<u8>),
    /// A streamed, sideband-framed packfile.
    Pack(mpsc::Receiver<Result<Bytes, std::io::Error>>),
}

/// Handle a `git-upload-pack` request body (transport framing already
/// stripped).
///
/// `repo` is a full [`RepoMetadata`], already loaded, and `visibility`
/// filters what `ls-refs` advertises — `fetch` never asks it.
///
/// # Errors
///
/// Returns an error if the request is malformed, or the object store fails.
// An `ls-refs` answers out of Postgres alone, so the cost fields stay
// `Empty` — absent from the span, not present as a zero.
#[tracing::instrument(
    name = "enroute_git_proto::upload_pack::upload_pack",
    skip(state, repo, body, visibility),
    fields(
        repo_id = %repo.id,
        cost_primary_get_class = tracing::field::Empty,
        cost_primary_put_class = tracing::field::Empty,
        cost_primary_deletes = tracing::field::Empty,
        cost_primary_bytes_read = tracing::field::Empty,
        cost_primary_bytes_written = tracing::field::Empty,
        cost_handoff_get_class = tracing::field::Empty,
        cost_handoff_put_class = tracing::field::Empty,
        cost_handoff_deletes = tracing::field::Empty,
        cost_handoff_bytes_read = tracing::field::Empty,
        cost_handoff_bytes_written = tracing::field::Empty,
        cost_lambda_invocations = tracing::field::Empty,
        cost_lambda_mb_millis = tracing::field::Empty,
    )
)]
pub async fn upload_pack(
    state: Storage,
    repo: RepoMetadata,
    body: &[u8],
    visibility: &dyn RefVisibility,
    actor: &Actor,
) -> Result<UploadPackResponse, Error> {
    let lines = command_request(body)?;
    let command = lines
        .first()
        .ok_or(Error::BadRequest("empty upload-pack request".into()))?;
    let command = trim_lf(command);

    match command {
        // Only ls-refs needs the refs map; fetch works purely off wants/haves
        // and skips the read entirely. It touches no store, hence no meter:
        // the cost fields stay `Empty` rather than arriving as twelve zeros.
        b"command=ls-refs" => {
            let refs = advertised_refs(&state, &repo, visibility, actor, Access::Read).await?;
            Ok(UploadPackResponse::Body(ls_refs(
                &refs,
                lines.get(1..).unwrap_or(&[]),
            )?))
        }
        b"command=fetch" => handle_fetch(state, &repo, lines.get(1..).unwrap_or(&[])).await,
        _ => Err(Error::BadRequest("unknown upload-pack command".into())),
    }
}

/// The data lines of a v2 `command-request`, up to its closing flush.
///
/// Enforces v2's own grammar: once a data line has been seen, a delim-pkt
/// (0001) must appear before that flush.
fn command_request(body: &[u8]) -> Result<Vec<&[u8]>, Error> {
    let mut lines: Vec<&[u8]> = Vec::new();
    let mut seen_delim = false;
    for packet in Packets::new(body) {
        match packet? {
            Packet::Data(line) => lines.push(line),
            Packet::Delimiter => seen_delim = true,
            Packet::Flush if lines.is_empty() || seen_delim => return Ok(lines),
            Packet::Flush => {
                return Err(Error::BadRequest(
                    "command-request missing delim-pkt (0001)".into(),
                ));
            }
            // response-end (0002): not valid in a request, but harmless to
            // skip since no real client sends one here.
            Packet::ResponseEnd => {}
        }
    }
    Err(Error::BadRequest("truncated pkt-line".into()))
}

// ── fetch, as a primitive ────────────────────────────────────────────────────

/// Everything reachable from `wants` and not from `haves`, as the packfile's
/// own bytes with chunk boundaries carrying no meaning.
///
/// Full history, since the one caller is a push: truncating it would need a
/// shallow boundary reported alongside the pack, which this does not carry.
///
/// # Errors
///
/// Returns an error if a want cannot be resolved, or the store fails.
#[tracing::instrument(
    name = "enroute_git_proto::upload_pack::fetch",
    skip(state, repo, wants, haves),
    fields(repo_id = %repo.id, wants = wants.len(), haves = haves.len())
)]
pub async fn fetch(
    state: Storage,
    repo: RepoMetadata,
    wants: Vec<ObjectId>,
    haves: Vec<ObjectId>,
) -> Result<mpsc::Receiver<Result<Bytes, std::io::Error>>, Error> {
    if wants.is_empty() {
        return Err(Error::BadRequest("fetch with no wants".into()));
    }

    let meter = Meter::new();
    let cost = tracing::Span::current();
    let state = state.metered(Arc::clone(&meter));

    let args = FetchArgs {
        wants,
        haves,
        // A caller reaching this directly has already finished negotiating:
        // there is no further round to defer to, so the plan is made now.
        done: true,
        filter: None,
        shallow: Vec::new(),
        deepen: None,
    };

    let batch = match plan_fetch(&state, &repo, args).await {
        Ok(batch) => batch,
        // Planning read tags and walked the graph before it failed, and that
        // is still billed — the same rule the pkt-line path follows.
        Err(e) => {
            meter.units().record_on(&cost);
            return Err(e);
        }
    };

    let (tx, rx) = mpsc::channel(PACK_CHANNEL_CAPACITY);
    crate::spawn_instrumented(stream_packs(
        state,
        repo,
        batch,
        tx,
        Framing::Raw,
        meter,
        cost,
    ));

    Ok(rx)
}

// ── fetch ─────────────────────────────────────────────────────────────────────

async fn handle_fetch(
    state: Storage,
    repo: &RepoMetadata,
    args: &[&[u8]],
) -> Result<UploadPackResponse, Error> {
    let args = parse_fetch_args(args)?;

    if args.wants.is_empty() {
        return Err(Error::BadRequest("fetch with no wants".into()));
    }

    // Answered from the commit graph alone, so it reaches no store and gets no
    // meter — its cost fields stay `Empty` rather than arriving as zeros.
    if !args.done {
        return negotiate(&state, repo, args.haves).await;
    }

    // Past here the fetch reads the store, so it takes its own view of it and
    // charges what it reads to this request and no other.
    let meter = Meter::new();
    let cost = tracing::Span::current();
    let state = state.metered(Arc::clone(&meter));

    match plan_fetch(&state, repo, args).await {
        Ok(batch) => {
            let (tx, rx) = mpsc::channel(PACK_CHANNEL_CAPACITY);
            crate::spawn_instrumented(stream_packs(
                state,
                repo.clone(),
                batch,
                tx,
                Framing::Sideband,
                meter,
                cost,
            ));
            Ok(UploadPackResponse::Pack(rx))
        }
        // Planning read tags and walked the graph before it failed. Recorded
        // here because there will be no stream to do it — the same rule the
        // push path follows: whatever was spent is reported, success or not.
        Err(e) => {
            meter.units().record_on(&cost);
            Err(e)
        }
    }
}

/// Work out everything a fetch will send, without sending any of it.
///
/// Split from [`handle_fetch`] so the meter has exactly one owner: a plan
/// that fails reports here, a plan that succeeds hands it to the stream.
async fn plan_fetch(
    state: &Storage,
    repo: &RepoMetadata,
    args: FetchArgs,
) -> Result<PackBatch, Error> {
    let FetchArgs {
        wants,
        haves,
        done: _,
        filter,
        shallow,
        deepen,
    } = args;

    // Split wants into commit SHAs, tag bytes, and loose blob wants.
    let (commit_wants, tag_objects, loose_wants) = resolve_wants(state, repo, &wants).await?;

    // Walks every commit reachable from `commit_wants` minus `haves`, cut at
    // `deepen`'s depth — zero object-storage traffic until streaming opens
    // the packs (see `fetch_commit_run`).
    let pass_deltas = may_pass_deltas(&haves, &shallow, deepen);
    let walked = crate::walk::needed(
        state.graph.repo(repo.id),
        state.objects.repo(repo.id),
        &commit_wants,
        &haves,
        &shallow,
        deepen,
    )
    .await
    .map_err(Error::from)?;

    // Drop loose wants already covered by the commit walk's bitmaps, dedup by
    // oid. `filter` is deliberately NOT applied: real git still serves a
    // directly-`want`ed blob under `filter blob:none`.
    let mut seen_loose: ObjectHashSet = ObjectHashSet::default();
    let mut loose_wants: Vec<LooseWant> = loose_wants
        .into_iter()
        .filter(|w| w.meta.object_seq.is_none_or(|seq| !walked.contains(seq)))
        .filter(|w| seen_loose.insert(w.oid))
        .collect();

    let keep_trees = filter.is_none_or(|f| f.keep(Kind::Tree));
    let keep_blobs = filter.is_none_or(|f| f.keep(Kind::Blob));

    // Backfill streams as loose wants too — unlike direct wants, `filter`
    // DOES apply here (backfill is server-computed, not client-requested).
    // Folded in before counting so header and streamed set share one pass.
    loose_wants.extend(
        walked
            .backfill
            .into_iter()
            .filter(|(_, meta)| filter.is_none_or(|f| f.keep(meta.kind)))
            .map(|(oid, meta)| LooseWant { oid, meta }),
    );

    // Exact dedup'd count via bitmap cardinality; tags and loose wants
    // (direct + backfill, already filtered above) live outside the packs.
    let outside_packs = u64::try_from(tag_objects.len() + loose_wants.len()).unwrap_or(u64::MAX);
    let total = walked
        .needed
        .object_count(keep_trees, keep_blobs)
        .saturating_add(outside_packs);
    let total_count = u32::try_from(total).map_err(|e| anyhow::anyhow!("pack too large: {e}"))?;

    // Emitted whenever the client engaged the shallow machinery at all, even
    // with empty lists — matches real git's `send_shallow_info`.
    let want_shallow_info = deepen.is_some() || !shallow.is_empty();
    let shallow_info = want_shallow_info.then_some(ShallowInfo {
        shallow: walked.shallow,
        unshallow: walked.unshallow,
    });

    Ok(PackBatch {
        needed: walked.needed.commits,
        filter,
        tag_objects,
        loose_wants,
        total_count,
        shallow_info,
        pass_deltas,
    })
}

/// A blob requested directly by OID — lazy partial-clone backfill.
///
/// Carries only index metadata; bytes stream later via `fetch_loose_entry`,
/// safe standalone even as a delta since that rebuilds it.
#[derive(Clone)]
struct LooseWant {
    oid: ObjectId,
    meta: ObjectMeta,
}

/// Resolve want OIDs into commit SHAs, tag objects, and loose blob wants.
///
/// Tags are peeled recursively until a non-tag target is found and added to
/// `commit_wants`.
///
/// # Errors
///
/// A want not found in the index, or a tag target that fails to fetch.
async fn resolve_wants(
    state: &Storage,
    repo: &RepoMetadata,
    wants: &[ObjectId],
) -> Result<(Vec<ObjectId>, Vec<(ObjectId, Bytes)>, Vec<LooseWant>), Error> {
    let mut commit_wants: Vec<ObjectId> = Vec::new();
    let mut tag_objects: Vec<(ObjectId, Bytes)> = Vec::new();
    let mut loose_wants: Vec<LooseWant> = Vec::new();
    let mut resolved: ObjectHashSet = ObjectHashSet::default();

    // One batched `lookup` per round: `lookup` already resolves commits, so
    // the common case (every want already a known commit) settles in a
    // single round trip. A tag-of-tag chain adds one further (still
    // batched) round per peel.
    let mut pending: Vec<ObjectId> = wants.to_vec();
    while !pending.is_empty() {
        let oids: Vec<ObjectId> = pending
            .drain(..)
            .filter(|&oid| resolved.insert(oid))
            .collect();
        let mut looked_up = enroute_git_retrieve::metas(state, repo.id, &oids).await?;
        for oid in oids {
            // Numbered but never placed is not something this repository can
            // send, so it is refused exactly as a want it never heard of. A
            // tag is the one kind with no location by design.
            let meta = looked_up
                .remove(&oid)
                .filter(ObjectMeta::is_stored)
                .ok_or(enroute_git_core::Error::Missing(oid))?;
            if let Some(target) = classify_want(
                state,
                repo,
                oid,
                meta,
                &mut commit_wants,
                &mut tag_objects,
                &mut loose_wants,
            )
            .await?
            {
                pending.push(target);
            }
        }
    }

    Ok((commit_wants, tag_objects, loose_wants))
}

/// Classify a resolved want into `commit_wants`, `tag_objects`, or
/// `loose_wants` (see [`LooseWant`]).
///
/// Returns a tag's target oid so the caller can resolve it in a further
/// round; never recurses itself.
///
/// # Errors
///
/// `meta.kind` is `Tree`, or a tag's bytes fail to fetch/decode.
async fn classify_want(
    state: &Storage,
    repo: &RepoMetadata,
    oid: ObjectId,
    meta: ObjectMeta,
    commit_wants: &mut Vec<ObjectId>,
    tag_objects: &mut Vec<(ObjectId, Bytes)>,
    loose_wants: &mut Vec<LooseWant>,
) -> Result<Option<ObjectId>, Error> {
    match meta.kind {
        Kind::Tag => {
            let sha = oid.to_hex().to_string();
            let loose = state
                .store
                .get_tag(repo, &sha)
                .await?
                .ok_or_else(|| anyhow::anyhow!("tag {sha} missing from store"))?;
            let (_, content) = decode_loose(&loose)?;
            let ObjectRefs::Tag(target) = object_refs(Kind::Tag, &content)? else {
                return Err(anyhow::anyhow!("tag {sha} does not decode as a tag").into());
            };
            tag_objects.push((oid, loose));
            Ok(Some(target))
        }
        Kind::Commit => {
            // Pushed outside the normal receive-pack path; still serviceable.
            commit_wants.push(oid);
            Ok(None)
        }
        Kind::Blob => {
            loose_wants.push(LooseWant { oid, meta });
            Ok(None)
        }
        Kind::Tree => {
            // A tree's completeness depends on its children too, so serving
            // it as a single loose object could silently return an
            // incomplete result.
            let sha = oid.to_hex().to_string();
            Err(Error::BadRequest(format!(
                "want {sha}: direct tree wants are not supported"
            )))
        }
    }
}

/// Read one self-describing pack entry off an open commit-pack stream,
/// reusing `header_buf`/`body_buf`'s allocations across calls.
///
/// Generic over the reader so a caller can hand it a length-bounded view of
/// one image inside a coalesced run.
async fn read_one_pack_entry<R>(
    reader: &mut R,
    header_buf: &mut BytesMut,
    body_buf: &mut BytesMut,
) -> Result<(enroute_git_store::PackEntryHeader, Bytes), Error>
where
    R: AsyncRead + Unpin,
{
    let mut header_len_byte = [0u8; 1];
    reader
        .read_exact(&mut header_len_byte)
        .await
        .map_err(|e| anyhow::anyhow!("reading pack entry header length: {e}"))?;
    let [header_length_byte] = header_len_byte;
    let header_length = usize::from(header_length_byte);

    header_buf.clear();
    header_buf.extend_from_slice(&header_len_byte);
    header_buf.resize(1 + header_length, 0);
    let (_, rest) = header_buf.split_at_mut(1);
    reader
        .read_exact(rest)
        .await
        .map_err(|e| anyhow::anyhow!("reading pack entry header: {e}"))?;
    let (header, _consumed) = decode_pack_entry_header(header_buf)
        .map_err(|e| anyhow::anyhow!("decoding pack entry header: {e}"))?;

    let compressed_len = usize::try_from(header.compressed_len)
        .map_err(|e| anyhow::anyhow!("compressed_len overflow for {}: {e}", header.sha))?;
    body_buf.resize(compressed_len, 0);
    reader
        .read_exact(body_buf)
        .await
        .map_err(|e| anyhow::anyhow!("reading pack entry body for {}: {e}", header.sha))?;
    // Zero-copy split: keeps `body_buf`'s spare capacity for the next call.
    let bytes = body_buf.split().freeze();

    Ok((header, bytes))
}

/// Fetch and decode one loose want's bytes as a single-item stream, in the
/// same shape as `fetch_commit_run` so it merges into the same pipeline.
///
/// Takes `want` by value: an `&LooseWant` from a `.map()` closure can't
/// unify its borrow lifetime with `state`'s under rustc's inference.
fn fetch_loose_entry<'a>(
    state: &'a Storage,
    repo: &'a RepoMetadata,
    want: LooseWant,
) -> impl Stream<Item = Result<PackEntry, Error>> + 'a {
    async_stream::try_stream! {
        let LooseWant { oid, meta } = want;
        let loc = meta
            .location
            .ok_or(enroute_git_core::Error::Missing(oid))?;
        let entry = state
            .store
            .get_segment_slice(
                repo,
                loc.segment.id,
                loc.segment_offset(),
                Some(loc.image.entry_len),
            )
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("commit pack {} not found", loc.image.pack_sha)
            })?;
        let (header, consumed) = decode_pack_entry_header(&entry)
            .map_err(|e| anyhow::anyhow!("decoding pack entry header: {e}"))?;
        if header.sha != oid {
            Err(anyhow::anyhow!(
                "commit pack {} entry at offset {} has sha {}, expected {}",
                loc.image.pack_sha, loc.image.offset, header.sha, oid
            ))?;
        }
        let bytes = entry.slice(consumed..);
        let body = match header.base {
            None => EntryBody::Stored { length: header.length, compressed: bytes },
            Some(base) => EntryBody::Rebuilt(
                rebuild_entry(state, repo, header.sha, base, header.length, &bytes).await?,
            ),
        };

        yield PackEntry { oid: header.sha, kind: header.kind, body };
    }
}

/// Build an `acknowledgments` section for a round that didn't end in
/// `done`; no packfile sent, client follows up with more `have`s or `done`.
///
/// Acks read the index rather than identity: an ACK stops the client
/// offering the commit, so it has to mean this side can serve from it.
async fn negotiate(
    state: &Storage,
    repo: &RepoMetadata,
    haves: Vec<ObjectId>,
) -> Result<UploadPackResponse, Error> {
    let known: ObjectHashSet = enroute_git_retrieve::metas(state, repo.id, &haves)
        .await?
        .into_iter()
        .filter(|(_, meta)| meta.kind == Kind::Commit && meta.is_stored())
        .map(|(oid, _)| oid)
        .collect();
    // `haves` may carry duplicates (parsing no longer dedups `have` lines up
    // front), so dedup locally to keep one ACK per unique have.
    let mut seen: ObjectHashSet = ObjectHashSet::default();
    let acked: Vec<ObjectId> = haves
        .into_iter()
        .filter(|oid| known.contains(oid) && seen.insert(*oid))
        .collect();

    let mut out = Body::new();
    out.line(b"acknowledgments\n")?;
    if acked.is_empty() {
        out.line(b"NAK\n")?;
    } else {
        for oid in &acked {
            out.line(format!("ACK {oid}\n").as_bytes())?;
        }
    }
    out.flush();

    Ok(UploadPackResponse::Body(out.into_bytes()))
}

/// A partial-clone object filter from the `filter` fetch-command argument.
///
/// `blob:none` is the only spec honored; anything else (`blob:limit=N`,
/// `sparse:oid=...`, `tree:<depth>`, ...) is rejected during parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PackFilter {
    /// `blob:none` — omit all blobs; keep commits and trees.
    BlobNone,
}

impl PackFilter {
    /// Parse a raw filter-spec string (the `filter <spec>` line, LF and
    /// `filter ` prefix already stripped).
    ///
    /// # Errors
    /// `spec` isn't one of the filter-specs git supports.
    fn parse(spec: &str) -> Result<Self, Error> {
        match spec {
            "blob:none" => Ok(Self::BlobNone),
            other => Err(Error::BadRequest(format!("filter '{other}' not supported"))),
        }
    }

    /// Whether an object of `kind` survives this filter.
    fn keep(self, kind: Kind) -> bool {
        match self {
            Self::BlobNone => kind != Kind::Blob,
        }
    }
}

/// The parsed arguments of a `fetch` command request.
struct FetchArgs {
    wants: Vec<ObjectId>,
    haves: Vec<ObjectId>,
    done: bool,
    filter: Option<PackFilter>,
    /// The client's existing shallow-boundary commits (`shallow <oid>`
    /// lines) — sent by already-shallow clients, with or without `deepen`.
    shallow: Vec<ObjectId>,
    /// A `deepen <n>` request: cut the fetch `n` commits deep from each want.
    ///
    /// Other deepen variants are rejected during parsing.
    deepen: Option<u64>,
}

fn parse_fetch_args(args: &[&[u8]]) -> Result<FetchArgs, Error> {
    let mut wants = Vec::new();
    let mut haves = Vec::new();
    let mut done = false;
    let mut filter = None;
    let mut shallow = Vec::new();
    let mut deepen = None;
    for line in args {
        let line = trim_lf(line);
        if let Some(Some(hex)) = line.strip_prefix(b"want ").map(|r| r.get(..40)) {
            wants.push(
                ObjectId::from_hex(hex)
                    .map_err(|e| Error::BadRequest(format!("invalid want oid: {e}")))?,
            );
        } else if let Some(Some(hex)) = line.strip_prefix(b"have ").map(|r| r.get(..40)) {
            haves.push(
                ObjectId::from_hex(hex)
                    .map_err(|e| Error::BadRequest(format!("invalid have oid: {e}")))?,
            );
        } else if line == b"done" {
            done = true;
        } else if let Some(spec) = line.strip_prefix(b"filter ") {
            if filter.is_some() {
                return Err(Error::BadRequest("multiple filter lines".into()));
            }
            let spec = std::str::from_utf8(spec)
                .map_err(|e| Error::BadRequest(format!("invalid filter spec: {e}")))?;
            filter = Some(PackFilter::parse(spec)?);
        } else if let Some(Some(hex)) = line.strip_prefix(b"shallow ").map(|r| r.get(..40)) {
            shallow.push(
                ObjectId::from_hex(hex)
                    .map_err(|e| Error::BadRequest(format!("invalid shallow oid: {e}")))?,
            );
        } else if let Some(rest) = line.strip_prefix(b"deepen ") {
            if deepen.is_some() {
                return Err(Error::BadRequest("multiple deepen lines".into()));
            }
            let depth: u64 = std::str::from_utf8(rest)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| Error::BadRequest("invalid deepen depth".into()))?;
            // Real git never sends a non-positive depth; match that instead
            // of inventing a meaning.
            if depth == 0 {
                return Err(Error::BadRequest("invalid deepen depth".into()));
            }
            deepen = Some(depth);
        } else if line.starts_with(b"deepen-since ")
            || line.starts_with(b"deepen-not ")
            || line == b"deepen-relative"
        {
            // Reject explicitly rather than mis-serve the cut: these need
            // committer-timestamp reads and in-fetch ref resolution.
            let arg = String::from_utf8_lossy(line);
            let arg = arg.split(' ').next().unwrap_or("deepen-arg");
            return Err(Error::BadRequest(format!("'{arg}' is not supported")));
        }
    }
    Ok(FetchArgs {
        wants,
        haves,
        done,
        filter,
        shallow,
        deepen,
    })
}

/// The shallow-info section of a fetch response — emitted (even when both
/// lists are empty) whenever the request engaged the shallow machinery.
#[derive(Debug)]
struct ShallowInfo {
    /// New boundary commits: `shallow <oid>` lines.
    shallow: Vec<ObjectId>,
    /// Client-shallow commits whose parents this fetch sends:
    /// `unshallow <oid>` lines.
    unshallow: Vec<ObjectId>,
}

struct PackBatch {
    /// Commits to stream, carrying the pack metadata `fetch_commit_run`
    /// needs.
    ///
    /// `plan_runs` regroups these by segment position before fetching.
    needed: Vec<NeededCommit>,
    /// The negotiated filter, applied per-commit during streaming (see
    /// `fetch_commit_run`); `total_count` already accounts for it.
    filter: Option<PackFilter>,
    tag_objects: Vec<(ObjectId, Bytes)>,
    /// Blobs wanted directly by OID, plus shallow-fetch backfill objects.
    ///
    /// Both already deduped against the commit walk in `handle_fetch`.
    loose_wants: Vec<LooseWant>,
    total_count: u32,
    /// `Some` when the response must carry a shallow-info section before
    /// the packfile section.
    shallow_info: Option<ShallowInfo>,
    /// Whether stored deltas may go out as-is — see [`may_pass_deltas`].
    pass_deltas: bool,
}

/// Coalesced runs fetched concurrently per streaming packfile.
///
/// A per-request cap on open pack-body streams, not process-wide S3
/// concurrency. High enough a large clone doesn't serialize into waves.
const FETCH_CONCURRENCY: usize = 64;

/// A commit pack's GET bound and entry count under `filter`.
///
/// Irrefutable `BlobNone` match: a new filter variant forces a decision here
/// instead of silently reading to EOF.
const fn read_plan(commit: &NeededCommit, filter: Option<PackFilter>) -> (u64, u32) {
    match filter {
        Some(PackFilter::BlobNone) => (commit.blob_offset, commit.pack_tree_count),
        None => (commit.segment.image_len, commit.pack_object_count),
    }
}

/// Commit-pack images read with a single GET, ascending offset order.
///
/// `head` is split from `images[0]` so the run is non-empty by construction.
struct CommitRun {
    head: NeededCommit,
    rest: Vec<NeededCommit>,
}

impl CommitRun {
    /// A run of one image, which is every run where deltas cannot pass.
    fn of_one(head: NeededCommit) -> Self {
        Self {
            head,
            rest: Vec::new(),
        }
    }

    /// The image the run's GET ends at.
    fn last(&self) -> &NeededCommit {
        self.rest.last().unwrap_or(&self.head)
    }

    /// Every image in the run, head first.
    fn into_images(self) -> impl Iterator<Item = NeededCommit> {
        std::iter::once(self.head).chain(self.rest)
    }
}

/// Group needed commits into runs of near-adjacent images sharing one GET.
///
/// `pass_deltas` false keeps every commit its own run, so a rebuild's base
/// fetch never serializes behind a run-mate's.
fn plan_runs(
    mut needed: Vec<NeededCommit>,
    filter: Option<PackFilter>,
    pass_deltas: bool,
) -> Vec<CommitRun> {
    if !pass_deltas {
        return needed.into_iter().map(CommitRun::of_one).collect();
    }

    needed.sort_unstable_by_key(|c| (c.segment.id, c.segment.base_offset));

    // How far a filtered read goes, not the whole image: a section it skips
    // is a gap, and whether that gap is worth bridging is the same question
    // `runs_of` answers everywhere else.
    let runs = enroute_git_retrieve::runs_of(&needed, |commit| {
        (
            commit.segment.id,
            commit.segment.base_offset,
            read_plan(commit, filter).0,
        )
    });

    let mut images = needed.into_iter();
    runs.into_iter()
        .filter_map(|run| {
            let mut run = images.by_ref().take(run.len());
            Some(CommitRun {
                head: run.next()?,
                rest: run.collect(),
            })
        })
        .collect()
}

/// Advance `reader` past `len` bytes, discarding them.
async fn skip_exact<R>(reader: &mut R, len: u64) -> Result<(), Error>
where
    R: AsyncRead + Unpin,
{
    if len == 0 {
        return Ok(());
    }
    let skipped = tokio::io::copy(&mut (&mut *reader).take(len), &mut tokio::io::sink())
        .await
        .map_err(|e| anyhow::anyhow!("skipping {len} bytes within a commit pack run: {e}"))?;
    if skipped != len {
        return Err(anyhow::anyhow!(
            "commit pack run ended {} bytes before its next image",
            len - skipped
        )
        .into());
    }
    Ok(())
}

/// Fetch and decode a run of commit packs from a single object-store GET.
///
/// The GET is bounded to the run's exact span, so it always runs to end of
/// range and the connection returns to the pool.
fn fetch_commit_run<'a>(
    state: &'a Storage,
    repo: &'a RepoMetadata,
    run: CommitRun,
    filter: Option<PackFilter>,
    pass_deltas: bool,
) -> impl Stream<Item = Result<PackEntry, Error>> + 'a {
    async_stream::try_stream! {
        let head = run.head;
        let (last_len, _) = read_plan(run.last(), filter);
        let run_start = head.segment.base_offset;
        let run_end = run.last().segment.base_offset + last_len;

        let stream = state
            .store
            .stream_segment_slice(repo, head.segment.id, run_start, Some(run_end - run_start))
            .await?
            .ok_or_else(|| anyhow::anyhow!("commit pack {} not found", head.oid))?;
        let mut reader = StreamReader::new(stream);

        // A whole entry's compressed body matches a pack entry body
        // byte-for-byte, so it needs no decompress/recompress round trip.
        let mut header_buf = BytesMut::new();
        let mut body_buf = BytesMut::new();
        let mut pos = run_start;
        for commit in run.into_images() {
            let (read_len, entry_count) = read_plan(&commit, filter);
            skip_exact(&mut reader, commit.segment.base_offset - pos).await?;
            let mut image = (&mut reader).take(read_len);

            let mut file_header = [0u8; enroute_git_store::COMMIT_PACK_HEADER_USIZE];
            image
                .read_exact(&mut file_header)
                .await
                .map_err(|e| anyhow::anyhow!("reading commit pack {} header: {e}", commit.oid))?;
            let header = decode_commit_pack_header(&file_header)
                .map_err(|e| anyhow::anyhow!("commit pack {} header: {e}", commit.oid))?;
            // Fail loudly on a Postgres/pack desync rather than misparse into the trailer.
            if header.object_count != commit.pack_object_count {
                Err(anyhow::anyhow!(
                    "commit pack {} header declares {} objects, but the commit graph expected {}",
                    commit.oid,
                    header.object_count,
                    commit.pack_object_count
                ))?;
            }

            for _ in 0..entry_count {
                let (header, bytes) =
                    read_one_pack_entry(&mut image, &mut header_buf, &mut body_buf).await?;
                let body = match header.base {
                    None => EntryBody::Stored { length: header.length, compressed: bytes },
                    Some(base) if pass_deltas => EntryBody::Delta {
                        base,
                        length: header.length,
                        compressed: bytes,
                    },
                    Some(base) => EntryBody::Rebuilt(
                        rebuild_entry(state, repo, header.sha, base, header.length, &bytes).await?,
                    ),
                };
                yield PackEntry { oid: header.sha, kind: header.kind, body };
            }

            // Leaves the reader on the next image: drops this one's trailer,
            // and under `blob:none` whatever of the tree section went unread.
            tokio::io::copy(&mut image, &mut tokio::io::sink())
                .await
                .map_err(|e| anyhow::anyhow!("draining commit pack {}: {e}", commit.oid))?;
            pos = commit.segment.base_offset + read_len;
        }
    }
}

/// One item of the merged fetch pipeline: a run of commit packs, or a loose
/// want.
///
/// Unifies both so `FETCH_CONCURRENCY` bounds concurrency across every
/// source via one `flatten_unordered` (see `fetch_source`).
enum PackSource {
    Run(CommitRun),
    Loose(LooseWant),
}

/// Dispatch one `PackSource` to `fetch_commit_run` or `fetch_loose_entry`.
///
/// `left_stream`/`right_stream` unify the two branches' distinct stream
/// types so `flatten_unordered` can treat every source alike.
fn fetch_source<'a>(
    state: &'a Storage,
    repo: &'a RepoMetadata,
    source: PackSource,
    filter: Option<PackFilter>,
    pass_deltas: bool,
) -> impl Stream<Item = Result<PackEntry, Error>> + 'a {
    match source {
        PackSource::Run(run) => {
            fetch_commit_run(state, repo, run, filter, pass_deltas).left_stream()
        }
        PackSource::Loose(want) => fetch_loose_entry(state, repo, want).right_stream(),
    }
}

#[tracing::instrument(name = "enroute_git_proto::upload_pack::stream_packs_inner", skip(state, repo, batch, sink), fields(repo_id = %repo.id, object_count = batch.total_count))]
async fn stream_packs_inner(
    state: &Storage,
    repo: &RepoMetadata,
    batch: PackBatch,
    mut sink: PackWriter,
) -> Result<(), Error> {
    let PackBatch {
        needed,
        filter,
        tag_objects,
        loose_wants,
        total_count,
        shallow_info,
        pass_deltas,
    } = batch;
    // These section markers are pkt-line punctuation. A raw consumer is told
    // the same things as typed values before the stream opens, so emitting
    // them here would be sending it a second, differently-shaped copy.
    if sink.framing() == Framing::Sideband {
        let mut header = Body::new();
        // shallow-info precedes packfile per protocol v2 section order
        // (acknowledgments, shallow-info, wanted-refs, packfile-uris,
        // packfile), separated by a delim-pkt.
        if let Some(info) = shallow_info {
            header.line(b"shallow-info\n")?;
            for oid in &info.shallow {
                header.line(format!("shallow {oid}\n").as_bytes())?;
            }
            for oid in &info.unshallow {
                header.line(format!("unshallow {oid}\n").as_bytes())?;
            }
            header.delim();
        }
        header.line(b"packfile\n")?;
        sink.send_raw(Bytes::from(header.into_bytes())).await?;
    }

    let mut pack_header = Vec::with_capacity(12);
    write_pack_header(total_count, &mut pack_header);
    sink.write(&pack_header).await?;

    // Emit annotated tag objects first.
    for (_, loose) in &tag_objects {
        let (kind, content) = decode_loose(loose)?;
        let mut entry = Vec::new();
        write_pack_entry(kind, &content, &mut entry)?;
        sink.write(&entry).await?;
    }

    // Objects arrive interleaved — fine, since every entry reaches the wire
    // whole. `seen` dedups OIDs emitted more than once, checked here since
    // only this merged consumer has the global view.
    let mut seen: ObjectHashSet = ObjectHashSet::default();

    // `loose_wants` goes first: `flatten_unordered` fills its concurrency
    // budget in source order, so a large `needed` would otherwise starve
    // the smaller direct-want/backfill GETs.
    let sources = loose_wants.into_iter().map(PackSource::Loose).chain(
        plan_runs(needed, filter, pass_deltas)
            .into_iter()
            .map(PackSource::Run),
    );
    let mut merged = stream::iter(sources)
        .map(|source| Box::pin(fetch_source(state, repo, source, filter, pass_deltas)))
        .flatten_unordered(Some(FETCH_CONCURRENCY));

    // A rebuilt entry has no stored zlib bytes to reuse, so it is compressed
    // here; the other two forms carry theirs.
    let mut header = Vec::new();
    let mut rebuilt = Vec::new();
    while let Some(entry) = merged.next().await {
        let PackEntry { oid, kind, body } = entry?;
        if !seen.insert(oid) {
            continue;
        }
        match body {
            EntryBody::Stored { length, compressed } => {
                header.clear();
                write_pack_entry_header(kind, length, &mut header)?;
                sink.write(&header).await?;
                sink.write_bytes(compressed).await?;
            }
            EntryBody::Delta {
                base,
                length,
                compressed,
            } => {
                header.clear();
                write_ref_delta_header(base, length, &mut header)?;
                sink.write(&header).await?;
                sink.write_bytes(compressed).await?;
            }
            EntryBody::Rebuilt(content) => {
                rebuilt.clear();
                write_pack_entry(kind, &content, &mut rebuilt)?;
                sink.write(&rebuilt).await?;
            }
        }
    }

    sink.finish().await
}

async fn stream_packs(
    state: Storage,
    repo: RepoMetadata,
    batch: PackBatch,
    mut tx: SidebandTx,
    framing: Framing,
    meter: Arc<Meter>,
    cost: tracing::Span,
) {
    let sink = PackWriter::new(tx.clone(), framing);
    if let Err(e) = stream_packs_inner(&state, &repo, batch, sink).await {
        match framing {
            Framing::Sideband => send_sideband_error(&e.to_string(), &mut tx).await,
            Framing::Raw => send_raw_error(&e.to_string(), &mut tx).await,
        }
    }
    // Recorded however the stream ended: a fetch that failed halfway still
    // read everything it had read by then, and that is still billed.
    meter.units().record_on(&cost);
}

#[cfg(test)]
mod tests {
    use enroute_git_test_support::{
        find_sideband3_error, make_faulty_state, make_state, pack_entries, seed_commit,
    };

    use futures::StreamExt as _;

    use enroute_git_retrieve::RUN_GAP_BYTES;

    use super::{Body, UploadPackResponse, plan_runs, upload_pack};
    use crate::visibility::AllRefsVisible;
    use enroute_git_ingest::Actor;
    use enroute_git_retrieve::{NeededCommit, RepoMetadata};
    use enroute_git_test_support::debug_pktlines;

    /// A `NeededCommit` carrying only what `plan_runs` reads: where its image
    /// sits, and where a `blob:none` read of it would stop.
    fn placed(segment: u64, base_offset: u64, image_len: u64, blob_offset: u64) -> NeededCommit {
        NeededCommit {
            oid: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
            segment: enroute_git_core::SegmentLocation {
                id: enroute_git_core::Ulid::from_parts(segment, 0),
                base_offset,
                image_len,
            },
            pack_object_count: 0,
            pack_tree_count: 0,
            blob_offset,
        }
    }

    /// The offsets each planned run covers, for comparing against intent.
    fn run_offsets(runs: &[super::CommitRun]) -> Vec<Vec<u64>> {
        runs.iter()
            .map(|run| {
                std::iter::once(run.head.segment.base_offset)
                    .chain(run.rest.iter().map(|c| c.segment.base_offset))
                    .collect()
            })
            .collect()
    }

    /// The clone case: images tiling one segment collapse into one GET, and
    /// walk order doesn't matter — offsets, not input order, drive grouping.
    #[test]
    fn plan_runs_coalesces_adjacent_images() {
        let runs = plan_runs(
            vec![
                placed(1, 200, 100, 50),
                placed(1, 0, 100, 50),
                placed(1, 100, 100, 50),
            ],
            None,
            true,
        );
        assert_eq!(run_offsets(&runs), vec![vec![0, 100, 200]]);
    }

    /// A truncating fetch's commits stay one per run, not serialized behind
    /// each other's awaited base fetch.
    #[test]
    fn plan_runs_leaves_rebuilding_fetches_uncoalesced() {
        let tiled = vec![
            placed(1, 0, 100, 50),
            placed(1, 100, 100, 50),
            placed(1, 200, 100, 50),
        ];
        assert_eq!(
            run_offsets(&plan_runs(tiled.clone(), None, true)),
            vec![vec![0, 100, 200]],
        );
        assert_eq!(
            run_offsets(&plan_runs(tiled, None, false)),
            vec![vec![0], vec![100], vec![200]],
        );
    }

    /// Under `blob:none` a read stops at `blob_offset`, so each skipped blob
    /// section becomes a gap the same break-even decides.
    ///
    /// An ordinary repo's sections sit well inside `RUN_GAP_BYTES`, so
    /// bridging beats a round trip per commit even though bytes are discarded.
    #[test]
    fn plan_runs_bridges_ordinary_blob_sections() {
        let image = 10 * 1024;
        let kept = 1024;
        let runs = plan_runs(
            vec![
                placed(1, 0, image, kept),
                placed(1, image, image, kept),
                placed(1, 2 * image, image, kept),
            ],
            Some(PackFilter::BlobNone),
            true,
        );
        assert_eq!(run_offsets(&runs), vec![vec![0, image, 2 * image]]);
    }

    /// A repo of large binaries is the case worth splitting for: skipping a
    /// section that big costs more transfer than the round trip it saves.
    #[test]
    fn plan_runs_splits_across_oversized_blob_sections() {
        let image = 4 * RUN_GAP_BYTES;
        let kept = 1024;
        let runs = plan_runs(
            vec![
                placed(1, 0, image, kept),
                placed(1, image, image, kept),
                placed(1, 2 * image, image, kept),
            ],
            Some(PackFilter::BlobNone),
            true,
        );
        assert_eq!(
            run_offsets(&runs),
            vec![vec![0], vec![image], vec![2 * image]],
            "a blob section past the break-even must break the run"
        );

        // Unfiltered, the same images read end to end with no gap at all, so
        // the split above is the filter's doing rather than the layout's.
        let joined = plan_runs(
            vec![
                placed(1, 0, image, kept),
                placed(1, image, image, kept),
                placed(1, 2 * image, image, kept),
            ],
            None,
            true,
        );
        assert_eq!(run_offsets(&joined), vec![vec![0, image, 2 * image]]);
    }

    /// A clone of a segment-tiled chain must read it in one GET, not one
    /// per commit — the point of `plan_runs`.
    #[tokio::test]
    async fn upload_pack_fetch_coalesces_tiled_commit_packs() {
        let (state, faulty) = make_faulty_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commits = enroute_git_test_support::seed_commit_chain_tiled(&state, &repo, 20, 0).await;
        let tip = commits.last().unwrap().clone();

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {tip}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        let error = find_sideband3_error(&raw);
        assert!(error.is_none(), "unexpected sideband-3 error: {error:?}");

        // Every object still arrives exactly once: coalescing changes how the
        // bytes are read, never which objects the pack carries.
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 60, "entries:\n{entries}");

        let bounded: Vec<_> = faulty
            .recorded_ranges()
            .into_iter()
            .flatten()
            .filter_map(|r| match r {
                object_store::GetRange::Bounded(range) => Some(range),
                _ => None,
            })
            .collect();
        assert_eq!(
            bounded.len(),
            1,
            "20 tiled commit packs must be read in a single GET, got {bounded:?}"
        );
        assert_eq!(bounded[0].start, 0, "the run must start at the first image");
    }

    /// On a tiled segment a `blob:none` read's skipped blobs become gaps.
    ///
    /// Sections far past the break-even must still break the runs, so a
    /// repo of large binaries isn't read and thrown away wholesale.
    #[tokio::test]
    async fn upload_pack_fetch_filter_blob_none_splits_on_large_blobs() {
        let (state, faulty) = make_faulty_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let blob_bytes = 4 * 32 * 1024;
        let commits =
            enroute_git_test_support::seed_commit_chain_tiled(&state, &repo, 5, blob_bytes).await;
        let tip = commits.last().unwrap().clone();

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {tip}\n").as_bytes(),
            b"filter blob:none\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        // 5 commits × (commit + tree), no blobs.
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 10, "entries:\n{entries}");

        let fetched: u64 = faulty
            .recorded_ranges()
            .into_iter()
            .flatten()
            .filter_map(|r| match r {
                object_store::GetRange::Bounded(range) => Some(range.end - range.start),
                _ => None,
            })
            .sum();
        // Against one blob, not all five: crossing even a single blob
        // section is the regression, and the kept commit+tree bytes are
        // orders of magnitude smaller than that.
        let one_blob = u64::try_from(blob_bytes).expect("blob size fits a u64");
        assert!(
            fetched < one_blob,
            "a filtered fetch read {fetched} bytes, past the {one_blob} of a single blob \
             it is supposed to skip — the runs coalesced across them"
        );
    }

    /// Encodes a command-request: `lines[0]` as `command=...`, then a
    /// delim-pkt, then the rest of `lines` as command-args, then a flush.
    fn encode_lines(lines: &[&[u8]]) -> Vec<u8> {
        let mut body = Body::new();
        let (command, args) = lines.split_first().expect("at least a command line");
        body.line(command).unwrap();
        body.delim();
        for line in args {
            body.line(line).unwrap();
        }
        body.flush();
        body.into_bytes()
    }

    async fn collect_pack(
        rx: futures::channel::mpsc::Receiver<Result<bytes::Bytes, std::io::Error>>,
    ) -> Vec<u8> {
        rx.map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flat_map(|b| b.to_vec())
            .collect()
    }

    async fn do_upload_pack(
        state: enroute_git_retrieve::Storage,
        repo: &RepoMetadata,
        body: &[u8],
    ) -> UploadPackResponse {
        upload_pack(
            state,
            repo.clone(),
            body,
            &AllRefsVisible,
            &Actor::new("tester"),
        )
        .await
        .unwrap()
    }

    // ── cost accounting ──────────────────────────────────────────────────────

    /// A fetch accounts for what it read from the spawned task that streams
    /// the pack, long after `upload_pack` has returned.
    ///
    /// Guards the join between two lists the compiler cannot: the field names
    /// `#[instrument]` declares, and the ones `enroute_git_cost` records.
    #[tokio::test]
    async fn a_fetch_records_every_cost_field_on_its_span() {
        // Held across the awaits below so the span `upload_pack` opens is
        // created against this subscriber rather than a global one.
        let (recorded, _guard) = enroute_git_test_support::capture_recorded_fields();

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"done\n",
        ]);
        let UploadPackResponse::Pack(rx) = do_upload_pack(state, &repo, &body).await else {
            panic!("expected Pack result");
        };
        // Cost is recorded once the stream ends, so draining it is what makes
        // the fields appear.
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none(), "fetch failed");

        recorded.assert_all_recorded(&enroute_git_cost::FIELD_NAMES);

        // Serving a pack means reading segments out of the primary store, so a
        // zero here would mean the meter never reached the code that spends.
        assert!(
            recorded
                .get(enroute_git_cost::PRIMARY_GET_CLASS)
                .unwrap_or(0)
                > 0,
            "a fetch that served a pack read nothing",
        );
        assert!(
            recorded
                .get(enroute_git_cost::PRIMARY_BYTES_READ)
                .unwrap_or(0)
                > 0,
            "a fetch that served a pack transferred nothing",
        );
        // Nothing on the fetch path stages or invokes a worker.
        assert_eq!(recorded.get(enroute_git_cost::HANDOFF_PUT_CLASS), Some(0));
        assert_eq!(recorded.get(enroute_git_cost::LAMBDA_INVOCATIONS), Some(0));
    }

    #[tokio::test]
    async fn upload_pack_ls_refs_empty_repo() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let body = encode_lines(&[b"command=ls-refs\n"]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Body(body) = result else {
            panic!("expected Body result");
        };
        insta::assert_snapshot!(debug_pktlines(&body), @"[flush]");
    }

    /// A command-request missing its delim-pkt is malformed per protocol v2
    /// grammar and must be rejected, matching real git-upload-pack servers.
    #[tokio::test]
    async fn upload_pack_ls_refs_missing_delim_is_rejected() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let mut body = Body::new();
        body.line(b"command=ls-refs\n").unwrap();
        body.flush();
        let body = body.into_bytes();

        let err = upload_pack(state, repo, &body, &AllRefsVisible, &Actor::new("tester"))
            .await
            .unwrap_err();
        insta::assert_snapshot!(err.to_string(), @"bad request: command-request missing delim-pkt (0001)");
    }

    #[tokio::test]
    async fn upload_pack_ls_refs_empty_repo_unborn() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let body = encode_lines(&[b"command=ls-refs\n", b"unborn\n"]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Body(body) = result else {
            panic!("expected Body result");
        };
        insta::assert_snapshot!(debug_pktlines(&body), @"
        unborn HEAD symref-target:refs/heads/main
        [flush]
        ");
    }

    #[tokio::test]
    async fn upload_pack_ls_refs_seeded_repo() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        seed_commit(&state, &repo).await;
        let body = encode_lines(&[b"command=ls-refs\n"]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Body(body) = result else {
            panic!("expected Body result");
        };
        insta::assert_snapshot!(debug_pktlines(&body), @"
        dcfacca1eeed1fb3b027b9a52ef16721c699c490 HEAD
        dcfacca1eeed1fb3b027b9a52ef16721c699c490 refs/heads/main
        [flush]
        ");
    }

    #[tokio::test]
    async fn upload_pack_fetch_sideband3_on_storage_error() {
        let (state, faulty) = make_faulty_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        // A fetch issues exactly one S3 get per commit pack (header+entries
        // in one GET; the commit graph itself is a Postgres query) —
        // failing the first get exercises the storage-error path.
        faulty.fail_gets_after(0);

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        let error = find_sideband3_error(&raw);
        assert!(error.is_some(), "expected a sideband-3 error packet");
        insta::assert_snapshot!(error.unwrap(), @"streaming segment slice");
    }

    #[tokio::test]
    async fn upload_pack_fetch_returns_pack() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        let error = find_sideband3_error(&raw);
        assert!(error.is_none(), "unexpected sideband-3 error: {error:?}");
        let entries = pack_entries(&raw);
        insta::assert_snapshot!(entries, @"
        blob 4b5fa63702dd96796042e92787f464e28f09f17d
        commit dcfacca1eeed1fb3b027b9a52ef16721c699c490
        tree 82ad2dff9cb502d849a8e74f6a4f8f1291c173fc
        ");
    }

    /// Regression test for concurrent commit-pack fan-in.
    ///
    /// Fetches more commits than `FETCH_CONCURRENCY` so several GETs are
    /// genuinely in flight, checking every object emits exactly once.
    #[tokio::test]
    async fn upload_pack_fetch_many_commits_concurrent() {
        use enroute_git_test_support::seed_commit_chain;

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commits = seed_commit_chain(&state, &repo, 20).await;
        let tip = commits.last().unwrap().clone();

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {tip}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        let error = find_sideband3_error(&raw);
        assert!(error.is_none(), "unexpected sideband-3 error: {error:?}");

        // 20 commits × (commit+tree+blob) = 60 unique objects;
        // `pack_entries` sorts, so a dedup regression shows as
        // extra/repeated lines.
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 60, "entries:\n{entries}");
        let unique: std::collections::HashSet<&str> = entries.lines().collect();
        assert_eq!(unique.len(), 60, "duplicate object emitted:\n{entries}");
    }

    /// A blob spanning several sideband frames exercises the zero-copy
    /// slicing path, not the small-object coalescing buffer.
    ///
    /// `pack_entries` re-hashes every entry, so a slicing bug shows up as a
    /// parse failure or wrong OID.
    #[tokio::test]
    async fn upload_pack_fetch_large_blob_spans_frames() {
        use gix_object::Kind;

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        // ~512 KiB of xorshift output: incompressible, so the zlib entry
        // exceeds one 65515-byte sideband frame.
        let mut content = Vec::with_capacity(512 * 1024);
        let mut x: u32 = 0x9e37_79b9;
        while content.len() < 512 * 1024 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            content.extend_from_slice(&x.to_le_bytes());
        }
        let commit_sha =
            enroute_git_test_support::seed_commit_with_blob(&state, &repo, &content).await;
        let (blob_oid, _) = enroute_git_core::encode_loose(Kind::Blob, &content).unwrap();

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        let error = find_sideband3_error(&raw);
        assert!(error.is_none(), "unexpected sideband-3 error: {error:?}");

        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 3, "entries:\n{entries}");
        assert!(
            entries.contains(&format!("blob {}", blob_oid.to_hex())),
            "large blob must round-trip byte-identically; entries:\n{entries}"
        );
    }

    #[tokio::test]
    async fn upload_pack_fetch_negotiation_round_acks_known_have() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        // `have` for a known object, no `done` — expects an acknowledgments
        // round, not a packfile.
        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            format!("have {commit_sha}\n").as_bytes(),
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Body(body) = result else {
            panic!("expected Body result");
        };
        insta::assert_snapshot!(debug_pktlines(&body), @r"
        acknowledgments
        ACK dcfacca1eeed1fb3b027b9a52ef16721c699c490
        [flush]
        ");
    }

    #[tokio::test]
    async fn upload_pack_fetch_negotiation_round_naks_unknown_have() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"have 0000000000000000000000000000000000000000\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Body(body) = result else {
            panic!("expected Body result");
        };
        insta::assert_snapshot!(debug_pktlines(&body), @r"
        acknowledgments
        NAK
        [flush]
        ");
    }

    // ── filter parsing ───────────────────────────────────────────────────

    use super::PackFilter;

    #[test]
    fn pack_filter_parses_supported_specs() {
        assert_eq!(
            PackFilter::parse("blob:none").unwrap(),
            PackFilter::BlobNone
        );
    }

    #[test]
    fn pack_filter_rejects_unsupported_specs() {
        for spec in [
            "blob:limit=100",
            "sparse:oid=abc",
            "combine:blob:none+tree:0",
            "tree:0",
            "tree:1",
        ] {
            let err = PackFilter::parse(spec).unwrap_err();
            assert_eq!(
                err.to_string(),
                format!("bad request: filter '{spec}' not supported")
            );
        }
    }

    #[tokio::test]
    async fn upload_pack_fetch_filter_unsupported_spec_rejected() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"filter blob:limit=100\n",
            b"done\n",
        ]);

        let err = upload_pack(state, repo, &body, &AllRefsVisible, &Actor::new("tester"))
            .await
            .unwrap_err();
        insta::assert_snapshot!(err.to_string(), @"bad request: filter 'blob:limit=100' not supported");
    }

    #[tokio::test]
    async fn upload_pack_fetch_filter_duplicate_line_rejected() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"filter blob:none\n",
            b"filter blob:none\n",
            b"done\n",
        ]);

        let err = upload_pack(state, repo, &body, &AllRefsVisible, &Actor::new("tester"))
            .await
            .unwrap_err();
        insta::assert_snapshot!(err.to_string(), @"bad request: multiple filter lines");
    }

    // ── filtered fetch (integration) ─────────────────────────────────────
    // `blob:none` filtering is implicit in `fetch_commit_run`'s entry-count
    // bound, so it has no unit test of its own.

    #[tokio::test]
    async fn upload_pack_fetch_filter_blob_none_omits_blobs() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"filter blob:none\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        let error = find_sideband3_error(&raw);
        assert!(error.is_none(), "unexpected sideband-3 error: {error:?}");
        let entries = pack_entries(&raw);
        insta::assert_snapshot!(entries, @"
        commit dcfacca1eeed1fb3b027b9a52ef16721c699c490
        tree 82ad2dff9cb502d849a8e74f6a4f8f1291c173fc
        ");
    }

    #[tokio::test]
    async fn upload_pack_fetch_filter_blob_none_multi_commit_chain() {
        use enroute_git_test_support::seed_commit_chain;

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commits = seed_commit_chain(&state, &repo, 5).await;
        let tip = commits.last().unwrap().clone();

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {tip}\n").as_bytes(),
            b"filter blob:none\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        let error = find_sideband3_error(&raw);
        assert!(error.is_none(), "unexpected sideband-3 error: {error:?}");

        // 5 commits × (commit+tree, blob dropped) = 10.
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 10, "entries:\n{entries}");
        assert!(
            !entries.contains("blob "),
            "blob:none must omit every blob; entries:\n{entries}"
        );
    }

    /// A `blob:none` fetch must bound its GET to `blob_offset`, not merely
    /// discard blob bytes after an unbounded tail read.
    #[tokio::test]
    async fn upload_pack_fetch_filter_blob_none_bounds_the_range_get() {
        let (state, faulty) = make_faulty_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        // Large and incompressible, so a range including it is unmistakably
        // larger than one that stops at the tree.
        let content: Vec<u8> = (0..64 * 1024_u32)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let commit_sha =
            enroute_git_test_support::seed_commit_with_blob(&state, &repo, &content).await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"filter blob:none\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        // The pack is fetched in one GET from offset 0, bounded to
        // `blob_offset` — confirm it stops before the blob, so the bytes
        // are never fetched at all.
        let ranges = faulty.recorded_ranges();
        let pack_get = ranges
            .iter()
            .filter_map(|r| r.as_ref())
            .find_map(|r| match r {
                object_store::GetRange::Bounded(range) => Some(range),
                _ => None,
            })
            .expect("blob:none fetch must issue a single bounded range GET for the commit pack");
        assert_eq!(
            pack_get.start, 0,
            "the coalesced commit-pack GET must start at offset 0 (header + kept entries)"
        );
        let bound = pack_get.end - pack_get.start;
        assert!(
            bound < u64::try_from(content.len()).unwrap(),
            "bounded range ({bound} bytes) should stop before the {}-byte blob, \
             not include it",
            content.len()
        );
    }

    // ── lazy single-object backfill (partial-clone) ──────────────────────

    /// Recomputes the OIDs `seed_commit_with_blob` would have produced for
    /// `blob_content`, without needing the commit itself.
    fn expected_blob_and_tree_oid(blob_content: &[u8]) -> (gix_hash::ObjectId, gix_hash::ObjectId) {
        use gix_object::Kind;

        let (blob_oid, _) = enroute_git_core::encode_loose(Kind::Blob, blob_content).unwrap();
        let mut tree_content = b"100644 hello.txt\0".to_vec();
        tree_content.extend_from_slice(blob_oid.as_slice());
        let (tree_oid, _) = enroute_git_core::encode_loose(Kind::Tree, &tree_content).unwrap();
        (blob_oid, tree_oid)
    }

    #[tokio::test]
    async fn upload_pack_fetch_lazy_blob_backfill() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        seed_commit(&state, &repo).await;
        let (blob_oid, _) = expected_blob_and_tree_oid(b"hello, world\n");

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {blob_oid}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());
        let entries = pack_entries(&raw);
        assert_eq!(entries, format!("blob {blob_oid}"));
    }

    /// Direct tree wants are rejected outright — see `PackFilter` and the
    /// `Kind::Tree` arm of `resolve_wants` for why.
    #[tokio::test]
    async fn upload_pack_fetch_lazy_tree_want_rejected() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        seed_commit(&state, &repo).await;
        let (_, tree_oid) = expected_blob_and_tree_oid(b"hello, world\n");

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {tree_oid}\n").as_bytes(),
            b"done\n",
        ]);

        let err = upload_pack(state, repo, &body, &AllRefsVisible, &Actor::new("tester"))
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("bad request: want {tree_oid}: direct tree wants are not supported")
        );
    }

    /// A commit want and an independent loose blob want in the same fetch.
    ///
    /// Both paths must contribute to the response pack with nothing dropped.
    #[tokio::test]
    async fn upload_pack_fetch_mixed_commit_and_lazy_blob_wants() {
        use gix_object::Kind;

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_a = seed_commit(&state, &repo).await;

        // Seeded directly into the store/index rather than via a second
        // commit, to avoid a second ref update real git would reject as
        // non-fast-forward.
        let (blob_b, blob_b_loose) =
            enroute_git_core::encode_loose(Kind::Blob, b"independent blob\n").unwrap();
        enroute_git_test_support::put_loose_as_singleton_pack(
            &state,
            &repo,
            &blob_b.to_hex().to_string(),
            blob_b_loose.into(),
        )
        .await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_a}\n").as_bytes(),
            format!("want {blob_b}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        // commit_a's commit+tree+blob (3) + independent blob_b (1) = 4.
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 4, "entries:\n{entries}");
        assert!(
            entries.contains(&format!("blob {blob_b}")),
            "entries:\n{entries}"
        );
    }

    /// A blob requested both directly and via a reachable commit must be
    /// emitted exactly once.
    #[tokio::test]
    async fn upload_pack_fetch_lazy_blob_already_in_commit_wants_no_duplicate() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;
        let (blob_oid, _) = expected_blob_and_tree_oid(b"hello, world\n");

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            format!("want {blob_oid}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 3, "entries:\n{entries}"); // commit + tree + blob, no duplicate
        let unique: std::collections::HashSet<&str> = entries.lines().collect();
        assert_eq!(unique.len(), 3, "duplicate object emitted:\n{entries}");
    }

    /// A negotiated filter must not apply to a directly-wanted (loose) blob,
    /// unlike commit-walked pack entries.
    ///
    /// Regression test: enroute once dropped the blob too, breaking every
    /// partial-clone client's first checkout of a missing blob.
    #[tokio::test]
    async fn upload_pack_fetch_filter_blob_none_keeps_direct_blob_want() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        seed_commit(&state, &repo).await;
        let (blob_oid, _) = expected_blob_and_tree_oid(b"hello, world\n");

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {blob_oid}\n").as_bytes(),
            b"filter blob:none\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());
        let entries = pack_entries(&raw);
        assert_eq!(
            entries,
            format!("blob {blob_oid}"),
            "blob:none must not drop a directly-wanted blob"
        );
    }

    // ── shallow (deepen) fetches ───────────────────────────────────────────

    use enroute_git_test_support::seed_commit_chain_accumulating;

    /// Whether pkt-encoded line `s` appears framed with its own pkt-line
    /// length prefix, not merely as a text substring.
    fn pkt_line_present(raw: &[u8], s: &str) -> bool {
        let encoded = format!("{:04x}{s}", s.len() + 4);
        raw.windows(encoded.len()).any(|w| w == encoded.as_bytes())
    }

    fn commit_line_count(entries: &str) -> usize {
        entries.lines().filter(|l| l.starts_with("commit ")).count()
    }

    #[tokio::test]
    async fn upload_pack_fetch_deepen_one_sends_complete_boundary_snapshot() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let chain = seed_commit_chain_accumulating(&state, &repo, 3).await;
        let tip = chain.last().unwrap().clone();

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {tip}\n").as_bytes(),
            b"deepen 1\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        // Tip's own commit+tree+blob, plus the two ancestor blobs its flat
        // tree references directly — no extra trees needed. Only the tip's
        // commit object: a depth-1 clone must not receive commits it can't
        // connect.
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 5, "entries:\n{entries}");
        assert_eq!(
            commit_line_count(&entries),
            1,
            "only the boundary commit itself, no ancestor commits: {entries}"
        );

        assert!(pkt_line_present(&raw, "shallow-info\n"));
        assert!(pkt_line_present(&raw, &format!("shallow {tip}\n")));
        assert!(
            !raw.windows(b"unshallow ".len()).any(|w| w == b"unshallow "),
            "a plain clone has nothing to unshallow"
        );
    }

    #[tokio::test]
    async fn upload_pack_fetch_deepen_deeper_than_history_is_full_clone() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let chain = seed_commit_chain_accumulating(&state, &repo, 3).await;
        let tip = chain.last().unwrap().clone();

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {tip}\n").as_bytes(),
            b"deepen 10\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        // 3 commits × (tree+blob) = 9 objects, no backfill (roots are never
        // boundary).
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 9, "entries:\n{entries}");
        assert_eq!(commit_line_count(&entries), 3);

        assert!(pkt_line_present(&raw, "shallow-info\n"));
        assert!(
            !raw.windows(b"shallow ".len()).any(|w| w == b"shallow "),
            "deepening past the root has no boundary commit to report"
        );
    }

    #[tokio::test]
    async fn upload_pack_fetch_deepen_blob_none_omits_backfill_blobs() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let chain = seed_commit_chain_accumulating(&state, &repo, 3).await;
        let tip = chain.last().unwrap().clone();

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {tip}\n").as_bytes(),
            b"deepen 1\n",
            b"filter blob:none\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        // Just the tip's own commit+tree: `blob:none` applies to backfill
        // too, and this fixture's backfill was blobs-only (see the
        // unfiltered version of this test), so nothing survives the filter.
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 2, "entries:\n{entries}");
        assert_eq!(commit_line_count(&entries), 1);
        assert!(!entries.contains("blob "), "entries:\n{entries}");
    }

    /// A nested reused subdirectory forces the boundary snapshot to recurse,
    /// proving `tree_closure` walks more than one BFS level.
    #[tokio::test]
    async fn upload_pack_fetch_deepen_backfill_includes_inherited_subtree() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let (_, commit1_sha, subtree_oid, blob0_oid) =
            enroute_git_test_support::seed_commit_pair_with_inherited_subtree(&state, &repo).await;

        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit1_sha}\n").as_bytes(),
            b"deepen 1\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        // Tip's own commit+root-tree+top.txt blob, plus the inherited `dir`
        // subtree and its blob, discovered by recursing past the boundary
        // tree's immediate children.
        let entries = pack_entries(&raw);
        assert_eq!(entries.lines().count(), 5, "entries:\n{entries}");
        assert_eq!(commit_line_count(&entries), 1);
        assert!(
            entries.contains(&format!("tree {subtree_oid}")),
            "entries:\n{entries}"
        );
        assert!(
            entries.contains(&format!("blob {blob0_oid}")),
            "entries:\n{entries}"
        );
    }

    #[tokio::test]
    async fn upload_pack_fetch_deepen_with_client_shallow_unshallows() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let chain = seed_commit_chain_accumulating(&state, &repo, 3).await;
        let (c0, c1, c2) = (chain[0].clone(), chain[1].clone(), chain[2].clone());

        // Client holds a depth-1 shallow clone at tip c2. Deepening fully
        // makes c1/c0 reachable and c2 is no longer boundary, so the
        // response must carry `unshallow c2` and the history below it.
        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {c2}\n").as_bytes(),
            format!("have {c2}\n").as_bytes(),
            format!("shallow {c2}\n").as_bytes(),
            b"deepen 10\n",
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());

        assert!(pkt_line_present(&raw, &format!("unshallow {c2}\n")));
        assert!(
            !pkt_line_present(&raw, &format!("shallow {c2}\n")),
            "c2 gained its parent, so it must not also appear as a shallow line"
        );

        // The two commits below the old shallow point, each with their own
        // new blob (c0's blob is also in c1's tree, but already reachable
        // from c1's own pack — no backfill needed).
        let entries = pack_entries(&raw);
        assert_eq!(commit_line_count(&entries), 2, "entries:\n{entries}");
        assert!(entries.contains(&format!("commit {c0}")), "{entries}");
        assert!(entries.contains(&format!("commit {c1}")), "{entries}");
        assert!(!entries.contains(&format!("commit {c2}")), "{entries}");
    }

    #[tokio::test]
    async fn upload_pack_fetch_shallow_lines_without_deepen_stay_shallow() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let chain = seed_commit_chain_accumulating(&state, &repo, 3).await;
        let (c1, c2) = (chain[1].clone(), chain[2].clone());

        // `have c1` is a deliberately inconsistent signal, to prove the
        // *want* graft, not `have`, is what stops the descent.
        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {c2}\n").as_bytes(),
            format!("have {c1}\n").as_bytes(),
            format!("shallow {c2}\n").as_bytes(),
            b"done\n",
        ]);

        let result = do_upload_pack(state, &repo, &body).await;
        let UploadPackResponse::Pack(rx) = result else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(find_sideband3_error(&raw).is_none());
        assert!(pkt_line_present(&raw, "shallow-info\n"));

        let entries = pack_entries(&raw);
        assert_eq!(
            commit_line_count(&entries),
            1,
            "only c2's own snapshot, entries:\n{entries}"
        );
        assert!(entries.contains(&format!("commit {c2}")), "{entries}");
        assert!(
            !entries.contains(&format!("commit {c1}")),
            "want graft must not expand past c2: {entries}"
        );
    }

    #[tokio::test]
    async fn upload_pack_fetch_deepen_arg_validation() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let commit_sha = seed_commit(&state, &repo).await;

        let cases: &[(&[u8], &str)] = &[
            (b"deepen 0\n", "bad request: invalid deepen depth"),
            (
                b"deepen not-a-number\n",
                "bad request: invalid deepen depth",
            ),
            (
                b"deepen-since 12345\n",
                "bad request: 'deepen-since' is not supported",
            ),
            (
                b"deepen-not refs/heads/main\n",
                "bad request: 'deepen-not' is not supported",
            ),
            (
                b"deepen-relative\n",
                "bad request: 'deepen-relative' is not supported",
            ),
        ];
        for (line, expected) in cases {
            let body = encode_lines(&[
                b"command=fetch\n",
                format!("want {commit_sha}\n").as_bytes(),
                line,
                b"done\n",
            ]);
            let err = upload_pack(
                state.clone(),
                repo.clone(),
                &body,
                &AllRefsVisible,
                &Actor::new("tester"),
            )
            .await
            .unwrap_err();
            assert_eq!(err.to_string(), *expected, "case: {line:?}");
        }

        // A second `deepen` line is rejected outright.
        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {commit_sha}\n").as_bytes(),
            b"deepen 1\n",
            b"deepen 2\n",
            b"done\n",
        ]);
        let err = upload_pack(state, repo, &body, &AllRefsVisible, &Actor::new("tester"))
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "bad request: multiple deepen lines");
    }

    /// A tree of `fillers` (named so they sort first) plus `blob_oid` at `f`,
    /// which puts `blob_oid` last in the commit pack's blob section.
    fn tree_with_fillers(
        fillers: &[gix_hash::ObjectId],
        blob_oid: gix_hash::ObjectId,
    ) -> (gix_hash::ObjectId, Vec<u8>) {
        let names: Vec<String> = (0..fillers.len()).map(|i| format!("a{i}")).collect();
        let mut entries: Vec<(&str, &str, gix_hash::ObjectId)> = names
            .iter()
            .zip(fillers)
            .map(|(name, &oid)| ("100644", name.as_str(), oid))
            .collect();
        entries.push(("100644", "f", blob_oid));
        enroute_git_test_support::tree_of(&entries)
    }

    /// Push `entries` as a packfile and move `refs/heads/main` from `old` to
    /// `new`, through the real receive-pack path.
    async fn push_pack(
        state: &enroute_git_retrieve::Storage,
        repo: &RepoMetadata,
        worker: &std::sync::Arc<dyn enroute_git_ingest::IngestWorker>,
        entries: &[enroute_git_test_support::PackEntry<'_>],
        old: gix_hash::ObjectId,
        new: gix_hash::ObjectId,
    ) {
        let mut head = Body::new();
        let line = format!(
            "{} {} refs/heads/main\0report-status\n",
            old.to_hex(),
            new.to_hex()
        );
        head.line(line.as_bytes()).unwrap();
        head.flush();
        let mut body = head.into_bytes();
        body.extend_from_slice(&enroute_git_test_support::make_pack_of(entries));

        let response = crate::receive_pack::receive_pack(
            state.clone(),
            repo.clone(),
            Actor::new("alice"),
            worker.clone(),
            std::sync::Arc::new(enroute_git_ingest::NoHooks),
            std::io::Cursor::new(body),
        )
        .await
        .unwrap();
        let crate::receive_pack::ReceivePackResponse::Body(report) = response else {
            panic!("expected an unsidebanded report-status");
        };
        let report = debug_pktlines(&report);
        assert!(
            report.contains("ok refs/heads/main"),
            "push rejected: {report}"
        );
    }

    /// The raw packfile bytes a full clone of `want` puts on the wire.
    async fn clone_pack(
        state: &enroute_git_retrieve::Storage,
        repo: &RepoMetadata,
        want: gix_hash::ObjectId,
    ) -> Vec<u8> {
        let body = encode_lines(&[
            b"command=fetch\n",
            format!("want {}\n", want.to_hex()).as_bytes(),
            b"done\n",
        ]);
        let UploadPackResponse::Pack(rx) = do_upload_pack(state.clone(), repo, &body).await else {
            panic!("expected Pack result");
        };
        let raw = collect_pack(rx).await;
        assert!(
            find_sideband3_error(&raw).is_none(),
            "sideband error: {:?}",
            find_sideband3_error(&raw)
        );
        enroute_git_test_support::extract_sideband_channel(&raw, 1)
    }

    /// How many clones to sample, since which copy of X wins the dedup is
    /// decided by the merge and not fixed by the format.
    ///
    /// Eight, not more: every run indexes a pack with a real `git`
    /// subprocess, and a regression here reappears in most runs, not one.
    const RUNS: usize = 8;

    /// Clone `RUNS` times, reporting how often X went out as a delta against
    /// `new_blob` and whatever git said about the packs it refused.
    async fn clone_rounds(
        state: &enroute_git_retrieve::Storage,
        repo: &RepoMetadata,
        want: gix_hash::ObjectId,
        cycle: (gix_hash::ObjectId, gix_hash::ObjectId),
        tmp: &std::path::Path,
    ) -> (usize, Vec<String>) {
        let (old_blob, new_blob) = cycle;
        let mut delta_wins = 0usize;
        let mut failures: Vec<String> = Vec::new();
        for _ in 0..RUNS {
            let pack = clone_pack(state, repo, want).await;
            assert_eq!(&pack[..4], b"PACK", "pack magic");
            let (whole, delta_bases) = enroute_git_test_support::classify_pack_entries(&pack);
            // X went out as a REF_DELTA exactly when c2's pack won the race
            // to `seen` against c0's whole copy.
            if !whole.contains(&old_blob) {
                delta_wins += 1;
                assert!(delta_bases.contains(&new_blob), "X delta'd against Y");
            }
            if let Err(stderr) = enroute_git_test_support::index_pack_with_git(tmp, &pack) {
                failures.push(stderr);
            }
        }
        (delta_wins, failures)
    }

    /// The stored entry for `oid`, header decoded — what the fetch path reads.
    async fn stored_base(
        state: &enroute_git_retrieve::Storage,
        repo: &RepoMetadata,
        oid: gix_hash::ObjectId,
    ) -> Option<gix_hash::ObjectId> {
        let meta = enroute_git_retrieve::meta(state, repo.id, oid)
            .await
            .unwrap()
            .expect("indexed");
        let loc = meta.location.expect("an indexed object has a location");
        let entry = state
            .store
            .get_segment_slice(
                repo,
                loc.segment.id,
                loc.segment_offset(),
                Some(loc.image.entry_len),
            )
            .await
            .unwrap()
            .expect("the segment the index points at");
        enroute_git_store::decode_pack_entry_header(&entry)
            .unwrap()
            .0
            .base
    }

    /// How many other files the root commit carries, enough above 1 to put
    /// X's whole copy behind c2's delta copy in the merge.
    const FILLERS: u8 = 8;

    /// A full clone whose pack can hold two entries that delta against each
    /// other, with no whole copy of either.
    ///
    /// Real `git index-pack` is the oracle: without `bound_chains` placing
    /// stored objects, every clone fails to resolve.
    #[tokio::test]
    async fn a_full_clone_never_ships_a_delta_cycle() {
        use enroute_git_test_support::{PackEntry, blob, commit, near_duplicate_content};
        use gix_object::Kind;

        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let worker: std::sync::Arc<dyn enroute_git_ingest::IngestWorker> =
            enroute_git_ingest::LocalIngestWorker::shared(
                state.clone(),
                std::sync::Arc::new(object_store::memory::InMemory::new()),
            );

        // The root commit carries other files besides the one that gets
        // reverted — the shape of any real repository, and what puts X late
        // in c0's pack while c2's pack is three entries long. Their tails
        // only have to differ from each other and from X and Y.
        let fillers: Vec<(gix_hash::ObjectId, Vec<u8>)> = (0..FILLERS)
            .map(|i| blob(&near_duplicate_content(b'q' + i)))
            .collect();
        let filler_oids: Vec<gix_hash::ObjectId> = fillers.iter().map(|(oid, _)| *oid).collect();

        let (old_blob, old_blob_bytes) = blob(&near_duplicate_content(b'0'));
        let (old_tree, old_tree_bytes) = tree_with_fillers(&filler_oids, old_blob);
        let (c0, c0_bytes) = commit(old_tree, None);

        let (new_blob, new_blob_bytes) = blob(&near_duplicate_content(b'1'));
        let (new_tree, new_tree_bytes) = tree_with_fillers(&filler_oids, new_blob);
        let (c1, c1_bytes) = commit(new_tree, Some(c0));

        // The revert: c2 restores c0's tree and — as real git would, the
        // remote already holding them — re-sends nothing but itself.
        let (c2, c2_bytes) = commit(old_tree, Some(c1));

        // Y arrives as the client's own delta against X, which is what git's
        // pack-objects sends for two versions of one path.
        let mut first_push: Vec<PackEntry<'_>> = fillers
            .iter()
            .map(|(_, bytes)| PackEntry::whole(Kind::Blob, bytes))
            .collect();
        first_push.extend([
            PackEntry::whole(Kind::Blob, &old_blob_bytes),
            PackEntry::whole(Kind::Tree, &old_tree_bytes),
            PackEntry::whole(Kind::Commit, &c0_bytes),
            PackEntry::delta(Kind::Blob, &new_blob_bytes, (old_blob, &old_blob_bytes)),
            PackEntry::whole(Kind::Tree, &new_tree_bytes),
            PackEntry::whole(Kind::Commit, &c1_bytes),
        ]);

        let null = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
        push_pack(&state, &repo, &worker, &first_push, null, c1).await;
        push_pack(
            &state,
            &repo,
            &worker,
            &[PackEntry::whole(Kind::Commit, &c2_bytes)],
            c1,
            c2,
        )
        .await;

        // Guard the fixture: without the cycle in storage there is nothing
        // for the wire path to ship.
        assert_eq!(
            state
                .objects
                .repo(repo.id)
                .packs_of(old_blob)
                .await
                .unwrap(),
            vec![c0, c2],
            "X must be stored twice"
        );
        assert_eq!(
            stored_base(&state, &repo, new_blob).await,
            Some(old_blob),
            "Y must be stored as a delta against X"
        );

        let tmp =
            std::env::temp_dir().join(format!("enroute-git-delta-cycle-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        assert!(
            enroute_git_test_support::run_git(&["init", "--bare", "-q"], Some(&tmp))
                .status
                .success()
        );

        let (delta_wins, failures) =
            clone_rounds(&state, &repo, c2, (old_blob, new_blob), &tmp).await;
        drop(std::fs::remove_dir_all(&tmp));

        assert!(
            failures.is_empty(),
            "a full clone shipped a pack real git cannot index: {}/{RUNS} runs \
             rejected, {delta_wins}/{RUNS} sent X as a REF_DELTA against Y \
             (whose own entry deltas against X, with no whole copy of either). \
             git index-pack: {:?}",
            failures.len(),
            failures.first()
        );
    }
}
