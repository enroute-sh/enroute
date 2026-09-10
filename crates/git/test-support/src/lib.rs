//! Test-only helpers, shared by every crate that drives a real git engine in
//! a test.
//!
//! A fault-injecting object store, state and repository seeding, and
//! wire-format decoding for assertions.
#![allow(missing_docs, reason = "internal test-only helper crate")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::as_conversions,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    clippy::new_without_default,
    clippy::similar_names,
    reason = "test-only helper crate never compiled into the production binary; \
              equivalent to the #[cfg(test)] module it replaces"
)]

mod cost;

pub use cost::{RecordedFields, capture_recorded_fields};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures::stream::BoxStream;
use gix_hash::ObjectId;
use gix_object::Kind;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use enroute_git_core::{
    NewCommit, NewObject, ObjectHashMap, PackImageLocation, SegmentLocation, decode_loose,
    encode_loose, hash_loose,
};
use enroute_git_retrieve::{RepoMetadata, Storage};
use enroute_git_store::{
    PackTrailerEntry, blob_section_offset, encode_commit_pack_header, encode_commit_pack_object,
    encode_pack_entry_header, encode_pack_trailer,
};

/// Wraps `InMemory` but fails every `get_opts` call once a counter reaches
/// zero, via `fail_gets_after(n)`.
///
/// Also records each call's `GetOptions::range`, so tests can assert a
/// caller requested a bounded range.
pub struct FaultyObjectStore {
    inner: InMemory,
    /// Counts down from the initial value; when it hits zero all gets fail.
    ///
    /// `usize::MAX` (the default) means "never fail".
    gets_remaining: AtomicUsize,
    recorded_ranges: std::sync::Mutex<Vec<Option<object_store::GetRange>>>,
}

impl FaultyObjectStore {
    pub fn new() -> Self {
        Self {
            inner: InMemory::new(),
            gets_remaining: AtomicUsize::new(usize::MAX),
            recorded_ranges: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// After `n` successful get operations every subsequent get will fail.
    pub fn fail_gets_after(&self, n: usize) {
        self.gets_remaining.store(n, Ordering::SeqCst);
    }

    /// The `range` argument of every `get_opts` call made so far, in order.
    pub fn recorded_ranges(&self) -> Vec<Option<object_store::GetRange>> {
        self.recorded_ranges.lock().unwrap().clone()
    }
}

impl std::fmt::Debug for FaultyObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FaultyObjectStore")
    }
}

impl std::fmt::Display for FaultyObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FaultyObjectStore")
    }
}

impl ObjectStore for FaultyObjectStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.put_opts(location, payload, opts).await })
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        opts: PutMultipartOptions,
    ) -> Pin<
        Box<
            dyn Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.put_multipart_opts(location, opts).await })
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        options: GetOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.recorded_ranges
            .lock()
            .unwrap()
            .push(options.range.clone());

        // Returning None means the counter was already at zero — inject a failure.
        let prev =
            self.gets_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| match n {
                    usize::MAX => Some(usize::MAX),
                    0 => None,
                    _ => Some(n - 1),
                });
        Box::pin(async move {
            if prev.is_err() {
                return Err(object_store::Error::Generic {
                    store: "FaultyObjectStore",
                    source: Box::new(std::io::Error::other("injected storage failure")),
                });
            }
            self.inner.get_opts(location, options).await
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 Path>,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.list_with_delimiter(prefix).await })
    }

    fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
        options: CopyOptions,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move { self.inner.copy_opts(from, to, options).await })
    }
}

/// Storage with nothing installed behind it: rows in a map, segments in a
/// map, bytes in a bucket in memory.
///
/// What a database answers differently is pinned where that database is, so
/// a test about the engine's behaviour does not need one.
pub fn make_state() -> Storage {
    Storage::in_memory(
        Arc::new(InMemory::new()),
        Arc::new(enroute_git_store::Store::new(Arc::new(InMemory::new()))),
    )
}

/// The same storage, with every permanent-object request answered by a store
/// that can be told to fail.
///
/// Only the bytes are faulty: the catalogs stay sound, so a test sees a
/// storage fault rather than a lost index.
pub fn make_faulty_state() -> (Storage, Arc<FaultyObjectStore>) {
    let faulty = Arc::new(FaultyObjectStore::new());
    let faulty_dyn: Arc<dyn ObjectStore> = faulty.clone();
    let state = Storage {
        store: Arc::new(enroute_git_store::Store::new(faulty_dyn)),
        ..make_state()
    };
    (state, faulty)
}

/// Creates a repository and returns its [`RepoMetadata`].
///
/// It has no name and no owner: a test that wants a repository called
/// something is testing an application, not this.
pub async fn create_repo(state: &Storage) -> RepoMetadata {
    state.rows.create(None).await.unwrap()
}

/// Parse raw pkt-line bytes and return the payload of the sideband-3 (error)
/// packets, if any, concatenated.
pub fn find_sideband3_error(data: &[u8]) -> Option<String> {
    let bytes = extract_sideband_channel(data, 3);
    (!bytes.is_empty()).then(|| String::from_utf8_lossy(&bytes).into_owned())
}

/// Concatenate every sideband frame on `channel` (1=data, 2=progress,
/// 3=error) in a sideband-multiplexed pkt-line stream, in order.
pub fn extract_sideband_channel(mut data: &[u8], channel: u8) -> Vec<u8> {
    use gix_packetline::PacketLineRef;
    use gix_packetline::decode::{PacketLineOrWantedSize, hex_prefix};

    let mut out = Vec::new();
    while let Some((prefix, rest)) = data.split_first_chunk::<4>() {
        let payload = match hex_prefix(prefix).unwrap() {
            PacketLineOrWantedSize::Line(PacketLineRef::Flush | _) => {
                data = rest;
                continue;
            }
            PacketLineOrWantedSize::Wanted(n) => {
                let n = usize::from(n);
                let payload = &rest[..n];
                data = &rest[n..];
                payload
            }
        };
        if payload.first() == Some(&channel) {
            out.extend_from_slice(&payload[1..]);
        }
    }
    out
}

/// Decodes pkt-line framing into a human-readable string for snapshot
/// testing.
///
/// Binary payloads are shown as `[binary N bytes]`; flush as `[flush]`.
pub fn debug_pktlines(mut data: &[u8]) -> String {
    use gix_packetline::PacketLineRef;
    use gix_packetline::decode::{PacketLineOrWantedSize, hex_prefix};
    let mut out = String::new();
    loop {
        if data.len() < 4 {
            break;
        }
        let (prefix, rest) = data.split_at(4);
        match hex_prefix(prefix).unwrap() {
            PacketLineOrWantedSize::Line(PacketLineRef::Flush) => {
                out.push_str("[flush]\n");
                data = rest;
            }
            PacketLineOrWantedSize::Line(_) => {
                out.push_str("[special-pktline]\n");
                data = rest;
            }
            PacketLineOrWantedSize::Wanted(n) => {
                let n = usize::from(n);
                let Some(payload) = rest.get(..n) else { break };
                if let Ok(s) = std::str::from_utf8(payload) {
                    out.push_str(&s.replace('\0', "<NUL>"));
                } else {
                    out.push_str("[binary ");
                    out.push_str(&n.to_string());
                    out.push_str(" bytes]\n");
                }
                let Some(next) = rest.get(n..) else { break };
                data = next;
            }
        }
    }
    out
}

/// Replaces this build's version in an advertisement with `[version]`, for
/// snapshot testing.
///
/// Every advertisement carries `agent=git/<version>`, so without this a
/// version bump fails snapshots that are about the pkt-lines around it.
#[must_use]
pub fn redact_agent(body: &str) -> String {
    body.replace(env!("CARGO_PKG_VERSION"), "[version]")
}

/// Parse a v2 fetch response into a sorted `<kind> <oid>` list for snapshot testing.
pub fn pack_entries(data: &[u8]) -> String {
    use std::io::Read as _;

    let pack = extract_sideband1_pack(data);

    assert_eq!(&pack[..4], b"PACK", "pack magic");
    let count = u32::from_be_bytes(pack[8..12].try_into().unwrap());
    let object_hash = gix_hash::Kind::Sha1;

    let mut offset = 12usize;
    let mut entries: Vec<(String, String)> = Vec::with_capacity(usize::try_from(count).unwrap());
    for _ in 0..count {
        let entry = gix_pack::data::Entry::from_bytes(
            &pack[offset..],
            u64::try_from(offset).unwrap(),
            object_hash,
        )
        .expect("parse pack entry");
        let kind = header_kind_str(entry.header);
        let body_start = usize::try_from(entry.data_offset).unwrap();
        let mut decoder = flate2::read::ZlibDecoder::new(&pack[body_start..]);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .expect("decompress entry");
        offset = body_start + usize::try_from(decoder.total_in()).unwrap();

        let obj_kind = header_object_kind(entry.header);
        let header =
            gix_object::encode::loose_header(obj_kind, u64::try_from(decompressed.len()).unwrap());
        let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
        h.update(&header);
        h.update(&decompressed);
        let oid = h.try_finalize().expect("hash").to_hex().to_string();
        entries.push((kind.to_string(), oid));
    }

    entries.sort();
    entries
        .into_iter()
        .map(|(k, o)| format!("{k} {o}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn header_kind_str(header: gix_pack::data::entry::Header) -> &'static str {
    use gix_pack::data::entry::Header;
    match header {
        Header::Commit => "commit",
        Header::Tree => "tree",
        Header::Blob => "blob",
        Header::Tag => "tag",
        Header::RefDelta { .. } | Header::OfsDelta { .. } => "delta",
    }
}

fn header_object_kind(header: gix_pack::data::entry::Header) -> Kind {
    use gix_pack::data::entry::Header;
    match header {
        Header::Commit => Kind::Commit,
        Header::Tree => Kind::Tree,
        Header::Blob => Kind::Blob,
        Header::Tag => Kind::Tag,
        // This server never emits deltas.
        Header::RefDelta { .. } | Header::OfsDelta { .. } => {
            panic!("unexpected delta entry in pack")
        }
    }
}

/// Concatenate all sideband-1 payloads from a v2 fetch response, skipping the
/// initial `packfile\n` line and any sideband-3 error frames.
fn extract_sideband1_pack(mut data: &[u8]) -> Vec<u8> {
    use gix_packetline::PacketLineRef;
    use gix_packetline::decode::{PacketLineOrWantedSize, hex_prefix};

    let mut pack = Vec::new();
    let mut saw_packfile = false;
    while let Some((prefix, rest)) = data.split_first_chunk::<4>() {
        let payload = match hex_prefix(prefix).unwrap() {
            PacketLineOrWantedSize::Line(PacketLineRef::Flush | _) => {
                data = rest;
                continue;
            }
            PacketLineOrWantedSize::Wanted(n) => {
                let n = usize::from(n);
                let payload = &rest[..n];
                data = &rest[n..];
                payload
            }
        };
        if !saw_packfile {
            saw_packfile = is_packfile_line(payload);
            continue;
        }
        if payload.first() == Some(&1u8) {
            pack.extend_from_slice(&payload[1..]);
        }
    }
    pack
}

fn is_packfile_line(payload: &[u8]) -> bool {
    payload.last() == Some(&b'\n') && &payload[..payload.len() - 1] == b"packfile"
}

/// Seeds a single loose git object into primary storage so tests can
/// simulate an object that arrived in a prior push.
///
/// A tree/blob/tag can't be its own pack, so it's wrapped in a synthetic
/// single-use commit pack to gain a resolvable location.
pub async fn put_loose_as_singleton_pack(
    state: &Storage,
    repo: &RepoMetadata,
    sha: &str,
    data: Bytes,
) {
    let (kind, _) = decode_loose(&data).unwrap();
    if kind == Kind::Commit {
        return put_commit_pack_with_trailer(
            state,
            repo,
            sha,
            &[(sha.to_string(), kind, data)],
            &[],
        )
        .await;
    }

    // Synthetic commit is never fetched, so its bytes just need to hash distinctly per SHA.
    let (pack_commit_oid, pack_commit_loose) = encode_loose(
        Kind::Commit,
        format!("singleton pack for {sha}\n").as_bytes(),
    )
    .unwrap();
    let pack_sha = pack_commit_oid.to_hex().to_string();
    put_commit_pack_with_trailer(
        state,
        repo,
        &pack_sha,
        &[
            (
                pack_sha.clone(),
                Kind::Commit,
                Bytes::from(pack_commit_loose),
            ),
            (sha.to_string(), kind, data),
        ],
        &[],
    )
    .await;
}

/// One object's absolute in-pack location, as produced by [`build_pack`].
struct PackedObject {
    oid: ObjectId,
    kind: Kind,
    /// Absolute byte offset of this object's self-describing inline header
    /// within the pack file.
    offset: u64,
    /// Byte span of the inline header plus compressed body together.
    entry_len: u64,
}

/// The well-known empty-tree oid — a valid, deterministic `root_tree` for
/// synthetic test commits that reference no real tree object.
fn empty_tree_oid() -> ObjectId {
    ObjectId::from_hex(b"4b825dc642cb6eb9a060e54bf8d69288fbee4904")
        .expect("well-known empty-tree sha")
}

/// Assembles commit-pack bytes for `objects`, alongside each object's
/// absolute in-pack location and the pack's blob-section start offset.
fn build_pack(objects: &[(String, Kind, Bytes)]) -> (Bytes, Vec<PackedObject>, u64) {
    let object_count = u32::try_from(objects.len()).unwrap();
    let header = encode_commit_pack_header(object_count);
    let mut pack_buf: Vec<u8> = header.to_vec();
    let mut offset = u64::try_from(header.len()).unwrap();
    let mut entries: Vec<PackedObject> = Vec::new();
    let mut trailer_entries: Vec<PackTrailerEntry> = Vec::new();

    for (sha, kind, loose) in objects {
        let (_, content) = decode_loose(loose).unwrap();
        let length = u64::try_from(content.len()).unwrap();
        let compressed = encode_commit_pack_object(&content).unwrap();
        let compressed_len = u64::try_from(compressed.len()).unwrap();
        let oid = ObjectId::from_hex(sha.as_bytes()).expect("sha must be a valid hex ObjectId");

        let entry_header =
            encode_pack_entry_header(oid, *kind, None, length, compressed_len).unwrap();
        let entry_len = u64::try_from(entry_header.len()).unwrap() + compressed_len;
        pack_buf.extend_from_slice(&entry_header);
        pack_buf.extend_from_slice(&compressed);

        entries.push(PackedObject {
            oid,
            kind: *kind,
            offset,
            entry_len,
        });
        trailer_entries.push(PackTrailerEntry {
            sha: oid,
            kind: *kind,
            length,
            offset,
            entry_len,
        });
        offset += entry_len;
    }

    pack_buf.extend_from_slice(&encode_pack_trailer(&trailer_entries));
    let blob_offset = blob_section_offset(&trailer_entries);
    (Bytes::from(pack_buf), entries, blob_offset)
}

/// Stores `objects` as a single commit pack keyed by `pack_sha`, exactly
/// what `receive_pack` does for a real push.
///
/// `objects` must contain exactly one `Kind::Commit` whose SHA is
/// `pack_sha`; the rest are its trees/blobs.
pub(crate) async fn put_commit_pack_with_trailer(
    state: &Storage,
    repo: &RepoMetadata,
    pack_sha: &str,
    objects: &[(String, Kind, Bytes)],
    parents: &[ObjectId],
) {
    let (bytes, locations, blob_offset) = build_pack(objects);
    // One image alone in its own segment — the degenerate case of what the
    // ingest writer produces, and all most fixtures here need. See
    // [`seed_commit_chain_tiled`] for the packed-segment case.
    let segment = SegmentLocation {
        id: enroute_git_core::Ulid::generate(),
        base_offset: 0,
        image_len: bytes.len() as u64,
    };
    let mut writer = state.store.segment_writer(repo, segment.id);
    writer.write(bytes).await.unwrap();
    writer.finish().await.unwrap();

    append_pack_metadata(
        state,
        repo,
        pack_sha,
        objects,
        parents,
        segment,
        &locations,
        blob_offset,
    )
    .await;
}

/// Records an already-written pack image's commit and non-commit objects.
///
/// The half of [`put_commit_pack_with_trailer`] that doesn't care where the
/// image landed, so a tiled segment can reuse it.
#[expect(
    clippy::too_many_arguments,
    reason = "every piece here is already computed by the caller"
)]
async fn append_pack_metadata(
    state: &Storage,
    repo: &RepoMetadata,
    pack_sha: &str,
    objects: &[(String, Kind, Bytes)],
    parents: &[ObjectId],
    segment: SegmentLocation,
    locations: &[PackedObject],
    blob_offset: u64,
) {
    let pack_oid =
        ObjectId::from_hex(pack_sha.as_bytes()).expect("pack_sha must be a valid hex ObjectId");
    // The commit's root tree: the one real `Kind::Tree` entry among `objects`,
    // or the well-known empty-tree oid if none — registered below as a real,
    // location-less object so `root_tree_seq` still resolves.
    let real_tree = objects
        .iter()
        .find(|(_, kind, _)| *kind == Kind::Tree)
        .map(|(sha, _, _)| ObjectId::from_hex(sha.as_bytes()).expect("tree sha must be valid hex"));
    let root_tree = real_tree.unwrap_or_else(empty_tree_oid);

    let mut new_commits = ObjectHashMap::default();
    let mut new_objects: Vec<NewObject> = Vec::new();
    if real_tree.is_none() {
        new_objects.push(NewObject {
            oid: root_tree,
            kind: Kind::Tree,
            locations: vec![],
            children: vec![],
        });
    }
    for (obj, (_, _, loose)) in locations.iter().zip(objects.iter()) {
        if obj.kind == Kind::Commit {
            new_commits.insert(
                obj.oid,
                NewCommit {
                    committer_date: 0,
                    root_tree,
                    parents: parents.to_vec(),
                    entry_len: obj.entry_len,
                    blob_offset,
                    segment,
                },
            );
        } else {
            let children = if obj.kind == Kind::Tree {
                let (_, content) = decode_loose(loose).expect("tree loose bytes must decode");
                gix_object::TreeRef::from_bytes(&content, gix_hash::Kind::Sha1)
                    .expect("tree bytes must parse")
                    .entries
                    .iter()
                    .filter(|e| !e.mode.is_commit())
                    .map(|e| ObjectId::from(e.oid))
                    .collect()
            } else {
                Vec::new()
            };
            new_objects.push(NewObject {
                oid: obj.oid,
                kind: obj.kind,
                locations: vec![PackImageLocation {
                    pack_sha: pack_oid,
                    offset: obj.offset,
                    entry_len: obj.entry_len,
                    base: None,
                }],
                children,
            });
        }
    }
    enroute_git_ingest::append(
        enroute_git_ingest::Engine {
            ids: state.rows.repo(repo.id),
            graph: state.graph.repo(repo.id),
            objects: state.objects.repo(repo.id),
            ledger: state.ledger.as_ref(),
        },
        &new_commits,
        &new_objects,
        &enroute_git_ingest::KnownIdentities::default(),
    )
    .await
    .unwrap();
}

/// Seeds a blob → tree → commit into `state` and sets `refs/heads/main`,
/// returning the commit SHA.
///
/// All three are stored in a single commit pack, matching what
/// `upload_staged_ordered` produces in production.
pub async fn seed_commit(state: &Storage, repo: &RepoMetadata) -> String {
    seed_commit_with_blob(state, repo, b"hello, world\n").await
}

/// Like [`seed_commit`] but with caller-provided blob content, so tests
/// can exercise size-dependent paths.
pub async fn seed_commit_with_blob(
    state: &Storage,
    repo: &RepoMetadata,
    blob_content: &[u8],
) -> String {
    let (blob_oid, blob_data) = encode_loose(Kind::Blob, blob_content).unwrap();

    let (tree_oid, tree_data) = loose_tree(&[("100644", "hello.txt", blob_oid)]);
    let tree_sha = tree_oid.to_hex().to_string();

    let (commit_oid, commit_data) = encode_loose(
        Kind::Commit,
        commit_text(tree_oid, None, SEED_IDENTITY, SEED_TIME, "initial commit").as_bytes(),
    )
    .unwrap();
    let commit_sha = commit_oid.to_hex().to_string();

    // Matches upload_staged_ordered + append, and records the commit so set_main_ref can resolve it.
    put_commit_pack_with_trailer(
        state,
        repo,
        &commit_sha,
        &[
            (commit_sha.clone(), Kind::Commit, Bytes::from(commit_data)),
            (tree_sha, Kind::Tree, Bytes::from(tree_data)),
            (
                blob_oid.to_hex().to_string(),
                Kind::Blob,
                Bytes::from(blob_data),
            ),
        ],
        &[],
    )
    .await;

    set_main_ref(state, repo, commit_oid).await;

    commit_sha
}

/// Points `refs/heads/main` at `commit` (unguarded create; only ever used
/// to set it for the first time).
///
/// `HEAD` is already a symbolic ref to `refs/heads/main`, so it needn't be
/// set again.
async fn set_main_ref(state: &Storage, repo: &RepoMetadata, commit: ObjectId) {
    let null = ObjectId::null(gix_hash::Kind::Sha1);
    let results = state
        .rows
        .repo(repo.id)
        .update_refs(&[enroute_git_retrieve::RefUpdate {
            refname: "refs/heads/main".to_string(),
            old_id: null,
            new_id: commit,
        }])
        .await
        .unwrap();
    // `update_refs` reports a rejected update as `Ok`, not `Err` — assert explicitly
    // so a caller ordering bug fails loudly instead of leaving the ref unset.
    assert_eq!(
        results,
        vec![enroute_git_retrieve::RefUpdateResult {
            refname: "refs/heads/main".to_string(),
            result: Ok(()),
        }],
        "set_main_ref: update_refs rejected the update"
    );
}

/// Seeds `count` linked commits into `state` and sets `refs/heads/main` to
/// the tip, returning the SHAs in chain order (oldest first).
///
/// Unlike [`seed_commit`], each commit has distinct content and a real
/// parent chain, so tests can exercise multi-commit fetches.
pub async fn seed_commit_chain(state: &Storage, repo: &RepoMetadata, count: usize) -> Vec<String> {
    seed_linear_chain(state, repo, count, |i| {
        let (blob_oid, blob_data) =
            encode_loose(Kind::Blob, format!("hello, world {i}\n").as_bytes()).unwrap();

        let (tree_oid, tree_data) = loose_tree(&[("100644", "hello.txt", blob_oid)]);

        (
            blob_oid,
            Bytes::from(blob_data),
            tree_oid.to_hex().to_string(),
            Bytes::from(tree_data),
        )
    })
    .await
}

/// Like [`seed_commit_chain`], but tiles every commit's pack image into
/// **one** segment, the way ingest lays a push down.
///
/// The fixture for a fetch that coalesces adjacent images into one GET.
/// `blob_bytes` sizes each commit's blob for `blob:none` skip tests.
pub async fn seed_commit_chain_tiled(
    state: &Storage,
    repo: &RepoMetadata,
    count: usize,
    blob_bytes: usize,
) -> Vec<String> {
    let segment_id = enroute_git_core::Ulid::generate();
    let mut parent_oid: Option<ObjectId> = None;
    let mut shas = Vec::with_capacity(count);
    let mut images: Vec<PendingImage> = Vec::with_capacity(count);

    // Build every image before writing any: they go out as one segment
    // object, and each one's offset depends on all those before it.
    for i in 0..count {
        // Xorshift rather than a counter pattern: the blob is stored
        // deflated, and anything periodic would compress away to nothing,
        // leaving `blob_bytes` with no bearing on the section's real size.
        let mut content = format!("hello, world {i}\n").into_bytes();
        let mut x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        content.extend((0..blob_bytes).map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            u8::try_from((x >> 24) & 0xff).expect("masked to a byte")
        }));
        let (blob_oid, blob_data) = encode_loose(Kind::Blob, &content).unwrap();
        let (tree_oid, tree_data) = loose_tree(&[("100644", "hello.txt", blob_oid)]);
        let (commit_oid, commit_data) = encode_loose(
            Kind::Commit,
            commit_text(
                tree_oid,
                parent_oid,
                SEED_IDENTITY,
                SEED_TIME,
                &format!("commit {i}"),
            )
            .as_bytes(),
        )
        .unwrap();
        let commit_sha = commit_oid.to_hex().to_string();

        let objects = vec![
            (commit_sha.clone(), Kind::Commit, Bytes::from(commit_data)),
            (
                tree_oid.to_hex().to_string(),
                Kind::Tree,
                Bytes::from(tree_data),
            ),
            (
                blob_oid.to_hex().to_string(),
                Kind::Blob,
                Bytes::from(blob_data),
            ),
        ];
        let (bytes, locations, blob_offset) = build_pack(&objects);
        images.push(PendingImage {
            bytes,
            locations,
            blob_offset,
            commit_sha: commit_sha.clone(),
            objects,
            parents: parent_oid.into_iter().collect(),
        });

        parent_oid = Some(commit_oid);
        shas.push(commit_sha);
    }

    let mut writer = state.store.segment_writer(repo, segment_id);
    let mut base_offset = 0;
    let mut placements = Vec::with_capacity(images.len());
    for image in &images {
        placements.push(SegmentLocation {
            id: segment_id,
            base_offset,
            image_len: image.bytes.len() as u64,
        });
        base_offset += image.bytes.len() as u64;
        writer.write(image.bytes.clone()).await.unwrap();
    }
    writer.finish().await.unwrap();

    for (segment, image) in placements.into_iter().zip(images.iter()) {
        append_pack_metadata(
            state,
            repo,
            &image.commit_sha,
            &image.objects,
            &image.parents,
            segment,
            &image.locations,
            image.blob_offset,
        )
        .await;
    }

    set_main_ref(state, repo, parent_oid.unwrap()).await;

    shas
}

/// A commit-pack image built but not yet placed, held while
/// [`seed_commit_chain_tiled`] works out where each one lands.
struct PendingImage {
    bytes: Bytes,
    locations: Vec<PackedObject>,
    blob_offset: u64,
    commit_sha: String,
    objects: Vec<(String, Kind, Bytes)>,
    parents: Vec<ObjectId>,
}

/// Like [`seed_commit_chain`], but *accumulating*: commit `i` adds a file
/// while keeping every earlier one, so each tree references ancestor blobs.
///
/// The fixture for shallow-fetch boundary completeness: completing the
/// tip's snapshot needs reaching into ancestor packs for inherited blobs.
pub async fn seed_commit_chain_accumulating(
    state: &Storage,
    repo: &RepoMetadata,
    count: usize,
) -> Vec<String> {
    assert!(count <= 10, "single-digit filenames keep tree order sorted");
    let mut blob_oids: Vec<ObjectId> = Vec::with_capacity(count);
    seed_linear_chain(state, repo, count, move |i| {
        let (blob_oid, blob_data) =
            encode_loose(Kind::Blob, format!("content {i}\n").as_bytes()).unwrap();
        blob_oids.push(blob_oid);

        let names: Vec<String> = (0..blob_oids.len())
            .map(|j| format!("file-{j}.txt"))
            .collect();
        let entries: Vec<(&str, &str, ObjectId)> = names
            .iter()
            .zip(&blob_oids)
            .map(|(name, &oid)| ("100644", name.as_str(), oid))
            .collect();
        let (tree_oid, tree_data) = loose_tree(&entries);

        (
            blob_oid,
            Bytes::from(blob_data),
            tree_oid.to_hex().to_string(),
            Bytes::from(tree_data),
        )
    })
    .await
}

/// Seeds two linear commits where `c1`'s tree reuses `c0`'s `dir/` subtree
/// byte-for-byte and adds a top-level file.
///
/// Exercises multi-level shallow-fetch backfill, recursing into an
/// inherited tree rather than just a leaf blob.
pub async fn seed_commit_pair_with_inherited_subtree(
    state: &Storage,
    repo: &RepoMetadata,
) -> (String, String, ObjectId, ObjectId) {
    let (blob0_oid, blob0_data) = encode_loose(Kind::Blob, b"nested\n").unwrap();
    let (subtree_oid, subtree_data) = loose_tree(&[("100644", "file.txt", blob0_oid)]);
    let (root0_oid, root0_data) = loose_tree(&[("40000", "dir", subtree_oid)]);

    let (commit0_oid, commit0_data) = encode_loose(
        Kind::Commit,
        commit_text(root0_oid, None, SEED_IDENTITY, SEED_TIME, "commit 0").as_bytes(),
    )
    .unwrap();
    let commit0_sha = commit0_oid.to_hex().to_string();

    put_commit_pack_with_trailer(
        state,
        repo,
        &commit0_sha,
        &[
            (commit0_sha.clone(), Kind::Commit, Bytes::from(commit0_data)),
            (
                root0_oid.to_hex().to_string(),
                Kind::Tree,
                Bytes::from(root0_data),
            ),
            (
                subtree_oid.to_hex().to_string(),
                Kind::Tree,
                Bytes::from(subtree_data),
            ),
            (
                blob0_oid.to_hex().to_string(),
                Kind::Blob,
                Bytes::from(blob0_data),
            ),
        ],
        &[],
    )
    .await;

    // c1: `dir/` reused unchanged, `top.txt` new.
    let (blob1_oid, blob1_data) = encode_loose(Kind::Blob, b"top\n").unwrap();
    let (root1_oid, root1_data) = loose_tree(&[
        ("40000", "dir", subtree_oid),
        ("100644", "top.txt", blob1_oid),
    ]);

    let (commit1_oid, commit1_data) = encode_loose(
        Kind::Commit,
        commit_text(
            root1_oid,
            Some(commit0_oid),
            SEED_IDENTITY,
            SEED_TIME,
            "commit 1",
        )
        .as_bytes(),
    )
    .unwrap();
    let commit1_sha = commit1_oid.to_hex().to_string();

    put_commit_pack_with_trailer(
        state,
        repo,
        &commit1_sha,
        &[
            (commit1_sha.clone(), Kind::Commit, Bytes::from(commit1_data)),
            (
                root1_oid.to_hex().to_string(),
                Kind::Tree,
                Bytes::from(root1_data),
            ),
            (
                blob1_oid.to_hex().to_string(),
                Kind::Blob,
                Bytes::from(blob1_data),
            ),
        ],
        &[commit0_oid],
    )
    .await;

    (commit0_sha, commit1_sha, subtree_oid, blob0_oid)
}

/// A commit object's text: `tree`, optional `parent`, `who` as both author
/// and committer at `when`, and `msg` as the message.
fn commit_text(
    root: ObjectId,
    parent: Option<ObjectId>,
    who: &str,
    when: u64,
    msg: &str,
) -> String {
    let parent_line = parent.map(|p| format!("parent {p}\n")).unwrap_or_default();
    format!(
        "tree {root}\n\
         {parent_line}\
         author {who} {when} +0000\n\
         committer {who} {when} +0000\n\
         \n\
         {msg}\n"
    )
}

/// Who the commits [`seed_commit`] and its siblings store are signed by.
const SEED_IDENTITY: &str = "Test <test@example.com>";

/// When they are dated, fixed so a seeded commit hashes the same every run.
const SEED_TIME: u64 = 1_234_567_890;

/// Who the objects the [`blob`]/[`commit`] builders make are signed by.
const BUILDER_IDENTITY: &str = "T <t@t>";

/// Shared skeleton of [`seed_commit_chain`] and
/// [`seed_commit_chain_accumulating`]: builds `count` linear commits.
///
/// Each stores whatever blob/tree `make` produces for its index; the
/// callers differ only in that.
async fn seed_linear_chain(
    state: &Storage,
    repo: &RepoMetadata,
    count: usize,
    mut make: impl FnMut(usize) -> (ObjectId, Bytes, String, Bytes),
) -> Vec<String> {
    let mut parent_oid: Option<ObjectId> = None;
    let mut shas = Vec::with_capacity(count);

    for i in 0..count {
        let (blob_oid, blob_data, tree_sha, tree_data) = make(i);

        let root = ObjectId::from_hex(tree_sha.as_bytes()).expect("tree sha must be valid hex");
        let (commit_oid, commit_data) = encode_loose(
            Kind::Commit,
            commit_text(
                root,
                parent_oid,
                SEED_IDENTITY,
                SEED_TIME,
                &format!("commit {i}"),
            )
            .as_bytes(),
        )
        .unwrap();
        let commit_sha = commit_oid.to_hex().to_string();

        let parents: Vec<ObjectId> = parent_oid.into_iter().collect();
        put_commit_pack_with_trailer(
            state,
            repo,
            &commit_sha,
            &[
                (commit_sha.clone(), Kind::Commit, Bytes::from(commit_data)),
                (tree_sha, Kind::Tree, tree_data),
                (blob_oid.to_hex().to_string(), Kind::Blob, blob_data),
            ],
            &parents,
        )
        .await;

        parent_oid = Some(commit_oid);
        shas.push(commit_sha);
    }

    set_main_ref(state, repo, parent_oid.unwrap()).await;

    shas
}

/// A blob → tree → commit triple with its encoded loose bytes, so a test
/// can assemble a pack without storing anything.
///
/// The write-side analogue of [`seed_commit`]/[`seed_commit_chain`].
#[derive(Debug, Clone)]
pub struct TestCommit {
    pub blob_oid: ObjectId,
    pub blob_content: Vec<u8>,
    pub tree_oid: ObjectId,
    pub tree_sha: String,
    tree_content: Vec<u8>,
    pub commit_oid: ObjectId,
    pub commit_sha: String,
    commit_text: String,
}

/// One object as a client would put it in a pack.
#[derive(Debug)]
pub struct PackEntry<'a> {
    kind: Kind,
    content: &'a [u8],
    /// Send it as a `REF_DELTA` against this version rather than whole.
    ///
    /// `REF_DELTA` rather than `OFS_DELTA`: naming a base by oid lets a
    /// fixture spell out which version it wants deltified against.
    base: Option<(ObjectId, &'a [u8])>,
    /// Delta instructions to send as-is, instead of encoding `content`
    /// against the base.
    ///
    /// The only way to tell bytes a server kept from bytes it re-derived.
    given: Option<Vec<u8>>,
}

impl<'a> PackEntry<'a> {
    #[must_use]
    pub fn whole(kind: Kind, content: &'a [u8]) -> Self {
        Self {
            kind,
            content,
            base: None,
            given: None,
        }
    }

    #[must_use]
    pub fn delta(kind: Kind, content: &'a [u8], base: (ObjectId, &'a [u8])) -> Self {
        Self {
            kind,
            content,
            base: Some(base),
            given: None,
        }
    }

    /// A delta the fixture spells out itself, rather than one the encoder
    /// finds.
    ///
    /// `content` is only what it must rebuild to.
    #[must_use]
    pub fn given_delta(kind: Kind, content: &'a [u8], base_oid: ObjectId, delta: Vec<u8>) -> Self {
        Self {
            kind,
            content,
            base: Some((base_oid, &[])),
            given: Some(delta),
        }
    }
}

/// A valid delta that copies nothing: base size, result size, then the whole
/// target as literal inserts.
///
/// Correct, and far larger than any delta an encoder would settle for, which
/// is what tells a kept delta from a re-encoded one.
#[must_use]
pub fn all_literal_delta(base: &[u8], target: &[u8]) -> Vec<u8> {
    let mut delta = Vec::new();
    enroute_git_packfile::write_varint(&mut delta, base.len() as u64);
    enroute_git_packfile::write_varint(&mut delta, target.len() as u64);
    for chunk in target.chunks(0x7f) {
        delta.push(u8::try_from(chunk.len()).unwrap());
        delta.extend_from_slice(chunk);
    }
    delta
}

/// A git packfile of `entries`: header, one entry each, SHA-1 trailer.
///
/// The same framing `receive_pack` reads off the wire.
#[must_use]
pub fn make_pack(entries: &[(Kind, &[u8])]) -> Vec<u8> {
    let entries: Vec<PackEntry<'_>> = entries
        .iter()
        .map(|(kind, content)| PackEntry::whole(*kind, content))
        .collect();
    make_pack_of(&entries)
}

/// As [`make_pack`], for a fixture that needs deltas in its pack.
#[must_use]
pub fn make_pack_of(entries: &[PackEntry<'_>]) -> Vec<u8> {
    let mut pack = Vec::new();
    enroute_git_packfile::write_pack_header(
        u32::try_from(entries.len()).unwrap_or(u32::MAX),
        &mut pack,
    );
    for entry in entries {
        let Some((base_oid, base_content)) = entry.base else {
            enroute_git_packfile::write_pack_entry(entry.kind, entry.content, &mut pack).unwrap();
            continue;
        };
        let delta = entry.given.clone().unwrap_or_else(|| {
            enroute_git_packfile::encode_delta(base_content, entry.content).unwrap()
        });
        enroute_git_packfile::write_ref_delta_header(
            base_oid,
            u64::try_from(delta.len()).unwrap(),
            &mut pack,
        )
        .unwrap();
        pack.extend_from_slice(&encode_commit_pack_object(&delta).unwrap());
    }
    let mut hasher = gix_hash::hasher(gix_hash::Kind::Sha1);
    hasher.update(&pack);
    pack.extend_from_slice(hasher.try_finalize().unwrap().as_slice());
    pack
}

/// Splits `pack`'s entries into whole object ids and `REF_DELTA` base ids.
///
/// An id that appears only as a base, never whole, is one real `git
/// index-pack` cannot resolve.
#[must_use]
pub fn classify_pack_entries(
    pack: &[u8],
) -> (
    std::collections::HashSet<ObjectId>,
    std::collections::HashSet<ObjectId>,
) {
    use std::io::Read as _;

    let count = u32::from_be_bytes(pack[8..12].try_into().unwrap());
    let mut offset = 12usize;
    let mut whole = std::collections::HashSet::new();
    let mut delta_bases = std::collections::HashSet::new();
    for _ in 0..count {
        let entry = gix_pack::data::Entry::from_bytes(
            &pack[offset..],
            u64::try_from(offset).unwrap(),
            gix_hash::Kind::Sha1,
        )
        .expect("parse pack entry");
        let body_start = usize::try_from(entry.data_offset).unwrap();
        let mut decoder = flate2::read::ZlibDecoder::new(&pack[body_start..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).expect("inflate");
        offset = body_start + usize::try_from(decoder.total_in()).unwrap();
        if let gix_pack::data::entry::Header::RefDelta { base_id } = entry.header {
            delta_bases.insert(base_id);
        } else {
            let kind = entry.header.as_kind().expect("a whole entry has a kind");
            whole.insert(hash_loose(kind, &decompressed).unwrap());
        }
    }
    (whole, delta_bases)
}

/// Hands `pack` to real `git index-pack` in the bare repository at `repo`,
/// `Err` carrying git's own stderr.
///
/// The oracle for a pack this workspace built: git resolves every delta, or
/// says which one it cannot.
///
/// # Errors
///
/// Returns git's stderr if `index-pack` refuses the pack.
pub fn index_pack_with_git(repo: &std::path::Path, pack: &[u8]) -> Result<(), String> {
    let idx_path = repo.join("in.idx");
    std::fs::write(repo.join("in.pack"), pack).map_err(|e| e.to_string())?;
    let out = run_git(&["index-pack", "-o", "in.idx", "in.pack"], Some(repo));
    drop(std::fs::remove_file(&idx_path));
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Content that deltas well: every version shares a long prefix and differs
/// only in its tail.
#[must_use]
pub fn near_duplicate_content(tail: u8) -> Vec<u8> {
    let mut content = vec![b'x'; 500];
    content.push(tail);
    content
}

impl TestCommit {
    /// `(kind, bytes)` triples in commit-pack order (blob, tree, commit),
    /// ready to pass straight to [`make_pack`].
    #[must_use]
    pub fn pack_entries(&self) -> Vec<(Kind, &[u8])> {
        vec![
            (Kind::Blob, self.blob_content.as_slice()),
            (Kind::Tree, self.tree_content.as_slice()),
            (Kind::Commit, self.commit_text.as_bytes()),
        ]
    }
}

// ── object builders ───────────────────────────────────────────────────────────

/// A blob object: its id, and `content` unchanged.
///
/// Raw content rather than loose bytes: a pack entry and a staged object
/// both want what the object says, not its loose framing.
#[must_use]
pub fn blob(content: &[u8]) -> (ObjectId, Vec<u8>) {
    (hash_loose(Kind::Blob, content).unwrap(), content.to_vec())
}

/// A tree of `entries`, each `(mode, name, oid)`, in the order given.
///
/// Git requires tree entries sorted by name, so a fixture whose order
/// matters supplies them sorted.
#[must_use]
pub fn tree_of(entries: &[(&str, &str, ObjectId)]) -> (ObjectId, Vec<u8>) {
    let mut content = Vec::new();
    for (mode, name, oid) in entries {
        content.extend_from_slice(format!("{mode} {name}\0").as_bytes());
        content.extend_from_slice(oid.as_slice());
    }
    (hash_loose(Kind::Tree, &content).unwrap(), content)
}

/// A tree with a single `100644` entry at `name` pointing at `blob_oid`.
#[must_use]
pub fn tree_with_blob(name: &str, blob_oid: ObjectId) -> (ObjectId, Vec<u8>) {
    tree_of(&[("100644", name, blob_oid)])
}

/// A minimal commit over `tree`, with `parent` as its sole parent if given.
#[must_use]
pub fn commit(tree: ObjectId, parent: Option<ObjectId>) -> (ObjectId, Vec<u8>) {
    let content = commit_text(tree, parent, BUILDER_IDENTITY, 0, "test").into_bytes();
    (hash_loose(Kind::Commit, &content).unwrap(), content)
}

/// A minimal annotated tag named `tag_name` over commit `target`.
#[must_use]
pub fn annotated_tag(target: ObjectId, tag_name: &str) -> (ObjectId, Vec<u8>) {
    let content = format!(
        "object {target}\ntype commit\ntag {tag_name}\n\
         tagger {BUILDER_IDENTITY} 0 +0000\n\nrelease\n"
    )
    .into_bytes();
    (hash_loose(Kind::Tag, &content).unwrap(), content)
}

/// [`tree_of`] as loose bytes, for the seeders that store what they build.
fn loose_tree(entries: &[(&str, &str, ObjectId)]) -> (ObjectId, Vec<u8>) {
    let (_, content) = tree_of(entries);
    encode_loose(Kind::Tree, &content).unwrap()
}

/// Builds (without storing) a blob → tree → commit triple for
/// `receive_pack` tests that assemble their own pack bytes.
///
/// `parent_sha` chains commits into a history; `timestamp`/`message` keep
/// otherwise-identical commits distinctly hashed.
#[must_use]
pub fn linear_commit(
    blob_content: &[u8],
    parent_sha: Option<&str>,
    timestamp: u64,
    message: &str,
) -> TestCommit {
    let (blob_oid, _) = encode_loose(Kind::Blob, blob_content).unwrap();

    let (tree_oid, tree_content) = tree_with_blob("hello.txt", blob_oid);
    let tree_sha = tree_oid.to_hex().to_string();

    let parent = parent_sha.map(|p| ObjectId::from_hex(p.as_bytes()).unwrap());
    let commit_text = commit_text(tree_oid, parent, BUILDER_IDENTITY, timestamp, message);
    let (commit_oid, _) = encode_loose(Kind::Commit, commit_text.as_bytes()).unwrap();
    let commit_sha = commit_oid.to_hex().to_string();

    TestCommit {
        blob_oid,
        blob_content: blob_content.to_vec(),
        tree_oid,
        tree_sha,
        tree_content,
        commit_oid,
        commit_sha,
        commit_text,
    }
}

// ── the system git ────────────────────────────────────────────────────────────

/// A `git` command for `args`, run in `dir` if one is given.
///
/// The identity and the configuration are fixed: real git reads the invoking
/// user's own otherwise, so a commit hashes differently on two machines.
pub fn git_command(args: &[&str], dir: Option<&std::path::Path>) -> std::process::Command {
    let mut cmd = std::process::Command::new("git");
    // A prompt has nobody to answer it here, and it hangs the run instead.
    cmd.env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.com")
        .env("GIT_AUTHOR_DATE", "1234567890 +0000")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.com")
        .env("GIT_COMMITTER_DATE", "1234567890 +0000");
    if let Some(dir) = dir {
        cmd.current_dir(dir);
    }
    cmd.args(args);
    cmd
}

/// Runs [`git_command`] to the end and hands back what it printed.
///
/// For a caller with nothing to add to the command; whoever has — an
/// asynchronous run, an extra header — builds it and runs it itself.
pub fn run_git(args: &[&str], dir: Option<&std::path::Path>) -> std::process::Output {
    git_command(args, dir)
        .output()
        .expect("git not found in PATH")
}
