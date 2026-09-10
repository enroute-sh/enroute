//! S3-backed git object store.
//!
//! Keys — `repos/{storage_key}/segments/{ulid}` and
//! `repos/{storage_key}/tags/{sha}` — are scoped by `RepoMetadata::storage_key`,
//! a random key from [`enroute_git_metadata`], deliberately
//! not `RepoMetadata::id`: that surrogate is small and monotonic, useful for
//! the metadata store's own sharding but a hot-partition-prone storage-key
//! prefix. A random token also keeps renaming a repository cheap — one row —
//! without coupling storage layout to the id scheme. Neither the commit graph
//! nor refs live here; both are in Postgres via `enroute_git_metadata`. Each
//! commit's introduced objects form one commit-pack image (see
//! `commit_pack`), concatenated into segment objects the ingest writer cuts
//! at a byte target; `commits.segment_id` says which segment holds one and
//! where. A segment is inert until the push's `append` transaction
//! references it, so a failed push leaves an orphan for the janitor, never
//! a dangling reference — ULIDs keep each segment's key unique to its writer.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt as _, TryStreamExt as _};
// Re-exported so callers assembling multi-segment packs don't need to depend on `object_store`.
pub use object_store::PutPayload;
use object_store::{
    Error as StoreError, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode,
    WriteMultipart, path::Path,
};

use enroute_git_core::Ulid;
use enroute_git_cost::{CountingStore, Meter, StoreRole};
use enroute_git_metadata::RepoMetadata;

/// Chunk size for streamed segment uploads ([`Store::segment_writer`]).
///
/// S3's multipart minimum part size, so it can't be lowered.
pub const MULTIPART_CHUNK_BYTES: usize = 5 * 1024 * 1024;

/// Cap on concurrently uploading parts per [`ObjectWriter`], bounding its
/// resident memory to about this many [`MULTIPART_CHUNK_BYTES`] parts.
pub(crate) const MULTIPART_MAX_INFLIGHT_PARTS: usize = 8;

pub mod commit_pack;
pub mod concurrency;
pub use commit_pack::{
    COMMIT_PACK_HEADER_USIZE, CommitPackHeader, MAX_PACK_ENTRY_HEADER_BYTES, ObjectDeflater,
    PackEntryHeader, PackTrailerEntry, blob_section_offset, decode_commit_pack_header,
    decode_commit_pack_object, decode_pack_entry_header, decode_pack_trailer,
    encode_commit_pack_header, encode_commit_pack_object, encode_pack_entry_header,
    encode_pack_trailer, has_pack_entry_header, trailer_suffix_len,
};
pub use concurrency::bounded;

/// S3-backed store for git objects, at the root of whatever it is given.
///
/// A deployment that wants its keys under a prefix wraps the bucket in one —
/// the two segment catalogs reach the bucket without passing through here.
#[derive(Debug)]
pub struct Store {
    inner: Arc<dyn ObjectStore>,
}

impl Store {
    /// Create a store backed by `inner`.
    ///
    /// Permanent objects only; a push's staging belongs to `enroute_git_ingest`.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// The same store with its requests charged to `meter` as
    /// [`StoreRole::Primary`].
    ///
    /// Cheap: one `Arc` around the existing backend.
    #[must_use]
    pub fn metered(&self, meter: Arc<Meter>) -> Self {
        Self {
            inner: CountingStore::wrap(Arc::clone(&self.inner), meter, StoreRole::Primary),
        }
    }

    /// The one place the segment key layout is spelled, shared by the
    /// per-segment path and the LIST.
    fn segments_prefix(repo: &RepoMetadata) -> Path {
        Path::from(format!("repos/{}/segments", repo.storage_key))
    }

    /// Absolute path of a segment object from its ULID.
    fn segment_path(repo: &RepoMetadata, id: Ulid) -> Path {
        Self::segments_prefix(repo).join(id.to_string().as_str())
    }

    fn tag_key(repo: &RepoMetadata, sha: &str) -> Path {
        Path::from(format!("repos/{}/tags/{sha}", repo.storage_key))
    }

    /// Open a writer for the segment object `id`, fed incrementally.
    ///
    /// Buffers until [`MULTIPART_CHUNK_BYTES`], then switches to a real
    /// multipart upload so a large segment never sits fully resident.
    #[must_use]
    pub fn segment_writer(&self, repo: &RepoMetadata, id: Ulid) -> ObjectWriter {
        ObjectWriter::new(self.inner.clone(), Self::segment_path(repo, id))
    }

    /// Every segment object under `repo`'s prefix, as `(id, size_bytes)` —
    /// the janitor's LIST side.
    ///
    /// A name that doesn't parse as a ULID isn't ours, and is skipped rather
    /// than becoming a deletion candidate.
    ///
    /// # Errors
    /// Returns an error if the object store is unavailable.
    pub async fn list_segments(&self, repo: &RepoMetadata) -> Result<Vec<(Ulid, u64)>> {
        let mut entries = Vec::new();
        let mut stream = self.inner.list(Some(&Self::segments_prefix(repo)));
        while let Some(meta) = stream.try_next().await.context("listing segments")? {
            let Some(id) = meta
                .location
                .filename()
                .and_then(|n| Ulid::from_string(n).ok())
            else {
                continue;
            };
            entries.push((id, meta.size));
        }
        Ok(entries)
    }

    /// Delete everything stored for `repo`, returning how many objects went
    /// and how many bytes they held.
    ///
    /// Deletes by prefix rather than by what the metadata records, since
    /// this runs for a repository being erased. Resumable.
    ///
    /// # Errors
    /// Returns an error if the object store is unavailable.
    pub async fn delete_repo_objects(&self, repo: &RepoMetadata) -> Result<(u64, u64)> {
        let prefix = Path::from(format!("repos/{}", repo.storage_key));
        // Summed as listed rather than as deleted, because a batch reports
        // which keys went and not what they held. The two agree wherever the
        // count is read at all: a failure part-way returns `Err` instead.
        let bytes = Arc::new(AtomicU64::new(0));
        let listed = Arc::clone(&bytes);
        let locations = self
            .inner
            .list(Some(&prefix))
            .map_ok(move |meta| {
                listed.fetch_add(meta.size, Ordering::Relaxed);
                meta.location
            })
            .boxed();

        let mut deletions = self.inner.delete_stream(locations);
        let mut deleted = 0;
        while let Some(_gone) = deletions
            .try_next()
            .await
            .context("deleting a repository's objects")?
        {
            deleted += 1;
        }
        Ok((deleted, bytes.load(Ordering::Relaxed)))
    }

    /// Delete a segment object.
    ///
    /// Idempotent: deleting an already-absent segment is not an error.
    ///
    /// # Errors
    /// Returns an error if the object store is unavailable.
    pub async fn delete_segment(&self, repo: &RepoMetadata, id: Ulid) -> Result<()> {
        match self.inner.delete(&Self::segment_path(repo, id)).await {
            Ok(()) | Err(StoreError::NotFound { .. }) => Ok(()),
            Err(e) => Err(e).context("deleting segment"),
        }
    }

    /// Store an annotated tag as a loose object at the dedicated tag path.
    ///
    /// Re-storing a tag is a no-op in effect: the key is the tag's own hash.
    ///
    /// # Errors
    /// Returns an error if the object store is unavailable.
    pub async fn put_tag(&self, repo: &RepoMetadata, sha: &str, data: Bytes) -> Result<()> {
        // Not create-only: object_store retries a dropped connection only on
        // a request it marked idempotent, and the conditional was never read.
        self.inner
            .put_opts(
                &Self::tag_key(repo, sha),
                data.into(),
                PutMode::Overwrite.into(),
            )
            .await
            .map(|_| ())
            .context("putting tag")
    }

    /// Fetch an annotated tag by SHA.
    ///
    /// # Errors
    /// Returns an error if the object store is unavailable.
    pub async fn get_tag(&self, repo: &RepoMetadata, sha: &str) -> Result<Option<Bytes>> {
        match self.inner.get(&Self::tag_key(repo, sha)).await {
            Ok(result) => Ok(Some(result.bytes().await?)),
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e).context("getting tag"),
        }
    }

    /// Stream a byte range from a segment object as it arrives, or `None` if
    /// no object exists for `id`.
    ///
    /// `len = Some(n)` bounds the read to `[offset, offset+n)`; `len = None`
    /// reads to the end of the object.
    ///
    /// # Errors
    /// Returns an error if the object store is unavailable.
    #[tracing::instrument(name = "enroute_git_store::stream_segment_slice", skip(self, repo), fields(repo_id = %repo.id))]
    pub async fn stream_segment_slice(
        &self,
        repo: &RepoMetadata,
        id: Ulid,
        offset: u64,
        len: Option<u64>,
    ) -> Result<Option<BoxStream<'static, Result<Bytes, std::io::Error>>>> {
        let range = match len {
            Some(n) => GetRange::Bounded(offset..offset + n),
            None => GetRange::Offset(offset),
        };
        match self
            .inner
            .get_opts(
                &Self::segment_path(repo, id),
                GetOptions {
                    range: Some(range),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(result) => {
                let stream = result.into_stream().map_err(std::io::Error::other).boxed();
                Ok(Some(stream))
            }
            Err(StoreError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e).context("streaming segment slice"),
        }
    }

    /// Fetch a byte range from a segment object into a buffer.
    ///
    /// See [`Self::stream_segment_slice`] for the offset/len semantics.
    ///
    /// # Errors
    /// Returns an error if the object store is unavailable.
    pub async fn get_segment_slice(
        &self,
        repo: &RepoMetadata,
        id: Ulid,
        offset: u64,
        len: Option<u64>,
    ) -> Result<Option<Bytes>> {
        let Some(stream) = self.stream_segment_slice(repo, id, offset, len).await? else {
            return Ok(None);
        };
        let mut chunks: Vec<Bytes> = stream
            .map_err(|e| anyhow::anyhow!(e))
            .try_collect()
            .await
            .context("reading segment slice")?;
        // Small ranged GETs usually arrive as one chunk — skip the concat copy.
        if chunks.len() == 1 {
            return Ok(chunks.pop());
        }
        Ok(Some(chunks.concat().into()))
    }
}

/// Start a real multipart upload and hand it every chunk buffered so far.
///
/// A free function rather than a method: `WriteMultipart` is `Send` but not
/// `Sync`, so a held `&ObjectWriter` would infect every async caller.
#[tracing::instrument(name = "enroute_git_store::start_multipart", skip(store, segments), fields(key = %key, chunk_count = segments.len()))]
async fn start_multipart(
    store: &Arc<dyn ObjectStore>,
    key: &Path,
    segments: Vec<Bytes>,
) -> Result<WriterMode> {
    let upload = store
        .put_multipart(key)
        .await
        .context("starting multipart upload")?;
    let mut writer = WriteMultipart::new_with_chunk_size(upload, MULTIPART_CHUNK_BYTES);
    for segment in segments {
        writer.put(segment);
    }
    Ok(WriterMode::Streaming(writer))
}

#[derive(Debug)]
enum WriterMode {
    /// Nothing uploaded yet — every segment seen so far is held here.
    Buffered { segments: Vec<Bytes>, size: usize },
    /// Buffered total crossed [`MULTIPART_CHUNK_BYTES`]; a real multipart
    /// upload is in progress.
    Streaming(WriteMultipart),
}

/// A writer for one object, streaming or buffered, on whichever store it was
/// opened against.
///
/// Hides buffered-vs-streaming behind one write/finish API: small objects
/// avoid multipart's round trips, large ones never sit fully resident.
#[derive(Debug)]
pub struct ObjectWriter {
    store: Arc<dyn ObjectStore>,
    key: Path,
    mode: WriterMode,
}

impl ObjectWriter {
    /// Write one object to `key` in `store`, buffering and then switching to a
    /// multipart upload once past [`MULTIPART_CHUNK_BYTES`].
    ///
    /// [`Store::segment_writer`] is the usual way in; this is for writing to
    /// a store `Store` doesn't own, e.g. a pack staged for handoff.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, key: Path) -> Self {
        Self {
            key,
            store,
            mode: WriterMode::Buffered {
                segments: Vec::new(),
                size: 0,
            },
        }
    }

    /// Buffer or stream `bytes` without copying it, starting a multipart
    /// upload once the running total reaches [`MULTIPART_CHUNK_BYTES`].
    ///
    /// # Errors
    /// Returns an error if crossing the threshold and starting the
    /// multipart upload fails.
    pub async fn write(&mut self, bytes: Bytes) -> Result<()> {
        let mode = std::mem::replace(
            &mut self.mode,
            WriterMode::Buffered {
                segments: Vec::new(),
                size: 0,
            },
        );
        self.mode = match mode {
            WriterMode::Streaming(mut writer) => {
                // Without this a producer outrunning the store piles every
                // queued part into memory. Bounds the writer at
                // MULTIPART_MAX_INFLIGHT_PARTS * MULTIPART_CHUNK_BYTES.
                writer
                    .wait_for_capacity(MULTIPART_MAX_INFLIGHT_PARTS)
                    .await
                    .context("waiting for multipart upload capacity")?;
                writer.put(bytes);
                WriterMode::Streaming(writer)
            }
            WriterMode::Buffered {
                mut segments,
                mut size,
            } => {
                size += bytes.len();
                segments.push(bytes);
                if size < MULTIPART_CHUNK_BYTES {
                    WriterMode::Buffered { segments, size }
                } else {
                    start_multipart(&self.store, &self.key, segments).await?
                }
            }
        };
        Ok(())
    }

    /// Complete the upload: one `PUT` if the object never crossed
    /// [`MULTIPART_CHUNK_BYTES`], otherwise the multipart completion.
    ///
    /// # Errors
    /// Returns an error if the put, or any part upload or completion, fails.
    #[tracing::instrument(
        name = "enroute_git_store::finish",
        skip(self),
        fields(key = %self.key, streaming = matches!(self.mode, WriterMode::Streaming(_)))
    )]
    pub async fn finish(self) -> Result<()> {
        match self.mode {
            WriterMode::Buffered { segments, .. } => {
                let payload: PutPayload = segments.into_iter().collect();
                // Overwrite rather than create-only: object_store retries a
                // dropped connection only on a request it marked idempotent,
                // and a key here belongs to exactly one writer, so the
                // conditional bought nothing but a failed push.
                self.store
                    .put_opts(&self.key, payload, PutMode::Overwrite.into())
                    .await
                    .map(|_| ())
                    .context("putting object")
            }
            WriterMode::Streaming(writer) => writer
                .finish()
                .await
                .map(|_| ())
                .context("finishing multipart upload"),
        }
    }

    /// Abort the upload, cleaning up any parts already uploaded.
    ///
    /// Call this on error instead of dropping the writer — an abandoned
    /// multipart upload otherwise leaks storage until S3 reaps it.
    ///
    /// # Errors
    /// Returns an error if the abort request itself fails.
    pub async fn abort(self) -> Result<()> {
        match self.mode {
            WriterMode::Buffered { .. } => Ok(()),
            WriterMode::Streaming(writer) => {
                writer.abort().await.context("aborting multipart upload")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bytes::Bytes;
    use enroute_git_core::{RepoId, StorageKey};
    use object_store::memory::InMemory;

    fn make_store() -> Arc<Store> {
        Arc::new(Store::new(Arc::new(InMemory::new())))
    }

    /// A [`RepoMetadata`] fixture for tests that only need *some* repo
    /// identity — only `storage_key` is read here.
    fn test_repo() -> RepoMetadata {
        RepoMetadata {
            id: RepoId::new(1),
            storage_key: StorageKey::new_v4(),
            default_branch: "refs/heads/main".to_string(),
        }
    }

    /// A distinct, reproducible segment id per test.
    fn seg(n: u64) -> Ulid {
        Ulid::from_parts(n, u128::from(n))
    }

    /// Write `data` as a whole segment — the shape most read-path tests want.
    async fn put_segment(store: &Arc<Store>, repo: &RepoMetadata, id: Ulid, data: Bytes) {
        let mut writer = store.clone().segment_writer(repo, id);
        writer.write(data).await.unwrap();
        writer.finish().await.unwrap();
    }

    /// Everything under the prefix, and nothing under a neighbour's.
    ///
    /// The bytes are asserted rather than assumed: `delete_stream` reports
    /// which keys went, not what they held.
    #[tokio::test]
    async fn delete_repo_objects_takes_the_repository_and_leaves_its_neighbour() {
        let store = make_store();
        let repo = test_repo();
        let neighbour = test_repo();

        let first = encode_commit_pack_object(b"first").unwrap();
        let second = encode_commit_pack_object(b"second").unwrap();
        let tag = Bytes::from_static(b"tag object bytes");
        put_segment(&store, &repo, seg(1), first.clone()).await;
        put_segment(&store, &repo, seg(2), second.clone()).await;
        store.put_tag(&repo, "abc123", tag.clone()).await.unwrap();
        put_segment(&store, &neighbour, seg(3), first.clone()).await;

        let (deleted, bytes) = store.delete_repo_objects(&repo).await.unwrap();
        assert_eq!(deleted, 3, "two segments and a tag");
        let stored = first.len() + second.len() + tag.len();
        assert_eq!(
            bytes,
            u64::try_from(stored).unwrap(),
            "the bytes reclaimed are the bytes that were there"
        );

        assert!(
            store
                .get_segment_slice(&repo, seg(1), 0, None)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get_segment_slice(&repo, seg(2), 0, None)
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.get_tag(&repo, "abc123").await.unwrap().is_none());
        assert!(
            store
                .get_segment_slice(&neighbour, seg(3), 0, None)
                .await
                .unwrap()
                .is_some(),
            "a storage key belongs to one repository, so the prefix cannot reach another's"
        );

        // Nothing left to take, and saying so is not an error: the janitor
        // re-runs over repositories it already swept.
        assert_eq!(store.delete_repo_objects(&repo).await.unwrap(), (0, 0));
    }

    #[tokio::test]
    async fn segment_roundtrip() {
        let store = make_store();
        let repo = test_repo();
        let data = encode_commit_pack_object(b"test content").unwrap();
        put_segment(&store, &repo, seg(0), data.clone()).await;
        assert_eq!(
            store
                .get_segment_slice(&repo, seg(0), 0, None)
                .await
                .unwrap()
                .unwrap(),
            data
        );
    }

    #[tokio::test]
    async fn segment_writer_roundtrip_single_small_write() {
        let store = make_store();
        let repo = test_repo();
        let key = seg(1);
        let mut writer = store.clone().segment_writer(&repo, key);
        writer
            .write(Bytes::from_static(b"header+manifest+object"))
            .await
            .unwrap();
        writer.finish().await.unwrap();

        assert_eq!(
            store
                .get_segment_slice(&repo, key, 0, None)
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(b"header+manifest+object")
        );
    }

    #[tokio::test]
    async fn segment_writer_duplicate_small_write_is_idempotent() {
        // A replayed write of the same id — a retried upload, say — must
        // leave the segment readable rather than failing the push.
        let store = make_store();
        let repo = test_repo();
        let key = seg(2);

        let mut first = store.clone().segment_writer(&repo, key);
        first.write(Bytes::from_static(b"content")).await.unwrap();
        first.finish().await.unwrap();

        let mut second = store.clone().segment_writer(&repo, key);
        second.write(Bytes::from_static(b"content")).await.unwrap();
        second.finish().await.unwrap();

        assert_eq!(
            store
                .get_segment_slice(&repo, key, 0, None)
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(b"content")
        );
    }

    #[tokio::test]
    async fn segment_writer_roundtrip_crosses_part_boundary() {
        let store = make_store();
        let repo = test_repo();
        let key = seg(3);
        let mut writer = store.clone().segment_writer(&repo, key);

        let chunk = Bytes::from(vec![7u8; 512 * 1024]);
        let mut expected = Vec::new();
        for _ in 0..12 {
            writer.write(chunk.clone()).await.unwrap();
            expected.extend_from_slice(&chunk);
        }
        writer.finish().await.unwrap();

        let stored = store
            .get_segment_slice(&repo, key, 0, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.len(), expected.len());
        assert_eq!(&stored[..], &expected[..]);
    }

    #[tokio::test]
    async fn segment_writer_abort_while_buffered_is_noop() {
        let store = make_store();
        let repo = test_repo();
        let key = seg(4);
        let mut writer = store.clone().segment_writer(&repo, key);
        writer.write(Bytes::from_static(b"partial")).await.unwrap();
        writer.abort().await.unwrap();

        assert!(
            store
                .get_segment_slice(&repo, key, 0, None)
                .await
                .unwrap()
                .is_none(),
            "aborted upload must not leave a visible object"
        );
    }

    #[tokio::test]
    async fn segment_writer_abort_after_crossing_threshold_leaves_no_object() {
        let store = make_store();
        let repo = test_repo();
        let key = seg(5);
        let mut writer = store.clone().segment_writer(&repo, key);

        let chunk = Bytes::from(vec![7u8; 512 * 1024]);
        for _ in 0..12 {
            writer.write(chunk.clone()).await.unwrap();
        }
        // Threshold crossed — exercises abort() against a real multipart upload, not the buffered no-op.
        writer.abort().await.unwrap();

        assert!(
            store
                .get_segment_slice(&repo, key, 0, None)
                .await
                .unwrap()
                .is_none(),
            "aborted multipart upload must not leave a visible object"
        );
    }

    #[tokio::test]
    async fn segment_slice_bounded() {
        let store = make_store();
        let repo = test_repo();
        put_segment(&store, &repo, seg(6), Bytes::from_static(b"hello world")).await;
        let slice = store
            .get_segment_slice(&repo, seg(6), 6, Some(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&slice[..], b"world");
    }

    #[tokio::test]
    async fn segment_slice_tail() {
        let store = make_store();
        let repo = test_repo();
        put_segment(&store, &repo, seg(7), Bytes::from_static(b"hello world")).await;
        let tail = store
            .get_segment_slice(&repo, seg(7), 6, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&tail[..], b"world");
    }

    #[tokio::test]
    async fn missing_segment_returns_none() {
        let store = make_store();
        let repo = test_repo();
        assert!(
            store
                .get_segment_slice(&repo, seg(8), 0, None)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn list_and_delete_segments() {
        let store = make_store();
        let repo = test_repo();

        put_segment(&store, &repo, seg(9), Bytes::from_static(b"11111")).await;
        put_segment(&store, &repo, seg(10), Bytes::from_static(b"222")).await;
        // Anything not named by a ULID isn't ours: never listed, so never a
        // deletion candidate.
        store
            .inner
            .put(
                &Path::from(format!("repos/{}/segments/stray", repo.storage_key)),
                Bytes::from_static(b"stray").into(),
            )
            .await
            .unwrap();

        let mut listed = store.list_segments(&repo).await.unwrap();
        listed.sort();
        assert_eq!(listed, vec![(seg(9), 5), (seg(10), 3)]);

        store.delete_segment(&repo, seg(9)).await.unwrap();
        // Idempotent: a second delete of the same segment is not an error.
        store.delete_segment(&repo, seg(9)).await.unwrap();
        let listed = store.list_segments(&repo).await.unwrap();
        assert_eq!(listed, vec![(seg(10), 3)]);
    }
}
