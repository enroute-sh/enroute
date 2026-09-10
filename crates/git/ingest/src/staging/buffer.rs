//! Push-scoped staging: what [`crate::materialise`] produces accumulates in
//! memory and flushes to the staging store in chunks.
//!
//! Content is staged already compressed at the commit-pack's chosen level,
//! so promotion copies it verbatim with no further zlib work.

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{Error, ObjectHashMap, ObjectHashSet, object_hash_map_with_capacity};
use enroute_git_retrieve::{RepoMetadata, Storage};
use enroute_git_store::decode_commit_pack_object;

use crate::staging::StagingSession;

use crate::upload::{StagedEncodings, StagedItem, StagedObjectLocation};

/// Maximum number of bytes accumulated in memory before flushing a staging chunk.
const STAGING_CHUNK_BYTES: usize = 5 * 1024 * 1024;

/// Accumulates one push's compressed objects in memory, flushing chunks to
/// the staging store as they fill and indexing each object's location.
///
/// Placing only: compression happens on the worker that resolved the
/// object, so nothing here is on the CPU path.
#[derive(Debug, Default)]
pub(crate) struct StagingBuffer {
    current: Vec<u8>,
    chunk_id: u32,
    index: ObjectHashMap<StagedEncodings>,
}

impl StagingBuffer {
    /// Buffer `compressed` — already at the commit-pack's chosen level —
    /// under `sha`.
    ///
    /// The compressing counterpart to [`push`](Self::push), for callers that
    /// compressed on a worker thread and hand the bytes back to be placed.
    ///
    /// # Errors
    /// A triggered chunk flush's staging store write fails.
    pub(crate) async fn push_compressed(
        &mut self,
        item: StagedItem,
        compressed: &[u8],
        session: &StagingSession,
        repo: &RepoMetadata,
    ) -> Result<(), Error> {
        let offset = self.current.len();
        self.current.extend_from_slice(compressed);
        self.index.entry(item.sha).or_default().insert(
            item.base,
            StagedObjectLocation {
                chunk_id: self.chunk_id,
                offset,
                compressed_len: compressed.len(),
                decompressed_len: item.decompressed_len,
                kind: item.kind,
            },
        );
        if self.current.len() >= STAGING_CHUNK_BYTES {
            self.flush(session, repo).await?;
        }
        Ok(())
    }

    /// Flush the unwritten buffer to a new staging chunk, if non-empty.
    ///
    /// # Errors
    /// Returns an error if the staging store write fails.
    pub(crate) async fn flush(
        &mut self,
        session: &StagingSession,
        repo: &RepoMetadata,
    ) -> Result<(), Error> {
        if self.current.is_empty() {
            return Ok(());
        }
        let data = Bytes::from(std::mem::take(&mut self.current));
        session.put_chunk(repo, self.chunk_id, data).await?;
        self.chunk_id += 1;
        Ok(())
    }

    /// Decompressed content and kind for `sha`, for delta-base application.
    ///
    /// Checks the unflushed buffer first, then a flushed chunk. `Ok(None)`
    /// if `sha` isn't in this pack.
    ///
    /// # Errors
    /// The staging store read or decompression fails.
    pub(crate) async fn get(
        &self,
        sha: &ObjectId,
        session: &StagingSession,
        repo: &RepoMetadata,
    ) -> Result<Option<(Kind, Bytes)>, Error> {
        let Some(&StagedObjectLocation {
            chunk_id,
            offset,
            compressed_len,
            decompressed_len,
            kind,
        }) = self.index.get(sha).and_then(StagedEncodings::whole)
        else {
            return Ok(None);
        };
        let compressed = if chunk_id == self.chunk_id {
            let Some(s) = self.current.get(offset..offset + compressed_len) else {
                return Ok(None);
            };
            Bytes::copy_from_slice(s)
        } else {
            session
                .get_chunk_range(repo, chunk_id, offset..offset + compressed_len)
                .await
                .map_err(|e| anyhow::anyhow!("staging chunk range read: {e:#}"))?
        };
        let expected_len = u64::try_from(decompressed_len)
            .map_err(|e| anyhow::anyhow!("decompressed length: {e}"))?;
        let content = decode_commit_pack_object(&compressed, expected_len)
            .map_err(|e| anyhow::anyhow!("decode staged object {sha}: {e}"))?;
        Ok(Some((kind, content)))
    }

    /// Consume the buffer, returning the finished `oid → location` index.
    #[must_use]
    pub(crate) fn into_index(self) -> ObjectHashMap<StagedEncodings> {
        self.index
    }
}

/// Delta base lookup: staging buffer first, then the primary store for
/// thin-pack bases.
///
/// Oids neither place has are omitted, letting a caller wait on a base this
/// push is about to resolve instead of giving up.
///
/// # Errors
/// Returns an error if a lookup or read fails.
pub(crate) async fn fetch_bases(
    shas: &[ObjectId],
    staging: &StagingBuffer,
    session: &StagingSession,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<ObjectHashMap<(Kind, Bytes)>, Error> {
    // Deduped here rather than trusted to the caller: a repeat costs a staging
    // read apiece.
    let distinct: ObjectHashSet = shas.iter().copied().collect();
    let mut found = object_hash_map_with_capacity(distinct.len());
    // Staging is this worker's own disk; the misses are what cross a network.
    let mut absent = Vec::new();
    for sha in distinct {
        match staging.get(&sha, session, repo).await? {
            Some(hit) => {
                found.insert(sha, hit);
            }
            None => absent.push(sha),
        }
    }
    found.extend(enroute_git_retrieve::objects(state, repo, &absent).await?);
    Ok(found)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use gix_hash::ObjectId;
    use gix_object::Kind;

    use super::{StagingBuffer, fetch_bases};
    use enroute_git_core::{Error, encode_loose, hash_loose};
    use enroute_git_retrieve::{RepoMetadata, Storage};
    use enroute_git_test_support::{make_state, put_loose_as_singleton_pack};

    use crate::staging::StagingSession;
    use crate::test_helpers::staging_store;

    /// The single-object lookup most of these tests are written against.
    async fn fetch_base(
        sha: ObjectId,
        staging: &StagingBuffer,
        session: &StagingSession,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> Result<Option<(Kind, Bytes)>, Error> {
        Ok(fetch_bases(&[sha], staging, session, state, repo)
            .await?
            .remove(&sha))
    }

    fn make_obj(content: &[u8]) -> ObjectId {
        hash_loose(Kind::Blob, content).unwrap()
    }

    /// Compress and buffer a blob, the way the resolve path's workers do.
    async fn stage(
        staging: &mut StagingBuffer,
        sha: ObjectId,
        content: &[u8],
        session: &StagingSession,
        repo: &RepoMetadata,
    ) {
        use crate::upload::StagedItem;
        let compressed = enroute_git_store::encode_commit_pack_object(content).unwrap();
        staging
            .push_compressed(
                StagedItem {
                    sha,
                    kind: Kind::Blob,
                    base: None,
                    decompressed_len: content.len(),
                },
                &compressed,
                session,
                repo,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fetch_base_returns_current_buffer_bytes() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = staging_store().start_session(&repo);
        let sha = make_obj(b"hello");
        let mut staging = StagingBuffer::default();
        stage(&mut staging, sha, b"hello", &session, &repo).await;
        // Object is in the unflushed buffer — no staging read needed.
        let (_, content) = fetch_base(sha, &staging, &session, &state, &repo)
            .await
            .unwrap()
            .expect("staged object should be found");
        assert_eq!(&content[..], b"hello");
    }

    #[tokio::test]
    async fn fetch_base_returns_flushed_chunk_bytes() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = staging_store().start_session(&repo);
        let sha = make_obj(b"hello");
        let mut staging = StagingBuffer::default();
        stage(&mut staging, sha, b"hello", &session, &repo).await;
        staging.flush(&session, &repo).await.unwrap();
        // Object is in a flushed chunk — requires a read of the chunk.
        let (_, content) = fetch_base(sha, &staging, &session, &state, &repo)
            .await
            .unwrap()
            .expect("staged object should be found");
        assert_eq!(&content[..], b"hello");
    }

    #[tokio::test]
    async fn fetch_base_falls_back_to_primary() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = staging_store().start_session(&repo);
        let (sha, loose) = encode_loose(Kind::Blob, b"hello").unwrap();
        put_loose_as_singleton_pack(&state, &repo, &sha.to_hex().to_string(), Bytes::from(loose))
            .await;
        let staging = StagingBuffer::default();
        let (_, content) = fetch_base(sha, &staging, &session, &state, &repo)
            .await
            .unwrap()
            .expect("staged object should be found");
        assert_eq!(&content[..], b"hello");
    }

    #[tokio::test]
    /// Absent must be an answer, not a failure: resolution waits on a base
    /// its own pack is about to produce; only a broken lookup errors.
    async fn fetch_base_reports_a_missing_object_as_absent() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = staging_store().start_session(&repo);
        let sha = make_obj(b"hello");
        let staging = StagingBuffer::default();
        assert!(
            fetch_base(sha, &staging, &session, &state, &repo)
                .await
                .unwrap()
                .is_none()
        );
    }

    /// Getting the staged/stored/absent split wrong is how a base comes back
    /// under the wrong oid, so the contents are checked, not just the count.
    #[tokio::test]
    async fn fetch_bases_answers_staged_stored_and_absent_together() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = staging_store().start_session(&repo);

        let staged = make_obj(b"staged");
        let mut staging = StagingBuffer::default();
        stage(&mut staging, staged, b"staged", &session, &repo).await;

        let (stored, loose) = encode_loose(Kind::Blob, b"stored").unwrap();
        put_loose_as_singleton_pack(
            &state,
            &repo,
            &stored.to_hex().to_string(),
            Bytes::from(loose),
        )
        .await;

        let absent = make_obj(b"absent");
        let found = fetch_bases(&[staged, stored, absent], &staging, &session, &state, &repo)
            .await
            .unwrap();

        assert_eq!(&found[&staged].1[..], b"staged");
        assert_eq!(&found[&stored].1[..], b"stored");
        assert!(
            !found.contains_key(&absent),
            "an oid neither place has must be omitted, not invented"
        );
    }

    /// Staged and flushed on purpose: that is the path where a repeat costs a
    /// read apiece, the store side collapsing duplicates in `batch_lookup`.
    #[tokio::test]
    async fn fetch_bases_answers_a_repeated_oid_once() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = staging_store().start_session(&repo);
        let sha = make_obj(b"shared");
        let mut staging = StagingBuffer::default();
        stage(&mut staging, sha, b"shared", &session, &repo).await;
        staging.flush(&session, &repo).await.unwrap();

        let found = fetch_bases(&[sha, sha, sha], &staging, &session, &state, &repo)
            .await
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(&found[&sha].1[..], b"shared");
    }
}
