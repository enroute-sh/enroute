//! Where a push's objects live before they are permanent.
//!
//! [`buffer`] is the near half, buffering in memory; [`store`] is the far
//! half it flushes to. Both live here, not in `enroute-git-store`.
//!
//! See [`StagingStore`] for why that distinction is load-bearing.

mod buffer;
mod store;

use std::sync::Arc;

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{Error, ObjectHashMap};
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::upload::{StagedEncodings, StagedItem};

pub(crate) use buffer::{StagingBuffer, fetch_bases};
pub(crate) use store::{StagingSession, StagingStore};

/// One push's staging area: the objects it has produced so far, and the
/// registration that makes them its own.
///
/// The session is the only way the buffer's reads and writes reach the
/// staging store, and it deletes them when the push ends.
#[derive(Debug)]
pub(crate) struct Staging {
    buffer: StagingBuffer,
    session: Arc<StagingSession>,
}

impl Staging {
    /// Register a fresh area for `repo` in `store`.
    pub(crate) fn new(store: Arc<StagingStore>, repo: &RepoMetadata) -> Self {
        Self {
            buffer: StagingBuffer::default(),
            // Shared so the promotion pump can hand a clone to each spawned
            // segment task, keeping chunks alive while any of them can read.
            session: Arc::new(store.start_session(repo)),
        }
    }

    /// Stage the object's own bytes.
    ///
    /// # Errors
    /// Returns an error if the underlying staging write fails.
    pub(crate) async fn whole(
        &mut self,
        sha: ObjectId,
        kind: Kind,
        compressed: &[u8],
        decompressed_len: usize,
        repo: &RepoMetadata,
    ) -> Result<(), Error> {
        self.push(
            StagedItem {
                sha,
                kind,
                base: None,
                decompressed_len,
            },
            compressed,
            repo,
        )
        .await
    }

    /// Stage a second encoding of an already-known object: the delta that
    /// rebuilds it from `base`.
    ///
    /// # Errors
    /// Returns an error if the underlying staging write fails.
    pub(crate) async fn encoding(
        &mut self,
        sha: ObjectId,
        kind: Kind,
        base: ObjectId,
        compressed: &[u8],
        decompressed_len: usize,
        repo: &RepoMetadata,
    ) -> Result<(), Error> {
        self.push(
            StagedItem {
                sha,
                kind,
                base: Some(base),
                decompressed_len,
            },
            compressed,
            repo,
        )
        .await
    }

    async fn push(
        &mut self,
        item: StagedItem,
        compressed: &[u8],
        repo: &RepoMetadata,
    ) -> Result<(), Error> {
        self.buffer
            .push_compressed(item, compressed, &self.session, repo)
            .await
    }

    /// Fetch `sha`'s decompressed content and kind — this push's staged
    /// objects first, then the primary store (thin-pack bases).
    ///
    /// `None` means neither has it, which for a delta base means the object
    /// is elsewhere in the same pack and hasn't been resolved yet.
    ///
    /// # Errors
    /// Returns an error if a lookup or read fails — as distinct from `sha`
    /// simply not being there.
    pub(crate) async fn fetch(
        &self,
        sha: ObjectId,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> Result<Option<(Kind, Bytes)>, Error> {
        // A batch of one: a second body for the same routing only lets the
        // two disagree.
        Ok(self.fetch_many(&[sha], state, repo).await?.remove(&sha))
    }

    /// [`fetch`](Self::fetch) for many objects at once, omitting those
    /// neither place has.
    ///
    /// # Errors
    /// Returns an error if a lookup or read fails.
    pub(crate) async fn fetch_many(
        &self,
        shas: &[ObjectId],
        state: &Storage,
        repo: &RepoMetadata,
    ) -> Result<ObjectHashMap<(Kind, Bytes)>, Error> {
        fetch_bases(shas, &self.buffer, &self.session, state, repo).await
    }

    /// Flush whatever is still in memory, and give up the index and the
    /// session promotion reads the chunks back through.
    ///
    /// # Errors
    /// Returns an error if the final staging write fails.
    pub(crate) async fn finish(
        mut self,
        repo: &RepoMetadata,
    ) -> Result<(ObjectHashMap<StagedEncodings>, Arc<StagingSession>), Error> {
        self.buffer.flush(&self.session, repo).await?;
        Ok((self.buffer.into_index(), self.session))
    }
}
