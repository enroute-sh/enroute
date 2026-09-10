//! One repository's rows: what it is, where its refs point, and what every
//! object in it is called.
//!
//! One table for all four kinds, because a commit is an object like the rest
//! and was only ever counted elsewhere by accident. Every kind is counted in
//! a numbering of its own, so a seq means nothing without the kind beside it
//! — the same number is a commit, a tree, a blob and a tag, and four
//! different objects. Everything above resolves oids here at the edges of a
//! walk and then works in seqs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{ObjectHashMap, RepoId, Ulid};

use crate::memory::Memory;
use crate::metadata::MetadataRef;

/// What identity says about one object.
///
/// The seq means nothing without the kind, since every kind is counted on
/// its own: the pair is the number, and neither half is one alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Identity {
    /// Its number, in the space its kind is counted by.
    pub seq: i64,
    /// What kind of object it is.
    pub kind: Kind,
}

/// Another writer numbered one of these objects first.
///
/// Reported rather than left as whatever a driver calls a unique violation,
/// so a caller asks what happened instead of matching on a SQLSTATE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("another writer numbered one of these objects first")]
pub struct Raced;

/// Every repository's rows, over whatever keeps them.
///
/// The handle the index stores take, and the one the facade composes: the
/// same shape as `CommitGraph` and `Objects` above it.
#[derive(Debug, Clone)]
pub struct Rows {
    store: MetadataRef,
}

impl Rows {
    /// Rows in `store`.
    #[must_use]
    pub const fn new(store: MetadataRef) -> Self {
        Self { store }
    }

    /// Rows in a map, kept for as long as this handle and its clones are.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::over_memory(Arc::new(Memory::new()))
    }

    /// Rows in a map somebody else is holding too.
    ///
    /// What a ledger needs: it writes the rows a push registers, and a store
    /// it did not share would be a second set of them.
    #[must_use]
    pub fn over_memory(rows: Arc<Memory>) -> Self {
        Self { store: rows }
    }

    /// This repository's rows.
    #[must_use]
    pub const fn repo(&self, repo: RepoId) -> RepoRows<'_> {
        RepoRows { rows: self, repo }
    }

    /// Create a repository, with a counter per kind so it can take a push.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn create(&self, default_branch: Option<&str>) -> Result<crate::RepoMetadata> {
        self.store.create(default_branch).await
    }

    /// Every repository not deleted.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn all(&self) -> Result<Vec<crate::RepoMetadata>> {
        self.store.all().await
    }

    /// Every repository deleted longer ago than `grace_secs`.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn deleted(&self, grace_secs: u64) -> Result<Vec<crate::RepoMetadata>> {
        self.store.deleted(grace_secs).await
    }

    /// Those of `repos` still present, oldest first, each with its last push.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn summarize(&self, repos: &[RepoId]) -> Result<Vec<crate::RepoSummary>> {
        self.store.summarize(repos).await
    }
}

/// One repository's rows.
#[derive(Debug, Clone, Copy)]
pub struct RepoRows<'a> {
    rows: &'a Rows,
    repo: RepoId,
}

impl RepoRows<'_> {
    /// Makes sure the repository has a counter per kind.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn create_counters(&self) -> Result<()> {
        self.rows.store.create_counters(self.repo).await
    }

    /// Takes `count` seqs out of `kind`'s space, returning the first.
    ///
    /// # Errors
    /// Whatever the store said, or a repository with no counter.
    pub async fn allocate(&self, kind: Kind, count: u64) -> Result<i64> {
        self.rows.store.allocate(self.repo, kind, count).await
    }

    /// Records what `named` are called.
    ///
    /// # Errors
    /// Whatever the store said, and [`Raced`] when another writer numbered
    /// one of these first — which the caller retries.
    pub async fn record(&self, named: &[(ObjectId, Identity)]) -> Result<()> {
        self.rows.store.record(self.repo, named).await
    }

    /// What every oid the repository holds is, keyed by oid.
    ///
    /// Oids it does not hold are absent rather than an error.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn identify(&self, oids: &[ObjectId]) -> Result<ObjectHashMap<Identity>> {
        self.rows.store.identify(self.repo, oids).await
    }

    /// The seq of every oid that is a `kind`, keyed by oid.
    ///
    /// An oid of another kind is absent, since a seq in the wrong space is a
    /// different object rather than the same one.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn seqs_of(&self, kind: Kind, oids: &[ObjectId]) -> Result<HashMap<ObjectId, i64>> {
        Ok(self
            .identify(oids)
            .await?
            .into_iter()
            .filter(|(_, held)| held.kind == kind)
            .map(|(oid, held)| (oid, held.seq))
            .collect())
    }

    /// The oid of every seq named in `kind`'s space, keyed by seq.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn oids_of(&self, kind: Kind, seqs: &[i64]) -> Result<HashMap<i64, ObjectId>> {
        self.rows.store.oids_of(self.repo, kind, seqs).await
    }

    /// Which repository this is.
    #[must_use]
    pub const fn repo_id(&self) -> RepoId {
        self.repo
    }

    /// This repository, or `None` if it is absent or deleted.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn lookup(&self) -> Result<Option<crate::RepoMetadata>> {
        self.rows.store.lookup(self.repo).await
    }

    /// Mark it deleted, reporting whether this call is what did it.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn mark_deleted(&self) -> Result<bool> {
        self.rows.store.mark_deleted(self.repo).await
    }

    /// Every ref it has, branches and everything else alike.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn ref_listing(&self) -> Result<Vec<crate::RefEntry>> {
        self.rows.store.ref_listing(self.repo).await
    }

    /// Which of `ids` it still names as holding a pack image.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn referenced_segments(&self, ids: &[Ulid]) -> Result<HashSet<Ulid>> {
        self.rows.store.referenced_segments(self.repo, ids).await
    }

    /// Which of `ids` still hold an image nothing has copied elsewhere.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn live_segments(&self, ids: &[Ulid]) -> Result<HashSet<Ulid>> {
        self.rows.store.live_segments(self.repo, ids).await
    }

    /// Drop the rows of segments retired longer than `grace_secs` ago.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn drop_retired_segments(&self, grace_secs: u64) -> Result<u64> {
        self.rows
            .store
            .drop_retired_segments(self.repo, grace_secs)
            .await
    }

    /// Its refs, keyed by name, with `HEAD` synthesized.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn refs_for(&self, repo: &crate::RepoMetadata) -> Result<crate::RefsMap> {
        self.rows.store.refs_for(repo).await
    }

    /// Just the refs named, for a caller that knows what it wants.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn refs_matching(&self, refnames: &[&str]) -> Result<crate::RefsMap> {
        self.rows.store.refs_matching(self.repo, refnames).await
    }

    /// Apply `updates`, each guarded by its own compare-and-set.
    ///
    /// # Errors
    /// Whatever the store said.
    pub async fn update_refs(
        &self,
        updates: &[crate::RefUpdate],
    ) -> Result<Vec<crate::RefUpdateResult>> {
        self.rows.store.update_refs(self.repo, updates).await
    }
}
