//! What the engine asks of whatever keeps its rows.
//!
//! A trait rather than a list of the implementations there are, so the one
//! that speaks to a database can live in the crate that owns the database and
//! this layer names neither it nor its driver.
//!
//! # Taking a repository
//! Every method takes one, since a store keeps rows for all of them.
//! [`RepoRows`] is the handle that carries one, and what callers use.
//!
//! [`RepoRows`]: crate::RepoRows

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{ObjectHashMap, RepoId, Ulid};

use crate::store::Identity;
use crate::{RefEntry, RefUpdate, RefUpdateResult, RefsMap, RepoMetadata, RepoSummary};

/// A store of the engine's rows, shared.
pub type MetadataRef = Arc<dyn Metadata>;

/// Everything the engine reads and writes as rows.
///
/// Nothing here takes a connection or a transaction. A write spanning two
/// stores belongs to the ledger, which is the only thing that can hold both.
#[async_trait]
pub trait Metadata: std::fmt::Debug + Send + Sync {
    /// Create a repository, with a counter per kind so it can take a push.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn create(&self, default_branch: Option<&str>) -> Result<RepoMetadata>;

    /// Every repository not deleted.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn all(&self) -> Result<Vec<RepoMetadata>>;

    /// Every repository deleted longer ago than `grace_secs`.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn deleted(&self, grace_secs: u64) -> Result<Vec<RepoMetadata>>;

    /// Make sure `repo` has a counter per kind.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn create_counters(&self, repo: RepoId) -> Result<()>;

    /// Take `count` seqs out of `kind`'s space, returning the first.
    ///
    /// # Errors
    /// Whatever the store said, or a repository with no counter.
    async fn allocate(&self, repo: RepoId, kind: Kind, count: u64) -> Result<i64>;

    /// Record what `named` are called.
    ///
    /// # Errors
    /// Whatever the store said, and [`Raced`] when another writer numbered
    /// one of these first — which the caller retries.
    ///
    /// [`Raced`]: crate::Raced
    async fn record(&self, repo: RepoId, named: &[(ObjectId, Identity)]) -> Result<()>;

    /// What every oid `repo` holds is, keyed by oid.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn identify(&self, repo: RepoId, oids: &[ObjectId]) -> Result<ObjectHashMap<Identity>>;

    /// The oid of every seq named in `kind`'s space, keyed by seq.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn oids_of(
        &self,
        repo: RepoId,
        kind: Kind,
        seqs: &[i64],
    ) -> Result<HashMap<i64, ObjectId>>;

    /// This repository, or `None` if it is absent or deleted.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn lookup(&self, repo: RepoId) -> Result<Option<RepoMetadata>>;

    /// Those of `repos` still present, oldest first, each with its last push.
    ///
    /// Fewer than asked for is an answer: a lister holds no lock on a delete.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn summarize(&self, repos: &[RepoId]) -> Result<Vec<RepoSummary>>;

    /// Mark it deleted, reporting whether this call is what did it.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn mark_deleted(&self, repo: RepoId) -> Result<bool>;

    /// Every ref it has, branches and everything else alike.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn ref_listing(&self, repo: RepoId) -> Result<Vec<RefEntry>>;

    /// Which of `ids` it still names as holding a pack image.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn referenced_segments(&self, repo: RepoId, ids: &[Ulid]) -> Result<HashSet<Ulid>>;

    /// Which of `ids` still hold an image nothing has copied elsewhere.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn live_segments(&self, repo: RepoId, ids: &[Ulid]) -> Result<HashSet<Ulid>>;

    /// Drop the rows of segments retired longer than `grace_secs` ago.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn drop_retired_segments(&self, repo: RepoId, grace_secs: u64) -> Result<u64>;

    /// Its refs, keyed by name, with `HEAD` synthesized.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn refs_for(&self, repo: &RepoMetadata) -> Result<RefsMap>;

    /// Just the refs named, for a caller that knows what it wants.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn refs_matching(&self, repo: RepoId, refnames: &[&str]) -> Result<RefsMap>;

    /// Apply `updates`, each guarded by its own compare-and-set.
    ///
    /// # Errors
    /// Whatever the store said.
    async fn update_refs(
        &self,
        repo: RepoId,
        updates: &[RefUpdate],
    ) -> Result<Vec<RefUpdateResult>>;
}
