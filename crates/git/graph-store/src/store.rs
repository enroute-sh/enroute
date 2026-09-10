//! The store itself: two segmented values over one shared numbering.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use gix_hash::ObjectId;
use gix_object::Kind;
use object_store::{ObjectStore, path::Path};

use enroute_git_core::RepoId;
use enroute_git_cost::{CountingStore, Meter, StoreRole};
use enroute_git_metadata::{Identity, RepoRows, Rows};
use enroute_lattice_core::{Key, KeyRange, Policy};
use enroute_lattice_store::{CatalogRef, Scope, Segments};

use crate::{CommitIndex, PackIndex};

/// The commit graph of every repository, as segments over a catalog.
///
/// Two segmented values over two tables, so a walk that only traverses never
/// reads the bitmaps it will not answer with.
#[derive(Debug, Clone)]
pub struct CommitGraph {
    pub(crate) ids: Rows,
    pub(crate) bucket: Arc<dyn ObjectStore>,
    pub(crate) graph: Arc<Segments<CommitIndex>>,
    pub(crate) packs: Arc<Segments<PackIndex>>,
}

impl CommitGraph {
    /// A store over two lists, graduating segments into `bucket` per `policy`.
    ///
    /// The lists are the caller's, so what they are kept in is a deployment's
    /// choice rather than something this crate decides by naming a driver.
    #[must_use]
    pub fn new(
        graph: CatalogRef,
        packs: CatalogRef,
        ids: Rows,
        bucket: Arc<dyn ObjectStore>,
        policy: Policy,
    ) -> Self {
        Self {
            ids,
            graph: Arc::new(Segments::new(graph, Arc::clone(&bucket), policy)),
            packs: Arc::new(Segments::new(packs, Arc::clone(&bucket), policy)),
            bucket,
        }
    }

    /// This same store, charging everything it reads or writes to `meter`.
    ///
    /// Its own rather than the caller's, so nothing above has to hold the
    /// undecorated bucket in order to wrap it.
    #[must_use]
    pub fn metered(&self, meter: Arc<Meter>) -> Self {
        self.with_bucket(CountingStore::wrap(
            Arc::clone(&self.bucket),
            meter,
            StoreRole::Primary,
        ))
    }

    /// This same store, reaching the bucket through `bucket` instead.
    ///
    /// What a walk spends in the bucket is charged where the rest of the
    /// operation is, so the meter has to reach the segments too.
    #[must_use]
    pub fn with_bucket(&self, bucket: Arc<dyn ObjectStore>) -> Self {
        Self {
            ids: self.ids.clone(),
            bucket: Arc::clone(&bucket),
            graph: Arc::new(self.graph.with_bucket(Arc::clone(&bucket))),
            packs: Arc::new(self.packs.with_bucket(bucket)),
        }
    }

    /// This repository's commit graph.
    #[must_use]
    pub fn repo(&self, repo: RepoId) -> RepoGraph<'_> {
        RepoGraph { store: self, repo }
    }

    /// The identity every kind is numbered by, which this shares.
    #[must_use]
    pub const fn ids(&self) -> &Rows {
        &self.ids
    }
}

/// One repository's commit graph.
#[derive(Debug, Clone, Copy)]
pub struct RepoGraph<'a> {
    pub(crate) store: &'a CommitGraph,
    pub(crate) repo: RepoId,
}

impl RepoGraph<'_> {
    /// Which repository this is.
    #[must_use]
    pub const fn repo_id(&self) -> RepoId {
        self.repo
    }

    /// Where this repository's tier-one segments live.
    ///
    /// Keyed by repository id, not storage key: an index segment is this
    /// store's own object rather than one of the repository's pack images.
    pub(crate) fn graph_scope(&self) -> Scope {
        self.scope("graph")
    }

    /// Where this repository's tier-two segments live.
    pub(crate) fn pack_scope(&self) -> Scope {
        self.scope("packs")
    }

    fn scope(&self, tier: &str) -> Scope {
        Scope {
            id: self.repo.as_i64(),
            prefix: Path::from(format!("commits/{}/{tier}", self.repo.as_i64())),
        }
    }

    /// The tier-one index covering `[floor, ceil]`, composed from whatever
    /// segments touch it.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub(crate) async fn index(&self, floor: i64, ceil: i64) -> Result<CommitIndex> {
        let Some(range) = span(floor, ceil) else {
            return Ok(CommitIndex::default());
        };
        Ok(self
            .store
            .graph
            .read(&self.graph_scope(), range)
            .await
            .context("reading the commit graph for a band")?
            .unwrap_or_default())
    }

    /// The tier-two facts for a scattered set of seqs.
    ///
    /// Read in clusters and kept as clusters: this index is a stride array,
    /// so one value over the hull would allocate the gaps between them.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub(crate) async fn packs_at(&self, seqs: &[i64]) -> Result<PackFacts> {
        let keys: Vec<Key> = seqs
            .iter()
            .filter_map(|seq| u64::try_from(*seq).ok())
            .map(Key::new)
            .collect();
        Ok(PackFacts(
            self.store
                .packs
                .read_at(&self.pack_scope(), &keys)
                .await
                .context("reading the commit packs for a scattered set")?,
        ))
    }

    /// The seq of every commit oid the repository *numbers*, keyed by oid.
    ///
    /// Numbering, not holding: a seq says what a commit is called, and only
    /// [`Self::contains`] says the graph has an entry for it.
    ///
    /// # Errors
    /// Whatever the database said.
    pub async fn seqs_of(&self, oids: &[ObjectId]) -> Result<HashMap<ObjectId, i64>> {
        self.ids().seqs_of(Kind::Commit, oids).await
    }

    /// The oid of every commit seq named, keyed by seq.
    ///
    /// # Errors
    /// Whatever the database said.
    pub async fn oids_of(&self, seqs: &[i64]) -> Result<HashMap<i64, ObjectId>> {
        self.ids().oids_of(Kind::Commit, seqs).await
    }

    /// Whether the graph has an entry for this commit.
    ///
    /// The index and not identity, so a commit numbered by a push that never
    /// finished answers `false` — which is what a reader can act on.
    ///
    /// # Errors
    /// Whatever the database, the catalog or the bucket said.
    pub async fn contains(&self, oid: ObjectId) -> Result<bool> {
        let Some(&seq) = self.seqs_of(&[oid]).await?.get(&oid) else {
            return Ok(false);
        };
        let named = [(
            oid,
            Identity {
                seq,
                kind: Kind::Commit,
            },
        )];
        Ok(self
            .locations_of(&named)
            .await?
            .into_iter()
            .any(|(_, meta)| meta.is_stored()))
    }

    /// This repository's identity, which every kind shares.
    pub(crate) fn ids(&self) -> RepoRows<'_> {
        self.store.ids.repo(self.repo)
    }
}

/// The key range a band covers, or `None` when there is no band.
pub(crate) fn span(floor: i64, ceil: i64) -> Option<KeyRange> {
    let first = u64::try_from(floor.max(0)).ok()?;
    let last = u64::try_from(ceil.max(0)).ok()?;
    KeyRange::new(Key::new(first), Key::new(last)).ok()
}

/// The pack facts for a scattered set of seqs, held as the clusters they
/// were read in.
///
/// Kept apart rather than joined: [`PackIndex`] is dense over its range, so
/// one value covering two distant clusters is mostly empty slots.
#[derive(Debug, Default)]
pub(crate) struct PackFacts(Vec<PackIndex>);

impl PackFacts {
    /// What the read knows about `seq`, from whichever cluster covers it.
    pub(crate) fn get(&self, seq: i64) -> Option<crate::PackEntry<'_>> {
        self.0.iter().find_map(|part| part.get(seq))
    }
}
