//! The store: two segmented values, one identity table, one counter.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use gix_hash::ObjectId;
use gix_object::Kind;
use object_store::{ObjectStore, path::Path};

use enroute_git_core::{ObjectSeq, RepoId};
use enroute_git_cost::{CountingStore, Meter, StoreRole};
use enroute_git_metadata::{Identity, RepoRows, Rows};
use enroute_lattice_core::{Key, Policy, compose};
use enroute_lattice_store::{CatalogRef, Scope, Segments};

use crate::index::ObjectIndex;

/// Which of the two catalogs a kind is held in, and counted in.
///
/// Tags are in neither: they are stored loose rather than packed, so there
/// is nothing about one a segment would hold and nothing to key it by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Shelf {
    /// Trees, whose records also carry their entries.
    Trees,
    /// Blobs, whose records are locations and nothing else.
    Blobs,
}

impl Shelf {
    /// Both shelves, for a reader that has to visit each in turn.
    pub(crate) const BOTH: [Self; 2] = [Self::Trees, Self::Blobs];

    /// Where `kind` is held, or `None` for a kind that is never packed.
    pub(crate) const fn for_kind(kind: Kind) -> Option<Self> {
        match kind {
            Kind::Tree => Some(Self::Trees),
            Kind::Blob => Some(Self::Blobs),
            Kind::Commit | Kind::Tag => None,
        }
    }

    /// What this shelf holds.
    #[must_use]
    pub const fn kind(self) -> Kind {
        match self {
            Self::Trees => Kind::Tree,
            Self::Blobs => Kind::Blob,
        }
    }
}

/// The object index of every repository, as segments over two catalogs.
///
/// Trees and blobs are held apart so a tree walk never reads a blob record,
/// which is most of them.
#[derive(Debug, Clone)]
pub struct Objects {
    pub(crate) ids: Rows,
    pub(crate) bucket: Arc<dyn ObjectStore>,
    pub(crate) trees: Arc<Segments<ObjectIndex>>,
    pub(crate) blobs: Arc<Segments<ObjectIndex>>,
}

impl Objects {
    /// A store over two shelves, graduating segments into `bucket` per
    /// `policy`.
    ///
    /// The lists are the caller's, so what they are kept in is a deployment's
    /// choice rather than something this crate decides by naming a driver.
    #[must_use]
    pub fn new(
        trees: CatalogRef,
        blobs: CatalogRef,
        ids: Rows,
        bucket: Arc<dyn ObjectStore>,
        policy: Policy,
    ) -> Self {
        Self {
            ids,
            trees: Arc::new(Segments::new(trees, Arc::clone(&bucket), policy)),
            blobs: Arc::new(Segments::new(blobs, Arc::clone(&bucket), policy)),
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
    /// What a read spends in the bucket is charged where the rest of the
    /// operation is, so the meter has to reach the segments too.
    #[must_use]
    pub fn with_bucket(&self, bucket: Arc<dyn ObjectStore>) -> Self {
        Self {
            ids: self.ids.clone(),
            bucket: Arc::clone(&bucket),
            trees: Arc::new(self.trees.with_bucket(Arc::clone(&bucket))),
            blobs: Arc::new(self.blobs.with_bucket(bucket)),
        }
    }

    /// This repository's object index.
    #[must_use]
    pub fn repo(&self, repo: RepoId) -> RepoObjects<'_> {
        RepoObjects { store: self, repo }
    }

    /// The identity every kind is numbered by, which this shares.
    #[must_use]
    pub const fn ids(&self) -> &Rows {
        &self.ids
    }
}

/// One repository's object index.
#[derive(Debug, Clone, Copy)]
pub struct RepoObjects<'a> {
    pub(crate) store: &'a Objects,
    pub(crate) repo: RepoId,
}

/// An object this store holds, in the width its bitmaps are in.
///
/// [`Identity`] carries the seq as the column holds it, and a roaring bitmap
/// counts in `u64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Held {
    /// Its number, in the space its kind is counted by.
    pub seq: u64,
    /// What kind of object it is.
    pub kind: Kind,
}

impl Held {
    /// What identity said, in this store's width.
    ///
    /// # Errors
    /// A seq below zero, which no counter here hands out.
    pub fn of(identity: Identity) -> Result<Self> {
        Ok(Self {
            seq: u64::try_from(identity.seq).context("an object seq below zero")?,
            kind: identity.kind,
        })
    }

    /// Which shelf holds this object's record, if any does.
    #[must_use]
    pub const fn shelf(&self) -> Option<Shelf> {
        Shelf::for_kind(self.kind)
    }

    /// The seq with the space it belongs to, or `None` for a commit.
    #[must_use]
    pub const fn numbered(&self) -> Option<ObjectSeq> {
        ObjectSeq::of(self.kind, self.seq)
    }
}

impl RepoObjects<'_> {
    /// Which repository this is.
    #[must_use]
    pub const fn repo_id(&self) -> RepoId {
        self.repo
    }

    /// The segments of one shelf.
    pub(crate) fn shelf(&self, held: Shelf) -> &Segments<ObjectIndex> {
        match held {
            Shelf::Trees => &self.store.trees,
            Shelf::Blobs => &self.store.blobs,
        }
    }

    /// Where a shelf's segments live for this repository.
    pub(crate) fn scope(&self, held: Shelf) -> Scope {
        let name = match held {
            Shelf::Trees => "trees",
            Shelf::Blobs => "blobs",
        };
        Scope {
            id: self.repo.as_i64(),
            prefix: Path::from(format!("objects/{}/{name}", self.repo.as_i64())),
        }
    }

    /// The index of one shelf covering `seqs`, composed from what touches it.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub(crate) async fn read(&self, held: Shelf, seqs: &[u64]) -> Result<ObjectIndex> {
        // Composed back into one value, unlike the commit tiers: this index
        // is sorted pairs, so a join costs the entries rather than the span
        // between them.
        let keys: Vec<Key> = seqs.iter().copied().map(Key::new).collect();
        let parts = self
            .shelf(held)
            .read_at(&self.scope(held), &keys)
            .await
            .context("reading the object index")?;
        Ok(compose(parts).unwrap_or_default())
    }

    /// What the repository holds each of `oids` as, keyed by oid.
    ///
    /// Absent rather than an error for an oid it does not hold, and for a
    /// commit, which the graph answers for.
    ///
    /// # Errors
    /// Whatever the database said.
    pub async fn identify(&self, oids: &[ObjectId]) -> Result<HashMap<ObjectId, Held>> {
        self.ids()
            .identify(oids)
            .await?
            .into_iter()
            .filter(|(_, identity)| identity.kind != Kind::Commit)
            .map(|(oid, identity)| Ok((oid, Held::of(identity)?)))
            .collect()
    }

    /// The oid of every seq named on one shelf, keyed by seq.
    ///
    /// # Errors
    /// Whatever the database said.
    pub async fn oids_of(&self, held: Shelf, seqs: &[u64]) -> Result<HashMap<u64, ObjectId>> {
        let wanted = as_i64(seqs)?;
        self.ids()
            .oids_of(held.kind(), &wanted)
            .await?
            .into_iter()
            .map(|(seq, oid)| Ok((u64::try_from(seq).context("an object seq below zero")?, oid)))
            .collect()
    }

    /// This repository's identity, which every kind shares.
    pub(crate) fn ids(&self) -> RepoRows<'_> {
        self.store.ids.repo(self.repo)
    }
}

/// Seqs as the `bigint[]` a query binds.
pub(crate) fn as_i64(seqs: &[u64]) -> Result<Vec<i64>> {
    seqs.iter()
        .map(|seq| i64::try_from(*seq).context("an object seq past a bigint"))
        .collect()
}
