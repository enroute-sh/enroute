//! Reading, writing and compacting one value held as segments.

use std::collections::HashSet;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};

use enroute_lattice_core::{
    Key, KeyRange, Placement, Policy, Residence, Segment, Tier, cluster, compose, plan,
};

use crate::catalog::{Body, CatalogRef, Entry, Listed, SegmentId, Written};
use crate::error::Error;
use crate::scope::Scope;

/// Bucket segments a read fetches at once.
///
/// A composed band is bounded by the policy, not by this, so the only thing
/// raising it buys is hiding latency behind the slowest GET.
const DEFAULT_READS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(15);

/// Ranges a scattered read is allowed to become.
///
/// Each one is a round trip and a fetch of whatever it lists, which is what
/// [`cluster`] weighs a cut against.
const DEFAULT_CLUSTERS: NonZeroUsize = NonZeroUsize::MIN.saturating_add(7);

/// What one compaction pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// Segments consumed by a merge.
    pub merged: usize,
    /// Merged segments that were large enough to graduate to the bucket.
    pub graduated: usize,
    /// Bytes written out.
    pub written: u64,
    /// Superseded bucket objects a delete failed on, left for a janitor.
    pub orphaned: usize,
}

/// What dropping a scope's bucket objects freed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Reclaimed {
    /// Objects deleted.
    pub objects: u64,
    /// Their summed encoded sizes.
    pub bytes: u64,
    /// Deletes that failed, left for a janitor.
    pub orphaned: usize,
}

/// When a sweep runs, and how long an unnamed object is left alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sweep {
    /// "Now", in milliseconds since the epoch, against a segment's own ULID.
    pub now_ms: u64,
    /// How old an object no row names must be to count as an orphan.
    pub grace_secs: u64,
    /// Report what would go, and delete nothing.
    pub dry_run: bool,
}

/// What one sweep of a scope's bucket objects found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Swept {
    /// Objects under the scope's prefix that are named as a segment is.
    pub scanned: u64,
    /// Ones no row named, past the grace window.
    pub orphans: u64,
    /// What deleting them freed, or would have under a dry run.
    pub bytes: u64,
}

impl Swept {
    /// Two sweeps' worth, summed.
    #[must_use]
    pub const fn plus(self, other: Self) -> Self {
        Self {
            scanned: self.scanned.saturating_add(other.scanned),
            orphans: self.orphans.saturating_add(other.orphans),
            bytes: self.bytes.saturating_add(other.bytes),
        }
    }
}

/// One listed bucket object, before anything is known about whether a row
/// names it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Found {
    id: SegmentId,
    path: Path,
    bytes: u64,
}

impl Report {
    /// Two passes' worth, summed.
    #[must_use]
    pub const fn plus(self, other: Self) -> Self {
        Self {
            merged: self.merged.saturating_add(other.merged),
            graduated: self.graduated.saturating_add(other.graduated),
            written: self.written.saturating_add(other.written),
            orphaned: self.orphaned.saturating_add(other.orphaned),
        }
    }
}

impl Reclaimed {
    /// Two purges' worth, summed.
    #[must_use]
    pub const fn plus(self, other: Self) -> Self {
        Self {
            objects: self.objects.saturating_add(other.objects),
            bytes: self.bytes.saturating_add(other.bytes),
            orphaned: self.orphaned.saturating_add(other.orphaned),
        }
    }
}

/// One segmented value per scope, over a catalog and an object store.
///
/// The catalog says what segments there are and holds the small ones; the
/// object store holds the rest. Which is which follows [`Policy`].
#[derive(Debug)]
pub struct Segments<S> {
    catalog: CatalogRef,
    bucket: Arc<dyn ObjectStore>,
    policy: Policy,
    clusters: NonZeroUsize,
    value: PhantomData<fn() -> S>,
}

impl<S: Segment> Segments<S> {
    /// A store over `catalog` and `bucket`, compacting to `policy`.
    #[must_use]
    pub fn new(catalog: CatalogRef, bucket: Arc<dyn ObjectStore>, policy: Policy) -> Self {
        Self {
            catalog,
            bucket,
            policy,
            clusters: DEFAULT_CLUSTERS,
            value: PhantomData,
        }
    }

    /// This same store, reading and writing through `bucket` instead.
    ///
    /// For a decorated bucket — a counting one, say — since the catalog and
    /// the policy are settled by then and only the bucket differs.
    #[must_use]
    pub fn with_bucket(&self, bucket: Arc<dyn ObjectStore>) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
            bucket,
            policy: self.policy,
            clusters: self.clusters,
            value: PhantomData,
        }
    }

    /// The catalog underneath, for whatever else a caller keeps in it.
    #[must_use]
    pub const fn catalog(&self) -> &CatalogRef {
        &self.catalog
    }

    /// Delete every bucket object this scope's segments hold, and report
    /// what that reclaimed.
    ///
    /// The rows are not touched: dropping a scope is the catalog owner's, and
    /// it knows what else the same transaction has to take with it.
    ///
    /// # Errors
    /// [`Error`] when the catalog cannot be listed. A failed delete is not an
    /// error — it leaves an orphan, which is a janitor's.
    pub async fn purge_bucket(&self, scope: &Scope) -> Result<Reclaimed, Error> {
        let entries = self.catalog.entries(scope).await.map_err(Error::Catalog)?;
        let mut reclaimed = Reclaimed::default();
        for entry in entries
            .iter()
            .filter(|entry| entry.residence == Residence::Bucket)
        {
            if self.bucket.delete(&scope.key(entry.id)).await.is_err() {
                reclaimed.orphaned += 1;
                continue;
            }
            reclaimed.objects += 1;
            reclaimed.bytes = reclaimed.bytes.saturating_add(entry.bytes);
        }
        Ok(reclaimed)
    }

    /// Delete this scope's bucket objects that no catalog row names.
    ///
    /// An unnamed object is either an orphan or a write still in flight, and
    /// `grace_secs` is what tells the two apart.
    ///
    /// # Errors
    /// [`Error`] when the bucket cannot be listed or the catalog cannot be
    /// read. A failed delete is not one: it leaves the object for the next
    /// pass.
    pub async fn sweep_bucket(&self, scope: &Scope, sweep: Sweep) -> Result<Swept, Error> {
        // Listed first, and the rows read after: a segment recorded between
        // the two reads as named. The other order calls it an orphan.
        let found = self.list(scope).await?;
        let named: HashSet<SegmentId> = self
            .catalog
            .entries(scope)
            .await
            .map_err(Error::Catalog)?
            .iter()
            .map(|entry| entry.id)
            .collect();

        let mut swept = Swept {
            scanned: u64::try_from(found.len()).unwrap_or(u64::MAX),
            ..Swept::default()
        };
        for orphan in orphans_of(found, &named, sweep) {
            if !sweep.dry_run && self.bucket.delete(&orphan.path).await.is_err() {
                continue;
            }
            swept.orphans += 1;
            swept.bytes = swept.bytes.saturating_add(orphan.bytes);
        }
        Ok(swept)
    }

    /// The value composed from every segment touching `range`.
    ///
    /// # Errors
    /// [`Error`] when the catalog, the bucket or a segment's bytes fail.
    pub async fn read(&self, scope: &Scope, range: KeyRange) -> Result<Option<S>, Error> {
        let listed = self
            .catalog
            .covering(scope, range)
            .await
            .map_err(Error::Catalog)?;
        Ok(compose(self.decode_narrowed(listed, range).await?))
    }

    /// Every segment touching `keys`, one value per range they cluster into.
    ///
    /// Kept apart rather than composed, since whether joining two distant
    /// ranges is cheap is the segment type's business, not this one's.
    ///
    /// # Errors
    /// [`Error`] when the catalog, the bucket or a segment's bytes fail.
    pub async fn read_at(&self, scope: &Scope, keys: &[Key]) -> Result<Vec<S>, Error> {
        // Ranges in flight times fetches inside each one, so the bucket sees
        // up to `clusters * reads` at once. Bounded and small in practice:
        // `cluster` returns one range unless splitting pays for itself.
        let parts: Vec<Option<S>> = stream::iter(
            cluster(keys, self.clusters)
                .into_iter()
                .map(|range| async move { self.read(scope, range).await }),
        )
        .buffer_unordered(self.clusters.get())
        .try_collect()
        .await?;
        Ok(parts.into_iter().flatten().collect())
    }

    /// Records `value` as a new tier-zero segment, or nothing when it is empty.
    ///
    /// # Errors
    /// [`Error`] when the bucket or the catalog fails.
    pub async fn write(&self, scope: &Scope, value: &S) -> Result<Option<SegmentId>, Error> {
        let Some(written) = self.prepare(scope, value).await? else {
            return Ok(None);
        };
        let id = written.id;
        self.catalog
            .insert(scope, written)
            .await
            .map_err(Error::Catalog)?;
        Ok(Some(id))
    }

    /// Encodes `value` and puts it wherever its size says, without recording
    /// it — for a caller that will insert the row in its own transaction.
    ///
    /// The object lands here and the row lands there, in that order, so a
    /// rollback leaves an orphan and never a row naming a missing key.
    ///
    /// # Errors
    /// [`Error`] when the bucket fails.
    pub async fn prepare(&self, scope: &Scope, value: &S) -> Result<Option<Written>, Error> {
        let Some(range) = value.range() else {
            return Ok(None);
        };
        Ok(Some(self.put(scope, range, Tier::ZERO, value).await?))
    }

    /// Runs one compaction pass over the scope.
    ///
    /// Safe to stop part way and safe to run again: every merge is a join,
    /// so a pass that half ran has merged some segments and no more.
    ///
    /// # Errors
    /// [`Error`] when the catalog, the bucket or a segment's bytes fail.
    pub async fn compact(&self, scope: &Scope) -> Result<Report, Error> {
        let entries = self.catalog.entries(scope).await.map_err(Error::Catalog)?;
        let placements: Vec<Placement> = entries.iter().map(Entry::placement).collect();

        let mut report = Report::default();
        for merge in plan(&placements, &self.policy) {
            let ids: Vec<SegmentId> = merge
                .inputs
                .iter()
                .filter_map(|index| entries.get(*index))
                .map(|entry| entry.id)
                .collect();
            if ids.len() < 2 {
                continue;
            }

            let listed = self
                .catalog
                .bodies(scope, &ids)
                .await
                .map_err(Error::Catalog)?;
            let superseded = bucket_keys(&listed);

            let Some(value) = compose(self.decode(listed).await?) else {
                continue;
            };
            let range = value.range().unwrap_or(merge.range);
            let written = self.put(scope, range, merge.tier, &value).await?;

            report.written = report.written.saturating_add(written.bytes);
            if written.body.residence() == Residence::Bucket {
                report.graduated += 1;
            }
            self.catalog
                .replace(scope, &ids, written)
                .await
                .map_err(Error::Catalog)?;
            report.merged += ids.len();

            // Only now, since until the replace commits the rows still name
            // these. A delete that fails leaves an object no row names, which
            // is what a janitor's anti-join already reclaims.
            report.orphaned += self.discard(&superseded).await;
        }
        Ok(report)
    }

    /// Encodes `value`, puts it wherever its size says, and names it.
    async fn put(
        &self,
        scope: &Scope,
        range: KeyRange,
        tier: Tier,
        value: &S,
    ) -> Result<Written, Error> {
        let id = SegmentId::fresh();
        let encoded = Bytes::from(value.encoded());
        let bytes = u64::try_from(encoded.len()).unwrap_or(u64::MAX);

        let body = match self.policy.residence_for(bytes) {
            Residence::Inline => Body::Inline(encoded),
            Residence::Bucket => {
                let path = scope.key(id);
                self.bucket
                    .put(&path, PutPayload::from(encoded))
                    .await
                    .map_err(|source| Error::Bucket { id, source })?;
                Body::Bucket(path)
            }
        };

        Ok(Written {
            id,
            range,
            tier,
            body,
            bytes,
        })
    }

    /// Decodes every listed segment whole, fetching the bucket ones
    /// concurrently.
    ///
    /// What a merge reads, since its output replaces its inputs and must
    /// hold every key they did.
    async fn decode(&self, listed: Vec<Listed>) -> Result<Vec<S>, Error> {
        self.decode_narrowed(listed, KeyRange::EVERYTHING).await
    }

    /// The same, keeping only what `range` covers.
    ///
    /// `range` is what the caller asked for, not what the segments hold: a
    /// compacted one spans everything merged into it.
    async fn decode_narrowed(&self, listed: Vec<Listed>, range: KeyRange) -> Result<Vec<S>, Error> {
        stream::iter(listed.into_iter().map(|one| self.decode_one(one, range)))
            .buffer_unordered(DEFAULT_READS.get())
            .try_collect()
            .await
    }

    /// Decodes one segment, reading it from the bucket if it is not inlined.
    async fn decode_one(&self, listed: Listed, range: KeyRange) -> Result<S, Error> {
        let id = listed.entry.id;
        let bytes = match listed.body {
            Body::Inline(bytes) => bytes,
            Body::Bucket(path) => self.fetch(id, &path).await?,
        };
        S::decode_range(&bytes, range).map_err(|source| Error::Corrupt {
            id,
            source: Box::new(source),
        })
    }

    /// Reads one bucket segment whole.
    async fn fetch(&self, id: SegmentId, path: &Path) -> Result<Bytes, Error> {
        let read = self.bucket.get(path).await;
        let bytes = match read {
            Ok(result) => result.bytes().await,
            Err(source) => Err(source),
        };
        bytes.map_err(|source| Error::Bucket { id, source })
    }

    /// Every object under the scope's prefix that is named as a segment is.
    ///
    /// One that is not is somebody else's, and this store deletes nothing it
    /// cannot recognise.
    async fn list(&self, scope: &Scope) -> Result<Vec<Found>, Error> {
        let prefix = &scope.prefix;
        let mut listing = self.bucket.list(Some(prefix));
        let mut found = Vec::new();
        while let Some(object) = listing.next().await {
            let object = object.map_err(Error::Listing)?;
            let Some(id) = object.location.filename().and_then(SegmentId::parse) else {
                continue;
            };
            found.push(Found {
                id,
                path: object.location,
                bytes: object.size,
            });
        }
        Ok(found)
    }

    /// Deletes superseded objects, returning how many outlived the attempt.
    async fn discard(&self, paths: &[Path]) -> usize {
        let mut orphaned = 0;
        for path in paths {
            if self.bucket.delete(path).await.is_err() {
                orphaned += 1;
            }
        }
        orphaned
    }
}

/// Which of `found` no row names and whose ULID is older than the window.
///
/// Apart from the deletes, since what makes an object an orphan is one
/// decision and carrying it out is a round trip each.
fn orphans_of(found: Vec<Found>, named: &HashSet<SegmentId>, sweep: Sweep) -> Vec<Found> {
    let window = sweep.grace_secs.saturating_mul(1000);
    found
        .into_iter()
        .filter(|one| !named.contains(&one.id))
        .filter(|one| sweep.now_ms.saturating_sub(one.id.made_at_ms()) > window)
        .collect()
}

/// The keys of whichever listed segments live in the bucket.
fn bucket_keys(listed: &[Listed]) -> Vec<Path> {
    listed
        .iter()
        .filter_map(|one| match &one.body {
            Body::Bucket(path) => Some(path.clone()),
            Body::Inline(_) => None,
        })
        .collect()
}
