//! A catalog in a map, for a store assembled with no database behind it.
//!
//! The same [`Scope`] a real one is keyed by, so it stands in for the catalog
//! a store above the substrate actually holds rather than for a simpler thing.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use async_trait::async_trait;

use enroute_lattice_core::KeyRange;

use crate::{Catalog, CatalogError, Entry, Listed, Scope, SegmentId, Written};

/// A catalog in a map.
///
/// Shared on clone, as `PostgresCatalog`'s pool is: a clone that forked the
/// rows would answer a question the real catalog never answers that way.
#[derive(Debug, Default, Clone)]
pub struct MemoryCatalog {
    /// A row is the [`Written`] it was recorded from, since a catalog stores
    /// exactly what it was handed.
    scopes: Arc<Mutex<BTreeMap<i64, Vec<Written>>>>,
}

impl MemoryCatalog {
    /// An empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The map, taking a poisoned lock as the state it was left in.
    fn locked(&self) -> MutexGuard<'_, BTreeMap<i64, Vec<Written>>> {
        self.scopes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What `pick` makes of every row of a scope, built under the lock.
    ///
    /// Under it rather than over a clone of the scope, since a read wants the
    /// few rows it asked for and not a copy of every body in the map.
    fn read<T>(&self, scope: i64, pick: impl FnMut(&Written) -> Option<T>) -> Vec<T> {
        let scopes = self.locked();
        scopes
            .get(&scope)
            .map(|rows| rows.iter().filter_map(pick).collect())
            .unwrap_or_default()
    }
}

/// What the catalog knows about a segment without reading its bytes.
fn entry(row: &Written) -> Entry {
    Entry {
        id: row.id,
        range: row.range,
        tier: row.tier,
        bytes: row.bytes,
        residence: row.body.residence(),
    }
}

/// The same, with the bytes or the key to read them by.
fn listed(row: &Written) -> Listed {
    Listed {
        entry: entry(row),
        body: row.body.clone(),
    }
}

#[async_trait]
impl Catalog for MemoryCatalog {
    async fn covering(&self, scope: &Scope, range: KeyRange) -> Result<Vec<Listed>, CatalogError> {
        Ok(self.read(scope.id, |row| {
            row.range.overlaps(range).then(|| listed(row))
        }))
    }

    async fn entries(&self, scope: &Scope) -> Result<Vec<Entry>, CatalogError> {
        Ok(self.read(scope.id, |row| Some(entry(row))))
    }

    async fn bodies(&self, scope: &Scope, ids: &[SegmentId]) -> Result<Vec<Listed>, CatalogError> {
        let wanted: HashSet<SegmentId> = ids.iter().copied().collect();
        Ok(self.read(scope.id, |row| {
            wanted.contains(&row.id).then(|| listed(row))
        }))
    }

    async fn insert(&self, scope: &Scope, segment: Written) -> Result<(), CatalogError> {
        self.locked().entry(scope.id).or_default().push(segment);
        Ok(())
    }

    async fn replace(
        &self,
        scope: &Scope,
        inputs: &[SegmentId],
        output: Written,
    ) -> Result<(), CatalogError> {
        let dropped: HashSet<SegmentId> = inputs.iter().copied().collect();
        let mut scopes = self.locked();
        let rows = scopes.entry(scope.id).or_default();
        rows.retain(|row| !dropped.contains(&row.id));
        rows.push(output);
        Ok(())
    }

    async fn purge(&self, scope: i64) -> Result<(), CatalogError> {
        self.locked().remove(&scope);
        Ok(())
    }
}
