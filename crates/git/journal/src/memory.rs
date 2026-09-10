//! A journal landed in memory, for a stack with no database to land it in.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;

use enroute_git_core::RepoId;
use enroute_git_metadata::{Memory, RepoMetadata};
use enroute_lattice_store::{CatalogRef, MemoryCatalog};

use crate::{Index, Journal, Ledger};

/// A journal landed against catalogs and rows the caller already holds.
///
/// One catalog per list rather than a list to search, and rows shared with
/// the [`Rows`] reading them: a store of its own would be a second set.
///
/// [`Rows`]: enroute_git_metadata::Rows
#[derive(Debug)]
pub struct MemoryLedger {
    rows: Arc<Memory>,
    lists: [CatalogRef; 4],
}

impl MemoryLedger {
    /// A ledger over `rows` and the catalogs the stores read through, in the
    /// order [`Index::ALL`] names them.
    #[must_use]
    pub fn new(rows: Arc<Memory>, lists: [CatalogRef; 4]) -> Self {
        Self { rows, lists }
    }

    /// A ledger over `rows`, with a catalog per list of its own.
    ///
    /// What `PostgresLedger::new` does with a pool, so a stack with no
    /// database is assembled the same way as one with a database.
    #[must_use]
    pub fn in_memory(rows: Arc<Memory>) -> Self {
        Self::new(
            rows,
            Index::ALL.map(|_| -> CatalogRef { Arc::new(MemoryCatalog::new()) }),
        )
    }
}

#[async_trait]
impl Ledger for MemoryLedger {
    fn catalog(&self, index: Index) -> CatalogRef {
        Arc::clone(index.of(&self.lists))
    }

    async fn commit(&self, repo: RepoId, journal: &Journal) -> Result<()> {
        if journal.is_empty() {
            return Ok(());
        }
        for listing in journal.listed() {
            listing
                .index
                .of(&self.lists)
                .insert(&listing.scope, listing.segment.clone())
                .await
                .map_err(anyhow::Error::from_boxed)
                .with_context(|| format!("listing a segment of the {:?} index", listing.index))?;
        }
        self.rows.register_segments(repo, journal.registered());
        self.rows.retire_segments(repo, journal.retired());
        Ok(())
    }

    /// A flag rather than an advisory lock, held for as long as the write is
    /// and released whether or not the write succeeded.
    async fn commit_held(&self, repo: &RepoMetadata, journal: &Journal) -> Result<bool> {
        if !self.rows.hold_maintenance(repo.id) {
            return Ok(false);
        }
        let landed = self.commit(repo.id, journal).await;
        // Before the error is raised, since a pass that failed still stops
        // being the one holding this.
        self.rows.release_maintenance(repo.id);
        landed?;
        Ok(true)
    }

    async fn erase(&self, repo: RepoId) -> Result<()> {
        for index in Index::ALL {
            index
                .of(&self.lists)
                .purge(repo.as_i64())
                .await
                .map_err(anyhow::Error::from_boxed)
                .with_context(|| format!("purging the {index:?} index"))?;
        }
        // Last, since everything else is keyed by it.
        self.rows.erase(repo);
        Ok(())
    }
}
