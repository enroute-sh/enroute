//! The one ledger this repository ships: a journal in one transaction.

use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::{PgConnection, PgPool};

use enroute_git_core::RepoId;
use enroute_git_journal::{Index, Journal, Ledger};
use enroute_git_metadata::RepoMetadata;
use enroute_lattice_store::CatalogRef;

use crate::{PostgresCatalog, Table};

/// A journal landed in one Postgres transaction.
///
/// Its own catalogs rather than the ones the stores read through, since what
/// a row needs is the list's name and nothing a read has to be told.
#[derive(Debug, Clone)]
pub struct PostgresLedger {
    pool: PgPool,
    lists: [PostgresCatalog; 4],
}

impl PostgresLedger {
    /// A ledger over `pool`, with a catalog per list.
    ///
    /// One per list rather than a list to search, so naming a list this
    /// ledger does not hold is not a thing that exists.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        let lists = Index::ALL.map(|index| PostgresCatalog::new(pool.clone(), Table::new(index)));
        Self { pool, lists }
    }

    /// Lands `journal` in a transaction the caller already holds.
    ///
    /// For a write whose transaction is doing something else as well — the
    /// gather's, which took the lock that makes it the only one copying.
    ///
    /// # Errors
    /// Whatever the database said.
    pub async fn commit_in(
        &self,
        tx: &mut PgConnection,
        repo: RepoId,
        journal: &Journal,
    ) -> Result<()> {
        for listing in journal.listed() {
            self.list(listing.index)
                .insert_with(&mut *tx, &listing.scope, &listing.segment)
                .await
                .with_context(|| format!("listing a segment of the {:?} index", listing.index))?;
        }

        crate::repos::register_segments(&mut *tx, repo, journal.registered()).await?;
        crate::repos::retire_segments(&mut *tx, repo, journal.retired()).await?;
        Ok(())
    }

    /// Drops every row of `repo` in all four lists.
    async fn purge_lists(&self, tx: &mut PgConnection, repo: RepoId) -> Result<()> {
        for index in Index::ALL {
            self.list(index)
                .purge_with(&mut *tx, repo.as_i64())
                .await
                .with_context(|| format!("purging the {index:?} index"))?;
        }
        Ok(())
    }

    /// The catalog of one list.
    fn list(&self, index: Index) -> &PostgresCatalog {
        index.of(&self.lists)
    }
}

#[async_trait]
impl Ledger for PostgresLedger {
    fn catalog(&self, index: Index) -> CatalogRef {
        Arc::new(self.list(index).clone())
    }

    async fn commit(&self, repo: RepoId, journal: &Journal) -> Result<()> {
        if journal.is_empty() {
            return Ok(());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .context("beginning the index transaction")?;
        self.commit_in(&mut tx, repo, journal).await?;
        tx.commit()
            .await
            .context("committing the index transaction")?;
        Ok(())
    }

    /// The lock lives as long as the transaction, which is why it is taken
    /// here: two gathers at once would copy the same images twice.
    async fn commit_held(&self, repo: &RepoMetadata, journal: &Journal) -> Result<bool> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("beginning the maintenance transaction")?;
        if !crate::repos::hold_maintenance_lock(&mut tx, repo).await? {
            return Ok(false);
        }
        self.commit_in(&mut tx, repo.id, journal).await?;
        tx.commit()
            .await
            .context("committing the maintenance transaction")?;
        Ok(true)
    }

    async fn erase(&self, repo: RepoId) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("beginning the erase transaction")?;
        crate::repos::purge_rows(&mut tx, repo).await?;
        self.purge_lists(&mut tx, repo).await?;
        // Last, since everything else points at it.
        crate::repos::erase(&mut tx, repo).await?;
        tx.commit()
            .await
            .context("committing the erase transaction")?;
        Ok(())
    }
}
