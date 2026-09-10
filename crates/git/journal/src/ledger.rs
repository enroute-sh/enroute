//! Where a journal's rows land.

use anyhow::Result;
use async_trait::async_trait;

use enroute_git_core::RepoId;
use enroute_git_metadata::RepoMetadata;
use enroute_lattice_store::CatalogRef;

use crate::{Index, Journal};

/// Whatever lands a [`Journal`], all of its rows or none of them.
///
/// Atomicity is the whole of what this seam is for: anything that could land
/// half a journal, or half a repository's deletion, is not one of these.
#[async_trait]
pub trait Ledger: core::fmt::Debug + Send + Sync {
    /// The list `index` is kept in, for the store that reads it.
    ///
    /// The same catalog this writes through, so a read and a write cannot
    /// disagree. Total: a ledger holds one per list.
    fn catalog(&self, index: Index) -> CatalogRef;

    /// Lands every row of `journal` against `repo`, or none of them.
    ///
    /// # Errors
    /// Whatever the store said. Nothing landed when this fails.
    async fn commit(&self, repo: RepoId, journal: &Journal) -> Result<()>;

    /// Lands it only if no other maintenance pass holds `repo`, and says
    /// whether it did.
    ///
    /// Refused rather than waited on: a gather told `false` has a copy to
    /// take back, and waiting would only make a second one.
    ///
    /// # Errors
    /// Whatever the store said. Nothing landed when this fails.
    async fn commit_held(&self, repo: &RepoMetadata, journal: &Journal) -> Result<bool>;

    /// Takes a repository apart: its rows, its four lists, and the row that
    /// names it.
    ///
    /// One write, because that is what makes a repository either whole or
    /// gone rather than half of one.
    ///
    /// # Errors
    /// Whatever the store said. Nothing was dropped when this fails.
    async fn erase(&self, repo: RepoId) -> Result<()>;
}
