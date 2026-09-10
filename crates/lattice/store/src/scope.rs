//! What one segmented value belongs to.
//!
//! Here rather than beside the one catalog that keeps its rows in a database,
//! so a catalog keeping them anywhere else is scoped the same way and can
//! stand in for it.

use object_store::path::Path;

use crate::catalog::SegmentId;

/// What one segmented value belongs to.
///
/// Both halves are the caller's, since only it knows what a scope means: the
/// number the rows are keyed by, and where its bucket segments go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// The number the rows are keyed by — a repository id, typically.
    pub id: i64,
    /// Where this scope's bucket segments are put.
    pub prefix: Path,
}

impl Scope {
    /// Where a bucket segment of this scope is put.
    ///
    /// Here rather than in the catalog, so a sweep listing the prefix and a
    /// write putting under it cannot disagree about where a segment is.
    #[must_use]
    pub fn key(&self, id: SegmentId) -> Path {
        self.prefix.clone().join(id.to_string().as_str())
    }
}
