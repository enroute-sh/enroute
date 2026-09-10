//! Pure in-memory algorithms over the commit/tree object graph.
//!
//! No storage dependencies: what a caller hands in is bytes, and what comes
//! back is what those bytes say.

mod ancestry;
mod diff;
mod refs;

pub use ancestry::{Ancestry, Boundaries, Push, Query};
pub use diff::{Change, DiffOptions, Removal, TreeDiff, TreeReader, diff_trees, diff_trees_with};
pub use refs::{CommitDetails, Identity, ObjectRefs, TreeChild, commit_details, object_refs};
