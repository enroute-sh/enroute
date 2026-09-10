//! What a repository is, in rows: which exist, where their refs point, and
//! what every object in them is called.
//!
//! Rows and nothing else — no segment, no bucket, no key range, and no
//! driver. [`Metadata`] is what the engine asks of whatever keeps them, and
//! this is the floor the index stores sit on: they resolve an oid here, work
//! in seqs, and come back here to answer. [`Memory`] keeps them in a map; the
//! store that keeps them in a database is `enroute-postgres`'s.

#[cfg(feature = "conformance")]
pub mod conformance;
mod memory;
mod metadata;
pub mod refs;
mod store;

pub use memory::Memory;
pub use metadata::{Metadata, MetadataRef};
pub use store::{Identity, Raced, RepoRows, Rows};

use std::collections::BTreeMap;
use std::fmt;

use anyhow::Result;
use gix_hash::ObjectId;

use enroute_git_core::{RepoId, StorageKey};

/// All refs for a repository, keyed by ref name.
///
/// Values are SHA1 hex strings or `"ref: <name>"` for symbolic refs — in
/// practice only `HEAD`, synthesized by [`MetadataStore::get_refs_for`].
pub type RefsMap = BTreeMap<String, String>;

/// A single ref update from a `receive-pack` request: move `refname` from
/// `old_id` to `new_id`.
///
/// `old_id = ObjectId::null(..)` means unguarded create/overwrite;
/// `new_id = ObjectId::null(..)` means delete.
#[derive(Debug, Clone)]
pub struct RefUpdate {
    /// The ref being updated, e.g. `refs/heads/main`.
    pub refname: String,
    /// The value `refname` must currently have, or `ObjectId::null(..)` if
    /// no current value is required.
    pub old_id: ObjectId,
    /// The value to set `refname` to, or `ObjectId::null(..)` to delete it.
    pub new_id: ObjectId,
}

/// Why one [`RefUpdate`] failed [`MetadataStore::update_refs`]'s CAS check —
/// the closed set of outcomes the ref store itself can produce.
///
/// [`MetadataStore::update_refs`]: crate::MetadataStore::update_refs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefUpdateRejection {
    /// `old_id` didn't match the ref's current value.
    NonFastForward,
    /// `new_id` names an object this repository does not hold.
    ///
    /// A branch must point at a recorded commit. Any other ref is checked
    /// only by [`move_refs`], since a push proves the same thing by walking.
    UnknownCommit,
    /// An unguarded create (`old_id` null) found the ref already existed.
    AlreadyExists,
    /// `refname` is not one git itself would accept.
    InvalidRefname,
}

impl fmt::Display for RefUpdateRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NonFastForward => "non-fast-forward",
            Self::UnknownCommit => "unknown commit",
            Self::AlreadyExists => "already exists",
            Self::InvalidRefname => "invalid refname",
        })
    }
}

/// Outcome of applying one [`RefUpdate`]: landed, or rejected with a reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefUpdateResult {
    /// The ref that was updated or rejected.
    pub refname: String,
    /// `Ok` if the update landed, else the [`RefUpdateRejection`] it failed with.
    pub result: Result<(), RefUpdateRejection>,
}

/// Whether a [`RefsMap`] value is a direct ref (40-char SHA1 hex) rather than
/// symbolic (`"ref: <name>"`).
///
/// Checks hex digits, not just length: a 35-char symbolic target is also 40
/// chars overall (`"ref: "` is 5), so length alone would misclassify it.
#[must_use]
pub fn is_direct_ref(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Whether `refname` is a branch (`refs/heads/*`).
///
/// The single source of truth for the prefix check, for callers inside and
/// outside this crate — see `branches` in `migrations/0001_engine_rows.sql`.
#[must_use]
pub fn is_branch_refname(refname: &str) -> bool {
    refname.starts_with("refs/heads/")
}

/// A resolved repository, threaded through the push/fetch paths instead of a
/// bare [`RepoId`].
///
/// Callers need `id` for `MetadataStore` calls and `storage_key` for
/// `enroute-git-store` keys, often both in one request.
#[derive(Debug, Clone)]
pub struct RepoMetadata {
    /// The repository's stable internal surrogate key, small and monotonic.
    pub id: RepoId,
    /// A random key `enroute-git-store` builds object keys from, so storage layout
    /// is independent of the id scheme.
    pub storage_key: StorageKey,
    /// What `HEAD` currently resolves to, e.g. `refs/heads/main`.
    pub default_branch: String,
}

/// A repository as a listing reports it.
///
/// A pair, because reading the two apart is a round trip per repository.
#[derive(Debug, Clone)]
pub struct RepoSummary {
    /// The repository itself.
    pub repo: RepoMetadata,
    /// When its default branch last moved; `None` until a first push.
    pub last_push_unix_seconds: Option<i64>,
}

/// One ref as the wire contract reports it: where it points, and when it last
/// moved.
///
/// An absolute timestamp, unlike [`RefSummary`]'s age — a caller across the
/// contract renders its own "3 days ago" against its own clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefEntry {
    /// The full refname, e.g. `refs/heads/main`.
    pub refname: String,
    /// What it points at.
    pub oid: ObjectId,
    /// When it last moved, in seconds since the epoch.
    pub updated_unix_seconds: i64,
}
