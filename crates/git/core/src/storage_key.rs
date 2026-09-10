use std::fmt;

use uuid::Uuid;

/// Opaque routing key for a repository's storage layout, kept separate from
/// [`crate::RepoId`] so storage layout is independent of the id scheme.
///
/// Wraps a [`Uuid`] purely for type safety, so call sites can't accidentally
/// pass the wrong identifier where a storage key is expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StorageKey(Uuid);

impl StorageKey {
    /// Generate a new random (v4) storage key.
    #[must_use]
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }
}

impl fmt::Display for StorageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<Uuid> for StorageKey {
    fn from(key: Uuid) -> Self {
        Self(key)
    }
}

impl From<StorageKey> for Uuid {
    fn from(key: StorageKey) -> Self {
        key.0
    }
}
