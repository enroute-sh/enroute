use std::fmt;

/// What an application calls a repository, kept apart from [`crate::RepoId`]
/// so a name a caller chose cannot stand in for the engine's own.
///
/// Holds whatever it was given: what a key may contain is the product's
/// question, answered where a key is first accepted rather than here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ExternalKey(String);

impl ExternalKey {
    /// Takes `key` as it stands.
    ///
    /// Checks nothing: a key reaching the engine was accepted by whoever
    /// accepted it, and a row read back was accepted once already.
    #[must_use]
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// The key as a database column and a wire field spell it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExternalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<ExternalKey> for String {
    fn from(key: ExternalKey) -> Self {
        key.0
    }
}
