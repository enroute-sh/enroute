use std::fmt;

/// The engine's identifier for a repository: the row key every table keyed by
/// a repository references.
///
/// Internal, and never on the wire: a sequence suits a foreign key and suits
/// nothing a caller holds. What names one outside is the contract's business.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RepoId(i64);

impl RepoId {
    /// Wrap a raw id, e.g. one just read back from `repositories.id`.
    #[must_use]
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    /// The underlying `i64`, e.g. to bind into a SQL query.
    #[must_use]
    pub fn as_i64(self) -> i64 {
        self.0
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl From<i64> for RepoId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

impl From<RepoId> for i64 {
    fn from(id: RepoId) -> Self {
        id.0
    }
}
