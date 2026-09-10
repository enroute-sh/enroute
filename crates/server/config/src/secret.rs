//! A configured value that nothing may print.
//!
//! The type exists for its [`Debug`], which is the whole of what it adds: a
//! configuration is a struct that derives `Debug` all the way down, and one
//! `tracing::debug!(?config)` would otherwise undo every care taken over where
//! these values come from. `ClickHouse` is the cautionary tale — its `from_env`
//! still leaks, because the resolved configuration is written to disk. Reading
//! the value takes [`Secret::expose`], which is a thing a reviewer can grep
//! for and a reader cannot do by accident.

use serde::{Deserialize, Deserializer};

/// A value the configuration names and nothing logs.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    /// The value itself.
    ///
    /// Named so the call sites are findable: there should be very few, and
    /// each one should be handing it straight to whatever needs it.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Secret {
    /// For a value that arrived from somewhere other than the file — a secret
    /// store's answer, say — which is not expanded and never was.
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl std::fmt::Debug for Secret {
    /// Not the value, and not its length either — a length is a fact about a
    /// key that a log has no reason to carry.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(redacted)")
    }
}

impl<'de> Deserialize<'de> for Secret {
    /// Expanded like any other string, so `${DATABASE_URL}` is how one
    /// arrives without the file holding it.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        crate::expand::string::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_does_not_print_itself() {
        let secret = Secret("hunter2".to_string());

        assert!(!format!("{secret:?}").contains("hunter2"));
        assert!(!format!("{secret:#?}").contains("hunter2"));
        assert_eq!(secret.expose(), "hunter2");
    }

    // A whole `Config` is checked in `file.rs`, which is where one can be
    // built from what a deployment would actually write.
}
