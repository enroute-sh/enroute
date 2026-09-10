//! What an application calls a repository, and the name every call between an
//! application and Enroute spells.
//!
//! Chosen by the application rather than minted here, so an application
//! addresses a repository by the id it already has and keeps no table mapping
//! ours onto its own. Here beside [`TenantId`] because a key is unique within
//! one tenant and means nothing outside it, which is what makes one customer's
//! keys unreachable from another's — isolation by the shape of the namespace
//! rather than by a check over a shared one. Opaque: Enroute stores a key,
//! compares it byte for byte, and never parses it or derives anything from it.
//! The shape is narrow on purpose: comparison is byte-exact, so anything that
//! lets two keys look alike while differing makes two repositories a person
//! cannot tell apart — outside ASCII that is homoglyphs and normalization
//! forms, and inside it whitespace. What is left needs no escaping wherever a
//! key is written, and holds no `/`, so a key is one path segment. No `/`
//! because a key wants to be stable and the names that hold one — `owner/repo`
//! above all — are the names that move: a rename or a transfer would change
//! what a repository is called and so make it a different one. An application
//! keys by an id of its own instead and maps its URLs onto that id in
//! `authorize`, which is a lookup it already has.
//!
//! [`TenantId`]: super::TenantId

use std::fmt;
use std::str::FromStr;

/// The most an application may hand us.
///
/// Bounded because it is recorded on spans and stored on every repository, so
/// the length is ours to cap rather than a caller's to choose.
pub const MAX_REPO_KEY_BYTES: usize = 256;

/// What an application calls one of its repositories.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepoKey(String);

impl RepoKey {
    /// The key as the ledger and a span want it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The string does not meet the rules a key has.
///
/// One refusal and not one per rule: the rules are short, the caller picked the
/// string, and saying all of them is what lets it pick another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a repository key is 1 to {MAX_REPO_KEY_BYTES} bytes of ASCII letters, digits, '-', '_' \
     and '.', starting and ending with a letter or digit"
)]
pub struct BadRepoKey;

impl fmt::Display for RepoKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for RepoKey {
    type Err = BadRepoKey;

    fn from_str(key: &str) -> Result<Self, Self::Err> {
        // Alphanumeric at both ends, so no key is `.` or `..`, none carries a
        // separator it does not need, and none differs from another only by
        // punctuation nobody can see at the edge of a line.
        let ends_well = |c: Option<char>| c.is_some_and(|c| c.is_ascii_alphanumeric());
        let held = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
        let ok = !key.is_empty()
            && key.len() <= MAX_REPO_KEY_BYTES
            && key.chars().all(held)
            && ends_well(key.chars().next())
            && ends_well(key.chars().next_back());
        ok.then(|| Self(key.to_owned())).ok_or(BadRepoKey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_round_trips_unchanged() {
        let key: RepoKey = "repo-1a2b3c".parse().unwrap();
        assert_eq!(key.to_string(), "repo-1a2b3c");
    }

    #[test]
    fn the_shapes_an_application_already_has_are_taken() {
        // A UUID, a ULID, a row id, a slug and a dotted name: whatever the
        // application calls the repository, it should not have to invent
        // something new to say it.
        for key in [
            "3f1b9a4e-0000-4000-8000-000000000000",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "42",
            "acme-backend",
            "repo_1.2",
        ] {
            key.parse::<RepoKey>()
                .unwrap_or_else(|bad| panic!("{key:?} refused: {bad}"));
        }
    }

    #[test]
    fn case_is_kept_and_never_folded() {
        // Two keys, because folding them together would make one repository
        // out of two an application means to keep apart.
        let upper: RepoKey = "Repo".parse().unwrap();
        let lower: RepoKey = "repo".parse().unwrap();
        assert_ne!(upper, lower);
    }

    #[test]
    fn a_key_is_bounded_at_both_ends() {
        assert_eq!("".parse::<RepoKey>(), Err(BadRepoKey));
        "k".repeat(MAX_REPO_KEY_BYTES).parse::<RepoKey>().unwrap();
        let over = "k".repeat(MAX_REPO_KEY_BYTES + 1);
        assert_eq!(over.parse::<RepoKey>(), Err(BadRepoKey));
    }

    #[test]
    fn nothing_outside_ascii_is_taken() {
        // The whole reason for the set: these render as `acme` or close to it
        // while differing byte for byte, so admitting them would make two
        // repositories nobody can tell apart.
        for key in ["ünïcode", "\u{0430}cme", "acme\u{0301}", "🔑"] {
            assert_eq!(key.parse::<RepoKey>(), Err(BadRepoKey), "{key:?}");
        }
    }

    #[test]
    fn whitespace_and_what_would_need_escaping_are_refused() {
        // A key reaches a URL, a log line and a metrics label, and none of
        // them should have to quote it.
        for key in [
            "a b", " acme", "acme ", "a\tb", "a\nb", "a%2Fb", "a?b", "a#b", "a:b", "a&b", "a\\b",
            "a\"b",
        ] {
            assert_eq!(key.parse::<RepoKey>(), Err(BadRepoKey), "{key:?}");
        }
    }

    #[test]
    fn a_key_starts_and_ends_with_a_letter_or_digit() {
        // Which also settles `.` and `..`, neither of which is a key.
        for key in ["-acme", "acme-", ".acme", "acme.", "_a_", ".", ".."] {
            assert_eq!(key.parse::<RepoKey>(), Err(BadRepoKey), "{key:?}");
        }
    }

    #[test]
    fn a_key_is_one_path_segment() {
        // No `/`, so a key needs no encoding to sit in a URL path and no
        // `%2F` to survive a proxy — and the names that hold one are the
        // names a rename moves.
        for key in ["acme/backend", "a//b", "a/../b", "/acme"] {
            assert_eq!(key.parse::<RepoKey>(), Err(BadRepoKey), "{key:?}");
        }
    }

    #[test]
    fn a_refusal_states_the_rules() {
        // The application picked the key, so the one refusal has to be enough
        // to fix it without reading the proto.
        let said = BadRepoKey.to_string();
        assert!(said.contains("letters"), "{said}");
        assert!(said.contains(&MAX_REPO_KEY_BYTES.to_string()), "{said}");
    }
}
