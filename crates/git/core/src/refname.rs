//! Whether a string is a refname git would accept.
//!
//! Validity, and only validity. Which refnames a repository *wants* is a
//! application's question and is asked in `pre-receive`; this is the part
//! nobody gets to decide, so it lives where both doors can reach it.

use bstr::ByteSlice as _;

/// Whether `refname` is one git's own `receive-pack` would refuse.
///
/// Git calls these funny refnames. A delete is held to less, because a ref
/// that should never have been created still has to be removable.
#[must_use]
pub fn is_funny_refname(refname: &str, is_delete: bool) -> bool {
    let Some(rest) = refname.strip_prefix("refs/") else {
        return true;
    };
    if !is_delete && !rest.contains('/') {
        return true;
    }
    gix_validate::reference::name(refname.as_bytes().as_bstr()).is_err()
}

#[cfg(test)]
mod tests {
    use super::is_funny_refname;

    // Verified against a real `git receive-pack` via hand-crafted pkt-line
    // requests — `git push` itself never constructs most of these.
    #[test]
    fn matches_real_git() {
        let cases: &[(&str, bool, bool)] = &[
            // (refname, is_delete, expected_funny)
            ("HEAD", false, true),
            ("FETCH_HEAD", false, true),
            ("foo", false, true),
            ("refs/foo", false, true), // one level under refs/, create
            ("refs/foo", true, false), // same, but delete: allowed
            ("refs/heads/../evil", false, true),
            ("refs/heads/ok-branch", false, false),
            ("refs/tags/v1.0", false, false),
        ];
        for &(refname, is_delete, expected_funny) in cases {
            assert_eq!(
                is_funny_refname(refname, is_delete),
                expected_funny,
                "refname={refname:?} is_delete={is_delete}"
            );
        }
    }

    /// A namespace nothing special is done with is still a valid refname.
    ///
    /// What a repository *admits* is an application's decision; this only says
    /// git would accept the name.
    #[test]
    fn any_valid_namespace_is_not_funny() {
        for refname in [
            "refs/notes/commits",
            "refs/merge-requests/42/head",
            "refs/pull/7/head",
        ] {
            assert!(!is_funny_refname(refname, false), "{refname}");
        }
    }
}
