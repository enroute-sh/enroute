//! What one ref update answers with.
//!
//! Here rather than beside either store, because both build the same two
//! results and a caller compares against them.

use crate::{RefUpdateRejection, RefUpdateResult};

/// One ref that landed.
#[must_use]
pub fn ok_result(refname: &str) -> RefUpdateResult {
    RefUpdateResult {
        refname: refname.to_string(),
        result: Ok(()),
    }
}

/// One ref that did not, and why.
#[must_use]
pub fn reject_result(refname: &str, rejection: RefUpdateRejection) -> RefUpdateResult {
    RefUpdateResult {
        refname: refname.to_string(),
        result: Err(rejection),
    }
}
