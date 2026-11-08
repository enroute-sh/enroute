//! Turning what the contract carries into what the engine holds, and back.
//!
//! Here rather than in each handler because `common.v1alpha1` states one rule
//! about an absent object id, and both the contract and the hooks keep it.
//!
//! # Why not `enroute-api`
//!
//! It holds the generated contract and no hand-written logic, so a conversion
//! reachable only from Rust would be contract no other language gets.

use std::time::{SystemTime, UNIX_EPOCH};

use tonic::Status;

use enroute_api::common::v1alpha1::{ObjectId as WireObjectId, RepoKey as WireRepoKey};

use crate::git::oid::Oid;

use super::repo_key::RepoKey;

/// Object ids cross the contract as hex: 40 digits in a SHA-1 repository,
/// 64 in a SHA-256 one. An id of the other format names nothing there, and
/// the ledger refuses it as a ref's value.
///
/// A malformed id is the caller's own string, not a repository fault, so it
/// comes back as `InvalidArgument`.
pub(crate) fn parse_oid(id: &WireObjectId) -> Result<Oid, Status> {
    parse_hex(&id.hex, "an object id")
}

/// An object id spelled as a bare string, which the page token is.
pub(crate) fn parse_hex(hex: &str, what: &str) -> Result<Oid, Status> {
    // Exact, because `Oid::from_hex` forgives surrounding whitespace.
    Oid::from_hex_exact(hex.as_bytes())
        .ok_or_else(|| Status::invalid_argument(format!("not {what}: {hex}")))
}

/// An object id the call cannot proceed without.
///
/// Unset is the caller's own omission, so it reads as `InvalidArgument`
/// beside a malformed one rather than as a repository fault.
pub(crate) fn require_oid(id: Option<&WireObjectId>, field: &str) -> Result<Oid, Status> {
    let id = id.ok_or_else(|| Status::invalid_argument(format!("{field} is required")))?;
    parse_oid(id)
}

/// An object id whose absence means "no object", as does git's all-zero id.
pub(crate) fn optional_oid(id: Option<&WireObjectId>) -> Result<Option<Oid>, Status> {
    Ok(id.map(parse_oid).transpose()?.filter(|oid| !oid.is_zero()))
}

/// An object id on its way out.
pub(crate) fn wire_oid(oid: Oid) -> WireObjectId {
    WireObjectId { hex: oid.to_hex() }
}

/// An object id on its way out, where the all-zero id means "no object".
pub(crate) fn wire_oid_or_unset(oid: Oid) -> Option<WireObjectId> {
    (!oid.is_zero()).then(|| wire_oid(oid))
}

/// A repository key on its way out.
pub(crate) fn wire_repo(key: &RepoKey) -> WireRepoKey {
    WireRepoKey {
        key: key.to_string(),
    }
}

/// A repository key on its way in.
///
/// A key the rules refuse reads as `InvalidArgument` and says which rule: an
/// application picks its own keys, so a refusal is something it can fix.
pub(crate) fn parse_repo(key: Option<&WireRepoKey>) -> Result<RepoKey, Status> {
    key.ok_or_else(|| Status::invalid_argument("no repository given"))?
        .key
        .parse()
        .map_err(|bad| Status::invalid_argument(format!("{bad}")))
}

/// A git timestamp on its way out.
///
/// Git counts whole seconds, so the nanoseconds a `Timestamp` carries are
/// always zero.
pub(crate) fn wire_time(unix_seconds: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: unix_seconds,
        nanos: 0,
    }
}

/// [`wire_time`] of a time Enroute recorded, in whole seconds; unset if
/// there is none or it does not fit.
pub(crate) fn wire_system_time(at: Option<SystemTime>) -> Option<prost_types::Timestamp> {
    at.and_then(|at| at.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| i64::try_from(since.as_secs()).ok())
        .map(wire_time)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::oid::ObjectFormat;

    fn wire(hex: &str) -> WireObjectId {
        WireObjectId { hex: hex.into() }
    }

    #[test]
    fn an_id_is_forty_or_sixty_four_hex_digits_and_nothing_else() {
        let hex = "9f2c1b7e4a8d3c5f6b0e2a1d4c7f8b3e5a6d9c0f";
        assert_eq!(parse_oid(&wire(hex)).unwrap().to_hex(), hex);
        assert_eq!(parse_oid(&wire(&hex.to_uppercase())).unwrap().to_hex(), hex);
        let long = "a".repeat(64);
        assert_eq!(
            parse_oid(&wire(&long)).unwrap().format(),
            ObjectFormat::Sha256
        );
        for bad in [
            "",
            " 9f2c1b7e4a8d3c5f6b0e2a1d4c7f8b3e5a6d9c0",
            &"a".repeat(63),
            &"a".repeat(41),
            "zz",
        ] {
            let refused = parse_oid(&wire(bad)).unwrap_err();
            assert_eq!(refused.code(), tonic::Code::InvalidArgument, "{bad:?}");
        }
    }

    #[test]
    fn the_zero_id_and_an_unset_one_both_mean_no_object() {
        assert_eq!(optional_oid(None).unwrap(), None);
        assert_eq!(optional_oid(Some(&wire(&"0".repeat(40)))).unwrap(), None);
        assert!(
            optional_oid(Some(&wire(&"1".repeat(40))))
                .unwrap()
                .is_some()
        );
        assert_eq!(wire_oid_or_unset(Oid::ZERO), None);
    }
}
