//! Turning what the contract carries into what the engine holds, and back.
//!
//! Here rather than in each handler because `common.v1alpha1` states one rule
//! about an absent object id, and both the contract and the hooks keep it.
//!
//! # Why not `enroute-api`
//!
//! It holds the generated contract and no hand-written logic, so a conversion
//! reachable only from Rust would be contract no other language gets.

use gix_hash::ObjectId;
use tonic::Status;

use enroute_api::common::v1alpha1::{ObjectId as WireObjectId, RepoKey as WireRepoKey};

use crate::tenancy::RepoKey;

/// Object ids cross the contract as hex.
///
/// A malformed one is the caller's own string, not a repository fault, so
/// it comes back as `InvalidArgument`.
pub(crate) fn parse_oid(id: &WireObjectId) -> Result<ObjectId, Status> {
    parse_hex(&id.hex, "an object id")
}

/// An object id spelled as a bare string, which the page token is.
pub(crate) fn parse_hex(hex: &str, what: &str) -> Result<ObjectId, Status> {
    ObjectId::from_hex(hex.as_bytes())
        .map_err(|_bad_hex| Status::invalid_argument(format!("not {what}: {hex}")))
}

/// An object id the call cannot proceed without.
///
/// Unset is the caller's own omission, so it reads as `InvalidArgument`
/// beside a malformed one rather than as a repository fault.
pub(crate) fn require_oid(id: Option<&WireObjectId>, field: &str) -> Result<ObjectId, Status> {
    let id = id.ok_or_else(|| Status::invalid_argument(format!("{field} is required")))?;
    parse_oid(id)
}

/// An object id whose absence means "no object", which the ref store spells
/// as git's all-zero oid.
pub(crate) fn oid_or_null(id: Option<&WireObjectId>) -> Result<ObjectId, Status> {
    id.map_or_else(|| Ok(ObjectId::null(gix_hash::Kind::Sha1)), parse_oid)
}

/// An object id on its way out.
pub(crate) fn wire_oid(oid: ObjectId) -> WireObjectId {
    WireObjectId {
        hex: oid.to_hex().to_string(),
    }
}

/// An object id on its way out, where the all-zero oid means "no object".
pub(crate) fn wire_oid_or_unset(oid: ObjectId) -> Option<WireObjectId> {
    (!oid.is_null()).then(|| wire_oid(oid))
}

/// An object id on its way out, from the hex a hook already holds.
///
/// The push path carries ids as hex strings rather than as parsed oids, so
/// this is the same rule reached from the other side.
pub(crate) fn wire_oid_from_hex(hex: Option<&String>) -> Option<WireObjectId> {
    hex.map(|hex| WireObjectId { hex: hex.clone() })
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
