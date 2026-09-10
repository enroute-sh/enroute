//! One handler per git route, and the protocol-version check they share.

mod info_refs;
mod receive_pack;
mod upload_pack;

use axum::http::HeaderMap;

pub(crate) use info_refs::info_refs;
pub(crate) use receive_pack::receive_pack;
pub(crate) use upload_pack::upload_pack;

use crate::error::HttpError;

/// Whether the client asked for protocol v2.
///
/// Read, not required, on the advertisement — required only on the body,
/// since v0's negotiation differs from v2's `command=fetch`.
fn speaks_v2(headers: &HeaderMap) -> bool {
    headers
        .get("Git-Protocol")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("version=2"))
}

/// Refuse a request whose body only a v2 client could have written.
fn require_v2(headers: &HeaderMap) -> Result<(), HttpError> {
    if speaks_v2(headers) {
        Ok(())
    } else {
        Err(HttpError::ProtocolVersionRequired)
    }
}
