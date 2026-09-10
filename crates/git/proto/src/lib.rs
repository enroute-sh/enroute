//! Transport-agnostic git wire-protocol logic: pkt-line framing, pack
//! encoding/decoding, and the `ls-refs`/`fetch`/`receive-pack` semantics.
//!
//! This crate knows nothing about HTTP; see `enroute-git-http` for the
//! smart-HTTP binding built on top of it.

mod capabilities;
mod error;
mod ls_refs;
mod pack;
pub mod pktline;
mod receive_pack;
mod upload_pack;
mod visibility;
mod walk;

pub use capabilities::{
    receive_pack_advertisement, upload_pack_capabilities, upload_pack_v0_advertisement,
};
pub use error::Error;
pub use receive_pack::{ReceivePackResponse, receive_pack};
pub use upload_pack::{UploadPackResponse, fetch, upload_pack};
pub use visibility::{Access, AllRefsVisible, RefVisibility, advertised_refs};
pub use walk::{ShallowNeeded, needed};

use std::future::Future;

use tracing::Instrument as _;

/// Spawns `fut` on its own task, carrying over the caller's current span.
///
/// Plain `tokio::spawn` starts the task with no span, silently orphaning
/// anything it logs from the caller's trace.
pub(crate) fn spawn_instrumented<F>(fut: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(fut.instrument(tracing::Span::current()))
}
