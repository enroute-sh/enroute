//! The ingest worker's two ends and the protocol between them.
//!
//! One crate is what makes the protocol impossible to change on one side
//! only: [`convert`] matches exhaustively in both directions, so a type that
//! gains a variant stops the default build rather than the unrebuilt end.

pub mod convert;
pub mod trace;
pub mod wire;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "server")]
pub mod boot;
#[cfg(feature = "server")]
pub mod server;
#[cfg(feature = "server")]
pub mod telemetry;
