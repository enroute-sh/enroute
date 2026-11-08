//! The protocol's types, generated from `proto/state.proto` at build time
//! (see `build.rs`).

// Generated code: prost's output trips lints that hand-written code must pass.
#![expect(
    unused_qualifications,
    missing_docs,
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::as_conversions,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    reason = "prost/tonic codegen"
)]

include!(concat!(env!("OUT_DIR"), "/enroute.internal.v1.rs"));

/// The largest message the protocol carries: proposals and `Peer` batches
/// carry inline layers (up to 1 MiB each, more when merged), far above
/// gRPC's default 4 MiB limit.
pub const MAX_MESSAGE: usize = 512 << 20;
