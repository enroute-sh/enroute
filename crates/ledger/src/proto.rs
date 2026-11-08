//! The ledger's own protocol types, generated from `proto/` at build time
//! (see `build.rs`): its log records, the `Peer` service between ledger
//! nodes, and the `Cluster` service of their membership.

// Generated code: prost's output trips lints that hand-written code must pass.
#![expect(
    unused_qualifications,
    unreachable_pub,
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::as_conversions,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::excessive_nesting,
    clippy::too_many_lines,
    reason = "prost/tonic codegen"
)]

include!(concat!(env!("OUT_DIR"), "/enroute.internal.peer.v1.rs"));

impl From<root_entry::Change> for RootEntry {
    fn from(change: root_entry::Change) -> Self {
        RootEntry {
            change: Some(change),
        }
    }
}
