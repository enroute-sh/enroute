//! What every Enroute node shares, whichever roles it runs (the ledger, the
//! compute): the object store, node heartbeats and rank, a repository's state
//! and the commands that change it, the ledger's protocol, and the few types
//! those carry (object ids).

#![deny(unsafe_code)]
#![expect(non_ascii_idents, reason = "zerocopy derive output")]

pub mod auth;
pub mod call_timing;
pub mod cluster;
pub mod durable;
pub mod format;
pub mod nodes;
pub mod oid;
pub mod proto;
pub mod repo_state;
pub mod roles;
pub mod state;
pub mod store;
pub mod trace_context;
pub mod version;
