//! What a deployment configures, in the shapes both ends of it share.
//!
//! Two things live here, and they are the two a process needs before it can do
//! anything: where a store's bytes are, as one string, and the file those
//! strings are written in. The front door reads both. The ingest function
//! reads neither from disk — its locations arrive in the call — but it builds
//! its stores out of the same grammar, and a grammar with two parsers is two
//! grammars. No flags: which of these a command line names, and what it calls
//! them, is the front door's business, and a crate the ingest function also
//! depends on has no business holding it.

mod expand;
#[cfg(feature = "file")]
mod file;
mod read;
mod secret;
mod store_uri;

#[cfg(feature = "file")]
pub use file::{
    Config, Database, Hooks, InProcess, Ingest, JustDatabase, JustMaintenance, Lambda, Listen,
    Local, Maintenance, Migrate, Off, Run, Sync, Telemetry, Tenants,
};
pub use read::{Capped, MAX_BYTES, read_capped};
pub use secret::Secret;
pub use store_uri::{Bucket, ObjectUri, ScratchUri, StoreUri};
