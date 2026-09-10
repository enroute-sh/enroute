//! One value held as segments, over a catalog that lists them and an object
//! store that holds the large ones.
//!
//! [`Catalog`] is the seam. It says what segments a scope has and carries the
//! inlined ones' bytes back with the list, which is the whole reason to inline
//! at all: many small segments is one query here and many GETs there. Nothing
//! above that seam says what a scope is, what the list is kept in, or what the
//! segments mean — [`Segments`] only reads, writes and compacts them, and the
//! algebra it does that with is [`enroute_lattice_core`]. A bucket object is
//! put before the row naming it, and deleted after the row stops, so a row
//! never names a key that is not there and a failed delete is an orphan for a
//! janitor rather than a dangling reference. Below the seam this crate ships
//! one catalog, [`MemoryCatalog`]; the one that speaks to a database lives in
//! the crate that owns the database, and names no driver here. The
//! `conformance` feature adds the suite that catalog answers to, so both
//! implementations of the seam are held to one contract.

mod catalog;
#[cfg(feature = "conformance")]
pub mod conformance;
#[cfg(feature = "conformance")]
mod counters;
mod error;
#[cfg(feature = "conformance")]
pub mod inspect;
pub mod memory;
mod scope;
mod segments;

pub use catalog::{Body, Catalog, CatalogError, CatalogRef, Entry, Listed, SegmentId, Written};
#[cfg(feature = "conformance")]
pub use counters::{Counters, Ragged};
pub use error::Error;
pub use memory::MemoryCatalog;
pub use scope::Scope;
pub use segments::{Reclaimed, Report, Segments, Sweep, Swept};
