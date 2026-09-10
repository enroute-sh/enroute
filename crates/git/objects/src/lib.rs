//! The object index as joinable segments: where a tree or a blob is stored,
//! and what a tree's entries are.
//!
//! A record per seq rather than a sorted pair per object. Every kind is
//! counted on its own, so each is dense in its own space, the key is the
//! position, and a lookup is arithmetic rather than a search. Both halves of
//! a record are grow-only — a pack is added and never edited, and a tree's
//! entries are fixed by its own bytes — so the join is a union, with nothing
//! to decide about which side wins.

mod format;
mod index;
mod read;
mod store;
mod write;

pub use format::Malformed;
pub use index::{Builder, Location, Object, ObjectIndex};
pub use store::{Held, Objects, RepoObjects, Shelf};
pub use write::Recorded;
