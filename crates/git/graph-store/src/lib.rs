//! The commit graph, as segments over a catalog rather than as rows.
//!
//! Everything about a commit except its content and its name: who its
//! parents are, and where its pack image sits. Two segmented values — the
//! parent graph and the pack facts — over two catalogs, with the bytes, the
//! store that reads them and the walks that schedule those reads in one
//! place, since none is any use without the others. Nothing recursive
//! reaches the database: a band is a composed read of the segments touching
//! it, a walk in seq space, and one lookup to answer in oids. What a commit
//! is called belongs to [`enroute_git_metadata`], which numbers every kind
//! alike; the object index, refs and branches belong to whatever holds the
//! objects.

mod bands;
mod format;
mod generation;
mod index;
mod packs;
mod source;
mod store;
mod strided;
mod walk;
mod write;

pub use bands::{DepthBand, FLAG_HAVE, FLAG_WANT, NotCovered, PaintBand, Reached};
pub use format::Malformed;
pub use index::{Builder, Commit, CommitIndex, DatedCommit, Entry, Parents, TooLarge, Unwritable};
pub use packs::{Pack, PackBuilder, PackEntry, PackIndex};
pub use source::Bases;
pub use store::{CommitGraph, RepoGraph};
pub use walk::{BandPolicy, NeededCommit, NeededCommits, ShallowCommits, WalkSeeds};
pub use write::{Ranks, Recorded};
