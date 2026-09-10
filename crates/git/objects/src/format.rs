//! What an object record holds, and the ways its bytes can fail to be one.
//!
//! The framing is [`enroute_lattice_core::frame`]'s, shared with every other
//! segment type here — but an entry carries its own key, where a commit
//! record is found by position. An object gains packs after it is first
//! recorded, so a re-inclusion segment holds a few objects scattered across
//! the whole space; a slot per seq would be the width of that scatter.

use thiserror::Error;

use enroute_lattice_core::frame::{Frame, Layout};

/// Bytes one entry takes: a seq, and where its locations and children are.
pub(crate) const ENTRY: usize = 20;

/// How an object index is framed.
pub(crate) const OBJECTS: Layout = Layout {
    magic: *b"OIX1",
    record: ENTRY,
    unit: 1,
};

/// Bytes one stored location takes.
pub(crate) const LOCATION: usize = 84;

/// Bytes that are not an object index this crate can read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Malformed {
    /// Not framed the way every segment here is.
    #[error(transparent)]
    Frame(#[from] Frame),

    /// A heap reference pointing outside the heap.
    #[error("a heap reference to offset {0}, which is not in the heap")]
    StrayHeap(u32),

    /// Entries that are not in ascending seq order.
    ///
    /// The order is what a lookup binary-searches, so reading them as
    /// written would answer wrongly rather than fail.
    #[error("object index entries out of order at {0}")]
    Unsorted(usize),

    /// A children bitmap that would not serialize, or would not read back.
    #[error("a tree's children are not a bitmap")]
    Bitmap,
}
