//! What a tier-one record holds, and what the two tiers report as corrupt.
//!
//! The framing itself — the header, the record stride, the tail — belongs to
//! [`enroute_lattice_core::frame`] and is shared with every other segment
//! type here. What is left is this crate's own: twenty bytes of parent graph,
//! and the ways those bytes can fail to be one.

use thiserror::Error;

use enroute_lattice_core::frame::{Frame, Layout, field32};

/// Bytes one tier-one record takes.
pub(crate) const RECORD: usize = 20;

/// The tier-one parent graph.
pub(crate) const GRAPH: Layout = Layout {
    magic: *b"CIX0",
    record: RECORD,
    unit: 4,
};

/// No parent, no extra parents, and no commit in this slot.
///
/// One value for all three, which costs the topmost seq of each space: a
/// repository reaching 2^32 - 1 commits or objects has outgrown `u32` anyway.
pub(crate) const NONE: u32 = u32::MAX;

/// One commit, as twenty bytes indexed by its seq.
///
/// Two parents inline because almost every commit has at most two; the rest
/// go out of line, which is what keeps the stride fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) parent1: u32,
    pub(crate) parent2: u32,
    pub(crate) root_tree: u32,
    pub(crate) generation: u32,
    pub(crate) extra: u32,
}

impl Record {
    /// A slot no commit occupies, which is what a hole in the range is.
    pub(crate) const ABSENT: Self = Self {
        parent1: NONE,
        parent2: NONE,
        root_tree: NONE,
        generation: 0,
        extra: NONE,
    };

    /// Whether a commit occupies this slot.
    pub(crate) const fn present(self) -> bool {
        self.root_tree != NONE
    }

    pub(crate) fn write(self, out: &mut Vec<u8>) {
        for field in [
            self.parent1,
            self.parent2,
            self.root_tree,
            self.generation,
            self.extra,
        ] {
            out.extend_from_slice(&field.to_le_bytes());
        }
    }

    /// Reads one record from exactly [`RECORD`] bytes.
    pub(crate) fn read(bytes: &[u8; RECORD]) -> Self {
        Self {
            parent1: field32(bytes, 0),
            parent2: field32(bytes, 4),
            root_tree: field32(bytes, 8),
            generation: field32(bytes, 12),
            extra: field32(bytes, 16),
        }
    }
}

/// Bytes that are not a commit index this crate can read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Malformed {
    /// Not framed the way every segment here is.
    #[error(transparent)]
    Frame(#[from] Frame),

    /// An out-of-line parent list pointing outside the tail.
    #[error("an octopus merge whose extra parents are not in the tail, at offset {0}")]
    StrayExtra(u32),

    /// A pack bitmap that would not serialize, or would not read back.
    #[error("a pack bitmap that is not one")]
    Bitmap,
}

/// The header's first key, as the seq space this crate walks names it.
///
/// # Errors
/// [`Frame::Unnameable`] for a key no catalog could have held, which
/// [`enroute_lattice_core::frame::read_header`] has already refused.
pub(crate) fn first_seq(first: enroute_lattice_core::Key) -> Result<i64, Frame> {
    i64::try_from(first.get())
        .ok()
        .ok_or(Frame::Unnameable(first.get()))
}
