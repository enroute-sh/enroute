//! The bytes a segment is framed in: a header, a record per key, and a tail
//! for whatever does not fit a fixed stride.
//!
//! A record per key rather than a key per record, which is what a dense key
//! space buys: a lookup is an index rather than a search, and the key itself
//! costs nothing to store. The tail is for what varies — a bitmap, a list —
//! and a record reaches it by offset. What a record holds is the segment
//! type's; this owns only the twenty-four bytes in front and the arithmetic
//! that says whether the rest is the length it claims.

use thiserror::Error;

use crate::key::{Key, KeyRange};

/// Bytes before the first record.
pub const HEADER: usize = 24;

/// The only framing this crate writes or reads.
pub const VERSION: u8 = 1;

/// How one segment type is framed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    /// What the first four bytes say this is.
    pub magic: [u8; 4],
    /// Bytes one record takes.
    pub record: usize,
    /// Bytes one element of the tail takes.
    pub unit: usize,
}

/// What a header claims the rest of the bytes are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The key the first record is for.
    pub first: Key,
    /// How many records follow.
    pub records: usize,
    /// How many tail units follow those.
    pub tail: usize,
}

impl Header {
    /// Which of these records fall inside `range`, as a half-open window.
    ///
    /// `None` when the two do not meet, which is what lets a read skip a
    /// segment it listed but does not want.
    #[must_use]
    pub fn slice_of(&self, range: KeyRange) -> Option<(usize, usize)> {
        let first = self.first.get();
        let last = first.checked_add(u64::try_from(self.records).ok()?.checked_sub(1)?)?;
        if range.last().get() < first || range.first().get() > last {
            return None;
        }
        let from = usize::try_from(range.first().get().max(first) - first).ok()?;
        let upto = usize::try_from(range.last().get().min(last) - first).ok()? + 1;
        Some((from.min(self.records), upto.min(self.records)))
    }
}

/// Bytes that are not a segment framed this way.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Frame {
    /// Too short to hold even a header.
    #[error("a segment is at least {HEADER} bytes, and {0} is not")]
    Short(usize),

    /// Not the segment type that was expected.
    #[error("a segment starting with {found:?}, where {wanted:?} was wanted")]
    Magic {
        /// What the bytes start with.
        found: [u8; 4],
        /// What the reader writes.
        wanted: [u8; 4],
    },

    /// A framing version this crate does not know.
    #[error("this crate reads framing version {VERSION}, not {0}")]
    Version(u8),

    /// The header's counts and the actual length disagree.
    #[error("a header claiming {expected} bytes of records and tail, over {actual}")]
    Truncated {
        /// What the header's counts add up to.
        expected: usize,
        /// What there actually is.
        actual: usize,
    },

    /// A first key no catalog could have named.
    ///
    /// A catalog holds a range as two signed integers, so a key above that
    /// cannot round-trip and the bytes did not come from a writer here.
    #[error("a segment starting at {0}, which no catalog could name")]
    Unnameable(u64),
}

/// Writes the twenty-four bytes `read_header` reads back.
pub fn write_header(header: &Header, layout: Layout, out: &mut Vec<u8>) {
    out.extend_from_slice(&layout.magic);
    out.push(VERSION);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(&header.first.get().to_le_bytes());
    out.extend_from_slice(&saturating(header.records).to_le_bytes());
    out.extend_from_slice(&saturating(header.tail).to_le_bytes());
}

/// Reads the header, checking it against how many bytes there really are.
///
/// # Errors
/// [`Frame`] when the bytes are not this layout, or are not as long as the
/// header says they are.
pub fn read_header(bytes: &[u8], layout: Layout) -> Result<Header, Frame> {
    let Some(head) = bytes.get(..HEADER) else {
        return Err(Frame::Short(bytes.len()));
    };
    let found = head.first_chunk::<4>().copied().unwrap_or_default();
    if found != layout.magic {
        return Err(Frame::Magic {
            found,
            wanted: layout.magic,
        });
    }
    let version = head.get(4).copied().unwrap_or_default();
    if version != VERSION {
        return Err(Frame::Version(version));
    }

    let first = field64(head, 8);
    if i64::try_from(first).is_err() {
        return Err(Frame::Unnameable(first));
    }
    let records = counted(head, 16);
    let tail = counted(head, 20);

    let expected = HEADER
        .saturating_add(records.saturating_mul(layout.record))
        .saturating_add(tail.saturating_mul(layout.unit));
    if expected != bytes.len() {
        return Err(Frame::Truncated {
            expected,
            actual: bytes.len(),
        });
    }
    Ok(Header {
        first: Key::new(first),
        records,
        tail,
    })
}

/// Reads a `u64` field out of a fixed-width record.
#[must_use]
pub fn field64(bytes: &[u8], at: usize) -> u64 {
    bytes
        .get(at..at + 8)
        .and_then(|slice| slice.first_chunk::<8>())
        .map_or(0, |chunk| u64::from_le_bytes(*chunk))
}

/// Reads a `u32` field out of a fixed-width record.
#[must_use]
pub fn field32(bytes: &[u8], at: usize) -> u32 {
    bytes
        .get(at..at + 4)
        .and_then(|slice| slice.first_chunk::<4>())
        .map_or(0, |chunk| u32::from_le_bytes(*chunk))
}

/// A count as the header holds it, saturating rather than wrapping.
fn saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// A header count, as a length this platform can index by.
fn counted(bytes: &[u8], at: usize) -> usize {
    usize::try_from(field32(bytes, at)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LAYOUT: Layout = Layout {
        magic: *b"TEST",
        record: 4,
        unit: 2,
    };

    fn framed(header: &Header) -> Vec<u8> {
        let mut out = Vec::new();
        write_header(header, LAYOUT, &mut out);
        out.resize(
            HEADER + header.records * LAYOUT.record + header.tail * LAYOUT.unit,
            0,
        );
        out
    }

    #[test]
    fn a_header_reads_back_as_written() {
        let header = Header {
            first: Key::new(9),
            records: 3,
            tail: 5,
        };
        let read = read_header(&framed(&header), LAYOUT).unwrap();
        assert_eq!(read, header);
    }

    #[test]
    fn another_layouts_bytes_are_refused() {
        let bytes = framed(&Header {
            first: Key::ZERO,
            records: 1,
            tail: 0,
        });
        let other = Layout {
            magic: *b"OTHR",
            ..LAYOUT
        };
        assert!(matches!(
            read_header(&bytes, other),
            Err(Frame::Magic { .. })
        ));
    }

    #[test]
    fn a_length_the_header_does_not_account_for_is_refused() {
        let mut bytes = framed(&Header {
            first: Key::ZERO,
            records: 2,
            tail: 1,
        });
        bytes.push(0);
        assert!(matches!(
            read_header(&bytes, LAYOUT),
            Err(Frame::Truncated { .. })
        ));
    }

    #[test]
    fn a_first_key_no_catalog_could_hold_is_refused() {
        let mut bytes = framed(&Header {
            first: Key::ZERO,
            records: 0,
            tail: 0,
        });
        bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            read_header(&bytes, LAYOUT),
            Err(Frame::Unnameable(u64::MAX))
        ));
    }

    #[test]
    fn too_few_bytes_for_a_header_is_short() {
        assert_eq!(read_header(&[0; 4], LAYOUT), Err(Frame::Short(4)));
    }
}

#[cfg(test)]
mod slice_tests {
    use super::Header;
    use crate::key::{Key, KeyRange};

    fn header(first: u64, records: usize) -> Header {
        Header {
            first: Key::new(first),
            records,
            tail: 0,
        }
    }

    fn range(first: u64, last: u64) -> KeyRange {
        KeyRange::new(Key::new(first), Key::new(last)).expect("first <= last")
    }

    #[test]
    fn a_range_inside_the_records_is_the_window_it_covers() {
        assert_eq!(header(10, 10).slice_of(range(12, 14)), Some((2, 5)));
    }

    #[test]
    fn a_range_reaching_past_either_end_is_clamped() {
        assert_eq!(header(10, 10).slice_of(range(0, 100)), Some((0, 10)));
        assert_eq!(header(10, 10).slice_of(range(0, 11)), Some((0, 2)));
        assert_eq!(header(10, 10).slice_of(range(18, 100)), Some((8, 10)));
    }

    #[test]
    fn a_range_that_does_not_meet_them_is_nothing() {
        assert_eq!(header(10, 10).slice_of(range(0, 9)), None);
        assert_eq!(header(10, 10).slice_of(range(20, 30)), None);
    }

    #[test]
    fn a_header_with_no_records_is_nothing() {
        assert_eq!(header(10, 0).slice_of(range(0, 100)), None);
    }
}
