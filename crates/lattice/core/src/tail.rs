//! The tail a fixed-stride record points into: length-prefixed blobs, and
//! the offsets that reach them.
//!
//! A record holds what every key has; what varies — a bitmap, a list — goes
//! behind it, and the record keeps a four-byte offset. Each blob carries its
//! own length in front, so a reader that has only the offset still knows
//! where the blob ends and can refuse one that runs off the end. Beside
//! [`crate::frame`] rather than in it: the header owns how many units the
//! tail has, and this owns what one blob in it looks like.

use roaring::RoaringTreemap;

/// A record pointing at no blob at all.
///
/// The top offset rather than a flag bit, which costs the last byte of a
/// tail no blob is ever written at and keeps the empty case out of it.
pub const NONE: u32 = u32::MAX;

/// Bytes the length in front of a blob takes.
const LENGTH: usize = 4;

/// Appends `blob` to `tail`, and says where it went.
///
/// [`NONE`] for an empty blob, which then costs the tail nothing.
pub fn stow(blob: &[u8], tail: &mut Vec<u8>) -> u32 {
    if blob.is_empty() {
        return NONE;
    }
    let at = u32::try_from(tail.len()).unwrap_or(NONE);
    tail.extend_from_slice(&u32::try_from(blob.len()).unwrap_or(0).to_le_bytes());
    tail.extend_from_slice(blob);
    at
}

/// Reads back what [`stow`] wrote, or `None` for an offset the tail does not
/// hold.
///
/// Read through rather than past: an offset beyond the end would otherwise
/// answer "nothing there", which is a lost value rather than a refused one.
#[must_use]
pub fn fetch(at: u32, tail: &[u8]) -> Option<&[u8]> {
    if at == NONE {
        return Some(&[]);
    }
    let start = usize::try_from(at).ok()?;
    let len = usize::try_from(length(at, tail)?).ok()?;
    let body = start.checked_add(LENGTH)?;
    tail.get(body..body.checked_add(len)?)
}

/// The length word a blob starts with, for a reader that counts something
/// other than bytes with it.
#[must_use]
pub fn length(at: u32, tail: &[u8]) -> Option<u32> {
    usize::try_from(at)
        .ok()
        .and_then(|start| tail.get(start..))
        .and_then(<[u8]>::first_chunk::<LENGTH>)
        .map(|chunk| u32::from_le_bytes(*chunk))
}

/// A bitmap as the bytes a tail holds, or `None` when it will not serialize.
///
/// Empty bytes for an empty bitmap, so [`stow`] keeps it out of the tail.
#[must_use]
pub fn encode_bitmap(bitmap: &RoaringTreemap) -> Option<Vec<u8>> {
    if bitmap.is_empty() {
        return Some(Vec::new());
    }
    let mut out = Vec::new();
    bitmap.serialize_into(&mut out).ok()?;
    Some(out)
}

/// The bitmap `bytes` are, or `None` when they are not one.
#[must_use]
pub fn decode_bitmap(bytes: &[u8]) -> Option<RoaringTreemap> {
    if bytes.is_empty() {
        return Some(RoaringTreemap::new());
    }
    RoaringTreemap::deserialize_from(bytes).ok()
}

/// Appends `bitmap` to `tail` as [`stow`] would any other blob.
///
/// `None` only when the bitmap will not serialize, which a caller holding
/// one built in memory can treat as impossible or as corrupt.
#[must_use]
pub fn stow_bitmap(bitmap: &RoaringTreemap, tail: &mut Vec<u8>) -> Option<u32> {
    Some(stow(&encode_bitmap(bitmap)?, tail))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blob_reads_back_where_it_was_stowed() {
        let mut tail = Vec::new();
        let first = stow(b"one", &mut tail);
        let second = stow(b"second", &mut tail);

        assert_eq!(fetch(first, &tail), Some(b"one".as_slice()));
        assert_eq!(fetch(second, &tail), Some(b"second".as_slice()));
    }

    #[test]
    fn an_empty_blob_costs_the_tail_nothing() {
        let mut tail = Vec::new();
        assert_eq!(stow(&[], &mut tail), NONE);
        assert!(tail.is_empty());
        assert_eq!(fetch(NONE, &tail), Some([].as_slice()));
    }

    #[test]
    fn an_offset_outside_the_tail_is_refused() {
        let mut tail = Vec::new();
        stow(b"one", &mut tail);
        assert_eq!(fetch(99, &tail), None);
    }

    /// A length longer than what follows it would otherwise read short.
    #[test]
    fn a_length_the_tail_does_not_hold_is_refused() {
        let mut tail = Vec::new();
        let at = stow(b"one", &mut tail);
        tail.truncate(tail.len() - 1);
        assert_eq!(fetch(at, &tail), None);
    }

    #[test]
    fn a_bitmap_reads_back_as_it_was_stowed() {
        let bitmap: RoaringTreemap = [1_u64, 2, 900_000].into_iter().collect();
        let mut tail = Vec::new();
        let at = stow_bitmap(&bitmap, &mut tail).expect("a bitmap that serializes");

        let read = decode_bitmap(fetch(at, &tail).expect("the blob")).expect("a bitmap");
        assert_eq!(read, bitmap);
    }

    #[test]
    fn an_empty_bitmap_costs_the_tail_nothing() {
        let mut tail = Vec::new();
        assert_eq!(stow_bitmap(&RoaringTreemap::new(), &mut tail), Some(NONE));
        assert!(tail.is_empty());
        assert_eq!(
            decode_bitmap(fetch(NONE, &tail).expect("no blob")),
            Some(RoaringTreemap::new())
        );
    }
}
