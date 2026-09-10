//! What a joinable value must do to be held as bytes.

use crate::join::Join;
use crate::key::KeyRange;

/// A joinable value that can cross between memory and storage.
///
/// The value reports its own range, so a catalog row is derived from the
/// bytes rather than kept in step with them.
pub trait Segment: Join + Sized {
    /// What a corrupt or unreadable segment is reported as.
    type Error: core::error::Error + Send + Sync + 'static;

    /// The keys this value holds, or `None` when it holds none.
    fn range(&self) -> Option<KeyRange>;

    /// Appends the encoding of this value to `out`.
    fn encode(&self, out: &mut Vec<u8>);

    /// Reads back the part of `bytes` that `range` covers.
    ///
    /// Returning more than `range` is correct — a caller indexes by key — so
    /// a layout with nothing to seek by may ignore it and read everything.
    ///
    /// # Errors
    /// [`Segment::Error`] when the bytes are not a segment of this type.
    fn decode_range(bytes: &[u8], range: KeyRange) -> Result<Self, Self::Error>;

    /// Reads back everything [`Segment::encode`] wrote.
    ///
    /// # Errors
    /// [`Segment::Error`] when the bytes are not a segment of this type.
    fn decode(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::decode_range(bytes, KeyRange::EVERYTHING)
    }

    /// The encoding of this value, as its own buffer.
    fn encoded(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }
}
