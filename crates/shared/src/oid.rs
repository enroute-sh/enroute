//! Object identifiers. A repository's ids are SHA-1 or SHA-256, chosen when
//! it is made (its [`ObjectFormat`]), and an [`Oid`] carries its own width
//! the way git's `object_id` does, so one type serves both. Hashing objects
//! is the compute's job.

use std::fmt;

/// The length of the longest object id, in bytes (SHA-256).
pub const MAX_OID_LEN: usize = 32;

/// How a repository names its objects. A configuration spells it as git
/// does: `"sha1"` or `"sha256"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObjectFormat {
    /// 20-byte SHA-1 ids, git's default.
    #[default]
    Sha1,
    /// 32-byte SHA-256 ids (git's `objectFormat = sha256`).
    Sha256,
}

impl ObjectFormat {
    /// Every format.
    pub const ALL: [ObjectFormat; 2] = [ObjectFormat::Sha1, ObjectFormat::Sha256];

    /// The length of an id, in bytes.
    #[must_use]
    #[expect(clippy::len_without_is_empty, reason = "a format, not a container")]
    pub const fn len(self) -> usize {
        match self {
            ObjectFormat::Sha1 => 20,
            ObjectFormat::Sha256 => 32,
        }
    }

    /// The length of an id in hex.
    #[must_use]
    pub const fn hex_len(self) -> usize {
        self.len() * 2
    }

    /// The format whose ids are `len` bytes long.
    #[must_use]
    pub(crate) fn from_len(len: usize) -> Option<ObjectFormat> {
        Self::ALL.into_iter().find(|f| f.len() == len)
    }

    /// The format whose ids are `len` hex digits long.
    #[must_use]
    fn from_hex_len(len: usize) -> Option<ObjectFormat> {
        Self::ALL.into_iter().find(|f| f.hex_len() == len)
    }

    /// Git's name for the format, as `object-format` spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            ObjectFormat::Sha1 => "sha1",
            ObjectFormat::Sha256 => "sha256",
        }
    }

    /// The format git calls `name`.
    #[must_use]
    pub fn from_name(name: &str) -> Option<ObjectFormat> {
        Self::ALL.into_iter().find(|f| f.name() == name)
    }

    /// The all-zero id of this format, which names no object.
    #[must_use]
    pub const fn zero(self) -> Oid {
        Oid {
            bytes: [0; MAX_OID_LEN],
            #[expect(
                clippy::as_conversions,
                clippy::cast_possible_truncation,
                reason = "a format's length fits a byte (no From in a const fn)"
            )]
            len: self.len() as u8,
            _pad: [0; 3],
        }
    }
}

impl fmt::Display for ObjectFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// An object id: the id's bytes, zero-padded to [`MAX_OID_LEN`], and its
/// length, which names its format. Ids of one format order by their bytes.
/// 36 bytes, so that records holding one keep the layout (and lack of
/// padding) they had with 20-byte SHA-1 ids.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    zerocopy::FromBytes,
    zerocopy::IntoBytes,
    zerocopy::Immutable,
    zerocopy::KnownLayout,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct Oid {
    bytes: [u8; MAX_OID_LEN],
    len: u8,
    /// Always zero, so equal ids are equal bytes.
    _pad: [u8; 3],
}

impl Default for Oid {
    fn default() -> Oid {
        Oid::ZERO
    }
}

impl Oid {
    /// The all-zero SHA-1 id, which names no object. A placeholder where
    /// the format does not matter; [`ObjectFormat::zero`] is the one to
    /// send.
    pub const ZERO: Oid = ObjectFormat::Sha1.zero();

    /// A SHA-1 id.
    #[must_use]
    pub const fn sha1(b: [u8; 20]) -> Oid {
        let mut o = ObjectFormat::Sha1.zero();
        if let Some(head) = o.bytes.first_chunk_mut::<20>() {
            *head = b;
        }
        o
    }

    /// A SHA-256 id.
    #[must_use]
    pub const fn sha256(b: [u8; 32]) -> Oid {
        let mut o = ObjectFormat::Sha256.zero();
        o.bytes = b;
        o
    }

    /// An id of `format` whose bytes all equal `b`, for tests.
    #[must_use]
    pub fn repeat(format: ObjectFormat, b: u8) -> Oid {
        let mut o = format.zero();
        o.id_mut().fill(b);
        o
    }

    /// The id of `format` at the start of `b`, which must be at least
    /// `format.len()` bytes long.
    ///
    /// # Panics
    ///
    /// Panics if `b` is shorter than an id of `format`.
    #[must_use]
    pub fn from_slice(format: ObjectFormat, b: &[u8]) -> Oid {
        // Fixed-size copies: this runs for every id a tree or record holds.
        let id = match format {
            ObjectFormat::Sha1 => b.first_chunk().map(|b| Oid::sha1(*b)),
            ObjectFormat::Sha256 => b.first_chunk().map(|b| Oid::sha256(*b)),
        };
        #[expect(
            clippy::unwrap_used,
            reason = "the documented contract: the caller passes a whole id"
        )]
        id.unwrap()
    }

    /// The id in `b`, whose length names its format.
    #[must_use]
    pub fn from_bytes(b: &[u8]) -> Option<Oid> {
        ObjectFormat::from_len(b.len()).map(|f| Oid::from_slice(f, b))
    }

    /// Parse an id from hex: 40 digits for SHA-1, 64 for SHA-256.
    /// Surrounding whitespace is ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if `s` is not an id of either length in hex.
    pub fn from_hex(s: &str) -> anyhow::Result<Oid> {
        let s = s.trim();
        Oid::from_hex_exact(s.as_bytes()).ok_or_else(|| anyhow::anyhow!("invalid object id {s:?}"))
    }

    /// An id written as exactly 40 (SHA-1) or 64 (SHA-256) hex digits, of
    /// either case, with nothing around them.
    #[must_use]
    pub fn from_hex_exact(hex: &[u8]) -> Option<Oid> {
        let mut o = ObjectFormat::from_hex_len(hex.len())?.zero();
        hex::decode_to_slice(hex, o.id_mut()).ok()?;
        Some(o)
    }

    /// The id as lowercase hex.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.as_bytes())
    }

    /// Whether this is an all-zero id.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.bytes == [0; MAX_OID_LEN]
    }

    /// The id's format.
    #[must_use]
    pub fn format(&self) -> ObjectFormat {
        if usize::from(self.len) == ObjectFormat::Sha256.len() {
            ObjectFormat::Sha256
        } else {
            ObjectFormat::Sha1
        }
    }

    /// The raw bytes: 20 for SHA-1, 32 for SHA-256.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        let n = self.format().len();
        #[expect(
            clippy::indexing_slicing,
            reason = "a format's length is at most MAX_OID_LEN"
        )]
        &self.bytes[..n]
    }

    /// The id's bytes, to fill in.
    fn id_mut(&mut self) -> &mut [u8] {
        let n = self.format().len();
        #[expect(
            clippy::indexing_slicing,
            reason = "a format's length is at most MAX_OID_LEN"
        )]
        &mut self.bytes[..n]
    }

    /// The first bytes of the id, which every format has: uniformly
    /// random, so they serve as hash values.
    #[must_use]
    pub fn prefix(&self) -> &[u8; 20] {
        const NONE: [u8; 20] = [0; 20];
        // (The bytes are MAX_OID_LEN long, so the fallback is never taken.)
        self.bytes.first_chunk().unwrap_or(&NONE)
    }
}

/// Ids are uniformly random, so their first 16 bytes are as good a hash
/// input as all of them, and one write is cheaper than three. Equal ids
/// have equal bytes, so they hash alike.
impl std::hash::Hash for Oid {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let head = self.bytes.first_chunk().copied().unwrap_or_default();
        state.write_u128(u128::from_ne_bytes(head));
    }
}

impl fmt::Debug for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// The protocol's `Oid` message (`message Oid { bytes id = 1; }`), so the
/// generated types hold our type rather than raw bytes. The id's length
/// names its format.
impl prost::Message for Oid {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        use prost::encoding::{WireType, encode_key, encode_varint};
        encode_key(1, WireType::LengthDelimited, buf);
        encode_varint(u64::from(self.len), buf);
        buf.put_slice(self.as_bytes());
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        if tag != 1 {
            return prost::encoding::skip_field(wire_type, tag, buf, ctx);
        }
        let mut v: Vec<u8> = Vec::new();
        prost::encoding::bytes::merge(wire_type, &mut v, buf, ctx)?;
        let Some(o) = Oid::from_bytes(&v) else {
            #[expect(deprecated, reason = "prost has no other way to make a decode error")]
            return Err(prost::DecodeError::new(
                "object id is neither 20 nor 32 bytes",
            ));
        };
        *self = o;
        Ok(())
    }

    fn encoded_len(&self) -> usize {
        prost::encoding::key_len(1) + 1 + self.as_bytes().len()
    }

    fn clear(&mut self) {
        *self = Oid::ZERO;
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn ids_round_trip_through_hex_and_the_wire() {
        for f in ObjectFormat::ALL {
            let o = Oid::repeat(f, 0xab);
            assert_eq!(o.format(), f);
            assert_eq!(o.as_bytes().len(), f.len());
            assert_eq!(Oid::from_hex(&o.to_hex()).unwrap(), o);
            assert_eq!(Oid::decode(o.encode_to_vec().as_slice()).unwrap(), o);
            assert_eq!(o.encode_to_vec().len(), o.encoded_len());
        }
    }

    #[test]
    fn formats_do_not_mix() {
        let a = Oid::repeat(ObjectFormat::Sha1, 1);
        assert_eq!(
            Oid::from_hex(&a.to_hex()).unwrap().format(),
            ObjectFormat::Sha1
        );
        assert_ne!(ObjectFormat::Sha1.zero(), ObjectFormat::Sha256.zero());
        Oid::decode([10u8, 3, 1, 2, 3].as_slice()).unwrap_err();
    }
}
