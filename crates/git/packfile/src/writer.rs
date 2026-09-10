//! Writing git packfiles: the entry encoding that pairs with
//! [`crate::reader`]'s decoding.

use std::io::Write as _;

use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::Error;

/// Write the 12-byte pack file header (`PACK`, version 2, object count) into
/// `out`.
pub fn write_pack_header(object_count: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    out.extend_from_slice(&object_count.to_be_bytes());
}

/// Write a pack entry header (type + decompressed length) into `out`, no
/// compressed body.
///
/// Commit-pack objects already zlib-compressed on disk skip a
/// decompress/recompress round trip; [`write_pack_entry`] takes decompressed content.
///
/// # Errors
/// Returns an error if the header cannot be encoded.
pub fn write_pack_entry_header(
    kind: Kind,
    decompressed_len: u64,
    out: &mut Vec<u8>,
) -> Result<(), Error> {
    use gix_pack::data::entry::Header as PackHeader;

    let pack_header = match kind {
        Kind::Commit => PackHeader::Commit,
        Kind::Tree => PackHeader::Tree,
        Kind::Blob => PackHeader::Blob,
        Kind::Tag => PackHeader::Tag,
    };
    pack_header
        .write_to(decompressed_len, out)
        .map_err(|e| anyhow::anyhow!("pack entry header: {e}"))?;
    Ok(())
}

/// Write a `REF_DELTA` entry header — type, delta length, base oid — with no
/// compressed body, for a caller holding already-compressed delta bytes.
///
/// `decompressed_len` is the delta instructions' length, not the object's —
/// the receiver resolves `base` by oid, so entry order doesn't matter.
///
/// # Errors
/// Returns an error if the header cannot be encoded.
pub fn write_ref_delta_header(
    base: ObjectId,
    decompressed_len: u64,
    out: &mut Vec<u8>,
) -> Result<(), Error> {
    use gix_pack::data::entry::Header as PackHeader;

    PackHeader::RefDelta { base_id: base }
        .write_to(decompressed_len, out)
        .map_err(|e| anyhow::anyhow!("ref delta entry header: {e}"))?;
    Ok(())
}

/// Write one base (non-delta) pack entry into `out`, compressing `content`
/// fresh — used for annotated tags, which stay zlib-loose.
///
/// # Errors
/// Returns an error if `content` is too large to encode, or compression
/// fails.
pub fn write_pack_entry(kind: Kind, content: &[u8], out: &mut Vec<u8>) -> Result<(), Error> {
    let decompressed_len =
        u64::try_from(content.len()).map_err(|e| anyhow::anyhow!("content too large: {e}"))?;
    write_pack_entry_header(kind, decompressed_len, out)?;
    let mut enc = flate2::write::ZlibEncoder::new(&mut *out, flate2::Compression::fast());
    enc.write_all(content)
        .map_err(|e| anyhow::anyhow!("entry zlib: {e}"))?;
    enc.finish()
        .map_err(|e| anyhow::anyhow!("entry zlib finish: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use gix_object::Kind;

    use super::{write_pack_entry, write_pack_header};
    use crate::{EntryHeader, PackReader};

    fn any_kind() -> impl proptest::strategy::Strategy<Value = Kind> {
        use proptest::prelude::*;
        proptest::prop_oneof![
            Just(Kind::Commit),
            Just(Kind::Tree),
            Just(Kind::Blob),
            Just(Kind::Tag),
        ]
    }

    proptest::proptest! {
        #[test]
        fn write_pack_entry_roundtrips_through_pack_reader(
            kind in any_kind(),
            content in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..2048),
        ) {
            let mut out = Vec::new();
            write_pack_entry(kind, &content, &mut out).unwrap();

            let mut reader = PackReader::new(out.as_slice());
            let (header, size) =
                futures::executor::block_on(reader.read_entry_header()).unwrap();
            proptest::prop_assert_eq!(header, EntryHeader::Full(kind));
            proptest::prop_assert_eq!(size, u64::try_from(content.len()).unwrap());
            let header_len = usize::try_from(reader.offset()).unwrap();
            futures::executor::block_on(reader.skip_entry_body(u64::try_from(content.len()).unwrap())).unwrap();
            proptest::prop_assert_eq!(reader.offset(), u64::try_from(out.len()).unwrap());

            let body = out.split_at(header_len).1;
            let decompressed = crate::inflate_entry(
                body,
                u64::try_from(content.len()).unwrap(),
            )
            .unwrap();
            proptest::prop_assert_eq!(&decompressed[..], &content[..]);
        }

        #[test]
        fn write_pack_header_roundtrips_through_pack_reader(object_count in proptest::prelude::any::<u32>()) {
            let mut out = Vec::new();
            write_pack_header(object_count, &mut out);
            let mut reader = PackReader::new(out.as_slice());
            let decoded = futures::executor::block_on(reader.read_pack_header()).unwrap();
            proptest::prop_assert_eq!(decoded, object_count);
        }
    }
}

#[cfg(test)]
mod ref_delta_tests {
    use super::write_ref_delta_header;
    use gix_hash::ObjectId;

    /// A mis-framed header silently yields garbage base ids rather than an
    /// error, since the receiver reads the base straight out of those 20 bytes.
    #[test]
    fn a_ref_delta_header_round_trips_through_the_reader() {
        let base = ObjectId::from_bytes_or_panic(&[0xab; 20]);
        let mut out = Vec::new();
        write_ref_delta_header(base, 1234, &mut out).unwrap();

        // Byte 0 is the type/size byte: bits 4-6 hold the type, 7 = REF_DELTA.
        let type_id = (out[0] >> 4) & 0b0111;
        assert_eq!(type_id, 7, "type nibble must say REF_DELTA, got {type_id}");
        assert_eq!(
            &out[out.len() - 20..],
            base.as_slice(),
            "header must end with the base oid"
        );
    }
}
