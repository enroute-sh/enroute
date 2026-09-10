use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use flate2::{Decompress, FlushDecompress, Status};
use gix_hash::ObjectId;
use gix_object::Kind;
use sha1::Digest as _;
use tokio::io::{AsyncBufRead, AsyncBufReadExt as _, AsyncReadExt as _};

use enroute_git_core::Error;

use crate::format::EntryHeader;

// Large pushes (multi-hundred-MiB packs are common), so this streams entry by
// entry and inflates no more than one compressed body at a time. Every hashed
// read feeds a running SHA1; verify_trailer() finalizes it against the 20-byte
// trailer, which is read without hashing.

/// Sink size for [`PackReader::skip_entry_body`]'s discarded output — only
/// large enough that the inflater makes progress per call.
const INFLATE_SCRATCH_BYTES: usize = 64 * 1024;

/// Reusable inflate state, allocated once per pack, not per entry — a fresh
/// `Decompress` and sink per entry would cost more than the inflating does.
#[derive(Debug)]
struct Inflate {
    dec: Decompress,
    sink: Vec<u8>,
}

/// Streaming decoder for one packfile.
///
/// Reads entry by entry, inflating one compressed body at a time, feeding a
/// running SHA1 [`verify_trailer`](PackReader::verify_trailer) checks.
#[derive(Debug)]
pub struct PackReader<R: AsyncBufRead + Unpin> {
    inner: R,
    /// Every byte accepted so far, for [`PackReader::retaining`] callers;
    /// `None` when nothing is kept, so a plain parse pays nothing for this.
    kept: Option<BytesMut>,
    // Plain SHA1, not the collision-detecting variant used for object IDs:
    // this is a transport integrity check over bytes the client chose, and
    // an attacker who controls the pack controls its trailer too. Git draws
    // the same line, reserving its "unsafe" SHA-1 for pack checksums.
    hasher: sha1::Sha1,
    offset: u64,
    inflate: Inflate,
}

impl<R: AsyncBufRead + Unpin> PackReader<R> {
    /// Start decoding the pack `inner` yields, keeping none of it.
    #[must_use]
    pub fn new(inner: R) -> Self {
        Self::with_kept(inner, None)
    }

    /// Start decoding, keeping a verbatim copy as it is parsed — for a caller
    /// that reads entries back in an order the stream can't give it.
    ///
    /// `capacity` reserves for that copy, which otherwise doubles its way up
    /// to as much as twice the pack.
    #[must_use]
    pub fn retaining(inner: R, capacity: usize) -> Self {
        Self::with_kept(inner, Some(BytesMut::with_capacity(capacity)))
    }

    fn with_kept(inner: R, kept: Option<BytesMut>) -> Self {
        Self {
            inner,
            kept,
            hasher: sha1::Sha1::default(),
            offset: 0,
            inflate: Inflate {
                dec: Decompress::new(true),
                sink: vec![0u8; INFLATE_SCRATCH_BYTES],
            },
        }
    }

    /// Take the verbatim copy built by [`PackReader::retaining`] — empty for
    /// a reader built with [`PackReader::new`], and after the first call.
    pub fn take_pack(&mut self) -> Bytes {
        self.kept.take().map(BytesMut::freeze).unwrap_or_default()
    }

    /// Byte offset of the current position from the very start of the pack,
    /// and so where an entry sits in what [`PackReader::take_pack`] returns.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Read `buf.len()` bytes, feed them to the SHA1 hasher, and advance the
    /// offset — every pack byte goes through here; pkt-line bytes do not.
    ///
    /// # Errors
    /// Returns an error if the stream ends before `buf` is filled.
    pub(crate) async fn read_exact_hashing(&mut self, buf: &mut [u8]) -> Result<(), Error> {
        self.inner
            .read_exact(buf)
            .await
            .map_err(|e| Error::Invalid(format!("read: {e}")))?;
        self.hasher.update(&*buf);
        keep(&mut self.kept, buf);
        self.offset = self
            .offset
            .saturating_add(u64::try_from(buf.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    async fn read_u8_hashing(&mut self) -> Result<u8, Error> {
        let mut b = [0u8; 1];
        self.read_exact_hashing(&mut b).await?;
        Ok(b[0])
    }

    /// Parse the 12-byte pack file header, returning the object count.
    ///
    /// Format: `PACK` (4 bytes) | version `u32` BE (must be 2) | object-count `u32` BE
    ///
    /// # Errors
    /// Returns [`Error::Invalid`] if the magic or version is wrong, or the
    /// stream ends inside the header.
    pub async fn read_pack_header(&mut self) -> Result<u32, Error> {
        let mut buf = [0u8; 12];
        self.read_exact_hashing(&mut buf).await?;

        if buf.get(..4) != Some(b"PACK".as_slice()) {
            return Err(Error::Invalid("pack missing PACK magic".into()));
        }
        let version = u32::from_be_bytes(
            buf.get(4..8)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| anyhow::anyhow!("pack version bytes"))?,
        );
        if version != 2 {
            return Err(Error::Invalid(format!(
                "unsupported pack version {version}"
            )));
        }
        Ok(u32::from_be_bytes(
            buf.get(8..12)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| anyhow::anyhow!("pack count bytes"))?,
        ))
    }

    /// Parse one entry's header, returning it and the payload's decompressed
    /// size — for a delta, its instructions', not the object's.
    ///
    /// # Errors
    /// Returns an error if the header is truncated or names an unknown type.
    pub async fn read_entry_header(&mut self) -> Result<(EntryHeader, u64), Error> {
        let c = self.read_u8_hashing().await?;
        let type_id = (c >> 4) & 0b0000_0111;
        let mut size = u64::from(c & 0b0000_1111);
        let mut shift = 4u32;
        let mut cur = c;
        while cur & 0b1000_0000 != 0 {
            cur = self.read_u8_hashing().await?;
            let bits = u64::from(cur & 0b0111_1111)
                .checked_shl(shift)
                .ok_or_else(|| Error::Invalid("entry size overflow".into()))?;
            size = size
                .checked_add(bits)
                .ok_or_else(|| Error::Invalid("entry size overflow".into()))?;
            shift += 7;
        }
        let header = match type_id {
            1 => EntryHeader::Full(Kind::Commit),
            2 => EntryHeader::Full(Kind::Tree),
            3 => EntryHeader::Full(Kind::Blob),
            4 => EntryHeader::Full(Kind::Tag),
            6 => EntryHeader::OfsDelta {
                base_distance: self.read_ofs_delta_distance().await?,
            },
            7 => {
                let mut sha = [0u8; 20];
                self.read_exact_hashing(&mut sha).await?;
                EntryHeader::RefDelta {
                    base_id: ObjectId::from_bytes_or_panic(&sha),
                }
            }
            other => {
                return Err(Error::Invalid(format!("unknown pack entry type {other}")));
            }
        };
        Ok((header, size))
    }

    async fn read_ofs_delta_distance(&mut self) -> Result<u64, Error> {
        let c = self.read_u8_hashing().await?;
        let mut value = u64::from(c & 0x7f);
        let mut cur = c;
        while cur & 0x80 != 0 {
            cur = self.read_u8_hashing().await?;
            value = value
                .checked_add(1)
                .and_then(|v| v.checked_shl(7))
                .and_then(|v| v.checked_add(u64::from(cur & 0x7f)))
                .ok_or_else(|| Error::Invalid("OFS_DELTA distance overflow".into()))?;
        }
        Ok(value)
    }

    /// Advances past one entry's compressed body, inflating only far enough
    /// to find where the zlib stream ends.
    ///
    /// A pack entry has no length prefix, so the inflater alone finds where
    /// the next entry starts — a caller wanting the bytes reads the tee below.
    ///
    /// # Errors
    /// Returns [`Error::Invalid`] if the body isn't valid zlib or doesn't
    /// inflate to `expected_size`.
    pub async fn skip_entry_body(&mut self, expected_size: u64) -> Result<(), Error> {
        let Self {
            inner,
            kept,
            hasher,
            offset,
            inflate,
        } = self;
        inflate.dec.reset(true);
        loop {
            let buf = inner
                .fill_buf()
                .await
                .map_err(|e| Error::Invalid(format!("read compressed: {e}")))?;
            if buf.is_empty() {
                return Err(Error::Invalid("unexpected EOF in compressed entry".into()));
            }
            let in_before = inflate.dec.total_in();
            // Overwrites the sink from the start each call, so output never
            // accumulates — only `total_out` is tracked, for the size check.
            let status = inflate
                .dec
                .decompress(buf, &mut inflate.sink, FlushDecompress::None)
                .map_err(|e| Error::Invalid(format!("zlib: {e}")))?;
            let consumed = usize::try_from(inflate.dec.total_in() - in_before)
                .map_err(|e| anyhow::anyhow!("consumed bytes: {e}"))?;
            let eaten = buf
                .get(..consumed)
                .ok_or_else(|| anyhow::anyhow!("consumed exceeds buf len"))?;
            hasher.update(eaten);
            keep(kept, eaten);
            *offset = offset.saturating_add(u64::try_from(consumed).unwrap_or(u64::MAX));
            Pin::new(&mut *inner).consume(consumed);
            if status == Status::StreamEnd {
                break;
            }
        }
        let produced = inflate.dec.total_out();
        if produced != expected_size {
            return Err(Error::Invalid(format!(
                "decompressed size mismatch: expected {expected_size}, got {produced}"
            )));
        }
        Ok(())
    }

    /// Finalizes the SHA1 and checks it against the 20-byte pack trailer,
    /// which is read directly (not hashed) since it's metadata, not content.
    ///
    /// # Errors
    /// Returns [`Error::Invalid`] if the trailer is missing or doesn't match
    /// the running hash of everything read.
    pub async fn verify_trailer(&mut self) -> Result<(), Error> {
        let computed = std::mem::take(&mut self.hasher).finalize();
        let mut trailer = [0u8; 20];
        self.inner
            .read_exact(&mut trailer)
            .await
            .map_err(|e| Error::Invalid(format!("read pack trailer: {e}")))?;
        // Kept though deliberately not hashed: a copy without it isn't the pack.
        keep(&mut self.kept, &trailer);
        if computed.as_slice() != trailer {
            return Err(Error::Invalid("pack checksum mismatch".into()));
        }
        Ok(())
    }
}

/// Append to the kept copy, if one is being built.
fn keep(kept: &mut Option<BytesMut>, bytes: &[u8]) {
    if let Some(kept) = kept.as_mut() {
        kept.extend_from_slice(bytes);
    }
}

// ── tests ── drive the real streaming parser rather than a duplicated sync
// copy, so they exercise the exact code path production traffic uses.

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use gix_pack::data::entry::Header as PackHeader;

    use super::*;
    use crate::format::EntryHeader;

    fn make_entry_header(type_id: u8, size: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        let size_lsb = u8::try_from(size & 0x0f).unwrap();
        let mut remaining = size >> 4;
        let first = if remaining > 0 { 0x80 } else { 0x00 } | (type_id << 4) | size_lsb;
        bytes.push(first);
        while remaining > 0 {
            let byte = u8::try_from(remaining & 0x7f).unwrap();
            remaining >>= 7;
            bytes.push(if remaining > 0 { byte | 0x80 } else { byte });
        }
        bytes
    }

    // ── pack header ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn pack_header_valid() {
        let mut data = [0u8; 12];
        data[..4].copy_from_slice(b"PACK");
        data[4..8].copy_from_slice(&2u32.to_be_bytes());
        data[8..12].copy_from_slice(&42u32.to_be_bytes());
        let mut reader = PackReader::new(data.as_slice());
        assert_eq!(reader.read_pack_header().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn pack_header_wrong_magic() {
        let mut data = [0u8; 12];
        data[..4].copy_from_slice(b"NOPE");
        data[4..8].copy_from_slice(&2u32.to_be_bytes());
        let mut reader = PackReader::new(data.as_slice());
        reader.read_pack_header().await.unwrap_err();
    }

    #[tokio::test]
    async fn pack_header_wrong_version() {
        let mut data = [0u8; 12];
        data[..4].copy_from_slice(b"PACK");
        data[4..8].copy_from_slice(&3u32.to_be_bytes());
        let mut reader = PackReader::new(data.as_slice());
        reader.read_pack_header().await.unwrap_err();
    }

    #[tokio::test]
    async fn pack_header_too_short() {
        let mut reader = PackReader::new([0u8; 8].as_slice());
        reader.read_pack_header().await.unwrap_err();
    }

    // ── entry header ── roundtrip coverage lives in the property test below;
    // these stay as fixed examples since they test rejection of malformed
    // input, not the encode/decode shape.

    #[tokio::test]
    async fn entry_header_unknown_type() {
        let header = make_entry_header(5, 10);
        let mut reader = PackReader::new(header.as_slice());
        reader.read_entry_header().await.unwrap_err();
    }

    #[tokio::test]
    async fn entry_header_type_zero_is_invalid() {
        // Type 0 is reserved.
        let mut reader = PackReader::new([0x00u8].as_slice());
        reader.read_entry_header().await.unwrap_err();
    }

    #[tokio::test]
    async fn entry_header_truncated_empty() {
        let mut reader = PackReader::new([].as_slice());
        reader.read_entry_header().await.unwrap_err();
    }

    #[tokio::test]
    async fn entry_header_truncated_mid_continuation() {
        // 0xB0 = blob (type 3) with MSB set, so a continuation byte is expected but none follows.
        let mut reader = PackReader::new([0xB0].as_slice());
        reader.read_entry_header().await.unwrap_err();
    }

    #[tokio::test]
    async fn entry_header_truncated_ref_delta_sha() {
        // REF_DELTA requires 20 SHA bytes after the size — only 10 supplied.
        let mut header = make_entry_header(7, 10);
        header.extend_from_slice(&[0xab; 10]);
        let mut reader = PackReader::new(header.as_slice());
        reader.read_entry_header().await.unwrap_err();
    }

    // ── entry header: property test ── encodes via gix_pack's own
    // `Header::write_to` (what `super::writer` uses) rather than a
    // hand-rolled varint encoder, so encoder and decoder can't silently
    // share a wrong mental model of the format.

    fn any_pack_header_and_size() -> impl proptest::strategy::Strategy<Value = (PackHeader, u64)> {
        use proptest::prelude::*;
        let header = prop_oneof![
            Just(PackHeader::Commit),
            Just(PackHeader::Tree),
            Just(PackHeader::Blob),
            Just(PackHeader::Tag),
            (0u64..(1u64 << 40)).prop_map(|base_distance| PackHeader::OfsDelta { base_distance }),
            proptest::collection::vec(any::<u8>(), 20).prop_map(|v| PackHeader::RefDelta {
                base_id: ObjectId::from_bytes_or_panic(&v),
            }),
        ];
        (header, 0u64..(1u64 << 40))
    }

    proptest::proptest! {
        #[test]
        fn entry_header_roundtrips_for_any_shape_and_size(
            (header, size) in any_pack_header_and_size(),
        ) {
            let mut bytes = Vec::new();
            header.write_to(size, &mut bytes).unwrap();
            let expected = match header {
                PackHeader::Commit => EntryHeader::Full(Kind::Commit),
                PackHeader::Tree => EntryHeader::Full(Kind::Tree),
                PackHeader::Blob => EntryHeader::Full(Kind::Blob),
                PackHeader::Tag => EntryHeader::Full(Kind::Tag),
                PackHeader::OfsDelta { base_distance } => {
                    EntryHeader::OfsDelta { base_distance }
                }
                PackHeader::RefDelta { base_id } => EntryHeader::RefDelta { base_id },
            };
            let mut reader = PackReader::new(bytes.as_slice());
            let (h, decoded_size) =
                futures::executor::block_on(reader.read_entry_header()).unwrap();
            proptest::prop_assert_eq!(h, expected);
            proptest::prop_assert_eq!(decoded_size, size);
        }
    }

    // ── decompression ─────────────────────────────────────────────────────────

    fn zlib_compress(data: &[u8]) -> Vec<u8> {
        use flate2::{Compression, write::ZlibEncoder};
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    // ── body capture ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn skip_entry_body_consumes_exactly_the_compressed_bytes() {
        // Larger than INFLATE_SCRATCH_BYTES so the discard path loops.
        let original: Vec<u8> = (0..200_000u32)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let compressed = zlib_compress(&original);
        let mut reader = PackReader::new(compressed.as_slice());
        reader
            .skip_entry_body(u64::try_from(original.len()).unwrap())
            .await
            .unwrap();
        assert_eq!(reader.offset, u64::try_from(compressed.len()).unwrap());
    }

    #[tokio::test]
    async fn skip_entry_body_stops_at_the_stream_boundary() {
        let original = b"abc";
        let compressed = zlib_compress(original);
        let mut data = compressed.clone();
        data.extend_from_slice(&make_entry_header(3, 6));
        let mut reader = PackReader::new(data.as_slice());
        reader
            .skip_entry_body(u64::try_from(original.len()).unwrap())
            .await
            .unwrap();
        let (h, size) = reader.read_entry_header().await.unwrap();
        assert_eq!(h, EntryHeader::Full(Kind::Blob));
        assert_eq!(size, 6);
    }

    #[tokio::test]
    async fn skip_entry_body_hashes_over_exactly_the_body() {
        let original = b"hello world, this is test data";
        let compressed = zlib_compress(original);
        let mut reader = PackReader::new(compressed.as_slice());
        reader
            .skip_entry_body(u64::try_from(original.len()).unwrap())
            .await
            .unwrap();

        assert_eq!(reader.offset, u64::try_from(compressed.len()).unwrap());
        let mut expected = sha1::Sha1::default();
        expected.update(&compressed);
        assert_eq!(
            std::mem::take(&mut reader.hasher).finalize(),
            expected.finalize()
        );
    }

    // ── retention ─────────────────────────────────────────────────────────────

    /// A whole one-blob pack, trailer and all — the shape `retaining` has to
    /// hand back byte for byte.
    fn one_blob_pack(content: &[u8]) -> Vec<u8> {
        let mut pack = Vec::new();
        pack.extend_from_slice(b"PACK");
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&1u32.to_be_bytes());
        pack.extend_from_slice(&make_entry_header(3, u64::try_from(content.len()).unwrap()));
        pack.extend_from_slice(&zlib_compress(content));
        let mut hasher = sha1::Sha1::default();
        hasher.update(&pack);
        pack.extend_from_slice(&hasher.finalize());
        pack
    }

    /// Drive a whole pack through, the way the ingest scan does.
    async fn read_whole_pack<R: AsyncBufRead + Unpin>(reader: &mut PackReader<R>) {
        assert_eq!(reader.read_pack_header().await.unwrap(), 1);
        let (_, size) = reader.read_entry_header().await.unwrap();
        reader.skip_entry_body(size).await.unwrap();
        reader.verify_trailer().await.unwrap();
    }

    #[tokio::test]
    async fn retaining_keeps_the_pack_byte_for_byte() {
        let pack = one_blob_pack(b"hello");
        let mut reader = PackReader::retaining(pack.as_slice(), pack.len());
        read_whole_pack(&mut reader).await;
        assert_eq!(&reader.take_pack()[..], &pack[..]);
    }

    /// What the ingest scan depends on: bytes appended after the trailer
    /// are not part of the pack.
    #[tokio::test]
    async fn retaining_stops_at_the_trailer() {
        let pack = one_blob_pack(b"hello");
        let mut stream = pack.clone();
        stream.extend_from_slice(b"whatever the client sent next");
        // Deliberately unhinted, so the copy also has to grow correctly.
        let mut reader = PackReader::retaining(stream.as_slice(), 0);
        read_whole_pack(&mut reader).await;
        assert_eq!(&reader.take_pack()[..], &pack[..]);
    }

    #[tokio::test]
    async fn a_plain_reader_keeps_nothing() {
        let pack = one_blob_pack(b"hello");
        let mut reader = PackReader::new(pack.as_slice());
        read_whole_pack(&mut reader).await;
        assert!(reader.take_pack().is_empty());
    }

    /// Taking the copy hands over ownership, so a second call cannot serve a
    /// stale one.
    #[tokio::test]
    async fn taking_the_pack_twice_yields_it_once() {
        let pack = one_blob_pack(b"hello");
        let mut reader = PackReader::retaining(pack.as_slice(), pack.len());
        read_whole_pack(&mut reader).await;
        assert!(!reader.take_pack().is_empty());
        assert!(reader.take_pack().is_empty());
    }

    #[tokio::test]
    async fn skip_entry_body_size_mismatch() {
        let compressed = zlib_compress(b"hello");
        let mut reader = PackReader::new(compressed.as_slice());
        reader.skip_entry_body(99).await.unwrap_err();
    }
}
