//! Commit-pack file format: constants, header codec, per-object entry codec,
//! and trailer codec.
//!
//! `[12B header][entry 1]...[entry N][trailer]`. Header: magic + version +
//! object count, letting a Postgres-free reader compute the trailer's exact
//! byte span (see [`trailer_suffix_len`]) for one range fetch. Entry:
//! `[header_length][sha][kind][base sha if delta][length][compressed_len]
//! [compressed body]`, self-delimiting and resync-safe. Trailer: one entry
//! per object (`sha/kind/length/offset/entry_len`), a write-pass by-product the
//! read path never touches — see the per-item docs below for field details.

use anyhow::{Context as _, Result};
use bytes::{Buf, BufMut, Bytes};
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_packfile::write_varint;

/// Magic bytes identifying a commit-pack file.
const COMMIT_PACK_MAGIC: [u8; 4] = *b"CPAK";
/// Current commit-pack format version.
const COMMIT_PACK_VERSION: u32 = 1;
/// Byte length of the commit-pack file header (`usize` mirror of
/// [`enroute_git_core::COMMIT_PACK_HEADER_SIZE`]'s `u64`).
///
/// Public so other crates can size a read buffer without the literal.
pub const COMMIT_PACK_HEADER_USIZE: usize = 12;

/// Encode the 12-byte commit-pack file header: magic, version, object count.
#[must_use]
pub fn encode_commit_pack_header(object_count: u32) -> [u8; COMMIT_PACK_HEADER_USIZE] {
    let mut h = [0u8; COMMIT_PACK_HEADER_USIZE];
    let mut buf: &mut [u8] = &mut h;
    buf.put_slice(&COMMIT_PACK_MAGIC);
    buf.put_u32(COMMIT_PACK_VERSION);
    buf.put_u32(object_count);
    h
}

/// A commit-pack file header, decoded and validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitPackHeader {
    /// Total number of self-describing entries in the pack (commit + trees +
    /// blobs) — see [`trailer_suffix_len`].
    pub object_count: u32,
}

/// Decode and validate a commit-pack file header at the start of `raw`.
///
/// # Errors
/// Returns an error if `raw` is too short for a header, or the magic/version
/// don't match what this build of the codec produces.
pub fn decode_commit_pack_header(mut raw: &[u8]) -> Result<CommitPackHeader> {
    let mut magic = [0u8; 4];
    raw.try_copy_to_slice(&mut magic)
        .map_err(|e| anyhow::anyhow!("commit pack too short for header: {e}"))?;
    if magic != COMMIT_PACK_MAGIC {
        anyhow::bail!("commit pack has invalid magic bytes {magic:?}");
    }
    let version = raw
        .try_get_u32()
        .map_err(|e| anyhow::anyhow!("commit pack too short for header: {e}"))?;
    if version != COMMIT_PACK_VERSION {
        anyhow::bail!("commit pack has unsupported version {version}");
    }
    let object_count = raw
        .try_get_u32()
        .map_err(|e| anyhow::anyhow!("commit pack too short for header: {e}"))?;
    Ok(CommitPackHeader { object_count })
}

/// Byte length of one serialised trailer entry.
const TRAILER_ENTRY_LEN: usize = 45;

/// Exact byte span of a commit pack's trailer, given its `object_count`.
///
/// Trailer entries are fixed-width, so a reader can fetch the whole trailer
/// in one `GetRange::Suffix` request with no entry walk.
#[must_use]
pub fn trailer_suffix_len(object_count: u32) -> u64 {
    let entry_len = u64::try_from(TRAILER_ENTRY_LEN).unwrap_or(u64::MAX);
    4 + u64::from(object_count) * entry_len
}

/// Compression level for commit-pack objects, on libdeflate's 1-12 scale.
///
/// Measured over a react push: 6 beats zlib level 5 on both instructions and
/// bytes; higher levels cost far more for little ratio gain.
const DEFLATE_LEVEL: i32 = 6;

/// [`DEFLATE_LEVEL`] as libdeflate wants it.
///
/// The level is validated at runtime because it is usually user input; here it
/// is a constant in range by inspection, so the fallback is unreachable.
fn deflate_level() -> libdeflater::CompressionLvl {
    libdeflater::CompressionLvl::new(DEFLATE_LEVEL)
        .unwrap_or_else(|_| libdeflater::CompressionLvl::default())
}

/// A reusable zlib compressor for commit-pack objects.
///
/// Compresses in one shot, which does materially less work than the wire
/// path's genuinely-streaming `flate2`. Reused per worker, not per object.
pub struct ObjectDeflater {
    compressor: libdeflater::Compressor,
    /// Sized before the compressed length is known, grown across calls, and
    /// capped by [`MAX_RETAINED_SCRATCH`].
    scratch: Vec<u8>,
}

/// Ceiling on the retained `scratch`; objects past it get a buffer allocated
/// for them and dropped with them.
///
/// The deflater is a thread-local on a process-lifetime pool, so an
/// uncapped scratch would pin one object's size on each thread forever.
const MAX_RETAINED_SCRATCH: usize = 1024 * 1024;

// The compressor is opaque and the scratch holds whatever the last object left,
// so size is the only thing worth printing.
impl std::fmt::Debug for ObjectDeflater {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectDeflater")
            .field(
                "compressor",
                &format_args!("libdeflate level {DEFLATE_LEVEL}"),
            )
            .field("scratch", &self.scratch.len())
            .finish()
    }
}

impl Default for ObjectDeflater {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectDeflater {
    /// A compressor holding its own deflate state.
    #[must_use]
    pub fn new() -> Self {
        Self {
            compressor: libdeflater::Compressor::new(deflate_level()),
            scratch: Vec::new(),
        }
    }

    /// Append `zlib(content)` to `out`, returning how many bytes it added.
    ///
    /// Compresses into a scratch and copies across, since `out` is later
    /// wrapped in `Bytes` and would adopt an oversized allocation.
    ///
    /// # Errors
    /// Returns an error if zlib compression fails.
    pub fn deflate_into(&mut self, content: &[u8], out: &mut Vec<u8>) -> Result<usize> {
        // libdeflate guarantees it never exceeds the bound, so a destination
        // this size makes "did it fit" unaskable.
        let bound = self.compressor.zlib_compress_bound(content.len());
        let mut oversized = Vec::new();
        let into = if bound <= MAX_RETAINED_SCRATCH {
            if self.scratch.len() < bound {
                self.scratch.resize(bound, 0);
            }
            self.scratch
                .get_mut(..bound)
                .ok_or_else(|| anyhow::anyhow!("deflate scratch shorter than its own bound"))?
        } else {
            oversized.resize(bound, 0);
            &mut oversized[..]
        };
        let written = self
            .compressor
            .zlib_compress(content, into)
            .map_err(|e| anyhow::anyhow!("commit pack object zlib: {e}"))?;
        let produced = into
            .get(..written)
            .ok_or_else(|| anyhow::anyhow!("deflate wrote past its own bound"))?;
        out.extend_from_slice(produced);
        Ok(written)
    }
}

/// Compress `content` on its own, allocating a deflate state per call.
///
/// For fixtures only: the push path compresses through a per-worker
/// [`ObjectDeflater`] straight into its destination.
///
/// # Errors
/// Returns an error if zlib compression fails.
pub fn encode_commit_pack_object(content: &[u8]) -> Result<Bytes> {
    let mut out = Vec::new();
    ObjectDeflater::new().deflate_into(content, &mut out)?;
    Ok(Bytes::from(out))
}

/// Decompress a single commit-pack object's content.
///
/// `expected_len` comes from the entry header or object index and presizes
/// the output instead of growing it by doubling.
///
/// # Errors
/// Returns an error if the bytes are not valid zlib.
pub fn decode_commit_pack_object(compressed: &[u8], expected_len: u64) -> Result<Bytes> {
    use std::io::Read as _;

    let capacity =
        usize::try_from(expected_len).map_err(|e| anyhow::anyhow!("expected_len: {e}"))?;
    let mut dec = flate2::read::ZlibDecoder::new(compressed);
    let mut content = Vec::with_capacity(capacity);
    dec.read_to_end(&mut content)
        .map_err(|e| anyhow::anyhow!("commit pack object unzlib: {e}"))?;
    Ok(Bytes::from(content))
}

/// Encode a git object `Kind` as a single byte, for both the inline entry
/// header and the trailer.
///
/// Uses git's own pack object type ids rather than a separate scheme.
fn encode_kind(kind: Kind) -> u8 {
    enroute_git_core::kind_to_u8(kind)
}

/// Decode a kind byte back into a git object `Kind`.
fn decode_kind(byte: u8) -> Result<Kind> {
    enroute_git_core::kind_from_u8(byte)
        .ok_or_else(|| anyhow::anyhow!("commit pack has unknown object kind byte {byte}"))
}

/// [`enroute_git_packfile::read_varint`] against a cursor, advancing `buf` past the
/// bytes consumed — the shape this format's decoders read in.
///
/// # Errors
/// Returns an error if `buf` ends before a terminating (continuation-bit-
/// clear) byte, or the varint is longer than a `u64` can hold.
fn read_varint(buf: &mut &[u8]) -> Result<u64> {
    let (value, consumed) = enroute_git_packfile::read_varint(buf).context("commit pack header")?;
    buf.advance(consumed);
    Ok(value)
}

/// One packed object's self-describing inline header, decoded from the
/// pack's own bytes (see the module docs for the wire format).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackEntryHeader {
    /// The object's SHA1 identifier.
    pub sha: ObjectId,
    /// The object's git type.
    pub kind: Kind,
    /// Object this entry's body is a delta against, or `None` when the body is
    /// the object itself.
    pub base: Option<ObjectId>,
    /// Decompressed length of the stored body — the delta stream, for a delta
    /// entry.
    pub length: u64,
    /// Byte count of the compressed body that follows this header.
    pub compressed_len: u64,
}

/// Marks a kind byte as introducing a delta entry, whose base sha follows it.
///
/// Kinds are git's pack type ids (1..=4), so the high bit is unused.
const DELTA_FLAG: u8 = 0x80;

/// Encode one object's self-describing inline header.
///
/// Callers write the `compressed_len` bytes of the compressed body
/// immediately after it.
///
/// # Errors
/// Returns an error if the fields together somehow exceed 255 bytes (max is
/// 61, so this should never actually happen).
pub fn encode_pack_entry_header(
    sha: ObjectId,
    kind: Kind,
    base: Option<ObjectId>,
    length: u64,
    compressed_len: u64,
) -> Result<Bytes> {
    // One allocation sized for the worst case (62 = 1 header_length byte +
    // sha(20) + kind(1) + base(20) + two 10-byte varints), with header_length
    // reserved up front and patched once the real length is known.
    let mut buf = Vec::with_capacity(62);
    buf.put_u8(0); // placeholder, patched below
    buf.put_slice(sha.as_slice());
    match base {
        Some(base) => {
            buf.put_u8(encode_kind(kind) | DELTA_FLAG);
            buf.put_slice(base.as_slice());
        }
        None => buf.put_u8(encode_kind(kind)),
    }
    write_varint(&mut buf, length);
    write_varint(&mut buf, compressed_len);
    let body_len = buf.len().saturating_sub(1);
    let header_length = u8::try_from(body_len)
        .map_err(|e| anyhow::anyhow!("pack entry header body too large: {e}"))?;
    if let Some(first) = buf.first_mut() {
        *first = header_length;
    }
    Ok(Bytes::from(buf))
}

/// Upper bound on an entry's `1 + header_length` framing bytes — all a
/// streaming reader needs before [`decode_pack_entry_header`] can succeed.
pub const MAX_PACK_ENTRY_HEADER_BYTES: usize = 1 + 255;

/// Whether `buf` holds a whole inline entry header.
///
/// Lets a streaming reader tell "need more bytes" from a real decode failure.
#[must_use]
pub fn has_pack_entry_header(buf: &[u8]) -> bool {
    buf.split_first()
        .is_some_and(|(&header_length, rest)| rest.len() >= usize::from(header_length))
}

/// Decode one object's inline header from the start of `buf`, returning it
/// and total bytes consumed.
///
/// Callers advance by that much, then read `compressed_len` more for the body.
///
/// # Errors
/// Returns an error if `buf` is too short, the kind byte is invalid, a
/// varint is malformed/truncated, or there are leftover bytes after the
/// varints (a corrupt `header_length`).
pub fn decode_pack_entry_header(mut buf: &[u8]) -> Result<(PackEntryHeader, usize)> {
    let header_length = buf
        .try_get_u8()
        .map_err(|e| anyhow::anyhow!("pack entry header: empty buffer: {e}"))?;
    let header_length = usize::from(header_length);
    let mut body = buf
        .get(..header_length)
        .ok_or_else(|| anyhow::anyhow!("pack entry header: buffer shorter than header_length"))?;

    let sha_bytes = body
        .get(..20)
        .ok_or_else(|| anyhow::anyhow!("pack entry header: too short for sha"))?;
    let sha = ObjectId::from_bytes_or_panic(sha_bytes);
    body.advance(20);
    let kind_byte = body
        .try_get_u8()
        .map_err(|e| anyhow::anyhow!("pack entry header: too short for kind: {e}"))?;
    let kind = decode_kind(kind_byte & !DELTA_FLAG)?;
    let base = if kind_byte & DELTA_FLAG == 0 {
        None
    } else {
        let base_bytes = body
            .get(..20)
            .ok_or_else(|| anyhow::anyhow!("pack entry header: too short for base sha"))?;
        let base = ObjectId::from_bytes_or_panic(base_bytes);
        body.advance(20);
        Some(base)
    };
    let length = read_varint(&mut body)?;
    let compressed_len = read_varint(&mut body)?;
    if !body.is_empty() {
        anyhow::bail!("pack entry header: trailing bytes after varints");
    }

    Ok((
        PackEntryHeader {
            sha,
            kind,
            base,
            length,
            compressed_len,
        },
        1 + header_length,
    ))
}

/// One entry in a commit-pack trailer: a single object's SHA1, kind,
/// decompressed length, byte offset, and header+body span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackTrailerEntry {
    /// The object's SHA1 identifier.
    pub sha: ObjectId,
    /// The object's git type.
    pub kind: Kind,
    /// Decompressed content length in bytes.
    pub length: u64,
    /// Byte offset of this object's inline header (its `header_length` byte)
    /// within the pack file.
    pub offset: u64,
    /// Byte span of this object's inline header plus its compressed body
    /// together — not the compressed body alone.
    pub entry_len: u64,
}

/// Absolute byte offset within a commit pack where the blob section begins,
/// given its trailer `entries` (ordered commit → trees → blobs).
///
/// The first blob entry's offset, or the end of the last entry if the pack
/// stores no blobs.
#[must_use]
pub fn blob_section_offset(entries: &[PackTrailerEntry]) -> u64 {
    entries.iter().find(|e| e.kind == Kind::Blob).map_or_else(
        || {
            entries
                .last()
                .map_or(enroute_git_core::COMMIT_PACK_HEADER_SIZE, |last| {
                    last.offset + last.entry_len
                })
        },
        |first_blob| first_blob.offset,
    )
}

/// Serialise a slice of trailer entries.
///
/// Wire format: `[u32 count BE][{20B sha | 1B kind | u64 length BE | u64 offset BE | u64 entry_len BE} × count]`.
#[must_use]
pub fn encode_pack_trailer(entries: &[PackTrailerEntry]) -> Bytes {
    let count = u32::try_from(entries.len()).unwrap_or(u32::MAX);
    let mut buf = Vec::with_capacity(4 + entries.len() * TRAILER_ENTRY_LEN);
    buf.put_u32(count);
    for e in entries {
        buf.put_slice(e.sha.as_slice());
        buf.put_u8(encode_kind(e.kind));
        buf.put_u64(e.length);
        buf.put_u64(e.offset);
        buf.put_u64(e.entry_len);
    }
    Bytes::from(buf)
}

/// Deserialise trailer bytes produced by [`encode_pack_trailer`].
///
/// # Errors
/// Returns an error if `data` is too short, its byte count does not match
/// the entry count encoded in the header, or a kind byte is invalid.
pub fn decode_pack_trailer(mut data: &[u8]) -> Result<Vec<PackTrailerEntry>> {
    use anyhow::Context as _;
    let original_len = data.len();
    let count = data
        .try_get_u32()
        .map_err(|e| anyhow::anyhow!("trailer too short for count header: {e}"))?;
    let count = usize::try_from(count).context("trailer entry count")?;
    let expected_len = 4 + count * TRAILER_ENTRY_LEN;
    if original_len != expected_len {
        return Err(anyhow::anyhow!(
            "trailer length {original_len} != expected {expected_len}"
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let sha_bytes = data
            .get(..20)
            .ok_or_else(|| anyhow::anyhow!("trailer sha out of bounds"))?;
        let sha = ObjectId::from_bytes_or_panic(sha_bytes);
        data.advance(20);
        let kind_byte = data
            .try_get_u8()
            .map_err(|e| anyhow::anyhow!("trailer kind out of bounds: {e}"))?;
        let kind = decode_kind(kind_byte)?;
        let length = data
            .try_get_u64()
            .map_err(|e| anyhow::anyhow!("trailer length out of bounds: {e}"))?;
        let offset = data
            .try_get_u64()
            .map_err(|e| anyhow::anyhow!("trailer offset out of bounds: {e}"))?;
        let entry_len = data
            .try_get_u64()
            .map_err(|e| anyhow::anyhow!("trailer entry_len out of bounds: {e}"))?;
        entries.push(PackTrailerEntry {
            sha,
            kind,
            length,
            offset,
            entry_len,
        });
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    /// A reused deflater must produce byte-identical output to a fresh one,
    /// or resolved objects would depend on where in a push they landed.
    #[test]
    fn reuse_matches_a_fresh_compressor() {
        use super::{ObjectDeflater, encode_commit_pack_object};

        let corpus: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"short".to_vec(),
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_vec(),
            (0..100_000u32)
                .map(|i| u8::try_from(i % 251).unwrap())
                .collect(),
            (0..70_000u32).map(|_| 0u8).collect(),
        ];

        let mut reused = ObjectDeflater::new();
        for content in &corpus {
            let mut out = Vec::new();
            let n = reused.deflate_into(content, &mut out).unwrap();
            assert_eq!(n, out.len());
            assert_eq!(&out[..], &encode_commit_pack_object(content).unwrap()[..]);
        }
    }

    /// Appending several objects to one buffer must not disturb what is
    /// already there.
    #[test]
    fn deflate_into_appends_without_touching_earlier_bytes() {
        use super::{ObjectDeflater, decode_commit_pack_object};

        let mut deflater = ObjectDeflater::new();
        let mut chunk = b"PREFIX".to_vec();
        let objects: Vec<Vec<u8>> = (0..8u8)
            .map(|i| vec![i; 1000 * usize::from(i) + 1])
            .collect();

        let mut spans = Vec::new();
        for content in &objects {
            let at = chunk.len();
            let n = deflater.deflate_into(content, &mut chunk).unwrap();
            spans.push((at, n));
        }

        assert_eq!(&chunk[..6], b"PREFIX");
        for ((at, n), content) in spans.iter().zip(&objects) {
            let body = &chunk.split_at(*at).1[..*n];
            let got =
                decode_commit_pack_object(body, u64::try_from(content.len()).unwrap()).unwrap();
            assert_eq!(&got[..], &content[..]);
        }
    }

    /// The deflater sits in a thread-local on a pool that outlives every push,
    /// so an oversized object must leave nothing of its size behind.
    #[test]
    fn an_oversized_object_does_not_grow_the_retained_scratch() {
        use super::{MAX_RETAINED_SCRATCH, ObjectDeflater, decode_commit_pack_object};

        let big: Vec<u8> = (0..MAX_RETAINED_SCRATCH * 2)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let mut deflater = ObjectDeflater::new();

        let mut out = Vec::new();
        let n = deflater.deflate_into(&big, &mut out).unwrap();
        assert_eq!(n, out.len());
        assert!(
            deflater.scratch.len() <= MAX_RETAINED_SCRATCH,
            "retained {} bytes for a {}-byte object",
            deflater.scratch.len(),
            big.len()
        );
        let got = decode_commit_pack_object(&out, u64::try_from(big.len()).unwrap()).unwrap();
        assert_eq!(&got[..], &big[..]);

        // A small object after a large one still round-trips.
        let small = b"after the big one".to_vec();
        let mut out = Vec::new();
        deflater.deflate_into(&small, &mut out).unwrap();
        let got = decode_commit_pack_object(&out, u64::try_from(small.len()).unwrap()).unwrap();
        assert_eq!(&got[..], &small[..]);
    }

    use enroute_git_core::COMMIT_PACK_HEADER_SIZE;
    use gix_hash::ObjectId;
    use gix_object::Kind;
    use proptest::prelude::any;

    use super::{
        COMMIT_PACK_VERSION, PackEntryHeader, PackTrailerEntry, blob_section_offset,
        decode_commit_pack_header, decode_pack_entry_header, decode_pack_trailer,
        encode_commit_pack_header, encode_pack_entry_header, encode_pack_trailer,
        trailer_suffix_len,
    };

    fn any_kind() -> impl proptest::strategy::Strategy<Value = Kind> {
        use proptest::prelude::*;
        prop_oneof![
            Just(Kind::Commit),
            Just(Kind::Tree),
            Just(Kind::Blob),
            Just(Kind::Tag),
        ]
    }

    fn any_sha() -> impl proptest::strategy::Strategy<Value = ObjectId> {
        use proptest::prelude::*;
        proptest::collection::vec(any::<u8>(), 20).prop_map(|b| ObjectId::from_bytes_or_panic(&b))
    }

    fn any_trailer_entry() -> impl proptest::strategy::Strategy<Value = PackTrailerEntry> {
        use proptest::prelude::*;
        (
            any_sha(),
            any_kind(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_map(|(sha, kind, length, offset, entry_len)| PackTrailerEntry {
                sha,
                kind,
                length,
                offset,
                entry_len,
            })
    }

    proptest::proptest! {
        #[test]
        fn trailer_roundtrips_for_any_entries(
            entries in proptest::collection::vec(any_trailer_entry(), 0..16),
        ) {
            let encoded = encode_pack_trailer(&entries);
            let decoded = decode_pack_trailer(&encoded).unwrap();
            proptest::prop_assert_eq!(decoded, entries);
        }

        #[test]
        fn pack_entry_header_roundtrips_for_any_values(
            sha in any_sha(),
            kind in any_kind(),
            base in proptest::option::of(any_sha()),
            length in any::<u64>(),
            compressed_len in any::<u64>(),
        ) {
            let encoded = encode_pack_entry_header(sha, kind, base, length, compressed_len).unwrap();
            let (decoded, consumed) = decode_pack_entry_header(&encoded).unwrap();
            proptest::prop_assert_eq!(consumed, encoded.len());
            proptest::prop_assert_eq!(
                decoded,
                PackEntryHeader { sha, kind, base, length, compressed_len }
            );
        }

        /// A trailing byte after one entry must not be consumed —
        /// `decode_pack_entry_header` reports exactly what it occupied.
        #[test]
        fn pack_entry_header_ignores_bytes_past_its_own_length(
            sha in any_sha(),
            kind in any_kind(),
            base in proptest::option::of(any_sha()),
            length in any::<u64>(),
            compressed_len in any::<u64>(),
            trailing in proptest::collection::vec(any::<u8>(), 0..8),
        ) {
            let mut encoded = encode_pack_entry_header(sha, kind, base, length, compressed_len).unwrap().to_vec();
            let header_only_len = encoded.len();
            encoded.extend_from_slice(&trailing);
            let (decoded, consumed) = decode_pack_entry_header(&encoded).unwrap();
            proptest::prop_assert_eq!(consumed, header_only_len);
            proptest::prop_assert_eq!(
                decoded,
                PackEntryHeader { sha, kind, base, length, compressed_len }
            );
        }
    }

    #[test]
    fn decode_pack_entry_header_rejects_empty_buffer() {
        decode_pack_entry_header(&[]).unwrap_err();
    }

    #[test]
    fn decode_pack_entry_header_rejects_truncated_body() {
        let sha = ObjectId::from_bytes_or_panic(&[7u8; 20]);
        let encoded = encode_pack_entry_header(sha, Kind::Blob, None, 10, 5).unwrap();
        decode_pack_entry_header(&encoded[..encoded.len() - 1]).unwrap_err();
    }

    #[test]
    fn commit_pack_header_roundtrips() {
        let header = encode_commit_pack_header(7);
        let decoded = decode_commit_pack_header(&header).unwrap();
        assert_eq!(decoded.object_count, 7);
        assert_eq!(
            header.len(),
            usize::try_from(COMMIT_PACK_HEADER_SIZE).unwrap()
        );
    }

    proptest::proptest! {
        #[test]
        fn commit_pack_header_roundtrips_for_any_object_count(object_count in any::<u32>()) {
            let header = encode_commit_pack_header(object_count);
            let decoded = decode_commit_pack_header(&header).unwrap();
            proptest::prop_assert_eq!(decoded.object_count, object_count);
        }
    }

    #[test]
    fn decode_commit_pack_header_rejects_truncated_input() {
        let header = encode_commit_pack_header(1);
        decode_commit_pack_header(&header[..4]).unwrap_err();
    }

    #[test]
    fn decode_commit_pack_header_rejects_bad_magic() {
        let mut header = encode_commit_pack_header(1);
        header[0] = b'X';
        decode_commit_pack_header(&header).unwrap_err();
    }

    #[test]
    fn decode_commit_pack_header_rejects_bad_version() {
        let mut header = encode_commit_pack_header(1);
        header[4..8].copy_from_slice(&(COMMIT_PACK_VERSION + 1).to_be_bytes());
        decode_commit_pack_header(&header).unwrap_err();
    }

    #[test]
    fn trailer_suffix_len_is_pure_function_of_count() {
        assert_eq!(trailer_suffix_len(0), 4);
        assert_eq!(trailer_suffix_len(3), 4 + 3 * 45);
    }

    /// Trailer entries laid out as a real commit pack's are: absolute
    /// offsets accumulating from `COMMIT_PACK_HEADER_SIZE`, one run per kind.
    fn trailer(kinds_and_lens: &[(Kind, u64)]) -> Vec<PackTrailerEntry> {
        let mut offset = COMMIT_PACK_HEADER_SIZE;
        kinds_and_lens
            .iter()
            .enumerate()
            .map(|(i, &(kind, compressed_len))| {
                let header_len = 25; // arbitrary fixed stand-in for this test
                let entry_len = header_len + compressed_len;
                let entry = PackTrailerEntry {
                    sha: ObjectId::from_bytes_or_panic(&[u8::try_from(i).unwrap(); 20]),
                    kind,
                    length: compressed_len * 2,
                    offset,
                    entry_len,
                };
                offset += entry_len;
                entry
            })
            .collect()
    }

    #[test]
    fn blob_section_offset_points_at_first_blob() {
        let entries = trailer(&[
            (Kind::Commit, 10),
            (Kind::Tree, 20),
            (Kind::Blob, 5),
            (Kind::Blob, 8),
        ]);
        let expected = COMMIT_PACK_HEADER_SIZE + (25 + 10) + (25 + 20);
        assert_eq!(blob_section_offset(&entries), expected);
    }

    #[test]
    fn blob_section_offset_of_blobless_pack_is_pack_size() {
        let entries = trailer(&[(Kind::Commit, 10), (Kind::Tree, 20), (Kind::Tree, 15)]);
        let expected = COMMIT_PACK_HEADER_SIZE + (25 + 10) + (25 + 20) + (25 + 15);
        assert_eq!(blob_section_offset(&entries), expected);
    }

    #[test]
    fn blob_section_offset_of_all_blobs_is_first_body_byte() {
        let entries = trailer(&[(Kind::Blob, 5), (Kind::Blob, 8)]);
        assert_eq!(blob_section_offset(&entries), COMMIT_PACK_HEADER_SIZE);
    }

    #[test]
    fn blob_section_offset_of_empty_pack_is_header_size() {
        let entries: Vec<PackTrailerEntry> = Vec::new();
        assert_eq!(blob_section_offset(&entries), COMMIT_PACK_HEADER_SIZE);
    }
}
