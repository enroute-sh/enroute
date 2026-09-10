//! Shared foundation for the git server: the crate-wide error type, loose
//! object codec, and commit metadata/ordering.
//!
//! A dependency-free leaf crate, so `enroute-git-graph` and
//! `enroute-git-metadata` never need to depend on each other.

mod commit_meta;
mod object_hashtable;
mod object_meta;
mod refname;
mod repo_id;
mod storage_key;

pub use commit_meta::{COMMIT_PACK_HEADER_SIZE, NewCommit, topo_order};
pub use object_hashtable::{ObjectHashMap, ObjectHashSet, object_hash_map_with_capacity};
pub use object_meta::{
    CommitPackLocation, NewObject, ObjectMeta, ObjectSeq, ObjectSeqs, PackImageLocation,
    SegmentLocation, Ulid, kind_from_u8, kind_to_u8,
};
pub use refname::is_funny_refname;
pub use repo_id::RepoId;
pub use storage_key::StorageKey;

use std::io::Write as _;

use bytes::Bytes;
use flate2::{Compression, write::ZlibEncoder};
use gix_object::Kind;

/// Errors from the object codec and graph algorithms.
///
/// Carries no HTTP semantics — `enroute-git-proto` maps it into its own
/// request-facing error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Any failure (I/O, store errors) that has no meaning to a caller
    /// beyond "this failed".
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
    /// Data supplied by a client was malformed — a corrupt pack entry, a
    /// delta that overruns its base.
    ///
    /// Kept apart from [`Error::Internal`] so it can surface as a 4xx.
    #[error("invalid input: {0}")]
    Invalid(String),
    /// The repository stores no such object.
    ///
    /// A kind of its own rather than a message a caller matches on, since
    /// "no such object" is an answer and everything else here is a fault.
    #[error("no object {0}")]
    Missing(gix_hash::ObjectId),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Internal(e.into())
    }
}

/// Decompress a stored loose object and return `(kind, raw_content)`.
///
/// The content is a zero-copy slice of the decompressed buffer.
///
/// # Errors
/// Returns an error if the data is not valid zlib, or the decompressed
/// content is not a well-formed loose object header.
pub fn decode_loose(compressed: &[u8]) -> Result<(Kind, Bytes), Error> {
    use std::io::Read as _;
    let mut decoder = flate2::read::ZlibDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| anyhow::anyhow!("decompress: {e}"))?;
    let (kind, _size, header_len) = gix_object::decode::loose_header(&decompressed)
        .map_err(|e| anyhow::anyhow!("loose header: {e}"))?;
    if header_len > decompressed.len() {
        return Err(anyhow::anyhow!("loose header_len exceeds data").into());
    }
    Ok((kind, Bytes::from(decompressed).slice(header_len..)))
}

/// Computes the git object id for `(kind, raw_content)` without compressing
/// it, for when only the OID is needed.
///
/// `gix_object::compute_hash` spelled out so the hash goes through
/// [`enroute_git_hash`] instead, to use the CPU's SHA-1 instructions.
///
/// # Errors
///
/// Returns an error if the content is a SHA-1 collision attempt.
pub fn hash_loose(kind: Kind, content: &[u8]) -> Result<gix_hash::ObjectId, Error> {
    let len =
        u64::try_from(content.len()).map_err(|e| anyhow::anyhow!("content too large: {e}"))?;
    let mut hasher = enroute_git_hash::Hasher::new();
    hasher.update(&gix_object::encode::loose_header(kind, len));
    hasher.update(content);
    let digest = hasher.try_finalize().map_err(|e| anyhow::anyhow!(e))?;
    Ok(gix_hash::ObjectId::Sha1(digest))
}

/// Given `(kind, raw_content)`, produce `(ObjectId, zlib_bytes)` suitable for loose object storage.
///
/// # Errors
/// Returns an error if hashing or zlib compression fails.
pub fn encode_loose(kind: Kind, content: &[u8]) -> Result<(gix_hash::ObjectId, Vec<u8>), Error> {
    let oid = hash_loose(kind, content)?;

    let header = gix_object::encode::loose_header(
        kind,
        u64::try_from(content.len()).map_err(|e| anyhow::anyhow!("content too large: {e}"))?,
    );
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::fast());
    enc.write_all(&header)
        .map_err(|e| anyhow::anyhow!("zlib header: {e}"))?;
    enc.write_all(content)
        .map_err(|e| anyhow::anyhow!("zlib content: {e}"))?;
    let compressed = enc
        .finish()
        .map_err(|e| anyhow::anyhow!("zlib finish: {e}"))?;

    Ok((oid, compressed))
}

/// A fixture object id: twenty copies of `byte`.
///
/// One home for the id every layer's tests name an object by, so that a
/// fixture cannot mean one thing here and another thing a crate away.
#[cfg(any(test, feature = "fixtures"))]
#[must_use]
pub fn oid(byte: u8) -> gix_hash::ObjectId {
    gix_hash::ObjectId::from_bytes_or_panic(&[byte; 20])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_loose_matches_known_blob_sha() {
        // `printf 'hello\n' | git hash-object --stdin`
        let (oid, _) = encode_loose(Kind::Blob, b"hello\n").unwrap();
        assert_eq!(
            oid.to_hex().to_string(),
            "ce013625030ba8dba906f756967f9e9ca394464a"
        );
    }

    #[test]
    fn encode_loose_matches_known_empty_blob_sha() {
        // `git hash-object --stdin < /dev/null`
        let (oid, _) = encode_loose(Kind::Blob, b"").unwrap();
        assert_eq!(
            oid.to_hex().to_string(),
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
        );
    }

    #[test]
    fn decode_loose_rejects_non_zlib_bytes() {
        decode_loose(b"not zlib data").unwrap_err();
    }

    #[test]
    fn decode_loose_rejects_truncated_header() {
        // Valid zlib framing around bytes that aren't a `<kind> <size>\0` header.
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(b"garbage").unwrap();
        let compressed = enc.finish().unwrap();
        decode_loose(&compressed).unwrap_err();
    }

    fn any_kind() -> impl proptest::strategy::Strategy<Value = Kind> {
        use proptest::prelude::*;
        prop_oneof![
            Just(Kind::Commit),
            Just(Kind::Tree),
            Just(Kind::Blob),
            Just(Kind::Tag),
        ]
    }

    proptest::proptest! {
        #[test]
        fn decode_loose_roundtrips_encode_loose_for_any_kind_and_content(
            kind in any_kind(),
            content in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096),
        ) {
            let (_, compressed) = encode_loose(kind, &content).unwrap();
            let (decoded_kind, decoded_content) = decode_loose(&compressed).unwrap();
            proptest::prop_assert_eq!(decoded_kind, kind);
            proptest::prop_assert_eq!(decoded_content, content);
        }
    }
}
