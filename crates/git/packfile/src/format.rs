use std::cell::RefCell;

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::Error;

thread_local! {
    /// One inflate state per resolve-pool thread: every object in a push comes
    /// through here, and rebuilding the state per entry is a real share of it.
    static INFLATER: RefCell<libdeflater::Decompressor> =
        RefCell::new(libdeflater::Decompressor::new());
}

/// Maximum allowed decompressed size for a single git object (100 MiB).
///
/// Both a policy limit (matching GitHub/GitLab per-file caps) and a
/// memory-safety guard, checked against the entry header before allocating.
pub const MAX_OBJECT_BYTES: u64 = 100 * 1024 * 1024;

/// Maximum number of objects a single pack may claim to contain.
///
/// Checked up front against the client-supplied header count, or a bogus
/// count (`u32::MAX`, say) could run the receive loop far past any real push.
pub const MAX_PACK_OBJECTS: usize = 10_000_000;

/// The parsed header of a single pack entry, excluding the compressed data.
///
/// Delta-ness is a type value, not a flag beside one, so only
/// [`EntryHeader::Full`] carries a kind — a delta's object takes its base's.
#[derive(Clone, Debug, PartialEq)]
pub enum EntryHeader {
    /// A complete object, with the kind any delta on it will inherit.
    Full(Kind),
    /// Deltified against an earlier entry in the same pack.
    OfsDelta {
        /// How far back that entry starts, in bytes from this one's start.
        base_distance: u64,
    },
    /// Deltified against an object by id, in this pack or already stored.
    RefDelta {
        /// The base object's id.
        base_id: ObjectId,
    },
}

/// Inflate one pack entry's body, which is a bare zlib stream.
///
/// `expected_len`, from the entry header, is a delta's instruction length,
/// not its object's — it sizes the output buffer and checks the result.
///
/// # Errors
/// Returns [`Error::Invalid`] if the bytes aren't valid zlib or don't inflate
/// to `expected_len`.
pub fn inflate_entry(body: &[u8], expected_len: u64) -> Result<Bytes, Error> {
    let capacity =
        usize::try_from(expected_len).map_err(|e| anyhow::anyhow!("entry length: {e}"))?;
    // The header's length is authoritative, so sizing the destination to it
    // exactly still catches both disagreements: too long runs out of room and
    // libdeflate rejects it, too short fails the check below.
    let mut out = vec![0u8; capacity];
    let produced = INFLATER
        .with(|d| d.borrow_mut().zlib_decompress(body, &mut out))
        .map_err(|e| Error::Invalid(format!("inflate entry: {e:?}")))?;
    if produced != capacity {
        return Err(Error::Invalid(format!(
            "entry inflated to {produced} bytes, expected {capacity}"
        )));
    }
    Ok(Bytes::from(out))
}
