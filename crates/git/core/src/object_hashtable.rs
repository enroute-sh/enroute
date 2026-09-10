//! `HashMap`/`HashSet` aliases for collections keyed by `ObjectId`.
//!
//! Backed by [`gix_hashtable`], whose hasher takes the first 8 bytes of the
//! key's hash output directly instead of re-hashing through `SipHash` — a real
//! win here since an `ObjectId` is already a full-entropy digest, but unsafe
//! to use with any other key type (the hasher panics on non-`ObjectId`
//! writes). Use these aliases everywhere a map or set is keyed by `ObjectId`.

use gix_hash::ObjectId;

/// A `HashMap` keyed by `ObjectId`.
///
/// See the module docs for why.
pub type ObjectHashMap<V> = gix_hashtable::HashMap<ObjectId, V>;

/// A `HashSet` of `ObjectId`.
///
/// See the module docs for why.
pub type ObjectHashSet = gix_hashtable::HashSet<ObjectId>;

/// Builds an empty [`ObjectHashMap`] pre-sized for `capacity` entries.
///
/// Spares callers from knowing [`ObjectHashMap`] takes a non-default hasher.
#[must_use]
pub fn object_hash_map_with_capacity<V>(capacity: usize) -> ObjectHashMap<V> {
    ObjectHashMap::with_capacity_and_hasher(capacity, gix_hashtable::hash::Builder)
}
