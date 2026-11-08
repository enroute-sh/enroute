//! Storage: immutable files placed across memory, local disk and the
//! object store, and the access methods the engine uses on top of them.
//! Everything the engine may use is re-exported here; the modules are
//! private.

mod files;
mod handles;
mod key;
mod layer;
mod local;
mod pool;
mod spill;
mod store;
mod tiered;

pub(crate) use layer::{Layer, LayerBuilder, NewCommit, NewObject, Ref, RefTarget, TableRecord};
pub(crate) use pool::Access;
pub(crate) use spill::{BitSet, OidIndex, TempTable, Workspace};
pub(crate) use store::stored_meta;
pub use store::{Sealed, Storage, new_layer_id};
pub(crate) use tiered::{NotResident, Pinned, load_pinned};

/// A size that the test suites lower through the environment `name`, so that
/// small data spans many units. Release builds ignore it: a production node
/// must not take a size from an unchecked variable, and zero is never valid.
pub(crate) fn test_size(name: &str) -> Option<u64> {
    if !cfg!(debug_assertions) {
        return None;
    }
    std::env::var(name).ok()?.parse().ok().filter(|&n| n > 0)
}
