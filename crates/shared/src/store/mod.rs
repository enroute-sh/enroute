//! The object store: the node's handle on it ([`Objects`]), and the local
//! filesystem as one (with conditional updates).

pub mod io;
mod local;
mod objects;

pub use io::{IoClass, IoConfig, IoLimiter, Share};
pub use local::StoreFiles;
pub use objects::{ObjectVersion, Objects, Store};
