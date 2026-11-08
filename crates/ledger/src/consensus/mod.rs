//! Consensus: one replicated log per repository, and the root log.
//!
//! Every repository has its own log whose state machine is that
//! repository's refs and layer manifest, on (up to) three replicas, one of
//! them its sequencer (see `repo`, and the [`shardlog`] module's model of the
//! protocol). Which repositories exist, in which incarnation, where their
//! logs are, and who the members are is the root log's state machine
//! ([`root`]), a raft-rs group (`TiKV`'s port of etcd/raft) hosted by every
//! node; its leader hands logs to new sequencers when theirs fail (see
//! `control`). All logs on a node share one log store (`logdb`) and one
//! event loop (see `engine`), so many logs cost no more fsyncs or RPCs
//! than one, and an idle log costs nothing.
//!
//! A node's id is its store id (`logdb::store_id`), and nodes find each
//! other through the node registry ([`crate::registry`]).

mod apply;
mod codec;
mod control;
mod engine;
mod repo;
pub(crate) mod root;
mod root_store;
pub mod shardlog;
mod stats;
mod transport;

pub(crate) use engine::Located;
pub use engine::{Engine, Hosted, NotSequencer, Stats};
pub use transport::Addresses;

/// The id of incarnation `epoch` of `repo`'s log: a hash, so nodes agree on
/// it without coordination (never 0).
pub(crate) fn log_id(repo: &str, epoch: u64) -> u64 {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(repo.as_bytes());
    h.update([0]);
    h.update(epoch.to_le_bytes());
    let d = h.finalize();
    // Little-endian first 8 bytes of the digest.
    d.iter()
        .take(8)
        .rev()
        .fold(0u64, |acc, b| acc << 8 | u64::from(*b))
        | 1
}
