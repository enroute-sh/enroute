//! How the ledger's logs are stored in the node's log store
//! ([`crate::logdb`]): the root log as raft-rs's own messages, with its
//! prost codec, and the repository logs as the ledger's own records.

use raft::prelude::Entry;
use raft_engine::{MessageExt, ValueCodec};

use crate::proto::StoredCell;

/// Values encoded with prost (raft-rs's codec).
pub(super) struct Prost;

impl<T: raft_prost::Message + Default> ValueCodec<T> for Prost {
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> raft_engine::Result<()> {
        v.encode(buf)
            .map_err(|e| raft_engine::Error::InvalidArgument(e.to_string()))
    }

    fn decode(bytes: &[u8]) -> raft_engine::Result<T> {
        T::decode(bytes).map_err(|e| raft_engine::Error::InvalidArgument(e.to_string()))
    }
}

/// The root log's raft-rs entries, as stored.
pub(super) struct Entries;

impl MessageExt<Prost> for Entries {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}

/// The ledger's own records (`proto/log.proto`).
pub(super) struct Proto;

impl<T: prost::Message + Default> ValueCodec<T> for Proto {
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> raft_engine::Result<()> {
        v.encode(buf)
            .map_err(|e| raft_engine::Error::InvalidArgument(e.to_string()))
    }

    fn decode(bytes: &[u8]) -> raft_engine::Result<T> {
        T::decode(bytes).map_err(|e| raft_engine::Error::InvalidArgument(e.to_string()))
    }
}

/// A repository log's cells, as stored.
pub(super) struct Cells;

impl MessageExt<Proto> for Cells {
    type Entry = StoredCell;

    fn index(e: &StoredCell) -> u64 {
        e.index
    }
}
