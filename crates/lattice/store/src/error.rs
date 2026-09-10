//! What can go wrong between a segment list and a composed value.

use thiserror::Error;

use crate::catalog::SegmentId;

/// A failure reading, writing or compacting a segmented value.
#[derive(Debug, Error)]
pub enum Error {
    /// The catalog could not be reached.
    #[error("the segment catalog could not be reached")]
    Catalog(#[source] Box<dyn core::error::Error + Send + Sync>),

    /// A bucket segment could not be read or written.
    #[error("segment {id} could not be reached in the bucket")]
    Bucket {
        /// Which segment.
        id: SegmentId,
        /// What the object store said.
        #[source]
        source: object_store::Error,
    },

    /// A scope's bucket objects could not be listed.
    ///
    /// Its own variant since a listing names no segment: what failed is the
    /// question of which ones are there at all.
    #[error("a scope's bucket segments could not be listed")]
    Listing(#[source] object_store::Error),

    /// A segment's bytes are not a segment of the expected type.
    ///
    /// The index is a source of truth, so this is data loss rather than a
    /// cache miss, and it is deliberately not recoverable here.
    #[error("segment {id} did not decode")]
    Corrupt {
        /// Which segment.
        id: SegmentId,
        /// What the codec said.
        #[source]
        source: Box<dyn core::error::Error + Send + Sync>,
    },
}
