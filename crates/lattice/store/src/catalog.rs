//! What lists the segments, and what one looks like to whoever asks.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use ulid::Ulid;

use enroute_lattice_core::{KeyRange, Placement, Residence, Tier};

use crate::Scope;

/// What a segment is called, for as long as it exists.
///
/// Its own identity rather than its first key, because two live segments can
/// start at the same key once ranges are allowed to overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(Ulid);

impl SegmentId {
    /// A name no segment has yet.
    #[must_use]
    pub fn fresh() -> Self {
        Self(Ulid::generate())
    }

    /// This name as the number a catalog column holds.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0.0
    }

    /// The segment an object's name belongs to, if it names one at all.
    ///
    /// A sweep reads names off a listing, and one that is not a segment's is
    /// somebody else's object rather than an orphan.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Ulid::from_string(name).ok().map(Self)
    }

    /// When this name was made, in milliseconds since the epoch.
    ///
    /// A ULID carries its own clock, which is what lets a sweep tell an
    /// orphan from a segment whose row has not committed yet.
    #[must_use]
    pub(crate) const fn made_at_ms(self) -> u64 {
        self.0.timestamp_ms()
    }
}

impl From<u128> for SegmentId {
    fn from(value: u128) -> Self {
        Self(Ulid(value))
    }
}

impl core::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A segment's bytes, or where to go and get them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// The bytes themselves, which came back with the listing.
    Inline(Bytes),
    /// The key to read them by.
    Bucket(Path),
}

impl Body {
    /// Which store these bytes are in.
    #[must_use]
    pub const fn residence(&self) -> Residence {
        match self {
            Self::Inline(_) => Residence::Inline,
            Self::Bucket(_) => Residence::Bucket,
        }
    }
}

/// What the catalog knows about a segment without reading its bytes.
///
/// This is the projection an advertisement pays for: enough to plan a
/// compaction and to pick what a read needs, with no tail carried along.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    /// What the segment is called.
    pub id: SegmentId,
    /// The keys it covers.
    pub range: KeyRange,
    /// Which generation of merge produced it.
    pub tier: Tier,
    /// How large its encoding is, which a bucket read does not carry.
    pub bytes: u64,
    /// Which store those bytes are in.
    pub residence: Residence,
}

impl Entry {
    /// This entry as compaction planning wants it.
    #[must_use]
    pub const fn placement(&self) -> Placement {
        Placement {
            range: self.range,
            tier: self.tier,
            bytes: self.bytes,
            residence: self.residence,
        }
    }
}

/// A segment together with its bytes, or the key to read them by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    /// What the catalog knows without the bytes.
    pub entry: Entry,
    /// The bytes, when the segment is inlined, or the key when it is not.
    pub body: Body,
}

/// A segment to record: whatever has already been written, named and sized.
///
/// A bucket segment's object is put before this is handed over, so a row can
/// never name a key that is not there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Written {
    /// What the segment is called.
    pub id: SegmentId,
    /// The keys it covers.
    pub range: KeyRange,
    /// Which generation of merge produced it.
    pub tier: Tier,
    /// Its bytes, or the key they were put at.
    pub body: Body,
    /// How large its encoding is.
    pub bytes: u64,
}

/// Whatever a catalog reported, boxed because only it knows what it is.
pub type CatalogError = Box<dyn core::error::Error + Send + Sync>;

/// Whatever lists a scope's segments and records changes to that list.
///
/// One scope is one value held as segments — for the commit index, one
/// repository. Nothing here says where the list is kept.
#[async_trait]
pub trait Catalog: core::fmt::Debug + Send + Sync {
    /// Every segment overlapping `range`, with the bytes of the inlined ones.
    ///
    /// # Errors
    /// Whatever the catalog says when the list cannot be read.
    async fn covering(&self, scope: &Scope, range: KeyRange) -> Result<Vec<Listed>, CatalogError>;

    /// Every segment in the scope, without any bytes.
    ///
    /// # Errors
    /// Whatever the catalog says when the list cannot be read.
    async fn entries(&self, scope: &Scope) -> Result<Vec<Entry>, CatalogError>;

    /// The named segments, with the bytes of the inlined ones.
    ///
    /// # Errors
    /// Whatever the catalog says when the list cannot be read.
    async fn bodies(&self, scope: &Scope, ids: &[SegmentId]) -> Result<Vec<Listed>, CatalogError>;

    /// Records one new segment.
    ///
    /// # Errors
    /// Whatever the catalog says when the row cannot be written.
    async fn insert(&self, scope: &Scope, segment: Written) -> Result<(), CatalogError>;

    /// Records `output` and drops `inputs`, both or neither.
    ///
    /// # Errors
    /// Whatever the catalog says when the rows cannot be written.
    async fn replace(
        &self,
        scope: &Scope,
        inputs: &[SegmentId],
        output: Written,
    ) -> Result<(), CatalogError>;

    /// Drops every segment row one scope lists.
    ///
    /// Keyed by the number alone, since the prefix says where bucket objects
    /// are and dropping a row is not what deletes one.
    ///
    /// # Errors
    /// Whatever the catalog says when the rows cannot be dropped.
    async fn purge(&self, scope: i64) -> Result<(), CatalogError>;
}

/// A catalog held behind a pointer, whichever kind it turns out to be.
///
/// What every holder above the substrate names, since the list's kind is a
/// deployment's choice rather than something to carry as a type parameter.
pub type CatalogRef = Arc<dyn Catalog>;
