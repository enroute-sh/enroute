//! Object-index types shared by the store that persists them and the
//! layers that read and build them.
//!
//! They live here for the same reason [`crate::NewCommit`] does: the
//! storage layer and its callers both need them, without either depending
//! on the other.

use gix_hash::ObjectId;
use gix_object::Kind;
pub use ulid::Ulid;

/// Encodes a git object [`Kind`] as a small integer, matching git's own
/// object type numbering.
///
/// The single source of truth for this mapping — the commit-pack codec and
/// the Postgres `objects.kind` column both build on it.
#[must_use]
pub fn kind_to_u8(kind: Kind) -> u8 {
    match kind {
        Kind::Commit => 1,
        Kind::Tree => 2,
        Kind::Blob => 3,
        Kind::Tag => 4,
    }
}

/// Inverse of [`kind_to_u8`]; `None` for an unrecognized value.
#[must_use]
pub fn kind_from_u8(byte: u8) -> Option<Kind> {
    match byte {
        1 => Some(Kind::Commit),
        2 => Some(Kind::Tree),
        3 => Some(Kind::Blob),
        4 => Some(Kind::Tag),
        _ => None,
    }
}

/// Where a commit-pack image's bytes live: a segment object, plus the byte
/// range within it.
///
/// A segment is a plain concatenation of images, so reading pack bytes is a
/// range GET at `base_offset` plus an intra-image offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentLocation {
    /// The segment object's ULID.
    pub id: Ulid,
    /// Byte offset of the owning pack image within the segment object.
    pub base_offset: u64,
    /// Byte length of the owning pack image (header, entries, trailer).
    ///
    /// Stored rather than taken as the gap to the next image, which is only
    /// an upper bound once append-time duplicates leave dead bytes behind.
    pub image_len: u64,
}

/// Where an object sits inside its commit pack's image, as recorded at
/// write time.
///
/// Which segment holds that image isn't here: the read path recovers it
/// from the pack commit (see [`CommitPackLocation`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackImageLocation {
    /// [`ObjectId`] of the commit whose pack contains this object.
    pub pack_sha: ObjectId,
    /// Byte offset of the object's self-describing inline header within the
    /// pack image (see `enroute_git_store::commit_pack`'s module docs).
    pub offset: u64,
    /// Byte span of the inline header plus the compressed body together.
    ///
    /// Lets a single range GET fetch both in one shot, regardless of the
    /// header's own encoded width.
    pub entry_len: u64,
    /// What this entry deltas against, or `None` when it holds the object
    /// itself.
    ///
    /// Per-location: the same content can be stored in several packs
    /// against different bases.
    pub base: Option<ObjectId>,
}

/// A [`PackImageLocation`] resolved to bytes: the read-side shape, carrying
/// the segment so a reader can range-GET it.
///
/// Tags have none — their S3 key is derivable from the SHA already in hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitPackLocation {
    /// Where in its pack image the object sits.
    pub image: PackImageLocation,
    /// The segment object holding that image.
    pub segment: SegmentLocation,
}

impl CommitPackLocation {
    /// Absolute byte offset of the entry within the segment object.
    ///
    /// Every reader wants this rather than either half alone, so the
    /// arithmetic lives here instead of at each call site.
    #[must_use]
    pub fn segment_offset(&self) -> u64 {
        self.segment.base_offset + self.image.offset
    }
}

/// A set of objects, held apart by the space each kind is counted in.
///
/// One bitmap could not do, since the numberings overlap: tree 7 and blob 7
/// are two objects.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ObjectSeqs {
    /// The trees, as tree seqs.
    pub trees: roaring::RoaringTreemap,
    /// The blobs, as blob seqs.
    pub blobs: roaring::RoaringTreemap,
}

impl ObjectSeqs {
    /// Whether the set holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.trees.is_empty() && self.blobs.is_empty()
    }

    /// How many objects the set holds.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.trees.len().saturating_add(self.blobs.len())
    }

    /// Whether `seq` is in the set, asked in the space it belongs to.
    ///
    /// A tag never is: loose rather than packed, it is in no pack and in no
    /// tree.
    #[must_use]
    pub fn contains(&self, seq: ObjectSeq) -> bool {
        match seq {
            ObjectSeq::Tree(seq) => self.trees.contains(seq),
            ObjectSeq::Blob(seq) => self.blobs.contains(seq),
            ObjectSeq::Tag(_) => false,
        }
    }

    /// Takes everything `other` holds.
    pub fn absorb(&mut self, other: &Self) {
        self.trees |= &other.trees;
        self.blobs |= &other.blobs;
    }

    /// Every object in the set, trees before blobs.
    pub fn iter(&self) -> impl Iterator<Item = ObjectSeq> + '_ {
        self.trees
            .iter()
            .map(ObjectSeq::Tree)
            .chain(self.blobs.iter().map(ObjectSeq::Blob))
    }
}

/// Where an object sits in the numbering its kind is counted by.
///
/// Every kind is counted apart, so a bare seq says nothing: the variant is
/// what says which space the number is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ObjectSeq {
    /// A seq in the tree numbering.
    Tree(u64),
    /// A seq in the blob numbering.
    Blob(u64),
    /// A seq in the tag numbering, which nothing keys a segment by.
    Tag(u64),
}

impl ObjectSeq {
    /// The number, once the space it belongs to is already known.
    #[must_use]
    pub const fn seq(self) -> u64 {
        match self {
            Self::Tree(seq) | Self::Blob(seq) | Self::Tag(seq) => seq,
        }
    }

    /// What kind of object is counted by this space.
    #[must_use]
    pub const fn kind(self) -> Kind {
        match self {
            Self::Tree(_) => Kind::Tree,
            Self::Blob(_) => Kind::Blob,
            Self::Tag(_) => Kind::Tag,
        }
    }

    /// `seq` in `kind`'s space, or `None` for a commit.
    ///
    /// A commit is numbered by the graph rather than here, and the two
    /// numberings are not the same space.
    #[must_use]
    pub const fn of(kind: Kind, seq: u64) -> Option<Self> {
        match kind {
            Kind::Tree => Some(Self::Tree(seq)),
            Kind::Blob => Some(Self::Blob(seq)),
            Kind::Tag => Some(Self::Tag(seq)),
            Kind::Commit => None,
        }
    }
}

/// Git object type and storage location of a git object.
///
/// Decompressed length isn't tracked here: a reader that needs it parses it
/// out of the object's inline pack header once fetched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Git object type.
    pub kind: Kind,
    /// The object's *introducing* pack: the earliest commit pack storing it.
    ///
    /// `None` for a tag, and for an object that is numbered but not recorded.
    /// Packs beyond the first are fetched separately via `containing_packs`.
    pub location: Option<CommitPackLocation>,
    /// The object's number, in whichever space its kind is counted by.
    ///
    /// `None` for a commit, which the graph numbers in a space of its own.
    pub object_seq: Option<ObjectSeq>,
}

impl ObjectMeta {
    /// Whether the repository can actually produce this object.
    ///
    /// Identity says an oid has a number; an index entry says the bytes are
    /// placed. A tag is the one kind stored loose, so it needs no location.
    #[must_use]
    pub fn is_stored(&self) -> bool {
        self.location.is_some() || self.kind == Kind::Tag
    }
}

/// A non-commit object (tree, blob, or tag) to record in the object index.
///
/// Commits are not represented here: they carry their own self-location on
/// [`crate::NewCommit`] and are recorded in the `commits` table directly.
#[derive(Debug, Clone)]
pub struct NewObject {
    /// The object's [`ObjectId`].
    pub oid: ObjectId,
    /// Git object type — always `Tree`, `Blob`, or `Tag`.
    pub kind: Kind,
    /// Commit-pack locations for this object.
    ///
    /// Empty for tags; every `pack_sha` is a commit created in the same push.
    pub locations: Vec<PackImageLocation>,
    /// Direct child OIDs (non-gitlink tree entries), for `Kind::Tree` only.
    ///
    /// Lets `enroute-git-metadata` build the direct-children bitmap without
    /// a second parse of the tree's bytes.
    pub children: Vec<ObjectId>,
}
