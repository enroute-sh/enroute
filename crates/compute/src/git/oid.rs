//! Object ids, object kinds and object hashing. [`Oid`] is shared with the
//! ledger, which never hashes an object, so kinds and hashing live here.

pub(crate) use enroute_shared::oid::{ObjectFormat, Oid};

/// Object kinds. The discriminants match git's pack type codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum Kind {
    Commit = 1,
    Tree = 2,
    Blob = 3,
    Tag = 4,
}

impl Kind {
    pub const ALL: [Kind; 4] = [Kind::Commit, Kind::Tree, Kind::Blob, Kind::Tag];

    pub fn name(self) -> &'static str {
        match self {
            Kind::Commit => "commit",
            Kind::Tree => "tree",
            Kind::Blob => "blob",
            Kind::Tag => "tag",
        }
    }

    pub(crate) fn from_code(c: u8) -> Option<Kind> {
        Some(match c {
            1 => Kind::Commit,
            2 => Kind::Tree,
            3 => Kind::Blob,
            4 => Kind::Tag,
            _ => return None,
        })
    }

    /// The pack type code.
    pub(crate) fn code(self) -> u8 {
        match self {
            Kind::Commit => 1,
            Kind::Tree => 2,
            Kind::Blob => 3,
            Kind::Tag => 4,
        }
    }

    /// Index into per-kind arrays.
    pub fn idx(self) -> usize {
        match self {
            Kind::Commit => 0,
            Kind::Tree => 1,
            Kind::Blob => 2,
            Kind::Tag => 3,
        }
    }
}

/// Hash an object the way git does (`"<kind> <len>\0<data>"`), in the
/// repository's `format`. SHA-1 hashes with collision detection (sha1dc):
/// content addressing is only sound if two different objects can't share
/// an id, so objects that carry a known collision attack are rejected, as
/// git does.
pub(crate) fn try_hash_object(
    format: ObjectFormat,
    kind: Kind,
    data: &[u8],
) -> anyhow::Result<Oid> {
    let len = data.len().to_string();
    let header = [kind.name().as_bytes(), b" ", len.as_bytes(), &[0u8]];
    match format {
        ObjectFormat::Sha1 => {
            let mut h = sha1dc::Hasher::new();
            for part in header {
                h.update(part);
            }
            h.update(data);
            let d = h.finalize().map_err(|e| {
                anyhow::anyhow!(
                    "SHA-1 collision attack detected in {} object: {e}",
                    kind.name()
                )
            })?;
            Ok(Oid::sha1(d.into()))
        }
        ObjectFormat::Sha256 => {
            use sha2::Digest;
            let mut h = sha2::Sha256::new();
            for part in header {
                h.update(part);
            }
            h.update(data);
            Ok(Oid::sha256(h.finalize().into()))
        }
    }
}

/// [`try_hash_object`] of a SHA-1 repository, for tests: they hash only
/// objects that they make.
#[cfg(test)]
pub(crate) fn hash_object(kind: Kind, data: &[u8]) -> Oid {
    hash_object_in(ObjectFormat::Sha1, kind, data)
}

/// [`try_hash_object`], for tests.
#[cfg(test)]
pub(crate) fn hash_object_in(format: ObjectFormat, kind: Kind, data: &[u8]) -> Oid {
    try_hash_object(format, kind, data).expect("collision attack in locally generated object")
}
