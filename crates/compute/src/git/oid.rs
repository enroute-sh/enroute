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
    let mut buf = [0u8; 32];
    let header = object_header(kind, data.len(), &mut buf);
    match format {
        ObjectFormat::Sha1 => {
            let mut h = sha1dc::Hasher::new();
            h.update(header);
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
            h.update(header);
            h.update(data);
            Ok(Oid::sha256(h.finalize().into()))
        }
    }
}

/// The `"<kind> <len>\0"` header that git hashes ahead of an object's data.
///
/// It is written right-aligned into `buf` and passed to the hasher in one
/// update: an allocated length string and four small updates cost about as
/// much as hashing a small object itself.
fn object_header(kind: Kind, len: usize, buf: &mut [u8; 32]) -> &[u8] {
    let mut digits = [0u8; 20];
    let mut count = 0;
    let mut rest = len;
    for d in &mut digits {
        *d = b'0' | u8::try_from(rest % 10).unwrap_or_default();
        count += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let reversed = std::iter::once(0)
        .chain(digits.iter().take(count).copied())
        .chain(std::iter::once(b' '))
        .chain(kind.name().bytes().rev());
    let mut used = 0;
    for (slot, b) in buf.iter_mut().rev().zip(reversed) {
        *slot = b;
        used += 1;
    }
    buf.split_at(buf.len().saturating_sub(used)).1
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_matches_git() {
        let mut buf = [0u8; 32];
        assert_eq!(object_header(Kind::Blob, 0, &mut buf), b"blob 0\0");
        assert_eq!(
            object_header(Kind::Commit, 1234, &mut buf),
            b"commit 1234\0"
        );
        assert_eq!(
            object_header(Kind::Tree, usize::MAX, &mut buf),
            format!("tree {}\0", usize::MAX).as_bytes()
        );
    }

    #[test]
    fn empty_blob_ids_match_git() {
        assert_eq!(
            hash_object_in(ObjectFormat::Sha1, Kind::Blob, b"").to_string(),
            "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
        );
        assert_eq!(
            hash_object_in(ObjectFormat::Sha256, Kind::Blob, b"").to_string(),
            "473a0f4c3be8a93681a267e3b1e9a7dcda1185436fe141f7749120a303721813"
        );
    }
}
