//! Extracts the SHAs a git object directly references (its outgoing edges
//! in the commit/tree graph), without re-decoding the object later.

use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::Error;

/// One entry of a tree object: the child OID, the mode bits attribution
/// needs to decide whether to descend or skip it, and the filename.
///
/// The filename is unused by attribution, but lets policy resolution find a
/// fixed path (the `.enroute` tree) within an already-parsed tree.
#[derive(Debug, Clone)]
pub struct TreeChild {
    /// The child object's OID.
    pub oid: ObjectId,
    /// Whether the mode marks it a subtree (`040000`).
    pub is_tree: bool,
    /// Whether the mode marks it a gitlink (`160000`, a submodule reference
    /// never fetched or stored locally).
    pub is_commit: bool,
    /// The entry's filename, raw (not guaranteed valid UTF-8).
    pub name: Vec<u8>,
}

/// The directly-referenced OIDs of a git object, tagged by object type.
///
/// Replaces a raw `Vec<ObjectId>` so callers never rely on positional
/// conventions.
#[derive(Debug, Clone)]
pub enum ObjectRefs {
    /// A commit: its root tree and zero or more parent commits.
    Commit {
        /// Root tree OID.
        root_tree: ObjectId,
        /// In the order they appear in the object.
        parents: Vec<ObjectId>,
        /// When it was last applied, in seconds since the epoch.
        ///
        /// What the commit index ranks a commit by, so a walk prunes on
        /// shape rather than on how much else the repository has taken.
        committer_date: i64,
    },
    /// A tree: its entries, carrying enough mode data to walk it without
    /// re-fetching and re-parsing its bytes.
    Tree(Vec<TreeChild>),
    /// An annotated tag: the single object it points at.
    Tag(ObjectId),
    /// A blob: no outgoing references.
    Blob,
}

impl ObjectRefs {
    /// All directly-referenced OIDs, flattened into a `Vec`.
    ///
    /// Submodule gitlinks are excluded: treating their oid as a dependency
    /// would make any history carrying one permanently unfetchable.
    #[must_use]
    pub fn deps(&self) -> Vec<ObjectId> {
        match self {
            Self::Commit {
                root_tree, parents, ..
            } => std::iter::once(*root_tree)
                .chain(parents.iter().copied())
                .collect(),
            Self::Tree(children) => {
                let mut deps = Vec::with_capacity(children.len());
                deps.extend(children.iter().filter(|c| !c.is_commit).map(|c| c.oid));
                deps
            }
            Self::Tag(target) => vec![*target],
            Self::Blob => vec![],
        }
    }

    /// The entries, if this is a tree.
    #[must_use]
    pub fn into_tree_children(self) -> Option<Vec<TreeChild>> {
        match self {
            Self::Tree(children) => Some(children),
            _ => None,
        }
    }
}

/// When a commit was made, for the generation number to be corrected from.
///
/// The author date is the fallback because gix's signature parser is stricter
/// than what git accepts, and a date of zero costs a subtree its pruning.
fn commit_date(commit: &gix_object::CommitRef<'_>) -> i64 {
    let seconds = |signature: Result<gix_actor::SignatureRef<'_>, _>| {
        signature.ok().and_then(|one| one.time().ok())
    };
    seconds(commit.committer())
        .or_else(|| seconds(commit.author()))
        .map_or(0, |time| time.seconds)
}

/// Parse `content` as a git object of `kind` and return its outgoing references.
///
/// # Errors
/// Returns an error if `content` is not a well-formed object of `kind`.
pub fn object_refs(kind: Kind, content: &[u8]) -> Result<ObjectRefs, Error> {
    match kind {
        Kind::Commit => {
            let commit = gix_object::CommitRef::from_bytes(content, gix_hash::Kind::Sha1)
                .map_err(|e| anyhow::anyhow!("parse commit: {e}"))?;
            Ok(ObjectRefs::Commit {
                root_tree: commit.tree(),
                parents: commit.parents().collect(),
                committer_date: commit_date(&commit),
            })
        }
        Kind::Tree => {
            let tree = gix_object::TreeRef::from_bytes(content, gix_hash::Kind::Sha1)
                .map_err(|e| anyhow::anyhow!("parse tree: {e}"))?;
            Ok(ObjectRefs::Tree(
                tree.entries
                    .iter()
                    .map(|e| TreeChild {
                        oid: ObjectId::from(e.oid),
                        is_tree: e.mode.is_tree(),
                        is_commit: e.mode.is_commit(),
                        name: e.filename.to_vec(),
                    })
                    .collect(),
            ))
        }
        Kind::Tag => {
            let tag = gix_object::TagRef::from_bytes(content, gix_hash::Kind::Sha1)
                .map_err(|e| anyhow::anyhow!("parse tag: {e}"))?;
            let target =
                ObjectId::from_hex(tag.target).map_err(|e| anyhow::anyhow!("tag target: {e}"))?;
            Ok(ObjectRefs::Tag(target))
        }
        Kind::Blob => Ok(ObjectRefs::Blob),
    }
}

/// Who made a commit, and when their own clock said it was.
///
/// The offset is kept beside the timestamp because a time without one loses
/// which day the commit was made on where it was made.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Raw, since git does not require a name to be UTF-8.
    pub name: Vec<u8>,
    /// Raw, for the same reason as `name`.
    pub email: Vec<u8>,
    /// Seconds since the epoch.
    pub seconds: i64,
    /// The zone that clock was in, in seconds east of UTC.
    pub offset_seconds: i32,
}

/// A commit as a reader wants it, rather than as edges to walk.
///
/// Beside [`ObjectRefs`] rather than inside it because the two answer
/// different questions: a walk that packs a fetch carries no message.
#[derive(Debug, Clone)]
pub struct CommitDetails {
    /// Root tree OID.
    pub root_tree: ObjectId,
    /// In the order they appear in the object.
    pub parents: Vec<ObjectId>,
    /// Who wrote the change.
    pub author: Identity,
    /// Who last applied it, which a rebase or an amend makes another person.
    pub committer: Identity,
    /// The first line, raw like every other string git stored.
    pub summary: Vec<u8>,
    /// Everything after the first line, less the blank line between them.
    pub body: Vec<u8>,
}

/// Split a commit message into its first line and the rest.
fn split_message(message: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let end = message
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(message.len());
    let summary = message.get(..end).unwrap_or_default().to_vec();
    let rest = message.get(end + 1..).unwrap_or_default();
    // Only the one separating blank line goes. Dropping every leading newline
    // would be a guess about which of them the author meant to keep.
    let body = rest.strip_prefix(b"\n").unwrap_or(rest);
    (summary, body.to_vec())
}

/// Parse `content` as a commit and return what it says about itself.
///
/// # Errors
/// Returns an error if `content` is not a well-formed commit.
pub fn commit_details(content: &[u8]) -> Result<CommitDetails, Error> {
    let commit = gix_object::CommitRef::from_bytes(content, gix_hash::Kind::Sha1)
        .map_err(|e| anyhow::anyhow!("parse commit: {e}"))?;

    let identity = |signature: gix_actor::SignatureRef<'_>| Identity {
        name: signature.name.to_vec(),
        email: signature.email.to_vec(),
        seconds: signature
            .time()
            .map(|time| time.seconds)
            .unwrap_or_default(),
        offset_seconds: signature.time().map(|time| time.offset).unwrap_or_default(),
    };

    let (summary, body) = split_message(commit.message);
    Ok(CommitDetails {
        root_tree: commit.tree(),
        parents: commit.parents().collect(),
        author: identity(
            commit
                .author()
                .map_err(|e| anyhow::anyhow!("parse commit author: {e}"))?,
        ),
        committer: identity(
            commit
                .committer()
                .map_err(|e| anyhow::anyhow!("parse commit committer: {e}"))?,
        ),
        summary,
        body,
    })
}

#[cfg(test)]
mod tests {
    use gix_object::Kind;

    use super::{ObjectRefs, commit_details, object_refs};

    /// A commit with a body, a second parent, and a name that is not ASCII.
    fn merge() -> Vec<u8> {
        let mut object = Vec::new();
        object.extend_from_slice(b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n");
        object.extend_from_slice(b"parent 1111111111111111111111111111111111111111\n");
        object.extend_from_slice(b"parent 2222222222222222222222222222222222222222\n");
        object.extend_from_slice(
            "author Ada Løvelace <ada@example.com> 1700000000 +0200\n".as_bytes(),
        );
        object.extend_from_slice(b"committer Somebody Else <else@example.com> 1700000060 -0800\n");
        object.extend_from_slice(b"\nmerge the widget\n\nwhy it was merged\nand a second line\n");
        object
    }

    #[test]
    fn reads_what_a_commit_says_about_itself() {
        let details = commit_details(&merge()).expect("a well-formed commit parses");

        assert_eq!(details.parents.len(), 2);
        assert_eq!(details.author.name, "Ada Løvelace".as_bytes());
        assert_eq!(details.author.email, b"ada@example.com");
        assert_eq!(details.author.seconds, 1_700_000_000);
        assert_eq!(details.author.offset_seconds, 7200);
        assert_eq!(details.committer.offset_seconds, -28_800);
        assert_eq!(details.summary, b"merge the widget");
        assert_eq!(details.body, b"why it was merged\nand a second line\n");
    }

    /// The same commit, with a committer timestamp too large for `i64`.
    ///
    /// git keeps a commit date as text and takes this one; gix parses it into
    /// an integer and cannot.
    fn odd_committer() -> Vec<u8> {
        let good = b"committer Somebody Else <else@example.com> 1700000060 -0800\n";
        let bad = b"committer Somebody Else <else@example.com> 99999999999999999999 -0800\n";
        let mut object = merge();
        let at = object
            .windows(good.len())
            .position(|window| window == good)
            .expect("the fixture has a committer line");
        object.splice(at..at + good.len(), bad.iter().copied());
        object
    }

    // gix's signature parser is stricter than what git accepts, and a zero
    // here would cost the commit and every descendant its date pruning.
    #[test]
    fn an_unparsable_committer_falls_back_to_the_author() {
        let refs = object_refs(Kind::Commit, &odd_committer()).expect("the commit still parses");
        let ObjectRefs::Commit { committer_date, .. } = refs else {
            panic!("a commit parses as a commit");
        };
        assert_eq!(
            committer_date, 1_700_000_000,
            "the author's seconds, since the committer's did not parse"
        );
    }

    // The commit index ranks a commit by this, so a zero here would flatten
    // a whole repository's history into one generation.
    #[test]
    fn a_commit_carries_the_date_it_was_applied() {
        let refs = object_refs(Kind::Commit, &merge()).expect("a well-formed commit parses");
        let ObjectRefs::Commit {
            parents,
            committer_date,
            ..
        } = refs
        else {
            panic!("a commit parses as a commit");
        };
        assert_eq!(parents.len(), 2);
        assert_eq!(
            committer_date, 1_700_000_060,
            "the committer's seconds, not the author's"
        );
    }

    /// The one blank line between them goes, and nothing else does.
    #[test]
    fn a_message_of_one_line_has_no_body() {
        let mut object = Vec::new();
        object.extend_from_slice(b"tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\n");
        object.extend_from_slice(b"author A <a@example.com> 1 +0000\n");
        object.extend_from_slice(b"committer A <a@example.com> 1 +0000\n");
        object.extend_from_slice(b"\nonly a summary\n");

        let details = commit_details(&object).expect("a well-formed commit parses");
        assert!(details.parents.is_empty());
        assert_eq!(details.summary, b"only a summary");
        assert!(details.body.is_empty());
    }

    #[test]
    fn refuses_what_is_not_a_commit() {
        commit_details(b"not a commit at all").unwrap_err();
    }
}
