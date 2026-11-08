//! The git object formats Enroute reads and writes: commits, trees and tag
//! targets, over gix-object.

use anyhow::{Context, Result};
use gix_object::WriteTo as _;

use super::fsck::{self, TreeCheck};
use super::oid::{Kind, ObjectFormat, Oid};

/// A commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Commit<'a> {
    pub tree: Oid,
    /// In the order the commit gives them.
    pub parents: Vec<Oid>,
    pub author: Signature<'a>,
    pub committer: Signature<'a>,
    /// Everything after the blank line that ends the headers.
    pub message: &'a [u8],
}

/// Someone a commit names: `Name <email> 1700000000 +0100`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Signature<'a> {
    /// Raw bytes: git does not require UTF-8.
    pub name: &'a [u8],
    pub email: &'a [u8],
    /// Seconds since the epoch.
    pub seconds: i64,
    /// The time zone, in seconds east of UTC.
    pub offset_seconds: i32,
}

/// The largest time zone offset git writes, `±9959`, in seconds.
const MAX_OFFSET_SECONDS: u32 = 99 * 3600 + 59 * 60;

impl<'a> Signature<'a> {
    /// What keeps git from reading the signature back as [`write_commit`]
    /// writes it, if anything does.
    pub(crate) fn unwritable(&self) -> Option<&'static str> {
        let plain = |field: &[u8]| {
            !field.is_empty() && !field.iter().any(|b| matches!(b, b'<' | b'>' | b'\n' | 0))
        };
        if !plain(self.name) {
            Some("name is empty or holds `<`, `>`, a newline or NUL")
        } else if !plain(self.email) {
            Some("email is empty or holds `<`, `>`, a newline or NUL")
        } else if self.seconds < 0 {
            Some("time is before 1970")
        } else if self.offset_seconds % 60 != 0
            || self.offset_seconds.unsigned_abs() > MAX_OFFSET_SECONDS
        {
            Some("UTC offset is not whole minutes within ±99:59")
        } else {
            None
        }
    }

    /// Read a person line as git's `split_ident_line` does: the name ends
    /// at `<` with its trailing spaces dropped, the email ends at the next
    /// `>`, and the date and time zone, after the last `>`, read as zero
    /// when missing.
    fn parse(line: &'a [u8]) -> Option<Self> {
        let open = line.iter().position(|&b| b == b'<')?;
        let name = line.get(..open)?.trim_ascii_end();
        let rest = line.get(open + 1..)?;
        let email = rest.get(..rest.iter().position(|&b| b == b'>')?)?;
        let last = line.iter().rposition(|&b| b == b'>')?;
        let when = line.get(last + 1..)?.trim_ascii_start();
        let (date, rest) = digits(when);
        let offset = match rest.trim_ascii_start().split_first() {
            Some((&sign @ (b'+' | b'-'), tz)) => digits(tz).0.and_then(|hhmm| {
                let seconds = (hhmm / 100).checked_mul(3600)? + hhmm % 100 * 60;
                let offset = i32::try_from(seconds).ok()?;
                Some(if sign == b'-' { -offset } else { offset })
            }),
            _ => None,
        };
        let (seconds, offset_seconds) = date.zip(offset).unwrap_or_default();
        Some(Signature {
            name,
            email,
            seconds,
            offset_seconds,
        })
    }
}

/// Read a commit as git does: `tree` and the `parent`s start its headers,
/// and the first `author` and `committer` lines name its people. With no
/// blank line after the headers the message is empty.
///
/// Pushed commits pass [`fsck::commit`] first; this also reads what that
/// would refuse.
///
/// # Errors
///
/// The tree is missing or a person line does not name anyone.
pub(crate) fn parse_commit(data: &[u8]) -> Result<Commit<'_>> {
    let (headers, message) = match data.windows(2).position(|w| w == b"\n\n") {
        Some(i) => (
            data.get(..=i).unwrap_or_default(),
            data.get(i + 2..).unwrap_or_default(),
        ),
        None => (data, &[][..]),
    };
    let mut lines = headers.split(|&b| b == b'\n').peekable();
    let tree = lines
        .next()
        .and_then(|l| l.strip_prefix(b"tree "))
        .and_then(Oid::from_hex_exact)
        .context("commit without a tree")?;
    let mut parents = Vec::new();
    while let Some(p) = lines.peek().and_then(|l| l.strip_prefix(b"parent ")) {
        parents.push(Oid::from_hex_exact(p).context("commit with a bad parent")?);
        lines.next();
    }
    let (mut author, mut committer) = (None, None);
    for line in lines {
        if let Some(who) = line.strip_prefix(b"author ") {
            author = author.or(Some(who));
        } else if let Some(who) = line.strip_prefix(b"committer ") {
            committer = committer.or(Some(who));
        }
    }
    Ok(Commit {
        tree,
        parents,
        author: author
            .and_then(Signature::parse)
            .context("commit without an author")?,
        committer: committer
            .and_then(Signature::parse)
            .context("commit without a committer")?,
        message,
    })
}

/// The number `s` starts with, if it fits, and what follows its digits.
fn digits(s: &[u8]) -> (Option<i64>, &[u8]) {
    let n = s.iter().take_while(|b| b.is_ascii_digit()).count();
    let (number, rest) = s.split_at(n);
    let number = std::str::from_utf8(number)
        .ok()
        .and_then(|n| n.parse().ok());
    (number, rest)
}

/// A commit as git writes it, with a final newline after the message.
///
/// # Errors
///
/// A signature is not one git can read back; see [`Signature::unwritable`].
pub(crate) fn write_commit(
    tree: Oid,
    parents: &[Oid],
    author: &Signature<'_>,
    committer: &Signature<'_>,
    message: &[u8],
) -> Result<Vec<u8>> {
    let signature = |who: &Signature<'_>| gix_actor::Signature {
        name: who.name.into(),
        email: who.email.into(),
        time: gix_date::Time::new(who.seconds, who.offset_seconds),
    };
    let mut message = message.to_vec();
    if !message.ends_with(b"\n") {
        message.push(b'\n');
    }
    let commit = gix_object::Commit {
        tree: id(tree),
        parents: parents.iter().copied().map(id).collect(),
        author: signature(author),
        committer: signature(committer),
        encoding: None,
        message: message.into(),
        extra_headers: Vec::new(),
    };
    let mut out = Vec::new();
    commit.write_to(&mut out)?;
    Ok(out)
}

/// The mode of a tree entry, which says what the entry is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EntryMode(pub u32);

impl EntryMode {
    pub(crate) const FILE: EntryMode = EntryMode(0o100_644);
    pub(crate) const EXECUTABLE: EntryMode = EntryMode(0o100_755);
    /// A symbolic link, whose blob is its target.
    pub(crate) const SYMLINK: EntryMode = EntryMode(0o120_000);
    /// A directory.
    pub(crate) const TREE: EntryMode = EntryMode(0o040_000);
    /// A commit in another repository.
    pub(crate) const GITLINK: EntryMode = EntryMode(0o160_000);
    const TYPE_BITS: u32 = 0o170_000;

    pub(crate) fn is_tree(self) -> bool {
        self.0 & Self::TYPE_BITS == Self::TREE.0
    }

    pub(crate) fn is_symlink(self) -> bool {
        self.0 & Self::TYPE_BITS == Self::SYMLINK.0
    }

    pub(crate) fn is_gitlink(self) -> bool {
        self.0 & Self::TYPE_BITS == Self::GITLINK.0
    }

    /// What an entry of this mode names in this repository; `None` for a
    /// gitlink.
    pub(crate) fn kind(self) -> Option<Kind> {
        match self.0 & Self::TYPE_BITS {
            bits if bits == Self::TREE.0 => Some(Kind::Tree),
            bits if bits == Self::GITLINK.0 => None,
            _ => Some(Kind::Blob),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TreeEntry<'a> {
    pub mode: EntryMode,
    pub name: &'a [u8],
    pub oid: Oid,
}

impl TreeEntry<'_> {
    /// What the entry names in this repository; `None` for a gitlink.
    pub(crate) fn kind(&self) -> Option<Kind> {
        self.mode.kind()
    }
}

/// The entries of a tree of a `format` repository, in the order it stores
/// them.
pub(crate) fn tree_entries(
    format: ObjectFormat,
    data: &[u8],
) -> impl Iterator<Item = Result<TreeEntry<'_>>> {
    gix_object::TreeRefIter::from_bytes(data, hash_kind(format)).map(move |entry| {
        let entry = entry?;
        Ok(TreeEntry {
            mode: EntryMode(u32::from(entry.mode.value())),
            name: entry.filename,
            oid: Oid::from_slice(format, entry.oid.as_bytes()),
        })
    })
}

/// The entries of a tree with the bytes each takes, in the order the tree
/// stores them.
pub(crate) fn tree_entries_at(
    format: ObjectFormat,
    data: &[u8],
) -> impl Iterator<Item = Result<(TreeEntry<'_>, std::ops::Range<usize>)>> {
    let mut start = 0;
    tree_entries(format, data).map(move |e| {
        let e = e?;
        // The name borrows from `data`; its id follows its NUL.
        let end = e.name.as_ptr().addr() - data.as_ptr().addr() + e.name.len() + 1 + format.len();
        let at = start..end;
        start = end;
        Ok((e, at))
    })
}

/// The most entries a `len`-byte tree of a `format` repository can hold:
/// the shortest entry is a five-digit mode, a space, a one-byte name, a NUL
/// and an id.
pub(crate) fn max_tree_entries(format: ObjectFormat, len: usize) -> usize {
    len / (format.len() + 8)
}

/// The order of two tree entries in git's tree order.
pub(crate) fn tree_order(a: &TreeEntry<'_>, b: &TreeEntry<'_>) -> std::cmp::Ordering {
    gix_object::tree::name_order(a.name, a.mode.is_tree(), b.name, b.mode.is_tree())
}

/// A tree in git's order, where a directory sorts as `name/`.
///
/// # Errors
///
/// An entry has a mode git does not know, or a name with a NUL.
pub(crate) fn write_tree<'a>(entries: impl IntoIterator<Item = TreeEntry<'a>>) -> Result<Vec<u8>> {
    let mut tree = gix_object::Tree {
        entries: entries
            .into_iter()
            .map(|entry| {
                Ok(gix_object::tree::Entry {
                    mode: entry
                        .mode
                        .0
                        .try_into()
                        .map_err(|mode| anyhow::anyhow!("tree entry mode {mode:o}"))?,
                    filename: entry.name.into(),
                    oid: id(entry.oid),
                })
            })
            .collect::<Result<_>>()?,
    };
    tree.entries.sort();
    let mut out = Vec::new();
    tree.write_to(&mut out)?;
    Ok(out)
}

/// What a tag tags, read as git does from the `object` line that starts
/// it. Pushed tags pass [`fsck::tag`] first.
///
/// # Errors
///
/// The tag does not start with an `object` line.
pub(crate) fn tag_target(data: &[u8]) -> Result<Oid> {
    data.split(|&b| b == b'\n')
        .next()
        .and_then(|l| l.strip_prefix(b"object "))
        .and_then(Oid::from_hex_exact)
        .context("tag without an object")
}

/// gix's name for `format`.
fn hash_kind(format: ObjectFormat) -> gix_hash::Kind {
    match format {
        ObjectFormat::Sha1 => gix_hash::Kind::Sha1,
        ObjectFormat::Sha256 => gix_hash::Kind::Sha256,
    }
}

fn id(oid: Oid) -> gix_hash::ObjectId {
    gix_hash::ObjectId::from_bytes_or_panic(oid.as_bytes())
}

/// Objects an object refers to (kind and id).
pub(crate) type References = Vec<(Kind, Oid)>;

/// Objects directly referenced by an object of a `format` repository, which
/// must pass git's fsck checks, and, for a commit, what it says.
pub(crate) fn references(
    format: ObjectFormat,
    kind: Kind,
    data: &[u8],
) -> Result<(References, Option<Commit<'_>>)> {
    Ok((
        match kind {
            Kind::Commit => {
                fsck::commit(format, data)?;
                let c = parse_commit(data)?;
                let mut v = vec![(Kind::Tree, c.tree)];
                v.extend(c.parents.iter().map(|&p| (Kind::Commit, p)));
                return Ok((v, Some(c)));
            }
            Kind::Tree => {
                let mut v = Vec::with_capacity(max_tree_entries(format, data.len()));
                let mut check = TreeCheck::default();
                for e in tree_entries_at(format, data) {
                    let (e, at) = e?;
                    check.entry(&e, data.get(at.start) == Some(&b'0'))?;
                    if let Some(k) = e.kind() {
                        v.push((k, e.oid));
                    }
                }
                v
            }
            Kind::Tag => {
                let (target, kind) = fsck::tag(format, data)?;
                vec![(kind, target)]
            }
            Kind::Blob => Vec::new(),
        },
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::oid::hash_object;

    const TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbe4904d";
    const PARENT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn trees_are_checked_as_git_fsck_does() {
        let tree = |entries: &[(&str, &str)]| {
            let mut t = Vec::new();
            for (mode, name) in entries {
                t.extend_from_slice(format!("{mode} {name}\0").as_bytes());
                t.extend_from_slice(&[7; 20]);
            }
            t
        };
        let refs = references(
            ObjectFormat::Sha1,
            Kind::Tree,
            &tree(&[("100644", "a"), ("40000", "b")]),
        )
        .unwrap()
        .0;
        assert_eq!(refs.len(), 2);
        for bad in [
            tree(&[("0100644", "a")]),
            tree(&[("100644", ".git")]),
            tree(&[("100644", "b"), ("100644", "a")]),
        ] {
            references(ObjectFormat::Sha1, Kind::Tree, &bad).unwrap_err();
        }
    }

    #[test]
    fn sha256_trees_hold_32_byte_ids() {
        let mut t = b"100644 a\0".to_vec();
        t.extend_from_slice(&[7; 32]);
        t.extend_from_slice(b"40000 b\0");
        t.extend_from_slice(&[8; 32]);
        let entries: Vec<_> = tree_entries(ObjectFormat::Sha256, &t)
            .collect::<Result<_>>()
            .unwrap();
        let oids: Vec<_> = entries.iter().map(|e| e.oid).collect();
        assert_eq!(oids, [Oid::sha256([7; 32]), Oid::sha256([8; 32])]);
        let refs = references(ObjectFormat::Sha256, Kind::Tree, &t).unwrap().0;
        assert_eq!(refs.len(), 2);
        // Read as SHA-1, the same bytes are not a tree.
        references(ObjectFormat::Sha1, Kind::Tree, &t).unwrap_err();
    }

    #[test]
    fn a_commit_reads_back_as_written() {
        let ada = Signature {
            name: b"Ada L",
            email: b"ada@example.com",
            seconds: 1_700_000_000,
            offset_seconds: -9000,
        };
        let tree = Oid::from_hex(TREE).unwrap();
        let parent = Oid::from_hex(PARENT).unwrap();
        let data = write_commit(tree, &[parent], &ada, &ada, b"subject\n\nbody").unwrap();
        assert!(
            data.ends_with(
                b"committer Ada L <ada@example.com> 1700000000 -0230\n\nsubject\n\nbody\n"
            )
        );
        let commit = parse_commit(&data).unwrap();
        assert_eq!((commit.tree, commit.parents), (tree, vec![parent]));
        assert_eq!((commit.author, commit.committer), (ada, ada));
        assert_eq!(commit.message, b"subject\n\nbody\n");
    }

    #[test]
    fn pushed_commits_are_checked_as_git_fsck_does() {
        let author = "author A <a> 1 +0000\n";
        let committer = "committer C <c> 5 +0130\n";
        let ok = format!("tree {TREE}\nparent {PARENT}\n{author}{committer}gpgsig x\n y\n\nm\n");
        let (refs, commit) = references(ObjectFormat::Sha1, Kind::Commit, ok.as_bytes()).unwrap();
        assert_eq!(refs.len(), 2);
        let commit = commit.unwrap();
        assert_eq!(commit.committer.seconds, 5);
        assert_eq!(commit.committer.offset_seconds, 5400);
        assert_eq!(commit.message, b"m\n");
        // No message at all is fine.
        references(
            ObjectFormat::Sha1,
            Kind::Commit,
            format!("tree {TREE}\n{author}{committer}").as_bytes(),
        )
        .unwrap();
        for bad in [
            format!("{author}tree {TREE}\n{committer}\nm\n"),
            format!("tree {TREE}\n{committer}{author}\nm\n"),
            format!("tree {TREE}\n{author}parent {PARENT}\n{committer}\nm\n"),
            format!("tree {TREE}\n{author}{author}{committer}\nm\n"),
            format!("tree {TREE}\n{committer}\nm\n"),
            format!("tree {TREE}\n{author}committer C <c> soon\n\nm\n"),
            format!("tree {TREE}\n{author}{committer}\nm\0\n"),
        ] {
            references(ObjectFormat::Sha1, Kind::Commit, bad.as_bytes()).unwrap_err();
        }
    }

    #[test]
    fn stored_commits_read_as_git_reads_them() {
        let data =
            format!("tree {TREE}\nauthor A  <a> 07 +0100\ncommitter <c@x>\nauthor B <b> 9 +0000\n");
        let commit = parse_commit(data.as_bytes()).unwrap();
        assert_eq!(
            (commit.author.name, commit.author.email),
            (&b"A"[..], &b"a"[..])
        );
        assert_eq!(
            (commit.author.seconds, commit.author.offset_seconds),
            (7, 3600)
        );
        assert_eq!(
            (commit.committer.name, commit.committer.seconds),
            (&b""[..], 0)
        );
        assert_eq!(commit.message, b"");
        let huge = format!("tree {TREE}\nauthor A <a> 1 +99999999999999999\ncommitter C <c> 1\n");
        let commit = parse_commit(huge.as_bytes()).unwrap();
        assert_eq!((commit.author.seconds, commit.committer.seconds), (0, 0));
        parse_commit(format!("tree {TREE}\nauthor A\ncommitter C <c> 1 +0000\n").as_bytes())
            .unwrap_err();
    }

    #[test]
    fn pushed_tags_are_checked_as_git_fsck_does() {
        let head = format!("object {PARENT}\ntype commit\ntag v1\n");
        let tagger = "tagger T <t> 1 +0000\n";
        for ok in [
            format!("{head}{tagger}\nm\n"),
            format!("{head}\nm\n"),
            format!("{head}{tagger}extra\n\nm\n"),
            // git's note on the extra header drops the bad tagger's error.
            format!("{head}tagger T <t>\nextra\n"),
            format!("object {PARENT}\ntype commit\ntag ..\n{tagger}"),
        ] {
            let (refs, _) = references(ObjectFormat::Sha1, Kind::Tag, ok.as_bytes()).unwrap();
            assert_eq!(refs, [(Kind::Commit, Oid::from_hex(PARENT).unwrap())]);
        }
        for bad in [
            format!("{head}tagger T <t>\n\nm\n"),
            format!("object {PARENT}\ntype thing\ntag v1\n{tagger}"),
            format!("object {PARENT}\ntag v1\n{tagger}"),
            format!("{head}{tagger}\0"),
            format!("object {PARENT}\ntype tag\ntag \n\n"),
        ] {
            references(ObjectFormat::Sha1, Kind::Tag, bad.as_bytes()).unwrap_err();
        }
        let tag = tag_target(format!("{head}tagger T <t>\n\nm\n").as_bytes()).unwrap();
        assert_eq!(tag, Oid::from_hex(PARENT).unwrap());
    }

    #[test]
    fn a_tree_sorts_a_directory_as_if_it_ended_in_a_slash() {
        let oid = hash_object(Kind::Blob, b"");
        let file = TreeEntry {
            mode: EntryMode::FILE,
            name: b"a.txt",
            oid,
        };
        let dir = TreeEntry {
            mode: EntryMode::TREE,
            name: b"a",
            oid,
        };
        let encoded = write_tree([dir, file]).unwrap();
        let read: Vec<TreeEntry<'_>> = tree_entries(ObjectFormat::Sha1, &encoded)
            .map(Result::unwrap)
            .collect();
        assert_eq!(read, [file, dir]);
    }

    #[test]
    fn modes_say_what_an_entry_is() {
        assert!(EntryMode::TREE.is_tree() && !EntryMode::TREE.is_gitlink());
        assert!(EntryMode::GITLINK.is_gitlink() && !EntryMode::GITLINK.is_tree());
        assert!(!EntryMode::FILE.is_tree() && !EntryMode::SYMLINK.is_gitlink());
        assert_eq!(EntryMode::TREE.kind(), Some(Kind::Tree));
        assert_eq!(EntryMode::GITLINK.kind(), None);
        assert_eq!(EntryMode::EXECUTABLE.kind(), Some(Kind::Blob));
    }
}
