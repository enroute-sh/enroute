//! Commits made on the server from a set of file changes.
//!
//! The new objects become a layer as a push's do: a small one goes inline
//! through the log, a larger one to the object store first. No hooks run,
//! because the caller is the application.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context as _, anyhow};
use bytes::Bytes;
use foldhash::{HashMap, HashMapExt, HashSet, HashSetExt};

use enroute_shared::state::{Commit, RefUpdate};

use crate::compute::Compute;
use crate::git::fsck;
use crate::git::object::{
    EntryMode, TreeEntry, references, tree_entries, write_commit, write_tree,
};
use crate::git::oid::{Kind, ObjectFormat, Oid, try_hash_object};
use crate::git::pack::{full_entry, pack_of};
use crate::git::refname;
use crate::repo::view::Snapshot;

use super::{Error, Identity, commit_tree, read_tree_data, snapshot};

/// Rounds before a commit that waives the check gives up on a branch that
/// other writers keep moving.
const COMMIT_ATTEMPTS: usize = 16;

/// What the branch must hold for a commit to land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expected {
    /// Anything, or nothing: no check.
    Any,
    /// Nothing, so the commit starts the branch.
    Absent,
    /// This commit, which becomes the parent.
    At(Oid),
}

/// What a file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileMode {
    /// A file, `100644`.
    Regular,
    /// A file with the executable bit, `100755`.
    Executable,
    /// A symbolic link whose content is its target, `120000`.
    Symlink,
}

impl From<FileMode> for EntryMode {
    fn from(mode: FileMode) -> Self {
        match mode {
            FileMode::Regular => EntryMode::FILE,
            FileMode::Executable => EntryMode::EXECUTABLE,
            FileMode::Symlink => EntryMode::SYMLINK,
        }
    }
}

/// One change to one path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PathEdit {
    /// Write a file, replacing any file at the path.
    Put {
        /// The bytes of the file, or the target of a link.
        content: Bytes,
        /// What the file is.
        mode: FileMode,
    },
    /// Remove the file at the path, if there is one.
    Delete,
}

/// A change, at a path from the root of the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PathChange {
    /// `/`-separated, with no empty, `.`, `..` or `.git` component.
    pub path: String,
    /// What happens there.
    pub edit: PathEdit,
}

/// A commit for [`create_commit`] to make.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewCommit {
    /// The full name of the branch, under `refs/heads/`.
    pub branch: String,
    /// The compare-and-swap on the branch.
    pub expected: Expected,
    /// The message, which gets a final newline if it has none.
    pub message: String,
    /// Who wrote the change.
    pub author: Identity,
    /// Who made the commit.
    pub committer: Identity,
    /// At least one, each path at most once.
    pub changes: Vec<PathChange>,
}

/// A commit that landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CreatedCommit {
    /// The new commit, where the branch now points.
    pub commit: Oid,
    /// Its root tree.
    pub tree: Oid,
    /// Where the branch pointed before, if it existed.
    pub parent: Option<Oid>,
}

/// Make `commit` in `repo` and move its branch onto it.
///
/// With [`Expected::Any`], a race with another writer makes the commit again
/// on the new tip. On return, this compute's view holds the new commit.
///
/// # Errors
///
/// [`Error::NotFound`] first, then [`Error::Invalid`], then
/// [`Error::Moved`] when the branch is not as expected.
pub(crate) async fn create_commit(
    compute: &Compute,
    repo: &str,
    commit: &NewCommit,
) -> Result<CreatedCommit, Error> {
    let mut snap = snapshot(compute, repo).await?;
    check_branch(&commit.branch)?;
    check_identity(&commit.author, "author")?;
    check_identity(&commit.committer, "committer")?;
    if commit.message.contains('\0') {
        return Err(Error::Invalid("the message holds a NUL byte".into()));
    }
    let edits = edits(&commit.changes, compute.config.max_object_bytes)?;
    for attempt in 0..COMMIT_ATTEMPTS {
        if attempt > 0 {
            snap = snapshot(compute, repo).await?;
        }
        let current = snap.meta.refs.get(&commit.branch).copied();
        let parent = match commit.expected {
            Expected::Any => current,
            Expected::Absent if current.is_none() => None,
            Expected::At(want) if current == Some(want) => current,
            Expected::Absent | Expected::At(_) => {
                return Err(Error::Moved(moved(&commit.branch, current)));
            }
        };
        let built = build(&snap, parent, &edits, commit).await?;
        let update = RefUpdate {
            name: commit.branch.clone(),
            old: current,
            new: Some(built.commit),
        };
        if publish(compute, repo, &snap, &built.objects, update).await? {
            return Ok(CreatedCommit {
                commit: built.commit,
                tree: built.tree,
                parent,
            });
        }
        if commit.expected != Expected::Any {
            return Err(Error::Moved(format!(
                "{} moved while the commit was made",
                commit.branch
            )));
        }
    }
    Err(Error::Contended("the branch"))
}

fn moved(branch: &str, current: Option<Oid>) -> String {
    match current {
        Some(oid) => format!("{branch} is at {oid}"),
        None => format!("{branch} does not exist"),
    }
}

/// Refuse a name that is not a branch git takes.
fn check_branch(branch: &str) -> Result<(), Error> {
    if refname::valid_branch(branch) {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "not a branch, which is a refname under refs/heads/: {branch:?}"
        )))
    }
}

/// Refuse an identity git would not parse back.
fn check_identity(who: &Identity, role: &str) -> Result<(), Error> {
    match who.signature().unwritable() {
        Some(what) => Err(Error::Invalid(format!("the {role}'s {what}"))),
        None => Ok(()),
    }
}

/// One checked change: its path, split.
#[derive(Debug)]
struct Edit<'a> {
    path: Vec<&'a str>,
    edit: &'a PathEdit,
}

/// Check every change, and split its path. A file over `max_object` bytes
/// is refused here, as a push would be.
fn edits(changes: &[PathChange], max_object: u64) -> Result<Vec<Edit<'_>>, Error> {
    if changes.is_empty() {
        return Err(Error::Invalid("no changes".into()));
    }
    let mut seen = HashSet::new();
    changes
        .iter()
        .map(|change| {
            let path = split_path(&change.path)?;
            if !seen.insert(change.path.as_str()) {
                return Err(Error::Invalid(format!(
                    "{:?} is changed more than once",
                    change.path
                )));
            }
            let symlink = matches!(
                change.edit,
                PathEdit::Put {
                    mode: FileMode::Symlink,
                    ..
                }
            );
            if let PathEdit::Put { content, .. } = &change.edit
                && u64::try_from(content.len()).unwrap_or(u64::MAX) > max_object
            {
                return Err(Error::Invalid(format!(
                    "{:?} is {} bytes, over the limit of {max_object} bytes",
                    change.path,
                    content.len()
                )));
            }
            // `git fsck` refuses it: git would follow it out of the tree.
            if symlink
                && path
                    .last()
                    .is_some_and(|name| fsck::is_dotgitmodules(name.as_bytes()))
            {
                return Err(Error::Invalid(
                    "a .gitmodules may not be a symbolic link".into(),
                ));
            }
            Ok(Edit {
                path,
                edit: &change.edit,
            })
        })
        .collect()
}

/// The components of a path, each one git checks out.
fn split_path(path: &str) -> Result<Vec<&str>, Error> {
    let parts: Vec<&str> = path.split('/').collect();
    let bad = |part: &&str| {
        part.is_empty()
            || *part == "."
            || *part == ".."
            || part.contains(['\0', '\\'])
            || fsck::is_dotgit(part.as_bytes())
    };
    if path.is_empty() || parts.iter().any(bad) {
        return Err(Error::Invalid(format!(
            "invalid path {path:?}: it is relative and `/`-separated, with no empty, `.`, `..` \
             or `.git` component, and no `\\` or NUL"
        )));
    }
    Ok(parts)
}

/// The objects of a commit, and the ids that matter.
#[derive(Debug)]
struct Built {
    commit: Oid,
    tree: Oid,
    /// The objects the repository does not hold yet.
    objects: Vec<(Kind, Bytes)>,
}

/// Build the commit of `edits` on `parent`.
async fn build(
    snap: &Arc<Snapshot>,
    parent: Option<Oid>,
    edits: &[Edit<'_>],
    commit: &NewCommit,
) -> Result<Built, Error> {
    let base = match parent {
        Some(parent) => Some(commit_tree(snap, parent).await?),
        None => None,
    };
    let trees = load_trees(snap, base, edits).await?;
    let mut builder = Builder {
        snap,
        trees,
        objects: Vec::new(),
        added: HashSet::new(),
    };
    let mut resolved = Vec::with_capacity(edits.len());
    for edit in edits {
        let change = match edit.edit {
            PathEdit::Put { content, mode } => Change::Put {
                mode: (*mode).into(),
                blob: builder.add(Kind::Blob, content.clone())?,
            },
            PathEdit::Delete => Change::Delete,
        };
        resolved.push((edit.path.as_slice(), change));
    }
    let tree = match builder.edit_tree(base, "", &resolved)? {
        Some(tree) => tree,
        None => builder.add(Kind::Tree, Bytes::new())?,
    };
    let text = write_commit(
        tree,
        parent.as_slice(),
        &commit.author.signature(),
        &commit.committer.signature(),
        commit.message.as_bytes(),
    )?;
    let id = builder.add(Kind::Commit, Bytes::from(text))?;
    Ok(Built {
        commit: id,
        tree,
        objects: builder.objects,
    })
}

/// The existing trees that `edits` pass through, from `root` down, read one
/// level at a time.
async fn load_trees(
    snap: &Arc<Snapshot>,
    root: Option<Oid>,
    edits: &[Edit<'_>],
) -> anyhow::Result<HashMap<Oid, Arc<Vec<u8>>>> {
    let mut trees: HashMap<Oid, Arc<Vec<u8>>> = HashMap::new();
    let mut level: HashMap<Oid, Vec<&[&str]>> = root
        .map(|root| (root, edits.iter().map(|e| e.path.as_slice()).collect()))
        .into_iter()
        .collect();
    while !level.is_empty() {
        let missing: Vec<Oid> = level
            .keys()
            .filter(|tree| !trees.contains_key(tree))
            .copied()
            .collect();
        trees.extend(read_tree_data(snap, &missing).await?);
        let mut next: HashMap<Oid, Vec<&[&str]>> = HashMap::new();
        for (tree, paths) in level {
            let data = trees.get(&tree).context("tree not read")?;
            descend(snap.format(), data, &paths, &mut next);
        }
        level = next;
    }
    Ok(trees)
}

/// Add to `next` the subtrees that `paths` pass through below the tree
/// `data`, each with the path left under it.
fn descend<'a>(
    format: ObjectFormat,
    data: &Arc<Vec<u8>>,
    paths: &[&'a [&'a str]],
    next: &mut HashMap<Oid, Vec<&'a [&'a str]>>,
) {
    for &path in paths {
        let Some((first, rest)) = path.split_first() else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        let sub = tree_entries(format, data)
            .filter_map(Result::ok)
            .find(|e| e.name == first.as_bytes() && e.kind() == Some(Kind::Tree));
        if let Some(sub) = sub {
            next.entry(sub.oid).or_default().push(rest);
        }
    }
}

/// A change with its blob stored.
#[derive(Debug, Clone, Copy)]
enum Change {
    Put { mode: EntryMode, blob: Oid },
    Delete,
}

/// The objects of one commit, as they are made.
struct Builder<'a> {
    snap: &'a Snapshot,
    /// The existing trees the edits pass through.
    trees: HashMap<Oid, Arc<Vec<u8>>>,
    objects: Vec<(Kind, Bytes)>,
    added: HashSet<Oid>,
}

impl Builder<'_> {
    /// Check an object as a push checks it, hash it, and keep it if the
    /// repository does not hold it.
    fn add(&mut self, kind: Kind, data: Bytes) -> Result<Oid, Error> {
        let format = self.snap.format();
        references(format, kind, &data)
            .map_err(|e| Error::Invalid(format!("git would refuse the {}: {e:#}", kind.name())))?;
        let oid = try_hash_object(format, kind, &data)?;
        if self.snap.lookup_kind(&oid, kind).is_none() && self.added.insert(oid) {
            self.objects.push((kind, data));
        }
        Ok(oid)
    }

    /// `tree` with `changes` made, at `prefix`; `None` when nothing is left.
    fn edit_tree(
        &mut self,
        tree: Option<Oid>,
        prefix: &str,
        changes: &[(&[&str], Change)],
    ) -> Result<Option<Oid>, Error> {
        let mut entries: BTreeMap<Vec<u8>, (EntryMode, Oid)> = BTreeMap::new();
        if let Some(tree) = tree {
            let data = self.trees.get(&tree).context("tree not loaded")?;
            for entry in tree_entries(self.snap.format(), data) {
                let entry = entry?;
                entries.insert(entry.name.to_vec(), (entry.mode, entry.oid));
            }
        }
        let mut below: BTreeMap<&str, Vec<(&[&str], Change)>> = BTreeMap::new();
        for (path, change) in changes {
            let Some((name, rest)) = path.split_first() else {
                continue;
            };
            let at = format!("{prefix}{name}");
            if !rest.is_empty() {
                below.entry(name).or_default().push((rest, *change));
                continue;
            }
            if entries
                .get(name.as_bytes())
                .is_some_and(|(mode, _)| mode.is_tree())
            {
                return Err(Error::Invalid(format!("{at} is a directory")));
            }
            match change {
                Change::Put { mode, blob } => {
                    entries.insert(name.as_bytes().to_vec(), (*mode, *blob));
                }
                Change::Delete => {
                    entries.remove(name.as_bytes());
                }
            }
        }
        for (name, changes) in below {
            let at = format!("{prefix}{name}/");
            let existing = match entries.get(name.as_bytes()) {
                Some((mode, oid)) if mode.is_tree() => Some(*oid),
                // Deletes under a file find nothing to delete.
                Some(_) if changes.iter().all(|(_, c)| matches!(c, Change::Delete)) => continue,
                Some(_) => {
                    return Err(Error::Invalid(format!(
                        "{} is a file",
                        at.trim_end_matches('/')
                    )));
                }
                None => None,
            };
            match self.edit_tree(existing, &at, &changes)? {
                Some(oid) => entries.insert(name.as_bytes().to_vec(), (EntryMode::TREE, oid)),
                None => entries.remove(name.as_bytes()),
            };
        }
        if entries.is_empty() {
            return Ok(None);
        }
        let tree = write_tree(entries.iter().map(|(name, (mode, oid))| TreeEntry {
            mode: *mode,
            name,
            oid: *oid,
        }))?;
        Ok(Some(self.add(Kind::Tree, Bytes::from(tree))?))
    }
}

/// Publish `objects` as a layer and move the branch, in one command.
///
/// Whether the branch moved: `false` when another writer moved it first.
async fn publish(
    compute: &Compute,
    repo: &str,
    snap: &Arc<Snapshot>,
    objects: &[(Kind, Bytes)],
    update: RefUpdate,
) -> anyhow::Result<bool> {
    // None, when an identical commit is stored already: only the branch moves.
    let layer = if objects.is_empty() {
        None
    } else {
        let entries: Vec<Vec<u8>> = objects
            .iter()
            .map(|(kind, data)| full_entry(*kind, data))
            .collect();
        let pack: Arc<dyn crate::git::io::ReadAt> =
            Arc::new(crate::git::io::Owned(pack_of(snap.format(), &entries)));
        let ws = compute.storage.workspace();
        let ingested = crate::repo::ingest::ingest_stored(
            snap.clone(),
            pack,
            ws,
            Vec::new(),
            None,
            compute.config.max_object_bytes,
        )
        .await?;
        ingested.layer
    };
    let commit = Commit {
        updates: vec![update],
        atomic: true,
        ..Default::default()
    };
    let out = compute
        .commit(repo, snap.meta.epoch, layer, commit)
        .await?
        .outcome;
    if let Some(error) = out.error.as_deref() {
        return Err(anyhow!("{repo}: {error}"));
    }
    Ok(out.ref_errors().all(|error| error.is_none()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity(name: &str, email: &str, seconds: i64, offset_seconds: i32) -> Identity {
        Identity {
            name: name.into(),
            email: email.into(),
            seconds,
            offset_seconds,
        }
    }

    #[test]
    fn a_path_is_what_git_checks_out() {
        assert_eq!(split_path("src/lib.rs").unwrap(), ["src", "lib.rs"]);
        assert_eq!(split_path(".github/x").unwrap(), [".github", "x"]);
        for bad in [
            "",
            "/etc/passwd",
            "a/",
            "a//b",
            ".",
            "a/./b",
            "../a",
            "a/..",
            ".git/config",
            "a/.GIT/b",
            ".git. ",
            "GIT~1/x",
            ".g\u{200c}it",
            "a\\b",
            "a\0b",
        ] {
            split_path(bad).unwrap_err();
        }
    }

    #[test]
    fn identities_are_what_git_parses_back() {
        let who = identity("Ada L", "ada@example.com", 1_700_000_000, -9000);
        check_identity(&who, "author").unwrap();
        for bad in [
            identity("", "a@b", 0, 0),
            identity("a", "", 0, 0),
            identity("a <b>", "a@b", 0, 0),
            identity("a", "a@b\n", 0, 0),
            identity("a", "a@b", -1, 0),
            identity("a", "a@b", 0, 30),
            identity("a", "a@b", 0, 100 * 3600),
        ] {
            check_identity(&bad, "author").unwrap_err();
        }
    }

    #[test]
    fn a_path_is_changed_once_and_gitmodules_is_never_a_link() {
        let put = |path: &str, mode| PathChange {
            path: path.into(),
            edit: PathEdit::Put {
                content: Bytes::from_static(b"x"),
                mode,
            },
        };
        edits(&[], u64::MAX).unwrap_err();
        edits(
            &[put("a", FileMode::Regular), put("a", FileMode::Executable)],
            u64::MAX,
        )
        .unwrap_err();
        edits(&[put("x/.gitmodules", FileMode::Symlink)], u64::MAX).unwrap_err();
        edits(&[put("x/GITMOD~1", FileMode::Symlink)], u64::MAX).unwrap_err();
        edits(&[put(".gitmodules", FileMode::Regular)], u64::MAX).unwrap();
        edits(&[put("a", FileMode::Regular)], 1).unwrap();
        edits(&[put("a", FileMode::Regular)], 0).unwrap_err();
    }
}
