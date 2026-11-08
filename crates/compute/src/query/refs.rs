//! The refs that one caller sees, as every ref listing shows them.

use std::sync::Arc;

use anyhow::{Context, Result};
use foldhash::{HashMap, HashMapExt};

use crate::access::Hidden;
use crate::git::oid::{Kind, ObjectFormat, Oid};
use crate::meta::RepoState;
use crate::repo::machine::peel;
use crate::repo::view::Snapshot;

/// The refs of a repository that one caller sees, under the prefixes it
/// asked for. Advertisements and listings only render it.
#[derive(Debug, Default)]
pub(crate) struct RefView<'a> {
    /// The branch `HEAD` names, and its tip if the branch exists: `None`
    /// when the caller does not see `HEAD` or no branch is named.
    pub head: Option<(&'a str, Option<Oid>)>,
    /// Sorted by name, byte for byte.
    pub refs: Vec<(&'a str, Oid)>,
    /// What the annotated tags among `refs` point at, after
    /// [`RefView::peel`].
    pub peeled: HashMap<Oid, Oid>,
    /// How the repository names its objects.
    pub format: ObjectFormat,
}

impl<'a> RefView<'a> {
    /// The refs of `meta` that are not `hidden`, under any of `prefixes` (all
    /// of them when there are none). `HEAD` is filtered as a ref named `HEAD`.
    pub(crate) fn new(meta: &'a RepoState, hidden: &Hidden, prefixes: &[impl AsRef<str>]) -> Self {
        let sees = |name: &str| {
            (prefixes.is_empty() || prefixes.iter().any(|p| name.starts_with(p.as_ref())))
                && !hidden.hides(name)
        };
        RefView {
            head: (sees("HEAD") && !meta.head.is_empty())
                .then(|| (meta.head.as_str(), meta.refs.get(&meta.head).copied())),
            refs: meta
                .refs
                .iter()
                .filter(|(name, _)| sees(name))
                .map(|(name, oid)| (name.as_str(), *oid))
                .collect(),
            peeled: HashMap::new(),
            format: meta.format(),
        }
    }

    /// Fill [`RefView::peeled`], loading tags that are not local without
    /// waiting on them.
    ///
    /// # Errors
    ///
    /// A read failed, or a tag's target is missing.
    pub(crate) async fn peel(&mut self, snap: &Arc<Snapshot>) -> Result<()> {
        let tags: Vec<_> = self
            .refs
            .iter()
            .filter_map(|(_, oid)| snap.lookup_kind(oid, Kind::Tag))
            .collect();
        for p in peel(snap, tags).await? {
            self.peeled.insert(
                snap.oid(p.start),
                snap.oid(p.end.context("tag target missing")?),
            );
        }
        Ok(())
    }
}
