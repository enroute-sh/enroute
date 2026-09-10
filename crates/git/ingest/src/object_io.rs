//! Reading objects during promotion: store/staging handles, metadata
//! prefetch, and the tree reads dotfiles and delta plans need.
//!
//! Pack membership itself is decided by [`crate::delta_plan`].

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use gix_hash::ObjectId;

use enroute_git_core::{Error, ObjectHashMap};
use enroute_git_graph::ObjectRefs;
use enroute_git_metadata::Identity;
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::staging::StagingSession;
use crate::timing::AtomicDuration;

use crate::upload::StagedEncodings;

/// Object I/O context: staging index and primary-store handles, all
/// `repo`-keyed.
pub(crate) struct ObjectIoCtx<'a> {
    pub(crate) chunk_index: &'a ObjectHashMap<StagedEncodings>,
    pub(crate) staging_session: Arc<StagingSession>,
    pub(crate) state: &'a Storage,
    pub(crate) repo: &'a RepoMetadata,
    pub(crate) counts: AttributionCounts,
}

/// Identities a caller already read, so [`append`] doesn't
/// have to read them again.
///
/// `None` means the read found nothing (plan as new); an oid absent from the
/// map is merely unknown. A stale `None` is caught by `append`'s retry.
#[derive(Debug, Default, Clone)]
pub struct KnownIdentities(ObjectHashMap<Option<Identity>>);

impl KnownIdentities {
    /// Wrap a completed read: every oid it covered, mapped to what was found
    /// for it or `None`.
    #[must_use]
    pub fn new(known: ObjectHashMap<Option<Identity>>) -> Self {
        Self(known)
    }

    /// What `oid` is, if the read found it.
    #[must_use]
    pub fn get(&self, oid: &ObjectId) -> Option<Identity> {
        self.0.get(oid).copied().flatten()
    }

    /// Whether the read covered `oid`, i.e. whether [`Self::get`] returning
    /// `None` means "absent" rather than "unknown".
    #[must_use]
    pub fn covers(&self, oid: &ObjectId) -> bool {
        self.0.contains_key(oid)
    }
}

/// Read every identity this push might need in one go, rather than one at a
/// time as `append` builds its plan.
///
/// Every kind, because one table answers for all of them, so the commits and
/// parents `append` used to ask about itself cost nothing extra here.
///
/// See [`KnownIdentities`] for what a miss in the result does and doesn't
/// license.
pub(crate) async fn prefetch_identities(
    object_refs: &ObjectHashMap<ObjectRefs>,
    state: &Storage,
    repo: &RepoMetadata,
    counts: &AttributionCounts,
) -> Result<KnownIdentities, Error> {
    let mut known = wanted(object_refs);
    if known.is_empty() {
        return Ok(KnownIdentities::default());
    }

    // Counted like the per-level reads it replaces, or `lookup_ms` would
    // report the collapse as a bigger win than it is by simply losing the
    // push's largest metadata read.
    let oids: Vec<ObjectId> = known.keys().copied().collect();
    let at = Instant::now();
    let found = state
        .rows
        .repo(repo.id)
        .identify(&oids)
        .await
        .map_err(|e| anyhow::anyhow!("prefetching identities: {e}"))?;
    counts.add(at.elapsed());

    for (oid, identity) in found {
        known.insert(oid, Some(identity));
    }
    Ok(KnownIdentities::new(known))
}

/// Every oid `append` will ask identity about, as an unanswered map.
///
/// What belongs here is whatever `plan_commits` and `plan_objects` resolve;
/// miss one and the push pays a round trip to learn it.
fn wanted(object_refs: &ObjectHashMap<ObjectRefs>) -> ObjectHashMap<Option<Identity>> {
    let mut known: ObjectHashMap<Option<Identity>> = ObjectHashMap::default();
    for (&oid, refs) in object_refs {
        match refs {
            ObjectRefs::Commit {
                root_tree, parents, ..
            } => {
                known.insert(oid, None);
                known.insert(*root_tree, None);
                // A parent outside this push is older history `append` ranks
                // against; one inside it is answered by this same map.
                known.extend(parents.iter().map(|parent| (*parent, None)));
            }
            ObjectRefs::Tree(children) => {
                known.extend(
                    children
                        .iter()
                        .filter(|c| !c.is_commit)
                        .map(|c| (c.oid, None)),
                );
            }
            ObjectRefs::Tag(_) => {
                known.insert(oid, None);
            }
            ObjectRefs::Blob => {}
        }
    }
    known
}

/// What promotion's metadata round trips cost, summed rather than spanned:
/// spanning each would arrive at the collector a push's worth of times.
#[derive(Default)]
pub(crate) struct AttributionCounts {
    spent: AtomicDuration,
    calls: AtomicU64,
}

impl AttributionCounts {
    fn add(&self, elapsed: Duration) {
        self.spent.add(elapsed);
        self.calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn elapsed(&self) -> Duration {
        self.spent.get()
    }

    pub(crate) fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
}

/// Await one metadata lookup, adding what it took to `io.counts`.
pub(crate) async fn counted<T>(io: &ObjectIoCtx<'_>, lookup: impl Future<Output = T>) -> T {
    let at = Instant::now();
    let out = lookup.await;
    io.counts.add(at.elapsed());
    out
}

#[cfg(test)]
mod tests {
    use enroute_git_graph::TreeChild;

    use super::wanted;
    use enroute_git_core::{ObjectHashMap, oid};
    use enroute_git_graph::ObjectRefs;

    fn child(byte: u8, is_commit: bool) -> TreeChild {
        TreeChild {
            oid: oid(byte),
            is_tree: false,
            is_commit,
            name: b"f".to_vec(),
        }
    }

    /// Everything `append` resolves is gathered, so an ordinary push asks
    /// identity once rather than once per planning half.
    ///
    /// The commit itself and its parents are the part that used to be
    /// missing, which cost `plan_commits` a round trip of its own.
    #[test]
    fn the_scan_covers_what_append_will_ask_about() {
        let refs: ObjectHashMap<ObjectRefs> = ObjectHashMap::from_iter([
            (
                oid(1),
                ObjectRefs::Commit {
                    root_tree: oid(2),
                    parents: vec![oid(9)],
                    committer_date: 0,
                },
            ),
            (oid(2), ObjectRefs::Tree(vec![child(3, false)])),
            (oid(3), ObjectRefs::Blob),
        ]);

        let scanned = wanted(&refs);
        for (byte, why) in [
            (1, "the commit, which plan_commits resolves"),
            (9, "its parent, which plan_commits ranks against"),
            (2, "its root tree, which build_commit needs a seq for"),
            (3, "a tree's child, which plan_objects resolves"),
        ] {
            assert!(scanned.contains_key(&oid(byte)), "{why}");
        }
    }

    // A gitlink names a commit in another repository, which this one has no
    // reason to hold and `append` never asks about.
    #[test]
    fn a_submodule_reference_is_not_asked_about() {
        let refs: ObjectHashMap<ObjectRefs> =
            ObjectHashMap::from_iter([(oid(2), ObjectRefs::Tree(vec![child(7, true)]))]);
        assert!(!wanted(&refs).contains_key(&oid(7)));
    }
}
