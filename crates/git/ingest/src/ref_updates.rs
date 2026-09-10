//! Classifying and pre-screening a push's already-parsed ref updates, then
//! applying whatever survives to the ref store.
//!
//! Wire-format parsing into [`RefUpdate`] happens upstream, in
//! `enroute-git-proto` — see [`crate::session::IngestSession::finalize`].

use std::collections::{HashMap, HashSet};
use std::fmt;

use gix_hash::ObjectId;

use enroute_git_core::{Error, ObjectHashMap, ObjectHashSet};
use enroute_git_graph::ObjectRefs;
use enroute_git_retrieve::Storage;
use enroute_git_retrieve::{
    RefUpdate, RefUpdateRejection, RefsMap, RepoMetadata, is_branch_refname,
};

use crate::concurrency::try_join_bounded;
use crate::connectivity::check_connectivity;

/// Why a push's ref update didn't land — either screened out before ever
/// reaching the ref store, or the ref store's own CAS rejected it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushRejection {
    /// The tip (or something reachable from it) isn't fully connected —
    /// missing from both this push's pack and the primary store.
    MissingObjects,
    /// `refname` is not one git itself would accept — see
    /// [`enroute_git_core::is_funny_refname`].
    FunnyRefname,
    /// A branch update's new value is a positively-known non-commit object.
    NonCommitObject,
    /// The repository's `pre-receive` hook refused this update, carrying
    /// the reason it gave — see `crate::hooks`.
    Policy(String),
    /// [`enroute_git_retrieve::MetadataStore::update_refs`]'s own CAS rejected it.
    RefStore(RefUpdateRejection),
    /// No result was recorded for this refname — an ingest invariant
    /// violation, not a real rejection reason.
    Internal,
}

impl fmt::Display for PushRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingObjects => f.write_str("missing-objects"),
            Self::FunnyRefname => f.write_str("funny refname"),
            Self::NonCommitObject => f.write_str("non-commit object"),
            Self::Policy(reason) => write!(f, "{reason}"),
            Self::RefStore(rejection) => write!(f, "{rejection}"),
            Self::Internal => f.write_str("internal error: no result for ref update"),
        }
    }
}

/// What applying a push's ref updates produced.
///
/// What happened, and what the application wants said about it.
#[derive(Debug, Clone, Default)]
pub struct Applied {
    /// One per update asked for, in the order they were asked for.
    pub outcomes: Vec<RefUpdateOutcome>,
    /// Lines for whoever pushed, from `post-receive`, and usually none.
    pub messages: Vec<String>,
}

/// Outcome of applying one ref update within a push, in report-status terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefUpdateOutcome {
    /// The ref that was updated or rejected.
    pub refname: String,
    /// `Ok` if the update landed, else the [`PushRejection`] it failed with.
    pub result: Result<(), PushRejection>,
}

pub(crate) fn nff_rejected_refs<'a>(
    updates: &'a [RefUpdate],
    existing_refs: &RefsMap,
    null: ObjectId,
) -> HashSet<&'a str> {
    updates
        .iter()
        .filter(|u| {
            if u.old_id == null || u.new_id == null {
                return false;
            }
            let current = existing_refs
                .get(&u.refname)
                .and_then(|s| ObjectId::from_hex(s.as_bytes()).ok())
                .unwrap_or(null);
            u.old_id != current
        })
        .map(|u| u.refname.as_str())
        .collect()
}

/// Branch updates whose new value is positively known, from this push's own
/// pack, to be a non-commit object.
///
/// Deliberately conservative: a `new_id` absent from `object_refs` is left
/// alone, so it falls through to `missing-objects` rather than a misreport.
pub(crate) fn non_commit_branch_updates(
    updates: &[RefUpdate],
    object_refs: &ObjectHashMap<ObjectRefs>,
    null: ObjectId,
) -> HashSet<String> {
    updates
        .iter()
        .filter(|u| u.new_id != null && is_branch_refname(&u.refname))
        .filter(|u| matches!(object_refs.get(&u.new_id), Some(refs) if !matches!(refs, ObjectRefs::Commit { .. })))
        .map(|u| u.refname.clone())
        .collect()
}

/// Refnames rejected purely by their shape, independent of pack contents or
/// connectivity.
///
/// Only validity. Which namespaces a repository admits is an application's
/// question, answered in `pre-receive`.
pub(crate) fn refname_rejections(
    updates: &[RefUpdate],
    null: ObjectId,
) -> HashMap<String, PushRejection> {
    updates
        .iter()
        .filter(|u| enroute_git_core::is_funny_refname(&u.refname, u.new_id == null))
        .map(|u| (u.refname.clone(), PushRejection::FunnyRefname))
        .collect()
}

/// Walk each update's new tip for connectivity to already-known objects,
/// skipping pre-screened or deleted refs.
///
/// Walks run concurrently — the dominant cost is per-walk index round
/// trips, not local work.
pub(crate) async fn check_ref_update_connectivity(
    updates: &[RefUpdate],
    pre_screened: &HashSet<&str>,
    object_refs: &ObjectHashMap<ObjectRefs>,
    state: &Storage,
    repo: &RepoMetadata,
    null: ObjectId,
) -> Result<(ObjectHashSet, HashSet<String>), Error> {
    let to_check: Vec<&RefUpdate> = updates
        .iter()
        .filter(|u| u.new_id != null && !pre_screened.contains(u.refname.as_str()))
        .collect();
    let results = try_join_bounded(
        to_check
            .iter()
            .map(|u| check_connectivity(state, repo, u.new_id, object_refs)),
    )
    .await?;

    let mut connected_commits: ObjectHashSet = ObjectHashSet::default();
    let mut connectivity_failures: HashSet<String> = HashSet::new();
    for (u, (connected, visited)) in to_check.into_iter().zip(results) {
        if connected {
            connected_commits.extend(
                visited
                    .into_iter()
                    .filter(|oid| matches!(object_refs.get(oid), Some(ObjectRefs::Commit { .. }))),
            );
        } else {
            connectivity_failures.insert(u.refname.clone());
        }
    }
    Ok((connected_commits, connectivity_failures))
}

/// Build the fixed-reason pre-rejection map for updates that never reach
/// `metadata.update_refs`.
///
/// Applied connectivity, then refname, then non-commit, so non-commit-object
/// beats funny-refname beats missing-objects when several apply.
pub(crate) fn build_pre_rejected(
    connectivity_failures: HashSet<String>,
    refname_rejected: HashMap<String, PushRejection>,
    non_commit_rejected: &HashSet<String>,
) -> HashMap<String, PushRejection> {
    let mut pre_rejected: HashMap<String, PushRejection> = connectivity_failures
        .into_iter()
        .map(|refname| (refname, PushRejection::MissingObjects))
        .collect();
    pre_rejected.extend(refname_rejected);
    for refname in non_commit_rejected {
        pre_rejected.insert(refname.clone(), PushRejection::NonCommitObject);
    }
    pre_rejected
}

/// What ingesting a push established about its ref updates, handed to
/// [`apply_ref_updates`] by whoever ingested it.
///
/// `rejected` is decided; `screened` is only *skipped*, re-checked inside
/// `update_refs`' own transaction since the ref may have moved since.
#[derive(Debug, Default, Clone)]
pub struct Ingested {
    /// Updates already refused, by refname.
    pub rejected: HashMap<String, PushRejection>,
    /// Refnames screened out of promotion as non-fast-forward, not refused.
    pub screened: Vec<String>,
}

/// Decide what lands, and land it: run the `pre-receive` hook over whatever
/// ingestion left alive, then apply the survivors to the ref store.
///
/// Called after [`IngestSession::finalize`], so objects are already stored
/// and the graph already written — a refusal here costs only that storage.
///
/// [`IngestSession::finalize`]: crate::IngestSession::finalize
///
/// # Errors
/// The hook could not be run, or the ref store update fails. A hook that
/// refuses is not an error — that is a per-ref outcome.
pub async fn apply_ref_updates(
    state: &Storage,
    repo: &RepoMetadata,
    actor: &crate::hooks::Actor,
    updates: &[RefUpdate],
    ingested: Ingested,
    hooks: &dyn crate::hooks::ReceiveHooks,
    progress: crate::progress::ProgressSink<'_>,
) -> Result<Applied, Error> {
    let null = ObjectId::null(gix_hash::Kind::Sha1);
    let Ingested {
        mut rejected,
        screened,
    } = ingested;

    // Nothing already out is put to the hook: a rejection it returned for one
    // of those would be a reason nobody could act on.
    let already_out: HashSet<&str> = screened
        .iter()
        .map(String::as_str)
        .chain(rejected.keys().map(String::as_str))
        .collect();
    let (rejected_by_hook, commands) =
        crate::hooks::pre_receive(hooks, state, repo, updates, &already_out, actor, null).await?;
    rejected.extend(rejected_by_hook);

    progress(crate::progress::IngestProgress::UpdatingReferences);
    let outcomes = resolve_ref_updates(state, repo, updates, &rejected).await?;

    // After the refs have moved, so it is told what happened rather than what
    // was asked for. Awaited rather than spawned, the way `receive-pack`
    // waits for `post-receive`: a detached task is one a shutdown loses, and
    // whatever it says still has to reach the client on this connection.
    let messages = crate::hooks::post_receive(hooks, repo.id, actor, commands, &outcomes).await;
    Ok(Applied { outcomes, messages })
}

/// Combine fixed-reason pre-rejections with the outcome of applying every
/// other update via `metadata.update_refs`.
pub(crate) async fn resolve_ref_updates(
    state: &Storage,
    repo: &RepoMetadata,
    updates: &[RefUpdate],
    pre_rejected: &HashMap<String, PushRejection>,
) -> Result<Vec<RefUpdateOutcome>, Error> {
    let mut results_by_ref: HashMap<String, Result<(), PushRejection>> = pre_rejected
        .iter()
        .map(|(refname, rejection)| (refname.clone(), Err(rejection.clone())))
        .collect();
    let to_apply: Vec<RefUpdate> = updates
        .iter()
        .filter(|u| !pre_rejected.contains_key(&u.refname))
        .cloned()
        .collect();
    if !to_apply.is_empty() {
        results_by_ref.extend(
            state
                .rows
                .repo(repo.id)
                .update_refs(&to_apply)
                .await?
                .into_iter()
                .map(|r| (r.refname, r.result.map_err(PushRejection::RefStore))),
        );
    }
    Ok(updates
        .iter()
        .map(|u| {
            let result = results_by_ref
                .remove(&u.refname)
                .unwrap_or(Err(PushRejection::Internal));
            RefUpdateOutcome {
                refname: u.refname.clone(),
                result,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use enroute_git_core::ObjectHashMap;
    use enroute_git_retrieve::RefUpdate;
    use gix_hash::ObjectId;

    use super::non_commit_branch_updates;

    fn oid_for_test(byte: u8) -> ObjectId {
        ObjectId::from_bytes_or_panic(&[byte; 20])
    }

    #[test]
    fn non_commit_branch_updates_is_conservative_about_unknown_objects() {
        // A new_id absent from this push's own pack must never be flagged —
        // only a positively-known non-commit is rejected here; anything else
        // is the connectivity walk's job.
        let null = ObjectId::null(gix_hash::Kind::Sha1);
        let unknown = oid_for_test(9);
        let updates = vec![RefUpdate {
            refname: "refs/heads/main".to_string(),
            old_id: null,
            new_id: unknown,
        }];
        let object_refs = ObjectHashMap::default();
        assert!(non_commit_branch_updates(&updates, &object_refs, null).is_empty());
    }
}
