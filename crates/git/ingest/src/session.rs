//! The public entry point for a push: [`IngestSession`] carries a pack from
//! the wire to the primary store.
//!
//! Callers never touch staging, connectivity, or ref-update internals; a
//! delete-only push carries no pack, so [`finalize`] runs with nothing read.
//!
//! [`finalize`]: IngestSession::finalize

use std::collections::HashSet;
use std::sync::Arc;

use gix_hash::ObjectId;
use tokio::io::AsyncBufRead;

use enroute_git_core::{Error, ObjectHashMap};
use enroute_git_graph::ObjectRefs;
use enroute_git_retrieve::{RefUpdate, RefsMap, RepoMetadata, Storage};

use crate::append::{Engine, append};
use crate::delta_plan::{DeltaPlan, bound_chains, commit_parents, plan_push};
use crate::pack::{WirePack, WirePacks, scan};
use crate::progress::{IngestProgress, ProgressSink};
use crate::ref_updates::{
    Ingested, build_pre_rejected, check_ref_update_connectivity, nff_rejected_refs,
    non_commit_branch_updates, refname_rejections,
};
use crate::staging::{Staging, StagingStore};
use crate::upload::{StagedPack, upload_staged_ordered};

/// One push's ingestion lifecycle, from first object to promotion.
///
/// Owns what the passes share — the repo handles, the packs, the staging
/// area, and what has been resolved so far — and runs them in order.
#[derive(Debug)]
pub(crate) struct IngestSession {
    state: Storage,
    repo: RepoMetadata,
    staging: Staging,
    /// Outgoing refs for every object this push resolved out of its packs.
    ///
    /// Anything a plan names that isn't here pre-dates the push.
    object_refs: ObjectHashMap<ObjectRefs>,
    /// The packs this push arrived in, kept for its whole life: every later
    /// pass reads bytes out of them, verbatim or rebuilt.
    wire: WirePacks,
    /// What each commit's pack stores and what each entry deltas against —
    /// empty until [`ingest_pack`] has resolved the objects to plan over.
    ///
    /// [`ingest_pack`]: IngestSession::ingest_pack
    plan: DeltaPlan,
}

impl IngestSession {
    /// Begin a session scoped to `repo`, with a fresh staging area in
    /// `staging`.
    #[must_use]
    pub(crate) fn new(state: Storage, staging: Arc<StagingStore>, repo: RepoMetadata) -> Self {
        Self {
            staging: Staging::new(staging, &repo),
            state,
            repo,
            object_refs: ObjectHashMap::default(),
            wire: WirePacks::default(),
            plan: DeltaPlan::default(),
        }
    }

    /// Read a pack off `body` and stage every object it obliges this push to
    /// store.
    ///
    /// `len_hint` is what the caller knows the pack to run to, if anything —
    /// see [`crate::IncomingPack`].
    ///
    /// # Errors
    /// Returns an error if the pack is malformed or fails its checksum, or if
    /// a staging read or write fails.
    pub(crate) async fn ingest_pack<R: AsyncBufRead + Unpin>(
        &mut self,
        body: R,
        len_hint: Option<u64>,
        progress: ProgressSink<'_>,
    ) -> Result<(), Error> {
        let (entries, raw) = scan(body, len_hint).await?;
        let oids = crate::resolve::identify_entries(
            &entries,
            &raw,
            &mut self.object_refs,
            &self.staging,
            &self.state,
            &self.repo,
            progress,
        )
        .await?;
        self.wire.adopt(WirePack::new(raw, entries, oids)?);

        let mut plan = plan_push(&self.object_refs, &self.state, &self.repo, progress).await?;
        crate::classify::classify(
            &mut plan,
            &self.wire,
            &self.object_refs,
            &self.state,
            &self.repo,
        )
        .await?;
        // Chains are bounded last, over the mixed plan: a kept client delta
        // and an encoded one both add a hop, so neither can be measured
        // alone.
        let parents_of = commit_parents(&self.object_refs);
        bound_chains(&mut plan.packs, &parents_of, &self.state, &self.repo).await?;

        crate::materialise::materialise_plan(
            &plan,
            &self.wire,
            &self.object_refs,
            &mut self.staging,
            &self.state,
            &self.repo,
            progress,
        )
        .await?;
        // Replaced rather than merged: a later pack's plan covers every
        // commit an earlier one's did, being computed from the whole
        // session's `object_refs`.
        self.plan = plan;
        Ok(())
    }

    /// Store everything this push brought, and say which of its updates
    /// already failed.
    ///
    /// No ref moves here — see [`crate::apply_ref_updates`], which whoever
    /// called this runs next. `self` drops either way, deleting its chunks.
    ///
    /// # Errors
    /// Returns an error if connectivity checking, promotion, or recording the
    /// new commit graph fails.
    #[tracing::instrument(name = "enroute_git_ingest::session::finalize", skip_all, fields(ref_count = updates.len()))]
    pub(crate) async fn finalize(
        self,
        existing: &RefsMap,
        updates: &[RefUpdate],
        progress: ProgressSink<'_>,
    ) -> Result<Ingested, Error> {
        let null = ObjectId::null(gix_hash::Kind::Sha1);

        // Pre-screen NFF updates so rejected commits aren't promoted to S3
        // and the commit graph for a ref nothing will point to. The
        // authoritative check still happens later, inside
        // `metadata.update_refs`'s transaction.
        let nff_rejected = nff_rejected_refs(updates, existing, null);
        let refname_rejected = refname_rejections(updates, null);
        let non_commit_rejected = non_commit_branch_updates(updates, &self.object_refs, null);
        let pre_screened: HashSet<&str> = nff_rejected
            .iter()
            .copied()
            .chain(refname_rejected.keys().map(String::as_str))
            .chain(non_commit_rejected.iter().map(String::as_str))
            .collect();

        progress(IngestProgress::CheckingConnectivity);
        let (connected_commits, connectivity_failures) = check_ref_update_connectivity(
            updates,
            &pre_screened,
            &self.object_refs,
            &self.state,
            &self.repo,
            null,
        )
        .await?;

        // Pre-screened updates never reach the ref store. Everything else
        // goes to `update_refs`, which re-validates against live state and
        // reports non-fast-forward per ref.
        let pre_rejected = build_pre_rejected(
            connectivity_failures,
            refname_rejected,
            &non_commit_rejected,
        );

        // Claimed here rather than where the per-commit counter lives: the
        // work between the two is promotion's as much as the upload is, and
        // until this fires it is all attributed to the preceding stage.
        progress(IngestProgress::UpdatingRepository { done: 0, total: 0 });
        // Moved out of `self` field by field: the rest of this method still
        // reads `state`/`repo`, which giving the staging area up by value
        // leaves untouched.
        let (chunk_index, staging_session) = self.staging.finish(&self.repo).await?;
        let promoted = upload_staged_ordered(
            &StagedPack {
                object_refs: &self.object_refs,
                connected_commits: &connected_commits,
                chunk_index: &chunk_index,
                plan: &self.plan,
                wire: self.wire.packs(),
                // Cloned so the session outlives promotion: dropping the last
                // handle deletes the chunks, and this method is not done.
                staging_session: Arc::clone(&staging_session),
                progress,
            },
            &self.state,
            &self.repo,
        )
        .await?;

        // Covers the policy pass below as well: the ancestry it asks about
        // reads back what `append` writes, so it cannot start until this
        // finishes.
        progress(IngestProgress::RecordingCommits);
        let metadata = &self.state;
        append(
            Engine {
                ids: metadata.rows.repo(self.repo.id),
                graph: metadata.graph.repo(self.repo.id),
                objects: metadata.objects.repo(self.repo.id),
                ledger: metadata.ledger.as_ref(),
            },
            &promoted.new_commits,
            &promoted.new_objects,
            &promoted.known_seqs,
        )
        .await?;

        Ok(Ingested {
            rejected: pre_rejected,
            screened: nff_rejected.into_iter().map(str::to_string).collect(),
        })
    }
}

/// Both halves of a push in one call, for tests whose subject is what a push
/// does rather than where each half runs.
///
/// Production runs them separately — a worker may be in another process, and
/// only the front door can ask an application what may land.
#[cfg(test)]
impl IngestSession {
    pub(crate) async fn finalize_and_apply(
        self,
        existing: &RefsMap,
        updates: &[RefUpdate],
        progress: ProgressSink<'_>,
        hooks: &dyn crate::hooks::ReceiveHooks,
    ) -> Result<Vec<crate::RefUpdateOutcome>, Error> {
        let state = self.state.clone();
        let repo = self.repo.clone();
        let ingested = self.finalize(existing, updates, progress).await?;
        Ok(crate::apply_ref_updates(
            &state,
            &repo,
            &crate::Actor::new(crate::test_helpers::TEST_USER),
            updates,
            ingested,
            hooks,
            progress,
        )
        .await?
        .outcomes)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use enroute_git_core::Error;
    use enroute_git_retrieve::{RefUpdate, RefUpdateRejection, RefsMap, RepoMetadata, Storage};
    use enroute_git_test_support::{
        PackEntry, blob, commit, make_pack_of, make_state, tree_of, tree_with_blob,
    };
    use gix_hash::ObjectId;
    use gix_object::Kind;

    use super::IngestSession;
    use crate::hooks::NoHooks;
    use crate::progress::{IngestProgress, ProgressSink, noop_progress};
    use crate::ref_updates::PushRejection;
    use crate::test_helpers::staging_store;

    fn null() -> ObjectId {
        ObjectId::null(gix_hash::Kind::Sha1)
    }

    fn update(refname: &str, old_id: ObjectId, new_id: ObjectId) -> RefUpdate {
        RefUpdate {
            refname: refname.to_string(),
            old_id,
            new_id,
        }
    }

    /// A session that has taken `entries` as this push's pack.
    ///
    /// The path a real push takes: every later pass reads an entry's bytes
    /// back out of the pack it arrived in.
    async fn ingested(
        state: &Storage,
        repo: &RepoMetadata,
        entries: &[PackEntry<'_>],
        progress: ProgressSink<'_>,
    ) -> IngestSession {
        let mut session = IngestSession::new(state.clone(), staging_store(), repo.clone());
        session
            .ingest_pack(Cursor::new(make_pack_of(entries)), None, progress)
            .await
            .unwrap();
        session
    }

    /// One root commit's objects, held together so their bytes outlive the
    /// pack entries that borrow them.
    struct Root {
        blob: Option<(ObjectId, Vec<u8>)>,
        tree: (ObjectId, Vec<u8>),
        commit: (ObjectId, Vec<u8>),
    }

    impl Root {
        /// A commit over a tree holding `blob_content` at one path.
        fn with_blob(blob_content: &[u8]) -> Self {
            let blob = blob(blob_content);
            let tree = tree_with_blob("f", blob.0);
            let commit = commit(tree.0, None);
            Self {
                blob: Some(blob),
                tree,
                commit,
            }
        }

        /// A commit over the empty tree — enough to move a ref with.
        fn empty() -> Self {
            let tree = tree_of(&[]);
            let commit = commit(tree.0, None);
            Self {
                blob: None,
                tree,
                commit,
            }
        }

        /// Its objects, each sent whole, in commit-pack order.
        fn entries(&self) -> Vec<PackEntry<'_>> {
            let mut entries: Vec<PackEntry<'_>> = self
                .blob
                .iter()
                .map(|(_, bytes)| PackEntry::whole(Kind::Blob, bytes))
                .collect();
            entries.push(PackEntry::whole(Kind::Tree, &self.tree.1));
            entries.push(PackEntry::whole(Kind::Commit, &self.commit.1));
            entries
        }

        /// The commit a ref update points at.
        fn tip(&self) -> ObjectId {
            self.commit.0
        }

        /// Every object it carries, for asserting what a push did or did not
        /// index.
        fn oids(&self) -> Vec<ObjectId> {
            self.blob
                .iter()
                .map(|(oid, _)| *oid)
                .chain([self.tree.0, self.commit.0])
                .collect()
        }
    }

    #[tokio::test]
    async fn finalize_reports_stages_in_order() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let root = Root::with_blob(b"hello");
        // The pack passes report stages of their own; this is about the ones
        // `finalize` reports after them.
        let session = ingested(&state, &repo, &root.entries(), &noop_progress).await;

        let reports: std::sync::Mutex<Vec<IngestProgress>> = std::sync::Mutex::new(Vec::new());
        let progress = |stage: IngestProgress| reports.lock().unwrap().push(stage);

        session
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), root.tip())],
                &progress,
                &NoHooks,
            )
            .await
            .unwrap();

        let reports = reports.into_inner().unwrap();
        let mut kinds: Vec<&str> = reports
            .iter()
            .map(crate::test_helpers::stage_name)
            .collect();
        // Counters within a stage repeat it; the order of the stages
        // themselves is what this is about.
        kinds.dedup();
        assert_eq!(
            kinds,
            [
                "checking-connectivity",
                "updating-repository",
                "recording-commits",
                "updating-references"
            ],
            "every stage runs once, in order"
        );
    }

    #[tokio::test]
    async fn finalize_rejects_disconnected_tip_as_missing_objects() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let session = IngestSession::new(state.clone(), staging_store(), repo.clone());
        let missing = ObjectId::from_hex(b"deadbeefdeadbeefdeadbeefdeadbeefdeadbeef").unwrap();

        let results = session
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), missing)],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].refname, "refs/heads/main");
        assert_eq!(results[0].result, Err(PushRejection::MissingObjects));
    }

    #[tokio::test]
    async fn finalize_rejects_commit_with_missing_tree() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let missing_tree = ObjectId::from_hex(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let (commit_oid, commit_bytes) = commit(missing_tree, None);

        let results = ingested(
            &state,
            &repo,
            &[PackEntry::whole(Kind::Commit, &commit_bytes)],
            &noop_progress,
        )
        .await
        .finalize_and_apply(
            &RefsMap::new(),
            &[update("refs/heads/main", null(), commit_oid)],
            &noop_progress,
            &NoHooks,
        )
        .await
        .unwrap();

        assert_eq!(results[0].result, Err(PushRejection::MissingObjects));
    }

    #[tokio::test]
    async fn finalize_connects_partial_push_against_primary_store() {
        // A commit-only push (no tree/blob resent) must resolve connectivity
        // for its unchanged tree/blob against the primary store, from the
        // previous push, not just this push's own pack.
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let root = Root::with_blob(b"hello");
        let (commit1, tree1) = (root.tip(), root.tree.0);
        ingested(&state, &repo, &root.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), commit1)],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();

        let (commit2, commit2_bytes) = commit(tree1, Some(commit1));
        let existing = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        let results = ingested(
            &state,
            &repo,
            &[PackEntry::whole(Kind::Commit, &commit2_bytes)],
            &noop_progress,
        )
        .await
        .finalize_and_apply(
            &existing,
            &[update("refs/heads/main", commit1, commit2)],
            &noop_progress,
            &NoHooks,
        )
        .await
        .unwrap();

        assert_eq!(results[0].result, Ok(()));
        assert!(
            enroute_git_retrieve::meta(&state, repo.id, commit2)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn finalize_nff_rejected_commit_not_indexed() {
        // A push whose old_id doesn't match the tip must be NFF-rejected and
        // must not promote any objects to the index.
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let first = Root::with_blob(b"hello");
        ingested(&state, &repo, &first.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), first.tip())],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();

        let wrong_old = ObjectId::from_hex(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        let second = Root::with_blob(b"world");
        let existing = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        let results = ingested(&state, &repo, &second.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &existing,
                &[update("refs/heads/main", wrong_old, second.tip())],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();

        assert_eq!(
            results[0].result,
            Err(PushRejection::RefStore(RefUpdateRejection::NonFastForward))
        );
        for oid in second.oids() {
            assert!(
                enroute_git_retrieve::meta(&state, repo.id, oid)
                    .await
                    .unwrap()
                    .is_none(),
                "{} must not be indexed after NFF rejection",
                oid.to_hex()
            );
        }
    }

    // Verified against real git: pushing a blob to a branch is rejected as a
    // non-commit object; the identical push to a lightweight tag succeeds.
    #[tokio::test]
    async fn finalize_rejects_blob_pushed_to_branch() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let (blob_oid, blob_bytes) = blob(b"just a blob\n");

        let results = ingested(
            &state,
            &repo,
            &[PackEntry::whole(Kind::Blob, &blob_bytes)],
            &noop_progress,
        )
        .await
        .finalize_and_apply(
            &RefsMap::new(),
            &[update("refs/heads/weird", null(), blob_oid)],
            &noop_progress,
            &NoHooks,
        )
        .await
        .unwrap();

        assert_eq!(results[0].result, Err(PushRejection::NonCommitObject));
        let refs = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        assert!(!refs.contains_key("refs/heads/weird"));
    }

    #[tokio::test]
    async fn finalize_allows_blob_pushed_to_tag() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let (blob_oid, blob_bytes) = blob(b"just a blob\n");

        let results = ingested(
            &state,
            &repo,
            &[PackEntry::whole(Kind::Blob, &blob_bytes)],
            &noop_progress,
        )
        .await
        .finalize_and_apply(
            &RefsMap::new(),
            &[update("refs/tags/blob-tag", null(), blob_oid)],
            &noop_progress,
            &NoHooks,
        )
        .await
        .unwrap();

        assert_eq!(results[0].result, Ok(()));
        let refs = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        assert_eq!(
            refs.get("refs/tags/blob-tag").map(String::as_str),
            Some(blob_oid.to_hex().to_string().as_str())
        );
    }

    /// A push may land any namespace git would accept, not only heads and tags.
    ///
    /// A repository that does not want `refs/notes/*`, or an application's own
    /// `refs/merge-requests/*`, refuses them in `pre-receive`.
    #[tokio::test]
    async fn finalize_accepts_any_valid_ref_namespace() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let root = Root::with_blob(b"hello");

        let results = ingested(&state, &repo, &root.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/notes/x", null(), root.tip())],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();

        assert_eq!(results[0].result, Ok(()));

        let refs = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        assert_eq!(
            refs.get("refs/notes/x").map(String::as_str),
            Some(root.tip().to_hex().to_string().as_str())
        );
    }

    /// A refname git itself would refuse is still refused, on both doors.
    #[tokio::test]
    async fn finalize_rejects_an_invalid_refname() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let root = Root::with_blob(b"hello");

        let results = ingested(&state, &repo, &root.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("HEAD", null(), root.tip())],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();

        assert_eq!(results[0].result, Err(PushRejection::FunnyRefname));
    }

    // ── receive hooks ────────────────────────────────────────────────────

    /// Hooks that refuse whatever refnames they were built with.
    ///
    /// Records the commands it was shown, so a test can assert on what a
    /// hook is given rather than only on the outcome.
    #[derive(Debug, Default)]
    struct Refusing {
        refuse: Vec<&'static str>,
        /// Refnames it says nothing at all about, as a policy loop over the
        /// wrong namespace does.
        ignore: Vec<&'static str>,
        seen: std::sync::Mutex<Vec<crate::hooks::RefCommand>>,
    }

    #[async_trait::async_trait]
    impl crate::hooks::ReceiveHooks for Refusing {
        async fn pre_receive(
            &self,
            _repo: enroute_git_core::RepoId,
            _actor: &crate::Actor,
            commands: &[crate::hooks::RefCommand],
        ) -> Result<Vec<crate::hooks::RefJudgement>, Error> {
            self.seen.lock().unwrap().extend(commands.iter().cloned());
            Ok(commands
                .iter()
                .filter(|c| !self.ignore.contains(&c.refname.as_str()))
                .map(|c| crate::hooks::RefJudgement {
                    refname: c.refname.clone(),
                    verdict: if self.refuse.contains(&c.refname.as_str()) {
                        crate::hooks::Verdict::Refuse(format!("{} is protected", c.refname))
                    } else {
                        crate::hooks::Verdict::Allow
                    },
                })
                .collect())
        }
    }

    /// Hooks that cannot answer.
    ///
    /// A push meeting one fails rather than deciding for itself.
    #[derive(Debug)]
    struct Unreachable;

    #[async_trait::async_trait]
    impl crate::hooks::ReceiveHooks for Unreachable {
        async fn pre_receive(
            &self,
            _repo: enroute_git_core::RepoId,
            _actor: &crate::Actor,
            _commands: &[crate::hooks::RefCommand],
        ) -> Result<Vec<crate::hooks::RefJudgement>, Error> {
            Err(anyhow::anyhow!("the hooks did not answer").into())
        }
    }

    /// What a hook refuses comes back as a policy rejection carrying its
    /// reason, and the refs it did not name still land.
    #[tokio::test]
    async fn a_hook_refusal_rejects_that_ref_alone() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let hooks = Arc::new(Refusing {
            refuse: vec!["refs/heads/main"],
            ..Refusing::default()
        });

        let root = Root::empty();
        let results = ingested(&state, &repo, &root.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[
                    update("refs/heads/main", null(), root.tip()),
                    update("refs/heads/topic", null(), root.tip()),
                ],
                &noop_progress,
                hooks.as_ref(),
            )
            .await
            .unwrap();

        assert!(
            matches!(&results[0].result, Err(PushRejection::Policy(reason))
                if reason == "refs/heads/main is protected"),
            "{:?}",
            results[0].result
        );
        assert_eq!(results[1].result, Ok(()), "the ref it did not name");
    }

    /// A create carries no `old_id` and is never a force, whatever else the
    /// push contains.
    #[tokio::test]
    async fn a_create_is_shown_as_a_create() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let hooks = Arc::new(Refusing::default());

        let root = Root::empty();
        ingested(&state, &repo, &root.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), root.tip())],
                &noop_progress,
                hooks.as_ref(),
            )
            .await
            .unwrap();

        let seen = hooks.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].refname, "refs/heads/main");
        assert_eq!(seen[0].old_id, None, "a create has no previous value");
        assert_eq!(
            seen[0].new_id.as_deref(),
            Some(root.tip().to_hex().to_string()).as_deref()
        );
        assert!(!seen[0].force, "a create rewrites no history");
    }

    /// A hook that cannot answer fails the push.
    ///
    /// Deciding for it would be deciding the thing this engine has no
    /// standing to decide.
    #[tokio::test]
    async fn a_hook_that_will_not_answer_fails_the_push() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let root = Root::empty();
        let failed = ingested(&state, &repo, &root.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), root.tip())],
                &noop_progress,
                &Unreachable,
            )
            .await;

        assert!(failed.is_err(), "a push nobody judged must not land");
    }

    /// A hook that judges some of a push and not the rest fails all of it.
    ///
    /// The ref it left out is the one a default would decide, so there is no
    /// default: an answer for part of a push is an answer for none of it.
    #[tokio::test]
    async fn a_command_left_unjudged_fails_the_push() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let hooks = Refusing {
            ignore: vec!["refs/heads/topic"],
            ..Refusing::default()
        };

        let root = Root::empty();
        let failed = ingested(&state, &repo, &root.entries(), &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[
                    update("refs/heads/main", null(), root.tip()),
                    update("refs/heads/topic", null(), root.tip()),
                ],
                &noop_progress,
                &hooks,
            )
            .await;

        let error = failed
            .expect_err("the unjudged ref must not land")
            .to_string();
        assert!(
            error.contains("refs/heads/topic"),
            "the error names what was left out: {error}"
        );
    }

    /// Content that deltas well: every version shares a long prefix and
    /// differs only in its tail, so a delta is a copy plus a few bytes.
    fn versioned(i: u8) -> Vec<u8> {
        let mut content = vec![b'x'; 500];
        content.push(b'0' + i);
        content
    }

    /// One version of a path, and the two objects that carry it into a push.
    type Version = (ObjectId, Vec<u8>);

    /// `count` linear commits, each replacing the previous version of one
    /// path: the versions, their trees, and their commits.
    fn version_chain(count: u8) -> (Vec<Version>, Vec<Version>, Vec<Version>) {
        let (mut versions, mut trees, mut commits) = (Vec::new(), Vec::new(), Vec::new());
        let mut parent = None;
        for i in 0..count {
            let version = blob(&versioned(i));
            let tree = tree_with_blob("f", version.0);
            let commit = commit(tree.0, parent);
            parent = Some(commit.0);
            versions.push(version);
            trees.push(tree);
            commits.push(commit);
        }
        (versions, trees, commits)
    }

    /// Every tree and commit of a chain, each sent whole, appended to the
    /// blob entries a test built itself.
    fn with_history<'a>(
        entries: &mut Vec<PackEntry<'a>>,
        trees: &'a [Version],
        commits: &'a [Version],
    ) {
        entries.extend(
            trees
                .iter()
                .map(|(_, bytes)| PackEntry::whole(Kind::Tree, bytes)),
        );
        entries.extend(
            commits
                .iter()
                .map(|(_, bytes)| PackEntry::whole(Kind::Commit, bytes)),
        );
    }

    /// The stored entry for `oid`, header decoded — what the fetch path reads.
    async fn stored_header(
        state: &Storage,
        repo: &RepoMetadata,
        oid: ObjectId,
    ) -> enroute_git_store::PackEntryHeader {
        let meta = enroute_git_retrieve::meta(state, repo.id, oid)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{oid} is not indexed"));
        let loc = meta.location.expect("an indexed object has a location");
        let entry = state
            .store
            .get_segment_slice(
                repo,
                loc.segment.id,
                loc.segment_offset(),
                Some(loc.image.entry_len),
            )
            .await
            .unwrap()
            .expect("the segment the index points at");
        enroute_git_store::decode_pack_entry_header(&entry)
            .unwrap()
            .0
    }

    /// Every entry of `pack_commit`'s stored pack, header decoded — the whole
    /// image, not just the one location `objects.first_*` points at.
    async fn pack_entries(
        state: &Storage,
        repo: &RepoMetadata,
        pack_commit: ObjectId,
    ) -> Vec<enroute_git_store::PackEntryHeader> {
        let meta = enroute_git_retrieve::meta(state, repo.id, pack_commit)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{pack_commit} is not indexed"));
        let loc = meta.location.expect("an indexed commit has a location");
        let image = state
            .store
            .get_segment_slice(
                repo,
                loc.segment.id,
                loc.segment.base_offset,
                Some(loc.segment.image_len),
            )
            .await
            .unwrap()
            .expect("the segment the index points at");
        let header = enroute_git_store::decode_commit_pack_header(&image).unwrap();
        let trailer_len =
            usize::try_from(enroute_git_store::trailer_suffix_len(header.object_count)).unwrap();
        let trailer =
            enroute_git_store::decode_pack_trailer(&image[image.len() - trailer_len..]).unwrap();
        trailer
            .iter()
            .map(|e| {
                let at = usize::try_from(e.offset).unwrap();
                enroute_git_store::decode_pack_entry_header(&image[at..])
                    .unwrap()
                    .0
            })
            .collect()
    }

    /// A revert split across two pushes: push 1 stores Y as a delta against
    /// X, push 2 puts X back without the push carrying it.
    ///
    /// Only `bound_chains` stands between X's second entry and a base of Y.
    #[tokio::test]
    async fn a_revert_split_across_pushes_stores_the_repeat_whole() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let (versions, trees, commits) = version_chain(2);
        let (old_blob, new_blob) = (versions[0].0, versions[1].0);
        let (c0, c1) = (commits[0].0, commits[1].0);

        let mut entries = vec![
            PackEntry::whole(Kind::Blob, &versions[0].1),
            // On a base c1's readers can reach, so it is kept — which is what
            // leaves Y needing X to rebuild.
            PackEntry::delta(
                Kind::Blob,
                &versions[1].1,
                (old_blob, versions[0].1.as_slice()),
            ),
        ];
        with_history(&mut entries, &trees, &commits);

        let results = ingested(&state, &repo, &entries, &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), c1)],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();
        assert!(results[0].result.is_ok(), "{:?}", results[0].result);

        assert_eq!(stored_header(&state, &repo, old_blob).await.base, None);
        assert_eq!(
            stored_header(&state, &repo, new_blob).await.base,
            Some(old_blob),
            "the setup needs Y stored as a delta against X"
        );

        // Push 2: revert the path to X by reusing c0's tree, carrying only
        // the commit — every other object is already in the repo.
        let (c2, c2_bytes) = commit(trees[0].0, Some(c1));
        let existing = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        let results = ingested(
            &state,
            &repo,
            &[PackEntry::whole(Kind::Commit, &c2_bytes)],
            &noop_progress,
        )
        .await
        .finalize_and_apply(
            &existing,
            &[update("refs/heads/main", c1, c2)],
            &noop_progress,
            &NoHooks,
        )
        .await
        .unwrap();
        assert!(results[0].result.is_ok(), "{:?}", results[0].result);

        // X is now stored twice: whole in c0's pack, and again in c2's.
        let packs = state
            .objects
            .repo(repo.id)
            .packs_of(old_blob)
            .await
            .unwrap();
        assert_eq!(packs, vec![c0, c2], "packs holding X");

        // Y still deltas against X, so the second entry must not delta against
        // Y: that pair would point at each other, and a fetch emitting one
        // copy per object could pick the delta and hand a client a cycle.
        assert_eq!(
            stored_header(&state, &repo, new_blob).await.base,
            Some(old_blob),
            "Y's entry in c1's pack"
        );
        let entries = pack_entries(&state, &repo, c2).await;
        let old_in_c2 = entries
            .iter()
            .find(|e| e.sha == old_blob)
            .unwrap_or_else(|| panic!("c2's pack holds no entry for X: {entries:?}"));
        assert_eq!(old_in_c2.base, None, "X's entry in c2's pack");

        // The point lookup resolves X through its canonical (whole) entry.
        let (_, content) = enroute_git_retrieve::object(&state, &repo, old_blob)
            .await
            .unwrap();
        assert_eq!(content.as_ref(), versioned(0).as_slice());
    }

    /// A later push whose pack must include an object an earlier push stored
    /// as a delta, without this push having carried it.
    ///
    /// Driven here rather than through `dev/e2e` because real git re-sends
    /// the blob, which skips this branch entirely.
    #[tokio::test]
    async fn a_pack_including_an_earlier_pushs_delta() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        // First push: three versions of one path, so v1 lands as a delta.
        let (versions, trees, commits) = version_chain(3);
        let tip = commits[2].0;
        let mut entries = vec![PackEntry::whole(Kind::Blob, &versions[0].1)];
        for at in 1..versions.len() {
            entries.push(PackEntry::delta(
                Kind::Blob,
                &versions[at].1,
                (versions[at - 1].0, versions[at - 1].1.as_slice()),
            ));
        }
        with_history(&mut entries, &trees, &commits);

        ingested(&state, &repo, &entries, &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), tip)],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();

        let middle = versions[1].0;
        assert!(
            stored_header(&state, &repo, middle).await.base.is_some(),
            "the setup needs v1 stored as a delta for this to test anything"
        );

        // Second push: a commit whose tree names v1 — which it does NOT carry.
        let (tree_oid, tree_bytes) = tree_with_blob("g", middle);
        let (commit_oid, commit_bytes) = commit(tree_oid, Some(tip));
        let existing = state.rows.repo(repo.id).refs_for(&repo).await.unwrap();
        let results = ingested(
            &state,
            &repo,
            &[
                PackEntry::whole(Kind::Tree, &tree_bytes),
                PackEntry::whole(Kind::Commit, &commit_bytes),
            ],
            &noop_progress,
        )
        .await
        .finalize_and_apply(
            &existing,
            &[update("refs/heads/main", tip, commit_oid)],
            &noop_progress,
            &NoHooks,
        )
        .await
        .unwrap();
        assert!(results[0].result.is_ok(), "{:?}", results[0].result);

        let (_, content) = enroute_git_retrieve::object(&state, &repo, middle)
            .await
            .unwrap();
        assert_eq!(content.as_ref(), versioned(1).as_slice());
    }

    /// The whole point of the lattice: where a client's own encodings cannot
    /// be kept, this server's deltas are what the chain is stored as.
    ///
    /// Every one of them must still read back byte-for-byte.
    #[tokio::test]
    async fn a_chain_of_versions_is_stored_as_deltas_and_reads_back_whole() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let (versions, trees, commits) = version_chain(8);
        let tip = commits[commits.len() - 1].0;

        // The client deltified each version against its *successor*, whose
        // pack no reader of the older commit can reach, so every one of those
        // is refused and the lattice encodes them instead. Only the last,
        // which names the root version, is readable and so kept.
        let last = versions.len() - 1;
        let mut entries = vec![PackEntry::whole(Kind::Blob, &versions[0].1)];
        for at in 1..last {
            entries.push(PackEntry::delta(
                Kind::Blob,
                &versions[at].1,
                (versions[at + 1].0, versions[at + 1].1.as_slice()),
            ));
        }
        entries.push(PackEntry::delta(
            Kind::Blob,
            &versions[last].1,
            (versions[0].0, versions[0].1.as_slice()),
        ));
        with_history(&mut entries, &trees, &commits);

        let results = ingested(&state, &repo, &entries, &noop_progress)
            .await
            .finalize_and_apply(
                &RefsMap::new(),
                &[update("refs/heads/main", null(), tip)],
                &noop_progress,
                &NoHooks,
            )
            .await
            .unwrap();
        assert!(results[0].result.is_ok(), "{:?}", results[0].result);

        // The first version roots the chain; every refused one deltas against
        // its predecessor, the base the lattice picks rather than the
        // successor the client asked for.
        let blobs: Vec<ObjectId> = versions.iter().map(|(oid, _)| *oid).collect();
        assert_eq!(stored_header(&state, &repo, blobs[0]).await.base, None);
        for at in 1..last {
            assert_eq!(
                stored_header(&state, &repo, blobs[at]).await.base,
                Some(blobs[at - 1]),
                "version {at} should delta against its predecessor"
            );
        }
        assert_eq!(
            stored_header(&state, &repo, blobs[last]).await.base,
            Some(blobs[0]),
            "the one client base a reader of this commit can reach is kept"
        );

        for (i, &blob_oid) in blobs.iter().enumerate() {
            let (kind, content) = enroute_git_retrieve::object(&state, &repo, blob_oid)
                .await
                .unwrap();
            assert_eq!(kind, Kind::Blob);
            assert_eq!(
                content.as_ref(),
                versioned(u8::try_from(i).unwrap()).as_slice(),
                "version {i} did not survive its delta chain"
            );
        }
    }
}
