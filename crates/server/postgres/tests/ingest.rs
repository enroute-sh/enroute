//! What a push does when the index has a hole under a parent's tree.
//!
//! Here rather than beside the push path because opening the hole means
//! taking an identity row out behind the engine's back, which nothing the
//! store offers a caller can do — only SQL against the table itself.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "a test: a store that cannot answer fails it, which is the point"
)]

use std::sync::Arc;

use gix_hash::ObjectId;
use object_store::memory::InMemory;
use sqlx::PgPool;

use enroute_git_ingest::{IncomingPack, IngestRequest, IngestWorker as _, LocalIngestWorker};
use enroute_git_retrieve::{RefUpdate, RefsMap, RepoMetadata, Storage};
use enroute_git_test_support::{PackEntry, TestCommit, linear_commit, make_pack_of};

/// Stores one commit's objects and its commit-graph rows.
///
/// The ref does not move: this worker ingests, and what a ref does about it
/// is the front door's. What is under test is what a push *stores*.
async fn push(state: &Storage, repo: &RepoMetadata, commit: &TestCommit) {
    let entries: Vec<PackEntry<'_>> = commit
        .pack_entries()
        .into_iter()
        .map(|(kind, bytes)| PackEntry::whole(kind, bytes))
        .collect();
    let pack = make_pack_of(&entries);

    let worker = LocalIngestWorker::new(state.clone(), Arc::new(InMemory::new()));
    let ingested = worker
        .ingest(
            IngestRequest {
                repo: repo.clone(),
                existing: RefsMap::new(),
                updates: vec![RefUpdate {
                    refname: "refs/heads/main".to_owned(),
                    old_id: ObjectId::null(gix_hash::Kind::Sha1),
                    new_id: commit.commit_oid,
                }],
            },
            IncomingPack {
                reader: Box::new(std::io::Cursor::new(pack.clone())),
                len_hint: u64::try_from(pack.len()).ok(),
            },
            &enroute_git_ingest::noop_progress,
            &enroute_git_cost::Meter::new(),
        )
        .await
        .unwrap();
    assert!(
        ingested.rejected.is_empty() && ingested.screened.is_empty(),
        "push did not store what it brought: {ingested:?}"
    );
}

/// Drop `oid`'s identity, as a partial promotion or a prior bug would leave
/// it, while everything above stays readable.
///
/// The segment still holds the record; nothing maps an oid to it any more,
/// which is the gap.
async fn open_index_gap(state: &Storage, repo: &RepoMetadata, pool: &PgPool, oid: ObjectId) {
    sqlx::query("DELETE FROM object_seqs WHERE oid = $1")
        .bind(oid.as_slice())
        .execute(pool)
        .await
        .unwrap();
    assert!(
        enroute_git_retrieve::meta(state, repo.id, oid)
            .await
            .unwrap()
            .is_none(),
        "the object must be unreadable for this to test anything"
    );
}

/// A push whose parent's tree has a hole in the index: a blob the parent's
/// tree names is gone, but the commit and its root tree remain.
///
/// The connectivity check stops at the first index hit and never looks under
/// a pre-existing parent's root tree, so this hole rejects nothing.
#[tokio::test]
async fn a_parent_side_object_missing_from_the_index() {
    let (state, pool) = enroute_postgres::ephemeral(&enroute_postgres::test_database_url())
        .await
        .expect("these tests need Postgres: set DATABASE_URL, or see docs/internals/quality-assurance.md");
    let repo = state.rows.create(None).await.unwrap();

    let first = linear_commit(b"one\n", None, 0, "c");
    push(&state, &repo, &first).await;

    open_index_gap(&state, &repo, &pool, first.blob_oid).await;

    // A child changing the same file. Nothing re-verifies under the parent's
    // root tree, so the hole is never reached and the push lands.
    let second = linear_commit(b"two\n", Some(&first.commit_sha), 1, "c");
    push(&state, &repo, &second).await;
}
