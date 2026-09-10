//! The worker's end, driven without a Lambda: an in-memory store stands in for
//! the directory bucket, and a real pack goes through the real ingest.
//!
//! Left untested is the envelope — event decoding, the response stream, the
//! secrets extension — which needs the deployed function.
#![cfg(feature = "server")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    reason = "test fixtures; a broken one should fail loudly rather than degrade"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt as _;
use futures::channel::mpsc;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _};

use enroute_git_ingest::LocalIngestWorker;
use enroute_git_retrieve::RepoMetadata;
use enroute_git_test_support::{TestCommit, create_repo, linear_commit, make_pack, make_state};
use enroute_ingest_lambda::server::Server;
use enroute_ingest_lambda::wire;

const ZEROS: &str = "0000000000000000000000000000000000000000";
const KEY: &str = "packs/test/one.pack";

fn request(repo: &RepoMetadata, commit: &TestCommit, pack: &wire::Pack) -> wire::Request {
    wire::Request {
        repo: wire::Repo {
            id: repo.id.as_i64(),
            storage_key: repo.storage_key.to_string(),
            default_branch: repo.default_branch.clone(),
        },
        existing: BTreeMap::new(),
        updates: vec![wire::RefUpdate {
            refname: "refs/heads/main".to_string(),
            old_id: ZEROS.to_string(),
            new_id: commit.commit_sha.clone(),
        }],
        pack: pack.clone(),
    }
}

/// Stands in for the function's configured memory, so the reported cost
/// carries a duration billed against something.
const MEMORY_MB: u64 = 1024;

async fn serve(
    state: enroute_git_retrieve::Storage,
    packs: Arc<dyn ObjectStore>,
    push: wire::Push,
) -> Vec<wire::Frame> {
    let (tx, rx) = mpsc::unbounded();
    Server::new(
        LocalIngestWorker::new(state, Arc::new(InMemory::new())),
        MEMORY_MB,
    )
    .serve(
        packs,
        wire::Call {
            traceparent: None,
            // Named on the call as a real one names them, though this test
            // hands `serve` the stores it already built.
            objects: "memory:///objects".parse().expect("a store URI"),
            staging: "memory:///staging".parse().expect("a store URI"),
            push,
        },
        &tx,
        Instant::now(),
    )
    .await;
    drop(tx);

    rx.collect::<Vec<_>>()
        .await
        .into_iter()
        .flat_map(|frame| frame.unwrap_or_default().to_vec())
        .collect::<Vec<u8>>()
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("every frame must be parseable"))
        .collect()
}

#[tokio::test]
async fn a_staged_pack_lands_and_is_then_collected() {
    let state = make_state();
    let repo = create_repo(&state).await;
    let commit = linear_commit(b"hello\n", None, 0, "init");

    let bytes = make_pack(&commit.pack_entries());
    let packs: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let key = StorePath::from(KEY);
    packs
        .put(&key, Bytes::from(bytes.clone()).into())
        .await
        .unwrap();
    let staged = wire::Pack::Staged(wire::Staged {
        key: KEY.to_string(),
        len: bytes.len() as u64,
    });

    let frames = serve(
        state.clone(),
        packs.clone(),
        wire::Push::Inline(request(&repo, &commit, &staged)),
    )
    .await;

    let [wire::Frame::Done { ingested, .. }] = frames.as_slice() else {
        panic!("expected exactly one Done frame, got {frames:?}");
    };
    assert!(ingested.rejected.is_empty(), "{ingested:?}");
    assert!(ingested.screened.is_empty(), "{ingested:?}");

    // The push is only real if the objects landed. The ref has deliberately
    // not moved: this worker stores the commit graph and says what it refused,
    // and the front door decides what points at it.
    assert!(
        state
            .graph
            .repo(repo.id)
            .contains(commit.commit_sha.parse().unwrap())
            .await
            .unwrap(),
        "the commit this push brought must be recorded"
    );

    // Ownership transferred at invoke, so cleanup is the worker's job.
    assert!(
        packs.get(&key).await.is_err(),
        "the worker should have deleted the pack it consumed"
    );
}

/// A pack small enough to have ridden in the call ingests the same way, and
/// there is nothing left for the worker to collect afterwards.
#[tokio::test]
async fn an_inline_pack_lands_without_touching_the_bucket() {
    let state = make_state();
    let repo = create_repo(&state).await;
    let commit = linear_commit(b"hello\n", None, 0, "init");

    let packs: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let inline = wire::Pack::Inline {
        bytes: Bytes::from(make_pack(&commit.pack_entries())),
    };

    let frames = serve(
        state.clone(),
        packs.clone(),
        wire::Push::Inline(request(&repo, &commit, &inline)),
    )
    .await;

    let [wire::Frame::Done { ingested, .. }] = frames.as_slice() else {
        panic!("expected exactly one Done frame, got {frames:?}");
    };
    assert!(ingested.rejected.is_empty(), "{ingested:?}");

    assert!(
        state
            .graph
            .repo(repo.id)
            .contains(commit.commit_sha.parse().unwrap())
            .await
            .unwrap(),
        "the commit this push brought must be recorded"
    );

    // The saving this exists for: the staging bucket was never involved, so
    // there is also no delete to issue against it.
    assert_eq!(
        packs.list(None).collect::<Vec<_>>().await.len(),
        0,
        "an inline push must not have used the staging bucket"
    );
}

/// A push too large for the payload quota is read back out of the bucket, and
/// both it and the pack it named are collected afterwards.
///
/// The case this exists for is a mirror of a repository with more refs than a
/// single call can describe — the one shape that has no fast path at all.
#[tokio::test]
async fn a_staged_push_is_read_back_and_both_objects_collected() {
    const PUSH_KEY: &str = "packs/test/one.push";

    let state = make_state();
    let repo = create_repo(&state).await;
    let commit = linear_commit(b"hello\n", None, 0, "init");

    let bytes = make_pack(&commit.pack_entries());
    let packs: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    packs
        .put(&StorePath::from(KEY), Bytes::from(bytes.clone()).into())
        .await
        .unwrap();

    // Both staged: the push in the bucket, naming a pack also in the bucket.
    let push = request(
        &repo,
        &commit,
        &wire::Pack::Staged(wire::Staged {
            key: KEY.to_string(),
            len: bytes.len() as u64,
        }),
    );
    let encoded = serde_json::to_vec(&push).unwrap();
    packs
        .put(
            &StorePath::from(PUSH_KEY),
            Bytes::from(encoded.clone()).into(),
        )
        .await
        .unwrap();

    let frames = serve(
        state.clone(),
        packs.clone(),
        wire::Push::Staged(wire::Staged {
            key: PUSH_KEY.to_string(),
            len: encoded.len() as u64,
        }),
    )
    .await;

    let [wire::Frame::Done { ingested, .. }] = frames.as_slice() else {
        panic!("expected exactly one Done frame, got {frames:?}");
    };
    assert!(ingested.rejected.is_empty(), "{ingested:?}");

    assert!(
        state
            .graph
            .repo(repo.id)
            .contains(commit.commit_sha.parse().unwrap())
            .await
            .unwrap(),
        "the commit this push brought must be recorded"
    );

    // Neither is the worker's to keep, and the pack's key was only knowable
    // once the push had been read.
    assert_eq!(
        packs.list(None).collect::<Vec<_>>().await.len(),
        0,
        "both the staged push and its pack should have been collected"
    );
}

/// A disagreement means this isn't the pack the front door staged — caught
/// before parsing, where it would surface as corruption instead.
#[tokio::test]
async fn a_length_that_disagrees_is_refused_before_parsing() {
    let state = make_state();
    let repo = create_repo(&state).await;
    let commit = linear_commit(b"hello\n", None, 0, "init");

    let bytes = make_pack(&commit.pack_entries());
    let packs: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    packs
        .put(&StorePath::from(KEY), Bytes::from(bytes.clone()).into())
        .await
        .unwrap();
    let staged = wire::Pack::Staged(wire::Staged {
        key: KEY.to_string(),
        len: bytes.len() as u64 + 1,
    });

    let frames = serve(
        state,
        packs.clone(),
        wire::Push::Inline(request(&repo, &commit, &staged)),
    )
    .await;

    let [wire::Frame::Failed { message, cost }] = frames.as_slice() else {
        panic!("expected exactly one Failed frame, got {frames:?}");
    };
    assert!(message.contains("expected"), "{message}");
    // A push that failed still read the handoff bucket to find that out, and
    // still burned the invocation. Reporting nothing would bias the record
    // toward cheap successes.
    let cost = cost.expect("a failed push still reports what it spent");
    assert!(cost.handoff.get_class > 0, "{cost:?}");
    assert_eq!(cost.memory_mb, MEMORY_MB);
}

/// A pack that isn't there is the shape the deployed smoke test provokes on
/// purpose, so it has to fail as a reported push rather than as a dead stream.
#[tokio::test]
async fn a_missing_pack_is_reported_not_dropped() {
    let state = make_state();
    let repo = create_repo(&state).await;
    let commit = linear_commit(b"hello\n", None, 0, "init");

    let packs: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let staged = wire::Pack::Staged(wire::Staged {
        key: KEY.to_string(),
        len: 12,
    });

    let frames = serve(
        state,
        packs.clone(),
        wire::Push::Inline(request(&repo, &commit, &staged)),
    )
    .await;

    let [wire::Frame::Failed { message, cost }] = frames.as_slice() else {
        panic!("expected exactly one Failed frame, got {frames:?}");
    };
    assert!(message.contains("staged pack"), "{message}");
    assert!(cost.is_some(), "a failed push still reports what it spent");
}
