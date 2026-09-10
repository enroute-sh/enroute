//! What a map cannot stand in for, so it is asked of a real database.
//!
//! Two kinds of thing: two connections in two transactions, where the locks
//! are what decide the outcome, and a row taken out behind the engine's back,
//! which nothing this store offers a caller can do.
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "a test: a store that cannot answer fails it, which is the point"
)]

use std::sync::Arc;

use gix_hash::ObjectId;
use gix_object::Kind;
use object_store::memory::InMemory;
// The schema name is this file's own: a literal and a fresh UUID.
use sqlx::{AssertSqlSafe, PgPool};

use enroute_git_core::{
    NewCommit, NewObject, ObjectHashMap, PackImageLocation, RepoId, SegmentLocation, oid,
};
use enroute_git_graph::{ObjectRefs, object_refs};
use enroute_git_ingest::KnownIdentities;
use enroute_git_metadata::{RefUpdate, RepoMetadata};
use enroute_git_retrieve::{Storage, TreeError, tree};
use enroute_git_test_support::{create_repo, seed_commit_pair_with_inherited_subtree};
use enroute_postgres::test_database_url;

/// A real multi-connection pool against a scratch, non-temp schema — what
/// genuinely concurrent transactions need and `pg_temp` cannot give.
///
/// Returns the schema name and the pool, for [`drop_concurrent_test_schema`].
async fn concurrent_test_store(max_connections: u32) -> anyhow::Result<(Storage, String, PgPool)> {
    let database_url = test_database_url();
    let schema = format!("concurrency_test_{}", uuid::Uuid::new_v4().simple());
    let schema_for_hook = schema.clone();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(move |conn, _meta| {
            let schema = schema_for_hook.clone();
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(format!("SET search_path TO {schema}")))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await?;
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&pool)
        .await?;
    let store = enroute_postgres::storage(
        &pool,
        Arc::new(InMemory::new()),
        Arc::new(enroute_git_store::Store::new(Arc::new(InMemory::new()))),
    );
    enroute_postgres::schema::apply(&pool).await?;
    Ok((store, schema, pool))
}

/// `let (store, schema, pool) = concurrent_test_store!(2);` — a fresh
/// multi-connection store in a scratch schema.
macro_rules! concurrent_test_store {
    ($n:expr) => {
        concurrent_test_store($n).await.expect(
            "these tests need Postgres: set DATABASE_URL, or see docs/internals/quality-assurance.md",
        )
    };
}

async fn drop_concurrent_test_schema(pool: &PgPool, schema: &str) {
    let result = sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(pool)
    .await;
    drop(result);
}

/// Spawn `fut_a` and `fut_b`, then wait for both under a timeout so a
/// lock-order regression panics instead of hanging the test suite.
async fn join_concurrent<Fut1, Fut2, T1, T2>(fut_a: Fut1, fut_b: Fut2) -> (T1, T2)
where
    Fut1: Future<Output = T1> + Send + 'static,
    Fut2: Future<Output = T2> + Send + 'static,
    T1: Send + 'static,
    T2: Send + 'static,
{
    let a = tokio::spawn(fut_a);
    let b = tokio::spawn(fut_b);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        (a.await.unwrap(), b.await.unwrap())
    })
    .await
    .expect("concurrent operation deadlocked")
}

/// Record a push the way the push path does.
async fn append_to(
    store: &Storage,
    repo_id: RepoId,
    new_commits: &ObjectHashMap<NewCommit>,
    new_objects: &[NewObject],
    known: &KnownIdentities,
) -> anyhow::Result<()> {
    enroute_git_ingest::append(
        enroute_git_ingest::Engine {
            ids: store.rows.repo(repo_id),
            graph: store.graph.repo(repo_id),
            objects: store.objects.repo(repo_id),
            ledger: store.ledger.as_ref(),
        },
        new_commits,
        new_objects,
        known,
    )
    .await
}

/// A fixed tree oid every `nc()` commit uses as its root tree.
fn placeholder_tree_oid() -> ObjectId {
    oid(0xAB)
}

fn nc(parents: Vec<ObjectId>) -> NewCommit {
    NewCommit {
        committer_date: 0,
        root_tree: placeholder_tree_oid(),
        parents,
        entry_len: 0,
        blob_offset: 0,
        segment: SegmentLocation {
            id: enroute_git_core::Ulid::from_parts(1, 1),
            base_offset: 0,
            image_len: 0,
        },
    }
}

/// A non-commit object with a single location in `pack`'s pack.
fn obj(oid: ObjectId, kind: Kind, pack: ObjectId) -> NewObject {
    NewObject {
        oid,
        kind,
        locations: vec![PackImageLocation {
            pack_sha: pack,
            offset: 100,
            entry_len: 40,
            base: None,
        }],
        children: vec![],
    }
}

/// Create a fresh repo, with the placeholder tree every `nc()` commit's
/// `root_tree_seq` resolves through.
async fn seed_repo(store: &Storage) -> RepoMetadata {
    let repo = store.rows.create(None).await.unwrap();
    append_to(
        store,
        repo.id,
        &ObjectHashMap::default(),
        &[NewObject {
            oid: placeholder_tree_oid(),
            kind: Kind::Tree,
            locations: vec![],
            children: vec![],
        }],
        &KnownIdentities::default(),
    )
    .await
    .unwrap();
    repo
}

/// A generous budget: this case is not about running out of one.
const PLENTY: usize = 50_000;

/// The root tree of a stored commit, read back out of its bytes.
async fn root_of(state: &Storage, repo: &RepoMetadata, commit: ObjectId) -> ObjectId {
    let (kind, bytes) = enroute_git_retrieve::object(state, repo, commit)
        .await
        .unwrap();
    let Ok(ObjectRefs::Commit { root_tree, .. }) = object_refs(kind, &bytes) else {
        panic!("seeded commit {commit} did not parse");
    };
    root_tree
}

/// One ref update, as a caller hands it over.
fn update(refname: &str, old_id: ObjectId, new_id: ObjectId) -> RefUpdate {
    RefUpdate {
        refname: refname.to_owned(),
        old_id,
        new_id,
    }
}

fn null_oid() -> ObjectId {
    ObjectId::null(gix_hash::Kind::Sha1)
}

/// One commit with no parents, so a branch may point at it.
async fn seed_commit(store: &Storage, repo_id: RepoId, oid: ObjectId) {
    append_to(
        store,
        repo_id,
        &ObjectHashMap::from_iter([(oid, nc(vec![]))]),
        &[],
        &KnownIdentities::default(),
    )
    .await
    .unwrap();
}

/// Regression test for the re-inclusion lost-update race: two genuinely
/// concurrent pushes re-including the same pre-existing blob.
///
/// `merge_reincluded_extras`'s `FOR UPDATE` read stops the second push
/// from clobbering the first's already-merged value.
#[tokio::test]
async fn append_concurrent_reincludes_of_same_object_both_land() {
    let (store, schema, pool) = concurrent_test_store!(2);
    let store = Arc::new(store);
    let repo_id = seed_repo(&store).await.id;
    let (pack_a, pack_b, pack_c, blob) = (oid(1), oid(2), oid(3), oid(4));

    append_to(
        &store,
        repo_id,
        &ObjectHashMap::from_iter([(pack_a, nc(vec![]))]),
        &[obj(blob, Kind::Blob, pack_a)],
        &KnownIdentities::default(),
    )
    .await
    .unwrap();

    let store_b = Arc::clone(&store);
    let store_c = Arc::clone(&store);
    let (result_b, result_c) = join_concurrent(
        async move {
            append_to(
                &store_b,
                repo_id,
                &ObjectHashMap::from_iter([(pack_b, nc(vec![]))]),
                &[obj(blob, Kind::Blob, pack_b)],
                &KnownIdentities::default(),
            )
            .await
        },
        async move {
            append_to(
                &store_c,
                repo_id,
                &ObjectHashMap::from_iter([(pack_c, nc(vec![]))]),
                &[obj(blob, Kind::Blob, pack_c)],
                &KnownIdentities::default(),
            )
            .await
        },
    )
    .await;
    result_b.unwrap();
    result_c.unwrap();

    let mut packs = store.objects.repo(repo_id).packs_of(blob).await.unwrap();
    packs.sort();
    let mut expected = vec![pack_a, pack_b, pack_c];
    expected.sort();
    assert_eq!(packs, expected, "both concurrent re-includes must land");

    drop_concurrent_test_schema(&pool, &schema).await;
}

/// Regression test for the lost-insert race: two genuinely concurrent
/// pushes introduce the same brand-new object for the first time.
///
/// The loser of `ON CONFLICT DO NOTHING` must still get its own fresh pack recorded.
#[tokio::test]
async fn append_concurrent_first_inclusion_of_same_new_object_both_pack_lists_land() {
    let (store, schema, pool) = concurrent_test_store!(2);
    let store = Arc::new(store);
    let repo_id = seed_repo(&store).await.id;
    let (pack_b, pack_c, blob) = (oid(1), oid(2), oid(3));

    let store_b = Arc::clone(&store);
    let store_c = Arc::clone(&store);
    let (result_b, result_c) = join_concurrent(
        async move {
            append_to(
                &store_b,
                repo_id,
                &ObjectHashMap::from_iter([(pack_b, nc(vec![]))]),
                &[obj(blob, Kind::Blob, pack_b)],
                &KnownIdentities::default(),
            )
            .await
        },
        async move {
            append_to(
                &store_c,
                repo_id,
                &ObjectHashMap::from_iter([(pack_c, nc(vec![]))]),
                &[obj(blob, Kind::Blob, pack_c)],
                &KnownIdentities::default(),
            )
            .await
        },
    )
    .await;
    result_b.unwrap();
    result_c.unwrap();

    let mut packs = store.objects.repo(repo_id).packs_of(blob).await.unwrap();
    packs.sort();
    let mut expected = vec![pack_b, pack_c];
    expected.sort();
    assert_eq!(
        packs, expected,
        "the race loser's pack must still be recorded"
    );

    drop_concurrent_test_schema(&pool, &schema).await;
}

/// Regression test for lock-order deadlock: two pushes re-include the
/// same two objects in opposite list order.
///
/// `merge_reincluded_extras` sorts oids before its `FOR UPDATE` select, same as `update_refs`.
#[tokio::test]
async fn append_concurrent_reincludes_of_two_objects_in_opposite_order_do_not_deadlock() {
    let (store, schema, pool) = concurrent_test_store!(2);
    let store = Arc::new(store);
    let repo_id = seed_repo(&store).await.id;
    let (pack_a, pack_b, pack_c) = (oid(1), oid(2), oid(3));
    let (x, y) = (oid(10), oid(20));

    append_to(
        &store,
        repo_id,
        &ObjectHashMap::from_iter([(pack_a, nc(vec![]))]),
        &[obj(x, Kind::Blob, pack_a), obj(y, Kind::Blob, pack_a)],
        &KnownIdentities::default(),
    )
    .await
    .unwrap();

    let store_b = Arc::clone(&store);
    let store_c = Arc::clone(&store);
    let (result_b, result_c) = join_concurrent(
        async move {
            append_to(
                &store_b,
                repo_id,
                &ObjectHashMap::from_iter([(pack_b, nc(vec![]))]),
                &[obj(y, Kind::Blob, pack_b), obj(x, Kind::Blob, pack_b)],
                &KnownIdentities::default(),
            )
            .await
        },
        async move {
            append_to(
                &store_c,
                repo_id,
                &ObjectHashMap::from_iter([(pack_c, nc(vec![]))]),
                &[obj(x, Kind::Blob, pack_c), obj(y, Kind::Blob, pack_c)],
                &KnownIdentities::default(),
            )
            .await
        },
    )
    .await;
    result_b.unwrap();
    result_c.unwrap();

    for &oid in &[x, y] {
        let mut packs = store.objects.repo(repo_id).packs_of(oid).await.unwrap();
        packs.sort();
        let mut expected = vec![pack_a, pack_b, pack_c];
        expected.sort();
        assert_eq!(packs, expected, "both concurrent re-includes must land");
    }

    drop_concurrent_test_schema(&pool, &schema).await;
}

/// Two pushes race on the same ref set in opposite refname order —
/// regression test for `update_refs` sorting by refname to avoid deadlock.
#[tokio::test]
async fn update_refs_concurrent_overlapping_pushes_do_not_deadlock() {
    let (store, schema, pool) = concurrent_test_store!(2);
    let store = Arc::new(store);
    let repo = seed_repo(&store).await;
    let repo_id = repo.id;
    let null = null_oid();
    for byte in [1, 2, 11, 12, 21, 22] {
        seed_commit(&store, repo_id, oid(byte)).await;
    }
    store
        .rows
        .repo(repo_id)
        .update_refs(&[
            update("refs/heads/a", null, oid(1)),
            update("refs/heads/b", null, oid(2)),
        ])
        .await
        .unwrap();

    let store_a = Arc::clone(&store);
    let store_b = Arc::clone(&store);
    let (results_a, results_b) = join_concurrent(
        async move {
            store_a
                .rows
                .repo(repo_id)
                .update_refs(&[
                    update("refs/heads/a", oid(1), oid(11)),
                    update("refs/heads/b", oid(2), oid(12)),
                ])
                .await
                .unwrap()
        },
        async move {
            store_b
                .rows
                .repo(repo_id)
                .update_refs(&[
                    update("refs/heads/b", oid(2), oid(22)),
                    update("refs/heads/a", oid(1), oid(21)),
                ])
                .await
                .unwrap()
        },
    )
    .await;

    // Exactly one of the two pushes should have won each ref (the loser
    // observes a stale old_id and gets non-fast-forward on both, since
    // both of its guarded updates raced against the winner atomically).
    let a_ok = results_a.iter().all(|r| r.result.is_ok());
    let b_ok = results_b.iter().all(|r| r.result.is_ok());
    assert_ne!(a_ok, b_ok, "exactly one push should fully succeed");

    drop_concurrent_test_schema(&pool, &schema).await;
}

/// A commit resolves to its root tree out of its own bytes, so a walk
/// can reach a tree identity never spoke for.
///
/// That is this side's inconsistency rather than the caller's, and the
/// only honest answer is to say so.
#[tokio::test]
async fn a_root_tree_the_index_lost_is_a_failure() {
    let (state, _schema, pool) = concurrent_test_store!(1);
    let repo = create_repo(&state).await;
    let (_, c1, _, _) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;
    let commit = ObjectId::from_hex(c1.as_bytes()).unwrap();
    let root = root_of(&state, &repo, commit).await;

    sqlx::query("DELETE FROM object_seqs WHERE oid = $1")
        .bind(root.as_slice())
        .execute(&pool)
        .await
        .unwrap();

    let refused = tree(&state, &repo, commit, PLENTY).await;

    // Reading the objects instead would not rescue it: every object read
    // resolves this same row before any bytes move.
    match refused {
        Err(TreeError::Failed(_)) => {}
        other => panic!("wanted Failed, got {other:?}"),
    }
}
