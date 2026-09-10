//! The catalog against a real Postgres, in a `pg_temp` schema per test.
//!
//! Empty without the feature that builds the catalog it tests.

use std::sync::Arc;

use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectStore, path::Path};
use sqlx::{AssertSqlSafe, PgPool};

use enroute_git_journal::Index;
use enroute_lattice_core::{KeyRange, Policy};
use enroute_lattice_store::{Counters, Scope, Segments, conformance};
use enroute_postgres::{PostgresCatalog, Table};

/// One dedicated connection, in a `pg_temp` schema of its own.
///
/// The catalog's own DDL rather than the schema's steps: this suite is about
/// one table, and applies it itself below.
async fn connect() -> anyhow::Result<PgPool> {
    enroute_postgres::session_pool(&enroute_postgres::test_database_url()).await
}

/// `let catalog = catalog!();` — a catalog with its table in this
/// connection's `pg_temp`.
///
/// A missing database fails rather than skips: this suite is behind
/// `postgres-tests`, so building it at all is asking for one.
macro_rules! catalog {
    () => {{
        let pool = connect().await.expect(
            "these tests need Postgres: set DATABASE_URL, or see docs/internals/quality-assurance.md",
        );
        {
            let catalog = PostgresCatalog::new(pool, Table::new(Index::CommitGraph));
            catalog.apply_ddl().await.expect("applying the DDL");
            catalog
        }
    }};
}

fn scope(id: i64) -> Scope {
    Scope {
        id,
        prefix: Path::from(format!("repos/{id}/index")),
    }
}

fn policy(graduation_bytes: u64) -> Policy {
    Policy {
        fanout: 4,
        max_inputs: 8,
        max_input_bytes: 1 << 20,
        graduation_bytes,
        inline_ceiling: 64,
    }
}

async fn objects(bucket: &InMemory) -> usize {
    bucket.list(None).count().await
}

// The same suite the in-memory catalog runs, which is what makes that one
// evidence about this one.
#[tokio::test]
async fn it_meets_the_catalog_contract() {
    let catalog = catalog!();
    conformance::check(&catalog, &scope(1))
        .await
        .expect("the catalog contract");
}

#[tokio::test]
async fn applying_the_ddl_again_is_harmless() {
    let catalog = catalog!();
    catalog.apply_ddl().await.expect("a second apply");
    conformance::check(&catalog, &scope(1))
        .await
        .expect("the catalog contract");
}

// The tail Postgres carries is a `bytea`, so it has to survive being one
// well past the point where Postgres stops storing it in the row.
#[tokio::test]
async fn an_inlined_segment_survives_being_larger_than_a_page() {
    let catalog = catalog!();
    let bucket = Arc::new(InMemory::new());
    let store: Segments<Counters> =
        Segments::new(Arc::new(catalog), bucket.clone(), policy(u64::MAX));
    let scope = scope(1);

    // 10_000 entries at twelve bytes each: 120 KB, far past TOAST's ~2 KB.
    let large = Counters::run(0, 10_000);
    store
        .write(&scope, &large)
        .await
        .expect("a large inline write");
    assert_eq!(objects(&bucket).await, 0, "it must not have graduated");

    let read = store
        .read(&scope, KeyRange::EVERYTHING)
        .await
        .expect("reading back");
    assert_eq!(read, Some(large));
}

// The constraint is where "exactly one of these is set" actually lives.
#[tokio::test]
async fn a_row_with_both_a_body_and_a_key_is_refused() {
    let catalog = catalog!();
    // A second connection cannot see the first's pg_temp, so this checks the
    // DDL text itself rather than the table the other test made.
    let pool = connect().await.expect(
        "these tests need Postgres: set DATABASE_URL, or see docs/internals/quality-assurance.md",
    );
    sqlx::raw_sql(AssertSqlSafe(catalog.table().ddl()))
        .execute(&pool)
        .await
        .expect("applying the DDL");

    for (inline, key) in [
        (Some(vec![1_u8]), Some("a/key")),
        (None::<Vec<u8>>, None::<&str>),
    ] {
        let refused = sqlx::query(
            "INSERT INTO commit_graph_segments
             (scope, id, first_key, last_key, tier, bytes, inline, object_key)
             VALUES (1, gen_random_uuid(), 0, 1, 0, 1, $1, $2)",
        )
        .bind(inline.as_deref())
        .bind(key)
        .execute(&pool)
        .await;
        assert!(refused.is_err(), "a row must hold exactly one of the two");
    }
}
