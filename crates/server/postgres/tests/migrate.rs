//! The real schema against a real Postgres.
//!
//! What a unit test cannot reach: that the list applies once, that a second
//! run does nothing, that an edited step is refused, and that two writers
//! racing leave one schema rather than two half-built ones.

// Every schema name below is this file's own, built from a literal and the
// process id.
use sqlx::postgres::PgPoolOptions;
use sqlx::{AssertSqlSafe, PgPool};

use enroute_postgres::schema;
use enroute_postgres::test_database_url;

/// Every step of the real list, as an operator reads them.
const EVERY_STEP: [&str; 5] = [
    "0001_engine_rows",
    "0002_commit_graph_catalogs",
    "0003_object_index_catalogs",
    "0004_tenancy",
    "0005_repository_keys",
];

/// One dedicated connection in its own `pg_temp`.
///
/// `pg_temp` is bound to one backend session, so the pool holds exactly one
/// connection for its whole life and two tests never see each other's tables.
async fn connect() -> Result<PgPool, sqlx::Error> {
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect(&test_database_url())
        .await?;
    sqlx::query("SET search_path TO pg_temp")
        .execute(&pool)
        .await?;
    Ok(pool)
}

/// `let pool = pool!();` — a private schema of this test's own.
macro_rules! pool {
    () => {
        connect().await.expect(
            "these tests need Postgres: set DATABASE_URL, or see docs/internals/quality-assurance.md",
        )
    };
}

/// Whether `table` exists where this connection resolves names.
async fn exists(pool: &PgPool, table: &str) -> Result<bool, sqlx::Error> {
    let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(table)
        .fetch_one(pool)
        .await?;
    Ok(found.is_some())
}

#[tokio::test]
async fn a_first_run_applies_every_step_and_a_second_applies_none() {
    let pool = pool!();

    let applied = schema::apply(&pool).await.expect("a first run");
    assert_eq!(applied, EVERY_STEP);
    assert!(
        exists(&pool, "repositories")
            .await
            .expect("asking for a table")
    );

    let again = schema::apply(&pool).await.expect("a second run");
    assert!(
        again.is_empty(),
        "a step that ran must never run again: {again:?}"
    );
}

#[tokio::test]
async fn a_database_with_no_ledger_has_run_nothing() {
    let pool = pool!();

    assert_eq!(
        schema::pending(&pool).await.expect("asking a bare schema"),
        EVERY_STEP
    );
    let error = schema::verify(&pool).await.expect_err("a bare schema");
    let said = error.to_string();
    assert!(
        said.contains("0001_engine_rows")
            && said.contains(&format!("{} step(s)", EVERY_STEP.len())),
        "expected the outstanding step to be named, got {said}"
    );
}

#[tokio::test]
async fn a_caught_up_database_verifies_and_has_nothing_pending() {
    let pool = pool!();
    schema::apply(&pool).await.expect("a first run");

    assert!(
        schema::pending(&pool)
            .await
            .expect("asking a built schema")
            .is_empty()
    );
    schema::verify(&pool).await.expect("a caught-up schema");
}

// The promise a deployment reads the ledger for: a step whose bytes changed
// after it ran stops the process and names the file, rather than running the
// new text against a database built by the old.
#[tokio::test]
async fn a_step_edited_after_it_ran_is_refused() {
    let pool = pool!();
    schema::apply(&pool).await.expect("a first run");

    // What editing the file would do, done to the ledger instead: the file is
    // history and a test must not rewrite it to prove that.
    sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = 1")
        .bind(vec![0_u8; 48])
        .execute(&pool)
        .await
        .expect("tampering with a recorded checksum");

    let error = schema::apply(&pool).await.expect_err("an edited step");
    let said = error.to_string();
    assert!(
        said.contains("0001_engine_rows"),
        "expected the edit to name the file, got {said}"
    );
}

// Two servers starting together is the ordinary case, not the exotic one:
// stateless compute means replicas, and they boot at once.
#[tokio::test]
async fn two_writers_of_one_schema_apply_it_once() {
    let shared = pool!();

    // A named schema rather than `pg_temp`, since a temporary one belongs to
    // one session and could not have a second writer to race.
    let schema = format!("migrate_race_{}", std::process::id());
    scrub(&shared, &schema).await.expect("a clean start");
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&shared)
        .await
        .expect("a schema to race in");

    let one = scoped(&schema).await.expect("a scoped pool");
    let two = scoped(&schema).await.expect("a scoped pool");
    let (first, second) = tokio::join!(schema::apply(&one), schema::apply(&two));
    first.expect("one racer");
    second.expect("the other");

    // The claim is about the database, not about which racer says it ran the
    // steps: the lock is what stops the list being applied twice.
    let recorded: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT count(*) FROM {schema}._sqlx_migrations"
    )))
    .fetch_one(&shared)
    .await
    .expect("counting the ledger");
    assert_eq!(
        recorded,
        i64::try_from(EVERY_STEP.len()).expect("a list that fits"),
        "the list must be recorded exactly once"
    );

    scrub(&shared, &schema).await.expect("cleaning up");
}

/// A pool resolving unqualified names in `schema`, as a deployment's URL does.
async fn scoped(schema: &str) -> Result<PgPool, sqlx::Error> {
    let named = schema.to_owned();
    PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |conn, _meta| {
            let named = named.clone();
            Box::pin(async move {
                sqlx::query(AssertSqlSafe(format!("SET search_path TO {named}")))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&test_database_url())
        .await
}

/// Drops `schema` if it is there, so a rerun starts from nothing.
async fn scrub(pool: &PgPool, schema: &str) -> Result<(), sqlx::Error> {
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(pool)
    .await
    .map(drop)
}
