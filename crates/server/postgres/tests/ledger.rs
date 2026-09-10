//! The ledger against a real Postgres, in a `pg_temp` schema per test.

use anyhow::Result;
use bytes::Bytes;
use object_store::path::Path;
use sqlx::{AssertSqlSafe, PgPool, Row as _};

use enroute_git_core::{RepoId, Ulid};
use enroute_git_journal::{Index, Journal, Ledger as _};
use enroute_git_metadata::Rows;
use enroute_lattice_core::{Key, KeyRange, Tier};
use enroute_lattice_store::{Body, SegmentId, Written};
use enroute_postgres::PostgresLedger;

/// The engine's rows and all four catalog tables, in a schema of its own.
///
/// A Postgres that does not answer fails the suite rather than skipping it: a
/// test reporting `ok` having run nothing is worse than one that will not run.
async fn connect() -> Result<PgPool> {
    let pool = enroute_postgres::session_pool(&enroute_postgres::test_database_url()).await?;
    enroute_postgres::schema::apply(&pool).await?;
    Ok(pool)
}

/// `let (pool, repo) = fixture!();` — an empty engine and one repository.
macro_rules! fixture {
    () => {{
        let pool = connect().await.expect(
            "these tests need Postgres: set DATABASE_URL, or see docs/internals/quality-assurance.md",
        );
        let repo = Rows::new(std::sync::Arc::new(enroute_postgres::Postgres::new(
            pool.clone(),
        )))
        .create(None)
        .await
        .expect("a repository")
        .id;
        (pool, repo)
    }};
}

fn scope(repo: RepoId) -> enroute_lattice_store::Scope {
    enroute_lattice_store::Scope {
        id: repo.as_i64(),
        prefix: Path::from("test"),
    }
}

/// One inlined segment, named `id`, covering a key range of its own.
fn segment(id: SegmentId, first: u64) -> Result<Written> {
    Ok(Written {
        id,
        range: KeyRange::new(Key::new(first), Key::new(first + 1))?,
        tier: Tier::ZERO,
        body: Body::Inline(Bytes::from_static(b"segment")),
        bytes: 7,
    })
}

async fn listed_in(pool: &PgPool, index: Index) -> Result<i64> {
    // The table is named by `Index`, an enum of this workspace's own.
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT count(*) AS n FROM {}",
        enroute_postgres::schema::table(index)
    )))
    .fetch_one(pool)
    .await?;
    Ok(row.try_get("n")?)
}

async fn registered(pool: &PgPool) -> Result<i64> {
    let row = sqlx::query("SELECT count(*) AS n FROM commit_segments")
        .fetch_one(pool)
        .await?;
    Ok(row.try_get("n")?)
}

#[tokio::test]
async fn one_journal_lands_every_list_it_touches() {
    let (pool, repo) = fixture!();
    let ledger = PostgresLedger::new(pool.clone());

    let mut journal = Journal::new();
    for (offset, index) in Index::ALL.into_iter().enumerate() {
        let first = u64::try_from(offset).expect("an offset") * 10;
        journal.list(
            index,
            &scope(repo),
            segment(SegmentId::fresh(), first).expect("a segment"),
        );
    }
    journal.register([Ulid(1), Ulid(2)]);

    ledger.commit(repo, &journal).await.expect("committing");

    for index in Index::ALL {
        assert_eq!(
            listed_in(&pool, index).await.expect("counting"),
            1,
            "the {index:?} list"
        );
    }
    assert_eq!(registered(&pool).await.expect("counting"), 2);
}

/// The whole reason this is one journal rather than four writes.
///
/// A commit listed in the graph while its objects are not is a commit every
/// later push reads as recorded and never records the objects of.
#[tokio::test]
async fn a_journal_that_cannot_land_whole_lands_nothing() {
    let (pool, repo) = fixture!();
    let ledger = PostgresLedger::new(pool.clone());

    // The same name twice in one list, which the catalog's primary key
    // refuses — with a listing in another list, and a registration, before it.
    let clash = SegmentId::fresh();
    let mut journal = Journal::new();
    journal.list(
        Index::Trees,
        &scope(repo),
        segment(SegmentId::fresh(), 0).expect("a segment"),
    );
    journal.list(
        Index::CommitGraph,
        &scope(repo),
        segment(clash, 10).expect("a segment"),
    );
    journal.list(
        Index::CommitGraph,
        &scope(repo),
        segment(clash, 20).expect("a segment"),
    );
    journal.register([Ulid(3)]);

    ledger
        .commit(repo, &journal)
        .await
        .expect_err("a duplicate segment name");

    for index in Index::ALL {
        assert_eq!(
            listed_in(&pool, index).await.expect("counting"),
            0,
            "the {index:?} list kept half a write"
        );
    }
    assert_eq!(
        registered(&pool).await.expect("counting"),
        0,
        "a pack image was kept alive by a write that never landed"
    );
}

/// A push whose objects were all already recorded still reaches the ledger.
#[tokio::test]
async fn an_empty_journal_is_not_a_transaction() {
    let (pool, repo) = fixture!();
    let ledger = PostgresLedger::new(pool.clone());

    ledger
        .commit(repo, &Journal::new())
        .await
        .expect("committing nothing");

    assert_eq!(registered(&pool).await.expect("counting"), 0);
}

/// The gather's shape: its rows land in the transaction holding its lock.
#[tokio::test]
async fn a_journal_lands_in_a_transaction_the_caller_holds() {
    let (pool, repo) = fixture!();
    let ledger = PostgresLedger::new(pool.clone());

    let mut journal = Journal::new();
    journal.list(
        Index::CommitPacks,
        &scope(repo),
        segment(SegmentId::fresh(), 0).expect("a segment"),
    );
    journal.register([Ulid(4)]);
    journal.retire([Ulid(4)]);

    let mut tx = pool.begin().await.expect("a transaction");
    ledger
        .commit_in(&mut tx, repo, &journal)
        .await
        .expect("landing in the caller's transaction");
    tx.rollback().await.expect("rolling back");

    assert_eq!(
        listed_in(&pool, Index::CommitPacks)
            .await
            .expect("counting"),
        0,
        "the caller's rollback did not take the journal with it"
    );
}
