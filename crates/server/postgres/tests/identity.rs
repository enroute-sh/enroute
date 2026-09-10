//! What only a real Postgres can answer for: the binary `COPY` that numbers
//! a push, and the driver error the race is read out of.
//!
//! Everything about behaviour is a unit test against the in-memory store.
//! What is left here is the mapping onto this driver, which is what makes
//! that store a stand-in for production rather than for itself.

use anyhow::Result;
use gix_hash::ObjectId;
use gix_object::Kind;
use sqlx::PgPool;

use enroute_git_core::{RepoId, oid};
use enroute_git_metadata::{Identity, Raced, Rows};

/// One dedicated connection, in a `pg_temp` schema of its own.
///
/// A Postgres that does not answer fails the suite rather than skipping it: a
/// test reporting `ok` having run nothing is worse than one that will not run.
async fn connect() -> Result<PgPool> {
    let pool = enroute_postgres::session_pool(&enroute_postgres::test_database_url()).await?;
    enroute_postgres::schema::apply(&pool).await?;
    Ok(pool)
}

/// `let rows = rows!();` — an empty identity table in a schema of its own.
macro_rules! rows {
    () => {
        Rows::new(std::sync::Arc::new(enroute_postgres::Postgres::new(
            connect().await.expect(
                "these tests need Postgres: set DATABASE_URL, or see docs/internals/quality-assurance.md",
            ),
        )))
    };
}

/// `n` oids, which need only differ from one another.
fn oids(n: u8) -> Vec<ObjectId> {
    (0..n).map(oid).collect()
}

/// `oids` of one kind, numbered from zero.
fn named(oids: &[ObjectId], kind: Kind) -> Vec<(ObjectId, Identity)> {
    oids.iter()
        .zip(0i64..)
        .map(|(&oid, seq)| (oid, Identity { seq, kind }))
        .collect()
}

async fn record(rows: &Rows, repo: RepoId, of: &[(ObjectId, Identity)]) -> Result<()> {
    rows.repo(repo).record(of).await
}

#[tokio::test]
async fn a_recorded_object_reads_back_as_what_it_was_called() {
    let rows = rows!();
    let repo = rows.create(None).await.expect("a repository").id;
    let oids = oids(64);
    let named = named(&oids, Kind::Tree);

    record(&rows, repo, &named).await.expect("recording");

    let read = rows.repo(repo).identify(&oids).await.expect("identifying");
    assert_eq!(read.len(), named.len());
    for (oid, identity) in &named {
        assert_eq!(read.get(oid), Some(identity), "for {oid}");
    }
}

/// Every kind is counted on its own, so one seq under four kinds is four
/// objects — which is what a copy writing its columns out of order loses.
#[tokio::test]
async fn one_seq_under_every_kind_is_four_objects() {
    let rows = rows!();
    let repo = rows.create(None).await.expect("a repository").id;
    let kinds = [Kind::Commit, Kind::Tree, Kind::Blob, Kind::Tag];
    let oids = oids(4);
    let named: Vec<(ObjectId, Identity)> = oids
        .iter()
        .zip(kinds)
        .map(|(&oid, kind)| (oid, Identity { seq: 7, kind }))
        .collect();

    record(&rows, repo, &named).await.expect("recording");

    let read = rows.repo(repo).identify(&oids).await.expect("identifying");
    for (oid, identity) in &named {
        assert_eq!(read.get(oid), Some(identity), "for {oid}");
    }
}

/// A duplicate oid here reads as the race ingest retries, with what Postgres
/// said still underneath it.
///
/// This is the one place a 23505 becomes that, and a copy error handed on as
/// itself would forward `source` past the [`sqlx::Error`] the mapping reads.
#[tokio::test]
async fn a_duplicate_oid_is_the_race_over_a_unique_violation() {
    let rows = rows!();
    let repo = rows.create(None).await.expect("a repository").id;
    let named = named(&oids(8), Kind::Blob);

    record(&rows, repo, &named).await.expect("recording");
    let err = record(&rows, repo, &named)
        .await
        .expect_err("a second recording of the same oids");

    assert!(
        err.downcast_ref::<Raced>().is_some(),
        "a lost race reads as a failed push: {err:?}"
    );
    let violation = err
        .chain()
        .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
        .and_then(sqlx::Error::as_database_error)
        .expect("a database error in the chain");
    assert_eq!(violation.code().as_deref(), Some("23505"), "{err:?}");
    assert!(
        violation
            .constraint()
            .is_some_and(|name| name.starts_with("object_seqs")),
        "constraint {:?} in {err:?}",
        violation.constraint(),
    );
}
