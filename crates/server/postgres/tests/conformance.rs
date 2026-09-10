//! Every case against Postgres and against memory, in that order.
//!
//! This is what makes the in-memory store a stand-in for production rather
//! than for itself: a case the two answer differently fails here. It found one
//! already — memory counted in whole seconds, so a grace window of none
//! answered differently from a `timestamptz`.
//!
//! # Running it
//! Needs a database, so it sits behind `postgres-tests` and a plain
//! `cargo test` does not build it. See `docs/internals/quality-assurance.md`.

#![allow(
    clippy::print_stdout,
    reason = "the harness shows a failing test's output, so which store was \
              answering is printed before the panic"
)]

use anyhow::Result;

use enroute_git_metadata::{Rows, conformance};
use enroute_postgres::test_database_url;

/// The engine's rows in a `pg_temp` schema of this connection's own.
async fn postgres() -> Result<Rows> {
    let pool = enroute_postgres::session_pool(&test_database_url()).await?;
    enroute_postgres::schema::apply(&pool).await?;
    Ok(Rows::new(std::sync::Arc::new(
        enroute_postgres::Postgres::new(pool),
    )))
}

#[tokio::test]
async fn both_stores_meet_the_contract() {
    // Printed rather than returned: the harness shows a failing test's
    // output, so a panic below is preceded by which store was answering and
    // which case it was on.
    println!("against Postgres");
    let rows = postgres()
        .await
        .expect("a Postgres: set DATABASE_URL, or see docs/internals/quality-assurance.md");
    conformance::check(&rows).await;

    println!("against memory");
    conformance::check(&Rows::in_memory()).await;
}
