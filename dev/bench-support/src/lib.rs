//! Shared setup for the dev-only harnesses.
//!
//! A scratch Postgres schema and a router served on a random port, plus
//! [`store`], the object store they measure against, [`collect`], which reads
//! their spans back, and [`hooks`], the application Enroute asks. For the one
//! that drives Enroute over the wire (`load-test`), the one that measures
//! ingest in process (`push-bench`), and the end-to-end suite (`e2e`).
#![allow(
    clippy::expect_used,
    clippy::print_stderr,
    reason = "dev harness support code, never compiled into the production build; \
              panicking on bind failure is fine here, and a failed cleanup is a \
              minor annoyance in a manually-invoked tool, not worth aborting for"
)]

pub mod collect;
pub mod hooks;
pub mod store;

use std::net::SocketAddr;

use anyhow::Result;
// The schema name is this helper's own: a caller's prefix and a fresh UUID,
// never anything a request carries.
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};

use enroute_postgres::test_database_url;

/// Creates a fresh, isolated Postgres schema (`{prefix}_<uuid>`) and a pool
/// of `max_connections` scoped to it.
///
/// The schema is empty: every layer's tables come from `Storage::migrate`,
/// which the caller runs once it has built one. Three layers write here.
///
/// # Errors
/// Returns an error if connecting or creating the schema fails.
pub async fn scratch_metadata_pool(prefix: &str, max_connections: u32) -> Result<(PgPool, String)> {
    let database_url = test_database_url();
    let schema = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let schema_for_hook = schema.clone();
    let pool = PgPoolOptions::new()
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
    Ok((pool, schema))
}

/// Drop `schema` (and everything in it), warning on stderr rather than
/// failing — a leaked scratch schema is a minor annoyance, not an error.
pub async fn drop_scratch_schema(pool: &PgPool, schema: &str) {
    if let Err(e) = sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS {schema} CASCADE"
    )))
    .execute(pool)
    .await
    {
        eprintln!("warning: failed to drop scratch schema {schema}: {e}");
    }
}

/// Binds `router` on `bind` (`"127.0.0.1:0"` picks an OS-assigned port),
/// serves it in the background, and returns the bound address.
///
/// For a caller with no reason to know the port before building the
/// router; whoever does wants [`serve_listener_in_background`].
///
/// # Panics
/// Panics if binding the listener fails.
pub async fn serve_in_background(router: axum::Router, bind: SocketAddr) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(bind).await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    serve_listener_in_background(router, listener);
    addr
}

/// Serves on a listener the caller already bound.
///
/// For a router that had to know its own address before it could be built — the
/// hook endpoint does, to check signatures against the URL.
///
/// # Panics
/// Panics if serving fails, which a harness cannot proceed without.
pub fn serve_listener_in_background(router: axum::Router, listener: tokio::net::TcpListener) {
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
}
