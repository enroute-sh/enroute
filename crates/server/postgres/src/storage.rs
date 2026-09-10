//! Every layer of the engine, assembled over one connection pool.
//!
//! The engine's own assembly takes a row store and a ledger and asks neither
//! what it is kept in; this is where those two are the ones that speak SQL.

use std::sync::Arc;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use sqlx::PgPool;

use enroute_git_metadata::Rows;
use enroute_git_retrieve::Storage;
use enroute_git_store::Store;

use crate::{Postgres, PostgresLedger};

/// One pinned connection, resolving unqualified names in its own `pg_temp`.
///
/// Pinned because `pg_temp` belongs to one backend session: a second
/// connection would not see what the first built.
///
/// # Why `pg_temp`
/// Rather than a `CREATE TEMPORARY TABLE` in a step, which would break that
/// step as production DDL.
///
/// # Errors
/// Whatever connecting said.
pub async fn session_pool(database_url: &str) -> Result<PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect(database_url)
        .await
        .context("connecting for a session-local schema")?;
    sqlx::query("SET search_path TO pg_temp")
        .execute(&pool)
        .await
        .context("setting search_path to pg_temp")?;
    Ok(pool)
}

/// Assemble every layer over `pool`, with segments and bytes in `bucket`.
#[must_use]
pub fn storage(pool: &PgPool, bucket: Arc<dyn ObjectStore>, store: Arc<Store>) -> Storage {
    Storage::assemble(
        Rows::new(Arc::new(Postgres::new(pool.clone()))),
        Arc::new(PostgresLedger::new(pool.clone())),
        bucket,
        store,
    )
}

/// Open one dedicated connection to `database_url` and scope every layer to
/// its private `pg_temp` schema.
///
/// The pool comes back beside the storage, for a caller wanting to read the
/// same tables directly — every layer holds one of its own.
///
/// # Errors
/// Whatever connecting or applying the DDL said.
pub async fn ephemeral(database_url: &str) -> Result<(Storage, PgPool)> {
    let pool = session_pool(database_url).await?;
    crate::schema::apply(&pool).await?;
    let assembled = storage(
        &pool,
        Arc::new(InMemory::new()),
        Arc::new(Store::new(Arc::new(InMemory::new()))),
    );
    Ok((assembled, pool))
}
