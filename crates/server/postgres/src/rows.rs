//! The engine's rows in Postgres.
//!
//! Every statement is here and nowhere above: what the layer asks for is
//! [`Metadata`], and this is one answer to it.
//!
//! # Where the tables are
//! Named unqualified, so the connection's `search_path` decides the schema.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use gix_hash::ObjectId;
use gix_object::Kind;
use sqlx::{PgConnection, PgPool, Row};
use sqlx_pg_copy::CopyIn;

use enroute_git_core::{ObjectHashMap, RepoId, Ulid, kind_from_u8, kind_to_u8};

use enroute_git_metadata::{
    Identity, Metadata, Raced, RefEntry, RefUpdate, RefUpdateResult, RefsMap, RepoMetadata,
    RepoSummary,
};

use crate::pg_oid::PgOid;

/// Every kind that is counted, which is every kind there is.
const COUNTED: [Kind; 4] = [Kind::Commit, Kind::Tree, Kind::Blob, Kind::Tag];

/// The engine's rows, in a Postgres.
#[derive(Debug, Clone)]
pub struct Postgres {
    pool: PgPool,
}

impl Postgres {
    /// Rows in `pool`.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Metadata for Postgres {
    async fn create(&self, default_branch: Option<&str>) -> Result<RepoMetadata> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("beginning create_repository transaction")?;
        let repo = crate::repos::insert(&mut tx, default_branch).await?;
        // In the same transaction as the row: a repository whose counters
        // never landed exists and cannot take a push.
        insert_counters(&mut tx, repo.id).await?;
        tx.commit()
            .await
            .context("committing create_repository transaction")?;
        Ok(repo)
    }

    async fn all(&self) -> Result<Vec<RepoMetadata>> {
        crate::repos::all(&self.pool).await
    }

    async fn deleted(&self, grace_secs: u64) -> Result<Vec<RepoMetadata>> {
        crate::repos::deleted(&self.pool, grace_secs).await
    }

    async fn create_counters(&self, repo: RepoId) -> Result<()> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .context("a connection to create counters on")?;
        insert_counters(&mut conn, repo).await
    }

    async fn allocate(&self, repo: RepoId, kind: Kind, count: u64) -> Result<i64> {
        if count == 0 {
            return Err(anyhow!("a push allocating no {kind} seqs"));
        }
        let count = i64::try_from(count).context("an object batch past a bigint")?;
        let first: Option<i64> = sqlx::query_scalar(
            "UPDATE repo_object_seq SET next_seq = next_seq + $3 \
             WHERE repo_id = $1 AND kind = $2 RETURNING next_seq - $3",
        )
        .bind(repo.as_i64())
        .bind(column(kind))
        .bind(count)
        .fetch_optional(&self.pool)
        .await
        .context("allocating object seqs")?;

        first.ok_or_else(|| anyhow!("repository {repo} has no {kind} counter"))
    }

    async fn record(&self, repo: RepoId, named: &[(ObjectId, Identity)]) -> Result<()> {
        if named.is_empty() {
            return Ok(());
        }
        let mut conn = self
            .pool
            .acquire()
            .await
            .context("a connection to number objects on")?;
        copy_identities(&mut conn, repo.as_i64(), named)
            .await
            .map_err(copy_error)
            .map_err(as_race)
            .context("recording object identities")
    }

    #[tracing::instrument(
        level = "debug",
        name = "enroute_git_metadata::identify",
        skip_all,
        fields(repo_id = %repo, oid_count = oids.len())
    )]
    async fn identify(&self, repo: RepoId, oids: &[ObjectId]) -> Result<ObjectHashMap<Identity>> {
        if oids.is_empty() {
            return Ok(ObjectHashMap::default());
        }
        let wanted: Vec<Vec<u8>> = oids.iter().map(|oid| oid.as_bytes().to_vec()).collect();
        let rows = sqlx::query(
            "SELECT oid, kind, seq FROM object_seqs WHERE repo_id = $1 AND oid = ANY($2)",
        )
        .bind(repo.as_i64())
        .bind(&wanted)
        .fetch_all(&self.pool)
        .await
        .context("resolving object oids to seqs")?;

        let mut found = ObjectHashMap::default();
        for row in rows {
            found.insert(oid_at(&row, 0)?, identity_of(&row)?);
        }
        Ok(found)
    }

    async fn oids_of(
        &self,
        repo: RepoId,
        kind: Kind,
        seqs: &[i64],
    ) -> Result<HashMap<i64, ObjectId>> {
        if seqs.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query(
            "SELECT seq, oid FROM object_seqs \
             WHERE repo_id = $1 AND kind = $2 AND seq = ANY($3)",
        )
        .bind(repo.as_i64())
        .bind(column(kind))
        .bind(seqs)
        .fetch_all(&self.pool)
        .await
        .context("resolving object seqs to oids")?;

        let mut found = HashMap::with_capacity(rows.len());
        for row in rows {
            found.insert(row.try_get::<i64, _>(0)?, oid_at(&row, 1)?);
        }
        Ok(found)
    }

    async fn lookup(&self, repo: RepoId) -> Result<Option<RepoMetadata>> {
        crate::repos::by_id(&self.pool, repo).await
    }

    async fn summarize(&self, repos: &[RepoId]) -> Result<Vec<RepoSummary>> {
        crate::repos::summarize(&self.pool, repos).await
    }

    async fn mark_deleted(&self, repo: RepoId) -> Result<bool> {
        crate::repos::mark_deleted(&self.pool, repo).await
    }

    async fn ref_listing(&self, repo: RepoId) -> Result<Vec<RefEntry>> {
        crate::repos::ref_listing(&self.pool, repo).await
    }

    async fn referenced_segments(&self, repo: RepoId, ids: &[Ulid]) -> Result<HashSet<Ulid>> {
        crate::repos::referenced_segments(&self.pool, repo, ids).await
    }

    async fn live_segments(&self, repo: RepoId, ids: &[Ulid]) -> Result<HashSet<Ulid>> {
        crate::repos::live_segments(&self.pool, repo, ids).await
    }

    async fn drop_retired_segments(&self, repo: RepoId, grace_secs: u64) -> Result<u64> {
        crate::repos::drop_retired_segments(&self.pool, repo, grace_secs).await
    }

    async fn refs_for(&self, repo: &RepoMetadata) -> Result<RefsMap> {
        crate::refs::get_refs_for(&self.pool, repo).await
    }

    async fn refs_matching(&self, repo: RepoId, refnames: &[&str]) -> Result<RefsMap> {
        crate::refs::get_refs_matching(&self.pool, repo, refnames).await
    }

    async fn update_refs(
        &self,
        repo: RepoId,
        updates: &[RefUpdate],
    ) -> Result<Vec<RefUpdateResult>> {
        crate::refs::update_refs(&self.pool, repo, updates).await
    }
}

/// One counter per kind for `repo`, leaving any that are already there.
async fn insert_counters(conn: &mut PgConnection, repo: RepoId) -> Result<()> {
    let kinds: Vec<i16> = COUNTED.iter().copied().map(column).collect();
    sqlx::query(
        "INSERT INTO repo_object_seq (repo_id, kind) \
         SELECT $1, * FROM unnest($2::smallint[]) \
         ON CONFLICT (repo_id, kind) DO NOTHING",
    )
    .bind(repo.as_i64())
    .bind(&kinds)
    .execute(conn)
    .await
    .context("creating a repository's object-seq counters")?;
    Ok(())
}

/// A kind as the identity column holds it.
fn column(kind: Kind) -> i16 {
    i16::from(kind_to_u8(kind))
}

/// Every row of `named` into `object_seqs`, as one binary copy.
async fn copy_identities(
    tx: &mut PgConnection,
    repo_id: i64,
    named: &[(ObjectId, Identity)],
) -> sqlx_pg_copy::Result<()> {
    let mut copy = CopyIn::begin(
        tx,
        "COPY object_seqs (repo_id, oid, kind, seq) FROM STDIN WITH (FORMAT binary)",
    )
    .await?;
    for &(oid, held) in named {
        let oid = PgOid::from(oid);
        let kind = column(held.kind);
        copy.write_row(|row| {
            row.push_value(&repo_id)?;
            row.push_value(&oid)?;
            row.push_value(&kind)?;
            row.push_value(&held.seq)
        })
        .await?;
    }
    copy.finish().await?;
    Ok(())
}

/// A copy error as an [`anyhow::Error`], unwrapping a driver one to itself.
///
/// `Error::Sqlx` is `#[error(transparent)]`, so it forwards `source` past the
/// [`sqlx::Error`] this reads a unique violation back out of.
fn copy_error(err: sqlx_pg_copy::Error) -> anyhow::Error {
    match err {
        sqlx_pg_copy::Error::Sqlx(err) => anyhow::Error::new(err),
        err => anyhow::Error::new(err),
    }
}

/// The same failure, saying [`Raced`] when that is what it was.
///
/// Named here, where the driver is, so a caller asks what happened rather
/// than what SQLSTATE Postgres uses for it.
fn as_race(err: anyhow::Error) -> anyhow::Error {
    if identity_violation(&err) {
        return err.context(Raced);
    }
    err
}

/// Whether `err` is a unique violation on the identity table.
fn identity_violation(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<sqlx::Error>())
        .filter_map(sqlx::Error::as_database_error)
        .any(|db| {
            db.code().as_deref() == Some("23505")
                && db
                    .constraint()
                    .is_some_and(|name| name.starts_with("object_seqs"))
        })
}

/// One row's kind and seq.
fn identity_of(row: &sqlx::postgres::PgRow) -> Result<Identity> {
    let kind: i16 = row.try_get("kind")?;
    Ok(Identity {
        seq: row.try_get("seq")?,
        kind: u8::try_from(kind)
            .ok()
            .and_then(kind_from_u8)
            .context("an object of no known kind")?,
    })
}

/// One `bytea` column as an oid.
fn oid_at(row: &sqlx::postgres::PgRow, at: usize) -> Result<ObjectId> {
    let bytes: Vec<u8> = row.try_get(at)?;
    ObjectId::try_from(bytes.as_slice()).context("an object oid that is not twenty bytes")
}
