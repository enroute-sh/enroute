//! Which repositories exist, and which segment objects hold their packs.
//!
//! Rows and nothing else: no segment, no bucket, no seq. A repository row
//! outlives everything that locates against it, which is why deleting one is
//! a flag here and a sweep elsewhere.

use std::collections::HashSet;

use anyhow::{Context, Result};
use gix_hash::ObjectId;
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Row};
use uuid::Uuid;

use enroute_git_core::{RepoId, StorageKey, Ulid};

use enroute_git_metadata::{RefEntry, RepoMetadata, RepoSummary};

/// Every table keyed by `repo_id` that this crate owns, ordered so each
/// delete runs after whatever references it.
///
/// `branches` names `object_seqs`, so refs go first. Not `repositories`,
/// which outlives what it locates, nor the self-purging catalogs.
const REPO_TABLES: [&str; 5] = [
    "branches",
    "refs",
    "commit_segments",
    "object_seqs",
    "repo_object_seq",
];

/// The three columns every repository lookup selects, in that order.
fn into_repo_metadata((id, storage_key, default_branch): (i64, Uuid, String)) -> RepoMetadata {
    RepoMetadata {
        id: RepoId::new(id),
        storage_key: StorageKey::from(storage_key),
        default_branch,
    }
}

/// Insert a repository row.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn insert(
    tx: &mut PgConnection,
    default_branch: Option<&str>,
) -> Result<RepoMetadata> {
    let row: (i64, Uuid, String) = sqlx::query_as(
        "INSERT INTO repositories (default_branch) \
         VALUES (COALESCE($1::text, 'refs/heads/main')) \
         RETURNING id, storage_key, default_branch",
    )
    .bind(default_branch)
    .fetch_one(&mut *tx)
    .await
    .context("inserting repository row")?;
    Ok(into_repo_metadata(row))
}

/// Mark a repository deleted, reporting whether this call is what did it.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn mark_deleted(pool: &PgPool, id: RepoId) -> Result<bool> {
    let deleted = sqlx::query(
        "UPDATE repositories SET deleted_at = now() \
         WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(id.as_i64())
    .execute(pool)
    .await
    .context("marking repository deleted")?;
    Ok(deleted.rows_affected() > 0)
}

/// One repository, or `None` if it is absent or deleted.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn by_id(pool: &PgPool, id: RepoId) -> Result<Option<RepoMetadata>> {
    let row: Option<(i64, Uuid, String)> = sqlx::query_as(
        "SELECT id, storage_key, default_branch FROM repositories \
         WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(id.as_i64())
    .fetch_optional(pool)
    .await
    .context("reading repo")?;
    Ok(row.map(into_repo_metadata))
}

/// Every repository not deleted.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn all(pool: &PgPool) -> Result<Vec<RepoMetadata>> {
    let rows: Vec<(i64, Uuid, String)> = sqlx::query_as(
        "SELECT id, storage_key, default_branch FROM repositories \
         WHERE deleted_at IS NULL",
    )
    .fetch_all(pool)
    .await
    .context("listing all repositories")?;
    Ok(rows.into_iter().map(into_repo_metadata).collect())
}

/// Those of `ids` still present, oldest first, each with its last push.
///
/// One query, not a [`by_id`] and a branch read each; left because a
/// repository never pushed to has no `branches` row.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn summarize(pool: &PgPool, ids: &[RepoId]) -> Result<Vec<RepoSummary>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let wanted: Vec<i64> = ids.iter().copied().map(RepoId::as_i64).collect();
    let rows: Vec<(i64, Uuid, String, Option<i64>)> = sqlx::query_as(
        "SELECT r.id, r.storage_key, r.default_branch, \
                EXTRACT(EPOCH FROM b.updated_at)::bigint \
         FROM repositories r \
         LEFT JOIN branches b ON b.repo_id = r.id AND b.refname = r.default_branch \
         WHERE r.id = ANY($1) AND r.deleted_at IS NULL \
         ORDER BY r.id",
    )
    .bind(&wanted)
    .fetch_all(pool)
    .await
    .context("summarizing repositories")?;

    Ok(rows
        .into_iter()
        .map(
            |(id, storage_key, default_branch, last_push_unix_seconds)| RepoSummary {
                repo: into_repo_metadata((id, storage_key, default_branch)),
                last_push_unix_seconds,
            },
        )
        .collect())
}

/// Every repository deleted longer ago than `grace_secs`.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn deleted(pool: &PgPool, grace_secs: u64) -> Result<Vec<RepoMetadata>> {
    let rows: Vec<(i64, Uuid, String)> = sqlx::query_as(
        "SELECT id, storage_key, default_branch FROM repositories \
         WHERE deleted_at < now() - ($1::bigint * interval '1 second') \
         ORDER BY deleted_at",
    )
    // A window past `i64::MAX` seconds saturates rather than wrapping, which
    // errs towards never reclaiming instead of reclaiming at once.
    .bind(i64::try_from(grace_secs).unwrap_or(i64::MAX))
    .fetch_all(pool)
    .await
    .context("listing deleted repositories")?;
    Ok(rows.into_iter().map(into_repo_metadata).collect())
}

/// Every ref of a repository, branches and everything else alike.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn ref_listing(pool: &PgPool, repo_id: RepoId) -> Result<Vec<RefEntry>> {
    let rows: Vec<(String, Vec<u8>, i64)> = sqlx::query_as(
        "SELECT refname, oid, EXTRACT(EPOCH FROM updated_at)::bigint FROM branches \
         WHERE repo_id = $1 \
         UNION ALL \
         SELECT refname, oid, EXTRACT(EPOCH FROM updated_at)::bigint FROM refs \
         WHERE repo_id = $1",
    )
    .bind(repo_id.as_i64())
    .fetch_all(pool)
    .await
    .context("listing refs")?;

    Ok(rows
        .into_iter()
        .map(|(refname, oid, updated_unix_seconds)| RefEntry {
            refname,
            oid: ObjectId::from_bytes_or_panic(&oid),
            updated_unix_seconds,
        })
        .collect())
}

/// Which of `ids` this repository still names as holding a pack image.
///
/// Retired ones count: the row is what keeps the sweep off an object a
/// reader may still be inside. [`live_segments`] is the other question.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn referenced_segments(
    pool: &PgPool,
    repo_id: RepoId,
    ids: &[Ulid],
) -> Result<HashSet<Ulid>> {
    matching_segments(pool, repo_id, ids, "")
        .await
        .context("querying referenced segments")
}

/// Which of `ids` still hold an image nothing has copied elsewhere.
///
/// What a gather may take: a retired segment's images already live in the
/// copy that retired it, so taking it again would copy dead bytes.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn live_segments(
    pool: &PgPool,
    repo_id: RepoId,
    ids: &[Ulid],
) -> Result<HashSet<Ulid>> {
    matching_segments(pool, repo_id, ids, "AND retired_at IS NULL")
        .await
        .context("querying live segments")
}

/// Which of `ids` this repository's segment rows name, under one more
/// condition.
///
/// The two questions above differ by that condition alone, and `narrower` is
/// this module's own text rather than anything a caller composes.
async fn matching_segments(
    pool: &PgPool,
    repo_id: RepoId,
    ids: &[Ulid],
    narrower: &str,
) -> Result<HashSet<Ulid>> {
    if ids.is_empty() {
        return Ok(HashSet::new());
    }
    let rows = sqlx::query(AssertSqlSafe(format!(
        "SELECT segment_id FROM commit_segments \
         WHERE repo_id = $1 AND segment_id = ANY($2) {narrower}"
    )))
    .bind(repo_id.as_i64())
    .bind(encoded(ids))
    .fetch_all(pool)
    .await?;
    rows.iter().map(segment_id).collect()
}

/// Segment ids as the `bytea[]` the segment rows are keyed by.
fn encoded(ids: &[Ulid]) -> Vec<Vec<u8>> {
    ids.iter().map(|id| id.to_bytes().to_vec()).collect()
}

/// Register the segment objects a push's fresh packs were appended to.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn register_segments(
    tx: &mut PgConnection,
    repo_id: RepoId,
    ids: &[Ulid],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO commit_segments (repo_id, segment_id) \
         SELECT $1, * FROM unnest($2::bytea[]) \
         ON CONFLICT (repo_id, segment_id) DO NOTHING",
    )
    .bind(repo_id.as_i64())
    .bind(encoded(ids))
    .execute(tx)
    .await
    .context("registering commit segments")?;
    Ok(())
}

/// Mark the segment objects whose images were copied away.
///
/// Not a delete: the sweep keys off the row, and a reader that composed the
/// index before the gather is still reading ranges out of this object.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn retire_segments(
    tx: &mut PgConnection,
    repo_id: RepoId,
    ids: &[Ulid],
) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE commit_segments SET retired_at = now() \
         WHERE repo_id = $1 AND segment_id = ANY($2) AND retired_at IS NULL",
    )
    .bind(repo_id.as_i64())
    .bind(encoded(ids))
    .execute(tx)
    .await
    .context("retiring commit segments")?;
    Ok(())
}

/// Drop the rows of segments retired longer than `grace_secs` ago, and
/// return how many went.
///
/// What hands an object to the sweep: until the row goes the anti-join calls
/// it referenced, and the window between the two is the reader's.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn drop_retired_segments(
    pool: &PgPool,
    repo_id: RepoId,
    grace_secs: u64,
) -> Result<u64> {
    let seconds = i64::try_from(grace_secs).unwrap_or(i64::MAX);
    let dropped = sqlx::query(
        "DELETE FROM commit_segments \
         WHERE repo_id = $1 \
           AND retired_at IS NOT NULL \
           AND retired_at < now() - make_interval(secs => $2::bigint)",
    )
    .bind(repo_id.as_i64())
    .bind(seconds)
    .execute(pool)
    .await
    .context("dropping retired commit segments")?;
    Ok(dropped.rows_affected())
}

/// Whether this transaction now holds the repository's maintenance lock.
///
/// Two gathers at once copy the same images twice, and the copy the join
/// drops is bytes nothing ever stops naming. Refused rather than waited on.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn hold_maintenance_lock(
    tx: &mut PgConnection,
    repo: &RepoMetadata,
) -> Result<bool> {
    // Keyed by the storage key, not the id: an advisory lock is the whole
    // database's, while `repositories.id` restarts per schema — so two
    // schemas' first repositories would take each other's lock. The key is
    // random and unique wherever the same database serves both.
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(lock_key(repo))
        .fetch_one(tx)
        .await
        .context("taking the maintenance lock")
}

/// A repository's storage key as the one integer an advisory lock takes.
///
/// The high half of a v4 uuid, which is 61 random bits once its version and
/// variant are excluded — enough that a collision costs a skipped gather.
fn lock_key(repo: &RepoMetadata) -> i64 {
    let key = Uuid::from(repo.storage_key);
    let (high, _low) = key.as_u64_pair();
    i64::from_ne_bytes(high.to_ne_bytes())
}

/// Drop every row the repository holds, keeping the row that names it.
///
/// Takes the caller's transaction: this is half of a write the ledger lands
/// across two stores, not a question this one answers on its own.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn purge_rows(tx: &mut PgConnection, repo: RepoId) -> Result<()> {
    for table in REPO_TABLES {
        // The table names are this constant's, never a caller's, so there is
        // nothing here for a format to interpolate unsafely.
        sqlx::query(AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE repo_id = $1"
        )))
        .bind(repo.as_i64())
        .execute(&mut *tx)
        .await
        .with_context(|| format!("purging {table}"))?;
    }
    Ok(())
}

/// Erase the row that names the repository.
///
/// Last of everything: every other table keyed by a repository points at this
/// one.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn erase(tx: &mut PgConnection, repo: RepoId) -> Result<()> {
    sqlx::query("DELETE FROM repositories WHERE id = $1")
        .bind(repo.as_i64())
        .execute(&mut *tx)
        .await
        .context("erasing repository row")?;
    Ok(())
}

/// Decode a sixteen-byte `bytea` segment id as a [`Ulid`].
///
/// Fixed width, so a short value means the column was written by something
/// other than a push.
fn segment_id(row: &sqlx::postgres::PgRow) -> Result<Ulid> {
    let bytes: Vec<u8> = row.try_get("segment_id")?;
    let bytes: [u8; 16] = bytes
        .as_slice()
        .try_into()
        .context("a segment id that is not sixteen bytes")?;
    Ok(Ulid::from_bytes(bytes))
}
