//! Ref reads and updates for [`Postgres`]: branch and tag CAS, and the
//! `HEAD`-synthesizing ref listing.
//!
//! [`Postgres`]: crate::Postgres

use anyhow::{Context, Result};
use gix_hash::ObjectId;
use sqlx::{PgConnection, PgPool, Row};

use enroute_git_core::RepoId;

use enroute_git_metadata::refs::{ok_result, reject_result};
use enroute_git_metadata::{
    RefUpdate, RefUpdateRejection, RefUpdateResult, RefsMap, RepoMetadata, is_branch_refname,
};

use crate::pg_oid::PgOid;

/// `branches ∪ refs` for one `repo_id` (bind `$1`) — one index range scan
/// each, no join, since both tables store the oid directly.
///
/// See `branches`'s doc comment in `migrations/0001_engine_rows.sql`.
const REFS_UNION_QUERY: &str = "SELECT refname, oid FROM branches WHERE repo_id = $1 \
     UNION ALL \
     SELECT refname, oid FROM refs WHERE repo_id = $1";

/// `branches ∪ refs` restricted to specific refnames, split by table so an
/// empty bound array costs zero index probes.
const REFS_BY_NAME_QUERY: &str = "SELECT refname, oid FROM branches \
     WHERE repo_id = $1 AND refname = ANY($2) \
     UNION ALL \
     SELECT refname, oid FROM refs WHERE repo_id = $1 AND refname = ANY($3)";

/// Decode `(refname, oid)` rows from either ref query into a [`RefsMap`].
fn refs_from_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<RefsMap> {
    let mut refs = RefsMap::new();
    for row in rows {
        let refname: String = row.try_get("refname")?;
        let oid: ObjectId = row.try_get::<PgOid, _>("oid")?.into();
        refs.insert(refname, oid.to_hex().to_string());
    }
    Ok(refs)
}

/// The full ref set for an already-resolved `repo`, with `HEAD` synthesized
/// from its default branch.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn get_refs_for(pool: &PgPool, repo: &RepoMetadata) -> Result<RefsMap> {
    let rows = sqlx::query(REFS_UNION_QUERY)
        .bind(repo.id.as_i64())
        .fetch_all(pool)
        .await
        .context("reading refs")?;
    let mut refs = refs_from_rows(rows)?;
    refs.insert("HEAD".to_string(), format!("ref: {}", repo.default_branch));
    Ok(refs)
}

/// Only the named refs, and no `HEAD`: a caller asking for names has one.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn get_refs_matching(
    pool: &PgPool,
    repo_id: RepoId,
    refnames: &[&str],
) -> Result<RefsMap> {
    let (branch_names, other_names): (Vec<&str>, Vec<&str>) = refnames
        .iter()
        .copied()
        .partition(|name| is_branch_refname(name));

    let rows = sqlx::query(REFS_BY_NAME_QUERY)
        .bind(repo_id.as_i64())
        .bind(&branch_names)
        .bind(&other_names)
        .fetch_all(pool)
        .await
        .context("reading refs by name")?;
    refs_from_rows(rows)
}

/// Apply `updates` atomically, answering in the order they were given.
///
/// # Errors
/// Whatever the database said.
pub(crate) async fn update_refs(
    pool: &PgPool,
    repo_id: RepoId,
    updates: &[RefUpdate],
) -> Result<Vec<RefUpdateResult>> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }

    // Apply in refname order, not request order, so overlapping pushes
    // always acquire row locks in the same order and can't deadlock.
    let mut ordered: Vec<(usize, &RefUpdate)> = updates.iter().enumerate().collect();
    ordered.sort_by(|(_, one), (_, other)| one.refname.cmp(&other.refname));

    let mut tx = pool
        .begin()
        .await
        .context("beginning update_refs transaction")?;

    let mut answered: Vec<(usize, RefUpdateResult)> = Vec::with_capacity(updates.len());
    for (at, update) in ordered {
        answered.push((at, apply_ref_update(&mut tx, repo_id, update).await?));
    }

    tx.commit()
        .await
        .context("committing update_refs transaction")?;

    answered.sort_by_key(|(at, _)| *at);
    Ok(answered.into_iter().map(|(_, result)| result).collect())
}

/// Apply one [`RefUpdate`], dispatching on refname namespace to
/// [`apply_branch_update`] or [`apply_other_update`].
///
/// `receive_pack` pre-screens the same restriction, but this enforces it
/// too: `update_refs` is public and other callers could invoke it directly.
async fn apply_ref_update(
    conn: &mut PgConnection,
    repo_id: RepoId,
    update: &RefUpdate,
) -> Result<RefUpdateResult> {
    // Validity is the only thing refused here, and it is refused for every
    // caller: an application reaching the contract does not run the git door's
    // checks, so a store that trusted its caller would let `HEAD` be written as
    // a ref. Which namespaces are *allowed* is nobody's business here.
    if enroute_git_core::is_funny_refname(&update.refname, update.new_id.is_null()) {
        return Ok(reject_result(
            &update.refname,
            RefUpdateRejection::InvalidRefname,
        ));
    }

    if is_branch_refname(&update.refname) {
        apply_branch_update(conn, repo_id, update).await
    } else {
        apply_other_update(conn, repo_id, update).await
    }
}

/// The rejection every CAS state machine here shares.
fn nff_result(refname: &str) -> RefUpdateResult {
    reject_result(refname, RefUpdateRejection::NonFastForward)
}

/// What a CAS statement came to: matching no row is the guard having moved.
fn outcome(rows_affected: u64, refname: &str) -> RefUpdateResult {
    if rows_affected == 0 {
        nff_result(refname)
    } else {
        ok_result(refname)
    }
}

/// The kind column's value for a commit, which a branch tip must be.
///
/// One table numbers every kind, so every guard here says which it means.
const COMMIT: i16 = 1;

/// Whether `oid` is numbered as a commit rather than as some other object.
///
/// Only reached to explain a rejected branch write, never on the accept path,
/// and only about numbering — `move_refs` is what asks the index.
async fn commit_exists(conn: &mut PgConnection, repo_id: RepoId, oid: ObjectId) -> Result<bool> {
    let found: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM object_seqs WHERE repo_id = $1 AND oid = $2 AND kind = $3",
    )
    .bind(repo_id.as_i64())
    .bind(PgOid::from(oid))
    .bind(COMMIT)
    .fetch_optional(&mut *conn)
    .await
    .context("looking up commit")?;
    Ok(found.is_some())
}

/// The two DELETE statements of one table's CAS — the two differ only in the
/// table, since both compare the stored oid.
struct RefTable {
    delete: &'static str,
    delete_cas: &'static str,
}

const BRANCHES: RefTable = RefTable {
    delete: "DELETE FROM branches WHERE repo_id = $1 AND refname = $2",
    delete_cas: "DELETE FROM branches WHERE repo_id = $1 AND refname = $2 AND oid = $3",
};

const OTHER: RefTable = RefTable {
    delete: "DELETE FROM refs WHERE repo_id = $1 AND refname = $2",
    delete_cas: "DELETE FROM refs WHERE repo_id = $1 AND refname = $2 AND oid = $3",
};

/// The delete arm of [`apply_branch_update`] and [`apply_other_update`].
///
/// An unguarded delete of a missing ref is a no-op; a guarded one matching
/// nothing is non-fast-forward, whether the ref is gone or moved.
async fn apply_ref_delete(
    conn: &mut PgConnection,
    repo_id: RepoId,
    update: &RefUpdate,
    table: &RefTable,
) -> Result<RefUpdateResult> {
    if update.old_id.is_null() {
        sqlx::query(table.delete)
            .bind(repo_id.as_i64())
            .bind(&update.refname)
            .execute(&mut *conn)
            .await
            .context("deleting ref")?;
        return Ok(ok_result(&update.refname));
    }
    let rows_affected = sqlx::query(table.delete_cas)
        .bind(repo_id.as_i64())
        .bind(&update.refname)
        .bind(PgOid::from(update.old_id))
        .execute(&mut *conn)
        .await
        .context("deleting ref")?
        .rows_affected();
    Ok(outcome(rows_affected, &update.refname))
}

/// Apply one [`RefUpdate`] to `refs/heads/*`.
///
/// Same CAS as [`apply_other_update`] plus a branch-only condition, fused in:
/// `new_id` must already be a recorded commit.
async fn apply_branch_update(
    conn: &mut PgConnection,
    repo_id: RepoId,
    update: &RefUpdate,
) -> Result<RefUpdateResult> {
    if update.new_id.is_null() {
        return apply_ref_delete(conn, repo_id, update, &BRANCHES).await;
    }

    if update.old_id.is_null() {
        // If `new_id` isn't a recorded commit the SELECT yields no row and
        // this inserts nothing, same as if the branch already existed — a
        // 0-row result pays for a second query only to tell the two apart.
        let rows_affected = sqlx::query(
            "INSERT INTO branches (repo_id, refname, oid) \
             SELECT $1, $2, $3 WHERE EXISTS \
             (SELECT 1 FROM object_seqs WHERE repo_id = $1 AND oid = $3 AND kind = $4) \
             ON CONFLICT (repo_id, refname) DO NOTHING",
        )
        .bind(repo_id.as_i64())
        .bind(&update.refname)
        .bind(PgOid::from(update.new_id))
        .bind(COMMIT)
        .execute(&mut *conn)
        .await
        .context("inserting branch")?
        .rows_affected();
        if rows_affected > 0 {
            return Ok(ok_result(&update.refname));
        }
        let reason = if commit_exists(conn, repo_id, update.new_id).await? {
            RefUpdateRejection::AlreadyExists
        } else {
            RefUpdateRejection::UnknownCommit
        };
        return Ok(reject_result(&update.refname, reason));
    }

    let rows_affected = sqlx::query(
        // `updated_at` is set here as well as by the insert's default: a ref
        // that moves is the repository being updated, and without this the
        // stamp would only ever record when a branch was first created.
        "UPDATE branches SET oid = $4, updated_at = now() \
         WHERE repo_id = $1 AND refname = $2 AND oid = $3 \
         AND EXISTS \
         (SELECT 1 FROM object_seqs WHERE repo_id = $1 AND oid = $4 AND kind = $5)",
    )
    .bind(repo_id.as_i64())
    .bind(&update.refname)
    .bind(PgOid::from(update.old_id))
    .bind(PgOid::from(update.new_id))
    .bind(COMMIT)
    .execute(&mut *conn)
    .await
    .context("updating branch")?
    .rows_affected();
    if rows_affected > 0 {
        return Ok(ok_result(&update.refname));
    }
    // Nothing matched, so one of the two conditions failed; reading the
    // branch back infers which. A concurrent push moving the same branch in
    // between skews that inference, which costs only the wording of a
    // rejection that happens either way.
    let cas_held: Option<i32> = sqlx::query_scalar(
        "SELECT 1 FROM branches WHERE repo_id = $1 AND refname = $2 AND oid = $3",
    )
    .bind(repo_id.as_i64())
    .bind(&update.refname)
    .bind(PgOid::from(update.old_id))
    .fetch_optional(&mut *conn)
    .await
    .context("reading branch after rejected update")?;
    Ok(if cas_held.is_some() {
        reject_result(&update.refname, RefUpdateRejection::UnknownCommit)
    } else {
        nff_result(&update.refname)
    })
}

/// Apply one [`RefUpdate`] to any ref that is not a branch.
///
/// Same CAS as [`apply_branch_update`] minus the recorded-commit condition —
/// only a branch has to point at something this store walks.
async fn apply_other_update(
    conn: &mut PgConnection,
    repo_id: RepoId,
    update: &RefUpdate,
) -> Result<RefUpdateResult> {
    if update.new_id.is_null() {
        return apply_ref_delete(conn, repo_id, update, &OTHER).await;
    }

    if update.old_id.is_null() {
        let rows_affected = sqlx::query(
            "INSERT INTO refs (repo_id, refname, oid) VALUES ($1, $2, $3) \
             ON CONFLICT (repo_id, refname) DO NOTHING",
        )
        .bind(repo_id.as_i64())
        .bind(&update.refname)
        .bind(PgOid::from(update.new_id))
        .execute(&mut *conn)
        .await
        .context("inserting tag")?
        .rows_affected();
        return Ok(if rows_affected == 0 {
            reject_result(&update.refname, RefUpdateRejection::AlreadyExists)
        } else {
            ok_result(&update.refname)
        });
    }

    let rows_affected = sqlx::query(
        "UPDATE refs SET oid = $4, updated_at = now() \
         WHERE repo_id = $1 AND refname = $2 AND oid = $3",
    )
    .bind(repo_id.as_i64())
    .bind(&update.refname)
    .bind(PgOid::from(update.old_id))
    .bind(PgOid::from(update.new_id))
    .execute(&mut *conn)
    .await
    .context("updating tag")?
    .rows_affected();

    Ok(outcome(rows_affected, &update.refname))
}
