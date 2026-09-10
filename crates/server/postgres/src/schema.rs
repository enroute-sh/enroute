//! Every table Enroute owns, as one ordered list, applied by sqlx's migrator.
//!
//! Nothing here names a schema. The tables land wherever the connection
//! resolves unqualified names, so a deployment that wants a named one says so
//! in its database URL — `?options=-csearch_path%3Denroute` — the same place
//! it says everything else about the connection.
//!
//! # Adding to it
//! Append. A step that has run is history and its bytes are checksummed, so a
//! column arrives as a new file rather than as an edit to an old one — and a
//! reworded comment is an edit.

use anyhow::{Result, bail};
use sqlx::migrate::{MigrateError, Migration, MigrationSource, MigrationType};
use sqlx::{AssertSqlSafe, PgPool, SqlSafeStr as _};

use enroute_git_journal::Index;

/// The table sqlx records applied steps in.
const LEDGER: &str = "_sqlx_migrations";

/// The table one list's segments are catalogued in.
///
/// A Postgres fact, so it lives here rather than beside the [`Index`] that
/// means the list — nothing above needs to know a list is a table at all.
#[must_use]
pub const fn table(index: Index) -> &'static str {
    match index {
        Index::CommitGraph => "commit_graph_segments",
        Index::CommitPacks => "commit_pack_segments",
        Index::Trees => "tree_segments",
        Index::Blobs => "blob_segments",
    }
}

/// The whole schema history, in the order it is applied.
///
/// Stated here rather than read from the directory, because the order a
/// ledger records is the list's own and never what a name sorts as.
#[must_use]
pub fn steps() -> Vec<Migration> {
    vec![
        step(
            1,
            "engine_rows",
            include_str!("../migrations/0001_engine_rows.sql"),
        ),
        step(
            2,
            "commit_graph_catalogs",
            include_str!("../migrations/0002_commit_graph_catalogs.sql"),
        ),
        step(
            3,
            "object_index_catalogs",
            include_str!("../migrations/0003_object_index_catalogs.sql"),
        ),
        // Holding no foreign key into the engine's tables: one Enroute serves
        // many customers out of one engine, which does not know it.
        step(4, "tenancy", include_str!("../migrations/0004_tenancy.sql")),
        step(
            5,
            "repository_keys",
            include_str!("../migrations/0005_repository_keys.sql"),
        ),
    ]
}

/// Runs every step this database has not run yet.
///
/// Returns what was outstanding when it started, which is empty for a database
/// already caught up — the ordinary answer, since most starts do nothing.
///
/// # Errors
/// When a step that has already run has changed, or one will not apply.
pub async fn apply(pool: &PgPool) -> Result<Vec<String>> {
    let outstanding = pending(pool).await?;
    migrator().await?.run(pool).await.map_err(refusal)?;
    warn_unknown(pool).await?;
    if !outstanding.is_empty() {
        tracing::info!(steps = outstanding.len(), "the schema moved up");
    }
    Ok(outstanding)
}

/// Which steps this database has not run, in the list's order.
///
/// # Errors
/// When the ledger cannot be read. A database with no ledger at all has run
/// nothing, which is an answer rather than an error.
pub async fn pending(pool: &PgPool) -> Result<Vec<String>> {
    let recorded = recorded(pool).await?;
    Ok(steps()
        .iter()
        .filter(|step| !recorded.contains(&step.version))
        .map(id_of)
        .collect())
}

/// Fails unless this database has run every step this build knows about.
///
/// What a deployment that applies its schema out of band calls at startup, so
/// being behind stops the process rather than the first query.
///
/// # Errors
/// When anything is outstanding, naming the first one.
pub async fn verify(pool: &PgPool) -> Result<()> {
    let outstanding = pending(pool).await?;
    let Some(first) = outstanding.first() else {
        return Ok(());
    };
    bail!(
        "this database is behind this build by {} step(s), the first being `{first}`",
        outstanding.len()
    )
}

/// One step of the history: a number, a name, and the SQL it runs.
///
/// The number is what the ledger records, so it is permanent — renaming one
/// makes a step that has already run look like a step that has not.
fn step(version: i64, name: &'static str, sql: &'static str) -> Migration {
    Migration::new(
        version,
        name.into(),
        MigrationType::Simple,
        sql.into_sql_str(),
        false,
    )
}

/// How a step is named to an operator, matching the file it came from.
fn id_of(migration: &Migration) -> String {
    format!("{:04}_{}", migration.version, migration.description)
}

/// sqlx's migrator over this list, told the one thing it defaults the other
/// way on.
///
/// A database holding steps this build does not know is a rolled-back binary,
/// which a rolling deploy has beside a new one: report, not refuse.
async fn migrator() -> Result<sqlx::migrate::Migrator> {
    let mut migrator = sqlx::migrate::Migrator::new(Embedded(steps())).await?;
    migrator.set_ignore_missing(true);
    Ok(migrator)
}

/// The same refusal, naming the file rather than the version sqlx counts by.
fn refusal(refused: MigrateError) -> anyhow::Error {
    let MigrateError::VersionMismatch(version) = refused else {
        return refused.into();
    };
    let named = steps()
        .iter()
        .find(|step| step.version == version)
        .map_or_else(|| version.to_string(), id_of);
    anyhow::anyhow!(
        "step `{named}` has already been applied and has changed since; \
         a step that has run is history, so add a new one instead of editing it"
    )
}

/// Says which steps the database holds that this build does not.
///
/// The counterpart to ignoring them: a rolled-back binary is a fact worth
/// saying, just not a reason to refuse to serve.
async fn warn_unknown(pool: &PgPool) -> Result<()> {
    let known: Vec<i64> = steps().iter().map(|step| step.version).collect();
    for version in recorded(pool).await? {
        if !known.contains(&version) {
            tracing::warn!(
                step = version,
                "the schema holds a step this build does not know"
            );
        }
    }
    Ok(())
}

/// Every step version this database's ledger holds.
///
/// A database with no ledger has run nothing, so a missing table answers
/// rather than fails.
async fn recorded(pool: &PgPool) -> Result<Vec<i64>> {
    let mut conn = pool.acquire().await?;
    let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
        .bind(LEDGER)
        .fetch_one(&mut *conn)
        .await?;
    if found.is_none() {
        return Ok(Vec::new());
    }
    Ok(sqlx::query_scalar(AssertSqlSafe(format!(
        "SELECT version FROM {LEDGER} ORDER BY version"
    )))
    .fetch_all(&mut *conn)
    .await?)
}

/// An already-resolved list, which is how a step embedded with `include_str!`
/// reaches a migrator that otherwise reads a directory.
#[derive(Debug)]
struct Embedded(Vec<Migration>);

impl MigrationSource<'static> for Embedded {
    fn resolve(
        self,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<Vec<Migration>, Box<dyn std::error::Error + Send + Sync>>>
                + Send
                + 'static,
        >,
    > {
        Box::pin(std::future::ready(Ok(self.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Table;

    #[test]
    fn every_table_enroute_reads_has_a_step() {
        let versions: Vec<i64> = steps().iter().map(|step| step.version).collect();
        assert_eq!(versions, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn a_step_is_named_by_the_file_it_came_from() {
        assert_eq!(
            id_of(&step(1, "engine_rows", "SELECT 1")),
            "0001_engine_rows"
        );
    }

    /// Two lists reaching the same table would put a write in the wrong one
    /// and never say so, since a catalog row carries no kind.
    #[test]
    fn no_two_lists_share_a_table() {
        let mut seen: Vec<&str> = Index::ALL.iter().map(|index| table(*index)).collect();
        seen.sort_unstable();
        let count = seen.len();
        seen.dedup();
        assert_eq!(seen.len(), count, "two lists name one table");
    }

    /// The catalog steps are what [`Table`] would create, which is the whole
    /// of what keeps a catalog's DDL and its statements in step.
    ///
    /// # When a catalog table changes
    /// Its shape changes by a new step that alters it, and this stops being
    /// true of the first two — replace it then rather than editing them.
    #[test]
    fn a_catalog_step_creates_what_the_statements_read() {
        let ddl = |first: Index, second: Index| {
            format!("{}\n{}", Table::new(first).ddl(), Table::new(second).ddl())
        };
        let held = steps();
        for (at, expected) in [
            (1, ddl(Index::CommitGraph, Index::CommitPacks)),
            (2, ddl(Index::Trees, Index::Blobs)),
        ] {
            let step = held.get(at).expect("a catalog step");
            assert_eq!(
                statements(step.sql.as_str()),
                statements(&expected),
                "{} is not what Table would create",
                step.description
            );
        }
    }

    /// `sql` without the lines that are only prose.
    ///
    /// So a migration can say why it is there, which is the one thing the
    /// generated form it is compared against has no room for.
    fn statements(sql: &str) -> Vec<&str> {
        sql.lines()
            .map(str::trim_end)
            .filter(|line| !line.is_empty() && !line.trim_start().starts_with("--"))
            .collect()
    }

    /// What every step of the history hashes to, pinned.
    ///
    /// A step that has run is checksummed in every deployment's ledger, so
    /// changing one refuses to serve. This fails in CI instead.
    const APPLIED: [(i64, &str); 5] = [
        (
            1,
            "9270cc4733750a491b26a1e57bd5668a8f7433b346226df45c2cc7e6635fd0c8\
             de5daa3b10f751ed59b01d9e68bd2737",
        ),
        (
            2,
            "aa3d628930f52823b6b65e4874401dd07307c867a60cd985d76b1ecfae58e7d5\
             f5edec8ba862bd6ab8c0e8d3b74cdfef",
        ),
        (
            3,
            "db431f132afa5d26d92bdc5bb8cf0085ab60fda09fbdb5b56898a243aa7ed03d\
             a73ea5fbb7c26afef187e1358b87a821",
        ),
        (
            4,
            "da6f73034773e8717462472c4bf6cba30ce21b368078a247709e396427c1379b\
             908e7686b1c58d2b9ca72974d19df713",
        ),
        (
            5,
            "913437f26917440a40b318d210158a48da0f6da42ffc30863aab88b6373fd332\
             debf984d0e223007c252f7605d4e56c8",
        ),
    ];

    /// An applied step's bytes are history, comments and all.
    #[test]
    fn no_applied_step_has_changed_what_it_hashes_to() {
        let held: Vec<(i64, String)> = steps()
            .iter()
            .map(|step| (step.version, hex(&step.checksum)))
            .collect();
        let pinned: Vec<(i64, String)> = APPLIED
            .iter()
            .map(|(version, sum)| (*version, (*sum).to_owned()))
            .collect();
        assert_eq!(held, pinned, "a step already in a ledger changed");
    }

    /// One checksum as the characters it prints as.
    fn hex(checksum: &[u8]) -> String {
        let mut out = String::with_capacity(checksum.len() * 2);
        for byte in checksum {
            out.push(nibble(byte >> 4));
            out.push(nibble(byte & 0x0f));
        }
        out
    }

    /// One nibble as the character it prints as.
    fn nibble(value: u8) -> char {
        char::from_digit(u32::from(value), 16).unwrap_or('?')
    }
}
