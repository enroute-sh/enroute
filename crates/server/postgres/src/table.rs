//! The table a scope's segment list is kept in, and the DDL that makes it.

use std::sync::Arc;

use sqlx::{AssertSqlSafe, SqlSafeStr as _, SqlStr};

use enroute_git_journal::Index;

/// The table one list's segments are kept in.
///
/// Named by its [`Index`] rather than by a string, so the only names that
/// reach a statement are `schema::table`'s own and none is a caller's.
#[derive(Debug, Clone)]
pub struct Table {
    name: &'static str,
    // Already `SqlStr`, so the assertion is made once here — beside the reason
    // it holds — rather than restated at each use.
    pub(crate) covering: SqlStr,
    pub(crate) entries: SqlStr,
    pub(crate) bodies: SqlStr,
    pub(crate) insert: SqlStr,
    pub(crate) delete: SqlStr,
    pub(crate) purge: SqlStr,
}

/// Every column a read names, in the order the row decoder expects.
const COLUMNS: &str = "id, first_key, last_key, tier, bytes, inline, object_key";

/// The same, with `inline` replaced by whether there is a key instead.
///
/// This is the projection the design turns on: a listing that costs nothing
/// for a tail it does not read.
const PROJECTION: &str =
    "id, first_key, last_key, tier, bytes, object_key IS NOT NULL AS in_bucket";

impl Table {
    /// The table `index` is kept in, and every statement that reads it.
    #[must_use]
    pub fn new(index: Index) -> Self {
        let name = crate::schema::table(index);
        Self {
            covering: statement(format!(
                "SELECT {COLUMNS} FROM {name} \
                 WHERE scope = $1 AND first_key <= $2 AND last_key >= $3"
            )),
            entries: statement(format!("SELECT {PROJECTION} FROM {name} WHERE scope = $1")),
            bodies: statement(format!(
                "SELECT {COLUMNS} FROM {name} WHERE scope = $1 AND id = ANY($2)"
            )),
            insert: statement(format!(
                "INSERT INTO {name} \
                 (scope, id, first_key, last_key, tier, bytes, inline, object_key) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
            )),
            delete: statement(format!(
                "DELETE FROM {name} WHERE scope = $1 AND id = ANY($2)"
            )),
            purge: statement(format!("DELETE FROM {name} WHERE scope = $1")),
            name,
        }
    }

    /// The DDL for this table, unqualified so `search_path` places it.
    ///
    /// It ships beside the statements rather than as a file elsewhere: a
    /// schema and its queries disagreeing fails at runtime, not at build.
    #[must_use]
    pub fn ddl(&self) -> String {
        let name = self.name;
        format!(
            "CREATE TABLE IF NOT EXISTS {name} (
    scope      bigint   NOT NULL,
    id         uuid     NOT NULL,
    first_key  bigint   NOT NULL,
    last_key   bigint   NOT NULL,
    tier       smallint NOT NULL,
    bytes      bigint   NOT NULL,
    inline     bytea,
    object_key text,
    PRIMARY KEY (scope, id),
    CHECK (first_key >= 0 AND last_key >= first_key),
    CHECK (tier BETWEEN 0 AND 255),
    CHECK (bytes >= 0),
    CHECK ((inline IS NULL) <> (object_key IS NULL))
);
CREATE INDEX IF NOT EXISTS {name}_covering ON {name} (scope, first_key, last_key);"
        )
    }
}

/// One statement, safe because the only name in it is `schema::table`'s, which
/// a test holds to what Postgres takes unquoted.
///
/// Through an `Arc`, so the clone each query takes is a refcount. A `SqlStr`
/// built from a `String` copies on every clone instead.
fn statement(sql: String) -> SqlStr {
    AssertSqlSafe(Arc::<str>::from(sql)).into_sql_str()
}

/// Whether Postgres would take `name` unquoted and lowercase.
#[cfg(test)]
fn is_plain_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let starts = chars
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == '_');
    starts
        && name.len() <= 63
        && chars.all(|char| char.is_ascii_lowercase() || char.is_ascii_digit() || char == '_')
}

#[cfg(test)]
mod tests {
    use enroute_git_journal::Index;

    use super::{Table, is_plain_identifier};
    use crate::schema::table;

    /// The name reaches a statement by interpolation, so this is the whole of
    /// what stands between `schema::table` and the query text.
    #[test]
    fn every_list_names_a_plain_table() {
        for index in Index::ALL {
            let name = table(index);
            assert!(is_plain_identifier(name), "{name:?} would need quoting");
        }
    }

    #[test]
    fn a_name_needing_quoting_is_not_plain() {
        for name in [
            "",
            "1segments",
            "Segments",
            "seg ments",
            "seg;ments",
            "seg\"ments",
            "segments--",
            &"s".repeat(64),
        ] {
            assert!(!is_plain_identifier(name), "{name:?} must be refused");
        }
    }

    #[test]
    fn the_ddl_and_the_queries_name_one_table() {
        let table = Table::new(Index::CommitGraph);
        let named = crate::schema::table(Index::CommitGraph);
        let ddl = table.ddl();
        for statement in [
            table.covering.as_str(),
            table.entries.as_str(),
            table.bodies.as_str(),
            table.insert.as_str(),
            table.delete.as_str(),
            table.purge.as_str(),
            ddl.as_str(),
        ] {
            assert!(statement.contains(named), "{statement}");
        }
    }
}
