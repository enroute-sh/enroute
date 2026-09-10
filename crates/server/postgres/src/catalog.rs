//! The catalog itself: rows in, [`Listed`] out.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use sqlx::{AssertSqlSafe, PgPool, Row, postgres::PgRow};
use thiserror::Error;
use uuid::Uuid;

use enroute_lattice_core::{Key, KeyRange, Residence, Tier};

use enroute_lattice_store::{
    Body, Catalog, CatalogError, Entry, Listed, Scope, SegmentId, Written,
};

use crate::table::Table;

/// A failure reaching the catalog, or a row that is not one.
#[derive(Debug, Error)]
pub enum Error {
    /// The database said no.
    #[error("the segment catalog could not be reached")]
    Sql(#[from] Box<sqlx::Error>),

    /// A key past what a `bigint` column holds.
    ///
    /// Keys are dense and start at zero, so this is a bug rather than a
    /// repository that grew: reaching it needs 2^63 of them.
    #[error("a key of {0} does not fit a bigint column")]
    KeyTooLarge(u64),

    /// A segment larger than a `bigint` column can record.
    #[error("a segment of {0} bytes does not fit a bigint column")]
    SegmentTooLarge(u64),

    /// A row with neither inlined bytes nor an object key.
    #[error("segment {0} has neither inlined bytes nor an object key")]
    Bodiless(Uuid),

    /// A row whose columns are not a segment.
    #[error("segment {0} holds {1}")]
    Malformed(Uuid, &'static str),
}

impl From<sqlx::Error> for Error {
    fn from(source: sqlx::Error) -> Self {
        Self::Sql(Box::new(source))
    }
}

/// A segment catalog in one Postgres table.
///
/// The table is unqualified, so the connecting role's `search_path` places
/// it — the same way the rest of this repository's schemas are reached.
#[derive(Debug, Clone)]
pub struct PostgresCatalog {
    pool: PgPool,
    table: Table,
}

impl PostgresCatalog {
    /// A catalog over `table` in `pool`.
    #[must_use]
    pub fn new(pool: PgPool, table: Table) -> Self {
        Self { pool, table }
    }

    /// The table this catalog reads.
    #[must_use]
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Creates the table if it is not there yet.
    ///
    /// # Errors
    /// [`Error::Sql`] when the DDL does not apply.
    pub async fn apply_ddl(&self) -> Result<(), Error> {
        sqlx::raw_sql(AssertSqlSafe(self.table.ddl()))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Records one segment through `executor`, which may be a transaction.
    ///
    /// How a caller writes a segment in the same transaction as whatever
    /// else the write means, rather than in one of its own.
    ///
    /// # Errors
    /// [`Error`] when the row cannot be written.
    pub(crate) async fn insert_with<'e, E>(
        &self,
        executor: E,
        scope: &Scope,
        segment: &Written,
    ) -> Result<(), Error>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        bind_insert(sqlx::query(self.table.insert.clone()), scope, segment)?
            .execute(executor)
            .await?;
        Ok(())
    }

    /// Drops every row of one scope through `executor`, which may be a
    /// transaction.
    ///
    /// How a scope's deletion takes its segments with it, in the same
    /// transaction as the rest of whatever is being taken apart.
    ///
    /// # Errors
    /// [`Error`] when the rows cannot be dropped.
    pub(crate) async fn purge_with<'e, E>(&self, executor: E, scope: i64) -> Result<(), Error>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        sqlx::query(self.table.purge.clone())
            .bind(scope)
            .execute(executor)
            .await?;
        Ok(())
    }

    /// The rows of a query returning every column, decoded.
    fn listed(rows: &[PgRow]) -> Result<Vec<Listed>, Error> {
        rows.iter()
            .map(|row| {
                let object_key: Option<String> = row.try_get("object_key")?;
                let entry = entry(row, residence(object_key.is_some()))?;
                Ok(Listed {
                    body: body(row, entry.id, object_key)?,
                    entry,
                })
            })
            .collect()
    }
}

#[async_trait]
impl Catalog for PostgresCatalog {
    async fn covering(&self, scope: &Scope, range: KeyRange) -> Result<Vec<Listed>, CatalogError> {
        let rows = sqlx::query(self.table.covering.clone())
            .bind(scope.id)
            .bind(bound(range.last()))
            .bind(bound(range.first()))
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)?;
        Ok(Self::listed(&rows)?)
    }

    async fn entries(&self, scope: &Scope) -> Result<Vec<Entry>, CatalogError> {
        let rows = sqlx::query(self.table.entries.clone())
            .bind(scope.id)
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)?;
        Ok(rows
            .iter()
            .map(|row| entry(row, residence(row.try_get("in_bucket")?)))
            .collect::<Result<Vec<_>, Error>>()?)
    }

    async fn bodies(&self, scope: &Scope, ids: &[SegmentId]) -> Result<Vec<Listed>, CatalogError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: Vec<Uuid> = ids.iter().map(|id| Uuid::from_u128(id.get())).collect();
        let rows = sqlx::query(self.table.bodies.clone())
            .bind(scope.id)
            .bind(&wanted)
            .fetch_all(&self.pool)
            .await
            .map_err(Error::from)?;
        Ok(Self::listed(&rows)?)
    }

    async fn insert(&self, scope: &Scope, segment: Written) -> Result<(), CatalogError> {
        Ok(self.insert_with(&self.pool, scope, &segment).await?)
    }

    async fn replace(
        &self,
        scope: &Scope,
        inputs: &[SegmentId],
        output: Written,
    ) -> Result<(), CatalogError> {
        let dropped: Vec<Uuid> = inputs.iter().map(|id| Uuid::from_u128(id.get())).collect();

        // One transaction, because a reader seeing neither the inputs nor the
        // output would be missing keys outright — the one thing the join
        // cannot paper over.
        let mut tx = self.pool.begin().await.map_err(Error::from)?;
        sqlx::query(self.table.delete.clone())
            .bind(scope.id)
            .bind(&dropped)
            .execute(&mut *tx)
            .await
            .map_err(Error::from)?;
        self.insert_with(&mut *tx, scope, &output).await?;
        tx.commit().await.map_err(Error::from)?;
        Ok(())
    }

    async fn purge(&self, scope: i64) -> Result<(), CatalogError> {
        Ok(self.purge_with(&self.pool, scope).await?)
    }
}

/// Binds one segment's columns, in the order the insert names them.
fn bind_insert<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    scope: &Scope,
    segment: &'q Written,
) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>, Error> {
    let (inline, object_key) = match &segment.body {
        Body::Inline(bytes) => (Some(bytes.as_ref()), None),
        Body::Bucket(path) => (None, Some(path.as_ref())),
    };
    Ok(query
        .bind(scope.id)
        .bind(Uuid::from_u128(segment.id.get()))
        .bind(column_key(segment.range.first())?)
        .bind(column_key(segment.range.last())?)
        .bind(i16::from(segment.tier.get()))
        .bind(
            i64::try_from(segment.bytes)
                .map_err(|_out_of_range| Error::SegmentTooLarge(segment.bytes))?,
        )
        .bind(inline)
        .bind(object_key))
}

/// What the catalog knows about a row without its bytes.
fn entry(row: &PgRow, residence: Residence) -> Result<Entry, Error> {
    let id: Uuid = row.try_get("id")?;
    let first: i64 = row.try_get("first_key")?;
    let last: i64 = row.try_get("last_key")?;
    let tier: i16 = row.try_get("tier")?;
    let bytes: i64 = row.try_get("bytes")?;

    let (Ok(first), Ok(last)) = (u64::try_from(first), u64::try_from(last)) else {
        return Err(Error::Malformed(id, "a key below zero"));
    };
    let range = KeyRange::new(Key::new(first), Key::new(last))
        .map_err(|_out_of_range| Error::Malformed(id, "a range ending below where it starts"))?;

    Ok(Entry {
        id: SegmentId::from(id.as_u128()),
        range,
        tier: Tier::new(
            u8::try_from(tier).map_err(|_out_of_range| Error::Malformed(id, "a tier past 255"))?,
        ),
        bytes: u64::try_from(bytes)
            .map_err(|_out_of_range| Error::Malformed(id, "a size below zero"))?,
        residence,
    })
}

/// A row's bytes, or the key they were put at, which the caller already read
/// to say where the segment lives.
fn body(row: &PgRow, id: SegmentId, object_key: Option<String>) -> Result<Body, Error> {
    let inline: Option<Vec<u8>> = row.try_get("inline")?;
    if let Some(inline) = inline {
        return Ok(Body::Inline(Bytes::from(inline)));
    }
    object_key
        .map(|key| Body::Bucket(Path::from(key)))
        .ok_or_else(|| Error::Bodiless(Uuid::from_u128(id.get())))
}

/// Which store a row's bytes are in, from whether it holds a key.
const fn residence(in_bucket: bool) -> Residence {
    if in_bucket {
        Residence::Bucket
    } else {
        Residence::Inline
    }
}

/// A key as the column holds it.
fn column_key(key: Key) -> Result<i64, Error> {
    i64::try_from(key.get()).map_err(|_out_of_range| Error::KeyTooLarge(key.get()))
}

/// A key as a query bound, saturating rather than failing.
///
/// No stored key can exceed `i64::MAX`, so a bound above it asks for
/// everything up to there — which is everything.
fn bound(key: Key) -> i64 {
    i64::try_from(key.get()).unwrap_or(i64::MAX)
}
