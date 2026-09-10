//! Everything Enroute keeps in Postgres, and the only crate that names a
//! driver.
//!
//! The engine says what it needs of a store — [`Metadata`], [`Ledger`],
//! [`Catalog`] — and this is where each is answered in SQL. One crate rather
//! than one per layer is what makes the layers above database-independent
//! rather than merely database-agnostic: a `git` or `lattice` crate able to
//! reach a connection would grow a shape only a database has, and nothing
//! would notice until something tried to keep it somewhere else.
//!
//! # The schema
//! [`schema`] is every table above as one ordered list. A deployment names a
//! schema, and that is what goes in it.
//!
//! [`Metadata`]: enroute_git_metadata::Metadata
//! [`Ledger`]: enroute_git_journal::Ledger
//! [`Catalog`]: enroute_lattice_store::Catalog

pub mod catalog;
mod ledger;
mod pg_oid;
mod refs;
mod repos;
mod rows;
pub mod schema;
mod storage;
mod table;
mod testing;

pub use catalog::PostgresCatalog;
pub use ledger::PostgresLedger;
pub use rows::Postgres;
pub use storage::{ephemeral, session_pool, storage};
pub use table::Table;
pub use testing::test_database_url;
