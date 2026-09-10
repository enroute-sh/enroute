//! Where a test and a dev tool find a database, so neither invents its own.

/// Default `DATABASE_URL` for local dev/test Postgres, used by
/// [`test_database_url`] when the env var isn't set.
///
/// Assumes a local `enroute` database; every caller manages its own schema
/// within it, so no database-per-purpose is needed.
// No username: falls back to `PGUSER`/the OS user, matching `psql`'s own
// default — don't add an explicit one here.
const DEFAULT_TEST_DATABASE_URL: &str = "postgresql://localhost:5432/enroute";

/// `DATABASE_URL` from the environment, or a local `enroute` on 5432.
///
/// Test/dev only — production code should read the var directly and fail
/// loudly if it is missing.
#[must_use]
pub fn test_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_TEST_DATABASE_URL.to_string())
}
