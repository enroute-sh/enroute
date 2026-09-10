//! Who Enroute's customers are, and which of them a request belongs to.
//!
//! Who the tenants are is configuration — a [`Directory`] read from a URI and
//! re-read from it — so a tenant is something a deployment reviews, diffs and
//! rolls back, and nothing can add one over the wire. What must still be a row
//! is which tenant a repository belongs to, since a repository comes into being
//! while the process runs: that is the *ledger*, and it keys by a tenant's
//! [`TenantId`], which is the one thing about a tenant that must never change.
//! A hostname may move, and no repository moves with it. The ledger also holds
//! what each tenant calls each of its repositories, which is what an
//! application addresses one by: a key is unique to a tenant, so resolving it
//! ([`Tenants::resolve`]) can only ever land inside the tenant that asked.

pub mod directory;
pub mod refresh;
mod repo_key;

use std::sync::Arc;

use std::time::Duration;

use anyhow::{Context as _, Result};
use sqlx::PgPool;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use url::Url;

use crate::ObjectUri;

use enroute_git_core::RepoId;

pub use directory::Directory;
pub use refresh::{Refresh, Source};
pub use repo_key::{BadRepoKey, MAX_REPO_KEY_BYTES, RepoKey};

/// What a tenant is, for as long as they exist.
///
/// A type and not a `String`, because [`directory`] is then the only thing
/// that can mint one, and only from a name it has checked.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(String);

impl TenantId {
    /// The id as the ledger and a span want it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Mint one from a name `directory` has checked.
    pub(super) fn checked(name: String) -> Self {
        Self(name)
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// So a `HashMap<TenantId, _>` answers a lookup by name without minting one.
impl std::borrow::Borrow<str> for TenantId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// One customer of this Enroute.
///
/// Cloned onto every request that resolves to it, so it holds what a request
/// needs and nothing it would have to go back for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tenant {
    /// What the ledger keys a repository by, and what a span records.
    pub id: TenantId,
    /// Where this tenant's application answers.
    ///
    /// Parsed once where the list is read, so the hook path neither re-parses
    /// it per call nor carries an error for something already proven.
    pub hook_endpoint_url: Url,
}

/// Enroute's tenancy: the tenants it serves, whose each repository is, and
/// what each of them calls it.
#[derive(Debug, Clone)]
pub struct Tenants {
    /// A receiver rather than an `Arc`, because a request must read what is
    /// current without a refresh having to reach every holder of one.
    directory: watch::Receiver<Arc<Directory>>,
    /// The ledger, which is one table.
    pool: PgPool,
}

impl Tenants {
    /// Tenancy over a directory nothing will replace.
    #[must_use]
    pub fn new(directory: Directory, pool: PgPool) -> Self {
        let (sender, tenants) = Self::over(directory, pool);
        // Dropped, so nothing can replace them — which is what a caller
        // building a directory in memory rather than reading one means.
        drop(sender);
        tenants
    }

    /// Tenancy read from `uri` and re-read from it every `every`.
    ///
    /// The first read is fatal, since a process that never read its tenants
    /// would serve nobody and do it quietly; every later one keeps serving.
    ///
    /// # Errors
    ///
    /// Returns an error if the tenants cannot be read, or do not load.
    pub async fn from_uri(
        uri: &ObjectUri,
        pool: PgPool,
        every: Duration,
    ) -> Result<(Arc<Self>, JoinHandle<()>)> {
        let source = Source::new(uri)?;
        let (first, tag) = source
            .load()
            .await
            .context("the tenants this was told to serve")?;
        let (sender, tenants) = Self::over(first, pool);
        let running = tokio::spawn(Refresh::new(source, sender, tag).run(every));
        Ok((Arc::new(tenants), running))
    }

    fn over(directory: Directory, pool: PgPool) -> (watch::Sender<Arc<Directory>>, Self) {
        let (sender, receiver) = watch::channel(Arc::new(directory));
        (
            sender,
            Self {
                directory: receiver,
                pool,
            },
        )
    }

    /// The tenants a request reads now, which a refresh may already have
    /// replaced.
    fn current(&self) -> Arc<Directory> {
        // Cloned out from under the guard rather than borrowed across the
        // lookup: the guard holds a lock a refresh would then wait on.
        Arc::clone(&self.directory.borrow())
    }

    /// How many tenants this serves now.
    #[must_use]
    pub fn count(&self) -> usize {
        self.current().len()
    }

    /// Read what serving a request will read, so a schema nobody applied fails
    /// at startup rather than at the call that needed it.
    ///
    /// # Errors
    ///
    /// Returns an error if the ledger is not there.
    pub async fn ready(&self) -> Result<()> {
        sqlx::query("SELECT repo_id, repo_key FROM tenant_repositories LIMIT 0")
            .execute(&self.pool)
            .await
            .context("the tenancy schema is not applied: run enroute-schema")?;
        Ok(())
    }

    /// The tenant a request arriving on `host` belongs to.
    pub(crate) fn by_host(&self, host: &str) -> Option<Tenant> {
        // The port is not part of the name: a deployment reached on :8080 in
        // development is the same tenant as one reached on :443.
        let host = host.split(':').next().unwrap_or(host).trim().to_lowercase();
        self.current().by_host(&host)
    }

    /// The tenant with this id, if this still serves them.
    #[must_use]
    pub fn by_id(&self, id: &str) -> Option<Tenant> {
        self.current().by_id(id)
    }

    /// The tenant `repo` belongs to, if it is claimed and they are served.
    ///
    /// The only lookup starting from a repository, not from what the caller
    /// said — a push's hooks need it, since the `Host` is layers back by then.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is unavailable.
    pub(crate) async fn by_repo(&self, repo: RepoId) -> Result<Option<(Tenant, RepoKey)>> {
        let owner: Option<(String, String)> = sqlx::query_as(
            "SELECT tenant_id, repo_key FROM tenant_repositories WHERE repo_id = $1",
        )
        .bind(repo.as_i64())
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("looking up the tenant owning repository {repo}"))?;
        // Resolved through the directory rather than joined, because the
        // directory is not in this database: a tenant the list no longer names
        // stops answering for what they still own.
        let Some((id, key)) = owner else {
            return Ok(None);
        };
        // A key this cannot read is a row nothing wrote: the column is checked
        // on the way in, so failing here is a fault, not a caller's doing.
        let key: RepoKey = key.parse().context("a stored repository key")?;
        Ok(self.by_id(&id).map(|tenant| (tenant, key)))
    }

    /// The repository `tenant` calls `key`, if they have one.
    ///
    /// The only way in from a key, and the whole of this layer's isolation:
    /// whoever else holds the same key, it names nothing for this tenant.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is unavailable.
    pub(crate) async fn resolve(&self, tenant: &Tenant, key: &RepoKey) -> Result<Option<RepoId>> {
        let found: Option<i64> = sqlx::query_scalar(
            "SELECT repo_id FROM tenant_repositories WHERE tenant_id = $1 AND repo_key = $2",
        )
        .bind(tenant.id.as_str())
        .bind(key.as_str())
        .fetch_optional(&self.pool)
        .await
        .context("resolving a repository key")?;
        Ok(found.map(RepoId::new))
    }

    /// Up to `limit` of `tenant`'s repositories, in key order, after `after`.
    ///
    /// Keyset, not `OFFSET`: a claim or release mid-walk cannot slide one past.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is unavailable.
    pub(crate) async fn repositories(
        &self,
        tenant: &Tenant,
        after: Option<&RepoKey>,
        limit: u32,
    ) -> Result<Vec<(RepoId, RepoKey)>> {
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT repo_id, repo_key FROM tenant_repositories \
             WHERE tenant_id = $1 AND repo_key > $2 \
             ORDER BY repo_key LIMIT $3",
        )
        .bind(tenant.id.as_str())
        // A key starts with a letter or digit, so the empty string sorts
        // before every one of them and starts the walk.
        .bind(after.map_or("", RepoKey::as_str))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("listing the repositories of tenant {}", tenant.id))?;
        rows.into_iter()
            .map(|(id, key)| {
                Ok((
                    RepoId::new(id),
                    key.parse().context("a stored repository key")?,
                ))
            })
            .collect()
    }

    /// Record that `tenant` calls `repo` `key`, and report which repository
    /// holds it now — `repo` itself, or the one that was already there.
    ///
    /// A taken key is not an error: a create repeated with one key is the same
    /// create, and the caller compares what comes back with what it offered.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is unavailable, or if `repo` was
    /// already claimed — a fault, since the caller mints it from a sequence.
    pub async fn claim(&self, tenant: &Tenant, repo: RepoId, key: &RepoKey) -> Result<RepoId> {
        // One statement rather than an insert and a read: only the index can
        // say which of two racing creates won, and `DO UPDATE` is what makes
        // the loser's `RETURNING` name the winner. The update is a no-op, the
        // key being set to what it already holds.
        let held: i64 = sqlx::query_scalar(
            "INSERT INTO tenant_repositories (repo_id, tenant_id, repo_key) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, repo_key) \
             DO UPDATE SET repo_key = EXCLUDED.repo_key \
             RETURNING repo_id",
        )
        .bind(repo.as_i64())
        .bind(tenant.id.as_str())
        .bind(key.as_str())
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("claiming {key} for tenant {}", tenant.id))?;
        Ok(RepoId::new(held))
    }

    /// Forget that `repo` belonged to anybody.
    ///
    /// Idempotent: a repository never claimed is already released.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is unavailable.
    pub(crate) async fn release(&self, repo: RepoId) -> Result<()> {
        sqlx::query("DELETE FROM tenant_repositories WHERE repo_id = $1")
            .bind(repo.as_i64())
            .execute(&self.pool)
            .await
            .with_context(|| format!("releasing repository {repo}"))?;
        Ok(())
    }
}

/// A ledger on a `pg_temp` schema of its own, thrown away when the connection
/// closes.
///
/// One connection, pinned: `pg_temp` is per-session. The whole schema, since
/// tenancy's table is one step of it and the engine's are the rest.
///
/// # Errors
///
/// Returns an error if the database is unreachable or a step does not apply.
pub async fn ephemeral_pool(database_url: &str) -> Result<PgPool> {
    let pool = enroute_postgres::session_pool(database_url).await?;
    enroute_postgres::schema::apply(&pool).await?;
    Ok(pool)
}
