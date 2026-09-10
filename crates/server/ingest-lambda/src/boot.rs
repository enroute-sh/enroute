//! Building the worker out of its environment.
//!
//! Runs on the first invocation rather than in `main`: the credentials come
//! from Secrets Manager, whose extension only answers during `INVOKE`. A warm
//! invocation then reuses the pool and the object store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use sqlx::postgres::PgPoolOptions;

use enroute_config::StoreUri;
use enroute_git_ingest::LocalIngestWorker;
use enroute_git_store::Store;

use crate::server::Server;

/// Where the worker stages a push's intermediates, exclusively owned in
/// the sense [`LocalIngestWorker::spawn_startup_staging_sweep`] needs.
const STAGING_DIR: &str = "/tmp/enroute-staging";

/// How long to wait on the secrets extension.
///
/// A loopback call, so anything near this is a failure, not slowness.
const SECRETS_TIMEOUT: Duration = Duration::from_secs(5);

/// The function's secret.
///
/// Deliberately opaque: holds a database URL and two credential sets that
/// nothing outside this module should print.
pub struct Secret(HashMap<String, String>);

impl std::fmt::Debug for Secret {
    /// Key names only — whatever logs this must not leak credentials.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Secret")
            .field(&self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Secret {
    fn get(&self, key: &str) -> Result<&str> {
        self.0
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| anyhow!("{key} missing from the function's secret"))
    }

    /// What every span export carries, if this deployment exports any.
    ///
    /// In the secret rather than the environment, because it is where the
    /// collector's token goes. `key=value,key=value`, as OTLP specifies.
    #[must_use]
    pub fn otlp_headers(&self) -> Option<&str> {
        self.get("OTEL_EXPORTER_OTLP_HEADERS").ok()
    }
}

/// Fetch the function's secret from the Parameters and Secrets extension.
///
/// # Errors
/// Returns an error if `ENROUTE_SECRET_ID` is unset, the extension doesn't
/// answer, or the secret isn't a flat JSON object of strings.
pub async fn secret() -> Result<Secret> {
    #[derive(serde::Deserialize)]
    struct Envelope {
        #[serde(rename = "SecretString")]
        secret_string: String,
    }

    let secret_id = env("ENROUTE_SECRET_ID")?;
    // Set by the runtime, and the extension's own authentication.
    let session = env("AWS_SESSION_TOKEN")?;

    let url = format!("http://localhost:2773/secretsmanager/get?secretId={secret_id}");
    let response = reqwest::Client::new()
        .get(&url)
        .header("X-Aws-Parameters-Secrets-Token", session)
        .timeout(SECRETS_TIMEOUT)
        .send()
        .await
        .context("asking the secrets extension")?;

    let status = response.status();
    let body = response.text().await.context("reading the secret")?;
    if !status.is_success() {
        // Body deliberately not included: on a failure it is an error document,
        // but that is not worth betting a credential on.
        bail!("the secrets extension returned {status}");
    }

    let envelope: Envelope =
        serde_json::from_str(&body).map_err(|e| anyhow!("parsing the extension's reply: {e}"))?;
    let values = serde_json::from_str(&envelope.secret_string)
        .map_err(|e| anyhow!("the secret is not a flat JSON object of strings: {e}"))?;
    Ok(Secret(values))
}

/// What an environment holds across invocations, and the call cannot name.
///
/// The pool because it is the expensive half of a cold start, the secret
/// because the extension only answers during `INVOKE`.
#[derive(Debug, Clone)]
pub struct Boot {
    pool: sqlx::PgPool,
    /// The storage credentials, as the option keys a [`StoreUri`] folds in.
    ///
    /// [`Secret`] values, so the derived `Debug` prints no key — redaction
    /// being the type's job rather than each struct's.
    ///
    /// [`Secret`]: enroute_config::Secret
    credentials: Vec<(String, enroute_config::Secret)>,
    memory_mb: u64,
}

/// Connect the pool and take the credentials out of `secret`.
///
/// # Errors
/// Returns an error if a required variable is missing, or the database is
/// unreachable.
pub async fn boot(secret: &Secret) -> Result<Boot> {
    // Sized for one push, not a fleet: an environment serves one invocation at
    // a time, so anything more only consumes the database's connection budget.
    // A `NonZeroU32` for the same reason `Database::max_connections` is one at
    // the front door: a pool of zero is a process that cannot serve.
    let max_connections: std::num::NonZeroU32 = env("DATABASE_MAX_CONNECTIONS")?
        .parse()
        .context("DATABASE_MAX_CONNECTIONS must be a non-zero u32")?;
    let pool = PgPoolOptions::new()
        .max_connections(max_connections.get())
        .connect(secret.get("DATABASE_URL")?)
        .await
        .context("connecting to the database")?;

    Ok(Boot {
        pool,
        credentials: vec![
            (
                "access_key_id".to_string(),
                enroute_config::Secret::from(secret.get("STORAGE_ACCESS_KEY_ID")?),
            ),
            (
                "secret_access_key".to_string(),
                enroute_config::Secret::from(secret.get("STORAGE_SECRET_ACCESS_KEY")?),
            ),
        ],
        memory_mb: memory_mb()?,
    })
}

/// Build the worker over the bucket the call named.
///
/// The bucket is not this function's to choose: the front door serves what
/// this writes, so it says where, and the credentials are folded over that.
///
/// # Errors
/// Returns an error if the store cannot be built or the staging directory
/// cannot be created.
pub fn server(boot: &Boot, objects: &StoreUri) -> Result<Server> {
    let credentials: Vec<(String, String)> = boot
        .credentials
        .iter()
        .map(|(name, value)| (name.clone(), value.expose().to_string()))
        .collect();
    let objects = objects
        .build_with(&credentials)
        .context("building the object store")?;

    std::fs::create_dir_all(STAGING_DIR).context("creating the staging directory")?;
    let staging: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(STAGING_DIR)?);

    let state = enroute_postgres::storage(
        &boot.pool,
        Arc::clone(&objects),
        Arc::new(Store::new(objects)),
    );
    let worker = LocalIngestWorker::new(state, staging);

    // Reclaims what a previous invocation on this environment left in `/tmp`:
    // a push killed mid-flight, whose `StagingSession` never dropped.
    worker.spawn_startup_staging_sweep();

    Ok(Server::new(worker, boot.memory_mb))
}

/// What Lambda bills this function's duration against, read from the variable
/// the runtime sets.
///
/// Hard-fails rather than defaulting: a default would report zero
/// GB-seconds as free instead of unmeasured.
fn memory_mb() -> Result<u64> {
    env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")?
        .parse()
        .context("AWS_LAMBDA_FUNCTION_MEMORY_SIZE must be a u64")
}

/// One thing an environment keeps between invocations, under its key.
///
/// Both of these key on something the call decides, so neither can be a
/// `OnceCell`: a front door pointed elsewhere gets a fresh one.
#[derive(Debug)]
pub struct Kept<T>(Mutex<Option<(String, T)>>);

impl<T: Clone> Kept<T> {
    /// An empty one, for a `static`.
    #[expect(
        clippy::new_without_default,
        reason = "only a `static` makes one, and a `const fn` is what that needs"
    )]
    pub const fn new() -> Self {
        Self(Mutex::new(None))
    }

    /// What was kept for `key`, or what `build` makes of it.
    ///
    /// # Errors
    ///
    /// Whatever `build` returned.
    pub fn get_or_build(&self, key: String, make: impl FnOnce() -> Result<T>) -> Result<T> {
        let mut cached = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, kept)) = cached.as_ref().filter(|(seen, _)| *seen == key) {
            return Ok(kept.clone());
        }

        let fresh = make()?;
        *cached = Some((key, fresh.clone()));
        Ok(fresh)
    }
}

/// The staging client, kept against the credentials and URI it was built from.
static PACKS: Kept<Arc<dyn ObjectStore>> = Kept::new();

/// The staging bucket the front door uploads packs to, as the call named it.
///
/// Kept on the role's rotating credentials as well as the URI, so a warm push
/// skips a handshake and a rotation does not reuse a stale client.
///
/// # Errors
/// Returns an error if `AWS_ACCESS_KEY_ID` is unset or the store cannot be
/// built.
pub fn packs(staging: &StoreUri) -> Result<Arc<dyn ObjectStore>> {
    // Keyed on the access key id: replaced along with the rest of the set, and
    // an identifier rather than a credential, so retaining it costs nothing.
    let key = format!("{}\n{}", env("AWS_ACCESS_KEY_ID")?, staging.to_uri());
    PACKS.get_or_build(key, || {
        staging
            .build()
            .with_context(|| format!("building a client for {}", staging.to_uri()))
    })
}

fn env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_unset| anyhow!("{key} is unset"))
}
