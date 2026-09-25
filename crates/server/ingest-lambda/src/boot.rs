//! Building the worker out of what the call carries.
//!
//! Built on the first invocation rather than in `main`: the credentials arrive
//! with the push, one deployment's per call. A warm invocation reuses what it
//! built, for as long as the call keeps naming the same things.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Once, PoisonError};

use anyhow::{Context as _, Result, anyhow};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use sqlx::postgres::PgPoolOptions;

use enroute_config::{Bucket, CredentialSource, StoreUri};
use enroute_git_ingest::LocalIngestWorker;
use enroute_git_store::Store;

use crate::server::Server;
use crate::wire;

/// Where the worker stages a push's intermediates, exclusively owned in
/// the sense [`LocalIngestWorker::spawn_startup_staging_sweep`] needs.
const STAGING_DIR: &str = "/tmp/enroute-staging";

/// Everything one call needs standing before its push can run.
#[derive(Debug, Clone)]
pub struct Derived {
    /// The worker, over the bucket the call named.
    pub server: Server,
    /// The bucket a staged push and a staged pack are read back from.
    pub packs: Arc<dyn ObjectStore>,
}

/// What the last call built, kept while the next asks for the same.
///
/// Not a `OnceCell`: every part of the key arrives with the push, so what an
/// environment built once is not what it must answer with forever.
static KEPT: Mutex<Option<(u64, Derived)>> = Mutex::new(None);

/// Whether this environment has already reclaimed [`STAGING_DIR`].
///
/// The directory is the process's and not any one call's, so a rebuild for
/// new credentials has nothing new left in it to find.
static SWEPT: Once = Once::new();

/// Everything that has to hold before a push can be attempted.
///
/// # Errors
/// Returns an error if a required variable is missing, the database is
/// unreachable, or either store cannot be built.
pub async fn ready(call: &wire::Call) -> Result<Derived> {
    let built = call.fingerprint();
    if let Some(kept) = kept(built) {
        return Ok(kept);
    }

    // No lock held across the connect: Lambda runs one invocation per
    // environment at a time, so there is no second builder to race.
    let derived = build(call).await?;
    *KEPT.lock().unwrap_or_else(PoisonError::into_inner) = Some((built, derived.clone()));
    Ok(derived)
}

/// What was built for `built`, if that is what was built last.
fn kept(built: u64) -> Option<Derived> {
    let kept = KEPT.lock().unwrap_or_else(PoisonError::into_inner);
    kept.as_ref()
        .filter(|(seen, _)| *seen == built)
        .map(|(_, derived)| derived.clone())
}

/// Connect the pool and build both stores out of the call.
async fn build(call: &wire::Call) -> Result<Derived> {
    // Sized by the caller, not by this function: one push runs at a time here,
    // but how far its fan-out may open depends on the database at the far end
    // rather than on the environment this happens to be running in.
    let database = &call.credentials.database;
    let pool = PgPoolOptions::new()
        .max_connections(database.max_connections.get())
        .connect(database.url.secret().expose())
        .await
        .context("connecting to the database")?;

    // Neither bucket is this function's to choose: the front door serves what
    // this writes and uploads what this reads, so it says where and with what.
    let objects =
        store(&call.objects, &call.credentials.objects).context("building the object store")?;
    let packs = store(&call.staging, &call.credentials.staging)
        .with_context(|| format!("building a client for {}", call.staging.to_uri()))?;

    std::fs::create_dir_all(STAGING_DIR).context("creating the staging directory")?;
    let staging: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(STAGING_DIR)?);

    let state =
        enroute_postgres::storage(&pool, Arc::clone(&objects), Arc::new(Store::new(objects)));
    let worker = LocalIngestWorker::new(state, staging);

    // Reclaims what a previous invocation left in `/tmp`: a push killed
    // mid-flight, whose `StagingSession` never dropped.
    SWEPT.call_once(|| worker.spawn_startup_staging_sweep());

    Ok(Derived {
        server: Server::new(worker, memory_mb()?),
        packs,
    })
}

/// The store at `uri`, reached from where the call said to reach it.
///
/// Built through [`Bucket`] when the call sent credentials, so both ends fold
/// a credential in the same way.
fn store(uri: &StoreUri, given: &wire::Given) -> Result<Arc<dyn ObjectStore>> {
    match given {
        wire::Given::Environment => uri.build(),
        wire::Given::Credentials(credentials) => {
            bucket(uri, credentials).build(CredentialSource::Sent)
        }
    }
}

/// A bucket as the front door spells one.
fn bucket(uri: &StoreUri, credentials: &BTreeMap<String, wire::Credential>) -> Bucket {
    Bucket {
        uri: uri.clone(),
        credentials: credentials
            .iter()
            .map(|(name, value)| (name.clone(), value.secret().clone()))
            .collect(),
    }
}

/// What Lambda bills this function's duration against, read from the variable
/// the runtime sets.
///
/// Hard-fails rather than defaulting: a default would report zero
/// GB-seconds as free instead of unmeasured.
fn memory_mb() -> Result<u64> {
    const KEY: &str = "AWS_LAMBDA_FUNCTION_MEMORY_SIZE";

    std::env::var(KEY)
        .map_err(|_unset| anyhow!("{KEY} is unset"))?
        .parse()
        .with_context(|| format!("{KEY} must be a u64"))
}
