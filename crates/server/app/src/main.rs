//! Enroute's entry point: the `enroute.api.v1alpha1` contract over gRPC,
//! and git over smart HTTP.
//!
//! Two listeners, because they answer two different callers. The contract is
//! for an application; the git port is for a git client. Names, members and
//! permission are an application's business and are not built in here — Enroute
//! asks the hook endpoint for them, one POST per decision. What a deployment
//! is — where objects live, where a push's work runs, what each listener
//! binds — is one file this is pointed at, so no host, provider or bucket is
//! named in this repository and none of it is typed on a command line.

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;

use enroute_config::{
    Bucket, Config, CredentialSource, Ingest, Lambda, Migrate, ObjectUri, Run, Secret, Telemetry,
};
use enroute_git_ingest::{IngestWorker, LocalIngestWorker};
use enroute_git_store::Store;
use enroute_ingest_lambda::client::{LambdaConfig, LambdaIngestWorker};

#[derive(Parser, Debug)]
#[command(name = "enroute", version, about = "The headless git platform.")]
struct Cli {
    /// Where this deployment is configured, as a URL down to the object.
    ///
    /// Any store the configuration's own `bucket` could name:
    /// `file:///etc/enroute/enroute.toml`, `s3://config/enroute.toml`.
    #[arg(long, env = "ENROUTE_CONFIG", value_name = "URI")]
    config: ObjectUri,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Read before the subscriber exists, because where spans go is one of the
    // things it says. A failure here is `main`'s return value rather than a
    // log line, which is the cost of the configuration being complete.
    let config = Config::read(&cli.config).await?;
    enroute::telemetry::init(config.telemetry.as_ref())?;
    tracing::info!(uri = %cli.config, "configured from here");

    let objects = config.bucket.build(CredentialSource::Environment)?;

    // Every name below is unqualified, so the connection places them: the
    // tables land wherever its `search_path` resolves, which is the database
    // URL's to say.
    let pg_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(config.database.max_connections.get())
        .connect(config.database.url.expose())
        .await?;
    // Before anything reads a table: replicas start together, so every step
    // runs in its own transaction under one advisory lock, and `off` is a
    // deployment whose DBA applies the same steps with `enroute-schema`.
    match config.database.migrate {
        Migrate::Auto => {
            let applied = enroute_postgres::schema::apply(&pg_pool).await?;
            tracing::info!(
                applied = applied.len(),
                of = enroute_postgres::schema::steps().len(),
                "the schema this build reads"
            );
        }
        Migrate::Off => enroute_postgres::schema::verify(&pg_pool).await?,
    }
    // The URI carried its own prefix, so `Store` adds none of its own.
    let state = enroute_postgres::storage(&pg_pool, objects.clone(), Arc::new(Store::new(objects)));
    // Loud, because it is the whole of what stands between a stranger and this
    // deployment's repositories: the contract listener authenticates nobody.
    tracing::warn!(
        api = %config.listen.api,
        "the contract listener authenticates nobody; it must not be reachable \
         except by the application"
    );

    // Two different stores, for two different jobs: scratch this process
    // sweeps, or a bucket the function reads a pack back out of.
    let worker: Arc<dyn IngestWorker> = match config.ingest {
        Ingest::Local(local) => LocalIngestWorker::shared(state.clone(), local.scratch.build()?),
        Ingest::Lambda(lambda) => Arc::new(
            worker(
                lambda,
                config.bucket,
                config.database.url.clone(),
                config.telemetry.clone(),
            )
            .await?,
        ),
    };

    // Before serving, so a deployment that chose it is doing the work from
    // the moment it is up rather than from the first request.
    if let Run::InProcess(pass) = &config.maintenance.run {
        let every = std::time::Duration::from_secs(pass.interval_secs.get());
        tracing::info!(
            interval_secs = pass.interval_secs.get(),
            grace_secs = config.maintenance.grace_secs,
            deleted_grace_secs = config.maintenance.deleted_grace_secs,
            "maintenance runs here"
        );
        drop(enroute::maintenance::spawn(
            state.clone(),
            enroute::maintenance::Maintenance::from_config(&config.maintenance, false),
            every,
        ));
    }

    enroute::serve(
        state,
        worker,
        enroute::Endpoint {
            url: config.hooks.endpoint_url,
            signing_key: enroute_signature::SigningKey::from_pem(config.hooks.signing_key.expose())
                .map_err(|error| anyhow::anyhow!("the hooks signing key: {error}"))?,
            timeout: std::time::Duration::from_secs(config.hooks.timeout_secs.get()),
        },
        enroute_git_remote::Reach {
            private: config.sync.allow_private_remotes,
        },
        enroute::Listeners {
            api: config.listen.api,
            git: config.listen.git,
        },
    )
    .await
}

/// The worker a push's work runs in, when it does not run here.
///
/// Both stores go on every call rather than into the function's environment,
/// so the two ends cannot be pointed at different buckets.
///
/// # Errors
///
/// Returns an error if the handoff store or the AWS configuration cannot be
/// built.
async fn worker(
    lambda: Lambda,
    objects: Bucket,
    database_url: Secret,
    telemetry: Option<Telemetry>,
) -> Result<LambdaIngestWorker> {
    // The SDK's own chain, so the identity can be a role whose credentials
    // refresh rather than a key pair read once at startup.
    let aws = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(lambda.region))
        .load()
        .await;

    Ok(LambdaIngestWorker::new(
        aws_sdk_lambda::Client::new(&aws),
        LambdaConfig {
            tenant: lambda.tenant,
            function: lambda.function,
            qualifier: lambda.qualifier,
            max_pack_bytes: lambda.max_pack_bytes.get(),
            objects,
            objects_credentials: lambda.objects_credentials,
            staging: lambda.handoff,
            staging_credentials: lambda.handoff_credentials,
            database_url,
            database_max_connections: lambda.database_max_connections,
            telemetry,
        },
    )?)
}
