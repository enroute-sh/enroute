//! Load-test / benchmark harness for `git-upload-pack` fetches.
#![allow(
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "load-test harness, never compiled into the production build; \
              panicking on setup failure is fine here, the cost/latency \
              report is meant to be read on the terminal, and request/byte \
              counts in a benchmark run stay far under 2^53 so u64-as-f64 \
              conversions for the dollar-cost math are exact"
)]

mod client;
mod cost;
mod hooks;
mod seed;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use bench_support::{drop_scratch_schema, scratch_metadata_pool};
use clap::Parser;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use rlt::{BenchSuite, IterInfo, IterReport, Status};
use tokio::time::Instant;

use bench_support::store::{LatencyProfile, LatencyStore, units_since};
use client::GitHttpClient;
use cost::{AuroraReadsReport, AuroraStorageReport, DataReport, SectionReport};
use enroute_git_cost::{CountingStore, Meter, StoreRole, StoreUnits};
use enroute_git_retrieve::Storage;
use enroute_git_store::Store;

// ── server ────────────────────────────────────────────────────────────────────

/// The bench server's sole user name, shared with `seed.rs`.
///
/// It needs it to send the same `Host` override this module sends.
pub(crate) const OWNER: &str = "bench";

// ── bench suite ───────────────────────────────────────────────────────────────

/// One iteration = the request sequence of a fresh clone, driven over raw
/// smart-HTTP v2 by [`GitHttpClient`] with the pack drained and discarded.
///
/// Shelling out to real `git clone` instead measures the bench machine, not
/// the server: working-tree churn, `index-pack`, and spawns dominate.
#[derive(Clone)]
struct CloneBench {
    url: String,
    /// `Host` header override, needed against a enroute server, which reads
    /// the namespace from `Host` — `None` for real remotes (GitHub, ...).
    host_override: Option<String>,
    depth: Option<u64>,
    /// Number of completed `bench()` calls, tracked independently of `rlt`
    /// so the final report can compute a per-clone request/cost breakdown.
    iterations: Arc<AtomicU64>,
}

#[async_trait]
impl BenchSuite for CloneBench {
    type WorkerState = GitHttpClient;

    async fn state(&self, _worker_id: u32) -> Result<Self::WorkerState> {
        Ok(GitHttpClient::new(
            self.url.clone(),
            self.host_override.clone(),
        ))
    }

    async fn bench(
        &mut self,
        client: &mut Self::WorkerState,
        _info: &IterInfo,
    ) -> Result<IterReport> {
        let start = Instant::now();
        client.capabilities().await?;
        let tip = client.ls_refs_tip().await?;
        let bytes = client.fetch(&tip, self.depth).await?;
        let duration = start.elapsed();
        self.iterations.fetch_add(1, Ordering::Relaxed);

        Ok(IterReport {
            duration,
            status: Status::success(200),
            bytes,
            items: 1,
        })
    }
}

// ── CLI ───────────────────────────────────────────────────────────────────────

/// Benchmark fresh-clone fetches against an in-memory enroute server, speaking
/// smart-HTTP v2 directly and discarding the returned packs.
#[derive(Parser)]
struct Cli {
    /// Number of commits to seed in the repo, ignored if `--from-repo` is given.
    #[arg(long, default_value = "10", env = "BENCH_COMMITS")]
    commits: usize,

    /// Push an existing local git repo's checked-out commit into the bench
    /// server instead of generating a synthetic one; ignores `--commits`.
    #[arg(long, value_name = "PATH", env = "BENCH_FROM_REPO")]
    from_repo: Option<std::path::PathBuf>,

    /// Run the clone bench against an existing remote repo URL instead of
    /// standing up an in-memory enroute server, as a baseline to compare against.
    ///
    /// Skips seeding and the storage-cost report, which only apply to the
    /// local in-memory backend this harness otherwise stands up.
    #[arg(
        long,
        value_name = "URL",
        env = "BENCH_AGAINST",
        conflicts_with_all = ["commits", "from_repo", "no_latency", "serve_only"]
    )]
    against: Option<String>,

    /// Override the `Host` header sent with every `--against` request.
    ///
    /// Only needed against a enroute server, which reads the namespace from
    /// `Host` — real remotes (GitHub, ...) don't need this.
    #[arg(
        long,
        value_name = "HOST",
        env = "BENCH_AGAINST_HOST",
        requires = "against"
    )]
    against_host: Option<String>,

    /// Stand up the local in-memory enroute server, seed it, print its URL,
    /// and block instead of running the clone bench.
    ///
    /// Meant to be the sole process a profiler traces, with a separate
    /// `--against` invocation driving load — see `dev/profiling.md`.
    #[arg(long)]
    serve_only: bool,

    /// Disable simulated S3 latency, exercising the in-memory backend's raw
    /// (near-zero-latency) throughput instead.
    #[arg(long)]
    no_latency: bool,

    /// Benchmark shallow clones (`--depth=N`) instead of full (the default).
    #[arg(long, value_name = "N", env = "BENCH_SHALLOW_DEPTH")]
    shallow_depth: Option<u64>,

    #[command(flatten)]
    rlt: rlt::cli::BenchCli,
}

/// Seed the bench server's repo per `cli`'s `--from-repo`/`--commits`
/// selection.
async fn seed(url: &str, cli: &Cli) -> Result<()> {
    match &cli.from_repo {
        Some(path) => seed::seed_via_existing_repo(url, path).await,
        None => seed::seed_via_push(url, cli.commits).await,
    }
}

/// Run the clone bench against an existing remote repo (`--against`)
/// instead of a local enroute server, for a side-by-side latency comparison.
///
/// No seeding, no store/metadata/cost report — just a fresh clone's requests.
async fn bench_remote(
    url: String,
    host_override: Option<String>,
    depth: Option<u64>,
    rlt: rlt::cli::BenchCli,
) -> Result<()> {
    let iterations = Arc::new(AtomicU64::new(0));
    let suite = CloneBench {
        url,
        host_override,
        depth,
        iterations,
    };
    rlt::cli::run(rlt, suite).await
}

/// Stand up a local in-memory enroute server, seed it per `cli`, and block
/// serving forever — see `--serve-only`.
///
/// Wraps the store in neither decorator: with no bench loop to report costs
/// for, that would be unused overhead in the profile this keeps clean.
async fn serve_forever(cli: &Cli) -> Result<()> {
    let owner = OWNER;
    let repo = "repo";

    let store = Arc::new(Store::new(Arc::new(InMemory::new())));
    let staging: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Sized generously since a separately-run `--against` client's
    // concurrency isn't known here.
    let (metadata_pool, schema) = scratch_metadata_pool("loadtest", 32).await?;
    let state = enroute_postgres::storage(&metadata_pool, Arc::new(InMemory::new()), store);
    enroute_postgres::schema::apply(&metadata_pool).await?;
    let created = state.rows.create(None).await?;
    let token = bench_support::hooks::TOKEN;

    let addr = serve_git(
        repo,
        state,
        staging,
        ([127, 0, 0, 1], 0).into(),
        metadata_pool.clone(),
        created.id,
    )
    .await?;
    // Credentials embedded in the URL: git and reqwest both read
    // `user:secret@host` as Basic auth without setup. No owner segment in
    // the path — Enroute reads the tenant from `Host`, via `--against-host`.
    let url = format!("http://{owner}:{token}@{addr}/{repo}.git");
    seed(&url, cli).await?;

    println!("serving {url}");
    println!("drive load against it from another terminal, e.g.:");
    println!(
        "  cargo run --profile profiling -p load-test -- --against {url} --against-host {owner} -c 20 -d 30s"
    );
    println!("Ctrl-C to stop");

    tokio::signal::ctrl_c().await?;

    // This mode only ever ends by being interrupted, so skipping this would
    // leave a scratch schema behind after every single profiling run.
    drop_scratch_schema(&metadata_pool, &schema).await;

    Ok(())
}

/// Seed and clone-bench a local in-memory enroute server, then print the
/// storage-cost report.
async fn bench_local(cli: Cli) -> Result<()> {
    let owner = OWNER;
    let repo = "repo";

    let (primary_latency, staging_latency) = if cli.no_latency {
        (None, None)
    } else {
        (
            Some(LatencyProfile::production()),
            Some(LatencyProfile::express()),
        )
    };
    // Counting outside the delay, so a request is charged once whether or
    // not the latency model is on. A meter each rather than one meter's two
    // roles: `StoreRole::Handoff` is the handoff bucket, and this staging
    // store is the local scratch that role documents itself as excluding.
    let raw_primary: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let primary_meter = Meter::new();
    let object_store = CountingStore::wrap(
        LatencyStore::wrap(Arc::clone(&raw_primary), primary_latency),
        Arc::clone(&primary_meter),
        StoreRole::Primary,
    );
    let raw_staging: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let staging_meter = Meter::new();
    let staging = CountingStore::wrap(
        LatencyStore::wrap(Arc::clone(&raw_staging), staging_latency),
        Arc::clone(&staging_meter),
        StoreRole::Primary,
    );
    let store = Arc::new(Store::new(Arc::clone(&object_store)));

    // Full multi-connection pool sized to the bench's concurrency — a load
    // test wants realistic pool behavior under concurrent clones, not the
    // single serialized connection `EphemeralStorage` uses.
    let (metadata_pool, schema) =
        scratch_metadata_pool("loadtest", cli.rlt.concurrency.get()).await?;
    let state = enroute_postgres::storage(&metadata_pool, Arc::new(InMemory::new()), store);
    enroute_postgres::schema::apply(&metadata_pool).await?;
    // Measured right after DDL, before any rows exist, and subtracted from
    // the end-of-run size below so empty-table overhead isn't misattributed
    // to the seed/clone workload.
    let baseline_bytes = schema_storage_bytes(&metadata_pool, &schema).await?;

    let created = state.rows.create(None).await?;
    let token = bench_support::hooks::TOKEN;

    let addr = serve_git(
        repo,
        state,
        staging,
        ([127, 0, 0, 1], 0).into(),
        metadata_pool.clone(),
        created.id,
    )
    .await?;

    // Credentials embedded in the URL: git and reqwest both read
    // `user:secret@host` as Basic auth without setup. No owner segment in
    // the path — Enroute reads the tenant from `Host`, via `CloneBench.host_override`.
    let url = format!("http://{owner}:{token}@{addr}/{repo}.git");
    seed(&url, &cli).await?;
    let after_seed = primary_meter.units().primary;
    let after_seed_staging = staging_meter.units().primary;
    // Aurora reads come from Postgres's cumulative `pg_statio_user_tables`
    // counters (unlike the S3 metrics above), so isolating the clone phase
    // means snapshotting here and diffing against the final read below.
    let after_seed_reads = schema_reads(&metadata_pool, &schema).await?;

    let iterations = Arc::new(AtomicU64::new(0));
    let suite = CloneBench {
        url,
        host_override: Some(owner.to_string()),
        depth: cli.shallow_depth,
        iterations: Arc::clone(&iterations),
    };

    rlt::cli::run(cli.rlt, suite).await?;

    // Diffed against `baseline_bytes` so the reported figure reflects the
    // seed/clone workload's own storage, not the empty schema's DDL overhead.
    let aurora_bytes = schema_storage_bytes(&metadata_pool, &schema)
        .await?
        .saturating_sub(baseline_bytes);
    let final_reads = schema_reads(&metadata_pool, &schema).await?;
    let clone_reads = final_reads.saturating_sub(after_seed_reads);

    // Explicit awaited cleanup rather than `Drop`-time so it reliably runs
    // before exit on the success path; won't run if killed/panicked first,
    // an acceptable risk for a manually-invoked dev tool.
    drop_scratch_schema(&metadata_pool, &schema).await;

    print_cost_report(
        AllUnits {
            primary: after_seed,
            staging: after_seed_staging,
        },
        AllUnits {
            primary: primary_meter.units().primary,
            staging: staging_meter.units().primary,
        },
        &raw_primary,
        &raw_staging,
        AuroraSnapshot {
            bytes: aurora_bytes,
            seed_reads: after_seed_reads,
            clone_reads,
        },
        iterations.load(Ordering::Relaxed),
    )
    .await
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.against.clone() {
        Some(url) => bench_remote(url, cli.against_host.clone(), cli.shallow_depth, cli.rlt).await,
        None if cli.serve_only => serve_forever(&cli).await,
        None => bench_local(cli).await,
    }
}

/// Total on-disk size (tables + their indexes/toast) of this run's schema,
/// used as the Aurora storage-cost input.
///
/// Measured directly from Postgres, since the metadata store itself
/// doesn't track row/index overhead.
async fn schema_storage_bytes(pool: &sqlx::PgPool, schema: &str) -> Result<u64> {
    let bytes: Option<i64> = sqlx::query_scalar(
        "SELECT sum(pg_total_relation_size(c.oid))::bigint \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind = 'r'",
    )
    .bind(schema)
    .fetch_one(pool)
    .await?;
    Ok(bytes.unwrap_or(0).max(0).cast_unsigned())
}

/// Physical page reads (buffer cache misses) against this run's schema.
///
/// Only the read side of Aurora's I/O billing (see [`cost::AuroraReadsReport`]).
async fn schema_reads(pool: &sqlx::PgPool, schema: &str) -> Result<u64> {
    let reads: Option<i64> = sqlx::query_scalar(
        "SELECT sum(heap_blks_read + COALESCE(idx_blks_read, 0) \
                + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0))::bigint \
         FROM pg_statio_user_tables WHERE schemaname = $1",
    )
    .bind(schema)
    .fetch_one(pool)
    .await?;
    Ok(reads.unwrap_or(0).max(0).cast_unsigned())
}

// ── cost report ───────────────────────────────────────────────────────────────

/// Both stores' units at one point in time, bundled for report handoff.
struct AllUnits {
    primary: StoreUnits,
    staging: StoreUnits,
}

/// Aurora storage/reads figures read back from Postgres, bundled for report
/// handoff alongside [`AllUnits`].
struct AuroraSnapshot {
    /// Total on-disk size of this run's schema, at the very end of the run.
    bytes: u64,
    /// Physical page reads accumulated during seeding.
    seed_reads: u64,
    /// Physical page reads accumulated during the clone loop only.
    clone_reads: u64,
}

/// Sum the size of every object currently held in the store.
///
/// Uses the raw store directly so this audit listing doesn't itself
/// perturb the metrics being reported.
async fn total_bytes_stored(object_store: &Arc<dyn ObjectStore>) -> Result<u64> {
    use futures::TryStreamExt;

    let mut total = 0u64;
    let mut objects = object_store.list(None);
    while let Some(meta) = objects.try_next().await? {
        total = total.saturating_add(meta.size);
    }
    Ok(total)
}

async fn print_cost_report(
    after_seed: AllUnits,
    final_units: AllUnits,
    raw_primary: &Arc<dyn ObjectStore>,
    raw_staging: &Arc<dyn ObjectStore>,
    aurora: AuroraSnapshot,
    clone_iterations: u64,
) -> Result<()> {
    let clone_only = units_since(final_units.primary, after_seed.primary);
    let clone_only_staging = units_since(final_units.staging, after_seed.staging);
    let bytes_stored = total_bytes_stored(raw_primary).await?;
    let bytes_stored_staging = total_bytes_stored(raw_staging).await?;

    println!("\n── cost report ─────────────────────────────────────────────");

    println!("seed:");
    // Excluded from SectionReport's total (it's read-only, see
    // AuroraReadsReport) but still printed alongside the other backends.
    println!(
        "{}",
        AuroraReadsReport {
            reads: aurora.seed_reads,
        }
    );
    println!(
        "{}",
        SectionReport::new(after_seed.primary, after_seed.staging)
    );

    println!("\nclone ({clone_iterations} clones, avg per clone):");
    if let Some(averaged) = (AuroraReadsReport {
        reads: aurora.clone_reads,
    })
    .per_clone(clone_iterations)
    {
        println!("{averaged}");
    }
    let clone_report = SectionReport::new(clone_only, clone_only_staging);
    match clone_report.per_clone(clone_iterations) {
        Some(averaged) => println!("{averaged}"),
        None => println!("  (no clones completed)"),
    }

    println!("\ndata (storage/month):");
    println!(
        "{}",
        DataReport {
            s3_standard: bytes_stored,
            s3_express: bytes_stored_staging,
            aurora: AuroraStorageReport {
                bytes: aurora.bytes,
            },
        }
    );

    Ok(())
}

/// Serve Enroute's git front door, and behind it the hook endpoint it asks.
///
/// What a client drives is the same front door a deployment runs, application
/// hop included — skipping it would measure a server nobody has.
async fn serve_git(
    repo_name: &str,
    state: Storage,
    staging: Arc<dyn ObjectStore>,
    bind: std::net::SocketAddr,
    ledger: sqlx::PgPool,
    repo: enroute_git_core::RepoId,
) -> Result<std::net::SocketAddr> {
    let signing_key = bench_support::hooks::signing_key();
    // Bound before it is served, so the URL both ends sign against is known
    // before either of them needs it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint_addr = listener.local_addr()?;
    let endpoint_url = bench_support::hooks::endpoint_url(&format!("http://{endpoint_addr}"));
    bench_support::serve_listener_in_background(
        hooks::router(
            repo_name,
            repo.as_i64(),
            vec![signing_key.verifying_key()],
            &endpoint_url,
        ),
        listener,
    );

    // Named once the endpoint has an address, since a tenant is what says
    // where its application answers, and `*` because the bench listens on
    // loopback under whatever `Host` a client sends.
    let directory = enroute::tenancy::Directory::from_toml(&format!(
        "[tenants.bench]\n\
         hook_endpoint_url = \"{endpoint_url}\"\ndomains = [\"*\"]\n"
    ))?;
    let tenants = Arc::new(enroute::tenancy::Tenants::new(directory, ledger));
    // The repository was minted straight into the engine above, so nothing
    // claimed it on the way through.
    let tenant = tenants
        .by_id("bench")
        .ok_or_else(|| anyhow::anyhow!("the tenant just named"))?;
    let key = repo_name
        .parse()
        .map_err(|bad| anyhow::anyhow!("the bench repository key: {bad}"))?;
    tenants.claim(&tenant, repo, &key).await?;

    let authorizer = Arc::new(enroute::hooks::Hooks::new(
        Arc::clone(&tenants),
        signing_key,
        // The stub answers in-process, so this only has to be non-zero.
        std::time::Duration::from_secs(10),
    )?);
    let worker = enroute_git_ingest::LocalIngestWorker::shared(state.clone(), staging);
    // The same application client at both: the load test drives the production
    // path.
    let hooks: Arc<dyn enroute_git_ingest::ReceiveHooks> = authorizer.clone();
    Ok(bench_support::serve_in_background(
        enroute::git::router(
            state,
            worker,
            authorizer,
            hooks,
            Arc::new(enroute_git_http::AllRefsVisible),
        ),
        bind,
    )
    .await)
}
