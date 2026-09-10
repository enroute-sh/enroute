//! Measures what one push costs the ingest path.
//!
//! # Two regimes, and which number to read
//!
//! CPU-bound `--storage none` reads **instructions retired** (see
//! [`counters`]); with a latency model, read **wall time** — mixing ranks nothing.
//!
//! # What it still doesn't model
//!
//! Postgres round trips (`queries` column), a shared pipe ([`bench_support::store`]
//! gives each request its own throughput), and the client (a real pack arrives over the network).

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "a benchmark harness, never compiled into the production build; \
              its report is meant to be read on the terminal"
)]

mod corpus;
mod counters;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use bench_support::collect::{Collector, KeyedBy, SpanTotals};
use bench_support::store::{LatencyProfile, LatencyStore};
use clap::{Parser, ValueEnum};
use cpu_time::ProcessTime;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer as _};

use bench_support::{drop_scratch_schema, scratch_metadata_pool};
use enroute_git_cost::{CountingStore, Meter, StoreRole, StoreUnits};
use enroute_git_ingest::{IngestRequest, IngestWorker, LocalIngestWorker, noop_progress};
use enroute_git_retrieve::{RefUpdate, RefsMap, RepoMetadata};
use enroute_git_store::Store;
use gix_hash::ObjectId;

use corpus::Corpus;
use counters::Counters;

const BRANCH: &str = "refs/heads/main";

/// Matches the fan-out cap ingest holds itself to, so the pool is never the
/// thing under test.
const POOL_CONNECTIONS: u32 = 64;

/// Noise floor: what the system-wide counter picks up from everything that
/// isn't this benchmark.
const IDLE_SAMPLE: Duration = Duration::from_secs(1);

/// The latency model to put in front of the permanent object store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum Latency {
    /// No delay at all, leaving the run CPU-bound.
    ///
    /// Rank on instructions.
    None,
    /// Same-region S3 Standard: ~20ms to first byte, ~85 MB/s.
    S3,
    /// Same-AZ S3 Express One Zone: ~3ms to first byte, ~300 MB/s.
    Express,
}

impl Latency {
    fn profile(self) -> Option<LatencyProfile> {
        match self {
            Self::S3 => Some(LatencyProfile::production()),
            Self::Express => Some(LatencyProfile::express()),
            Self::None => None,
        }
    }
}

/// Where a push's staging lives for the duration of the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum Staging {
    /// A real directory on disk, as the ingest Lambda uses (`/tmp`, via
    /// `LocalFileSystem`).
    Disk,
    /// Free staging, isolating the push from whatever this machine's disk is
    /// doing.
    Memory,
}

#[derive(Debug, Parser)]
#[command(about = "Measure what ingesting one push costs")]
struct Cli {
    /// Build (and cache) the pack a first push of this checkout's `HEAD`
    /// would send, then measure ingesting it.
    #[arg(long, value_name = "PATH", required_unless_present = "pack")]
    from_repo: Option<PathBuf>,

    /// Measure an existing packfile instead of building one.
    ///
    /// Needs `--tip` unless a sidecar `.json` sits beside it.
    #[arg(long, value_name = "PATH", conflicts_with = "from_repo")]
    pack: Option<PathBuf>,

    /// The commit `--pack`'s ref update should point at.
    #[arg(long, value_name = "SHA", requires = "pack")]
    tip: Option<String>,

    /// How many times to ingest the pack.
    ///
    /// The report's headline is the median, so an even count wastes a run.
    #[arg(long, default_value_t = 3, value_name = "N")]
    runs: usize,

    /// Latency model for the permanent object store.
    ///
    /// Defaults to none, keeping the run CPU-bound and comparable across runs.
    #[arg(long, value_enum, default_value_t = Latency::None)]
    storage: Latency,

    /// Where staging goes, defaulting to disk to match the ingest Lambda.
    #[arg(long, value_enum, default_value_t = Staging::Disk)]
    staging: Staging,

    /// Emit the report as JSON on stdout and nothing else, for a caller that
    /// is comparing runs rather than reading them.
    #[arg(long)]
    json: bool,
}

/// What one store was asked to do during a run.
///
/// Requests rather than time, since a count survives the machine it ran on —
/// multiply by the target deployment's round trip for its floor.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
struct StoreOps {
    /// GET and HEAD.
    get_class: u64,
    /// PUT, COPY and LIST, including each part of a completed multipart.
    put_class: u64,
    deletes: u64,
    bytes_down: u64,
    bytes_up: u64,
}

impl From<StoreUnits> for StoreOps {
    fn from(u: StoreUnits) -> Self {
        Self {
            get_class: u.get_class,
            put_class: u.put_class,
            deletes: u.deletes,
            bytes_down: u.bytes_read,
            bytes_up: u.bytes_written,
        }
    }
}

impl StoreOps {
    /// The number to multiply by a round trip.
    fn requests(self) -> u64 {
        self.get_class + self.put_class + self.deletes
    }
}

/// What one ingestion cost.
#[derive(Debug, Clone, serde::Serialize)]
struct RunReport {
    run: usize,
    instructions_process: Option<u64>,
    /// The one that can see Postgres, and so the one to optimise.
    instructions_system: Option<u64>,
    /// User plus system, across every thread.
    cpu_ms: u64,
    wall_ms: u64,
    /// Rows in `objects` afterwards — half the correctness digest.
    objects: i64,
    /// The other half.
    commits: i64,
    primary: StoreOps,
    staging: StoreOps,
    /// Excludes `COPY FROM STDIN` bodies, which `sqlx` doesn't log.
    queries: u64,
    spans: BTreeMap<String, SpanTotals>,
}

/// A whole invocation: what was measured, and what it cost.
#[derive(Debug, serde::Serialize)]
struct Report {
    corpus: Corpus,
    /// So a run is reproducible from the report alone.
    pack: String,
    /// Decides whether the instruction columns or the wall column is the
    /// headline.
    storage: Latency,
    staging: Staging,
    /// The resolve pool is sized from it, so reports from two machines aren't
    /// comparable without it.
    parallelism: usize,
    /// Anything the platform refused to measure, so an empty column is never
    /// mistaken for a zero.
    notes: Vec<String>,
    /// The noise floor under `instructions_system`; compare against it before
    /// trusting a small difference.
    idle_instructions_per_s: Option<u64>,
    /// The headline number.
    median_instructions_system: Option<u64>,
    median_instructions_process: Option<u64>,
    /// Reported, not optimised.
    median_cpu_ms: u64,
    /// Reported, not optimised.
    median_wall_ms: u64,
    runs: Vec<RunReport>,
}

fn median<T: Ord + Copy>(mut values: Vec<T>) -> Option<T> {
    values.sort_unstable();
    values.get(values.len() / 2).copied()
}

/// Everything is built fresh per run — schema, stores, repository.
///
/// A second push of the same objects into the same repo would find them
/// already recorded and measure almost nothing.
async fn run_once(
    cli: &Cli,
    pack: &'static [u8],
    tip: ObjectId,
    collector: &Collector,
    counters: &mut Counters,
    run: usize,
) -> Result<RunReport> {
    let (pool, schema) = scratch_metadata_pool("push_bench", POOL_CONNECTIONS).await?;
    // Before the collector is reset, so the statements the schema takes are
    // not counted against the push being measured.
    enroute_postgres::schema::apply(&pool).await?;
    let outcome = ingest_into(cli, &pool, pack, tip, collector, counters, run).await;
    drop_scratch_schema(&pool, &schema).await;
    outcome
}

/// The caller has to hold the returned directory: dropping it deletes the tree.
fn staging_backend(mode: Staging) -> Result<(Arc<dyn ObjectStore>, Option<tempfile::TempDir>)> {
    match mode {
        Staging::Memory => Ok((Arc::new(InMemory::new()), None)),
        Staging::Disk => {
            let dir = tempfile::tempdir().context("creating a staging directory")?;
            let backend = LocalFileSystem::new_with_prefix(dir.path())
                .context("opening the staging directory")?;
            Ok((Arc::new(backend), Some(dir)))
        }
    }
}

/// The measured half of [`run_once`], split out so its scratch schema is
/// dropped whether or not this succeeds.
async fn ingest_into(
    cli: &Cli,
    pool: &sqlx::PgPool,
    pack: &'static [u8],
    tip: ObjectId,
    collector: &Collector,
    counters: &mut Counters,
    run: usize,
) -> Result<RunReport> {
    // Not wrapped in a counter here: `ingest` charges the primary store to
    // the meter it is handed, so counting again outside would double every
    // request the push made.
    let primary = LatencyStore::wrap(Arc::new(InMemory::new()), cli.storage.profile());
    let meter = Meter::new();
    let state = enroute_postgres::storage(
        pool,
        Arc::new(InMemory::new()),
        Arc::new(Store::new(primary)),
    );
    let repo: RepoMetadata = state.rows.create(None).await?;
    // Bound, never read: it has to outlive the push it holds the tree for.
    let (staging_backend, _staging_dir) = staging_backend(cli.staging)?;
    // Counted but never delayed: on disk the filesystem supplies the real
    // latency, and in memory the point of the mode is that there is none.
    // Its own meter, since `ingest` charges only the primary store to the
    // one it is given.
    let staging_meter = Meter::new();
    let staging = CountingStore::wrap(
        staging_backend,
        Arc::clone(&staging_meter),
        StoreRole::Primary,
    );

    let worker = LocalIngestWorker::new(state, staging);
    let request = IngestRequest {
        repo,
        existing: RefsMap::new(),
        updates: vec![RefUpdate {
            refname: BRANCH.to_owned(),
            old_id: ObjectId::null(gix_hash::Kind::Sha1),
            new_id: tip,
        }],
    };

    collector.reset();
    let before = counters.sample();
    let cpu = ProcessTime::now();
    let wall = Instant::now();
    let ingested = worker
        .ingest(
            request,
            enroute_git_ingest::IncomingPack {
                reader: Box::new(Cursor::new(pack)),
                len_hint: u64::try_from(pack.len()).ok(),
            },
            &noop_progress,
            &meter,
        )
        .await?;
    let wall_ms = u64::try_from(wall.elapsed().as_millis()).unwrap_or(u64::MAX);
    let cpu_ms = u64::try_from(cpu.elapsed().as_millis()).unwrap_or(u64::MAX);
    let retired = counters.sample().since(before);

    // A push that rejected its ref did some other, cheaper thing than the one
    // being measured — a failed measurement, not a slow one. Only ingestion
    // runs here, so nothing refused or skipped is the proof the work happened.
    if let Some((refname, rejection)) = ingested.rejected.iter().next() {
        bail!("push rejected {refname}: {rejection:?}");
    }
    if let Some(refname) = ingested.screened.first() {
        bail!("push screened {refname} before storing anything for it");
    }

    Ok(RunReport {
        run,
        instructions_process: retired.process,
        instructions_system: retired.system,
        cpu_ms,
        wall_ms,
        primary: meter.units().primary.into(),
        staging: staging_meter.units().primary.into(),
        queries: collector.queries(),
        // After the counters, so the digest's own two queries aren't in them.
        // One table numbers every kind now, and `kind = 1` is a commit
        // (`enroute_git_core::kind_to_u8`).
        objects: sqlx::query_scalar("SELECT count(*) FROM object_seqs")
            .fetch_one(pool)
            .await?,
        commits: sqlx::query_scalar("SELECT count(*) FROM object_seqs WHERE kind = 1")
            .fetch_one(pool)
            .await?,
        spans: collector.snapshot(),
    })
}

/// The correctness gate: the cheapest way to make ingestion faster is to do
/// less of it, and a measurement not pinned to work stored would reward that.
fn check_runs_agree(runs: &[RunReport]) -> Result<()> {
    let Some(first) = runs.first() else {
        return Ok(());
    };
    for run in runs {
        if (run.objects, run.commits) != (first.objects, first.commits) {
            bail!(
                "run {} stored {}/{} objects/commits but run {} stored {}/{}",
                run.run,
                run.objects,
                run.commits,
                first.run,
                first.objects,
                first.commits
            );
        }
    }
    Ok(())
}

async fn idle_rate(counters: &mut Counters) -> Option<u64> {
    let before = counters.sample();
    tokio::time::sleep(IDLE_SAMPLE).await;
    let idle = counters.sample().since(before).system?;
    idle.checked_div(IDLE_SAMPLE.as_secs())
}

/// Display only — integer division rather than `f64`, so no measurement
/// passes through a cast.
fn secs(ms: u64) -> String {
    format!("{}.{:02}s", ms / 1000, (ms % 1000) / 10)
}

/// Instructions in billions; see [`secs`].
fn giga(n: Option<u64>) -> String {
    n.map_or_else(
        || "-".to_owned(),
        |n| {
            format!(
                "{}.{:03}G",
                n / 1_000_000_000,
                (n % 1_000_000_000) / 1_000_000
            )
        },
    )
}

/// Bytes as mebibytes; see [`secs`].
fn mib(bytes: u64) -> String {
    format!("{}.{:02}M", bytes / (1 << 20), (bytes % (1 << 20)) / 10486)
}

fn print_store(label: &str, ops: StoreOps) {
    println!(
        "  {label:<8} {:>7} req   get-class {:>6}  put-class {:>6}  del {:>5}   \
         down {:>9}  up {:>9}",
        ops.requests(),
        ops.get_class,
        ops.put_class,
        ops.deletes,
        mib(ops.bytes_down),
        mib(ops.bytes_up)
    );
}

fn print_report(report: &Report) {
    println!(
        "{} objects, {} bytes, tip {}",
        report.corpus.objects, report.corpus.bytes, report.corpus.tip
    );
    println!("pack {}", report.corpus.pack_id);
    println!(
        "{} run(s), resolve pool sized from parallelism {}",
        report.runs.len(),
        report.parallelism
    );
    // Stated rather than left to the reader: reading instructions off a
    // latency-bound run, or wall off a CPU-bound one, is the mistake this
    // harness invites.
    match report.storage {
        Latency::None => println!(
            "storage: no latency, staging on {:?} — rank on instructions, not time",
            report.staging
        ),
        model => println!(
            "storage: {model:?} latency, staging on {:?} — rank on wall time",
            report.staging
        ),
    }
    if let Some(idle) = report.idle_instructions_per_s {
        println!("idle machine retires {}/s", giga(Some(idle)));
    }
    for note in &report.notes {
        println!("note: {note}");
    }

    println!(
        "\n  run     instr(sys)   instr(proc)        cpu       wall    queries    objects    commits"
    );
    for run in &report.runs {
        println!(
            "  {:>3} {:>14} {:>13} {:>10} {:>10} {:>10} {:>10} {:>10}",
            run.run,
            giga(run.instructions_system),
            giga(run.instructions_process),
            secs(run.cpu_ms),
            secs(run.wall_ms),
            run.queries,
            run.objects,
            run.commits
        );
    }
    println!(
        "\n  median instr(sys) {}   instr(proc) {}   cpu {}   wall {}\n",
        giga(report.median_instructions_system),
        giga(report.median_instructions_process),
        secs(report.median_cpu_ms),
        secs(report.median_wall_ms)
    );

    if let Some(last) = report.runs.last() {
        println!("i/o (last run)");
        print_store("primary", last.primary);
        print_store("staging", last.staging);
        println!();
    }

    // The last run's spans rather than an average: mixing runs would invent
    // numbers no single push produced.
    if let Some(last) = report.runs.last() {
        println!("phases (last run)");
        for (name, totals) in &last.spans {
            println!("  {name}  x{}  {}", totals.count, secs(totals.wall_ms()));
            for (field, value) in &totals.fields {
                println!("      {field} = {value}");
            }
        }
    }
}

async fn measure(cli: &Cli, counters: &mut Counters, mut notes: Vec<String>) -> Result<Report> {
    let collector = Collector::keyed_by(KeyedBy::Name);
    tracing_subscriber::registry()
        .with(collector.clone().with_filter(EnvFilter::new(
            // `sqlx` logs each statement at debug, which is what makes the
            // query count free.
            "enroute_git_ingest=info,enroute_git_store=info,enroute_git_retrieve=info,sqlx::query=debug",
        )))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(EnvFilter::from_default_env()),
        )
        .try_init()?;

    let quiet = cli.json;
    let (pack_path, corpus) = match (&cli.from_repo, &cli.pack) {
        (Some(repo), _) => corpus::for_repo(repo, &corpus::cache_dir()?, |note| {
            if !quiet {
                eprintln!("{note}");
            }
        })?,
        (None, Some(pack)) => corpus::for_pack(pack, cli.tip.as_deref())?,
        (None, None) => bail!("one of --from-repo or --pack is required"),
    };

    let tip = ObjectId::from_hex(corpus.tip.as_bytes())
        .with_context(|| format!("{} is not an object id", corpus.tip))?;
    // Leaked so the reader handed to `ingest` can be `'static`, as the
    // worker's boxed `PackReader` requires.
    let pack: &'static [u8] = Vec::leak(
        std::fs::read(&pack_path).with_context(|| format!("reading {}", pack_path.display()))?,
    );

    let idle = idle_rate(counters).await;

    let mut runs = Vec::with_capacity(cli.runs);
    for run in 0..cli.runs {
        if !quiet {
            eprintln!("run {run}...");
        }
        runs.push(run_once(cli, pack, tip, &collector, counters, run).await?);
    }
    check_runs_agree(&runs)?;

    if runs.iter().any(|r| r.instructions_system.is_none()) {
        notes.push("no system-wide count, so Postgres is not in these numbers".to_owned());
    }

    Ok(Report {
        corpus,
        pack: pack_path.display().to_string(),
        storage: cli.storage,
        staging: cli.staging,
        parallelism: std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
        notes,
        idle_instructions_per_s: idle,
        median_instructions_system: median(
            runs.iter().filter_map(|r| r.instructions_system).collect(),
        ),
        median_instructions_process: median(
            runs.iter().filter_map(|r| r.instructions_process).collect(),
        ),
        median_cpu_ms: median(runs.iter().map(|r| r.cpu_ms).collect()).unwrap_or_default(),
        median_wall_ms: median(runs.iter().map(|r| r.wall_ms).collect()).unwrap_or_default(),
        runs,
    })
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Before the runtime is built, and so before any thread this process will
    // ever have: an inherited counter follows threads created after it opens,
    // and tokio's workers are created with the runtime.
    let (mut counters, notes) = Counters::start();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let report = runtime.block_on(measure(&cli, &mut counters, notes))?;

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report(&report);
    }
    Ok(())
}
