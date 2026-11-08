//! Enroute's entry point: the `enroute.api.v1alpha1` contract over gRPC,
//! git over smart HTTP, and the operator's commands.
//!
//! Two public listeners, because they answer two different callers. The
//! contract is for an application; the git port is for a git client. Names,
//! members and permission are an application's business and are not built in
//! here — Enroute asks the hook endpoint for the ones a deployment enables.
//! What a deployment is — where objects live, where the node keeps its log,
//! what each listener binds — is one file this is pointed at. `enroute dev`
//! writes that file in memory instead, and serves it the same way.

mod cli;

use std::io::Write as _;

use anyhow::Result;
use clap::{CommandFactory as _, FromArgMatches as _, Parser, Subcommand};

use enroute::config::{Config, ObjectUri};
use enroute_ledger::cluster;

/// Under load, the system allocator costs a compute much of its CPU.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "enroute", version, about = "The headless Git platform.")]
struct Cli {
    /// Where this deployment is configured, as a URL down to the object.
    ///
    /// Any store the configuration's own `bucket` could name:
    /// `file:///etc/enroute/enroute.toml`, `s3://config/enroute.toml`.
    #[arg(long, global = true, env = "ENROUTE_CONFIG", value_name = "URI")]
    config: Option<ObjectUri>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Serve Git, the gRPC API and the internal listener (the default).
    Serve,
    /// Run a local node with no configuration, for development.
    Dev(cli::dev::Args),
    /// Create, get, list and delete repositories.
    Repo {
        #[command(flatten)]
        api: cli::data::Api,
        #[command(subcommand)]
        command: cli::data::RepoCommand,
    },
    /// List a repository's refs.
    Refs {
        #[command(flatten)]
        api: cli::data::Api,
        #[command(flatten)]
        refs: cli::data::RefsArgs,
    },
    /// Make a commit on a branch without a checkout.
    Commit {
        #[command(flatten)]
        api: cli::data::Api,
        #[command(flatten)]
        commit: cli::data::CommitArgs,
    },
    /// Check the object store, the ledger's nodes and each node's status.
    Diagnose(cli::diagnose::Args),
    /// List the ledger's members, or remove one for good.
    ///
    /// Remove a node whose disk is lost, or that is retired: every log then
    /// drops it. Run it on a ledger node: it signs with that node's key.
    Members {
        /// The node id to remove.
        #[arg(long, value_name = "ID")]
        remove: Option<u64>,
        /// Remove it even if the members left have no live majority.
        #[arg(long, requires = "remove")]
        force: bool,
        /// Print JSON.
        #[arg(long)]
        json: bool,
    },
}

fn main() -> Result<()> {
    // mimalloc frees and reuses short-lived buffers in fresh 2 MiB regions, so
    // with transparent huge pages the kernel zeroes each region again, which
    // costs pushes and cold clones much of their CPU.
    #[cfg(target_os = "linux")]
    rustix::thread::disable_transparent_huge_pages(true)?;
    let command = cli::dotenv::with_defaults(Cli::command(), std::path::Path::new("."));
    let cli = Cli::from_arg_matches(&command.get_matches()).unwrap_or_else(|error| error.exit());
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // Blocking work is disk I/O and layer reads. Each thread adds to the
        // cost of shared structures (page slots, allocator arenas), so more
        // than the disk needs only costs CPU.
        .max_blocking_threads(64)
        .build()?
        .block_on(async move {
            let config = cli.config;
            match cli.command.unwrap_or(Command::Serve) {
                Command::Serve => serve(&required(config)).await,
                Command::Dev(args) => cli::dev::run(args).await,
                Command::Repo { api, command } => cli::data::repo(&api, command).await,
                Command::Refs { api, refs } => cli::data::refs(&api, refs).await,
                Command::Commit { api, commit } => cli::data::commit(&api, commit).await,
                Command::Diagnose(args) => cli::diagnose::run(config.as_ref(), args).await,
                Command::Members {
                    remove,
                    force,
                    json,
                } => members(&required(config), remove, force, json).await,
            }
        })
}

/// The `--config` a command needs, or a usage error.
fn required(config: Option<ObjectUri>) -> ObjectUri {
    // Not `required = true`: `dev` and the data commands take no file.
    config.unwrap_or_else(|| {
        Cli::command()
            .error(
                clap::error::ErrorKind::MissingRequiredArgument,
                "--config <URI> (or ENROUTE_CONFIG) is required",
            )
            .exit()
    })
}

async fn serve(uri: &ObjectUri) -> Result<()> {
    // Read before the subscriber exists, because where spans go is one of the
    // things it says. A failure here is `main`'s return value rather than a
    // log line, which is the cost of the configuration being complete.
    let config = Config::read(uri).await?;
    run(config, Some(uri), "info", || {}).await
}

/// Serve the deployment `config` describes until a listener stops or the
/// process gets a stop signal.
///
/// The console logs at `console` unless `RUST_LOG` is set. `ready` runs once
/// the node has started.
///
/// # Errors
///
/// Returns an error if the node does not start, or a listener fails.
pub(crate) async fn run(
    config: Config,
    uri: Option<&ObjectUri>,
    console: &str,
    ready: impl FnOnce(),
) -> Result<()> {
    let _flush = enroute::telemetry::init(config.telemetry.as_ref(), Some(&config.node), console)?;
    if let Some(uri) = uri {
        tracing::info!(%uri, "read the configuration");
    }

    // A ledger node serves only the internal listener, so binds nothing else.
    let public = config.node.role.compute().then_some(&config.listen);
    // Loud, because it is the whole of what stands between a stranger and this
    // deployment's repositories.
    if let Some(listen) = public
        && !listen.api.ip().is_loopback()
    {
        tracing::warn!(
            api = %listen.api,
            "the contract listener authenticates nobody; it must not be reachable \
             except by the application"
        );
    }

    let reach = enroute::Reach {
        private: config.git.allow_private_remotes,
    };
    let listeners = enroute::Listeners::bind(
        public.map(|l| l.api),
        public.map(|l| l.git),
        config.node.internal_listen,
    )
    .await?;
    let hooks = config
        .endpoint()?
        .map(enroute::Endpoint::hooks)
        .transpose()?
        .unwrap_or_default();
    let enroute = enroute::Enroute::start(config.node()?, hooks, reach).await?;
    ready();
    enroute.serve(listeners, stop_signal()).await
}

/// Ctrl-C, or SIGTERM where there is one.
async fn stop_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!("cannot listen for Ctrl-C: {error}");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    {
        let terminate = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut signal) => drop(signal.recv().await),
                Err(error) => {
                    tracing::warn!("cannot listen for SIGTERM: {error}");
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            () = interrupt => {}
            () = terminate => {}
        }
    }
    #[cfg(not(unix))]
    interrupt.await;
}

/// Print the ledger's members, after removing `remove` if it names one.
async fn members(uri: &ObjectUri, remove: Option<u64>, force: bool, json: bool) -> Result<()> {
    let config = Config::read(uri).await?;
    let _flush = enroute::telemetry::init(None, None, "info")?;
    let objects = config.bucket.store()?.objects();
    let members = match remove {
        Some(id) => cluster::remove(&objects, &config.node.data_dir, id, force).await?,
        None => cluster::members(&objects).await?,
    };
    if json {
        return cli::print_json(&serde_json::json!({
            "members": members.members,
            "removed": members.removed,
        }));
    }
    let list = |ids: &[u64]| match ids {
        [] => "none".to_owned(),
        ids => ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", "),
    };
    writeln!(
        std::io::stdout().lock(),
        "members: {}\nremoved: {}",
        list(&members.members),
        list(&members.removed)
    )?;
    Ok(())
}
