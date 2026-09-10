//! Enroute's maintenance pass, run beside the server rather than inside it.
//!
//! The same pass `[maintenance.run.in-process]` runs on a timer — see
//! `enroute::maintenance`. Pointed at the deployment's own configuration, so
//! it walks the bucket the server writes and erases on the same windows.
//! One shot by default, so a scheduler invokes it and it exits; `--every`
//! makes it a long-lived process instead, for a deployment that would rather
//! run a container than schedule one.
#![allow(
    clippy::print_stdout,
    reason = "admin CLI binary, never compiled into the server"
)]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use sqlx::postgres::PgPoolOptions;

use enroute::maintenance::{Maintenance, run};
use enroute_config::ObjectUri;
use enroute_git_store::Store;

#[derive(Parser)]
#[command(name = "enroute-maintenance", version)]
struct Cli {
    /// The deployment this maintains, as the server's own `--config` names it.
    ///
    /// Read from there rather than named again: a pass walking a different
    /// bucket from the server would find every object an orphan.
    #[arg(long, env = "ENROUTE_CONFIG", value_name = "URI")]
    config: ObjectUri,
    /// Report what a pass would take, and take nothing.
    #[arg(long)]
    dry_run: bool,
    /// Keep running, a pass every this many seconds, instead of exiting.
    #[arg(long, value_name = "SECS", value_parser = clap::value_parser!(u64).range(1..))]
    every: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Not the whole `Config`: this signs nothing, so it must not need the
    // hook signing key in its environment to start.
    let deployment = enroute_config::JustMaintenance::read(&cli.config).await?;

    // The URI carried its own prefix, so `Store` adds none of its own, and
    // one store rather than two: a second build is a second credential chain
    // and a second connection pool over the same bucket.
    let objects = deployment.bucket.build()?;
    let storage = enroute_postgres::storage(
        &PgPoolOptions::new()
            .connect(deployment.database.url.expose())
            .await?,
        objects.clone(),
        Arc::new(Store::new(objects)),
    );
    // From the file, not from a flag: the same pass runs in the server, and
    // two ends erasing on different windows means the shorter one decides.
    let config = Maintenance::from_config(&deployment.maintenance, cli.dry_run);

    let Some(every) = cli.every else {
        println!("{}", run(&storage, config).await?);
        return Ok(());
    };

    // A failure here is printed and waited out rather than returned: a
    // long-lived pass that exits on the first unavailable store is a
    // scheduler's job done worse than the scheduler would do it.
    let mut ticker = tokio::time::interval(Duration::from_secs(every));
    loop {
        ticker.tick().await;
        match run(&storage, config).await {
            Ok(pass) => println!("{pass}"),
            Err(error) => println!("maintenance pass failed: {error:#}"),
        }
    }
}
