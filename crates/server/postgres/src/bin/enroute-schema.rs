//! Applying the schema out of band, for a deployment that runs the server
//! with `--migrate off`.
//!
//! The same ordered steps the server itself would apply, over the same ledger,
//! so which of the two ran them is not a difference a database can tell.
#![allow(
    clippy::print_stdout,
    reason = "admin CLI binary, never compiled into the server"
)]

use anyhow::Result;
use clap::Parser;

use enroute_config::ObjectUri;

#[derive(Parser)]
#[command(name = "enroute-schema", version)]
struct Cli {
    /// The deployment to apply against, as the server's own `--config`
    /// names it.
    ///
    /// The same file the server reads, so a schema is never applied to one
    /// database while another is served.
    ///
    /// # Only `[database]`
    /// This runs before a deployment is complete, so it waits on no bucket
    /// and no signing key.
    #[arg(long, env = "ENROUTE_CONFIG", value_name = "URI")]
    config: ObjectUri,
    /// Say what is outstanding and apply none of it.
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let deployment = enroute_config::JustDatabase::read(&cli.config).await?;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(deployment.database.url.expose())
        .await?;
    if cli.dry_run {
        report(
            "outstanding",
            &enroute_postgres::schema::pending(&pool).await?,
        );
    } else {
        report("applied", &enroute_postgres::schema::apply(&pool).await?);
    }
    pool.close().await;
    Ok(())
}

/// Print what a schema's run came to, naming each step rather than counting.
///
/// An operator reading this is deciding whether to run it for real, and a
/// count of three says nothing about which three.
fn report(verb: &str, steps: &[String]) {
    if steps.is_empty() {
        println!("up to date");
        return;
    }
    println!("{} step(s) {verb}", steps.len());
    for step in steps {
        println!("  {step}");
    }
}
