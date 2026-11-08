//! `enroute diagnose`: whether a deployment's parts can reach each other.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context as _, Result};
use serde_json::{Value, json};

use enroute::config::{Bucket, Config, ObjectUri, StoreUri};
use enroute_ledger::cluster;

/// How long one node has to answer `/status`.
const STATUS_TIMEOUT: Duration = Duration::from_secs(3);

/// The `enroute diagnose` arguments.
#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    /// The object store to check, instead of the one `--config` names.
    #[arg(long, value_name = "STORE_URI")]
    bucket: Option<StoreUri>,
    /// Print JSON.
    #[arg(long)]
    json: bool,
}

/// One check, and what it found.
struct Check {
    name: String,
    ok: bool,
    detail: String,
    status: Option<Value>,
}

/// Run `enroute diagnose`, which fails if a check does.
///
/// # Errors
///
/// Returns an error if neither a configuration nor a bucket is given, the
/// configuration does not load, or a check fails.
pub(crate) async fn run(config: Option<&ObjectUri>, args: Args) -> Result<()> {
    let (bucket, local) = match (config, args.bucket) {
        (Some(_), Some(_)) => anyhow::bail!("give --config or --bucket, not both"),
        (None, None) => anyhow::bail!("give --config <URI> (or ENROUTE_CONFIG), or --bucket"),
        (None, Some(uri)) => (
            Bucket {
                uri,
                credentials: BTreeMap::new(),
            },
            None,
        ),
        (Some(uri), None) => {
            let config = Config::read(uri).await?;
            let local = config.node.internal_listen;
            (config.bucket, Some(local))
        }
    };
    let checks = checks(&bucket, local).await;
    if args.json {
        let rows: Vec<Value> = checks
            .iter()
            .map(|c| json!({ "check": c.name, "ok": c.ok, "detail": c.detail, "status": c.status }))
            .collect();
        super::print_json(&Value::Array(rows))?;
    } else {
        let mut text = String::new();
        for c in &checks {
            let mark = if c.ok { "ok  " } else { "FAIL" };
            writeln!(text, "{mark}  {}: {}", c.name, c.detail)?;
        }
        super::print(&text)?;
    }
    let failed = checks.iter().filter(|c| !c.ok).count();
    anyhow::ensure!(failed == 0, "{failed} of {} checks failed", checks.len());
    Ok(())
}

/// Every check, in order, with a check that cannot run as failed.
async fn checks(bucket: &Bucket, local: Option<SocketAddr>) -> Vec<Check> {
    let mut checks = Vec::new();
    let check = |name: &str, ok: bool, detail: String| Check {
        name: name.to_string(),
        ok,
        detail,
        status: None,
    };
    let objects = match bucket.store().map(|store| store.objects()) {
        Ok(objects) => objects,
        Err(error) => {
            checks.push(check("object store", false, format!("{error:#}")));
            return checks;
        }
    };
    let named = match cluster::named(&objects).await {
        Ok(named) => named,
        Err(error) => {
            checks.push(check("object store", false, format!("{error:#}")));
            return checks;
        }
    };
    checks.push(check("object store", true, "reachable".to_string()));
    let url = |id: u64| named.iter().find(|m| m.id == id).map(|m| m.url.clone());
    let members = cluster::members(&objects).await;
    match &members {
        Ok(m) if !named.is_empty() => checks.push(check(
            "members",
            true,
            format!(
                "members {:?}, removed {:?}, {} alive",
                m.members,
                m.removed,
                m.alive.len()
            ),
        )),
        Ok(_) => {}
        Err(error) => checks.push(check("members", false, format!("{error:#}"))),
    }
    let members = members.unwrap_or_default();
    let mut targets: Vec<(String, String)> = Vec::new();
    for &id in &members.members {
        let live = members.alive.contains(&id);
        let address = url(id).unwrap_or_default();
        let detail = if live { "alive" } else { "not answering" };
        checks.push(check(
            &format!("ledger node {id:016x}"),
            live,
            format!("{detail} {address}"),
        ));
        if live && let Some(u) = url(id) {
            targets.push((format!("node {id:016x} /status"), u));
        }
    }
    // A node with no advertise_url is in no cluster: ask the one configured.
    if targets.is_empty()
        && let Some(addr) = local
    {
        let host = if addr.ip().is_unspecified() {
            SocketAddr::from(([127, 0, 0, 1], addr.port()))
        } else {
            addr
        };
        targets.push(("local node /status".to_string(), format!("http://{host}")));
    }
    let client = reqwest::Client::builder().timeout(STATUS_TIMEOUT).build();
    for (name, base) in targets {
        let found = match &client {
            Ok(client) => status(client, &base).await,
            Err(error) => Err(anyhow::anyhow!("{error}")),
        };
        checks.push(match found {
            Ok(body) => Check {
                name,
                ok: true,
                detail: format!("{base} answered"),
                status: Some(body),
            },
            Err(error) => check(&name, false, format!("{base}: {error:#}")),
        });
    }
    checks
}

/// A node's `/status`, as JSON.
async fn status(client: &reqwest::Client, base: &str) -> Result<Value> {
    let url = format!("{}/status", base.trim_end_matches('/'));
    let response = client.get(&url).send().await?.error_for_status()?;
    let body = response.bytes().await?;
    serde_json::from_slice(&body).context("the status is not JSON")
}
