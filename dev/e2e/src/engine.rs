//! Enroute, with one node that runs both roles, served in process.
//!
//! The only module that may name an Enroute crate, so the rest of the suite
//! tests the wire alone. It starts the node through `enroute::Enroute`, as the
//! binary does.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};

pub(crate) use enroute::config::PreReceiveMode;
use enroute::node::{Config, Store};
use enroute_compute::testing::settled;

/// Where a deployment calls its application, and the key it signs with.
#[derive(Debug)]
pub(crate) struct App {
    /// The full hook endpoint URL.
    pub(crate) endpoint_url: String,
    /// The key the deployment signs each hook call with.
    pub(crate) signing_key: enroute_signature::SigningKey,
    /// The hooks the deployment calls, by their configuration names.
    pub(crate) enabled: Vec<&'static str>,
    /// What `pre_receive` sees.
    pub(crate) pre_receive_mode: PreReceiveMode,
}

/// Every hook, by its configuration name.
pub(crate) const ALL_HOOKS: [&str; 4] =
    ["authorize", "visible_refs", "pre_receive", "post_receive"];

/// How long [`Enroute::maintain`] waits for maintenance to settle.
const MAINTAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Both doors over a node, with its disk and its object store in one
/// temporary directory.
///
/// Dropping one stops it.
#[derive(Debug)]
pub(crate) struct Enroute {
    // Fields drop in order: the node stops before its directory goes.
    serving: enroute::Serving,
    _data: tempfile::TempDir,
    git_url: String,
    grpc_url: String,
}

impl Enroute {
    /// Start an empty deployment that calls `app` for the hooks it enables,
    /// or calls no hook if there is no `app`.
    pub(crate) async fn start(app: Option<App>) -> Result<Self> {
        let data = tempfile::tempdir()?;
        let mut node = Config::local(
            data.path().join("node"),
            Store::local(&data.path().join("store"))?,
        );
        // Many deployments share one test process.
        node.cache_bytes = 64 << 20;
        node.page_cache_bytes = 32 << 20;
        // Flush and compact after a few pushes, so that reads meet stored
        // and merged layers too, not only inline ones.
        node.compute.flush_interval = Duration::from_millis(100);
        node.compute.merge_factor = 2;
        // The differential suite runs SHA-256 cases too.
        node.ledger.allow_sha256 = true;
        let hooks = match app {
            Some(app) => enroute::Endpoint {
                url: app.endpoint_url.parse().context("a hook endpoint URL")?,
                signing_key: app.signing_key,
                // The stub answers at once, so any budget above zero is enough.
                timeout: Duration::from_secs(10),
                enabled: app
                    .enabled
                    .iter()
                    .map(|name| name.parse())
                    .collect::<Result<_>>()?,
                pre_receive_mode: app.pre_receive_mode,
            }
            .hooks()?,
            None => enroute::HostHooks::default(),
        };
        let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
        let listeners = enroute::Listeners::bind(Some(loopback), Some(loopback), loopback).await?;
        let git_url = format!(
            "http://{}",
            listeners.git.as_ref().context("git")?.local_addr()?
        );
        let grpc_url = format!(
            "http://{}",
            listeners.api.as_ref().context("api")?.local_addr()?
        );
        // The suite's remotes are plain HTTP on loopback.
        let reach = enroute::Reach { private: true };
        let serving = enroute::Enroute::start(node, hooks, reach)
            .await?
            .spawn(listeners);

        Ok(Self {
            serving,
            _data: data,
            git_url,
            grpc_url,
        })
    }

    /// The git smart HTTP base URL, with no path, such as `http://127.0.0.1:8080`.
    pub(crate) fn git_url(&self) -> &str {
        &self.git_url
    }

    /// The gRPC contract endpoint, such as `http://127.0.0.1:50051`.
    pub(crate) fn grpc_url(&self) -> &str {
        &self.grpc_url
    }

    /// Wait until the background maintenance has flushed every layer of
    /// `repo` and has nothing left to compact.
    ///
    /// The node has no call to run maintenance on demand, but the short
    /// windows in [`Enroute::start`] make it due soon.
    pub(crate) async fn maintain(&self, repo: &str) -> Result<()> {
        let compute = self
            .serving
            .node()
            .compute()
            .context("a node that runs the compute")?;
        let deadline = Instant::now() + MAINTAIN_TIMEOUT;
        loop {
            let meta = compute
                .state(repo)
                .with_context(|| format!("the compute does not follow {repo}"))?;
            if settled(&meta, compute.config()) {
                return Ok(());
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "maintenance of {repo} did not settle in {MAINTAIN_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
