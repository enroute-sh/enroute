//! Enroute itself: the service that stores repositories and answers the
//! `enroute.api.v1alpha1` contract.
//!
//! It knows a repository by an opaque id and nothing else — not its name,
//! not who owns it, not who was allowed to ask. Every one of those questions
//! belongs to whatever application sits in front of this, which reaches it
//! the same way any other caller would. The repositories are a [`node`]'s,
//! run in this process, and the repository key is the engine's repository
//! name. This crate reads the [`config`], starts that node and serves what
//! it runs: git, the contract and the internal listener.

pub mod config;
mod git;
pub mod node;
pub mod telemetry;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use tokio::net::TcpListener;

use config::Hook;
pub use enroute_compute::HostHooks;
pub use enroute_compute::Reach;
use enroute_compute::{HookClient, PreReceiveMode};

use node::Node;

/// The bound listeners a node is served on.
///
/// Bound by the caller, so that a caller can bind port 0 and learn the port.
#[derive(Debug)]
pub struct Listeners {
    /// Where the contract is served, for an application to call.
    ///
    /// None on a node that runs no compute, which serves no contract.
    pub api: Option<TcpListener>,
    /// Where git is served, for a git client.
    ///
    /// None on a node that runs no compute, which serves no git.
    pub git: Option<TcpListener>,
    /// Where other nodes and operators reach this node: `GET /status`, and the
    /// gRPC between nodes.
    pub internal: TcpListener,
}

impl Listeners {
    /// Bind each address given.
    ///
    /// # Errors
    ///
    /// Returns an error if an address cannot be bound.
    pub async fn bind(
        api: Option<SocketAddr>,
        git: Option<SocketAddr>,
        internal: SocketAddr,
    ) -> Result<Self> {
        let bind = |at: SocketAddr, what: &'static str| async move {
            TcpListener::bind(at)
                .await
                .with_context(|| format!("binding the {what} listener on {at}"))
        };
        let api = match api {
            Some(at) => Some(bind(at, "contract").await?),
            None => None,
        };
        let git = match git {
            Some(at) => Some(bind(at, "git").await?),
            None => None,
        };
        Ok(Self {
            api,
            git,
            internal: bind(internal, "internal").await?,
        })
    }
}

/// The application this deployment asks, and what it is asked with.
///
/// One struct because these travel together everywhere: the URL is what a
/// signature covers, so a key without the URL it signs for proves nothing.
#[derive(Debug)]
pub struct Endpoint {
    /// Where the application answers.
    pub url: url::Url,
    /// The key every call is signed with.
    ///
    /// The key rather than the PEM it was read from, so the private half is
    /// not a `String` sitting in a struct somebody derived `Debug` on.
    pub signing_key: enroute_signature::SigningKey,
    /// How long the application has to answer before the git request fails.
    pub timeout: std::time::Duration,
    /// The hooks to call; each one not here takes its default.
    pub enabled: BTreeSet<Hook>,
    /// What `pre_receive` sees, if it is enabled.
    pub pre_receive_mode: PreReceiveMode,
}

/// Enroute, with its node running in this process.
#[derive(Debug)]
pub struct Enroute {
    node: Arc<Node>,
    reach: Reach,
    /// Whether the `authorize` hook guards git.
    authorize: bool,
}

impl Endpoint {
    /// The hooks this endpoint enables, as signed calls to it.
    ///
    /// # Errors
    ///
    /// Returns an error if the hook client cannot be built.
    pub fn hooks(self) -> Result<HostHooks> {
        let names: Vec<&str> = self.enabled.iter().map(|h| h.name()).collect();
        let client = Arc::new(HookClient::new(
            self.url,
            self.signing_key,
            self.timeout,
            self.enabled,
            self.pre_receive_mode,
        )?);
        tracing::info!(
            keyid = client.verifying_key().keyid(),
            enabled = %names.join(","),
            "signing hooks endpoint calls"
        );
        Ok(client.hooks())
    }
}

impl Enroute {
    /// Start the node that `config` describes, calling `hooks`.
    ///
    /// A sync may push to the remotes that `reach` allows.
    ///
    /// # Errors
    ///
    /// Returns an error if the node does not start.
    pub async fn start(config: node::Config, hooks: HostHooks, reach: Reach) -> Result<Self> {
        let authorize = hooks.authorize.is_some();
        let node = Box::pin(Node::start(config, hooks))
            .await
            .context("starting the node")?;
        Ok(Self {
            node,
            reach,
            authorize,
        })
    }

    /// The node this serves.
    #[must_use]
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    /// Serve everything the node runs until one listener stops or `shutdown`
    /// completes.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract's services cannot be built, if a
    /// compute node lacks a listener, or if a listener stops.
    pub async fn serve(
        self,
        listeners: Listeners,
        shutdown: impl Future<Output = ()> + Send,
    ) -> Result<()> {
        let mut served = tokio::task::JoinSet::new();
        if let Some(git) = self.node.git_router().map(git::front) {
            let listener = listeners
                .git
                .context("a compute node needs a git listener")?;
            self.warn_if_open(listener.local_addr()?);
            spawn_http(&mut served, listener, git, "git")?;
        }
        let routes = self
            .node
            .api_routes(self.reach)
            .context("the contract's services")?;
        if let Some(routes) = routes {
            // Through axum, as the internal listener serves its gRPC.
            let api = routes.into_axum_router();
            let listener = listeners
                .api
                .context("a compute node needs a contract listener")?;
            spawn_http(&mut served, listener, api, "enroute.api.v1alpha1")?;
        }
        let internal = self.node.internal_router();
        spawn_http(&mut served, listeners.internal, internal, "internal")?;
        let first = until_stopped(&mut served, shutdown).await;
        served.shutdown().await;
        self.node.shutdown().await;
        first
    }

    /// Warn when git with no `authorize` hook is reachable off this host.
    fn warn_if_open(&self, git: SocketAddr) {
        if !self.authorize && !git.ip().is_loopback() {
            tracing::warn!(
                %git,
                "the authorize hook is off and git is not on loopback: anyone \
                 who can reach it can read and push"
            );
        }
    }
}

/// Wait until a listener in `served` stops, or `shutdown` completes.
async fn until_stopped(
    served: &mut tokio::task::JoinSet<Result<()>>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<()> {
    // Any one stopping is this process being finished: an Enroute serving
    // only half of what it promises should be replaced, not nursed.
    tokio::select! {
        joined = served.join_next() => match joined {
            Some(joined) => joined.context("a listener panicked")?,
            None => Err(anyhow::anyhow!("the node serves nothing")),
        },
        () = shutdown => {
            tracing::info!("shutting down");
            Ok(())
        }
    }
}

impl Drop for Enroute {
    fn drop(&mut self) {
        // A dropped node must release its log store, so that the next one
        // on the same disk can open it.
        self.node.stop();
    }
}

/// An Enroute served in the background until this is dropped.
///
/// For a process that runs nodes beside other work, such as a test.
#[derive(Debug)]
pub struct Serving {
    task: tokio::task::JoinHandle<()>,
    node: Arc<Node>,
}

impl Serving {
    /// The node being served.
    #[must_use]
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        // Abort, not a graceful stop: that waits for the ledger, and keeps
        // the compute alive while the next node starts on the same disk.
        self.task.abort();
        // The abort completes later; the log store must close now.
        self.node.stop();
    }
}

impl Enroute {
    /// Serve on `listeners` in a task of its own, until the handle drops.
    #[must_use]
    pub fn spawn(self, listeners: Listeners) -> Serving {
        let node = self.node.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = self.serve(listeners, std::future::pending()).await {
                tracing::error!("serving stopped: {error:#}");
            }
        });
        Serving { task, node }
    }
}

/// Serve `router` on `listener` in `served`.
///
/// # Errors
///
/// Returns an error if the listener has no local address.
fn spawn_http(
    served: &mut tokio::task::JoinSet<Result<()>>,
    listener: TcpListener,
    router: axum::Router,
    what: &str,
) -> Result<()> {
    let at = listener.local_addr()?;
    // Responses go out in pieces (acks, side-band frames), which Nagle and
    // delayed ACKs would stall.
    let listener = axum::serve::ListenerExt::tap_io(listener, |tcp| {
        if let Err(error) = tcp.set_nodelay(true) {
            tracing::debug!("TCP_NODELAY: {error}");
        }
    });
    tracing::info!("serving {what} on {at}");
    served.spawn(async move { Ok(axum::serve(listener, router).await?) });
    Ok(())
}
