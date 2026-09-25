//! Enroute itself: the service that stores repositories and answers the
//! `enroute.api.v1alpha1` contract.
//!
//! It knows a repository by an opaque id and nothing else — not its name,
//! not who owns it, not who was allowed to ask. Every one of those questions
//! belongs to whatever application sits in front of this, which reaches it
//! the same way any other caller would.

pub mod git;
pub mod grpc;
pub mod hooks;
pub mod maintenance;
mod repo_key;
pub mod telemetry;
mod wire;

use std::net::SocketAddr;
use std::sync::Arc;

pub use enroute_config::ObjectUri;
pub use enroute_git_retrieve::Storage;
pub use repo_key::{BadRepoKey, MAX_REPO_KEY_BYTES, RepoKey};

/// What each of the two listeners is, since a deployment that serves one of
/// them and not the other is not one this repository describes.
#[derive(Debug, Clone)]
pub struct Listeners {
    /// Where the contract is served, for an application to call.
    pub api: SocketAddr,
    /// Where git is served, for a git client.
    pub git: SocketAddr,
}

/// The application this deployment asks, and what it is asked with.
///
/// One struct because the three travel together everywhere: the URL is what a
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
}

/// Everything Enroute serves, on both of its listeners, until one of them
/// stops.
///
/// One function because a prior deployment once served only half of this: a
/// second entry point answered an application and served no git at all.
///
/// # Errors
///
/// Returns an error if either address is already bound, or if either listener
/// stops.
pub async fn serve(
    state: Storage,
    worker: Arc<dyn enroute_git_ingest::IngestWorker>,
    endpoint: Endpoint,
    reach: enroute_git_remote::Reach,
    listeners: Listeners,
) -> anyhow::Result<()> {
    let Listeners {
        api: addr,
        git: git_addr,
    } = listeners;
    // Every git request asks the application before it is served, and every
    // push asks again before a ref moves. A key is required rather than
    // defaulted: an Enroute that signed with none could not be told from
    // anyone else calling an application.
    let hooks = Arc::new(hooks::Hooks::new(
        state.clone(),
        endpoint.url,
        endpoint.signing_key,
        endpoint.timeout,
    )?);
    tracing::info!(
        keyid = hooks.verifying_key().keyid(),
        "signing hooks endpoint calls"
    );

    // One `Hooks` answers every question a *git client* raises: who may reach a
    // repository, which refs it may be told about, and which of a push's refs
    // may land. The contract door asks none of them, the application being the
    // caller there.
    let git_routes = git::router(
        state.clone(),
        worker.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks,
    );
    let remotes = enroute_git_remote::Client::new(reach)
        .map_err(|error| anyhow::anyhow!("the client a sync pushes with: {error}"))?;
    let services = grpc::services(state, remotes);

    tracing::info!("serving enroute.api.v1alpha1 on {addr}, git on {git_addr}");
    let grpc = grpc::router(services)?.serve(addr);

    let listener = tokio::net::TcpListener::bind(git_addr).await?;
    let http = axum::serve(listener, git_routes);

    // Either one stopping is this process being finished, so neither is
    // restarted around the other: an Enroute serving only half of what it
    // promises should be replaced, not nursed.
    tokio::select! {
        served = grpc => served?,
        served = http => served?,
    }

    Ok(())
}
