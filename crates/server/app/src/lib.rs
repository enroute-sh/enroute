//! Enroute itself: the service that stores repositories and answers the
//! `enroute.api.v1alpha1` contract.
//!
//! It knows a repository by an opaque id and nothing else — not its name,
//! not who owns it, not who was allowed to ask. Every one of those questions
//! belongs to whatever application sits in front of this, which reaches it
//! the same way any other caller would.

pub mod callers;
pub mod git;
pub mod grpc;
pub mod hooks;
pub mod maintenance;
pub mod telemetry;
pub mod tenancy;
mod wire;

use std::net::SocketAddr;
use std::sync::Arc;

pub use enroute_config::ObjectUri;
pub use enroute_git_retrieve::Storage;

/// What each of the two listeners is, since a deployment that serves one of
/// them and not the other is not one this repository describes.
#[derive(Debug, Clone)]
pub struct Listeners {
    /// Where the contract is served, for an application to call.
    pub api: SocketAddr,
    /// The header naming which tenant a contract call is for.
    ///
    /// Beside the listener it describes, so the next thing that listener needs
    /// is a field here rather than another argument to [`serve`].
    pub tenant_header: http::HeaderName,
    /// Where git is served, for a git client.
    pub git: SocketAddr,
}

/// Everything Enroute serves, on both of its listeners, until one of them
/// stops.
///
/// One function because a prior deployment once served only half of this: a
/// second entry point answered an application and served no git at all.
///
/// # Errors
///
/// Returns an error if `signing_key_pem` is not a key, if either address is
/// already bound, or if either listener stops.
pub async fn serve(
    state: Storage,
    worker: Arc<dyn enroute_git_ingest::IngestWorker>,
    tenants: Arc<tenancy::Tenants>,
    signing_key_pem: &str,
    hook_timeout: std::time::Duration,
    reach: enroute_git_remote::Reach,
    listeners: Listeners,
) -> anyhow::Result<()> {
    let Listeners {
        api: addr,
        tenant_header,
        git: git_addr,
    } = listeners;
    // Every git request asks an application before it is served, and every push
    // asks again before a ref moves — which application is the tenant's to say.
    // A key is required rather than defaulted: a Enroute that signed with none
    // could not be told from anyone else calling an application.
    let hooks = Arc::new(hooks::Hooks::new(
        Arc::clone(&tenants),
        enroute_signature::SigningKey::from_pem(signing_key_pem)
            .map_err(|error| anyhow::anyhow!("the hooks signing key: {error}"))?,
        hook_timeout,
    )?);
    tracing::info!(
        keyid = hooks.verifying_key().keyid(),
        "signing hooks endpoint calls"
    );

    // One `Hooks` answers every question a *git client* raises: who may reach a
    // repository, which refs it may be told about, and which of a push's refs
    // may land. The contract door asks none of them, because the application is
    // the caller there.
    let git_routes = git::router(
        state.clone(),
        worker.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks,
    );
    let remotes = enroute_git_remote::Client::new(reach)
        .map_err(|error| anyhow::anyhow!("the client a sync pushes with: {error}"))?;
    let services = grpc::services(state, tenants, remotes, tenant_header);

    tracing::info!("serving enroute.api.v1alpha1 on {addr}, git on {git_addr}");
    let grpc = grpc::router(services)?.serve(addr);

    let listener = tokio::net::TcpListener::bind(git_addr).await?;
    let http = axum::serve(listener, git_routes);

    // Either one stopping is this process being finished, so neither is
    // restarted around the other: a Enroute serving only half of what it
    // promises should be replaced, not nursed.
    tokio::select! {
        served = grpc => served?,
        served = http => served?,
    }

    Ok(())
}
