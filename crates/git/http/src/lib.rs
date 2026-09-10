//! The smart-HTTP transport binding for `git`: axum routes, header/status
//! handling, and streaming request/response bodies.
//!
//! Wire-protocol logic lives in [`enroute_git_proto`]; this crate only
//! adapts it to HTTP.

mod auth;
mod error;
mod handlers;
mod path;

pub use auth::{AuthError, Authorized, Authorizer, Challenge, GitRequest};
// The other hook an application answers, and re-exported for the same reason
// the routes take it: whoever mounts them implements it, and one import says
// so. `Access` answers both hooks, which ask the same thing.
pub use enroute_git_proto::{Access, AllRefsVisible, RefVisibility};

use std::sync::Arc;

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::middleware::from_fn;
use axum::routing::{get, post};
use axum::{Extension, Router};
use enroute_git_ingest::{IngestWorker, ReceiveHooks};
use enroute_git_retrieve::Storage;
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::map_request_body::MapRequestBodyLayer;

/// The smart HTTP git routes, serving a repository path of any depth.
///
/// Mount it as a fallback and not under a prefix: the repository is the
/// front of the path, and [`path`] is what tells it from the service.
///
/// # Hooks
///
/// Every hook is an argument rather than an extension the caller is trusted
/// to layer on, so omitting one fails to compile.
pub fn router(
    state: Storage,
    worker: Arc<dyn IngestWorker>,
    authorizer: Arc<dyn Authorizer>,
    hooks: Arc<dyn ReceiveHooks>,
    visibility: Arc<dyn RefVisibility>,
) -> Router {
    // Only a fetch's rpc body arrives gzipped; a push streams raw. Scoping
    // decompression to the one route that can receive it keeps every
    // inflated body under a limit, which the streamed push has no way to be.
    let upload_pack = Router::new()
        .route("/git-upload-pack", post(handlers::upload_pack))
        .layer(MapRequestBodyLayer::new(Body::new))
        .layer(RequestDecompressionLayer::new())
        .layer(DefaultBodyLimit::max(UPLOAD_PACK_BODY_LIMIT));

    let services = Router::new()
        .route("/info/refs", get(handlers::info_refs))
        .route("/git-receive-pack", post(handlers::receive_pack))
        .merge(upload_pack)
        .layer(Extension(worker))
        .layer(Extension(authorizer))
        .layer(Extension(hooks))
        .layer(Extension(visibility))
        .with_state(state);

    // `Router::layer` wraps each route and the fallback, so it runs after a
    // path is matched. The cut has to happen before that, hence the wrapper.
    Router::new()
        .fallback_service(services)
        .layer(from_fn(path::resolve))
}

/// Ceiling on a decompressed `git-upload-pack` body, past axum's 2 MiB
/// default — a fetch against many refs runs to megabytes.
const UPLOAD_PACK_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// Who this crate's tests push as — an arbitrary name, since the point is
/// that a single [`Authorizer`] decides it, not the routes.
#[cfg(test)]
pub(crate) const TEST_USER: &str = "alice";

/// Collect a response body — here, not in `enroute-git-test-support`, since
/// that crate deliberately names no transport.
#[cfg(test)]
pub(crate) async fn body_bytes(body: Body) -> bytes::Bytes {
    use http_body_util::BodyExt as _;
    body.collect().await.unwrap().to_bytes()
}

/// An id no repository has, for the tests that ask what happens when an
/// authorizer names one that is not there.
#[cfg(test)]
pub(crate) fn missing_repo() -> enroute_git_core::RepoId {
    enroute_git_core::RepoId::new(i64::MAX)
}

/// Mount [`router`] alone (no healthz) behind an authorizer that admits
/// every caller and answers with `repo`, for this crate's own tests.
///
/// The repository is passed in, not looked up from the path — mapping a
/// path to an id is what the authorizer is for, tested elsewhere.
#[cfg(test)]
pub(crate) fn test_app(state: Storage, repo: enroute_git_core::RepoId) -> Router {
    test_app_with(state, repo, Arc::new(AllRefsVisible))
}

/// [`test_app`], behind an application that hides `hidden` from every
/// advertisement.
#[cfg(test)]
pub(crate) fn test_app_hiding(
    state: Storage,
    repo: enroute_git_core::RepoId,
    hidden: &'static str,
) -> Router {
    test_app_with(state, repo, Arc::new(HidesOne(hidden)))
}

#[cfg(test)]
fn test_app_with(
    state: Storage,
    repo: enroute_git_core::RepoId,
    visibility: Arc<dyn RefVisibility>,
) -> Router {
    // Staging into memory of its own: these tests exercise the routes, not
    // where a push's staged bytes land.
    let worker = enroute_git_ingest::LocalIngestWorker::shared(
        state.clone(),
        Arc::new(object_store::memory::InMemory::new()),
    );
    router(
        state,
        worker,
        Arc::new(TestAuthorizer(repo)),
        Arc::new(enroute_git_ingest::NoHooks),
        visibility,
    )
}

/// A repository with two branches on one commit, so a test can hide one and
/// still have something left to advertise.
#[cfg(test)]
pub(crate) async fn two_branches(state: &Storage) -> enroute_git_retrieve::RepoMetadata {
    use enroute_git_retrieve::RefUpdate;

    let repo = enroute_git_test_support::create_repo(state).await;
    let commit = enroute_git_test_support::seed_commit(state, &repo).await;
    let new_id = commit.parse().expect("a commit id");
    let updates = [RefUpdate {
        refname: "refs/heads/hidden".into(),
        old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
        new_id,
    }];
    // `seed_commit` already left `refs/heads/main` here, so only the second
    // branch is this helper's to add.
    let applied = state
        .rows
        .repo(repo.id)
        .update_refs(&updates)
        .await
        .expect("seeding a second branch");
    assert!(
        applied.iter().all(|applied| applied.result.is_ok()),
        "the second branch was rejected: {applied:?}"
    );
    repo
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct HidesOne(&'static str);

#[cfg(test)]
#[async_trait::async_trait]
impl RefVisibility for HidesOne {
    async fn visible_refs(
        &self,
        _repo: enroute_git_core::RepoId,
        _actor: &enroute_git_ingest::Actor,
        _access: Access,
        refs: &[&str],
    ) -> Result<Vec<String>, enroute_git_core::Error> {
        Ok(refs
            .iter()
            .filter(|name| **name != self.0)
            .map(|name| (*name).to_string())
            .collect())
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestAuthorizer(pub(crate) enroute_git_core::RepoId);

#[cfg(test)]
#[async_trait::async_trait]
impl Authorizer for TestAuthorizer {
    async fn authorize(&self, _request: &GitRequest<'_>) -> Result<Authorized, AuthError> {
        Ok(Authorized {
            repo: self.0,
            actor: enroute_git_ingest::Actor::new(TEST_USER),
        })
    }
}
