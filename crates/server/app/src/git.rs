//! Enroute's git front door: smart HTTP, terminated here.
//!
//! The routes and the protocol are `enroute-git-http`'s. What this adds is the
//! two things the engine has no way to decide — which tenant a request belongs
//! to, and which repository within it — by handing it an [`Authorizer`] that
//! resolves the first and asks that tenant's application about the second.
//!
//! [`Authorizer`]: enroute_git_http::Authorizer

use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use enroute_git_http::{Authorizer, RefVisibility};
use enroute_git_ingest::IngestWorker;
use enroute_git_retrieve::Storage;

/// The git routes, taking a repository path of any depth.
///
/// The path means nothing here. It is relayed to the authorizer, which is
/// the only party that knows what a repository is called.
pub fn router(
    state: Storage,
    worker: Arc<dyn IngestWorker>,
    authorizer: Arc<dyn Authorizer>,
    hooks: Arc<dyn enroute_git_ingest::ReceiveHooks>,
    visibility: Arc<dyn RefVisibility>,
) -> Router {
    Router::new()
        // On this port rather than the contract's: a health check has no
        // credential, and the contract refuses everyone who has none.
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        // A fallback, because a repository path claims the whole URL. The
        // route above matches only itself, so a repository may still be
        // called `healthz` — git reaches it at `/healthz/info/refs`.
        .fallback_service(enroute_git_http::router(
            state, worker, authorizer, hooks, visibility,
        ))
        .layer(axum::middleware::from_fn(name_the_host))
}

/// Makes sure a request carries a `Host` header, whichever protocol it
/// arrived on.
///
/// HTTP/2 sends none — the name travels as `:authority`, which hyper puts
/// in the URI instead.
async fn name_the_host(mut request: Request, next: Next) -> Response {
    if !request.headers().contains_key(http::header::HOST)
        && let Some(authority) = request.uri().authority().cloned()
        && let Ok(value) = authority.as_str().parse()
    {
        request.headers_mut().insert(http::header::HOST, value);
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{HeaderMap, StatusCode, header};
    use axum::routing::get;
    use tower::ServiceExt as _;

    use super::*;

    /// Echo whatever `Host` the handler ended up seeing, so a test can tell
    /// what the middleware left behind rather than what it was sent.
    fn echo() -> Router {
        Router::new()
            .route(
                "/",
                get(|headers: HeaderMap| async move {
                    headers
                        .get(header::HOST)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("<none>")
                        .to_string()
                }),
            )
            .layer(axum::middleware::from_fn(name_the_host))
    }

    async fn host_seen(request: Request) -> String {
        let response = echo().oneshot(request).await.expect("a response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .expect("a body");
        String::from_utf8(body.to_vec()).expect("utf-8")
    }

    /// What HTTP/2 delivers: the name in the URI, no header.
    #[tokio::test]
    async fn an_authority_in_the_uri_becomes_a_host_header() {
        let request = Request::builder()
            .uri("https://acme.enroute.sh/")
            .body(Body::empty())
            .expect("a request");
        assert_eq!(host_seen(request).await, "acme.enroute.sh");
    }

    /// An existing header is the truth — a proxy's upstream address must
    /// not replace the name the client actually asked for.
    #[tokio::test]
    async fn an_existing_host_header_is_left_alone() {
        let request = Request::builder()
            .uri("https://internal.example/")
            .header(header::HOST, "acme.enroute.sh")
            .body(Body::empty())
            .expect("a request");
        assert_eq!(host_seen(request).await, "acme.enroute.sh");
    }

    #[tokio::test]
    async fn a_request_naming_no_host_at_all_gains_none() {
        let request = Request::builder()
            .uri("/")
            .body(Body::empty())
            .expect("a request");
        assert_eq!(host_seen(request).await, "<none>");
    }
}
