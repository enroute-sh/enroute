use std::sync::Arc;

use axum::{
    extract::{Extension, Query, State},
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use enroute_git_proto::{Access, RefVisibility, advertised_refs};
use enroute_git_retrieve::Storage;

use crate::error::HttpError;
use crate::path::RepoPath;
use crate::{Authorizer, GitRequest};

use super::speaks_v2;

#[derive(Deserialize)]
pub(crate) struct InfoRefsQuery {
    service: String,
}

pub(crate) async fn info_refs(
    Extension(authorizer): Extension<Arc<dyn Authorizer>>,
    Extension(visibility): Extension<Arc<dyn RefVisibility>>,
    Extension(RepoPath(repo)): Extension<RepoPath>,
    Query(q): Query<InfoRefsQuery>,
    State(state): State<Storage>,
    headers: HeaderMap,
) -> Result<Response, HttpError> {
    // The advertisement a push begins with is itself a write: refusing it
    // here is what stops a reader from learning a repo's refs are writable.
    let access = if q.service == "git-receive-pack" {
        Access::Write
    } else {
        Access::Read
    };
    let cleared = crate::auth::authorize(
        &*authorizer,
        &state,
        GitRequest {
            repo: &repo,
            headers: &headers,
            access,
        },
    )
    .await?;
    let repo = &cleared.repo;

    match q.service.as_str() {
        "git-upload-pack" => {
            // A client that asked for v2 gets the command list; one that did
            // not gets the refs themselves. Refusing the second would make
            // this server unreachable to every client that sends no
            // `Git-Protocol` header, which go-git does not.
            let body = if speaks_v2(&headers) {
                enroute_git_proto::upload_pack_capabilities()?
            } else {
                let refs = advertised(&state, repo, &cleared, &*visibility, Access::Read).await?;
                enroute_git_proto::upload_pack_v0_advertisement(&refs, &repo.default_branch)?
            };
            Ok((
                [
                    (
                        header::CONTENT_TYPE,
                        "application/x-git-upload-pack-advertisement",
                    ),
                    (header::CACHE_CONTROL, "no-cache"),
                ],
                body,
            )
                .into_response())
        }
        "git-receive-pack" => {
            let refs = advertised(&state, repo, &cleared, &*visibility, Access::Write).await?;
            let body = enroute_git_proto::receive_pack_advertisement(&refs)?;
            Ok((
                [
                    (
                        header::CONTENT_TYPE,
                        "application/x-git-receive-pack-advertisement",
                    ),
                    (header::CACHE_CONTROL, "no-cache"),
                ],
                body,
            )
                .into_response())
        }
        _ => Err(enroute_git_proto::Error::BadRequest("unknown service".into()).into()),
    }
}

/// [`advertised_refs`] against an authorized request, in this crate's error
/// type.
async fn advertised(
    state: &Storage,
    repo: &enroute_git_retrieve::RepoMetadata,
    cleared: &crate::auth::Resolved,
    visibility: &dyn RefVisibility,
    access: Access,
) -> Result<enroute_git_retrieve::RefsMap, HttpError> {
    advertised_refs(state, repo, visibility, &cleared.actor, access)
        .await
        .map_err(|error| enroute_git_proto::Error::from(error).into())
}

#[cfg(test)]
mod tests {
    use crate::body_bytes;
    use enroute_git_test_support::{debug_pktlines, make_state};

    use axum::{body::Body, http::Request, http::StatusCode, http::header};
    use tower::ServiceExt as _;

    /// A client sending no `Git-Protocol` header gets the v0 advertisement,
    /// not a refusal — go-git never sends one.
    ///
    /// Which branch ran, not what it wrote: the bytes are pinned where they
    /// are built, in `enroute_git_proto::capabilities`.
    #[tokio::test]
    async fn info_refs_without_the_v2_header_advertises_v0() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .uri("/hello.git/info/refs?service=git-upload-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = debug_pktlines(&body_bytes(resp.into_body()).await);
        assert!(body.contains("capabilities^{}"), "{body}");
    }

    /// The `upload-pack` body is v2-only, though — v0's negotiation is a
    /// different protocol from v2's.
    #[tokio::test]
    async fn upload_pack_body_still_requires_v2() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hello.git/git-upload-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn info_refs_missing_repo_returns_404_once_authenticated() {
        let state = make_state();
        let resp = crate::test_app(state, crate::missing_repo())
            .oneshot(
                Request::builder()
                    .uri("/hello.git/info/refs?service=git-upload-pack")
                    .header("Git-Protocol", "version=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// Git reads the advertisement off the content type, so that and the
    /// status are what this layer answers for.
    #[tokio::test]
    async fn info_refs_upload_pack_returns_capabilities() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .uri("/hello.git/info/refs?service=git-upload-pack")
                    .header("Git-Protocol", "version=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/x-git-upload-pack-advertisement")
        );
    }

    #[tokio::test]
    async fn info_refs_receive_pack_returns_capabilities() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .uri("/hello.git/info/refs?service=git-receive-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/x-git-receive-pack-advertisement")
        );
    }

    /// The advertisement a client with no `Git-Protocol` header gets is
    /// filtered too — hiding a ref from v2 alone would publish it to go-git.
    #[tokio::test]
    async fn a_hidden_ref_is_left_out_of_the_v0_advertisement() {
        let state = make_state();
        let repo = crate::two_branches(&state).await;
        let resp = crate::test_app_hiding(state, repo.id, "refs/heads/hidden")
            .oneshot(
                Request::builder()
                    .uri("/hello.git/info/refs?service=git-upload-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = debug_pktlines(&body_bytes(resp.into_body()).await);
        assert!(body.contains("refs/heads/main"), "{body}");
        assert!(!body.contains("refs/heads/hidden"), "{body}");
    }

    /// A pusher is told about the same refs a fetcher is, and no more:
    /// advertising a hidden ref here would leak it just as well.
    #[tokio::test]
    async fn a_hidden_ref_is_left_out_of_the_push_advertisement() {
        let state = make_state();
        let repo = crate::two_branches(&state).await;
        let resp = crate::test_app_hiding(state, repo.id, "refs/heads/hidden")
            .oneshot(
                Request::builder()
                    .uri("/hello.git/info/refs?service=git-receive-pack")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = debug_pktlines(&body_bytes(resp.into_body()).await);
        assert!(body.contains("refs/heads/main"), "{body}");
        assert!(!body.contains("refs/heads/hidden"), "{body}");
    }

    #[tokio::test]
    async fn info_refs_unknown_service_returns_400() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .uri("/hello.git/info/refs?service=git-nonsense")
                    .header("Git-Protocol", "version=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
