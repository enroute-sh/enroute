use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Extension, State},
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use enroute_git_proto::{RefVisibility, UploadPackResponse};
use enroute_git_retrieve::Storage;

use crate::error::HttpError;
use crate::path::RepoPath;
use crate::{Access, Authorizer, GitRequest};

use super::require_v2;

pub(crate) async fn upload_pack(
    Extension(authorizer): Extension<Arc<dyn Authorizer>>,
    Extension(visibility): Extension<Arc<dyn RefVisibility>>,
    Extension(RepoPath(repo)): Extension<RepoPath>,
    State(state): State<Storage>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, HttpError> {
    require_v2(&headers)?;

    let cleared = crate::auth::authorize(
        &*authorizer,
        &state,
        GitRequest {
            repo: &repo,
            headers: &headers,
            access: Access::Read,
        },
    )
    .await?;
    let result =
        enroute_git_proto::upload_pack(state, cleared.repo, &body, &*visibility, &cleared.actor)
            .await?;

    let headers = [
        (header::CONTENT_TYPE, "application/x-git-upload-pack-result"),
        (header::CACHE_CONTROL, "no-cache"),
    ];
    Ok(match result {
        UploadPackResponse::Body(bytes) => (headers, bytes).into_response(),
        UploadPackResponse::Pack(rx) => (headers, Body::from_stream(rx)).into_response(),
    })
}

#[cfg(test)]
mod tests {
    use crate::body_bytes;
    use enroute_git_test_support::{debug_pktlines, make_state};

    use axum::{body::Body, http::Request, http::header};
    use tower::ServiceExt as _;

    #[tokio::test]
    async fn upload_pack_routes_and_sets_headers() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let mut body = Vec::new();
        gix_packetline::blocking_io::encode::data_to_write(b"command=ls-refs\n", &mut body)
            .unwrap();
        gix_packetline::blocking_io::encode::delim_to_write(&mut body).unwrap();
        gix_packetline::blocking_io::encode::flush_to_write(&mut body).unwrap();

        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hello.git/git-upload-pack")
                    .header("Git-Protocol", "version=2")
                    .header(
                        header::CONTENT_TYPE,
                        "application/x-git-upload-pack-request",
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/x-git-upload-pack-result")
        );
        assert_eq!(
            resp.headers()
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-cache")
        );
        let body = debug_pktlines(&body_bytes(resp.into_body()).await);
        insta::assert_snapshot!(body, @"[flush]");
    }

    /// git gzips a buffered rpc body once it is big enough to be worth it, so
    /// a fetch whose negotiation grows past that threshold arrives compressed.
    #[tokio::test]
    async fn upload_pack_accepts_a_gzipped_body() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let mut body = Vec::new();
        gix_packetline::blocking_io::encode::data_to_write(b"command=ls-refs\n", &mut body)
            .unwrap();
        gix_packetline::blocking_io::encode::delim_to_write(&mut body).unwrap();
        gix_packetline::blocking_io::encode::flush_to_write(&mut body).unwrap();

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, &body).unwrap();
        let gzipped = encoder.finish().unwrap();

        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hello.git/git-upload-pack")
                    .header("Git-Protocol", "version=2")
                    .header(
                        header::CONTENT_TYPE,
                        "application/x-git-upload-pack-request",
                    )
                    .header(header::CONTENT_ENCODING, "gzip")
                    .body(Body::from(gzipped))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let body = debug_pktlines(&body_bytes(resp.into_body()).await);
        insta::assert_snapshot!(body, @"[flush]");
    }

    /// A repo with enough refs pushes the `want` list past axum's default
    /// buffered-body limit, which applies to the decompressed bytes.
    #[tokio::test]
    async fn upload_pack_accepts_a_negotiation_larger_than_the_default_limit() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let mut body = Vec::new();
        gix_packetline::blocking_io::encode::data_to_write(b"command=ls-refs\n", &mut body)
            .unwrap();
        gix_packetline::blocking_io::encode::delim_to_write(&mut body).unwrap();
        // Past 2 MiB once inflated, while staying small enough on the wire that
        // the limit is what's under test rather than the compressed size.
        for i in 0..60_000 {
            let prefix = format!("ref-prefix refs/heads/branch-{i:012}\n");
            gix_packetline::blocking_io::encode::data_to_write(prefix.as_bytes(), &mut body)
                .unwrap();
        }
        gix_packetline::blocking_io::encode::flush_to_write(&mut body).unwrap();
        assert!(body.len() > 2 * 1024 * 1024);

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, &body).unwrap();
        let gzipped = encoder.finish().unwrap();

        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hello.git/git-upload-pack")
                    .header("Git-Protocol", "version=2")
                    .header(
                        header::CONTENT_TYPE,
                        "application/x-git-upload-pack-request",
                    )
                    .header(header::CONTENT_ENCODING, "gzip")
                    .body(Body::from(gzipped))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
    }

    /// The call this whole hook exists for: what `ls-refs` lists is what the
    /// application said it may list.
    #[tokio::test]
    async fn ls_refs_lists_only_what_the_application_admits_to() {
        let state = make_state();
        let repo = crate::two_branches(&state).await;

        let mut body = Vec::new();
        gix_packetline::blocking_io::encode::data_to_write(b"command=ls-refs\n", &mut body)
            .unwrap();
        gix_packetline::blocking_io::encode::delim_to_write(&mut body).unwrap();
        gix_packetline::blocking_io::encode::flush_to_write(&mut body).unwrap();

        let resp = crate::test_app_hiding(state, repo.id, "refs/heads/hidden")
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hello.git/git-upload-pack")
                    .header("Git-Protocol", "version=2")
                    .header(
                        header::CONTENT_TYPE,
                        "application/x-git-upload-pack-request",
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        let body = debug_pktlines(&body_bytes(resp.into_body()).await);
        assert!(body.contains("refs/heads/main"), "{body}");
        assert!(!body.contains("refs/heads/hidden"), "{body}");
    }

    #[tokio::test]
    async fn upload_pack_requires_protocol_v2() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let mut body = Vec::new();
        gix_packetline::blocking_io::encode::data_to_write(b"command=ls-refs\n", &mut body)
            .unwrap();
        gix_packetline::blocking_io::encode::delim_to_write(&mut body).unwrap();
        gix_packetline::blocking_io::encode::flush_to_write(&mut body).unwrap();

        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hello.git/git-upload-pack")
                    .header(
                        header::CONTENT_TYPE,
                        "application/x-git-upload-pack-request",
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::BAD_REQUEST);
    }
}
