use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Extension, State},
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
};
use futures::TryStreamExt as _;
use tokio_util::io::StreamReader;

use enroute_git_ingest::{IngestWorker, ReceiveHooks};
use enroute_git_proto::ReceivePackResponse;
use enroute_git_retrieve::Storage;

use crate::auth::Resolved;
use crate::error::HttpError;
use crate::path::RepoPath;
use crate::{Access, Authorizer, GitRequest};

pub(crate) async fn receive_pack(
    Extension(authorizer): Extension<Arc<dyn Authorizer>>,
    // Chosen once per deployment by the binary, not here: this crate can't name
    // a deployment-specific worker without depending on one.
    Extension(worker): Extension<Arc<dyn IngestWorker>>,
    // Where the push's `pre-receive` runs. Beside the authorizer rather than
    // inside the worker: both are questions for whoever owns the answers, and
    // a worker may be in a process that cannot ask.
    Extension(hooks): Extension<Arc<dyn ReceiveHooks>>,
    Extension(RepoPath(repo)): Extension<RepoPath>,
    State(state): State<Storage>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, HttpError> {
    let Resolved { repo, actor } = crate::auth::authorize(
        &*authorizer,
        &state,
        GitRequest {
            repo: &repo,
            headers: &headers,
            access: Access::Write,
        },
    )
    .await?;
    let stream = body.into_data_stream().map_err(std::io::Error::other);
    let reader = StreamReader::new(stream);

    let result = enroute_git_proto::receive_pack(state, repo, actor, worker, hooks, reader).await?;

    let headers = [
        (
            header::CONTENT_TYPE,
            "application/x-git-receive-pack-result",
        ),
        (header::CACHE_CONTROL, "no-cache"),
    ];
    Ok(match result {
        ReceivePackResponse::Body(bytes) => (headers, bytes).into_response(),
        ReceivePackResponse::Streamed(rx) => (headers, Body::from_stream(rx)).into_response(),
    })
}

#[cfg(test)]
mod tests {
    use crate::body_bytes;
    use enroute_git_test_support::{debug_pktlines, make_state};

    use axum::{body::Body, http::Request, http::header};
    use tower::ServiceExt as _;

    fn make_pack() -> Vec<u8> {
        let mut pack = Vec::new();
        pack.extend_from_slice(b"PACK");
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&0u32.to_be_bytes());
        let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
        h.update(&pack);
        pack.extend_from_slice(h.try_finalize().unwrap().as_slice());
        pack
    }

    fn receive_pack_body(updates: &[(&str, &str, &str)], pack: &[u8]) -> Vec<u8> {
        use gix_packetline::blocking_io::encode as pkt;
        let mut body = Vec::new();
        for (i, (old_id, new_id, refname)) in updates.iter().enumerate() {
            let line = if i == 0 {
                format!("{old_id} {new_id} {refname}\0report-status\n")
            } else {
                format!("{old_id} {new_id} {refname}\n")
            };
            pkt::data_to_write(line.as_bytes(), &mut body).unwrap();
        }
        pkt::flush_to_write(&mut body).unwrap();
        body.extend_from_slice(pack);
        body
    }

    #[tokio::test]
    async fn receive_pack_routes_and_reports_status() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let zeros = "0000000000000000000000000000000000000000";
        let missing = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        let resp = crate::test_app(state, repo.id)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hello.git/git-receive-pack")
                    .header(
                        header::CONTENT_TYPE,
                        "application/x-git-receive-pack-request",
                    )
                    .body(Body::from(receive_pack_body(
                        &[(zeros, missing, "refs/heads/main")],
                        &make_pack(),
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), axum::http::StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/x-git-receive-pack-result")
        );
        let body = debug_pktlines(&body_bytes(resp.into_body()).await);
        insta::assert_snapshot!(body, @"
        unpack ok
        ng refs/heads/main missing-objects
        [flush]
        ");
    }

    #[tokio::test]
    async fn receive_pack_missing_repo_returns_404() {
        let zeros = "0000000000000000000000000000000000000000";
        let missing = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

        // Authenticated: permission is checked before repo existence, so an
        // anonymous caller would get 401 here rather than 404.
        let state = make_state();
        let resp = crate::test_app(state, crate::missing_repo())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hello.git/git-receive-pack")
                    .header(
                        header::CONTENT_TYPE,
                        "application/x-git-receive-pack-request",
                    )
                    .body(Body::from(receive_pack_body(
                        &[(zeros, missing, "refs/heads/main")],
                        &make_pack(),
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), axum::http::StatusCode::NOT_FOUND);
    }
}
