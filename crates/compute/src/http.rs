//! Smart HTTP frontend.

use std::io::Read;
use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use tracing::Instrument as _;

use crate::access::{Access, Denial, GitRequest, Grant, Hidden, hidden_refs};
use crate::compute::Compute;
use crate::contract::repo_key::RepoKey;
use crate::git::oid::Oid;
use crate::hooks::PushContext;
use crate::protocol::{self, Refusal, receive_pack, upload_pack, upload_pack_v0};
use crate::query::RefView;
use crate::repo::serve::FetchArgs;
use crate::repo::view::Snapshot;
use crate::util::micros;

const MAX_UPLOAD_PACK_REQUEST: usize = 256 << 20;

/// Git's smart HTTP (the git port).
pub fn git_router(node: Arc<Compute>) -> Router {
    Router::new().fallback(dispatch).with_state(node)
}

fn error(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain")], msg.into()).into_response()
}

#[expect(
    clippy::unwrap_used,
    reason = "the headers are fixed constants or content types from this module"
)]
fn git_response(content_type: &str, body: Body) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "no-cache")
        .body(body)
        .unwrap()
}

/// The services a git path can end in, and their names.
const SERVICES: [(&str, &str); 3] = [
    ("/info/refs", "info/refs"),
    ("/git-upload-pack", "git-upload-pack"),
    ("/git-receive-pack", "git-receive-pack"),
];

/// The longest repository path that a host is asked about.
const MAX_HOST_PATH: usize = 1024;

/// A git path: the repository path, percent-decoded and with any `.git`,
/// and the service. `None` (a 404) for a path that names no repository: no
/// service at its end, or a repository path that is empty, too long, or has
/// an empty, `.` or `..` segment, an encoded `/`, bytes that are not UTF-8
/// or a control character.
fn route(path: &str) -> Option<(String, &'static str)> {
    let (prefix, service) = SERVICES
        .iter()
        .find_map(|(suffix, service)| Some((path.strip_suffix(suffix)?, *service)))?;
    let prefix = prefix.strip_prefix('/')?;
    if prefix.is_empty() || prefix.len() > MAX_HOST_PATH {
        return None;
    }
    let mut decoded = Vec::new();
    for raw in prefix.split('/') {
        let segment = percent_encoding::percent_decode_str(raw)
            .decode_utf8()
            .ok()?;
        let bad = segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains('/')
            || segment.chars().any(char::is_control);
        if bad {
            return None;
        }
        decoded.push(segment);
    }
    Some((decoded.join("/"), service))
}

/// [`handle`] in a span for the request, which joins a trace that the
/// caller (a proxy, say) sends. A fetch's pack task holds the span until the
/// pack is sent.
async fn dispatch(
    State(node): State<Arc<Compute>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let span = tracing::info_span!(
        "enroute::git::request",
        http.request.method = %method,
        repo = tracing::field::Empty,
        git.service = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
    );
    enroute_shared::trace_context::adopt(&span, &headers);
    let res = handle(node, method, uri, headers, body)
        .instrument(span.clone())
        .await;
    span.record("http.response.status_code", res.status().as_u16());
    res
}

async fn handle(
    node: Arc<Compute>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let Some((spelled, service)) = route(uri.path()) else {
        return error(StatusCode::NOT_FOUND, "not found");
    };
    let spelled = spelled.as_str();
    let path = spelled.strip_suffix(".git").unwrap_or(spelled);
    // A host names the repository, so only without one is the path a key.
    if node.hooks.authorize.is_none() && !RepoKey::is_valid(path) {
        return error(StatusCode::BAD_REQUEST, "invalid repository name");
    }
    let query = uri.query().unwrap_or("");
    let access = match (service, method) {
        // The push advertisement is a write: else a reader learns which refs
        // it could push to.
        ("info/refs", Method::GET) if service_param(query) == "git-receive-pack" => Access::Write,
        ("info/refs", Method::GET) | ("git-upload-pack", Method::POST) => Access::Read,
        ("git-receive-pack", Method::POST) => Access::Write,
        ("info/refs" | "git-upload-pack" | "git-receive-pack", _) => {
            return error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
        }
        _ => return error(StatusCode::NOT_FOUND, "not found"),
    };
    tracing::Span::current()
        .record("repo", path)
        .record("git.service", service);
    let grant = match authorize(&node, spelled, path, &headers, access).await {
        Ok(grant) => grant,
        Err(denial) => return denied(path, denial),
    };
    let t0 = std::time::Instant::now();
    let (res, kind) = match service {
        "info/refs" => (info_refs(&node, &grant, query, &headers).await, "info_refs"),
        "git-upload-pack" => (
            upload_pack(&node, &grant, &headers, body).await,
            "upload_pack",
        ),
        _ => (
            receive_pack(&node, &grant, &headers, body).await,
            "receive_pack",
        ),
    };
    let repo = grant.repo.as_str();
    // The handler's whole time (a push's stages are in `enroute::push_timing`):
    // what a client's latency is beyond it is the network, HTTP, and the
    // client itself.
    tracing::debug!(target: "enroute::request_timing", kind, total_us = micros(t0.elapsed()), "request timing");
    res.unwrap_or_else(|e| {
        if crate::compute::is_not_found(&e) {
            return error(StatusCode::NOT_FOUND, "repository not found");
        }
        tracing::warn!(repo, "request failed: {e:#}");
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            protocol::client_message(&e),
        )
    })
}

fn wants_v2(headers: &HeaderMap) -> bool {
    headers
        .get_all("git-protocol")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.split(':').any(|p| p.trim() == "version=2"))
}

/// The `service` of an `info/refs` query.
fn service_param(query: &str) -> &str {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix("service="))
        .unwrap_or("")
}

/// Ask the host's authorizer about the request. Without an authorizer,
/// `path` is the repository and the context is empty.
async fn authorize(
    node: &Compute,
    spelled: &str,
    path: &str,
    headers: &HeaderMap,
    access: Access,
) -> std::result::Result<Grant, Denial> {
    let Some(authorizer) = &node.hooks.authorize else {
        return Ok(Grant {
            repo: path.to_string(),
            ctx: PushContext::default(),
        });
    };
    let request = GitRequest {
        path,
        spelled,
        headers,
        access,
    };
    let grant = authorizer.authorize(&request).await?;
    if !RepoKey::is_valid(&grant.repo) {
        let e = anyhow::anyhow!("invalid repository name {:?}", grant.repo);
        return Err(Denial::Internal(e));
    }
    Ok(grant)
}

/// The response that refuses a request to `path`.
fn denied(path: &str, denial: Denial) -> Response {
    match denial {
        // Git asks for credentials only on a 401 with WWW-Authenticate.
        Denial::Unauthorized(challenge) => (
            StatusCode::UNAUTHORIZED,
            [
                (header::WWW_AUTHENTICATE, challenge.www_authenticate),
                (header::CONTENT_TYPE, "text/plain".to_string()),
            ],
            challenge.help,
        )
            .into_response(),
        Denial::Forbidden => error(StatusCode::FORBIDDEN, "forbidden"),
        Denial::NotFound => error(StatusCode::NOT_FOUND, "repository not found"),
        // The cause can hold the host's secrets, so only the log gets it.
        Denial::Internal(e) => {
            tracing::warn!(path, "authorization failed: {e:#}");
            error(StatusCode::INTERNAL_SERVER_ERROR, "authorization failed")
        }
    }
}

/// What one request reads of its repository: a snapshot, and the refs that
/// the host hides from the caller, which it asks for once and only when the
/// answer needs them.
struct Reading<'a> {
    node: &'a Compute,
    grant: &'a Grant,
    access: Access,
    snap: Arc<Snapshot>,
    hidden: Option<Hidden>,
    /// `snap` is as of every write committed before the request.
    fresh: bool,
}

impl<'a> Reading<'a> {
    /// The repository of `grant`, or `None` if it is not there. A request
    /// that reads refs opens `linearizable`, not to read the view twice.
    async fn open(
        node: &'a Compute,
        grant: &'a Grant,
        access: Access,
        linearizable: bool,
    ) -> Result<Option<Reading<'a>>> {
        Ok(node
            .repo_for_read(&grant.repo, linearizable)
            .await?
            .map(|r| Reading {
                node,
                grant,
                access,
                snap: r.snapshot(),
                hidden: None,
                fresh: linearizable,
            }))
    }

    async fn hidden(&mut self) -> Result<Hidden> {
        if self.hidden.is_none() {
            let hidden = hidden_refs(
                self.node.hooks.visibility.as_deref(),
                &self.grant.ctx,
                &self.grant.repo,
                self.access,
                &self.snap.meta,
            )
            .await?;
            self.hidden = Some(hidden);
        }
        Ok(self.hidden.clone().unwrap_or_default())
    }

    /// The refs the caller sees under `prefixes`, peeled if `peel`, as of
    /// every push that completed before the request, through any node.
    async fn refs(&mut self, prefixes: &[&str], peel: bool) -> Result<RefView<'_>> {
        self.catch_up().await?;
        let hidden = self.hidden().await?;
        let mut view = RefView::new(&self.snap.meta, &hidden, prefixes);
        if peel {
            view.peel(&self.snap).await?;
        }
        Ok(view)
    }

    /// Read the snapshot as of every write committed so far, unless it is
    /// already: a read barrier makes this node catch up with the log.
    async fn catch_up(&mut self) -> Result<()> {
        if self.fresh {
            return Ok(());
        }
        if let Some(r) = self.node.repo_for_read(&self.grant.repo, true).await? {
            self.snap = r.snapshot();
            self.hidden = None;
        }
        self.fresh = true;
        Ok(())
    }

    /// Read a newer snapshot if this one lacks a want. Over stateless HTTP,
    /// the ref read that named the want can come from another node that is
    /// further ahead. Only a miss pays for the barrier.
    async fn with_wants(&mut self, wants: &[Oid]) -> Result<()> {
        if wants.iter().all(|w| self.snap.lookup(w).is_some()) {
            return Ok(());
        }
        self.catch_up().await
    }

    /// Hide from an `include-tag` the tags the caller does not see.
    async fn hide_tags(&mut self, args: &mut FetchArgs) -> Result<()> {
        if args.include_tag {
            args.hidden = self.hidden().await?;
        }
        Ok(())
    }
}

async fn info_refs(
    node: &Compute,
    grant: &Grant,
    query: &str,
    headers: &HeaderMap,
) -> Result<Response> {
    match service_param(query) {
        "git-upload-pack" => {
            // The v0 advertisement reads refs; a v2 one lists capabilities
            // only.
            let v0 = !wants_v2(headers);
            let Some(mut reading) = Reading::open(node, grant, Access::Read, v0).await? else {
                return Ok(error(StatusCode::NOT_FOUND, "repository not found"));
            };
            let body = if v0 {
                upload_pack_v0::advertisement(&reading.refs(&[], true).await?)
            } else {
                upload_pack::advertisement(reading.snap.format())
            };
            Ok(git_response(
                "application/x-git-upload-pack-advertisement",
                Body::from(body),
            ))
        }
        "git-receive-pack" => {
            // If pushes create repositories, a missing one advertises no
            // refs, in the format a push would create it in.
            let body = match Reading::open(node, grant, Access::Write, true).await? {
                Some(mut reading) => receive_pack::advertisement(&reading.refs(&[], false).await?),
                None if node.config.create_on_push => receive_pack::advertisement(&RefView {
                    format: node.config.default_object_format,
                    ..RefView::default()
                }),
                None => return Ok(error(StatusCode::NOT_FOUND, "repository not found")),
            };
            Ok(git_response(
                "application/x-git-receive-pack-advertisement",
                Body::from(body),
            ))
        }
        _ => Ok(error(StatusCode::FORBIDDEN, "dumb http is not supported")),
    }
}

fn is_gzip(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip") || v.eq_ignore_ascii_case("x-gzip"))
}

async fn upload_pack(
    node: &Compute,
    grant: &Grant,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response> {
    let raw = axum::body::to_bytes(body, MAX_UPLOAD_PACK_REQUEST)
        .await
        .map_err(|e| Refusal::new(format!("reading the request: {e}")))?;
    let raw = if is_gzip(headers) {
        let mut out = Vec::new();
        sirocco::inflate::Reader::new(sirocco::Format::Gzip, &raw[..])
            .take(u64::try_from(MAX_UPLOAD_PACK_REQUEST).unwrap_or(u64::MAX))
            .read_to_end(&mut out)?;
        out
    } else {
        raw.to_vec()
    };
    // Of the commands, only v2 `ls-refs` reads refs.
    let v2 = wants_v2(headers)
        .then(|| upload_pack::parse_request(&raw))
        .transpose()?;
    let ls_refs = v2.as_ref().is_some_and(|r| r.command == "ls-refs");
    let Some(mut reading) = Reading::open(node, grant, Access::Read, ls_refs).await? else {
        return Ok(error(StatusCode::NOT_FOUND, "repository not found"));
    };
    // Wants are not checked against the hidden refs: as with git's
    // `uploadpack.hideRefs`, an object ID stays fetchable.
    let Some(req) = v2 else {
        let mut req = upload_pack_v0::parse(reading.snap.format(), &raw)?;
        reading.with_wants(&req.args.wants).await?;
        if req.sends_pack() {
            reading.hide_tags(&mut req.args).await?;
        }
        let rx = upload_pack_v0::handle(reading.snap, req);
        return Ok(git_response(
            "application/x-git-upload-pack-result",
            Body::from_stream(crate::util::received(rx)),
        ));
    };
    req.check_format(reading.snap.format())?;
    let body = match req.command {
        "ls-refs" => {
            let ask = upload_pack::parse_ls_refs(&req.args)?;
            let view = reading.refs(&ask.prefixes, ask.peel).await?;
            Body::from(upload_pack::ls_refs(&view, ask.symrefs, ask.unborn))
        }
        "fetch" => {
            let mut fetch = upload_pack::parse_fetch(req.format, &req.args)?;
            reading.with_wants(&fetch.args.wants).await?;
            reading.hide_tags(&mut fetch.args).await?;
            Body::from_stream(crate::util::received(upload_pack::fetch(
                reading.snap,
                fetch,
            )))
        }
        // The client asked for nothing: `parse_request` passes no other
        // command.
        _ => Body::from(Bytes::new()),
    };
    Ok(git_response("application/x-git-upload-pack-result", body))
}

async fn receive_pack(
    node: &Compute,
    grant: &Grant,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response> {
    let res = node
        .push(
            &grant.repo,
            body.into_data_stream(),
            is_gzip(headers),
            &grant.ctx,
        )
        .await;
    Ok(git_response(
        "application/x-git-receive-pack-result",
        Body::from(res?),
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_route_decodes_the_repository_path() {
        for (path, repo) in [
            ("/acme/w.git/info/refs", "acme/w.git"),
            ("/a/b/c/git-upload-pack", "a/b/c"),
            ("/Hello%20World~1/git-receive-pack", "Hello World~1"),
            ("/.hidden/info/refs", ".hidden"),
            ("/a/info/refs/info/refs", "a/info/refs"),
            ("/%C3%A9/info/refs", "\u{e9}"),
        ] {
            assert_eq!(
                super::route(path).map(|(r, _)| r).as_deref(),
                Some(repo),
                "{path}"
            );
        }
        let long = format!("/{}/info/refs", "x".repeat(1025));
        for path in [
            "/info/refs",
            "//info/refs",
            "/a//b/info/refs",
            "/a/./b/info/refs",
            "/a/%2E%2E/info/refs",
            "/a%2Fb/info/refs",
            "/a%0Ab/info/refs",
            "/a%FF/info/refs",
            "/a/objects/info/packs",
            long.as_str(),
        ] {
            assert_eq!(super::route(path), None, "{path}");
        }
    }

    #[test]
    fn a_route_keeps_a_key_as_sent() {
        assert_eq!(
            super::route("/acme/w.git/info/refs"),
            Some(("acme/w.git".to_string(), "info/refs"))
        );
        assert_eq!(
            super::route("/acme/w/git-upload-pack"),
            Some(("acme/w".to_string(), "git-upload-pack"))
        );
    }
}
