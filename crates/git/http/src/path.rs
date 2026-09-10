//! Where a git URL stops naming a repository and starts naming the protocol.
//!
//! The repository is a path of any depth and the service is a fixed suffix.
//! No route table can match that shape, since `matchit` takes a catch-all
//! only as the final segment, so the cut happens here and routing sees what
//! is left.

use axum::extract::Request;
use axum::http::Uri;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use percent_encoding::percent_decode_str;

use crate::error::HttpError;

/// The repository a git request named, exactly as the client spelled it.
///
/// Percent-decoded, and any `.git` left on: the suffix is a convention with
/// no meaning in the protocol, so what it means is the application's to say.
#[derive(Debug, Clone)]
pub(crate) struct RepoPath(pub(crate) String);

/// What git appends to a repository URL to reach a service.
///
/// Only the smart protocol's three. The dumb protocol's paths are served by
/// nobody here, so a request for one names no repository.
const SERVICES: [&str; 3] = ["/info/refs", "/git-upload-pack", "/git-receive-pack"];

/// Ceiling on a repository path, which nesting leaves otherwise unbounded.
///
/// It is recorded on spans and handed to an application that may store it,
/// so the length is ours to bound rather than the client's to choose.
const MAX_REPO_PATH: usize = 1024;

/// Cut the repository off the front of the path, leaving the route table
/// the git service on its own.
pub(crate) async fn resolve(mut request: Request, next: Next) -> Response {
    let Some((prefix, service)) = split(request.uri().path()) else {
        return refused();
    };
    let Some(repo) = repo_path(prefix) else {
        return refused();
    };
    let Some(uri) = rewrite(request.uri(), service) else {
        return refused();
    };
    *request.uri_mut() = uri;
    request.extensions_mut().insert(RepoPath(repo));
    next.run(request).await
}

/// Split `path` into what precedes a git service suffix, and that suffix.
///
/// Cut at the end and never at the first match: the service is always last,
/// so a repository whose own path ends in `info/refs` still resolves.
fn split(path: &str) -> Option<(&str, &'static str)> {
    SERVICES
        .iter()
        .find_map(|service| Some((path.strip_suffix(service)?, *service)))
}

/// Validate and percent-decode the repository half of a git URL.
///
/// Every refusal is the same one, because a path that will not be served
/// names no repository, and separating the reasons only helps a guess.
fn repo_path(prefix: &str) -> Option<String> {
    let prefix = prefix.strip_prefix('/')?;
    if prefix.is_empty() || prefix.len() > MAX_REPO_PATH {
        return None;
    }

    let mut path = String::with_capacity(prefix.len());
    for segment in prefix.split('/') {
        // Empty means `//` or a trailing slash, which a client that follows
        // the protocol never sends: it MUST strip a trailing `/` first.
        if segment.is_empty() {
            return None;
        }
        let decoded = percent_decode_str(segment).decode_utf8().ok()?;
        // `.` and `..` traverse whatever an application resolves this
        // against, and an encoded `/` forges a segment boundary.
        if decoded == "." || decoded == ".." || decoded.contains('/') {
            return None;
        }
        // It reaches spans, logs and another service, none of which want a
        // control character it never had a reason to hold.
        if decoded.chars().any(char::is_control) {
            return None;
        }
        if !path.is_empty() {
            path.push('/');
        }
        path.push_str(&decoded);
    }
    Some(path)
}

/// Rebuild `uri` with `service` as the whole of its path.
///
/// The query stays, since the advertisement is chosen by it, and so does the
/// authority, which is where HTTP/2 puts the name.
fn rewrite(uri: &Uri, service: &str) -> Option<Uri> {
    let mut parts = uri.clone().into_parts();
    let path_and_query = match uri.query() {
        Some(query) => format!("{service}?{query}"),
        None => service.to_string(),
    };
    parts.path_and_query = Some(path_and_query.parse().ok()?);
    Uri::from_parts(parts).ok()
}

fn refused() -> Response {
    HttpError::NotFound.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What an authorizer would be handed for `path`, or nothing if the
    /// request names no repository this will serve.
    fn resolved(path: &str) -> Option<String> {
        let (prefix, _) = split(path)?;
        repo_path(prefix)
    }

    #[test]
    fn one_segment_resolves_as_it_always_did() {
        assert_eq!(resolved("/hello/info/refs").as_deref(), Some("hello"));
        assert_eq!(resolved("/hello/git-upload-pack").as_deref(), Some("hello"));
        assert_eq!(
            resolved("/hello/git-receive-pack").as_deref(),
            Some("hello")
        );
    }

    #[test]
    fn a_path_nests_to_any_depth() {
        assert_eq!(
            resolved("/owner/repo/info/refs").as_deref(),
            Some("owner/repo")
        );
        assert_eq!(
            resolved("/a/b/c/d/e/info/refs").as_deref(),
            Some("a/b/c/d/e")
        );
    }

    /// The suffix is the client's to spell, so both spellings reach the
    /// application and it is told which one it was.
    #[test]
    fn a_dot_git_suffix_survives() {
        assert_eq!(
            resolved("/owner/repo.git/info/refs").as_deref(),
            Some("owner/repo.git")
        );
        assert_eq!(
            resolved("/owner/repo/info/refs").as_deref(),
            Some("owner/repo")
        );
    }

    /// The shape `gitprotocol-http(5)` documents for a submodule: `.git` on
    /// a segment that is not the last one.
    #[test]
    fn dot_git_inside_the_path_is_not_a_boundary() {
        assert_eq!(
            resolved("/git/repo.git/path/submodule.git/info/refs").as_deref(),
            Some("git/repo.git/path/submodule.git")
        );
    }

    /// Cutting at the last suffix is what makes this unambiguous — the
    /// service is appended, so it is always the trailing one.
    #[test]
    fn a_repository_named_like_a_service_still_resolves() {
        assert_eq!(
            resolved("/a/info/refs/info/refs").as_deref(),
            Some("a/info/refs")
        );
        assert_eq!(
            resolved("/a/git-upload-pack/git-upload-pack").as_deref(),
            Some("a/git-upload-pack")
        );
        assert_eq!(resolved("/a/info/info/refs").as_deref(), Some("a/info"));
    }

    #[test]
    fn a_path_naming_no_service_is_refused() {
        assert_eq!(resolved("/owner/repo"), None);
        assert_eq!(resolved("/owner/repo/objects/info/packs"), None);
        assert_eq!(resolved("/"), None);
    }

    #[test]
    fn a_service_with_no_repository_is_refused() {
        assert_eq!(resolved("/info/refs"), None);
        assert_eq!(resolved("/git-upload-pack"), None);
    }

    #[test]
    fn an_empty_segment_is_refused() {
        assert_eq!(resolved("/owner//repo/info/refs"), None);
        assert_eq!(resolved("//info/refs"), None);
    }

    /// Both spellings, since a check on the raw text alone would miss the
    /// second one.
    #[test]
    fn a_traversal_segment_is_refused() {
        assert_eq!(resolved("/owner/../etc/info/refs"), None);
        assert_eq!(resolved("/owner/%2e%2e/etc/info/refs"), None);
        assert_eq!(resolved("/owner/./repo/info/refs"), None);
    }

    /// Decoding before splitting would read this as two segments, so the
    /// one shape that could forge a boundary is refused instead.
    #[test]
    fn an_encoded_separator_is_refused() {
        assert_eq!(resolved("/owner%2Fevil/info/refs"), None);
        assert_eq!(resolved("/owner%2fevil/info/refs"), None);
    }

    #[test]
    fn a_segment_is_percent_decoded() {
        assert_eq!(
            resolved("/owner/my%20repo/info/refs").as_deref(),
            Some("owner/my repo")
        );
    }

    #[test]
    fn a_control_character_is_refused() {
        assert_eq!(resolved("/owner/re%00po/info/refs"), None);
        assert_eq!(resolved("/owner/re%0apo/info/refs"), None);
    }

    #[test]
    fn invalid_utf8_is_refused() {
        assert_eq!(resolved("/owner/%ff/info/refs"), None);
    }

    #[test]
    fn a_path_past_the_ceiling_is_refused() {
        let long = "a".repeat(MAX_REPO_PATH + 1);
        assert_eq!(resolved(&format!("/{long}/info/refs")), None);
        let allowed = "a".repeat(MAX_REPO_PATH);
        assert_eq!(
            resolved(&format!("/{allowed}/info/refs")).as_deref(),
            Some(allowed.as_str())
        );
    }

    /// The advertisement is selected by the query, so losing it in the
    /// rewrite would serve a fetch to a push and the reverse.
    #[test]
    fn the_rewrite_keeps_the_query() {
        let uri: Uri = "/owner/repo.git/info/refs?service=git-upload-pack"
            .parse()
            .expect("a uri");
        let rewritten = rewrite(&uri, "/info/refs").expect("a rewrite");
        assert_eq!(rewritten.path(), "/info/refs");
        assert_eq!(rewritten.query(), Some("service=git-upload-pack"));
    }

    /// HTTP/2 carries the name in the URI, and the host middleware ahead of
    /// this one reads it from there.
    #[test]
    fn the_rewrite_keeps_the_authority() {
        let uri: Uri = "https://acme.enroute.sh/owner/repo.git/git-upload-pack"
            .parse()
            .expect("a uri");
        let rewritten = rewrite(&uri, "/git-upload-pack").expect("a rewrite");
        assert_eq!(
            rewritten
                .authority()
                .map(axum::http::uri::Authority::as_str),
            Some("acme.enroute.sh")
        );
        assert_eq!(rewritten.path(), "/git-upload-pack");
    }
}

/// Routing, rather than the split alone: what a request actually reaches,
/// and what the application is told it named.
#[cfg(test)]
mod routing {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt as _;

    use enroute_git_core::RepoId;
    use enroute_git_retrieve::Storage;

    use crate::{AuthError, Authorized, Authorizer, GitRequest};

    /// Keeps every path it was asked about, so a test can assert both what
    /// the application saw and that it was asked at all.
    #[derive(Debug)]
    struct Recording {
        repo: RepoId,
        seen: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Authorizer for Recording {
        async fn authorize(&self, request: &GitRequest<'_>) -> Result<Authorized, AuthError> {
            self.seen
                .lock()
                .expect("the recorded paths")
                .push(request.repo.to_string());
            Ok(Authorized {
                repo: self.repo,
                actor: enroute_git_ingest::Actor::new(crate::TEST_USER),
            })
        }
    }

    fn app(state: Storage, repo: RepoId) -> (axum::Router, Arc<Recording>) {
        let authorizer = Arc::new(Recording {
            repo,
            seen: Mutex::new(Vec::new()),
        });
        let worker = enroute_git_ingest::LocalIngestWorker::shared(
            state.clone(),
            Arc::new(object_store::memory::InMemory::new()),
        );
        let router = crate::router(
            state,
            worker,
            authorizer.clone(),
            Arc::new(enroute_git_ingest::NoHooks),
            Arc::new(crate::AllRefsVisible),
        );
        (router, authorizer)
    }

    async fn advertise(uri: &str) -> (StatusCode, Vec<String>) {
        let state = enroute_git_test_support::make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let (app, recorded) = app(state, repo.id);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("a request"),
            )
            .await
            .expect("a response");
        let seen = recorded.seen.lock().expect("the recorded paths").clone();
        (response.status(), seen)
    }

    /// The whole point: a path of two segments is served, and the `.git` the
    /// client spelled is still on it when the application is asked.
    #[tokio::test]
    async fn a_nested_path_reaches_the_application_whole() {
        let (status, seen) =
            advertise("/enroute-sh/enroute.git/info/refs?service=git-upload-pack").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(seen, ["enroute-sh/enroute.git"]);
    }

    /// Both spellings reach the application, which is told which one it was
    /// and decides for itself whether they are one repository.
    #[tokio::test]
    async fn the_suffix_is_not_stripped_on_the_way_through() {
        let (_, seen) = advertise("/enroute-sh/enroute/info/refs?service=git-upload-pack").await;
        assert_eq!(seen, ["enroute-sh/enroute"]);
    }

    /// A refusal here is the route table's, and the shape of the bug this
    /// change fixes: the application was never asked at all.
    #[tokio::test]
    async fn a_refused_path_never_reaches_the_application() {
        let (status, seen) = advertise("/owner/../etc/info/refs?service=git-upload-pack").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(seen.is_empty(), "the application was asked: {seen:?}");
    }

    #[tokio::test]
    async fn a_path_naming_no_service_is_not_served() {
        let (status, seen) = advertise("/enroute-sh/enroute.git").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(seen.is_empty(), "the application was asked: {seen:?}");
    }
}
