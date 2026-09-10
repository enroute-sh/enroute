//! The application Enroute asks, minus whatever the asking harness decides.
//!
//! Enroute serves no git until an application says which repository a URL
//! names and who is asking, and a real one is a customer's own service, which
//! a Rust harness cannot spawn. This answers the four calls, and verifies the
//! signature as a real application must: Enroute signing calls nobody checks
//! would pass every test here and be useless in production. What is left —
//! which repositories exist, which refs may land, which are visible — is a
//! [`Policy`] the harness writes.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use prost::Message as _;

use enroute_api::common::v1alpha1 as common;
use enroute_api::hook::v1alpha1 as pb;
use enroute_signature::VerifyingKey;

/// Where this application answers.
///
/// Enroute is told the whole URL, so this only has to match on both sides —
/// which [`endpoint_url`] guarantees.
const ENDPOINT_PATH: &str = "/enroute/hooks";

/// The key Enroute signs endpoint calls with in every harness here.
///
/// RFC 9421's own published `test-key-ed25519`, so a harness cannot sign with
/// a key it does not itself hold. `dev/config/enroute.toml` carries it too.
const SIGNING_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
    MC4CAQAwBQYDK2VwBCIEIJ+DYvh6SEqVTm50DFtMDoQikTmiCqirVv9mWG9qfSnF\n\
    -----END PRIVATE KEY-----\n";

/// What a git client must present as the Basic password or bearer token.
///
/// Fixed and public: this guards nothing, and a generated one would only have
/// to be threaded through every caller.
pub const TOKEN: &str = "stub-hook-token";

/// The URL to register a tenant with, for an application served at `public_url`.
#[must_use]
pub fn endpoint_url(public_url: &str) -> String {
    format!("{}{ENDPOINT_PATH}", public_url.trim_end_matches('/'))
}

/// [`SIGNING_KEY_PEM`], parsed.
///
/// # Panics
/// Panics if that literal stops being a key, which no caller can go on from.
#[must_use]
pub fn signing_key() -> enroute_signature::SigningKey {
    enroute_signature::SigningKey::from_pem(SIGNING_KEY_PEM).expect("RFC 9421's published test key")
}

/// Everything a stub application decides once a call is verified and decoded.
///
/// Each answer but [`Policy::resolve`] has a default that permits everything,
/// so a harness states only the decisions it is there to exercise.
pub trait Policy: Clone + Send + Sync + 'static {
    /// The realm a challenge names, which reaches the git client verbatim.
    const REALM: &'static str;

    /// Who a granted request is attributed to, which Enroute records.
    const ACTOR: &'static str;

    /// The repository key `name` stands for, or `None` to deny it exists.
    ///
    /// `name` has already lost the `.git` a clone URL conventionally ends in,
    /// since serving both spellings as one repository is the usual choice.
    fn resolve(&self, name: &str) -> Option<String>;

    /// What `Granted.context` carries, to be played back to this application.
    fn context(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Which of a push's refs may land.
    ///
    /// Every one of them, unless the harness has a rejection to prove.
    fn pre_receive(&self, call: &pb::PreReceiveRequest) -> pb::PreReceiveResponse {
        pb::PreReceiveResponse {
            judgements: call
                .commands
                .iter()
                .map(|command| pb::RefJudgement {
                    refname: command.refname.clone(),
                    judgement: i32::from(pb::Judgement::Allow),
                    reason: String::new(),
                })
                .collect(),
        }
    }

    /// Which refs an advertisement shows.
    ///
    /// Every one of them, unless the harness has a hidden ref to prove.
    fn visible_refs(&self, call: &pb::VisibleRefsRequest) -> pb::VisibleRefsResponse {
        pb::VisibleRefsResponse {
            refnames: call.refnames.clone(),
        }
    }

    /// What to say about a push that landed, and where to record it.
    ///
    /// Nothing, by default. Answering at all is what tells Enroute an
    /// application handled the call rather than being too old to know it.
    fn post_receive(&self, call: &pb::PostReceiveRequest) -> pb::PostReceiveResponse {
        let _ = call;
        pb::PostReceiveResponse {
            messages: Vec::new(),
        }
    }
}

/// The one POST route Enroute calls, answering `policy`.
///
/// The signature is checked against `public_url` rather than the arriving
/// headers — the same choice a real application behind a proxy makes.
///
/// # Panics
/// Panics unless `public_url` is a URL naming a host, which it must be.
pub fn router<P: Policy>(policy: P, keys: Vec<VerifyingKey>, public_url: &str) -> Router {
    let uri: axum::http::Uri = public_url
        .parse()
        .expect("the stub application's public URL");
    let state = Stub {
        policy,
        keys: Arc::new(keys),
        authority: uri
            .authority()
            .expect("the stub application's public URL names a host")
            .to_string(),
        path: uri.path().to_string(),
    };
    Router::new()
        .route(ENDPOINT_PATH, post(handle::<P>))
        .with_state(state)
}

#[derive(Clone)]
struct Stub<P> {
    policy: P,
    keys: Arc<Vec<VerifyingKey>>,
    authority: String,
    path: String,
}

async fn handle<P: Policy>(
    State(stub): State<Stub<P>>,
    method: axum::http::Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let covered = enroute_signature::Covered {
        method: method.as_str(),
        authority: &stub.authority,
        path: &stub.path,
        body: &body,
    };
    let now = enroute_signature::now_unix_secs();
    if let Err(why) = enroute_signature::verify(&stub.keys, &covered, &headers, now) {
        // The part a stub must not skip: Enroute signing calls that nobody
        // checks would pass every test here and be useless in production.
        tracing::warn!(%why, "rejected an unsigned call to the stub application");
        return (StatusCode::UNAUTHORIZED, "bad signature").into_response();
    }

    let request = match pb::HookRequest::decode(body) {
        Ok(request) => request,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };

    let answer = match request.call {
        Some(pb::hook_request::Call::Authorize(call)) => {
            pb::hook_response::Answer::Authorize(authorize(&stub, &call))
        }
        Some(pb::hook_request::Call::PreReceive(call)) => {
            pb::hook_response::Answer::PreReceive(stub.policy.pre_receive(&call))
        }
        Some(pb::hook_request::Call::VisibleRefs(call)) => {
            pb::hook_response::Answer::VisibleRefs(stub.policy.visible_refs(&call))
        }
        Some(pb::hook_request::Call::PostReceive(call)) => {
            pb::hook_response::Answer::PostReceive(stub.policy.post_receive(&call))
        }
        // A call this application does not know. Answering nothing is what
        // tells Enroute to refuse rather than proceed on an answer nobody gave.
        None => return encoded(&pb::HookResponse { answer: None }),
    };
    encoded(&pb::HookResponse {
        answer: Some(answer),
    })
}

fn authorize<P: Policy>(stub: &Stub<P>, call: &pb::AuthorizeRequest) -> pb::AuthorizeResponse {
    let named = call
        .repo_path
        .strip_suffix(".git")
        .unwrap_or(&call.repo_path);
    let outcome = if !presented_the_token(call) {
        // Permission before existence, as a real application should: the other
        // order answers "does this repository exist?" to a caller holding no
        // credential.
        pb::authorize_response::Outcome::Denied(pb::Denied {
            denial: i32::from(pb::Denial::Unauthorized),
            challenge: Some(pb::Challenge {
                www_authenticate: format!("Basic realm=\"{}\"", P::REALM),
                help: "this server authenticates git with an access token.\n".to_string(),
            }),
        })
    } else if let Some(key) = stub.policy.resolve(named) {
        pb::authorize_response::Outcome::Granted(pb::Granted {
            repo: Some(common::RepoKey { key }),
            actor: P::ACTOR.to_string(),
            context: stub.policy.context().into(),
        })
    } else {
        pb::authorize_response::Outcome::Denied(pb::Denied {
            denial: i32::from(pb::Denial::NotFound),
            challenge: None,
        })
    };
    pb::AuthorizeResponse {
        outcome: Some(outcome),
    }
}

/// Whether the relayed client headers carry [`TOKEN`], under either scheme.
///
/// The Basic username is ignored: git requires *some* username to send a
/// password at all, and every client picks its own placeholder.
fn presented_the_token(call: &pb::AuthorizeRequest) -> bool {
    let Some(value) = call
        .headers
        .iter()
        .find(|header| {
            header
                .name
                .eq_ignore_ascii_case(header::AUTHORIZATION.as_str())
        })
        .map(|header| header.value.as_str())
    else {
        return false;
    };

    if let Some(bearer) = strip_scheme(value, "Bearer") {
        return bearer == TOKEN;
    }
    let Some(encoded) = strip_scheme(value, "Basic") else {
        return false;
    };
    let Ok(decoded) = STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(decoded) = String::from_utf8(decoded) else {
        return false;
    };
    decoded
        .split_once(':')
        .is_some_and(|(_user, password)| password == TOKEN)
}

/// Strip a scheme name and the space after it, matching case-insensitively
/// per RFC 9110.
fn strip_scheme<'a>(value: &'a str, scheme: &str) -> Option<&'a str> {
    let (name, rest) = value.split_at_checked(scheme.len())?;
    name.eq_ignore_ascii_case(scheme).then_some(())?;
    rest.strip_prefix(' ')
}

fn encoded(response: &pb::HookResponse) -> Response {
    (
        [(header::CONTENT_TYPE, "application/x-protobuf")],
        response.encode_to_vec(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The key `dev/config/enroute.toml` and `local-stack.md` publish.
    ///
    /// A drifted copy would document one no harness here can verify.
    #[test]
    fn compose_signs_with_this_key() {
        let configured = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../config/enroute.toml"),
        )
        .unwrap();

        // The lines between the markers, which is the whole of what the TOML
        // multi-line string around them holds.
        let begin = "-----BEGIN PRIVATE KEY-----";
        let end = "-----END PRIVATE KEY-----";
        let body: Vec<&str> = configured
            .lines()
            .map(str::trim)
            .skip_while(|line| *line != begin)
            .take_while(|line| *line != end)
            .collect();
        assert!(!body.is_empty(), "the local config carries no signing key");
        let pem = format!("{}\n{end}\n", body.join("\n"));

        assert_eq!(
            enroute_signature::SigningKey::from_pem(&pem)
                .unwrap()
                .verifying_key()
                .keyid(),
            signing_key().verifying_key().keyid(),
            "compose.yaml signs with a key no harness here can verify",
        );
    }

    fn authorization(value: &str) -> pb::AuthorizeRequest {
        pb::AuthorizeRequest {
            repo_path: "hello".to_string(),
            headers: vec![pb::Header {
                name: "authorization".to_string(),
                value: value.to_string(),
            }],
            access: i32::from(pb::Access::Read),
        }
    }

    #[test]
    fn the_token_is_accepted_under_either_scheme() {
        assert!(presented_the_token(&authorization(&format!(
            "Bearer {TOKEN}"
        ))));
        assert!(presented_the_token(&authorization(
            "Basic YWxpY2U6c3R1Yi1ob29rLXRva2Vu"
        )));
        // RFC 9110 makes the scheme name case-insensitive.
        assert!(presented_the_token(&authorization(&format!(
            "bearer {TOKEN}"
        ))));
    }

    #[test]
    fn anything_else_is_not_the_token() {
        assert!(!presented_the_token(&authorization("Bearer nope")));
        assert!(!presented_the_token(&authorization("Basic bm9wZTpub3Bl")));
        assert!(!presented_the_token(&authorization("")));
        assert!(!presented_the_token(&pb::AuthorizeRequest {
            repo_path: "hello".to_string(),
            headers: Vec::new(),
            access: i32::from(pb::Access::Read),
        }));
    }
}
