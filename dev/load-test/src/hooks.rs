//! The application Enroute asks, for the one repository under test.
//!
//! Enroute serves no git until an application says which repository a URL
//! names and who is asking, so a load test needs one to exist. It decides
//! nothing else: one repository, one token, and every ref may land and is
//! visible. The endpoint itself, signature check included, is shared.

use axum::Router;

use bench_support::hooks::Policy;
use enroute_signature::VerifyingKey;

/// Who a granted request is attributed to, which Enroute records on spans.
pub(crate) const ACTOR: &str = "bench";

/// The one POST route Enroute calls, answering for `repo_name` alone.
pub(crate) fn router(repo_name: &str, keys: Vec<VerifyingKey>, public_url: &str) -> Router {
    bench_support::hooks::router(
        OneRepo {
            name: repo_name.to_string(),
        },
        keys,
        public_url,
    )
}

/// The whole of what this application decides: the repository under test
/// exists, and nothing else does.
///
/// A benchmark measures the round trip, and a policy would only add a
/// decision nobody is timing.
#[derive(Clone)]
struct OneRepo {
    /// Both what a URL says and what the row is keyed by, the load test
    /// creating the repository under this name.
    name: String,
}

impl Policy for OneRepo {
    const REALM: &'static str = "bench";
    const ACTOR: &'static str = ACTOR;

    fn resolve(&self, name: &str) -> Option<String> {
        (name == self.name).then(|| self.name.clone())
    }
}
