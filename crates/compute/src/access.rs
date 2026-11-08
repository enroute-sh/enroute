//! Access hooks: a host's authorizer and ref visibility, called from the
//! smart HTTP frontend ([`crate::http`]).
//!
//! Enroute authenticates nothing. The host decides who may reach a repository,
//! which repository a URL names, and which refs a caller may see.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use axum::http::HeaderMap;

use crate::hooks::PushContext;
use crate::meta::RepoState;

/// What a request does to the repository.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    /// A fetch, and the advertisements before it.
    Read,
    /// A push, and the advertisement before it.
    Write,
}

/// One git request, as the authorizer sees it.
#[derive(Clone, Copy, Debug)]
pub struct GitRequest<'a> {
    /// The URL path without the leading `/`, the `.git` suffix and the
    /// service, for example `acme/widgets`. Percent-decoded, and not always
    /// a valid repository name: the host maps it to one.
    pub path: &'a str,
    /// The URL path as the client sent it (percent-decoded), without the
    /// leading `/` and the service, for example `acme/widgets.git`. A host
    /// that tells `widgets` from `widgets.git` reads this.
    pub spelled: &'a str,
    /// The request headers, with the credential (`Authorization`).
    pub headers: &'a HeaderMap,
    /// What the route does to the repository.
    pub access: Access,
}

/// A request that the authorizer accepted.
#[derive(Clone, Debug)]
pub struct Grant {
    /// The repository to serve, in place of the path.
    pub repo: String,
    /// The caller. Enroute gives it to [`RefVisibility`] and to the push hooks
    /// unchanged.
    pub ctx: PushContext,
}

/// How a caller without a usable credential must authenticate.
#[derive(Clone, Debug)]
pub struct Challenge {
    /// The `WWW-Authenticate` value. Git asks for credentials only when a
    /// `401` has one.
    pub www_authenticate: String,
    /// A plain-text body. Git shows it as `remote:` lines.
    pub help: String,
}

impl Challenge {
    /// A `Basic` challenge for `realm`, with `help` as the body. Put a
    /// newline at the end of `help`, because git shows it as it is.
    #[must_use]
    pub fn basic(realm: &str, help: &str) -> Self {
        Self {
            www_authenticate: format!("Basic realm=\"{realm}\""),
            help: help.to_string(),
        }
    }
}

/// Why the authorizer did not accept a request.
#[derive(Debug)]
pub enum Denial {
    /// No usable credential (`401`, with the challenge).
    Unauthorized(Challenge),
    /// A valid credential without this access (`403`).
    Forbidden,
    /// No such repository, or one the caller may not know of (`404`).
    NotFound,
    /// The authorizer failed and made no decision (`500`).
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for Denial {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

/// The host's authorizer. It runs before every git route serves anything.
#[async_trait::async_trait]
pub trait Authorize: Send + Sync + std::fmt::Debug {
    /// The repository that `request` names and who the caller is, or why
    /// the request is refused.
    ///
    /// # Errors
    ///
    /// Returns a [`Denial`] when the request may not be served.
    async fn authorize(&self, request: &GitRequest<'_>) -> Result<Grant, Denial>;
}

/// The host's ref visibility. It runs before every ref advertisement, and
/// before a fetch includes tags on its own (`include-tag`).
///
/// It hides refnames, not objects: a client that knows the object ID of a
/// hidden ref's tip can still fetch it, as with git's `uploadpack.hideRefs`.
#[async_trait::async_trait]
pub trait RefVisibility: Send + Sync + std::fmt::Debug {
    /// The refnames among `refs` that the caller may see. Enroute hides the
    /// others. A refname not in `refs` has no effect.
    ///
    /// `HEAD` is not in `refs`: Enroute hides it together with the branch that
    /// it points at.
    ///
    /// # Errors
    ///
    /// An error fails the request, so that no ref shows without a decision.
    async fn visible_refs(
        &self,
        ctx: &PushContext,
        repo: &str,
        access: Access,
        refs: &[&str],
    ) -> Result<Vec<String>>;
}

/// The refnames that one caller may not see. `HEAD` is one of them when
/// the branch it points at is.
#[derive(Clone, Debug, Default)]
pub(crate) struct Hidden(Arc<HashSet<String>>);

impl Hidden {
    /// Whether the ref `name` (or `HEAD`) is hidden.
    #[must_use]
    pub(crate) fn hides(&self, name: &str) -> bool {
        self.0.contains(name)
    }
}

/// The refs of `meta` that `visibility` hides from the caller `ctx`.
/// Without `visibility`, none.
pub(crate) async fn hidden_refs(
    visibility: Option<&dyn RefVisibility>,
    ctx: &PushContext,
    repo: &str,
    access: Access,
    meta: &RepoState,
) -> Result<Hidden> {
    let Some(visibility) = visibility else {
        return Ok(Hidden::default());
    };
    let names: Vec<&str> = meta.refs.keys().map(String::as_str).collect();
    // A fresh clone of an empty repository needs no call.
    if names.is_empty() {
        return Ok(Hidden::default());
    }
    let visible: HashSet<String> = visibility
        .visible_refs(ctx, repo, access, &names)
        .await?
        .into_iter()
        .collect();
    let mut hidden: HashSet<String> = names
        .into_iter()
        .filter(|name| !visible.contains(*name))
        .map(str::to_string)
        .collect();
    // An unborn HEAD names no ref, so it stays: else a clone of an empty
    // branch would get the client's default branch.
    if hidden.contains(&meta.head) {
        hidden.insert("HEAD".to_string());
    }
    Ok(Hidden(Arc::new(hidden)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::oid::Oid;

    #[derive(Debug)]
    struct Keep(Vec<&'static str>);

    #[async_trait::async_trait]
    impl RefVisibility for Keep {
        async fn visible_refs(
            &self,
            _ctx: &PushContext,
            _repo: &str,
            _access: Access,
            _refs: &[&str],
        ) -> Result<Vec<String>> {
            Ok(self.0.iter().map(|s| (*s).to_string()).collect())
        }
    }

    fn meta(head: &str, refs: &[&str]) -> RepoState {
        RepoState {
            head: head.into(),
            refs: refs.iter().map(|r| ((*r).to_string(), Oid::ZERO)).collect(),
            ..Default::default()
        }
    }

    async fn hidden(keep: &[&'static str], meta: &RepoState) -> Vec<String> {
        let vis = Keep(keep.to_vec());
        let h = hidden_refs(Some(&vis), &PushContext::default(), "r", Access::Read, meta)
            .await
            .unwrap();
        let mut out: Vec<String> = h.0.iter().cloned().collect();
        out.sort();
        out
    }

    #[tokio::test]
    async fn head_follows_its_branch() {
        let m = meta("refs/heads/main", &["refs/heads/main", "refs/heads/x"]);
        assert_eq!(
            hidden(&["refs/heads/x"], &m).await,
            ["HEAD", "refs/heads/main"]
        );
        assert_eq!(hidden(&["refs/heads/main"], &m).await, ["refs/heads/x"]);
        // An unborn HEAD stays, and an unknown refname has no effect.
        let m = meta("refs/heads/none", &["refs/heads/x"]);
        assert_eq!(hidden(&["refs/heads/none"], &m).await, ["refs/heads/x"]);
        assert!(
            hidden_refs(None, &PushContext::default(), "r", Access::Read, &m)
                .await
                .unwrap()
                .0
                .is_empty()
        );
    }
}
