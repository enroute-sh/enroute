//! The hook where an application decides who may reach a repository over git.
//!
//! This crate authenticates nothing and resolves no names: it hands an
//! [`Authorizer`] the request and gets back the repository to serve. The
//! application answering is not in this process, so everything crossing the
//! trait must be something one service can say to another — never a handle into
//! storage, or the application would have to hold the storage.

use async_trait::async_trait;
use axum::http::HeaderMap;

use enroute_git_core::RepoId;
use enroute_git_ingest::Actor;
use enroute_git_proto::Access;
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::error::HttpError;

/// One git request, as much of it as an authorizer can need.
#[derive(Debug)]
pub struct GitRequest<'a> {
    /// The repository path, exactly as the client spelled it.
    ///
    /// What it is relative to, and what a `.git` on the end of it means, is
    /// the authorizer's business and not this crate's.
    pub repo: &'a str,
    /// The full request headers, carrying both the credential and whatever
    /// the authorizer scopes it by.
    pub headers: &'a HeaderMap,
    /// What the route about to run would do to the repository.
    pub access: Access,
}

/// How a caller who was refused is told to authenticate — read when a 401
/// is actually produced, not carried through every request.
#[derive(Debug, Clone)]
pub struct Challenge {
    /// The `WWW-Authenticate` value that git's credential handling engages
    /// off — netrc, credential helpers, a prompted retry.
    pub www_authenticate: String,
    /// A plain-text body, which git prints as `remote:` lines — the one
    /// place to say why the credential they hold will never work.
    pub help: String,
}

impl Challenge {
    /// A `Basic` challenge naming `realm`, with `help` as the body.
    ///
    /// Include `help`'s trailing newline, since git renders it verbatim.
    #[must_use]
    pub fn basic(realm: &str, help: &str) -> Self {
        Self {
            www_authenticate: format!("Basic realm=\"{realm}\""),
            help: help.to_string(),
        }
    }
}

/// A request that cleared authorization.
#[derive(Debug, Clone)]
pub struct Authorized {
    /// The repository to serve, named by the id this engine stores it under.
    ///
    /// An id, not a resolved [`RepoMetadata`] — an authorizer in another
    /// process has no storage to resolve one against.
    pub repo: RepoId,
    /// Whoever the authorizer admitted this as, played back to it at hook
    /// time — the one party not trusted to say who it is is the git client.
    pub actor: Actor,
}

/// An authorized request with its repository resolved to storage — what the
/// handlers actually run against.
pub(crate) struct Resolved {
    pub(crate) repo: RepoMetadata,
    pub(crate) actor: Actor,
}

/// Why a request may not be served — narrower than
/// [`enroute_git_proto::Error`]: only outcomes of *who may reach a repository*.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// No usable credential was presented, and here is how to present one.
    ///
    /// Travels with the refusal, not read off the authorizer afterwards — only
    /// the application that refused knows its own realm.
    #[error("unauthorized")]
    Unauthorized(Challenge),
    /// A valid credential that doesn't grant this access.
    #[error("forbidden")]
    Forbidden,
    /// No such repository — or one the caller may not be told about.
    #[error("not found")]
    NotFound,
    /// The authorizer itself failed, distinct from a decision — reported as
    /// a 500 rather than a denial.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Decides who may reach a repository over git and which repository a
/// request names, supplied to [`crate::router`] by the binary.
#[async_trait]
pub trait Authorizer: Send + Sync {
    /// Resolve `request` to the repository it addresses, or reject it.
    ///
    /// "No such repository" and "no credential" are one decision here — the
    /// order would tell an anonymous caller which repositories exist.
    async fn authorize(&self, request: &GitRequest<'_>) -> Result<Authorized, AuthError>;
}

/// Authorize one request and resolve what it named, turning a refusal into
/// its HTTP status and, for a 401, the challenge that says how to try again.
///
/// An id that resolves to nothing is a 404, not a 500 — the authorizer may
/// name a repository this engine has since deleted, which is a race, not a fault.
pub(crate) async fn authorize(
    authorizer: &dyn Authorizer,
    state: &Storage,
    request: GitRequest<'_>,
) -> Result<Resolved, HttpError> {
    let cleared = match authorizer.authorize(&request).await {
        Ok(cleared) => cleared,
        Err(AuthError::Unauthorized(challenge)) => {
            return Err(HttpError::Unauthorized(challenge));
        }
        Err(AuthError::Forbidden) => return Err(HttpError::Forbidden),
        Err(AuthError::NotFound) => return Err(HttpError::NotFound),
        Err(AuthError::Internal(e)) => return Err(e.into()),
    };
    let repo = state
        .rows
        .repo(cleared.repo)
        .lookup()
        .await?
        .ok_or(HttpError::NotFound)?;
    Ok(Resolved {
        repo,
        actor: cleared.actor,
    })
}
