//! Asking the application the questions Enroute cannot answer.
//!
//! Enroute knows a repository only by an opaque id; which one a URL names, and
//! whether the request's credential may have it, are the application's to say.
//! One application answers for the whole deployment, at `hooks.endpoint_url`.
//! One POST per call, nothing streamed and nothing held open, so the
//! application on the far side can be a serverless function.

use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use prost::Message as _;

use enroute_api::hook::v1alpha1 as pb;
use enroute_git_core::{Error, RepoId};
use enroute_git_http::{
    Access, AuthError, Authorized, Authorizer, Challenge, GitRequest, RefVisibility,
};
use enroute_git_ingest::{Actor, ReceiveHooks, RefCommand, RefJudgement, Verdict};
use enroute_signature::{Covered, SigningKey};

use crate::repo_key::RepoKey;
use crate::wire::{wire_oid_from_hex, wire_repo};

/// The most `Granted.context` an application may hand back.
///
/// It rides on every hook call of the push it was captured for, so it is
/// bounded here rather than left to an application's own restraint.
const MAX_CONTEXT: usize = 8 * 1024;

/// The application, reached over HTTP.
///
/// One client and not one per call: a `reqwest::Client` is a connection pool.
#[derive(Debug)]
pub struct Hooks {
    http: reqwest::Client,
    state: enroute_git_retrieve::Storage,
    endpoint: url::Url,
    key: SigningKey,
}

impl Hooks {
    /// A client for the application at `endpoint`, signing with `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if an HTTP client cannot be built.
    pub fn new(
        state: enroute_git_retrieve::Storage,
        endpoint: url::Url,
        key: SigningKey,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder().timeout(timeout).build()?,
            state,
            endpoint,
            key,
        })
    }

    /// The public half, for whoever has to configure an application.
    #[must_use]
    pub fn verifying_key(&self) -> enroute_signature::VerifyingKey {
        self.key.verifying_key()
    }

    /// Make one call to the application and read the answer back.
    async fn call(&self, request: &pb::HookRequest) -> anyhow::Result<pb::HookResponse> {
        let url = &self.endpoint;
        let authority = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("the hooks endpoint URL names no host"))?;
        // The signature covers the authority as the request addresses it, so a
        // non-default port is part of it and a default one is not.
        let authority = match url.port() {
            Some(port) => format!("{authority}:{port}"),
            None => authority.to_string(),
        };
        let path = url.path().to_string();

        let body = request.encode_to_vec();
        let covered = Covered {
            method: "POST",
            authority: &authority,
            path: &path,
            body: &body,
        };

        let mut post = self
            .http
            .post(url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf");
        for (name, value) in
            enroute_signature::sign(&self.key, &covered, enroute_signature::now_unix_secs())
        {
            post = post.header(name, value);
        }

        let response = post.body(body).send().await?.error_for_status()?;
        Ok(pb::HookResponse::decode(response.bytes().await?)?)
    }
}

#[async_trait]
impl Authorizer for Hooks {
    #[tracing::instrument(
        name = "enroute::hooks::authorize",
        skip(self, request),
        fields(actor = tracing::field::Empty)
    )]
    async fn authorize(&self, request: &GitRequest<'_>) -> Result<Authorized, AuthError> {
        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::Authorize(pb::AuthorizeRequest {
                repo_path: request.repo.to_string(),
                headers: headers(request),
                access: i32::from(match request.access {
                    Access::Read => pb::Access::Read,
                    Access::Write => pb::Access::Write,
                }),
            })),
        };

        // An unreachable or misbehaving application is a fault, not a refusal —
        // a 500, so a push fails closed rather than being quietly allowed.
        let response = self.call(&call).await.map_err(AuthError::Internal)?;
        let Some(pb::hook_response::Answer::Authorize(answer)) = response.answer else {
            return Err(AuthError::Internal(anyhow::anyhow!(
                "the hooks did not answer the authorize call"
            )));
        };

        let (key, actor) = match answer.outcome {
            Some(pb::authorize_response::Outcome::Granted(granted)) => granted_by(granted)?,
            Some(pb::authorize_response::Outcome::Denied(denied)) => return Err(refused(denied)),
            None => {
                return Err(AuthError::Internal(anyhow::anyhow!(
                    "the hooks neither granted nor denied"
                )));
            }
        };

        // Where the actor is learned, and the only span a read path has: a
        // fetch is attributable to nobody if it is not recorded here.
        tracing::Span::current().record("actor", actor.id.as_str());

        // A key no repository holds is a 404: the application named something
        // that is not here, which no credential would change.
        let repo = self
            .state
            .rows
            .by_key(key.external())
            .await
            .map_err(AuthError::Internal)?
            .ok_or(AuthError::NotFound)?
            .id;

        Ok(Authorized { repo, actor })
    }
}

impl Hooks {
    /// What the application calls `repo`, which is what every hook call spells.
    ///
    /// Started from a repository rather than from a URL, since git is being
    /// served by now. An unregistered one has no name to spell.
    async fn key_of(&self, repo: RepoId) -> Result<RepoKey, Error> {
        let key = self
            .state
            .rows
            .repo(repo)
            .key_of()
            .await?
            .ok_or_else(|| anyhow::anyhow!("repository {repo} has no key"))?;
        Ok(key.as_str().parse().context("a stored repository key")?)
    }
}

#[async_trait]
impl ReceiveHooks for Hooks {
    // `err`, because this call now fails for an application's own bug as well
    // as for an unreachable one, and whoever owns the endpoint is usually not
    // whoever pushed — the only other place it is reported.
    #[tracing::instrument(
        name = "enroute::hooks::pre_receive",
        err,
        skip(self, actor, commands),
        fields(
            actor = %actor.id,
            commands = commands.len()
        )
    )]
    async fn pre_receive(
        &self,
        repo: RepoId,
        actor: &Actor,
        commands: &[RefCommand],
    ) -> Result<Vec<RefJudgement>, Error> {
        let key = self.key_of(repo).await?;

        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::PreReceive(pb::PreReceiveRequest {
                repo: Some(wire_repo(&key)),
                actor: actor.id.clone(),
                commands: commands.iter().map(wire_command).collect(),
                context: actor.context.clone().into(),
            })),
        };

        let response = self.call(&call).await?;
        // Same rule as `authorize`: an application that did not answer has not
        // decided, and `receive-pack` fails a hook it could not run rather
        // than guessing which way it would have gone.
        let Some(pb::hook_response::Answer::PreReceive(answer)) = response.answer else {
            return Err(anyhow::anyhow!("the hooks did not answer the pre-receive call").into());
        };

        // Whether every command was judged is the engine's to check, since
        // the engine is what holds the commands. This only reads what the
        // answer says.
        answer.judgements.into_iter().map(judged).collect()
    }

    #[tracing::instrument(
        name = "enroute::hooks::post_receive",
        skip(self, actor, commands),
        fields(
            actor = %actor.id,
            commands = commands.len()
        )
    )]
    async fn post_receive(
        &self,
        repo: RepoId,
        actor: &Actor,
        commands: &[RefCommand],
    ) -> Result<Vec<String>, Error> {
        let key = self.key_of(repo).await?;

        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::PostReceive(
                pb::PostReceiveRequest {
                    repo: Some(wire_repo(&key)),
                    actor: actor.id.clone(),
                    commands: commands.iter().map(wire_command).collect(),
                    context: actor.context.clone().into(),
                },
            )),
        };

        // The opposite rule to `pre_receive`'s. Silence there means nobody
        // decided and the push fails; here the refs have already moved, so a
        // application too old to know this call is one that will not hear about
        // pushes rather than one that breaks them.
        let response = self.call(&call).await?;
        let Some(pb::hook_response::Answer::PostReceive(answer)) = response.answer else {
            tracing::debug!("the hooks does not answer post-receive");
            return Ok(Vec::new());
        };
        Ok(answer.messages)
    }
}

#[async_trait]
impl RefVisibility for Hooks {
    #[tracing::instrument(
        name = "enroute::hooks::visible_refs",
        skip(self, refs),
        fields(refs = refs.len())
    )]
    async fn visible_refs(
        &self,
        repo: RepoId,
        actor: &Actor,
        access: Access,
        refs: &[&str],
    ) -> Result<Vec<String>, Error> {
        let key = self.key_of(repo).await?;

        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::VisibleRefs(
                pb::VisibleRefsRequest {
                    repo: Some(wire_repo(&key)),
                    actor: actor.id.clone(),
                    context: actor.context.clone().into(),
                    access: i32::from(match access {
                        Access::Read => pb::Access::Read,
                        Access::Write => pb::Access::Write,
                    }),
                    refnames: refs.iter().map(|name| (*name).to_string()).collect(),
                },
            )),
        };

        // `pre_receive`'s rule, for the same reason: an application that did not
        // answer has not decided, and advertising every ref would publish the
        // very ones it may have meant to hide.
        let response = self.call(&call).await?;
        let Some(pb::hook_response::Answer::VisibleRefs(answer)) = response.answer else {
            return Err(anyhow::anyhow!("the hooks did not answer the visible-refs call").into());
        };

        Ok(answer.refnames)
    }
}

/// One judgement as the application spelled it.
///
/// # Errors
///
/// Returns an error if it named a ref and then decided nothing about it,
/// which proto3 cannot tell from a field nobody set.
fn judged(judgement: pb::RefJudgement) -> Result<RefJudgement, Error> {
    let verdict = match judgement.judgement() {
        pb::Judgement::Allow => Verdict::Allow,
        pb::Judgement::Refuse => Verdict::Refuse(judgement.reason),
        pb::Judgement::Unspecified => {
            return Err(
                anyhow::anyhow!("the hooks judged {} neither way", judgement.refname).into(),
            );
        }
    };
    Ok(RefJudgement {
        refname: judgement.refname,
        verdict,
    })
}

fn wire_command(command: &RefCommand) -> pb::RefCommand {
    pb::RefCommand {
        refname: command.refname.clone(),
        old_object_id: wire_oid_from_hex(command.old_id.as_ref()),
        new_object_id: wire_oid_from_hex(command.new_id.as_ref()),
        force: command.force,
    }
}

fn headers(request: &GitRequest<'_>) -> Vec<pb::Header> {
    request
        .headers
        .iter()
        .filter_map(|(name, value)| {
            Some(pb::Header {
                name: name.as_str().to_string(),
                // A header whose bytes are not text cannot cross a protobuf
                // string. Nothing an application authorizes on is binary.
                value: value.to_str().ok()?.to_string(),
            })
        })
        .collect()
}

fn granted_by(granted: pb::Granted) -> Result<(RepoKey, Actor), AuthError> {
    let key = granted
        .repo
        .ok_or_else(|| AuthError::Internal(anyhow::anyhow!("the hooks granted no repository")))?
        .key;

    // A key no repository could have is a repository that is not there. Saying
    // more would only report an application's own bug to whoever is pushing.
    let key: RepoKey = key.parse().map_err(|_bad| AuthError::NotFound)?;

    if granted.context.len() > MAX_CONTEXT {
        return Err(AuthError::Internal(anyhow::anyhow!(
            "the hooks returned {} bytes of context, over the {MAX_CONTEXT} allowed",
            granted.context.len()
        )));
    }

    Ok((
        key,
        Actor {
            id: granted.actor,
            context: granted.context.to_vec(),
        },
    ))
}

fn refused(denied: pb::Denied) -> AuthError {
    match denied.denial() {
        pb::Denial::Forbidden => AuthError::Forbidden,
        pb::Denial::NotFound => AuthError::NotFound,
        // A denial nobody spelled is the one that leaks least: it says only
        // that this caller did not get in, not whether the repository exists.
        pb::Denial::Unauthorized | pb::Denial::Unspecified => {
            AuthError::Unauthorized(challenge(denied.challenge))
        }
    }
}

fn challenge(challenge: Option<pb::Challenge>) -> Challenge {
    let Some(challenge) = challenge else {
        // The contract asks for one here. Without it git has nothing to
        // retry against, so send something rather than turning the application's
        // omission into a 500 for the person at the terminal.
        tracing::warn!("the hooks refused a caller without saying how to authenticate");
        return Challenge::basic("git", "");
    };
    Challenge {
        www_authenticate: challenge.www_authenticate,
        help: challenge.help,
    }
}
