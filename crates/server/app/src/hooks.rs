//! Asking the application the questions Enroute cannot answer — and working out
//! which application to ask.
//!
//! Enroute knows a repository only by an opaque id; which one a URL names,
//! and whether the request's credential may have it, are the application's to
//! say. *Which* application is answered by the hostname (see [`crate::tenancy`]),
//! so one Enroute serves many customers over one port. One POST per call,
//! nothing streamed and nothing held open, so the application on the far side can
//! be a serverless function.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use prost::Message as _;

use enroute_api::hook::v1alpha1 as pb;
use enroute_git_core::{Error, RepoId};
use enroute_git_http::{
    Access, AuthError, Authorized, Authorizer, Challenge, GitRequest, RefVisibility,
};
use enroute_git_ingest::{Actor, ReceiveHooks, RefCommand, RefJudgement, Verdict};
use enroute_signature::{Covered, SigningKey};

use crate::tenancy::{RepoKey, Tenant, Tenants};
use crate::wire::{wire_oid_from_hex, wire_repo};

/// The most `Granted.context` an application may hand back.
///
/// It rides on every hook call of the push it was captured for, so it is
/// bounded here rather than left to an application's own restraint.
const MAX_CONTEXT: usize = 8 * 1024;

/// Every tenant's application, reached over HTTP.
///
/// One client, not one per tenant: a `reqwest::Client` is a connection
/// pool, and what varies per tenant is only the URL, which is looked up.
#[derive(Debug)]
pub struct Hooks {
    http: reqwest::Client,
    tenants: Arc<Tenants>,
    key: SigningKey,
}

impl Hooks {
    /// A client for every tenant's application, signing with `key`.
    ///
    /// One key for the whole deployment: the signature covers the authority
    /// and path, so a call meant for one tenant does not verify at another's.
    ///
    /// # Errors
    ///
    /// Returns an error if an HTTP client cannot be built.
    pub fn new(tenants: Arc<Tenants>, key: SigningKey, timeout: Duration) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder().timeout(timeout).build()?,
            tenants,
            key,
        })
    }

    /// The public half, for whoever has to configure an application.
    #[must_use]
    pub fn verifying_key(&self) -> enroute_signature::VerifyingKey {
        self.key.verifying_key()
    }

    /// Make one call to `tenant`'s application and read the answer back.
    async fn call(
        &self,
        tenant: &Tenant,
        request: &pb::HookRequest,
    ) -> anyhow::Result<pb::HookResponse> {
        let url = &tenant.hook_endpoint_url;
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

    /// The tenant a git request is for, from the host it arrived on.
    ///
    /// An unregistered hostname is a 404, not a 401 — no credential would
    /// make it exist.
    fn tenant(&self, request: &GitRequest<'_>) -> Result<Tenant, AuthError> {
        let host = request
            .headers
            .get(http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthError::NotFound)?;
        self.tenants.by_host(host).ok_or(AuthError::NotFound)
    }
}

#[async_trait]
impl Authorizer for Hooks {
    #[tracing::instrument(
        name = "enroute::hooks::authorize",
        skip(self, request),
        fields(tenant = tracing::field::Empty, actor = tracing::field::Empty)
    )]
    async fn authorize(&self, request: &GitRequest<'_>) -> Result<Authorized, AuthError> {
        let tenant = self.tenant(request)?;
        // What every unit this request goes on to spend is attributable to,
        // and a span with no tenant could not be attributed later even in
        // principle. The id, since it is the one field configuration may not
        // edit and a record of what was spent may not stop meaning what it did.
        tracing::Span::current().record("tenant", tenant.id.as_str());

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
        let response = self
            .call(&tenant, &call)
            .await
            .map_err(AuthError::Internal)?;
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

        // An application is a customer's own code, so a buggy or compromised
        // one must not reach another customer's storage by answering with a key
        // it read somewhere. It cannot: a key is resolved against the tenant
        // this request arrived for, so another tenant's is simply not there.
        let repo = self
            .tenants
            .resolve(&tenant, &key)
            .await
            .map_err(AuthError::Internal)?
            .ok_or(AuthError::NotFound)?;

        Ok(Authorized { repo, actor })
    }
}

impl Hooks {
    /// Whose application answers for `repo` and what they call it, recorded on
    /// the current span.
    ///
    /// Started from a repository rather than a hostname, since git is being
    /// served by now. An unclaimed one has no application, and no push may land.
    async fn tenant_for(&self, repo: RepoId) -> Result<(Tenant, RepoKey), Error> {
        let (tenant, key) = self
            .tenants
            .by_repo(repo)
            .await?
            .ok_or_else(|| anyhow::anyhow!("repository {repo} belongs to no live tenant"))?;
        tracing::Span::current().record("tenant", tenant.id.as_str());
        Ok((tenant, key))
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
            tenant = tracing::field::Empty,
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
        let (tenant, key) = self.tenant_for(repo).await?;

        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::PreReceive(pb::PreReceiveRequest {
                repo: Some(wire_repo(&key)),
                actor: actor.id.clone(),
                commands: commands.iter().map(wire_command).collect(),
                context: actor.context.clone().into(),
            })),
        };

        let response = self.call(&tenant, &call).await?;
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
            tenant = tracing::field::Empty,
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
        let (tenant, key) = self.tenant_for(repo).await?;

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
        let response = self.call(&tenant, &call).await?;
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
        fields(tenant = tracing::field::Empty, refs = refs.len())
    )]
    async fn visible_refs(
        &self,
        repo: RepoId,
        actor: &Actor,
        access: Access,
        refs: &[&str],
    ) -> Result<Vec<String>, Error> {
        let (tenant, key) = self.tenant_for(repo).await?;

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
        let response = self.call(&tenant, &call).await?;
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

    // A key no repository could have is one this tenant does not have. To
    // whoever is pushing that is the same as a repository that is not there,
    // and saying more would only report an application's own bug to them.
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
