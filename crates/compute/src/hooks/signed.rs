//! Asking the application the questions Enroute cannot answer.
//!
//! [`HookClient`] implements [`Authorize`], [`RefVisibility`] and
//! [`PushHooks`] as signed HTTP calls. Enroute knows a repository only by an opaque id; which one a URL names, and
//! whether the request's credential may have it, are the application's to say.
//! One application answers for the whole deployment, at `hooks.endpoint_url`.
//! One POST per call, nothing streamed and nothing held open, so the
//! application on the far side can be a serverless function. Enroute calls a
//! repository by its name, which is the repository key, so no call here reads
//! a store.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::http::HeaderMap;
use prost::Message as _;

use enroute_api::hook::v1alpha1 as pb;
use enroute_signature::{Covered, SigningKey};

use crate::access::{Access, Authorize, Challenge, Denial, GitRequest, Grant, RefVisibility};
use crate::contract::repo_key::RepoKey;
use crate::contract::wire::{wire_oid, wire_repo};
use crate::git::oid::Oid;

use super::{
    Hook, HookCommand, HostHooks, Judgement, PreReceiveMode, PushContext, PushHooks, RefJudgement,
};

/// The most `Granted.context` an application may hand back.
///
/// It rides on every hook call of the push it was captured for, so it is
/// bounded here rather than left to an application's own restraint.
const MAX_CONTEXT: usize = 8 * 1024;

/// The application, reached over HTTP: the four calls, built, signed and
/// read back.
///
/// One client and not one per call: a `reqwest::Client` is a connection pool.
#[derive(Debug)]
pub struct HookClient {
    http: reqwest::Client,
    endpoint: url::Url,
    key: SigningKey,
    /// The hooks to call; each other one takes its default.
    enabled: BTreeSet<Hook>,
    /// What `pre_receive` sees, if it is enabled.
    pre_receive_mode: PreReceiveMode,
}

/// What the application answered to `authorize`.
#[derive(Debug)]
enum Admission {
    /// The request may be served.
    Granted {
        /// The repository to serve.
        key: RepoKey,
        /// Who is asking, in the application's namespace.
        actor: String,
        /// Bytes to play back on the later calls of this request.
        context: Vec<u8>,
    },
    /// No usable credential, and how to present one.
    Unauthorized {
        /// The `WWW-Authenticate` value.
        www_authenticate: String,
        /// A body that git shows as `remote:` lines.
        help: String,
    },
    /// A valid credential without this access.
    Forbidden,
    /// No such repository, or one the caller may not know of.
    NotFound,
}

impl HookClient {
    /// A client for the application at `endpoint`, signing with `key`, that
    /// gives up on a call after `timeout` and calls only the hooks `enabled`
    /// names.
    ///
    /// # Errors
    ///
    /// Returns an error if an HTTP client cannot be built.
    pub fn new(
        endpoint: url::Url,
        key: SigningKey,
        timeout: Duration,
        enabled: BTreeSet<Hook>,
        pre_receive_mode: PreReceiveMode,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder().timeout(timeout).build()?,
            endpoint,
            key,
            enabled,
            pre_receive_mode,
        })
    }

    /// This client as the host's hooks, for the hooks it calls.
    #[must_use]
    pub fn hooks(self: Arc<Self>) -> HostHooks {
        let on = |hook| self.enabled.contains(&hook);
        let authorize = on(Hook::Authorize).then(|| -> Arc<dyn Authorize> { self.clone() });
        let visibility = on(Hook::VisibleRefs).then(|| -> Arc<dyn RefVisibility> { self.clone() });
        let push = (on(Hook::PreReceive) || on(Hook::PostReceive))
            .then(|| -> Arc<dyn PushHooks> { self.clone() });
        HostHooks {
            authorize,
            visibility,
            push,
        }
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

        // The hook's spans can then join the push's trace.
        let mut traced = HeaderMap::new();
        enroute_shared::trace_context::inject(&mut traced);
        let response = post
            .headers(traced)
            .body(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(pb::HookResponse::decode(response.bytes().await?)?)
    }

    /// Which repository `repo_path` names, and who is asking.
    ///
    /// # Errors
    ///
    /// Returns an error if the application did not decide: it was not
    /// reached, it answered no outcome, or its grant broke the contract.
    async fn admit(
        &self,
        repo_path: &str,
        request_headers: &HeaderMap,
        access: pb::Access,
    ) -> anyhow::Result<Admission> {
        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::Authorize(pb::AuthorizeRequest {
                repo_path: repo_path.to_string(),
                headers: headers(request_headers),
                access: i32::from(access),
            })),
        };

        // An unreachable or misbehaving application is a fault, not a refusal —
        // a 500, so a push fails closed rather than being quietly allowed.
        let response = self.call(&call).await?;
        let Some(pb::hook_response::Answer::Authorize(answer)) = response.answer else {
            anyhow::bail!("the hooks did not answer the authorize call");
        };

        match answer.outcome {
            Some(pb::authorize_response::Outcome::Granted(granted)) => granted_by(granted),
            Some(pb::authorize_response::Outcome::Denied(denied)) => Ok(refused(denied)),
            None => anyhow::bail!("the hooks neither granted nor denied"),
        }
    }
}

#[async_trait]
impl Authorize for HookClient {
    #[tracing::instrument(
        name = "enroute::hooks::authorize",
        skip(self, request),
        fields(actor = tracing::field::Empty)
    )]
    async fn authorize(&self, request: &GitRequest<'_>) -> Result<Grant, Denial> {
        // The contract sends the path as the client spelled it, `.git` too.
        let admission = self
            .admit(
                request.spelled,
                request.headers,
                wire_access(request.access),
            )
            .await?;
        match admission {
            Admission::Granted {
                key,
                actor,
                context,
            } => {
                // Where the actor is learned, and the only span a read path
                // has: a fetch is attributable to nobody if not recorded here.
                tracing::Span::current().record("actor", actor.as_str());
                // A key that names no repository is a 404 from Enroute, as the
                // contract asks.
                Ok(Grant {
                    repo: key.as_str().to_string(),
                    ctx: PushContext {
                        actor,
                        context: context.into(),
                    },
                })
            }
            Admission::Unauthorized {
                www_authenticate,
                help,
            } => Err(Denial::Unauthorized(Challenge {
                www_authenticate,
                help,
            })),
            Admission::Forbidden => Err(Denial::Forbidden),
            Admission::NotFound => Err(Denial::NotFound),
        }
    }
}

#[async_trait]
impl RefVisibility for HookClient {
    #[tracing::instrument(
        name = "enroute::hooks::visible_refs",
        skip(self, ctx, refs),
        fields(refs = refs.len())
    )]
    async fn visible_refs(
        &self,
        ctx: &PushContext,
        repo: &str,
        access: Access,
        refs: &[&str],
    ) -> anyhow::Result<Vec<String>> {
        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::VisibleRefs(
                pb::VisibleRefsRequest {
                    repo: Some(wire_repo(&key_of(repo)?)),
                    actor: ctx.actor.clone(),
                    context: ctx.context.clone(),
                    access: i32::from(wire_access(access)),
                    refnames: refs.iter().map(|name| (*name).to_string()).collect(),
                },
            )),
        };

        // `pre_receive`'s rule, for the same reason: an application that did
        // not answer has not decided, and advertising every ref would publish
        // the very ones it may have meant to hide.
        let response = self.call(&call).await?;
        let Some(pb::hook_response::Answer::VisibleRefs(answer)) = response.answer else {
            anyhow::bail!("the hooks did not answer the visible-refs call");
        };
        Ok(answer.refnames)
    }
}

#[async_trait]
impl PushHooks for HookClient {
    fn pre_receive_mode(&self) -> Option<PreReceiveMode> {
        self.enabled
            .contains(&Hook::PreReceive)
            .then_some(self.pre_receive_mode)
    }

    // `err`, because the owner of the endpoint is usually not the pusher.
    #[tracing::instrument(
        name = "enroute::hooks::pre_receive",
        err,
        skip(self, ctx, commands, options),
        fields(actor = %ctx.actor, commands = commands.len())
    )]
    async fn pre_receive(
        &self,
        ctx: &PushContext,
        repo: &str,
        commands: &[HookCommand],
        options: &[String],
    ) -> anyhow::Result<Vec<RefJudgement>> {
        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::PreReceive(pb::PreReceiveRequest {
                repo: Some(wire_repo(&key_of(repo)?)),
                actor: ctx.actor.clone(),
                commands: commands.iter().map(wire_command).collect(),
                context: ctx.context.clone(),
                push_options: options.to_vec(),
            })),
        };

        let response = self.call(&call).await?;
        // An application that did not answer has not decided, and the push
        // fails rather than guessing which way it would have gone. Enroute also
        // fails the push for a command with no judgement.
        let Some(pb::hook_response::Answer::PreReceive(answer)) = response.answer else {
            anyhow::bail!("the hooks did not answer the pre-receive call");
        };
        answer.judgements.into_iter().map(judged).collect()
    }

    // Enroute only logs an error from here, because the refs have moved.
    #[tracing::instrument(
        name = "enroute::hooks::post_receive",
        skip(self, ctx, commands, options),
        fields(actor = %ctx.actor, commands = commands.len())
    )]
    async fn post_receive(
        &self,
        ctx: &PushContext,
        repo: &str,
        commands: &[HookCommand],
        options: &[String],
    ) -> anyhow::Result<Vec<String>> {
        if !self.enabled.contains(&Hook::PostReceive) {
            return Ok(Vec::new());
        }
        let call = pb::HookRequest {
            call: Some(pb::hook_request::Call::PostReceive(
                pb::PostReceiveRequest {
                    repo: Some(wire_repo(&key_of(repo)?)),
                    actor: ctx.actor.clone(),
                    commands: commands.iter().map(wire_command).collect(),
                    context: ctx.context.clone(),
                    push_options: options.to_vec(),
                },
            )),
        };

        // The opposite rule to `pre_receive`'s: the refs have already moved,
        // so an application too old to know this call is one that will not
        // hear about pushes rather than one that breaks them.
        let response = self.call(&call).await?;
        let Some(pb::hook_response::Answer::PostReceive(answer)) = response.answer else {
            tracing::debug!("the hooks does not answer post-receive");
            return Ok(Vec::new());
        };
        Ok(answer.messages)
    }
}

/// The key of the repository `repo`.
///
/// # Errors
///
/// Returns an error if `repo` is not a key, which only a repository that
/// Enroute did not create can be.
fn key_of(repo: &str) -> anyhow::Result<RepoKey> {
    repo.parse()
        .map_err(|bad| anyhow::anyhow!("repository {repo:?} has no key: {bad}"))
}

fn wire_access(access: Access) -> pb::Access {
    match access {
        Access::Read => pb::Access::Read,
        Access::Write => pb::Access::Write,
    }
}

/// One judgement as the application spelled it.
///
/// # Errors
///
/// Returns an error if it named a ref and then decided nothing about it,
/// which proto3 cannot tell from a field nobody set.
fn judged(judgement: pb::RefJudgement) -> anyhow::Result<RefJudgement> {
    let decided = match judgement.judgement() {
        pb::Judgement::Allow => Judgement::Allow,
        pb::Judgement::Refuse => Judgement::Refuse(judgement.reason),
        pb::Judgement::Unspecified => {
            anyhow::bail!("the hooks judged {} neither way", judgement.refname);
        }
    };
    Ok(RefJudgement {
        refname: judgement.refname,
        judgement: decided,
    })
}

/// `command` as the hook contract spells it: hex, and no ID for an absent
/// side.
fn wire_command(command: &HookCommand) -> pb::RefCommand {
    let wire = |oid: Option<Oid>| oid.map(wire_oid);
    pb::RefCommand {
        refname: command.refname.clone(),
        old_object_id: wire(command.old),
        new_object_id: wire(command.new),
        force: command.force,
    }
}

fn headers(headers: &HeaderMap) -> Vec<pb::Header> {
    headers
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

fn granted_by(granted: pb::Granted) -> anyhow::Result<Admission> {
    let key = granted
        .repo
        .ok_or_else(|| anyhow::anyhow!("the hooks granted no repository"))?
        .key;

    // A key no repository could have is a repository that is not there. Saying
    // more would only report an application's own bug to whoever is pushing.
    let Ok(key) = key.parse::<RepoKey>() else {
        return Ok(Admission::NotFound);
    };

    anyhow::ensure!(
        granted.context.len() <= MAX_CONTEXT,
        "the hooks returned {} bytes of context, over the {MAX_CONTEXT} allowed",
        granted.context.len()
    );

    Ok(Admission::Granted {
        key,
        actor: granted.actor,
        context: granted.context.to_vec(),
    })
}

fn refused(denied: pb::Denied) -> Admission {
    match denied.denial() {
        pb::Denial::Forbidden => Admission::Forbidden,
        pb::Denial::NotFound => Admission::NotFound,
        // A denial nobody spelled is the one that leaks least: it says only
        // that this caller did not get in, not whether the repository exists.
        pb::Denial::Unauthorized | pb::Denial::Unspecified => {
            let Some(challenge) = denied.challenge else {
                // The contract asks for one here. Without it git has nothing to
                // retry against, so send something rather than turning the
                // application's omission into a 500 for the person at the
                // terminal.
                tracing::warn!("the hooks refused a caller without saying how to authenticate");
                let basic = Challenge::basic("git", "");
                return Admission::Unauthorized {
                    www_authenticate: basic.www_authenticate,
                    help: basic.help,
                };
            };
            Admission::Unauthorized {
                www_authenticate: challenge.www_authenticate,
                help: challenge.help,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_command_keeps_force_and_absent_sides() {
        let oid = Oid::from_hex("9f2c1b7e4a8d3c5f6b0e2a1d4c7f8b3e5a6d9c0f").unwrap();
        let create = wire_command(&HookCommand {
            refname: "refs/heads/a".into(),
            old: None,
            new: Some(oid),
            force: false,
        });
        assert_eq!(create.old_object_id, None);
        assert_eq!(create.new_object_id.map(|id| id.hex), Some(oid.to_hex()));
        let forced = wire_command(&HookCommand {
            refname: "refs/heads/a".into(),
            old: Some(oid),
            new: Some(oid),
            force: true,
        });
        assert!(forced.force);
    }

    #[test]
    fn only_a_repository_key_is_a_key() {
        key_of("repo-1").unwrap();
        key_of("acme/widgets").unwrap();
        key_of("acme//widgets").unwrap_err();
    }
}
