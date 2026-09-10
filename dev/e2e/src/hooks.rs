//! The policy this suite's stub application answers with.
//!
//! Enroute serves no git until an application says which repository a URL
//! names and who is asking. The endpoint itself, signature check included, is
//! shared with the bench harnesses; what is here is the map of repositories,
//! the one ref it refuses, the one it judges neither way, the one it hides,
//! and the record of every push it was told had landed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;

use bench_support::hooks::Policy;
use enroute_api::hook::v1alpha1 as pb;
use enroute_signature::VerifyingKey;

/// Who a granted request is attributed to, which a hook can gate on.
pub(crate) const ACTOR: &str = "alice";

/// What this application hands back as `Granted.context`, to be played back to it.
///
/// Opaque to Enroute by contract, so the suite asserts on the bytes arriving
/// unchanged rather than on anything meaning them.
pub(crate) const CONTEXT: &[u8] = b"stub-hook-context";

/// What this application's `post-receive` says about each ref that landed.
///
/// Reaches a git client as a `remote:` line, which is the whole of what the
/// call is for.
pub(crate) const LANDED_PREFIX: &str = "stub application saw";

/// The one refname this application's `pre-receive` refuses.
///
/// Everything else lands.
pub(crate) const PROTECTED: &str = "refs/heads/protected";

/// The one refname this application's `pre-receive` judges neither way.
///
/// What a policy loop that misses a namespace does. The push must fail: an
/// answer for part of a push is an answer for none of it.
pub(crate) const UNJUDGED: &str = "refs/heads/unjudged";

/// The one refname this application hides from an advertisement.
///
/// Everything else is visible. A real application decides this from an actor and a
/// policy; this only has to prove the ref never reaches the client.
pub(crate) const HIDDEN: &str = "refs/heads/hidden";

/// The repositories this application will admit to: a git URL's name, to the id
/// Enroute minted.
///
/// Cloning shares the names, so a test can hand one to [`router`] and keep
/// naming repositories afterwards — an application per case ran out of connections.
#[derive(Debug, Clone, Default)]
pub(crate) struct Repos(Arc<Mutex<HashMap<String, String>>>);

impl Repos {
    /// Name `id` — the id Enroute minted — as `name`, in the builder style.
    #[must_use]
    pub(crate) fn with(self, name: &str, id: impl std::fmt::Display) -> Self {
        self.name(name, id);
        self
    }

    /// Name `id` as `name`, on an application that is already serving.
    pub(crate) fn name(&self, name: &str, id: impl std::fmt::Display) {
        self.0
            .lock()
            .expect("the stub application's names")
            .insert(name.to_string(), id.to_string());
    }

    fn resolve(&self, name: &str) -> Option<String> {
        self.0.lock().ok()?.get(name).cloned()
    }
}

/// Every `post-receive` this application has been told about, newest last.
///
/// Shared with whoever built it, like [`Repos`], because the whole of what a
/// test can assert about this hook is that it arrived and what it said.
#[derive(Debug, Clone, Default)]
pub(crate) struct Landed(Arc<Mutex<Vec<Push>>>);

/// One push Enroute said had landed.
///
/// The smoke tests read the whole of it. The proptest binary only proves the
/// call arrived, so two fields are written and never read there.
#[derive(Debug, Clone)]
pub(crate) struct Push {
    /// The repository id, as Enroute spells it on the wire.
    pub(crate) repo: String,
    /// Who pushed, as `authorize` named them.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "asserted on by the smoke tests")
    )]
    pub(crate) actor: String,
    /// What `authorize` handed back, played back here byte for byte.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "asserted on by the smoke tests")
    )]
    pub(crate) context: Vec<u8>,
    /// Every command that landed: refname, and the id it now points at.
    pub(crate) commands: Vec<(String, String)>,
}

impl Landed {
    /// What this application has been told, in the order it was told.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn seen(&self) -> Vec<Push> {
        self.0
            .lock()
            .expect("the stub application's landed pushes")
            .clone()
    }

    fn record(&self, call: &pb::PostReceiveRequest) {
        let push = Push {
            repo: call
                .repo
                .as_ref()
                .map(|r| r.key.clone())
                .unwrap_or_default(),
            actor: call.actor.clone(),
            context: call.context.to_vec(),
            commands: call
                .commands
                .iter()
                .map(|c| {
                    (
                        c.refname.clone(),
                        crate::contract::hex(c.new_object_id.as_ref()),
                    )
                })
                .collect(),
        };
        tracing::info!(repo = %push.repo, commands = push.commands.len(), "post-receive");
        if let Ok(mut seen) = self.0.lock() {
            seen.push(push);
        }
    }
}

/// The one POST route Enroute calls, naming `repos` and recording into `landed`.
pub(crate) fn router(
    repos: Repos,
    landed: Landed,
    keys: Vec<VerifyingKey>,
    public_url: &str,
) -> Router {
    bench_support::hooks::router(Stub { repos, landed }, keys, public_url)
}

#[derive(Clone)]
struct Stub {
    repos: Repos,
    landed: Landed,
}

impl Policy for Stub {
    const REALM: &'static str = "stub";
    const ACTOR: &'static str = ACTOR;

    fn resolve(&self, name: &str) -> Option<String> {
        self.repos.resolve(name)
    }

    fn context(&self) -> Vec<u8> {
        CONTEXT.to_vec()
    }

    /// Refuse [`PROTECTED`], say nothing about [`UNJUDGED`], and allow the rest.
    ///
    /// A real application decides this from context this does not have, so it answers
    /// by name — enough to prove a rejection reaches the git client.
    fn pre_receive(&self, call: &pb::PreReceiveRequest) -> pb::PreReceiveResponse {
        pb::PreReceiveResponse {
            judgements: call
                .commands
                .iter()
                .filter(|command| command.refname != UNJUDGED)
                .map(|command| {
                    let (judgement, reason) = if command.refname == PROTECTED {
                        (
                            pb::Judgement::Refuse,
                            format!("{} is protected by the stub application", command.refname),
                        )
                    } else {
                        (pb::Judgement::Allow, String::new())
                    };
                    pb::RefJudgement {
                        refname: command.refname.clone(),
                        judgement: i32::from(judgement),
                        reason,
                    }
                })
                .collect(),
        }
    }

    /// Hide [`HIDDEN`] and advertise everything else.
    fn visible_refs(&self, call: &pb::VisibleRefsRequest) -> pb::VisibleRefsResponse {
        pb::VisibleRefsResponse {
            refnames: call
                .refnames
                .iter()
                .filter(|refname| *refname != HIDDEN)
                .cloned()
                .collect(),
        }
    }

    /// Record the push, and say a line about each ref that landed.
    ///
    /// The line is what proves the whole path: a real `git push` printing
    /// what an application decided to say.
    fn post_receive(&self, call: &pb::PostReceiveRequest) -> pb::PostReceiveResponse {
        self.landed.record(call);
        pb::PostReceiveResponse {
            messages: call
                .commands
                .iter()
                .map(|c| format!("{LANDED_PREFIX} {}", c.refname))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_named_repository_resolves_and_others_do_not() {
        let repos = Repos::default().with("hello", 7);
        assert_eq!(repos.resolve("hello").as_deref(), Some("7"));
        assert_eq!(repos.resolve("nope"), None);
    }

    /// What lets one application serve a whole run: a clone handed to the router
    /// keeps seeing names added afterwards.
    #[test]
    fn a_clone_shares_the_names() {
        let repos = Repos::default();
        let serving = repos.clone();
        repos.name("added-later", 7);
        assert_eq!(serving.resolve("added-later").as_deref(), Some("7"));
    }
}
