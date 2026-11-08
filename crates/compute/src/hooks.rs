//! Push hooks: a host's `pre-receive` and `post-receive`, called from
//! [`Compute::push`](crate::Compute::push).

mod signed;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use crate::access::{Authorize, RefVisibility};
use crate::git::oid::Oid;

pub use self::signed::HookClient;

/// Who sent a push, as the host knows them. Enroute does not read it: it gives
/// it to the hooks unchanged.
#[derive(Clone, Debug, Default)]
pub struct PushContext {
    /// The pusher's identity.
    pub actor: String,
    /// Opaque data from the host's authorization step.
    pub context: Bytes,
}

/// One ref update of a push, as a hook sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HookCommand {
    /// The full refname, for example `refs/heads/main`.
    pub refname: String,
    /// The current value. `None` creates the ref.
    pub old: Option<Oid>,
    /// The new value. `None` deletes the ref.
    pub new: Option<Oid>,
    /// Whether `old` is not an ancestor of `new`. Always false for a create
    /// or a delete.
    pub force: bool,
}

/// A hook's decision on one command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Judgement {
    /// The command may land.
    Allow,
    /// The command may not land. The text goes to the client.
    Refuse(String),
}

/// A judgement of the command that updates `refname`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefJudgement {
    /// The refname of the command, as [`HookCommand::refname`] has it.
    pub refname: String,
    /// The decision.
    pub judgement: Judgement,
}

/// What a `pre_receive` hook can see when it decides.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreReceiveMode {
    /// The hook gets the commands, with `force`. One log entry.
    #[default]
    Commands,
    /// The hook can also read the pushed objects through the API. Two log entries.
    Objects,
}

impl PreReceiveMode {
    const ALL: [PreReceiveMode; 2] = [PreReceiveMode::Commands, PreReceiveMode::Objects];

    /// The name a configuration spells.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            PreReceiveMode::Commands => "commands",
            PreReceiveMode::Objects => "objects",
        }
    }
}

impl std::str::FromStr for PreReceiveMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|mode| mode.name() == s)
            .ok_or_else(|| {
                let names: Vec<&str> = Self::ALL.into_iter().map(Self::name).collect();
                anyhow::anyhow!("{s} is not a pre_receive mode: one of {}", names.join(", "))
            })
    }
}

/// One hook a deployment can enable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Hook {
    /// Which repository a git request names, and whether it may proceed.
    Authorize,
    /// Which refs a caller sees.
    VisibleRefs,
    /// Which commands of a push may land.
    PreReceive,
    /// What a push landed.
    PostReceive,
}

impl Hook {
    /// Every hook, in the order a push meets them.
    pub const ALL: [Hook; 4] = [
        Hook::Authorize,
        Hook::VisibleRefs,
        Hook::PreReceive,
        Hook::PostReceive,
    ];

    /// The name a configuration spells.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Hook::Authorize => "authorize",
            Hook::VisibleRefs => "visible_refs",
            Hook::PreReceive => "pre_receive",
            Hook::PostReceive => "post_receive",
        }
    }
}

impl std::str::FromStr for Hook {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|hook| hook.name() == s)
            .ok_or_else(|| {
                let names: Vec<&str> = Self::ALL.into_iter().map(Self::name).collect();
                anyhow::anyhow!("{s} is not a hook: one of {}", names.join(", "))
            })
    }
}

/// The host's hooks. Each one that is `None` takes its default.
#[derive(Clone, Debug, Default)]
pub struct HostHooks {
    /// Without it, every request is served, the URL path names the
    /// repository, and the caller has an empty context.
    pub authorize: Option<Arc<dyn Authorize>>,
    /// Without it, every ref shows.
    pub visibility: Option<Arc<dyn RefVisibility>>,
    /// Without them, a push is one log entry.
    pub push: Option<Arc<dyn PushHooks>>,
}

/// The host's hooks on pushes. A compute commits each push in one log entry,
/// unless `pre_receive` reads the pushed objects: then it commits the objects
/// first and the refs after `pre_receive` allows them.
#[async_trait::async_trait]
pub trait PushHooks: Send + Sync + std::fmt::Debug {
    /// What `pre_receive` sees, or `None` for no `pre_receive`. Each host
    /// must choose: the mode sets the cost of a push.
    fn pre_receive_mode(&self) -> Option<PreReceiveMode>;

    /// Which `commands` may land, pushed with the `git push -o` `options`.
    /// No ref has moved yet.
    ///
    /// In [`PreReceiveMode::Objects`], the pushed objects are committed, and
    /// readable through the API on every compute node. In
    /// [`PreReceiveMode::Commands`], they are not stored yet. Each command
    /// needs a judgement. A missing judgement or an error fails the whole
    /// push.
    async fn pre_receive(
        &self,
        ctx: &PushContext,
        repo: &str,
        commands: &[HookCommand],
        options: &[String],
    ) -> Result<Vec<RefJudgement>>;

    /// The `commands` that landed, pushed with the `git push -o` `options`.
    /// Returns lines for the client. An error is only logged, because the
    /// refs have already moved.
    async fn post_receive(
        &self,
        ctx: &PushContext,
        repo: &str,
        commands: &[HookCommand],
        options: &[String],
    ) -> Result<Vec<String>>;
}

/// Why the push failed, if `judgements` leaves a command of `commands`
/// unjudged; else each command's refusal reason, if refused. A judgement
/// for a refname not in `commands` is ignored, and any refusal of a
/// refname wins over an allow.
pub(crate) fn apply_judgements(
    commands: &[HookCommand],
    judgements: Vec<RefJudgement>,
) -> std::result::Result<Vec<Option<String>>, String> {
    let mut by_ref: HashMap<String, Option<String>> = HashMap::new();
    for j in judgements {
        let slot = by_ref.entry(j.refname).or_default();
        if let Judgement::Refuse(reason) = j.judgement {
            slot.get_or_insert(reason);
        }
    }
    commands
        .iter()
        .map(|c| match by_ref.get(&c.refname) {
            Some(r) => Ok(r.clone()),
            None => Err(format!("pre-receive hook did not judge {}", c.refname)),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(name: &str) -> HookCommand {
        HookCommand {
            refname: name.into(),
            old: None,
            new: None,
            force: false,
        }
    }

    fn judge(name: &str, j: Judgement) -> RefJudgement {
        RefJudgement {
            refname: name.into(),
            judgement: j,
        }
    }

    #[test]
    fn judgements_fail_closed() {
        let cmds = [cmd("refs/heads/a"), cmd("refs/heads/b")];
        let refuse = || Judgement::Refuse("no".into());
        assert_eq!(
            apply_judgements(
                &cmds,
                vec![
                    judge("refs/heads/a", Judgement::Allow),
                    judge("refs/heads/b", Judgement::Allow),
                    judge("refs/heads/b", refuse()),
                    judge("refs/heads/other", refuse()),
                ]
            ),
            Ok(vec![None, Some("no".into())])
        );
        assert_eq!(
            apply_judgements(&cmds, vec![judge("refs/heads/a", Judgement::Allow)]),
            Err("pre-receive hook did not judge refs/heads/b".into())
        );
    }
}
