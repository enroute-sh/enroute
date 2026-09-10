//! Git's receive hooks, for a repository whose hooks live in another
//! process.
//!
//! `pre-receive` is exactly the question this engine cannot answer for itself —
//! branches anybody agreed to protect, reviews anybody requires — so the hook
//! stays where git puts it and only the transport changes: not a program
//! reading stdin, but a trait the binary implements against an application. One
//! departure from git: a real `pre-receive` is all-or-nothing, but here one
//! call answers per ref, since a round trip per ref would cost more than the
//! fidelity is worth.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use gix_hash::ObjectId;

use enroute_git_core::{Error, RepoId};
use enroute_git_retrieve::{RefUpdate, RepoMetadata, Storage};

use crate::concurrency::try_join_bounded;
use crate::ref_updates::PushRejection;

/// One ref update a push is asking for — git's own "command".
#[derive(Debug, Clone)]
pub struct RefCommand {
    /// The full refname, e.g. `refs/heads/main`.
    pub refname: String,
    /// Hex object id of the ref's value before this update, or `None` when
    /// the push creates it.
    ///
    /// Git spells absent as the all-zero oid; this does not, because "is it
    /// a create" is the thing a hook actually asks.
    pub old_id: Option<String>,
    /// Hex object id of the ref's value after this update, or `None` when
    /// the push deletes it.
    pub new_id: Option<String>,
    /// Whether `old_id` is not an ancestor of `new_id`.
    ///
    /// Answered here because the hook is elsewhere and the commit graph is
    /// not. Only ever `true` when both ids are set.
    pub force: bool,
}

/// What a hook decided about one command.
#[derive(Debug, Clone)]
pub struct RefJudgement {
    /// Which command this judges, as the command spelled its refname.
    pub refname: String,
    /// Whether it may land.
    pub verdict: Verdict,
}

/// Whether one command may land.
#[derive(Debug, Clone)]
pub enum Verdict {
    /// It may.
    Allow,
    /// It may not, and this is what to tell whoever pushed it.
    ///
    /// The reason reaches the client verbatim, the way a hook's stderr does.
    Refuse(String),
}

/// Whoever a git request was admitted as, carried to the hooks under it.
#[derive(Clone)]
pub struct Actor {
    /// Whoever is asking, in the namespace of whoever decided.
    ///
    /// Recorded, so it holds an id and never a credential; never split, so
    /// what it looks like inside stays the decider's business.
    pub id: String,
    /// Whatever the decider wants handed back at hook time, byte for byte.
    ///
    /// Never parsed and never recorded: it may hold credential material.
    pub context: Vec<u8>,
}

impl Actor {
    /// An actor with nothing handed back about them.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            context: Vec::new(),
        }
    }
}

impl std::fmt::Debug for Actor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The context must not reach a log by sitting inside a struct
        // somebody derived `Debug` on. Its length is not a secret.
        write!(f, "Actor({}, {} bytes)", self.id, self.context.len())
    }
}

/// What runs at `receive-pack`'s hook points.
///
/// Supplied to [`LocalIngestWorker`] by the binary, the only layer that knows
/// an application exists.
///
/// [`LocalIngestWorker`]: crate::LocalIngestWorker
#[async_trait]
pub trait ReceiveHooks: Send + Sync + std::fmt::Debug {
    /// Every command this push still asks for, before any ref moves.
    ///
    /// Commands Enroute already refused on its own are not passed, so a
    /// hook is never asked about something that cannot land anyway.
    ///
    /// # Returns
    ///
    /// One judgement per command. A command left unjudged is an error, not
    /// an allowance — see [`pre_receive`].
    ///
    /// # Errors
    ///
    /// Returns an error if the hook could not be reached or would not
    /// answer, rather than proceed on a decision nobody made.
    async fn pre_receive(
        &self,
        repo: RepoId,
        actor: &Actor,
        commands: &[RefCommand],
    ) -> Result<Vec<RefJudgement>, Error>;

    /// Every command that landed, once the refs have moved.
    ///
    /// What `pre_receive` judged, less whatever failed to land, and never
    /// nothing. A hook reports on what it was asked about.
    ///
    /// # Returns
    ///
    /// Lines to print to whoever pushed, as git prints a hook's stdout.
    ///
    /// # Errors
    ///
    /// Returns an error if the hook could not be reached, which the caller
    /// logs and carries on from: the push has already succeeded.
    async fn post_receive(
        &self,
        _repo: RepoId,
        _actor: &Actor,
        _commands: &[RefCommand],
    ) -> Result<Vec<String>, Error> {
        Ok(Vec::new())
    }
}

/// Hooks that let every command land, for tests whose subject is not policy.
///
/// The equivalent of having no hooks installed.
#[derive(Debug, Clone, Copy)]
pub struct NoHooks;

#[async_trait]
impl ReceiveHooks for NoHooks {
    async fn pre_receive(
        &self,
        _repo: RepoId,
        _actor: &Actor,
        commands: &[RefCommand],
    ) -> Result<Vec<RefJudgement>, Error> {
        Ok(commands
            .iter()
            .map(|command| RefJudgement {
                refname: command.refname.clone(),
                verdict: Verdict::Allow,
            })
            .collect())
    }
}

/// Run `hooks`' `pre-receive` over every update still live, and return one
/// rejection per refname it refused, with the commands it was asked about.
///
/// The commands come back because `post-receive` needs the same ones, less
/// whatever failed to land — and `force` costs an ancestry read to work out.
///
/// # Errors
///
/// Returns an error if the ancestry reads fail, if the hook would not
/// answer, or if it judged fewer commands than it was shown.
pub(crate) async fn pre_receive(
    hooks: &dyn ReceiveHooks,
    state: &Storage,
    repo: &RepoMetadata,
    updates: &[RefUpdate],
    pre_screened: &HashSet<&str>,
    actor: &Actor,
    null: ObjectId,
) -> Result<(HashMap<String, PushRejection>, Vec<RefCommand>), Error> {
    let live: Vec<&RefUpdate> = updates
        .iter()
        .filter(|u| !pre_screened.contains(u.refname.as_str()))
        .collect();
    if live.is_empty() {
        return Ok((HashMap::new(), Vec::new()));
    }

    // One `is_ancestor` round trip per update (skipped entirely for
    // create/delete, which are never force-pushes) — independent per update,
    // so run them concurrently rather than one DB round trip at a time.
    let forces: Vec<bool> = try_join_bounded(live.iter().map(|u| {
        let is_real_update = u.old_id != null && u.new_id != null;
        async move {
            if !is_real_update {
                return Ok(false);
            }
            let is_ancestor = state
                .graph
                .repo(repo.id)
                .is_ancestor(u.old_id, u.new_id)
                .await?;
            Ok::<bool, anyhow::Error>(!is_ancestor)
        }
    }))
    .await
    .map_err(Error::from)?;

    let commands: Vec<RefCommand> = live
        .iter()
        .zip(forces)
        .map(|(u, force)| {
            // Both null is not a real update and never occurs in practice;
            // delete (checked first) wins the classification for it.
            let is_delete = u.new_id == null;
            let is_create = !is_delete && u.old_id == null;
            RefCommand {
                refname: u.refname.clone(),
                old_id: (!is_create).then(|| u.old_id.to_hex().to_string()),
                new_id: (!is_delete).then(|| u.new_id.to_hex().to_string()),
                force,
            }
        })
        .collect();

    let judgements = hooks.pre_receive(repo.id, actor, &commands).await?;

    let mut judged: HashMap<String, Verdict> = HashMap::with_capacity(commands.len());
    for judgement in judgements {
        // Two answers for one ref means one of them is a mistake, and a
        // refusal is the reading whose cost is a push somebody retries.
        let seen = judged.entry(judgement.refname).or_insert(Verdict::Allow);
        if matches!(seen, Verdict::Allow) {
            *seen = judgement.verdict;
        }
    }

    // Read by command, which is what drops a judgement naming a refname
    // nobody pushed — that keeps a confused hook from refusing an update it
    // was never shown — and what finds the command nobody judged.
    let mut refused = HashMap::new();
    let mut unjudged: Vec<&str> = Vec::new();
    for command in &commands {
        match judged.remove(&command.refname) {
            Some(Verdict::Refuse(reason)) => {
                refused.insert(command.refname.clone(), PushRejection::Policy(reason));
            }
            Some(Verdict::Allow) => {}
            None => unjudged.push(command.refname.as_str()),
        }
    }

    // A command nobody judged is not one to guess at. An application that
    // answered for part of a push has not decided the rest, and the ref it
    // left out is exactly the one where a guess costs the most.
    if !unjudged.is_empty() {
        // Whatever is left in `judged` matched no command, and a refname
        // spelt wrong is both the likeliest cause of this and invisible from
        // the application's side. Name it, or the error names only the ref
        // whose judgement the typo was.
        let unmatched: Vec<&str> = judged.keys().map(String::as_str).collect();
        let stray = if unmatched.is_empty() {
            String::new()
        } else {
            format!(
                " — it judged {}, which this push does not ask for",
                first_few(&unmatched)
            )
        };
        return Err(anyhow::anyhow!(
            "the pre-receive hook judged {} of {} commands, and not {}{stray}",
            commands.len() - unjudged.len(),
            commands.len(),
            first_few(&unjudged),
        )
        .into());
    }

    Ok((refused, commands))
}

/// The first few of `refnames`, and a count of the rest.
///
/// A push can ask for thousands of refs, and an error that names them all is
/// one nobody reads.
fn first_few(refnames: &[&str]) -> String {
    const NAMED: usize = 5;
    // `get` rather than a range index: this runs on a path that is already
    // failing, and a panic there would replace the reason with its own.
    let named = refnames.get(..NAMED).unwrap_or(refnames).join(", ");
    match refnames.len().saturating_sub(NAMED) {
        0 => named,
        rest => format!("{named} and {rest} more"),
    }
}

/// Tell `hooks` what landed, and never fail the push for what it says.
///
/// The refs have already moved, so an error is logged and dropped. Returns
/// whatever the hook wants printed to whoever pushed.
pub(crate) async fn post_receive(
    hooks: &dyn ReceiveHooks,
    repo: RepoId,
    actor: &Actor,
    commands: Vec<RefCommand>,
    outcomes: &[crate::ref_updates::RefUpdateOutcome],
) -> Vec<String> {
    let landed: HashSet<&str> = outcomes
        .iter()
        .filter(|outcome| outcome.result.is_ok())
        .map(|outcome| outcome.refname.as_str())
        .collect();
    let commands: Vec<RefCommand> = commands
        .into_iter()
        .filter(|command| landed.contains(command.refname.as_str()))
        .collect();

    // A push that landed nothing is not news. Calling with an empty list would
    // ask an application to distinguish "nothing happened" from "something
    // did", for no reason: it can only ever be the first.
    if commands.is_empty() {
        return Vec::new();
    }

    match hooks.post_receive(repo, actor, &commands).await {
        Ok(messages) => messages,
        Err(error) => {
            tracing::warn!(%repo, %error, "post-receive hook could not be reached");
            Vec::new()
        }
    }
}
