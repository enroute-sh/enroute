//! Which refs a caller may be told about, for a repository whose visibility
//! rules live in another process.
//!
//! Every advertisement passes through here: the refs Enroute would send go to
//! an application, and what comes back is what a client sees. A list rather
//! than a pattern, because a pattern would only be the subset of policies
//! somebody thought to spell.
//!
//! # What it does not do
//!
//! It hides refs, not objects — a client holding a hidden tip's object id may
//! still fetch it, as with git's own `uploadpack.hideRefs`.

use std::collections::HashSet;

use async_trait::async_trait;

use enroute_git_core::{Error, RepoId};
use enroute_git_ingest::Actor;
use enroute_git_retrieve::{RefsMap, RepoMetadata, Storage};

/// What a request would do to the repository.
///
/// One distinction, asked twice: whether the caller may reach the repository
/// at all, and which of its refs that caller is told about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Fetching, and the `ls-refs`/v0 advertisements that precede it.
    Read,
    /// Pushing, and the ref advertisement `receive-pack` precedes itself with.
    Write,
}

/// Decides which refs a caller may see, supplied by the binary — the only layer
/// that knows an application exists.
#[async_trait]
pub trait RefVisibility: Send + Sync + std::fmt::Debug {
    /// The subset of `refs` this caller may be told about.
    ///
    /// `HEAD` is never asked about: it names no ref of its own, and
    /// [`hide_refs`] hides it with the branch it points at.
    ///
    /// # Errors
    ///
    /// Returns an error if the application could not be reached or would not
    /// answer, rather than advertise refs nobody cleared.
    async fn visible_refs(
        &self,
        repo: RepoId,
        actor: &Actor,
        access: Access,
        refs: &[&str],
    ) -> Result<Vec<String>, Error>;
}

/// Visibility that hides nothing, for tests whose subject is not policy.
///
/// The equivalent of an application with no rules.
#[derive(Debug, Clone, Copy)]
pub struct AllRefsVisible;

#[async_trait]
impl RefVisibility for AllRefsVisible {
    async fn visible_refs(
        &self,
        _repo: RepoId,
        _actor: &Actor,
        _access: Access,
        refs: &[&str],
    ) -> Result<Vec<String>, Error> {
        Ok(refs.iter().map(|name| (*name).to_string()).collect())
    }
}

/// The refs an advertisement starts from: everything `repo` holds, less
/// whatever `visibility` will not admit to.
///
/// The one way to read refs for a client, so a transport added later cannot
/// advertise what a forge hid by forgetting to ask.
///
/// # Errors
///
/// Returns an error if the store is unavailable or the hook would not answer.
pub async fn advertised_refs(
    state: &Storage,
    repo: &RepoMetadata,
    visibility: &dyn RefVisibility,
    actor: &Actor,
    access: Access,
) -> Result<RefsMap, Error> {
    let refs = state.rows.repo(repo.id).refs_for(repo).await?;
    hide_refs(visibility, repo.id, actor, access, refs).await
}

/// Ask `visibility` which of `refs` this caller may see, and drop the rest.
///
/// `HEAD` goes with the branch it names, since a symref target names the very
/// branch a rule was hiding. One that names no branch yet stays.
///
/// # Errors
///
/// Returns an error if the visibility hook would not answer.
pub(crate) async fn hide_refs(
    visibility: &dyn RefVisibility,
    repo: RepoId,
    actor: &Actor,
    access: Access,
    refs: RefsMap,
) -> Result<RefsMap, Error> {
    let named: Vec<&str> = refs
        .keys()
        .filter(|name| *name != "HEAD")
        .map(String::as_str)
        .collect();
    // A repository with nothing to hide is not worth a round trip, and an
    // empty one is the first thing every fresh clone asks about.
    if named.is_empty() {
        return Ok(refs);
    }
    let visible: HashSet<String> = visibility
        .visible_refs(repo, actor, access, &named)
        .await?
        .into_iter()
        .collect();

    // Hidden only when its target is a ref the application saw and left out: an
    // unborn `HEAD` names a branch that does not exist yet, and dropping it
    // would land a clone of an empty repository on `init.defaultBranch`. Read
    // here because the filter below has consumed the map by then.
    let hide_head = refs
        .get("HEAD")
        .and_then(|value| value.strip_prefix("ref: "))
        .is_some_and(|target| refs.contains_key(target) && !visible.contains(target));

    // No intersection to take: this filters the refs that exist, so an
    // application answering with one it was never asked about has not made it
    // exist.
    Ok(refs
        .into_iter()
        .filter(|(name, _)| match name.as_str() {
            "HEAD" => !hide_head,
            name => visible.contains(name),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{Access, Actor, AllRefsVisible, RefVisibility, hide_refs};
    use enroute_git_core::{Error, RepoId};
    use enroute_git_retrieve::RefsMap;

    const OID: &str = "1111111111111111111111111111111111111111";

    /// Answers with whatever it was built with, and records what it was asked.
    #[derive(Debug)]
    struct Answers {
        with: Vec<String>,
        asked: Mutex<Vec<String>>,
    }

    impl Answers {
        fn new(with: &[&str]) -> Self {
            Self {
                with: with.iter().map(|name| (*name).to_string()).collect(),
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl RefVisibility for Answers {
        async fn visible_refs(
            &self,
            _repo: RepoId,
            _actor: &Actor,
            _access: Access,
            refs: &[&str],
        ) -> Result<Vec<String>, Error> {
            *self.asked.lock().unwrap() = refs.iter().map(|name| (*name).to_string()).collect();
            Ok(self.with.clone())
        }
    }

    fn refs(pairs: &[(&str, &str)]) -> RefsMap {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect()
    }

    async fn hidden_by(visibility: &dyn RefVisibility, map: RefsMap) -> Vec<String> {
        hide_refs(
            visibility,
            RepoId::new(1),
            &Actor::new("alice"),
            Access::Read,
            map,
        )
        .await
        .unwrap()
        .into_keys()
        .collect()
    }

    #[tokio::test]
    async fn a_ref_the_application_left_out_is_dropped() {
        let visibility = Answers::new(&["refs/heads/main"]);
        let left = hidden_by(
            &visibility,
            refs(&[("refs/heads/main", OID), ("refs/heads/secret", OID)]),
        )
        .await;
        assert_eq!(left, ["refs/heads/main"]);
    }

    /// A symref target names the very branch a rule was hiding, so `HEAD` has
    /// to go with it.
    #[tokio::test]
    async fn head_goes_with_the_branch_it_points_at() {
        let visibility = Answers::new(&["refs/heads/other"]);
        let left = hidden_by(
            &visibility,
            refs(&[
                ("HEAD", "ref: refs/heads/main"),
                ("refs/heads/main", OID),
                ("refs/heads/other", OID),
            ]),
        )
        .await;
        assert_eq!(left, ["refs/heads/other"]);
    }

    #[tokio::test]
    async fn head_stays_while_its_branch_does() {
        let visibility = Answers::new(&["refs/heads/main"]);
        let left = hidden_by(
            &visibility,
            refs(&[("HEAD", "ref: refs/heads/main"), ("refs/heads/main", OID)]),
        )
        .await;
        assert_eq!(left, ["HEAD", "refs/heads/main"]);
    }

    /// The hook answers which refs may be seen, not which exist — so a name it
    /// invents is not one.
    #[tokio::test]
    async fn a_ref_that_was_never_asked_about_is_ignored() {
        let visibility = Answers::new(&["refs/heads/main", "refs/heads/invented"]);
        let left = hidden_by(&visibility, refs(&[("refs/heads/main", OID)])).await;
        assert_eq!(left, ["refs/heads/main"]);
    }

    /// An empty repository advertises `HEAD` alone, and a clone lands on the
    /// branch it names.
    ///
    /// Dropping it because that branch is not there yet would leave the clone
    /// on whatever `init.defaultBranch` says.
    #[tokio::test]
    async fn an_unborn_head_survives() {
        let visibility = Answers::new(&["refs/heads/other"]);
        let left = hidden_by(
            &visibility,
            refs(&[("HEAD", "ref: refs/heads/main"), ("refs/heads/other", OID)]),
        )
        .await;
        assert_eq!(left, ["HEAD", "refs/heads/other"]);
    }

    /// A repository with no branches is never put to the application: there is
    /// nothing to hide, and every fresh clone starts by asking.
    #[tokio::test]
    async fn an_empty_repository_asks_nobody() {
        let visibility = Answers::new(&[]);
        let left = hidden_by(&visibility, refs(&[("HEAD", "ref: refs/heads/main")])).await;
        assert_eq!(left, ["HEAD"]);
        assert!(visibility.asked.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn head_is_never_put_to_the_hook() {
        let visibility = Answers::new(&["refs/heads/main"]);
        hidden_by(
            &visibility,
            refs(&[("HEAD", "ref: refs/heads/main"), ("refs/heads/main", OID)]),
        )
        .await;
        assert_eq!(*visibility.asked.lock().unwrap(), ["refs/heads/main"]);
    }

    #[tokio::test]
    async fn visibility_with_no_rules_hides_nothing() {
        let map = refs(&[("HEAD", "ref: refs/heads/main"), ("refs/heads/main", OID)]);
        let left = hidden_by(&AllRefsVisible, map).await;
        assert_eq!(left, ["HEAD", "refs/heads/main"]);
    }
}
