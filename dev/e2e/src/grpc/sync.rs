//! Pushing out of Enroute, into a git server that is really `git`.
//!
//! The far end is a bare repository served by `git receive-pack`, so what is
//! proved here is that git itself reads the pack and the commands Enroute
//! sends. A stub would only agree with whatever was sent to it.

use std::path::PathBuf;

use enroute_api::api::v1alpha1::{RefSpec, ref_push_outcome::Status};

use crate::contract::{Client, hex};
use crate::support::{Servers, git, remote_ref, spawn_git_remote};

use super::support::{pushed_repo_serving, status_of};

const TRUNK: &str = "refs/heads/main";

/// A repository holding two commits, and a git server to push it at.
struct Fixture {
    client: Client,
    repo: String,
    /// Where the remote is, as a caller would name it.
    url: String,
    remote: tempfile::TempDir,
    /// A checkout of the repository, to read oids out of.
    local: PathBuf,
    _work: tempfile::TempDir,
    _servers: Servers,
}

impl Fixture {
    async fn new() -> Self {
        let (client, repo, work, local, servers) = pushed_repo_serving("first\n").await;
        // A second commit, so a test has a history to rewind rather than one
        // commit and nothing behind it.
        std::fs::write(local.join("a.txt"), "second\n").unwrap();
        git(&["commit", "-am", "second"], Some(&local)).await;
        git(&["push", "origin", "main"], Some(&local)).await;

        let remote = tempfile::tempdir().unwrap();
        let (address, remote_servers) = spawn_git_remote(remote.path()).await;
        Self {
            client,
            repo,
            url: format!("http://{address}"),
            remote,
            local,
            _work: work,
            _servers: servers.and(remote_servers),
        }
    }

    /// An oid in the checkout, e.g. `HEAD` or `HEAD~1`.
    async fn oid(&self, revision: &str) -> String {
        git(&["rev-parse", revision], Some(&self.local))
            .await
            .trim()
            .to_string()
    }

    /// Where the remote holds `refname`, if it holds it at all.
    async fn there(&self, refname: &str) -> Option<String> {
        remote_ref(self.remote.path(), refname).await
    }

    async fn push(&self, refs: Vec<RefSpec>) -> Vec<enroute_api::api::v1alpha1::RefPushOutcome> {
        self.client
            .push_to_remote(&self.repo, &self.url, refs)
            .await
            .unwrap()
    }
}

/// One refspec, in the form most pushes take.
fn spec(source: &str, destination: &str, force: bool) -> Vec<RefSpec> {
    vec![RefSpec {
        source: source.into(),
        destination: destination.into(),
        force,
    }]
}

/// A redirect is refused rather than followed: the caller's credentials are
/// already on the request, and the hop is a URL they did not name.
#[tokio::test]
async fn a_remote_that_redirects_is_refused() {
    let fixture = Fixture::new().await;

    let failure = fixture
        .client
        .push_to_remote(
            &fixture.repo,
            &format!("{}/moved", fixture.url),
            spec(TRUNK, TRUNK, false),
        )
        .await
        .expect_err("a redirect is not followed");

    let status = status_of(&failure);
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
    assert!(status.message().contains("redirects"), "{status:?}");
    assert_eq!(fixture.there(TRUNK).await, None);
}

/// The whole point: a branch here reaches a git server there, with the
/// objects behind it.
#[tokio::test]
async fn a_branch_and_its_objects_reach_the_remote() {
    let fixture = Fixture::new().await;
    let head = fixture.oid("HEAD").await;

    let outcomes = fixture.push(spec(TRUNK, TRUNK, false)).await;

    let outcome = outcomes.first().expect("one refspec, one outcome");
    assert_eq!(outcome.status(), Status::Updated, "{outcome:?}");
    assert_eq!(hex(outcome.new_object_id.as_ref()), head);
    assert_eq!(fixture.there(TRUNK).await.as_deref(), Some(head.as_str()));
    // The remote holds the objects and not only the ref: `fsck` walks what
    // the pack Enroute built actually carried.
    git(&["fsck", "--strict"], Some(fixture.remote.path())).await;
}

/// A push onto a remote that already holds history sends a pack cut down by
/// what it advertised, and git must still be able to walk the result.
#[tokio::test]
async fn a_later_push_carries_only_what_the_remote_lacks() {
    let fixture = Fixture::new().await;
    fixture.push(spec(TRUNK, TRUNK, false)).await;

    std::fs::write(fixture.local.join("a.txt"), "third\n").unwrap();
    git(&["commit", "-am", "third"], Some(&fixture.local)).await;
    git(&["push", "origin", "main"], Some(&fixture.local)).await;
    let third = fixture.oid("HEAD").await;

    let outcomes = fixture.push(spec(TRUNK, TRUNK, false)).await;

    assert_eq!(
        outcomes.first().expect("one outcome").status(),
        Status::Updated
    );
    assert_eq!(fixture.there(TRUNK).await.as_deref(), Some(third.as_str()));
    // Every object the second pack referred to has to be there, whether it
    // came in this pack or the first one.
    git(&["fsck", "--strict"], Some(fixture.remote.path())).await;
}

/// A ref the remote already holds is answered without sending anything.
#[tokio::test]
async fn a_second_push_of_the_same_ref_is_already_there() {
    let fixture = Fixture::new().await;

    fixture.push(spec(TRUNK, TRUNK, false)).await;
    let outcomes = fixture.push(spec(TRUNK, TRUNK, false)).await;

    assert_eq!(
        outcomes.first().expect("one outcome").status(),
        Status::UpToDate
    );
}

/// A source may land under another name, which is what mirroring one branch
/// onto another is — and an empty source takes it away again.
#[tokio::test]
async fn a_ref_can_be_pushed_under_another_name_and_then_deleted() {
    let fixture = Fixture::new().await;
    let head = fixture.oid("HEAD").await;
    let mirror = "refs/heads/mirror";

    let outcomes = fixture.push(spec(TRUNK, mirror, false)).await;
    assert_eq!(
        outcomes.first().expect("one outcome").status(),
        Status::Updated
    );
    assert_eq!(fixture.there(mirror).await.as_deref(), Some(head.as_str()));

    let outcomes = fixture.push(spec("", mirror, false)).await;
    assert_eq!(
        outcomes.first().expect("one outcome").status(),
        Status::Deleted
    );
    assert_eq!(fixture.there(mirror).await, None);
}

/// Git's wire protocol carries no force bit, so the refusal is Enroute's
/// own: a push that would drop what the remote holds does not go out.
#[tokio::test]
async fn a_push_that_would_lose_the_remote_s_history_needs_force() {
    let fixture = Fixture::new().await;
    let head = fixture.oid("HEAD").await;
    let previous = fixture.oid("HEAD~1").await;
    let rewound = "refs/heads/rewound";

    fixture.push(spec(TRUNK, TRUNK, false)).await;
    // A branch at the commit before the remote's tip: what a rewind looks
    // like from the remote's side.
    git(
        &["push", "origin", &format!("HEAD~1:{rewound}")],
        Some(&fixture.local),
    )
    .await;

    let refused = fixture.push(spec(rewound, TRUNK, false)).await;
    let refused = refused.first().expect("one outcome");
    assert_eq!(refused.status(), Status::Rejected, "{refused:?}");
    assert!(refused.message.contains("fast-forward"), "{refused:?}");
    assert_eq!(
        fixture.there(TRUNK).await.as_deref(),
        Some(head.as_str()),
        "a refused push moved the remote anyway"
    );

    let forced = fixture.push(spec(rewound, TRUNK, true)).await;
    assert_eq!(
        forced.first().expect("one outcome").status(),
        Status::Updated
    );
    assert_eq!(
        fixture.there(TRUNK).await.as_deref(),
        Some(previous.as_str())
    );
}

/// A refspec naming a ref this repository does not hold is refused here,
/// with nothing sent.
#[tokio::test]
async fn a_ref_this_repository_does_not_hold_is_refused() {
    let fixture = Fixture::new().await;

    let outcomes = fixture
        .push(spec("refs/heads/nowhere", "refs/heads/nowhere", false))
        .await;

    let outcome = outcomes.first().expect("one outcome");
    assert_eq!(outcome.status(), Status::Rejected, "{outcome:?}");
    assert_eq!(fixture.there("refs/heads/nowhere").await, None);
}

/// A refspec that is not a fully qualified refname is a caller's mistake,
/// answered as one rather than guessed at.
#[tokio::test]
async fn a_refspec_that_is_not_fully_qualified_is_refused() {
    let fixture = Fixture::new().await;

    let error = fixture
        .client
        .push_to_remote(&fixture.repo, &fixture.url, spec("main", "", false))
        .await
        .expect_err("not a refname");

    assert_eq!(status_of(&error).code(), tonic::Code::InvalidArgument);
}
