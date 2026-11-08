//! Fetching into Enroute, from a git server that is really `git`.
//!
//! The far end is a bare repository served by `git upload-pack` over
//! protocol v2, so what is proved here is that Enroute reads what git itself
//! sends.

use std::path::{Path, PathBuf};

use enroute_api::api::v1alpha1::ref_fetch_outcome::Status;

use crate::contract::{Client, hex};
use crate::support::{Servers, Stack, git, spawn_git_remote};

use super::support::{status_of, tip};

const TRUNK: &str = "refs/heads/main";

/// An empty repository, a git server to fetch from, and a checkout that
/// pushes to that server.
struct Fixture {
    client: Client,
    stack: Stack,
    repo: String,
    url: String,
    remote: tempfile::TempDir,
    local: PathBuf,
    _work: tempfile::TempDir,
    _servers: Servers,
}

impl Fixture {
    async fn new() -> Self {
        let stack = Stack::start().await;
        let client = stack.client().await;
        let repo = stack.create("mirror").await;

        let remote = tempfile::tempdir().unwrap();
        let (address, servers) = spawn_git_remote(remote.path()).await;
        let work = tempfile::tempdir().unwrap();
        let local = work.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        git(&["init", "-b", "main"], Some(&local)).await;
        let fixture = Self {
            client,
            stack,
            repo,
            url: format!("http://{address}"),
            remote,
            local,
            _work: work,
            _servers: servers,
        };
        fixture.commit("first\n").await;
        fixture.commit("second\n").await;
        fixture.publish(&["main"]).await;
        fixture
    }

    async fn commit(&self, body: &str) {
        std::fs::write(self.local.join("a.txt"), body).unwrap();
        git(&["add", "."], Some(&self.local)).await;
        git(&["commit", "-m", body.trim()], Some(&self.local)).await;
    }

    /// Push refspecs straight into the served bare repository.
    async fn publish(&self, refspecs: &[&str]) {
        let mut args = vec!["push", "--force", path(self.remote.path())];
        args.extend_from_slice(refspecs);
        git(&args, Some(&self.local)).await;
    }

    async fn oid(&self, revision: &str) -> String {
        git(&["rev-parse", revision], Some(&self.local))
            .await
            .trim()
            .to_string()
    }

    async fn fetch(&self, prefixes: &[&str]) -> Vec<enroute_api::api::v1alpha1::RefFetchOutcome> {
        self.client
            .fetch_from_remote(&self.repo, &self.url, prefixes)
            .await
            .unwrap()
    }

    /// Where Enroute holds `refname`, as hex.
    async fn here(&self, refname: &str) -> Option<String> {
        tip(&self.client, &self.repo, refname).await
    }
}

fn path(dir: &Path) -> &str {
    dir.to_str().unwrap()
}

/// The whole point: the remote's branches and tags arrive, with every object
/// behind them, in a form git can clone back out.
#[tokio::test]
async fn a_remote_s_branches_and_tags_arrive_with_their_objects() {
    let fixture = Fixture::new().await;
    git(&["tag", "-a", "v1", "-m", "one"], Some(&fixture.local)).await;
    git(&["branch", "topic", "HEAD~1"], Some(&fixture.local)).await;
    fixture.publish(&["main", "topic", "v1"]).await;

    let outcomes = fixture.fetch(&["refs/heads/", "refs/tags/"]).await;

    let names: Vec<&str> = outcomes.iter().map(|o| o.name.as_str()).collect();
    assert_eq!(
        names,
        ["refs/heads/main", "refs/heads/topic", "refs/tags/v1"]
    );
    assert!(
        outcomes.iter().all(|o| o.status() == Status::Updated),
        "{outcomes:?}"
    );
    for (refname, revision) in [
        (TRUNK, "main"),
        ("refs/heads/topic", "topic"),
        ("refs/tags/v1", "v1"),
    ] {
        assert_eq!(
            fixture.here(refname).await,
            Some(fixture.oid(revision).await),
            "{refname}"
        );
    }

    let clone = tempfile::tempdir().unwrap();
    git(
        &[
            "clone",
            "--mirror",
            &fixture.stack.url("mirror"),
            path(clone.path()),
        ],
        None,
    )
    .await;
    git(&["fsck", "--strict"], Some(clone.path())).await;
}

/// A second fetch with nothing new changes nothing; one after a commit moves
/// the branch, with a pack cut down by what Enroute already holds.
#[tokio::test]
async fn a_later_fetch_brings_only_what_changed() {
    let fixture = Fixture::new().await;
    fixture.fetch(&["refs/heads/"]).await;

    assert!(fixture.fetch(&["refs/heads/"]).await.is_empty());

    fixture.commit("third\n").await;
    fixture.publish(&["main"]).await;
    let outcomes = fixture.fetch(&["refs/heads/"]).await;

    let outcome = outcomes.first().expect("one outcome");
    assert_eq!(outcome.status(), Status::Updated, "{outcome:?}");
    assert_eq!(
        hex(outcome.new_object_id.as_ref()),
        fixture.oid("HEAD").await
    );
    assert_eq!(fixture.here(TRUNK).await, Some(fixture.oid("HEAD").await));
}

/// A mirror follows a rewind and a delete, which is what makes it a mirror
/// rather than a fast-forward-only copy.
#[tokio::test]
async fn a_rewound_branch_rewinds_and_a_deleted_one_goes() {
    let fixture = Fixture::new().await;
    git(&["branch", "topic"], Some(&fixture.local)).await;
    fixture.publish(&["topic"]).await;
    fixture.fetch(&["refs/heads/"]).await;
    let previous = fixture.oid("HEAD~1").await;

    fixture.publish(&["HEAD~1:main", ":topic"]).await;
    let outcomes = fixture.fetch(&["refs/heads/"]).await;

    let statuses: Vec<(&str, Status)> = outcomes
        .iter()
        .map(|o| (o.name.as_str(), o.status()))
        .collect();
    assert_eq!(
        statuses,
        [
            (TRUNK, Status::Updated),
            ("refs/heads/topic", Status::Deleted)
        ]
    );
    assert_eq!(fixture.here(TRUNK).await, Some(previous));
    assert_eq!(fixture.here("refs/heads/topic").await, None);
}

/// Refs outside the prefixes are not the fetch's to change, here or there.
#[tokio::test]
async fn refs_outside_the_prefixes_are_left_alone() {
    let fixture = Fixture::new().await;
    git(&["tag", "v1"], Some(&fixture.local)).await;
    fixture.publish(&["v1"]).await;
    fixture.fetch(&["refs/heads/"]).await;

    let outcomes = fixture.fetch(&["refs/tags/"]).await;

    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(fixture.here(TRUNK).await, Some(fixture.oid("main").await));
}

/// A prefix outside `refs/` would let a fetch write `HEAD`.
#[tokio::test]
async fn a_prefix_outside_refs_is_refused() {
    let fixture = Fixture::new().await;

    let error = fixture
        .client
        .fetch_from_remote(&fixture.repo, &fixture.url, &["HEAD"])
        .await
        .expect_err("not under refs/");

    let status = status_of(&error);
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    assert_eq!(fixture.here(TRUNK).await, None);
}
