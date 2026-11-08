//! SHA-256 repositories: created through the API, served to git clients
//! with `object-format=sha256`, and kept apart from SHA-1 clients.

#![expect(
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{Git, Server, api, text};
use enroute_compute::RepoState;
use enroute_compute::testing::settled;

/// A source repository in SHA-256, at `name`.
async fn init_sha256(git: &Git, name: &str) -> PathBuf {
    git.init_with(name, &["--object-format=sha256"]).await
}

/// A server that takes SHA-256 repositories, merging layers early.
async fn server() -> Server {
    Server::start_with(|c| {
        c.ledger.allow_sha256 = true;
        c.compute.merge_factor = 2;
        c.compute.flush_interval = Duration::from_millis(200);
    })
    .await
}

/// Clone `url` (with extra `args`) into `name` and check it.
async fn clone(git: &Git, url: &str, name: &str, args: &[&str]) -> PathBuf {
    let dst = git.dir(name);
    let mut all = args.to_vec();
    all.extend(["clone", "-q", url, dst.to_str().unwrap()]);
    git.run(git.home.path(), &all).await;
    git.run(&dst, &["fsck", "--strict"]).await;
    let format = git.run(&dst, &["rev-parse", "--show-object-format"]).await;
    assert_eq!(format.trim(), "sha256", "{name}");
    dst
}

/// Whether git's or Enroute's refusal names the format mismatch.
fn names_the_format(err: &str) -> bool {
    ["object format", "hash algorithm", "mismatched algorithms"]
        .iter()
        .any(|m| err.contains(m))
}

async fn same(git: &Git, a: &Path, b: &Path, r: &str) {
    assert_eq!(git.rev(a, r).await, git.rev(b, r).await, "{r}");
}

#[tokio::test]
async fn a_sha256_create_is_refused_unless_the_ledger_allows_it() {
    let server = Server::start().await;
    let mut client = server.api_client();
    let err = client
        .create_repo_in("r", api::ObjectFormat::Sha256)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err}");
    let made = client
        .create_repo_in("s", api::ObjectFormat::Sha1)
        .await
        .unwrap();
    assert_eq!(made.object_format(), api::ObjectFormat::Sha1);
}

/// A push to a missing repository creates it in `default_object_format`.
///
/// So does a create that names no format, and a push in the other format
/// to a missing repository is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_format_names_what_a_push_creates() {
    let server = Server::start_with(|c| {
        c.ledger.allow_sha256 = true;
        c.compute.default_object_format = enroute_shared::oid::ObjectFormat::Sha256;
    })
    .await;
    let git = Git::new();
    let src = init_sha256(&git, "src").await;
    git.commit_file(&src, "a.txt", &text(1, 40), "one").await;
    let url = server.repo_url("pushed");
    git.run(&src, &["push", "-q", &url, "main"]).await;
    let full = clone(&git, &url, "full", &[]).await;
    same(&git, &src, &full, "main").await;

    let old = git.init("old").await;
    git.commit_file(&old, "a.txt", &text(2, 40), "one").await;
    let (ok, _, err) = git
        .try_run(&old, &["push", "-q", &server.repo_url("missing"), "main"])
        .await;
    assert!(!ok && names_the_format(&err), "{err}");

    let mut client = server.api_client();
    let made = client
        .create_repo_in("unnamed", api::ObjectFormat::Unspecified)
        .await
        .unwrap();
    assert_eq!(made.object_format(), api::ObjectFormat::Sha256);
}

/// Push, clone, fetch, shallow and v0 clones of a SHA-256 repository, over
/// merged layers too; SHA-1 clients are refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sha256_repositories_round_trip_through_git() {
    let server = server().await;
    let mut client = server.api_client();
    let made = client
        .create_repo_in("r", api::ObjectFormat::Sha256)
        .await
        .unwrap();
    assert_eq!(made.object_format(), api::ObjectFormat::Sha256);
    let url = server.repo_url("r");

    let git = Git::new();
    let src = init_sha256(&git, "src").await;
    // Growing files give deltas; a tag and a branch give other objects.
    for i in 0..12u64 {
        git.commit_file(
            &src,
            &format!("d{}/f{}.txt", i % 3, i % 4),
            &text(i % 4, 60 + 5 * usize::try_from(i).unwrap()),
            &format!("c{i}"),
        )
        .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
    }
    git.run(&src, &["tag", "-a", "v1", "-m", "a tag"]).await;
    git.run(&src, &["branch", "side", "HEAD~3"]).await;
    git.run(&src, &["push", "-q", &url, "v1", "side"]).await;

    let listed = git.run(&src, &["ls-remote", &url]).await;
    let head = git.rev(&src, "main").await;
    assert_eq!(head.len(), 64);
    assert!(
        listed.contains(&format!("{head}\trefs/heads/main")),
        "{listed}"
    );

    let full = clone(&git, &url, "full", &[]).await;
    same(&git, &src, &full, "main").await;
    same(&git, &src, &full, "v1").await;
    assert_eq!(
        git.rev(&src, "side").await,
        git.rev(&full, "origin/side").await
    );

    // Incremental fetch: a thin pack against what the clone has.
    git.commit_file(&src, "d0/f0.txt", &text(99, 80), "more")
        .await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    git.run(&full, &["fetch", "-q", "origin"]).await;
    git.run(&full, &["fsck", "--strict"]).await;
    assert_eq!(
        git.rev(&src, "main").await,
        git.rev(&full, "origin/main").await
    );

    let shallow = clone(&git, &url, "shallow", &[]).await;
    git.run(&shallow, &["fetch", "-q", "--depth=1", "origin"])
        .await;
    let shallow2 = git.dir("shallow2");
    git.run(
        git.home.path(),
        &["clone", "-q", "--depth=2", &url, shallow2.to_str().unwrap()],
    )
    .await;
    git.run(&shallow2, &["fsck"]).await;
    same(&git, &src, &shallow2, "main").await;

    let v0 = clone(&git, &url, "v0", &["-c", "protocol.version=0"]).await;
    same(&git, &src, &v0, "main").await;

    // After compaction merges the layers, a clone still matches.
    let done = || {
        let meta: RepoState = server
            .node
            .consensus()
            .unwrap()
            .repo_meta("r")
            .unwrap()
            .into();
        settled(&meta, server.node.compute().unwrap().config()) && meta.layers.len() == 1
    };
    assert!(
        common::wait_until(done, 20).await,
        "expected the layers merged into one"
    );
    let merged = clone(&git, &url, "merged", &[]).await;
    same(&git, &src, &merged, "main").await;
    same(&git, &src, &merged, "v1").await;

    // A SHA-1 repository cannot push into it, nor fetch from it.
    let sha1 = git.init("sha1").await;
    git.commit_file(&sha1, "a", "a\n", "a").await;
    let (ok, _, err) = git.try_run(&sha1, &["push", &url, "main:other"]).await;
    assert!(!ok, "a SHA-1 push was taken");
    assert!(names_the_format(&err), "{err}");
    let (ok, _, err) = git.try_run(&sha1, &["fetch", &url, "main"]).await;
    assert!(!ok, "a SHA-1 fetch was served");
    assert!(names_the_format(&err), "{err}");
}

/// A SHA-1 repository refuses a SHA-256 client.
#[tokio::test]
async fn sha1_repositories_refuse_sha256_clients() {
    let server = server().await;
    let url = server.repo_url("one");
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "a", "a\n", "a").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;

    let other = init_sha256(&git, "other").await;
    git.commit_file(&other, "b", "b\n", "b").await;
    let (ok, _, err) = git.try_run(&other, &["push", &url, "main:x"]).await;
    assert!(!ok, "a SHA-256 push was taken");
    assert!(names_the_format(&err), "{err}");
    let (ok, _, err) = git.try_run(&other, &["fetch", &url, "main"]).await;
    assert!(!ok, "a SHA-256 fetch was served");
    assert!(names_the_format(&err), "{err}");
}
