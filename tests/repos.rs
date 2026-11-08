//! A repository's life, on one node and on a cluster.
//!
//! Explicit creation with a default branch, listing, the last push, and
//! deletion with the name free again at once (a new incarnation).

#![expect(
    clippy::excessive_nesting,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use common::{Git, Server, clone_matches, cluster, local_store, push_commits, text, wait_until};
use enroute::node::Config;
use enroute_shared::state::{Commit, RefUpdate};

/// The ids of `repo`'s layer files in the (file) object store under `root`.
fn files(root: &Path, repo: &str) -> HashSet<String> {
    let dir = root.join("store/data/repos").join(repo).join("layers");
    std::fs::read_dir(dir)
        .map(|d| {
            d.flatten()
                .filter_map(|e| {
                    e.file_name()
                        .to_str()?
                        .strip_suffix(".layer")
                        .map(String::from)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Whether `url` serves an empty repository whose HEAD names `head`: a
/// clone works, has no commit, and takes the unborn HEAD.
async fn is_empty_with_head(git: &Git, url: &str, head: &str, name: &str) -> bool {
    let dst = git.dir(name);
    let (ok, _, err) = git
        .try_run(git.home.path(), &["clone", url, dst.to_str().unwrap()])
        .await;
    assert!(ok, "clone of {url}: {err}");
    let (born, _, _) = git.try_run(&dst, &["rev-parse", "--verify", "HEAD"]).await;
    let local_head = git.run(&dst, &["symbolic-ref", "HEAD"]).await;
    !born && local_head.trim() == head
}

/// Whether git fails to clone `url`, saying the repository is not there.
async fn clone_fails(git: &Git, url: &str, name: &str) -> bool {
    let dst = git.dir(name);
    let (ok, out, err) = git
        .try_run(git.home.path(), &["clone", url, dst.to_str().unwrap()])
        .await;
    !ok && format!("{out}{err}").contains("not found")
}

/// Push `src`'s main to `url` as `branch`; whether git accepted it.
async fn push(git: &Git, src: &Path, url: &str, branch: &str) -> (bool, String) {
    let (ok, out, err) = git
        .try_run(src, &["push", url, &format!("main:refs/heads/{branch}")])
        .await;
    (ok, format!("{out}{err}"))
}

/// Explicit creation: HEAD names the default branch while it is unborn.
///
/// Creating again changes nothing; the last push time follows pushes that
/// move a ref, not refused ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn created_with_a_default_branch() {
    let server = Server::start().await;
    let compute = server.node.compute().unwrap();
    let git = Git::new();
    let r = compute.create_repo("r", "develop").await.unwrap();
    assert_eq!((r.head.as_str(), r.last_push), ("refs/heads/develop", None));
    let again = compute.create_repo("r", "refs/heads/other").await.unwrap();
    assert_eq!(again, r, "creating again changes nothing");
    let url = server.repo_url("r");
    assert!(is_empty_with_head(&git, &url, "refs/heads/develop", "empty").await);

    let src = git.init("src").await;
    git.commit_file(&src, "a", &text(1, 10), "c1").await;
    assert!(push(&git, &src, &url, "feature").await.0);
    let r = compute.get_repo("r").await.unwrap().unwrap();
    assert_eq!(r.head, "refs/heads/develop", "HEAD stays unborn");
    let first = r.last_push.expect("a push moved a ref");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(push(&git, &src, &url, "develop").await.0);
    let r = compute.get_repo("r").await.unwrap().unwrap();
    let second = r.last_push.unwrap();
    assert!(second > first);
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, dst.to_str().unwrap()],
    )
    .await;
    assert_eq!(
        git.run(&dst, &["symbolic-ref", "HEAD"]).await.trim(),
        "refs/heads/develop"
    );
    assert_eq!(git.rev(&dst, "HEAD").await, git.rev(&src, "main").await);

    // A refused push (a stale compare-and-swap) leaves the time.
    let epoch = compute.state("r").unwrap().epoch;
    let stale = enroute_shared::oid::Oid::sha1([9; 20]);
    let out = compute
        .commit(
            "r",
            epoch,
            None,
            Commit {
                updates: vec![RefUpdate {
                    name: "refs/heads/develop".into(),
                    old: Some(stale),
                    new: Some(stale),
                }],
                pushed_at_ms: u64::MAX / 2,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .outcome;
    assert!(out.ref_results[0].error.is_some());
    assert_eq!(
        compute.get_repo("r").await.unwrap().unwrap().last_push,
        Some(second)
    );

    // Invalid names and branches.
    compute.create_repo("a b", "main").await.unwrap_err();
    compute.create_repo("ok", "bad..branch").await.unwrap_err();
    assert!(compute.get_repo("missing").await.unwrap().is_none());
}

/// A push or an API commit with an object over `max_object_bytes` fails with
/// the reason, and stores nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objects_over_the_limit_are_refused() {
    let server = Server::start_with(|c| c.compute.max_object_bytes = 1000).await;
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "small", "a\n", "c1").await;
    let url = server.repo_url("limited");
    assert!(push(&git, &src, &url, "main").await.0);
    git.commit_file(&src, "big", &"x".repeat(1001), "c2").await;
    let (ok, out) = push(&git, &src, &url, "main").await;
    assert!(!ok && out.contains("over the limit"), "{out}");
    let e = server
        .api_client()
        .create_commit(common::commit(
            "limited",
            "main",
            "c",
            vec![common::put("big", "x".repeat(1001))],
        ))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
}

/// Without `create_on_push`, a push, a clone or an API commit to a missing
/// repository is not found, and creates nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushes_do_not_create_unless_configured() {
    let root = tempfile::tempdir().unwrap();
    let server = Server::start_config({
        let mut c = Config::local(root.path().join("data"), local_store(root.path()));
        c.compute.create_on_push = false;
        c
    })
    .await;
    let compute = server.node.compute().unwrap();
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "a", "a\n", "c1").await;
    let url = server.repo_url("nope");
    let (ok, out) = push(&git, &src, &url, "main").await;
    assert!(!ok && out.contains("not found"), "{out}");
    assert!(clone_fails(&git, &url, "dst").await);
    let e = server
        .api_client()
        .create_commit(common::commit(
            "nope",
            "main",
            "c",
            vec![common::put("f", "x")],
        ))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound, "{e}");
    assert!(compute.get_repo("nope").await.unwrap().is_none());
    assert!(
        compute
            .list_repos("", "", 100)
            .await
            .unwrap()
            .repos
            .is_empty()
    );
    assert!(!root.path().join("store/ledger/repos/nope").exists());
    assert!(!root.path().join("store/data/repos/nope").exists());

    compute.create_repo("nope", "").await.unwrap();
    assert!(push(&git, &src, &url, "main").await.0);
    clone_matches(&git, &src, &url, "after").await;
}

/// Listing: byte order, a prefix, pages after a name, deleted repositories
/// left out, a new one in at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repositories_are_listed() {
    let server = Server::start().await;
    let compute = server.node.compute().unwrap();
    for name in ["b", "a", "abc", "ab", "a.x", "B"] {
        compute.create_repo(name, "main").await.unwrap();
    }
    compute.delete_repo("ab").await.unwrap();
    let names = |page: &enroute_compute::RepoPage| {
        page.repos
            .iter()
            .map(|r| r.name.clone())
            .collect::<Vec<_>>()
    };
    let all = compute.list_repos("", "", 100).await.unwrap();
    assert_eq!(names(&all), ["B", "a", "a.x", "abc", "b"]);
    assert_eq!(all.next, None);

    // Pages of one under "a": a deleted name takes a place.
    let (mut seen, mut after, mut pages) = (Vec::new(), String::new(), 0);
    loop {
        let page = compute.list_repos("a", &after, 1).await.unwrap();
        pages += 1;
        seen.extend(names(&page));
        match page.next {
            Some(n) => after = n,
            None => break,
        }
    }
    assert_eq!(seen, ["a", "a.x", "abc"]);
    assert!(pages >= 4, "{pages} pages");
    let page = compute.list_repos("", "a.x", 2).await.unwrap();
    assert_eq!(names(&page), ["abc"], "ab is deleted");
    assert_eq!(page.next.as_deref(), Some("abc"));

    compute.create_repo("ab", "main").await.unwrap();
    compute.create_repo("new", "main").await.unwrap();
    let all = compute.list_repos("", "", 100).await.unwrap();
    assert_eq!(names(&all), ["B", "a", "a.x", "ab", "abc", "b", "new"]);
}

fn cluster_config(_: usize, c: &mut Config) {
    c.compute.create_on_push = false;
    c.compute.flush_interval = Duration::from_millis(200);
}

async fn start(root: &Path, i: usize, addr: std::net::SocketAddr) -> Server {
    let mut c = Config::local(root.join(format!("n{i}")), local_store(root));
    c.ledger.advertise = Some(format!("http://{addr}"));
    cluster_config(i, &mut c);
    Server::start_on(c, common::rebind(addr).await).await
}

/// Deleting through one node of three frees the name at once on all of them.
///
/// Created again, the repository is new and empty everywhere, and listings
/// through any node agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deleted_and_created_again_on_a_cluster() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, cluster_config).await;
    let compute = |i: usize| servers[i].node.compute().unwrap();
    let git = Git::new();
    let src = git.init("src").await;
    compute(0).create_repo("r", "main").await.unwrap();
    for s in &servers {
        let names: Vec<_> = s
            .node
            .compute()
            .unwrap()
            .list_repos("", "", 10)
            .await
            .unwrap()
            .repos;
        assert_eq!(names.len(), 1, "listed at once through every node");
    }
    push_commits(&git, &src, &servers[0].repo_url("r"), 0, 3).await;
    clone_matches(&git, &src, &servers[1].repo_url("r"), "before").await;
    assert!(compute(1).state("r").is_some(), "node 1 follows it");

    compute(2).delete_repo("r").await.unwrap();
    compute(2).delete_repo("r").await.unwrap();
    compute(2).delete_repo("never").await.unwrap();
    for (i, s) in servers.iter().enumerate() {
        assert!(
            s.node
                .compute()
                .unwrap()
                .get_repo("r")
                .await
                .unwrap()
                .is_none()
        );
        let listed = s
            .node
            .compute()
            .unwrap()
            .list_repos("", "", 10)
            .await
            .unwrap()
            .repos;
        assert!(listed.is_empty(), "node {i} lists {listed:?}");
    }
    git.commit_file(&src, "x", "x\n", "after delete").await;
    for (i, s) in servers.iter().enumerate() {
        let (ok, out) = push(&git, &src, &s.repo_url("r"), "main").await;
        assert!(!ok, "node {i} took a push to a deleted repository: {out}");
    }
    let gone = || compute(1).state("r").is_none();
    assert!(wait_until(gone, 10).await, "node 1 still follows it");
    assert!(clone_fails(&git, &servers[1].repo_url("r"), "deleted").await);

    let r = compute(1).create_repo("r", "trunk").await.unwrap();
    assert_eq!((r.head.as_str(), r.last_push), ("refs/heads/trunk", None));
    for (i, s) in servers.iter().enumerate() {
        let r = s
            .node
            .compute()
            .unwrap()
            .get_repo("r")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.head, "refs/heads/trunk", "node {i}");
        let url = s.repo_url("r");
        let mut empty = false;
        for attempt in 0..30 {
            if is_empty_with_head(&git, &url, "refs/heads/trunk", &format!("e{i}-{attempt}")).await
            {
                empty = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(empty, "node {i} serves the old repository");
    }
    let fresh = git.init("fresh").await;
    git.commit_file(&fresh, "n", "new\n", "new").await;
    assert!(
        push(&git, &fresh, &servers[1].repo_url("r"), "trunk")
            .await
            .0
    );
    let want = git.rev(&fresh, "main").await;
    for i in [0, 2] {
        // Fetches have no read barrier: a compute may lag a little.
        let url = servers[i].repo_url("r");
        let caught_up = async || git.run(&fresh, &["ls-remote", &url]).await.contains(&want);
        let mut ok = false;
        for _ in 0..50 {
            if caught_up().await {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(ok, "node {i} does not serve the new push");
        clone_matches(&git, &fresh, &url, &format!("new{i}")).await;
    }
    let epochs: Vec<_> = servers
        .iter()
        .map(|s| s.node.consensus().unwrap().hosted("r").map(|h| h.epoch))
        .collect();
    assert!(epochs.contains(&Some(2)), "{epochs:?}");
}

/// A replica that was down during a deletion never takes writes for the old one.
///
/// Any sequencer has the tombstone, and at startup the replica drops its copy
/// once the state object says deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_lagging_replica_never_serves_the_deleted_repository() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, cluster_config).await;
    let mut servers: Vec<Option<Server>> = servers.into_iter().map(Some).collect();
    let git = Git::new();
    let src = git.init("src").await;
    let s0 = |s: &[Option<Server>]| s[0].as_ref().unwrap().node.compute().unwrap().clone();
    s0(&servers).create_repo("r", "main").await.unwrap();
    push_commits(
        &git,
        &src,
        &servers[0].as_ref().unwrap().repo_url("r"),
        0,
        2,
    )
    .await;
    let hosted = |s: &Option<Server>| {
        s.as_ref()
            .unwrap()
            .node
            .consensus()
            .unwrap()
            .hosted("r")
            .is_some()
    };
    assert!(wait_until(|| servers.iter().all(hosted), 20).await);

    // Node 2 goes down; the repository is deleted meanwhile.
    let down = servers[2].take().unwrap();
    let addr = down.internal;
    down.node.shutdown().await;
    drop(down);
    s0(&servers).delete_repo("r").await.unwrap();

    servers[2] = Some(start(root.path(), 2, addr).await);
    let lagging = servers[2].as_ref().unwrap();
    git.commit_file(&src, "x", "x\n", "late").await;
    let (ok, out) = push(&git, &src, &lagging.repo_url("r"), "main").await;
    assert!(!ok, "the lagging node took a push: {out}");
    let engine = lagging.node.consensus().unwrap();
    let dropped = || engine.hosted("r").is_none_or(|h| h.deleted);
    assert!(
        wait_until(dropped, 10).await,
        "it still hosts the old incarnation live"
    );
    let (ok, _) = push(&git, &src, &lagging.repo_url("r"), "main").await;
    assert!(!ok);
    assert!(
        lagging
            .node
            .compute()
            .unwrap()
            .get_repo("r")
            .await
            .unwrap()
            .is_none()
    );
    for s in servers.iter().flatten() {
        assert!(
            s.node
                .compute()
                .unwrap()
                .get_repo("r")
                .await
                .unwrap()
                .is_none()
        );
    }
}

/// Restarting every node keeps a deletion, and then a new incarnation.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deletions_and_new_incarnations_survive_restarts() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, cluster_config).await;
    let mut servers: Vec<Option<Server>> = servers.into_iter().map(Some).collect();
    let git = Git::new();
    let src = git.init("src").await;
    let compute =
        |s: &[Option<Server>], i: usize| s[i].as_ref().unwrap().node.compute().unwrap().clone();
    let url = |s: &[Option<Server>], i: usize| s[i].as_ref().unwrap().repo_url("r");
    compute(&servers, 0).create_repo("r", "main").await.unwrap();
    push_commits(&git, &src, &url(&servers, 0), 0, 2).await;
    compute(&servers, 1).delete_repo("r").await.unwrap();

    let restart_all = async |servers: &mut Vec<Option<Server>>| {
        let addrs: Vec<_> = servers
            .iter()
            .map(|s| s.as_ref().unwrap().internal)
            .collect();
        for s in servers.iter_mut() {
            let s = s.take().unwrap();
            s.node.shutdown().await;
        }
        for (i, addr) in addrs.into_iter().enumerate() {
            servers[i] = Some(start(root.path(), i, addr).await);
        }
    };
    restart_all(&mut servers).await;
    for i in 0..3 {
        assert!(compute(&servers, i).get_repo("r").await.unwrap().is_none());
        assert!(clone_fails(&git, &url(&servers, i), &format!("gone{i}")).await);
    }

    compute(&servers, 2).create_repo("r", "main").await.unwrap();
    let fresh = git.init("fresh").await;
    push_commits(&git, &fresh, &url(&servers, 2), 10, 2).await;
    restart_all(&mut servers).await;
    for i in 0..3 {
        // A read barrier first: a restarted follower learns the commit index
        // only from a sequencer, and fetches take none.
        assert!(compute(&servers, i).get_repo("r").await.unwrap().is_some());
        clone_matches(&git, &fresh, &url(&servers, i), &format!("back{i}")).await;
    }
}

/// The layer files of a deleted incarnation are reclaimed after the retention.
///
/// The trash marker reclaims them while the repository stays deleted, and the
/// layer sweep too once it is created again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleted_layer_files_are_reclaimed() {
    let root = tempfile::tempdir().unwrap();
    let mut c = Config::local(root.path().join("data"), local_store(root.path()));
    c.compute.inline_layer_bytes = 0; // every push uploads a layer
    c.compute.merge_factor = 0;
    c.compute.sweep_interval = Duration::from_millis(100);
    c.compute.sweep_retention = Duration::from_millis(300);
    let server = Server::start_config(c).await;
    let compute = server.node.compute().unwrap();
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("r");
    push_commits(&git, &src, &url, 0, 3).await;
    assert_eq!(files(root.path(), "r").len(), 3);

    compute.delete_repo("r").await.unwrap();
    let trash = root.path().join("store/data/trash/r");
    assert!(
        std::fs::read_dir(&trash).is_ok_and(|mut d| d.next().is_some()),
        "a trash marker"
    );
    let marked = || std::fs::read_dir(&trash).is_ok_and(|mut d| d.next().is_some());
    let reclaimed = || files(root.path(), "r").is_empty() && !marked();
    assert!(
        wait_until(reclaimed, 30).await,
        "not reclaimed: {:?}",
        files(root.path(), "r")
    );

    // Created again: the old incarnation's files go, the new one's stay.
    push_commits(&git, &src, &url, 10, 2).await;
    let old = files(root.path(), "r");
    compute.delete_repo("r").await.unwrap();
    compute.create_repo("r", "main").await.unwrap();
    let fresh = git.init("fresh").await;
    push_commits(&git, &fresh, &url, 20, 2).await;
    let named = || -> HashSet<String> {
        compute
            .state("r")
            .map(|m| m.layers.iter().map(|l| l.id.clone()).collect())
            .unwrap_or_default()
    };
    let settled = || files(root.path(), "r") == named() && !named().is_empty();
    assert!(
        wait_until(settled, 30).await,
        "old files remain: {:?}",
        files(root.path(), "r")
            .intersection(&old)
            .collect::<Vec<_>>()
    );
    clone_matches(&git, &fresh, &url, "after").await;
}
