//! Compaction: merged layers stay correct.

#![expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::time::Duration;

use common::{Git, Server, text};
use enroute_compute::RepoState;
use enroute_compute::testing::settled;

/// Compaction merges flushed layers, and every kind of fetch stays correct
/// on them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merged_layers_stay_correct() {
    let server = Server::start_with(|c| {
        c.compute.merge_factor = 2;
        c.compute.flush_interval = Duration::from_millis(200);
    })
    .await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("part");
    for i in 0..30 {
        git.commit_file(
            &src,
            &format!("d{}/f{}.txt", i % 4, i % 7),
            &text(i, 40 + i as usize),
            &format!("c{i}"),
        )
        .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
        if i % 10 == 4 {
            git.run(&src, &["checkout", "-q", "-b", &format!("b{i}")])
                .await;
            git.commit_file(&src, "branch.txt", &text(1000 + i, 20), "branch")
                .await;
            git.run(&src, &["push", "-q", &url, &format!("b{i}")]).await;
            git.run(&src, &["checkout", "-q", "main"]).await;
        }
        if i % 10 == 9 {
            git.run(&src, &["tag", "-a", &format!("t{i}"), "-m", "t"])
                .await;
            git.run(&src, &["push", "-q", &url, &format!("t{i}")]).await;
        }
    }
    // Wait until everything is flushed and compaction has settled.
    // The layers are all in the smallest size class, so they merge into
    // fewer than `merge_factor`.
    let done = || {
        let meta: RepoState = server
            .node
            .consensus()
            .unwrap()
            .repo_meta("part")
            .unwrap()
            .into();
        settled(&meta, server.node.compute().unwrap().config()) && meta.layers.len() == 1
    };
    assert!(
        common::wait_until(done, 20).await,
        "expected the layers merged into one"
    );

    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", "--mirror", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "main").await);
    assert_eq!(git.rev(&src, "t29").await, git.rev(&dst, "t29").await);
    for i in [4, 14, 24] {
        assert_eq!(
            git.rev(&src, &format!("b{i}")).await,
            git.rev(&dst, &format!("b{i}")).await
        );
    }
    // Incremental fetch on top of partitioned storage (thin pack bases).
    let work = git.dir("work");
    git.run(
        git.home.path(),
        &["clone", "-q", "-b", "main", &url, work.to_str().unwrap()],
    )
    .await;
    git.commit_file(&src, "d0/f0.txt", &text(99, 60), "after")
        .await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    git.run(&work, &["pull", "-q", "--ff-only"]).await;
    git.run(&work, &["fsck", "--strict"]).await;
    // Shallow and blobless clones too.
    let sh = git.dir("sh");
    git.run(
        git.home.path(),
        &["clone", "-q", "--depth=1", &url, sh.to_str().unwrap()],
    )
    .await;
    git.run(&sh, &["fsck"]).await;
    let pc = git.dir("pc");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            "--filter=blob:none",
            &url,
            pc.to_str().unwrap(),
        ],
    )
    .await;
    git.run(&pc, &["fsck"]).await;
}

/// Whether the ledger comes to report `repo` as pending (`want`) or not,
/// within 20 s, asking as the node's compute `compute`.
async fn becomes_pending(
    ledger: &enroute_ledger::Ledger,
    compute: u64,
    repo: &str,
    want: bool,
) -> bool {
    for _ in 0..400 {
        // The node's one compute: every repository is its own.
        let pending = enroute_shared::roles::pending(ledger, compute, Vec::new())
            .await
            .unwrap();
        if pending.repos.iter().any(|r| r.repo == repo) == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// The ledger reports a repository whose layers changed until the compute
/// settles it, and again once its layers change.
///
/// The merge policy is the compute's alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settled_repositories_are_pending_again_when_their_layers_change() {
    let root = tempfile::tempdir().unwrap();
    let mut config =
        enroute::node::Config::local(root.path().join("data"), common::local_store(root.path()));
    config.compute.flush_interval = Duration::from_millis(200);
    let server = Server::start_config(config).await;
    let ledger = server.node.ledger().unwrap();
    let compute = enroute_compute::compute_id(&root.path().join("data")).unwrap();
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("settle");
    for i in 0..2 {
        common::push_commits(&git, &src, &url, i, 1).await;
        assert!(
            becomes_pending(ledger, compute, "settle", true).await,
            "push {i}: never pending"
        );
        assert!(
            becomes_pending(ledger, compute, "settle", false).await,
            "push {i}: never settled"
        );
    }
}
