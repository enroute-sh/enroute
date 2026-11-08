//! A cold node serves from the object store and never waits on it, even
//! within a small disk budget.

#![expect(
    clippy::cognitive_complexity,
    clippy::indexing_slicing,
    clippy::multiple_unsafe_ops_per_block,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use common::{Git, Server, copy_dir, local_store, text, wait_stored};
use enroute::node::Config;

/// A node with no local copies loads every byte of layer data asynchronously.
///
/// A synchronous read of bytes that are not local fails, so each operation
/// that succeeds shows its path loads what it reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_node_never_waits_on_the_object_store() {
    // A budget that never evicts: chunks are loaded only as they are read.
    cold_node(1 << 30).await;
}

/// The same within a disk budget far smaller than the repository: chunks
/// are evicted while every operation runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_node_within_a_small_disk_budget() {
    cold_node(48 << 10).await;
}

#[expect(
    unsafe_code,
    reason = "the engine reads its test-only tuning from the environment"
)]
async fn cold_node(disk_budget: u64) {
    // SAFETY: set before any thread reads it.
    unsafe {
        std::env::set_var("ENROUTE_TIER_CHUNK", "512");
        std::env::set_var("ENROUTE_PAGE_SIZE", "512");
        // Shallow fetches rebuild boundary objects in bulk, in many groups.
        std::env::set_var("ENROUTE_REPLAY_MIN", "1");
        std::env::set_var("ENROUTE_REPLAY_GROUP_BYTES", "2000");
    }
    let git = Git::new();
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let src = git.init("src").await;
    // Nested trees and files edited over many commits: delta chains across
    // trees and blobs, and tags (annotated, and a tag of a tag).
    for i in 0..30u64 {
        for f in [
            format!("a/b/c{}.txt", i % 3),
            format!("d/e{}.txt", i % 4),
            "top.txt".to_string(),
        ] {
            let content = format!("{}{}", text(i % 5, 80), text(i, 3));
            git.commit_file(&src, &f, &content, &format!("c{i}")).await;
        }
        if i % 6 == 0 {
            // Large blobs: the data region spans many pages.
            git.commit_file(
                &src,
                &format!("big/{}.txt", i % 4),
                &text(1000 + i, 4000),
                &format!("big {i}"),
            )
            .await;
        }
        if i % 10 == 9 {
            git.run(&src, &["tag", "-a", &format!("v{i}"), "-m", "release"])
                .await;
        }
    }
    git.run(&src, &["tag", "-a", "nested", "-m", "tag of a tag", "v29"])
        .await;

    let data_a = root.path().join("a");
    {
        let mut config = Config::local(data_a.clone(), store.clone());
        config.compute.flush_interval = std::time::Duration::from_millis(200);
        let server = Server::start_config(config).await;
        git.run(
            &src,
            &[
                "push",
                "-q",
                "--follow-tags",
                &server.repo_url("cold"),
                "main",
            ],
        )
        .await;
        git.run(&src, &["push", "-q", &server.repo_url("cold"), "nested"])
            .await;
        wait_stored(&server, "cold").await;
        server.node.shutdown().await;
    }

    let data_b = root.path().join("b");
    // The ledger's data: its logs and their state.
    copy_dir(&data_a.join("log"), &data_b.join("log"));
    copy_dir(&data_a.join("state"), &data_b.join("state"));
    // Links to the store's files would leave nothing to load.
    let mut config = common::own_copies(Config::local(data_b, store));
    config.disk_cache_bytes = disk_budget;
    let server = Server::start_config(config).await;
    let url = server.repo_url("cold");
    let stat = |k: &str| {
        server.node.compute().unwrap().storage().status()["local_copies"][k]
            .as_u64()
            .unwrap()
    };

    git.run(git.home.path(), &["ls-remote", &url]).await;

    let client = server.api_client();
    let file = |rev: &str, path: &str| {
        let (rev, path) = (rev.to_string(), path.to_string());
        let mut client = client.clone();
        async move {
            anyhow::Ok(String::from_utf8(
                client.read_file("cold", &rev, &path).await?,
            )?)
        }
    };
    assert_eq!(
        file("refs/tags/v9", "d/e1.txt").await.unwrap(),
        git.run(&src, &["show", "v9:d/e1.txt"]).await
    );
    assert_eq!(
        file("main", "a/b/c2.txt").await.unwrap(),
        git.run(&src, &["show", "main:a/b/c2.txt"]).await
    );

    // A push on top of the history: a thin pack whose bases are only in the
    // object store, and segments diffed against parent trees.
    git.commit_file(
        &src,
        "a/b/c1.txt",
        &format!("{}pushed\n", text(4, 80)),
        "pushed",
    )
    .await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    // Server-side commits: small ones travel inline in the log, large ones
    // are ingested into a layer; both edit trees deep in the history.
    for (name, content) in [("inline", "x\n".to_string()), ("layer", text(7, 40_000))] {
        let changes = vec![
            common::put(&format!("a/b/{name}.txt"), content),
            common::delete("d/e2.txt"),
        ];
        client
            .clone()
            .create_commit(common::commit("cold", "main", name, changes))
            .await
            .unwrap();
    }

    let shallow = git.dir("shallow");
    git.run(
        git.home.path(),
        &["clone", "-q", "--depth=1", &url, shallow.to_str().unwrap()],
    )
    .await;
    git.run(&shallow, &["fetch", "-q", "--depth=5"]).await;
    git.run(&shallow, &["fetch", "-q", "--unshallow", "--tags"])
        .await;
    git.run(&shallow, &["fsck", "--strict"]).await;

    let blobless = git.dir("blobless");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            "--filter=blob:none",
            "--no-checkout",
            &url,
            blobless.to_str().unwrap(),
        ],
    )
    .await;

    let full = git.dir("full");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, full.to_str().unwrap()],
    )
    .await;
    git.run(&full, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&src, "main").await, git.rev(&full, "HEAD~2").await);
    assert_eq!(
        git.rev(&src, "nested").await,
        git.rev(&full, "nested").await
    );

    assert_eq!(
        git.run(&full, &["show", "HEAD:a/b/inline.txt"]).await,
        "x\n"
    );

    assert!(stat("remote_opens") > 0);
    if disk_budget < 1 << 30 {
        let disk = &server.node.compute().unwrap().storage().status()["disk_cache"];
        assert!(disk["evictions"].as_u64().unwrap() > 0, "{disk}");
    }
    assert_eq!(stat("hydrations"), 0);
}
