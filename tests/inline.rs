//! Inline layers: small pushes travel in the log, large ones upload first,
//! and flushes merge before they upload.

#![expect(
    clippy::as_conversions,
    clippy::excessive_nesting,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::time::Duration;

use common::{Git, Server, cluster, local_store, text, wait_until};
use enroute::node::Config;

/// Inline layers (not flushed to the object store yet).
fn pending_layers(server: &Server, repo: &str) -> usize {
    server
        .node
        .consensus()
        .unwrap()
        .repo_meta(repo)
        .map_or(0, |m| m.layers.iter().filter(|l| !l.is_stored()).count())
}

/// Inline layers travel in the log, so a push survives an object store outage.
///
/// Every node serves it, through the loss of the sequencer and the maintainer;
/// another node flushes it once the store is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn inline_layers_replicate_through_the_log() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let outage = common::Outage::wrap(&mut Config::local(
        root.path().join("unused"),
        store.clone(),
    ));
    let mut servers: Vec<Option<Server>> = cluster(root.path(), &store, 3, |_, c| {
        c.compute.merge_factor = 0;
        c.compute.flush_interval = Duration::from_millis(200);
        c.store = enroute::node::Store::new(outage.clone());
    })
    .await
    .into_iter()
    .map(Some)
    .collect();
    // Create the repository (that needs the object store) through node 1,
    // which leads it.
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "f0", &text(0, 10), "c0").await;
    git.run(
        &src,
        &[
            "push",
            "-q",
            &servers[0].as_ref().unwrap().repo_url("q"),
            "main",
        ],
    )
    .await;
    assert!(
        wait_until(
            || servers[0]
                .as_ref()
                .unwrap()
                .node
                .consensus()
                .unwrap()
                .hosted("q")
                .is_some_and(|h| h.leading),
            10
        )
        .await
    );
    assert!(
        wait_until(
            || servers
                .iter()
                .flatten()
                .all(|s| pending_layers(s, "q") == 0),
            20
        )
        .await
    );
    // Every node serves the repository (its compute follows it, with the
    // stored layer loaded) before the store goes down.
    for (i, s) in servers.iter().flatten().enumerate() {
        let dst = git.dir(&format!("warm{i}"));
        git.run(
            git.home.path(),
            &["clone", "-q", &s.repo_url("q"), dst.to_str().unwrap()],
        )
        .await;
    }
    outage.set_down(true);

    git.commit_file(&src, "f", &text(1, 40), "c1").await;
    git.run(
        &src,
        &[
            "push",
            "-q",
            &servers[1].as_ref().unwrap().repo_url("q"),
            "main",
        ],
    )
    .await;
    assert!(
        wait_until(
            || servers
                .iter()
                .flatten()
                .all(|s| pending_layers(s, "q") == 1),
            10
        )
        .await
    );
    for (i, s) in servers.iter().flatten().enumerate() {
        let dst = git.dir(&format!("d{i}"));
        git.run(
            git.home.path(),
            &["clone", "-q", &s.repo_url("q"), dst.to_str().unwrap()],
        )
        .await;
        git.run(&dst, &["fsck", "--strict"]).await;
        assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "HEAD").await);
    }

    // The sequencer dies; the store recovers; a compute node flushes.
    let dead = servers[0].take().unwrap();
    dead.node.shutdown().await;
    drop(dead);
    outage.set_down(false);
    assert!(
        wait_until(
            || servers
                .iter()
                .flatten()
                .all(|s| pending_layers(s, "q") == 0),
            40
        )
        .await,
        "no node flushed the inline layer"
    );
    for l in &servers[2]
        .as_ref()
        .unwrap()
        .node
        .consensus()
        .unwrap()
        .repo_meta("q")
        .unwrap()
        .layers
    {
        assert_eq!(
            l.id.len(),
            32 + 1 + 16,
            "layer ids are 128 random bits and an upload generation"
        );
        assert!(enroute_shared::state::layer_gen(&l.id).is_some());
        assert_eq!(l.root.len(), 32);
    }
    let dst = git.dir("after");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            &servers[2].as_ref().unwrap().repo_url("q"),
            dst.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "HEAD").await);
}

/// Layers over the inline limit are in the object store before the push is
/// acknowledged: with the store down, such a push fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_layers_are_uploaded_before_commit() {
    let mut outage = None;
    let server = Server::start_with(|c| {
        outage = Some(common::Outage::wrap(c));
        c.compute.merge_factor = 0;
        c.compute.inline_layer_bytes = 0;
        // Every layer is uploaded, so a ledger that refuses inline ones
        // takes each push.
        c.ledger.max_inline_layer_bytes = 0;
    })
    .await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("big");
    git.commit_file(&src, "f", &text(0, 30), "c0").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    assert_eq!(
        pending_layers(&server, "big"),
        0,
        "uploaded before the push returned"
    );
    outage.as_ref().unwrap().set_down(true);
    git.commit_file(&src, "f", &text(1, 30), "c1").await;
    assert!(
        !git.try_run(&src, &["push", "-q", &url, "main"]).await.0,
        "push must fail while the store is down"
    );
    outage.as_ref().unwrap().set_down(false);
    git.run(&src, &["push", "-q", &url, "main"]).await;
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, dst.to_str().unwrap()],
    )
    .await;
    assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "HEAD").await);
}

/// A ledger refuses a push that carries a layer over its inline limit,
/// whatever the compute is set to, and takes one within it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ledger_refuses_inline_layers_over_its_limit() {
    let server = Server::start_with(|c| {
        c.compute.inline_layer_bytes = 1 << 20;
        c.compute.flush_interval = Duration::from_secs(3600);
        c.ledger.max_inline_layer_bytes = 16 << 10;
    })
    .await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("limit");
    git.commit_file(&src, "f", "small\n", "c0").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    assert_eq!(
        pending_layers(&server, "limit"),
        1,
        "the small layer is inline"
    );
    git.commit_file(&src, "f", &text(0, 4000), "c1").await;
    let (ok, _, stderr) = git.try_run(&src, &["push", "-q", &url, "main"]).await;
    assert!(!ok, "an inline layer over the limit went through");
    assert!(stderr.contains("at most 16384 bytes"), "{stderr}");
    assert_eq!(pending_layers(&server, "limit"), 1);
}

/// A ledger whose limit is 0 refuses every push that carries its layer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ledger_without_inline_layers_refuses_them() {
    let server = Server::start_with(|c| c.ledger.max_inline_layer_bytes = 0).await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("small");
    git.commit_file(&src, "f", "small\n", "c0").await;
    let (ok, _, stderr) = git.try_run(&src, &["push", "-q", &url, "main"]).await;
    assert!(!ok, "an inline layer went through");
    assert!(stderr.contains("at most 0 bytes"), "{stderr}");
    assert_eq!(pending_layers(&server, "small"), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn merge_before_upload_batches_puts() {
    let mut outage = None;
    let server = Server::start_with(|c| {
        outage = Some(common::Outage::wrap(c));
        c.compute.merge_factor = 0;
        c.compute.flush_interval = Duration::from_secs(2);
    })
    .await;
    let git = Git::new();
    let url = server.repo_url("batch");
    // Concurrent pushers on their own branches.
    let mut dirs = Vec::new();
    for w in 0..6 {
        let d = git.init(&format!("w{w}")).await;
        dirs.push(d);
    }
    let pushes = 6 * 5;
    futures::future::join_all(dirs.iter().enumerate().map(|(w, d)| {
        let (git, url) = (&git, url.clone());
        async move {
            for i in 0..5 {
                git.commit_file(d, "f", &text(w as u64 * 100 + i, 20), &format!("c{i}"))
                    .await;
                git.run(
                    d,
                    &["push", "-q", "-f", &url, &format!("HEAD:refs/heads/w{w}")],
                )
                .await;
            }
        }
    }))
    .await;
    assert!(
        wait_until(|| pending_layers(&server, "batch") == 0, 30).await,
        "inline layers not flushed"
    );
    let puts = server.node.compute().unwrap().storage().status()["object_store"]["uploads"]
        .as_u64()
        .unwrap();
    assert!(
        puts * 3 <= pushes,
        "expected batched uploads: {puts} layer uploads for {pushes} pushes"
    );
    // Everything is still served correctly from the merged layers.
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", "--mirror", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    for (w, d) in dirs.iter().enumerate() {
        assert_eq!(
            git.rev(d, "HEAD").await,
            git.rev(&dst, &format!("w{w}")).await
        );
    }
}
