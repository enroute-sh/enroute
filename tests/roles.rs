//! Separate roles: ledger-only nodes hold the logs, compute-only nodes do the
//! git work, follow repositories over gRPC and maintain them.
//!
//! A ledger node binds none of the ports that serve git and the contract.

#![expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::time::Duration;

use common::{Git, Server, clone_matches, cluster, local_store, push_commits, wait_until};
use enroute::node::{Config, Role};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn compute_nodes_work_against_a_ledger_cluster() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let ledgers = cluster(root.path(), &store, 3, |_, c| {
        c.role = Role::Ledger;
    })
    .await;
    let mut compute_nodes = Vec::new();
    for i in 0..2 {
        let mut c = Config::local(root.path().join(format!("f{i}")), store.clone());
        c.role = Role::Compute;
        c.compute.flush_interval = Duration::from_millis(200);
        compute_nodes.push(Server::start_config(c).await);
    }
    let git = Git::new();
    let src = git.init("src").await;
    let (a, b) = (
        compute_nodes[0].repo_url("r"),
        compute_nodes[1].repo_url("r"),
    );

    // Writes through either compute, reads through the other.
    push_commits(&git, &src, &a, 0, 1).await;
    clone_matches(&git, &src, &b, "b0").await;
    push_commits(&git, &src, &b, 1, 1).await;
    push_commits(&git, &src, &a, 2, 1).await;
    clone_matches(&git, &src, &b, "b1").await;
    clone_matches(&git, &src, &a, "a1").await;

    // The ledger has it all, and a compute flushes the inline layers (the
    // ledger nodes run no maintenance).
    let state = || ledgers[0].node.consensus().unwrap().repo_meta("r");
    assert!(
        wait_until(
            || state().is_some_and(|m| m
                .layers
                .iter()
                .all(enroute_shared::proto::LayerMeta::is_stored)),
            20
        )
        .await,
        "inline layers were not flushed"
    );

    // Once its log is idle (every replica caught up), reads and writes
    // through either compute still work, in the same incarnation.
    let lagging = || {
        ledgers
            .iter()
            .map(|l| l.node.consensus().unwrap().stats().lagging_logs)
            .sum::<usize>()
    };
    assert!(
        wait_until(|| lagging() == 0, 30).await,
        "logs still lagging"
    );
    clone_matches(&git, &src, &a, "a2").await;
    push_commits(&git, &src, &b, 3, 1).await;
    clone_matches(&git, &src, &a, "a3").await;
    assert!(ledgers.iter().all(|l| {
        l.node
            .consensus()
            .unwrap()
            .hosted("r")
            .is_some_and(|h| h.epoch == 1)
    }));
}

/// A compute that follows a repository gets its whole state first, past
/// gRPC's 4 MiB default.
///
/// A compute that followed again under load could not decode it before.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn computes_follow_repositories_with_large_states() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = format!("http://{}", listener.local_addr().unwrap());
    let mut c = Config::local(root.path().join("l"), store.clone());
    c.role = Role::Ledger;
    c.ledger.advertise = Some(peer.clone());
    c.ledger.max_inline_layer_bytes = 4 << 20;
    let _ledger = Server::start_on(c, listener).await;
    let compute = |i: usize| {
        let mut c = Config::local(root.path().join(format!("f{i}")), store.clone());
        c.role = Role::Compute;
        // Everything stays inline.
        c.compute.inline_layer_bytes = 4 << 20;
        c.compute.max_inline_bytes = 64 << 20;
        c.compute.flush_interval = Duration::from_secs(3600);
        c.compute.flush_max_layers = 10_000;
        c
    };
    let a = Server::start_config(compute(0)).await;
    let mut api = a.api_client();
    api.create_repo("big").await.unwrap();
    // Six commits of 1 MiB that does not compress.
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for i in 0..6 {
        let blob: Vec<u8> = (0..1 << 20)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        api.create_commit(common::commit(
            "big",
            "main",
            &format!("c{i}"),
            vec![common::put(&format!("f{i}"), blob)],
        ))
        .await
        .unwrap();
    }

    // Another compute follows it from scratch.
    let b = Server::start_config(compute(1)).await;
    let mut follower = b.api_client();
    let refs = tokio::time::timeout(Duration::from_secs(30), follower.refs("big"))
        .await
        .expect("following timed out")
        .unwrap();
    let want = a.api_client().refs("big").await.unwrap();
    assert_eq!(refs.get("refs/heads/main"), want.get("refs/heads/main"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ledger_node_binds_only_its_internal_listener() {
    let root = tempfile::tempdir().unwrap();
    let free = || {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    };
    let (internal, api, git) = (free(), free(), free());
    let file = root.path().join("ledger.toml");
    std::fs::write(
        &file,
        format!(
            "[bucket]\nuri = \"file://{store}\"\n\
             [node]\nrole = \"ledger\"\ndata_dir = \"{data}\"\n\
             internal_listen = \"{internal}\"\nadvertise_url = \"http://{internal}\"\n\
             [listen]\napi = \"{api}\"\ngit = \"{git}\"\n",
            store = root.path().join("store").display(),
            data = root.path().join("data").display(),
        ),
    )
    .unwrap();
    let mut node = tokio::process::Command::new(env!("CARGO_BIN_EXE_enroute"))
        .arg("--config")
        .arg(format!("file://{}", file.display()))
        .env("RUST_LOG", "warn")
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let status = format!("http://{internal}/status");
    let mut up = false;
    for _ in 0..600 {
        up = reqwest::get(&status)
            .await
            .is_ok_and(|r| r.status().is_success());
        if up {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(up, "the ledger node did not answer {status}");

    // Free while the node runs: it never bound them.
    for at in [api, git] {
        let bound = std::net::TcpListener::bind(at);
        assert!(bound.is_ok(), "{at} is bound by the ledger node: {bound:?}");
    }
    node.kill().await.unwrap();
}

/// The first live compute node in a repository's order maintains it.
///
/// It does so even if it serves none of the repository's requests, and the
/// next node takes over once it stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_first_live_compute_in_order_maintains_a_repository() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let ledgers = cluster(root.path(), &store, 1, |_, c| {
        c.role = Role::Ledger;
    })
    .await;
    let compute = |id: u64| {
        let dir = root.path().join(format!("f{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("compute-id"), format!("{id:016x}")).unwrap();
        let mut c = Config::local(dir, store.clone());
        c.role = Role::Compute;
        c.compute.flush_interval = Duration::from_millis(200);
        c
    };
    let a = Server::start_config(compute(1)).await;
    let b = Server::start_config(compute(2)).await;
    // a lists the compute nodes every 2 s: until it sees b, it maintains
    // every repository.
    tokio::time::sleep(Duration::from_secs(3)).await;
    // A repository that b maintains.
    let repo = (0..100)
        .map(|i| format!("r{i}"))
        .find(|r| enroute_shared::nodes::first(r, &[1, 2]) == Some(2))
        .unwrap();
    let flushed = || {
        ledgers[0]
            .node
            .consensus()
            .unwrap()
            .repo_meta(&repo)
            .is_some_and(|m| {
                !m.layers.is_empty()
                    && m.layers
                        .iter()
                        .all(enroute_shared::proto::LayerMeta::is_stored)
            })
    };

    // Pushes go through a only; b follows the repository and flushes it.
    let git = Git::new();
    let src = git.init("src").await;
    push_commits(&git, &src, &a.repo_url(&repo), 0, 1).await;
    let b_follows = || b.node.compute().unwrap().state(&repo).is_some();
    assert!(
        wait_until(|| flushed() && b_follows(), 20).await,
        "flushed: {}, b follows: {}",
        flushed(),
        b_follows()
    );

    // b stops; a maintains the repository once b's entry is old.
    b.node.shutdown().await;
    drop(b);
    push_commits(&git, &src, &a.repo_url(&repo), 1, 1).await;
    assert!(wait_until(flushed, 30).await, "a did not take over");
    clone_matches(&git, &src, &a.repo_url(&repo), "a0").await;
}
