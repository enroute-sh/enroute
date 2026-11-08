//! A ledger node's lifetime: moving it, losing its disk, and removing it.
//!
//! Moving a node by its disk freezes and restores the ledger. A node removed
//! with pushes in flight must not have them reported refused.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{
    Git, Server, clone_matches, cluster, copy_dir, local_store, push_commits, wait_until,
};
use enroute::node::Config;
use enroute_shared::oid::Oid;
use enroute_shared::state::{Commit, RefUpdate};

fn config(data: &std::path::Path, store: &enroute::node::Store) -> Config {
    let mut c = Config::local(data.to_path_buf(), store.clone());
    c.compute.flush_interval = Duration::from_millis(200);
    c
}

/// A stopped node's data directory, copied elsewhere, is the same node.
///
/// Its identity is its log store. Every repository comes back with its
/// history, on the same replicas, and takes writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_moves_with_its_disk() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let git = Git::new();
    let mut srcs = Vec::new();
    let id = {
        let a = Server::start_config(config(&root.path().join("a"), &store)).await;
        for r in 0..3 {
            let src = git.init(&format!("src{r}")).await;
            push_commits(&git, &src, &a.repo_url(&format!("r{r}")), r * 10, 3).await;
            srcs.push(src);
        }
        let id = a.node.consensus().unwrap().id;
        a.node.shutdown().await;
        id
    };
    copy_dir(&root.path().join("a"), &root.path().join("b"));

    let b = Server::start_config(config(&root.path().join("b"), &store)).await;
    assert_eq!(
        b.node.consensus().unwrap().id,
        id,
        "the copy is the same node"
    );
    for (r, src) in srcs.iter().enumerate() {
        clone_matches(&git, src, &b.repo_url(&format!("r{r}")), &format!("b{r}")).await;
    }
    push_commits(&git, &srcs[1], &b.repo_url("r1"), 100, 2).await;
    clone_matches(&git, &srcs[1], &b.repo_url("r1"), "b1-after").await;
    assert_eq!(
        b.node.consensus().unwrap().hosted("r1").map(|h| h.epoch),
        Some(1),
        "the same incarnation"
    );
}

/// A node whose disk is lost comes back as a new node, never as the old one.
///
/// It joins as a new member; once the operator removes the old node, the
/// repository's log moves to it, and has three replicas again.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_node_that_lost_its_disk_is_replaced() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let mut servers = cluster(root.path(), &store, 3, |_, c| {
        c.compute.flush_interval = Duration::from_millis(200);
    })
    .await;
    let git = Git::new();
    let src = git.init("src").await;
    push_commits(&git, &src, &servers[0].repo_url("r"), 0, 3).await;
    assert!(
        wait_until(
            || servers
                .iter()
                .all(|s| s.node.consensus().unwrap().hosted("r").is_some()),
            20
        )
        .await,
        "replicas joined"
    );
    let old = servers[2].node.consensus().unwrap().id;

    // Node 2 loses its disk and comes back at the same address.
    let addr = servers.pop().unwrap().internal;
    let mut c = config(&root.path().join("n2-new"), &store);
    c.ledger.advertise = Some(format!("http://{addr}"));
    servers.push(Server::start_on(c, common::rebind(addr).await).await);
    let fresh = servers[2].node.consensus().unwrap().id;
    assert_ne!(fresh, old, "a new store is a new node");
    let replicas = |s: &Server| common::replicas(s, "r");
    let member = || {
        servers[0]
            .node
            .consensus()
            .unwrap()
            .members()
            .0
            .contains(&fresh)
    };
    assert!(wait_until(member, 20).await, "the new node is a member");
    let four = vec![
        servers[0].node.consensus().unwrap().id,
        servers[1].node.consensus().unwrap().id,
        old,
        fresh,
    ];

    // The operator removes the old node, on node 0.
    let objects = &servers[0].node.ledger().unwrap().objects;
    enroute_ledger::cluster::remove(objects, &root.path().join("n0"), old, false)
        .await
        .unwrap();
    let mut three: Vec<u64> = four.into_iter().filter(|&id| id != old).collect();
    three.sort_unstable();
    assert!(
        wait_until(|| servers.iter().all(|s| replicas(s) == three), 20).await,
        "the log did not move off the old node"
    );

    push_commits(&git, &src, &servers[1].repo_url("r"), 3, 2).await;
    clone_matches(&git, &src, &servers[2].repo_url("r"), "a").await;
}

/// A node away while a repository was deleted and created again drops the
/// ended incarnation as its root log catches up, and hosts the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_node_away_during_a_delete_drops_the_ended_incarnation() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let mut servers = cluster(root.path(), &store, 3, |_, c| {
        c.compute.flush_interval = Duration::from_millis(200);
    })
    .await;
    let git = Git::new();
    let src = git.init("src").await;
    push_commits(&git, &src, &servers[0].repo_url("r"), 0, 2).await;
    let epoch = |s: &Server| s.node.consensus().unwrap().hosted("r").map(|h| h.epoch);
    assert!(
        wait_until(|| servers.iter().all(|s| epoch(s) == Some(1)), 20).await,
        "replicas joined"
    );

    // Node 2 goes away; the repository is deleted and created again.
    let away = servers.pop().unwrap();
    let addr = away.internal;
    away.node.shutdown().await;
    drop(away);
    let compute = servers[0].node.compute().unwrap();
    compute.delete_repo("r").await.unwrap();
    let again = git.init("again").await;
    push_commits(&git, &again, &servers[1].repo_url("r"), 50, 2).await;
    assert_eq!(epoch(&servers[0]), Some(2));

    // It comes back with its disk, and catches up.
    let mut c = config(&root.path().join("n2"), &store);
    c.ledger.advertise = Some(format!("http://{addr}"));
    servers.push(Server::start_on(c, common::rebind(addr).await).await);
    assert!(
        wait_until(|| epoch(&servers[2]) == Some(2), 20).await,
        "the returning node hosts {:?}",
        epoch(&servers[2])
    );
    clone_matches(&git, &again, &servers[2].repo_url("r"), "back").await;
}

/// Commit to ref after ref of `repo` until `stop`: the commits refused, and
/// those that failed.
///
/// Writer `k` alone writes refs named `w{k}...`, so a refusal can only be its
/// own update applied twice. After a failure it starts a new ref.
async fn write_own_refs(
    compute: Arc<enroute_compute::Compute>,
    repo: String,
    k: u32,
    stop: Arc<AtomicBool>,
) -> (Vec<String>, Vec<String>) {
    let (mut branch, mut old, mut n) = (0u32, None, 0u32);
    let (mut refused, mut failed) = (Vec::new(), Vec::new());
    while !stop.load(Ordering::Relaxed) {
        n += 1;
        let mut oid = [0u8; 20];
        oid[..4].copy_from_slice(&n.to_be_bytes());
        let new = Oid::sha1(oid);
        let commit = Commit {
            updates: vec![RefUpdate {
                name: format!("refs/heads/w{k}b{branch}"),
                old,
                new: Some(new),
            }],
            ..Default::default()
        };
        let ref_name = format!("{repo} w{k}b{branch}");
        match compute.commit(&repo, 1, None, commit).await {
            Ok(c) => match c.outcome.ref_results.first().and_then(|r| r.error.as_ref()) {
                None => {
                    old = Some(new);
                    continue;
                }
                Some(e) => refused.push(format!("{ref_name}: {e}")),
            },
            Err(e) => failed.push(format!("{ref_name}: {e:#}")),
        }
        (branch, old) = (branch + 1, None);
    }
    (refused, failed)
}

/// A push in flight when its log's sequencer is removed lands, as told.
///
/// The removed node hands each log to another replica before its replica
/// moves, so it learns the outcome of every proposal it appended.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_push_in_flight_when_its_sequencer_is_removed_is_not_refused() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let servers = cluster(root.path(), &store, 4, |_, c| {
        c.compute.flush_interval = Duration::from_millis(200);
    })
    .await;
    let repos: Vec<String> = (0..32).map(|r| format!("r{r}")).collect();
    let created = servers[0].node.compute().unwrap();
    for r in &repos {
        created.create_repo(r, "main").await.unwrap();
    }
    let sequencer = |r: &str| {
        servers[0]
            .node
            .consensus()
            .unwrap()
            .hosted(r)
            .map(|h| h.sequencer)
    };
    assert!(
        wait_until(
            || repos.iter().all(|r| sequencer(r).is_some_and(|s| s != 0)),
            20
        )
        .await,
        "every log has a sequencer"
    );
    // Remove the node that sequences the first log; write through another.
    let victim = sequencer(&repos[0]).unwrap();
    let writer = servers
        .iter()
        .position(|s| s.node.consensus().unwrap().id != victim)
        .unwrap();
    let compute = servers[writer].node.compute().unwrap().clone();
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = repos
        .iter()
        .flat_map(|repo| (0..4).map(move |k| (repo, k)))
        .map(|(repo, k)| {
            let task = write_own_refs(compute.clone(), repo.clone(), k, stop.clone());
            tokio::spawn(task)
        })
        .collect();

    tokio::time::sleep(Duration::from_secs(1)).await;
    let objects = &servers[writer].node.ledger().unwrap().objects;
    let signer = root.path().join(format!("n{writer}"));
    enroute_ledger::cluster::remove(objects, &signer, victim, false)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Relaxed);
    let (mut refused, mut failed) = (Vec::new(), Vec::new());
    for task in writers {
        let (r, f) = task.await.unwrap();
        refused.extend(r);
        failed.extend(f);
    }
    assert!(
        refused.is_empty(),
        "refused by their own retry: {refused:?}"
    );
    assert!(failed.is_empty(), "failed: {failed:?}");

    // Its logs moved off it, each to three replicas, before it let them go.
    let (gone, kept): (Vec<&Server>, Vec<&Server>) = servers
        .iter()
        .partition(|s| s.node.consensus().unwrap().id == victim);
    let three = |s: &Server| {
        repos.iter().all(|r| {
            let replicas = common::replicas(s, r);
            replicas.len() == 3 && !replicas.contains(&victim)
        })
    };
    assert!(
        wait_until(|| kept.iter().all(|s| three(s)), 20).await,
        "every log has three replicas, none of them the removed node"
    );
    let hosted = || {
        let c = gone[0].node.consensus().unwrap();
        repos.iter().filter(|r| c.hosted(r).is_some()).count()
    };
    assert!(
        wait_until(|| hosted() == 0, 20).await,
        "the removed node hosts {}",
        hosted()
    );
}
