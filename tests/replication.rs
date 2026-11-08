//! Replication: failover, catch-up by snapshot, quiet idle logs, and
//! replicas as members come and go.

mod common;

use std::time::Duration;

use enroute_shared::proto::{ListRequest, ledger_server};
use enroute_shared::roles::sequencer_named;
use enroute_shared::state::Command;

use common::{
    Git, Server, clone_matches, cluster, commit, local_store, put, repo_status, text, wait_until,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn replicated_push_and_failover() {
    let root = tempfile::tempdir().unwrap();
    let mut servers: Vec<Option<Server>> =
        cluster(root.path(), &local_store(root.path()), 3, |_, _| {})
            .await
            .into_iter()
            .map(Some)
            .collect();
    let git = Git::new();
    let src = git.init("src").await;
    for i in 0..4 {
        git.commit_file(&src, &format!("f{i}"), &text(i, 50), &format!("c{i}"))
            .await;
    }
    // Push via node 1, clone from nodes 2 and 3.
    let url1 = servers[0].as_ref().unwrap().repo_url("rep");
    git.run(&src, &["push", "-q", &url1, "main"]).await;
    for (k, server) in servers.iter().enumerate().skip(1) {
        clone_matches(
            &git,
            &src,
            &server.as_ref().unwrap().repo_url("rep"),
            &format!("dst{k}"),
        )
        .await;
    }

    // Kill the repo log's sequencer; the root appoints another and they keep
    // accepting pushes.
    let sequencer = servers
        .iter()
        .position(|s| {
            s.as_ref().is_some_and(|s| {
                s.node
                    .consensus()
                    .unwrap()
                    .hosted("rep")
                    .is_some_and(|h| h.leading)
            })
        })
        .unwrap();
    let dead = servers[sequencer].take().unwrap();
    dead.node.shutdown().await;
    drop(dead);
    let alive: Vec<usize> = (0..3).filter(|i| servers[*i].is_some()).collect();
    let new_sequencer = || {
        alive.iter().any(|i| {
            servers[*i]
                .as_ref()
                .unwrap()
                .node
                .consensus()
                .unwrap()
                .hosted("rep")
                .is_some_and(|h| h.leading)
        })
    };
    assert!(
        wait_until(new_sequencer, 10).await,
        "no new sequencer after failover"
    );

    git.commit_file(&src, "after", "failover\n", "after failover")
        .await;
    let url_a = servers[alive[0]].as_ref().unwrap().repo_url("rep");
    git.run(&src, &["push", "-q", &url_a, "main"]).await;
    clone_matches(
        &git,
        &src,
        &servers[alive[1]].as_ref().unwrap().repo_url("rep"),
        "after",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lagging_replica_catches_up_via_install() {
    let root = tempfile::tempdir().unwrap();
    let configure = |_: usize, c: &mut enroute::node::Config| {
        c.compute.merge_factor = 0;
        c.compute.flush_interval = Duration::from_millis(200);
        c.ledger.snapshot_every = 5;
    };
    let mut servers: Vec<Option<Server>> =
        cluster(root.path(), &local_store(root.path()), 3, configure)
            .await
            .into_iter()
            .map(Some)
            .collect();

    // Take down a node other than the one that will create the repository.
    let replica = 2;
    let down = servers[replica].take().unwrap();
    let addr = down.internal;
    down.node.shutdown().await;
    drop(down);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Many writes while it is down: the log gets snapshotted and truncated.
    let git = Git::new();
    let src = git.init("src").await;
    let live = (0..3).find(|i| servers[*i].is_some()).unwrap();
    let url = servers[live].as_ref().unwrap().repo_url("lag");
    for i in 0..12 {
        git.commit_file(&src, "f", &text(i, 20), &format!("c{i}"))
            .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
    }
    // Truncated once the pushes' inline layers are flushed and that is
    // durable (the apply thread reports it every SYNC_EVERY), on both live
    // nodes (either may lead).
    common::wait_stored(servers[live].as_ref().unwrap(), "lag").await;
    let truncated = || {
        servers
            .iter()
            .flatten()
            .all(|s| s.node.consensus().unwrap().stats().compactions > 0)
    };
    assert!(wait_until(truncated, 20).await, "the log was not truncated");

    // Restart the replica on its old data directory (same port).
    let mut config = enroute::node::Config::local(
        root.path().join(format!("n{replica}")),
        local_store(root.path()),
    );
    config.ledger.advertise = Some(format!("http://{addr}"));
    configure(replica, &mut config);
    servers[replica] = Some(Server::start_on(config, common::rebind(addr).await).await);
    let s = servers[replica].as_ref().unwrap();
    let want = git.rev(&src, "main").await;
    // It joins the live incarnation when asked for the repository, and
    // catches up (via a snapshot: the log was truncated meanwhile).
    git.run(&src, &["ls-remote", &s.repo_url("lag")]).await;
    let got = || {
        s.node.consensus().unwrap().repo_meta("lag").and_then(|m| {
            m.refs
                .get("refs/heads/main")
                .map(enroute_shared::oid::Oid::to_hex)
        })
    };
    assert!(
        wait_until(|| got().as_ref() == Some(&want), 10).await,
        "replica did not catch up"
    );
    // (Its statistics are published at the end of the round that installed it.)
    assert!(
        wait_until(
            || s.node.consensus().unwrap().stats().snapshots_installed > 0,
            5
        )
        .await,
        "it caught up from a snapshot"
    );
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &s.repo_url("lag"), dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&dst, "HEAD").await, want);
}

/// A member behind the root log's start catches up from a root snapshot.
///
/// Raft's applied index must move with it, or the member's next election
/// panics on compacted entries (debug builds check it after every round).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lagging_member_catches_up_via_root_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let configure = |_: usize, c: &mut enroute::node::Config| {
        c.ledger.snapshot_every = 5;
    };
    let mut servers: Vec<Option<Server>> =
        cluster(root.path(), &local_store(root.path()), 3, configure)
            .await
            .into_iter()
            .map(Some)
            .collect();
    let follower = 2;
    let down = servers[follower].take().unwrap();
    let addr = down.internal;
    down.node.shutdown().await;
    drop(down);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Each creation is a root entry: enough of them compact the root log
    // past what the stopped member has.
    let mut client = servers[0].as_ref().unwrap().api_client();
    for i in 0..20 {
        client.create_repo(&format!("r{i}")).await.unwrap();
    }
    let compacted = || {
        servers
            .iter()
            .flatten()
            .all(|s| s.node.consensus().unwrap().stats().root_compactions > 0)
    };
    assert!(
        wait_until(compacted, 20).await,
        "the root log was not compacted"
    );

    let mut config = enroute::node::Config::local(
        root.path().join(format!("n{follower}")),
        local_store(root.path()),
    );
    config.ledger.advertise = Some(format!("http://{addr}"));
    configure(follower, &mut config);
    servers[follower] = Some(Server::start_on(config, common::rebind(addr).await).await);
    let s = servers[follower].as_ref().unwrap();
    assert!(
        wait_until(
            || s.node.consensus().unwrap().stats().root_snapshots > 0,
            10
        )
        .await,
        "it caught up from a root snapshot"
    );
    // It serves as a member after.
    let mut client = s.api_client();
    client.create_repo("after").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn idle_repos_go_quiet() {
    let root = tempfile::tempdir().unwrap();
    // Computes flush inline layers soon after the writes (a flush is a
    // write to the log, not idle traffic).
    let servers = cluster(root.path(), &local_store(root.path()), 3, |_, c| {
        c.compute.flush_interval = Duration::from_millis(200);
    })
    .await;
    let s0 = &servers[0];
    // Many repositories, each with its own log, created and written once.
    let n = 200;
    let mut client = s0.api_client();
    for i in 0..n {
        client.create_repo(&format!("idle{i}")).await.unwrap();
        client
            .create_commit(commit(
                &format!("idle{i}"),
                "main",
                "init",
                vec![put("f", "x")],
            ))
            .await
            .unwrap();
    }
    let sent = || -> u64 {
        servers
            .iter()
            .map(|s| s.node.consensus().unwrap().stats().log_messages)
            .sum()
    };
    let lagging = || -> usize {
        servers
            .iter()
            .map(|s| s.node.consensus().unwrap().stats().lagging_logs)
            .sum()
    };
    // Every replica caught up: nothing is resent. (Once the computes have
    // flushed the repositories' inline layers: those are writes.)
    assert!(
        wait_until(|| lagging() == 0, 20).await,
        "logs still lagging"
    );
    for _ in 0..15 {
        let before = sent();
        tokio::time::sleep(Duration::from_secs(2)).await;
        if sent() == before {
            break;
        }
    }
    let before = sent();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let idle_traffic = sent() - before;
    assert_eq!(
        idle_traffic, 0,
        "{n} idle repos sent {idle_traffic} messages in 3s"
    );

    // A quiet repo still works on demand, through any node.
    let s2 = &servers[2];
    s2.api_client()
        .create_commit(commit("idle7", "main", "wake", vec![put("f", "y")]))
        .await
        .unwrap();
    let git = Git::new();
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            &servers[1].repo_url("idle7"),
            dst.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(std::fs::read_to_string(dst.join("f")).unwrap(), "y");
}

/// A log with two replicas in one zone moves one of them to a node in a zone
/// it lacks once one joins, and keeps taking writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn replicas_spread_over_zones() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let zones = ["a", "a", "b"];
    let mut servers = cluster(root.path(), &store, 3, |i, c| {
        c.ledger.zone = zones[i].to_string();
    })
    .await;
    let n = 6;
    for i in 0..n {
        let mut api = servers[i % servers.len()].api_client();
        api.create_repo(&format!("r{i}")).await.unwrap();
    }
    let id = |s: &Server| s.node.consensus().unwrap().id;

    // A node in zone c joins: every log ends on a, b and c.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut c = enroute::node::Config::local(root.path().join("n3"), store.clone());
    c.ledger.advertise = Some(format!("http://{}", listener.local_addr().unwrap()));
    c.ledger.zone = "c".into();
    servers.push(Server::start_on(c, listener).await);
    let zone_of: Vec<(u64, &str)> = (servers.iter().map(id)).zip(["a", "a", "b", "c"]).collect();
    let zone = |n: u64| zone_of.iter().find(|(m, _)| *m == n).map(|(_, z)| *z);
    let spread_one = |i: usize| {
        let c = servers[0].node.consensus().unwrap();
        c.replicas(&format!("r{i}"))
            .is_some_and(|(replicas, joint)| {
                let zones: std::collections::BTreeSet<_> =
                    replicas.iter().map(|&r| zone(r)).collect();
                joint.is_empty() && replicas.len() == 3 && zones.len() == 3
            })
    };
    let spread = || (0..n).all(spread_one);
    assert!(
        wait_until(spread, 30).await,
        "the logs did not spread over the zones"
    );
    for i in 0..n {
        servers[i % servers.len()]
            .api_client()
            .create_commit(commit(
                &format!("r{i}"),
                "main",
                "after",
                vec![put("f", "y")],
            ))
            .await
            .unwrap();
    }
}

/// A log's replicas follow the members: new logs go to a new member.
///
/// A removed member's logs move to the others (through a joint
/// configuration), taking writes throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn replicas_follow_the_members() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let mut servers = cluster(root.path(), &store, 3, |_, _| {}).await;
    let n = 10;
    for i in 0..n {
        let mut api = servers[i % servers.len()].api_client();
        api.create_repo(&format!("r{i}")).await.unwrap();
        api.create_commit(commit(
            &format!("r{i}"),
            "main",
            "init",
            vec![put("f", "x")],
        ))
        .await
        .unwrap();
    }
    let id = |s: &Server| s.node.consensus().unwrap().id;
    let replicas = |s: &Server, repo: &str| {
        let mut r = (s.node.consensus().unwrap().replicas(repo)).unwrap_or_default();
        r.0.sort_unstable();
        r
    };
    let mut three: Vec<u64> = servers.iter().map(id).collect();
    three.sort_unstable();
    for i in 0..n {
        assert_eq!(replicas(&servers[0], &format!("r{i}")).0, three);
    }

    // A fourth node joins: it takes logs created from now on.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut c = enroute::node::Config::local(root.path().join("n3"), store.clone());
    c.ledger.advertise = Some(format!("http://{}", listener.local_addr().unwrap()));
    servers.push(Server::start_on(c, listener).await);
    let new = id(&servers[3]);
    let member = || {
        servers[0]
            .node
            .consensus()
            .unwrap()
            .members()
            .0
            .contains(&new)
    };
    assert!(wait_until(member, 30).await, "the new node is not a member");

    // Node 0 is removed: every log moves off it, onto the new node.
    let gone = id(&servers[0]);
    let objects = &servers[1].node.ledger().unwrap().objects;
    enroute_ledger::cluster::remove(objects, &root.path().join("n1"), gone, true)
        .await
        .unwrap();
    let mut want: Vec<u64> = three.iter().copied().filter(|&n| n != gone).collect();
    want.push(new);
    want.sort_unstable();
    let moved =
        || (0..n).all(|i| replicas(&servers[1], &format!("r{i}")) == (want.clone(), Vec::new()));
    assert!(
        wait_until(moved, 30).await,
        "the logs did not move off the removed node"
    );
    // It drops its logs.
    assert!(
        wait_until(
            || servers[0].node.consensus().unwrap().repos().is_empty(),
            10
        )
        .await,
        "the removed node kept logs"
    );
    // The new replica caught up.
    let caught_up = || {
        (0..n).all(|i| {
            (servers[3]
                .node
                .consensus()
                .unwrap()
                .repo_meta(&format!("r{i}")))
            .is_some_and(|m| m.refs.contains_key("refs/heads/main"))
        })
    };
    assert!(
        wait_until(caught_up, 20).await,
        "the new replica did not catch up"
    );

    // Writes through the remaining nodes, read through the new one.
    servers[1]
        .api_client()
        .create_commit(commit("r0", "main", "after", vec![put("f", "y")]))
        .await
        .unwrap();
    let git = Git::new();
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            &servers[3].repo_url("r0"),
            dst.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(std::fs::read_to_string(dst.join("f")).unwrap(), "y");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_node_names_the_sequencer() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, |_, _| {}).await;
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "f", "x\n", "c").await;
    git.run(&src, &["push", "-q", &servers[0].repo_url("rep"), "main"])
        .await;

    let sequencer = servers
        .iter()
        .find(|s| {
            s.node
                .consensus()
                .unwrap()
                .hosted("rep")
                .is_some_and(|h| h.leading)
        })
        .map(|s| s.node.consensus().unwrap().status()["node"].clone())
        .unwrap();
    // Replicas learn the sequencer from its first message.
    let mut agree = false;
    for _ in 0..100 {
        let mut named = Vec::new();
        for s in &servers {
            named.push(
                repo_status(s, "rep")
                    .await
                    .map(|v| (v["sequencer"].clone(), v["term"].clone())),
            );
        }
        if named
            .iter()
            .all(|n| n.as_ref().is_some_and(|(l, _)| *l == sequencer))
            && named.windows(2).all(|w| w[0] == w[1])
        {
            agree = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(agree, "the nodes do not all name {sequencer} at one term");
    assert_eq!(repo_status(&servers[0], "missing").await, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_node_lists_a_repository_once_created() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, |_, _| {}).await;
    for i in 0..5 {
        let repo = format!("listed{i}");
        servers[i % 3]
            .api_client()
            .create_repo(&repo)
            .await
            .unwrap();
        for s in &servers {
            let ledger = s.node.ledger().unwrap();
            let page = ledger_server::Ledger::list(
                &**ledger,
                tonic::Request::new(ListRequest {
                    prefix: repo.clone(),
                    after: String::new(),
                    limit: 10,
                }),
            )
            .await
            .unwrap()
            .into_inner();
            assert!(
                page.repos.iter().any(|r| r.name == repo),
                "{repo} not listed at once by every node"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_compute_proposes_at_the_sequencer() {
    let root = tempfile::tempdir().unwrap();
    let servers = cluster(root.path(), &local_store(root.path()), 3, |_, _| {}).await;
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "f", "x\n", "c").await;
    git.run(&src, &["push", "-q", &servers[0].repo_url("rep"), "main"])
        .await;
    let leading = |s: &Server| {
        s.node
            .consensus()
            .unwrap()
            .hosted("rep")
            .is_some_and(|h| h.leading)
    };
    let sequencer = servers.iter().position(leading).unwrap();
    let sequencer_url = format!("http://{}", servers[sequencer].internal);
    // Another replica refuses a proposal, naming the sequencer.
    let replica = servers[(sequencer + 1) % 3].node.ledger().unwrap();
    let refused = ledger_server::Ledger::propose(
        &**replica,
        tonic::Request::new(Command::advance_upload_gen("rep")),
    )
    .await
    .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable);
    assert_eq!(sequencer_named(&refused), Some(sequencer_url));
    // Every node takes the push: its compute sends the proposal to the
    // sequencer its subscription names.
    for (k, s) in servers.iter().enumerate() {
        git.commit_file(&src, "f", &format!("{k}\n"), &format!("c{k}"))
            .await;
        git.run(&src, &["push", "-q", &s.repo_url("rep"), "main"])
            .await;
    }
    assert!(leading(&servers[sequencer]), "the sequencer stays put");
    clone_matches(
        &git,
        &src,
        &servers[(sequencer + 1) % 3].repo_url("rep"),
        "dst",
    )
    .await;
}

/// A failed node's logs go to the survivors by load: the root's leader
/// appoints the least loaded of the most up-to-date replicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn failover_spreads_logs_across_survivors() {
    let root = tempfile::tempdir().unwrap();
    let mut servers: Vec<Option<Server>> =
        cluster(root.path(), &local_store(root.path()), 3, |_, _| {})
            .await
            .into_iter()
            .map(Some)
            .collect();
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "f", "x\n", "c").await;
    // Created through one node, which sequences them all.
    let repos: Vec<String> = (0..10).map(|i| format!("r{i}")).collect();
    let url = |s: &Option<Server>, r: &str| s.as_ref().unwrap().repo_url(r);
    for r in &repos {
        git.run(&src, &["push", "-q", &url(&servers[0], r), "main"])
            .await;
    }
    let led = |s: &Server| {
        let c = s.node.consensus().unwrap();
        repos
            .iter()
            .filter(|r| c.hosted(r).is_some_and(|h| h.leading))
            .count()
    };
    let first = servers.iter().flatten().map(led).collect::<Vec<_>>();
    assert_eq!(first[0], 10, "{first:?}");
    // Node 0 goes: its logs spread over the two others.
    let dead = servers[0].take().unwrap();
    dead.node.shutdown().await;
    drop(dead);
    let spread = || {
        let counts: Vec<usize> = servers.iter().flatten().map(led).collect();
        counts.iter().sum::<usize>() == 10 && counts.iter().all(|&n| n >= 4)
    };
    assert!(wait_until(spread, 20).await, "the logs did not spread");
    // And take pushes through either.
    for (k, r) in repos.iter().enumerate() {
        git.commit_file(&src, "f", &format!("{k}\n"), &format!("c{k}"))
            .await;
        git.run(
            &src,
            &["push", "-q", "-f", &url(&servers[1 + k % 2], r), "main"],
        )
        .await;
    }
    clone_matches(&git, &src, &url(&servers[2], "r9"), "dst").await;
}
