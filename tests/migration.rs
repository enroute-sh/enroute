//! A live deployment moves between the deployment models without losing an
//! acknowledged write.
//!
//! One node, then one ledger with compute nodes, then a replicated ledger
//! (docs/operate/deployment-profiles.md).

#![expect(
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{Git, Server, clone_matches, commit, local_store, push_commits, put, wait_until};
use enroute::node::{Config, Role, Store};

fn config(data: &Path, store: &Store, role: Role) -> Config {
    let mut c = Config::local(data.to_path_buf(), store.clone());
    c.role = role;
    c.compute.flush_interval = Duration::from_millis(200);
    c
}

/// A free port for a ledger node that starts later: until then, calls to
/// it are refused, as a stopped node's are.
fn port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

/// A ledger node of a cluster, advertised on `at`.
async fn ledger(data: &Path, store: &Store, at: std::net::SocketAddr) -> Server {
    let listener = common::rebind(at).await;
    let mut c = config(data, store, Role::Ledger);
    c.ledger.advertise = Some(format!("http://{}", listener.local_addr().unwrap()));
    Server::start_on(c, listener).await
}

/// Whether `enroute members` comes to print `want` within 20 s (a node
/// becomes a member once it joins).
async fn members_become(root: &Path, want: &[u64]) -> bool {
    for _ in 0..80 {
        if members_cli(root).await == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

/// `enroute members` against the store under `root`: the member ids it prints.
async fn members_cli(root: &Path) -> Vec<u64> {
    let file = root.join("members.toml");
    std::fs::write(
        &file,
        format!(
            "[bucket]\nuri = \"{}\"\n[node]\ndata_dir = \"{}\"\n",
            common::store_uri(root),
            root.join("cli").display()
        ),
    )
    .unwrap();
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_enroute"))
        .args([
            "members",
            "--json",
            "--config",
            &format!("file://{}", file.display()),
        ])
        .env_remove("ENROUTE_CONFIG")
        .env("RUST_LOG", "warn")
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let listed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let mut ids: Vec<u64> = listed
        .get("members")
        .and_then(serde_json::Value::as_array)
        .unwrap()
        .iter()
        .map(|id| id.as_u64().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

/// Push `src`'s main to `url`, retrying while the ledger appoints sequencers
/// (a refused push is not acknowledged, so a retry is the same write).
async fn push_retrying(git: &Git, src: &Path, url: &str) {
    for attempt in 0.. {
        let (ok, out, err) = git.try_run(src, &["push", "-q", url, "main"]).await;
        if ok {
            return;
        }
        assert!(attempt < 30, "push to {url}:\n{err}\n{out}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// What every phase wrote and had acknowledged, checked again at the end.
struct Acked {
    /// Repositories pushed with git, by their local clone.
    pushed: Vec<(String, PathBuf)>,
    /// What the API last committed to `notes.txt` in repository `api`.
    api_file: String,
}

impl Acked {
    async fn check(&self, git: &Git, compute: &Server, phase: &str) {
        for (repo, src) in &self.pushed {
            clone_matches(
                git,
                src,
                &compute.repo_url(repo),
                &format!("{phase}-{repo}"),
            )
            .await;
        }
        let (repo, src) = self.pushed.first().unwrap();
        let remote = git.run(src, &["ls-remote", &compute.repo_url(repo)]).await;
        let want = git.rev(src, "v1").await;
        assert!(
            remote.contains(&format!("{want}\trefs/tags/v1")),
            "{phase}: {remote}"
        );
        let want = git.rev(src, "topic").await;
        assert!(
            remote.contains(&format!("{want}\trefs/heads/topic")),
            "{phase}: {remote}"
        );
        // Reads have no read barrier, so a new compute may lag.
        let mut api = compute.api_client();
        for attempt in 0.. {
            let got = api.read_file("api", "main", "notes.txt").await;
            if got.as_ref().is_ok_and(|b| *b == self.api_file.as_bytes()) {
                return;
            }
            assert!(attempt < 30, "{phase}: {got:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Commit `text` through the API of `compute`, retrying while the ledger
    /// appoints sequencers.
    async fn api_commit(&mut self, compute: &Server, text: &str) {
        let mut api = compute.api_client();
        for attempt in 0.. {
            let request = commit(
                "api",
                "main",
                text,
                vec![put("notes.txt", text.to_string())],
            );
            match api.create_commit(request).await {
                Ok(_) => break,
                Err(e) => assert!(attempt < 30, "{e}"),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        self.api_file = text.into();
    }
}

/// Model 1: write to a single node that runs both roles.
///
/// Three pushed repositories (the first with a tag and a second branch), and
/// one written through the API.
async fn single_node(git: &Git, single: &Server) -> Acked {
    let mut pushed = Vec::new();
    for r in 0..3u64 {
        let name = format!("r{r}");
        let src = git.init(&format!("src{r}")).await;
        push_commits(git, &src, &single.repo_url(&name), r * 10, 3).await;
        pushed.push((name, src));
    }
    let src0 = pushed.first().unwrap().1.clone();
    git.run(&src0, &["tag", "v1"]).await;
    git.run(&src0, &["branch", "topic"]).await;
    let url0 = single.repo_url("r0");
    git.run(&src0, &["push", "-q", &url0, "v1", "topic"]).await;
    single.api_client().create_repo("api").await.unwrap();
    let mut acked = Acked {
        pushed,
        api_file: String::new(),
    };
    acked.api_commit(single, "one\n").await;
    acked.check(git, single, "single").await;
    acked
}

/// One node becomes one ledger with a compute, then a ledger of three: every
/// repository, ref and object stays, and every phase takes writes.
///
/// Its store id is its Raft id, so it stays the only voter of every log
/// and becomes the only member; later members join each log.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn one_node_grows_into_a_replicated_ledger() {
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let git = Git::new();
    let a_data = root.path().join("a");

    let single = Server::start_config(config(&a_data, &store, Role::Both)).await;
    let id_a = single.node.consensus().unwrap().id;
    let mut acked = single_node(&git, &single).await;
    assert!(
        members_cli(root.path()).await.is_empty(),
        "a standalone node is in no cluster"
    );
    single.node.shutdown().await;
    drop(single);

    // Model 2: the same data directory as a ledger node, and a compute node,
    // which finds the ledger's members in the store as they change.
    let [la, lb, lc] = [port(), port(), port()];
    let compute_config = |dir: &str| config(&root.path().join(dir), &store, Role::Compute);
    let a = ledger(&a_data, &store, la).await;
    assert_eq!(a.node.consensus().unwrap().id, id_a, "the same node");
    assert!(
        members_become(root.path(), &[id_a]).await,
        "node a is the member"
    );
    let compute = Server::start_config(compute_config("f")).await;
    acked.check(&git, &compute, "ledger").await;
    for (repo, _) in &acked.pushed {
        assert_eq!(common::replicas(&a, repo), vec![id_a], "{repo}");
    }
    // No write to r0 and r2: idle logs without a sequencer must join too.
    push_commits(&git, &acked.pushed[1].1, &compute.repo_url("r1"), 100, 2).await;
    let src = git.init("src3").await;
    push_commits(&git, &src, &compute.repo_url("r3"), 200, 2).await;
    acked.pushed.push(("r3".into(), src));
    acked.api_commit(&compute, "two\n").await;
    acked.check(&git, &compute, "ledger-writes").await;

    // Model 3: two more ledger nodes join, and every log grows onto them.
    let b = ledger(&root.path().join("b"), &store, lb).await;
    let c = ledger(&root.path().join("c"), &store, lc).await;
    let mut ids = vec![
        id_a,
        b.node.consensus().unwrap().id,
        c.node.consensus().unwrap().id,
    ];
    ids.sort_unstable();
    assert!(
        members_become(root.path(), &ids).await,
        "a, b and c are the members"
    );
    let repos: Vec<String> = acked
        .pushed
        .iter()
        .map(|(r, _)| r.clone())
        .chain(["api".to_string()])
        .collect();
    let settled = || {
        [&a, &b, &c]
            .iter()
            .all(|s| repos.iter().all(|r| common::replicas(s, r) == ids))
    };
    assert!(
        wait_until(settled, 60).await,
        "not every log has every member as a replica: {:?}",
        repos
            .iter()
            .map(|r| (r, common::replicas(&b, r)))
            .collect::<Vec<_>>()
    );

    // The original node fails: two of three replicas still commit.
    a.node.shutdown().await;
    drop(a);
    let src2 = &acked.pushed[2].1;
    git.commit_file(src2, "after", "failover\n", "after failover")
        .await;
    push_retrying(&git, src2, &compute.repo_url("r2")).await;
    let src = git.init("src4").await;
    git.commit_file(&src, "f", "four\n", "c4").await;
    push_retrying(&git, &src, &compute.repo_url("r4")).await;
    acked.pushed.push(("r4".into(), src));
    acked.api_commit(&compute, "three\n").await;

    // Every acknowledged write of every phase, through a new compute too.
    acked.check(&git, &compute, "ha").await;
    let fresh = Server::start_config(compute_config("g")).await;
    acked.check(&git, &fresh, "ha-fresh").await;
}
