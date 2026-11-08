//! Crash safety: real `enroute` processes killed with SIGKILL mid-write.
//!
//! Every acknowledged write must survive on every node.

#![expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::excessive_nesting,
    clippy::panic,
    clippy::too_many_lines,
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::Git;
use parking_lot::Mutex;

struct Proc {
    child: Option<Child>,
    port: u16,
    /// For the API.
    api: u16,
    /// For other nodes (gRPC) and `/status`.
    internal: u16,
    data: std::path::PathBuf,
}

/// The application: every repository name is its own key.
#[derive(Clone)]
struct Open;

impl test_support::hooks::Policy for Open {
    const REALM: &'static str = "crash";
    const ACTOR: &'static str = "crash";

    fn resolve(&self, name: &str) -> Option<String> {
        Some(name.to_string())
    }
}

/// Serve [`Open`] until the test ends, and return its endpoint URL.
async fn application() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url =
        test_support::hooks::endpoint_url(&format!("http://{}", listener.local_addr().unwrap()));
    let router = test_support::hooks::router(
        Open,
        vec![test_support::hooks::signing_key().verifying_key()],
        &url,
    );
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    url
}

/// The deployment file of node `p`.
fn config(p: &Proc, store: &str, endpoint: &str) -> String {
    format!(
        "[bucket]\nuri = \"{store}\"\n\
         [node]\ndata_dir = \"{data}\"\n\
         internal_listen = \"127.0.0.1:{internal}\"\n\
         advertise_url = \"http://127.0.0.1:{internal}\"\n\
         [layers]\nflush_interval = \"300ms\"\n\
         [listen]\ngit = \"127.0.0.1:{git}\"\napi = \"127.0.0.1:{api}\"\n\
         [hooks]\nenabled = [\"authorize\", \"visible_refs\", \"pre_receive\", \"post_receive\"]\nendpoint_url = \"{endpoint}\"\nsigning_key = '''\n{key}'''\n",
        data = p.data.display(),
        internal = p.internal,
        git = p.port,
        api = p.api,
        key = test_support::hooks::SIGNING_KEY_PEM,
    )
}

/// A node process dies with the test, even one that fails.
impl Drop for Proc {
    fn drop(&mut self) {
        kill(self);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn(p: &mut Proc, store: &str, endpoint: &str) {
    let log = std::fs::File::create(p.data.with_extension("log")).unwrap();
    let file = p.data.with_extension("toml");
    std::fs::write(&file, config(p, store, endpoint)).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_enroute"))
        .arg("--config")
        .arg(format!("file://{}", file.display()))
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    p.child = Some(child);
}

fn kill(p: &mut Proc) {
    if let Some(mut c) = p.child.take() {
        drop(c.kill()); // SIGKILL
        drop(c.wait());
    }
}

async fn wait_ready(client: &reqwest::Client, procs: &[Proc]) {
    // Ready: every live node answers (repositories get their sequencers when
    // they become active).
    for _ in 0..300 {
        let mut up = true;
        for p in procs.iter().filter(|p| p.child.is_some()) {
            up &= answers(client, p, "/status").await
                && tokio::net::TcpStream::connect(("127.0.0.1", p.api))
                    .await
                    .is_ok();
        }
        if up {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("cluster did not become ready");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acknowledged_writes_survive_sigkill() {
    let root = tempfile::tempdir().unwrap();
    let store = format!("file://{}", root.path().join("store").display());
    let mut procs: Vec<Proc> = (1..=3)
        .map(|id| Proc {
            child: None,
            port: free_port(),
            api: free_port(),
            internal: free_port(),
            data: root.path().join(format!("n{id}")),
        })
        .collect();
    let endpoint = application().await;
    for p in &mut procs {
        spawn(p, &store, &endpoint);
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    wait_ready(&client, &procs).await;
    let ports: Vec<u16> = procs.iter().map(|p| p.port).collect();
    let api_ports: Vec<u16> = procs.iter().map(|p| p.api).collect();

    // Create the repo through node 1 (it campaigns for, and leads, its log).
    let apis: Vec<_> = api_ports
        .iter()
        .map(|p| common::api_client(&format!("http://127.0.0.1:{p}")))
        .collect();
    let mut first = apis[0].clone();
    first.create_repo("crash").await.unwrap();
    // Every node hosts the log, so one node down leaves its replicas a
    // quorum (a repository made before all joined has fewer at first).
    hosted_everywhere(&client, &procs).await;
    first
        .create_commit(common::commit(
            "crash",
            "main",
            "init",
            vec![common::put("f", "0")],
        ))
        .await
        .unwrap();

    // Writers: one branch each, rotating through the nodes; record the last
    // acknowledged commit per branch.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let acked: Arc<Mutex<Vec<Option<String>>>> = Arc::new(Mutex::new(vec![None; 6]));
    let mut writers = Vec::new();
    for w in 0..6usize {
        let (stop, acked, apis) = (stop.clone(), acked.clone(), apis.clone());
        writers.push(tokio::spawn(async move {
            let mut i = 0u64;
            let (mut ok, mut failed) = (0, 0);
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                i += 1;
                let mut api = apis[(w + i as usize) % 3].clone();
                let req = common::commit(
                    "crash",
                    &format!("w{w}"),
                    &format!("w{w} {i}"),
                    vec![common::put(&format!("w{w}.txt"), format!("{i}\n"))],
                );
                if let Ok(Ok(r)) =
                    tokio::time::timeout(Duration::from_secs(10), api.create_commit(req)).await
                {
                    acked.lock()[w] = Some(r.commit_id.unwrap().hex);
                    ok += 1;
                } else {
                    failed += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            (ok, failed)
        }));
    }

    let phase = |secs: u64| tokio::time::sleep(Duration::from_secs(secs));
    phase(2).await;
    kill(&mut procs[0]); // the repo's sequencer, mid-write
    phase(2).await;
    spawn(&mut procs[0], &store, &endpoint);
    phase(2).await;
    kill(&mut procs[1]);
    phase(1).await;
    spawn(&mut procs[1], &store, &endpoint);
    phase(2).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut total_ok = 0;
    for w in writers {
        let (ok, _failed) = w.await.unwrap();
        total_ok += ok;
    }
    assert!(
        total_ok > 50,
        "too few acknowledged writes ({total_ok}) to mean anything"
    );
    let acked = acked.lock().clone();

    // Every node must converge on refs that contain every acknowledged commit.
    let git = Git::new();
    for (n, port) in ports.iter().enumerate() {
        let url = format!(
            "http://git:{}@127.0.0.1:{port}/crash.git",
            test_support::hooks::TOKEN
        );
        let dst = git.dir(&format!("n{n}"));
        let mut cloned = false;
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            drop(std::fs::remove_dir_all(&dst));
            if git
                .try_run(
                    git.home.path(),
                    &["clone", "-q", "--mirror", &url, dst.to_str().unwrap()],
                )
                .await
                .0
                && all_present(&git, &dst, &acked).await
            {
                cloned = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(cloned, "node {} is missing acknowledged commits", n + 1);
        git.run(&dst, &["fsck", "--strict"]).await;
    }
    for p in &mut procs {
        kill(p);
    }
}

/// Whether `p` answers `GET path` on its internal port with a success.
async fn answers(client: &reqwest::Client, p: &Proc, path: &str) -> bool {
    client
        .get(format!("http://127.0.0.1:{}{path}", p.internal))
        .send()
        .await
        .is_ok_and(|r| r.status().is_success())
}

/// Wait until every node hosts the repository's log.
async fn hosted_everywhere(client: &reqwest::Client, procs: &[Proc]) {
    for _ in 0..300 {
        let mut all = true;
        for p in procs {
            all &= answers(client, p, "/status/repos/crash").await;
        }
        if all {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("not every node came to host the repository's log");
}

/// Each branch contains its last acknowledged commit.
async fn all_present(git: &Git, repo: &Path, acked: &[Option<String>]) -> bool {
    for (w, a) in acked.iter().enumerate() {
        let Some(a) = a else { continue };
        let (ok, _, _) = git
            .try_run(repo, &["merge-base", "--is-ancestor", a, &format!("w{w}")])
            .await;
        if !ok {
            return false;
        }
    }
    true
}
