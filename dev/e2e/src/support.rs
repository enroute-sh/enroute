//! Shared helpers for enroute's end-to-end tests: starting the deployment
//! under test, running the system `git` client, and diffing working trees.
//!
//! Wire-level only, so the conformance suite names no engine crate.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    reason = "integration test support code, never compiled into the production build"
)]
// Most callers of these helpers are `#[cfg(test)]` modules, so a build without
// `cfg(test)` sees the helpers only those modules use as unused. The allow is
// conditional to keep the lint live in the configuration that can act on it.
#![cfg_attr(
    not(test),
    allow(dead_code, reason = "used only by the `cfg(test)` modules")
)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use tracing::Instrument as _;
use tracing_subscriber::Layer as _;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use test_support::collect::Collector;
pub(crate) use test_support::git::isolated as isolated_git;

use crate::contract::Client;
use crate::engine::{ALL_HOOKS, App, Enroute, PreReceiveMode};

/// Who the stub application attributes a granted request to.
///
/// The Basic username in every URL below, which the stub ignores — git
/// requires *some* username to send a password at all.
pub(crate) const ALICE: &str = crate::hooks::ACTOR;

/// A deployment under test, and the stub application it calls.
///
/// Dropping it stops both.
#[derive(Debug)]
pub(crate) struct Stack {
    pub(crate) deployment: Enroute,
    /// The names the stub resolves, which a test adds to.
    pub(crate) repos: crate::hooks::Repos,
    /// Every push the stub was told had landed.
    pub(crate) landed: crate::hooks::Landed,
    _app: Servers,
}

impl Stack {
    /// An empty deployment, with a stub
    /// application that names no repository.
    pub(crate) async fn start() -> Self {
        Self::start_hooks(&ALL_HOOKS).await
    }

    /// A deployment that calls only the hooks `enabled` names.
    pub(crate) async fn start_hooks(enabled: &[&'static str]) -> Self {
        Self::start_app(enabled, PreReceiveMode::Commands).await
    }

    /// A deployment that calls every hook, with the pushed objects readable
    /// during `pre_receive`.
    pub(crate) async fn start_reading_objects() -> Self {
        Self::start_app(&ALL_HOOKS, PreReceiveMode::Objects).await
    }

    async fn start_app(enabled: &[&'static str], pre_receive_mode: PreReceiveMode) -> Self {
        let repos = crate::hooks::Repos::default();
        let landed = crate::hooks::Landed::default();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint_url = test_support::hooks::endpoint_url(&format!(
            "http://{}",
            listener.local_addr().unwrap()
        ));
        let router = crate::hooks::router(
            repos.clone(),
            landed.clone(),
            vec![endpoint_signing_key().verifying_key()],
            &endpoint_url,
        );
        let app = serve_until_dropped(listener, router);

        let deployment = Enroute::start(Some(App {
            endpoint_url,
            signing_key: endpoint_signing_key(),
            enabled: enabled.to_vec(),
            pre_receive_mode,
        }))
        .await
        .expect("starting the deployment under test");
        Self {
            deployment,
            repos,
            landed,
            _app: app,
        }
    }

    /// A contract client for this deployment.
    pub(crate) async fn client(&self) -> Client {
        Client::connect(self.deployment.grpc_url().to_string())
            .await
            .unwrap()
    }

    /// Create a repository over the contract, and name it `name` to the stub.
    ///
    /// Returns the repository key, which [`key_for`] keeps different from
    /// `name`.
    pub(crate) async fn create(&self, name: &str) -> String {
        self.create_in(name, crate::model::Format::Sha1).await
    }

    /// [`Stack::create`], naming the repository's objects in `format`.
    pub(crate) async fn create_in(&self, name: &str, format: crate::model::Format) -> String {
        use enroute_api::api::v1alpha1::ObjectFormat;
        let key = key_for(name);
        let format = match format {
            crate::model::Format::Sha1 => ObjectFormat::Sha1,
            crate::model::Format::Sha256 => ObjectFormat::Sha256,
        };
        self.client()
            .await
            .create_repository_in(&key, "", format)
            .await
            .unwrap();
        self.repos.name(name, &key);
        key
    }

    /// The git URL the stub resolves as `name`, with [`ALICE`]'s credentials.
    pub(crate) fn url(&self, name: &str) -> String {
        let token = test_support::hooks::TOKEN;
        let base = self
            .deployment
            .git_url()
            .replacen("://", &format!("://{ALICE}:{token}@"), 1);
        format!("{base}/{name}.git")
    }

    /// Store the history of `tip` in `repo`, and leave `refname` on `tip`.
    ///
    /// A test that moves a ref onto objects a git client made pushes them over
    /// git first.
    pub(crate) async fn seed(&self, repo: &str, refname: &str, scratch: &Scratch, tip: &Commit) {
        self.repos.name(repo, repo);
        scratch.push(&self.url(repo), refname, tip).await;
    }

    /// Wait for the deployment's upkeep of `repo` to finish.
    pub(crate) async fn maintain(&self, repo: &str) {
        self.deployment
            .maintain(repo)
            .await
            .expect("maintenance to settle");
    }
}

/// The key Enroute signs endpoint calls with in these tests.
///
/// The stub application's own, because what is under test is that both ends
/// agree.
pub(crate) use test_support::hooks::signing_key as endpoint_signing_key;

/// A server a test spawned, stopped when this is dropped.
#[derive(Debug)]
#[must_use = "dropping this stops the server the test is about to use"]
pub(crate) struct Servers {
    _stop: tokio::sync::oneshot::Sender<()>,
}

impl Servers {
    /// A handle that stops one server when it is dropped.
    pub(crate) fn stopping(stop: tokio::sync::oneshot::Sender<()>) -> Self {
        Self { _stop: stop }
    }
}

/// Serve `router` on `listener` until the returned handle is dropped.
pub(crate) fn serve_until_dropped(
    listener: tokio::net::TcpListener,
    router: axum::Router,
) -> Servers {
    let (stop, stopped) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        axum::serve(listener, router)
            // `Err` when the sender is dropped, which is exactly the signal.
            .with_graceful_shutdown(async move {
                let _dropped = stopped.await;
            })
            .await
            .unwrap();
    });
    Servers::stopping(stop)
}

/// A bare git repository served over smart HTTP by real `git`, to push at
/// and fetch from.
///
/// What `git http-backend` does, by hand — real git on the far end being the
/// point, since a stub would only agree with whatever Enroute sent it.
pub(crate) async fn spawn_git_remote(dir: &Path) -> (std::net::SocketAddr, Servers) {
    git(&["init", "--bare", "--initial-branch=main", "."], Some(dir)).await;

    let advertise = dir.to_path_buf();
    let receive = dir.to_path_buf();
    let upload = dir.to_path_buf();
    let router = axum::Router::new()
        .route(
            "/info/refs",
            axum::routing::get(
                move |query: axum::extract::RawQuery, headers: axum::http::HeaderMap| {
                    let dir = advertise.clone();
                    async move {
                        if query.0.as_deref() == Some("service=git-upload-pack") {
                            upload_pack_capabilities(&dir, &headers).await
                        } else {
                            advertise_refs(&dir).await
                        }
                    }
                },
            ),
        )
        .route(
            "/git-receive-pack",
            axum::routing::post(move |body: axum::body::Bytes| {
                let dir = receive.clone();
                async move { receive_pack(&dir, &body).await }
            }),
        )
        .route(
            "/git-upload-pack",
            axum::routing::post(
                move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                    let dir = upload.clone();
                    async move { upload_pack(&dir, &headers, &body).await }
                },
            ),
        )
        // The same repository behind a redirect, which is what a host that
        // has renamed one answers with.
        .route(
            "/moved/info/refs",
            axum::routing::get(|| async {
                axum::response::Redirect::permanent("/info/refs?service=git-receive-pack")
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, serve_until_dropped(listener, router))
}

/// The advertisement, with the service line smart HTTP puts in front of it.
///
/// `--advertise-refs` writes what the connection itself would; the framing
/// around it belongs to the transport, which here is this function.
async fn advertise_refs(dir: &Path) -> axum::response::Response {
    let advertised = tokio::process::Command::from(isolated_git())
        .args(["receive-pack", "--advertise-refs"])
        .arg(dir)
        .output()
        .await
        .expect("git receive-pack");

    let mut body = Vec::new();
    gix_packetline::blocking_io::encode::data_to_write(b"# service=git-receive-pack\n", &mut body)
        .unwrap();
    gix_packetline::blocking_io::encode::flush_to_write(&mut body).unwrap();
    body.extend_from_slice(&advertised.stdout);

    git_response("application/x-git-receive-pack-advertisement", body)
}

/// One push, handed to `git receive-pack` on its own standard input.
async fn receive_pack(dir: &Path, body: &[u8]) -> axum::response::Response {
    let mut command = tokio::process::Command::from(isolated_git());
    command.args(["receive-pack", "--stateless-rpc"]).arg(dir);
    stateless_rpc(command, body, "application/x-git-receive-pack-result").await
}

/// Run one `--stateless-rpc` request: `body` on standard input, the answer
/// from standard output.
async fn stateless_rpc(
    mut command: tokio::process::Command,
    body: &[u8],
    content_type: &str,
) -> axum::response::Response {
    use tokio::io::AsyncWriteExt as _;

    let mut child = command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("git");
    // Written while the output is read, not before it: a request large
    // enough to make git answer before it has read all of stdin would
    // deadlock the two pipes. The block owns the handle, so it closes when
    // the write does — git waits for the end of its input.
    let mut stdin = child.stdin.take().expect("a pipe");
    let write = async move {
        stdin.write_all(body).await.expect("writing the request");
    };
    let ((), done) = tokio::join!(write, child.wait_with_output());
    git_response(content_type, done.expect("git").stdout)
}

/// `git upload-pack` run as `http-backend` runs it: in the protocol version
/// the request's `Git-Protocol` header asks for.
fn upload_pack_command(dir: &Path, headers: &axum::http::HeaderMap) -> tokio::process::Command {
    let mut command = tokio::process::Command::from(isolated_git());
    command.args(["upload-pack", "--stateless-rpc"]);
    if let Some(protocol) = headers.get("git-protocol") {
        command.env("GIT_PROTOCOL", protocol.to_str().unwrap());
    }
    command.arg(dir);
    command
}

/// The v2 capability advertisement.
///
/// `http-backend` writes no service line in front of it, so neither does this.
async fn upload_pack_capabilities(
    dir: &Path,
    headers: &axum::http::HeaderMap,
) -> axum::response::Response {
    let advertised = upload_pack_command(dir, headers)
        .arg("--advertise-refs")
        .output()
        .await
        .expect("git upload-pack");
    git_response(
        "application/x-git-upload-pack-advertisement",
        advertised.stdout,
    )
}

/// One v2 command, handed to `git upload-pack` on its own standard input.
async fn upload_pack(
    dir: &Path,
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> axum::response::Response {
    stateless_rpc(
        upload_pack_command(dir, headers),
        body,
        "application/x-git-upload-pack-result",
    )
    .await
}

fn git_response(content_type: &str, body: Vec<u8>) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    ([(axum::http::header::CONTENT_TYPE, content_type)], body).into_response()
}

/// Where a ref points in a bare repository, as hex.
pub(crate) async fn remote_ref(dir: &Path, refname: &str) -> Option<String> {
    let (ok, out) = git_allowing_failure(&["rev-parse", refname], dir).await;
    ok.then(|| out.trim().to_string())
}

/// What the stub application calls the repository served at `path`.
///
/// Deliberately not `path`: which repository a path means is the whole of what
/// `authorize` is asked, and one string for both would never exercise it.
pub(crate) fn key_for(path: &str) -> String {
    format!("key-{path}")
}

/// A commit made in a [`Scratch`] repository, by the ids of what it holds.
#[derive(Debug, Clone)]
pub(crate) struct Commit {
    pub(crate) oid: String,
    pub(crate) tree: String,
    /// The one blob, at `hello.txt`.
    pub(crate) blob: String,
}

/// A local repository to make commits in, so a test can name an object
/// before any deployment holds it.
#[derive(Debug)]
pub(crate) struct Scratch(tempfile::TempDir);

impl Scratch {
    pub(crate) async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        git(&["init", "-b", "main"], Some(dir.path())).await;
        Self(dir)
    }

    /// Where the repository is, to run git in.
    pub(crate) fn path(&self) -> &Path {
        self.0.path()
    }

    /// A commit whose tree holds only `body` at `hello.txt`, on `parent` if given.
    pub(crate) async fn commit(
        &self,
        body: &[u8],
        parent: Option<&Commit>,
        message: &str,
    ) -> Commit {
        let dir = self.0.path();
        std::fs::write(dir.join("hello.txt"), body).unwrap();
        git(&["add", "hello.txt"], Some(dir)).await;
        let tree = git(&["write-tree"], Some(dir)).await.trim().to_string();
        let mut args = vec!["commit-tree", tree.as_str(), "-m", message];
        if let Some(parent) = parent {
            args.extend(["-p", parent.oid.as_str()]);
        }
        let oid = git(&args, Some(dir)).await.trim().to_string();
        let blob = git(&["rev-parse", &format!("{tree}:hello.txt")], Some(dir))
            .await
            .trim()
            .to_string();
        Commit { oid, tree, blob }
    }

    /// Push `tip` and its history to `refname` at `url`.
    pub(crate) async fn push(&self, url: &str, refname: &str, tip: &Commit) {
        let refspec = format!("{}:{refname}", tip.oid);
        git(&["push", url, &refspec], Some(self.0.path())).await;
    }
}

/// A repository on its own deployment, and a local checkout whose `origin`
/// points at it.
///
/// The [`tempfile::TempDir`] comes back so the caller keeps it alive.
pub(crate) async fn spawn_smoke_repo() -> (tempfile::TempDir, PathBuf, Stack) {
    let stack = Stack::start().await;
    let (tmp, local, _) = smoke_repo_on(&stack).await;
    (tmp, local, stack)
}

/// [`spawn_smoke_repo`] on `stack`, with the repository key.
pub(crate) async fn smoke_repo_on(stack: &Stack) -> (tempfile::TempDir, PathBuf, String) {
    let name = format!("repo-{}", uuid::Uuid::new_v4());
    let key = stack.create(&name).await;
    let url = stack.url(&name);

    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().to_path_buf();
    git(&["init", "-b", "main"], Some(&local)).await;
    git(&["remote", "add", "origin", &url], Some(&local)).await;
    (tmp, local, key)
}

/// An [`isolated_git`] command for `args`, run in `dir` if one is given.
fn git_command(args: &[&str], dir: Option<&Path>) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::from(isolated_git());
    if let Some(dir) = dir {
        cmd.current_dir(dir);
    }
    cmd.args(args);
    cmd
}

/// Send a `Host` the spawned server would never otherwise see.
///
/// It listens on a loopback address with no DNS name, so this overrides the
/// header git sends while still connecting to the URL's literal address.
pub(crate) fn with_host_override(cmd: &mut tokio::process::Command, host: &str) {
    cmd.env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "http.extraHeader")
        .env("GIT_CONFIG_VALUE_0", format!("Host: {host}"));
}

/// Run a git command asynchronously, asserting success.
///
/// Returns combined stdout+stderr.
pub(crate) async fn git(args: &[&str], dir: Option<&Path>) -> String {
    let out = run_git(args, dir).await;
    let stderr = String::from_utf8_lossy(&out.stderr);
    let combined = combined_output(&out);
    assert!(out.status.success(), "git {args:?} failed:\n{combined}");
    // Real git prints this exact message and silently falls back when it
    // requested a capability (filter, ref-in-want, ...) the server didn't
    // advertise — previously let a broken `filter` advertisement hide
    // behind partial-clone tests passing for the wrong reason.
    assert!(
        !stderr.contains("not recognized by server"),
        "git {args:?} silently ignored an unsupported capability:\n{combined}"
    );
    combined
}

/// Like [`git`], but a nonzero exit is an answer rather than a failure — for
/// the tests where being refused is the thing under test.
///
/// The same environment either way: two runners setting it separately once
/// let one drift from the other, so a refusal was read off the wrong command.
pub(crate) async fn git_allowing_failure(args: &[&str], dir: &Path) -> (bool, String) {
    let out = run_git(args, Some(dir)).await;
    (out.status.success(), combined_output(&out))
}

/// The id of a commit on the branch the stub refuses, pushed and refused.
///
/// The push must fail with the stub's reason.
pub(crate) async fn push_refused_commit(local: &Path) -> String {
    let branch = crate::hooks::PROTECTED
        .strip_prefix("refs/heads/")
        .expect("the stub protects a branch");
    git(&["checkout", "-b", branch], Some(local)).await;
    std::fs::write(local.join("a.txt"), "refused\n").unwrap();
    git(&["add", "."], Some(local)).await;
    git(&["commit", "-m", "refused"], Some(local)).await;
    let commit = git(&["rev-parse", "HEAD"], Some(local)).await;

    let (ok, out) = git_allowing_failure(&["push", "origin", branch], local).await;
    assert!(!ok, "a push the hook refused must fail:\n{out}");
    assert!(
        out.contains("is protected by the stub application"),
        "{out}"
    );
    commit.trim().to_string()
}

fn combined_output(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

async fn run_git(args: &[&str], dir: Option<&Path>) -> std::process::Output {
    let span = tracing::info_span!("git", op = %git_subcommand(args));
    async move {
        // Asynchronous because these clones and pushes talk to a server in
        // this same runtime, which a blocking wait would never let answer.
        let mut cmd = git_command(args, dir);
        with_host_override(&mut cmd, ALICE);
        cmd.output().await.expect("git not found in PATH")
    }
    .instrument(span)
    .await
}

/// Pick out the subcommand (`clone`, `pull`, `log`, ...) from a git argv,
/// skipping leading `-c key=value` overrides.
fn git_subcommand<'a>(args: &'a [&'a str]) -> &'a str {
    let mut iter = args.iter();
    while let Some(&arg) = iter.next() {
        if arg == "-c" {
            iter.next();
            continue;
        }
        if !arg.starts_with('-') {
            return arg;
        }
    }
    "git"
}

/// Read the full commit graph reachable from `main`: one `"<commit> <tree>
/// <parents...>"` line per commit, oldest first.
///
/// Fixed author/committer identity and timestamps make replicated history
/// produce identical oids — a stronger check than comparing working trees.
pub(crate) async fn read_graph(dir: &Path) -> Vec<String> {
    git(
        &["log", "--format=%H %T %P", "--topo-order", "main"],
        Some(dir),
    )
    .await
    .lines()
    .map(str::to_string)
    .collect()
}

/// Read the set of objects reachable from `main` that aren't present locally
/// — the gap a partial-clone filter leaves behind.
///
/// `git rev-list --objects --missing=print` prefixes each missing line with
/// `?`; used to compare a enroute-backed repo against its oracle counterpart.
pub(crate) async fn read_missing_objects(dir: &Path) -> BTreeSet<String> {
    git(
        &["rev-list", "--objects", "--missing=print", "main"],
        Some(dir),
    )
    .await
    .lines()
    .filter_map(|line| line.strip_prefix('?'))
    .map(str::to_string)
    .collect()
}

/// Read every tag ref: one `"<refname> <objectname> <peeled-objectname>"`
/// line per tag, sorted by refname.
///
/// Comparing both columns catches divergence in either the tag's own
/// encoding or the commit it resolves to.
pub(crate) async fn read_tags(dir: &Path) -> Vec<String> {
    git(
        &[
            "for-each-ref",
            "--format=%(refname) %(objectname) %(*objectname)",
            "--sort=refname",
            "refs/tags",
        ],
        Some(dir),
    )
    .await
    .lines()
    .map(str::to_string)
    .collect()
}

/// Handle returned by [`init_tracing`]; call [`TimingReport::print`] once the
/// run is done to see the aggregated breakdown.
pub(crate) struct TimingReport(Collector);

impl TimingReport {
    /// Print a table of operations sorted by total time spent, slowest first.
    pub(crate) fn print(&self) {
        let stats = self.0.snapshot();
        if stats.is_empty() {
            return;
        }
        let mut rows: Vec<_> = stats.iter().collect();
        rows.sort_by_key(|(_, s)| std::cmp::Reverse(s.wall));
        println!("\ntiming summary (slowest first):");
        for (op, s) in rows {
            let avg = s.wall / u32::try_from(s.count).unwrap_or(1);
            println!(
                "  {op:<12} count={:<5} total={:>9.1?} avg={:>8.1?}",
                s.count, s.wall, avg
            );
        }
    }
}

/// Install a tracing subscriber that both aggregates per-operation timing
/// (always on) and, if `RUST_LOG` is set, prints regular log/span events too.
///
/// Keyed by each span's `op` field: one `git` span stands for every
/// subcommand, and the span name alone would file them all together.
pub(crate) fn init_tracing() -> TimingReport {
    let collector = Collector::default();
    tracing_subscriber::registry()
        .with(collector.clone())
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_filter(tracing_subscriber::EnvFilter::from_default_env()),
        )
        .init();
    TimingReport(collector)
}
