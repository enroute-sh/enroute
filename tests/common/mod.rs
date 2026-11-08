#![allow(dead_code, reason = "each test binary uses a different part")]
#![expect(
    clippy::as_conversions,
    clippy::format_push_string,
    clippy::panic,
    clippy::unreadable_literal,
    clippy::unwrap_used,
    clippy::used_underscore_binding,
    reason = "a test fails by panicking, on values it chose itself"
)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::net::TcpListener;

use enroute::node::{Config, Node, Store};
use enroute::{Enroute, HostHooks, Listeners, Reach, Serving};

pub(crate) struct Server {
    /// The git address (smart HTTP).
    pub url: String,
    /// The API's address (gRPC).
    pub api: String,
    /// The address other nodes and operators reach it at.
    pub internal: SocketAddr,
    pub node: Arc<Node>,
    // Fields drop in order: the node stops before its directory goes.
    _serving: Serving,
    /// Its data and store, if they are its own.
    _dir: Option<TempDir>,
}

impl Server {
    pub(crate) async fn start() -> Server {
        Self::start_with(|_| {}).await
    }

    /// A standalone node with a data directory and store of its own.
    pub(crate) async fn start_with(f: impl FnOnce(&mut Config)) -> Server {
        Self::start_hooked(HostHooks::default(), f).await
    }

    /// [`Server::start_with`], calling `hooks`.
    pub(crate) async fn start_hooked(hooks: HostHooks, f: impl FnOnce(&mut Config)) -> Server {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::local(dir.path().join("data"), local_store(dir.path()));
        f(&mut config);
        let mut s = Self::start_inner(config, hooks, None).await;
        s._dir = Some(dir);
        s
    }

    /// A standalone node.
    pub(crate) async fn start_config(config: Config) -> Server {
        Self::start_inner(config, HostHooks::default(), None).await
    }

    /// A cluster node, serving other nodes on `internal` (the address it
    /// advertises).
    pub(crate) async fn start_on(config: Config, internal: std::net::TcpListener) -> Server {
        Self::start_inner(config, HostHooks::default(), Some(internal)).await
    }

    async fn start_inner(
        config: Config,
        hooks: HostHooks,
        internal: Option<std::net::TcpListener>,
    ) -> Server {
        // Another test in this binary may have installed it already.
        drop(
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "warn".into()),
                )
                .with_test_writer()
                .try_init(),
        );
        let loopback = || async { TcpListener::bind("127.0.0.1:0").await.unwrap() };
        let internal = match internal {
            Some(l) => {
                l.set_nonblocking(true).unwrap();
                TcpListener::from_std(l).unwrap()
            }
            None => loopback().await,
        };
        let (api, git) = (loopback().await, loopback().await);
        let addr = |l: &TcpListener| l.local_addr().unwrap();
        let (url, api_at) = (addr(&git), addr(&api));
        let listeners = Listeners {
            api: Some(api),
            git: Some(git),
            internal,
        };
        let internal = addr(&listeners.internal);
        let serving = Enroute::start(config, hooks, Reach::default())
            .await
            .unwrap()
            .spawn(listeners);
        Server {
            url: format!("http://{url}"),
            api: format!("http://{api_at}"),
            internal,
            node: serving.node().clone(),
            _serving: serving,
            _dir: None,
        }
    }

    pub(crate) fn repo_url(&self, name: &str) -> String {
        format!("{}/{}.git", self.url, name)
    }
}

pub(crate) struct Git {
    pub home: TempDir,
}

impl Git {
    pub(crate) fn new() -> Git {
        Git {
            home: tempfile::tempdir().unwrap(),
        }
    }

    pub(crate) fn dir(&self, name: &str) -> PathBuf {
        self.home.path().join(name)
    }

    pub(crate) fn cmd(cwd: &Path, args: &[&str]) -> Command {
        let mut c = test_support::git::isolated();
        c.args(args).current_dir(cwd);
        c
    }

    /// Run git in an async-friendly way (the server runs on the same runtime).
    pub(crate) async fn run(&self, cwd: &Path, args: &[&str]) -> String {
        let (ok, out, err) = self.try_run(cwd, args).await;
        assert!(ok, "git {args:?} failed:\n{err}\n{out}");
        out
    }

    pub(crate) async fn try_run(&self, cwd: &Path, args: &[&str]) -> (bool, String, String) {
        let mut c = Self::cmd(cwd, args);
        let out = tokio::task::spawn_blocking(move || c.output().unwrap())
            .await
            .unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }

    pub(crate) async fn init(&self, name: &str) -> PathBuf {
        self.init_with(name, &[]).await
    }

    /// [`Git::init`], with more `git init` arguments.
    pub(crate) async fn init_with(&self, name: &str, args: &[&str]) -> PathBuf {
        let d = self.dir(name);
        std::fs::create_dir_all(&d).unwrap();
        let mut all = vec!["init", "-q", "-b", "main"];
        all.extend_from_slice(args);
        self.run(&d, &all).await;
        d
    }

    pub(crate) async fn commit_file(
        &self,
        repo: &Path,
        file: &str,
        content: &str,
        msg: &str,
    ) -> String {
        let p = repo.join(file);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, content).unwrap();
        self.run(repo, &["add", "-A"]).await;
        self.run(repo, &["commit", "-q", "-m", msg]).await;
        self.run(repo, &["rev-parse", "HEAD"])
            .await
            .trim()
            .to_string()
    }

    pub(crate) async fn rev(&self, repo: &Path, r: &str) -> String {
        self.run(repo, &["rev-parse", r]).await.trim().to_string()
    }
}

/// Deterministic pseudo-random text.
pub(crate) fn text(seed: u64, lines: usize) -> String {
    let mut x = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let mut s = String::new();
    for i in 0..lines {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.push_str(&format!(
            "line {i} {:x} {}\n",
            x,
            "lorem ipsum dolor sit amet".repeat((x % 4) as usize)
        ));
    }
    s
}

pub(crate) use enroute_api::api::v1alpha1 as api;
use enroute_api::common::v1alpha1::{ObjectId, RepoKey};

/// The API a node serves on its API port: the contract's services.
#[derive(Clone)]
pub(crate) struct Api {
    commits: api::commit_service_client::CommitServiceClient<tonic::transport::Channel>,
    repos: api::repository_service_client::RepositoryServiceClient<tonic::transport::Channel>,
    refs: api::ref_service_client::RefServiceClient<tonic::transport::Channel>,
    objects: api::object_service_client::ObjectServiceClient<tonic::transport::Channel>,
}

/// A client for a node's API at `url` (its API port), connecting lazily.
pub(crate) fn api_client(url: &str) -> Api {
    let channel = tonic::transport::Channel::from_shared(url.to_string())
        .unwrap()
        .connect_lazy();
    Api {
        commits: api::commit_service_client::CommitServiceClient::new(channel.clone()),
        repos: api::repository_service_client::RepositoryServiceClient::new(channel.clone()),
        refs: api::ref_service_client::RefServiceClient::new(channel.clone()),
        objects: api::object_service_client::ObjectServiceClient::new(channel),
    }
}

impl Api {
    /// Create `repo` (again: a repeat is the same create).
    pub(crate) async fn create_repo(&mut self, repo: &str) -> Result<(), tonic::Status> {
        self.repos
            .create_repository(api::CreateRepositoryRequest {
                repo: Some(key(repo)),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Create `repo` naming its objects in `format`.
    pub(crate) async fn create_repo_in(
        &mut self,
        repo: &str,
        format: api::ObjectFormat,
    ) -> Result<api::Repository, tonic::Status> {
        let r = self
            .repos
            .create_repository(api::CreateRepositoryRequest {
                repo: Some(key(repo)),
                object_format: format.into(),
                ..Default::default()
            })
            .await?;
        Ok(r.into_inner().repository.unwrap_or_default())
    }

    pub(crate) async fn create_commit(
        &mut self,
        request: api::CreateCommitRequest,
    ) -> Result<api::CreateCommitResponse, tonic::Status> {
        Ok(self.commits.create_commit(request).await?.into_inner())
    }

    /// `repo`'s refs, as hex by name.
    pub(crate) async fn refs(
        &mut self,
        repo: &str,
    ) -> Result<std::collections::BTreeMap<String, String>, tonic::Status> {
        let listing = self
            .refs
            .list_refs(api::ListRefsRequest {
                repo: Some(key(repo)),
                prefixes: Vec::new(),
            })
            .await?
            .into_inner();
        Ok(listing
            .refs
            .into_iter()
            .map(|r| (r.name, r.object_id.map(|id| id.hex).unwrap_or_default()))
            .collect())
    }

    /// An object's bytes, and its kind.
    async fn object(
        &mut self,
        repo: &str,
        hex: &str,
    ) -> anyhow::Result<(api::ObjectKind, Vec<u8>)> {
        let mut stream = self
            .objects
            .get_object(api::GetObjectRequest {
                repo: Some(key(repo)),
                object_id: Some(ObjectId { hex: hex.into() }),
            })
            .await?
            .into_inner();
        let (mut kind, mut data) = (api::ObjectKind::Unspecified, Vec::new());
        while let Some(message) = stream.message().await? {
            match message.chunk {
                Some(api::get_object_response::Chunk::Header(h)) => kind = h.kind(),
                Some(api::get_object_response::Chunk::Data(d)) => data.extend_from_slice(&d),
                None => anyhow::bail!("an empty chunk"),
            }
        }
        Ok((kind, data))
    }

    /// The file at `path` of `rev` (a ref, a branch's short name or a commit
    /// id), its tags peeled.
    pub(crate) async fn read_file(
        &mut self,
        repo: &str,
        rev: &str,
        path: &str,
    ) -> anyhow::Result<Vec<u8>> {
        let refs = self.refs(repo).await?;
        let mut tip = refs
            .get(rev)
            .or_else(|| refs.get(&format!("refs/heads/{rev}")))
            .cloned()
            .unwrap_or_else(|| rev.to_string());
        while let (api::ObjectKind::Tag, data) = self.object(repo, &tip).await? {
            let text = String::from_utf8(data)?;
            tip = text
                .lines()
                .find_map(|line| line.strip_prefix("object "))
                .ok_or_else(|| anyhow::anyhow!("a tag of nothing"))?
                .to_string();
        }
        let mut stream = self
            .objects
            .list_tree(api::ListTreeRequest {
                repo: Some(key(repo)),
                object_id: Some(ObjectId { hex: tip }),
            })
            .await?
            .into_inner();
        let mut blob = None;
        while let Some(batch) = stream.message().await? {
            blob = blob.or_else(|| {
                batch
                    .entries
                    .into_iter()
                    .find(|e| e.path == path)
                    .and_then(|e| e.object_id)
            });
        }
        let blob = blob.ok_or_else(|| anyhow::anyhow!("no {path} at {rev}"))?;
        Ok(self.object(repo, &blob.hex).await?.1)
    }
}

fn key(repo: &str) -> RepoKey {
    RepoKey { key: repo.into() }
}

/// Write `content` to `path`.
pub(crate) fn put(path: &str, content: impl Into<bytes::Bytes>) -> api::PathChange {
    put_as(path, content, api::FileMode::Regular)
}

/// Write `content` to `path`, as a file of `mode`.
pub(crate) fn put_as(
    path: &str,
    content: impl Into<bytes::Bytes>,
    mode: api::FileMode,
) -> api::PathChange {
    api::PathChange {
        path: path.into(),
        change: Some(api::path_change::Change::Put(api::PutFile {
            content: content.into(),
            mode: mode.into(),
        })),
    }
}

pub(crate) fn delete(path: &str) -> api::PathChange {
    api::PathChange {
        path: path.into(),
        change: Some(api::path_change::Change::Delete(api::DeleteFile {})),
    }
}

/// A commit of `changes` on `branch` (a short name) of `repo`.
pub(crate) fn commit(
    repo: &str,
    branch: &str,
    message: &str,
    changes: Vec<api::PathChange>,
) -> api::CreateCommitRequest {
    api::CreateCommitRequest {
        repo: Some(key(repo)),
        branch: format!("refs/heads/{branch}"),
        message: message.into(),
        author: Some(api::Identity {
            name: "Enroute".into(),
            email: "enroute@localhost".into(),
            timestamp: None,
            utc_offset_seconds: 0,
        }),
        changes,
        ..Default::default()
    }
}

/// `request`, landing only if its branch is at `hex`.
pub(crate) fn expecting(hex: &str, request: api::CreateCommitRequest) -> api::CreateCommitRequest {
    api::CreateCommitRequest {
        expected_old_commit: Some(api::ExpectedOldCommit {
            expectation: Some(api::expected_old_commit::Expectation::CommitId(ObjectId {
                hex: hex.into(),
            })),
        }),
        ..request
    }
}

impl Server {
    pub(crate) fn api_client(&self) -> Api {
        api_client(&self.api)
    }
}

/// An object store that can be taken down, which fails reads and writes.
///
/// Listing and deleting still work.
#[derive(Debug)]
pub(crate) struct Outage {
    inner: Arc<dyn object_store::ObjectStore>,
    down: std::sync::atomic::AtomicBool,
}

impl Outage {
    /// Make `config` use its store through an outage switch.
    pub(crate) fn wrap(config: &mut Config) -> Arc<Outage> {
        let outage = Arc::new(Outage {
            inner: config.store.inner().clone(),
            down: false.into(),
        });
        config.store = Store::new(outage.clone());
        outage
    }

    pub(crate) fn set_down(&self, down: bool) {
        self.down.store(down, std::sync::atomic::Ordering::SeqCst);
    }

    fn check(&self) -> object_store::Result<()> {
        if self.down.load(std::sync::atomic::Ordering::SeqCst) {
            Err(object_store::Error::Generic {
                store: "outage",
                source: "the store is down".into(),
            })
        } else {
            Ok(())
        }
    }
}

impl std::fmt::Display for Outage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Outage({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for Outage {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.check()?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.check()?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.check()?;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.check()?;
        self.inner.copy_opts(from, to, options).await
    }
}

/// A store under `root`.
pub(crate) fn local_store(root: &Path) -> Store {
    Store::local(&root.join("store")).unwrap()
}

/// [`local_store`], as a bucket URI for the CLI.
pub(crate) fn store_uri(root: &Path) -> String {
    format!("file://{}", root.join("store").display())
}

/// Give the node local layer copies of its own, not links to the files of
/// its local store, so a test can damage or evict them.
pub(crate) fn own_copies(mut config: Config) -> Config {
    config.store = Store::new(config.store.inner().clone());
    config
}

/// Poll `f` for up to `secs` seconds; whether it became true.
pub(crate) async fn wait_until(mut f: impl FnMut() -> bool, secs: u64) -> bool {
    for _ in 0..secs * 20 {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// The internal URLs of cluster nodes, for a compute-only node.
pub(crate) fn urls(servers: &[Server]) -> Vec<String> {
    servers
        .iter()
        .map(|s| format!("http://{}", s.internal))
        .collect()
}

/// `n` nodes of a cluster that share `store`, with data under `root`/n<i>.
///
/// Each runs both roles unless `f`, which adjusts node i's config, says otherwise.
pub(crate) async fn cluster(
    root: &Path,
    store: &Store,
    n: usize,
    f: impl Fn(usize, &mut Config),
) -> Vec<Server> {
    // Reserve ports first so every node knows its peers.
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let mut servers = Vec::new();
    for (i, l) in listeners.into_iter().enumerate() {
        let mut c = Config::local(root.join(format!("n{i}")), store.clone());
        c.ledger.advertise = Some(format!("http://{}", l.local_addr().unwrap()));
        f(i, &mut c);
        servers.push(Server::start_on(c, l).await);
    }
    // Nodes become members once they join after they start, and root
    // voters once caught up: wait, so every repository the test creates has
    // them all as replicas and the root keeps a quorum with one node down.
    let ready = || {
        (servers.iter()).all(|s| {
            let c = s.node.consensus().unwrap();
            c.members().0.len() == n && c.root_voters().len() == n
        })
    };
    assert!(
        wait_until(ready, 30).await,
        "not all {n} nodes became members and root voters"
    );
    servers
}

/// `repo`'s replicas as the directory on `server` has them (sorted; none
/// while they change, or if it has no live incarnation).
pub(crate) fn replicas(server: &Server, repo: &str) -> Vec<u64> {
    let mut v = server
        .node
        .consensus()
        .unwrap()
        .replicas(repo)
        .filter(|(_, joint)| joint.is_empty())
        .map(|(r, _)| r)
        .unwrap_or_default();
    v.sort_unstable();
    v
}

/// Wait until every layer of `repo` is in the object store (pushes are
/// acknowledged once in the log; the flush follows).
pub(crate) async fn wait_stored(server: &Server, repo: &str) {
    let stored = || {
        server
            .node
            .consensus()
            .unwrap()
            .repo_meta(repo)
            .is_some_and(|m| {
                m.layers
                    .iter()
                    .all(enroute_shared::proto::LayerMeta::is_stored)
            })
    };
    assert!(
        wait_until(stored, 20).await,
        "layers of {repo} were not flushed"
    );
}

/// Commits `from..from + n` to `src`, each pushed to `url`.
pub(crate) async fn push_commits(git: &Git, src: &Path, url: &str, from: u64, n: u64) {
    for i in from..from + n {
        git.commit_file(src, &format!("f{}", i % 3), &text(i, 40), &format!("c{i}"))
            .await;
        git.run(src, &["push", "-q", url, "main"]).await;
    }
}

/// Clone `url` and check it has `src`'s main, retrying for a while.
///
/// Fetches have no read barrier, so a compute may lag a write through another node.
pub(crate) async fn clone_matches(git: &Git, src: &Path, url: &str, name: &str) {
    let want = git.rev(src, "main").await;
    for attempt in 0.. {
        let dst = git.dir(&format!("{name}-{attempt}"));
        git.run(
            git.home.path(),
            &["clone", "-q", url, dst.to_str().unwrap()],
        )
        .await;
        git.run(&dst, &["fsck", "--strict"]).await;
        // (An empty clone, from a compute that has not seen the push yet.)
        let (_, got, _) = git.try_run(&dst, &["rev-parse", "HEAD"]).await;
        let got = got.trim().to_string();
        if got == want {
            return;
        }
        assert!(attempt < 30, "clone {name} of {url}: {got} is not {want}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub(crate) fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let p = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &p);
        } else {
            std::fs::copy(e.path(), p).unwrap();
        }
    }
}

/// Listen on `addr` again once a stopped server's listener there is gone
/// (its tasks are aborted asynchronously).
pub(crate) async fn rebind(addr: SocketAddr) -> std::net::TcpListener {
    for _ in 0..100 {
        if let Ok(l) = std::net::TcpListener::bind(addr) {
            return l;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{addr} stayed in use");
}

/// `GET /status/repos/<repo>` on `s`, or `None` if it does not host `repo`.
pub(crate) async fn repo_status(s: &Server, repo: &str) -> Option<serde_json::Value> {
    let url = format!("http://{}/status/repos/{repo}", s.internal);
    let r = reqwest::get(url).await.unwrap();
    if r.status() == reqwest::StatusCode::NOT_FOUND {
        return None;
    }
    Some(serde_json::from_slice(&r.bytes().await.unwrap()).unwrap())
}
