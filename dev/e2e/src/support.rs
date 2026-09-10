//! Shared helpers for enroute's end-to-end tests: spawning the enroute server,
//! running the system `git` client, and diffing working trees.

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
use std::sync::Arc;
use std::time::Duration;

use object_store::memory::InMemory;
use tokio::sync::OnceCell;
use tracing::Instrument as _;
use tracing_subscriber::Layer as _;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use bench_support::collect::{Collector, KeyedBy};
use enroute::tenancy::Tenants;
use enroute_git_core::RepoId;
use enroute_git_ingest::LocalIngestWorker;
use enroute_git_retrieve::Storage;
use enroute_git_store::Store;
use enroute_git_test_support::{TestCommit, git_command, make_pack};
use enroute_postgres::test_database_url;

/// One ephemeral metadata store, shared across every `init_test()` call.
///
/// `proptest-state-machine` calls `init_test` hundreds of times per run, so
/// isolation comes from a fresh `repo_id` per call rather than a fresh store.
static METADATA_STORE: OnceCell<Storage> = OnceCell::const_new();

/// Who the stub application attributes a granted request to.
///
/// The Basic username in every URL below, which the stub ignores — git
/// requires *some* username to send a password at all.
pub(crate) const ALICE: &str = crate::hooks::ACTOR;

async fn shared_metadata_store() -> Storage {
    METADATA_STORE
        .get_or_init(|| async {
            enroute_postgres::ephemeral(&test_database_url())
                .await
                .expect("provisioning e2e metadata store")
                .0
        })
        .await
        .clone()
}

fn in_memory_store() -> Arc<Store> {
    Arc::new(Store::new(Arc::new(InMemory::new())))
}

pub(crate) async fn make_state() -> Storage {
    Storage {
        store: in_memory_store(),
        ..shared_metadata_store().await
    }
}

/// Like [`make_state`], but on a dedicated ephemeral store.
///
/// The fuzzer's shared store is capped at 1 connection, and a smoke test
/// contending with hundreds of concurrent `init_test()` calls on it timed out.
pub(crate) async fn make_isolated_state() -> Storage {
    make_isolated_state_on_pool().await.0
}

/// The same, with the pool beside it, for a check reading the tables
/// directly.
pub(crate) async fn make_isolated_state_on_pool() -> (Storage, sqlx::PgPool) {
    let (storage, pool) = enroute_postgres::ephemeral(&test_database_url())
        .await
        .expect("provisioning isolated metadata store");
    (
        Storage {
            store: in_memory_store(),
            ..storage
        },
        pool,
    )
}

/// A ledger on a throwaway schema, with real rows and real queries.
///
/// One per caller, and every one pins a Postgres connection: a `pg_temp`
/// schema lives only as long as that connection — see [`Servers`].
pub(crate) async fn ephemeral_ledger() -> sqlx::PgPool {
    enroute::tenancy::ephemeral_pool(&test_database_url())
        .await
        .expect("provisioning an e2e ledger")
}

/// Tenancy from a named list, on a throwaway ledger schema.
///
/// How every deployment is configured: nothing is ever registered, and only
/// which repository is whose is a row.
pub(crate) async fn listed_tenancy(toml: &str) -> Arc<Tenants> {
    let directory = enroute::tenancy::Directory::from_toml(toml).expect("a tenants list");
    Arc::new(Tenants::new(directory, ephemeral_ledger().await))
}

/// Tenancy read from a file on disk, re-read on `every`.
///
/// A real `file://` store and a real timer, because what a refresh has to
/// prove is that a running server picks the change up on its own.
pub(crate) async fn refreshing_tenancy(path: &Path, every: Duration) -> Arc<Tenants> {
    let uri: enroute::ObjectUri = format!("file://{}", path.display())
        .parse()
        .expect("a file URI");
    let (tenants, refreshing) = Tenants::from_uri(&uri, ephemeral_ledger().await, every)
        .await
        .expect("the tenants written above");
    drop(refreshing);
    tenants
}

/// Replace a tenants file the way an operator should: written beside, then
/// renamed, so no reader can see it half-written.
pub(crate) fn rewrite(path: &Path, toml: &str) {
    let beside = path.with_extension("next");
    std::fs::write(&beside, toml).expect("writing the next tenants");
    std::fs::rename(&beside, path).expect("renaming the next tenants into place");
}

/// The tenants a suite names, as the list `--tenants` would spell them, with
/// every one of them claiming `domains`.
pub(crate) fn tenants_file(tenants: &[(&str, &str)], domains: &str) -> String {
    let mut toml = String::new();
    for (id, endpoint) in tenants {
        // Pushed rather than formatted: `format_push_string` is denied, and
        // writing into a String would leave a `Result` nothing can do with.
        toml.push_str("[tenants.");
        toml.push_str(id);
        toml.push_str("]\nhook_endpoint_url = \"");
        toml.push_str(endpoint);
        toml.push_str("\"\n");
        toml.push_str(domains);
        toml.push('\n');
    }
    toml
}

/// The header a contract call names its tenant in, as `--tenant-header`
/// defaults to.
pub(crate) const TENANT_HEADER: &str = "x-enroute-tenant";

/// The same, as the server takes it.
pub(crate) fn tenant_header() -> axum::http::HeaderName {
    axum::http::HeaderName::from_static(TENANT_HEADER)
}

/// The tenant the single-tenant helpers serve.
pub(crate) const E2E_TENANT: &str = "e2e";

/// One tenant, reachable at whatever `Host` a git client sends.
///
/// The servers here listen on loopback with no DNS name, so a `*` claim is
/// what makes any hostname resolve.
pub(crate) async fn one_tenant(hook_endpoint_url: &str) -> Arc<Tenants> {
    let toml = tenants_file(&[(E2E_TENANT, hook_endpoint_url)], "domains = [\"*\"]\n");
    listed_tenancy(&toml).await
}

/// The contract, with a stub application registered behind it.
///
/// The application is there for the tenant to name, not to be asked: the
/// contract door runs no hooks. Tests use this to prove exactly that.
pub(crate) async fn spawn_contract_with_hooks(
    state: Storage,
) -> (std::net::SocketAddr, String, Servers) {
    let (hooks, _token, _landed, hook_servers) = spawn_hooks(&[]).await;
    let tenants = one_tenant(&bench_support::hooks::endpoint_url(&format!(
        "http://{hooks}"
    )))
    .await;

    // No `Hooks` here. The contract door asks an application nothing — the
    // application is the caller — so the stub above exists only to give the
    // tenant an endpoint URL to be named with.
    let services = enroute::grpc::services(state, tenants, local_remotes(), tenant_header());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        enroute::grpc::router(services)
            .unwrap()
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _dropped = stopped.await;
                },
            )
            .await
            .unwrap();
    });

    (
        addr,
        E2E_TENANT.to_string(),
        Servers(vec![stop]).and(hook_servers),
    )
}

/// A sync client allowed to dial the loopback remotes these tests spin up.
pub(crate) fn local_remotes() -> enroute_git_remote::Client {
    enroute_git_remote::Client::new(enroute_git_remote::Reach { private: true })
        .expect("a sync client")
}

/// Serve the `enroute.api.v1alpha1` contract over gRPC on a loopback port.
pub(crate) async fn spawn_enroute(state: Storage, tenants: Arc<Tenants>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // No hooks: these tests are about the contract's own primitives, and the
    // `pre-receive` path has its own coverage over git.
    let services = enroute::grpc::services(state, tenants, local_remotes(), tenant_header());
    tokio::spawn(async move {
        enroute::grpc::router(services)
            .unwrap()
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

/// The key Enroute signs endpoint calls with in these tests.
///
/// The stub application's own, because what is under test is that both ends
/// agree.
pub(crate) use bench_support::hooks::signing_key as endpoint_signing_key;

/// The servers a test spawned, stopped when this is dropped.
///
/// Holding one is not optional: a `pg_temp` schema is pinned to its
/// connection, so a server that never stops starves later `init_test()` calls.
#[must_use = "dropping this stops the servers the test is about to use"]
pub(crate) struct Servers(Vec<tokio::sync::oneshot::Sender<()>>);

impl Servers {
    /// Fold another test's servers into this handle, so one value keeps
    /// everything a test spawned alive.
    pub(crate) fn and(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self
    }
}

/// Serve `router` on `listener` until the returned handle is dropped.
fn serve_until_dropped(listener: tokio::net::TcpListener, router: axum::Router) -> Servers {
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
    Servers(vec![stop])
}

/// A bare git repository served over smart HTTP by real `git`, to push at.
///
/// What `git http-backend` does, by hand — real git on the far end being the
/// point, since a stub would only agree with whatever Enroute sent it.
pub(crate) async fn spawn_git_remote(dir: &Path) -> (std::net::SocketAddr, Servers) {
    git(&["init", "--bare", "--initial-branch=main", "."], Some(dir)).await;

    let advertise = dir.to_path_buf();
    let receive = dir.to_path_buf();
    let router = axum::Router::new()
        .route(
            "/info/refs",
            axum::routing::get(move || {
                let dir = advertise.clone();
                async move { advertise_refs(&dir).await }
            }),
        )
        .route(
            "/git-receive-pack",
            axum::routing::post(move |body: axum::body::Bytes| {
                let dir = receive.clone();
                async move { receive_pack(&dir, &body).await }
            }),
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
    let advertised = tokio::process::Command::new("git")
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
    use tokio::io::AsyncWriteExt as _;

    let mut child = tokio::process::Command::new("git")
        .args(["receive-pack", "--stateless-rpc"])
        .arg(dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("git receive-pack");
    // Written while the output is read, not before it: a push large enough
    // to make receive-pack answer before it has read all of stdin would
    // deadlock the two pipes. The block owns the handle, so it closes when
    // the write does — receive-pack waits for the end of its input.
    let mut stdin = child.stdin.take().expect("a pipe");
    let write = async move {
        stdin.write_all(body).await.expect("writing the push");
    };
    let ((), done) = tokio::join!(write, child.wait_with_output());

    git_response(
        "application/x-git-receive-pack-result",
        done.expect("git receive-pack").stdout,
    )
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

/// Enroute's git front door over `state`, deciding with `authorizer`.
///
/// This reaches storage directly rather than through the contract: Enroute
/// serves git itself, with no second process between pkt-lines and objects.
pub(crate) async fn spawn_git(
    state: Storage,
    authorizer: Arc<dyn enroute_git_http::Authorizer>,
    hooks: Arc<dyn enroute_git_ingest::ReceiveHooks>,
    visibility: Arc<dyn enroute_git_http::RefVisibility>,
) -> (std::net::SocketAddr, Servers) {
    let worker = LocalIngestWorker::shared(state.clone(), Arc::new(InMemory::new()));
    let router = enroute::git::router(state, worker, authorizer, hooks, visibility);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, serve_until_dropped(listener, router))
}

/// What the stub application calls the repository served at `path`.
///
/// Deliberately not `path`: which repository a path means is the whole of what
/// `authorize` is asked, and one string for both would never exercise it.
pub(crate) fn key_for(path: &str) -> String {
    format!("key-{path}")
}

/// A stub application of its own, naming every repository in `repos`.
///
/// A stub because a real application now lives in TypeScript, which a Rust test
/// cannot spawn — this implements the same endpoint protocol instead.
pub(crate) async fn spawn_hooks(
    repos: &[(&str, RepoId)],
) -> (std::net::SocketAddr, String, crate::hooks::Landed, Servers) {
    let named = repos
        .iter()
        .fold(crate::hooks::Repos::default(), |named, (name, _)| {
            named.with(name, key_for(name))
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let landed = crate::hooks::Landed::default();
    let router = crate::hooks::router(
        named,
        landed.clone(),
        vec![endpoint_signing_key().verifying_key()],
        &bench_support::hooks::endpoint_url(&format!("http://{addr}")),
    );
    (
        addr,
        bench_support::hooks::TOKEN.to_string(),
        landed,
        serve_until_dropped(listener, router),
    )
}

/// A git front door over `state`, serving every repository in `repos`.
///
/// Three processes' worth of wiring in one: Enroute, the application behind
/// it, and the git port asking it. Returns the git port and [`ALICE`]'s token.
pub(crate) async fn spawn_server(
    state: Storage,
    repos: &[(&str, RepoId)],
) -> (std::net::SocketAddr, String, crate::hooks::Landed, Servers) {
    let (hooks, token, landed, hook_servers) = spawn_hooks(repos).await;

    // Named once the application has a port, since a tenant is what says where
    // its application answers.
    let tenants = one_tenant(&bench_support::hooks::endpoint_url(&format!(
        "http://{hooks}"
    )))
    .await;
    let tenant = tenants.by_id(E2E_TENANT).expect("the tenant just named");
    // The engine minted these repositories directly, so nothing claimed them
    // on the way through. A real create goes over the contract, which claims
    // as it creates.
    for (name, id) in repos {
        let key = key_for(name).parse().expect("an e2e repository key");
        tenants
            .claim(&tenant, *id, &key)
            .await
            .expect("claiming an e2e repository");
    }

    // One `Hooks` answers every one of a git client's questions — who may reach
    // a repository, which refs it sees, and which may land — as the real binary
    // wires it.
    let hooks = Arc::new(
        // The suite's stub answers at once, so the budget only has to be
        // longer than nothing.
        enroute::hooks::Hooks::new(tenants, endpoint_signing_key(), Duration::from_secs(10))
            .expect("building the hooks endpoint client"),
    );
    let (addr, git_servers) = spawn_git(state, hooks.clone(), hooks.clone(), hooks).await;
    (addr, token, landed, git_servers.and(hook_servers))
}

/// Store every object of `commits` and leave `refname` on the last of them.
///
/// The contract cannot introduce an object, so a test about moving a ref onto
/// one needs this first — the push that stores it, run in-process.
pub(crate) async fn seed_pack(state: &Storage, refname: &str, commits: &[&TestCommit]) {
    let tip = &commits.last().expect("a seed needs a commit").commit_sha;
    let null = gix_hash::ObjectId::null(gix_hash::Kind::Sha1);
    let update = enroute_git_retrieve::RefUpdate {
        refname: refname.to_string(),
        old_id: null,
        new_id: gix_hash::ObjectId::from_hex(tip.as_bytes()).expect("a tip is an object id"),
    };
    let repo = only_repo(state).await;
    let repo_id = repo.id;
    let existing = state
        .rows
        .repo(repo_id)
        .refs_matching(&[refname])
        .await
        .expect("reading the refs this seed names");

    let entries: Vec<_> = commits
        .iter()
        .flat_map(|commit| commit.pack_entries())
        .collect();
    let worker = LocalIngestWorker::shared(state.clone(), Arc::new(InMemory::new()));
    let request = enroute_git_ingest::IngestRequest {
        repo: repo.clone(),
        existing,
        updates: vec![update.clone()],
    };
    let incoming = enroute_git_ingest::IncomingPack {
        reader: Box::new(std::io::Cursor::new(make_pack(&entries))),
        len_hint: None,
    };
    let meter = enroute_git_cost::Meter::new();
    let ingested = worker
        .ingest(
            request,
            incoming,
            &enroute_git_ingest::noop_progress,
            &meter,
        )
        .await
        .expect("ingesting the seed pack");

    let outcomes = enroute_git_ingest::apply_ref_updates(
        state,
        &repo,
        &enroute_git_ingest::Actor::new(ALICE),
        &[update],
        ingested,
        &enroute_git_ingest::NoHooks,
        &enroute_git_ingest::noop_progress,
    )
    .await
    .expect("applying the seed's ref update");
    assert!(
        outcomes
            .outcomes
            .iter()
            .all(|outcome| outcome.result.is_ok()),
        "the seed push has to land: {:?}",
        outcomes.outcomes
    );
}

/// An authorizer that answers every request with one repository, for the
/// tests that are about the git paths rather than about who may reach them.
#[derive(Debug)]
pub(crate) struct OneRepo(pub(crate) RepoId);

#[async_trait::async_trait]
impl enroute_git_http::Authorizer for OneRepo {
    async fn authorize(
        &self,
        _request: &enroute_git_http::GitRequest<'_>,
    ) -> Result<enroute_git_http::Authorized, enroute_git_http::AuthError> {
        Ok(enroute_git_http::Authorized {
            repo: self.0,
            actor: enroute_git_ingest::Actor::new(ALICE),
        })
    }
}

/// Enroute's git front door over `state`, serving `repo` under any name.
pub(crate) async fn spawn_git_for(state: Storage, repo: RepoId) -> (std::net::SocketAddr, Servers) {
    spawn_git(
        state,
        Arc::new(OneRepo(repo)),
        Arc::new(enroute_git_ingest::NoHooks),
        Arc::new(enroute_git_http::AllRefsVisible),
    )
    .await
}

/// The preamble every wire-level smoke test opens with.
///
/// A fresh repo on its own server, and a local checkout whose `origin` points
/// at it. The [`tempfile::TempDir`] comes back so the caller keeps it alive.
pub(crate) async fn spawn_smoke_repo() -> (tempfile::TempDir, PathBuf, crate::hooks::Landed, Servers)
{
    let state = make_isolated_state().await;
    let name = format!("repo-{}", uuid::Uuid::new_v4());
    let repo = state.rows.create(None).await.unwrap();
    let (addr, token, landed, servers) = spawn_server(state, &[(&name, repo.id)]).await;
    let repo = name;
    let url = format!("http://{ALICE}:{token}@{addr}/{repo}.git");

    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().to_path_buf();
    git(&["init", "-b", "main"], Some(&local)).await;
    git(&["remote", "add", "origin", &url], Some(&local)).await;
    (tmp, local, landed, servers)
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
        let mut cmd = tokio::process::Command::from(git_command(args, dir));
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
    let collector = Collector::keyed_by(KeyedBy::Op);
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

/// Serves the `enroute.api.v1alpha1` services on a loopback port, as the one tenant
/// `test-contract-token` authenticates as.
///
/// Every repository these tests create goes over that contract, which
/// claims as it creates — so ownership checks are exercised, not stepped around.
pub(crate) async fn spawn_contract_without_hooks(state: Storage) -> std::net::SocketAddr {
    // No git is served here, so this tenant's application is never called and
    // an address nothing listens on is the honest thing to name.
    let tenants = one_tenant("http://127.0.0.1:1/never-called").await;
    spawn_enroute(state, tenants).await
}

/// Enroute's git front door over `state`, serving `id` under whatever
/// name the test asks for.
///
/// A real deployment asks an application which repository a name means; these
/// tests are about the git paths, so the answer is fixed instead.
pub(crate) async fn front_door_for(state: Storage) -> (std::net::SocketAddr, Servers) {
    let repo = only_repo(&state).await;
    spawn_git_for(state, repo.id).await
}

/// The repository an isolated store holds, for a test that made exactly one.
///
/// A store here is per-test and holds one repository, which spares a caller a
/// tenant handle just to turn a key back into a row.
pub(crate) async fn only_repo(state: &Storage) -> enroute_git_retrieve::RepoMetadata {
    // `all` skips deleted repositories, so nought here is as likely to mean
    // "this test deleted it" as "this test made none".
    let mut held = state.rows.all().await.expect("listing repositories");
    assert_eq!(
        held.len(),
        1,
        "this helper reads the one repository a store holds, and this one holds \
         {} that are not deleted — name the repository instead",
        held.len()
    );
    held.pop().expect("one repository")
}
