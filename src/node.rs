//! A node: the parts its roles share (the object store, the local store),
//! and the roles it runs ([`Role`]).
//!
//! The [`Ledger`] holds the replicated logs, and decides each repository's
//! incarnation and its state in the object store. The [`Compute`] holds views
//! and storage, serves git traffic and does maintenance. A node that runs both
//! calls its ledger in-process. A compute-only node reaches the ledger nodes at
//! its configured URLs over gRPC.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde::Deserialize;

use enroute_compute::LedgerUrls;
use enroute_compute::{Compute, ComputeConfig, HostHooks, RemoteLedger, Storage};
use enroute_ledger::consensus::Engine;
use enroute_ledger::{Ledger, LedgerConfig};
use enroute_shared::auth::Bearer;
use enroute_shared::roles::LedgerRef;
pub use enroute_shared::store::Store;
use enroute_shared::store::{IoClass, IoConfig, IoLimiter};

/// What a node runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Both roles, for a small deployment.
    #[default]
    Both,
    /// The replicated logs only, served on the internal listener alone.
    Ledger,
    /// The git work only, with the ledger's members found in the object
    /// store.
    Compute,
}

impl Role {
    /// The role as a configuration names it.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Role::Both => "both",
            Role::Ledger => "ledger",
            Role::Compute => "compute",
        }
    }

    /// Whether the node runs the ledger.
    #[must_use]
    pub fn ledger(self) -> bool {
        self != Role::Compute
    }

    /// Whether the node runs the compute.
    #[must_use]
    pub fn compute(self) -> bool {
        self != Role::Ledger
    }
}

/// How to start a [`Node`].
#[derive(Clone, Debug)]
pub struct Config {
    /// What the node runs.
    pub role: Role,
    /// Local disk for the log, the state and the layer copies.
    pub data_dir: PathBuf,
    /// Where the objects live.
    pub store: Store,
    /// The node's store request slots, and each IO class's share of them.
    pub io: IoConfig,
    /// The decoded object and entry caches, in bytes.
    pub cache_bytes: usize,
    /// The buffer pool for layer pages, in bytes.
    pub page_cache_bytes: usize,
    /// Workers in the shared CPU pool (0: one per core).
    pub cpu_threads: usize,
    /// Local disk for layer copies, in bytes (0: no limit).
    ///
    /// Chunks load as read and leave under pressure. With no limit, a layer
    /// opened remotely is copied in full in the background.
    pub disk_cache_bytes: u64,
    /// Files the layer storage may keep open (None: the system's soft
    /// limit less a reserve).
    ///
    /// A value above that ceiling is lowered to it.
    pub max_open_files: Option<usize>,
    /// The ledger role.
    pub ledger: LedgerConfig,
    /// The compute role.
    pub compute: ComputeConfig,
}

impl Config {
    /// A standalone node that runs both roles, with every other value at its
    /// default.
    #[must_use]
    pub fn local(data_dir: PathBuf, store: Store) -> Self {
        Config {
            role: Role::default(),
            data_dir,
            store,
            io: IoConfig::default(),
            cache_bytes: 512 << 20,
            page_cache_bytes: 256 << 20,
            cpu_threads: 0,
            disk_cache_bytes: 0,
            max_open_files: None,
            ledger: LedgerConfig::default(),
            compute: ComputeConfig::default(),
        }
    }
}

/// A running node.
pub struct Node {
    /// What it runs.
    pub role: Role,
    ledger: Option<Arc<Ledger>>,
    compute: Option<Arc<Compute>>,
    io: Arc<IoLimiter>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl Node {
    /// Start the roles that `config` names, calling `hooks` from the compute.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store or the local stores cannot be
    /// opened, or the ledger cannot start.
    pub async fn start(config: Config, hooks: HostHooks) -> Result<Arc<Node>> {
        let role = config.role;
        let io = IoLimiter::new(config.io)?;
        let objects = config.store.objects_in(&io, None);
        enroute_shared::format::check(&objects, role.ledger()).await?;
        let ledger = if role.ledger() {
            let ledger = Ledger::open(
                &config.data_dir,
                config.ledger.clone(),
                // Trash markers: what deletions wait on.
                config.store.objects_in(&io, Some(IoClass::Commit)),
            );
            Some(ledger.await?)
        } else {
            None
        };
        let compute = if role.compute() {
            // Its own ledger first, which may not host a repository yet; its
            // peers with the ledger's own token. A compute-only node does not
            // register (nobody dials it), but joins the ledger for a token
            // (see `enroute_compute::join`).
            let api: LedgerRef = if let Some(l) = &ledger {
                with_peers(l, config.ledger.advertise.clone())
            } else {
                let urls = enroute_compute::members(objects.clone());
                let token = enroute_compute::join(urls.clone(), objects.clone());
                Arc::new(RemoteLedger::new(urls, None, Bearer::new(token)))
            };
            enroute_compute::init_cpu_pool(config.cpu_threads);
            let storage = Arc::new(Storage::new(
                objects.clone(),
                config.data_dir.join("local"),
                config.page_cache_bytes,
                config.disk_cache_bytes,
                config.max_open_files,
            )?);
            Some(Compute::start(
                config.compute.clone(),
                hooks,
                objects,
                storage,
                config.cache_bytes,
                api,
                enroute_compute::compute_id(&config.data_dir)?,
            ))
        } else {
            None
        };
        Ok(Arc::new(Node {
            role,
            ledger,
            compute,
            io,
        }))
    }

    /// The consensus engine, on a node that runs the ledger.
    #[must_use]
    pub fn consensus(&self) -> Option<&Arc<Engine>> {
        self.ledger.as_ref().map(|l| &l.engine)
    }

    /// The ledger role, if the node runs it.
    #[must_use]
    pub fn ledger(&self) -> Option<&Arc<Ledger>> {
        self.ledger.as_ref()
    }

    /// The compute role, if the node runs it.
    #[must_use]
    pub fn compute(&self) -> Option<&Arc<Compute>> {
        self.compute.as_ref()
    }

    /// Git's smart HTTP, on a node that runs the compute.
    #[must_use]
    pub(crate) fn git_router(&self) -> Option<axum::Router> {
        self.compute.clone().map(enroute_compute::git_router)
    }

    /// The `enroute.api.v1alpha1` services, on a node that runs the compute.
    ///
    /// A sync pushes to the remotes that `reach` allows.
    ///
    /// # Errors
    ///
    /// Returns an error if the client for remotes cannot be built, or the
    /// compiled-in descriptors do not resolve.
    pub(crate) fn api_routes(
        &self,
        reach: enroute_compute::Reach,
    ) -> Result<Option<tonic::service::Routes>> {
        let Some(compute) = self.compute.clone() else {
            return Ok(None);
        };
        let remotes = enroute_compute::RemoteClient::new(reach)?;
        Ok(Some(enroute_compute::api_router(compute, remotes)?))
    }

    /// `GET /status`, `GET /status/repos/<repo>`, and on a ledger node the
    /// `Peer`, `Ledger`, `Membership` and `Cluster` gRPC services.
    ///
    /// Only other nodes and operators use them, so keep them inside the cluster.
    pub(crate) fn internal_router(&self) -> axum::Router {
        let (ledger, compute, io) = (self.ledger.clone(), self.compute.clone(), self.io.clone());
        let repo_ledger = ledger.clone();
        let status = move || async move {
            let mut s = compute.map_or_else(|| serde_json::json!({}), |f| f.status());
            if let Some(fields) = s.as_object_mut() {
                let consensus = ledger.as_ref().map(|l| l.engine.status());
                fields.insert("consensus".into(), serde_json::json!(consensus));
                fields.insert("store_io".into(), io.status());
            }
            axum::Json(s)
        };
        let router = match &self.ledger {
            Some(l) => {
                use enroute_shared::proto::ledger_server::LedgerServer;
                use enroute_shared::proto::membership_server::MembershipServer;
                let auth = l.clone();
                tonic::service::Routes::new(l.engine.peer_server())
                    .add_service(
                        LedgerServer::from_arc(l.clone())
                            .max_decoding_message_size(enroute_shared::proto::MAX_MESSAGE),
                    )
                    .add_service(MembershipServer::from_arc(l.clone()))
                    .add_service(enroute_ledger::cluster::server(
                        l.engine.clone(),
                        l.objects.clone(),
                    ))
                    .into_axum_router()
                    .layer(axum::middleware::from_fn(move |req, next| {
                        authenticate(auth.clone(), req, next)
                    }))
                    .layer(axum::middleware::from_fn(join_trace))
            }
            None => axum::Router::new(),
        };
        let repo_status = move |axum::extract::Path(repo): axum::extract::Path<String>| async move {
            use axum::http::StatusCode;
            use axum::response::IntoResponse;
            match repo_ledger
                .as_ref()
                .and_then(|l| l.engine.repo_status(&repo))
            {
                Some(s) => axum::Json(s).into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        };
        router
            .route("/status", axum::routing::get(status))
            .route("/status/repos/{*repo}", axum::routing::get(repo_status))
    }

    /// Stop the consensus engine at once, which releases the log store.
    pub fn stop(&self) {
        if let Some(c) = self.consensus() {
            c.shutdown();
        }
    }

    /// Stop, and wait up to 10 s for the ledger to close its stores.
    pub async fn shutdown(&self) {
        self.stop();
        if let Some(engine) = self.consensus() {
            // A timeout only: shutdown goes on either way.
            drop(tokio::time::timeout(Duration::from_secs(10), engine.stopped()).await);
        }
    }
}

/// The ledger API of a node that runs both roles, advertised as `me`.
///
/// Its own ledger first, then the live members, with the ledger's own
/// token. Just its own ledger when it does not replicate.
fn with_peers(ledger: &Arc<Ledger>, me: Option<String>) -> LedgerRef {
    let (Some(token), Some(me)) = (ledger.token(), me) else {
        return ledger.clone();
    };
    let engine = Arc::downgrade(&ledger.engine);
    let peers: LedgerUrls = Arc::new(move || {
        let Some(engine) = engine.upgrade() else {
            return Arc::default();
        };
        let alive = engine.alive();
        let mut urls: Vec<String> = (engine.peers().into_iter())
            .filter(|p| alive.contains(&p.id))
            .map(|p| p.url)
            .collect();
        urls.sort_unstable();
        urls.dedup();
        urls.into()
    });
    Arc::new(RemoteLedger::new(
        peers,
        Some((me, ledger.clone())),
        Bearer::new(token),
    ))
}

/// Serve a gRPC call that carries a trace context in a span of that trace,
/// so that the ledger's spans of a push are in the compute's trace.
///
/// Calls without one, such as `Peer` calls, get no span: they would be traces of
/// their own, one per message.
async fn join_trace(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use tracing::Instrument as _;
    if !enroute_shared::trace_context::carried(req.headers()) {
        return next.run(req).await;
    }
    let span = tracing::info_span!("enroute::grpc::server", rpc.path = req.uri().path());
    enroute_shared::trace_context::adopt(&span, req.headers());
    next.run(req).instrument(span).await
}

/// Let a gRPC call to the ledger through only with a token that `ledger`
/// accepts (an `authorization: Bearer` header).
///
/// `Membership` calls, which earn tokens, need none; `Ledger` calls take
/// any; every other service (`Peer`) needs a ledger node's own.
async fn authenticate(
    ledger: Arc<Ledger>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    use enroute_shared::proto::{ledger_server, membership_server};
    let service = req.uri().path().split('/').nth(1).unwrap_or_default();
    // Joining and listing the members act on what the object store says;
    // removing one takes a ledger node's own token (`members --remove`).
    let open_cluster = service == enroute_ledger::cluster::SERVICE_NAME
        && req.uri().path().rsplit('/').next() != Some("Remove");
    if service == membership_server::SERVICE_NAME || open_cluster {
        return next.run(req).await;
    }
    let ledger_only = service != ledger_server::SERVICE_NAME;
    let token = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let refused = match token {
        None => "no token: join the ledger first (the Membership service)".to_string(),
        Some(t) => match ledger.authenticate(t) {
            // A token a node earned by joining reaches the Ledger service alone.
            Ok(claims) if ledger_only && !claims.is_ledger() => {
                "this service needs a ledger node's own token".to_string()
            }
            Ok(claims) => {
                req.extensions_mut().insert(claims);
                return next.run(req).await;
            }
            Err(e) => e.to_string(),
        },
    };
    tonic::Status::unauthenticated(refused)
        .into_http::<axum::body::Body>()
        .into_response()
}
