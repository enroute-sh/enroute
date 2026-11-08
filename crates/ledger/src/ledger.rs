//! The ledger: repositories' logs, and the root log that says which
//! repositories exist and who the members are (see `consensus::root`). The
//! compute reaches it through the `Ledger` service
//! (`enroute_shared::roles`), which it implements.
//!
//! **Activation.** A request for a repository this node does not host
//! reads the directory, after a read barrier on the root log, so it sees
//! every create and deletion committed before it. A live incarnation is
//! hosted from the initial state the directory implies (its sequencer then
//! catches the log up). Without one, a request that creates the repository
//! proposes its next incarnation to the root, which places it on (up to)
//! three replicas and appoints one sequencer; the replicas host it as they
//! apply the entry. A live incarnation is never taken over: its replicas
//! resume it, and if a majority of them lost their logs, recovering it is
//! an operator's decision (see "Recovery" in
//! `docs/internals/ledger-and-compute.md`).
//!
//! **Deletion** commits a tombstone to the repository's log, writes a
//! trash marker for its layer files (which the compute reclaims), then ends
//! the incarnation in the root log, whose members then drop its log. An
//! incarnation that ended is never hosted again, and a new one has a new log.
//!
//! **Members** are the root log's too, with their addresses and token
//! keys: a node joins and an operator removes one through it (see
//! [`crate::cluster`]), and the root's controller moves the replicas of
//! logs on removed members to others.
//!
//! Nothing here is in the object store but the trash markers and what
//! `crate::cluster` keeps under `cluster/`. Taking a whole ledger down and back up (on other
//! machines, say) is done by stopping its nodes and moving their disks: a
//! node's identity is its log store (see "Freezing the ledger" there).

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use jsonwebtoken::DecodingKey;
use tonic::{Request, Response, Status};
use tracing::Instrument as _;

use enroute_shared::auth::{Bearer, Token};
use enroute_shared::nodes;
use enroute_shared::oid::ObjectFormat;
use enroute_shared::proto::{
    ActivateRequest, ActivateResponse, DeleteRequest, DeleteResponse, DescribeRequest,
    DescribeResponse, ListRequest, ListResponse, PendingRepo, PendingRequest, PendingResponse,
    ReadBarrierRequest, ReadBarrierResponse, RepoSummary, WatchRequest, ledger_server,
};
use enroute_shared::repo_state::{Trash, trash_path};
use enroute_shared::roles::{
    Follow, FollowStream, NotHosted, WatchStream, not_hosted, not_sequencer_status,
};
use enroute_shared::state::{Command, Outcome, Refusal};
use enroute_shared::store::Objects;

use crate::cluster;
use crate::consensus::Located;
use crate::consensus::{Engine, NotSequencer};
use crate::logdb;
use crate::proto::{Delete, Member, root_entry};
use crate::state::store::StateStore;
use crate::token::{self, Claims, Issuer, Refused, Verifier};

/// Settings for a ledger node.
#[derive(Clone, Debug)]
pub struct LedgerConfig {
    /// This node's address for its peers; None: a standalone node, the
    /// only replica of its logs (no registry, no transport).
    pub advertise: Option<String>,
    /// This node's failure domain, such as an availability zone (empty:
    /// none): a repository's log has its replicas in distinct zones where
    /// it can.
    pub zone: String,
    /// Snapshot each log (dropping older entries) every N applied entries.
    pub snapshot_every: u64,
    /// The state store's memory: its block cache and memtables.
    pub state_memory_bytes: usize,
    /// The largest layer whose bytes may travel in the proposal.
    ///
    /// A larger layer must be in the object store before a command names
    /// it. 0: no repository content passes through the ledger.
    pub max_inline_layer_bytes: u64,
    /// Create repositories whose objects are named in SHA-256.
    ///
    /// Off until every node of the deployment runs a release that knows
    /// SHA-256: an older node cannot read such a repository's state.
    pub allow_sha256: bool,
    /// How long the tokens this node gives joining nodes last (see
    /// `Membership` in `proto/ledger.proto`).
    pub token_ttl: Duration,
}

impl Default for LedgerConfig {
    fn default() -> Self {
        LedgerConfig {
            advertise: None,
            zone: String::new(),
            snapshot_every: 1000,
            state_memory_bytes: 512 << 20,
            max_inline_layer_bytes: 1 << 20,
            allow_sha256: false,
            token_ttl: token::TOKEN_TTL,
        }
    }
}

/// A ledger node: its repositories' logs and the root log.
pub struct Ledger {
    /// Its settings.
    pub config: LedgerConfig,
    /// The consensus engine that hosts the logs.
    pub engine: Arc<Engine>,
    /// The object store that holds the trash markers.
    pub objects: Arc<Objects>,
    /// Signs the tokens this node gives.
    issuer: Arc<Issuer>,
    /// Tokens checked, until they expire.
    verifier: Verifier,
    /// The members as this node last wrote them to the object store (see
    /// `cluster::publish`).
    published: tokio::sync::Mutex<Vec<Member>>,
    /// This node's own token, for its calls to its peers (None for a
    /// standalone node).
    token: Option<Token>,
    /// Challenges given and not answered yet (see `crate::membership`).
    pub(crate) challenges: parking_lot::Mutex<HashMap<String, crate::membership::Pending>>,
    /// Activations in progress, one at a time per repository.
    activating: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Repositories whose log this node leads that may need maintenance
    /// (see `Pending` in `ledger.proto`).
    pending: parking_lot::Mutex<HashMap<String, Work>>,
    /// When each compute last asked which repositories it maintains.
    computes: parking_lot::Mutex<HashMap<u64, Instant>>,
    /// Itself, for the subscriptions of a `Watch` call, which outlive it.
    me: std::sync::Weak<Ledger>,
}

impl std::fmt::Debug for Ledger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ledger")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Lifecycle rounds run this often.
const ROUND_EVERY: Duration = Duration::from_secs(1);

/// An error as a status: [`NotSequencer`] is unavailable (the request did not
/// reach the log) and names the sequencer's internal URL, for the compute to ask
/// it; the rest as for any role ([`enroute_shared::roles::status`]).
fn status(e: anyhow::Error) -> Status {
    if e.chain().any(<dyn std::error::Error>::is::<Sha256Off>) {
        return Status::failed_precondition(format!("{e:#}"));
    }
    match e.chain().find_map(|c| c.downcast_ref::<NotSequencer>()) {
        Some(NotSequencer { url, .. }) => not_sequencer_status(format!("{e:#}"), url),
        None => enroute_shared::roles::status(e),
    }
}

impl Ledger {
    /// The error naming node `sequencer` as the repository's sequencer.
    fn not_sequencer(&self, sequencer: u64) -> anyhow::Error {
        NotSequencer {
            sequencer,
            url: self.engine.url(sequencer),
        }
        .into()
    }
}

/// A SHA-256 repository to create on a ledger without `allow_sha256`.
#[derive(Debug)]
struct Sha256Off;

impl std::fmt::Display for Sha256Off {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SHA-256 repositories are off on this ledger (ledger.allow_sha256)")
    }
}

impl std::error::Error for Sha256Off {}

fn not_member() -> anyhow::Error {
    anyhow::Error::new(NotHosted).context("this node is not a member of the ledger")
}

impl Ledger {
    /// Open the ledger on `data_dir` and start it. Its log store's id is the
    /// node's id in the ledger. In a cluster (`advertise`), it announces
    /// itself under that id and joins (see `crate::cluster`); a new
    /// cluster's first node starts the root log.
    ///
    /// Then it runs lifecycle rounds every second.
    ///
    /// # Errors
    ///
    /// Returns an error if the log store, the state store, the object
    /// store or the engine fails to start, or the node was removed.
    pub async fn open(
        data_dir: &Path,
        config: LedgerConfig,
        objects: Arc<Objects>,
    ) -> Result<Arc<Ledger>> {
        let db = logdb::open(&data_dir.join("log"))?;
        let id = logdb::store_id(&db)?;
        let issuer = Arc::new(Issuer::load_or_create(&data_dir.join(token::KEY_FILE), id)?);
        let bootstrap = cluster::bootstraps(&db, &objects, id, config.advertise.is_some()).await?;
        let me = config.advertise.as_ref().map(|url| Member {
            id,
            url: url.clone(),
            key: token::encode_key(&issuer.public()),
            zone: config.zone.clone(),
        });
        if let Some(me) = &me {
            cluster::announce(&objects, me).await?;
        }
        let token = me
            .as_ref()
            .map(|_| token::self_issued(issuer.clone(), config.token_ttl));
        let peers = token
            .clone()
            .map(|t| (crate::consensus::Addresses::default(), Bearer::new(t)));
        let state = Arc::new(StateStore::open(
            &data_dir.join("state"),
            config.state_memory_bytes,
        )?);
        let engine = Engine::start(db, state, config.snapshot_every, id, peers, bootstrap)?;
        if let Some(me) = me {
            tokio::spawn(cluster::join(Arc::downgrade(&engine), objects.clone(), me));
        }
        let ledger = Arc::new_cyclic(|me| Ledger {
            config,
            engine,
            objects,
            issuer,
            verifier: Verifier::default(),
            published: tokio::sync::Mutex::default(),
            token,
            challenges: parking_lot::Mutex::default(),
            activating: parking_lot::Mutex::default(),
            pending: parking_lot::Mutex::default(),
            computes: parking_lot::Mutex::default(),
            me: me.clone(),
        });
        tokio::spawn(lifecycle_loop(Arc::downgrade(&ledger)));
        Ok(ledger)
    }

    /// The key that checks ledger node `id`'s tokens: this node's own, or
    /// a peer's as the directory has it (or as it was introduced, before
    /// this node has the directory).
    fn key_of(&self, id: u64) -> Option<DecodingKey> {
        if id == self.issuer.id() {
            return Some(self.issuer.decoding_key().clone());
        }
        token::decoding_key(&self.engine.known(id)?.key)
    }

    /// Check a token presented to this node (see `Membership` in
    /// `proto/ledger.proto`).
    ///
    /// # Errors
    ///
    /// Returns why the token is refused.
    pub fn authenticate(&self, token: &str) -> Result<Arc<Claims>, Refused> {
        if let Some(checked) = self.verifier.cached(token, token::now()) {
            return checked;
        }
        let id = token::issuer(token).ok_or(token::MALFORMED)?;
        let key = self.key_of(id).ok_or(token::UNKNOWN_ISSUER)?;
        let claims = Arc::new(token::verify(token, id, &key)?);
        self.verifier.insert(token, claims.clone());
        Ok(claims)
    }

    /// A token for `sub`, signed by this node, lasting `token_ttl`.
    #[must_use]
    pub fn issue(&self, sub: &str) -> String {
        self.issuer.issue(sub, self.config.token_ttl)
    }

    /// This node's own token for its peers, which a compute in the same
    /// process presents too (None for a standalone node).
    #[must_use]
    pub fn token(&self) -> Option<Token> {
        self.token.clone()
    }

    /// Whether this node hosts `repo`'s log, and has not applied its
    /// deletion.
    fn live_here(&self, repo: &str) -> bool {
        self.engine.hosted(repo).is_some_and(|h| !h.deleted)
    }

    /// Make `repo` active on this node (hosting its log): host its live
    /// incarnation if this node is a member, or create it (`create`, in
    /// that format, with HEAD `head` if given; see `ActivateRequest`). Returns whether it is
    /// active (false: it does not exist). An explicit create (`head`)
    /// always consults the root, so a node that missed a deletion does not
    /// answer with the deleted repository.
    async fn activate(
        &self,
        repo: &str,
        create: Option<ObjectFormat>,
        head: Option<&str>,
    ) -> Result<bool> {
        if head.is_none() && self.live_here(repo) {
            return Ok(true);
        }
        let lock = self
            .activating
            .lock()
            .entry(repo.to_string())
            .or_default()
            .clone();
        let activated = {
            let _g = lock.lock().await;
            self.activate_now(repo, create, head).await
        };
        let mut activating = self.activating.lock();
        // Ours and the map's: nobody else waits on it.
        if Arc::strong_count(&lock) == 2 {
            activating.remove(repo);
        }
        activated
    }

    async fn activate_now(
        &self,
        repo: &str,
        create: Option<ObjectFormat>,
        head: Option<&str>,
    ) -> Result<bool> {
        let mut root_down = None;
        for attempt in 0..10 {
            if (head.is_none() || attempt > 0) && self.live_here(repo) {
                return Ok(true);
            }
            // A deletion that stopped after its tombstone: finish it first.
            if self.engine.hosted(repo).is_some_and(|h| h.deleted) {
                self.end_incarnation(repo).await?;
            }
            // The root log may be electing a leader: wait for it.
            if let Err(e) = self.engine.root_barrier().await {
                root_down = Some(e);
                continue;
            }
            match self.engine.locate(repo).await? {
                Located::Here => {}
                // Its sequencer serves it.
                Located::Elsewhere(sequencer) => return Err(self.not_sequencer(sequencer)),
                Located::Absent if create.is_none() => return Ok(false),
                Located::Absent if !self.engine.members().0.contains(&self.engine.id) => {
                    return Err(not_member());
                }
                Located::Absent
                    if create == Some(ObjectFormat::Sha256) && !self.config.allow_sha256 =>
                {
                    return Err(Sha256Off.into());
                }
                Located::Absent => {
                    let format =
                        enroute_shared::proto::ObjectFormat::from(create.unwrap_or_default());
                    let create = self
                        .engine
                        .create(repo, head.map(str::to_string), format.into());
                    let entry = root_entry::Change::Create(create).into();
                    root_down = self.engine.propose_root(entry).await.err();
                }
            }
        }
        Err(root_down
            .unwrap_or_else(|| anyhow::anyhow!("the repository kept changing"))
            .context(format!("could not activate {repo}")))
    }

    /// Delete `repo`: commit its tombstone, then end its incarnation (see
    /// `Delete` in `ledger.proto`).
    async fn delete(&self, repo: &str) -> Result<()> {
        for _ in 0..10 {
            self.engine.root_barrier().await?;
            let Some(inc) = self.engine.incarnation(repo).filter(|i| !i.deleted) else {
                return Ok(());
            };
            if let Located::Elsewhere(sequencer) = self.engine.locate(repo).await? {
                return Err(self.not_sequencer(sequencer));
            }
            // Answered once applied here, so the tombstone is in the state
            // this node reads.
            let out = self
                .engine
                .propose(Command::delete(repo).at(inc.epoch))
                .await?;
            match out.refused() {
                None | Some(Refusal::Deleted) => return self.end_incarnation(repo).await,
                // The directory here was behind a newer incarnation.
                Some(Refusal::OtherIncarnation) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Some(Refusal::Invalid | Refusal::StaleUpload | Refusal::Unspecified) => {
                    bail!("deleting {repo}: {}", out.error.unwrap_or_default())
                }
            }
        }
        bail!("could not delete {repo}")
    }

    /// End the incarnation of `repo` whose deletion this node applied: write
    /// the trash marker for its files, then the root entry that ends it
    /// (nodes then drop its log).
    async fn end_incarnation(&self, repo: &str) -> Result<()> {
        let name = repo.to_string();
        let rec = self
            .blocking(move |e| e.record(&name))
            .await?
            .ok_or(NotHosted)?;
        let meta = rec.meta.unwrap_or_default();
        anyhow::ensure!(meta.deleted, "{repo} is not deleted here");
        // Before the incarnation ends: no deletion leaves files that nobody
        // reclaims.
        let trash = Trash {
            upload_gen: meta.upload_gen,
            ..Default::default()
        };
        let data = serde_json::to_vec(&trash)?.into();
        self.objects
            .put_if(&trash_path(repo, rec.epoch), data, None)
            .await?;
        let delete = Delete {
            repo: repo.to_string(),
            epoch: rec.epoch,
            version: meta.version,
            upload_gen: meta.upload_gen,
        };
        self.engine
            .propose_root(root_entry::Change::Delete(delete).into())
            .await
    }

    /// `repo` as of everything committed before the call, if it exists.
    async fn describe(&self, repo: &str) -> Result<Option<RepoSummary>> {
        for _ in 0..3 {
            if !self.activate(repo, None, None).await? {
                return Ok(None);
            }
            match self.engine.read_barrier(repo).await {
                Ok(()) => {}
                // No longer hosted here (it ended): activate again.
                Err(e) if not_hosted(&e) => continue,
                Err(e) => return Err(e),
            }
            let name = repo.to_string();
            // (None: dropped meanwhile; activate again.)
            if let Some(s) = self.blocking(move |e| e.summary(&name)).await? {
                return Ok((!s.deleted).then_some(s));
            }
        }
        bail!("could not describe {repo}")
    }

    /// Run `f` on the engine off the async runtime: it reads the stores.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Engine) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let engine = self.engine.clone();
        tokio::task::spawn_blocking(move || f(&engine)).await?
    }

    /// One round of lifecycle work: write the members to the object store
    /// if they changed (the root's leader does), and track what the
    /// repositories that changed may need.
    async fn lifecycle_round(&self) {
        if self.token.is_some() && self.engine.leads_root() {
            let mut last = self.published.lock().await;
            cluster::publish(&self.engine, &self.objects, &mut last).await;
        }
        self.track_pending(self.engine.take_events()).await;
    }

    /// Whether this node leads `repo`'s live log.
    fn leads(&self, repo: &str) -> bool {
        self.engine
            .hosted(repo)
            .is_some_and(|h| h.leading && !h.deleted)
    }

    /// Note what the `changed` repositories may need, and forget those this
    /// node no longer leads. Only a log's sequencer tracks it: a new sequencer
    /// hears of it as changed.
    async fn track_pending(&self, changed: HashSet<String>) {
        self.pending.lock().retain(|repo, _| self.leads(repo));
        let repos: Vec<String> = changed.into_iter().filter(|r| self.leads(r)).collect();
        if repos.is_empty() {
            return;
        }
        let work = match self.blocking(move |e| work(e, repos)).await {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!("tracking repositories that need maintenance: {e:#}");
                return;
            }
        };
        let mut pending = self.pending.lock();
        for (repo, w) in work {
            match w {
                // The same layers: a settled repository stays settled.
                Some(w) if pending.get(&repo).is_some_and(|p| p.layers == w.layers) => {}
                Some(w) => {
                    pending.insert(repo, w);
                }
                None => {
                    pending.remove(&repo);
                }
            }
        }
    }

    /// Compute `compute` asked now: the computes heard from within
    /// [`nodes::ALIVE`], sorted. In memory only, as a cluster controller's
    /// failure monitor: who maintains what is advisory, since every
    /// maintenance step is a compare-and-swap.
    fn heard(&self, compute: u64) -> Vec<u64> {
        let now = Instant::now();
        let mut heard = self.computes.lock();
        heard.insert(compute, now);
        heard.retain(|_, at| now.duration_since(*at) < nodes::ALIVE);
        let mut live: Vec<u64> = heard.keys().copied().collect();
        live.sort_unstable();
        live
    }

    /// The repositories this node leads that are not settled and whose
    /// first compute in rendezvous order among `computes` is `compute`.
    fn pending(&self, computes: &[u64], compute: u64) -> Vec<PendingRepo> {
        self.pending
            .lock()
            .iter()
            .filter(|(repo, w)| !w.settled && nodes::first(repo, computes) == Some(compute))
            .map(|(repo, w)| PendingRepo {
                repo: repo.clone(),
                version: w.version,
            })
            .collect()
    }

    /// Settle each of `repos` still pending as of the version given.
    fn settle(&self, repos: Vec<PendingRepo>) {
        let mut pending = self.pending.lock();
        for r in repos {
            if let Some(w) = pending.get_mut(&r.repo)
                && w.version == r.version
            {
                w.settled = true;
            }
        }
    }
}

/// Runs the lifecycle rounds until the ledger is gone.
async fn lifecycle_loop(weak: std::sync::Weak<Ledger>) {
    loop {
        tokio::time::sleep(ROUND_EVERY).await;
        let Some(v) = weak.upgrade() else { return };
        v.lifecycle_round().await;
    }
}

/// A repository whose layers may need maintenance, which the compute
/// decides (see `Pending` in `ledger.proto`).
struct Work {
    /// A hash of its layers (ids, and which are inline).
    layers: u64,
    /// Its version, read after its layers.
    version: u64,
    /// A compute found nothing to do on these layers.
    settled: bool,
}

/// What each of `repos` may need (None: nothing, whatever the policy).
fn work(engine: &Engine, repos: Vec<String>) -> Result<Vec<(String, Option<Work>)>> {
    use std::hash::{Hash, Hasher};
    repos
        .into_iter()
        .map(|repo| {
            let layers = engine.layers(&repo)?.unwrap_or_default();
            // After the layers: a compute that settles from a state this new
            // has seen them.
            let version = engine.hosted(&repo).map_or(0, |h| h.version);
            let inline = layers.iter().any(|l| !l.is_stored());
            // Nothing to flush, and a merge takes two layers at least.
            if !inline && layers.len() < 2 {
                return Ok((repo, None));
            }
            let mut h = std::hash::DefaultHasher::new();
            for l in &layers {
                (&l.id, l.is_stored()).hash(&mut h);
            }
            let w = Work {
                layers: h.finish(),
                version,
                settled: false,
            };
            Ok((repo, Some(w)))
        })
        .collect()
}

/// The `Ledger` service (`proto/ledger.proto`): what the compute calls, in
/// this process or over gRPC.
#[tonic::async_trait]
impl ledger_server::Ledger for Ledger {
    #[tracing::instrument(name = "enroute::ledger::activate", skip_all, fields(repo = %req.get_ref().repo))]
    async fn activate(
        &self,
        req: Request<ActivateRequest>,
    ) -> Result<Response<ActivateResponse>, Status> {
        let r = req.into_inner();
        let create = r.create.then(|| r.object_format().into());
        let exists = Ledger::activate(self, &r.repo, create, r.head.as_deref())
            .await
            .map_err(status)?;
        Ok(Response::new(ActivateResponse { exists }))
    }

    #[tracing::instrument(name = "enroute::ledger::delete", skip_all, fields(repo = %req.get_ref().repo))]
    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        Ledger::delete(self, &req.into_inner().repo)
            .await
            .map_err(status)?;
        Ok(Response::new(DeleteResponse {}))
    }

    #[tracing::instrument(name = "enroute::ledger::describe", skip_all, fields(repo = %req.get_ref().repo))]
    async fn describe(
        &self,
        req: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        let repo = Ledger::describe(self, &req.into_inner().repo)
            .await
            .map_err(status)?;
        Ok(Response::new(DescribeResponse { repo }))
    }

    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let r = req.into_inner();
        // Saturating is safe: the limit is clamped.
        let limit = usize::try_from(r.limit)
            .unwrap_or(usize::MAX)
            .clamp(1, 1000);
        // Linearizable, as `describe` is: a follower's directory may lag.
        self.engine.root_barrier().await.map_err(status)?;
        let (repos, more) = self
            .blocking(move |e| e.list(&r.prefix, &r.after, limit))
            .await
            .map_err(status)?;
        Ok(Response::new(ListResponse { repos, more }))
    }

    async fn pending(
        &self,
        req: Request<PendingRequest>,
    ) -> Result<Response<PendingResponse>, Status> {
        let r = req.into_inner();
        self.settle(r.settled);
        let computes = self.heard(r.compute);
        Ok(Response::new(PendingResponse {
            repos: Ledger::pending(self, &computes, r.compute),
            computes,
        }))
    }

    #[tracing::instrument(name = "enroute::ledger::propose", skip_all, fields(repo = %req.get_ref().repo))]
    async fn propose(&self, req: Request<Command>) -> Result<Response<Outcome>, Status> {
        let t0 = Instant::now();
        let sent = enroute_shared::call_timing::sent(&req);
        let cmd = req.into_inner();
        let limit = self.config.max_inline_layer_bytes;
        let out = if cmd
            .largest_inline_layer()
            .is_some_and(|bytes| bytes > limit)
        {
            let mut out = Outcome::default();
            out.refuse_invalid(format!(
                "this ledger takes inline layers of at most {limit} bytes: \
                 upload larger layers to the object store first"
            ));
            out
        } else {
            let replicate = tracing::info_span!("enroute::ledger::replicate");
            self.engine
                .propose(cmd)
                .instrument(replicate)
                .await
                .map_err(status)?
        };
        Ok(enroute_shared::call_timing::stamp(
            Response::new(out),
            sent,
            t0,
        ))
    }

    #[tracing::instrument(name = "enroute::ledger::read_barrier", skip_all, fields(repo = %req.get_ref().repo))]
    async fn read_barrier(
        &self,
        req: Request<ReadBarrierRequest>,
    ) -> Result<Response<ReadBarrierResponse>, Status> {
        let t0 = Instant::now();
        let sent = enroute_shared::call_timing::sent(&req);
        let repo = req.into_inner().repo;
        self.engine.read_barrier(&repo).await.map_err(status)?;
        let version = self.engine.hosted(&repo).map_or(0, |h| h.version);
        Ok(enroute_shared::call_timing::stamp(
            Response::new(ReadBarrierResponse { version }),
            sent,
            t0,
        ))
    }

    type WatchStream = WatchStream;

    async fn watch(
        &self,
        req: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<WatchStream>, Status> {
        let me = self
            .me
            .upgrade()
            .ok_or_else(|| Status::unavailable("the ledger has stopped"))?;
        let expires = req.extensions().get::<Arc<Claims>>().map(|c| c.exp);
        Ok(Response::new(crate::watch::serve(
            me,
            req.into_inner(),
            self.engine.stopping(),
            expires,
        )))
    }
}

#[tonic::async_trait]
impl Follow for Ledger {
    async fn follow(&self, repo: &str) -> Result<FollowStream, Status> {
        use futures::StreamExt;
        let rx = self.engine.follow(repo).await.map_err(status)?;
        let updates =
            futures::stream::unfold(rx, |mut rx| async move { Some((Ok(rx.recv().await?), rx)) });
        Ok(updates.boxed())
    }
}
