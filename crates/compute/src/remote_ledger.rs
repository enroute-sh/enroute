//! How a compute reaches the ledger in a cluster: the `Ledger` service
//! (`proto/ledger.proto`) on the ledger nodes it is configured with,
//! as an implementation of the same service ([`RemoteLedger`]), so the
//! compute calls it as it would the ledger in its own process. Any ledger
//! node hosting a repository's log answers its requests, except the ones
//! only the log's sequencer takes (proposals, deletions, read barriers,
//! activations and descriptions): every subscription
//! names the sequencer, and so does a node that refuses one. A node running
//! both roles asks its own ledger first.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::StreamExt;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::Instrument as _;

use enroute_shared::auth::{self, AuthChannel, Bearer};
use enroute_shared::proto::{
    ActivateRequest, ActivateResponse, DeleteRequest, DeleteResponse, DescribeRequest,
    DescribeResponse, ListRequest, ListResponse, PendingRepo, PendingRequest, PendingResponse,
    ReadBarrierRequest, ReadBarrierResponse, RepoSummary, Update, WatchRequest, ledger_client,
    ledger_server, update,
};
use enroute_shared::roles::{
    DynLedger, Follow, FollowStream, LedgerRef, WatchStream, sequencer_named,
};
use enroute_shared::state::{Command, Outcome};

use crate::subscriptions::Mux;

type Client = ledger_client::LedgerClient<AuthChannel>;

/// A client for the ledger node at `url` (its internal address) that
/// presents no token, connecting lazily (and again after failures).
///
/// # Errors
///
/// Returns an error if `url` is not a valid URI.
pub fn client(url: &str) -> Result<ledger_client::LedgerClient<Channel>> {
    Ok(ledger_client::LedgerClient::new(auth::channel(url)?))
}

/// The internal URLs of the ledger nodes, sorted and distinct: as
/// configured, or, on a node that runs both roles, its ledger's peers.
pub type LedgerUrls = Arc<dyn Fn() -> Arc<[String]> + Send + Sync>;

/// One connection to a ledger node: its calls, and the subscriptions of
/// every repository followed on it.
#[derive(Clone)]
struct Link {
    client: Client,
    follows: Arc<Mux>,
}

impl Link {
    fn new(url: &str, bearer: &Bearer) -> Result<Link> {
        let channel = auth::channel(url)?;
        Ok(Link {
            client: bearer.traced().ledger(channel.clone()),
            follows: Arc::new(Mux::new(channel, bearer)),
        })
    }
}

/// The `Ledger` service of the ledger nodes at [`LedgerUrls`].
/// Requests about a repository (proposals, reads, activations, deletions)
/// go to its log's sequencer, as its subscription or a refusing node last
/// named it ([`RemoteLedger::at_sequencer`]); a subscription stays on the
/// repository's [rank](enroute_shared::nodes::rank)-first node that hosts
/// its log. Each moves on while a node cannot serve it: unreachable, or not
/// hosting the repository's log. A proposal is sent again only then, or
/// when refused as not the sequencer, since otherwise it may already be in
/// the log.
pub struct RemoteLedger {
    urls: LedgerUrls,
    /// The ledger in this process, asked first, and its internal URL.
    local: Option<(String, LedgerRef)>,
    /// Nodes that could not serve a call lately, until when they go last.
    down: parking_lot::Mutex<HashMap<String, Instant>>,
    /// The token calls carry.
    bearer: Bearer,
    /// Connections to each ledger node, used in turn: one HTTP/2
    /// connection is driven by one task, which every call to that node
    /// (thousands a second, each proposal carrying its inline layer) then
    /// waits on.
    clients: parking_lot::Mutex<HashMap<String, Vec<Link>>>,
    next: std::sync::atomic::AtomicUsize,
    sequencers: Arc<Sequencers>,
}

/// How long a call waits for the repository's log to have a sequencer that
/// answers: a failover or two.
const SEQUENCER_SETTLES: Duration = Duration::from_secs(5);

/// How long a node that could not serve a call goes last.
const DOWN: Duration = Duration::from_secs(2);

/// How long one attempt at a call waits for its answer: past the ledger's
/// own 30 s wait for a proposal or a read to commit. Past it the call fails
/// and is not sent again, since a proposal may already be in the log.
const CALL_DEADLINE: Duration = Duration::from_secs(35);

/// A request for one attempt at a call, bounded by [`CALL_DEADLINE`].
fn bounded<T>(r: T) -> Request<T> {
    let mut req = enroute_shared::call_timing::send(Request::new(r));
    req.set_timeout(CALL_DEADLINE);
    req
}

/// How long a call waits for this node's first token.
const JOIN_WAIT: Duration = Duration::from_secs(30);

/// Connections per ledger node: one per core, since each connection's
/// encoding and decoding runs on one task.
fn connections() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::thread::available_parallelism().map_or(8, std::num::NonZero::get))
}

/// The connection for one call: the `turn`-th of `cs`, in turn.
#[expect(clippy::indexing_slicing, reason = "connections() is at least one")]
fn pick(cs: &[Link], turn: usize) -> Link {
    cs[turn % cs.len()].clone()
}

impl std::fmt::Debug for RemoteLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteLedger")
            .field("local", &self.local.as_ref().map(|(url, _)| url))
            .finish_non_exhaustive()
    }
}

/// Where a call goes.
enum Node<'a> {
    /// The ledger in this process.
    Local(&'a DynLedger),
    /// Another ledger node.
    Remote(Box<Link>),
}

/// What a node's refusal of a call means for where to send it next.
enum Refusal {
    /// Another node may serve it: this one was unreachable.
    Unreachable,
    /// Another node may serve it: this one does not host the log (yet).
    NotHosted,
    /// Only the log's sequencer takes it: the one this node names (its
    /// internal URL), if it knows one.
    NotSequencer(Option<String>),
    /// No other node would answer otherwise.
    Final,
}

fn refusal(s: &Status) -> Refusal {
    if let Some(sequencer) = sequencer_named(s) {
        Refusal::NotSequencer(named(sequencer))
    } else if enroute_shared::roles::not_hosted_status(s) {
        Refusal::NotHosted
    } else if s.code() == tonic::Code::Unavailable {
        Refusal::Unreachable
    } else {
        Refusal::Final
    }
}

/// The sequencer a refusal or an update names: empty names none.
fn named(url: String) -> Option<String> {
    (!url.is_empty()).then_some(url)
}

/// A followed repository's sequencer, as a subscription or a refusing node
/// last named it.
#[derive(Default)]
struct Hint {
    sequencer: Option<String>,
    /// Its live subscriptions: the hint goes with the last.
    subscriptions: usize,
}

/// The [`Hint`] of each followed repository.
#[derive(Default)]
struct Sequencers(parking_lot::Mutex<HashMap<String, Hint>>);

impl Sequencers {
    fn get(&self, repo: &str) -> Option<String> {
        self.0.lock().get(repo).and_then(|h| h.sequencer.clone())
    }

    fn set(&self, repo: &str, sequencer: Option<String>) {
        if let Some(h) = self.0.lock().get_mut(repo) {
            h.sequencer = sequencer;
        }
    }

    fn follow(&self, repo: &str) {
        self.0
            .lock()
            .entry(repo.to_string())
            .or_default()
            .subscriptions += 1;
    }

    fn unfollow(&self, repo: &str) {
        let mut sequencers = self.0.lock();
        if let Some(h) = sequencers.get_mut(repo) {
            h.subscriptions -= 1;
            if h.subscriptions == 0 {
                sequencers.remove(repo);
            }
        }
    }
}

/// Ends a subscription's share in [`Sequencers`].
struct Unfollow {
    sequencers: Arc<Sequencers>,
    repo: String,
}

impl Drop for Unfollow {
    fn drop(&mut self) {
        self.sequencers.unfollow(&self.repo);
    }
}

impl RemoteLedger {
    /// A ledger that calls the ledger nodes at `urls`, with `bearer`'s
    /// token. A node running both roles gives its own ledger and internal
    /// URL as `local`: it is asked first.
    pub fn new(
        urls: LedgerUrls,
        local: Option<(String, LedgerRef)>,
        bearer: Bearer,
    ) -> RemoteLedger {
        RemoteLedger {
            urls,
            local,
            down: parking_lot::Mutex::default(),
            bearer,
            clients: parking_lot::Mutex::default(),
            next: std::sync::atomic::AtomicUsize::default(),
            sequencers: Arc::default(),
        }
    }

    /// The local ledger, if any.
    fn local(&self) -> Option<&DynLedger> {
        self.local.as_ref().map(|(_, l)| &**l)
    }

    /// The other ledger nodes, once this node has a token to call them
    /// with: in the rank of `repo` if given, else starting at the next node
    /// in turn; nodes that could not serve a call lately go last.
    async fn others(&self, rank_by: Option<&str>) -> Result<Vec<String>, Status> {
        self.joined().await?;
        let me = self.local.as_ref().map(|(url, _)| url.as_str());
        let mut urls: Vec<String> = (self.urls)()
            .iter()
            .filter(|u| Some(u.as_str()) != me)
            .cloned()
            .collect();
        if let Some(repo) = rank_by {
            enroute_shared::nodes::rank(repo, &mut urls);
        } else {
            let turn = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let n = urls.len().max(1);
            urls.rotate_left(turn % n);
        }
        let now = Instant::now();
        {
            let down = self.down.lock();
            if !down.is_empty() {
                urls.sort_by_key(|u| down.get(u).is_some_and(|until| *until > now));
            }
        }
        Ok(urls)
    }

    /// Put the node at `url` last for a while: it could not serve a call.
    fn went_down(&self, url: &str) {
        self.down
            .lock()
            .insert(url.to_string(), Instant::now() + DOWN);
    }

    /// A connection to the ledger node at `url`, the `turn`-th of its
    /// connections.
    fn link(&self, url: &str, turn: usize) -> Result<Link, Status> {
        let mut clients = self.clients.lock();
        if let Some(cs) = clients.get(url) {
            return Ok(pick(cs, turn));
        }
        let cs = (0..connections())
            .map(|_| Link::new(url, &self.bearer))
            .collect::<Result<Vec<_>>>()
            .map_err(|e| Status::invalid_argument(format!("ledger address {url}: {e:#}")))?;
        let c = pick(&cs, turn);
        clients.insert(url.to_string(), cs);
        Ok(c)
    }

    /// Where to reach the ledger node at `url`.
    async fn node(&self, url: &str) -> Result<Node<'_>, Status> {
        if let Some((me, l)) = &self.local
            && me == url
        {
            return Ok(Node::Local(&**l));
        }
        self.joined().await?;
        let turn = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Node::Remote(Box::new(self.link(url, turn)?)))
    }

    /// Wait (a while) for the token: calls before the first join would be
    /// refused.
    async fn joined(&self) -> Result<(), Status> {
        if self.bearer.has_token() {
            return Ok(());
        }
        let mut token = self.bearer.token();
        let wait = token.wait_for(Option::is_some);
        match tokio::time::timeout(JOIN_WAIT, wait).await {
            Ok(Ok(_)) => Ok(()),
            _ => Err(Status::unavailable(
                "this node has not joined the ledger yet",
            )),
        }
    }

    /// Ask the local ledger, then the other ledger nodes (see
    /// [`RemoteLedger::clients`]), until one can answer (see the type).
    async fn each<'a, T, F, Fut>(&'a self, rank_by: Option<&str>, call: F) -> Result<T, Status>
    where
        F: Fn(Node<'a>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let mut last = None;
        if let Some(l) = self.local() {
            match call(Node::Local(l)).await {
                Err(e) if elsewhere(&e) => last = Some(e),
                answer => return answer,
            }
        }
        for url in self.others(rank_by).await? {
            match call(self.node(&url).await?).await {
                Err(e) if matches!(refusal(&e), Refusal::Unreachable) => {
                    self.went_down(&url);
                    last = Some(e);
                }
                Err(e) if elsewhere(&e) => last = Some(e),
                answer => return answer,
            }
        }
        Err(last.unwrap_or_else(|| Status::unavailable("no ledger node is alive")))
    }

    /// Answer a request only the sequencer of `repo`'s log takes with `call`
    /// on it: the sequencer its subscription names, else the nodes in the
    /// repository's rank (the first usually leads; another names the
    /// sequencer). A refusal naming the sequencer is sent there; while no
    /// sequencer is known (a failover), or the named one cannot answer, it
    /// asks again, for [`SEQUENCER_SETTLES`].
    async fn at_sequencer<'a, T, F, Fut>(&'a self, repo: &str, call: F) -> Result<T, Status>
    where
        F: Fn(Node<'a>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let t0 = Instant::now();
        let mut pause = Duration::from_millis(25);
        let mut sequencer = self.sequencers.get(repo);
        let mut first = true;
        loop {
            let answer = match &sequencer {
                Some(url) => match self.node(url).await {
                    Ok(node) => call(node).await,
                    Err(e) => Err(e),
                },
                None => self.each(Some(repo), &call).await,
            };
            let e = match answer {
                Err(e) if !matches!(refusal(&e), Refusal::Final) => e,
                a => return a,
            };
            if t0.elapsed() > SEQUENCER_SETTLES {
                return Err(e);
            }
            // The first redirect goes at once; after that, the sequencer is
            // moving: wait for it to settle.
            if !std::mem::take(&mut first) {
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(Duration::from_millis(400));
            }
            sequencer = match refusal(&e) {
                Refusal::NotSequencer(named) => named,
                _ => None,
            };
            if sequencer.is_some() {
                self.sequencers.set(repo, sequencer.clone());
            }
        }
    }

    /// `stream`, recording the sequencer each update names for `repo`.
    fn hinting(&self, repo: &str, stream: FollowStream) -> FollowStream {
        self.sequencers.follow(repo);
        let share = Unfollow {
            sequencers: self.sequencers.clone(),
            repo: repo.to_string(),
        };
        stream
            .inspect(move |u| {
                if let Ok(Update {
                    kind: Some(update::Kind::Sequencer(url)),
                }) = u
                {
                    share.sequencers.set(&share.repo, named(url.clone()));
                }
            })
            .boxed()
    }
}

/// Whether another ledger node may serve a request this status answered.
fn elsewhere(s: &Status) -> bool {
    matches!(refusal(s), Refusal::Unreachable | Refusal::NotHosted)
}

/// Answer `req` with `$method` at the nodes `$route` picks (see
/// [`RemoteLedger::at_sequencer`]), logging the
/// call's timing as `$call`.
macro_rules! forward {
    ($self:ident, $req:ident, $method:ident, $call:literal, $route:ident) => {{
        let r = $req.into_inner();
        let span = tracing::info_span!("enroute::ledger::call", rpc.method = $call, repo = %r.repo);
        let t0 = std::time::Instant::now();
        let res = $self
            .$route(&r.repo.clone(), |node| {
                let r = r.clone();
                async move {
                    match node {
                        Node::Local(l) => l.$method(Request::new(r)).await,
                        Node::Remote(link) => {
                            let mut c = link.client;
                            c.$method(bounded(r)).await
                        }
                    }
                }
            })
            .instrument(span)
            .await;
        enroute_shared::call_timing::log($call, &res, t0);
        res
    }};
}

/// The answers to `$req` of the local ledger node and every other live
/// one: one that cannot be reached answers nothing, and the call fails as
/// unavailable if none answers.
macro_rules! ask_all {
    ($self:ident, $req:ident, $method:ident) => {{
        let r = $req.into_inner();
        let turn = $self
            .next
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let links = $self
            .others(None)
            .await?
            .iter()
            .map(|url| $self.link(url, turn))
            .collect::<Result<Vec<_>, Status>>()?;
        let asked = links.into_iter().map(|link| {
            let (r, mut c) = (r.clone(), link.client);
            async move { c.$method(bounded(r)).await }
        });
        let local = async {
            match $self.local() {
                Some(l) => Some(l.$method(Request::new(r.clone())).await),
                None => None,
            }
        };
        let (mine, replies) = tokio::join!(local, futures::future::join_all(asked));
        let mut answers = Vec::new();
        if let Some(mine) = mine {
            answers.push(mine?.into_inner());
        }
        for a in replies {
            match a {
                Ok(a) => answers.push(a.into_inner()),
                Err(e) if e.code() == tonic::Code::Unavailable => {}
                Err(e) => return Err(e),
            }
        }
        if answers.is_empty() {
            return Err(Status::unavailable("no ledger node is alive"));
        }
        answers
    }};
}

#[tonic::async_trait]
impl ledger_server::Ledger for RemoteLedger {
    async fn activate(
        &self,
        req: Request<ActivateRequest>,
    ) -> Result<Response<ActivateResponse>, Status> {
        forward!(self, req, activate, "activate", at_sequencer)
    }

    async fn propose(&self, req: Request<Command>) -> Result<Response<Outcome>, Status> {
        forward!(self, req, propose, "propose", at_sequencer)
    }

    async fn read_barrier(
        &self,
        req: Request<ReadBarrierRequest>,
    ) -> Result<Response<ReadBarrierResponse>, Status> {
        forward!(self, req, read_barrier, "read barrier", at_sequencer)
    }

    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        forward!(self, req, delete, "delete", at_sequencer)
    }

    async fn describe(
        &self,
        req: Request<DescribeRequest>,
    ) -> Result<Response<DescribeResponse>, Status> {
        forward!(self, req, describe, "describe", at_sequencer)
    }

    /// Every live ledger node's list, merged: one that lags a create or a
    /// deletion is outvoted by one that has it (the node that answered the
    /// create or the deletion, at least).
    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        Ok(Response::new(merge_lists(ask_all!(self, req, list))))
    }

    /// Every live ledger node's pending repositories: each node settles and
    /// answers for the logs it leads. One that cannot be reached answers nothing; its
    /// logs get new sequencers, which report them.
    async fn pending(
        &self,
        req: Request<PendingRequest>,
    ) -> Result<Response<PendingResponse>, Status> {
        // Two nodes may both report a repository while its sequencer changes:
        // the newer version wins.
        let mut repos: HashMap<String, u64> = HashMap::new();
        // A compute that reaches only some ledger nodes is still alive.
        let mut computes = Vec::new();
        for a in ask_all!(self, req, pending) {
            computes.extend(a.computes);
            for r in a.repos {
                let v = repos.entry(r.repo).or_default();
                *v = (*v).max(r.version);
            }
        }
        computes.sort_unstable();
        computes.dedup();
        Ok(Response::new(PendingResponse {
            repos: repos
                .into_iter()
                .map(|(repo, version)| PendingRepo { repo, version })
                .collect(),
            computes,
        }))
    }

    type WatchStream = WatchStream;

    /// A compute serves no `Watch` calls: it makes them, through [`Follow`].
    async fn watch(
        &self,
        _req: Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<Response<WatchStream>, Status> {
        Err(Status::unimplemented("a compute node does not serve Watch"))
    }
}

#[tonic::async_trait]
impl Follow for RemoteLedger {
    /// The local ledger's follow if it hosts the repository, else a
    /// subscription on the connection's shared `Watch` stream, on the
    /// repository's rank-first node. Either names the log's sequencer, which
    /// proposals then go to.
    async fn follow(&self, repo: &str) -> Result<FollowStream, Status> {
        let stream = self
            .each(Some(repo), |node| async move {
                match node {
                    Node::Local(l) => l.follow(repo).await,
                    Node::Remote(link) => link.follows.follow(repo).await,
                }
            })
            .await?;
        Ok(self.hinting(repo, stream))
    }
}

/// The lists of several ledger nodes as one. A node may lag a create or a
/// deletion, so for each name the most advanced answer wins: the newest
/// incarnation, deleted over live, then the newest version. A node that
/// stopped at its limit knows nothing past its last name, so the merged
/// list stops there too.
fn merge_lists(answers: impl IntoIterator<Item = ListResponse>) -> ListResponse {
    let mut repos = std::collections::BTreeMap::<String, RepoSummary>::new();
    let mut bound: Option<String> = None;
    for a in answers {
        if a.more
            && let Some(last) = a.repos.last()
            && bound.as_ref().is_none_or(|b| last.name < *b)
        {
            bound = Some(last.name.clone());
        }
        for r in a.repos {
            let rank = |r: &RepoSummary| (r.epoch, r.deleted, r.version);
            match repos.get(&r.name) {
                Some(have) if rank(have) >= rank(&r) => {}
                _ => drop(repos.insert(r.name.clone(), r)),
            }
        }
    }
    let repos = repos
        .into_values()
        .filter(|r| bound.as_ref().is_none_or(|b| r.name <= *b))
        .collect();
    ListResponse {
        repos,
        more: bound.is_some(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(name: &str, epoch: u64, deleted: bool) -> RepoSummary {
        RepoSummary {
            name: name.into(),
            epoch,
            deleted,
            ..Default::default()
        }
    }

    /// The most advanced answer wins, and a node that hit its limit bounds
    /// the page.
    #[test]
    fn lists_merge() {
        let a = ListResponse {
            repos: vec![summary("a", 1, true), summary("b", 1, false)],
            more: true,
        };
        let b = ListResponse {
            repos: vec![
                summary("a", 1, false),
                summary("b", 2, false),
                summary("c", 1, false),
            ],
            more: false,
        };
        let m = merge_lists([a, b]);
        assert!(m.more);
        assert_eq!(m.repos, vec![summary("a", 1, true), summary("b", 2, false)]);
        let m = merge_lists([ListResponse {
            repos: vec![summary("x", 1, false)],
            more: false,
        }]);
        assert!(!m.more && m.repos.len() == 1);
    }
}
