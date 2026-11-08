//! The repositories this node follows: the one owner of each one's
//! lifecycle. Activation on the ledger, the subscription to its log
//! ([`roles::follow`]), its latest state, the view built from it, and
//! teardown. A repository is followed on demand ([`States::open`]) and until
//! the compute stops needing it ([`States::evict`], [`States::unfollow`]).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::watch;

use enroute_shared::oid::ObjectFormat;
use enroute_shared::proto::update;
use enroute_shared::roles::{self, LedgerRef};

use crate::meta::RepoState;
use crate::repo::{Repo, Repos};
use crate::util::coalesce::Coalesce;
use crate::util::micros;

/// How fresh a view [`States::open`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consistency {
    /// The latest state this node knows (after it follows the repository,
    /// if it did not).
    Known,
    /// Every write committed before the call.
    Latest,
}

/// How long to wait for a subscription's first state.
const FIRST_STATE: Duration = Duration::from_secs(30);
/// Updates applied (and published) together.
const BATCH: usize = 64;

pub(crate) struct States {
    pub repos: Arc<Repos>,
    ledger: LedgerRef,
    followed: Mutex<HashMap<String, Followed>>,
    /// Repositories with a state published since [`States::take_changed`].
    changed: Mutex<HashSet<String>>,
    next: std::sync::atomic::AtomicU64,
}

struct Followed {
    /// Tells this subscription from a later one for the same repository.
    id: u64,
    /// The latest state (None until the first arrives).
    state: watch::Sender<Option<Arc<RepoState>>>,
    task: tokio::task::AbortHandle,
    /// Read barriers on the log: concurrent linearizable reads share one.
    barrier: Arc<Coalesce<u64>>,
}

impl Drop for Followed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl States {
    pub(crate) fn new(repos: Arc<Repos>, ledger: LedgerRef) -> Arc<States> {
        Arc::new(States {
            repos,
            ledger,
            followed: Mutex::default(),
            changed: Mutex::default(),
            next: std::sync::atomic::AtomicU64::default(),
        })
    }

    /// The latest state of `repo`, if this node follows it (and it is not
    /// deleted).
    pub(crate) fn get(&self, repo: &str) -> Option<Arc<RepoState>> {
        self.followed
            .lock()
            .get(repo)
            .and_then(|f| f.state.borrow().clone())
            .filter(|m| !m.deleted)
    }

    /// Wait until the followed state of `repo` reaches `version` (or says
    /// the repository was deleted), and return it.
    pub(crate) async fn wait_version(&self, repo: &str, version: u64) -> Result<Arc<RepoState>> {
        let (id, rx) = {
            let followed = self.followed.lock();
            let f = followed
                .get(repo)
                .context("stopped following the repository")?;
            (f.id, f.state.subscribe())
        };
        let reached = |s: &Option<Arc<RepoState>>| {
            s.as_ref()
                .is_some_and(|m| m.version >= version || m.deleted)
        };
        let errors = (
            "timed out waiting for the repository's state",
            "stopped following the repository",
        );
        self.wait_for(repo, id, rx, reached, errors).await
    }

    /// The repositories this node follows (with a state, not deleted).
    pub(crate) fn names(&self) -> Vec<String> {
        self.followed
            .lock()
            .iter()
            .filter(|(_, f)| f.state.borrow().as_ref().is_some_and(|m| !m.deleted))
            .map(|(n, _)| n.clone())
            .collect()
    }

    /// Follow `repo` (its log must be live), if this node does not already,
    /// and return its state once there is one (maybe deleted: the
    /// subscription then ends).
    pub(crate) async fn follow(self: &Arc<Self>, repo: &str) -> Result<Arc<RepoState>> {
        let (id, rx) = {
            let mut followed = self.followed.lock();
            if let Some(f) = followed.get(repo) {
                (f.id, f.state.subscribe())
            } else {
                let id = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let (tx, rx) = watch::channel(None);
                let task =
                    tokio::spawn(run(Arc::downgrade(self), repo.to_string(), id, tx.clone()))
                        .abort_handle();
                followed.insert(
                    repo.to_string(),
                    Followed {
                        id,
                        state: tx,
                        task,
                        barrier: Coalesce::new(),
                    },
                );
                (id, rx)
            }
        };
        let errors = (
            "timed out following the repository",
            "could not follow the repository",
        );
        self.wait_for(repo, id, rx, Option::is_some, errors).await
    }

    /// Wait until subscription `id` of `repo` (whose state `rx` watches) is
    /// `reached`, and return the state. `errors` say what timed out, and
    /// what the subscription ending means.
    ///
    /// One that does not get there in time is dropped, so that the next
    /// request follows again instead of waiting on it: it may never get
    /// there, as when its first event or the replica it follows is stuck.
    async fn wait_for(
        &self,
        repo: &str,
        id: u64,
        mut rx: watch::Receiver<Option<Arc<RepoState>>>,
        reached: impl Fn(&Option<Arc<RepoState>>) -> bool,
        (timed_out, ended): (&'static str, &'static str),
    ) -> Result<Arc<RepoState>> {
        let Ok(state) = tokio::time::timeout(FIRST_STATE, rx.wait_for(&reached)).await else {
            self.drop_unless(repo, id, reached);
            anyhow::bail!(timed_out);
        };
        let state = state.context(ended)?;
        #[expect(
            clippy::expect_used,
            reason = "wait_for returns only once a state is set"
        )]
        let state = state.clone().expect("waited for a state");
        Ok(state)
    }

    /// Whether this node follows `repo`'s log.
    pub(crate) fn following(&self, repo: &str) -> bool {
        self.get(repo).is_some()
    }

    /// The view of `repo` as it is, if this node follows it.
    pub(crate) fn current(&self, repo: &str) -> Option<Arc<Repo>> {
        self.repos.get(repo).filter(|_| self.following(repo))
    }

    /// Follow `repo`, making its log live on the ledger first if this node
    /// does not follow it yet. None: it does not exist.
    pub(crate) async fn follow_active(
        self: &Arc<Self>,
        repo: &str,
    ) -> Result<Option<Arc<RepoState>>> {
        if !self.following(repo) && !roles::activate(&*self.ledger, repo, None).await? {
            return Ok(None);
        }
        Ok(Some(self.follow(repo).await?))
    }

    /// Make `repo`'s log live (creating the repository with `create`), and
    /// open it as of everything committed before the call. None: it does
    /// not exist.
    pub(crate) async fn ensure(
        self: &Arc<Self>,
        repo: &str,
        create: Option<ObjectFormat>,
    ) -> Result<Option<Arc<Repo>>> {
        if !roles::activate(&*self.ledger, repo, create).await? {
            return Ok(None);
        }
        self.follow(repo).await?;
        self.open(repo, Consistency::Latest).await
    }

    /// The view of `name`, at least as fresh as `consistency` asks. None:
    /// it does not exist.
    pub(crate) async fn open(
        self: &Arc<Self>,
        name: &str,
        consistency: Consistency,
    ) -> Result<Option<Arc<Repo>>> {
        let meta = match consistency {
            Consistency::Known => match self.get(name) {
                Some(meta) => Some(meta),
                None => self.follow_active(name).await?,
            },
            Consistency::Latest => self.latest(name, self.following(name)).await?,
        };
        let Some(meta) = meta.filter(|m| !m.deleted) else {
            return Ok(None);
        };
        let repo = self.repos.handle(name);
        // The view may have gone after the subscription published this state
        // (eviction, a new incarnation): ask for it again.
        if repo.snapshot().version() < meta.version {
            self.repos.on_apply(name, &meta);
        }
        repo.wait_version(meta.version).await?;
        repo.touch();
        Ok(Some(repo))
    }

    /// The version of `name` as of everything committed to its log before
    /// the call. Concurrent calls share one request to the ledger, as in
    /// etcd's linearizable read loop ([`Coalesce`]).
    async fn read_barrier(&self, name: &str) -> Result<u64> {
        let barrier = self.followed.lock().get(name).map(|f| f.barrier.clone());
        let Some(barrier) = barrier else {
            return roles::read_barrier(&*self.ledger, name).await;
        };
        let ask = || {
            let (ledger, repo) = (self.ledger.clone(), name.to_string());
            async move { roles::read_barrier(&*ledger, &repo).await }
        };
        Ok(barrier.call(ask).await?)
    }

    /// `name`'s state as of everything committed before the call;
    /// `followed`: this node follows it, so its log is live. None: it does
    /// not exist.
    #[expect(
        clippy::unwrap_used,
        reason = "the instants are taken in order, so the differences cannot be negative"
    )]
    async fn latest(
        self: &Arc<Self>,
        name: &str,
        followed: bool,
    ) -> Result<Option<Arc<RepoState>>> {
        let t0 = std::time::Instant::now();
        // A live subscription means an active log: the read needs only the
        // barrier (a round trip saved on every push's ref advertisement).
        let Some(mut meta) = self.follow_active(name).await? else {
            return Ok(None);
        };
        if meta.deleted {
            return Ok(None);
        }
        let t_follow = t0.elapsed();
        // Everything committed so far: wait for this node's subscription to
        // reach it.
        let version = match self.read_barrier(name).await {
            Ok(v) => v,
            // The ledger node no longer hosts it (it restarted, or the
            // incarnation was superseded): activate it again.
            Err(e) if followed && roles::not_hosted(&e) => {
                if !roles::activate(&*self.ledger, name, None).await? {
                    return Ok(None);
                }
                meta = self.follow(name).await?;
                self.read_barrier(name).await?
            }
            Err(e) => return Err(e),
        };
        let t_barrier = t0.elapsed();
        if meta.version < version {
            meta = self.wait_version(name, version).await?;
        }
        tracing::debug!(
            target: "enroute::read_timing",
            follow_us = micros(t_follow),
            barrier_us = micros(t_barrier.checked_sub(t_follow).unwrap()),
            catch_up_us = micros(t0.elapsed().checked_sub(t_barrier).unwrap()),
            "linearizable read timing"
        );
        Ok(Some(meta))
    }

    /// Stop following `repo`, and drop its view.
    pub(crate) fn unfollow(&self, repo: &str) {
        self.followed.lock().remove(repo);
        self.repos.remove(repo);
    }

    /// Drop the views of repositories not used for `idle`, and stop
    /// following them; but not those that `keep` keeps. Returns how many
    /// went.
    pub(crate) fn evict(&self, idle: Duration, keep: impl Fn(&str) -> bool) -> usize {
        let evicted = self.repos.evict(idle, keep);
        let mut followed = self.followed.lock();
        for name in &evicted {
            followed.remove(name);
        }
        evicted.len()
    }

    /// The repositories with a state published since the last call.
    pub(crate) fn take_changed(&self) -> HashSet<String> {
        std::mem::take(&mut *self.changed.lock())
    }

    /// Drop the subscription `id` of `repo` (aborting its task), unless a
    /// later one replaced it or its state is `kept`.
    fn drop_unless(&self, repo: &str, id: u64, kept: impl Fn(&Option<Arc<RepoState>>) -> bool) {
        let mut followed = self.followed.lock();
        if followed
            .get(repo)
            .is_some_and(|f| f.id == id && !kept(&f.state.borrow()))
        {
            followed.remove(repo);
        }
    }

    /// The subscription `id` of `repo` is over (dropping its last sender).
    fn ended(&self, repo: &str, id: u64) {
        self.drop_unless(repo, id, |_| false);
    }
}

/// Apply a batch of updates to `cur`. False if the stream broke (skipping
/// an update would diverge from the log): the caller follows again, from a
/// new state.
fn apply_batch(
    cur: &mut Option<Arc<RepoState>>,
    batch: Vec<Result<enroute_shared::proto::Update>>,
    repo: &str,
) -> bool {
    for u in batch {
        let u = match u {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(repo, "following the repository: {e:#}");
                return false;
            }
        };
        match u.kind {
            Some(update::Kind::State(s)) => *cur = Some(Arc::new(s.into())),
            Some(update::Kind::Change(c)) => {
                if let Some(m) = cur.as_mut() {
                    // Copies only if a reader still holds the last published
                    // state (at most once per batch), and then shares its refs.
                    Arc::make_mut(m).apply_change(&c);
                }
            }
            // Where to send proposals: not part of the state.
            Some(update::Kind::Sequencer(_)) | None => {}
        }
    }
    true
}

/// Publish `cur` to the subscribers and the view. False if it is a
/// deletion, which ends the subscription.
fn publish(
    tx: &watch::Sender<Option<Arc<RepoState>>>,
    repos: &Arc<Repos>,
    repo: &str,
    cur: Option<&Arc<RepoState>>,
    failures: &mut u32,
) -> bool {
    let Some(m) = cur else { return true };
    let epoch = tx.borrow().as_ref().map(|p| p.epoch);
    if m.deleted || epoch.is_some_and(|e| e != m.epoch) {
        repos.remove(repo);
    }
    tx.send_replace(Some(m.clone()));
    if m.deleted {
        return false;
    }
    repos.on_apply(repo, m);
    *failures = 0;
    true
}

/// Follow `repo` until this node stops following it (which aborts this
/// task), publishing every state to `tx` and to the repository's view. A
/// stream that ends (a ledger node went away, the incarnation moved) is
/// followed again. A deletion ends it, and the
/// view with it; so does a new incarnation, whose view starts afresh.
async fn run(
    states: Weak<States>,
    repo: String,
    id: u64,
    tx: watch::Sender<Option<Arc<RepoState>>>,
) {
    let mut failures: u32 = 0;
    'follow: loop {
        let Some(s) = states.upgrade() else { return };
        let updates = match roles::follow(&*s.ledger, &repo).await {
            Ok(u) => u,
            Err(e) => {
                failures += 1;
                if failures > 5 {
                    tracing::warn!(repo, "following the repository: {e:#}");
                    break;
                }
                // Not active on that ledger node: (re)activate it.
                if let Ok(false) = roles::activate(&*s.ledger, &repo, None).await {
                    break; // deleted
                }
                drop(s);
                tokio::time::sleep(Duration::from_millis(100 << failures)).await;
                continue;
            }
        };
        let repos = s.repos.clone();
        drop(s);
        let mut cur: Option<Arc<RepoState>> = None;
        let mut batches = updates.ready_chunks(BATCH);
        let mut broken = false;
        while let Some(batch) = batches.next().await {
            if !apply_batch(&mut cur, batch, &repo) {
                broken = true;
            }
            let live = publish(&tx, &repos, &repo, cur.as_ref(), &mut failures);
            if let Some(s) = states.upgrade() {
                s.changed.lock().insert(repo.clone());
            }
            if !live {
                break 'follow;
            }
            if broken {
                break;
            }
        }
    }
    if let Some(s) = states.upgrade() {
        s.ended(&repo, id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use tonic::{Request, Response, Status};

    use enroute_shared::proto::{
        ActivateRequest, ActivateResponse, DeleteRequest, DeleteResponse, DescribeRequest,
        DescribeResponse, ListRequest, ListResponse, PendingRequest, PendingResponse,
        ReadBarrierRequest, ReadBarrierResponse, Update, WatchRequest, ledger_server,
    };
    use enroute_shared::roles::{Follow, FollowStream, WatchStream};
    use enroute_shared::state::{Command, Outcome};

    use super::*;
    use crate::storage::Storage;

    /// A ledger whose subscriptions start at version `serves`, and send
    /// nothing more; with `serves` at 0 they never send their first state.
    #[derive(Default)]
    struct Stuck {
        serves: AtomicU64,
        follows: AtomicU64,
    }

    #[tonic::async_trait]
    impl ledger_server::Ledger for Stuck {
        async fn activate(
            &self,
            _req: Request<ActivateRequest>,
        ) -> Result<Response<ActivateResponse>, Status> {
            Err(Status::unimplemented("activate"))
        }

        async fn propose(&self, _req: Request<Command>) -> Result<Response<Outcome>, Status> {
            Err(Status::unimplemented("propose"))
        }

        async fn read_barrier(
            &self,
            _req: Request<ReadBarrierRequest>,
        ) -> Result<Response<ReadBarrierResponse>, Status> {
            Err(Status::unimplemented("read_barrier"))
        }

        async fn delete(
            &self,
            _req: Request<DeleteRequest>,
        ) -> Result<Response<DeleteResponse>, Status> {
            Err(Status::unimplemented("delete"))
        }

        async fn describe(
            &self,
            _req: Request<DescribeRequest>,
        ) -> Result<Response<DescribeResponse>, Status> {
            Err(Status::unimplemented("describe"))
        }

        async fn list(&self, _req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
            Err(Status::unimplemented("list"))
        }

        async fn pending(
            &self,
            _req: Request<PendingRequest>,
        ) -> Result<Response<PendingResponse>, Status> {
            Err(Status::unimplemented("pending"))
        }

        type WatchStream = WatchStream;

        async fn watch(
            &self,
            _req: Request<tonic::Streaming<WatchRequest>>,
        ) -> Result<Response<WatchStream>, Status> {
            Err(Status::unimplemented("watch"))
        }
    }

    #[tonic::async_trait]
    impl Follow for Stuck {
        async fn follow(&self, _repo: &str) -> Result<FollowStream, Status> {
            self.follows.fetch_add(1, Ordering::Relaxed);
            let version = self.serves.load(Ordering::Relaxed);
            if version == 0 {
                // As a first event that never comes.
                std::future::pending::<()>().await;
            }
            let state = enroute_shared::state::RepoMeta {
                version,
                ..enroute_shared::state::RepoMeta::new()
            };
            let first = Update {
                kind: Some(update::Kind::State(state)),
            };
            Ok(futures::stream::once(async { Ok(first) })
                .chain(futures::stream::pending())
                .boxed())
        }
    }

    /// States over `ledger`, with repositories in a directory of their own.
    fn states(ledger: &Arc<Stuck>) -> (tempfile::TempDir, Arc<States>) {
        let dir = tempfile::tempdir().unwrap();
        let repos = Repos::new(Storage::for_test(dir.path()), 16 << 20);
        let ledger: LedgerRef = ledger.clone();
        (dir, States::new(repos, ledger))
    }

    /// A follow whose first state never comes fails, and the next request
    /// follows again rather than waiting on it until the node restarts.
    #[tokio::test(start_paused = true)]
    async fn a_follow_with_no_first_state_is_followed_again() {
        let ledger = Arc::new(Stuck::default());
        let (_dir, states) = states(&ledger);
        let e = states.follow("r").await.unwrap_err();
        assert!(e.to_string().contains("timed out following"), "{e:#}");

        ledger.serves.store(1, Ordering::Relaxed);
        assert_eq!(states.follow("r").await.unwrap().version, 1);
        assert_eq!(ledger.follows.load(Ordering::Relaxed), 2);
    }

    /// A subscription that stops short of a version fails the wait, and is
    /// dropped: the next follow subscribes again, and gets there.
    #[tokio::test(start_paused = true)]
    async fn a_subscription_that_stops_advancing_is_followed_again() {
        let ledger = Arc::new(Stuck::default());
        ledger.serves.store(1, Ordering::Relaxed);
        let (_dir, states) = states(&ledger);
        assert_eq!(states.follow("r").await.unwrap().version, 1);
        let e = states.wait_version("r", 2).await.unwrap_err();
        assert!(e.to_string().contains("timed out waiting"), "{e:#}");
        assert!(!states.following("r"));

        ledger.serves.store(2, Ordering::Relaxed);
        assert_eq!(states.follow("r").await.unwrap().version, 2);
        assert_eq!(states.wait_version("r", 2).await.unwrap().version, 2);
        assert_eq!(ledger.follows.load(Ordering::Relaxed), 2);
    }
}
