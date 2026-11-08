//! Background maintenance on a compute node: flushes and compaction
//! ([`compact`]), the layer sweep ([`sweep`]), both on the repositories
//! this node maintains ([`owners`]); following those of them that the
//! ledger says need work; and eviction of idle views.

mod compact;
pub(crate) mod owners;
mod policy;
mod sweep;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use enroute_shared::roles::{self, LedgerRef};
use enroute_shared::state::{Command, Outcome};
use enroute_shared::store::{IoClass, Objects};
use futures::StreamExt;

use crate::compute::ComputeConfig;
use crate::repo::{Repo, Repos};
use crate::states::{Consistency, States};
use crate::storage::Storage;
use compact::Maintenance;
use owners::Owners;

pub use compact::settled;
pub use owners::compute_id;
pub(crate) use policy::MERGE_FACTOR;

/// How often idle views are evicted.
const EVICT_EVERY: Duration = Duration::from_secs(1);
/// How often the ledger is asked which repositories need this node's
/// maintenance. Only those are ticked, so a repository's flush age starts
/// up to this long after its first inline layer.
const ADOPT_EVERY: Duration = Duration::from_secs(2);
/// Repositories followed at once to maintain them.
const ADOPT_CONCURRENCY: usize = 16;

/// This node's maintenance. Its loops stop once it is dropped.
pub(crate) struct Maintainer {
    states: Arc<States>,
    repos: Arc<Repos>,
    storage: Arc<Storage>,
    objects: Arc<Objects>,
    /// Maintenance proposes its own commands, outside the commit path.
    ledger: LedgerRef,
    /// Which repositories this node maintains.
    owners: Arc<Owners>,
    /// Flushes and compactions in flight here.
    in_flight: Arc<Maintenance>,
    config: ComputeConfig,
}

impl Maintainer {
    /// Propose `cmd` to its repository's log (which must be live): it
    /// neither waits for the outcome nor retries.
    async fn propose(&self, cmd: Command) -> Result<Outcome> {
        roles::propose(&*self.ledger, cmd).await
    }

    /// `repo` after everything committed so far (None: it does not exist).
    async fn latest(&self, repo: &str) -> Result<Option<Arc<Repo>>> {
        self.states.open(repo, Consistency::Latest).await
    }

    /// Follow the repositories that need this node's maintenance, so that
    /// it sees their work (it may serve none of their requests).
    async fn adopt(&self) {
        let settled = self.in_flight.take_settled();
        let asked = roles::pending(&*self.ledger, self.owners.me(), settled.clone());
        let pending = match asked.await {
            Ok(p) => {
                self.owners.set_live(p.computes);
                p.repos
            }
            Err(e) => {
                tracing::warn!("asking which repositories need maintenance: {e:#}");
                self.in_flight.keep_settled(settled);
                return;
            }
        };
        let new: Vec<String> = pending
            .iter()
            .filter(|r| !self.states.following(&r.repo))
            .map(|r| r.repo.clone())
            .collect();
        self.in_flight.set_pending(pending);
        futures::stream::iter(new)
            .for_each_concurrent(ADOPT_CONCURRENCY, |repo| async move {
                if let Err(e) = self.states.follow_active(&repo).await {
                    tracing::warn!(repo, "following a repository to maintain it: {e:#}");
                }
            })
            .await;
    }

    fn evict_idle(&self) {
        // Its maintainer keeps a repository until its work is done.
        let keep = |repo: &str| self.in_flight.holds(repo);
        let evicted = self.states.evict(self.config.evict_after, keep);
        if evicted > 0 {
            tracing::debug!(evicted, "evicted idle repositories");
        }
    }
}

/// Run `work` after each `pause`, while the node lives. The loop holds the
/// node only during `work`, so a dropped node stops it.
fn every<P, W>(
    node: &Arc<Maintainer>,
    pause: impl Fn(&Maintainer) -> P + Send + 'static,
    work: impl Fn(Arc<Maintainer>) -> W + Send + 'static,
) where
    P: Future<Output = ()> + Send + 'static,
    W: Future<Output = ()> + Send + 'static,
{
    let weak = Arc::downgrade(node);
    tokio::spawn(async move {
        loop {
            let Some(wait) = weak.upgrade().map(|n| pause(&n)) else {
                return;
            };
            wait.await;
            let Some(node) = weak.upgrade() else { return };
            IoClass::Background.scope(work(node)).await;
        }
    });
}

/// Start maintenance: flushing inline layers, compaction, layer sweeps,
/// eviction of idle views, and cleanup of local copies left from before a
/// restart, on the repositories that `owners` gives this node. It runs
/// while the returned handle lives.
pub(crate) fn start(
    config: &ComputeConfig,
    states: Arc<States>,
    repos: Arc<Repos>,
    storage: Arc<Storage>,
    objects: Arc<Objects>,
    ledger: LedgerRef,
    owners: Arc<Owners>,
) -> Arc<Maintainer> {
    let node = Arc::new(Maintainer {
        states,
        repos,
        storage,
        objects,
        ledger,
        owners,
        in_flight: Arc::default(),
        config: config.clone(),
    });
    compact::spawn(&node);
    sweep::spawn(&node);
    every(
        &node,
        |_| tokio::time::sleep(EVICT_EVERY),
        |n| async move { n.evict_idle() },
    );
    every(
        &node,
        |_| tokio::time::sleep(ADOPT_EVERY),
        |n| async move { n.adopt().await },
    );
    node
}
