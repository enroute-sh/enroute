//! Background maintenance of layer manifests (LSM-style), run by the compute
//! that maintains the repository.
//!
//! - **Flush.** Inline layers (the memtable: their files are in the log)
//!   move to the object store. A contiguous run of them is merged into one
//!   layer first, so a busy repository costs one PUT per flush rather than
//!   one per push (at ~1000 pushes/s, ~86M PUTs a day) and the store does
//!   not fill with tiny objects; a run of one is uploaded as is. A run is
//!   flushed once its oldest layer has been inline for `flush_interval`, or
//!   once it holds [`FLUSH_BYTES`] or `flush_max_layers`.
//! - **Compaction.** Stored layers accumulate as pushes land. Runs of
//!   layers of about one size are merged into bigger ones, so layer sizes
//!   grow geometrically (see [`policy::pick`]).
//!   Merging copies stored pack entries verbatim (no recompression), drops
//!   duplicate objects, and rewrites the commit graph and segments with
//!   fresh layer-local refs.
//!
//! Either way the run is swapped for its replacement with one
//! compare-and-swap on the manifest, so concurrent pushes are never blocked
//! and a lost race is harmless: outputs are new files (layer ids are
//! never reused). Replaced layers, and outputs of lost races, are left to
//! the layer sweep ([`super::sweep`]), which deletes files nothing names.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use enroute_shared::proto::PendingRepo;
use enroute_shared::state::{Command, Commit, LayerMeta};
use enroute_shared::store::IoClass;

use crate::compute::ComputeConfig;
use crate::meta::RepoState;
use crate::repo::merge::{Merged, WINDOW, merge_layers};
use crate::repo::view::LayerView;
use crate::storage::{Sealed, new_layer_id, stored_meta};

use super::{Maintainer, policy};

/// Flushes of one repository in flight on a node at most. Under heavy
/// writes inline layers arrive faster than one flush at a time (merge,
/// upload, swap) moves them, and every layer left inline makes lookups and
/// view builds slower; flushes of disjoint runs swap independently.
const MAX_FLUSHES: usize = 4;

/// A run of inline layers this big is flushed whatever its age.
pub(crate) const FLUSH_BYTES: u64 = 32 << 20;

/// Compactions of one repository in flight on a node at most. A big merge
/// can take minutes; small layers keep merging next to it meanwhile.
const MAX_COMPACTIONS: usize = 2;

/// Compactions in flight on a node at most, over all repositories: they
/// share its local disk and network. Many repositories are due at once
/// after a change of policy, or of `merge_factor`.
const MAX_NODE_COMPACTIONS: usize = 4;

/// Pending repositories with the versions the ledger gave them, shared
/// between the tick and its callers.
type Repos = Arc<HashMap<String, u64>>;

/// This node's maintenance: what it has in flight per repository, and a
/// wake-up for the next tick.
#[derive(Default)]
pub(super) struct Maintenance {
    /// The repositories that the ledger says may need this node's
    /// maintenance, with their versions: as last asked, and as the last
    /// tick saw them. A tick visits those whose state changed, that became
    /// pending or got a new version since, or whose recheck is due, so its
    /// cost follows the work, not the number of pending repositories.
    /// Settled from a state at least as new as its version, a repository
    /// needs no maintenance until its layers change.
    pending: parking_lot::Mutex<(Repos, Repos)>,
    /// Repositories found settled, for the ledger, with their versions.
    settled: parking_lot::Mutex<HashMap<String, u64>>,
    repos: parking_lot::Mutex<HashMap<String, InFlight>>,
    /// Wakes maintenance before its next tick (a flush finished, and more
    /// may be due).
    wake: tokio::sync::Notify,
}

#[derive(Default)]
struct InFlight {
    /// When this node first saw each inline layer, by first id (flush age).
    seen: BTreeMap<u32, Instant>,
    /// The id ranges of inline runs being flushed (several may be, on
    /// disjoint layers).
    flushes: Vec<Range<u64>>,
    /// The id ranges of stored runs being compacted. They are disjoint from
    /// each other and from flushes, so no swap invalidates another.
    compactions: Vec<Range<u64>>,
    /// When to look at the repository again without a change to its state:
    /// when its oldest inline layer is due to flush, or as soon as a job
    /// ends.
    recheck: Option<Instant>,
}

impl Maintenance {
    pub(super) fn set_pending(&self, repos: Vec<PendingRepo>) {
        self.pending.lock().0 = Arc::new(repos.into_iter().map(|r| (r.repo, r.version)).collect());
    }

    /// The repositories found settled since the last call.
    pub(super) fn take_settled(&self) -> Vec<PendingRepo> {
        std::mem::take(&mut *self.settled.lock())
            .into_iter()
            .map(|(repo, version)| PendingRepo { repo, version })
            .collect()
    }

    /// Settle `repos` again with the next call (telling the ledger failed).
    pub(super) fn keep_settled(&self, repos: Vec<PendingRepo>) {
        let mut settled = self.settled.lock();
        for r in repos {
            settled.entry(r.repo).or_insert(r.version);
        }
    }

    /// Whether `repo` has work here: pending, or in flight.
    pub(super) fn holds(&self, repo: &str) -> bool {
        self.pending.lock().0.contains_key(repo) || self.repos.lock().contains_key(repo)
    }
}

impl InFlight {
    fn is_idle(&self) -> bool {
        self.flushes.is_empty() && self.compactions.is_empty()
    }
}

/// A flush or a compaction of the layers in an id range, in flight; its
/// layers are free again once it is dropped.
struct Job {
    maintenance: Arc<Maintenance>,
    repo: String,
    ids: Range<u64>,
}

impl Drop for Job {
    fn drop(&mut self) {
        let mut repos = self.maintenance.repos.lock();
        if let Some(w) = repos.get_mut(&self.repo) {
            // Flushes and compactions hold disjoint ranges.
            w.flushes.retain(|r| *r != self.ids);
            w.compactions.retain(|r| *r != self.ids);
            // Its layers are free: more may be due (a failed job, too).
            w.recheck = Some(Instant::now());
        }
    }
}

pub(super) fn spawn(node: &Arc<Maintainer>) {
    super::every(
        node,
        |n| {
            let m = n.in_flight.clone();
            async move {
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(100)) => {}
                    () = m.wake.notified() => {}
                }
            }
        },
        |n| async move { tick(&n) },
    );
}

/// Start the work due for each repository pending here: a flush if one is
/// due, and compactions if some are. They run independently (a flush
/// never waits for a compaction, which can take minutes), since each takes
/// layers no other job holds. So several can be in flight (up to
/// [`MAX_FLUSHES`] and [`MAX_COMPACTIONS`]).
fn tick(node: &Arc<Maintainer>) {
    let now = Instant::now();
    let changed = node.states.take_changed();
    let (pending, before) = {
        let mut p = node.in_flight.pending.lock();
        let pending = p.0.clone();
        (pending.clone(), std::mem::replace(&mut p.1, pending))
    };
    let mut due: Vec<(Arc<RepoState>, Job, Range<usize>)> = Vec::new();
    {
        let mut repos = node.in_flight.repos.lock();
        let mut visit: HashSet<String> = changed
            .into_iter()
            .filter(|r| pending.contains_key(r))
            .collect();
        if !Arc::ptr_eq(&pending, &before) {
            // Jobs on a repository no longer pending here still run to the end.
            repos.retain(|r, w| pending.contains_key(r) || !w.is_idle());
            // A change seen while it was not pending here was skipped; a new
            // version may be settled from the state seen.
            visit.extend(
                pending
                    .iter()
                    .filter(|(r, v)| before.get(*r) != Some(*v))
                    .map(|(r, _)| r.clone()),
            );
        }
        visit.extend(
            repos
                .iter()
                .filter(|(_, w)| w.recheck.is_some_and(|t| t <= now))
                .map(|(r, _)| r.clone()),
        );
        let mut compactions: usize = repos.values().map(|w| w.compactions.len()).sum();
        for repo in &visit {
            // Not followed yet: its first state is a change.
            let Some(meta) = node.states.get(repo) else {
                continue;
            };
            let w = repos.entry(repo.clone()).or_default();
            let inline = meta.layers.iter().filter(|l| !l.is_stored());
            w.seen = inline
                .map(|l| (l.first_id, w.seen.get(&l.first_id).copied().unwrap_or(now)))
                .collect();
            let job = |ids| Job {
                maintenance: node.in_flight.clone(),
                repo: repo.clone(),
                ids,
            };
            if let Some((run, ids)) = next_flush(w, &meta, now, &node.config) {
                w.flushes.push(ids.clone());
                due.push((meta.clone(), job(ids.clone()), run));
            }
            let room = MAX_NODE_COMPACTIONS.saturating_sub(compactions);
            for (run, ids) in next_compactions(w, &meta, room, &node.config) {
                w.compactions.push(ids.clone());
                compactions += 1;
                due.push((meta.clone(), job(ids), run));
            }
            w.recheck = if room == 0
                && w.compactions.len() < MAX_COMPACTIONS
                && merge_due(&meta, &node.config)
            {
                // Waits for room on this node: no event says when.
                Some(now)
            } else {
                flush_deadline(w, &node.config)
            };
            if w.is_idle()
                && settled(&meta, &node.config)
                && let Some(&version) = pending.get(repo)
                && meta.version >= version
            {
                node.in_flight.settled.lock().insert(repo.clone(), version);
            }
        }
    }
    for (meta, job, run) in due {
        tokio::spawn(IoClass::Background.scope(run_job(node.clone(), meta, job, run)));
    }
}

/// Run one flush or compaction of `meta`, then wake maintenance if a flush
/// freed inline layers.
async fn run_job(node: Arc<Maintainer>, meta: Arc<RepoState>, job: Job, run: Range<usize>) {
    let repo = job.repo.clone();
    match replace_run(&node, &repo, &meta, run).await {
        Err(e) => tracing::warn!(repo, "layer maintenance failed (will retry): {e:#}"),
        Ok(Some(version)) => {
            // Keep its layers marked until this node's state shows the
            // swap: picked again before that, they would be merged and
            // uploaded again only for the ledger to reject the swap (under
            // load, hundreds of times a minute).
            drop(node.repos.handle(&repo).wait_version(version).await);
            // More may be due already: look again now, with this job's
            // layers free.
            drop(job);
            node.in_flight.wake.notify_one();
        }
        Ok(None) => {}
    }
}

/// Whether some stored layers of `meta` are due to merge.
fn merge_due(meta: &RepoState, config: &ComputeConfig) -> bool {
    !policy::due(&policy::stored_sizes(&meta.layers), config.merge_factor).is_empty()
}

/// When the oldest inline layer that no flush holds is due to flush by
/// age (None: there is none, or the flushes are at their cap). A flush
/// that its size makes due starts on the change that made it so.
fn flush_deadline(w: &InFlight, config: &ComputeConfig) -> Option<Instant> {
    // At the cap, the end of a flush is the recheck.
    if w.flushes.len() >= MAX_FLUSHES {
        return None;
    }
    w.seen
        .iter()
        .filter(|(id, _)| !w.flushes.iter().any(|r| r.contains(&u64::from(**id))))
        .map(|(_, t)| *t + config.flush_interval)
        .min()
}

/// Whether maintenance has nothing left to do on `meta`: every layer is
/// stored, and no run is due to merge.
#[must_use]
pub fn settled(meta: &RepoState, config: &ComputeConfig) -> bool {
    policy::settled(&meta.layers, config.merge_factor)
}

/// The runs of stored layers of `meta` due to merge that no compaction in
/// flight holds, with their id ranges: at most `room`, and only as many as
/// [`MAX_COMPACTIONS`] allows.
fn next_compactions(
    w: &InFlight,
    meta: &RepoState,
    room: usize,
    config: &ComputeConfig,
) -> Vec<(Range<usize>, Range<u64>)> {
    let room = room.min(MAX_COMPACTIONS.saturating_sub(w.compactions.len()));
    if room == 0 {
        return Vec::new();
    }
    let mut runs = policy::due(&policy::stored_sizes(&meta.layers), config.merge_factor);
    // The newest run goes first: big old merges must not take every slot
    // while small layers pile up.
    if let Some(newest) = runs.pop() {
        runs.insert(0, newest);
    }
    runs.into_iter()
        .map(|run| {
            let ids = id_range(meta, &run);
            (run, ids)
        })
        .filter(|(_, ids)| !w.compactions.iter().any(|r| overlaps(r, ids)))
        .take(room)
        .collect()
}

/// The id range of layers `run` of `meta`.
#[expect(
    clippy::indexing_slicing,
    reason = "runs are non-empty ranges of meta.layers"
)]
fn id_range(meta: &RepoState, run: &Range<usize>) -> Range<u64> {
    let layers = &meta.layers[run.clone()];
    u64::from(layers[0].first_id)..layers[layers.len() - 1].end_id()
}

fn overlaps(a: &Range<u64>, b: &Range<u64>) -> bool {
    a.start < b.end && b.start < a.end
}

/// The next run of inline layers of `meta` due to flush, with its id range,
/// if this node may flush more of the repository now.
fn next_flush(
    w: &InFlight,
    meta: &RepoState,
    now: Instant,
    config: &ComputeConfig,
) -> Option<(Range<usize>, Range<u64>)> {
    if w.flushes.len() >= MAX_FLUSHES {
        return None;
    }
    let moving = |l: &LayerMeta| w.flushes.iter().any(|r| r.contains(&u64::from(l.first_id)));
    let runs = inline_runs(meta.layers.iter().map(|l| (l, moving(l))));
    due_run(meta, &w.seen, runs, now, config)
}

/// The first of `runs` due to flush at `now`, with its layers' id range.
fn due_run(
    meta: &RepoState,
    seen: &BTreeMap<u32, Instant>,
    runs: Vec<Range<usize>>,
    now: Instant,
    config: &ComputeConfig,
) -> Option<(Range<usize>, Range<u64>)> {
    runs.into_iter().find_map(|run| {
        #[expect(clippy::indexing_slicing, reason = "runs are ranges of meta.layers")]
        let layers = &meta.layers[run.clone()];
        let ids = id_range(meta, &run);
        let oldest = layers
            .iter()
            .filter_map(|l| seen.get(&l.first_id))
            .min()
            .copied()
            .unwrap_or(now);
        let due = now.duration_since(oldest) >= config.flush_interval
            || layers.iter().map(|l| l.bytes).sum::<u64>() >= FLUSH_BYTES
            || run.len() >= config.flush_max_layers;
        due.then_some((run, ids))
    })
}

/// Maximal contiguous runs of inline layers, of those not excluded (being
/// flushed already).
fn inline_runs<'a>(layers: impl IntoIterator<Item = (&'a LayerMeta, bool)>) -> Vec<Range<usize>> {
    let mut runs: Vec<Range<usize>> = Vec::new();
    for (i, (l, excluded)) in layers.into_iter().enumerate() {
        if l.is_stored() || excluded {
            continue;
        }
        match runs.last_mut() {
            Some(r) if r.end == i => r.end = i + 1,
            _ => runs.push(i..i + 1),
        }
    }
    runs
}

/// Publish merged layer `m` of `name`, under a new id of upload generation
/// `generation`.
async fn publish_merged(
    node: &Maintainer,
    name: &str,
    generation: u64,
    m: Merged,
) -> Result<LayerMeta> {
    let id = new_layer_id(generation);
    let written = node.storage.publish_built(name, &id, m.builder).await?;
    Ok(LayerMeta {
        first_id: m.ids.start,
        id_span: m.ids.end - m.ids.start,
        ..stored_meta(id, &written, m.objects)
    })
}

/// Replace layers `run` of `meta` with stored layers: a single inline layer
/// is uploaded as is (as a new file, under a new id), anything else merged
/// first. New files are of `meta`'s upload generation. Returns the
/// repository's version with the swap, unless it did not happen (the
/// manifest moved on).
async fn replace_run(
    node: &Maintainer,
    name: &str,
    meta: &RepoState,
    run: Range<usize>,
) -> Result<Option<u64>> {
    #[expect(
        clippy::indexing_slicing,
        reason = "the run is of this manifest's layers"
    )]
    let replaced = &meta.layers[run.clone()];
    let t0 = Instant::now();
    let added = match replaced {
        [l] if !l.is_stored() => {
            let sealed = Sealed::of(l.data.clone().unwrap_or_default())?;
            let id = new_layer_id(meta.upload_gen);
            node.storage.publish_layer(name, &id, sealed).await?;
            vec![LayerMeta {
                id,
                data: None,
                ..l.clone()
            }]
        }
        _ => {
            let snap = node.repos.handle(name).wait_version(meta.version).await?;
            #[expect(clippy::indexing_slicing, reason = "a run has at least one layer")]
            let start = snap
                .layers
                .partition_point(|lv| lv.base < replaced[0].first_id);
            let same =
                |w: &[Arc<LayerView>]| w.iter().zip(replaced).all(|(lv, m)| lv.layer.id == m.id);
            if !snap
                .layers
                .get(start..start + replaced.len())
                .is_some_and(same)
            {
                return Ok(None); // the manifest moved on; next tick decides again
            }
            let out = merge_layers(
                &snap,
                start..start + replaced.len(),
                node.storage.workspace(),
                WINDOW,
            )
            .await?;
            // A run of nothing but copies of older objects goes away.
            match out {
                Some(m) => vec![publish_merged(node, name, meta.upload_gen, m).await?],
                None => Vec::new(),
            }
        }
    };
    let (objects, bytes) = added.iter().fold((0u64, 0u64), |(o, b), l| {
        (o + u64::from(l.objects), b + l.bytes)
    });
    let res = node
        .propose(
            Command::commit(
                name,
                Commit {
                    add_layers: added.clone(),
                    remove_layers: replaced.iter().map(Into::into).collect(),
                    updates: vec![],
                    atomic: true,
                    ..Default::default()
                },
            )
            .at(meta.epoch),
        )
        .await?;
    if let Some(e) = res.error {
        // The outputs are left to the layer sweep. (A slow merge whose
        // upload generation expired lands here too, and is done again.)
        tracing::debug!(
            repo = name,
            "another writer changed the layers first: {e:#}"
        );
        return Ok(None);
    }
    tracing::debug!(
        repo = name,
        replaced = replaced.len(),
        outputs = added.len(),
        objects,
        bytes,
        ms = crate::util::millis(t0.elapsed()),
        "replaced layers"
    );
    Ok(Some(res.repo_version))
}
