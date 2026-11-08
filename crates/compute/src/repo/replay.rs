//! Bulk reconstruction of many objects: rebuild every object of a large set
//! (e.g. the boundary trees of a shallow fetch) by replaying their delta
//! chains in storage order, instead of walking each chain on its own.
//!
//! Deltas are forward (a base is an older version than its delta), so
//! storage order (layer by layer, each front to back) puts every base
//! before its deltas. Two sequential passes therefore resolve all chains at
//! once:
//!
//! 1. **Mark.** Every object's delta-base link (one table scan per snapshot,
//!    shared by all requests) is followed from the targets, in memory, to
//!    mark every chain member; one more table scan locates the members'
//!    entries.
//! 2. **Replay.** The members' entries are read in storage order: a full
//!    entry is inflated, a delta applied to its base (already rebuilt, since
//!    it came earlier). A rebuilt object is kept only while a later member
//!    still needs it; finished targets are compressed and handed out.
//!
//! All chains of a large set run side by side across the whole history, so
//! targets are replayed in groups bounded in size, one data pass per group.
//! Groups are independent and run as tasks on the shared CPU pool, as does
//! compression; each group first reserves its size from a node-wide budget,
//! so concurrent requests together stay within it. The requesting thread
//! only writes the pack: pack order is free, so entries go out as they are
//! finished.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, mpsc};

use anyhow::{Context, Result};
use foldhash::{HashMap, HashMapExt};
use roaring::RoaringBitmap;
use tokio::sync::Semaphore;

use crate::git::pack::{StoredEntry, full_entry};
use crate::storage::{Access, NotResident, Pinned, Ref, test_size};
use crate::util::to_u64;

use super::machine::{Ticket, Waits};
use super::view::{DenseId, LayerView, Snapshot};

/// Re-encode sets at least this large are rebuilt by replay; smaller ones
/// walk each chain on its own (a replay scans whole tables). Tests lower it
/// with `ENROUTE_REPLAY_MIN`.
pub(crate) fn replay_min() -> u64 {
    static MIN: OnceLock<u64> = OnceLock::new();
    *MIN.get_or_init(|| test_size("ENROUTE_REPLAY_MIN").unwrap_or(10_000))
}

/// Uncompressed bytes of targets replayed per data pass: bounds the
/// objects held in memory while a group's chains are in progress. Tests
/// lower it with `ENROUTE_REPLAY_GROUP_BYTES`.
fn group_bytes() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| test_size("ENROUTE_REPLAY_GROUP_BYTES").unwrap_or(128 << 20))
}

/// Uncompressed bytes of finished targets compressed by one task.
const BATCH_BYTES: usize = 4 << 20;

/// Compressed bytes a request may have waiting for its client before it
/// starts more groups.
const QUEUED_BYTES: u64 = 64 << 20;

/// Node-wide memory for replays (rebuilt objects of groups in flight), in
/// bytes: a group holds that many permits (at most all) while it runs.
const BUDGET_BYTES: u32 = 512 << 20;

/// Where the group of targets from `start` ends, and their total size.
fn group_end(sizes: &[(DenseId, u64)], start: usize) -> (usize, u64) {
    let (mut end, mut bytes) = (start, 0);
    for &(_, size) in sizes.iter().skip(start) {
        if end != start && bytes + size > group_bytes() {
            break;
        }
        bytes += size;
        end += 1;
    }
    (end, bytes)
}

fn budget() -> &'static Arc<Semaphore> {
    static BUDGET: OnceLock<Arc<Semaphore>> = OnceLock::new();
    BUDGET.get_or_init(|| {
        Arc::new(Semaphore::new(
            usize::try_from(BUDGET_BYTES).unwrap_or(Semaphore::MAX_PERMITS),
        ))
    })
}

/// One group: its targets, its chain members, how many later members use
/// each member as base, and the targets' total size.
struct Group {
    targets: RoaringBitmap,
    members: RoaringBitmap,
    uses: HashMap<DenseId, u32>,
    bytes: u64,
}

/// What a request's tasks share.
struct Shared {
    snap: Arc<Snapshot>,
    links: Arc<Vec<u32>>,
    locs: HashMap<DenseId, (u64, u32)>,
    /// The writer stopped (client gone, or a task failed): wind down.
    cancel: AtomicBool,
    /// Compressed bytes sent to the writer and not yet written.
    queued: AtomicU64,
    /// Signalled whenever a task sends a message.
    wake: Arc<tokio::sync::Notify>,
}

impl Shared {
    /// `d`'s delta base link.
    #[expect(clippy::indexing_slicing, reason = "`links` has an entry per object")]
    fn link(&self, d: DenseId) -> Ref {
        Ref(self.links[usize::try_from(d).unwrap_or(usize::MAX)])
    }
}

enum Msg {
    Entries(Vec<(DenseId, Vec<u8>)>),
    GroupDone(Result<()>),
}

/// How far a group's bytes are.
enum Load {
    NotRequested,
    /// Pinned, some still loading.
    Loading(Ticket, Vec<Pinned>),
    /// Local and pinned until the group is replayed.
    Ready(Vec<Pinned>),
}

/// Rebuilds every object of a set (see the module docs), as a resumable
/// part of a [`Machine`](super::machine::Machine): a group goes to the CPU
/// pool once its chain members' bytes are local and its memory is reserved;
/// the next groups' bytes load meanwhile. Nothing waits on a thread.
pub(crate) struct Replay {
    shared: Arc<Shared>,
    groups: VecDeque<(Group, Load)>,
    in_flight: usize,
    rx: mpsc::Receiver<Msg>,
    tx: Option<mpsc::Sender<Msg>>,
    failed: Option<anyhow::Error>,
    /// Parked until a task reports (`Shared::wake`).
    woken: Option<Ticket>,
    /// Parked until budget is freed.
    budget_wait: Option<Ticket>,
}

/// Groups whose bytes are loaded ahead of the one running.
const LOOKAHEAD: usize = 2;

impl Replay {
    /// Mark and group the chains of `targets` (local metadata only).
    pub(crate) fn new(snap: &Arc<Snapshot>, targets: &RoaringBitmap) -> Result<Replay> {
        let links = snap.chain_links()?;
        // Mark every chain member, then locate the members and size the
        // targets in one table scan.
        let (tx, rx) = mpsc::channel::<Msg>();
        let mut shared = Shared {
            snap: snap.clone(),
            links,
            locs: HashMap::new(),
            cancel: AtomicBool::new(false),
            queued: AtomicU64::new(0),
            wake: Arc::new(tokio::sync::Notify::new()),
        };
        let mut all = RoaringBitmap::new();
        for t in targets {
            mark(&shared, t, &mut all, &mut HashMap::new())?;
        }
        let mut sizes: Vec<(DenseId, u64)> =
            Vec::with_capacity(usize::try_from(targets.len()).unwrap_or_default());
        for lv in &snap.layers {
            scan_layer(lv, &all, targets, &mut shared.locs, &mut sizes)?;
        }
        // Groups of targets within the group size, each with its own chains
        // (a member may belong to several groups).
        let mut groups = VecDeque::new();
        let mut start = 0;
        while start < sizes.len() {
            let (end, bytes) = group_end(&sizes, start);
            let mut g = Group {
                targets: RoaringBitmap::new(),
                members: RoaringBitmap::new(),
                uses: HashMap::new(),
                bytes,
            };
            for &(d, _) in sizes.iter().take(end).skip(start) {
                g.targets.insert(d);
                mark(&shared, d, &mut g.members, &mut g.uses)?;
            }
            groups.push_back((g, Load::NotRequested));
            start = end;
        }
        Ok(Replay {
            shared: Arc::new(shared),
            groups,
            in_flight: 0,
            rx,
            tx: Some(tx),
            failed: None,
            woken: None,
            budget_wait: None,
        })
    }

    /// Make progress: emit finished entries (in no particular order), load
    /// and start groups. Returns whether everything is done.
    pub(crate) fn step(
        &mut self,
        waits: &mut Waits,
        emit: &mut dyn FnMut(DenseId, &[u8]) -> Result<()>,
    ) -> Result<bool> {
        let shared = self.shared.clone();
        loop {
            if self.drain(&shared, emit) {
                return self.finish().map(|()| true);
            }
            // Load the next groups' bytes.
            self.load_ahead(&shared, waits)?;
            // Start groups whose bytes are local, within the budget and the
            // client's backlog.
            let started = self.start_groups(&shared, waits);
            if (self.groups.is_empty() || self.failed.is_some()) && self.tx.take().is_some() {
                // No more groups: the channel closes once every task is
                // done (perhaps already).
                continue;
            }
            if !started {
                break;
            }
        }
        // Tasks wake us when they report.
        if self.woken.is_none() && (self.in_flight > 0 || self.tx.is_none()) {
            let wake = shared.wake.clone();
            self.woken = Some(waits.park_on(async move {
                wake.notified().await;
                Ok(())
            }));
        }
        Ok(false)
    }

    /// Request the bytes of the next groups.
    fn load_ahead(&mut self, shared: &Shared, waits: &mut Waits) -> Result<()> {
        let ahead = self.groups.iter_mut().take(LOOKAHEAD);
        for (g, load) in ahead.filter(|e| matches!(e.1, Load::NotRequested)) {
            let (pins, need) = group_bytes_needed(shared, g)?;
            *load = if need.is_empty() {
                Load::Ready(pins)
            } else {
                Load::Loading(waits.park_as(need, Access::Scan), pins)
            };
        }
        Ok(())
    }

    /// With no group running, park until other requests' groups free
    /// `permits` of the budget.
    fn wait_for_budget(&mut self, waits: &mut Waits, permits: u32) {
        if self.in_flight == 0 && self.budget_wait.is_none() {
            self.budget_wait = Some(waits.park_on(async move {
                drop(budget().clone().acquire_many_owned(permits).await?);
                Ok(())
            }));
        }
    }

    /// Handle finished work. Returns whether the channel closed.
    fn drain(
        &mut self,
        shared: &Shared,
        emit: &mut dyn FnMut(DenseId, &[u8]) -> Result<()>,
    ) -> bool {
        loop {
            match self.rx.try_recv() {
                Ok(Msg::Entries(batch)) => self.on_entries(&batch, shared, emit),
                Ok(Msg::GroupDone(res)) => self.on_group_done(res),
                Err(mpsc::TryRecvError::Empty) => return false,
                Err(mpsc::TryRecvError::Disconnected) => return true,
            }
        }
    }

    fn on_entries(
        &mut self,
        batch: &[(DenseId, Vec<u8>)],
        shared: &Shared,
        emit: &mut dyn FnMut(DenseId, &[u8]) -> Result<()>,
    ) {
        if self.failed.is_none() {
            self.emit_all(batch, emit);
        }
        let n: u64 = batch.iter().map(|(_, e)| to_u64(e.len())).sum();
        shared.queued.fetch_sub(n, Ordering::Relaxed);
    }

    fn on_group_done(&mut self, res: Result<()>) {
        self.in_flight -= 1;
        if let Err(e) = res {
            self.fail(e);
        }
    }

    fn emit_all(
        &mut self,
        batch: &[(DenseId, Vec<u8>)],
        emit: &mut dyn FnMut(DenseId, &[u8]) -> Result<()>,
    ) {
        for (d, e) in batch {
            if let Err(e) = emit(*d, e) {
                self.fail(e);
                break;
            }
        }
    }

    /// Start groups whose bytes are local, within the budget and the
    /// client's backlog. Returns whether any started.
    #[expect(
        clippy::expect_used,
        clippy::unreachable,
        reason = "the front group was checked as `Ready`, and the channel is open while groups remain"
    )]
    fn start_groups(&mut self, shared: &Arc<Shared>, waits: &mut Waits) -> bool {
        let mut started = false;
        while self.failed.is_none() && shared.queued.load(Ordering::Relaxed) < QUEUED_BYTES {
            let Some((g, Load::Ready(_))) = self.groups.front() else {
                break;
            };
            let permits =
                u32::try_from(g.bytes.min(u64::from(BUDGET_BYTES))).unwrap_or(BUDGET_BYTES);
            let Ok(reserved) = budget().clone().try_acquire_many_owned(permits) else {
                // A running group frees budget and wakes us; with none
                // running, wait until other requests' groups free enough.
                self.wait_for_budget(waits, permits);
                break;
            };
            let (g, Load::Ready(pins)) = self.groups.pop_front().expect("peeked") else {
                unreachable!()
            };
            let (shared, tx) = (
                shared.clone(),
                self.tx.as_ref().expect("open while groups remain").clone(),
            );
            self.in_flight += 1;
            started = true;
            rayon::spawn(move || {
                let res = replay_group(&shared, g, &tx);
                drop((reserved, pins));
                // The receiver is gone once the request ends.
                drop(tx.send(Msg::GroupDone(res)));
                // The writer may see the channel close only after this.
                drop(tx);
                shared.wake.notify_one();
            });
        }
        started
    }

    /// What `ticket` waited for happened; returns whether it was ours.
    pub(crate) fn resume(&mut self, ticket: Ticket) -> bool {
        if self.woken == Some(ticket) {
            self.woken = None;
            return true;
        }
        if self.budget_wait == Some(ticket) {
            self.budget_wait = None;
            return true;
        }
        for (_, load) in &mut self.groups {
            if let Load::Loading(t, pins) = load
                && *t == ticket
            {
                *load = Load::Ready(std::mem::take(pins));
                return true;
            }
        }
        false
    }

    fn fail(&mut self, e: anyhow::Error) {
        if self.failed.is_none() {
            self.failed = Some(e);
            self.shared.cancel.store(true, Ordering::Relaxed);
        }
    }

    fn finish(&mut self) -> Result<()> {
        self.failed.take().map_or(Ok(()), Err)
    }
}

/// Record where the members and sizes of the targets are in `lv`'s layer.
fn scan_layer(
    lv: &LayerView,
    all: &RoaringBitmap,
    targets: &RoaringBitmap,
    locs: &mut HashMap<DenseId, (u64, u32)>,
    sizes: &mut Vec<(DenseId, u64)>,
) -> Result<()> {
    lv.layer.for_each_record(|l, r| {
        let d = lv.own(l);
        if all.contains(d) {
            locs.insert(d, (r.entry_off(), r.entry_len()));
        }
        if targets.contains(d) {
            sizes.push((d, r.size()));
        }
    })
}

/// Pins on a group's chain members' bytes (runs of nearby entries per
/// layer), and those of them that are not local.
#[expect(
    clippy::indexing_slicing,
    reason = "layer positions come from the snapshot"
)]
fn group_bytes_needed(shared: &Shared, g: &Group) -> Result<(Vec<Pinned>, Vec<NotResident>)> {
    // (layer position, start, end) of every member's entry, merged when
    // close.
    let mut ranges: Vec<(usize, u64, u64)> =
        Vec::with_capacity(usize::try_from(g.members.len()).unwrap_or_default());
    for d in &g.members {
        let (off, len) = *shared
            .locs
            .get(&d)
            .context("chain member without a record")?;
        ranges.push((shared.snap.layer_at(d), off, off + u64::from(len)));
    }
    // Members are in id order, which is storage order.
    let mut merged: Vec<(usize, u64, u64)> = Vec::new();
    for r in ranges {
        match merged.last_mut() {
            Some(m) if m.0 == r.0 && r.1 <= m.2 + (64 << 10) => m.2 = m.2.max(r.2),
            _ => merged.push(r),
        }
    }
    let (mut pins, mut need) = (Vec::with_capacity(merged.len()), Vec::new());
    for (layer, start, end) in merged {
        let lv = &shared.snap.layers[layer];
        pins.push(lv.layer.pin_entries(start, end));
        need.extend(lv.layer.entries_missing(start, end));
    }
    Ok((pins, need))
}

/// Add `t` and the members of its chain to `members`, counting each link in
/// `uses` (how many members use a member as base).
fn mark(
    shared: &Shared,
    t: DenseId,
    members: &mut RoaringBitmap,
    uses: &mut HashMap<DenseId, u32>,
) -> Result<()> {
    if !members.insert(t) {
        return Ok(());
    }
    shared.snap.walk_chain(t, |b| {
        if b != t {
            *uses.entry(b).or_default() += 1;
            if !members.insert(b) {
                return Ok(None);
            }
        }
        Ok(Some(shared.link(b)))
    })
}

/// Replay one group's chains in storage order; finished targets go to
/// compression tasks in batches.
#[expect(clippy::expect_used, reason = "every base use was counted by `mark`")]
fn replay_group(shared: &Arc<Shared>, g: Group, tx: &mpsc::Sender<Msg>) -> Result<()> {
    let snap = &shared.snap;
    let mut uses = g.uses;
    let mut live: HashMap<DenseId, Arc<Vec<u8>>> = HashMap::new();
    let mut done: Vec<(DenseId, Arc<Vec<u8>>)> = Vec::new();
    let mut done_bytes = 0;
    // Ids go up in storage order, and layers and merges write a delta's
    // base before it: in id order, every base is rebuilt before its deltas.
    for (i, d) in g.members.iter().enumerate() {
        if i % 4096 == 0 && shared.cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let (lv, _) = snap.locate(d);
        let (off, len) = *shared
            .locs
            .get(&d)
            .context("chain member without a record")?;
        let base = snap.base_from(d, shared.link(d))?;
        let base_content = base
            .map(|b| {
                live.get(&b)
                    .context("delta base not rebuilt before its delta")
            })
            .transpose()?;
        let content = lv.layer.with_entry_at(off, len, Access::Scan, |entry| {
            StoredEntry::parse(lv.layer.format(), entry)?.decode(base_content.map(|c| c.as_slice()))
        })??;
        if let Some(b) = base {
            let n = uses.get_mut(&b).expect("counted use");
            *n -= 1;
            if *n == 0 {
                live.remove(&b);
            }
        }
        let content = Arc::new(content);
        if uses.get(&d).is_some_and(|&n| n > 0) {
            live.insert(d, content.clone());
        }
        // A target is emitted by its own group only: other groups may
        // rebuild it as a member of their chains.
        if g.targets.contains(d) {
            done_bytes += content.len();
            done.push((d, content));
            if done_bytes >= BATCH_BYTES {
                done_bytes = 0;
                compress(shared, std::mem::take(&mut done), tx);
            }
        }
    }
    compress(shared, done, tx);
    Ok(())
}

/// Compress `batch` on the CPU pool and send it to the writer.
fn compress(shared: &Arc<Shared>, batch: Vec<(DenseId, Arc<Vec<u8>>)>, tx: &mpsc::Sender<Msg>) {
    if batch.is_empty() {
        return;
    }
    let (shared, tx) = (shared.clone(), tx.clone());
    rayon::spawn(move || {
        if !shared.cancel.load(Ordering::Relaxed) {
            let out: Vec<(DenseId, Vec<u8>)> = batch
                .iter()
                .map(|(d, c)| (*d, full_entry(shared.snap.kind(*d), c)))
                .collect();
            shared.queued.fetch_add(
                out.iter().map(|(_, e)| to_u64(e.len())).sum(),
                Ordering::Relaxed,
            );
            // The receiver is gone once the request ends.
            drop(tx.send(Msg::Entries(out)));
        }
        // The writer may see the channel close only after this.
        drop(tx);
        shared.wake.notify_one();
    });
}
