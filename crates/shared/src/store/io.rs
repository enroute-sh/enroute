//! One node-wide pool of object store request slots, shared by IO classes.
//!
//! Each class has a reservation (slots that only it may use), a limit (the
//! most slots it may hold) and a weight (its share of the unreserved slots),
//! as in mClock (Gulati et al., OSDI 2010) with slots in place of rates.
//! A freed slot goes first to the class that is furthest below its
//! reservation, then to the class with the lowest start-time fair queueing
//! tag. The class of a request is that of its handle ([`IoClass`] pinned),
//! else that of the task that makes it ([`IoClass::scope`]).

use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, Stream, StreamExt};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result,
    UploadPart, path::Path,
};
use parking_lot::Mutex;
use tokio::sync::oneshot;

/// What a store request is for, which decides its share of the slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IoClass {
    /// Writes that a push or a deletion waits on.
    Commit,
    /// Reads that a request waits on (the default).
    Foreground,
    /// Work that no request waits on: hydration, compaction, sweeps.
    Background,
}

impl IoClass {
    /// Every class, in status order.
    pub const ALL: [IoClass; 3] = [IoClass::Commit, IoClass::Foreground, IoClass::Background];

    /// The class of the current task (Foreground outside any scope).
    #[must_use]
    pub fn current() -> IoClass {
        CLASS.try_with(|c| *c).unwrap_or(IoClass::Foreground)
    }

    /// Run `f` with store requests in this class.
    pub async fn scope<F: Future>(self, f: F) -> F::Output {
        CLASS.scope(self, f).await
    }

    /// The name of the class in status and configuration.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            IoClass::Commit => "commit",
            IoClass::Foreground => "foreground",
            IoClass::Background => "background",
        }
    }
}

tokio::task_local! {
    static CLASS: IoClass;
}

/// Spawn `f` in the class of the current task, which `tokio::spawn` drops.
pub fn spawn<F>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(IoClass::current().scope(f))
}

/// A class's share of the slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Share {
    /// Slots that only this class may use, and that stay idle without it.
    pub reserve: usize,
    /// The most slots this class may hold at once.
    pub limit: usize,
    /// Its share of the unreserved slots, against the other classes' weights.
    pub weight: u32,
}

/// The slot pool and each class's share of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IoConfig {
    /// Concurrent store requests for the whole node.
    pub slots: usize,
    /// See [`IoClass::Commit`].
    pub commit: Share,
    /// See [`IoClass::Foreground`].
    pub foreground: Share,
    /// See [`IoClass::Background`].
    pub background: Share,
}

impl Default for IoConfig {
    /// Commits keep the slots they need under any load;
    /// background work fills idle slots but never more than half. With
    /// pushes spread over many repositories nearly every push needs a
    /// flush of its own, so maintenance needs that much to keep up (at 100k
    /// repositories a limit of 32 flushed ~0.7k pushes/s, 128 ~2.2k/s).
    fn default() -> IoConfig {
        let share = |reserve, limit, weight| Share {
            reserve,
            limit,
            weight,
        };
        IoConfig {
            slots: 256,
            commit: share(16, 128, 4),
            foreground: share(8, 128, 2),
            background: share(4, 128, 1),
        }
    }
}

impl IoConfig {
    /// The share of `class`.
    #[must_use]
    pub(crate) fn share(&self, class: IoClass) -> Share {
        match class {
            IoClass::Commit => self.commit,
            IoClass::Foreground => self.foreground,
            IoClass::Background => self.background,
        }
    }

    /// Check that the shares fit the pool.
    ///
    /// # Errors
    ///
    /// Returns an error that names the first share that does not fit.
    pub fn check(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.slots > 0, "slots must be at least 1");
        let mut reserved = 0;
        for c in IoClass::ALL {
            let s = self.share(c);
            let name = c.name();
            anyhow::ensure!(s.limit > 0, "{name}.limit must be at least 1");
            anyhow::ensure!(s.weight > 0, "{name}.weight must be at least 1");
            anyhow::ensure!(
                s.reserve <= s.limit,
                "{name}.reserve must not be above {name}.limit"
            );
            reserved += s.reserve;
        }
        anyhow::ensure!(
            reserved <= self.slots,
            "the reservations ({reserved}) must not be above slots ({})",
            self.slots
        );
        Ok(())
    }
}

/// Fair queueing tags count in these units per grant to a class of weight 1.
const TAG_UNIT: u64 = 1 << 20;

/// The node's slot pool.
pub struct IoLimiter {
    config: IoConfig,
    state: Mutex<State>,
    waits: PerClass<WaitHistogram>,
}

impl fmt::Debug for IoLimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoLimiter")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

struct State {
    free: usize,
    classes: PerClass<ClassState>,
    /// The start tag of the last grant by weight.
    vtime: u64,
}

#[derive(Default)]
struct ClassState {
    in_flight: usize,
    waiters: VecDeque<oneshot::Sender<Slot>>,
    /// The finish tag of this class's last grant by weight.
    finish: u64,
    granted: u64,
}

#[derive(Default)]
struct PerClass<T> {
    commit: T,
    foreground: T,
    background: T,
}

impl<T> PerClass<T> {
    fn get(&self, c: IoClass) -> &T {
        match c {
            IoClass::Commit => &self.commit,
            IoClass::Foreground => &self.foreground,
            IoClass::Background => &self.background,
        }
    }

    fn get_mut(&mut self, c: IoClass) -> &mut T {
        match c {
            IoClass::Commit => &mut self.commit,
            IoClass::Foreground => &mut self.foreground,
            IoClass::Background => &mut self.background,
        }
    }
}

impl State {
    /// Slots that classes below their reservation may still claim.
    fn unmet(&self, config: &IoConfig) -> usize {
        IoClass::ALL
            .iter()
            .map(|&c| {
                config
                    .share(c)
                    .reserve
                    .saturating_sub(self.classes.get(c).in_flight)
            })
            .sum()
    }

    /// The class that the next free slot goes to, if any can take one.
    fn pick(&self, config: &IoConfig) -> Option<(IoClass, bool)> {
        if self.free == 0 {
            return None;
        }
        let waiting = |c: IoClass| !self.classes.get(c).waiters.is_empty();
        // Furthest below its reservation first (cross-multiplied ratios).
        let reserved = IoClass::ALL
            .into_iter()
            .filter(|&c| waiting(c) && self.classes.get(c).in_flight < config.share(c).reserve)
            .min_by_key(|&c| {
                let (held, reserve) = (self.classes.get(c).in_flight, config.share(c).reserve);
                // held / reserve, compared exactly.
                (held * 1024).checked_div(reserve).unwrap_or(0)
            });
        if let Some(c) = reserved {
            return Some((c, true));
        }
        // The rest of the free slots are held back for unmet reservations.
        if self.free <= self.unmet(config) {
            return None;
        }
        IoClass::ALL
            .into_iter()
            .filter(|&c| waiting(c) && self.classes.get(c).in_flight < config.share(c).limit)
            .min_by_key(|&c| self.classes.get(c).finish.max(self.vtime))
            .map(|c| (c, false))
    }

    fn grant(&mut self, config: &IoConfig, c: IoClass, by_reservation: bool) {
        let vtime = self.vtime;
        let class = self.classes.get_mut(c);
        class.in_flight += 1;
        class.granted += 1;
        // A grant from the reservation does not count against the weight
        // share, as in mClock.
        if !by_reservation {
            let start = class.finish.max(vtime);
            class.finish = start + TAG_UNIT / u64::from(config.share(c).weight);
            self.vtime = start;
        }
        self.free -= 1;
    }
}

/// A request slot, given back to the pool when dropped.
struct Slot {
    io: Arc<IoLimiter>,
    class: IoClass,
    /// Unset once the slot is given back without a drop.
    held: bool,
}

impl Drop for Slot {
    fn drop(&mut self) {
        if self.held {
            self.io.release(self.class);
        }
    }
}

impl IoLimiter {
    /// A pool as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if the shares do not fit the pool.
    pub fn new(config: IoConfig) -> anyhow::Result<Arc<IoLimiter>> {
        config.check()?;
        Ok(IoLimiter::build(config))
    }

    /// A pool at the default shares.
    #[must_use]
    pub fn with_defaults() -> Arc<IoLimiter> {
        IoLimiter::build(IoConfig::default())
    }

    fn build(config: IoConfig) -> Arc<IoLimiter> {
        Arc::new(IoLimiter {
            config,
            state: Mutex::new(State {
                free: config.slots,
                classes: PerClass::default(),
                vtime: 0,
            }),
            waits: PerClass::default(),
        })
    }

    /// Wait for a slot in `class`.
    async fn acquire(self: &Arc<Self>, class: IoClass) -> Slot {
        let t0 = Instant::now();
        let (tx, rx) = oneshot::channel();
        self.state
            .lock()
            .classes
            .get_mut(class)
            .waiters
            .push_back(tx);
        self.dispatch();
        #[expect(
            clippy::expect_used,
            reason = "the pool drops a waiter's sender only after sending to it"
        )]
        let slot = rx.await.expect("a queued waiter always gets a slot");
        self.waits.get(class).record(t0.elapsed());
        slot
    }

    fn release(self: &Arc<Self>, class: IoClass) {
        {
            let mut st = self.state.lock();
            st.classes.get_mut(class).in_flight -= 1;
            st.free += 1;
        }
        self.dispatch();
    }

    /// Hand free slots to waiters, while the policy allows.
    fn dispatch(self: &Arc<Self>) {
        while let Some((tx, class)) = self.next_grant() {
            let slot = Slot {
                io: self.clone(),
                class,
                held: true,
            };
            // The waiter gave up: take the slot back here, as a drop would
            // dispatch again from inside this loop.
            if let Err(mut slot) = tx.send(slot) {
                slot.held = false;
                let mut st = self.state.lock();
                st.classes.get_mut(class).in_flight -= 1;
                st.free += 1;
            }
        }
    }

    /// Take a free slot for the waiter that the policy picks, if any.
    fn next_grant(&self) -> Option<(oneshot::Sender<Slot>, IoClass)> {
        let mut st = self.state.lock();
        let (class, by_reservation) = st.pick(&self.config)?;
        let tx = st.classes.get_mut(class).waiters.pop_front()?;
        st.grant(&self.config, class, by_reservation);
        Some((tx, class))
    }

    /// Slots in use, waiters and queue waits per class, for status.
    #[must_use]
    pub fn status(&self) -> serde_json::Value {
        let st = self.state.lock();
        let classes: serde_json::Map<String, serde_json::Value> = IoClass::ALL
            .iter()
            .map(|&c| {
                let (s, share) = (st.classes.get(c), self.config.share(c));
                let v = serde_json::json!({
                    "reserve": share.reserve,
                    "limit": share.limit,
                    "weight": share.weight,
                    "in_flight": s.in_flight,
                    "waiting": s.waiters.len(),
                    "granted": s.granted,
                    "wait": self.waits.get(c).json(),
                });
                (c.name().to_string(), v)
            })
            .collect();
        serde_json::json!({
            "slots": self.config.slots,
            "free": st.free,
            "classes": classes,
        })
    }
}

/// Queue waits in power-of-two buckets of microseconds.
struct WaitHistogram {
    /// Bucket `i` counts waits below `2^i` µs (and at least `2^(i-1)`).
    buckets: [AtomicU64; BUCKETS],
    total_us: AtomicU64,
}

const BUCKETS: usize = 40;

impl Default for WaitHistogram {
    fn default() -> WaitHistogram {
        WaitHistogram {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            total_us: AtomicU64::new(0),
        }
    }
}

impl WaitHistogram {
    fn record(&self, wait: Duration) {
        let us = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX);
        let i = usize::try_from(u64::BITS - us.leading_zeros()).unwrap_or(BUCKETS);
        if let Some(b) = self.buckets.get(i.min(BUCKETS - 1)) {
            b.fetch_add(1, Relaxed);
        }
        self.total_us.fetch_add(us, Relaxed);
    }

    /// Counts per bucket (by upper bound), with quantiles as bucket bounds.
    fn json(&self) -> serde_json::Value {
        let counts: Vec<u64> = self.buckets.iter().map(|b| b.load(Relaxed)).collect();
        let n: u64 = counts.iter().sum();
        let upper = |i: usize| 1u64.checked_shl(u32::try_from(i).unwrap_or(u32::MAX));
        let quantile = |q: u64| {
            // The first bucket that holds the q-th percent of waits.
            let target = (n * q).div_ceil(100).max(1);
            let mut seen = 0;
            counts.iter().enumerate().find_map(|(i, &c)| {
                seen += c;
                (seen >= target).then(|| upper(i))
            })
        };
        let buckets: serde_json::Map<String, serde_json::Value> = counts
            .iter()
            .enumerate()
            .filter(|&(_, &c)| c > 0)
            .map(|(i, &c)| {
                let bound = upper(i).map_or_else(|| "inf".to_string(), |u| u.to_string());
                (format!("lt_{bound}_us"), serde_json::json!(c))
            })
            .collect();
        serde_json::json!({
            "count": n,
            "total_us": self.total_us.load(Relaxed),
            "p50_lt_us": (n > 0).then(|| quantile(50)).flatten().flatten(),
            "p99_lt_us": (n > 0).then(|| quantile(99)).flatten().flatten(),
            "max_lt_us": counts.iter().rposition(|&c| c > 0).and_then(upper),
            "buckets": buckets,
        })
    }
}

/// A store whose requests take slots from `io`, in `pinned` or the caller's
/// class, each held until its response (or a read's body) is consumed.
pub(super) struct ClassStore {
    pub(super) inner: Arc<dyn ObjectStore>,
    pub(super) io: Arc<IoLimiter>,
    pub(super) pinned: Option<IoClass>,
}

impl fmt::Debug for ClassStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl fmt::Display for ClassStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClassStore({:?}, {})", self.pinned, self.inner)
    }
}

impl ClassStore {
    fn class(&self) -> IoClass {
        self.pinned.unwrap_or_else(IoClass::current)
    }

    async fn slot(&self) -> Slot {
        self.io.acquire(self.class()).await
    }

    /// `stream`, holding a slot of the caller's class while it runs.
    fn held<T: Send + 'static>(
        &self,
        open: impl FnOnce() -> futures::stream::BoxStream<'static, T> + Send + 'static,
    ) -> futures::stream::BoxStream<'static, T> {
        let (io, class) = (self.io.clone(), self.class());
        async move {
            let slot = io.acquire(class).await;
            Holding {
                inner: open(),
                _slot: slot,
            }
        }
        .into_stream()
        .flatten()
        .boxed()
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for ClassStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let _slot = self.slot().await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = {
            let _slot = self.slot().await;
            self.inner.put_multipart_opts(location, opts).await?
        };
        Ok(Box::new(ClassUpload {
            upload,
            io: self.io.clone(),
            // Parts may be put from a blocking thread, outside the task.
            class: self.class(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let slot = self.slot().await;
        let r = self.inner.get_opts(location, options).await?;
        let payload = match r.payload {
            GetResultPayload::Stream(s) => GetResultPayload::Stream(
                Holding {
                    inner: s,
                    _slot: slot,
                }
                .boxed(),
            ),
            file @ GetResultPayload::File(..) => file,
        };
        Ok(GetResult { payload, ..r })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let _slot = self.slot().await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, Result<Path>>,
    ) -> futures::stream::BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        self.held(move || inner.delete_stream(locations))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, Result<ObjectMeta>> {
        let (inner, prefix) = (self.inner.clone(), prefix.cloned());
        self.held(move || inner.list(prefix.as_ref()))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> futures::stream::BoxStream<'static, Result<ObjectMeta>> {
        let (inner, prefix, offset) = (self.inner.clone(), prefix.cloned(), offset.clone());
        self.held(move || inner.list_with_offset(prefix.as_ref(), &offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let _slot = self.slot().await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let _slot = self.slot().await;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let _slot = self.slot().await;
        self.inner.rename_opts(from, to, options).await
    }
}

/// A stream that holds a slot until it is dropped.
struct Holding<S> {
    inner: S,
    _slot: Slot,
}

impl<S: Stream + Unpin> Stream for Holding<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<S::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// A multipart upload whose parts each take a slot of `class`.
#[derive(Debug)]
struct ClassUpload {
    upload: Box<dyn MultipartUpload>,
    io: Arc<IoLimiter>,
    class: IoClass,
}

#[async_trait]
impl MultipartUpload for ClassUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let upload = self.upload.put_part(data);
        let (io, class) = (self.io.clone(), self.class);
        Box::pin(async move {
            let _slot = io.acquire(class).await;
            upload.await
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let _slot = self.io.acquire(self.class).await;
        self.upload.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _slot = self.io.acquire(self.class).await;
        self.upload.abort().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(slots: usize, shares: [Share; 3]) -> IoConfig {
        let [commit, foreground, background] = shares;
        IoConfig {
            slots,
            commit,
            foreground,
            background,
        }
    }

    const fn share(reserve: usize, limit: usize, weight: u32) -> Share {
        Share {
            reserve,
            limit,
            weight,
        }
    }

    /// Takes `n` slots of `class` at once, and holds them.
    async fn take(io: &Arc<IoLimiter>, class: IoClass, n: usize) -> Vec<Slot> {
        let mut out = Vec::new();
        for _ in 0..n {
            out.push(io.acquire(class).await);
        }
        out
    }

    fn queue(io: &Arc<IoLimiter>, class: IoClass) -> tokio::task::JoinHandle<Slot> {
        let io = io.clone();
        tokio::spawn(async move { io.acquire(class).await })
    }

    async fn settle() {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn a_reservation_stays_free_for_its_class() {
        let io =
            IoLimiter::new(config(8, [share(2, 8, 1), share(0, 8, 1), share(0, 8, 1)])).unwrap();
        let held = take(&io, IoClass::Background, 6).await;
        let blocked = queue(&io, IoClass::Background);
        settle().await;
        assert!(!blocked.is_finished(), "background took a reserved slot");
        // Commits still get both of their slots at once.
        let commits = take(&io, IoClass::Commit, 2).await;
        drop(commits);
        drop(held);
        blocked.await.unwrap();
    }

    #[tokio::test]
    async fn a_limit_caps_a_class_with_free_slots() {
        let io =
            IoLimiter::new(config(8, [share(0, 8, 1), share(0, 8, 1), share(0, 2, 1)])).unwrap();
        let _held = take(&io, IoClass::Background, 2).await;
        let blocked = queue(&io, IoClass::Background);
        settle().await;
        assert!(!blocked.is_finished());
        assert_eq!(take(&io, IoClass::Foreground, 6).await.len(), 6);
    }

    #[tokio::test]
    async fn free_slots_go_by_weight() {
        let io =
            IoLimiter::new(config(1, [share(0, 1, 3), share(0, 1, 1), share(0, 1, 1)])).unwrap();
        let first = io.acquire(IoClass::Foreground).await;
        let mut waiting = Vec::new();
        for _ in 0..40 {
            waiting.push((IoClass::Commit, queue(&io, IoClass::Commit)));
            waiting.push((IoClass::Foreground, queue(&io, IoClass::Foreground)));
        }
        settle().await;
        drop(first);
        // Of the first 20 grants, commits (weight 3) get about three in four.
        let mut commits = 0;
        for _ in 0..20 {
            settle().await;
            let i = waiting.iter().position(|(_, h)| h.is_finished()).unwrap();
            let (class, h) = waiting.swap_remove(i);
            commits += usize::from(class == IoClass::Commit);
            drop(h.await.unwrap());
        }
        assert!((14..=16).contains(&commits), "{commits} of 20 to commits");
    }

    #[tokio::test]
    async fn a_waiter_that_gives_up_loses_no_slot() {
        let io =
            IoLimiter::new(config(1, [share(0, 1, 1), share(0, 1, 1), share(0, 1, 1)])).unwrap();
        let held = io.acquire(IoClass::Foreground).await;
        let gone = queue(&io, IoClass::Background);
        settle().await;
        gone.abort();
        drop(gone.await);
        drop(held);
        let _again = io.acquire(IoClass::Commit).await;
        assert_eq!(io.state.lock().free, 0);
    }

    async fn spawned_class() -> IoClass {
        spawn(futures::future::lazy(|_| IoClass::current()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn the_task_class_reaches_spawned_tasks() {
        let class = IoClass::Background.scope(spawned_class()).await;
        assert_eq!(class, IoClass::Background);
        assert_eq!(IoClass::current(), IoClass::Foreground);
    }

    #[tokio::test]
    async fn waits_are_counted_per_class() {
        let io = IoLimiter::new(IoConfig::default()).unwrap();
        drop(io.acquire(IoClass::Commit).await);
        let s = io.status();
        assert_eq!(s["classes"]["commit"]["wait"]["count"], 1);
        assert_eq!(s["classes"]["foreground"]["wait"]["count"], 0);
    }

    #[test]
    fn shares_must_fit_the_pool() {
        let c = IoConfig {
            slots: 20,
            ..IoConfig::default()
        };
        assert!(c.check().unwrap_err().to_string().contains("reservations"));
        let mut c = IoConfig::default();
        c.background.weight = 0;
        assert!(
            c.check()
                .unwrap_err()
                .to_string()
                .contains("background.weight")
        );
    }
}
