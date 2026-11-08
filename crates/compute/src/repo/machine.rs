//! Resumable work over layer data that may not be local yet.
//!
//! Nothing synchronous waits on a remote tier: reads of bytes that are not
//! on local disk fail with [`NotResident`].
//! Algorithms that read layer data are therefore state machines
//! ([`Machine`]): `run` makes all the progress it can with local bytes and
//! parks each item that needs remote bytes under a [`Ticket`]; [`drive`]
//! loads what parked items need, each load as its own task, and resumes a
//! ticket as soon as all of its loads are done, without waiting for the
//! others. Nothing is ever restarted or redone.
//!
//! [`Objects`] is the common case: a queue of items that each need one
//! object's content (tree walks, tag peeling).

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use foldhash::{HashMap, HashMapExt};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use roaring::RoaringBitmap;

use enroute_shared::store::io;

use crate::git::object::{tag_target, tree_entries};
use crate::git::oid::Kind;
use crate::storage::{Access, NotResident, Pinned, load_pinned};

use super::view::{DenseId, Snapshot};

/// Identifies a parked item.
pub(crate) type Ticket = u64;

/// What a machine waits for: bytes to load, or any other event.
enum Wait {
    Load(Vec<NotResident>, Access),
    Event(futures::future::BoxFuture<'static, Result<()>>),
}

/// Waits requested by a [`Machine`] run.
#[derive(Default)]
pub(crate) struct Waits {
    next: Ticket,
    new: Vec<(Ticket, Wait)>,
}

impl Waits {
    /// Park something until all of `need` is local; the machine is resumed
    /// with the returned ticket then.
    pub(crate) fn park(&mut self, need: Vec<NotResident>) -> Ticket {
        self.park_as(need, Access::Hot)
    }

    /// [`park`](Self::park), telling the disk cache how the bytes will be
    /// used (`Scan`: once, e.g. streamed into a pack).
    pub(crate) fn park_as(&mut self, need: Vec<NotResident>, access: Access) -> Ticket {
        self.add(Wait::Load(need, access))
    }

    /// Park something until `event` completes (e.g. work on the CPU pool, or
    /// memory becoming free).
    pub(crate) fn park_on(
        &mut self,
        event: impl Future<Output = Result<()>> + Send + 'static,
    ) -> Ticket {
        self.add(Wait::Event(Box::pin(event)))
    }

    fn add(&mut self, w: Wait) -> Ticket {
        let t = self.next;
        self.next += 1;
        self.new.push((t, w));
        t
    }
}

/// An algorithm over layer data, resumable when remote bytes arrive.
pub(crate) trait Machine: Send + 'static {
    type Output: Send + 'static;

    /// Make all the progress possible with local bytes (reads of other
    /// bytes fail instead of waiting), parking what needs remote bytes.
    /// Returns the output once everything is done.
    fn run(&mut self, waits: &mut Waits) -> Result<Option<Self::Output>>;

    /// What `ticket` waited for has happened. Bytes it waited for are
    /// local, and stay so while `pins` live: a machine keeps them until it
    /// has read the bytes (so the disk cache cannot evict them first).
    fn resume(&mut self, ticket: Ticket, pins: Vec<Pinned>);
}

/// Run `m` to completion: synchronous steps on a blocking thread, loads as
/// tasks, and a ticket resumed as soon as its own loads are done.
pub(crate) async fn drive<M: Machine>(mut m: M) -> Result<M::Output> {
    let mut waits = Waits::default();
    // Waits in flight, each spawned (it progresses while `run` computes).
    let mut pending = FuturesUnordered::new();
    loop {
        let (m2, w2, r) = tokio::task::spawn_blocking(move || {
            let r = m.run(&mut waits);
            (m, waits, r)
        })
        .await?;
        (m, waits) = (m2, w2);
        if let Some(out) = r? {
            return Ok(out);
        }
        for (t, wait) in waits.new.drain(..) {
            let h = match wait {
                // Loads of the same bytes for other tickets share one fetch.
                Wait::Load(need, access) => io::spawn(load_pinned(need, access)),
                Wait::Event(f) => io::spawn(async move { f.await.map(|()| Vec::new()) }),
            };
            pending.push(async move { (t, h.await) });
        }
        // Wait for the next ticket, then take whatever else is ready.
        let Some((t, pins)) = pending.next().await else {
            bail!("no progress: nothing ready and nothing loading");
        };
        m.resume(t, pins??);
        while let Some(Some((t, pins))) = futures::FutureExt::now_or_never(pending.next()) {
            m.resume(t, pins??);
        }
    }
}

/// Work on items that each need (at most) one object's content.
pub(crate) trait ObjectWork: Send + 'static {
    type Item: Send + 'static;
    type Output: Send + 'static;

    /// The object `item` needs, or `None` if it needs none (e.g. a tree a
    /// walk has already seen).
    fn object(&self, snap: &Snapshot, item: &Self::Item) -> Option<DenseId>;

    /// Process `item` with that object's content, queueing follow-up items.
    fn process(
        &mut self,
        snap: &Snapshot,
        item: Self::Item,
        content: Option<Arc<Vec<u8>>>,
        more: &mut Vec<Self::Item>,
    ) -> Result<()>;

    fn finish(self) -> Self::Output;
}

/// A [`Machine`] over [`ObjectWork`]: items whose object is local are
/// processed at once; the others are parked until their object, with its
/// whole delta chain, is loaded.
pub(crate) struct Objects<W: ObjectWork> {
    snap: Arc<Snapshot>,
    work: Option<W>,
    /// Items to process, with pins on what they waited for.
    ready: Vec<(W::Item, Vec<Pinned>)>,
    parked: HashMap<Ticket, W::Item>,
}

impl<W: ObjectWork> Objects<W> {
    pub(crate) fn new(
        snap: Arc<Snapshot>,
        work: W,
        items: impl IntoIterator<Item = W::Item>,
    ) -> Self {
        Objects {
            snap,
            work: Some(work),
            ready: items.into_iter().map(|i| (i, Vec::new())).collect(),
            parked: HashMap::new(),
        }
    }
}

impl<W: ObjectWork> Machine for Objects<W> {
    type Output = W::Output;

    #[expect(clippy::expect_used, reason = "running a finished machine is a bug")]
    fn run(&mut self, waits: &mut Waits) -> Result<Option<W::Output>> {
        let work = self.work.as_mut().expect("not finished");
        let mut more = Vec::new();
        while let Some((item, _pins)) = self.ready.pop() {
            let content = match work.object(&self.snap, &item) {
                None => None,
                Some(d) => match self.snap.read(d) {
                    Ok(c) => Some(c),
                    Err(e) => {
                        let t = waits.park(self.snap.missing_after(d, e)?);
                        self.parked.insert(t, item);
                        continue;
                    }
                },
            };
            work.process(&self.snap, item, content, &mut more)?;
            self.ready.extend(more.drain(..).map(|i| (i, Vec::new())));
        }
        if !self.parked.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.work.take().expect("not finished").finish()))
    }

    fn resume(&mut self, ticket: Ticket, pins: Vec<Pinned>) {
        if let Some(item) = self.parked.remove(&ticket) {
            self.ready.push((item, pins));
        }
    }
}

/// Make objects readable without waiting (their entries and whole delta
/// chains local), loading what is not; they stay local while the returned
/// pins live.
pub(crate) async fn load_objects(snap: &Arc<Snapshot>, ds: Vec<DenseId>) -> Result<Vec<Pinned>> {
    if ds.is_empty() {
        return Ok(Vec::new());
    }
    let s = snap.clone();
    let need = tokio::task::spawn_blocking(move || -> Result<Vec<NotResident>> {
        let mut need = Vec::new();
        for d in ds {
            need.extend(s.missing_all(d)?);
        }
        Ok(need)
    })
    .await??;
    load_pinned(need, Access::Hot).await
}

impl Snapshot {
    /// The content of `ds` (deltas resolved), each once, loading what is not
    /// local.
    pub(crate) async fn load_many(
        self: &Arc<Self>,
        ds: impl IntoIterator<Item = DenseId>,
    ) -> Result<HashMap<DenseId, Arc<Vec<u8>>>> {
        struct Read(HashMap<DenseId, Arc<Vec<u8>>>);
        impl ObjectWork for Read {
            type Item = DenseId;
            type Output = HashMap<DenseId, Arc<Vec<u8>>>;
            fn object(&self, _: &Snapshot, d: &DenseId) -> Option<DenseId> {
                (!self.0.contains_key(d)).then_some(*d)
            }
            fn process(
                &mut self,
                _: &Snapshot,
                d: DenseId,
                c: Option<Arc<Vec<u8>>>,
                _: &mut Vec<DenseId>,
            ) -> Result<()> {
                // No content: read already.
                self.0.extend(c.map(|c| (d, c)));
                Ok(())
            }
            fn finish(self) -> Self::Output {
                self.0
            }
        }
        let ds: Vec<DenseId> = ds.into_iter().collect();
        drive(Objects::new(self.clone(), Read(HashMap::new()), ds)).await
    }

    /// The content of `d` (deltas resolved), loading it if it is not local.
    pub(crate) async fn load(self: &Arc<Self>, d: DenseId) -> Result<Arc<Vec<u8>>> {
        self.load_many([d])
            .await?
            .remove(&d)
            .context("object not read")
    }
}

/// Every object reachable from some trees (including them), skipping
/// objects of kind `skip`: each tree is parsed as soon as it is local.
struct TreeWalk {
    out: RoaringBitmap,
    skip: Option<Kind>,
}

impl ObjectWork for TreeWalk {
    type Item = DenseId;
    type Output = RoaringBitmap;

    fn object(&self, _: &Snapshot, t: &DenseId) -> Option<DenseId> {
        (!self.out.contains(*t)).then_some(*t)
    }

    fn process(
        &mut self,
        snap: &Snapshot,
        t: DenseId,
        data: Option<Arc<Vec<u8>>>,
        more: &mut Vec<DenseId>,
    ) -> Result<()> {
        // No content: seen already. Parked twice: the first one won.
        let Some(data) = data else { return Ok(()) };
        if !self.out.insert(t) {
            return Ok(());
        }
        for e in tree_entries(snap.format(), &data) {
            let e = e?;
            let Some(k) = e.kind() else { continue };
            if Some(k) == self.skip {
                continue;
            }
            let Some(d) = snap.lookup_kind(&e.oid, k) else {
                anyhow::bail!("missing {} {}", k.name(), e.oid);
            };
            if k != Kind::Tree {
                self.out.insert(d);
            } else if !self.out.contains(d) {
                more.push(d);
            }
        }
        Ok(())
    }

    fn finish(self) -> RoaringBitmap {
        self.out
    }
}

/// Everything reachable from `roots` (see [`TreeWalk`]).
pub(crate) async fn tree_closure(
    snap: &Arc<Snapshot>,
    roots: Vec<DenseId>,
    skip: Option<Kind>,
) -> Result<RoaringBitmap> {
    drive(Objects::new(
        snap.clone(),
        TreeWalk {
            out: RoaringBitmap::new(),
            skip,
        },
        roots,
    ))
    .await
}

/// An annotated tag followed to the first non-tag object.
pub(crate) struct Peeled {
    pub start: DenseId,
    /// The tags passed (`start` first, if it is a tag).
    pub tags: Vec<DenseId>,
    /// The first non-tag object; `None` if a target is missing or the chain
    /// is too deep.
    pub end: Option<DenseId>,
}

struct Peel(Vec<Peeled>);

impl ObjectWork for Peel {
    type Item = Peeled;
    type Output = Vec<Peeled>;

    fn object(&self, snap: &Snapshot, p: &Peeled) -> Option<DenseId> {
        let cur = p.end?;
        (snap.kind(cur) == Kind::Tag && p.tags.len() < 64).then_some(cur)
    }

    #[expect(
        clippy::expect_used,
        reason = "content is read only for a tag with a target"
    )]
    fn process(
        &mut self,
        snap: &Snapshot,
        mut p: Peeled,
        data: Option<Arc<Vec<u8>>>,
        more: &mut Vec<Peeled>,
    ) -> Result<()> {
        let Some(data) = data else {
            if p.end.is_some_and(|d| snap.kind(d) == Kind::Tag) {
                p.end = None; // too deep
            }
            self.0.push(p);
            return Ok(());
        };
        let cur = p.end.expect("reading a tag");
        p.tags.push(cur);
        p.end = snap.lookup(&tag_target(&data)?);
        match p.end {
            Some(_) => more.push(p),
            None => self.0.push(p),
        }
        Ok(())
    }

    fn finish(self) -> Vec<Peeled> {
        self.0
    }
}

/// Peel each of `ds` (in order of `ds`).
pub(crate) async fn peel(snap: &Arc<Snapshot>, ds: Vec<DenseId>) -> Result<Vec<Peeled>> {
    let order: HashMap<DenseId, usize> = ds.iter().enumerate().map(|(i, d)| (*d, i)).collect();
    let items = ds.into_iter().map(|d| Peeled {
        start: d,
        tags: Vec::new(),
        end: Some(d),
    });
    let mut out = drive(Objects::new(snap.clone(), Peel(Vec::new()), items)).await?;
    #[expect(clippy::indexing_slicing, reason = "every peeled start is in `order`")]
    out.sort_by_key(|p| order[&p.start]);
    Ok(out)
}
