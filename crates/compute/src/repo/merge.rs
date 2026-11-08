//! Merging a run of a snapshot's layers into a new layer file, for flushes
//! and compaction.

use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::storage::{Access, Layer, LayerBuilder, NewCommit, NewObject, Pinned, Workspace};
use crate::util::blocking;

use super::view::{LayerView, Snapshot};

/// Bytes of a source layer's entries loaded at a time. A merge holds two
/// windows: the one it copies, and the next one, loading meanwhile.
pub(crate) const WINDOW: u64 = 64 << 20;

/// Source layers in one window at most: a backlog of tiny layers shares
/// windows, but each still takes a load.
const WINDOW_LAYERS: usize = 4096;

/// The layer produced by a merge, not yet written (see
/// [`Storage::publish_built`](crate::storage::Storage::publish_built)).
pub(crate) struct Merged {
    pub builder: LayerBuilder,
    pub objects: u32,
    /// The id range of the source layers.
    pub ids: Range<u32>,
}

/// Objects `locals` of source layer `layer`, stored at `bytes`, copied
/// with the layer's commits after them if `last`.
#[derive(Clone)]
struct Piece {
    layer: usize,
    locals: Range<u32>,
    bytes: Range<u64>,
    last: bool,
}

/// Merge layers `range` of `snap` into one layer that keeps every object's
/// id: `None` if every object of the run has an older copy.
///
/// Sources are in id order, which is a topological order: a layer refers
/// only to itself and to older layers. Entries are copied verbatim, in
/// storage order (which is table order): layer by layer, oldest first, so
/// every delta's base precedes it (see `replay`). Copies of objects with an
/// older copy anywhere in the snapshot are dropped (leaving holes in the
/// id range).
///
/// Memory and local disk stay bounded whatever the size of the run: the
/// merge loads entries `window` bytes at a time (the next window while it
/// copies one), never waiting on a remote tier in a blocking thread.
#[expect(
    clippy::indexing_slicing,
    reason = "the caller passes a non-empty range of the snapshot"
)]
pub(crate) async fn merge_layers(
    snap: &Arc<Snapshot>,
    range: Range<usize>,
    ws: Workspace,
    window: u64,
) -> Result<Option<Merged>> {
    let views: Arc<[Arc<LayerView>]> = snap.layers[range].into();
    let ids = views[0].base..views[views.len() - 1].end();
    let mut b = LayerBuilder::new(&ws, snap.format());
    b.set_ids(ids.clone());
    let cut = views.clone();
    let windows = blocking(move || windows(&cut, window)).await?;
    let mut loads = windows.iter().map(|w| load_window(&views, w));
    let mut next = loads.next();
    for w in &windows {
        let Some(load) = next else { break };
        next = loads.next();
        let pins = load.await??;
        let (views, w) = (views.clone(), w.clone());
        b = with_builder(b, move |b| copy_window(b, &views, &w)).await?;
        drop(pins);
    }
    if b.is_empty() {
        return Ok(None);
    }
    let objects = u32::try_from(b.object_count()).context("too many objects in a layer")?;
    let graph =
        blocking(move || super::view::merged_graph(&views).context("commit graph too large"))
            .await?;
    b.set_graph(graph);
    Ok(Some(Merged {
        builder: b,
        objects,
        ids,
    }))
}

/// `views` cut into windows of about `window` bytes each (at least one
/// object per piece), in storage order.
fn windows(views: &[Arc<LayerView>], window: u64) -> Result<Vec<Vec<Piece>>> {
    let mut out: Vec<Vec<Piece>> = Vec::new();
    let mut room = 0u64;
    for (layer, lv) in views.iter().enumerate() {
        let l = &lv.layer;
        let mut start = 0;
        while start < l.len() {
            if room == 0 || out.last().is_some_and(|w| w.len() >= WINDOW_LAYERS) {
                out.push(Vec::new());
                room = window;
            }
            let from = entry_start(l, start)?;
            let end = fit(l, start, from.saturating_add(room))?;
            let to = entry_start(l, end)?;
            room = room.saturating_sub(to - from);
            if let Some(w) = out.last_mut() {
                w.push(Piece {
                    layer,
                    locals: start..end,
                    bytes: from..to,
                    last: end == l.len(),
                });
            }
            start = end;
        }
    }
    Ok(out)
}

/// Where the entry of `local` starts in `l`: the end of its data region
/// past the last object.
fn entry_start(l: &Layer, local: u32) -> Result<u64> {
    if local == l.len() {
        return Ok(l.data_end());
    }
    Ok(l.record(local)?.entry_off())
}

/// The first object of `l` after `start` whose entry does not end by
/// `limit`: `start + 1` at least, so every piece makes progress.
fn fit(l: &Layer, start: u32, limit: u64) -> Result<u32> {
    let (mut lo, mut hi) = (start + 1, l.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let fits = entry_start(l, mid)? < limit;
        (lo, hi) = if fits { (mid + 1, hi) } else { (lo, mid) };
    }
    Ok(lo)
}

/// Start loading the bytes of window `w` of `views`. Spawned, so that it
/// loads while the window before it is copied.
#[expect(clippy::indexing_slicing, reason = "pieces index `views`")]
fn load_window(
    views: &[Arc<LayerView>],
    w: &[Piece],
) -> tokio::task::JoinHandle<Result<Vec<Pinned>>> {
    let loads: Vec<_> = w
        .iter()
        .map(|p| views[p.layer].layer.load_range(p.bytes.clone()))
        .collect();
    enroute_shared::store::io::spawn(futures::future::try_join_all(loads))
}

/// Add the canonical objects of window `w` of `views` (local now) to `b`,
/// with their ids.
#[expect(clippy::indexing_slicing, reason = "pieces index `views`")]
fn copy_window(b: &mut LayerBuilder, views: &[Arc<LayerView>], w: &[Piece]) -> Result<()> {
    for p in w {
        let lv = &views[p.layer];
        for local in p.locals.clone() {
            if lv.dense(local) != lv.own(local) {
                continue;
            }
            let rec = lv.layer.object_with(local, Access::Scan)?;
            b.add(NewObject {
                oid: rec.oid,
                kind: rec.kind,
                size: rec.size,
                entry: &rec.entry,
                base: lv.layer.ref_oid(rec.base),
                base_ctx: lv.layer.ref_oid(rec.ctx),
                depth: rec.depth,
                id: Some(lv.own(local)),
            })?;
        }
        if p.last {
            copy_commits(b, lv)?;
        }
    }
    Ok(())
}

/// Run `f` on `b` on the blocking pool.
async fn with_builder(
    mut b: LayerBuilder,
    f: impl FnOnce(&mut LayerBuilder) -> Result<()> + Send + 'static,
) -> Result<LayerBuilder> {
    blocking(move || {
        f(&mut b)?;
        Ok(b)
    })
    .await
}

/// Add the canonical commits of `lv` to `b`.
fn copy_commits(b: &mut LayerBuilder, lv: &LayerView) -> Result<()> {
    let l = &lv.layer;
    for c in l.commits()? {
        if lv.dense(c.local) != lv.own(c.local) {
            continue;
        }
        b.add_commit(&NewCommit {
            oid: l.oid(c.local),
            tree: l.ref_oid(c.tree).context("commit without tree")?,
            parents: c
                .parents
                .iter()
                .map(|p| l.ref_oid(*p).context("bad parent"))
                .collect::<Result<_>>()?,
            time: c.time,
            members: c.members.iter().filter_map(|m| l.ref_oid(*m)).collect(),
        })?;
    }
    Ok(())
}
