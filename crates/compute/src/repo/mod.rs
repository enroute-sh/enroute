//! Repositories as seen by this node: materialized, immutable snapshots that
//! follow the replicated state. A view outlives its subscription: if that
//! ends, the view keeps serving the last state until it is followed again
//! or the compute evicts it.

pub(crate) mod graph;
pub(crate) mod ingest;
pub(crate) mod machine;
pub(crate) mod merge;
pub(crate) mod packindex;
pub(crate) mod replay;
pub(crate) mod serve;
pub(crate) mod view;

use std::collections::BTreeMap;

use foldhash::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use tokio::sync::watch;

use crate::meta::RepoState;
use crate::storage::{Layer, Storage};
use enroute_shared::state::LayerMeta;
use view::{Caches, Snapshot};

/// One repository's view and the state that builds it.
pub struct Repo {
    pub name: String,
    view: ArcSwap<Snapshot>,
    /// Latest metadata from the state machine not yet materialized.
    pending: Mutex<Option<Arc<RepoState>>>,
    materializing: tokio::sync::Mutex<()>,
    version_tx: watch::Sender<u64>,
    /// When the view was last used (for eviction).
    used: Mutex<Instant>,
    /// New inline layers being opened (or opened) ahead of the snapshot
    /// that will include them, by first id (and layer id): started as their
    /// commands apply, in parallel, so the materializer (one at a time per
    /// repository) finds them open. Dropped below each published state's
    /// `next_id`: included by then, or replaced before.
    opening: Mutex<BTreeMap<u64, (String, Opening)>>,
    /// The repository's `next_id` as last applied: layers with ids from it
    /// on are new pushes.
    seen_next_id: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for Repo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Repo")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

type Opening = futures::future::Shared<
    futures::future::BoxFuture<'static, Result<Arc<Layer>, Arc<anyhow::Error>>>,
>;

/// Layers opened ahead and not yet in a snapshot, at most, per repository
/// (a stuck materializer does not pile them up).
const MAX_OPENING: usize = 4096;

impl Repo {
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.view.load_full()
    }

    /// Whether a state has been materialized.
    pub fn exists(&self) -> bool {
        self.view.load().version() > 0
    }

    /// Note a use of the view (it is kept a while longer).
    pub(crate) fn touch(&self) {
        *self.used.lock() = Instant::now();
    }

    /// Where the view stands: its version, the newest state waiting to be
    /// materialized, layers being opened ahead, and whether a snapshot is
    /// being built.
    pub fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "view": self.snapshot().version(),
            "layers": self.snapshot().layers.len(),
            "pending": self.pending.lock().as_ref().map(|m| m.version),
            "opening": self.opening.lock().len(),
            "building": self.materializing.try_lock().is_err(),
        })
    }

    /// Wait until the materialized view reflects at least `version`.
    pub(crate) async fn wait_version(&self, version: u64) -> Result<Arc<Snapshot>> {
        let mut rx = self.version_tx.subscribe();
        tokio::time::timeout(Duration::from_secs(30), rx.wait_for(|v| *v >= version))
            .await
            .context("timed out waiting for repository view")??;
        Ok(self.snapshot())
    }
}

/// The repositories of this node, by name.
pub(crate) struct Repos {
    storage: Arc<Storage>,
    cache: Arc<Caches>,
    by_name: RwLock<HashMap<String, Arc<Repo>>>,
}

impl std::fmt::Debug for Repos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Repos").finish_non_exhaustive()
    }
}

impl Repos {
    /// Creates an empty set of repositories over `storage`.
    pub(crate) fn new(storage: Arc<Storage>, cache_bytes: usize) -> Arc<Self> {
        Arc::new(Repos {
            storage,
            cache: Arc::new(Caches::new(cache_bytes)),
            by_name: RwLock::default(),
        })
    }

    /// The storage that backs the repositories.
    #[cfg(test)]
    pub(crate) fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }

    /// The materialized repository `name`, if it exists.
    pub(crate) fn get(&self, name: &str) -> Option<Arc<Repo>> {
        self.by_name
            .read()
            .get(name)
            .cloned()
            .filter(|r| r.exists())
    }

    /// Handle for `name`, whether or not it has been materialized yet.
    pub(crate) fn handle(&self, name: &str) -> Arc<Repo> {
        if let Some(r) = self.by_name.read().get(name) {
            return r.clone();
        }
        self.by_name
            .write()
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(Repo {
                    name: name.to_string(),
                    view: ArcSwap::from_pointee(Snapshot::empty(self.cache.clone())),
                    pending: Mutex::new(None),
                    materializing: tokio::sync::Mutex::new(()),
                    version_tx: watch::channel(0).0,
                    used: Mutex::new(Instant::now()),
                    opening: Mutex::default(),
                    seen_next_id: 0.into(),
                })
            })
            .clone()
    }

    /// A new state of `name` (must be cheap): materialized in the
    /// background.
    pub(crate) fn on_apply(self: &Arc<Self>, name: &str, meta: &Arc<RepoState>) {
        let repo = self.handle(name);
        self.open_ahead(&repo, meta);
        {
            let mut p = repo.pending.lock();
            if p.as_ref().is_some_and(|m| m.version >= meta.version) {
                return;
            }
            *p = Some(meta.clone());
        }
        let this = self.clone();
        tokio::spawn(async move { this.materialize(repo).await });
    }

    /// Start opening the inline layers `meta` added since the last
    /// command (pushes: appended, with ids from the previous `next_id` on),
    /// each in a task of its own.
    fn open_ahead(&self, repo: &Arc<Repo>, meta: &RepoState) {
        let prev = repo
            .seen_next_id
            .fetch_max(meta.next_id, std::sync::atomic::Ordering::AcqRel);
        if meta.next_id <= prev {
            return;
        }
        let new = meta
            .layers
            .iter()
            .rev()
            .take_while(|m| u64::from(m.first_id) >= prev);
        let mut opening = repo.opening.lock();
        for m in new {
            let Some(data) = m.data.clone() else { continue };
            if opening.len() >= MAX_OPENING {
                return;
            }
            let (storage, id, root) = (self.storage.clone(), m.id.clone(), m.root.clone());
            let open: futures::future::BoxFuture<'static, _> = Box::pin(async move {
                storage
                    .open_inline_layer(&id, &root, &data)
                    .await
                    .map_err(Arc::new)
            });
            let open = futures::FutureExt::shared(open);
            // Polled to completion in a task of its own; the materializer
            // awaits the same result.
            tokio::spawn(open.clone());
            opening.insert(m.first_id.into(), (m.id.clone(), open));
        }
    }

    async fn materialize(self: Arc<Self>, repo: Arc<Repo>) {
        let _g = repo.materializing.lock().await;
        let mut backoff = Duration::from_millis(50);
        loop {
            let Some(meta) = repo.pending.lock().clone() else {
                return;
            };
            let cur = repo.snapshot();
            if meta.version <= cur.version() {
                repo.pending.lock().take_if(|m| m.version <= cur.version());
                return;
            }
            let t0 = Instant::now();
            match self.build_snapshot(&repo, &cur, &meta).await {
                Ok((snap, built)) => {
                    Self::publish(&repo, snap, built, t0);
                    backoff = Duration::from_millis(50);
                }
                Err(e) => {
                    tracing::warn!(repo = %repo.name, "materialize failed (retrying): {e:#}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    /// Make `snap` the view of `repo` and start follow-up work.
    fn publish(repo: &Arc<Repo>, snap: Snapshot, built: usize, t0: Instant) {
        tracing::debug!(
            repo = repo.name,
            version = snap.version(),
            layers = snap.layers.len(),
            built,
            build_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX),
            "snapshot built"
        );
        let v = snap.version();
        let snap = Arc::new(snap);
        repo.view.store(snap.clone());
        repo.version_tx.send_replace(v);
        repo.pending.lock().take_if(|m| m.version <= v);
        let mut opening = repo.opening.lock();
        *opening = opening.split_off(&snap.meta.next_id);
    }

    /// The snapshot of `meta` following `cur`, and how many views it built.
    async fn build_snapshot(
        &self,
        repo: &Repo,
        cur: &Arc<Snapshot>,
        meta: &Arc<RepoState>,
    ) -> Result<(Snapshot, usize)> {
        let reuse = cur.reuse(meta.clone());
        // Open the layers that need views concurrently (a cold node may
        // need many); building views stays sequential.
        // A plain loop: a closure over `&LayerMeta` here makes the future
        // fail `Send` checks (higher-ranked lifetimes).
        let mut opens = Vec::new();
        for m in reuse.missing() {
            opens.push(self.open_missing(repo, m));
        }
        let opened = futures::future::try_join_all(opens).await?;
        let built = opened.len();
        let prev = cur.clone();
        let snap = tokio::task::spawn_blocking(move || prev.build(reuse, opened)).await??;
        Ok((snap, built))
    }

    /// Open the layer `m`, from the open ahead if there is one.
    async fn open_missing(&self, repo: &Repo, m: &LayerMeta) -> Result<Arc<Layer>> {
        let ahead = repo
            .opening
            .lock()
            .get(&m.first_id.into())
            .filter(|(id, _)| *id == m.id)
            .map(|(_, o)| o.clone());
        match (&m.data, ahead) {
            (Some(_), Some(open)) => open.await.map_err(|e| anyhow::anyhow!("{e:#}")),
            (Some(data), None) => self.storage.open_inline_layer(&m.id, &m.root, data).await,
            (None, _) => self.storage.open_layer(&repo.name, m).await,
        }
    }

    /// Drop the views of repositories not used for `idle` that `keep`
    /// does not keep (readers holding a snapshot keep it), and let their
    /// local copies go. Returns the repositories dropped.
    pub(crate) fn evict(&self, idle: Duration, keep: impl Fn(&str) -> bool) -> Vec<String> {
        let idle_repos: Vec<String> = self
            .by_name
            .read()
            .iter()
            .filter(|(name, r)| r.used.lock().elapsed() >= idle && !keep(name))
            .map(|(name, _)| name.clone())
            .collect();
        for name in &idle_repos {
            self.remove(name);
        }
        idle_repos
    }

    /// Drop the view of `name`. Its local copies go once its layers close.
    pub(crate) fn remove(&self, name: &str) {
        self.by_name.write().remove(name);
    }

    /// The names of the repositories that exist.
    pub(crate) fn names(&self) -> Vec<String> {
        self.by_name
            .read()
            .iter()
            .filter(|(_, r)| r.exists())
            .map(|(k, _)| k.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::oid::{Kind, ObjectFormat, Oid, try_hash_object};
    use crate::git::pack::{encode_entry_header, full_entry, pack_of};
    use crate::storage::{Access, Sealed};
    use bytes::Bytes;
    use enroute_shared::state::LayerMeta;

    // Pack type code of a blob.
    const BLOB_CODE: u8 = 3;

    /// Repositories over a store in a directory of their own.
    fn repos() -> (tempfile::TempDir, Arc<Repos>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::for_test(dir.path());
        (dir, Repos::new(storage, 16 << 20))
    }

    /// A layer of `blobs`, ingested against an empty snapshot (so each is
    /// new there, whatever other layers hold), and its object count.
    async fn layer_of(repos: &Repos, blobs: &[&[u8]]) -> (Sealed, u32) {
        let entries: Vec<_> = blobs.iter().map(|b| full_entry(Kind::Blob, b)).collect();
        layer_of_entries(repos, &entries).await
    }

    /// A layer of pack `entries`, as [`layer_of`] builds it.
    async fn layer_of_entries(repos: &Repos, entries: &[Vec<u8>]) -> (Sealed, u32) {
        let pack = pack_of(ObjectFormat::Sha1, entries);
        let empty = repos.handle("empty").snapshot();
        let pack: Arc<dyn crate::git::io::ReadAt> = Arc::new(crate::git::io::Owned(pack));
        let ws = repos.storage().workspace();
        ingest::ingest_stored(empty, pack, ws, vec![], None, u64::MAX)
            .await
            .unwrap()
            .layer
            .unwrap()
    }

    async fn stored(repos: &Repos, repo: &str, (sealed, objects): (Sealed, u32)) -> LayerMeta {
        let meta = LayerMeta {
            id_span: objects,
            ..sealed.meta(objects, 0)
        };
        repos
            .storage()
            .publish_layer(repo, &meta.id, sealed)
            .await
            .unwrap();
        meta
    }

    async fn inline((sealed, objects): (Sealed, u32)) -> LayerMeta {
        let meta = LayerMeta {
            id_span: objects,
            ..sealed.meta(objects, 0)
        };
        LayerMeta {
            data: Some(sealed.into_bytes().await.unwrap()),
            ..meta
        }
    }

    /// `a` and `b` as layer metadata, in that order: the stored one first,
    /// or the inline one.
    async fn stored_and_inline(
        repos: &Repos,
        repo: &str,
        a: (Sealed, u32),
        b: (Sealed, u32),
        stored_first: bool,
    ) -> (LayerMeta, LayerMeta) {
        if stored_first {
            (stored(repos, repo, a).await, inline(b).await)
        } else {
            (inline(a).await, stored(repos, repo, b).await)
        }
    }

    /// Each object's id in the source layers, by oid.
    fn source_ids(snap: &Snapshot, layers: &[LayerMeta]) -> HashMap<Oid, u32> {
        let mut ids = HashMap::default();
        for (lv, m) in snap.layers.iter().zip(layers) {
            lv.layer
                .for_each_oid(|l, oid| {
                    ids.entry(*oid).or_insert(lv.layer.id_of(m.first_id, l));
                })
                .unwrap();
        }
        ids
    }

    /// Publish a merge output (`streamed` straight into its upload, or
    /// through a local file), check it, and return its root, object count
    /// and id range.
    async fn publish_merged(
        repos: &Repos,
        repo: &str,
        m: merge::Merged,
        streamed: bool,
        source_ids: &HashMap<Oid, u32>,
    ) -> (Bytes, u32, std::ops::Range<u32>) {
        let (objects, range) = (m.objects, m.ids.clone());
        let id = crate::storage::new_layer_id(0);
        let storage = repos.storage();
        let written = if streamed {
            storage.stream_layer(repo, &id, m.builder).await.unwrap()
        } else {
            storage.publish_built(repo, &id, m.builder).await.unwrap()
        };
        let meta = crate::storage::stored_meta(id, &written, objects);
        let layer = repos.storage().open_layer(repo, &meta).await.unwrap();
        let root = meta.root;
        assert_eq!(layer.id_span(), range.end - range.start);
        // Every object keeps the id of its oldest copy.
        layer
            .for_each_oid(|l, oid| {
                assert_eq!(layer.id_of(range.start, l), source_ids[oid], "{oid}");
            })
            .unwrap();
        (root, objects, range)
    }

    /// Give `layers` consecutive id ranges, as pushes get them.
    fn number(layers: &mut [LayerMeta]) {
        let mut next = 0;
        for l in layers {
            l.first_id = next;
            next += l.id_span;
        }
    }

    /// An object with copies in a stored and an inline layer looks up to
    /// the older copy, in either order: the inline index names an inline
    /// object's oldest copy, which may be in an older stored layer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lookups_find_the_oldest_copy() {
        let (_dir, repos) = repos();
        let shared: &[u8] = b"the same object, in two layers\n";
        let oid = try_hash_object(ObjectFormat::Sha1, Kind::Blob, shared).unwrap();
        for (repo, stored_first) in [("stored-first", true), ("inline-first", false)] {
            let a = layer_of(&repos, &[shared, format!("{repo} a\n").as_bytes()]).await;
            let b = layer_of(&repos, &[shared, format!("{repo} b\n").as_bytes()]).await;
            let (first, second) = stored_and_inline(&repos, repo, a, b, stored_first).await;
            let mut meta = RepoState::new();
            meta.version = 2;
            meta.layers = vec![first.clone(), second];
            number(&mut meta.layers);
            repos.on_apply(repo, &Arc::new(meta));
            let snap = repos.handle(repo).wait_version(2).await.unwrap();
            assert_eq!(snap.layers.len(), 2);
            let (lv, _) = snap.locate(snap.lookup(&oid).unwrap());
            assert_eq!(lv.layer.id, first.id, "{repo}: not the oldest copy");
            assert_eq!(snap.lookup_kind(&oid, Kind::Blob), snap.lookup(&oid));
            snap.check_lookups().unwrap();
        }
    }

    /// A flush replaces inline layers with a merged one, keeping their ids:
    /// the views of the layers after it are reused as they are, and still
    /// resolve to the oldest copy (now in the merged layer, which has a
    /// hole where a duplicate was dropped).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flushes_keep_later_views() {
        let (_dir, repos) = repos();
        let repo = "flush";
        let shared: &[u8] = b"in layers 0, 1 and 2\n";
        let oid = try_hash_object(ObjectFormat::Sha1, Kind::Blob, shared).unwrap();
        let mut layers = Vec::new();
        for blobs in [
            &[shared, b"a\n"][..],
            &[shared],
            &[shared, b"b\n"],
            &[b"c\n"],
        ] {
            layers.push(inline(layer_of(&repos, blobs).await).await);
        }
        number(&mut layers);
        let mut meta = RepoState::new();
        meta.version = 2;
        meta.layers = layers.clone();
        repos.on_apply(repo, &Arc::new(meta.clone()));
        let r = repos.handle(repo);
        let before = r.wait_version(2).await.unwrap();
        let d = before.lookup(&oid).unwrap();
        assert_eq!(before.locate(d).0.layer.id, layers[0].id);
        before.check_lookups().unwrap();

        let ws = repos.storage().workspace();
        let out = merge::merge_layers(&before, 0..2, ws, merge::WINDOW)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.ids, 0..3);
        let id = crate::storage::new_layer_id(0);
        let written = repos
            .storage()
            .publish_built(repo, &id, out.builder)
            .await
            .unwrap();
        let merged = LayerMeta {
            first_id: 0,
            id_span: 3,
            ..crate::storage::stored_meta(id, &written, out.objects)
        };
        meta.version = 3;
        meta.layers = [std::slice::from_ref(&merged), &layers[2..]].concat();
        repos.on_apply(repo, &Arc::new(meta));
        let after = r.wait_version(3).await.unwrap();
        assert_eq!(after.layers.len(), 3);
        for (a, b) in after.layers[1..].iter().zip(&before.layers[2..]) {
            assert!(Arc::ptr_eq(a, b), "the view of {} was rebuilt", a.layer.id);
        }
        let gone: Vec<&str> = before
            .layers
            .iter()
            .map(|l| l.layer.id.as_str())
            .filter(|id| after.layers.iter().all(|l| l.layer.id != *id))
            .collect();
        assert_eq!(
            gone,
            vec![layers[0].id.as_str(), layers[1].id.as_str()],
            "the flushed layers are the dropped ones"
        );
        assert_eq!(after.lookup(&oid), Some(d), "the oldest copy keeps its id");
        assert_eq!(after.locate(d).0.layer.id, merged.id);
        let (lv, _) = after.locate(d);
        assert_eq!(lv.local(2), None, "the dropped duplicate leaves a hole");
        after.check_lookups().unwrap();
    }

    /// A merge keeps the id range of its source layers and each object's
    /// oldest copy, in windows or not, through a local file or straight into
    /// its upload; a run of nothing but copies of older objects merges into
    /// nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn merges_keep_ids() {
        let (_dir, repos) = repos();
        let repo = "merge";
        let shared: &[u8] = b"in layers 0 and 1\n";
        let mut layers = Vec::new();
        for blobs in [&[shared, b"a\n"][..], &[shared], &[b"b\n", b"c\n"]] {
            layers.push(stored(&repos, repo, layer_of(&repos, blobs).await).await);
        }
        number(&mut layers);
        let mut meta = RepoState::new();
        meta.version = 2;
        meta.layers = layers.clone();
        repos.on_apply(repo, &Arc::new(meta));
        let snap = repos.handle(repo).wait_version(2).await.unwrap();
        let source_ids = source_ids(&snap, &layers);
        let ws = repos.storage().workspace();
        let out = merge::merge_layers(&snap, 0..3, ws.clone(), merge::WINDOW)
            .await
            .unwrap();
        let got = publish_merged(&repos, repo, out.unwrap(), false, &source_ids).await;
        assert_eq!((got.1, got.2.clone()), (4, 0..5));
        // One entry per window, streamed: the same file.
        let out = merge::merge_layers(&snap, 0..3, ws.clone(), 1)
            .await
            .unwrap();
        let streamed = publish_merged(&repos, repo, out.unwrap(), true, &source_ids).await;
        assert_eq!(streamed, got);
        let dup = merge::merge_layers(&snap, 1..2, ws, merge::WINDOW)
            .await
            .unwrap();
        assert!(dup.is_none(), "layer 1 holds only an older object");
    }

    /// Lookups find the oldest copy after a compaction replaced stored
    /// layers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compactions_keep_the_oldest_copy() {
        let (_dir, repos) = repos();
        let repo = "compact";
        let shared: &[u8] = b"in stored layers 1 and 3, and inline\n";
        let oid = try_hash_object(ObjectFormat::Sha1, Kind::Blob, shared).unwrap();
        let mut layers = Vec::new();
        for i in 0..5 {
            let own = format!("layer {i}\n");
            let blobs: Vec<&[u8]> = match i {
                1 | 3 => vec![shared, own.as_bytes()],
                _ => vec![own.as_bytes()],
            };
            layers.push(stored(&repos, repo, layer_of(&repos, &blobs).await).await);
        }
        layers.push(inline(layer_of(&repos, &[shared, b"inline\n"]).await).await);
        number(&mut layers);
        let mut meta = RepoState::new();
        meta.version = 2;
        meta.layers = layers.clone();
        repos.on_apply(repo, &Arc::new(meta.clone()));
        let r = repos.handle(repo);
        let snap = r.wait_version(2).await.unwrap();
        let (lv, _) = snap.locate(snap.lookup(&oid).unwrap());
        assert_eq!(lv.layer.id, layers[1].id, "not the oldest copy");
        snap.check_lookups().unwrap();

        // Compaction replaces stored layers 2 and 3 with one. (A stand-in
        // with a copy of the shared object and their range, ids in its
        // storage order.)
        let merged = layer_of(&repos, &[shared, b"layer 2\n", b"layer 3\n"]).await;
        let mut merged = stored(&repos, repo, merged).await;
        merged.first_id = layers[2].first_id;
        assert_eq!(merged.id_span, layers[2].id_span + layers[3].id_span);
        meta.version = 3;
        meta.layers = [&layers[..2], &[merged], &layers[4..]].concat();
        repos.on_apply(repo, &Arc::new(meta));
        let snap = r.wait_version(3).await.unwrap();
        let (lv, _) = snap.locate(snap.lookup(&oid).unwrap());
        assert_eq!(
            lv.layer.id, layers[1].id,
            "not the oldest copy after compaction"
        );
        snap.check_lookups().unwrap();
    }

    /// A pushed full object keeps the zlib data it was sent with, here at
    /// a level that ingest does not use itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingest_keeps_sent_zlib_data() {
        let (_dir, repos) = repos();
        let repo = "verbatim";
        let blob = b"stored as sent, not compressed again\n".repeat(8);
        let z = sirocco::compress(sirocco::Format::Zlib, sirocco::deflate::Level::NONE, &blob);
        let mut sent = Vec::new();
        encode_entry_header(BLOB_CODE, u64::try_from(blob.len()).unwrap(), &mut sent);
        sent.extend_from_slice(&z);
        assert_ne!(sent, full_entry(Kind::Blob, &blob));
        let layer = layer_of_entries(&repos, std::slice::from_ref(&sent)).await;
        let mut meta = RepoState::new();
        meta.version = 1;
        meta.layers = vec![inline(layer).await];
        number(&mut meta.layers);
        repos.on_apply(repo, &Arc::new(meta));
        let snap = repos.handle(repo).wait_version(1).await.unwrap();
        let d = snap
            .lookup(&try_hash_object(ObjectFormat::Sha1, Kind::Blob, &blob).unwrap())
            .unwrap();
        let loc = snap.entry_loc(d).unwrap();
        let stored = snap
            .with_entry_at(d, &loc, Access::Hot, <[u8]>::to_vec)
            .unwrap();
        assert_eq!(stored, sent);
        assert_eq!(*snap.read(d).unwrap(), blob);
    }
}
