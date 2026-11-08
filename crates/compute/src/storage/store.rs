//! Durable storage of layer files ([`FileKey`]): the object store, plus
//! each node's local copies ([`super::local`]), and the layers opened from
//! them.
//!
//! The **object store** is the long-term home of every stored layer.
//! Inline layers are in the log, and are opened from their bytes there.
//!
//! **Layer ids are names, not content addresses**: random, given when a
//! file is published, and never reused, so each object-store key is
//! written by one upload. A layer that left the manifest can then never
//! come back under its name: a deletion cannot remove a file something
//! references again. Each id ends in the upload generation it was
//! published in (see `RepoMeta.upload_gen`), which is what lets the layer
//! sweep (`maintenance/sweep.rs`) delete files nothing will name. The
//! file's root and sizes ([`Written`]) are in its manifest entry.
//!
//! Files are read through the buffer pool in tiers ([`super::tiered`]): a
//! file not on local disk is served by range from the object store while a
//! local copy is fetched in the background.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt as _;

use enroute_shared::state::LayerMeta;
use enroute_shared::store::Objects;

use super::files::{self, Scratch, ScratchBudget};
use super::key::FileKey;
use super::layer::{Layer, LayerBuilder, Written};
use super::local::LocalCopies;
use super::pool::{BufferPool, PagedFile};
use super::tiered::{RangeFetch, StoreFetch, TieredSource};

/// A fresh layer id of upload generation `generation`: 128 random bits,
/// then the generation.
#[must_use]
pub fn new_layer_id(generation: u64) -> String {
    enroute_shared::state::layer_id(rand::random(), generation)
}

/// Layers this big are written straight into their upload. Smaller ones go
/// through a local file, which the node keeps as its copy: they are mostly
/// flush outputs, the data read most.
const STREAM_BYTES: u64 = 256 << 20;

/// A manifest entry for stored layer `id` (file `w`, holding `objects`
/// objects), without object ids: the commit adding it assigns them, or the
/// caller sets those of the layers it replaces.
pub(crate) fn stored_meta(id: String, w: &Written, objects: u32) -> LayerMeta {
    LayerMeta {
        id,
        root: bytes::Bytes::copy_from_slice(&w.root),
        objects,
        bytes: w.bytes,
        meta_bytes: w.meta_bytes,
        ..Default::default()
    }
}

/// A finished layer file, not yet published: its facts and bytes.
#[derive(Debug)]
pub struct Sealed {
    written: Written,
    data: Scratch,
}

impl Sealed {
    pub(crate) fn new(written: Written, data: Scratch) -> Sealed {
        Sealed { written, data }
    }

    /// Layer file `data` (for example an inline layer uploaded as is).
    ///
    /// # Errors
    ///
    /// Returns an error if `data` is not a valid layer file.
    pub fn of(data: bytes::Bytes) -> Result<Sealed> {
        Ok(Sealed::new(Written::of(&data)?, Scratch::Mem(data)))
    }

    /// A manifest entry for this file, holding `objects` objects, under a
    /// new id of upload generation `generation`: stored (the caller adds
    /// `data` for an inline layer), without object ids (the commit adding
    /// it assigns them).
    pub fn meta(&self, objects: u32, generation: u64) -> LayerMeta {
        stored_meta(new_layer_id(generation), &self.written, objects)
    }

    /// The file's size in bytes.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.data.len()
    }

    /// Whether the file has no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The file's bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the scratch file cannot be read.
    pub async fn into_bytes(self) -> Result<bytes::Bytes> {
        self.data.into_bytes().await
    }

    /// Move the file to `path` (through `tmp`, see [`Scratch::persist`]).
    pub(super) async fn persist(
        self,
        tmp: &Path,
        path: &Path,
        budget: &ScratchBudget,
    ) -> Result<()> {
        self.data.persist(tmp, path, budget).await
    }
}

/// Layer storage: the object store, local copies and the open layers.
pub struct Storage {
    /// The object store (shared with the rest of the node).
    objects: Arc<Objects>,
    dir: PathBuf,
    /// Open stored layers, so concurrent readers share one (its pages and
    /// metadata).
    layers: Mutex<OpenLayers>,
    /// Stored layers being opened: one opener per layer, the others wait.
    opening: Mutex<HashMap<FileKey, Arc<tokio::sync::Mutex<()>>>>,
    /// Object-store request counters (uploads, bulk deletes).
    stats: StoreStats,
    /// Pages of open files.
    pub(super) pool: Arc<BufferPool>,
    /// The local copies of stored layers.
    pub(super) local: Arc<LocalCopies>,
    scratch: ScratchBudget,
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

/// Open layers by key (weakly: a layer stays open while it is used).
#[derive(Default)]
struct OpenLayers {
    map: HashMap<FileKey, Weak<Layer>>,
    /// Closed layers' entries are pruned when the map reaches this size.
    prune_at: usize,
}

impl OpenLayers {
    fn get(&self, key: &FileKey) -> Option<Arc<Layer>> {
        self.map.get(key).and_then(Weak::upgrade)
    }

    fn insert(&mut self, key: FileKey, layer: &Arc<Layer>) {
        if self.map.len() >= self.prune_at {
            self.map.retain(|_, l| l.strong_count() > 0);
            self.prune_at = (2 * self.map.len()).max(64);
        }
        self.map.insert(key, Arc::downgrade(layer));
    }
}

/// Local copies of layers no longer in use are removed after this delay
/// (in case they are opened again meanwhile).
const EVICT_DELAY: std::time::Duration = std::time::Duration::from_secs(120);

/// Lets the local copy of a stored layer go when the layer closes: after
/// [`EVICT_DELAY`], unless the layer is open again by then. Its source
/// stays open meanwhile, so that a reopen finds the chunks it loaded.
pub(super) struct Release {
    storage: Weak<Storage>,
    key: FileKey,
    source: Option<Arc<TieredSource>>,
}

impl Drop for Release {
    fn drop(&mut self) {
        // Outside a runtime the process is ending, and copies stay.
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (storage, key, source) = (self.storage.clone(), self.key.clone(), self.source.take());
        rt.spawn(async move {
            tokio::time::sleep(EVICT_DELAY).await;
            drop(source);
            // A storage that was closed meanwhile no longer owns the copy.
            if let Some(storage) = storage.upgrade() {
                storage.remove_local(&key).await;
            }
        });
    }
}

#[derive(Debug, Default)]
struct StoreStats {
    pub uploads: std::sync::atomic::AtomicU64,
    pub upload_bytes: std::sync::atomic::AtomicU64,
    pub deletes: std::sync::atomic::AtomicU64,
}

impl StoreStats {
    pub(crate) fn json(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering::Relaxed;
        serde_json::json!({
            "uploads": self.uploads.load(Relaxed),
            "upload_bytes": self.upload_bytes.load(Relaxed),
            "delete_requests": self.deletes.load(Relaxed),
        })
    }
}

impl Storage {
    /// `dir`: the local copies, emptied first: local disk never outlives
    /// the process that filled it. `disk_budget`: bound the
    /// local disk they use to this many bytes: chunks are loaded as they
    /// are read and evicted under pressure (see
    /// [`DiskCache`](super::tiered::DiskCache)). 0:
    /// unbounded, and files opened remotely are copied in full in the
    /// background. `max_open_files`: how many files the storage may keep
    /// open, layer files and scratch files together, split 3:1 (None: as
    /// many as the system limit allows, less a reserve).
    ///
    /// # Errors
    ///
    /// Returns an error if a directory under `dir` cannot be created or
    /// read, or if the system's open-file limit is too low.
    pub fn new(
        objects: Arc<Objects>,
        dir: PathBuf,
        page_cache_bytes: usize,
        disk_budget: u64,
        max_open_files: Option<usize>,
    ) -> Result<Self> {
        wipe(&dir)?;
        let limits = super::handles::resolve(max_open_files)?;
        let scratch = ScratchBudget::new(limits.scratch);
        let local = LocalCopies::new(
            dir.clone(),
            objects.clone(),
            disk_budget,
            limits.cache,
            scratch.clone(),
        )?;
        Ok(Storage {
            pool: BufferPool::new(page_cache_bytes),
            objects,
            dir,
            layers: Mutex::default(),
            opening: Mutex::default(),
            stats: StoreStats::default(),
            local: Arc::new(local),
            scratch,
        })
    }

    /// Storage in `dir` over a store that it reaches only through its API,
    /// so it never links a local copy to the store's file.
    #[cfg(test)]
    pub(crate) fn for_test(dir: &Path) -> Arc<Storage> {
        let store = enroute_shared::store::Store::local(&dir.join("store")).unwrap();
        let objects = enroute_shared::store::Store::new(store.inner().clone()).objects();
        Arc::new(Storage::new(objects, dir.join("local"), 64 << 20, 0, None).unwrap())
    }

    /// Scratch space for one operation (temp tables, layer builders).
    pub fn workspace(&self) -> super::spill::Workspace {
        super::spill::Workspace::new(&self.tmp_dir(), self.pool.clone(), self.scratch.clone())
    }

    pub(crate) fn tmp_dir(&self) -> PathBuf {
        self.dir.join("tmp")
    }

    /// Receive a request body (see [`files::spool`]).
    pub(crate) async fn spool<S, E>(
        &self,
        body: S,
        gzip: bool,
        mem_limit: usize,
        tap: tokio::sync::mpsc::Sender<bytes::Bytes>,
    ) -> Result<Scratch>
    where
        S: futures::Stream<Item = std::result::Result<bytes::Bytes, E>> + Unpin,
        E: Into<anyhow::Error>,
    {
        files::spool(
            &self.pool,
            &self.scratch,
            &self.tmp_dir(),
            body,
            gzip,
            mem_limit,
            tap,
        )
        .await
    }

    /// Counters of the object store, local copies and caches.
    #[must_use]
    pub fn status(&self) -> serde_json::Value {
        serde_json::json!({
            "object_store": self.stats.json(),
            "local_copies": self.local.json(),
            "page_cache": self.pool.json(),
            "disk_cache": self.local.disk_json(),
        })
    }

    // ---- layers ----

    /// Make a sealed layer of `repo` stored, as `id` (from its
    /// [`Sealed::meta`]): in the object store (and kept locally) when this
    /// returns.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload or the local copy fails.
    pub async fn publish_layer(&self, repo: &str, id: &str, sealed: Sealed) -> Result<()> {
        let key = FileKey::new(repo, id);
        self.upload(&key, &sealed.data).await?;
        self.local.put(&key, sealed).await
    }

    /// Publish the layer that `b` builds as layer `id` of `repo`: a large
    /// one written straight into its upload ([`stream_layer`]), a small one
    /// through a local file that this node keeps. Returns the file's root
    /// and size.
    ///
    /// [`stream_layer`]: Self::stream_layer
    pub(crate) async fn publish_built(
        &self,
        repo: &str,
        id: &str,
        b: LayerBuilder,
    ) -> Result<Written> {
        if b.approx_size() >= STREAM_BYTES {
            return self.stream_layer(repo, id, b).await;
        }
        let sealed = tokio::task::spawn_blocking(move || b.finish()).await??;
        let out = sealed.written;
        self.publish_layer(repo, id, sealed).await?;
        Ok(out)
    }

    /// Publish the layer that `b` builds as layer `id` of `repo`, writing it
    /// straight into the upload: no local file holds it whole, so a large
    /// merge needs no local disk for its output. Nothing keeps a local
    /// copy; readers fetch it like any other stored layer.
    pub(crate) async fn stream_layer(
        &self,
        repo: &str,
        id: &str,
        b: LayerBuilder,
    ) -> Result<Written> {
        let bridge = tokio_util::io::SyncIoBridge::new(self.multipart(&FileKey::new(repo, id)));
        let (written, bridge) = tokio::task::spawn_blocking(move || {
            // The layer writer makes small writes; each through the bridge
            // would block on the runtime.
            let mut w = std::io::BufWriter::with_capacity(1 << 20, bridge);
            let written = b.write_to(&mut w);
            let bridge = w.into_parts().0;
            (written, bridge)
        })
        .await?;
        let mut upload = bridge.into_inner();
        let written = match written {
            Ok(done) => done,
            Err(e) => {
                // Leave no unfinished multipart upload behind.
                if let Err(a) = upload.abort().await {
                    tracing::warn!(repo, id, "aborting an upload: {a}");
                }
                return Err(e);
            }
        };
        self.count_upload(written.bytes);
        upload.shutdown().await.context("upload")?;
        Ok(written)
    }

    /// Copy the stored layer `from` of `repo` to `to`, within the object
    /// store (republishing it under a new id without uploading it again).
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot copy the file.
    pub async fn copy_layer(&self, repo: &str, from: &str, to: &str) -> Result<()> {
        let (from, to) = (
            FileKey::new(repo, from).store_path(),
            FileKey::new(repo, to).store_path(),
        );
        self.objects
            .inner()
            .copy(&from, &to)
            .await
            .with_context(|| format!("copying {from} to {to}"))
    }

    /// The ids of `repo`'s layer files in the object store.
    pub(crate) async fn list_layers(&self, repo: &str) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        let mut list = self
            .objects
            .inner()
            .list(Some(&FileKey::store_prefix(repo)));
        while let Some(meta) = list.next().await {
            if let Some(id) = meta?
                .location
                .filename()
                .and_then(|f| f.strip_suffix(".layer"))
            {
                ids.push(id.to_string());
            }
        }
        Ok(ids)
    }

    /// Open stored layer `m`.
    pub(crate) async fn open_layer(
        self: &Arc<Self>,
        repo: &str,
        m: &LayerMeta,
    ) -> Result<Arc<Layer>> {
        let key = FileKey::new(repo, &m.id);
        if let Some(l) = self.layers.lock().get(&key) {
            return Ok(l);
        }
        let lock = self.opening.lock().entry(key.clone()).or_default().clone();
        let opened = {
            let _g = lock.lock().await;
            let open = self.layers.lock().get(&key);
            match open {
                Some(l) => Ok(l),
                None => self.load_layer(&key, m).await,
            }
        };
        let mut opening = self.opening.lock();
        // Ours and the map's: nobody else waits on it.
        if Arc::strong_count(&lock) == 2 {
            opening.remove(&key);
        }
        opened
    }

    async fn load_layer(self: &Arc<Self>, key: &FileKey, m: &LayerMeta) -> Result<Arc<Layer>> {
        let file = self.open_file(key, m.bytes).await?;
        let mut layer = Layer::open(key.id.clone(), file, m.meta_bytes).await?;
        layer.release = Some(Release {
            storage: Arc::downgrade(self),
            key: key.clone(),
            source: self.local.source(key),
        });
        let layer = Arc::new(layer);
        self.layers.lock().insert(key.clone(), &layer);
        Ok(layer)
    }

    /// Open an inline layer, whose file `data` is in the log, from those
    /// bytes (shared, not copied): no local copy, which a push per layer
    /// would write and read back only to serve what memory already holds.
    /// Not shared with other openers (the materializer opens each inline
    /// layer once, ahead), nor with the layer's stored copy once flushed.
    pub(crate) async fn open_inline_layer(
        &self,
        id: &str,
        root: &[u8],
        data: &bytes::Bytes,
    ) -> Result<Arc<Layer>> {
        // The chunks are checked as the layer opens.
        let written = Written::of(data)?;
        anyhow::ensure!(
            written.root.as_slice() == root,
            "inline layer {id} does not match its root"
        );
        let f = self.pool.open(Arc::new(super::pool::Memory(data.clone())));
        let layer = Layer::open(id.to_string(), f, written.meta_bytes)
            .await
            .with_context(|| format!("inline layer {id}"))?;
        Ok(Arc::new(layer))
    }

    fn is_open(&self, key: &FileKey) -> bool {
        self.layers.lock().get(key).is_some()
    }

    /// Remove a layer's local copy, unless it is open; whether it was
    /// removed.
    async fn remove_local(&self, key: &FileKey) -> bool {
        if self.is_open(key) {
            return false;
        }
        self.local.remove(key).await;
        true
    }

    /// Open a stored layer's file, `len` bytes (see [`LocalCopies::open`]).
    async fn open_file(&self, key: &FileKey, len: u64) -> Result<Arc<PagedFile>> {
        Ok(self
            .pool
            .open(self.local.open(key, self.remote(key), len).await?))
    }

    /// Ranged reads of a file in the object store.
    fn remote(&self, key: &FileKey) -> Arc<dyn RangeFetch> {
        Arc::new(StoreFetch {
            store: self.objects.inner().clone(),
            key: key.store_path(),
        })
    }

    // ---- object store ----

    /// Upload a layer file (its key is new: ids are never reused).
    async fn upload(&self, key: &FileKey, data: &Scratch) -> Result<()> {
        self.count_upload(data.len());
        let file = match data {
            Scratch::Mem(b) => {
                self.objects
                    .inner()
                    .put(&key.store_path(), PutPayload::from(b.clone()))
                    .await
                    .context("upload")?;
                return Ok(());
            }
            Scratch::File(f) => f.path()?,
        };
        let mut file = tokio::fs::File::open(&file).await?;
        let mut upload = self.multipart(key);
        tokio::io::copy(&mut file, &mut upload).await?;
        upload.shutdown().await.context("upload")?;
        Ok(())
    }

    /// A writer that uploads `key`: a plain put below its capacity, a
    /// multipart upload above it. In the caller's IO class, as
    /// [`Self::stream_layer`] drives it from a blocking thread.
    fn multipart(&self, key: &FileKey) -> BufWriter {
        BufWriter::with_capacity(self.objects.in_current_class(), key.store_path(), 16 << 20)
            .with_max_concurrency(4)
    }

    fn count_upload(&self, bytes: u64) {
        self.stats
            .uploads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.stats
            .upload_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Remove layers `ids` of `repo` everywhere, using bulk deletes (up to
    /// 1000 keys per request).
    pub(crate) async fn delete_layers(&self, repo: &str, ids: &[String]) -> Result<()> {
        let keys: Vec<FileKey> = ids.iter().map(|id| FileKey::new(repo, id)).collect();
        let keys = &keys[..];
        if keys.is_empty() {
            return Ok(());
        }
        self.stats.deletes.fetch_add(
            u64::try_from(keys.len().div_ceil(1000)).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        self.objects
            .delete_objects(keys.iter().map(FileKey::store_path))
            .await?;
        for k in keys {
            self.remove_local(k).await;
        }
        Ok(())
    }
}

/// Names under the local directory that are being deleted.
const WIPED: &str = "wiped-";

/// Empty the local directory `dir` of what an earlier process left: its
/// scratch and its local copies.
///
/// Local disk is a cache that lives as long as the process that fills it,
/// so every start is a fresh disk and nothing on it needs checking. The
/// old directories are moved aside at once and deleted in the background,
/// so a large cache does not hold up the start.
fn wipe(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    for name in ["tmp", "layers"] {
        let aside = dir.join(format!("{WIPED}{}", rand::random::<u64>()));
        match std::fs::rename(dir.join(name), &aside) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(e)
                    .with_context(|| format!("moving {} aside", dir.join(name).display()));
            }
            _ => {}
        }
    }
    std::fs::create_dir_all(dir.join("tmp"))?;
    let wiped: Vec<PathBuf> = std::fs::read_dir(dir)?
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with(WIPED))
        .map(|e| e.path())
        .collect();
    if !wiped.is_empty() {
        std::thread::Builder::new()
            .name("wipe-local".into())
            .spawn(move || delete_all(&wiped))?;
    }
    Ok(())
}

/// Delete the directories `paths`, logging what fails.
fn delete_all(paths: &[PathBuf]) {
    for path in paths {
        if let Err(e) = std::fs::remove_dir_all(path) {
            tracing::warn!(path = %path.display(), "deleting old local files: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::oid::{Kind, ObjectFormat, Oid, hash_object};
    use crate::git::pack::full_entry;
    use crate::storage::LayerBuilder;
    use crate::storage::layer::tests::blob;
    use enroute_shared::store::Store;

    /// An inline layer opens from its bytes, with no local copy (the log
    /// has them); a published layer has one.
    #[tokio::test]
    async fn inline_layers_have_no_local_copy() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::for_test(dir.path());
        let (sealed, oid) = one_object(&storage, b"inline\n");
        let meta = sealed.meta(1, 0);
        let data = sealed.into_bytes().await.unwrap();
        let l = storage
            .open_inline_layer(&meta.id, &meta.root, &data)
            .await
            .unwrap();
        assert_eq!(l.find(&oid), Some(0));
        assert!(
            storage.local.written.lock().is_empty(),
            "an inline layer was recorded"
        );

        let (sealed, _) = one_object(&storage, b"stored\n");
        let meta = sealed.meta(1, 0);
        let id = meta.id.clone();
        storage.publish_layer("r", &id, sealed).await.unwrap();
        assert_eq!(
            storage
                .local
                .written
                .lock()
                .iter()
                .map(|k| k.id.as_str())
                .collect::<Vec<_>>(),
            vec![id.as_str()]
        );
    }

    /// A layer closed and opened again within the release delay reuses
    /// its open local copy, with the chunks it already has.
    #[tokio::test]
    async fn a_reopened_layer_reuses_its_local_copy() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::for_test(dir.path());
        let (sealed, oid) = one_object(&storage, b"reopened\n");
        let meta = sealed.meta(1, 0);
        storage.publish_layer("r", &meta.id, sealed).await.unwrap();
        let key = FileKey::new("r", &meta.id);
        let layer = storage.open_layer("r", &meta).await.unwrap();
        let source = storage.local.source(&key).unwrap();
        drop(layer);
        let layer = storage.open_layer("r", &meta).await.unwrap();
        assert_eq!(layer.find(&oid), Some(0));
        assert!(Arc::ptr_eq(&storage.local.source(&key).unwrap(), &source));
    }

    /// Opening the storage empties its local directory: what an earlier
    /// process left there is gone once the background deletion ends.
    #[test]
    fn opening_wipes_the_local_directory() {
        let dir = tempfile::tempdir().unwrap();
        let old = dir.path().join("layers/r/a.layer");
        std::fs::create_dir_all(old.parent().unwrap()).unwrap();
        std::fs::write(&old, b"old").unwrap();
        std::fs::create_dir_all(dir.path().join("tmp")).unwrap();
        std::fs::write(dir.path().join("tmp/junk"), b"x").unwrap();
        wipe(dir.path()).unwrap();
        assert!(!old.exists());
        assert!(dir.path().join("tmp").is_dir());
        let left = || {
            std::fs::read_dir(dir.path())
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        };
        for _ in 0..100 {
            if left() == ["tmp"] {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(left(), ["tmp"]);
        assert_eq!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
            0
        );
    }

    /// Layer ids are fresh names, not content addresses: the same bytes
    /// published twice are two files. An inline layer's bytes are checked
    /// against the root in its manifest entry, in a chunk and in the tail.
    #[tokio::test]
    async fn layer_ids_are_never_reused() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::for_test(dir.path());
        let (sealed, _) = one_object(&storage, b"x");
        let meta = sealed.meta(1, 0);
        let data = sealed.into_bytes().await.unwrap();

        let (a, b) = (
            Sealed::of(data.clone()).unwrap().meta(0, 7),
            Sealed::of(data.clone()).unwrap().meta(0, 7),
        );
        assert_ne!(a.id, b.id);
        assert_eq!(
            (
                enroute_shared::state::layer_gen(&a.id),
                enroute_shared::state::layer_gen(&b.id)
            ),
            (Some(7), Some(7))
        );
        assert_eq!((&a.root, &b.root), (&meta.root, &meta.root));
        storage
            .open_inline_layer(&meta.id, &meta.root, &data)
            .await
            .unwrap();

        for at in [0, data.len() - 1] {
            let mut bytes = data.to_vec();
            bytes[at] ^= 1;
            let err = storage
                .open_inline_layer(&meta.id, &meta.root, &bytes.into())
                .await
                .map(|_| ())
                .expect_err("damaged inline layer opened");
            assert!(format!("{err:#}").contains("does not match its"), "{err:#}");
        }
    }

    /// Storage on a `file://` store in `dir`, with this link function.
    fn local_storage(
        dir: &Path,
        disk_budget: u64,
        link: fn(&Path, &Path) -> std::io::Result<()>,
    ) -> Arc<Storage> {
        let root = dir.join("store");
        let objects = Store::local(&root).unwrap().objects();
        let mut storage =
            Storage::new(objects, dir.join("local"), 64 << 20, disk_budget, None).unwrap();
        Arc::get_mut(&mut storage.local).unwrap().link = link;
        Arc::new(storage)
    }

    /// A one-object layer of `data`, and the object's id.
    fn one_object(storage: &Storage, data: &[u8]) -> (Sealed, Oid) {
        let mut b = LayerBuilder::new(&storage.workspace(), ObjectFormat::Sha1);
        let oid = hash_object(Kind::Blob, data);
        b.add(blob(oid, &full_entry(Kind::Blob, data), None))
            .unwrap();
        (b.finish().unwrap(), oid)
    }

    /// The local copy of `key` and its file in the store under `dir`.
    fn both(storage: &Storage, dir: &Path, key: &FileKey) -> (PathBuf, PathBuf) {
        let stored = dir.join("store").join(key.store_path().as_ref());
        (key.local_path(&storage.dir), stored)
    }

    fn inode(path: &Path) -> u64 {
        std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(path).unwrap())
    }

    fn links(storage: &Storage) -> u64 {
        storage.status()["local_copies"]["links"].as_u64().unwrap()
    }

    /// On a local store, a local copy is a hard link to the stored file,
    /// made on publish and on open, and kept out of the disk budget.
    ///
    /// Releasing and deleting remove the names they removed
    /// before: the stored file stays until the layer is deleted.
    #[tokio::test]
    async fn local_copies_link_to_a_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let storage = local_storage(dir.path(), 1 << 20, |a, b| std::fs::hard_link(a, b));
        let (sealed, oid) = one_object(&storage, b"linked\n");
        let meta = sealed.meta(1, 0);
        let id = meta.id.clone();
        storage.publish_layer("r", &id, sealed).await.unwrap();
        let key = FileKey::new("r", &id);
        let (local, stored) = both(&storage, dir.path(), &key);
        assert_eq!(inode(&local), inode(&stored));

        let layer = storage.open_layer("r", &meta).await.unwrap();
        assert_eq!(layer.find(&oid), Some(0));
        assert_eq!(inode(&local), inode(&stored));
        assert_eq!(storage.status()["disk_cache"]["used"], 0);
        assert_eq!(links(&storage), 2);
        drop(layer);

        assert!(storage.remove_local(&key).await);
        assert!(!local.exists() && stored.exists());
        drop(storage.open_layer("r", &meta).await.unwrap());
        assert_eq!(inode(&local), inode(&stored));

        drop(storage.open_layer("r", &meta).await.unwrap());
        storage
            .delete_layers("r", std::slice::from_ref(&id))
            .await
            .unwrap();
        assert!(!local.exists() && !stored.exists());
    }

    /// A link that fails, as across filesystems, falls back to a copy.
    #[tokio::test]
    async fn a_failed_link_falls_back_to_a_copy() {
        let dir = tempfile::tempdir().unwrap();
        let storage = local_storage(dir.path(), 0, |_, _| {
            Err(std::io::ErrorKind::CrossesDevices.into())
        });
        let (sealed, oid) = one_object(&storage, b"copied\n");
        let meta = sealed.meta(1, 0);
        let id = meta.id.clone();
        storage.publish_layer("r", &id, sealed).await.unwrap();
        let (local, stored) = both(&storage, dir.path(), &FileKey::new("r", &id));
        assert_ne!(inode(&local), inode(&stored));
        assert_eq!(
            std::fs::read(&local).unwrap(),
            std::fs::read(&stored).unwrap()
        );
        assert!(
            !storage
                .local
                .linking
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        let layer = storage.open_layer("r", &meta).await.unwrap();
        assert_eq!(layer.find(&oid), Some(0));
        assert_eq!(links(&storage), 0);
    }
}
