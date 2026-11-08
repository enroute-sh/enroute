//! Each node's local copies of stored layer files
//! (`<data>/local/layers/...`). Local disk is a cache that lives as long as
//! the process: the storage empties it at startup, so every copy on disk
//! was written by this process, and which of its chunks are present is
//! known in memory. A copy is written without fsync and is evictable. A
//! partial copy stays open (in memory) for as long as its file is kept.
//!
//! When the object store is a `file://` directory on the same filesystem,
//! a local copy is a **hard link** to the stored file instead, so each
//! layer is on disk once. Stored files are written once and never changed,
//! so the link is read-only and outside the disk budget (see
//! [`LocalCopy::linked`]). Elsewhere, or if the link fails, it is a copy.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Weak};

use anyhow::Result;
use parking_lot::Mutex;

use enroute_shared::store::{IoClass, Objects};

use super::files::ScratchBudget;
use super::handles::Handles;
use super::key::FileKey;
use super::pool::{Access, PageSource};
use super::store::Sealed;
use super::tiered::{DiskCache, LocalCopy, RangeFetch, TieredSource};

pub(super) struct LocalCopies {
    dir: PathBuf,
    /// The object store, for the paths of its files when it is local.
    objects: Arc<Objects>,
    /// The open local copies: a partial copy is usable only through its
    /// source, which knows its chunks.
    pub(super) live: Mutex<HashMap<FileKey, Weak<TieredSource>>>,
    /// The disk budget for local copies, if any.
    disk: Option<Arc<DiskCache>>,
    /// Open descriptors of the local copies, bounded node-wide.
    handles: Arc<Handles>,
    /// Bounds the temporary files of new copies.
    scratch: ScratchBudget,
    /// Complete copies written by this process (published layers): they
    /// open with every chunk present and checked.
    pub(super) written: Mutex<HashSet<FileKey>>,
    /// Local directories known to exist (created at most once per process,
    /// not on every push).
    dirs: Mutex<HashSet<PathBuf>>,
    /// How a local copy links to a stored file (replaced in tests).
    pub(super) link: fn(&Path, &Path) -> std::io::Result<()>,
    /// Cleared when the store's files are on another filesystem.
    pub(super) linking: AtomicBool,
    stats: LocalStats,
}

#[derive(Default)]
struct LocalStats {
    /// Files opened before a local copy existed (served by range).
    remote_opens: AtomicU64,
    /// Local copies completed in the background (no disk budget).
    hydrations: AtomicU64,
    /// Local copies made as hard links to the store's files.
    links: AtomicU64,
}

impl LocalCopies {
    /// The local copies under `dir`.
    /// `disk_budget`: see [`super::Storage::new`]. `max_open`: layer files
    /// kept open.
    pub(super) fn new(
        dir: PathBuf,
        objects: Arc<Objects>,
        disk_budget: u64,
        max_open: usize,
        scratch: ScratchBudget,
    ) -> Result<Self> {
        let handles = Handles::new(max_open, dir.join("tmp").join("handles"))?;
        Ok(LocalCopies {
            dir,
            objects,
            live: Mutex::default(),
            disk: (disk_budget > 0).then(|| DiskCache::new(disk_budget)),
            handles,
            scratch,
            written: Mutex::default(),
            dirs: Mutex::default(),
            link: |from, to| std::fs::hard_link(from, to),
            linking: AtomicBool::new(true),
            stats: LocalStats::default(),
        })
    }

    pub(super) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "remote_opens": self.stats.remote_opens.load(Relaxed),
            "hydrations": self.stats.hydrations.load(Relaxed),
            "links": self.stats.links.load(Relaxed),
        })
    }

    pub(super) fn disk_json(&self) -> Option<serde_json::Value> {
        self.disk.as_ref().map(|d| d.json())
    }

    fn tmp_dir(&self) -> PathBuf {
        self.dir.join("tmp")
    }

    /// The open source of `key`'s local copy, if any.
    pub(super) fn source(&self, key: &FileKey) -> Option<Arc<TieredSource>> {
        self.live.lock().get(key).and_then(Weak::upgrade)
    }

    /// Put a complete local copy of a file in place.
    pub(super) async fn put(&self, key: &FileKey, sealed: Sealed) -> Result<()> {
        if self.link(key).await.is_none() {
            let local = key.local_path(&self.dir);
            #[expect(clippy::unwrap_used, reason = "local paths are under `dir`")]
            let parent = local.parent().unwrap();
            self.ensure_dir(parent).await?;
            // New files are written under a temporary name and moved into
            // place, so a concurrent reader never opens a partial file.
            sealed
                .persist(&self.tmp_dir(), &local, &self.scratch)
                .await?;
        }
        self.written.lock().insert(key.clone());
        Ok(())
    }

    /// Open the local copy of `key`, a file of `len` bytes: the open one,
    /// a complete one this process wrote, else a new one that loads fill in
    /// from `remote` (served at once, by range).
    /// Without a disk budget, the rest of an incomplete copy is loaded in
    /// the background.
    pub(super) async fn open(
        self: &Arc<Self>,
        key: &FileKey,
        remote: Arc<dyn RangeFetch>,
        len: u64,
    ) -> Result<Arc<TieredSource>> {
        if let Some(source) = self.source(key) {
            return Ok(source);
        }
        let path = key.local_path(&self.dir);
        #[expect(clippy::unwrap_used, reason = "local paths are under `dir`")]
        let parent = path.parent().unwrap();
        self.ensure_dir(parent).await?;
        let copy = self.find(key, &path, len).await;
        let source = TieredSource::open(&self.handles, remote, path, self.disk.clone(), copy)?;
        // The storage opens each file once at a time.
        self.live
            .lock()
            .insert(key.clone(), Arc::downgrade(&source));
        if self.disk.is_none() && !source.complete() {
            let (this, key, source) = (self.clone(), key.clone(), source.clone());
            let hydrate = async move { this.hydrate(&key, &source).await };
            tokio::spawn(IoClass::Background.scope(hydrate));
        }
        Ok(source)
    }

    /// Load the rest of `source`'s local copy.
    async fn hydrate(&self, key: &FileKey, source: &TieredSource) {
        match source.load(0..source.len(), Access::Scan).await {
            Ok(()) => {
                self.stats.hydrations.fetch_add(1, Relaxed);
            }
            Err(e) => tracing::warn!(
                repo = key.repo,
                id = key.id,
                "completing the local copy (still served remotely): {e:#}"
            ),
        }
    }

    /// What the local copy of `key` at `path` is when it opens.
    async fn find(&self, key: &FileKey, path: &Path, len: u64) -> LocalCopy {
        // A link made now, or one left from before that a new link could
        // not replace: either shares the stored file.
        let linked = match self.link(key).await {
            Some(len) => Some(len),
            None => self
                .objects
                .file(&key.store_path())
                .and_then(|stored| shares_inode(path, &stored)),
        };
        if let Some(len) = linked {
            return LocalCopy {
                len,
                complete: true,
                linked: true,
                trusted: false,
            };
        }
        if self.written.lock().contains(key) {
            return LocalCopy {
                len,
                complete: true,
                linked: false,
                trusted: true,
            };
        }
        self.stats.remote_opens.fetch_add(1, Relaxed);
        LocalCopy {
            len,
            ..Default::default()
        }
    }

    /// Make the local copy of `key` a hard link to its file in a `file://`
    /// store, and return its length. None: the store is elsewhere, or the
    /// link failed (the caller copies instead).
    async fn link(&self, key: &FileKey) -> Option<u64> {
        if !self.linking.load(Relaxed) {
            return None;
        }
        let stored = self.objects.file(&key.store_path())?;
        let local = key.local_path(&self.dir);
        self.ensure_dir(local.parent()?).await.ok()?;
        // Linked beside, then renamed over any older copy: a reader never
        // sees the path missing.
        let tmp = self
            .tmp_dir()
            .join(format!("{}.link", rand::random::<u64>()));
        let link = self.link;
        let linked = tokio::task::spawn_blocking(move || {
            let made = link(&stored, &tmp)
                .and_then(|()| std::fs::rename(&tmp, &local))
                .and_then(|()| std::fs::metadata(&local));
            // A rename onto another link of the same file does nothing.
            drop(std::fs::remove_file(&tmp));
            made
        })
        .await
        .ok()?;
        match linked {
            Ok(meta) => {
                self.stats.links.fetch_add(1, Relaxed);
                Some(meta.len())
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::CrossesDevices {
                    tracing::info!("the object store is on another filesystem; copying layers");
                    self.linking.store(false, Relaxed);
                } else {
                    tracing::debug!(repo = key.repo, id = key.id, "linking the local copy: {e}");
                }
                None
            }
        }
    }

    /// Remove the local copy of `key`, if any.
    pub(super) async fn remove(&self, key: &FileKey) {
        self.written.lock().remove(key);
        self.live.lock().remove(key);
        let path = key.local_path(&self.dir);
        // Already missing is fine: the copy is meant to be gone.
        drop(tokio::fs::remove_file(path).await);
    }

    async fn ensure_dir(&self, dir: &Path) -> Result<()> {
        if !self.dirs.lock().contains(dir) {
            tokio::fs::create_dir_all(dir).await?;
            self.dirs.lock().insert(dir.to_path_buf());
        }
        Ok(())
    }
}

/// The length of `local`, if it is the same file as `stored`.
fn shares_inode(local: &Path, stored: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let (a, b) = (
        std::fs::metadata(local).ok()?,
        std::fs::metadata(stored).ok()?,
    );
    (a.dev() == b.dev() && a.ino() == b.ino()).then_some(a.len())
}
