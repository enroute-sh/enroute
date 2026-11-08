//! Bounds the descriptors of local layer files process-wide: only
//! `max_open` stay open, and a closed one reopens on demand. Each source
//! reopens through a private hard link, so unlinking or replacing its
//! public path does not matter.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Result, ensure};
use quick_cache::sync::Cache;

const MIN_OPEN: u64 = 64;
/// Cap for very high (or no) limits: a bigger cache gains nothing.
const MAX_OPEN: u64 = 1 << 20;
/// Descriptors kept back for sockets and the state store: at
/// least this many, or a quarter of the limit.
const MIN_RESERVE: u64 = 256;

const MIN_SCRATCH: u64 = 16;

/// The split of the open-file limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Limits {
    /// Layer files kept open.
    pub cache: usize,
    /// Scratch files open at once.
    pub scratch: usize,
}

fn reserve(soft: u64) -> u64 {
    MIN_RESERVE.max(soft / 4)
}

/// Split the open-file limit between layer and scratch files.
///
/// # Errors
///
/// Returns an error if not even [`MIN_OPEN`] files fit under the system
/// limit.
pub(super) fn plan(configured: Option<usize>, soft: u64) -> Result<Limits> {
    let reserve = reserve(soft);
    let ceiling = soft.saturating_sub(reserve).min(MAX_OPEN);
    ensure!(
        ceiling >= MIN_OPEN,
        "the open-file limit ({soft}) leaves too few descriptors for layer files: \
         {ceiling} after a reserve of {reserve}, but at least {MIN_OPEN} are needed"
    );
    let want = configured.map_or(ceiling, |c| u64::try_from(c).unwrap_or(u64::MAX));
    // The ceiling is at most `MAX_OPEN`.
    let total = want.clamp(MIN_OPEN, ceiling);
    let scratch = MIN_SCRATCH.max(total / 4);
    let to_usize = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
    Ok(Limits {
        cache: to_usize(total - scratch),
        scratch: to_usize(scratch),
    })
}

/// The limit for this process, logged once.
///
/// # Errors
///
/// See [`plan`].
pub(super) fn resolve(configured: Option<usize>) -> Result<Limits> {
    let soft = crate::sys::open_file_limit();
    let limits = plan(configured, soft)?;
    let effective = limits.cache.saturating_add(limits.scratch);
    if configured.is_some_and(|c| c > effective) {
        tracing::warn!(
            configured,
            effective,
            "node.max_open_files is above what the open-file limit allows; using the ceiling"
        );
    }
    tracing::info!(
        effective,
        layer_files = limits.cache,
        scratch_files = limits.scratch,
        ?configured,
        soft_rlimit = soft,
        reserve = reserve(soft),
        "open files the storage may keep"
    );
    Ok(limits)
}

pub(super) struct Handles {
    /// Private hard links, one per source.
    dir: PathBuf,
    open: Cache<u64, Arc<File>>,
    next_id: AtomicU64,
}

impl Handles {
    /// # Errors
    ///
    /// Returns an error if `dir` cannot be created.
    pub(super) fn new(max_open: usize, dir: PathBuf) -> std::io::Result<Arc<Handles>> {
        std::fs::create_dir_all(&dir)?;
        Ok(Arc::new(Handles {
            dir,
            open: Cache::new(max_open),
            next_id: AtomicU64::new(0),
        }))
    }

    pub(super) fn register(
        self: &Arc<Self>,
        path: &Path,
        linked: bool,
        file: File,
    ) -> std::io::Result<Handle> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        std::fs::hard_link(path, self.link_path(id))?;
        self.open.insert(id, Arc::new(file));
        Ok(Handle {
            handles: self.clone(),
            id,
            linked,
        })
    }

    fn link_path(&self, id: u64) -> PathBuf {
        self.dir.join(id.to_string())
    }
}

/// One source's handle. Dropping it forgets the source.
pub(super) struct Handle {
    handles: Arc<Handles>,
    id: u64,
    /// Read-only (a hard link to a stored file).
    linked: bool,
}

impl Handle {
    pub(super) fn get(&self) -> std::io::Result<Arc<File>> {
        let h = &self.handles;
        h.open.get_or_insert_with(&self.id, || {
            std::fs::OpenOptions::new()
                .read(true)
                .write(!self.linked)
                .open(h.link_path(self.id))
                .map(Arc::new)
        })
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.handles.open.remove(&self.id);
        drop(std::fs::remove_file(self.handles.link_path(self.id)));
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::FileExt;

    use super::*;

    fn handles(dir: &Path, max_open: usize) -> Arc<Handles> {
        Handles::new(max_open, dir.join("private")).unwrap()
    }

    fn make(dir: &Path, name: &str, linked: bool, h: &Arc<Handles>) -> Handle {
        let path = dir.join(name);
        std::fs::write(&path, name.as_bytes()).unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(!linked)
            .open(&path)
            .unwrap();
        h.register(&path, linked, f).unwrap()
    }

    fn read(h: &Handle) -> Vec<u8> {
        let f = h.get().unwrap();
        let mut b = vec![0u8; usize::try_from(f.metadata().unwrap().len()).unwrap()];
        f.read_exact_at(&mut b, 0).unwrap();
        b
    }

    #[test]
    fn an_evicted_source_reopens_and_reads_the_same_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let h = handles(dir.path(), 1);
        let a = make(dir.path(), "a", false, &h);
        let b = make(dir.path(), "b", false, &h);
        assert_eq!(h.open.len(), 1);
        assert_eq!(read(&a), b"a");
        assert_eq!(read(&b), b"b");
        assert_eq!(read(&a), b"a");
        assert_eq!(h.open.len(), 1);
    }

    #[test]
    fn a_handle_in_use_survives_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let h = handles(dir.path(), 1);
        let a = make(dir.path(), "a", false, &h);
        let held = a.get().unwrap();
        let _b = make(dir.path(), "b", false, &h);
        assert_eq!(h.open.len(), 1);
        let mut buf = [0u8; 1];
        held.read_exact_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"a");
    }

    #[test]
    fn reopened_handles_keep_their_mode() {
        let dir = tempfile::tempdir().unwrap();
        let h = handles(dir.path(), 1);
        let rw = make(dir.path(), "rw", false, &h);
        let ro = make(dir.path(), "ro", true, &h);
        // `rw` was evicted by `ro`; reopen it, then evict it again.
        rw.get().unwrap().write_all_at(b"x", 0).unwrap();
        ro.get().unwrap().write_all_at(b"x", 0).unwrap_err();
        rw.get().unwrap().write_all_at(b"x", 0).unwrap();
    }

    #[test]
    fn an_unlinked_public_path_stays_readable_after_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let h = handles(dir.path(), 1);
        let a = make(dir.path(), "a", false, &h);
        let _b = make(dir.path(), "b", false, &h);
        std::fs::remove_file(dir.path().join("a")).unwrap();
        assert_eq!(read(&a), b"a");
    }

    #[test]
    fn a_replaced_public_path_keeps_the_original_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let h = handles(dir.path(), 1);
        let a = make(dir.path(), "a", false, &h);
        let _b = make(dir.path(), "b", false, &h);
        std::fs::write(dir.path().join("other"), b"other").unwrap();
        std::fs::rename(dir.path().join("other"), dir.path().join("a")).unwrap();
        assert_eq!(read(&a), b"a");
    }

    #[test]
    fn dropping_a_handle_removes_its_private_link() {
        let dir = tempfile::tempdir().unwrap();
        let h = handles(dir.path(), 4);
        let a = make(dir.path(), "a", false, &h);
        let link = h.link_path(a.id);
        assert!(link.exists());
        drop(a);
        assert!(!link.exists());
        assert_eq!(h.open.len(), 0);
    }

    fn lim(cache: usize, scratch: usize) -> Limits {
        Limits { cache, scratch }
    }

    #[test]
    fn the_limit_keeps_a_reserve_below_the_system_limit() {
        // 768 in all: a quarter to scratch files.
        assert_eq!(plan(None, 1024).unwrap(), lim(576, 192));
        assert_eq!(plan(None, 65536).unwrap(), lim(36864, 12288));
        assert_eq!(plan(Some(100), 1024).unwrap(), lim(75, 25));
    }

    #[test]
    fn a_configured_limit_above_the_ceiling_is_lowered() {
        assert_eq!(plan(Some(5000), 1024).unwrap(), lim(576, 192));
        assert_eq!(plan(Some(1), 1024).unwrap(), lim(48, 16));
    }

    #[test]
    fn scratch_files_get_at_least_the_floor() {
        // 64 / 4 = 16, and 100 / 4 = 25.
        assert_eq!(plan(Some(64), 1024).unwrap(), lim(48, 16));
        assert_eq!(plan(Some(67), 1024).unwrap(), lim(51, 16));
        assert_eq!(plan(Some(80), 1024).unwrap(), lim(60, 20));
    }

    #[test]
    fn a_tiny_system_limit_is_an_error() {
        plan(None, 256).unwrap_err();
        plan(Some(64), 300).unwrap_err();
        assert_eq!(plan(None, 320).unwrap(), lim(48, 16));
    }

    #[test]
    fn an_unbounded_system_limit_is_capped() {
        assert_eq!(plan(None, u64::MAX).unwrap(), lim(786_432, 262_144));
        assert_eq!(plan(None, 1 << 40).unwrap(), lim(786_432, 262_144));
        assert_eq!(plan(Some(10), u64::MAX).unwrap(), lim(48, 16));
    }
}
