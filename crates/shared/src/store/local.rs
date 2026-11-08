//! The local filesystem as an object store (`file://`), with conditional
//! updates.
//!
//! `object_store`'s `LocalFileSystem` supports conditional creates but not
//! conditional updates (`PutMode::Update`, compare-and-swap on the `ETag`).
//! This adds them under a lock file, held with `flock` across the check
//! and the write, so that processes sharing the store (on one machine, the
//! only ones that can) see a compare-and-swap. Everything else is passed
//! through.

use std::fmt;
use std::ops::Range;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result,
};

#[derive(Debug)]
pub(super) struct LocalStore {
    inner: LocalFileSystem,
    /// Held across every conditional update.
    lock: PathBuf,
    /// Taken before `lock`: waiters for the `flock` would otherwise fill the
    /// blocking pool, and its holder could not finish its write.
    local: tokio::sync::Mutex<()>,
}

impl LocalStore {
    pub(super) fn new(root: &std::path::Path) -> anyhow::Result<LocalStore> {
        Ok(LocalStore {
            // The ledger truncates its log once a layer is in the store, so
            // a write must survive a power loss before it returns.
            inner: LocalFileSystem::new_with_prefix(root)?.with_fsync(true),
            lock: root.join(".cas.lock"),
            local: tokio::sync::Mutex::new(()),
        })
    }

    /// Take the store's update lock (blocking until it is free).
    async fn locked(&self) -> Result<(tokio::sync::MutexGuard<'_, ()>, std::fs::File)> {
        let local = self.local.lock().await;
        let path = self.lock.clone();
        let generic = |e: std::io::Error| Error::Generic {
            store: "LocalStore",
            source: Box::new(e),
        };
        let file = tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)?;
            f.lock()?;
            Ok(f)
        })
        .await
        .map_err(|e| Error::Generic {
            store: "LocalStore",
            source: Box::new(e),
        })?
        .map_err(generic)?;
        Ok((local, file))
    }
}

/// Where a `file://` store keeps each object on disk.
#[derive(Debug)]
pub struct StoreFiles(LocalFileSystem);

impl StoreFiles {
    /// The files of the store rooted at `root`.
    ///
    /// # Errors
    ///
    /// Returns an error if `root` is not an absolute path.
    pub fn new(root: &std::path::Path) -> anyhow::Result<StoreFiles> {
        Ok(StoreFiles(LocalFileSystem::new_with_prefix(root)?))
    }

    /// The file that holds `location`, whether or not it exists.
    #[must_use]
    pub fn path(&self, location: &Path) -> Option<PathBuf> {
        self.0.path_to_filesystem(location).ok()
    }
}

impl fmt::Display for LocalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocalStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutMode::Update(expected) = &opts.mode else {
            return self.inner.put_opts(location, payload, opts).await;
        };
        let _lock = self.locked().await?;
        let current = match self.inner.head(location).await {
            Ok(meta) => meta.e_tag,
            Err(Error::NotFound { .. }) => None,
            Err(e) => return Err(e),
        };
        if current.is_none() || current != expected.e_tag {
            return Err(Error::Precondition {
                path: location.to_string(),
                source: format!("expected version {:?}, found {current:?}", expected.e_tag).into(),
            });
        }
        let opts = PutOptions {
            mode: PutMode::Overwrite,
            ..opts
        };
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use object_store::UpdateVersion;

    use super::*;

    #[tokio::test]
    async fn conditional_updates() {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalStore::new(dir.path()).unwrap();
        let p = Path::from("a/state");
        let create = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let v1 = s.put_opts(&p, "1".into(), create.clone()).await.unwrap();
        assert!(matches!(
            s.put_opts(&p, "x".into(), create).await,
            Err(Error::AlreadyExists { .. })
        ));
        let update = |r: &PutResult| PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: r.e_tag.clone(),
                version: r.version.clone(),
            }),
            ..Default::default()
        };
        let v2 = s.put_opts(&p, "2".into(), update(&v1)).await.unwrap();
        // A stale version loses.
        assert!(matches!(
            s.put_opts(&p, "3".into(), update(&v1)).await,
            Err(Error::Precondition { .. })
        ));
        s.put_opts(&p, "3".into(), update(&v2)).await.unwrap();
        assert_eq!(s.get(&p).await.unwrap().bytes().await.unwrap(), "3");
        // Updating a missing object fails too.
        assert!(matches!(
            s.put_opts(&Path::from("b"), "1".into(), update(&v2)).await,
            Err(Error::Precondition { .. })
        ));
    }

    async fn create_then_update(s: std::sync::Arc<LocalStore>, i: usize) {
        let p = Path::from(format!("r{i}"));
        let v = s.put(&p, "1".into()).await.unwrap();
        let opts = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: v.e_tag,
                version: v.version,
            }),
            ..Default::default()
        };
        s.put_opts(&p, "2".into(), opts).await.unwrap();
    }

    /// More concurrent updates than blocking threads still finish.
    #[test]
    fn updates_beyond_the_blocking_pool_finish() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .max_blocking_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let s = std::sync::Arc::new(LocalStore::new(dir.path()).unwrap());
        rt.block_on(async {
            let updates = (0..32).map(|i| tokio::spawn(create_then_update(s.clone(), i)));
            let all = futures::future::try_join_all(updates);
            tokio::time::timeout(std::time::Duration::from_secs(30), all)
                .await
                .unwrap()
                .unwrap();
        });
    }
}
