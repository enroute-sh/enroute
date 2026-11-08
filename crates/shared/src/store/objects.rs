//! The object store, as the node uses it: handles whose requests share the
//! node's slot pool ([`IoLimiter`]), small metadata objects (repository
//! state, the cluster's membership) with conditional writes, and access to the
//! store for the bulk transfers of layer files.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as StorePath};

use super::io::{ClassStore, IoClass, IoLimiter};
use super::local::StoreFiles;

/// The version of an object, for compare-and-swap ([`Objects::put_if`]).
pub type ObjectVersion = object_store::UpdateVersion;

/// Where the objects live: a store, and its directory if it is on this
/// machine.
#[derive(Clone, Debug)]
pub struct Store {
    store: Arc<dyn ObjectStore>,
    /// Lets a node link to a layer's file instead of copying it.
    files: Option<Arc<StoreFiles>>,
}

impl Store {
    /// A store that the node reaches only through `store`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Store {
        Store { store, files: None }
    }

    /// The store in the local directory `root`, which is created if it does
    /// not exist.
    ///
    /// Its own store, not `object_store`'s, because conditional writes (the
    /// cluster object, sweep records, trash markers) need compare-and-swap.
    ///
    /// # Errors
    ///
    /// Returns an error if `root` cannot be created, or is not an absolute
    /// path.
    pub fn local(root: &Path) -> Result<Store> {
        std::fs::create_dir_all(root)?;
        Ok(Store {
            store: Arc::new(super::local::LocalStore::new(root)?),
            files: Some(Arc::new(StoreFiles::new(root)?)),
        })
    }

    /// The raw store.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// A handle on this store, with a slot pool of its own at the default
    /// shares, for tools and tests.
    #[must_use]
    pub fn objects(&self) -> Arc<Objects> {
        self.objects_in(&IoLimiter::with_defaults(), None)
    }

    /// A handle on this store whose requests take slots from `io`, in class
    /// `pinned` or else the class of the task that makes them.
    ///
    /// Unbounded fan-out (a burst of uploads or deletes) opens hundreds of
    /// TLS connections at once and runs into connect timeouts.
    #[must_use]
    pub fn objects_in(&self, io: &Arc<IoLimiter>, pinned: Option<IoClass>) -> Arc<Objects> {
        Arc::new(Objects {
            store: Arc::new(ClassStore {
                inner: self.store.clone(),
                io: io.clone(),
                pinned,
            }),
            raw: self.store.clone(),
            io: io.clone(),
            pinned,
            files: self.files.clone(),
        })
    }
}

/// A node's handle on a store, whose requests take slots from its pool.
pub struct Objects {
    store: Arc<dyn ObjectStore>,
    raw: Arc<dyn ObjectStore>,
    io: Arc<IoLimiter>,
    pinned: Option<IoClass>,
    files: Option<Arc<StoreFiles>>,
}

impl fmt::Debug for Objects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Objects")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

impl Objects {
    /// The file that holds `key`, if the store is a local directory.
    #[must_use]
    pub fn file(&self, key: &StorePath) -> Option<std::path::PathBuf> {
        self.files.as_ref()?.path(key)
    }

    /// The store, under the node's slot pool, for bulk transfers.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The store, with its requests in the caller's class wherever they are
    /// made: for writers driven from a blocking thread.
    #[must_use]
    pub fn in_current_class(&self) -> Arc<dyn ObjectStore> {
        Arc::new(ClassStore {
            inner: self.raw.clone(),
            io: self.io.clone(),
            pinned: Some(self.pinned.unwrap_or_else(IoClass::current)),
        })
    }

    /// Read the object `path`, its version and when the object store says
    /// it was last written (its own clock, not this node's), if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the store request fails.
    pub async fn get_timed(
        &self,
        path: &str,
    ) -> Result<Option<(bytes::Bytes, ObjectVersion, std::time::SystemTime)>> {
        match self.store.get(&StorePath::from(path)).await {
            Ok(r) => {
                let version = ObjectVersion {
                    e_tag: r.meta.e_tag.clone(),
                    version: r.meta.version.clone(),
                };
                let modified = r.meta.last_modified.into();
                Ok(Some((r.bytes().await?, version, modified)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Compare-and-swap the object `path`: replace it if it is still at
    /// `expected`, or create it if `expected` is None and it does not
    /// exist. Returns the new version, or None if another writer got there
    /// first.
    ///
    /// # Errors
    ///
    /// Returns an error if the store request fails for a reason other than
    /// a lost race.
    pub async fn put_if(
        &self,
        path: &str,
        data: bytes::Bytes,
        expected: Option<&ObjectVersion>,
    ) -> Result<Option<ObjectVersion>> {
        let mode = match expected {
            Some(v) => object_store::PutMode::Update(v.clone()),
            None => object_store::PutMode::Create,
        };
        let opts = object_store::PutOptions {
            mode,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&StorePath::from(path), PutPayload::from(data), opts)
            .await
        {
            Ok(r) => Ok(Some(ObjectVersion {
                e_tag: r.e_tag,
                version: r.version,
            })),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Read the object `path`, if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the store request fails.
    pub async fn get_object(&self, path: &str) -> Result<Option<bytes::Bytes>> {
        Ok(self.get_timed(path).await?.map(|(data, ..)| data))
    }

    /// Paths of the objects under `prefix`, at any depth.
    ///
    /// # Errors
    ///
    /// Returns an error if the store request fails.
    pub async fn list_paths(&self, prefix: &str) -> Result<Vec<String>> {
        let mut list = self.store.list(Some(&StorePath::from(prefix)));
        let mut out = Vec::new();
        while let Some(meta) = list.next().await {
            out.push(meta?.location.to_string());
        }
        Ok(out)
    }

    /// Whether the store holds no object at all.
    ///
    /// # Errors
    ///
    /// Returns an error if the store request fails.
    pub(crate) async fn is_empty(&self) -> Result<bool> {
        Ok(self.store.list(None).next().await.transpose()?.is_none())
    }

    /// Write the object `path`, replacing it if it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the store request fails.
    pub async fn put_object(&self, path: &str, data: bytes::Bytes) -> Result<()> {
        self.store
            .put(&StorePath::from(path), PutPayload::from(data))
            .await?;
        Ok(())
    }

    /// Delete objects (missing ones are fine).
    ///
    /// # Errors
    ///
    /// Returns an error if the store request fails.
    pub async fn delete_objects(&self, paths: impl IntoIterator<Item = StorePath>) -> Result<()> {
        let items: Vec<object_store::Result<StorePath>> = paths.into_iter().map(Ok).collect();
        let mut results = self
            .store
            .delete_stream(futures::stream::iter(items).boxed());
        while let Some(r) = results.next().await {
            match r {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}
