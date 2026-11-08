//! Names of layer files, independent of where their bytes are (memory,
//! local disk, the object store).

use std::path::{Path, PathBuf};

use object_store::path::Path as StorePath;

use enroute_shared::repo_state::data_path;

/// A layer file: a repository and the layer's id (a random name, never reused).
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(super) struct FileKey {
    pub repo: String,
    pub id: String,
}

impl FileKey {
    pub(crate) fn new(repo: &str, id: &str) -> FileKey {
        FileKey {
            repo: repo.to_string(),
            id: id.to_string(),
        }
    }

    /// Key in the object store.
    pub(crate) fn store_path(&self) -> StorePath {
        StorePath::from(format!(
            "{}/layers/{}.layer",
            data_path(&self.repo),
            self.id
        ))
    }

    /// Prefix of `repo`'s layers in the object store.
    pub(crate) fn store_prefix(repo: &str) -> StorePath {
        StorePath::from(format!("{}/layers", data_path(repo)))
    }

    /// Path of the local copy under `root`.
    pub(crate) fn local_path(&self, root: &Path) -> PathBuf {
        root.join("layers")
            .join(self.repo.replace('/', "%2F"))
            .join(format!("{}.layer", self.id))
    }
}
