//! A repository's objects in the object store that both roles name.
//!
//! The store splits by who writes: the ledger writes under `ledger/`, and
//! compute nodes only under `data/` (where the ledger writes too). So a
//! bucket policy that lets compute nodes write only `data/` keeps them from
//! forging the ledger's registry (and its token keys).
//!
//! What the ledger keeps under `ledger/` is its own (see
//! `enroute_ledger::repo_state`); the compute never reads it.
//!
//! ```text
//! data/repos/<repo>/              the repository's layer files
//! data/trash/<repo>/<E>           incarnation E was deleted and its layer
//!                                 files may remain (see [`Trash`])
//! ```

/// The prefix of `repo`'s objects that compute nodes write.
#[must_use]
pub fn data_path(repo: &str) -> String {
    format!("data/repos/{repo}")
}

/// The prefix of the trash objects.
pub const TRASH_PREFIX: &str = "data/trash/";

/// The trash object of incarnation `epoch` of `repo`.
#[must_use]
pub fn trash_path(repo: &str, epoch: u64) -> String {
    format!("{TRASH_PREFIX}{repo}/{epoch:016x}")
}

/// The repository and incarnation of a trash path.
#[must_use]
pub fn parse_trash_path(path: &str) -> Option<(&str, u64)> {
    let (repo, epoch) = path.strip_prefix(TRASH_PREFIX)?.rsplit_once('/')?;
    Some((repo, u64::from_str_radix(epoch, 16).ok()?))
}

/// A deleted incarnation's layer files to reclaim, as JSON at its trash
/// path. The ledger writes it once the incarnation's tombstone is committed
/// and before the incarnation ends, so no deletion leaves files nobody will
/// reclaim; the compute removes it once
/// they are gone. Every file of the incarnation (or an older one) has an
/// upload generation of at most `upload_gen`, and no later incarnation
/// takes such a file (it starts above `upload_gen + UPLOAD_GEN_WINDOW`).
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Trash {
    /// The highest upload generation of the files to reclaim.
    pub upload_gen: u64,
    /// The reclaim's progress, as the layer sweep records it.
    #[serde(default)]
    pub unnamed: Unnamed,
}

/// Layer files seen unnamed, each with since when by the object store's
/// clock (milliseconds since the Unix epoch), once known.
pub type Unnamed = std::collections::BTreeMap<String, Option<u64>>;
