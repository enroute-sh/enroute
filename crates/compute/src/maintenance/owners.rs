//! Which compute node maintains a repository (flushes its inline layers,
//! compacts it, sweeps its orphans): the first of the live compute nodes in
//! the repository's rendezvous order ([`nodes::first`]), as ledger nodes
//! order themselves. The live computes are those the ledger nodes heard ask
//! for their pending repositories lately (`Pending` in `ledger.proto`),
//! which every compute does every few seconds.
//!
//! Correctness does not depend on it: every maintenance step is a manifest
//! compare-and-swap in the log, so two maintainers at once (while their
//! lists differ) only waste a merge. No node needs to win anything, so a
//! dead maintainer's repositories move as soon as the ledger stops hearing
//! from it.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use parking_lot::RwLock;

use enroute_shared::nodes;

/// The id of the compute node with `data_dir`, made on its first start.
/// It stays across restarts, so a node comes back to the repositories that
/// its disk has.
///
/// # Errors
///
/// Returns an error if the id cannot be read or written.
pub fn compute_id(data_dir: &Path) -> Result<u64> {
    let path = data_dir.join("compute-id");
    if let Ok(s) = std::fs::read_to_string(&path) {
        return u64::from_str_radix(s.trim(), 16).context("malformed compute-id");
    }
    std::fs::create_dir_all(data_dir)?;
    let id = rand::random::<u64>();
    // Durably: a torn file would fail every later start.
    enroute_shared::durable::write(&path, format!("{id:016x}\n").as_bytes(), 0o644)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(id)
}

pub(crate) struct Owners {
    me: u64,
    /// The live compute nodes, sorted, as the ledger last named them (None:
    /// not named yet, and this node maintains nothing).
    live: RwLock<Option<Arc<Vec<u64>>>>,
}

impl Owners {
    /// Compute node `me`, which maintains nothing until the ledger names
    /// the live computes ([`Owners::set_live`]).
    pub(crate) fn new(me: u64) -> Arc<Owners> {
        Arc::new(Owners {
            me,
            live: RwLock::default(),
        })
    }

    pub(super) fn me(&self) -> u64 {
        self.me
    }

    /// The live compute nodes are `live` (this one is, whatever it says).
    pub(super) fn set_live(&self, mut live: Vec<u64>) {
        live.push(self.me);
        live.sort_unstable();
        live.dedup();
        *self.live.write() = Some(Arc::new(live));
    }

    /// Whether this node maintains `repo`.
    pub(super) fn owns(&self, repo: &str) -> bool {
        self.live
            .read()
            .as_ref()
            .is_some_and(|live| nodes::first(repo, live) == Some(self.me))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_repository_has_one_owner() {
        let nodes: Vec<Arc<Owners>> = (1..=3).map(Owners::new).collect();
        assert!(
            !nodes[0].owns("a"),
            "nothing before the ledger names the live computes"
        );
        for n in &nodes {
            n.set_live(vec![1, 2, 3]);
        }
        for repo in ["a", "b", "c", "d", "e"] {
            let owners = nodes.iter().filter(|n| n.owns(repo)).count();
            assert_eq!(owners, 1, "{repo}");
        }
    }
}
