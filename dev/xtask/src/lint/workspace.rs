//! Discovers which crate names belong to this workspace, as opposed to
//! genuine third-party dependencies.
//!
//! Used by [`super::imports`] to split imports into "external crate" vs.
//! "workspace-local crate" groups.

use std::collections::HashSet;
use std::path::PathBuf;

use super::manifest::{Manifests, package_name};

/// A crate root: the package name, and the directory its sources live in.
pub(crate) struct CrateRoot {
    pub(crate) name: String,
    pub(crate) dir: PathBuf,
}

/// Every workspace member's crate name, as it appears in `use` paths
/// (dashes converted to underscores).
pub(crate) fn crate_names(manifests: &Manifests) -> HashSet<String> {
    manifests
        .members
        .iter()
        .filter_map(|(_, manifest)| package_name(manifest).map(str::to_string))
        .map(|name| name.replace('-', "_"))
        .collect()
}

/// Every workspace member, paired with the directory holding its sources.
///
/// Used by [`super::dead_code`] to say which crate a file belongs to.
pub(crate) fn crate_roots(manifests: &Manifests) -> Vec<CrateRoot> {
    manifests
        .members
        .iter()
        .filter_map(|(file, manifest)| {
            Some(CrateRoot {
                name: package_name(manifest)?.to_string(),
                dir: file.parent()?.to_path_buf(),
            })
        })
        .collect()
}
