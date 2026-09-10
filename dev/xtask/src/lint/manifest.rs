//! Shared `Cargo.toml` reading/parsing helpers for the manifest-based lint
//! rules ([`super::workspace`] and [`super::workspace_inheritance`]).

use std::path::{Path, PathBuf};

use toml::Value;

/// Every workspace manifest, parsed once and shared by both manifest-based
/// rules, which used to each re-read and re-parse the same files.
pub(crate) struct Manifests {
    pub(crate) root: Value,
    /// `(manifest path, parsed manifest)` for each member that read cleanly.
    pub(crate) members: Vec<(PathBuf, Value)>,
    /// Manifest paths that failed to read or parse.
    pub(crate) unreadable_members: Vec<PathBuf>,
}

pub(crate) fn load(workspace_root: &Path) -> Option<Manifests> {
    let root = read(&workspace_root.join("Cargo.toml"))?;
    let mut members = Vec::new();
    let mut unreadable_members = Vec::new();

    for member in workspace_members(&root) {
        let manifest_path = workspace_root.join(member).join("Cargo.toml");
        match read(&manifest_path) {
            Some(manifest) => members.push((manifest_path, manifest)),
            None => unreadable_members.push(manifest_path),
        }
    }

    Some(Manifests {
        root,
        members,
        unreadable_members,
    })
}

fn read(path: &Path) -> Option<Value> {
    let text = std::fs::read_to_string(path).ok()?;
    toml::from_str(&text).ok()
}

/// The root manifest's `[workspace] members`, as repo-relative paths.
fn workspace_members(root_manifest: &Value) -> Vec<String> {
    root_manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(Value::as_array)
        .map(|members| {
            members
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// A member manifest's `[package] name`.
pub(crate) fn package_name(member_manifest: &Value) -> Option<&str> {
    member_manifest.get("package")?.get("name")?.as_str()
}

/// Every table a manifest can declare a dependency in.
///
/// Shared, so a rule reading one never quietly reads fewer than another.
pub(crate) const DEPENDENCY_TABLES: [&str; 3] =
    ["dependencies", "dev-dependencies", "build-dependencies"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_members_and_package_name() {
        let root: Value = toml::from_str(
            "
            [workspace]
            members = [\"crates/server/app\", \"crates/git/core\"]
            ",
        )
        .expect("valid toml fixture");
        assert_eq!(
            workspace_members(&root),
            vec![
                "crates/server/app".to_string(),
                "crates/git/core".to_string()
            ]
        );

        let member: Value = toml::from_str(
            "
            [package]
            name = \"enroute-git-core\"
            ",
        )
        .expect("valid toml fixture");
        assert_eq!(package_name(&member), Some("enroute-git-core"));
    }
}
