//! Checks the one-way dependencies between the layers under `crates/`:
//! `api`, `lattice`, `git` and `server`.
//!
//! `api` is the wire contract, `lattice` the storage substrate, `git` the
//! engine built on it, and `server` the service built on both. `api` and
//! `lattice` may each name only themselves, for one reason: an `api` crate
//! has an end outside Rust, and a substrate able to read the commit graph
//! would be shaped by it. Neither gap shows up in a build here — the first
//! surfaces when somebody generates a client in another language, the second
//! when something else tries to store something in the substrate. Cargo
//! accepts any acyclic graph, so nothing but this rule stops a crate reaching
//! sideways or upward, which is how the layers blurred before; a crate's
//! layer is its directory, so moving a crate changes what it may depend on.

use std::collections::HashMap;
use std::path::Path;

use toml::Value;

use super::Violation;
use super::manifest::{DEPENDENCY_TABLES, Manifests, package_name};

pub(crate) fn check(manifests: &Manifests) -> Vec<Violation> {
    let mut violations = Vec::new();

    // A directory under `crates/` naming no layer would be waved through
    // unconstrained — the same hole this rule exists to close, reached by a
    // typo'd or unregistered directory rather than by a dependency edge.
    for (file, _) in &manifests.members {
        if let Some(dir) = layer_dir(file)
            && layer(dir).is_none()
        {
            violations.push(Violation {
                file: file.clone(),
                line: None,
                message: format!(
                    "`crates/{dir}` names no layer: a crate under `crates/` belongs to one of {}",
                    Layer::ALL.map(Layer::name).join(", ")
                ),
            });
        }
    }

    let owner: HashMap<&str, Layer> = manifests
        .members
        .iter()
        .filter_map(|(file, manifest)| Some((package_name(manifest)?, layer_of(file)?)))
        .collect();

    for (file, manifest) in &manifests.members {
        // `dev/` tooling drives every layer by design, so it sits outside the
        // rule rather than being an exception inside it.
        let Some(from) = layer_of(file) else { continue };
        for (dep, to) in reaches_too_far(manifest, from, &owner) {
            violations.push(Violation {
                file: file.clone(),
                line: None,
                message: format!(
                    "`{dep}` is a `{}` crate, and a `{}` crate may name only {}",
                    to.name(),
                    from.name(),
                    from.allowed()
                        .iter()
                        .map(|layer| format!("`{}`", layer.name()))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            });
        }
    }

    violations
}

/// Every workspace dependency of `manifest` a crate in `from` may not name,
/// with the layer it belongs to.
///
/// A dependency outside the workspace answers to no layer and is skipped.
fn reaches_too_far<'a>(
    manifest: &'a Value,
    from: Layer,
    owner: &HashMap<&'a str, Layer>,
) -> Vec<(&'a str, Layer)> {
    DEPENDENCY_TABLES
        .iter()
        .filter_map(|table| manifest.get(*table).and_then(Value::as_table))
        .flat_map(toml::Table::keys)
        .filter_map(|dep| {
            let to = owner.get(dep.as_str()).copied()?;
            (!from.may_name(to)).then_some((dep.as_str(), to))
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layer {
    Api,
    Lattice,
    Git,
    Server,
}

impl Layer {
    const ALL: [Self; 4] = [Self::Api, Self::Lattice, Self::Git, Self::Server];

    fn name(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Lattice => "lattice",
            Self::Git => "git",
            Self::Server => "server",
        }
    }

    /// Every layer a crate in this one may name, itself included.
    fn allowed(self) -> &'static [Self] {
        match self {
            Self::Api => &[Self::Api],
            Self::Lattice => &[Self::Lattice],
            Self::Git => &[Self::Git, Self::Lattice],
            Self::Server => &[Self::Api, Self::Lattice, Self::Git, Self::Server],
        }
    }

    fn may_name(self, other: Self) -> bool {
        self.allowed().contains(&other)
    }
}

fn layer(dir: &str) -> Option<Layer> {
    Layer::ALL.into_iter().find(|layer| layer.name() == dir)
}

/// The layer a member belongs to, or `None` for one outside `crates/`.
fn layer_of(manifest_path: &Path) -> Option<Layer> {
    layer_dir(manifest_path).and_then(layer)
}

/// The directory naming a member's layer in `crates/<layer>/<crate>/
/// Cargo.toml`, or `None` outside `crates/`.
///
/// `dev/` tooling drives every layer and answers to none.
fn layer_dir(manifest_path: &Path) -> Option<&str> {
    let layer = manifest_path.parent()?.parent()?;
    if layer.parent()?.file_name()? != "crates" {
        return None;
    }
    layer.file_name()?.to_str()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::{TempDir, tempdir};

    use super::check;
    use crate::lint::manifest;

    fn write(path: &std::path::Path, contents: &str) {
        fs::create_dir_all(path.parent().expect("has parent")).expect("mkdir");
        fs::write(path, contents).expect("write manifest");
    }

    /// A workspace of `(directory, package name, dependency)` members, laid
    /// out on disk so the rule reads the paths `manifest::load` builds.
    fn workspace(members: &[(&str, &str, &str)]) -> TempDir {
        let dir = tempdir().expect("tempdir");
        let list = members
            .iter()
            .map(|(path, ..)| format!("\"{path}\""))
            .collect::<Vec<_>>()
            .join(", ");
        write(
            &dir.path().join("Cargo.toml"),
            &format!("[workspace]\nmembers = [{list}]\n"),
        );
        for (path, name, dep) in members {
            write(
                &dir.path().join(path).join("Cargo.toml"),
                &format!("[package]\nname = \"{name}\"\n[dependencies]\n{dep}\n"),
            );
        }
        dir
    }

    fn violations(members: &[(&str, &str, &str)]) -> Vec<String> {
        let dir = workspace(members);
        check(&manifest::load(dir.path()).expect("manifests load"))
            .into_iter()
            .map(|violation| violation.message)
            .collect()
    }

    #[test]
    fn a_enroute_git_crate_may_not_depend_on_a_server_crate() {
        let found = violations(&[
            ("crates/git/http", "enroute-git-http", "enroute = \"1\""),
            ("crates/server/app", "enroute", ""),
        ]);
        assert_eq!(found.len(), 1, "expected exactly one upward dependency");
        assert!(found[0].contains("is a `server` crate"), "{found:?}");
    }

    // The whole point of the rule is that it constrains one direction only.
    #[test]
    fn a_server_crate_may_depend_on_a_enroute_git_crate() {
        assert!(
            violations(&[
                ("crates/git/http", "enroute-git-http", ""),
                ("crates/server/app", "enroute", "enroute-git-http = \"1\""),
            ])
            .is_empty(),
            "the service is allowed to name the git engine"
        );
    }

    // A second consumer only tests the contract if it cannot read the engine
    // directly, which is what the `api` rule is for.
    /// The Rust SDK and the example application were both dropped as layers
    /// of their own: there is no SDK, and the application is the customer's.
    ///
    /// Re-adding either as a Rust crate must register a layer deliberately,
    /// not sit in a directory that quietly answers to no rule.
    #[test]
    fn a_reintroduced_sdk_or_application_must_register_its_layer() {
        for dir in ["crates/sdk/git", "crates/hooks/app"] {
            let found = violations(&[(dir, "whatever", "")]);
            assert_eq!(found.len(), 1, "{dir}: {found:?}");
            assert!(found[0].contains("names no layer"), "{dir}: {found:?}");
        }
    }

    // The substrate is generic, and a dependency edge is the only way that
    // claim can be broken. Enforcing it is the same rule `api` answers to.
    #[test]
    fn the_substrate_may_not_depend_on_what_is_stored_in_it() {
        let found = violations(&[
            (
                "crates/lattice/core",
                "enroute-lattice-core",
                "enroute-git-graph = \"1\"",
            ),
            ("crates/git/graph", "enroute-git-graph", ""),
        ]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("may name only `lattice`"), "{found:?}");
    }

    #[test]
    fn the_engine_may_store_its_graph_in_the_substrate() {
        assert!(
            violations(&[
                ("crates/lattice/core", "enroute-lattice-core", ""),
                (
                    "crates/git/graph",
                    "enroute-git-graph",
                    "enroute-lattice-core = \"1\"",
                ),
            ])
            .is_empty(),
            "the engine is allowed to name the substrate"
        );
    }

    // Both ends name the contract, so anything it named would land in both.
    #[test]
    fn the_contract_may_not_depend_on_either_end() {
        let found = violations(&[
            (
                "crates/api/types",
                "enroute-api",
                "enroute-git-core = \"1\"",
            ),
            ("crates/git/core", "enroute-git-core", ""),
        ]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("may name only `api`"), "{found:?}");
    }

    // `dev/` tooling drives every layer by design, so it sits outside the
    // rule rather than being an exception inside it.
    #[test]
    fn dev_tooling_answers_to_no_layer() {
        assert!(
            violations(&[
                ("dev/e2e", "e2e", "enroute = \"1\""),
                ("crates/server/app", "enroute", ""),
            ])
            .is_empty(),
            "`dev/` is unconstrained"
        );
    }

    // Otherwise a crate could escape the rule by sitting in a directory the
    // rule has never heard of, which is the failure it exists to prevent.
    #[test]
    fn a_crates_directory_naming_no_layer_is_flagged() {
        let found = violations(&[("crates/edge/proxy", "edge-proxy", "")]);
        assert_eq!(found.len(), 1, "expected exactly one unknown-layer report");
        assert!(
            found[0].contains("`crates/edge` names no layer"),
            "{found:?}"
        );
    }
}
