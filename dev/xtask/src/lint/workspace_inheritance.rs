//! Checks that member crates inherit shared `[package]` fields and deps
//! from the workspace root, via `field.workspace = true`, not restated.
//!
//! Two rules: restating a dep the root declares is a violation, and so is
//! one absent from `[workspace.dependencies]` — the second closes the
//! first's hole, where a new dep the root never learns of drifts crate by
//! crate. `path` and `git` deps are exempt, naming a location, not a version.
//! A crate that must differ says so in `[package.xtask] overrides`,
//! which turns the exception into something a reader can grep for.

use std::collections::HashSet;
use std::path::Path;

use toml::Value;

use super::Violation;
use super::manifest::{DEPENDENCY_TABLES, Manifests};

pub(crate) fn check(manifests: &Manifests) -> Vec<Violation> {
    let workspace_package_fields = table_keys(&manifests.root, "workspace", "package");
    let workspace_dep_names = table_keys(&manifests.root, "workspace", "dependencies");

    let mut violations: Vec<Violation> = manifests
        .unreadable_members
        .iter()
        .map(|file| Violation {
            file: file.clone(),
            line: None,
            message: "failed to read/parse manifest".to_string(),
        })
        .collect();

    for (file, member_manifest) in &manifests.members {
        violations.extend(check_member(
            file,
            member_manifest,
            &workspace_package_fields,
            &workspace_dep_names,
        ));
    }

    violations
}

/// The `[package]` fields this crate declares it will not inherit.
///
/// `[package.xtask] overrides = ["license"]` — cargo ignores it, and
/// naming a field here is the review that letting one differ deserves.
fn declared_overrides(package: Option<&toml::Table>) -> HashSet<&str> {
    package
        .and_then(|package| package.get("metadata"))
        .and_then(Value::as_table)
        .and_then(|metadata| metadata.get("xtask"))
        .and_then(Value::as_table)
        .and_then(|xtask| xtask.get("overrides"))
        .and_then(Value::as_array)
        .map(|fields| fields.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

fn check_member(
    file: &Path,
    member_manifest: &Value,
    workspace_package_fields: &HashSet<String>,
    workspace_dep_names: &HashSet<String>,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    let violation = |message: String| Violation {
        file: file.to_path_buf(),
        line: None,
        message,
    };

    let package = member_manifest.get("package").and_then(Value::as_table);
    let overrides = declared_overrides(package);
    for field in workspace_package_fields {
        let Some(value) = package.and_then(|package| package.get(field.as_str())) else {
            continue;
        };
        if inherits_workspace(value) || overrides.contains(field.as_str()) {
            continue;
        }
        violations.push(violation(format!(
            "[package].{field} should be `{field}.workspace = true` (inherit from \
             [workspace.package]), found {value}"
        )));
    }

    for table_name in DEPENDENCY_TABLES {
        let Some(table) = member_manifest.get(table_name).and_then(Value::as_table) else {
            continue;
        };
        for (dep_name, dep_value) in table {
            if inherits_workspace(dep_value) || is_local(dep_value) {
                continue;
            }
            let fix = if workspace_dep_names.contains(dep_name) {
                "should inherit via `workspace = true`"
            } else {
                "should be declared in [workspace.dependencies] and inherited here via \
                 `workspace = true`"
            };
            violations.push(violation(format!(
                "[{table_name}].{dep_name} {fix}, found {dep_value}"
            )));
        }
    }

    violations
}

/// The keys of `manifest.section.key`, or empty if either is missing —
/// e.g. `("workspace", "package")` for `[workspace.package]`'s fields.
fn table_keys(manifest: &Value, section: &str, key: &str) -> HashSet<String> {
    manifest
        .get(section)
        .and_then(|value| value.get(key))
        .and_then(Value::as_table)
        .map(|table| table.keys().cloned().collect())
        .unwrap_or_default()
}

/// A dep pointing at a location rather than a registry version: nothing to
/// unify across the workspace, so neither rule applies.
fn is_local(value: &Value) -> bool {
    value
        .as_table()
        .is_some_and(|table| table.contains_key("path") || table.contains_key("git"))
}

/// `dep = { workspace = true, features = [...] }` inherits the workspace
/// value; anything else restates it instead.
fn inherits_workspace(value: &Value) -> bool {
    value
        .as_table()
        .and_then(|table| table.get("workspace"))
        .and_then(Value::as_bool)
        == Some(true)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::check;
    use crate::lint::manifest;

    fn write(path: &std::path::Path, contents: &str) {
        fs::create_dir_all(path.parent().expect("has parent")).expect("mkdir");
        fs::write(path, contents).expect("write manifest");
    }

    #[test]
    fn restated_field_and_dependency_are_flagged() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();

        write(
            &root.join("Cargo.toml"),
            "
            [workspace]
            members = [\"crates/foo\"]

            [workspace.package]
            edition = \"2024\"

            [workspace.dependencies]
            tokio = \"1\"
            ",
        );
        write(
            &root.join("crates/foo/Cargo.toml"),
            "
            [package]
            name = \"foo\"
            edition = \"2024\"

            [dependencies]
            tokio = \"1\"
            ",
        );

        let manifests = manifest::load(root).expect("manifests load");
        let violations = check(&manifests);
        assert!(
            violations
                .iter()
                .any(|v| v.message.contains("[package].edition")
                    && v.message.contains("workspace = true")),
            "expected a restated-field violation"
        );
        assert!(
            violations
                .iter()
                .any(|v| v.message.contains("[dependencies].tokio")),
            "expected a restated-dependency violation"
        );
    }

    #[test]
    fn inherited_field_and_dependency_are_not_flagged() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();

        write(
            &root.join("Cargo.toml"),
            "
            [workspace]
            members = [\"crates/foo\"]

            [workspace.package]
            edition = \"2024\"

            [workspace.dependencies]
            tokio = \"1\"
            ",
        );
        write(
            &root.join("crates/foo/Cargo.toml"),
            "
            [package]
            name = \"foo\"
            edition.workspace = true

            [dependencies]
            tokio = { workspace = true, features = [\"full\"] }
            ",
        );

        let manifests = manifest::load(root).expect("manifests load");
        assert!(check(&manifests).is_empty());
    }

    /// The hole the second rule closes: a dep the root has never heard of, so
    /// a restatement check has nothing to compare against.
    #[test]
    fn external_dependency_missing_from_the_root_is_flagged() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();

        write(
            &root.join("Cargo.toml"),
            "
            [workspace]
            members = [\"crates/foo\"]

            [workspace.dependencies]
            tokio = \"1\"
            ",
        );
        write(
            &root.join("crates/foo/Cargo.toml"),
            "
            [package]
            name = \"foo\"

            [dependencies]
            tokio = { workspace = true }
            serde = \"1\"
            ",
        );

        let manifests = manifest::load(root).expect("manifests load");
        let violations = check(&manifests);
        let messages: Vec<&str> = violations.iter().map(|v| v.message.as_str()).collect();
        assert_eq!(violations.len(), 1, "{messages:?}");
        assert!(
            violations[0].message.contains("serde")
                && violations[0].message.contains("[workspace.dependencies]"),
            "{}",
            violations[0].message
        );
    }

    /// `path` and `git` deps name a location, not a version, so there is
    /// nothing to unify.
    #[test]
    fn local_dependencies_are_exempt() {
        let dir = tempdir().expect("tempdir");
        let root = dir.path();

        write(
            &root.join("Cargo.toml"),
            "
            [workspace]
            members = [\"crates/foo\"]
            ",
        );
        write(
            &root.join("crates/foo/Cargo.toml"),
            "
            [package]
            name = \"foo\"

            [dependencies]
            sibling = { path = \"../sibling\" }
            upstream = { git = \"https://example.invalid/x\" }
            ",
        );

        let manifests = manifest::load(root).expect("manifests load");
        assert!(check(&manifests).is_empty());
    }
}
