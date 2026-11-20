//! Waives a named rule for the files of one crate, as that crate's manifest
//! declares in `[package.metadata.xtask] lint-debt`.
//!
//! For code imported with its debt (the engine crates), so that the rule stays on
//! for every other crate. A waiver that waives nothing is a violation, so a
//! crate that pays its debt must also remove the waiver.

use std::path::{Path, PathBuf};

use toml::Value;

use super::Violation;
use super::manifest::Manifests;

/// The rules a crate can waive: the ones that read its source files.
const RULES: [&str; 4] = ["comments", "dead_code", "deps", "imports"];

struct Waiver {
    manifest: PathBuf,
    dir: PathBuf,
    rule: String,
    waived: usize,
}

/// Every waiver the workspace declares, with how much each one waived.
pub(crate) struct Debt {
    waivers: Vec<Waiver>,
}

impl Debt {
    pub(crate) fn load(manifests: &Manifests) -> Self {
        let waivers = manifests
            .members
            .iter()
            .flat_map(|(file, manifest)| {
                let dir = file.parent().unwrap_or(file).to_path_buf();
                declared(manifest).into_iter().map(move |rule| Waiver {
                    manifest: file.clone(),
                    dir: dir.clone(),
                    rule,
                    waived: 0,
                })
            })
            .collect();
        Self { waivers }
    }

    /// `violations` of `rule`, less those in a crate that waives it.
    pub(crate) fn waive(&mut self, rule: &str, violations: Vec<Violation>) -> Vec<Violation> {
        violations
            .into_iter()
            .filter(|violation| !self.covers(rule, &violation.file))
            .collect()
    }

    fn covers(&mut self, rule: &str, file: &Path) -> bool {
        let Some(waiver) = self
            .waivers
            .iter_mut()
            .find(|waiver| waiver.rule == rule && file.starts_with(&waiver.dir))
        else {
            return false;
        };
        waiver.waived = waiver.waived.saturating_add(1);
        true
    }

    /// One violation per waiver that names no rule or waived nothing.
    pub(crate) fn stale(self) -> Vec<Violation> {
        self.waivers
            .into_iter()
            .filter_map(|waiver| {
                let message = if !RULES.contains(&waiver.rule.as_str()) {
                    format!(
                        "lint-debt names `{}`, which is not a rule it can waive: one of {}",
                        waiver.rule,
                        RULES.join(", ")
                    )
                } else if waiver.waived == 0 {
                    format!(
                        "lint-debt waives `{}`, and this crate has no such violation: remove it",
                        waiver.rule
                    )
                } else {
                    return None;
                };
                Some(Violation {
                    file: waiver.manifest,
                    line: None,
                    message,
                })
            })
            .collect()
    }
}

/// The rules a member manifest waives.
fn declared(manifest: &Value) -> Vec<String> {
    manifest
        .get("package")
        .and_then(|package| package.get("metadata"))
        .and_then(|metadata| metadata.get("xtask"))
        .and_then(|xtask| xtask.get("lint-debt"))
        .and_then(Value::as_array)
        .map_or_default(|rules| {
            rules
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use tempfile::tempdir;

    use super::Debt;
    use crate::lint::{Violation, manifest};

    fn violation(file: &Path) -> Violation {
        Violation {
            file: file.to_path_buf(),
            line: Some(1),
            message: "finding".to_string(),
        }
    }

    fn debt(waived: &str) -> (tempfile::TempDir, Debt) {
        let dir = tempdir().expect("tempdir");
        let write = |path: &str, contents: &str| {
            let path = dir.path().join(path);
            fs::create_dir_all(path.parent().expect("has parent")).expect("mkdir");
            fs::write(path, contents).expect("write manifest");
        };
        write(
            "Cargo.toml",
            "[workspace]\nmembers = [\"crates/a/old\", \"crates/a/new\"]\n",
        );
        write(
            "crates/a/old/Cargo.toml",
            &format!(
                "[package]\nname = \"old\"\n[package.metadata.xtask]\nlint-debt = [{waived}]\n"
            ),
        );
        write("crates/a/new/Cargo.toml", "[package]\nname = \"new\"\n");
        let debt = Debt::load(&manifest::load(dir.path()).expect("manifests load"));
        (dir, debt)
    }

    #[test]
    fn only_the_waiving_crate_and_rule_are_waived() {
        let (dir, mut debt) = debt("\"comments\"");
        let old = dir.path().join("crates/a/old/src/lib.rs");
        let new = dir.path().join("crates/a/new/src/lib.rs");

        let comments = debt.waive("comments", vec![violation(&old), violation(&new)]);
        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].file, new);

        assert_eq!(debt.waive("imports", vec![violation(&old)]).len(), 1);
        assert!(debt.stale().is_empty());
    }

    #[test]
    fn a_waiver_with_nothing_to_waive_is_reported() {
        let (_dir, debt) = debt("\"comments\"");
        let stale = debt.stale();
        assert_eq!(stale.len(), 1);
        assert!(
            stale[0].message.contains("remove it"),
            "{}",
            stale[0].message
        );
    }

    #[test]
    fn a_waiver_of_an_unknown_rule_is_reported() {
        let (_dir, debt) = debt("\"layering\"");
        let stale = debt.stale();
        assert_eq!(stale.len(), 1);
        assert!(
            stale[0].message.contains("not a rule"),
            "{}",
            stale[0].message
        );
    }
}
