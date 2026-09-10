//! Runs `cargo machete` over the workspace, so a dependency nothing names is
//! caught by the same command as everything else.
//!
//! Shelling out rather than reimplementing, as [`super::proto`] does with
//! `buf`: an unused dependency is invisible to every other check here, because
//! it compiles, it links, and it passes. A missing binary is a violation rather
//! than a skip for the same reason — a check that quietly passes when its
//! checker is absent is the shape of a check nobody notices is dead. It reads
//! source rather than resolving names, so a dependency reached only through a
//! macro can be reported; `[package.metadata.cargo-machete]` in the crate's own
//! manifest is where such a one is excused, beside the dependency it is about.

use std::path::{Path, PathBuf};

use super::Violation;

pub(crate) fn check(root: &Path) -> Vec<Violation> {
    // The binary and not `cargo machete`: cargo passes a subcommand's name
    // through as an argument, and under a `cargo` that is already running this
    // one reads it as a path to search instead of as its own name.
    match super::checker("cargo-machete", &[], root) {
        Ok(output) => parse(&output, root),
        Err(message) => vec![Violation {
            file: root.join("Cargo.toml"),
            line: None,
            message: format!(
                "could not run `cargo-machete` ({message}): \
                 cargo install cargo-machete"
            ),
        }],
    }
}

/// One violation per unused dependency, against the manifest naming it.
fn parse(output: &str, root: &Path) -> Vec<Violation> {
    // A manifest, then its dependencies indented under it:
    //     enroute-git-http -- ./crates/git/http/Cargo.toml:
    //         enroute-git-store
    let mut violations = Vec::new();
    let mut manifest: Option<PathBuf> = None;

    for line in output.lines() {
        if let Some(path) = manifest_of(line) {
            manifest = Some(root.join(path));
            continue;
        }
        // Indented under a manifest, and one word: everything else it prints
        // is prose, and the advice at the end is indented too.
        let dependency = line.trim();
        let indented = line.starts_with(['\t', ' ']);
        if !indented || dependency.is_empty() || dependency.contains(' ') {
            continue;
        }
        let Some(file) = manifest.clone() else {
            continue;
        };
        violations.push(Violation {
            line: names(&file, dependency),
            file,
            message: format!("`{dependency}` is a dependency nothing in this crate names"),
        });
    }
    violations
}

/// The manifest a `<crate> -- <path>:` line names.
fn manifest_of(line: &str) -> Option<&str> {
    let path = line.split_once(" -- ")?.1.strip_suffix(':')?;
    // Relative to the workspace root, and `Path::join` would take an absolute
    // one as the whole answer.
    let path = path.strip_prefix("./").unwrap_or(path);
    path.ends_with("Cargo.toml").then_some(path)
}

/// The line `dependency` is declared on, so a finding is somewhere to go.
fn names(manifest: &Path, dependency: &str) -> Option<usize> {
    let text = std::fs::read_to_string(manifest).ok()?;
    text.lines()
        .position(|line| {
            line.strip_prefix(dependency)
                .is_some_and(|rest| rest.starts_with([' ', '.', '=']))
        })
        .map(|index| index + 1)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{manifest_of, parse};

    const FOUND: &str = "\
Analyzing dependencies of crates in this directory...
cargo-machete found the following unused dependencies in this directory:
enroute-git-http -- ./crates/git/http/Cargo.toml:
\tenroute-git-store
xtask -- ./dev/xtask/Cargo.toml:
\tserde
\tserde_json

If you believe cargo-machete has detected an unused dependency incorrectly,
you can add the dependency to the list of dependencies to ignore in the
`[package.metadata.cargo-machete]` section of the appropriate Cargo.toml.
";

    #[test]
    fn every_unused_dependency_is_reported_against_its_own_manifest() {
        let violations = parse(FOUND, Path::new("/repo"));
        let found: Vec<(String, String)> = violations
            .iter()
            .map(|v| (v.file.display().to_string(), v.message.clone()))
            .collect();

        assert_eq!(found.len(), 3, "{found:?}");
        assert_eq!(found[0].0, "/repo/crates/git/http/Cargo.toml");
        assert!(found[0].1.contains("enroute-git-store"), "{found:?}");
        assert_eq!(found[1].0, "/repo/dev/xtask/Cargo.toml");
        assert!(found[1].1.contains("serde`"), "{found:?}");
        assert_eq!(found[2].0, "/repo/dev/xtask/Cargo.toml");
        assert!(found[2].1.contains("serde_json"), "{found:?}");
    }

    /// The advice it prints is indented too, and would otherwise read as a
    /// dependency of whichever manifest came last.
    #[test]
    fn the_trailing_advice_is_not_a_finding() {
        let violations = parse(FOUND, Path::new("/repo"));
        assert!(
            violations.iter().all(|v| !v.message.contains("section")),
            "prose became a finding"
        );
    }

    #[test]
    fn a_clean_run_finds_nothing() {
        let clean = "Analyzing dependencies of crates in this directory...\n\
                     cargo-machete didn't find any unused dependencies. Good job!\n";
        assert!(parse(clean, Path::new("/repo")).is_empty());
    }

    #[test]
    fn a_manifest_line_is_read_relative_to_the_root() {
        assert_eq!(
            manifest_of("xtask -- ./dev/xtask/Cargo.toml:"),
            Some("dev/xtask/Cargo.toml")
        );
        assert_eq!(manifest_of("cargo-machete found the following:"), None);
        assert_eq!(manifest_of("\tserde"), None);
    }
}
