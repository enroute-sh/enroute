//! Walks the workspace running every registered lint rule.
//!
//! [`imports`]/[`comments`] run per file, [`dead_code`] over all files at
//! once, [`layering`]/[`workspace_inheritance`]/[`deps`] over manifests, and
//! [`proto`]/[`skill`] over the wire contract and what the skill copies of
//! it — room for more as separate modules.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "this tool's output is meant for the terminal/CI log: status on stdout, findings on stderr"
)]

mod comments;
mod dead_code;
mod deps;
mod imports;
mod layering;
mod manifest;
mod proto;
mod skill;
mod workspace;
mod workspace_inheritance;

use std::ffi::OsStr;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use walkdir::{DirEntry, WalkDir};

/// One lint finding: file, optional line, and message.
///
/// Every rule returns these, so this module owns the one place that turns
/// them into output — rules don't each invent their own format.
struct Violation {
    file: PathBuf,
    line: Option<usize>,
    message: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line {
            Some(line) => write!(f, "{}:{line}: {}", self.file.display(), self.message),
            None => write!(f, "{}: {}", self.file.display(), self.message),
        }
    }
}

/// Runs an installed checker, returning everything it printed.
///
/// Both streams, since a checker's findings and its complaints do not reliably
/// share one; a non-zero exit is a finding, so only a failure to run is `Err`.
fn checker(program: &str, args: &[&str], root: &Path) -> Result<String, String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|err| err.to_string())?;

    let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    Ok(combined)
}

pub(crate) fn run() -> ExitCode {
    let root = crate::root::workspace();

    let Some(manifests) = manifest::load(&root) else {
        return report(&[Violation {
            file: root.join("Cargo.toml"),
            line: None,
            message: "failed to read/parse root manifest".to_string(),
        }]);
    };

    let workspace_crates = workspace::crate_names(&manifests);
    let mut violations = workspace_inheritance::check(&manifests);
    violations.extend(layering::check(&manifests));
    violations.extend(proto::check(&root));
    violations.extend(skill::check(&root));
    violations.extend(deps::check(&root));
    let mut sources = Vec::new();

    for entry in WalkDir::new(&root).into_iter().filter_entry(|entry| {
        entry.file_name() != OsStr::new("target")
            && entry.file_name() != OsStr::new(".git")
            && !is_nested_checkout(entry, &root)
    }) {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                eprintln!("warning: {err}");
                continue;
            }
        };

        if !entry.file_type().is_file() {
            continue;
        }
        if entry.path().extension().and_then(OsStr::to_str) != Some("rs") {
            continue;
        }

        let source = match std::fs::read_to_string(entry.path()) {
            Ok(source) => source,
            Err(err) => {
                eprintln!("warning: failed to read {}: {err}", entry.path().display());
                continue;
            }
        };

        violations.extend(imports::check(&source, &workspace_crates, entry.path()));
        violations.extend(comments::check(&source, entry.path()));
        sources.push((entry.path().to_path_buf(), source));
    }

    violations.extend(dead_code::check(
        &sources,
        &workspace::crate_roots(&manifests),
    ));

    report(&violations)
}

/// True for any directory other than `root` with its own `.git` entry —
/// e.g. a linked worktree under `.claude/worktrees/`.
///
/// A separate checkout, sometimes mid-edit, so walking in would lint
/// content outside this run.
fn is_nested_checkout(entry: &DirEntry, root: &Path) -> bool {
    entry.file_type().is_dir() && entry.path() != root && entry.path().join(".git").exists()
}

fn report(violations: &[Violation]) -> ExitCode {
    if violations.is_empty() {
        println!("lint: ok");
        return ExitCode::SUCCESS;
    }

    for violation in violations {
        eprintln!("{violation}");
    }
    eprintln!("\n{} lint violation(s)", violations.len());
    ExitCode::FAILURE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_checkout_excluded_but_root_is_not() {
        let root = std::env::temp_dir().join(format!("xtask-lint-test-{}", std::process::id()));
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::create_dir_all(root.join("plain")).unwrap();
        std::fs::write(root.join("nested/.git"), "gitdir: /elsewhere").unwrap();

        let entry_named = |name: &str| {
            WalkDir::new(&root)
                .min_depth(1)
                .max_depth(1)
                .into_iter()
                .filter_map(Result::ok)
                .find(|entry| entry.file_name() == name)
                .unwrap()
        };

        assert!(is_nested_checkout(&entry_named("nested"), &root));
        assert!(!is_nested_checkout(&entry_named("plain"), &root));

        std::fs::remove_dir_all(&root).unwrap();
    }
}
