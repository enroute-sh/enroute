//! Runs `buf` over the protobuf module, so the wire contract is checked by
//! the same command as everything else rather than by a habit.
//!
//! Shelling out rather than reimplementing: `buf` owns the rule set, and a
//! second opinion on what a valid `.proto` is would be worse than no opinion.
//! That makes `buf` a required tool once the workspace has a `.proto`, which is
//! why both a missing binary and a missing config are violations rather than
//! skips — a check that quietly passes when its checker is absent is the shape
//! of a check nobody notices is dead.

use std::path::{Path, PathBuf};

use walkdir::WalkDir;

use super::Violation;

/// Where the protobuf module's config lives, relative to the workspace root.
const CONFIG: &str = "buf.yaml";

pub(crate) fn check(root: &Path) -> Vec<Violation> {
    let config = root.join(CONFIG);
    // Decided by what needs checking rather than by whether the checker is
    // configured. A workspace with no `.proto` has nothing to lint; one that
    // has them and no config is unlinted, which is the answer this check
    // exists to give — otherwise renaming `buf.yaml` silently retires it.
    let Some(found) = first_proto(root) else {
        return Vec::new();
    };
    if !config.exists() {
        return vec![Violation {
            file: found,
            line: None,
            message: format!("protobuf is not linted: no {CONFIG} at the workspace root"),
        }];
    }

    // Lint only. `buf format` used to run here too, and does not any more:
    // `//tools/format` runs the same `buf` over the same files, along with every
    // other formatter, and two checks of one thing are two answers waiting to
    // disagree about which command fixes it.
    match super::checker("buf", &["lint", "--error-format", "text"], root) {
        Ok(output) => output
            .lines()
            .filter_map(|line| parse(line, root))
            .collect(),
        Err(message) => vec![missing(&config, &message)],
    }
}

/// The first `.proto` in the workspace, or `None` if it has none.
///
/// Generated trees are skipped: what a dependency ships is not this
/// contract, and `node_modules` alone dwarfs the rest of the tree.
fn first_proto(root: &Path) -> Option<PathBuf> {
    WalkDir::new(root)
        .into_iter()
        .filter_entry(|entry| {
            !matches!(
                entry.file_name().to_str(),
                Some(".git" | "target" | "node_modules")
            )
        })
        .filter_map(Result::ok)
        .map(walkdir::DirEntry::into_path)
        .find(|path| path.extension().is_some_and(|ext| ext == "proto"))
}

fn missing(config: &Path, message: &str) -> Violation {
    Violation {
        file: config.to_path_buf(),
        line: None,
        message: format!(
            "could not run `buf` ({message}): see https://buf.build/docs/installation"
        ),
    }
}

/// One `buf` finding: `path:line:col:message`, split to stop after the
/// position since the message carries colons of its own.
fn parse(line: &str, root: &Path) -> Option<Violation> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }

    let mut parts = line.splitn(4, ':');
    let file = parts.next()?;
    let number = parts.next().and_then(|n| n.parse().ok());
    let _column = parts.next();
    // Anything `buf` prints that isn't a located finding — a config error, a
    // usage message — still has to reach the caller, so it becomes a
    // violation against the config rather than being dropped.
    let Some(message) = parts.next() else {
        return Some(Violation {
            file: root.join(CONFIG),
            line: None,
            message: line.to_string(),
        });
    };

    Some(Violation {
        file: root.join(file),
        line: number,
        message: message.trim().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::parse;

    #[test]
    fn a_located_finding_keeps_its_file_and_line() {
        let violation = parse(
            "proto/enroute/v1/git.proto:21:9:Service name \"Git\" should be suffixed with \"Service\".",
            Path::new("/repo"),
        )
        .expect("a finding");
        assert_eq!(
            violation.file,
            Path::new("/repo/proto/enroute/v1/git.proto")
        );
        assert_eq!(violation.line, Some(21));
        assert_eq!(
            violation.message,
            "Service name \"Git\" should be suffixed with \"Service\"."
        );
    }

    // A message containing colons is the case a plain `split(':')` gets wrong,
    // and buf's own messages quote type names that contain them.
    #[test]
    fn a_message_containing_colons_survives_intact() {
        let violation = parse(
            "proto/a.proto:3:1:import \"b.proto\": file does not exist",
            Path::new("/repo"),
        )
        .expect("a finding");
        assert_eq!(violation.message, "import \"b.proto\": file does not exist");
    }

    // Losing these would mean a broken config reports as a clean run.
    #[test]
    fn an_unlocated_line_is_reported_against_the_config() {
        let violation = parse("Failure: invalid buf.yaml", Path::new("/repo")).expect("a finding");
        assert_eq!(violation.file, Path::new("/repo/buf.yaml"));
        assert_eq!(violation.line, None);
        assert_eq!(violation.message, "Failure: invalid buf.yaml");
    }

    #[test]
    fn blank_output_is_not_a_finding() {
        assert!(parse("   ", Path::new("/repo")).is_none());
    }
}
