//! Checks that a file's leading `use` block is grouped std → external →
//! workspace-local → `crate`/`self`/`super`, sorted within each group.
//!
//! Stable `rustfmt` sorts existing blocks but won't enforce this grouping:
//! `group_imports` is nightly-only, and only knows the three-way split, not
//! the workspace-local tier.

use std::collections::HashSet;
use std::path::Path;

use super::Violation;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Group {
    Std,
    External,
    WorkspaceLocal,
    CrateLocal,
}

struct UseStatement {
    line: usize,
    group: Group,
    path: String,
}

/// Returns one violation per problem in `source`'s leading `use` block.
///
/// `workspace_crates` is the set of crate names, as they appear in `use`
/// paths, that belong to this workspace rather than being third-party.
pub(crate) fn check(
    source: &str,
    workspace_crates: &HashSet<String>,
    file: &Path,
) -> Vec<Violation> {
    let lines: Vec<&str> = source.lines().collect();
    let statements = leading_use_statements(&lines, workspace_crates);
    let violation = |line: usize, message: String| Violation {
        file: file.to_path_buf(),
        line: Some(line),
        message,
    };

    let mut violations = Vec::new();
    let mut max_group = Group::Std;

    for statement in &statements {
        if statement.group < max_group {
            violations.push(violation(
                statement.line,
                format!(
                    "`use {}` is out of order — expected std, then external crates, then \
                     workspace-local crates, then crate::/self::/super::, but a later group \
                     already started",
                    statement.path
                ),
            ));
        } else {
            max_group = statement.group;
        }
    }

    for (prev, next) in statements.iter().zip(statements.iter().skip(1)) {
        if prev.group == next.group && next.path < prev.path {
            violations.push(violation(
                next.line,
                format!(
                    "`use {}` should be sorted before `use {}` on line {}",
                    next.path, prev.path, prev.line
                ),
            ));
        }
    }

    violations
}

/// Strips a leading `pub`, `pub(crate)`, `pub(super)`, etc. visibility
/// modifier, returning the remainder (starting at `use`).
fn strip_visibility(statement: &str) -> &str {
    let after_pub = statement
        .strip_prefix("pub")
        .map_or(statement, str::trim_start);
    let Some(paren_rest) = after_pub.strip_prefix('(') else {
        return after_pub;
    };
    match paren_rest.split_once(')') {
        Some((_, after_paren)) => after_paren.trim_start(),
        None => after_pub,
    }
}

fn classify(
    line: usize,
    statement: &str,
    workspace_crates: &HashSet<String>,
) -> Option<UseStatement> {
    let path = strip_visibility(statement)
        .strip_prefix("use ")?
        .trim_end_matches(';')
        .trim();

    let first_segment = path.split("::").next().unwrap_or(path);
    let group = match first_segment {
        "std" | "core" | "alloc" => Group::Std,
        "crate" | "self" | "super" => Group::CrateLocal,
        seg if workspace_crates.contains(seg) => Group::WorkspaceLocal,
        _ => Group::External,
    };

    Some(UseStatement {
        line,
        group,
        path: path.to_string(),
    })
}

fn delim_delta(s: &str, open: char, close: char) -> i32 {
    let opens = i32::try_from(s.matches(open).count()).unwrap_or(i32::MAX);
    let closes = i32::try_from(s.matches(close).count()).unwrap_or(i32::MAX);
    opens - closes
}

/// State for [`leading_use_statements`]'s single forward pass.
///
/// Only one is ever active — folding "which multi-line thing are we
/// mid-way through" into one enum makes the exclusion structural.
enum Scan {
    /// Skipping blanks/`//` comments/a leading `#![...]` attribute, or just
    /// finished a `use` statement and back to looking for the next one.
    Leading,
    /// Inside a (possibly multi-line) `#![...]` attribute; tracks `[`/`]`
    /// depth.
    InAttr(i32),
    /// Accumulating a (possibly multi-line) `use {...}` statement; tracks
    /// `{`/`}` depth.
    InUse {
        line: usize,
        acc: String,
        depth: i32,
    },
}

/// Collects every leading `use` statement, joining ones that span multiple
/// lines, up to the first line that isn't a `use`, blank, comment, or `#![`.
fn leading_use_statements(lines: &[&str], workspace_crates: &HashSet<String>) -> Vec<UseStatement> {
    let mut statements = Vec::new();
    let mut scan = Scan::Leading;

    for (idx, raw_line) in lines.iter().enumerate() {
        let line_no = idx + 1;
        let trimmed = raw_line.trim();

        scan = match scan {
            Scan::InAttr(depth) => {
                let depth = depth + delim_delta(trimmed, '[', ']');
                if depth > 0 {
                    Scan::InAttr(depth)
                } else {
                    Scan::Leading
                }
            }

            Scan::InUse {
                line,
                mut acc,
                depth,
            } => {
                acc.push(' ');
                acc.push_str(trimmed);
                let depth = depth + delim_delta(trimmed, '{', '}');
                if depth > 0 {
                    Scan::InUse { line, acc, depth }
                } else {
                    statements.extend(classify(line, &acc, workspace_crates));
                    Scan::Leading
                }
            }

            Scan::Leading if trimmed.is_empty() || trimmed.starts_with("//") => Scan::Leading,

            // Only before any use statement has been seen — a `#![...]`
            // between real `use` items wouldn't be valid Rust anyway.
            Scan::Leading if statements.is_empty() && trimmed.starts_with("#!") => {
                Scan::InAttr(delim_delta(trimmed, '[', ']'))
            }

            Scan::Leading => {
                let vis_stripped = strip_visibility(trimmed);
                if !vis_stripped.starts_with("use ") {
                    break;
                }
                let depth = delim_delta(trimmed, '{', '}');
                if depth > 0 {
                    Scan::InUse {
                        line: line_no,
                        acc: trimmed.to_string(),
                        depth,
                    }
                } else {
                    statements.extend(classify(line_no, trimmed, workspace_crates));
                    Scan::Leading
                }
            }
        };
    }

    statements
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;

    use super::check;

    fn crates(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    fn file() -> &'static Path {
        Path::new("test.rs")
    }

    #[test]
    fn well_grouped_file_has_no_violations() {
        let source = "
use std::collections::HashMap;

use gix_hash::ObjectId;
use tokio::io::BufReader;

use crate::Error;
use crate::pack::PackReader;
";
        assert!(check(source, &crates(&[]), file()).is_empty());
    }

    #[test]
    fn workspace_local_group_sits_between_external_and_crate_local() {
        let source = "
use std::collections::HashMap;

use tokio::io::BufReader;

use enroute_git_core::hash_loose;
use enroute_git_retrieve::RepoMetadata;

use crate::Error;
";
        assert!(
            check(
                source,
                &crates(&["enroute_git_core", "enroute_git_retrieve"]),
                file()
            )
            .is_empty()
        );
    }

    #[test]
    fn workspace_local_before_external_is_flagged() {
        let source = "
use enroute_git_core::hash_loose;
use tokio::io::BufReader;
";
        let violations = check(source, &crates(&["enroute_git_core"]), file());
        assert!(
            violations
                .iter()
                .any(|v| v.message.contains("tokio::io::BufReader")
                    && v.message.contains("out of order")),
            "expected an out-of-order violation, got: {:?}",
            violations.iter().map(|v| &v.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn workspace_local_after_crate_local_is_flagged() {
        let source = "
use crate::Error;
use enroute_git_core::hash_loose;
use enroute_git_retrieve::RepoMetadata;
";
        let violations = check(
            source,
            &crates(&["enroute_git_core", "enroute_git_retrieve"]),
            file(),
        );
        assert!(
            violations
                .iter()
                .any(|v| v.message.contains("enroute_git_core::hash_loose")
                    && v.message.contains("out of order")),
            "expected an out-of-order violation, got: {:?}",
            violations.iter().map(|v| &v.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn unsorted_within_group_is_flagged() {
        let source = "
use enroute_git_store::Store;
use enroute_git_core::hash_loose;
";
        let violations = check(
            source,
            &crates(&["enroute_git_store", "enroute_git_core"]),
            file(),
        );
        assert!(
            violations
                .iter()
                .any(|v| v.message.contains("enroute_git_core::hash_loose")
                    && v.message.contains("sorted before")),
            "expected a sort-order violation, got: {:?}",
            violations.iter().map(|v| &v.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn multi_line_and_visibility_variants_are_handled() {
        let source = "
use std::collections::HashSet;

use bytes::{Bytes, BytesMut};

pub(crate) use delta::apply_delta;
pub use crate::{
    Error,
    pack::PackReader,
};
";
        assert!(check(source, &crates(&[]), file()).is_empty());
    }

    #[test]
    fn leading_inner_attribute_is_skipped() {
        let source = "\
#![allow(
    clippy::print_stdout,
    reason = \"multi-line attribute before the use block\"
)]

use std::collections::HashMap;

use crate::Error;
";
        assert!(check(source, &crates(&[]), file()).is_empty());
    }
}
