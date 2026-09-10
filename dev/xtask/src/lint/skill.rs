//! Holds what the skill ships to what this repository holds.
//!
//! `npx skills add` installs a skill directory and nothing else of the
//! repository, so the skill carries its own copy of the contract and of the
//! local stack. Copies drift, and these are the ones nothing here builds or
//! runs, so nothing but this would notice.
//!
//! # What is checked how
//!
//! Byte-identical, except the two `compose.yaml`: one builds from source and
//! one runs the published image, so only their services are compared.
//!
//! # The file that names the skill
//!
//! `SKILL.md`'s frontmatter is parsed the way an installer parses it: a block
//! that does not read back is a skill nothing installs at all.

use std::path::{Path, PathBuf};

use walkdir::WalkDir;
use yaml_rust2::{Yaml, YamlLoader};

/// The skill directory, relative to the workspace root.
const SKILL: &str = "skills/build-on-enroute";

/// The file that names the skill to an installer.
const MANIFEST: &str = "SKILL.md";

/// What its frontmatter must name.
///
/// An installer holding neither has nothing to list the skill by, and skips
/// the whole of it.
const REQUIRED: &[&str] = &["name", "description"];

/// The published image, which the skill's stack starts and this repository's
/// own does not.
const IMAGE: &str = "ghcr.io/enroute-sh/enroute";

/// Trees the skill mirrors whole, as `(here, inside the skill)`.
const TREES: &[(&str, &str)] = &[("proto/enroute", "proto/enroute")];

/// Single files the skill mirrors, as `(here, inside the skill)`.
const FILES: &[(&str, &str)] = &[
    ("dev/config/enroute.toml", "stack/config/enroute.toml"),
    ("dev/config/tenants.toml", "stack/config/tenants.toml"),
];

/// The two stack definitions, compared by what they declare rather than byte
/// for byte.
const COMPOSE: (&str, &str) = ("compose.yaml", "stack/compose.yaml");

use super::Violation;

pub(crate) fn check(root: &Path) -> Vec<Violation> {
    let mut violations = Vec::new();

    for (here, there) in TREES {
        violations.extend(tree(root, here, there));
    }
    for (here, there) in FILES {
        violations.extend(file(root, here, there));
    }
    violations.extend(compose(root));
    violations.extend(pin(root));
    violations.extend(frontmatter(root));

    violations
}

/// The frontmatter of `SKILL.md`, read by a YAML parser and not by eye.
///
/// An installer parses this before anything else the skill ships, so a block
/// that does not read back is no skill at all.
fn frontmatter(root: &Path) -> Vec<Violation> {
    let file = root.join(SKILL).join(MANIFEST);
    let Ok(text) = std::fs::read_to_string(&file) else {
        return vec![Violation {
            file,
            line: None,
            message: format!("the skill ships no {MANIFEST}, so an installer finds no skill"),
        }];
    };

    findings(&text)
        .into_iter()
        .map(|(line, message)| Violation {
            file: file.clone(),
            line,
            message,
        })
        .collect()
}

/// Everything an installer would refuse this `SKILL.md` for, as `(line, what)`.
fn findings(text: &str) -> Vec<(Option<usize>, String)> {
    let block = match block(text) {
        Ok(block) => block,
        Err(message) => return vec![(Some(1), message.to_string())],
    };

    let documents = match YamlLoader::load_from_str(&block) {
        Ok(documents) => documents,
        // The reason alone: the parser numbers the block it was handed, and
        // this finding is already against the line in the file.
        Err(error) => {
            let line = error.marker().line() + 1;
            return vec![(
                Some(line),
                format!("the frontmatter does not parse: {}", error.info()),
            )];
        }
    };

    let [Yaml::Hash(mapping)] = documents.as_slice() else {
        return vec![(
            None,
            "the frontmatter is not one mapping of keys to values".to_string(),
        )];
    };

    let mut findings = Vec::new();
    for key in REQUIRED {
        match mapping.get(&Yaml::String((*key).to_string())) {
            Some(Yaml::String(value)) if !value.trim().is_empty() => {}
            Some(_) => findings.push((
                line_of(text, key),
                format!("`{key}` is not a string an installer can show"),
            )),
            None => findings.push((None, format!("names no `{key}`, which an installer needs"))),
        }
    }
    findings
}

/// What sits between the opening `---` and the one that closes it.
fn block(text: &str) -> Result<String, &'static str> {
    let Some(rest) = text.strip_prefix("---\n") else {
        return Err("opens with no `---`, so an installer reads no frontmatter");
    };

    let mut block = String::new();
    for line in rest.lines() {
        if line.trim_end() == "---" {
            return Ok(block);
        }
        block.push_str(line);
        block.push('\n');
    }
    Err("opens frontmatter that no closing `---` ends")
}

/// The line a key sits on, for a finding that points at it.
fn line_of(text: &str, key: &str) -> Option<usize> {
    text.lines()
        .position(|line| line.starts_with(&format!("{key}:")))
        .map(|index| index + 1)
}

/// The image the skill's stack starts, which must be this release.
///
/// A skill ships beside one release and its proto describes that Enroute, so
/// pinning is what keeps an old skill correct instead of silently newer.
fn pin(root: &Path) -> Vec<Violation> {
    let (_, there) = COMPOSE;
    let copy = root.join(SKILL).join(there);
    let (Some(version), Ok(text)) = (workspace_version(root), std::fs::read_to_string(&copy))
    else {
        return Vec::new();
    };

    match pinned(&text) {
        Some(pinned) if pinned == version => Vec::new(),
        Some(pinned) => vec![Violation {
            file: copy,
            line: None,
            message: format!("starts Enroute {pinned}; this release is {version}"),
        }],
        None => vec![Violation {
            file: copy,
            line: None,
            message: format!("names no Enroute image: expected `{IMAGE}:{version}`"),
        }],
    }
}

/// `version` from `[workspace.package]`.
fn workspace_version(root: &Path) -> Option<String> {
    let text = std::fs::read_to_string(root.join("Cargo.toml")).ok()?;
    // `toml::from_str` and not `str::parse`, which reads one TOML value rather
    // than a document and so fails on every manifest.
    let manifest: toml::Value = toml::from_str(&text).ok()?;
    let version = manifest
        .get("workspace")?
        .get("package")?
        .get("version")?
        .as_str()?;
    Some(version.to_string())
}

/// The tag the stack starts Enroute from.
///
/// Whatever follows the image name, so a moving tag and a variable are both
/// read rather than skipped: each is a way for the pin to stop being one.
fn pinned(text: &str) -> Option<String> {
    text.lines()
        .find_map(|line| line.trim().strip_prefix(&format!("image: {IMAGE}:")))
        .map(|tag| tag.trim().to_string())
}

/// Every `.proto` under both, matched in both directions.
fn tree(root: &Path, here: &str, there: &str) -> Vec<Violation> {
    let source = root.join(here);
    let copy = root.join(SKILL).join(there);
    if !source.exists() {
        return Vec::new();
    }
    // Gone wholesale is the loudest way this drifts, and comparing nothing to
    // nothing is how it would go unsaid.
    if !copy.exists() {
        return vec![Violation {
            file: copy,
            line: None,
            message: format!("the skill ships no copy of {here} at all"),
        }];
    }

    let mut violations = Vec::new();
    for relative in protos(&source) {
        let theirs = copy.join(&relative);
        let Ok(ours) = std::fs::read(source.join(&relative)) else {
            continue;
        };
        match std::fs::read(&theirs) {
            Err(_) => violations.push(Violation {
                file: theirs,
                line: None,
                message: format!(
                    "the skill ships no copy of {here}/{}: cp -R {here} {SKILL}/{there}/..",
                    relative.display()
                ),
            }),
            Ok(found) if found != ours => violations.push(stale(&theirs, here, &relative)),
            Ok(_) => {}
        }
    }

    // The other direction, so dropping a file from the contract does not leave
    // the skill shipping one the server has never heard of.
    for relative in protos(&copy) {
        if !source.join(&relative).exists() {
            violations.push(Violation {
                file: copy.join(&relative),
                line: None,
                message: format!("{here}/{} is gone; delete this copy", relative.display()),
            });
        }
    }

    violations
}

/// One mirrored file, which must match byte for byte.
///
/// Gone on either side is a finding too: a pair that quietly stops being
/// compared is how the skill ships a file the repository has deleted.
fn file(root: &Path, here: &str, there: &str) -> Vec<Violation> {
    let copy = root.join(SKILL).join(there);
    let ours = std::fs::read(root.join(here));
    let theirs = std::fs::read(&copy);

    match (ours, theirs) {
        (Ok(ours), Ok(theirs)) if ours == theirs => Vec::new(),
        (Ok(_), Ok(_)) => vec![Violation {
            file: copy,
            line: None,
            message: format!("differs from {here}: the skill's stack would behave differently"),
        }],
        (Err(_), Ok(_)) => vec![Violation {
            file: copy,
            line: None,
            message: format!("{here} is gone; delete this copy or stop mirroring it"),
        }],
        (Ok(_), Err(_)) => vec![Violation {
            file: copy,
            line: None,
            message: format!("the skill ships no copy of {here}"),
        }],
        (Err(_), Err(_)) => vec![Violation {
            file: root.join(here),
            line: None,
            message: format!("neither {here} nor the skill's copy exists; drop the pair"),
        }],
    }
}

/// The services each `compose.yaml` declares, which must be the same set.
///
/// A service added, renamed or dropped here and not there is what breaks the
/// skill's instructions, and it is the part the two files must agree on.
fn compose(root: &Path) -> Vec<Violation> {
    let (here, there) = COMPOSE;
    let copy = root.join(SKILL).join(there);
    let Ok(ours) = std::fs::read_to_string(root.join(here)) else {
        return Vec::new();
    };
    let Ok(theirs) = std::fs::read_to_string(&copy) else {
        return vec![Violation {
            file: copy,
            line: None,
            message: format!("the skill ships no stack: {there} is missing beside {here}"),
        }];
    };

    let (ours, theirs) = (services(&ours), services(&theirs));
    if ours == theirs {
        return Vec::new();
    }
    vec![Violation {
        file: copy,
        line: None,
        message: format!("declares {theirs:?}; {here} declares {ours:?}"),
    }]
}

/// The keys one level under `services:`, sorted.
fn services(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        if line.starts_with("services:") {
            inside = true;
            continue;
        }
        if !inside {
            continue;
        }
        // A key back at column zero ends the block; a blank line or a comment
        // does not, since both sit between services.
        let column_zero = !line.starts_with([' ', '\t']);
        if column_zero && !line.trim().is_empty() && !line.starts_with('#') {
            break;
        }
        if let Some(rest) = line.strip_prefix("  ")
            && !rest.starts_with([' ', '\t', '#'])
            && let Some(name) = rest.strip_suffix(':')
        {
            names.push(name.to_string());
        }
    }
    names.sort();
    names
}

/// Every `.proto` under `dir`, named relative to it.
fn protos(dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(dir)
        .into_iter()
        .filter_map(Result::ok)
        .map(walkdir::DirEntry::into_path)
        .filter(|path| path.extension().is_some_and(|ext| ext == "proto"))
        .filter_map(|path| path.strip_prefix(dir).ok().map(Path::to_path_buf))
        .collect()
}

fn stale(file: &Path, here: &str, relative: &Path) -> Violation {
    Violation {
        file: file.to_path_buf(),
        line: None,
        message: format!(
            "differs from {here}/{}: an agent installing the skill would generate the wrong contract",
            relative.display()
        ),
    }
}

#[cfg(test)]
mod frontmatter_tests {
    use super::findings;

    /// A `SKILL.md` whose frontmatter holds `entries`.
    fn skill(entries: &str) -> String {
        format!("---\n{entries}---\n\n# Build on Enroute\n\nProse.\n")
    }

    #[test]
    fn a_frontmatter_that_parses_and_names_both_keys_is_a_finding_of_nothing() {
        let text = skill("name: build-on-enroute\ndescription: Start a stack.\n");
        assert_eq!(findings(&text), Vec::new());
    }

    /// The bug this rule exists for: a plain scalar ends at `: `, so the rest
    /// of the description reads as a mapping nested where none may sit.
    #[test]
    fn a_colon_in_a_plain_scalar_is_the_nested_mapping_it_reads_as() {
        let text = skill("name: build-on-enroute\ndescription: patterns: branches, CI.\n");
        let findings = findings(&text);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].0, Some(3));
        assert!(findings[0].1.starts_with("the frontmatter does not parse"));
    }

    /// The same description, quoted, which is what the colon costs.
    #[test]
    fn a_quoted_colon_is_one_value() {
        let text = skill("name: build-on-enroute\ndescription: \"patterns: branches, CI.\"\n");
        assert_eq!(findings(&text), Vec::new());
    }

    #[test]
    fn an_unclosed_quote_is_caught_the_same_way() {
        let text = skill("name: build-on-enroute\ndescription: \"branches, CI.\n");
        let findings = findings(&text);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].1.starts_with("the frontmatter does not parse"));
    }

    #[test]
    fn a_file_that_opens_no_frontmatter_names_nothing() {
        let findings = findings("# Build on Enroute\n\nProse.\n");
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].1.contains("opens with no `---`"));
    }

    /// An unclosed block swallows the prose below it, which parses or does not
    /// depending on what the prose holds. Either way it is not frontmatter.
    #[test]
    fn a_frontmatter_no_second_marker_closes_is_not_one() {
        let findings = findings("---\nname: build-on-enroute\n\n# Build on Enroute\n");
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].1.contains("no closing `---`"));
    }

    #[test]
    fn a_key_an_installer_needs_is_reported_by_name() {
        let findings = findings(&skill("name: build-on-enroute\n"));
        assert_eq!(
            findings,
            vec![(
                None,
                "names no `description`, which an installer needs".to_string()
            )]
        );
    }

    /// Present but empty is the same to whoever lists the skill, and a parser
    /// has no complaint about it.
    #[test]
    fn a_key_that_holds_nothing_is_reported_against_its_line() {
        let text = skill("name: build-on-enroute\ndescription: \"\"\n");
        let findings = findings(&text);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].0, Some(3));
    }

    #[test]
    fn a_key_that_holds_a_list_is_no_description() {
        let text = skill("name: build-on-enroute\ndescription: [a, b]\n");
        let findings = findings(&text);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert!(findings[0].1.contains("not a string"));
    }
}

#[cfg(test)]
mod tests {
    use super::services;

    #[test]
    fn it_reads_the_service_keys_and_nothing_below_them() {
        let yaml = "\
services:
  postgres:
    image: postgres:17-alpine
    ports:
      - \"5433:5432\"

  # A comment between services does not end the block.
  enroute:
    image: enroute:local

volumes:
  pgdata:
";
        assert_eq!(services(yaml), vec!["enroute", "postgres"]);
    }

    #[test]
    fn a_file_with_no_services_declares_none() {
        assert_eq!(services("volumes:\n  pgdata:\n"), Vec::<String>::new());
    }

    #[test]
    fn it_reads_the_version_the_stack_starts() {
        let yaml = "  enroute:\n    image: ghcr.io/enroute-sh/enroute:0.1.0-alpha.1\n";
        assert_eq!(super::pinned(yaml).as_deref(), Some("0.1.0-alpha.1"));
    }

    /// A moving tag is the drift this rule exists to catch, so it has to be
    /// read rather than skipped.
    #[test]
    fn a_channel_tag_reads_as_the_pin_it_is_not() {
        let yaml = "    image: ghcr.io/enroute-sh/enroute:alpha\n";
        assert_eq!(super::pinned(yaml).as_deref(), Some("alpha"));
    }

    /// So does a variable: it is a pin that whoever runs the stack decides.
    #[test]
    fn a_variable_reads_as_the_pin_it_is_not() {
        let yaml = "    image: ghcr.io/enroute-sh/enroute:${ENROUTE_VERSION:-alpha}\n";
        assert_eq!(
            super::pinned(yaml).as_deref(),
            Some("${ENROUTE_VERSION:-alpha}")
        );
    }

    #[test]
    fn a_stack_starting_some_other_image_names_no_version() {
        assert_eq!(super::pinned("    image: enroute:local\n"), None);
    }
}
