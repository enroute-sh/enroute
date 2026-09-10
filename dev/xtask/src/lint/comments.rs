//! Checks a doc comment's shape, not its length: one summary sentence, an
//! optional short body, then whatever sections and links the item needs.
//!
//! Length is a symptom — the defect is a comment that buries its point in
//! the middle of a paragraph, and a one-sentence summary has nowhere to
//! bury it. `missing_docs` is denied workspace-wide, so every public item
//! carries one of these, which is what makes a ban impossible.

use std::path::Path;

use super::Violation;

/// Lines a summary paragraph may use.
///
/// Two, not one: rustfmt does not wrap comments, so one line would really
/// be a character budget a rename could break.
const SUMMARY_MAX_LINES: usize = 2;

/// Lines the optional body paragraph may use, and the cap on a `#` section's
/// content for the same reason.
const BODY_MAX_LINES: usize = 2;

/// Lines a whole `//!` module doc may use.
///
/// A module doc orients rather than describes one item, so it earns more
/// room — bounded, since the 44-line one that prompted this had stopped.
const MODULE_MAX_LINES: usize = 25;

/// Lines an unbroken run of `//` comments may use.
const INLINE_MAX_LINES: usize = 4;

/// Word forms whose trailing period ends an abbreviation, not a sentence.
const ABBREVIATIONS: [&str; 6] = ["e.g", "i.e", "etc", "cf", "vs", "approx"];

/// Paths kept verbatim from a third party, with license headers not ours to shorten.
const VENDORED: [&str; 4] = ["ubc_check.rs", "scalar.rs", "hw.rs", "simd.rs"];

/// Which marker opened a comment block, since the three answer to different
/// rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Inline,
    Item,
    Module,
}

/// One unbroken run of comment lines, with its marker stripped.
struct Block {
    kind: Kind,
    line: usize,
    content: Vec<String>,
}

/// A run of non-blank lines inside a block, classified by what it is doing
/// there — the distinction the shape rule is written in terms of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Para {
    Prose,
    Heading,
    Links,
}

/// Returns one violation per doc comment in `source` whose shape breaks.
///
/// A multi-sentence or over-long summary, a body past [`BODY_MAX_LINES`], a
/// second body paragraph, or an inline run past [`INLINE_MAX_LINES`].
pub(crate) fn check(source: &str, file: &Path) -> Vec<Violation> {
    if is_vendored(file) {
        return Vec::new();
    }

    let violation = |line: usize, message: String| Violation {
        file: file.to_path_buf(),
        line: Some(line),
        message,
    };

    let mut violations = Vec::new();

    for block in blocks(source) {
        match block.kind {
            Kind::Inline => {
                let len = block.content.len();
                if len > INLINE_MAX_LINES {
                    violations.push(violation(
                        block.line,
                        format!(
                            "comment runs {len} lines; at most {INLINE_MAX_LINES} — cut it to \
                             the reason, or move what is left to the item's doc comment"
                        ),
                    ));
                }
            }
            Kind::Item | Kind::Module => {
                violations.extend(check_doc(&block, &violation));
            }
        }
    }

    violations
}

/// The shape rule itself, over one `///` or `//!` block.
fn check_doc(block: &Block, violation: &impl Fn(usize, String) -> Violation) -> Vec<Violation> {
    let mut violations = Vec::new();
    let paragraphs = paragraphs(&block.content);

    if block.kind == Kind::Module && block.content.len() > MODULE_MAX_LINES {
        violations.push(violation(
            block.line,
            format!(
                "module doc runs {} lines; at most {MODULE_MAX_LINES} — a module doc orients, \
                 it does not narrate",
                block.content.len()
            ),
        ));
    }

    let Some((kind, first, offset)) = paragraphs.first() else {
        return violations;
    };

    // A block that opens on a heading is a section-only doc (`# Safety` on a
    // re-export, say). It has no summary to hold to a shape.
    if *kind != Para::Prose {
        return violations;
    }

    let at = block.line.saturating_add(*offset);

    if first.len() > SUMMARY_MAX_LINES {
        violations.push(violation(
            at,
            format!(
                "summary runs {} lines; at most {SUMMARY_MAX_LINES} — say what the item is, \
                 and move the reason to a paragraph below",
                first.len()
            ),
        ));
    } else {
        let sentences = sentence_count(&first.join(" "));
        if sentences > 1 {
            violations.push(violation(
                at,
                format!(
                    "summary is {sentences} sentences; it must be one — keep the first and \
                     move the rest to a paragraph below"
                ),
            ));
        }
    }

    let mut bodies = 0usize;
    let mut in_sections = false;

    for (kind, lines, offset) in paragraphs.iter().skip(1) {
        let at = block.line.saturating_add(*offset);
        match kind {
            Para::Heading => in_sections = true,
            Para::Links => {}
            Para::Prose if in_sections => {
                if lines.len() > BODY_MAX_LINES {
                    violations.push(violation(
                        at,
                        format!(
                            "section runs {} lines; at most {BODY_MAX_LINES}",
                            lines.len()
                        ),
                    ));
                }
            }
            Para::Prose => {
                bodies = bodies.saturating_add(1);
                if bodies > 1 {
                    violations.push(violation(
                        at,
                        "second body paragraph; a doc comment gets one — the rest belongs in \
                         the module doc or in prose next to the code"
                            .to_string(),
                    ));
                } else if lines.len() > BODY_MAX_LINES && block.kind == Kind::Item {
                    violations.push(violation(
                        at,
                        format!(
                            "body runs {} lines; at most {BODY_MAX_LINES} — keep the reason \
                             and cut the rest",
                            lines.len()
                        ),
                    ));
                }
            }
        }
    }

    violations
}

/// Splits a block's content on blank lines, tagging each run with what it is
/// and where it starts relative to the block.
fn paragraphs(content: &[String]) -> Vec<(Para, Vec<String>, usize)> {
    let mut out: Vec<(Para, Vec<String>, usize)> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut start = 0usize;

    for (index, line) in content.iter().enumerate() {
        if line.is_empty() {
            if !current.is_empty() {
                out.push((classify(&current), std::mem::take(&mut current), start));
            }
            continue;
        }
        if current.is_empty() {
            start = index;
        }
        current.push(line.clone());
    }

    if !current.is_empty() {
        out.push((classify(&current), current, start));
    }

    out
}

/// What a paragraph is doing: a `#` heading, a reference list (link
/// definitions or `See ...`), or prose the shape rule counts.
fn classify(lines: &[String]) -> Para {
    let opens_with = |prefix: &str| lines.first().is_some_and(|line| line.starts_with(prefix));

    if opens_with("# ") {
        return Para::Heading;
    }
    if opens_with("See ") || opens_with("see ") {
        return Para::Links;
    }
    if lines
        .iter()
        .all(|line| line.starts_with('[') && line.contains("]:"))
    {
        return Para::Links;
    }
    Para::Prose
}

/// Counts sentences by finding a period closing a word, followed by
/// whitespace and a capital — deliberately blunt, not English parsing.
fn sentence_count(text: &str) -> usize {
    let chars: Vec<char> = text.chars().collect();
    let mut sentences = 1usize;
    let mut word = String::new();

    for (index, ch) in chars.iter().enumerate() {
        if ch.is_whitespace() {
            word.clear();
            continue;
        }
        if *ch != '.' {
            word.push(*ch);
            continue;
        }

        let closes_word = word
            .chars()
            .last()
            .is_some_and(|c| c.is_alphanumeric() || matches!(c, ')' | ']' | '`' | '"'));
        let abbreviated = ABBREVIATIONS.contains(&word.as_str());
        word.push('.');

        if !closes_word || abbreviated {
            continue;
        }

        let mut next = index.saturating_add(1);
        // A sentence ends at whitespace; `main.rs` and `1.2` do not.
        if !chars.get(next).is_some_and(|c| c.is_whitespace()) {
            continue;
        }
        while chars.get(next).is_some_and(|c| c.is_whitespace()) {
            next = next.saturating_add(1);
        }
        if chars
            .get(next)
            .is_some_and(|c| c.is_uppercase() || matches!(c, '`' | '[' | '*'))
        {
            sentences = sentences.saturating_add(1);
        }
    }

    sentences
}

/// Groups consecutive comment lines, dropping the marker.
///
/// A blank line ends a `//` run but is kept as a paragraph break for
/// `///`/`//!`, which is why the two are read apart here.
fn blocks(source: &str) -> Vec<Block> {
    let mut out: Vec<Block> = Vec::new();
    let mut current: Option<Block> = None;
    let mut in_string = false;

    for (index, raw) in source.lines().enumerate() {
        let line = index.saturating_add(1);

        // A multi-line `"..."` literal (this lint's own tests hold several)
        // can contain lines that look like comments; skip until it closes.
        if in_string {
            if quote_count(raw) % 2 == 1 {
                in_string = false;
            }
            continue;
        }

        let trimmed = raw.trim_start();
        let parsed = if let Some(rest) = trimmed.strip_prefix("//!") {
            Some((Kind::Module, rest))
        } else if trimmed.starts_with("////") {
            // Four or more slashes is a plain comment to rustdoc, and usually a
            // divider rule.
            Some((Kind::Inline, ""))
        } else if let Some(rest) = trimmed.strip_prefix("///") {
            Some((Kind::Item, rest))
        } else {
            trimmed.strip_prefix("//").map(|rest| (Kind::Inline, rest))
        };

        let Some((kind, rest)) = parsed else {
            if let Some(block) = current.take() {
                out.push(block);
            }
            if quote_count(raw) % 2 == 1 {
                in_string = true;
            }
            continue;
        };

        let content = rest.trim().to_string();
        match current.as_mut() {
            Some(block) if block.kind == kind => block.content.push(content),
            _ => {
                if let Some(block) = current.take() {
                    out.push(block);
                }
                current = Some(Block {
                    kind,
                    line,
                    content: vec![content],
                });
            }
        }
    }

    if let Some(block) = current.take() {
        out.push(block);
    }

    out
}

/// `"` bytes that could open or close a string literal — excludes a char
/// literal holding one, whose quote is not one.
fn quote_count(line: &str) -> usize {
    let bytes = line.as_bytes();
    (0..bytes.len())
        .filter(|&i| {
            bytes.get(i) == Some(&b'"')
                && !(i > 0 && bytes.get(i - 1) == Some(&b'\'') && bytes.get(i + 1) == Some(&b'\''))
        })
        .count()
}

/// True for a file kept verbatim from a third party.
fn is_vendored(file: &Path) -> bool {
    let Some(name) = file.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    file.components().any(|part| part.as_os_str() == "hash") && VENDORED.contains(&name)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{check, sentence_count};

    fn file() -> &'static Path {
        Path::new("test.rs")
    }

    fn messages(source: &str) -> Vec<String> {
        check(source, file())
            .into_iter()
            .map(|violation| violation.message)
            .collect()
    }

    #[test]
    fn well_shaped_doc_has_no_violations() {
        let source = "
/// Resolves a wire repository id to an engine one for the caller's tenant.
///
/// An application is a customer's own code, so its answer is not taken on
/// trust.
///
/// # Errors
///
/// Returns `NotFound` when the id is not the tenant's.
fn resolve() {}
";
        assert!(messages(source).is_empty());
    }

    #[test]
    fn two_sentence_summary_is_reported() {
        let source = "
/// Resolves an id. It also checks the tenant.
fn resolve() {}
";
        let found = messages(source);
        assert_eq!(found.len(), 1);
        assert!(found.first().is_some_and(|m| m.contains("2 sentences")));
    }

    #[test]
    fn summary_over_two_lines_is_reported() {
        let source = "
/// Resolves a wire repository id to an engine one for the caller's tenant,
/// refusing an id the tenant does not own, because an application is a
/// customer's own code and its answer is not taken on trust in this path.
fn resolve() {}
";
        let found = messages(source);
        assert_eq!(found.len(), 1);
        assert!(
            found
                .first()
                .is_some_and(|m| m.contains("summary runs 3 lines"))
        );
    }

    #[test]
    fn long_body_is_reported() {
        let source = "
/// Resolves an id.
///
/// One line of reason.
/// Two lines of reason.
/// Three lines of reason.
fn resolve() {}
";
        let found = messages(source);
        assert_eq!(found.len(), 1);
        assert!(
            found
                .first()
                .is_some_and(|m| m.contains("body runs 3 lines"))
        );
    }

    #[test]
    fn second_body_paragraph_is_reported() {
        let source = "
/// Resolves an id.
///
/// One reason.
///
/// Another reason entirely.
fn resolve() {}
";
        let found = messages(source);
        assert_eq!(found.len(), 1);
        assert!(
            found
                .first()
                .is_some_and(|m| m.contains("second body paragraph"))
        );
    }

    #[test]
    fn long_inline_run_is_reported() {
        let source = "
fn f() {
    // one
    // two
    // three
    // four
    // five
    g();
}
";
        let found = messages(source);
        assert_eq!(found.len(), 1);
        assert!(found.first().is_some_and(|m| m.contains("runs 5 lines")));
    }

    #[test]
    fn blank_line_separates_inline_runs() {
        let source = "
fn f() {
    // one
    // two
    // three

    // four
    // five
    // six
    g();
}
";
        assert!(messages(source).is_empty());
    }

    #[test]
    fn references_paragraph_is_not_a_body() {
        let source = "
/// Resolves an id.
///
/// The reason it works this way.
///
/// See [`Other::thing`] for the write-path twin.
fn resolve() {}
";
        assert!(messages(source).is_empty());
    }

    #[test]
    fn link_definitions_are_not_a_body() {
        let source = "
/// Resolves an id.
///
/// The reason it works this way.
///
/// [`Other::thing`]: crate::other::Other::thing
/// [`Third::thing`]: crate::third::Third::thing
fn resolve() {}
";
        assert!(messages(source).is_empty());
    }

    #[test]
    fn long_section_is_reported() {
        let source = "
/// Resolves an id.
///
/// # Errors
///
/// One condition.
/// Two conditions.
/// Three conditions.
fn resolve() {}
";
        let found = messages(source);
        assert_eq!(found.len(), 1);
        assert!(
            found
                .first()
                .is_some_and(|m| m.contains("section runs 3 lines"))
        );
    }

    #[test]
    fn module_doc_may_have_a_longer_body() {
        let source = "
//! Orients a reader in this file.
//!
//! Four lines of context that an item doc would not be allowed to carry,
//! because a module doc is the one place a file explains itself before
//! anybody reads a signature, and that is worth more room than a single
//! item's summary ever needs.
";
        assert!(messages(source).is_empty());
    }

    #[test]
    fn module_doc_past_the_cap_is_reported() {
        let mut source = String::from("//! Orients a reader in this file.\n//!\n");
        for index in 0..30 {
            let line = format!("//! line {index} of narration here.\n");
            source.push_str(&line);
        }
        let found = messages(&source);
        assert!(found.iter().any(|m| m.contains("module doc runs")));
    }

    #[test]
    fn vendored_files_are_skipped() {
        let source = "
// A license header that runs
// well past the inline cap and
// is not ours to shorten, so it
// must not be reported at all
// by this rule.
";
        let vendored = Path::new("crates/git/hash/src/ubc_check.rs");
        assert!(check(source, vendored).is_empty());
        assert!(!check(source, file()).is_empty());
    }

    #[test]
    fn abbreviations_do_not_end_a_sentence() {
        assert_eq!(
            sentence_count("Holds a name, e.g. `Foo`, for the caller."),
            1
        );
        assert_eq!(sentence_count("Reads `main.rs` from the root."), 1);
        assert_eq!(sentence_count("Holds a name. Checks the tenant."), 2);
        assert_eq!(sentence_count("Covers RFC 9421 4.2 and nothing else."), 1);
    }
}
