//! Finds `pub` items that nothing in the workspace uses.
//!
//! `rustc`'s dead-code lint stops at the crate boundary, assuming a `pub`
//! item has a caller it cannot see; nothing here is published, so the whole
//! set of callers is in this repository. The analysis is syntactic — parses
//! each file with tree-sitter, records every `pub` declaration and every
//! identifier mentioned outside one, and calls a name dead if nothing
//! mentions it. There is no name resolution, so two items sharing a name
//! keep each other alive: it under-reports dead code and never reports live
//! code as dead, which is the trade that makes it usable as a gate. A `use`
//! is not a mention (`unused_imports` covers that, and counting it would
//! keep a re-export alive forever), and neither is the name in its own
//! declaration — but an attribute argument and a format-string capture are,
//! since real references hide there. Of its two verdicts, dead is reported
//! outright; over-visible is checked against the compiler by [`settle`],
//! which withdraws it wherever `pub(crate)` would not build. A third
//! verdict — "only `#[cfg(test)]` code names it" — is deliberately absent:
//! nothing syntactic tells that apart from a shipped invariant another
//! crate's tests check.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};

use tree_sitter::{Node, Parser};

use super::Violation;
use super::workspace::CrateRoot;

/// A file belonging to no workspace member.
///
/// Its mentions still count, the conservative direction, but it declares nothing.
const OUTSIDE: usize = usize::MAX;

/// What the workspace does with a name.
#[derive(Default)]
struct Mentions {
    /// Every crate that names it, tests included — a test in another crate
    /// needs an item public exactly as shipped code does.
    crates: HashSet<usize>,
    /// `pub` items whose signature names it, e.g. the `E` in `pub fn f() ->
    /// Result<T, E>` — reachable via `f` even if no file names `E` directly.
    ///
    /// Names, not a flag, since it only stays reachable while `f` stays
    /// `pub` — see [`settle`].
    escapes_through: HashSet<String>,
    /// A `pub use` carries the name out of the crate that declares it, so it
    /// has to stay `pub` however few crates write it.
    re_exported: bool,
}

struct Declaration {
    name: String,
    kind: &'static str,
    krate: usize,
    /// The name to print for `krate`.
    ///
    /// Absent for a sibling target or an unowned file, which keeps them out
    /// of the narrowing verdict — `pub(crate)` names nothing there.
    krate_name: Option<String>,
    file: PathBuf,
    line: usize,
    /// Whether the item writes its own `pub`, so narrowing it is possible.
    ///
    /// A variant or trait method takes its holder's visibility and cannot.
    own_visibility: bool,
}

pub(crate) fn check(sources: &[(PathBuf, String)], crates: &[CrateRoot]) -> Vec<Violation> {
    let mut parser = Parser::new();
    if parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .is_err()
    {
        return vec![Violation {
            file: PathBuf::from("<tree-sitter>"),
            line: None,
            message: "failed to load the Rust grammar".to_string(),
        }];
    }

    let mut declarations: Vec<Declaration> = Vec::new();
    let mut mentions: HashMap<String, Mentions> = HashMap::new();

    // A member's binaries, examples and integration tests each compile as
    // their own crate against the member's library. They get their own id,
    // so that "only its own crate names it" cannot suggest `pub(crate)` for
    // an item a sibling target needs.
    let mut targets: HashMap<&Path, usize> = HashMap::new();
    let libraries: HashSet<&Path> = sources
        .iter()
        .filter(|(file, _)| file.ends_with("src/lib.rs"))
        .filter_map(|(file, _)| file.parent()?.parent())
        .collect();

    for (file, source) in sources {
        let member = owning_crate(file, crates);
        let route = route(file, member, crates, &libraries);
        let krate = match route.target_root {
            Some(root) => {
                let next = crates.len() + targets.len();
                *targets.entry(root).or_insert(next)
            }
            None => member,
        };
        let Some(tree) = parser.parse(source.as_bytes(), None) else {
            continue;
        };

        let mut scan = Scan {
            source,
            file,
            krate,
            krate_name: route
                .target_root
                .is_none()
                .then(|| crates.get(member).map(|krate| krate.name.as_str()))
                .flatten(),
            declarations: Vec::new(),
            mentions: HashMap::new(),
        };
        scan.node(
            tree.root_node(),
            Context {
                in_tests: route.in_tests,
                ..Context::default()
            },
        );

        declarations.append(&mut scan.declarations);
        for (name, flags) in scan.mentions {
            // `entry` would allocate the key on every file that repeats a
            // name, and most do.
            let entry = match mentions.get_mut(name) {
                Some(entry) => entry,
                None => mentions.entry(name.to_string()).or_default(),
            };
            if flags.mentioned {
                entry.crates.insert(krate);
            }
            entry
                .escapes_through
                .extend(flags.escapes_through.into_iter().map(str::to_string));
            entry.re_exported |= flags.re_exported;
        }
    }

    let narrowable = settle(&declarations, &mentions);
    let mut violations: Vec<Violation> = declarations
        .iter()
        .filter_map(|declaration| verdict(declaration, &mentions, &narrowable))
        .collect();
    violations.sort_by(|a, b| (&a.file, a.line).cmp(&(&b.file, b.line)));
    violations
}

/// The names that can go from `pub` to `pub(crate)`: those no other crate
/// names, minus those a signature carries out of the crate anyway.
///
/// The two conditions feed each other: narrowing `f` frees `E` in `pub fn
/// f() -> E`, but if `f` must stay public, so does `E`. Repeats to a fixpoint.
fn settle(declarations: &[Declaration], mentions: &HashMap<String, Mentions>) -> HashSet<String> {
    let mut narrowable: HashSet<String> = declarations
        .iter()
        .filter(|declaration| {
            declaration.own_visibility
                && declaration.krate_name.is_some()
                && mentions.get(&declaration.name).is_some_and(|mentions| {
                    !mentions.re_exported
                        && mentions
                            .crates
                            .iter()
                            .all(|krate| *krate == declaration.krate)
                })
        })
        .map(|declaration| declaration.name.clone())
        .collect();

    loop {
        let leaked: Vec<String> = narrowable
            .iter()
            .filter(|name| {
                mentions.get(*name).is_some_and(|mentions| {
                    mentions
                        .escapes_through
                        .iter()
                        .any(|owner| owner != *name && !narrowable.contains(owner))
                })
            })
            .cloned()
            .collect();
        if leaked.is_empty() {
            return narrowable;
        }
        for name in leaked {
            narrowable.remove(&name);
        }
    }
}

fn verdict(
    declaration: &Declaration,
    mentions: &HashMap<String, Mentions>,
    narrowable: &HashSet<String>,
) -> Option<Violation> {
    let name = &declaration.name;
    let kind = declaration.kind;
    let named_by = mentions
        .get(name)
        .map_or(0, |mentions| mentions.crates.len());

    let message = if named_by == 0 {
        format!("`pub {kind} {name}` is dead: no file in the workspace names it")
    } else if narrowable.contains(name) {
        format!(
            "`pub {kind} {name}` is named only inside `{}`: make it `pub(crate)`, so that \
             rustc's dead-code lint can see it",
            declaration.krate_name.as_deref()?
        )
    } else {
        return None;
    };

    Some(Violation {
        file: declaration.file.clone(),
        line: Some(declaration.line),
        message,
    })
}

/// Cargo compiles one crate per file in `src/bin/`, `examples/`, `tests/`,
/// `benches/`, and `src/main.rs` beside a `src/lib.rs`.
#[derive(Default)]
struct Route<'a> {
    /// The file, when it roots a crate of its own rather than being a module
    /// of the member's library.
    target_root: Option<&'a Path>,
    in_tests: bool,
}

/// Which crate `file` compiles into, and whether that crate is test code.
///
/// A root is an exact directory match, since `tests/common/mod.rs` is a
/// module, not a target; test-ness is a prefix match, since it still is one.
fn route<'a>(
    file: &'a Path,
    member: usize,
    crates: &[CrateRoot],
    libraries: &HashSet<&Path>,
) -> Route<'a> {
    const TARGET_DIRS: [&str; 4] = ["src/bin", "examples", "tests", "benches"];
    let Some(dir) = crates.get(member).map(|krate| krate.dir.as_path()) else {
        return Route::default();
    };
    let roots_a_crate = file.parent().is_some_and(|parent| {
        TARGET_DIRS.iter().any(|target| parent == dir.join(target))
            || (file == dir.join("src/main.rs") && libraries.contains(dir))
    });
    Route {
        target_root: roots_a_crate.then_some(file),
        in_tests: ["tests", "benches"]
            .iter()
            .any(|target| file.starts_with(dir.join(target))),
    }
}

fn owning_crate(file: &Path, crates: &[CrateRoot]) -> usize {
    crates
        .iter()
        .enumerate()
        .filter(|(_, krate)| file.starts_with(&krate.dir))
        .max_by_key(|(_, krate)| krate.dir.as_os_str().len())
        .map_or(OUTSIDE, |(index, _)| index)
}

/// What the enclosing syntax says about the node below it.
#[derive(Default, Clone, Copy)]
struct Context<'a> {
    /// The item is in a `#[cfg(test)]` module, so anything it names is only
    /// named by tests.
    in_tests: bool,
    /// In a `pub` trait or `pub` enum body, which is what makes a trait
    /// method or enum variant public — they carry no visibility of their own.
    inherits_pub: bool,
    /// In `impl Trait for Type`: not a new item, but still a declaration,
    /// not a mention.
    in_trait_impl: bool,
    /// The `pub` item whose signature — parameters, return type, fields,
    /// never body — holds this node, riding a name mentioned here public.
    signature_of: Option<&'a str>,
}

struct Scan<'a> {
    source: &'a str,
    file: &'a Path,
    krate: usize,
    krate_name: Option<&'a str>,
    declarations: Vec<Declaration>,
    mentions: HashMap<&'a str, Flags<'a>>,
}

/// What one file does with a name, folded into [`Mentions`] once done.
///
/// A `pub use` creates an entry without setting `mentioned`, keeping a
/// re-exported but uncalled item dead.
#[derive(Default)]
struct Flags<'a> {
    mentioned: bool,
    escapes_through: HashSet<&'a str>,
    re_exported: bool,
}

impl<'a> Scan<'a> {
    fn node(&mut self, node: Node<'a>, context: Context<'a>) {
        match node.kind() {
            "use_declaration" => {
                if is_pub(node, context) {
                    self.re_export(node);
                }
                return;
            }
            "line_comment" | "block_comment" => return,
            "identifier"
            | "type_identifier"
            | "field_identifier"
            | "shorthand_field_identifier" => {
                self.mention(node.byte_range(), context);
                return;
            }
            "string_literal" | "raw_string_literal" => {
                self.format_captures(node, context);
                return;
            }
            // `#[arg(...)]` says `arg`, which names no item, but its
            // arguments can name several.
            "attribute" => {
                if let Some(arguments) = node.child_by_field_name("arguments") {
                    self.node(arguments, context);
                }
                return;
            }
            _ => {}
        }

        // What an item says about its body has to survive the list node that
        // holds the body, so a list passes the context through unchanged.
        let body_list = matches!(node.kind(), "declaration_list" | "enum_variant_list");
        let mut child_context = Context {
            in_tests: context.in_tests,
            inherits_pub: context.inherits_pub && body_list,
            in_trait_impl: context.in_trait_impl && body_list,
            // A `pub` item puts everything it names in the public surface
            // until the walk reaches a body, where the names are private
            // again. A module is the one `pub` item with no signature.
            signature_of: match node.kind() {
                "block" | "mod_item" => None,
                kind if item_kind(kind).is_some() && is_pub(node, context) => node
                    .child_by_field_name("name")
                    .map(|name| self.text(name.byte_range()))
                    .or(context.signature_of),
                _ => context.signature_of,
            },
        };
        match node.kind() {
            "mod_item" => child_context.in_tests |= self.declares_test_module(node),
            "impl_item" => {
                child_context.in_trait_impl = node.child_by_field_name("trait").is_some();
            }
            "trait_item" | "enum_item" => child_context.inherits_pub = is_pub(node, context),
            _ => {}
        }

        // `declare` is a no-op for the kinds handled above, so it runs
        // unconditionally rather than once per arm.
        let declared_name = self.declare(node, context);

        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            if Some(child.id()) != declared_name.map(|name| name.id()) {
                self.node(child, child_context);
            }
        }
    }

    /// Records `node` as a declaration when reportable, and returns its
    /// name node either way — the caller must not walk into a name.
    fn declare(&mut self, node: Node<'a>, context: Context<'a>) -> Option<Node<'a>> {
        let kind = item_kind(node.kind())?;
        let name = node.child_by_field_name("name")?;

        let reportable = !context.in_trait_impl
            && !context.in_tests
            && is_pub(node, context)
            && !self.exempt(node);
        if reportable {
            self.declarations.push(Declaration {
                name: self.text(name.byte_range()).to_string(),
                kind,
                krate: self.krate,
                krate_name: self.krate_name.map(str::to_string),
                file: self.file.to_path_buf(),
                line: name.start_position().row + 1,
                own_visibility: visibility(node).is_some(),
            });
        }

        Some(name)
    }

    /// True when an attribute above `node` says it has a user this rule
    /// cannot see.
    fn exempt(&self, node: Node<'a>) -> bool {
        const MARKERS: [&str; 8] = [
            "test",
            "bench",
            "no_mangle",
            "export_name",
            "macro_export",
            "proc_macro",
            "dead_code",
            "unused",
        ];
        self.attribute_words(node)
            .any(|word| MARKERS.contains(&word))
    }

    fn declares_test_module(&self, node: Node<'a>) -> bool {
        self.attribute_words(node).any(|word| word == "test")
    }

    /// Every identifier in the attributes directly above `node` — the
    /// attribute's own path and the identifiers among its arguments.
    ///
    /// Identifiers, not raw text: `#[expect(lint, reason = "…")]` reasons
    /// often contain the word "test", and matching text would exempt it silently.
    fn attribute_words(&self, node: Node<'a>) -> impl Iterator<Item = &'a str> {
        let mut words = Vec::new();
        let mut sibling = node.prev_sibling();
        while let Some(previous) = sibling {
            match previous.kind() {
                "attribute_item" => collect_identifiers(previous, self.source, &mut words),
                "line_comment" | "block_comment" => {}
                _ => break,
            }
            sibling = previous.prev_sibling();
        }
        words.into_iter()
    }

    /// Marks every name a `pub use` carries: not a mention, so a re-exported
    /// but uncalled item is still dead, but it pins the visibility at `pub`.
    fn re_export(&mut self, node: Node<'a>) {
        if matches!(node.kind(), "identifier" | "type_identifier") {
            self.mentions
                .entry(self.text(node.byte_range()))
                .or_default()
                .re_exported = true;
            return;
        }
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            self.re_export(child);
        }
    }

    fn text(&self, range: Range<usize>) -> &'a str {
        self.source.get(range).unwrap_or_default()
    }

    fn mention(&mut self, range: Range<usize>, context: Context<'a>) {
        let flags = self.mentions.entry(self.text(range)).or_default();
        flags.mentioned = true;
        if let Some(owner) = context.signature_of {
            flags.escapes_through.insert(owner);
        }
    }

    /// Records the names an inline format capture holds — `{name}` and
    /// `{name:spec}` — the one reference that lives inside a string.
    fn format_captures(&mut self, node: Node<'a>, context: Context<'a>) {
        let start = node.start_byte();
        let text = self.text(node.byte_range());
        let mut opens = text
            .as_bytes()
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'{')
            .map(|(offset, _)| offset)
            .peekable();

        while let Some(open) = opens.next() {
            // `{{` is an escaped brace, not the start of a capture.
            if opens.peek() == Some(&(open + 1)) {
                opens.next();
                continue;
            }
            let Some(tail) = text.get(open + 1..) else {
                continue;
            };
            let length = tail.find(['}', ':']).unwrap_or(tail.len());
            if tail.get(..length).is_some_and(is_identifier) {
                self.mention(start + open + 1..start + open + 1 + length, context);
            }
        }
    }
}

/// Every identifier below `node`, skipping string literals so that the prose
/// in an attribute's `reason = "..."` never reads as a word.
fn collect_identifiers<'a>(node: Node<'_>, source: &'a str, out: &mut Vec<&'a str>) {
    if node.kind() == "identifier" {
        out.extend(source.get(node.byte_range()));
        return;
    }
    if matches!(node.kind(), "string_literal" | "raw_string_literal") {
        return;
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_identifiers(child, source, out);
    }
}

/// Public to other crates, the only visibility `rustc` cannot reason about.
///
/// `pub(crate)` is covered by its own dead-code lint. A trait method or enum
/// variant has no visibility of its own and takes its holder's.
fn is_pub(node: Node<'_>, context: Context<'_>) -> bool {
    match visibility(node) {
        // `pub(crate)` and friends parse as a `visibility_modifier` holding
        // the restriction; bare `pub` holds only itself.
        Some(modifier) => modifier.child_count() == 1,
        None => context.inherits_pub,
    }
}

fn visibility(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .find(|child| child.kind() == "visibility_modifier")
}

fn is_identifier(text: &str) -> bool {
    !text.is_empty()
        && !text.starts_with(|first: char| first.is_ascii_digit())
        && text
            .chars()
            .all(|character| character.is_alphanumeric() || character == '_')
}

fn item_kind(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "function_item" => "fn",
        "function_signature_item" => "trait fn",
        "struct_item" => "struct",
        "enum_item" => "enum",
        "union_item" => "union",
        "trait_item" => "trait",
        "type_item" => "type",
        "associated_type" => "associated type",
        "const_item" => "const",
        "static_item" => "static",
        "enum_variant" => "variant",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn findings(files: &[(&str, &str)]) -> Vec<String> {
        let crates = vec![
            CrateRoot {
                name: "a".to_string(),
                dir: PathBuf::from("a"),
            },
            CrateRoot {
                name: "b".to_string(),
                dir: PathBuf::from("b"),
            },
        ];
        let sources: Vec<(PathBuf, String)> = files
            .iter()
            .map(|(path, source)| (PathBuf::from(path), (*source).to_string()))
            .collect();
        check(&sources, &crates)
            .into_iter()
            .map(|violation| violation.message)
            .collect()
    }

    #[test]
    fn unused_pub_item_is_dead() {
        let found = findings(&[("a/src/lib.rs", "pub fn orphan() {}")]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("`pub fn orphan` is dead"), "{found:?}");
    }

    #[test]
    fn use_of_a_name_in_another_crate_keeps_it_alive() {
        let found = findings(&[
            ("a/src/lib.rs", "pub fn used() {}"),
            ("b/src/lib.rs", "fn caller() { used() }"),
        ]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn an_import_alone_is_not_a_use() {
        let found = findings(&[
            ("a/src/lib.rs", "pub fn used() {}"),
            ("b/src/lib.rs", "use a::used;"),
        ]);
        assert!(found[0].contains("is dead"), "{found:?}");
    }

    #[test]
    fn a_re_export_alone_does_not_keep_an_item_alive() {
        let found = findings(&[
            ("a/src/lib.rs", "pub struct Thing;"),
            ("b/src/lib.rs", "pub use a::Thing;"),
        ]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("`pub struct Thing` is dead"), "{found:?}");
    }

    #[test]
    fn prose_in_an_attribute_reason_is_not_a_marker() {
        let found = findings(&[(
            "a/src/lib.rs",
            "#[expect(clippy::similar_names, reason = \"pairs are intentional in tests\")]\n\
             pub fn orphan() {}",
        )]);
        assert!(found[0].contains("is dead"), "{found:?}");
    }

    #[test]
    fn a_re_exported_name_cannot_be_narrowed() {
        let found = findings(&[(
            "a/src/lib.rs",
            "pub use inner::Thing;\nmod inner { pub struct Thing; }\nfn f(t: Thing) {}",
        )]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_test_in_another_crate_needs_it_public_too() {
        let found = findings(&[
            ("a/src/lib.rs", "pub fn helper() {}\nfn f() { helper() }"),
            (
                "b/src/lib.rs",
                "#[cfg(test)]\nmod tests { fn t() { helper() } }",
            ),
        ]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_type_a_public_signature_leaks_stays_public() {
        let found = findings(&[(
            "a/src/lib.rs",
            "pub struct Attack;\npub fn hash() -> Result<u8, Attack> { do_it() }",
        )]);
        // `hash` itself is unused, but `Attack` reaches every caller of it
        // without any of them writing the name.
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("`pub fn hash`"), "{found:?}");
    }

    #[test]
    fn a_name_used_only_at_home_asks_for_pub_crate() {
        let found = findings(&[("a/src/lib.rs", "pub fn local() {}\nfn caller() { local() }")]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("make it `pub(crate)`"), "{found:?}");
    }

    #[test]
    fn a_name_only_its_own_tests_use_asks_for_pub_crate() {
        let found = findings(&[(
            "a/src/lib.rs",
            "pub fn helper() {}\n#[cfg(test)]\nmod tests {\n  fn t() { helper() }\n}",
        )]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("make it `pub(crate)`"), "{found:?}");
    }

    #[test]
    fn restricted_visibility_is_left_to_rustc() {
        let found = findings(&[("a/src/lib.rs", "pub(crate) fn orphan() {}")]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn an_implemented_but_uncalled_trait_method_is_dead() {
        let found = findings(&[
            ("a/src/lib.rs", "pub trait Sink { fn drain(&self); }"),
            (
                "b/src/lib.rs",
                "struct S;\nimpl Sink for S { fn drain(&self) {} }\nfn f() { let _: &dyn Sink; }",
            ),
        ]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("`pub trait fn drain` is dead"),
            "{found:?}"
        );
    }

    #[test]
    fn a_called_trait_method_is_alive() {
        let found = findings(&[
            ("a/src/lib.rs", "pub trait Sink { fn drain(&self); }"),
            ("b/src/lib.rs", "fn f(s: &dyn Sink) { s.drain() }"),
        ]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_variant_takes_the_visibility_of_its_enum() {
        let found = findings(&[
            ("a/src/lib.rs", "pub enum E { Kept, Dropped }"),
            ("b/src/lib.rs", "fn f(e: E) { matches!(e, E::Kept); }"),
        ]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("`pub variant Dropped` is dead"),
            "{found:?}"
        );
    }

    #[test]
    fn comments_do_not_count_as_mentions() {
        let found = findings(&[
            ("a/src/lib.rs", "pub struct Orphan;"),
            ("b/src/lib.rs", "// Orphan\n/// [`Orphan`]\nstruct S;"),
        ]);
        assert!(found[0].contains("is dead"), "{found:?}");
    }

    #[test]
    fn an_attribute_argument_counts_as_a_mention() {
        let found = findings(&[
            ("a/src/lib.rs", "pub const LIMIT: u8 = 1;"),
            (
                "b/src/lib.rs",
                "struct Args {\n  #[arg(long, default_value_t = LIMIT)]\n  n: u8,\n}",
            ),
        ]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_format_capture_counts_as_a_mention() {
        let found = findings(&[
            ("a/src/lib.rs", "pub const DIR: &str = \".enroute\";"),
            (
                "b/src/lib.rs",
                "fn f(name: &str) { format!(\"{DIR}/{name}\"); }",
            ),
        ]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn an_escaped_brace_is_not_a_capture() {
        let found = findings(&[
            ("a/src/lib.rs", "pub const DIR: &str = \".enroute\";"),
            ("b/src/lib.rs", "fn f() { format!(\"{{DIR}}\"); }"),
        ]);
        assert!(found[0].contains("is dead"), "{found:?}");
    }

    #[test]
    fn an_allow_of_dead_code_is_respected() {
        let found = findings(&[(
            "a/src/lib.rs",
            "/// doc\n#[allow(dead_code)]\npub fn orphan() {}",
        )]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn items_declared_under_cfg_test_are_left_alone() {
        let found = findings(&[(
            "a/src/lib.rs",
            "#[cfg(test)]\nmod tests {\n  pub fn fixture() {}\n}",
        )]);
        assert!(found.is_empty(), "{found:?}");
    }
}
