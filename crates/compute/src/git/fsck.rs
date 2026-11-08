//! Git fsck's checks of commits, trees and tags, as a git server with
//! `receive.fsckObjects` runs them.
//!
//! Under that setting git's warnings are errors, so an object passes only
//! what `fsck_commit`, `fsck_tree` and `fsck_tag` (`fsck.c`) report as
//! neither. The name checks follow
//! `is_hfs_dotgit` (`utf8.c`) and `is_ntfs_dotgit` (`path.c`): names that a
//! Mac or Windows checkout reads as `.git` or `.gitmodules` let an older
//! client write into its own repository (CVE-2014-9390, CVE-2018-11235).

use anyhow::{Context, Result, bail};

use super::object::{EntryMode, TreeEntry};
use super::oid::{Kind, ObjectFormat, Oid};

/// The longest entry name git's fsck takes (its `largePathname` default).
const MAX_NAME: usize = 4096;

/// The checks of one tree, fed its entries in the order the tree stores them.
#[derive(Default)]
pub(crate) struct TreeCheck<'a> {
    last: Option<(EntryMode, &'a [u8])>,
    /// Names of files that a directory of the same name may still follow,
    /// as `verify_ordered` tracks them.
    files: Vec<&'a [u8]>,
}

impl<'a> TreeCheck<'a> {
    /// Check `e`, whose mode the tree writes with a leading zero when
    /// `zero_padded`.
    pub(crate) fn entry(&mut self, e: &TreeEntry<'a>, zero_padded: bool) -> Result<()> {
        let name = e.name;
        if e.oid.is_zero() {
            bail!("contains entries pointing to null sha1");
        }
        if name.contains(&b'/') {
            bail!("contains full pathnames");
        }
        if name.is_empty() {
            bail!("contains empty pathname");
        }
        if name == b"." {
            bail!("contains '.'");
        }
        if name == b".." {
            bail!("contains '..'");
        }
        if is_dotgit(name) || after_backslashes(name).any(is_ntfs_dotgit) {
            bail!("contains '.git'");
        }
        if zero_padded {
            bail!("contains zero-padded file modes");
        }
        if name.len() > MAX_NAME {
            bail!("contains excessively large pathname");
        }
        if e.mode.is_symlink()
            && (is_dotgitmodules(name) || after_backslashes(name).any(is_ntfs_dotgitmodules))
        {
            bail!(".gitmodules is a symbolic link");
        }
        if let Some((mode, last)) = self.last {
            self.order(mode, last, e.mode, name)?;
        }
        self.last = Some((e.mode, name));
        Ok(())
    }

    /// Whether `name2` may follow `name1`, as git's `verify_ordered`
    /// decides: in tree order, where a directory sorts as `name/`, and
    /// with no name twice, even when entries come between the two.
    fn order(
        &mut self,
        mode1: EntryMode,
        name1: &'a [u8],
        mode2: EntryMode,
        name2: &[u8],
    ) -> Result<()> {
        if let Some((a, b)) = name1.iter().zip(name2).find(|(a, b)| a != b) {
            if a < b {
                return Ok(());
            }
            bail!("not properly sorted");
        }
        let len = name1.len().min(name2.len());
        let mut c1 = name1.get(len).copied().unwrap_or(0);
        let mut c2 = name2.get(len).copied().unwrap_or(0);
        if c1 == 0 && c2 == 0 {
            bail!("contains duplicate file entries");
        }
        if c1 == 0 && mode1.is_tree() {
            c1 = b'/';
        }
        if c2 == 0 && mode2.is_tree() {
            c2 = b'/';
        }
        if c1 == 0 && less_than_slash(c2) {
            self.files.push(name1);
        } else if c2 == b'/' && less_than_slash(c1) && self.names_a_file(name2) {
            bail!("contains duplicate file entries");
        }
        if c1 >= c2 {
            bail!("not properly sorted");
        }
        Ok(())
    }

    /// Whether the directory `dir` has the name of a file that came before
    /// it. Files that may still match a later directory stay on the stack.
    fn names_a_file(&mut self, dir: &[u8]) -> bool {
        while let Some(file) = self.files.pop() {
            match dir.strip_prefix(file).map(<[u8]>::first) {
                Some(None) => return true,
                Some(Some(&c)) if less_than_slash(c) => {
                    self.files.push(file);
                    return false;
                }
                _ => {}
            }
        }
        false
    }
}

fn less_than_slash(c: u8) -> bool {
    0 < c && c < b'/'
}

/// The parts of `name` after each backslash, which Windows reads as a
/// directory separator.
fn after_backslashes(name: &[u8]) -> impl Iterator<Item = &[u8]> {
    name.iter()
        .enumerate()
        .filter(|&(_, &c)| c == b'\\')
        .map(|(i, _)| name.get(i + 1..).unwrap_or_default())
}

/// Whether a Mac or Windows checkout reads `name` as `.git`.
pub(crate) fn is_dotgit(name: &[u8]) -> bool {
    is_hfs_dot(name, b"git") || is_ntfs_dotgit(name)
}

/// Whether a Mac or Windows checkout reads `name` as `.gitmodules`.
pub(crate) fn is_dotgitmodules(name: &[u8]) -> bool {
    is_hfs_dot(name, b"gitmodules") || is_ntfs_dotgitmodules(name)
}

/// `.` and `needle` in any case, with the code points that HFS+ ignores
/// anywhere. A byte that is not UTF-8 ends the name, as in git.
fn is_hfs_dot(name: &[u8], needle: &[u8]) -> bool {
    let mut s = name;
    if next_hfs(&mut s) != u32::from(b'.') {
        return false;
    }
    for &want in needle {
        let c = u8::try_from(next_hfs(&mut s)).ok().filter(u8::is_ascii);
        if c.map(|c| c.to_ascii_lowercase()) != Some(want) {
            return false;
        }
    }
    matches!(next_hfs(&mut s), 0 | 0x2f)
}

/// The next code point HFS+ keeps, or 0 at the end of `s` or at bytes that
/// are not UTF-8 (after which `s` is empty).
fn next_hfs(s: &mut &[u8]) -> u32 {
    loop {
        let Some((c, n)) = utf8_char(s) else {
            *s = &[];
            return 0;
        };
        *s = s.get(n..).unwrap_or_default();
        if !matches!(c, 0x200c..=0x200f | 0x202a..=0x202e | 0x206a..=0x206f | 0xfeff) {
            return c;
        }
    }
}

/// The code point `s` starts with and its length, as git's
/// `pick_one_utf8_char` reads it: no overlong forms, surrogates, U+FFFE or
/// U+FFFF.
fn utf8_char(s: &[u8]) -> Option<(u32, usize)> {
    let n = match *s.first()? {
        0..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf7 => 4,
        _ => return None,
    };
    let c = u32::from(std::str::from_utf8(s.get(..n)?).ok()?.chars().next()?);
    (c != 0xfffe && c != 0xffff).then_some((c, n))
}

/// `.git` or `git~1` in any case, then only spaces and periods up to a
/// separator or an alternate data stream's `:`.
fn is_ntfs_dotgit(name: &[u8]) -> bool {
    let rest = match strip_prefix_ci(name, b".git") {
        Some(rest) => rest,
        None => match strip_prefix_ci(name, b"git").and_then(|r| r.strip_prefix(b"~1")) {
            Some(rest) => rest,
            None => return false,
        },
    };
    for &c in rest {
        if matches!(c, b'/' | b'\\' | b':') {
            return true;
        }
        if c != b'.' && c != b' ' {
            return false;
        }
    }
    true
}

fn is_ntfs_dotgitmodules(name: &[u8]) -> bool {
    is_ntfs_dot(name, b"gitmodules", *b"gi7eba")
}

/// `.` and `dotname` in any case, or a short name NTFS gives it: its first
/// six characters and `~1` to `~4`, or the fallback `short` with any `~N`.
/// Then only spaces and periods, up to an alternate data stream's `:`. This
/// is git's `is_ntfs_dot_generic`.
fn is_ntfs_dot(name: &[u8], dotname: &[u8], short: [u8; 6]) -> bool {
    let only_spaces_and_periods = |rest: &[u8]| {
        for &c in rest {
            if c == b':' {
                return true;
            }
            if c != b' ' && c != b'.' {
                return false;
            }
        }
        true
    };
    if let Some(rest) = name
        .strip_prefix(b".")
        .and_then(|r| strip_prefix_ci(r, dotname))
    {
        return only_spaces_and_periods(rest);
    }
    if let Some(rest) = strip_prefix_ci(name, dotname.get(..6).unwrap_or_default())
        && let [b'~', b'1'..=b'4', rest @ ..] = rest
    {
        return only_spaces_and_periods(rest);
    }
    let at = |i: usize| name.get(i).copied().unwrap_or(0);
    let mut saw_tilde = false;
    let mut i = 0;
    while i < 8 {
        let c = at(i);
        if c == 0 {
            return false;
        } else if saw_tilde {
            if !c.is_ascii_digit() {
                return false;
            }
        } else if c == b'~' {
            i += 1;
            if !(b'1'..=b'9').contains(&at(i)) {
                return false;
            }
            saw_tilde = true;
        } else if i >= 6 || !c.is_ascii() || short.get(i) != Some(&c.to_ascii_lowercase()) {
            return false;
        }
        i += 1;
    }
    only_spaces_and_periods(name.get(i..).unwrap_or_default())
}

fn strip_prefix_ci<'a>(s: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let (head, rest) = s.split_at_checked(prefix.len())?;
    head.eq_ignore_ascii_case(prefix).then_some(rest)
}

/// Check a commit as `fsck_commit` does: `tree`, the `parent`s, one
/// `author` and a `committer` start its headers, and it holds no NUL.
pub(crate) fn commit(format: ObjectFormat, data: &[u8]) -> Result<()> {
    verify_headers(data)?;
    let p = header(data, 0, b"tree ").context("invalid format - expected 'tree' line")?;
    let (_, mut p) = oid_line(format, data, p).context("invalid 'tree' line format - bad sha1")?;
    while let Some(v) = header(data, p, b"parent ") {
        (_, p) = oid_line(format, data, v).context("invalid 'parent' line format - bad sha1")?;
    }
    let mut authors = 0;
    while let Some(v) = header(data, p, b"author ") {
        authors += 1;
        ident(data, v)?;
        p = next_line(data, v);
    }
    match authors {
        0 => bail!("invalid format - expected 'author' line"),
        1 => {}
        _ => bail!("invalid format - multiple 'author' lines"),
    }
    let v = header(data, p, b"committer ").context("invalid format - expected 'committer' line")?;
    ident(data, v)?;
    if data.contains(&0) {
        bail!("NUL byte in the commit object body");
    }
    Ok(())
}

/// Check a tag as `fsck_tag` does, and give what it tags: `object`, `type`
/// and `tag` start its headers, then `tagger` if any. A bad tag name, no
/// tagger and headers after the tagger are only git's notes, which pass.
pub(crate) fn tag(format: ObjectFormat, data: &[u8]) -> Result<(Oid, Kind)> {
    // git's tag reader, which a push also runs, refuses anything shorter
    // than an id in hex and 24 bytes more.
    if data.len() < format.hex_len() + 24 {
        bail!("invalid tag: too short");
    }
    verify_headers(data)?;
    let p = header(data, 0, b"object ").context("invalid format - expected 'object' line")?;
    let (target, p) =
        oid_line(format, data, p).context("invalid 'object' line format - bad sha1")?;
    let p = header(data, p, b"type ").context("invalid format - expected 'type' line")?;
    let eol = line_end(data, p).context("invalid format - unexpected end after 'type' line")?;
    let kind = Kind::ALL
        .into_iter()
        .find(|k| data.get(p..eol) == Some(k.name().as_bytes()))
        .context("invalid 'type' value")?;
    let p = header(data, eol + 1, b"tag ").context("invalid format - expected 'tag' line")?;
    let mut p = line_end(data, p).context("invalid format - unexpected end after 'tag' line")? + 1;
    let mut tagger = Ok(());
    if let Some(v) = header(data, p, b"tagger ") {
        tagger = ident(data, v);
        p = next_line(data, v);
    }
    // git notes a header after the tagger with a report that also drops a
    // bad tagger's error, so such a tag passes.
    if p >= data.len() || at(data, p) == b'\n' {
        tagger?;
    }
    Ok((target, kind))
}

/// Check that the headers end before any NUL, at a blank line or at the
/// final newline, as `verify_headers` does.
fn verify_headers(data: &[u8]) -> Result<()> {
    for (i, &b) in data.iter().enumerate() {
        match b {
            0 => bail!("unterminated header: NUL at offset {i}"),
            b'\n' if at(data, i + 1) == b'\n' => return Ok(()),
            _ => {}
        }
    }
    if data.last() == Some(&b'\n') {
        Ok(())
    } else {
        bail!("unterminated header")
    }
}

/// Check a person line from `p`, `Name <email> 1700000000 +0100`, as
/// `fsck_ident` does.
fn ident(data: &[u8], mut p: usize) -> Result<()> {
    if at(data, p) == b'<' {
        bail!("invalid author/committer line - missing space before email");
    }
    p = skip_name(data, p);
    match at(data, p) {
        b'>' => bail!("invalid author/committer line - bad name"),
        b'<' => {}
        _ => bail!("invalid author/committer line - missing email"),
    }
    if at(data, p - 1) != b' ' {
        bail!("invalid author/committer line - missing space before email");
    }
    p = skip_name(data, p + 1);
    if at(data, p) != b'>' {
        bail!("invalid author/committer line - bad email");
    }
    if at(data, p + 1) != b' ' {
        bail!("invalid author/committer line - missing space before date");
    }
    p += 2;
    while matches!(at(data, p), b' ' | b'\t') {
        p += 1;
    }
    if !at(data, p).is_ascii_digit() {
        bail!("invalid author/committer line - bad date");
    }
    if at(data, p) == b'0' && at(data, p + 1) != b' ' {
        bail!("invalid author/committer line - zero-padded date");
    }
    // git reads the date as an unsigned number that must fit a signed time.
    let mut seconds = Some(0_i64);
    while at(data, p).is_ascii_digit() {
        let digit = i64::from(at(data, p) - b'0');
        seconds = seconds.and_then(|s| s.checked_mul(10)?.checked_add(digit));
        p += 1;
    }
    if seconds.is_none() {
        bail!("invalid author/committer line - date causes integer overflow");
    }
    if at(data, p) != b' ' {
        bail!("invalid author/committer line - bad date");
    }
    p += 1;
    if !matches!(at(data, p), b'+' | b'-')
        || !(1..=4).all(|i| at(data, p + i).is_ascii_digit())
        || at(data, p + 5) != b'\n'
    {
        bail!("invalid author/committer line - bad time zone");
    }
    Ok(())
}

/// The byte at `i`, or NUL past the end, as git's buffers end.
fn at(data: &[u8], i: usize) -> u8 {
    data.get(i).copied().unwrap_or(0)
}

/// Where the value of the header `name` starts, if the line at `p` is one.
fn header(data: &[u8], p: usize, name: &[u8]) -> Option<usize> {
    data.get(p..)?.starts_with(name).then_some(p + name.len())
}

/// The id at `p` and where the next line starts, if an id of `format` in
/// hex and a newline follow.
fn oid_line(format: ObjectFormat, data: &[u8], p: usize) -> Option<(Oid, usize)> {
    let n = format.hex_len();
    let id = Oid::from_hex_exact(data.get(p..p + n)?)?;
    (at(data, p + n) == b'\n').then_some((id, p + n + 1))
}

/// The newline that ends the line at `p`.
fn line_end(data: &[u8], p: usize) -> Option<usize> {
    Some(p + data.get(p..)?.iter().position(|&b| b == b'\n')?)
}

/// Where the line after the one at `p` starts, or the end.
fn next_line(data: &[u8], p: usize) -> usize {
    line_end(data, p).map_or(data.len(), |eol| eol + 1)
}

/// The first `<`, `>`, newline or end from `p`.
fn skip_name(data: &[u8], mut p: usize) -> usize {
    while !matches!(at(data, p), b'<' | b'>' | b'\n' | 0) {
        p += 1;
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idents_are_checked_as_git_fsck_does() {
        let check = |who: &str| ident(format!("{who}\n").as_bytes(), 0);
        check("A <a> 1 +0000").unwrap();
        check("A <a> \t 0 -1234").unwrap();
        check("A <a> 9223372036854775807 +9999").unwrap();
        for bad in [
            "<a> 1 +0000",
            "A> <a> 1 +0000",
            "A a 1 +0000",
            "A<a> 1 +0000",
            "A <a 1 +0000",
            "A <a>1 +0000",
            "A <a> x +0000",
            "A <a> 01 +0000",
            "A <a> 9223372036854775808 +0000",
            "A <a> 1+0000",
            "A <a> 1 +000",
            "A <a> 1 +00000",
            "A <a> 1 0000",
        ] {
            check(bad).unwrap_err();
        }
    }

    #[test]
    fn headers_end_before_any_nul() {
        verify_headers(b"a\n").unwrap();
        verify_headers(b"a\n\nb\0").unwrap();
        verify_headers(b"a").unwrap_err();
        verify_headers(b"").unwrap_err();
        verify_headers(b"a\0\n\n").unwrap_err();
    }

    const ID: Oid = Oid::sha1([1; 20]);

    fn check(entries: &[(u32, &[u8])]) -> Result<()> {
        let mut c = TreeCheck::default();
        for &(mode, name) in entries {
            let e = TreeEntry {
                mode: EntryMode(mode),
                name,
                oid: ID,
            };
            c.entry(&e, false)?;
        }
        Ok(())
    }

    #[test]
    fn names_a_checkout_reads_as_dotgit() {
        for name in [
            ".git",
            ".GIT",
            ".git.",
            ".git ..",
            "git~1",
            "GIT~1 ",
            ".git::$INDEX_ALLOCATION",
            ".g\u{200c}it",
            ".git\u{feff}",
        ] {
            assert!(is_dotgit(name.as_bytes()), "{name:?}");
        }
        // Bytes that are not UTF-8 end the name for HFS+.
        assert!(is_dotgit(b".git\xff"));
        for name in [".github", "git", ".gitx", "git~2", "x.git", ".gi"] {
            assert!(!is_dotgit(name.as_bytes()), "{name:?}");
        }
    }

    #[test]
    fn names_a_checkout_reads_as_dotgitmodules() {
        for name in [
            ".gitmodules",
            ".GITMODULES.",
            "gitmod~1",
            "GITMOD~4",
            "gi7eba~1",
            "gi7eba~9",
            "~1234567",
            ".gitmodules::$DATA",
            ".g\u{200c}itmodules",
        ] {
            assert!(is_dotgitmodules(name.as_bytes()), "{name:?}");
        }
        for name in [".gitmodules.B", "gitmod~5", "gi7ebb~1", ".gitmodule", "x"] {
            assert!(!is_dotgitmodules(name.as_bytes()), "{name:?}");
        }
    }

    #[test]
    fn trees_git_fsck_refuses() {
        const FILE: u32 = 0o100_644;
        const DIR: u32 = 0o040_000;
        const LINK: u32 = 0o120_000;
        let long = vec![b'z'; MAX_NAME + 1];
        for bad in [
            vec![(FILE, &b"a/b"[..])],
            vec![(FILE, b"")],
            vec![(FILE, b".")],
            vec![(DIR, b"..")],
            vec![(DIR, b".Git")],
            vec![(FILE, b"x\\.git")],
            vec![(FILE, b"git~1\\x")],
            vec![(LINK, b".gitmodules")],
            vec![(LINK, b"x\\gitmod~1")],
            vec![(FILE, &long[..])],
            vec![(FILE, b"b"), (FILE, b"a")],
            vec![(FILE, b"a"), (FILE, b"a")],
            vec![(DIR, b"a"), (FILE, b"a.c")],
            // Not next to each other: a file and a directory of one name.
            vec![(FILE, b"x"), (FILE, b"x.1"), (DIR, b"x")],
        ] {
            check(&bad).unwrap_err();
        }
        for good in [
            vec![(FILE, &b".gitignore"[..]), (FILE, b".gitmodules")],
            vec![(FILE, b"a-"), (FILE, b"a.c"), (DIR, b"a")],
            vec![(FILE, b"a.c"), (DIR, b"a")],
            vec![(FILE, b"x"), (FILE, b"x.1"), (DIR, b"x.1.2"), (DIR, b"y")],
            vec![(LINK, b".gitignore"), (FILE, &long[..MAX_NAME])],
        ] {
            check(&good).unwrap();
        }
    }
}
