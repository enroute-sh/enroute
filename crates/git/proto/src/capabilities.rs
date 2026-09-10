use enroute_git_retrieve::{RefsMap, is_direct_ref};

use crate::Error;
use crate::pktline::Body;

/// pkt-line body advertising `git-upload-pack` capabilities.
///
/// # Errors
///
/// Returns an error if pkt-line encoding fails.
pub fn upload_pack_capabilities() -> Result<Vec<u8>, Error> {
    let mut out = Body::new();
    out.line(b"# service=git-upload-pack\n")?;
    out.flush();
    out.line(b"version 2\n")?;
    out.line(b"ls-refs=unborn\n")?;
    // Features ride on the `fetch` capability's value: a standalone
    // `filter\n` line is invisible to real git and silently downgrades
    // `--filter` to a full fetch.
    out.line(b"fetch=shallow filter\n")?;
    // "server-option" omitted: advertised args aren't read or acted on yet.
    out.line(b"object-format=sha1\n")?;
    out.flush();
    Ok(out.into_bytes())
}

/// The v0 `git-upload-pack` advertisement: the refs themselves, rather than
/// a list of commands.
///
/// A server that speaks only v2 is unreachable to a client that never sends
/// `Git-Protocol` — go-git is one, `git -c protocol.version=0` another.
///
/// # Errors
///
/// Returns an error if pkt-line encoding fails.
pub fn upload_pack_v0_advertisement(
    refs: &RefsMap,
    default_branch: &str,
) -> Result<Vec<u8>, Error> {
    // Short on purpose: advertising a capability this server then ignores is
    // worse than not advertising it, because the client cannot tell from the
    // response.
    let caps = format!(
        "symref=HEAD:{default_branch} object-format=sha1 agent=git/{}",
        env!("CARGO_PKG_VERSION")
    );
    // What `HEAD` resolves to: the default branch is named, and `HEAD` is
    // whatever it currently points at. It leads and the refs still follow, so
    // an unresolvable `HEAD` leaves the caps to the pseudo-ref.
    let head = refs
        .iter()
        .find(|(name, _)| name.as_str() == default_branch)
        .map(|(_, oid)| ("HEAD", oid.as_str()));

    advertisement("git-upload-pack", &caps, head, direct_refs(refs))
}

/// pkt-line body advertising `git-receive-pack` capabilities and the
/// repository's current refs.
///
/// Lets the client compute correct fast-forward/CAS `old-id`s instead of
/// assuming every ref is absent. `HEAD` is skipped, matching real git.
///
/// # Errors
///
/// Returns an error if pkt-line encoding fails.
pub fn receive_pack_advertisement(refs: &RefsMap) -> Result<Vec<u8>, Error> {
    // `side-band-64k` lets a slow ingest emit progress frames instead of
    // holding the connection silent long enough to trip a timeout.
    const CAPS: &str = concat!(
        "report-status delete-refs ofs-delta side-band-64k object-format=sha1 agent=git/",
        env!("CARGO_PKG_VERSION")
    );

    // A push is offered the refs and nothing else, so the first of them
    // carries the capabilities.
    let mut refs = direct_refs(refs);
    let carrier = refs.next();
    advertisement("git-receive-pack", CAPS, carrier, refs)
}

/// The all-zero id of `capabilities^{}`, the pseudo-ref that carries the
/// capability list when no real ref can.
const NO_REF: &str = "0000000000000000000000000000000000000000";

/// The v0 advertisement both services answer `GET /info/refs` with: a service
/// line, the line carrying `caps`, then `rest`.
///
/// One shape, since a client reads both the same way: the capability list is
/// on the first ref line and nowhere else.
fn advertisement<'a>(
    service: &str,
    caps: &str,
    carrier: Option<(&str, &str)>,
    rest: impl Iterator<Item = (&'a str, &'a str)>,
) -> Result<Vec<u8>, Error> {
    let mut out = Body::new();
    out.line(format!("# service={service}\n").as_bytes())?;
    out.flush();

    let (name, oid) = carrier.unwrap_or(("capabilities^{}", NO_REF));
    out.line(format!("{oid} {name}\0{caps}\n").as_bytes())?;
    for (name, oid) in rest {
        out.line(format!("{oid} {name}\n").as_bytes())?;
    }

    out.flush();
    Ok(out.into_bytes())
}

/// Every ref that names an object, in name order.
///
/// `HEAD` is left out: it names no ref of its own, and each service says its
/// own thing about it.
fn direct_refs(refs: &RefsMap) -> impl Iterator<Item = (&str, &str)> {
    // `refs` is a `BTreeMap`, so this is already refname-sorted.
    refs.iter()
        .filter(|(name, value)| name.as_str() != "HEAD" && is_direct_ref(value))
        .map(|(name, value)| (name.as_str(), value.as_str()))
}

#[cfg(test)]
mod tests {
    use enroute_git_retrieve::RefsMap;
    use enroute_git_test_support::{debug_pktlines, redact_agent};

    /// The refs of a repository with `main` and a tag on it, plus the `HEAD`
    /// symref every repository carries.
    fn populated() -> RefsMap {
        [
            ("HEAD".to_string(), "ref: refs/heads/main".to_string()),
            ("refs/heads/main".to_string(), "1".repeat(40)),
            ("refs/tags/v1".to_string(), "2".repeat(40)),
        ]
        .into_iter()
        .collect()
    }

    /// A repository with nothing in it but its unborn `HEAD`.
    fn empty() -> RefsMap {
        [("HEAD".to_string(), "ref: refs/heads/main".to_string())]
            .into_iter()
            .collect()
    }

    #[test]
    fn upload_pack_capabilities_snapshot() {
        let body = debug_pktlines(&super::upload_pack_capabilities().unwrap());
        insta::assert_snapshot!(body, @"
        # service=git-upload-pack
        [flush]
        version 2
        ls-refs=unborn
        fetch=shallow filter
        object-format=sha1
        [flush]
        ");
    }

    /// `HEAD` leads, carrying the capabilities, and the rest of the refs
    /// follow it in name order.
    #[test]
    fn upload_pack_v0_advertisement_snapshot() {
        let body = redact_agent(&debug_pktlines(
            &super::upload_pack_v0_advertisement(&populated(), "refs/heads/main").unwrap(),
        ));
        insta::assert_snapshot!(body, @r"
        # service=git-upload-pack
        [flush]
        1111111111111111111111111111111111111111 HEAD<NUL>symref=HEAD:refs/heads/main object-format=sha1 agent=git/[version]
        1111111111111111111111111111111111111111 refs/heads/main
        2222222222222222222222222222222222222222 refs/tags/v1
        [flush]
        ");
    }

    /// An unborn `HEAD` names no object, so the capabilities fall back to the
    /// pseudo-ref real git uses for an empty repository.
    #[test]
    fn upload_pack_v0_advertisement_empty_repo_snapshot() {
        let body = redact_agent(&debug_pktlines(
            &super::upload_pack_v0_advertisement(&empty(), "refs/heads/main").unwrap(),
        ));
        insta::assert_snapshot!(body, @"
        # service=git-upload-pack
        [flush]
        0000000000000000000000000000000000000000 capabilities^{}<NUL>symref=HEAD:refs/heads/main object-format=sha1 agent=git/[version]
        [flush]
        ");
    }

    /// A default branch that does not exist yet leaves the capabilities on
    /// the pseudo-ref, and the refs that do exist still follow it.
    #[test]
    fn upload_pack_v0_advertisement_without_its_default_branch_snapshot() {
        let body = redact_agent(&debug_pktlines(
            &super::upload_pack_v0_advertisement(&populated(), "refs/heads/trunk").unwrap(),
        ));
        insta::assert_snapshot!(body, @r"
        # service=git-upload-pack
        [flush]
        0000000000000000000000000000000000000000 capabilities^{}<NUL>symref=HEAD:refs/heads/trunk object-format=sha1 agent=git/[version]
        1111111111111111111111111111111111111111 refs/heads/main
        2222222222222222222222222222222222222222 refs/tags/v1
        [flush]
        ");
    }

    #[test]
    fn receive_pack_advertisement_empty_repo_snapshot() {
        let body = redact_agent(&debug_pktlines(
            &super::receive_pack_advertisement(&empty()).unwrap(),
        ));
        insta::assert_snapshot!(body, @"
        # service=git-receive-pack
        [flush]
        0000000000000000000000000000000000000000 capabilities^{}<NUL>report-status delete-refs ofs-delta side-band-64k object-format=sha1 agent=git/[version]
        [flush]
        ");
    }

    /// Regression test: the advertisement used to be static, so every push
    /// saw `old=0000...0` even for established branches.
    #[test]
    fn receive_pack_advertisement_lists_existing_refs() {
        let body = redact_agent(&debug_pktlines(
            &super::receive_pack_advertisement(&populated()).unwrap(),
        ));
        insta::assert_snapshot!(body, @r"
        # service=git-receive-pack
        [flush]
        1111111111111111111111111111111111111111 refs/heads/main<NUL>report-status delete-refs ofs-delta side-band-64k object-format=sha1 agent=git/[version]
        2222222222222222222222222222222222222222 refs/tags/v1
        [flush]
        ");
    }
}
