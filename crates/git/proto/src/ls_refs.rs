use enroute_git_retrieve::{RefsMap, is_direct_ref};

use crate::Error;
use crate::pktline::{Body, trim_lf};

/// Handle the `ls-refs` command, returning the pkt-line response body.
///
/// # Errors
///
/// Returns an error if pkt-line encoding fails.
pub(crate) fn ls_refs(refs: &RefsMap, args: &[&[u8]]) -> Result<Vec<u8>, Error> {
    let want_symrefs = args.iter().any(|l| trim_lf(l) == b"symrefs");
    let want_unborn = args.iter().any(|l| trim_lf(l) == b"unborn");

    let mut out = Body::new();

    if let Some(head_val) = refs.get("HEAD") {
        let (head_oid, symref_target) = if let Some(target) = head_val.strip_prefix("ref: ") {
            let oid = refs
                .get(target)
                .filter(|v| is_direct_ref(v))
                .map(String::as_str);
            (oid, Some(target))
        } else if is_direct_ref(head_val) {
            (Some(head_val.as_str()), None)
        } else {
            (None, None)
        };

        if let Some(oid) = head_oid {
            let mut line = format!("{oid} HEAD");
            if want_symrefs && let Some(target) = symref_target {
                line.push_str(" symref-target:");
                line.push_str(target);
            }
            line.push('\n');
            out.line(line.as_bytes())?;
        } else if want_unborn && let Some(target) = symref_target {
            // Advertise the unborn branch per `ls-refs=unborn` so clients
            // land on it instead of their own `init.defaultBranch`.
            out.line(format!("unborn HEAD symref-target:{target}\n").as_bytes())?;
        }
    }

    for (refname, val) in refs {
        if refname == "HEAD" || !is_direct_ref(val) {
            continue;
        }
        out.line(format!("{val} {refname}\n").as_bytes())?;
    }

    out.flush();

    Ok(out.into_bytes())
}
