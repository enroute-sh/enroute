//! `git-receive-pack` (protocol v0/v1, which v2 clients also use for push).

use anyhow::{Result, bail};

use crate::git::oid::{ObjectFormat, Oid};
use crate::query::RefView;

use super::AGENT;
use super::Refusal;
use super::pktline::{self, Reader};

/// The capabilities advertised, before the repository's `object-format`.
pub(crate) const CAPABILITIES: &str =
    "report-status delete-refs side-band-64k quiet atomic ofs-delta push-options";

/// The ref advertisement of `view`, which has no `HEAD` line.
pub(crate) fn advertisement(view: &RefView<'_>) -> Vec<u8> {
    let refs: Vec<(Oid, String)> = view
        .refs
        .iter()
        .map(|(name, oid)| (*oid, (*name).to_string()))
        .collect();
    super::advertise_refs(
        "git-receive-pack",
        view.format,
        &refs,
        &format!("{CAPABILITIES} object-format={} agent={AGENT}", view.format),
    )
}

#[derive(Debug, Clone)]
pub(crate) struct PushCommand {
    pub old: Oid,
    pub new: Oid,
    pub name: String,
}

pub(crate) struct PushRequest {
    pub commands: Vec<PushCommand>,
    pub capabilities: Vec<String>,
    /// The object format of the commands and the pack: SHA-1 unless the
    /// client names another with `object-format`, as git assumes.
    pub format: ObjectFormat,
    /// The client's `git push -o` options, in the order sent. Empty unless it
    /// asked for `push-options`.
    pub options: Vec<String>,
    /// Offset of the pack data in the request body.
    pub pack_offset: usize,
}

impl PushRequest {
    pub(crate) fn has(&self, cap: &str) -> bool {
        self.capabilities.iter().any(|c| c == cap)
    }

    /// Whether the client asked for a report. A v1 report is also a valid
    /// v2 one: its `option` lines are optional.
    pub(crate) fn wants_status(&self) -> bool {
        self.has("report-status") || self.has("report-status-v2")
    }
}

/// Read the commands of a push, as git's `read_head_info` does. Any packet
/// but data ends them. Then, if the client asked for `push-options`, its
/// options follow up to the next flush, as git's `read_push_options` reads
/// them, and the pack follows.
pub(crate) fn parse_request(body: &[u8]) -> Result<PushRequest> {
    let mut r = Reader::new(body);
    let mut commands = Vec::new();
    let mut capabilities: Vec<String> = Vec::new();
    let mut format = ObjectFormat::Sha1;
    loop {
        let d = match r.next_data()? {
            Some(Ok(d)) => d,
            Some(Err(_)) => break,
            None => bail!(Refusal::new("truncated command list (no flush)")),
        };
        if d.len() > 8
            && let Some(id) = d.strip_prefix(b"shallow ")
        {
            // The client's shallow boundary; connectivity checking rejects
            // pushes that would need new shallow roots. As in git, nothing
            // after the id is read, capabilities included.
            if super::oid_prefix(format, id).is_none() {
                bail!(Refusal::new(format!(
                    "protocol error: expected shallow sha, got '{}'",
                    pktline::c_text(id)
                )));
            }
            continue;
        }
        let (line, caps) = match pktline::split_once(d, 0) {
            Some((line, caps)) => (line, Some(caps)),
            None => (d, None),
        };
        if let Some(c) = caps {
            for c in pktline::c_text(c).split(' ').filter(|s| !s.is_empty()) {
                capabilities.push(c.to_string());
            }
            format =
                super::named_format(capabilities.iter().map(String::as_str))?.unwrap_or_default();
        }
        if line == b"push-cert" {
            bail!(Refusal::new("signed pushes are not supported"));
        }
        commands.push(parse_command(format, line).ok_or_else(|| {
            Refusal::new(format!(
                "protocol error: expected old/new/ref, got '{}'",
                pktline::c_text(line)
            ))
        })?);
    }
    let mut options = Vec::new();
    if capabilities.iter().any(|c| c == "push-options") {
        loop {
            match r.next_data()? {
                Some(Ok(d)) => options.push(pktline::c_text(d)),
                Some(Err(_)) => break,
                None => bail!(Refusal::new("truncated push options (no flush)")),
            }
        }
    }
    Ok(PushRequest {
        commands,
        capabilities,
        format,
        options,
        pack_offset: r.pos,
    })
}

/// `old new name`, with ids of `format`, as git's `queue_command` reads it.
fn parse_command(format: ObjectFormat, line: &[u8]) -> Option<PushCommand> {
    let n = format.hex_len();
    let old = super::oid_prefix(format, line)?;
    let rest = line.get(n..)?.strip_prefix(b" ")?;
    let new = super::oid_prefix(format, rest)?;
    let name = rest.get(n..)?.strip_prefix(b" ")?;
    let name = std::str::from_utf8(name).ok()?.to_string();
    Some(PushCommand { old, new, name })
}

/// A push's report, after `messages` as progress lines (git prints them as
/// `remote:` lines). Without side-band the client has no channel for them,
/// so they are dropped, as git does. Without `status` (the client did not
/// ask for `report-status`) only the messages are sent.
pub(crate) fn report(
    status: bool,
    unpack: Result<(), String>,
    results: &[(String, Option<String>)],
    sideband: bool,
    messages: &[String],
) -> Vec<u8> {
    let rep = if status {
        status_lines(unpack, results)
    } else {
        Vec::new()
    };
    if !sideband {
        return rep;
    }
    let mut out = Vec::new();
    for m in messages {
        pktline::sideband(
            &mut out,
            2,
            format!("{}\n", m.trim_end_matches('\n')).as_bytes(),
        );
    }
    if status {
        pktline::sideband(&mut out, 1, &rep);
    }
    pktline::flush(&mut out);
    out
}

/// The `report-status` lines and their flush.
fn status_lines(unpack: Result<(), String>, results: &[(String, Option<String>)]) -> Vec<u8> {
    let mut rep = Vec::new();
    match unpack {
        Ok(()) => pktline::line(&mut rep, "unpack ok\n"),
        Err(e) => pktline::line(&mut rep, &format!("unpack {}\n", e.replace('\n', " "))),
    }
    for (name, r) in results {
        match r {
            None => pktline::line(&mut rep, &format!("ok {name}\n")),
            Some(e) => pktline::line(&mut rep, &format!("ng {name} {}\n", e.replace('\n', " "))),
        }
    }
    pktline::flush(&mut rep);
    rep
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "c058a00c064d286f3d5e059266726fd6c76d3753";
    const B: &str = "9a1462e6c341a4fcffab5bb3c16c6af4a2d49cab";

    fn body(lines: &[&[u8]], end: fn(&mut Vec<u8>)) -> Vec<u8> {
        let mut out = Vec::new();
        for l in lines {
            pktline::data(&mut out, l);
        }
        end(&mut out);
        out
    }

    #[test]
    fn reads_commands_as_git_does() {
        let first = format!("{A} {B} refs/heads/main\0 report-status side-band-64k\n");
        let shallow = format!("shallow {A}xyz\0 atomic");
        let req = parse_request(&body(
            &[shallow.as_bytes(), first.as_bytes()],
            pktline::flush,
        ))
        .expect("parsed");
        assert_eq!(req.commands.len(), 1);
        assert_eq!(
            req.commands.first().map(|c| c.name.as_str()),
            Some("refs/heads/main")
        );
        // A shallow line's capabilities are not read.
        assert!(req.has("report-status") && !req.has("atomic"));

        // A delim ends the commands, as a flush does.
        let second = format!("{A} {B} refs/heads/dev");
        let req = parse_request(&body(&[first.as_bytes()], pktline::delim)).expect("parsed");
        assert_eq!(req.commands.len(), 1);
        let mut both = body(&[first.as_bytes()], pktline::delim);
        both.extend(body(&[second.as_bytes()], pktline::flush));
        assert_eq!(parse_request(&both).expect("parsed").commands.len(), 1);
    }

    #[test]
    fn reads_push_options_only_when_asked_for() {
        let asked = format!("{A} {B} refs/heads/main\0 report-status push-options\n");
        let mut req = body(&[asked.as_bytes()], pktline::flush);
        req.extend(body(&[b"ci.skip\n", b"title=Fix it"], pktline::flush));
        let pack_at = req.len();
        req.extend(b"PACK");
        let parsed = parse_request(&req).expect("parsed");
        assert_eq!(parsed.options, ["ci.skip", "title=Fix it"]);
        assert_eq!(parsed.pack_offset, pack_at);

        // Without the capability, what follows the flush is the pack.
        let plain = format!("{A} {B} refs/heads/main\0 report-status\n");
        let mut req = body(&[plain.as_bytes()], pktline::flush);
        let pack_at = req.len();
        req.extend(body(&[b"ci.skip"], pktline::flush));
        let parsed = parse_request(&req).expect("parsed");
        assert!(parsed.options.is_empty());
        assert_eq!(parsed.pack_offset, pack_at);

        // Options cut off before their flush are refused.
        let mut cut = body(&[asked.as_bytes()], pktline::flush);
        pktline::data(&mut cut, b"ci.skip");
        assert!(parse_request(&cut).is_err());
    }

    #[test]
    fn reads_sha256_commands_when_the_client_names_the_format() {
        let (a, b) = ("ab".repeat(32), "cd".repeat(32));
        let (a, b) = (a.as_str(), b.as_str());
        let shallow = format!("shallow {a}");
        let first = format!("{a} {b} refs/heads/main\0 report-status object-format=sha256");
        let req = parse_request(&body(
            &[shallow.as_bytes(), first.as_bytes()],
            pktline::flush,
        ))
        .expect("parsed");
        assert_eq!(req.format, ObjectFormat::Sha256);
        let c = req.commands.first().expect("a command");
        assert_eq!(
            (c.old.to_hex(), c.new.to_hex()),
            (a.to_owned(), b.to_owned())
        );
        // Without the capability, the ids are read as SHA-1 and do not fit.
        let bare = format!("{a} {b} refs/heads/main\0 report-status");
        assert!(parse_request(&body(&[bare.as_bytes()], pktline::flush)).is_err());
    }

    #[test]
    fn refuses_what_git_refuses() {
        let format = format!("{A} {B} refs/heads/main\0 report-status object-format=sha256");
        for line in [
            b"shallow ".as_slice(),
            b"shallow c058a00c064d286f3d5e",
            format.as_bytes(),
            format!("{A}  {B} refs/heads/main").as_bytes(),
            format!("{A} {B}").as_bytes(),
        ] {
            let error = parse_request(&body(&[line], pktline::flush))
                .err()
                .expect("refused");
            assert!(error.downcast_ref::<Refusal>().is_some(), "{error:#}");
        }
    }

    #[test]
    fn reports_only_when_asked() {
        let results = [("refs/heads/main".to_owned(), None)];
        assert!(report(false, Ok(()), &results, false, &[]).is_empty());
        let sideband = report(false, Ok(()), &results, true, &["hi".to_owned()]);
        assert!(!String::from_utf8_lossy(&sideband).contains("unpack"));
        let asked = report(true, Ok(()), &results, false, &[]);
        assert!(String::from_utf8_lossy(&asked).contains("ok refs/heads/main"));
    }
}
