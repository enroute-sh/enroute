//! What the remote did with the push, read from its `report-status` answer.
//!
//! Two levels of refusal: the pack as a whole, and then each ref. A remote
//! that stored the pack can still refuse every command in it.

use std::collections::HashMap;

use enroute_git_proto::pktline::{Packet, Packets, text};

use crate::Error;

/// The remote's answer to a push.
#[derive(Debug, Default)]
pub(crate) struct Report {
    /// Why the remote would not store the pack, if it would not.
    pub(crate) unpack_error: Option<String>,
    /// Why each ref was refused, or `None` where it landed.
    pub(crate) refs: HashMap<String, Option<String>>,
}

/// Read a `report-status` body.
pub(crate) fn parse(body: &[u8]) -> Result<Report, Error> {
    let mut report = Report::default();
    let mut seen_unpack = false;

    for packet in Packets::new(body) {
        let Packet::Data(line) = packet? else {
            continue;
        };
        let line = text(line);
        if let Some(status) = line.strip_prefix("unpack ") {
            seen_unpack = true;
            if status != "ok" {
                report.unpack_error = Some(status.to_owned());
            }
        } else if let Some(refname) = line.strip_prefix("ok ") {
            report.refs.insert(refname.to_owned(), None);
        } else if let Some(rest) = line.strip_prefix("ng ") {
            let (refname, why) = rest.split_once(' ').unwrap_or((rest, "refused"));
            report.refs.insert(refname.to_owned(), Some(why.to_owned()));
        }
        // Anything else is a line this side does not read. A remote may say
        // more than the report, and none of it changes what landed.
    }

    if !seen_unpack {
        return Err(Error::Protocol(
            "the remote sent no report-status; what landed is unknown".into(),
        ));
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(lines: &[&str]) -> Vec<u8> {
        use enroute_git_proto::pktline::Body;
        let mut body = Body::new();
        for line in lines {
            body.line(format!("{line}\n").as_bytes()).expect("encoded");
        }
        body.flush();
        body.into_bytes()
    }

    #[test]
    fn reads_what_landed_and_what_did_not() {
        let read = parse(&body(&[
            "unpack ok",
            "ok refs/heads/main",
            "ng refs/heads/wip non-fast-forward",
        ]))
        .expect("a report");

        assert!(read.unpack_error.is_none());
        assert_eq!(read.refs.get("refs/heads/main"), Some(&None));
        assert_eq!(
            read.refs.get("refs/heads/wip"),
            Some(&Some("non-fast-forward".to_owned()))
        );
    }

    #[test]
    fn reads_a_pack_the_remote_would_not_store() {
        let read = parse(&body(&["unpack index-pack failed"])).expect("a report");
        assert_eq!(read.unpack_error.as_deref(), Some("index-pack failed"));
    }

    #[test]
    fn refuses_an_answer_with_no_report_in_it() {
        parse(&body(&["ok refs/heads/main"])).unwrap_err();
        parse(b"").unwrap_err();
    }
}
