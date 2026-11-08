//! What the remote did with the push, read from its `report-status` answer.
//!
//! Two levels of refusal: the pack as a whole, and then each ref. A remote
//! that stored the pack can still refuse every command in it.

use std::collections::HashMap;

use crate::protocol::pktline::{self, Pkt};

use super::{Error, packets};

/// The remote's answer to a push.
#[derive(Debug, Default)]
pub(super) struct Report {
    /// Why the remote would not store the pack, if it would not.
    pub(super) unpack_error: Option<String>,
    /// Why each ref was refused, or `None` where it landed.
    pub(super) refs: HashMap<String, Option<String>>,
}

/// Read a `report-status` body as git's send-pack reads it.
///
/// That is an `unpack` line, then `ok` and `ng` lines (`option` lines may
/// follow an `ok`), then a flush. The last line about a ref wins, and a line
/// git cannot read fails the read.
pub(super) fn parse(body: &[u8]) -> Result<Report, Error> {
    let mut lines = packets(body);
    let mut next = || match lines.next() {
        Some(Ok(Pkt::Data(line))) => Ok(Some(pktline::c_text(line))),
        Some(Ok(_)) => Ok(None),
        Some(Err(error)) => Err(error),
        None => Err(Error::Protocol(
            "the remote's report ends before its flush".into(),
        )),
    };

    let mut report = Report::default();
    let unpack = next()?.unwrap_or_default();
    let Some(status) = unpack.strip_prefix("unpack ") else {
        return Err(Error::Protocol(
            "the remote sent no report-status; what landed is unknown".into(),
        ));
    };
    if status != "ok" {
        report.unpack_error = Some(status.to_owned());
    }

    // Whether the last status line was an `ok`, which `option` lines extend.
    let mut after_ok = false;
    while let Some(line) = next()? {
        let Some((head, rest)) = line.split_once(' ') else {
            return Err(Error::Protocol(format!(
                "invalid status line from remote: {line}"
            )));
        };
        let (refname, why) = rest.split_once(' ').unwrap_or((rest, ""));
        match head {
            // Options say more about an update that landed, and this side
            // asks for none of it, so they are only checked for place.
            "option" if after_ok => continue,
            "option" => {
                return Err(Error::Protocol(
                    "'option' without a matching 'ok/ng' directive".into(),
                ));
            }
            "ok" => report.refs.insert(refname.to_owned(), None),
            "ng" if why.is_empty() => report
                .refs
                .insert(refname.to_owned(), Some("failed".into())),
            "ng" => report.refs.insert(refname.to_owned(), Some(why.to_owned())),
            _ => {
                return Err(Error::Protocol(format!(
                    "invalid ref status from remote: {head}"
                )));
            }
        };
        after_ok = head == "ok";
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(lines: &[&str]) -> Vec<u8> {
        use crate::protocol::pktline::{flush, try_data};
        let mut body = Vec::new();
        for line in lines {
            try_data(&mut body, format!("{line}\n").as_bytes()).expect("encoded");
        }
        flush(&mut body);
        body
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

    #[test]
    fn reads_lines_as_git_does() {
        let read = parse(&body(&[
            "unpack ok\0junk",
            "ok refs/heads/a message",
            "option forced-update",
            "ng refs/heads/b",
            "ng refs/heads/c why\0not",
            "ok refs/heads/d\0e",
            "ng refs/heads/d late",
        ]))
        .expect("a report");
        assert!(read.unpack_error.is_none());
        assert_eq!(read.refs.get("refs/heads/a"), Some(&None));
        assert_eq!(
            read.refs.get("refs/heads/b"),
            Some(&Some("failed".to_owned()))
        );
        assert_eq!(read.refs.get("refs/heads/c"), Some(&Some("why".to_owned())));
        assert_eq!(
            read.refs.get("refs/heads/d"),
            Some(&Some("late".to_owned()))
        );
    }

    #[test]
    fn refuses_what_git_refuses() {
        for lines in [
            &["unpack ok", "option refname refs/heads/x"][..],
            &[
                "unpack ok",
                "ok refs/heads/a",
                "ng refs/heads/b y",
                "option forced-update",
            ],
            &["unpack ok", "ok\0 refs/heads/a"],
            &["unpack ok", "okay refs/heads/a"],
        ] {
            parse(&body(lines)).expect_err(&format!("{lines:?}"));
        }
        // No flush after the last line.
        let mut cut = body(&["unpack ok", "ok refs/heads/main"]);
        cut.truncate(cut.len() - 4);
        parse(&cut).unwrap_err();
    }
}
