//! What the remote says it holds, read from its `receive-pack`
//! advertisement.
//!
//! Two answers come out of it: where each of the remote's refs points, which
//! is what a push's commands are built against, and every object id it named,
//! which is what the pack is cut down by.

use std::collections::{HashMap, HashSet};

use crate::git::oid::{ObjectFormat, Oid};
use crate::protocol::pktline::{self, Pkt, split_once};
use crate::protocol::{named_format, oid_prefix, same_format};

use super::{Error, packets, text};

/// The pseudo-ref an empty repository advertises its capabilities on.
const NO_REFS: &[u8] = b"capabilities^{}";

/// What a remote holds, and what it can do.
#[derive(Debug, Default)]
pub(super) struct Advertisement {
    /// Where each of the remote's refs points.
    pub(super) refs: HashMap<String, Oid>,
    /// Every object id advertised, peeled tag targets, an alternate's
    /// `.have`s and a shallow remote's boundary included, each of which the
    /// remote holds and the pack need not carry.
    pub(super) tips: Vec<Oid>,
    /// What the remote offers, as the tokens it wrote them in.
    pub(super) capabilities: HashSet<String>,
}

impl Advertisement {
    pub(super) fn offers(&self, capability: &str) -> bool {
        self.capabilities.contains(capability)
    }
}

/// Read the advertisement a remote answered `GET /info/refs` with, as git's
/// `get_remote_heads` (`connect.c`) reads it: refs, then `shallow` lines,
/// to a flush. Only the first line's capabilities count. An empty
/// repository sends one `capabilities^{}` line, under the null id, in place
/// of its refs. Anything else is refused, an answer cut off before its
/// flush among them, so a partial listing is never read as the remote's.
///
/// The remote must speak `format`, the repository's: one that names
/// another `object-format` (or none, which is SHA-1) is refused.
pub(super) fn parse(format: ObjectFormat, body: &[u8]) -> Result<Advertisement, Error> {
    let mut packets = packets(body);
    service_header(&mut packets)?;

    let mut advertisement = Advertisement::default();
    let mut expecting = Expecting::FirstRef;
    loop {
        let line = match packets.next().transpose()? {
            Some(Pkt::Data(line)) => pktline::chomp(line),
            // The advertisement ends at the first flush; anything a server
            // sends after it is not part of this answer.
            Some(Pkt::Flush) => return Ok(advertisement),
            Some(Pkt::Delim | Pkt::ResponseEnd) => {
                return Err(Error::Protocol(
                    "invalid packet in the advertisement".into(),
                ));
            }
            None => {
                return Err(Error::Protocol(
                    "the remote's advertisement ended before its flush".into(),
                ));
            }
        };
        expecting = read_line(format, &mut advertisement, expecting, line)?;
    }
}

/// Where in the advertisement a line is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expecting {
    /// The first line, which carries the capabilities.
    FirstRef,
    Ref,
    /// Past the refs, where only `shallow` lines may come.
    Shallow,
}

/// Read one line, and say where the next one is.
fn read_line(
    format: ObjectFormat,
    advertisement: &mut Advertisement,
    expecting: Expecting,
    line: &[u8],
) -> Result<Expecting, Error> {
    // A later line's capabilities end its name and are otherwise ignored.
    let (line, capabilities) = match split_once(line, 0) {
        Some((line, capabilities)) => (line, Some(capabilities)),
        None => (line, None),
    };
    if expecting == Expecting::FirstRef {
        if let Some(capabilities) = capabilities {
            advertisement.capabilities.extend(
                pktline::c_text(capabilities)
                    .split_whitespace()
                    .map(str::to_owned),
            );
        }
        // The remote serves, so it is the server.
        named_format(advertisement.capabilities.iter().map(String::as_str))
            .and_then(|remote| same_format(remote.unwrap_or_default(), format))
            .map_err(|refusal| Error::Unsupported(refusal.to_string()))?;
    }
    match (expecting, ref_line(format, line)) {
        (Expecting::FirstRef, Some((oid, NO_REFS))) if oid.is_zero() => Ok(Expecting::Shallow),
        (Expecting::FirstRef | Expecting::Ref, Some((_, NO_REFS))) => {
            Err(Error::Protocol(format!(
                "unexpected {}: it stands alone, under the null id",
                text(NO_REFS)
            )))
        }
        (Expecting::FirstRef | Expecting::Ref, Some((oid, name))) => {
            advertisement.tips.push(oid);
            // A peeled tag, and an alternate's `.have`, name objects the
            // remote holds, which is why they are tips, but neither is a
            // ref anything can be pushed onto.
            if !name.ends_with(b"^{}") && name != b".have" {
                let name = String::from_utf8_lossy(name).into_owned();
                advertisement.refs.insert(name, oid);
            }
            Ok(Expecting::Ref)
        }
        _ => {
            let shallow = line.strip_prefix(b"shallow ").ok_or_else(|| {
                Error::Protocol(format!("not a line of an advertisement: {}", text(line)))
            })?;
            // The remote is a shallow clone. Its boundary commits need not
            // be sent, and the push goes on as git's does.
            let shallow = oid_prefix(format, shallow).ok_or_else(|| {
                Error::Protocol(format!("not a shallow commit id: {}", text(shallow)))
            })?;
            advertisement.tips.push(shallow);
            Ok(Expecting::Shallow)
        }
    }
}

/// The id (of `format`) and name of an `<oid> <name>` line, if it is one,
/// as git's `process_ref` reads it.
pub(super) fn ref_line(format: ObjectFormat, line: &[u8]) -> Option<(Oid, &[u8])> {
    let oid = oid_prefix(format, line)?;
    Some((oid, line.get(format.hex_len()..)?.strip_prefix(b" ")?))
}

/// Check the `# service=git-receive-pack` line every smart-HTTP server opens
/// with, so a proxy's error page is not read as an empty repository.
fn service_header<'a>(
    packets: &mut impl Iterator<Item = Result<Pkt<'a>, Error>>,
) -> Result<(), Error> {
    let first = packets
        .next()
        .transpose()?
        .ok_or_else(|| Error::Protocol("the remote answered with nothing".into()))?;
    match first {
        Pkt::Data(line) if pktline::chomp(line) == b"# service=git-receive-pack" => {}
        Pkt::Data(line) => {
            return Err(Error::Protocol(format!(
                "the remote does not serve receive-pack here: {}",
                text(line)
            )));
        }
        Pkt::Flush | Pkt::Delim | Pkt::ResponseEnd => {
            return Err(Error::Protocol(
                "the remote named no service in its advertisement".into(),
            ));
        }
    }
    // The service line stands alone in a section of its own.
    match packets.next().transpose()? {
        Some(Pkt::Flush) => Ok(()),
        _ => Err(Error::Protocol(
            "the remote's service line was not closed".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::pktline::{flush, try_data};

    /// The bytes a server writes, so the test reads what production reads.
    fn advertisement(lines: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        try_data(&mut body, b"# service=git-receive-pack\n").expect("encoded");
        flush(&mut body);
        for line in lines {
            try_data(&mut body, line).expect("encoded");
        }
        flush(&mut body);
        body
    }

    const MAIN: &str = "1111111111111111111111111111111111111111";
    const TAG: &str = "2222222222222222222222222222222222222222";
    const PEELED: &str = "3333333333333333333333333333333333333333";
    const ZERO: &str = "0000000000000000000000000000000000000000";

    #[test]
    fn reads_refs_capabilities_and_every_tip() {
        let body = advertisement(&[
            format!("{MAIN} refs/heads/main\0report-status atomic agent=git/2.45\n").as_bytes(),
            format!("{TAG} refs/tags/v1\n").as_bytes(),
            format!("{PEELED} refs/tags/v1^{{}}\n").as_bytes(),
        ]);
        let read = parse(ObjectFormat::Sha1, &body).expect("well-formed");

        assert_eq!(read.refs.len(), 2);
        assert_eq!(
            read.refs.get("refs/heads/main"),
            Some(&Oid::from_hex(MAIN).expect("hex"))
        );
        assert!(!read.refs.contains_key("refs/tags/v1^{}"));
        assert_eq!(read.tips.len(), 3);
        assert!(read.offers("atomic"));
        assert!(read.offers("report-status"));
    }

    #[test]
    fn reads_an_empty_repository_as_no_refs_and_still_reads_its_capabilities() {
        let body =
            advertisement(&[format!("{ZERO} capabilities^{{}}\0report-status\n").as_bytes()]);
        let read = parse(ObjectFormat::Sha1, &body).expect("well-formed");

        assert!(read.refs.is_empty());
        assert!(read.tips.is_empty());
        assert!(read.offers("report-status"));
    }

    #[test]
    fn refuses_an_answer_that_is_not_a_receive_pack_advertisement() {
        let mut body = Vec::new();
        try_data(&mut body, b"# service=git-upload-pack\n").expect("encoded");
        flush(&mut body);

        parse(ObjectFormat::Sha1, &body).unwrap_err();
        parse(ObjectFormat::Sha1, b"").unwrap_err();
    }

    #[test]
    fn refuses_a_listing_cut_off_before_its_flush() {
        for lines in [&[format!("{MAIN} refs/heads/main\n").into_bytes()][..], &[]] {
            let lines: Vec<&[u8]> = lines.iter().map(Vec::as_slice).collect();
            let mut body = advertisement(&lines);
            body.truncate(body.len() - 4);
            parse(ObjectFormat::Sha1, &body).unwrap_err();
        }
    }

    #[test]
    fn refuses_a_delimiter_or_response_end_in_a_listing() {
        for end in [b"0001", b"0002"] {
            let mut body = advertisement(&[format!("{MAIN} refs/heads/main\n").as_bytes()]);
            body.truncate(body.len() - 4);
            body.extend_from_slice(end);
            parse(ObjectFormat::Sha1, &body).unwrap_err();
        }
    }

    #[test]
    fn the_empty_repository_line_stands_alone_under_the_null_id() {
        // Under another id.
        parse(
            ObjectFormat::Sha1,
            &advertisement(&[format!("{MAIN} capabilities^{{}}\0report-status\n").as_bytes()]),
        )
        .unwrap_err();
        // With a ref after it.
        parse(
            ObjectFormat::Sha1,
            &advertisement(&[
                format!("{ZERO} capabilities^{{}}\0report-status\n").as_bytes(),
                format!("{MAIN} refs/heads/main\n").as_bytes(),
            ]),
        )
        .unwrap_err();
        // After a ref.
        parse(
            ObjectFormat::Sha1,
            &advertisement(&[
                format!("{MAIN} refs/heads/main\0report-status\n").as_bytes(),
                format!("{ZERO} capabilities^{{}}\n").as_bytes(),
            ]),
        )
        .unwrap_err();
    }

    #[test]
    fn only_the_first_line_carries_capabilities() {
        let read = parse(
            ObjectFormat::Sha1,
            &advertisement(&[
                format!("{MAIN} refs/heads/main\0report-status agent=x\0atomic\n").as_bytes(),
                format!("{TAG} refs/tags/v1\0atomic\n").as_bytes(),
            ]),
        )
        .expect("well-formed");
        assert!(read.offers("report-status"));
        assert!(read.offers("agent=x"));
        assert!(!read.offers("atomic"));
        assert_eq!(
            read.refs.get("refs/tags/v1"),
            Some(&Oid::from_hex(TAG).expect("hex"))
        );
    }

    #[test]
    fn reads_a_shallow_remote_and_an_alternate_as_tips_only() {
        let read = parse(
            ObjectFormat::Sha1,
            &advertisement(&[
                format!("{MAIN} refs/heads/main\0report-status\n").as_bytes(),
                format!("{TAG} .have\n").as_bytes(),
                format!("shallow {PEELED}\n").as_bytes(),
            ]),
        )
        .expect("well-formed");
        assert_eq!(read.refs.len(), 1);
        assert_eq!(read.tips.len(), 3);

        // Refs come before the shallow lines, and a shallow line names a
        // commit.
        parse(
            ObjectFormat::Sha1,
            &advertisement(&[
                format!("shallow {PEELED}\n").as_bytes(),
                format!("{MAIN} refs/heads/main\n").as_bytes(),
            ]),
        )
        .unwrap_err();
        parse(ObjectFormat::Sha1, &advertisement(&[b"shallow nope\n"])).unwrap_err();
        parse(ObjectFormat::Sha1, &advertisement(&[b"not a ref\n"])).unwrap_err();
    }
}
