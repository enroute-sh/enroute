//! What the remote says it holds, read from its `receive-pack`
//! advertisement.
//!
//! Two answers come out of it: where each of the remote's refs points, which
//! is what a push's commands are built against, and every object id it named,
//! which is what the pack is cut down by.

use std::collections::{HashMap, HashSet};

use gix_hash::ObjectId;

use enroute_git_proto::pktline::{Packet, Packets, text, trim_lf};

use crate::Error;

/// The pseudo-ref an empty repository advertises its capabilities on.
const NO_REFS: &str = "capabilities^{}";

/// What a remote holds, and what it can do.
#[derive(Debug, Default)]
pub(crate) struct Advertisement {
    /// Where each of the remote's refs points.
    pub(crate) refs: HashMap<String, ObjectId>,
    /// Every object id advertised, peeled tag targets included, each of
    /// which the remote holds and the pack need not carry.
    pub(crate) tips: Vec<ObjectId>,
    /// What the remote offers, as the tokens it wrote them in.
    pub(crate) capabilities: HashSet<String>,
}

impl Advertisement {
    pub(crate) fn offers(&self, capability: &str) -> bool {
        self.capabilities.contains(capability)
    }
}

/// Read the advertisement a remote answered `GET /info/refs` with.
pub(crate) fn parse(body: &[u8]) -> Result<Advertisement, Error> {
    let mut packets = Packets::new(body);
    service_header(&mut packets)?;

    let mut advertisement = Advertisement::default();
    for packet in packets {
        match packet? {
            Packet::Data(line) => read_ref(&mut advertisement, line)?,
            // The advertisement ends at the first flush; anything a server
            // sends after it is not part of this answer.
            Packet::Flush => break,
            Packet::Delimiter | Packet::ResponseEnd => {}
        }
    }
    Ok(advertisement)
}

/// Check the `# service=git-receive-pack` line every smart-HTTP server opens
/// with, so a proxy's error page is not read as an empty repository.
fn service_header(packets: &mut Packets<'_>) -> Result<(), Error> {
    let first = packets
        .next()
        .transpose()?
        .ok_or_else(|| Error::Protocol("the remote answered with nothing".into()))?;
    match first {
        Packet::Data(line) if trim_lf(line) == b"# service=git-receive-pack" => {}
        Packet::Data(line) => {
            return Err(Error::Protocol(format!(
                "the remote does not serve receive-pack here: {}",
                text(line)
            )));
        }
        Packet::Flush | Packet::Delimiter | Packet::ResponseEnd => {
            return Err(Error::Protocol(
                "the remote named no service in its advertisement".into(),
            ));
        }
    }
    // The service line stands alone in a section of its own.
    match packets.next().transpose()? {
        Some(Packet::Flush) => Ok(()),
        _ => Err(Error::Protocol(
            "the remote's service line was not closed".into(),
        )),
    }
}

/// Read one `<oid> <refname>` line, with the capability list the first of
/// them carries.
fn read_ref(advertisement: &mut Advertisement, line: &[u8]) -> Result<(), Error> {
    let line = trim_lf(line);
    let (oid, rest) = split_once(line, b' ')
        .ok_or_else(|| Error::Protocol(format!("not an advertised ref: {}", text(line))))?;
    let oid = ObjectId::from_hex(oid)
        .map_err(|error| Error::Protocol(format!("not an object id: {error}")))?;

    let (name, capabilities) = match split_once(rest, 0) {
        Some((name, capabilities)) => (name, Some(capabilities)),
        None => (rest, None),
    };
    if let Some(capabilities) = capabilities {
        advertisement.capabilities.extend(
            String::from_utf8_lossy(capabilities)
                .split_whitespace()
                .map(str::to_owned),
        );
    }

    let name = String::from_utf8_lossy(name).into_owned();
    if name == NO_REFS {
        return Ok(());
    }
    advertisement.tips.push(oid);
    // A peeled tag names an object the remote holds, which is why it is a
    // tip, but it is not a ref anything can be pushed onto.
    if !name.ends_with("^{}") {
        advertisement.refs.insert(name, oid);
    }
    Ok(())
}

fn split_once(line: &[u8], at: u8) -> Option<(&[u8], &[u8])> {
    let index = line.iter().position(|byte| *byte == at)?;
    let (before, after) = line.split_at_checked(index)?;
    Some((before, after.get(1..)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use enroute_git_proto::pktline::Body;

    /// The bytes a server writes, so the test reads what production reads.
    fn advertisement(lines: &[&[u8]]) -> Vec<u8> {
        let mut body = Body::new();
        body.line(b"# service=git-receive-pack\n").expect("encoded");
        body.flush();
        for line in lines {
            body.line(line).expect("encoded");
        }
        body.flush();
        body.into_bytes()
    }

    const MAIN: &str = "1111111111111111111111111111111111111111";
    const TAG: &str = "2222222222222222222222222222222222222222";
    const PEELED: &str = "3333333333333333333333333333333333333333";

    #[test]
    fn reads_refs_capabilities_and_every_tip() {
        let body = advertisement(&[
            format!("{MAIN} refs/heads/main\0report-status atomic agent=git/2.45\n").as_bytes(),
            format!("{TAG} refs/tags/v1\n").as_bytes(),
            format!("{PEELED} refs/tags/v1^{{}}\n").as_bytes(),
        ]);
        let read = parse(&body).expect("well-formed");

        assert_eq!(read.refs.len(), 2);
        assert_eq!(
            read.refs.get("refs/heads/main"),
            Some(&ObjectId::from_hex(MAIN.as_bytes()).expect("hex"))
        );
        assert!(!read.refs.contains_key("refs/tags/v1^{}"));
        assert_eq!(read.tips.len(), 3);
        assert!(read.offers("atomic"));
        assert!(read.offers("report-status"));
    }

    #[test]
    fn reads_an_empty_repository_as_no_refs_and_still_reads_its_capabilities() {
        let zero = "0".repeat(40);
        let body =
            advertisement(&[format!("{zero} capabilities^{{}}\0report-status\n").as_bytes()]);
        let read = parse(&body).expect("well-formed");

        assert!(read.refs.is_empty());
        assert!(read.tips.is_empty());
        assert!(read.offers("report-status"));
    }

    #[test]
    fn refuses_an_answer_that_is_not_a_receive_pack_advertisement() {
        let mut body = Body::new();
        body.line(b"# service=git-upload-pack\n").expect("encoded");
        body.flush();

        parse(&body.into_bytes()).unwrap_err();
        parse(b"").unwrap_err();
    }
}
