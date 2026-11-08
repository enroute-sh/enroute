//! Fetching a remote's refs, over git protocol v2's `ls-refs` and `fetch`.
//!
//! Version 2 only: it lets the fetch name the ref prefixes it reads, so a
//! remote with many `refs/pull/` refs does not list them all, and the hosts
//! this is for all speak it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Buf as _, Bytes, BytesMut};
use reqwest::RequestBuilder;
use reqwest::header::{ACCEPT, CONTENT_TYPE};

use crate::git::oid::{ObjectFormat, Oid};
use crate::meta::Map;
use crate::protocol::pktline::{self, Pkt};
use crate::protocol::receive_pack::PushCommand;
use crate::protocol::{AGENT, parse_format, same_format};
use crate::repo::view::Snapshot;

use super::advertise::ref_line;
use super::endpoint::Endpoint;
use super::{
    Client, Credentials, Error, PackStream, Remote, authorize, check_status, next_chunk, packets,
    read_capped, refuse_redirect, text,
};

/// The header that asks a smart-HTTP server for protocol v2.
const GIT_PROTOCOL: &str = "Git-Protocol";

/// The largest `ls-refs` answer this reads. Bounded by how many refs the
/// remote holds under the prefixes, not by a repository's size.
const MAX_REFS: usize = 64 << 20;

/// How long a fetch may take, end to end: a backstop against a remote that
/// sends a pack slowly for ever.
const FETCH_TIMEOUT: Duration = Duration::from_hours(1);

/// What a fetch asks for.
#[derive(Debug, Clone, Default)]
pub(crate) struct FetchRequest {
    /// Where it comes from.
    pub(crate) remote: Remote,
    /// The ref name prefixes this repository matches the remote under.
    pub(crate) prefixes: Vec<String>,
}

/// A fetch's result before it lands: the changes, and the pack carrying the
/// objects they need.
pub(crate) struct Fetched {
    /// The refs to change here, in name order. The all-zero id is no value,
    /// as on the wire: an `old` of zero creates the ref, a `new` of zero
    /// deletes it.
    pub(crate) changes: Vec<PushCommand>,
    /// The pack's bytes, without the side-band framing. `None` when this
    /// repository already holds every object the changes name.
    pub(crate) pack: Option<PackStream>,
    /// Why `pack` stopped early, once it has.
    pub(crate) failure: Failure,
}

impl std::fmt::Debug for Fetched {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("Fetched")
            .field("changes", &self.changes)
            .finish_non_exhaustive()
    }
}

/// The error that stopped a pack's stream.
///
/// The stream's reader gets only an I/O error, and the caller needs to know
/// whether the remote or the transport was at fault.
#[derive(Debug, Clone, Default)]
pub(crate) struct Failure(Arc<Mutex<Option<Error>>>);

impl Failure {
    /// The error, if the stream stopped early.
    pub(crate) fn take(&self) -> Option<Error> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    fn record(&self, error: Error) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
    }
}

/// What the remote offers over v2: each capability's name, with its value
/// if it has one.
type Capabilities = BTreeMap<String, String>;

impl Client {
    /// Read the remote's refs under `request.prefixes`, and start the pack
    /// carrying what `snap` lacks.
    ///
    /// # Errors
    ///
    /// Returns an error if the request names no prefix, or if the remote
    /// cannot be reached or dialed, or does not answer git v2.
    #[tracing::instrument(
        name = "enroute::remote::fetch",
        skip(self, snap, request),
        fields(prefixes = request.prefixes.len(), remote = tracing::field::Empty)
    )]
    pub(crate) async fn fetch(
        &self,
        snap: &Arc<Snapshot>,
        request: &FetchRequest,
    ) -> Result<Fetched, Error> {
        check_prefixes(&request.prefixes)?;
        let credentials = request.remote.credentials.as_ref();
        let endpoint = Endpoint::parse(&request.remote.url, self.reach)?;
        tracing::Span::current().record("remote", tracing::field::display(endpoint.url()));

        let capabilities = self.capabilities(&endpoint, credentials).await?;
        for needed in ["ls-refs", "fetch"] {
            if !capabilities.contains_key(needed) {
                return Err(Error::Unsupported(needed.into()));
            }
        }
        let local = snap.format();
        let format = object_format(&capabilities, local)?;
        let remote = self
            .ls_refs(&endpoint, credentials, format, &request.prefixes)
            .await?;

        let changes = plan(local, &request.prefixes, &snap.meta.refs, &remote);
        let wants: BTreeSet<Oid> = changes
            .iter()
            .map(|change| change.new)
            .filter(|new| !new.is_zero() && snap.lookup(new).is_none())
            .collect();
        if wants.is_empty() {
            return Ok(Fetched {
                changes,
                pack: None,
                failure: Failure::default(),
            });
        }
        // Only tips under the prefixes: they are what this repository shares
        // with the remote, and every other one costs the remote a walk.
        let haves: BTreeSet<Oid> = snap
            .meta
            .refs
            .iter()
            .filter(|&(name, _)| under(&request.prefixes, name))
            .map(|(_, &oid)| oid)
            .collect();
        let body = fetch_request(format, &wants, &haves)?;
        let response = command(&self.http, &endpoint, credentials)?
            .timeout(FETCH_TIMEOUT)
            .body(body)
            .send()
            .await?;
        check_status(&response)?;
        let failure = Failure::default();
        Ok(Fetched {
            changes,
            pack: Some(demux(response, failure.clone())),
            failure,
        })
    }

    /// Read what the remote offers, and that it speaks v2 at all.
    async fn capabilities(
        &self,
        endpoint: &Endpoint,
        credentials: Option<&Credentials>,
    ) -> Result<Capabilities, Error> {
        let response = authorize(
            self.http.get(endpoint.upload_pack_capabilities()?),
            credentials,
        )
        .header(ACCEPT, "application/x-git-upload-pack-advertisement")
        .header(GIT_PROTOCOL, "version=2")
        .timeout(super::DISCOVERY_TIMEOUT)
        .send()
        .await?;

        refuse_redirect(&response)?;
        check_status(&response)?;
        let body = read_capped(response, super::MAX_ADVERTISEMENT).await?;
        parse_capabilities(&body)
    }

    /// The remote's refs under `prefixes`.
    async fn ls_refs(
        &self,
        endpoint: &Endpoint,
        credentials: Option<&Credentials>,
        format: Option<ObjectFormat>,
        prefixes: &[String],
    ) -> Result<BTreeMap<String, Oid>, Error> {
        let mut arguments = Vec::new();
        for prefix in prefixes {
            line(&mut arguments, &format!("ref-prefix {prefix}"))?;
        }
        let body = request("ls-refs", format, &arguments)?;
        let response = command(&self.http, endpoint, credentials)?
            .timeout(super::DISCOVERY_TIMEOUT)
            .body(body)
            .send()
            .await?;
        check_status(&response)?;
        let body = read_capped(response, MAX_REFS).await?;
        parse_refs(format.unwrap_or_default(), &body, prefixes)
    }
}

/// Refuse prefixes that are not under `refs/`.
///
/// A prefix outside `refs/` would let a fetch write `HEAD` or a name git
/// does not read as a ref.
fn check_prefixes(prefixes: &[String]) -> Result<(), Error> {
    if prefixes.is_empty() {
        return Err(Error::Request("a fetch names no ref prefix".into()));
    }
    match prefixes.iter().find(|prefix| !prefix.starts_with("refs/")) {
        Some(prefix) => Err(Error::Request(format!(
            "the ref prefix {prefix:?} is not under refs/"
        ))),
        None => Ok(()),
    }
}

/// The `object-format` to name in each command, if the remote names one.
/// A remote that names none speaks SHA-1, as git assumes; one whose format
/// is not the repository's, `local`, is refused: its objects would have
/// other names here.
fn object_format(
    capabilities: &Capabilities,
    local: ObjectFormat,
) -> Result<Option<ObjectFormat>, Error> {
    let named = capabilities.get("object-format").map(String::as_str);
    named
        .map(parse_format)
        .transpose()
        .and_then(|remote| {
            same_format(remote.unwrap_or_default(), local)?;
            Ok(remote)
        })
        .map_err(|refusal| Error::Unsupported(refusal.to_string()))
}

/// A POST of one v2 command.
fn command(
    http: &reqwest::Client,
    endpoint: &Endpoint,
    credentials: Option<&Credentials>,
) -> Result<RequestBuilder, Error> {
    Ok(authorize(http.post(endpoint.upload_pack()?), credentials)
        .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
        .header(ACCEPT, "application/x-git-upload-pack-result")
        .header(GIT_PROTOCOL, "version=2"))
}

/// Read a v2 capability advertisement.
///
/// git's `http-backend` leaves out the `# service=` line for v2 and other
/// servers send it, so it is read when it is there, as git's client does.
fn parse_capabilities(body: &[u8]) -> Result<Capabilities, Error> {
    let lines: Vec<Pkt<'_>> = packets(body).collect::<Result<_, _>>()?;
    let mut lines = lines.into_iter().peekable();
    if let Some(&Pkt::Data(first)) = lines.peek()
        && pktline::chomp(first) == b"# service=git-upload-pack"
    {
        lines.next();
        if lines.next() != Some(Pkt::Flush) {
            return Err(Error::Protocol(
                "the remote's service line was not closed".into(),
            ));
        }
    }
    match lines.next() {
        Some(Pkt::Data(version)) if pktline::chomp(version) == b"version 2" => {}
        _ => return Err(Error::Unsupported("git protocol version 2".into())),
    }
    let mut capabilities = Capabilities::default();
    for packet in lines {
        let line = match packet {
            Pkt::Data(line) => pktline::chomp(line),
            Pkt::Flush => return Ok(capabilities),
            Pkt::Delim | Pkt::ResponseEnd => {
                return Err(Error::Protocol(
                    "invalid packet in the capability advertisement".into(),
                ));
            }
        };
        let line = String::from_utf8_lossy(line);
        let (name, value) = line.split_once('=').unwrap_or((&line, ""));
        capabilities.insert(name.to_owned(), value.to_owned());
    }
    Err(Error::Protocol(
        "the remote's capabilities ended before their flush".into(),
    ))
}

/// One v2 command: its name, the capabilities it is sent with, then its
/// arguments.
fn request(name: &str, format: Option<ObjectFormat>, arguments: &[u8]) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    line(&mut body, &format!("command={name}"))?;
    line(&mut body, &format!("agent={AGENT}"))?;
    if let Some(format) = format {
        line(&mut body, &format!("object-format={format}"))?;
    }
    pktline::delim(&mut body);
    body.extend_from_slice(arguments);
    pktline::flush(&mut body);
    Ok(body)
}

/// Append `text` and a newline as one pkt-line.
fn line(out: &mut Vec<u8>, text: &str) -> Result<(), Error> {
    pktline::try_data(out, format!("{text}\n").as_bytes())
        .map_err(|error| Error::Request(format!("{error:#}")))
}

/// The `fetch` command: the pack holds `wants` and leaves out what `haves`
/// reach.
///
/// `done` ends the negotiation in this one round. The haves are the tips of
/// a repository that is usually a copy of the remote, so a second round
/// would rarely save anything.
fn fetch_request(
    format: Option<ObjectFormat>,
    wants: &BTreeSet<Oid>,
    haves: &BTreeSet<Oid>,
) -> Result<Vec<u8>, Error> {
    let mut arguments = Vec::new();
    for argument in ["thin-pack", "ofs-delta", "no-progress"] {
        line(&mut arguments, argument)?;
    }
    for want in wants {
        line(&mut arguments, &format!("want {want}"))?;
    }
    for have in haves {
        line(&mut arguments, &format!("have {have}"))?;
    }
    line(&mut arguments, "done")?;
    request("fetch", format, &arguments)
}

/// Read an `ls-refs` answer: `<oid> <name>` lines, each perhaps with
/// attributes after it, to a flush.
///
/// A ref outside `prefixes` is dropped: a server may treat `ref-prefix` as a
/// hint, and this fetch changes nothing outside them.
fn parse_refs(
    format: ObjectFormat,
    body: &[u8],
    prefixes: &[String],
) -> Result<BTreeMap<String, Oid>, Error> {
    let mut refs = BTreeMap::new();
    for packet in packets(body) {
        let line = match packet? {
            Pkt::Data(line) => pktline::chomp(line),
            Pkt::Flush => return Ok(refs),
            Pkt::Delim | Pkt::ResponseEnd => {
                return Err(Error::Protocol("invalid packet in the ref listing".into()));
            }
        };
        let (oid, name) = ref_line(format, line)
            .ok_or_else(|| Error::Protocol(format!("not a ref listing line: {}", text(line))))?;
        let name = name.split(|&byte| byte == b' ').next().unwrap_or_default();
        let name = std::str::from_utf8(name)
            .map_err(|_utf8| Error::Protocol(format!("a ref name is not UTF-8: {}", text(line))))?;
        if !under(prefixes, name) {
            continue;
        }
        if refs.insert(name.to_owned(), oid).is_some() {
            return Err(Error::Protocol(format!("the remote lists {name} twice")));
        }
    }
    Err(Error::Protocol(
        "the remote's ref listing ended before its flush".into(),
    ))
}

fn under(prefixes: &[String], name: &str) -> bool {
    prefixes
        .iter()
        .any(|prefix| name.starts_with(prefix.as_str()))
}

/// The changes that make `local` match `remote` under `prefixes`, in name
/// order, with ids of `format`.
fn plan(
    format: ObjectFormat,
    prefixes: &[String],
    local: &Map<Oid>,
    remote: &BTreeMap<String, Oid>,
) -> Vec<PushCommand> {
    let local: BTreeMap<&str, Oid> = local
        .iter()
        .filter(|&(name, _)| under(prefixes, name))
        .map(|(name, &oid)| (name.as_str(), oid))
        .collect();
    let names: BTreeSet<&str> = local
        .keys()
        .copied()
        .chain(remote.keys().map(String::as_str))
        .collect();
    names
        .into_iter()
        .filter_map(|name| {
            let old = local.get(name).copied().unwrap_or(format.zero());
            let new = remote.get(name).copied().unwrap_or(format.zero());
            (old != new).then(|| PushCommand {
                name: name.to_owned(),
                old,
                new,
            })
        })
        .collect()
}

/// The pack in a `fetch` answer, out of its side-band packets as they
/// arrive. The error that ends it early goes to `failure` too.
fn demux(response: reqwest::Response, failure: Failure) -> PackStream {
    let state = (response, BytesMut::new(), false, failure);
    let stream = futures::stream::try_unfold(
        state,
        |(mut response, mut buf, mut in_pack, failure)| async move {
            match next_piece(&mut response, &mut buf, &mut in_pack).await {
                Ok(piece) => Ok(piece.map(|piece| (piece, (response, buf, in_pack, failure)))),
                Err(error) => {
                    let io = std::io::Error::other(error.to_string());
                    failure.record(error);
                    Err(io)
                }
            }
        },
    );
    Box::pin(stream)
}

/// The next piece of the pack, or `None` at its end. `in_pack` is whether
/// the `packfile` line has gone by.
async fn next_piece(
    response: &mut reqwest::Response,
    buf: &mut BytesMut,
    in_pack: &mut bool,
) -> Result<Option<Bytes>, Error> {
    loop {
        let Some(packet) = take_packet(buf)? else {
            match next_chunk(response).await? {
                Some(chunk) => buf.extend_from_slice(&chunk),
                None => {
                    return Err(Error::Protocol(
                        "the remote's pack ended before its flush".into(),
                    ));
                }
            }
            continue;
        };
        match (*in_pack, packet) {
            (false, Packet::Data(line)) if pktline::chomp(&line) == b"packfile" => {
                *in_pack = true;
            }
            (false, Packet::Data(line)) => {
                return Err(Error::Protocol(format!(
                    "expected the packfile section, got: {}",
                    text(&line)
                )));
            }
            (true, Packet::Data(payload)) => {
                let Some((&band, data)) = payload.split_first() else {
                    continue;
                };
                match band {
                    1 => return Ok(Some(payload.slice(1..))),
                    // Progress, though none was asked for.
                    2 => {}
                    3 => {
                        return Err(Error::Remote(format!(
                            "the remote failed the fetch: {}",
                            text(data)
                        )));
                    }
                    other => {
                        return Err(Error::Protocol(format!("unknown side-band {other}")));
                    }
                }
            }
            (true, Packet::Flush) => return Ok(None),
            (false, Packet::Flush) | (_, Packet::Special) => {
                return Err(Error::Protocol("invalid packet in the fetch answer".into()));
            }
        }
    }
}

/// One packet taken off the front of `buf`.
enum Packet {
    Data(Bytes),
    Flush,
    /// A delimiter or a response end, which a pack answer does not hold.
    Special,
}

/// The packet at the front of `buf`, or `None` until all of it is there.
fn take_packet(buf: &mut BytesMut) -> Result<Option<Packet>, Error> {
    let Some(prefix) = buf.get(..4) else {
        return Ok(None);
    };
    let length = pktline::length(prefix).map_err(|error| Error::Protocol(format!("{error:#}")))?;
    let packet = match length {
        0 => Packet::Flush,
        1 | 2 => Packet::Special,
        3 => return Err(Error::Protocol("invalid pkt-line length 3".into())),
        length if buf.len() < length => return Ok(None),
        length => {
            let mut packet = buf.split_to(length).freeze();
            packet.advance(4);
            return Ok(Some(Packet::Data(packet)));
        }
    };
    buf.advance(4);
    Ok(Some(packet))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::pktline::{delim, flush, try_data};

    fn oid(byte: char) -> Oid {
        Oid::from_hex(&String::from(byte).repeat(40)).expect("hex")
    }

    fn packets_of(lines: &[&[u8]]) -> Vec<u8> {
        let mut body = Vec::new();
        for line in lines {
            if line.is_empty() {
                flush(&mut body);
            } else {
                try_data(&mut body, line).expect("encoded");
            }
        }
        body
    }

    #[test]
    fn reads_capabilities_with_and_without_the_service_line() {
        let bare = packets_of(&[b"version 2\n", b"ls-refs=unborn\n", b"fetch\n", b""]);
        let mut served = packets_of(&[b"# service=git-upload-pack\n", b""]);
        served.extend_from_slice(&bare);

        for body in [bare, served] {
            let capabilities = parse_capabilities(&body).expect("read");
            assert!(capabilities.contains_key("ls-refs"));
            assert!(capabilities.contains_key("fetch"));
            assert!(!capabilities.contains_key("object-format"));
        }
    }

    /// A v0 server answers with its refs; that is not an answer to read
    /// as v2.
    #[test]
    fn a_remote_without_v2_is_refused() {
        let body = packets_of(&[
            b"# service=git-upload-pack\n",
            b"",
            format!("{} refs/heads/main\0multi_ack\n", oid('a')).as_bytes(),
            b"",
        ]);
        assert!(matches!(
            parse_capabilities(&body),
            Err(Error::Unsupported(_))
        ));
    }

    #[test]
    fn another_object_format_is_refused() {
        let mut capabilities = Capabilities::default();
        capabilities.insert("object-format".into(), "sha256".into());
        assert!(matches!(
            object_format(&capabilities, ObjectFormat::Sha1),
            Err(Error::Unsupported(_))
        ));
    }

    #[test]
    fn reads_refs_under_the_prefixes_and_drops_their_attributes() {
        let body = packets_of(&[
            format!("{} refs/heads/main\n", oid('a')).as_bytes(),
            format!("{} refs/tags/v1 peeled:{}\n", oid('b'), oid('c')).as_bytes(),
            format!("{} refs/pull/1/head\n", oid('d')).as_bytes(),
            b"",
        ]);
        let prefixes = vec!["refs/heads/".to_owned(), "refs/tags/".to_owned()];

        let refs = parse_refs(ObjectFormat::Sha1, &body, &prefixes).expect("read");

        assert_eq!(refs.get("refs/heads/main"), Some(&oid('a')));
        assert_eq!(refs.get("refs/tags/v1"), Some(&oid('b')));
        assert_eq!(refs.len(), 2);
    }

    /// A listing cut off before its flush is not read as the remote's refs:
    /// the missing ones would be deleted here.
    #[test]
    fn a_ref_listing_without_its_flush_is_refused() {
        let body = packets_of(&[format!("{} refs/heads/main\n", oid('a')).as_bytes()]);
        parse_refs(ObjectFormat::Sha1, &body, &["refs/".to_owned()]).unwrap_err();
    }

    #[test]
    fn the_plan_creates_moves_and_deletes_only_under_the_prefixes() {
        let prefixes = vec!["refs/heads/".to_owned()];
        let local: Map<Oid> = [
            ("refs/heads/gone".to_owned(), oid('a')),
            ("refs/heads/main".to_owned(), oid('b')),
            ("refs/heads/same".to_owned(), oid('c')),
            ("refs/notes/x".to_owned(), oid('d')),
        ]
        .into_iter()
        .collect();
        let remote = BTreeMap::from([
            ("refs/heads/main".to_owned(), oid('e')),
            ("refs/heads/new".to_owned(), oid('f')),
            ("refs/heads/same".to_owned(), oid('c')),
        ]);

        let changes: Vec<_> = plan(ObjectFormat::Sha1, &prefixes, &local, &remote)
            .into_iter()
            .map(|change| (change.name, change.old, change.new))
            .collect();

        assert_eq!(
            changes,
            [
                ("refs/heads/gone".to_owned(), oid('a'), Oid::ZERO),
                ("refs/heads/main".to_owned(), oid('b'), oid('e')),
                ("refs/heads/new".to_owned(), Oid::ZERO, oid('f')),
            ]
        );
    }

    #[test]
    fn prefixes_outside_refs_are_refused() {
        check_prefixes(&[]).unwrap_err();
        check_prefixes(&["HEAD".to_owned()]).unwrap_err();
        check_prefixes(&["refs/heads/".to_owned()]).unwrap();
    }

    #[test]
    fn the_fetch_command_ends_its_one_round_with_done() {
        let body = fetch_request(
            Some(ObjectFormat::Sha1),
            &BTreeSet::from([oid('a')]),
            &BTreeSet::from([oid('b')]),
        )
        .expect("encoded");

        let mut expected = Vec::new();
        for line in [
            "command=fetch\n".to_owned(),
            format!("agent={AGENT}\n"),
            "object-format=sha1\n".to_owned(),
        ] {
            try_data(&mut expected, line.as_bytes()).expect("encoded");
        }
        delim(&mut expected);
        for line in [
            "thin-pack\n".to_owned(),
            "ofs-delta\n".to_owned(),
            "no-progress\n".to_owned(),
            format!("want {}\n", oid('a')),
            format!("have {}\n", oid('b')),
            "done\n".to_owned(),
        ] {
            try_data(&mut expected, line.as_bytes()).expect("encoded");
        }
        flush(&mut expected);
        assert_eq!(body, expected);
    }

    #[test]
    fn packets_are_taken_only_once_they_have_fully_arrived() {
        let mut buf = BytesMut::from(&b"0009\x01pack0000"[..]);
        buf.truncate(6);
        assert!(take_packet(&mut buf).expect("framed").is_none());

        let mut buf = BytesMut::from(&b"0009\x01pack0000"[..]);
        let Some(Packet::Data(data)) = take_packet(&mut buf).expect("framed") else {
            panic!("a data packet")
        };
        assert_eq!(&data[..], b"\x01pack");
        assert!(matches!(take_packet(&mut buf), Ok(Some(Packet::Flush))));
        assert!(buf.is_empty());
    }
}
