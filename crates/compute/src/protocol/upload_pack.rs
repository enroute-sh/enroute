//! Protocol v2 `git-upload-pack`: capability advertisement, `ls-refs` and `fetch`.

use std::sync::Arc;

use anyhow::{Result, bail};
use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::Instrument as _;

use crate::git::oid::{ObjectFormat, Oid};
use crate::query::RefView;
use crate::repo::serve::{FetchArgs, Filter, Plan, plan, write_pack};
use crate::repo::view::Snapshot;

use super::AGENT;
use super::Refusal;
use super::pktline::{self, Pkt, Reader};

/// What this side advertises: each capability's name, its value, and
/// whether it is a command. As git's `capabilities[]`, the one list both
/// writes the advertisement and decides which keys a request may send.
/// `object-format` follows them, with the repository's format.
const CAPABILITIES: [(&str, Option<&str>, bool); 4] = [
    ("agent", Some(AGENT), false),
    ("ls-refs", Some("unborn"), true),
    ("fetch", Some("shallow wait-for-done filter"), true),
    ("server-option", None, false),
];

/// The capability advertisement of a `format` repository.
pub(crate) fn advertisement(format: ObjectFormat) -> Vec<u8> {
    let mut out = Vec::new();
    pktline::line(&mut out, "version 2\n");
    for (name, value, _) in CAPABILITIES {
        match value {
            Some(v) => pktline::line(&mut out, &format!("{name}={v}\n")),
            None => pktline::line(&mut out, &format!("{name}\n")),
        }
    }
    pktline::line(&mut out, &format!("object-format={format}\n"));
    pktline::flush(&mut out);
    out
}

/// Whether `name` is an advertised command (or else capability).
fn advertised(name: &str, command: bool) -> bool {
    CAPABILITIES
        .iter()
        .any(|&(n, _, c)| n == name && c == command)
}

#[derive(Default)]
pub(crate) struct Request<'a> {
    /// `ls-refs` or `fetch`, or empty when the client asked for nothing.
    pub command: &'a str,
    pub args: Vec<&'a str>,
    /// The object format the client speaks: SHA-1 unless it says
    /// otherwise, as git assumes. It must be the repository's (see
    /// [`Request::check_format`]).
    pub format: ObjectFormat,
}

impl Request<'_> {
    /// Refuse a request in another format than the repository's, `server`,
    /// as git's `process_request` does.
    pub(crate) fn check_format(&self, server: ObjectFormat) -> Result<(), Refusal> {
        if self.command.is_empty() {
            return Ok(());
        }
        super::same_format(server, self.format)
    }
}

/// Read a request as git's `process_request` does: capabilities and the
/// command, then after a delim the command's arguments, up to a flush.
pub(crate) fn parse_request(body: &[u8]) -> Result<Request<'_>> {
    let mut r = Reader::new(body);
    let mut command = None;
    let mut format = ObjectFormat::Sha1;
    let mut seen = false;
    let has_args = loop {
        match r.next_line()? {
            // A client that closes before a request, or sends only a flush,
            // is done with the connection.
            None if r.pos == 0 => return Ok(Request::default()),
            None => bail!(Refusal::new("the remote end hung up unexpectedly")),
            Some(Ok(l)) => {
                parse_key(l, &mut command, &mut format)?;
                seen = true;
            }
            Some(Err(Pkt::Flush)) if !seen => return Ok(Request::default()),
            Some(Err(Pkt::Flush)) => break false,
            Some(Err(Pkt::Delim)) => break true,
            Some(Err(p)) => bail!(Refusal::new(format!("unexpected packet {p:?}"))),
        }
    };
    let command = command.ok_or_else(|| Refusal::new("no command requested"))?;
    let args = if has_args {
        read_args(&mut r, command)?
    } else {
        Vec::new()
    };
    Ok(Request {
        command,
        args,
        format,
    })
}

/// The arguments of `command`, which a flush must end.
fn read_args<'a>(r: &mut Reader<'a>, command: &str) -> Result<Vec<&'a str>> {
    let mut args = Vec::new();
    loop {
        match r.next_line()? {
            Some(Ok(l)) => args.push(l),
            Some(Err(Pkt::Flush)) => return Ok(args),
            _ => bail!(Refusal::new(format!(
                "expected flush after {command} arguments"
            ))),
        }
    }
}

/// One line before the arguments: the command, or a capability that this
/// side advertises.
fn parse_key<'a>(
    line: &'a str,
    command: &mut Option<&'a str>,
    format: &mut ObjectFormat,
) -> Result<()> {
    if let Some(c) = line.strip_prefix("command=") {
        if let Some(before) = command {
            bail!(Refusal::new(format!(
                "command '{c}' requested after already requesting command '{before}'"
            )));
        }
        if !advertised(c, true) {
            bail!(Refusal::new(format!("invalid command '{c}'")));
        }
        *command = Some(c);
        return Ok(());
    }
    let (key, value) = match line.split_once('=') {
        Some((key, value)) => (key, Some(value)),
        None => (line, None),
    };
    match (key, value) {
        ("object-format", None) => bail!(Refusal::new(
            "object-format capability requires an argument"
        )),
        ("object-format", Some(v)) => {
            *format = super::parse_format(v)?;
            Ok(())
        }
        _ if advertised(key, false) => Ok(()),
        _ => bail!(Refusal::new(format!("unknown capability '{line}'"))),
    }
}

/// What an `ls-refs` asks for.
#[derive(Debug, Default)]
pub(crate) struct LsRefs<'a> {
    pub symrefs: bool,
    pub peel: bool,
    pub unborn: bool,
    pub prefixes: Vec<&'a str>,
}

/// As git: past this many prefixes, they are dropped, and every ref is
/// listed.
const TOO_MANY_PREFIXES: usize = 65536;

/// Parse the arguments of `ls-refs`.
pub(crate) fn parse_ls_refs<'a>(args: &[&'a str]) -> Result<LsRefs<'a>> {
    let mut ask = LsRefs::default();
    for a in args {
        match *a {
            "symrefs" => ask.symrefs = true,
            "peel" => ask.peel = true,
            "unborn" => ask.unborn = true,
            _ => ask.prefixes.push(
                a.strip_prefix("ref-prefix ")
                    .ok_or_else(|| Refusal::new(format!("unexpected line: '{a}'")))?,
            ),
        }
    }
    if ask.prefixes.len() >= TOO_MANY_PREFIXES {
        ask.prefixes.clear();
    }
    Ok(ask)
}

/// Answer `ls-refs` with `view`, naming `HEAD`'s branch if `symrefs` and an
/// unborn `HEAD` if `unborn`.
pub(crate) fn ls_refs(view: &RefView<'_>, symrefs: bool, unborn: bool) -> Vec<u8> {
    let mut out = Vec::new();
    match view.head {
        Some((branch, Some(oid))) => {
            let symref = if symrefs {
                format!(" symref-target:{branch}")
            } else {
                String::new()
            };
            pktline::line(&mut out, &format!("{oid} HEAD{symref}\n"));
        }
        // As git, an unborn HEAD is listed only with its target.
        Some((branch, None)) if unborn && symrefs => {
            pktline::line(&mut out, &format!("unborn HEAD symref-target:{branch}\n"));
        }
        _ => {}
    }
    for (name, oid) in &view.refs {
        let peeled = view
            .peeled
            .get(oid)
            .map(|p| format!(" peeled:{p}"))
            .unwrap_or_default();
        pktline::line(&mut out, &format!("{oid} {name}{peeled}\n"));
    }
    pktline::flush(&mut out);
    out
}

/// Read into `args` one line that both protocols of a fetch share, as its
/// `key` and `value`, the way git's upload-pack reads it, with ids of
/// `format`. Whether it was one.
pub(super) fn fetch_line(
    format: ObjectFormat,
    args: &mut FetchArgs,
    key: &str,
    value: &str,
) -> Result<bool> {
    // The message is built only for a bad line: a fetch can send many
    // thousands of good ones.
    let oid = |error: fn(&str) -> String| {
        super::oid_prefix(format, value.as_bytes()).ok_or_else(|| Refusal::new(error(value)))
    };
    match key {
        "want" => args.wants.push(oid(|v| {
            format!("git upload-pack: protocol error, expected to get oid, not 'want {v}'")
        })?),
        "have" => args.haves.push(oid(|v| {
            format!("git upload-pack: expected SHA1 object, got '{v}'")
        })?),
        "shallow" => args
            .client_shallows
            .push(oid(|v| format!("invalid shallow line: shallow {v}"))?),
        "deepen" => {
            let depth = deepen(value)
                .ok_or_else(|| Refusal::new(format!("Invalid deepen: deepen {value}")))?;
            args.deepen = Some(depth);
        }
        "filter" => {
            if args.filter != Filter::None {
                bail!(Refusal::new("multiple filter-specs cannot be combined"));
            }
            args.filter = Filter::parse(value)?;
        }
        "deepen-since" | "deepen-not" => bail!(Refusal::new(format!("{key} is not supported"))),
        _ => return Ok(false),
    }
    Ok(true)
}

/// A depth as git reads it: `(int)strtol(value, &end, 0)`, which must use
/// all of `value` and be positive.
fn deepen(value: &str) -> Option<u32> {
    let (negative, magnitude, rest) = crate::util::c_number(value)?;
    if !rest.is_empty() {
        return None;
    }
    // strtol saturates to a 64-bit long, and the cast keeps its low 32 bits.
    let long = match (negative, magnitude.and_then(|m| i64::try_from(m).ok())) {
        (false, Some(m)) => m,
        (false, None) => i64::MAX,
        (true, Some(m)) => -m,
        (true, None) => i64::MIN,
    };
    let low = u32::try_from(long.rem_euclid(1 << 32)).ok()?;
    (1..1 << 31).contains(&low).then_some(low)
}

/// The haves that `snap` holds, in the client's order.
pub(super) fn common_haves(snap: &Snapshot, haves: &[Oid]) -> Vec<Oid> {
    haves
        .iter()
        .filter(|h| snap.lookup(h).is_some())
        .copied()
        .collect()
}

/// The `shallow` and `unshallow` lines of `plan`.
pub(super) fn write_shallow_lines(out: &mut Vec<u8>, plan: &Plan) {
    for s in &plan.shallow {
        pktline::line(out, &format!("shallow {s}\n"));
    }
    for s in &plan.unshallow {
        pktline::line(out, &format!("unshallow {s}\n"));
    }
}

pub(crate) struct FetchRequest {
    pub args: FetchArgs,
    pub done: bool,
    pub wait_for_done: bool,
}

/// Parse the arguments of `fetch` of a `format` repository, as git's
/// `process_args` does: a line that this side did not advertise is refused.
pub(crate) fn parse_fetch(format: ObjectFormat, args: &[&str]) -> Result<FetchRequest> {
    let mut req = FetchRequest {
        args: FetchArgs::default(),
        done: false,
        wait_for_done: false,
    };
    for l in args {
        if let Some((k, v)) = l.split_once(' ')
            && fetch_line(format, &mut req.args, k, v)?
        {
            continue;
        }
        match *l {
            "done" => req.done = true,
            "include-tag" => req.args.include_tag = true,
            "wait-for-done" => req.wait_for_done = true,
            "deepen-relative" => req.args.deepen_relative = true,
            "ofs-delta" => req.args.ofs_delta = true,
            // Enroute never sends a thin pack, nor progress.
            "thin-pack" | "no-progress" => {}
            _ => bail!(Refusal::new(format!("unexpected line: '{l}'"))),
        }
    }
    Ok(req)
}

/// Handle `fetch` (as [`parse_fetch`] reads it). Returns the response as a
/// stream of chunks.
pub(crate) fn fetch(snap: Arc<Snapshot>, req: FetchRequest) -> mpsc::Receiver<Result<Bytes>> {
    let mut pre = Vec::new();
    // As git: no wants is a fetch of nothing, and a fetch with no haves
    // has nothing to acknowledge.
    if req.args.wants.is_empty() && !req.wait_for_done {
        return reply_now(pre);
    }
    if !req.done && !req.args.haves.is_empty() {
        let common = common_haves(&snap, &req.args.haves);
        pktline::line(&mut pre, "acknowledgments\n");
        if common.is_empty() {
            pktline::line(&mut pre, "NAK\n");
        }
        for c in &common {
            pktline::line(&mut pre, &format!("ACK {c}\n"));
        }
        let ready = !req.wait_for_done && !common.is_empty();
        if !ready {
            pktline::flush(&mut pre);
            return reply_now(pre);
        }
        pktline::line(&mut pre, "ready\n");
        pktline::delim(&mut pre);
    }
    spawn_respond(snap, true, |snap| async move {
        let p = plan(&snap, &req.args).await?;
        if !p.shallow.is_empty() || !p.unshallow.is_empty() || req.args.deepen.is_some() {
            pktline::line(&mut pre, "shallow-info\n");
            write_shallow_lines(&mut pre, &p);
            pktline::delim(&mut pre);
        }
        pktline::line(&mut pre, "packfile\n");
        Ok((pre, Some(p)))
    })
}

/// [`respond`] on a task of its own, into the stream it returns. `head`
/// gets the snapshot to plan with.
pub(super) fn spawn_respond<F>(
    snap: Arc<Snapshot>,
    sideband: bool,
    head: impl FnOnce(Arc<Snapshot>) -> F,
) -> mpsc::Receiver<Result<Bytes>>
where
    F: Future<Output = Result<(Vec<u8>, Option<Plan>)>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<Bytes>>(16);
    // Under the request's span, which ends only when the pack is sent.
    let work = respond(snap.clone(), head(snap), sideband, tx);
    tokio::spawn(work.instrument(tracing::Span::current()));
    rx
}

/// A response of `bytes` alone, with no pack and no plan: empty bytes are
/// an empty response.
pub(super) fn reply_now(bytes: Vec<u8>) -> mpsc::Receiver<Result<Bytes>> {
    let (tx, rx) = mpsc::channel::<Result<Bytes>>(1);
    if !bytes.is_empty() {
        // A new channel has room for one.
        drop(tx.try_send(Ok(Bytes::from(bytes))));
    }
    rx
}

/// Answer a fetch (either protocol): what `head` produces (the response up
/// to the pack, and the pack's plan, if one is sent), then the pack,
/// side-band multiplexed if `sideband` (and then ended by a flush). An
/// error before anything was sent is answered with `ERR`, one after on
/// side-band 3 (if multiplexed).
async fn respond(
    snap: Arc<Snapshot>,
    head: impl Future<Output = Result<(Vec<u8>, Option<Plan>)>>,
    sideband: bool,
    tx: mpsc::Sender<Result<Bytes>>,
) {
    let gone = |_| anyhow::anyhow!("client went away");
    let mut sent = false;
    let res: Result<()> = async {
        let (pre, plan) = head
            .instrument(tracing::info_span!("enroute::fetch::plan"))
            .await?;
        sent = true;
        tx.send(Ok(Bytes::from(pre))).await.map_err(gone)?;
        let Some(p) = plan else { return Ok(()) };
        let sink = crate::util::channel_sink(tx.clone(), move |chunk| {
            if !sideband {
                return Bytes::copy_from_slice(chunk);
            }
            let mut out = Vec::with_capacity(chunk.len() + 16);
            pktline::sideband(&mut out, 1, chunk);
            Bytes::from(out)
        });
        let objects = p.objects.len();
        write_pack(snap, p, sink)
            .instrument(tracing::info_span!("enroute::fetch::write_pack", objects))
            .await?;
        if sideband {
            let mut end = Vec::new();
            pktline::flush(&mut end);
            tx.send(Ok(Bytes::from(end))).await.map_err(gone)?;
        }
        Ok(())
    }
    .await;
    if let Err(e) = res {
        tracing::warn!("fetch failed: {e:#}");
        let mut out = Vec::new();
        match (sent, sideband) {
            (false, _) => pktline::line(&mut out, &format!("ERR {}\n", super::client_message(&e))),
            (true, true) => {
                let message = format!("enroute: {}\n", super::client_message(&e));
                pktline::sideband(&mut out, 3, message.as_bytes());
            }
            (true, false) => return,
        }
        pktline::flush(&mut out);
        // A closed receiver means the client left; nobody reads the error.
        drop(tx.send(Ok(Bytes::from(out))).await);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(keys: &[&str], args: Option<&[&str]>) -> Vec<u8> {
        let mut out = Vec::new();
        for k in keys {
            pktline::line(&mut out, &format!("{k}\n"));
        }
        if let Some(args) = args {
            pktline::delim(&mut out);
            for a in args {
                pktline::line(&mut out, &format!("{a}\n"));
            }
        }
        pktline::flush(&mut out);
        out
    }

    #[test]
    fn reads_requests_as_git_does() {
        let body = request(
            &[
                "command=fetch",
                "agent=git/2.51",
                "object-format=sha1",
                "server-option=x",
            ],
            Some(&["done"]),
        );
        let req = parse_request(&body).expect("parsed");
        assert_eq!((req.command, req.args), ("fetch", vec!["done"]));
        // Nothing sent, or a bare flush, ends the session.
        assert_eq!(parse_request(b"").expect("ended").command, "");
        assert_eq!(parse_request(b"0000").expect("ended").command, "");
    }

    #[test]
    fn refuses_requests_git_refuses() {
        for body in [
            request(&["command=fetch", "command=fetch"], None),
            request(&["command=ls-refs", "command=fetch"], None),
            request(&["command=push"], None),
            request(&["command=fetch", "nope=1"], None),
            request(&["command=fetch", "session-id=y"], None),
            request(&["command=fetch", "object-format=sha3"], None),
            request(&["agent=git/2.51"], None),
        ] {
            assert!(
                parse_request(&body).is_err(),
                "{}",
                String::from_utf8_lossy(&body)
            );
        }
        // A client of the other format is refused once the repository's is
        // known.
        let other = request(&["command=fetch", "object-format=sha256"], None);
        let req = parse_request(&other).expect("parsed");
        assert!(req.check_format(ObjectFormat::Sha1).is_err());
        req.check_format(ObjectFormat::Sha256).expect("same format");
        let plain = request(&["command=fetch"], None);
        let req = parse_request(&plain).expect("parsed");
        assert!(req.check_format(ObjectFormat::Sha256).is_err());
        // The arguments must end in a flush.
        let mut cut = request(&["command=fetch"], Some(&["done"]));
        cut.truncate(cut.len() - 4);
        parse_request(&cut).err().expect("no flush");
    }

    #[test]
    fn reads_fetch_arguments_as_git_does() {
        let a = "c058a00c064d286f3d5e059266726fd6c76d3753";
        let want = format!("want {a}junk");
        let upper = format!("want {}", a.to_uppercase());
        let have = format!("have {a} x");
        let req = parse_fetch(
            ObjectFormat::Sha1,
            &[&want, &upper, &have, "thin-pack", "deepen 010"],
        )
        .expect("parsed");
        assert_eq!(req.args.wants.len(), 2);
        assert_eq!(req.args.haves.len(), 1);
        assert_eq!(req.args.deepen, Some(8));
        for bad in [
            "bogus",
            "done x",
            "want-ref refs/heads/main",
            "deepen 0",
            "deepen 4294967296",
            "deepen 2x",
            "want abc",
        ] {
            parse_fetch(ObjectFormat::Sha1, &[bad]).err().expect(bad);
        }
        parse_fetch(ObjectFormat::Sha1, &["filter blob:none", "filter tree:0"])
            .err()
            .expect("two filters");
        let hex = parse_fetch(ObjectFormat::Sha1, &["filter blob:limit=0x10"]).expect("parsed");
        assert_eq!(hex.args.filter, Filter::BlobLimit(16));
    }

    #[test]
    fn reads_ls_refs_arguments_as_git_does() {
        parse_ls_refs(&["symrefs", "peel", "unborn", "ref-prefix refs/"]).expect("parsed");
        parse_ls_refs(&["bogus"]).expect_err("unknown");
    }
}
