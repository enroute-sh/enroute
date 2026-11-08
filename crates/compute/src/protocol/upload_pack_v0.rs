//! Protocol v0/v1 `git-upload-pack` over smart HTTP (stateless RPC).
//!
//! Some clients still fetch this way (notably go-git), so it is served from
//! the same planner and pack streaming as v2; only the wire format differs:
//!
//! - the ref advertisement carries capabilities on its first line,
//! - each POST holds all wants (with capabilities on the first), optional
//!   shallow/deepen/filter lines, a flush, then the haves of all rounds so
//!   far and, eventually, `done`,
//! - negotiation uses `multi_ack_detailed`: `ACK <id> common` per known have,
//!   `ACK <id> ready` once we can send a pack, and `NAK` ending each round,
//! - shallow updates are sent at the start of every response when a deepen
//!   is requested, and the pack is side-band multiplexed if asked for.

use std::sync::Arc;

use anyhow::{Result, bail};
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::git::oid::{ObjectFormat, Oid};
use crate::query::RefView;
use crate::repo::serve::{FetchArgs, plan};
use crate::repo::view::Snapshot;

use super::AGENT;
use super::Refusal;
use super::pktline::{self, Pkt, Reader};
use super::upload_pack::{common_haves, fetch_line, reply_now, spawn_respond, write_shallow_lines};

pub(crate) const CAPABILITIES: &str = "multi_ack_detailed side-band-64k thin-pack ofs-delta \
     shallow deepen-relative no-progress include-tag allow-tip-sha1-in-want \
     allow-reachable-sha1-in-want filter";

/// The ref advertisement of `view`, peeled.
pub(crate) fn advertisement(view: &RefView<'_>) -> Vec<u8> {
    let mut caps = format!("{CAPABILITIES} object-format={} agent={AGENT}", view.format);
    let mut lines: Vec<(Oid, String)> = Vec::new();
    if let Some((branch, Some(oid))) = view.head {
        caps = format!("{caps} symref=HEAD:{branch}");
        lines.push((oid, "HEAD".into()));
    }
    for (name, oid) in &view.refs {
        lines.push((*oid, (*name).to_string()));
        if let Some(p) = view.peeled.get(oid) {
            lines.push((*p, format!("{name}^{{}}")));
        }
    }
    super::advertise_refs("git-upload-pack", view.format, &lines, &caps)
}

/// How far a request got.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Only wants. A stateless client's first deepen request ends right
    /// after the wants; it is answered with the shallow update only.
    Wants,
    /// A negotiation section (haves) after the wants.
    Negotiating,
    /// The client sent `done`.
    Done,
}

/// One stateless-RPC upload-pack request.
pub(crate) struct Request {
    /// What the client asks for.
    pub args: FetchArgs,
    stage: Stage,
    sideband: bool,
    multi_ack_detailed: bool,
}

/// The oid of a `want` line. The first want also carries the capabilities.
fn take_want_caps<'a>(v: &'a str, caps: &mut Vec<String>) -> &'a str {
    let (oid, rest) = v.split_once(' ').unwrap_or((v, ""));
    if caps.is_empty() {
        *caps = rest
            .split(' ')
            .filter(|c| !c.is_empty())
            .map(str::to_string)
            .collect();
    }
    oid
}

/// Refuse a client whose first want's capabilities, `caps`, name another
/// object format than the repository's, `server`. Git's v0 client names
/// none (it checks the advertisement itself), and its wants are then
/// parsed as the repository's ids.
fn check_format(server: ObjectFormat, caps: &[String]) -> Result<()> {
    if let Some(client) = super::named_format(caps.iter().map(String::as_str))? {
        super::same_format(server, client)?;
    }
    Ok(())
}

/// Parse one stateless-RPC upload-pack request to a `format` repository.
/// A client that names another format with `object-format` is refused.
pub(crate) fn parse(format: ObjectFormat, body: &[u8]) -> Result<Request> {
    let mut r = Reader::new(body);
    let mut args = FetchArgs::default();
    let mut caps: Vec<String> = Vec::new();
    let mut done = false;
    let mut flushes = 0;
    let mut negotiation = false;
    loop {
        match r.next_line()? {
            None => break,
            Some(Err(Pkt::Flush)) => {
                if flushes > 0 {
                    negotiation = true;
                }
                flushes += 1;
            }
            Some(Err(p)) => bail!(Refusal::new(format!("unexpected packet {p:?}"))),
            Some(Ok(line)) => {
                let (k, mut v) = line.split_once(' ').unwrap_or((line, ""));
                // The first want carries the capabilities.
                if k == "want" {
                    v = take_want_caps(v, &mut caps);
                    check_format(format, &caps)?;
                }
                if fetch_line(format, &mut args, k, v)? {
                    negotiation |= k == "have";
                } else if k == "done" {
                    negotiation = true;
                    done = true;
                }
            }
        }
    }
    let has = |c: &str| caps.iter().any(|x| x == c);
    args.ofs_delta = has("ofs-delta");
    args.include_tag = has("include-tag");
    args.deepen_relative = has("deepen-relative");
    let stage = match (done, negotiation) {
        (true, _) => Stage::Done,
        (false, true) => Stage::Negotiating,
        (false, false) => Stage::Wants,
    };
    Ok(Request {
        args,
        stage,
        sideband: has("side-band-64k"),
        multi_ack_detailed: has("multi_ack_detailed"),
    })
}

impl Request {
    /// Whether the answer includes a pack.
    pub(crate) fn sends_pack(&self) -> bool {
        self.stage == Stage::Done && !self.args.wants.is_empty()
    }
}

/// A negotiation round: acknowledge, maybe declare readiness, and end the
/// round; the client follows up with `done`.
fn ack_round(pre: &mut Vec<u8>, common: &[Oid], multi_ack_detailed: bool) {
    if multi_ack_detailed {
        for c in common {
            pktline::line(pre, &format!("ACK {c} common\n"));
        }
        if let Some(last) = common.last() {
            pktline::line(pre, &format!("ACK {last} ready\n"));
        }
    } else if let Some(first) = common.first() {
        pktline::line(pre, &format!("ACK {first}\n"));
    }
    pktline::line(pre, "NAK\n");
}

/// Handle one stateless-RPC upload-pack request: plan if needed, then
/// answer the negotiation, then send the pack.
pub(crate) fn handle(snap: Arc<Snapshot>, req: Request) -> mpsc::Receiver<Result<Bytes>> {
    if req.args.wants.is_empty() {
        // Nothing wanted (e.g. an up-to-date fetch): empty response.
        return reply_now(Vec::new());
    }
    spawn_respond(snap, req.sideband, |snap| async move {
        // As in git, a shallow section is sent only when deepening; a
        // shallow client's plain fetch (e.g. tag following) gets none.
        let shallow = req.args.deepen.is_some();
        // Negotiation rounds only need a plan for the shallow update.
        let done = req.stage == Stage::Done;
        let p = if done || shallow {
            Some(plan(&snap, &req.args).await?)
        } else {
            None
        };
        let mut pre = Vec::new();
        if let Some(p) = p.as_ref().filter(|_| shallow) {
            write_shallow_lines(&mut pre, p);
            pktline::flush(&mut pre);
        }
        if req.stage == Stage::Wants {
            return Ok((pre, None));
        }
        let common = common_haves(&snap, &req.args.haves);
        if !done {
            ack_round(&mut pre, &common, req.multi_ack_detailed);
            return Ok((pre, None));
        }
        match common.last() {
            Some(last) => pktline::line(&mut pre, &format!("ACK {last}\n")),
            None => pktline::line(&mut pre, "NAK\n"),
        }
        Ok((pre, p))
    })
}
