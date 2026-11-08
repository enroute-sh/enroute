//! The Git wire protocol: pkt-lines and the upload-pack and receive-pack
//! services.

use crate::git::oid::{ObjectFormat, Oid};

pub mod pktline;
pub(crate) mod receive_pack;
pub(crate) mod upload_pack;
pub(crate) mod upload_pack_v0;

/// An error in what the client sent, or a feature it asked for that Enroute
/// does not have. Only this kind of error is shown to the client: any other
/// can name local paths or the object store, so it stays in the log.
#[derive(Debug)]
pub(crate) struct Refusal(String);

impl Refusal {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Refusal {}

/// What a client sees for `e`: the message of a [`Refusal`], else nothing
/// more than that the server failed.
pub(crate) fn client_message(e: &anyhow::Error) -> String {
    e.downcast_ref::<Refusal>()
        .map_or_else(|| "internal error".to_owned(), ToString::to_string)
}

/// The object id of `format` that `s` starts with, as git's `get_oid_hex`
/// reads it: hex digits of either case, as many as the format's ids take,
/// whatever follows them.
pub(crate) fn oid_prefix(format: ObjectFormat, s: &[u8]) -> Option<Oid> {
    Oid::from_hex_exact(s.get(..format.hex_len())?)
}

/// The format git calls `name`. A format Enroute does not know is refused.
pub(crate) fn parse_format(name: &str) -> Result<ObjectFormat, Refusal> {
    ObjectFormat::from_name(name)
        .ok_or_else(|| Refusal::new(format!("unknown object format '{name}'")))
}

/// The format an `object-format` capability among `caps` names, if one
/// does (the last, as git reads them). Git assumes SHA-1 where none does.
pub(crate) fn named_format<'a>(
    caps: impl IntoIterator<Item = &'a str>,
) -> Result<Option<ObjectFormat>, Refusal> {
    let mut format = None;
    for c in caps {
        if let Some(name) = c.strip_prefix("object-format=") {
            format = Some(parse_format(name)?);
        }
    }
    Ok(format)
}

/// Refuse a `client` of another format than the `server`'s, in git's
/// words: their ids name different objects.
pub(crate) fn same_format(server: ObjectFormat, client: ObjectFormat) -> Result<(), Refusal> {
    if server == client {
        return Ok(());
    }
    Err(Refusal::new(format!(
        "mismatched object format: server {server}; client {client}"
    )))
}

/// The agent string sent in capability advertisements.
pub const AGENT: &str = concat!("enroute/", env!("CARGO_PKG_VERSION"));

/// A protocol v0 ref advertisement for `service` of a `format` repository:
/// `refs` (oid, name), the first carrying `caps` (or, with no refs, a
/// `capabilities^{}` line).
pub(crate) fn advertise_refs(
    service: &str,
    format: ObjectFormat,
    refs: &[(Oid, String)],
    caps: &str,
) -> Vec<u8> {
    let mut out = Vec::new();
    pktline::line(&mut out, &format!("# service={service}\n"));
    pktline::flush(&mut out);
    if refs.is_empty() {
        pktline::data(
            &mut out,
            format!("{} capabilities^{{}}\0{caps}\n", format.zero()).as_bytes(),
        );
    }
    for (i, (oid, name)) in refs.iter().enumerate() {
        match i {
            0 => pktline::data(&mut out, format!("{oid} {name}\0{caps}\n").as_bytes()),
            _ => pktline::line(&mut out, &format!("{oid} {name}\n")),
        }
    }
    pktline::flush(&mut out);
    out
}
