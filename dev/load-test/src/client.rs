//! Minimal smart-HTTP protocol-v2 git client for the bench loop.
//!
//! Issues the same requests a fresh `git clone` does (capability
//! advertisement, `ls-refs`, `fetch`) but drains the pack into nothing,
//! keeping only a running SHA1 for the trailer — no subprocess spawns, no
//! `index-pack`, no working-tree churn, all of which dominated measured
//! latency under concurrency when the harness shelled out to real git. The
//! HTTP client also keeps connections alive across iterations, unlike real
//! git, so TCP setup is mostly paid once per worker rather than once per
//! clone — noise on loopback, since everything protocol-level still happens
//! per iteration.

use anyhow::{Context as _, Result, bail};
use bytes::Bytes;
use sha1::Digest as _;

/// One pkt-line-encoded data line: 4 hex digits of total length (including
/// the 4 length digits themselves), then the payload.
fn pktline(payload: &str) -> String {
    format!("{:04x}{payload}", payload.len() + 4)
}

/// The flush-pkt terminating each request body.
const FLUSH_PKT: &str = "0000";

/// The delim-pkt separating a command's capability-list from its args, per
/// protocol v2's `command-request` grammar.
///
/// Required even with zero capabilities: enroute tolerates omitting it, but
/// strict servers like GitHub's reject a request missing it.
const DELIM_PKT: &str = "0001";

/// Length of the SHA1 trailer closing a pack stream.
const PACK_TRAILER_LEN: usize = 20;

pub(crate) struct GitHttpClient {
    http: reqwest::Client,
    /// Repo base URL, ending in `.git`.
    url: String,
    /// `Host` header override, set only when `url` points at a enroute
    /// server, which reads the namespace from `Host` rather than the path.
    host_override: Option<String>,
}

impl GitHttpClient {
    pub(crate) fn new(url: String, host_override: Option<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            url,
            host_override,
        }
    }

    /// `GET /info/refs` — the capability advertisement opening every
    /// smart-HTTP conversation.
    ///
    /// Response is drained and discarded, but a real clone always pays
    /// this round trip, so the bench pays it too.
    pub(crate) async fn capabilities(&self) -> Result<()> {
        let mut req = self
            .http
            .get(format!("{}/info/refs?service=git-upload-pack", self.url))
            .header("Git-Protocol", "version=2");
        if let Some(host) = &self.host_override {
            req = req.header(reqwest::header::HOST, host);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            bail!("info/refs: HTTP {}", resp.status());
        }
        resp.bytes().await?;
        Ok(())
    }

    /// POST one command to `git-upload-pack` and buffer the full response.
    async fn upload_pack(&self, body: String) -> Result<Bytes> {
        let mut req = self
            .http
            .post(format!("{}/git-upload-pack", self.url))
            .header("Git-Protocol", "version=2")
            .header("Content-Type", "application/x-git-upload-pack-request");
        if let Some(host) = &self.host_override {
            req = req.header(reqwest::header::HOST, host);
        }
        let resp = req.body(body).send().await?;
        if !resp.status().is_success() {
            bail!("git-upload-pack: HTTP {}", resp.status());
        }
        Ok(resp.bytes().await?)
    }

    /// `ls-refs` — the OID at the tip of the first advertised ref (the
    /// bench repo has exactly one branch).
    pub(crate) async fn ls_refs_tip(&self) -> Result<String> {
        let body = format!("{}{DELIM_PKT}{FLUSH_PKT}", pktline("command=ls-refs\n"));
        let resp = self.upload_pack(body).await?;
        let mut lines = PktLines::new(&resp);
        while let Some(line) = lines.next_line()? {
            // Each ref line is "<40-hex-oid> <refname>\n".
            if let Some(oid) = line.get(..40)
                && oid.iter().all(u8::is_ascii_hexdigit)
            {
                return Ok(std::str::from_utf8(oid)?.to_string());
            }
        }
        bail!("ls-refs advertised no refs")
    }

    /// `fetch` with a single `want` and an immediate `done`: the shape of a
    /// fresh clone, full or shallow (`deepen <depth>`) when `depth` is given.
    ///
    /// The sideband-framed pack is discarded after verifying its trailing
    /// SHA1 — the check `index-pack` performs, minus the actual indexing.
    pub(crate) async fn fetch(&self, oid: &str, depth: Option<u64>) -> Result<u64> {
        let deepen_line = depth.map_or_else(String::new, |d| pktline(&format!("deepen {d}\n")));
        let body = format!(
            "{}{DELIM_PKT}{}{deepen_line}{}{FLUSH_PKT}",
            pktline("command=fetch\n"),
            pktline(&format!("want {oid}\n")),
            pktline("done\n"),
        );
        let resp = self.upload_pack(body).await?;

        let mut pack = Vec::new();
        let mut lines = PktLines::new(&resp);
        while let Some(line) = lines.next_line()? {
            match line.split_first() {
                // Sideband channel 1 carries pack data; 2 is progress chatter.
                Some((1, data)) => pack.extend_from_slice(data),
                Some((2, _progress)) => {}
                Some((3, msg)) => bail!("server error: {}", String::from_utf8_lossy(msg)),
                _ if line == b"packfile\n" => {}
                // `deepen` responses carry a `shallow-info` section (this
                // line, then `shallow`/`unshallow` lines) before the pack.
                _ if line == b"shallow-info\n" => {}
                _ if line.starts_with(b"shallow ") || line.starts_with(b"unshallow ") => {}
                _ => bail!(
                    "unexpected fetch response line: {:?}",
                    String::from_utf8_lossy(line)
                ),
            }
        }

        verify_pack_trailer(&pack)?;
        u64::try_from(pack.len()).context("pack length overflow")
    }
}

/// Check the pack's trailing SHA1 against a hash of everything before it.
fn verify_pack_trailer(pack: &[u8]) -> Result<()> {
    let body_len = pack
        .len()
        .checked_sub(PACK_TRAILER_LEN)
        .context("pack shorter than its SHA1 trailer")?;
    let (body, trailer) = pack.split_at(body_len);
    if sha1::Sha1::digest(body).as_slice() != trailer {
        bail!("pack trailer SHA1 mismatch");
    }
    Ok(())
}

/// Iterator over the data payloads of a buffered pkt-line stream.
///
/// Control packets (flush, delim, response-end) are skipped — callers key
/// on payload contents, not section boundaries.
struct PktLines<'a> {
    buf: &'a [u8],
}

impl<'a> PktLines<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    fn next_line(&mut self) -> Result<Option<&'a [u8]>> {
        while !self.buf.is_empty() {
            let len_hex = self.buf.get(..4).context("truncated pkt-line length")?;
            let len = usize::from_str_radix(std::str::from_utf8(len_hex)?, 16)
                .context("invalid pkt-line length")?;
            if len < 4 {
                self.buf = self.buf.get(4..).unwrap_or(&[]);
                continue;
            }
            let payload = self
                .buf
                .get(4..len)
                .context("pkt-line length past end of buffer")?;
            self.buf = self.buf.get(len..).unwrap_or(&[]);
            return Ok(Some(payload));
        }
        Ok(None)
    }
}
