//! pkt-line framing, in both directions.
//!
//! One decoder for every pkt-line Enroute reads, whether it is a request this
//! server answers or an answer a remote gave it. Which packets a caller will
//! accept is that caller's grammar, checked against what this reports.

use gix_packetline::{
    Channel, PacketLineRef,
    blocking_io::encode,
    decode::{PacketLineOrWantedSize, hex_prefix},
};
use tokio::io::AsyncReadExt as _;

/// The flush-pkt that closes a section.
pub(crate) const FLUSH: &[u8] = b"0000";

/// The delim-pkt that separates one section from the next.
const DELIM: &[u8] = b"0001";

/// Bytes that are not the pkt-line their length prefix claims.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Malformed(String);

/// One pkt-line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Packet<'a> {
    /// A line's payload, with any trailing newline still on it.
    Data(&'a [u8]),
    /// `0000`, which ends a section.
    Flush,
    /// `0001`, which separates one section from the next.
    Delimiter,
    /// `0002`, which ends a response.
    ResponseEnd,
}

/// The pkt-lines in a body, in order.
#[derive(Debug)]
pub struct Packets<'a> {
    rest: &'a [u8],
}

impl<'a> Packets<'a> {
    /// Read `body` as pkt-lines.
    #[must_use]
    pub fn new(body: &'a [u8]) -> Self {
        Self { rest: body }
    }
}

impl<'a> Iterator for Packets<'a> {
    type Item = Result<Packet<'a>, Malformed>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rest.is_empty() {
            return None;
        }
        let Some((prefix, tail)) = self.rest.split_at_checked(4) else {
            self.rest = &[];
            return Some(Err(Malformed("truncated pkt-line".into())));
        };
        let wanted = match hex_prefix(prefix) {
            Ok(PacketLineOrWantedSize::Line(line)) => {
                self.rest = tail;
                return Some(Ok(match line {
                    PacketLineRef::Flush => Packet::Flush,
                    PacketLineRef::Delimiter => Packet::Delimiter,
                    // `hex_prefix` reports a payload as `Wanted`, never as a
                    // line, so response-end is all that is left.
                    PacketLineRef::ResponseEnd | PacketLineRef::Data(_) => Packet::ResponseEnd,
                }));
            }
            Ok(PacketLineOrWantedSize::Wanted(wanted)) => usize::from(wanted),
            Err(error) => {
                self.rest = &[];
                return Some(Err(Malformed(format!("invalid pkt-line: {error}"))));
            }
        };
        let Some((data, tail)) = tail.split_at_checked(wanted) else {
            self.rest = &[];
            return Some(Err(Malformed("truncated pkt-line data".into())));
        };
        self.rest = tail;
        Some(Ok(Packet::Data(data)))
    }
}

/// Read data lines from `reader` up to a flush-pkt, trailing `\n` and all.
///
/// Markers are skipped rather than reported: the one caller that reads a body
/// this way has no sections for them to separate.
pub(crate) async fn read_lines<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<Vec<u8>>, Malformed> {
    let mut lines = Vec::new();
    loop {
        let mut prefix = [0u8; 4];
        reader
            .read_exact(&mut prefix)
            .await
            .map_err(|e| Malformed(format!("read pkt-line prefix: {e}")))?;
        match hex_prefix(&prefix).map_err(|e| Malformed(format!("invalid pkt-line: {e}")))? {
            PacketLineOrWantedSize::Line(PacketLineRef::Flush) => return Ok(lines),
            PacketLineOrWantedSize::Line(_) => {}
            PacketLineOrWantedSize::Wanted(n) => {
                let n = usize::from(n);
                let mut data = vec![0u8; n];
                reader
                    .read_exact(&mut data)
                    .await
                    .map_err(|e| Malformed(format!("read pkt-line data: {e}")))?;
                lines.push(data);
            }
        }
    }
}

/// A line's payload without the newline git puts on the end of most of them.
#[must_use]
pub fn trim_lf(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

/// A line's payload as text, for the lines whose grammar is ASCII.
///
/// Lossy rather than an error: a remote that answers with something not
/// UTF-8 is reporting, and a garbled report reads better than none.
#[must_use]
pub fn text(line: &[u8]) -> String {
    String::from_utf8_lossy(trim_lf(line)).into_owned()
}

/// A pkt-line body being written.
#[derive(Debug, Default)]
pub struct Body(Vec<u8>);

impl Body {
    /// An empty body.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one data line carrying `payload`.
    ///
    /// # Errors
    ///
    /// Returns an error if `payload` is empty, or longer than one pkt-line
    /// can carry.
    pub fn line(&mut self, payload: &[u8]) -> Result<(), std::io::Error> {
        encode::data_to_write(payload, &mut self.0)?;
        Ok(())
    }

    /// Append a flush-pkt.
    pub fn flush(&mut self) {
        self.0.extend_from_slice(FLUSH);
    }

    /// Append a delim-pkt.
    pub(crate) fn delim(&mut self) {
        self.0.extend_from_slice(DELIM);
    }

    /// Everything written so far.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

/// One sideband frame carrying `payload` on `channel`.
///
/// # Errors
///
/// Returns an error if `payload` is empty, or longer than one frame carries.
pub(crate) fn band(channel: Channel, payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut frame = Vec::with_capacity(5 + payload.len());
    encode::band_to_write(channel, payload, &mut frame)?;
    Ok(frame)
}

// ── tests ────────────────────────────────────────────────────────────────
//
// Roundtrip arbitrary data-line payloads through this module's own encoder
// and check the decoders reconstruct them exactly.

#[cfg(test)]
mod tests {
    use super::*;

    fn any_payloads() -> impl proptest::strategy::Strategy<Value = Vec<Vec<u8>>> {
        use proptest::prelude::*;
        // Zero-length data lines are indistinguishable from other
        // zero-length special packets, so `Body::line` rejects them.
        proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..200), 0..8)
    }

    fn encoded(payloads: &[Vec<u8>]) -> Vec<u8> {
        let mut body = Body::new();
        for payload in payloads {
            body.line(payload).unwrap();
        }
        body.flush();
        body.into_bytes()
    }

    proptest::proptest! {
        #[test]
        fn packets_roundtrip_any_data_lines(payloads in any_payloads()) {
            let buf = encoded(&payloads);
            let read: Vec<Packet<'_>> =
                Packets::new(&buf).collect::<Result<_, _>>().unwrap();

            let mut expected: Vec<Packet<'_>> =
                payloads.iter().map(|p| Packet::Data(p)).collect();
            expected.push(Packet::Flush);
            proptest::prop_assert_eq!(read, expected);
        }

        #[test]
        fn read_lines_roundtrips_any_data_lines(payloads in any_payloads()) {
            let buf = encoded(&payloads);
            let mut cursor = buf.as_slice();
            let lines = futures::executor::block_on(read_lines(&mut cursor)).unwrap();
            proptest::prop_assert_eq!(lines, payloads);
        }
    }

    /// Whatever follows a flush is the next section's, so the iterator hands
    /// it back rather than stopping.
    #[test]
    fn reads_on_past_a_flush() {
        let read: Vec<Packet<'_>> = Packets::new(b"000ahello\n00000001")
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            read,
            vec![Packet::Data(b"hello\n"), Packet::Flush, Packet::Delimiter]
        );
    }

    #[test]
    fn refuses_a_truncated_length_prefix() {
        assert!(Packets::new(b"00").any(|packet| packet.is_err()));
    }

    #[test]
    fn refuses_truncated_data() {
        assert!(Packets::new(b"000abc").any(|packet| packet.is_err()));
    }

    #[test]
    fn refuses_a_length_that_is_not_hex() {
        assert!(Packets::new(b"zzzz").any(|packet| packet.is_err()));
    }

    #[test]
    fn trims_only_the_trailing_newline() {
        assert_eq!(trim_lf(b"ok refs/heads/main\n"), b"ok refs/heads/main");
        assert_eq!(trim_lf(b"ok refs/heads/main"), b"ok refs/heads/main");
    }
}
