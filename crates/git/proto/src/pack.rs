//! Outbound side of the pack pipeline: sideband framing, the running pack
//! SHA1, and the channel feeding the HTTP response body.
//!
//! The pack format itself is [`enroute_git_packfile`], and the pkt-line
//! encoding is [`crate::pktline`]. This is only what sits between.

use bytes::{Bytes, BytesMut};
use futures::SinkExt as _;
use futures::channel::mpsc;
use gix_packetline::Channel;
use sha1::Digest as _;

use crate::Error;
use crate::pktline;

/// Max pkt-line data (65516) minus 1 sideband byte.
pub(crate) const MAX_SIDEBAND: usize = 65515;

pub(crate) type SidebandTx = mpsc::Sender<Result<Bytes, std::io::Error>>;

/// Send one sideband frame on `channel`, copying `payload` into a single
/// contiguous pkt-line.
///
/// For pack bytes already held as `Bytes`, [`send_frame_zero_copy`] sends the
/// same frame without the copy.
pub(crate) async fn send_frame_copy(
    channel: Channel,
    payload: &[u8],
    tx: &mut SidebandTx,
) -> Result<(), Error> {
    let frame =
        pktline::band(channel, payload).map_err(|e| anyhow::anyhow!("sideband framing: {e}"))?;
    tx.send(Ok(Bytes::from(frame)))
        .await
        .map_err(|e| anyhow::anyhow!("sideband stream closed: {e}"))?;
    Ok(())
}

/// Send `payload` on `Channel::Data`, chunked at [`MAX_SIDEBAND`] if needed.
pub(crate) async fn send_data_frames(payload: &[u8], tx: &mut SidebandTx) -> Result<(), Error> {
    for chunk in payload.chunks(MAX_SIDEBAND) {
        send_frame_copy(Channel::Data, chunk, tx).await?;
    }
    Ok(())
}

/// Send git's own no-op keepalive: a `Channel::Data` band carrying no bytes.
///
/// Hand-rolled because `gix-packetline` rejects an empty payload outright,
/// never seeing that `0005` is well-formed where `0004` would not be.
pub(crate) async fn send_keepalive(tx: &mut SidebandTx) -> Result<(), Error> {
    tx.send(Ok(Bytes::from_static(b"0005\x01")))
        .await
        .map_err(|e| anyhow::anyhow!("sideband stream closed: {e}"))?;
    Ok(())
}

/// Send `msg` as a `Channel::Error` frame, best-effort: if the client's
/// gone, there's nothing more to do.
pub(crate) async fn send_sideband_error(msg: &str, tx: &mut SidebandTx) {
    drop(send_frame_copy(Channel::Error, msg.as_bytes(), tx).await);
}

/// Report a failure to a consumer taking the pack's own bytes: the error
/// travels as the stream's own item, and the stream ends there.
pub(crate) async fn send_raw_error(msg: &str, tx: &mut SidebandTx) {
    drop(tx.send(Err(std::io::Error::other(msg.to_string()))).await);
}

/// Send the closing flush-pkt that ends a sideband-multiplexed response —
/// this one isn't itself sideband-framed.
pub(crate) async fn send_close(tx: &mut SidebandTx) -> Result<(), Error> {
    tx.send(Ok(Bytes::from_static(pktline::FLUSH)))
        .await
        .map_err(|e| anyhow::anyhow!("sideband stream closed: {e}"))?;
    Ok(())
}

/// 5-byte pkt-line prefix for a sideband-1 frame of `payload_len` bytes.
///
/// Used by [`send_frame_zero_copy`] to send prefix and payload as separate
/// channel items, avoiding a payload copy.
fn sideband1_prefix(payload_len: usize) -> [u8; 5] {
    let total = 4 + 1 + payload_len;
    debug_assert!(total <= 0xffff);
    [
        hex_nibble(total >> 12),
        hex_nibble(total >> 8),
        hex_nibble(total >> 4),
        hex_nibble(total),
        1u8, // sideband-1: pack data
    ]
}

fn hex_nibble(nibble: usize) -> u8 {
    match u8::try_from(nibble & 0xf).unwrap_or(0) {
        d @ 0..=9 => b'0' + d,
        d => b'a' + (d - 10),
    }
}

/// Send one sideband-1 frame without copying the payload: the prefix and
/// payload go out as two channel items.
///
/// `Body::from_stream` writes each `Bytes` as-is, so the frame need not be
/// contiguous.
async fn send_frame_zero_copy(payload: Bytes, tx: &mut SidebandTx) -> Result<(), Error> {
    let prefix = Bytes::copy_from_slice(&sideband1_prefix(payload.len()));
    tx.send(Ok(prefix))
        .await
        .map_err(|e| anyhow::anyhow!("pack stream closed: {e}"))?;
    tx.send(Ok(payload))
        .await
        .map_err(|e| anyhow::anyhow!("pack stream closed: {e}"))?;
    Ok(())
}

/// How a pack's bytes reach whoever asked for them.
///
/// The pack is identical either way, only the wrapping differs — keeping the
/// choice here stops the expensive assembly above it from existing twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Framing {
    /// sideband-1 pkt-lines, for a git client on the far end.
    Sideband,
    /// The pack's own bytes, for a caller that frames it itself, with chunk
    /// boundaries carrying no meaning.
    Raw,
}

/// Writer for the pack body: everything written feeds the running pack SHA1
/// and leaves in whichever [`Framing`] was asked for.
///
/// Uses plain SHA1, not the collision-detecting variant: everything hashed
/// here was already checked on the way into the store.
pub(crate) struct PackWriter {
    hasher: sha1::Sha1,
    buf: BytesMut,
    tx: SidebandTx,
    framing: Framing,
}

impl PackWriter {
    pub(crate) fn new(tx: SidebandTx, framing: Framing) -> Self {
        Self {
            hasher: sha1::Sha1::default(),
            buf: BytesMut::new(),
            tx,
            framing,
        }
    }

    pub(crate) fn framing(&self) -> Framing {
        self.framing
    }

    /// Send one chunk of pack bytes, framed or not per [`Framing`].
    ///
    /// Takes the chunk owned, letting the raw path send it as-is with no
    /// copy. The sideband path copies regardless, for its header.
    async fn send_chunk(&mut self, payload: Bytes) -> Result<(), Error> {
        match self.framing {
            Framing::Sideband => send_frame_copy(Channel::Data, &payload, &mut self.tx).await,
            Framing::Raw => {
                self.tx
                    .send(Ok(payload))
                    .await
                    .map_err(|e| anyhow::anyhow!("pack stream closed: {e}"))?;
                Ok(())
            }
        }
    }

    /// Send already-framed pkt-line bytes as-is (the `packfile\n` section
    /// header), not hashed since it isn't part of the pack.
    pub(crate) async fn send_raw(&mut self, pkt: Bytes) -> Result<(), Error> {
        self.tx
            .send(Ok(pkt))
            .await
            .map_err(|e| anyhow::anyhow!("pack stream closed: {e}"))?;
        Ok(())
    }

    /// Hash `data` and coalesce it into `buf`, sending any completed frames.
    pub(crate) async fn write(&mut self, data: &[u8]) -> Result<(), Error> {
        self.hasher.update(data);
        self.buf.extend_from_slice(data);
        self.flush_full_frames().await
    }

    /// Hash and send an already-owned payload.
    ///
    /// Payloads of at least one full frame skip the coalescing buffer: only
    /// the sub-frame tail is copied into `buf`.
    pub(crate) async fn write_bytes(&mut self, mut data: Bytes) -> Result<(), Error> {
        self.hasher.update(&data);
        if data.len() < MAX_SIDEBAND {
            self.buf.extend_from_slice(&data);
            return self.flush_full_frames().await;
        }
        self.flush().await?;
        while data.len() >= MAX_SIDEBAND {
            let chunk = data.split_to(MAX_SIDEBAND);
            match self.framing {
                Framing::Sideband => send_frame_zero_copy(chunk, &mut self.tx).await?,
                // Already owned and already the right size: the one path
                // where a raw pack chunk costs no copy at all.
                Framing::Raw => self
                    .tx
                    .send(Ok(chunk))
                    .await
                    .map_err(|e| anyhow::anyhow!("pack stream closed: {e}"))?,
            }
        }
        self.buf.extend_from_slice(&data);
        Ok(())
    }

    async fn flush_full_frames(&mut self) -> Result<(), Error> {
        while self.buf.len() >= MAX_SIDEBAND {
            // `split_to` hands over the head and leaves the tail in place,
            // where draining it moved every remaining byte down.
            let chunk = self.buf.split_to(MAX_SIDEBAND).freeze();
            self.send_chunk(chunk).await?;
        }
        Ok(())
    }

    /// Send whatever remains in `buf` as one (possibly undersized) frame.
    async fn flush(&mut self) -> Result<(), Error> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let chunk = self.buf.split().freeze();
        self.send_chunk(chunk).await
    }

    /// Append the pack's trailing SHA1 (not itself hashed) and flush the
    /// final chunk.
    ///
    /// Sideband additionally sends the closing flush-pkt; a raw stream ends
    /// when the channel does.
    pub(crate) async fn finish(mut self) -> Result<(), Error> {
        let digest = std::mem::take(&mut self.hasher).finalize();
        self.buf.extend_from_slice(&digest);
        let chunk = self.buf.split().freeze();
        self.send_chunk(chunk).await?;
        match self.framing {
            Framing::Sideband => send_close(&mut self.tx).await,
            Framing::Raw => Ok(()),
        }
    }
}

// ── tests ── roundtrip each encoder against the real decoder that reads its
// output in production (`PackReader`).

#[cfg(test)]
mod tests {
    use std::future::Future;

    use futures::StreamExt as _;
    use gix_packetline::PacketLineRef;
    use gix_packetline::decode::{PacketLineOrWantedSize, hex_prefix};

    use super::*;

    /// Decode a `PackWriter`'s sideband-1 frames back into the flat data
    /// payload, stopping at the closing flush-pkt.
    fn collect_sideband_data(bytes: &[u8]) -> Vec<u8> {
        let mut data = bytes;
        let mut payload = Vec::new();
        loop {
            let (prefix, rest) = data.split_at(4);
            match hex_prefix(prefix).unwrap() {
                PacketLineOrWantedSize::Line(PacketLineRef::Flush) => return payload,
                PacketLineOrWantedSize::Wanted(n) => {
                    let (band_and_data, rest) = rest.split_at(usize::from(n));
                    // First byte of the pkt-line body is the sideband channel marker.
                    payload.extend_from_slice(&band_and_data[1..]);
                    data = rest;
                }
                PacketLineOrWantedSize::Line(_) => {
                    panic!("unexpected non-data pkt-line in sideband stream")
                }
            }
        }
    }

    /// Drives `write_fut` and drains `rx` concurrently.
    ///
    /// `SidebandTx` is bounded, so awaiting the producer first can deadlock
    /// once it sends past the channel's capacity.
    fn drive_writer_and_collect<F: Future<Output = ()>>(
        write_fut: F,
        rx: mpsc::Receiver<Result<Bytes, std::io::Error>>,
    ) -> Vec<u8> {
        let ((), sent) =
            futures::executor::block_on(futures::future::join(write_fut, rx.collect::<Vec<_>>()));
        sent.into_iter()
            .map(|item| item.unwrap())
            .flat_map(Vec::from)
            .collect()
    }

    proptest::proptest! {
        #[test]
        fn pack_writer_roundtrips_arbitrary_writes(
            chunks in proptest::collection::vec(proptest::collection::vec(proptest::prelude::any::<u8>(), 0..500), 0..10),
        ) {
            let expected: Vec<u8> = chunks.iter().flatten().copied().collect();

            let (tx, rx) = mpsc::channel(0);
            let bytes = drive_writer_and_collect(
                async move {
                    let mut writer = PackWriter::new(tx, Framing::Sideband);
                    for chunk in &chunks {
                        writer.write(chunk).await.unwrap();
                    }
                    writer.finish().await.unwrap();
                },
                rx,
            );

            let mut hasher = sha1::Sha1::default();
            hasher.update(&expected);
            let mut want = expected;
            want.extend_from_slice(&hasher.finalize());

            proptest::prop_assert_eq!(collect_sideband_data(&bytes), want);
        }
    }

    #[tokio::test]
    async fn pack_writer_write_bytes_handles_payload_larger_than_max_sideband() {
        // Exercises the zero-copy split-frame path in `write_bytes`, unreached by `write()` above.
        let original: Vec<u8> = (0..u32::try_from(MAX_SIDEBAND).unwrap() + 100)
            .map(|i| u8::try_from(i % 256).unwrap())
            .collect();

        let (tx, rx) = mpsc::channel(0);
        let original_for_write = original.clone();
        let bytes = drive_writer_and_collect(
            async move {
                let mut writer = PackWriter::new(tx, Framing::Sideband);
                writer
                    .write_bytes(Bytes::from(original_for_write))
                    .await
                    .unwrap();
                writer.finish().await.unwrap();
            },
            rx,
        );

        let mut hasher = sha1::Sha1::default();
        hasher.update(&original);
        let mut want = original;
        want.extend_from_slice(&hasher.finalize());

        assert_eq!(collect_sideband_data(&bytes), want);
    }
}
