//! The transport between ledger nodes: the `Peer` gRPC service
//! (`proto/peer.proto`).
//!
//! All frames for all logs going to the same peer are coalesced into one
//! `Deliver` call per batch, the way `TiKV` batches its Raft messages; each
//! peer has one HTTP/2 connection, kept open and re-established by tonic.
//! A frame is a raft-rs message for the root log (encoded by raft-rs, which
//! uses an older prost), a repository log's replication traffic, or a
//! liveness ping.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use raft::prelude::Message;
use raft_prost::Message as _;
use tokio::sync::mpsc;

use enroute_shared::auth::{AuthChannel, Bearer};

pub(crate) use crate::proto::Frame;
use crate::proto::{self, frame, peer_client::PeerClient, peer_server};

use super::engine::{Engine, Input};
use super::root::ROOT;

/// A frame of `kind` for log `log`.
pub(super) fn frame(log: u64, kind: frame::Kind) -> Frame {
    Frame {
        log,
        kind: Some(kind),
    }
}

/// A root log message, as a frame.
pub(super) fn root_frame(m: &Message) -> Frame {
    frame(ROOT, frame::Kind::Root(m.encode_to_vec().into()))
}

/// The root log message a frame carries.
pub(super) fn root_message(data: &[u8]) -> Result<Message> {
    Ok(Message::decode(data)?)
}

/// Serves `Peer` for this node's engine.
#[derive(Debug)]
pub struct PeerService(pub(super) Weak<Engine>);

#[tonic::async_trait]
impl peer_server::Peer for PeerService {
    async fn deliver(
        &self,
        req: tonic::Request<proto::Frames>,
    ) -> Result<tonic::Response<proto::Delivered>, tonic::Status> {
        let engine = self
            .0
            .upgrade()
            .ok_or_else(|| tonic::Status::unavailable("consensus engine stopped"))?;
        let req = req.into_inner();
        // A removed node learns so from its peers, as etcd's does, once its
        // logs moved off it: the root's leader no longer sends it the root
        // log.
        if engine.departed(req.from) {
            return Err(tonic::Status::permission_denied(format!(
                "node {} was removed from the ledger",
                req.from
            )));
        }
        engine.receive(req.from, req.frames);
        Ok(tonic::Response::new(proto::Delivered {}))
    }
}

/// The `Peer` service for `engine`, to mount on the node's server.
pub(super) fn server(engine: Weak<Engine>) -> peer_server::PeerServer<PeerService> {
    peer_server::PeerServer::new(PeerService(engine))
        .max_decoding_message_size(enroute_shared::proto::MAX_MESSAGE)
}

/// Peers' addresses by id, as the directory has them, shared with the
/// transport.
pub type Addresses = Arc<parking_lot::RwLock<HashMap<u64, String>>>;

/// Node `id`'s internal URL as `addresses` has it: empty if unknown, as when
/// the ledger does not replicate, or names no node (id 0).
pub(crate) fn url_of(addresses: Option<&Addresses>, id: u64) -> String {
    addresses
        .and_then(|a| a.read().get(&id).cloned())
        .unwrap_or_default()
}

/// Sends frames to peers; one background sender per peer coalesces
/// whatever is queued into a single call. Peers are addressed by id,
/// resolved through the directory's members.
pub(super) struct Transport {
    /// This node's id, which every batch names.
    me: u64,
    addresses: Addresses,
    bearer: Bearer,
    senders: Mutex<HashMap<u64, mpsc::UnboundedSender<Frame>>>,
    unreachable: Unreachable,
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport").finish_non_exhaustive()
    }
}

/// Told which peers could not be reached with root log messages.
type Unreachable = mpsc::UnboundedSender<Input>;

impl Transport {
    pub(super) fn new(
        me: u64,
        addresses: Addresses,
        bearer: Bearer,
        unreachable: Unreachable,
    ) -> Transport {
        Transport {
            me,
            addresses,
            bearer,
            senders: Mutex::default(),
            unreachable,
        }
    }

    pub(super) fn send(&self, to: u64, frame: Frame) {
        let mut senders = self.senders.lock();
        let tx = senders.entry(to).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(sender(
                self.me,
                self.addresses.clone(),
                self.bearer.clone(),
                to,
                rx,
                self.unreachable.clone(),
            ));
            tx
        });
        // A closed receiver means the sender task ended; the frame is lost.
        drop(tx.send(frame));
    }
}

/// A client for `url`, its calls carrying `bearer`'s token, connecting
/// lazily (and again after failures).
fn client(url: &str, bearer: &Bearer) -> Result<PeerClient<AuthChannel>> {
    Ok(
        PeerClient::new(bearer.on(enroute_shared::auth::channel(url)?))
            .max_encoding_message_size(enroute_shared::proto::MAX_MESSAGE),
    )
}

async fn sender(
    me: u64,
    addresses: Addresses,
    bearer: Bearer,
    peer: u64,
    mut rx: mpsc::UnboundedReceiver<Frame>,
    unreachable: Unreachable,
) {
    // The peer's address, and a client for it (rebuilt if it moves).
    let mut conn: Option<(String, PeerClient<AuthChannel>)> = None;
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while let Ok(f) = rx.try_recv() {
            batch.push(f);
            if batch.len() >= 4096 {
                break;
            }
        }
        let root = batch
            .iter()
            .any(|f| matches!(f.kind, Some(frame::Kind::Root(_))));
        let url = addresses.read().get(&peer).cloned();
        if let Some(url) = &url
            && conn.as_ref().is_none_or(|(u, _)| u != url)
        {
            conn = client(url, &bearer).ok().map(|c| (url.clone(), c));
        }
        let ok = match &mut conn {
            // Not (yet) a member, or a malformed address.
            None => false,
            Some((_, c)) => {
                let frames = proto::Frames {
                    frames: batch,
                    from: me,
                };
                let mut req = tonic::Request::new(frames);
                req.set_timeout(Duration::from_secs(5));
                match c.deliver(req).await {
                    Ok(_) => true,
                    Err(s) if s.code() == tonic::Code::PermissionDenied => {
                        drop(unreachable.send(Input::Retire));
                        false
                    }
                    Err(_) => false,
                }
            }
        };
        if !ok {
            if root {
                // A closed receiver means the engine stopped.
                drop(unreachable.send(Input::Unreachable(peer)));
            }
            // Don't spin against a dead peer: what was lost is sent again
            // (raft retransmits; sequencers retry).
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_messages_roundtrip() {
        let m = Message {
            to: 2,
            from: 1,
            term: 5,
            ..Default::default()
        };
        let f = root_frame(&m);
        assert_eq!(f.log, ROOT);
        let Some(frame::Kind::Root(data)) = f.kind else {
            panic!("not a root frame")
        };
        assert_eq!(root_message(&data).unwrap(), m);
    }
}
