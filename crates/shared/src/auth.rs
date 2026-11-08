//! What callers of the internal port share with the ledger: the message a
//! joining node signs, and the token its calls then carry. Issuing and
//! checking tokens is the ledger's (`enroute_ledger::token`).

use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;

use crate::proto::ledger_client::LedgerClient;

/// The SHA-256 of a public key (its 32 bytes), in hex: a node's name in its
/// tokens.
#[must_use]
pub fn thumbprint(public: &[u8; 32]) -> String {
    hex::encode(Sha256::digest(public))
}

/// What a joining node signs: the nonce bound to its key, as ACME's key
/// authorization (RFC 8555 §8.1), so a nonce read by anyone else answers
/// only for this key.
#[must_use]
pub fn challenge_message(nonce: &[u8], public: &[u8; 32]) -> String {
    format!("{}.{}", hex::encode(nonce), thumbprint(public))
}

/// How often a connection to another node is pinged, as gRPC's keepalive
/// does: a peer that is frozen or cut off still has its kernel acknowledge
/// TCP, so without a ping its calls and streams wait forever.
const KEEPALIVE_EVERY: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_secs(10)
};

/// How long a ping waits for its answer before the connection is closed:
/// gRPC's default, since the answer queues behind the data already sent.
const KEEPALIVE_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(20)
};

/// A connection to the node at `url` (its internal address), made lazily
/// (and again after failures), and closed once the node stops answering
/// pings, which fails its calls as unavailable.
///
/// # Errors
///
/// Returns an error if `url` is not a valid URI.
pub fn channel(url: &str) -> anyhow::Result<Channel> {
    Ok(Channel::from_shared(url.to_string())?
        .connect_timeout(Duration::from_secs(2))
        .http2_keep_alive_interval(KEEPALIVE_EVERY)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .connect_lazy())
}

/// The token a node presents, renewed as it goes (None until it has one).
pub type Token = watch::Receiver<Option<Arc<str>>>;

/// Puts a node's current token on each of its calls.
#[derive(Clone, Debug)]
pub struct Bearer {
    token: Token,
    /// The token's header, made once per renewal.
    header: Option<MetadataValue<Ascii>>,
    /// Whether calls also carry the caller's trace context.
    traced: bool,
}

/// A connection whose calls carry a [`Bearer`] token.
pub type AuthChannel = tonic::service::interceptor::InterceptedService<Channel, Bearer>;

impl Bearer {
    /// Calls with the tokens `token` gives.
    #[must_use]
    pub fn new(token: Token) -> Bearer {
        Bearer {
            token,
            header: None,
            traced: false,
        }
    }

    /// These calls, which also carry the caller's trace context.
    ///
    /// Only for calls that a request makes: on a stream that many requests
    /// share, or on `Peer` calls, the context of whichever request opened it
    /// would be wrong.
    #[must_use]
    pub fn traced(&self) -> Bearer {
        Bearer {
            traced: true,
            ..self.clone()
        }
    }

    /// The token, to follow its renewals.
    #[must_use]
    pub fn token(&self) -> Token {
        self.token.clone()
    }

    /// Whether there is a token yet.
    #[must_use]
    pub fn has_token(&self) -> bool {
        self.token.borrow().is_some()
    }

    /// `channel`, its calls carrying the token.
    #[must_use]
    pub fn on(&self, channel: Channel) -> AuthChannel {
        tonic::service::interceptor::InterceptedService::new(channel, self.clone())
    }

    /// A `Ledger` client on `channel`, its calls carrying the token.
    #[must_use]
    pub fn ledger(&self, channel: Channel) -> LedgerClient<AuthChannel> {
        // Both ways: proposals carry inline layers, and `Describe`, `List`
        // and `Watch` answers can be large.
        LedgerClient::new(self.on(channel))
            .max_encoding_message_size(crate::proto::MAX_MESSAGE)
            .max_decoding_message_size(crate::proto::MAX_MESSAGE)
    }
}

impl tonic::service::Interceptor for Bearer {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if self.header.is_none() || self.token.has_changed().unwrap_or(false) {
            let token = self.token.borrow_and_update().clone();
            self.header = token.and_then(|t| format!("Bearer {t}").parse().ok());
        }
        if let Some(h) = &self.header {
            req.metadata_mut().insert("authorization", h.clone());
        }
        if self.traced {
            crate::trace_context::inject(req.metadata_mut().as_mut());
        }
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt as _;

    use super::*;
    use crate::proto::ListRequest;

    async fn drain(mut tcp: tokio::net::TcpStream) {
        let mut buf = [0u8; 4096];
        while tcp.read(&mut buf).await.is_ok_and(|n| n > 0) {}
    }

    /// A node whose kernel takes the connection and its bytes but which
    /// never answers, as one that is frozen.
    #[tokio::test]
    async fn a_call_to_a_frozen_node_fails() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            drain(tcp).await;
        });
        let mut client = LedgerClient::new(channel(&url).unwrap());
        let answer =
            tokio::time::timeout(Duration::from_secs(10), client.list(ListRequest::default()))
                .await
                .expect("the call ends once the node misses its pings");
        assert_eq!(answer.unwrap_err().code(), tonic::Code::Unavailable);
    }
}
