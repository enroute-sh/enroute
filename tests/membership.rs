//! Who may call a ledger node: only nodes that joined it (`Membership`).
//!
//! A node joins by reading a nonce from the object store and signing it
//! with its key; every other call needs the token it gets.

#![expect(
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use common::Server;
use ed25519_dalek::Signer as _;
use enroute_shared::auth;
use enroute_shared::proto::{
    ChallengeRequest, JoinRequest, ListRequest, ListResponse, membership_client,
};
use tonic::Code;
use tonic::transport::Channel;

/// A node's key.
fn generate() -> ed25519_dalek::SigningKey {
    ed25519_dalek::SigningKey::from_bytes(&rand::random())
}

async fn channel(server: &Server) -> Channel {
    Channel::from_shared(format!("http://{}", server.internal))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

/// Join `server`'s ledger with `key`, as a compute node does: its token.
async fn join(server: &Server, key: &ed25519_dalek::SigningKey) -> anyhow::Result<String> {
    let url = format!("http://{}", server.internal);
    let objects = &server.node.ledger().unwrap().objects;
    Ok(enroute_compute::join_at(&url, objects, key).await?.0)
}

/// Call `method` (a full gRPC path) with `token`, if any: its status.
async fn call(server: &Server, method: &'static str, token: Option<&str>) -> Code {
    let mut grpc = tonic::client::Grpc::new(channel(server).await);
    grpc.ready().await.unwrap();
    let mut req = tonic::Request::new(ListRequest::default());
    if let Some(t) = token {
        req.metadata_mut()
            .insert("authorization", format!("Bearer {t}").parse().unwrap());
    }
    let codec = tonic_prost::ProstCodec::<ListRequest, ListResponse>::default();
    let path = http::uri::PathAndQuery::from_static(method);
    match grpc.unary(req, path, codec).await {
        Ok(_) => Code::Ok,
        Err(s) => s.code(),
    }
}

const LIST: &str = "/enroute.internal.v1.Ledger/List";
const PEER: &str = "/enroute.internal.peer.v1.Peer/Deliver";

/// Without a token the ledger refuses; a joined node's token opens the
/// Ledger service, but not Peer, which takes a ledger node's own token.
#[tokio::test]
async fn calls_need_a_token() {
    let server = Server::start().await;
    assert_eq!(call(&server, LIST, None).await, Code::Unauthenticated);
    assert_eq!(
        call(&server, LIST, Some("x.y.z")).await,
        Code::Unauthenticated
    );
    let token = join(&server, &generate()).await.unwrap();
    assert_eq!(call(&server, LIST, Some(&token)).await, Code::Ok);
    assert_eq!(
        call(&server, PEER, Some(&token)).await,
        Code::Unauthenticated
    );
    let ledger = server.node.ledger().unwrap();
    let own = ledger.issue(&format!("ledger-{:016x}", ledger.engine.id));
    assert_ne!(call(&server, PEER, Some(&own)).await, Code::Unauthenticated);
}

/// A challenge is answered only once, only with the key it was given for,
/// and only by a signature of its nonce.
#[tokio::test]
async fn challenges_bind_to_their_key() {
    let server = Server::start().await;
    let mut m = membership_client::MembershipClient::new(channel(&server).await);
    let (key, other) = (generate(), generate());
    let public = key.verifying_key();
    let c = m
        .challenge(ChallengeRequest {
            public_key: public.as_bytes().to_vec().into(),
        })
        .await
        .unwrap()
        .into_inner();
    let objects = &server.node.ledger().unwrap().objects;
    let nonce = objects.get_object(&c.object).await.unwrap().unwrap();
    // Another key signing the same nonce: refused, and the challenge spent.
    let theirs = auth::challenge_message(&nonce, other.verifying_key().as_bytes());
    let err = m
        .join(JoinRequest {
            public_key: other.verifying_key().as_bytes().to_vec().into(),
            id: c.id.clone(),
            signature: other.sign(theirs.as_bytes()).to_bytes().to_vec().into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    let ours = auth::challenge_message(&nonce, public.as_bytes());
    let err = m
        .join(JoinRequest {
            public_key: public.as_bytes().to_vec().into(),
            id: c.id,
            signature: key.sign(ours.as_bytes()).to_bytes().to_vec().into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied, "{err:?}");
    // A fresh challenge answered right gives a token naming the key.
    let token = join(&server, &key).await.unwrap();
    let ledger = server.node.ledger().unwrap();
    let claims = ledger.authenticate(&token).unwrap();
    assert_eq!(claims.sub, auth::thumbprint(public.as_bytes()));
}
