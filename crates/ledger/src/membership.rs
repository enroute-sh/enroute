//! The ledger's `Membership` service (`proto/ledger.proto`): how a node
//! earns a token. It names an Ed25519 key; this node puts a single-use nonce
//! at an unguessable path in the object store; the node reads it and signs it
//! bound to its key (ACME's key authorization, RFC 8555 §8.1). So a token
//! goes only to a node that can read the object store and holds the key
//! it names.

use std::time::{Duration, Instant};

use ed25519_dalek::VerifyingKey;
use tonic::{Request, Response, Status};

use enroute_shared::auth;
use enroute_shared::proto::{
    ChallengeRequest, ChallengeResponse, JoinRequest, JoinResponse, membership_server,
};

use crate::ledger::Ledger;
use crate::token;

/// How long a challenge waits for its answer.
const ANSWER_WITHIN: Duration = Duration::from_secs(60);

/// Challenges a node holds unanswered at once: joining costs it memory
/// and an object only up to this.
const MAX_PENDING: usize = 4096;

/// Where challenge `id`'s nonce goes in the object store.
fn object(id: &str) -> String {
    format!("data/challenges/{id}")
}

/// A challenge given and not answered yet.
pub(crate) struct Pending {
    nonce: [u8; 32],
    key: VerifyingKey,
    deadline: Instant,
}

impl Ledger {
    /// Take challenge `id` out of the pending ones, if it is there and not
    /// expired.
    fn take_challenge(&self, id: &str) -> Option<Pending> {
        let taken = self.challenges.lock().remove(id);
        self.expire_challenges();
        self.delete_challenges(taken.iter().map(|_| id.to_string()));
        taken.filter(|p| p.deadline > Instant::now())
    }

    /// Drop the expired challenges, and their objects.
    fn expire_challenges(&self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .challenges
            .lock()
            .extract_if(|_, p| p.deadline <= now)
            .map(|(id, _)| id)
            .collect();
        self.delete_challenges(expired.into_iter());
    }

    /// Delete challenges' objects, in the background (a leftover one only
    /// costs its few bytes: its nonce is single-use).
    fn delete_challenges(&self, ids: impl Iterator<Item = String>) {
        let paths: Vec<_> = ids
            .map(|id| object_store::path::Path::from(object(&id)))
            .collect();
        if paths.is_empty() {
            return;
        }
        let store = self.objects.clone();
        tokio::spawn(async move {
            if let Err(e) = store.delete_objects(paths).await {
                tracing::debug!("deleting answered challenges: {e:#}");
            }
        });
    }
}

#[tonic::async_trait]
impl membership_server::Membership for Ledger {
    async fn challenge(
        &self,
        req: Request<ChallengeRequest>,
    ) -> Result<Response<ChallengeResponse>, Status> {
        let key = token::public_key(&req.into_inner().public_key)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        // Expired ones first make room.
        self.expire_challenges();
        if self.challenges.lock().len() >= MAX_PENDING {
            return Err(Status::resource_exhausted(
                "too many challenges are waiting for answers: try again later",
            ));
        }
        let id = hex::encode(rand::random::<[u8; 16]>());
        let nonce: [u8; 32] = rand::random();
        let object = object(&id);
        let written = self
            .objects
            .put_if(&object, bytes::Bytes::copy_from_slice(&nonce), None)
            .await
            .map_err(|e| Status::unavailable(format!("writing the challenge: {e:#}")))?;
        if written.is_none() {
            return Err(Status::internal("a challenge with this id exists"));
        }
        let pending = Pending {
            nonce,
            key,
            deadline: Instant::now() + ANSWER_WITHIN,
        };
        self.challenges.lock().insert(id.clone(), pending);
        Ok(Response::new(ChallengeResponse { id, object }))
    }

    async fn join(&self, req: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let r = req.into_inner();
        let refused = || Status::permission_denied("the challenge was not answered");
        let key = token::public_key(&r.public_key)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        // Single use: a wrong answer spends it too.
        let pending = self.take_challenge(&r.id).ok_or_else(refused)?;
        let message = auth::challenge_message(&pending.nonce, key.as_bytes());
        if pending.key != key || !token::signed(&key, message.as_bytes(), &r.signature) {
            return Err(refused());
        }
        let token = self.issue(&auth::thumbprint(key.as_bytes()));
        Ok(Response::new(JoinResponse {
            token,
            expires_in: self.config.token_ttl.as_secs(),
        }))
    }
}
