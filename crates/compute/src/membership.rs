//! How a compute node finds the ledger and earns the token its calls to it
//! carry. It reads the members from the object store
//! ([`enroute_shared::cluster`]), and keeps reading them, so it calls every
//! member directly. It joins through the `Membership` service
//! (`proto/ledger.proto`) with a key of its own, generated at start, by
//! reading the nonce a ledger node puts in the object store and signing
//! it; it joins again before the token expires.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use ed25519_dalek::{Signer as _, SigningKey};
use tokio::sync::watch;

use enroute_shared::auth::{self, Token};
use enroute_shared::proto::{ChallengeRequest, JoinRequest, membership_client};
use enroute_shared::store::Objects;

use crate::remote_ledger::LedgerUrls;

/// How often a compute node reads the members again.
const MEMBERS_EVERY: Duration = Duration::from_secs(10);

/// The ledger's members' URLs as the object store has them, read now and
/// every [`MEMBERS_EVERY`] while anybody holds them.
#[must_use]
pub fn members(objects: Arc<Objects>) -> LedgerUrls {
    let urls: Arc<parking_lot::RwLock<Arc<[String]>>> = Arc::default();
    let weak = Arc::downgrade(&urls);
    tokio::spawn(async move {
        loop {
            let read = member_urls(&objects).await;
            let Some(urls) = weak.upgrade() else { return };
            match read {
                Ok(now) => *urls.write() = now,
                Err(e) => tracing::warn!("reading the ledger's members: {e:#}"),
            }
            drop(urls);
            tokio::time::sleep(MEMBERS_EVERY).await;
        }
    });
    Arc::new(move || urls.read().clone())
}

/// The members' URLs, sorted and distinct.
async fn member_urls(objects: &Objects) -> Result<Arc<[String]>> {
    let cluster = enroute_shared::cluster::read(objects).await?;
    let mut urls: Vec<String> = (cluster.unwrap_or_default().members.into_iter())
        .map(|m| m.url)
        .collect();
    urls.sort_unstable();
    urls.dedup();
    Ok(urls.into())
}

/// A failed join is tried again after this long.
const RETRY: Duration = Duration::from_secs(1);

/// A ledger node that has not answered a join in this long is skipped.
const JOIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Join one of the ledger nodes at `urls`, reading challenges from
/// `objects`, and keep the token fresh until nobody holds it. It is None
/// until the first join succeeds.
#[must_use]
pub fn join(urls: LedgerUrls, objects: Arc<Objects>) -> Token {
    let (tx, rx) = watch::channel(None);
    tokio::spawn(async move {
        let key = SigningKey::from_bytes(&rand::random());
        loop {
            let wait = match join_once(&urls, &objects, &key).await {
                Ok((token, ttl)) => {
                    tx.send_replace(Some(Arc::from(token)));
                    // Renew with a third of its life left.
                    ttl * 2 / 3
                }
                Err(e) => {
                    tracing::warn!("joining the ledger: {e:#}");
                    RETRY
                }
            };
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = tx.closed() => return,
            }
        }
    });
    rx
}

/// Join one of the ledger nodes: a token and how long it lasts.
async fn join_once(
    urls: &LedgerUrls,
    objects: &Objects,
    key: &SigningKey,
) -> Result<(String, Duration)> {
    let mut urls = urls().to_vec();
    anyhow::ensure!(
        !urls.is_empty(),
        "the object store names no ledger member yet"
    );
    // Spread joins over the ledger nodes.
    let start = rand::random_range(0..urls.len());
    urls.rotate_left(start);
    let mut last = None;
    for url in urls {
        let joined = tokio::time::timeout(JOIN_TIMEOUT, join_at(&url, objects, key))
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("no answer in {JOIN_TIMEOUT:?}")));
        match joined {
            Ok(joined) => return Ok(joined),
            Err(e) => last = Some(e.context(format!("ledger node at {url}"))),
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("no ledger node answered")))
}

/// Join the ledger node at `url` with `key`, reading its challenge from
/// `objects`: a token and how long it lasts.
///
/// # Errors
///
/// Returns an error if the ledger node refuses, or cannot be reached.
pub async fn join_at(url: &str, objects: &Objects, key: &SigningKey) -> Result<(String, Duration)> {
    let mut client = membership_client::MembershipClient::new(auth::channel(url)?);
    let public = key.verifying_key();
    let challenge = client
        .challenge(ChallengeRequest {
            public_key: public.as_bytes().to_vec().into(),
        })
        .await?
        .into_inner();
    let nonce = objects
        .get_object(&challenge.object)
        .await?
        .context("the challenge's nonce is not in the object store")?;
    let message = auth::challenge_message(&nonce, public.as_bytes());
    let joined = client
        .join(JoinRequest {
            public_key: public.as_bytes().to_vec().into(),
            id: challenge.id,
            signature: key.sign(message.as_bytes()).to_bytes().to_vec().into(),
        })
        .await?
        .into_inner();
    Ok((joined.token, Duration::from_secs(joined.expires_in)))
}
