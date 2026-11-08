//! The ledger's `Watch` call (`proto/ledger.proto`) for the [`Ledger`]: each
//! subscription is one follow, and their updates share the one response
//! stream.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{BoxStream, FuturesUnordered, SelectAll};
use futures::{FutureExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tonic::{Status, Streaming};

use enroute_shared::proto::{
    Ended, Subscribe, Update, WatchEvent, WatchRequest, watch_event, watch_request,
};
use enroute_shared::roles::{Follow, WatchStream};

use crate::ledger::Ledger;

/// Events queued for the response stream before the subscriptions wait.
const QUEUE: usize = 1024;

/// Serve one `Watch` call with `ledger`, until the client ends it,
/// `stopping` becomes true, or its token expires (at `expires`, seconds
/// since the epoch, unless the client refreshes it).
///
/// A stopping server waits for its open streams, and this one would
/// otherwise not end.
pub(crate) fn serve(
    ledger: Arc<Ledger>,
    requests: Streaming<WatchRequest>,
    mut stopping: tokio::sync::watch::Receiver<bool>,
    expires: Option<u64>,
) -> WatchStream {
    let (tx, rx) = mpsc::channel(QUEUE);
    // Without a sender, nothing stops it.
    let stopped = async move {
        if stopping.wait_for(|&s| s).await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    let serve = run(ledger, requests, tx.clone(), expires);
    tokio::spawn(async move {
        tokio::select! {
            () = serve => {}
            () = stopped => {
                let stop = Status::unavailable("the ledger node is stopping");
                drop(tx.send(Err(stop)).await);
            }
        }
    });
    futures::stream::unfold(rx, |mut rx| async move { Some((rx.recv().await?, rx)) }).boxed()
}

/// The end of subscription `id`, with `status` if it failed.
fn ended(id: u64, status: Option<&Status>) -> WatchEvent {
    let (code, message) = status.map_or((0, String::new()), |s| {
        (i32::from(s.code()), s.message().to_string())
    });
    WatchEvent {
        id,
        kind: Some(watch_event::Kind::Ended(Ended { code, message })),
    }
}

/// A subscription's events: its updates until it ends or `cancel` fires
/// (or is dropped), then its end.
fn events(
    id: u64,
    updates: BoxStream<'static, Result<Update, Status>>,
    cancel: oneshot::Receiver<()>,
) -> BoxStream<'static, WatchEvent> {
    let updates = updates.take_until(cancel);
    futures::stream::unfold(Some(updates), move |updates| async move {
        let mut updates = updates?;
        Some(match updates.next().await {
            Some(Ok(u)) => {
                let kind = Some(watch_event::Kind::Update(u));
                (WatchEvent { id, kind }, Some(updates))
            }
            Some(Err(s)) => (ended(id, Some(&s)), None),
            None => (ended(id, None), None),
        })
    })
    .boxed()
}

/// When a token that expires at `exp` (seconds since the epoch) does.
fn instant(exp: u64) -> tokio::time::Instant {
    let left = exp.saturating_sub(crate::token::now());
    tokio::time::Instant::now() + std::time::Duration::from_secs(left)
}

/// Serve one `Watch` stream until the client ends it or goes away, or
/// its token expires (see [`serve`]).
async fn run(
    ledger: Arc<Ledger>,
    mut requests: Streaming<WatchRequest>,
    out: mpsc::Sender<Result<WatchEvent, Status>>,
    expires: Option<u64>,
) {
    let mut cancels: HashMap<u64, oneshot::Sender<()>> = HashMap::new();
    let mut starting = FuturesUnordered::new();
    let mut live: SelectAll<BoxStream<'static, WatchEvent>> = SelectAll::new();
    // Without a token (a standalone node), nothing expires: the arm is off.
    let expiry = tokio::time::sleep_until(instant(expires.unwrap_or_default()));
    tokio::pin!(expiry);
    loop {
        let event = tokio::select! {
            () = &mut expiry, if expires.is_some() => {
                drop(out.send(Err(Status::unauthenticated("the token expired"))).await);
                return;
            }
            r = requests.next() => match r.map(|r| r.map(|r| r.kind)) {
                Some(Ok(Some(watch_request::Kind::Subscribe(Subscribe { id, repo })))) => {
                    let (cancel, cancelled) = oneshot::channel();
                    cancels.insert(id, cancel);
                    let ledger = ledger.clone();
                    starting.push(async move {
                        (id, ledger.follow(&repo).await, cancelled)
                    }.boxed());
                    continue;
                }
                Some(Ok(Some(watch_request::Kind::Cancel(id)))) => {
                    cancels.remove(&id);
                    continue;
                }
                Some(Ok(Some(watch_request::Kind::Refresh(token)))) => {
                    match ledger.authenticate(&token) {
                        Ok(claims) => {
                            expiry.as_mut().reset(instant(claims.exp));
                            continue;
                        }
                        Err(e) => {
                            drop(out.send(Err(Status::unauthenticated(e.to_string()))).await);
                            return;
                        }
                    }
                }
                Some(Ok(None)) => continue,
                Some(Err(_)) | None => return,
            },
            Some((id, r, cancelled)) = starting.next() => match r {
                Ok(updates) => {
                    live.push(events(id, updates, cancelled));
                    continue;
                }
                Err(s) => {
                    cancels.remove(&id);
                    ended(id, Some(&s))
                }
            },
            Some(event) = live.next() => {
                if matches!(event.kind, Some(watch_event::Kind::Ended(_))) {
                    cancels.remove(&event.id);
                }
                event
            }
        };
        if out.send(Ok(event)).await.is_err() {
            return;
        }
    }
}
