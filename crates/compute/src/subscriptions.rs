//! Follow many repositories on one ledger connection: the client of the
//! ledger's `Watch` call (`proto/ledger.proto`). Each [`Mux`] keeps one
//! `Watch` stream on its connection, opened on first use and again after it
//! breaks, and every follow through it is one subscription there.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tonic::Status;
use tonic::transport::Channel;

use enroute_shared::auth::{AuthChannel, Bearer, Token};
use enroute_shared::proto::{
    Ended, Subscribe, Update, WatchRequest, ledger_client, watch_event, watch_request,
};
use enroute_shared::roles::FollowStream;

/// Updates a subscription holds for its follower. One that falls this far
/// behind holds up the call, so the ledger node's queue for it fills
/// instead, and the node sends what it missed once it catches up.
const QUEUE: usize = 256;

/// How long one follower may hold up its connection's other subscriptions:
/// past it, its subscription ends and it follows again (from a whole
/// state), so a stuck follower cannot stall every repository on the call.
const STALL: Duration = Duration::from_secs(1);

/// The `Watch` stream of one ledger connection, shared by its follows.
#[derive(Debug)]
pub struct Mux {
    client: ledger_client::LedgerClient<AuthChannel>,
    /// The token the calls carry, sent again on the live call when it is
    /// renewed (the call outlives it).
    token: Token,
    stream: Mutex<Option<Arc<Stream>>>,
}

/// One `Watch` call, and its live subscriptions.
#[derive(Debug)]
struct Stream {
    requests: mpsc::UnboundedSender<WatchRequest>,
    subs: Mutex<Subs>,
}

#[derive(Debug, Default)]
struct Subs {
    next: u64,
    /// Where each subscription's updates go.
    live: HashMap<u64, mpsc::Sender<Result<Update, Status>>>,
    /// The call is over: nothing subscribes on it again.
    over: bool,
}

impl Mux {
    /// A mux on a lazy connection to the ledger node at `url`, presenting
    /// `bearer`'s token.
    ///
    /// # Errors
    ///
    /// Returns an error if `url` is not a valid URI.
    pub fn connect(url: &str, bearer: &Bearer) -> anyhow::Result<Mux> {
        Ok(Mux::new(enroute_shared::auth::channel(url)?, bearer))
    }

    /// A mux on `channel`, one ledger connection, presenting `bearer`'s
    /// token.
    pub(crate) fn new(channel: Channel, bearer: &Bearer) -> Mux {
        // A subscription starts with the repository's whole state, inline
        // layers included (well over tonic's 4 MiB default under load).
        let client = bearer.ledger(channel);
        Mux {
            client,
            token: bearer.token(),
            stream: Mutex::default(),
        }
    }

    /// Follow `repo`: its updates, or the status the ledger node refused
    /// the subscription with.
    ///
    /// # Errors
    ///
    /// Returns the status of a refused or failed subscription.
    pub async fn follow(&self, repo: &str) -> Result<FollowStream, Status> {
        let (id, stream, mut rx) = self.subscribe(repo);
        let guard = Cancel { stream, id };
        let first = match rx.recv().await {
            Some(Ok(u)) => u,
            Some(Err(s)) => return Err(s),
            None => return Ok(futures::stream::empty().boxed()),
        };
        let rest = futures::stream::unfold((rx, guard), |(mut rx, guard)| async move {
            Some((rx.recv().await?, (rx, guard)))
        });
        Ok(futures::stream::once(async { Ok(first) })
            .chain(rest)
            .boxed())
    }

    /// Register a subscription to `repo` on the live call (opening one if
    /// there is none), and send it.
    fn subscribe(&self, repo: &str) -> (u64, Arc<Stream>, mpsc::Receiver<Result<Update, Status>>) {
        let (tx, rx) = mpsc::channel(QUEUE);
        let mut current = self.stream.lock();
        loop {
            let stream = match &*current {
                Some(s) => s.clone(),
                None => current.insert(self.open()).clone(),
            };
            let mut subs = stream.subs.lock();
            if subs.over {
                drop(subs);
                *current = None;
                continue;
            }
            let id = subs.next;
            subs.next += 1;
            subs.live.insert(id, tx);
            drop(subs);
            let sub = Subscribe {
                id,
                repo: repo.to_string(),
            };
            send(&stream, watch_request::Kind::Subscribe(sub));
            return (id, stream, rx);
        }
    }

    /// Start a `Watch` call, which a task then reads.
    fn open(&self) -> Arc<Stream> {
        let (requests, rx) = mpsc::unbounded_channel();
        let stream = Arc::new(Stream {
            requests,
            subs: Mutex::default(),
        });
        let mut token = self.token.clone();
        token.mark_unchanged();
        tokio::spawn(read(self.client.clone(), rx, stream.clone()));
        tokio::spawn(refresh(token, Arc::downgrade(&stream)));
        stream
    }
}

/// Send each renewed token on the call, while it lasts.
async fn refresh(mut token: Token, stream: std::sync::Weak<Stream>) {
    while token.changed().await.is_ok() {
        let renewed = token.borrow_and_update().clone();
        let Some(stream) = stream.upgrade() else {
            return;
        };
        if stream.subs.lock().over {
            return;
        }
        if let Some(t) = renewed {
            send(&stream, watch_request::Kind::Refresh(t.to_string()));
        }
    }
}

/// Send `kind` on the call, unless it is over (then nobody reads it).
fn send(stream: &Stream, kind: watch_request::Kind) {
    drop(stream.requests.send(WatchRequest { kind: Some(kind) }));
}

/// Make the call and hand each event to its subscription, until the call
/// ends. Then every subscription still live ends with the call's error.
async fn read(
    mut client: ledger_client::LedgerClient<AuthChannel>,
    requests: mpsc::UnboundedReceiver<WatchRequest>,
    stream: Arc<Stream>,
) {
    let requests =
        futures::stream::unfold(
            requests,
            |mut rx| async move { Some((rx.recv().await?, rx)) },
        );
    let error = match client.watch(requests).await {
        Ok(events) => {
            let mut events = events.into_inner();
            loop {
                match events.next().await {
                    Some(Ok(e)) => deliver(&stream, e.id, e.kind).await,
                    Some(Err(s)) => break s,
                    None => break Status::unavailable("the ledger node ended the subscriptions"),
                }
            }
        }
        Err(s) => s,
    };
    let live = {
        let mut subs = stream.subs.lock();
        subs.over = true;
        std::mem::take(&mut subs.live)
    };
    for tx in live.into_values() {
        drop(tx.try_send(Err(error.clone())));
    }
}

/// Hand an event to subscription `id`, waiting while its follower's queue
/// is full (as etcd's watch streams do), for up to [`STALL`]. Dropping it at
/// once would make the follower follow again, which costs the ledger node a
/// whole state.
async fn deliver(stream: &Stream, id: u64, kind: Option<watch_event::Kind>) {
    match kind {
        Some(watch_event::Kind::Update(u)) => {
            let Some(tx) = stream.subs.lock().live.get(&id).cloned() else {
                return;
            };
            // A follower that is gone has cancelled (see [`Cancel`]).
            if tokio::time::timeout(STALL, tx.send(Ok(u))).await.is_err()
                && stream.subs.lock().live.remove(&id).is_some()
            {
                // Its updates end once it drains them: it follows again.
                tracing::info!(id, "a follower fell behind; it follows again");
                send(stream, watch_request::Kind::Cancel(id));
            }
        }
        Some(watch_event::Kind::Ended(e)) => {
            if let Some(tx) = stream.subs.lock().live.remove(&id)
                && let Some(s) = ended_status(&e)
            {
                drop(tx.try_send(Err(s)));
            }
        }
        None => {}
    }
}

/// The status an [`Ended`] carries, if the subscription failed.
fn ended_status(e: &Ended) -> Option<Status> {
    (e.code != 0).then(|| Status::new(tonic::Code::from(e.code), e.message.clone()))
}

/// Cancels its subscription when the follower drops the updates.
struct Cancel {
    stream: Arc<Stream>,
    id: u64,
}

impl Drop for Cancel {
    fn drop(&mut self) {
        if self.stream.subs.lock().live.remove(&self.id).is_some() {
            send(&self.stream, watch_request::Kind::Cancel(self.id));
        }
    }
}
