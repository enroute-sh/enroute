//! The worker's end: read the staged pack, ingest it, stream frames back.
//!
//! Knows nothing about Lambda — the binary owns the event shape and the
//! response stream — so a push can be exercised without a deployed function.

use std::convert::Infallible;
use std::io::Cursor;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use bytes::Bytes;
use futures::channel::mpsc;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt as _};
use tokio_util::io::StreamReader;

use enroute_git_core::Error;
use enroute_git_cost::{CountingStore, Meter, StoreRole};
use enroute_git_ingest::{IngestProgress, IngestWorker, Ingested, LocalIngestWorker};

use crate::{convert, wire};

/// How often to emit a progress frame.
///
/// Matches the front door's poll interval, so an emitted frame is what makes
/// its next poll see something new.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

/// Where a worker's frames go.
///
/// `Infallible` because the errors that matter are carried *in* the stream,
/// as [`wire::Frame::Failed`].
pub type FrameSink = mpsc::UnboundedSender<Result<Bytes, Infallible>>;

/// Everything an invocation needs, built once per execution environment.
///
/// Holds a [`LocalIngestWorker`]: the point of the Lambda is that the same
/// in-process ingestion runs on more cores, not that it runs differently.
#[derive(Debug, Clone)]
pub struct Server {
    worker: LocalIngestWorker,
    /// What Lambda bills this function's duration against.
    ///
    /// Passed in rather than read from the environment — this module has no
    /// business knowing it runs in one — and zero when unconfigured.
    memory_mb: u64,
}

impl Server {
    /// Ingest with `worker`, on a function configured with `memory_mb`.
    #[must_use]
    pub fn new(worker: LocalIngestWorker, memory_mb: u64) -> Self {
        Self { worker, memory_mb }
    }

    /// Serve one push, writing frames into `frames` until the push is decided.
    ///
    /// Never returns an error: a failure is a [`wire::Frame::Failed`], because
    /// the front door is waiting on the stream and has nowhere else to look.
    pub async fn serve(
        &self,
        packs: Arc<dyn ObjectStore>,
        call: wire::Call,
        frames: &FrameSink,
        started: Instant,
    ) {
        let meter = Meter::new();
        // Reads of what the front door left here are this invocation's, and
        // nobody else can see them.
        let packs = CountingStore::wrap(packs, Arc::clone(&meter), StoreRole::Handoff);

        // Filled in by `run` as each key is read: the pack's is inside the
        // push, so a staged push has to be fetched before it is even known.
        let mut staged = Vec::new();
        let outcome = self
            .run(&meter, packs.as_ref(), call.push, frames, &mut staged)
            .await;
        // Read once the push has settled, either way: a failure spent
        // everything it had spent by then, and reporting nothing for it would
        // leave the record biased toward cheap successes.
        let cost = Some(convert::cost_to_wire(
            meter.units(),
            self.memory_mb,
            started.elapsed(),
        ));
        let frame = match outcome {
            Ok(ingested) => wire::Frame::Done { ingested, cost },
            // Alternate form so an `anyhow` chain renders: the outermost message
            // alone is often just "reading staged pack X".
            Err(e) => {
                // Logged as well as reported: without this the only account of
                // a failed push is the sentence that reached the client.
                tracing::error!(error = %format!("{e:#}"), "ingest failed");
                wire::Frame::Failed {
                    message: format!("{e:#}"),
                    cost,
                }
            }
        };
        send(frames, &frame);

        // Unconditional, including after a failure: the client re-pushes
        // rather than retrying this pack. The lifecycle rule is the backstop
        // for packs whose worker died before getting here.
        for key in staged {
            if let Err(e) = packs.delete(&key).await {
                tracing::warn!(error = %e, key = %key, "leaving staged object for the lifecycle rule");
            }
        }
    }

    async fn run(
        &self,
        meter: &Arc<Meter>,
        packs: &dyn ObjectStore,
        push: wire::Push,
        frames: &FrameSink,
        staged: &mut Vec<StorePath>,
    ) -> Result<wire::Ingested, Error> {
        let request = match push {
            wire::Push::Inline(request) => request,
            wire::Push::Staged(from) => {
                // Registered before the read, so a push that fails to parse is
                // still collected rather than left for the lifecycle rule.
                staged.push(StorePath::from(from.key.as_str()));
                read_push(packs, &from).await?
            }
        };
        if let wire::Pack::Staged(from) = &request.pack {
            staged.push(StorePath::from(from.key.as_str()));
        }

        let pack = open(packs, &request.pack).await?;
        let request = convert::request_from_wire(request)?;

        let ingested = self.ingest_reporting(meter, request, pack, frames).await?;
        Ok(convert::ingested_to_wire(ingested))
    }

    /// Run the ingest, emitting the stage it most recently reached every
    /// [`PROGRESS_INTERVAL`].
    ///
    /// Progress arrives per resolved object — hundreds of thousands on a large
    /// push — so it lands in a latest-value-wins slot the ticker reads.
    async fn ingest_reporting(
        &self,
        meter: &Arc<Meter>,
        request: enroute_git_ingest::IngestRequest,
        pack: enroute_git_ingest::IncomingPack,
        frames: &FrameSink,
    ) -> Result<Ingested, Error> {
        let latest = Mutex::new(None);
        let on_progress = |progress: IngestProgress| {
            *latest.lock().unwrap_or_else(PoisonError::into_inner) = Some(progress);
        };

        // Already `Pin<Box<_>>`, so no further pinning to poll by reference.
        let mut ingest = self.worker.ingest(request, pack, &on_progress, meter);
        let mut ticker = tokio::time::interval(PROGRESS_INTERVAL);
        // Mirrors the front door's keepalive: `Delay` so a stage blocking past
        // an interval emits no burst of catch-up frames, and the immediate
        // first tick discarded because no stage has been reached yet.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            tokio::select! {
                outcomes = &mut ingest => return outcomes,
                _ = ticker.tick() => {
                    // Re-sent even when unchanged: a frame every interval is
                    // also what tells the front door the worker is alive.
                    let stage = *latest.lock().unwrap_or_else(PoisonError::into_inner);
                    if let Some(stage) = stage {
                        send(frames, &wire::Frame::Progress(wire::Progress::from_git(stage)));
                    }
                }
            }
        }
    }
}

/// Fetch what the front door staged, refusing a length it doesn't claim.
///
/// Caught here rather than downstream, where a mismatch would surface as a
/// corrupt pack or malformed JSON and misdirect whoever reads it.
async fn get_staged(
    packs: &dyn ObjectStore,
    what: &str,
    from: &wire::Staged,
) -> Result<object_store::GetResult, Error> {
    let path = StorePath::from(from.key.as_str());
    let result = packs
        .get(&path)
        .await
        .with_context(|| format!("reading staged {what} {path}"))?;

    let expected = from.len;
    if result.meta.size != expected {
        return Err(Error::Invalid(format!(
            "staged {what} {path} is {} bytes, expected {expected}",
            result.meta.size
        )));
    }
    Ok(result)
}

/// Read a push the call was too small to carry.
///
/// Whole rather than streamed: it is one JSON document, and `serde_json` parses
/// a slice without copying the strings out of it.
async fn read_push(packs: &dyn ObjectStore, from: &wire::Staged) -> Result<wire::Request, Error> {
    let key = &from.key;
    let body = get_staged(packs, "push", from)
        .await?
        .bytes()
        .await
        .with_context(|| format!("reading staged push {key}"))?;
    Ok(serde_json::from_slice(&body).with_context(|| format!("parsing staged push {key}"))?)
}

/// A reader over the pack, wherever it arrived from.
///
/// A staged one streams rather than fetches in concurrent ranges: ingestion
/// is CPU-bound on inflate, so a single reader keeps well ahead of it.
async fn open(
    packs: &dyn ObjectStore,
    pack: &wire::Pack,
) -> Result<enroute_git_ingest::IncomingPack, Error> {
    let from = match pack {
        // Cheap to clone — the bytes are refcounted, not copied.
        wire::Pack::Inline { bytes } => {
            return Ok(enroute_git_ingest::IncomingPack {
                reader: Box::new(Cursor::new(bytes.clone())),
                len_hint: u64::try_from(bytes.len()).ok(),
            });
        }
        wire::Pack::Staged(from) => from,
    };

    let result = get_staged(packs, "pack", from).await?;
    Ok(enroute_git_ingest::IncomingPack {
        reader: Box::new(StreamReader::new(result.into_stream())),
        len_hint: Some(from.len),
    })
}

/// Write one frame, newline-terminated.
///
/// Best-effort by construction: the only way this fails is the front door
/// having hung up.
pub fn send(frames: &FrameSink, frame: &wire::Frame) {
    let Ok(mut line) = serde_json::to_vec(frame) else {
        tracing::error!("failed to encode a frame");
        return;
    };
    line.push(b'\n');
    drop(frames.unbounded_send(Ok(Bytes::from(line))));
}
