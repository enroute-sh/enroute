//! The ingest worker: one invocation per push.
//!
//! Owns the Lambda-shaped parts — the event, the response stream, the
//! lifecycle — not what a push means, which is
//! [`enroute_ingest_lambda::server`]. Streamed frames arrive unbuffered
//! every ~250ms, so progress needs no padding. Credentials arrive with the
//! push, so anything built from them is built on the first invocation and
//! kept against them. Spans need an explicit flush before the stream ends,
//! or a queued batch is lost if Lambda reaps the environment first.

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};

use std::time::Instant;

use bytes::Bytes;
use futures::channel::mpsc;
use http::{HeaderMap, HeaderValue, StatusCode};
use lambda_runtime::{Error, LambdaEvent, MetadataPrelude, StreamResponse, service_fn};
use tracing::Instrument as _;

use enroute_ingest_lambda::server::{self, FrameSink};
use enroute_ingest_lambda::{boot, telemetry, trace, wire};

/// False only for the first invocation an environment serves, so a slow push
/// can be told from one that waited on a cold start.
static WARM: AtomicBool = AtomicBool::new(false);

async fn handler(
    event: LambdaEvent<wire::Call>,
) -> Result<StreamResponse<mpsc::UnboundedReceiver<Result<Bytes, Infallible>>>, Error> {
    // First thing, because Lambda bills from here — including the boot below,
    // on a cold start.
    let invoked = Instant::now();
    let cold = !WARM.swap(true, Ordering::Relaxed);
    let LambdaEvent { payload, context } = event;

    // Unbounded because the producer is a progress ticker that cannot await: it
    // emits a few dozen bytes every 250ms, so there is nothing here for
    // backpressure to protect.
    let (tx, rx) = mpsc::unbounded::<Result<Bytes, Infallible>>();

    let booting = Instant::now();
    // Before anything that might want a span exported, and before the pool, so
    // a worker that cannot connect still has a console to say so on.
    telemetry::install(payload.telemetry.as_ref());
    let ready = boot::ready(&payload).await;

    let span = tracing::info_span!(
        "ingest_lambda.invoke",
        faas.invocation_id = %context.request_id,
        faas.coldstart = cold,
        // A field, not a child span: the span cannot exist until this work has
        // built the layer it would be recorded through.
        boot_ms = u64::try_from(booting.elapsed().as_millis()).unwrap_or(u64::MAX),
    );
    // What makes a push one trace rather than two.
    trace::adopt_caller(&span, payload.traceparent.as_deref());

    tokio::spawn(async move {
        // Dropped before the flush: a span reaches the exporter when it
        // *closes*, so flushing inside it would export an empty batch.
        run(ready, payload, &tx, invoked).instrument(span).await;
        // Before the stream ends, deliberately — see the module docs. The front
        // door has its outcomes by now and doesn't wait on this.
        telemetry::flush().await;
        drop(tx);
    });

    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-ndjson"),
    );

    Ok(StreamResponse {
        // Always 200, with failures reported as frames: a message the front door
        // can show the pushing client beats a status code it must translate.
        metadata_prelude: MetadataPrelude {
            status_code: StatusCode::OK,
            headers,
            cookies: vec![],
        },
        stream: rx,
    })
}

async fn run(
    ready: Result<boot::Derived, anyhow::Error>,
    call: wire::Call,
    frames: &FrameSink,
    invoked: Instant,
) {
    let derived = match ready {
        Ok(derived) => derived,
        Err(e) => return refuse(frames, &e),
    };
    derived
        .server
        .serve(derived.packs, call, frames, invoked)
        .await;
}

/// Reported in the stream rather than as a status, and logged too so it lands
/// in the trace even if the front door has given up on the connection.
fn refuse(frames: &FrameSink, error: &anyhow::Error) {
    tracing::error!(error = %format!("{error:#}"), "refusing the push");
    server::send(
        frames,
        &wire::Frame::Failed {
            message: format!("{error:#}"),
            // Nothing was built, so nothing was spent — as distinct from a
            // push that failed after spending, which reports what it spent.
            cost: None,
        },
    );
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // No subscriber here: the collector arrives with the call. See
    // `telemetry::install`.
    lambda_runtime::run(service_fn(handler)).await
}
