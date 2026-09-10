//! The ingest worker: one invocation per push.
//!
//! Owns the Lambda-shaped parts — the event, the response stream, the
//! lifecycle — not what a push means, which is
//! [`enroute_ingest_lambda::server`]. Streamed frames arrive unbuffered
//! every ~250ms, so progress needs no padding. The secrets extension
//! answers during `INVOKE` only, so anything built from a secret is built
//! on the first invocation. Spans need an explicit flush before the stream
//! ends, or a queued batch is lost if Lambda reaps the environment first.

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};

use std::time::Instant;

use bytes::Bytes;
use futures::channel::mpsc;
use http::{HeaderMap, HeaderValue, StatusCode};
use lambda_runtime::{Error, LambdaEvent, MetadataPrelude, StreamResponse, service_fn};
use tokio::sync::OnceCell;
use tracing::Instrument as _;

use enroute_ingest_lambda::server::{self, FrameSink, Server};
use enroute_ingest_lambda::{boot, telemetry, trace, wire};

/// False only for the first invocation an environment serves, so a slow push
/// can be told from one that waited on a cold start.
static WARM: AtomicBool = AtomicBool::new(false);

/// Built on the first invocation, for want of a secret during `INIT`.
///
/// Left uninitialized on failure, so the next invocation retries.
static BOOT: OnceCell<boot::Boot> = OnceCell::const_new();

/// The worker, kept against the bucket URI it was built for.
///
/// Not in [`BOOT`], because which bucket this writes to arrives in the call:
/// a warm environment reuses it while the front door keeps naming the same one.
static SERVER: boot::Kept<Server> = boot::Kept::new();

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

    // Before the span: on a cold start this attaches the export layer, and a
    // span created before that exists is never exported.
    let booting = Instant::now();
    let ready = ready().await;

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
    ready: Result<&'static boot::Boot, anyhow::Error>,
    call: wire::Call,
    frames: &FrameSink,
    invoked: Instant,
) {
    let booted = match ready {
        Ok(booted) => booted,
        Err(e) => return refuse(frames, &e),
    };

    // Both stores come from the call, so the front door and this cannot be
    // pointed at different buckets.
    let server = match server(booted, &call.objects) {
        Ok(server) => server,
        Err(e) => return refuse(frames, &e),
    };
    // Per invocation, unlike the worker: signed with the execution role's
    // credentials, which the runtime refreshes between invocations.
    let packs = match boot::packs(&call.staging) {
        Ok(packs) => packs,
        Err(e) => return refuse(frames, &e),
    };

    server.serve(packs, call, frames, invoked).await;
}

/// The worker for `objects`, reused while the front door keeps naming it.
fn server(
    booted: &boot::Boot,
    objects: &enroute_config::StoreUri,
) -> Result<Server, anyhow::Error> {
    SERVER.get_or_build(objects.to_uri(), || boot::server(booted, objects))
}

/// Everything that has to hold before a push can be attempted.
async fn ready() -> Result<&'static boot::Boot, anyhow::Error> {
    // A warm invocation has everything the secret feeds, so re-fetching would
    // cost a round trip per push. Rotation therefore doesn't reach a warm
    // environment — documented behaviour of the Lambda runtime.
    if let Some(booted) = BOOT.get() {
        return Ok(booted);
    }

    // From the result rather than after `?`, so a worker that can't read its
    // secret still has a console to say so on.
    let secret = boot::secret().await;
    telemetry::install(secret.as_ref().ok().and_then(boot::Secret::otlp_headers));
    let secret = secret?;

    BOOT.get_or_try_init(|| boot::boot(&secret)).await
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
    // No subscriber here: it needs a secret only `INVOKE` can reach. See
    // `telemetry::install`.
    lambda_runtime::run(service_fn(handler)).await
}
