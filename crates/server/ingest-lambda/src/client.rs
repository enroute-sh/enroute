//! The front door's end: deliver the pack, invoke the worker, relay what comes
//! back.
//!
//! A pack under [`INLINE_MAX`] rides inside the call, and the push does the
//! same if the encoded call still fits [`PAYLOAD_MAX`] — so a size can make
//! a push slower but never unrunnable. Staging here is deliberately not
//! `enroute_git_ingest`'s, whose sessions delete on `Drop`; ownership
//! transfers at invoke instead, and the lifecycle rule collects the rest.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context as _;
use aws_sdk_lambda::error::DisplayErrorContext;
use aws_sdk_lambda::operation::invoke_with_response_stream::InvokeWithResponseStreamOutput;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvokeWithResponseStreamResponseEvent as StreamEvent;
use bytes::BytesMut;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use tokio::io::AsyncReadExt as _;

use enroute_config::{Bucket, StoreUri};
use enroute_git_core::Error;
use enroute_git_cost::{CountingStore, Meter, StoreRole};
use enroute_git_ingest::{
    IncomingPack, IngestProgress, IngestRequest, IngestWorker, Ingested, PackReader, ProgressSink,
};
use enroute_git_store::ObjectWriter;

use crate::{convert, wire};

/// How much pack to buffer per staging write.
///
/// Matches the store's multipart part size, so a write maps onto one part.
const UPLOAD_CHUNK: usize = enroute_git_store::MULTIPART_CHUNK_BYTES;

/// What the read buffer starts at, and all a typical push ever needs.
///
/// Reserving a whole `UPLOAD_CHUNK` up front would cost a multi-megabyte
/// mapping per push to hold a few KB.
const INITIAL_READ: usize = 64 * 1024;

/// What Lambda allows a synchronous invocation's payload to be — a hard
/// quota, counted on the JSON bytes as sent.
const PAYLOAD_MAX: usize = 6 * 1024 * 1024;

/// Largest pack to send inside the call, dividing streamed from resident.
///
/// Far below [`PAYLOAD_MAX`]: a pack that size would take the staged-*push*
/// path instead, base64 in a document that is parsed, not streamed.
const INLINE_MAX: usize = 1024 * 1024;

// What lets one `fill` decide: it stops at `UPLOAD_CHUNK` or the pack's end, so
// a buffer at or under `INLINE_MAX` can only be the whole pack.
const _: () = assert!(INLINE_MAX < UPLOAD_CHUNK);

/// What Lambda puts between the response-streaming prelude and the payload.
///
/// A Function URL or API Gateway consumes the prelude; a direct invoke
/// hands it to us, so the payload starts after this.
const PRELUDE_END: [u8; 8] = [0; 8];

/// How far to look for [`PRELUDE_END`] before giving up.
///
/// AWS documents it as arriving within the first 16KB of the stream.
const PRELUDE_MAX: usize = 16 * 1024;

/// Where and how to reach the worker.
#[derive(Debug, Clone)]
pub struct LambdaConfig {
    /// Function name or ARN.
    pub function: String,
    /// Version or alias to pin, or `None` for `$LATEST` — a direct invoke
    /// can name a numbered version, which a Function URL cannot.
    pub qualifier: Option<String>,
    /// Largest pack accepted, enforced while uploading so an oversized
    /// push is refused in its first seconds, not after a full transfer.
    pub max_pack_bytes: u64,
    /// The bucket the worker writes through to, sent on every call.
    ///
    /// This process's own, so the two ends cannot be pointed at different
    /// ones — which nothing but a deploy checklist used to prevent.
    pub objects: StoreUri,
    /// The bucket a staged push and a staged pack are left in, sent beside
    /// the key so a key alone never has to be enough.
    ///
    /// Its credentials stay here: only `staging.uri` goes on the call, since
    /// an invoke payload is a thing `CloudTrail` keeps.
    pub staging: Bucket,
}

/// Ingests by invoking a Lambda directly.
#[derive(Debug, Clone)]
pub struct LambdaIngestWorker {
    /// A plain backend, not a `StagingStore`: nothing sweeps it, since the
    /// worker still reading it outlives this process's request.
    staging: Arc<dyn ObjectStore>,
    config: LambdaConfig,
    lambda: aws_sdk_lambda::Client,
}

impl LambdaIngestWorker {
    /// Invoke `config.function` through `lambda`, staging packs into the
    /// bucket `config.staging` names.
    ///
    /// Built here rather than passed in, so the bucket this uploads to and
    /// the one the call names are one value and cannot be given two answers.
    ///
    /// # Errors
    ///
    /// Returns an error if the staging store cannot be built.
    pub fn new(lambda: aws_sdk_lambda::Client, config: LambdaConfig) -> Result<Self, Error> {
        let staging = config
            .staging
            .build()
            .map_err(|error| error.context("the staging bucket"))?;
        Ok(Self {
            staging,
            config,
            lambda,
        })
    }

    /// Get `pack` to the worker: in the call if small enough, otherwise
    /// through the staging bucket at `key`.
    ///
    /// A claimed length is untrustworthy, so the first chunk is read either
    /// way and its size decides.
    #[tracing::instrument(skip_all, fields(inline, pack_bytes))]
    async fn deliver(
        &self,
        staging: &Arc<dyn ObjectStore>,
        key: &str,
        mut pack: PackReader,
    ) -> Result<wire::Pack, Error> {
        // Read straight into the buffer the writer takes, so a pack byte is
        // copied once on its way through.
        let mut buf = BytesMut::new();
        fill(&mut pack, &mut buf).await?;

        // A cap below `INLINE_MAX` lowers the threshold rather than being a
        // second check against it: `stage` enforces it for every pack alike.
        let inline_max = usize::try_from(self.config.max_pack_bytes)
            .unwrap_or(usize::MAX)
            .min(INLINE_MAX);

        // Recorded because which path a push took is otherwise invisible, and
        // the whole point of the threshold is how often it is met.
        let span = tracing::Span::current();
        span.record("inline", buf.len() <= inline_max);
        if buf.len() <= inline_max {
            span.record("pack_bytes", u64::try_from(buf.len()).unwrap_or(u64::MAX));
            // Frozen where it lies rather than copied out: `fill` doubles, so
            // the slack it carries is bounded by the pack's own size.
            return Ok(wire::Pack::Inline {
                bytes: buf.freeze(),
            });
        }

        let staged = self.stage(staging, key, pack, buf).await?;
        span.record("pack_bytes", staged.len);
        Ok(wire::Pack::Staged(staged))
    }

    /// Stream the rest of `pack` into staging behind the bytes already
    /// read into `buf`, refusing it past `max_pack_bytes`.
    async fn stage(
        &self,
        staging: &Arc<dyn ObjectStore>,
        key: &str,
        mut pack: PackReader,
        mut buf: BytesMut,
    ) -> Result<wire::Staged, Error> {
        let mut writer = ObjectWriter::new(staging.clone(), StorePath::from(key));
        let mut total: u64 = 0;

        while !buf.is_empty() {
            total = total.saturating_add(u64::try_from(buf.len()).unwrap_or(u64::MAX));
            if total > self.config.max_pack_bytes {
                // Abort rather than drop: an unfinished multipart upload is not
                // an object, so the expiry rule wouldn't see it.
                drop(writer.abort().await);
                return Err(Error::Invalid(format!(
                    "pack exceeds {} bytes",
                    self.config.max_pack_bytes
                )));
            }
            writer
                .write(buf.split().freeze())
                .await
                .context("staging pack")?;
            buf.reserve(UPLOAD_CHUNK);
            fill(&mut pack, &mut buf).await?;
        }

        writer.finish().await.context("finishing staged pack")?;

        Ok(wire::Staged {
            key: key.to_string(),
            len: total,
        })
    }

    /// The payload for one push, with the push moved to the bucket if the
    /// call cannot hold it.
    ///
    /// Encoded before the size is judged: the quota counts bytes as sent.
    async fn encode_call(
        &self,
        staging: &Arc<dyn ObjectStore>,
        push_key: &str,
        traceparent: Option<String>,
        push: wire::Request,
    ) -> Result<Vec<u8>, Error> {
        // Pre-sized: `to_vec` starts from nothing and doubles, so a payload
        // carrying a pack is copied through every size on the way up.
        let mut body = Vec::with_capacity(body_capacity(&push));
        let mut call = wire::Call {
            traceparent,
            objects: self.config.objects.clone(),
            staging: self.config.staging.uri.clone(),
            push: wire::Push::Inline(push),
        };
        serde_json::to_writer(&mut body, &call).context("encoding ingest call")?;
        if body.len() < PAYLOAD_MAX {
            return Ok(body);
        }

        // Only the push moves out of the payload, so only the push is written
        // again: a field rebuilt by hand is a field a later one can be missed
        // beside.
        let wire::Push::Inline(push) = call.push else {
            // Already a key and still over the quota — unreachable from here,
            // and there would be nothing left to move out of the payload.
            return Ok(body);
        };
        // `body` goes first: it holds a second copy of an inline pack, and
        // staging it runs for seconds.
        drop(body);
        call.push = wire::Push::Staged(self.stage_push(staging, push_key, push).await?);
        serde_json::to_vec(&call)
            .context("encoding ingest call")
            .map_err(Error::from)
    }

    /// Write the push into staging, for one whose refs run past the payload
    /// quota.
    ///
    /// Re-encodes rather than slicing the push out of the body already
    /// built, which would tie this to how `serde` nests it.
    async fn stage_push(
        &self,
        staging: &Arc<dyn ObjectStore>,
        key: &str,
        request: wire::Request,
    ) -> Result<wire::Staged, Error> {
        let encoded = serde_json::to_vec(&request).context("encoding staged push")?;
        let len = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        drop(request);

        let mut writer = ObjectWriter::new(staging.clone(), StorePath::from(key));
        writer
            .write(bytes::Bytes::from(encoded))
            .await
            .context("staging push")?;
        writer.finish().await.context("finishing staged push")?;

        Ok(wire::Staged {
            key: key.to_string(),
            len,
        })
    }

    /// Invoke the worker, returning its response stream.
    ///
    /// The SDK resolves and signs with credentials that refresh.
    async fn invoke(&self, body: Vec<u8>) -> Result<InvokeWithResponseStreamOutput, Error> {
        let mut call = self
            .lambda
            .invoke_with_response_stream()
            .function_name(&self.config.function)
            .payload(Blob::new(body));
        if let Some(qualifier) = &self.config.qualifier {
            call = call.qualifier(qualifier);
        }

        // `DisplayErrorContext`, or the cause — throttling, access denied —
        // is lost behind the outermost type name.
        let response = call.send().await.map_err(|e| {
            Error::from(anyhow::anyhow!(
                "invoking ingest worker: {}",
                DisplayErrorContext(&e)
            ))
        })?;

        Ok(response)
    }

    /// Read the worker's frames, reporting progress and returning the outcomes.
    async fn collect(
        mut response: InvokeWithResponseStreamOutput,
        progress: ProgressSink<'_>,
        meter: &Arc<Meter>,
    ) -> Result<Ingested, Error> {
        let mut frames = FrameStream::default();

        while let Some(event) = response.event_stream.recv().await.map_err(|e| {
            Error::from(anyhow::anyhow!(
                "reading worker stream: {}",
                DisplayErrorContext(&e)
            ))
        })? {
            // Returns on the outcome, not end-of-stream: the worker flushes
            // traces after reporting and the client needn't wait for that.
            if let Some(outcomes) = absorb(event, &mut frames, progress, meter)? {
                return Ok(outcomes);
            }
        }

        // A stream that ended without `Done` means the worker died mid-push.
        // Reporting the refs as rejected would claim knowledge we don't have.
        Err(Error::from(anyhow::anyhow!(
            "ingest worker closed its stream without reporting an outcome"
        )))
    }
}

/// About what `push` encodes to — a hint only, too small costs one realloc.
fn body_capacity(push: &wire::Request) -> usize {
    let pack = match &push.pack {
        // Exact: four characters per three bytes, and base64's alphabet has
        // nothing JSON would escape.
        wire::Pack::Inline { bytes } => bytes.len().div_ceil(3) * 4,
        wire::Pack::Staged(_) => 0,
    };
    // Counted rather than averaged: a refname has no length a constant could
    // assume, and undershooting costs the copy this exists to avoid.
    let updates: usize = push.updates.iter().map(|u| u.refname.len() + 128).sum();
    let existing: usize = push.existing.keys().map(|name| name.len() + 56).sum();
    pack + updates + existing + 512
}

/// Read until `buf` holds a whole part, or the pack ends.
///
/// Grows the buffer itself, since `read_buf` only reserves 64 bytes at a
/// time. Doubling stops once the pack is past [`INLINE_MAX`].
async fn fill(pack: &mut PackReader, buf: &mut BytesMut) -> Result<(), Error> {
    while buf.len() < UPLOAD_CHUNK {
        if buf.capacity() == buf.len() {
            let target = if buf.len() > INLINE_MAX {
                UPLOAD_CHUNK
            } else {
                buf.capacity()
                    .saturating_mul(2)
                    .clamp(INITIAL_READ, UPLOAD_CHUNK)
            };
            buf.reserve(target - buf.len());
        }
        if pack.read_buf(buf).await.context("reading pack")? == 0 {
            break;
        }
    }
    Ok(())
}

/// The worker's response, reassembled across chunk boundaries.
#[derive(Default)]
struct FrameStream {
    pending: Vec<u8>,
    /// Set once the delimiter has gone by — everything before belongs to
    /// Lambda, not the worker.
    past_prelude: bool,
}

impl FrameStream {
    fn push(&mut self, bytes: &[u8]) {
        self.pending.extend_from_slice(bytes);
    }

    /// Drop the metadata prelude, reporting whether the payload has begun.
    fn skip_prelude(&mut self) -> Result<bool, Error> {
        if self.past_prelude {
            return Ok(true);
        }

        let Some(at) = self
            .pending
            .windows(PRELUDE_END.len())
            .position(|w| w == PRELUDE_END)
        else {
            // Documented to arrive inside the first 16KB, so buffering past that
            // means this stream isn't the shape we think it is — better said
            // outright than as a missing outcome once it ends.
            if self.pending.len() > PRELUDE_MAX {
                return Err(Error::from(anyhow::anyhow!(
                    "no metadata prelude delimiter in the worker's first {PRELUDE_MAX} bytes"
                )));
            }
            return Ok(false);
        };

        self.pending.drain(..at + PRELUDE_END.len());
        self.past_prelude = true;
        Ok(true)
    }

    /// Frames, once the prelude is behind us.
    ///
    /// `None` while the delimiter has yet to arrive — those bytes are
    /// metadata, not frames.
    fn drain(
        &mut self,
        progress: ProgressSink<'_>,
        meter: &Arc<Meter>,
    ) -> Result<Option<Ingested>, Error> {
        if !self.skip_prelude()? {
            return Ok(None);
        }

        drain_frames(&mut self.pending, progress, meter)
    }
}

/// Fold one stream event into `frames`, returning the outcomes once a frame
/// carries them and `None` while the push is still running.
fn absorb(
    event: StreamEvent,
    frames: &mut FrameStream,
    progress: ProgressSink<'_>,
    meter: &Arc<Meter>,
) -> Result<Option<Ingested>, Error> {
    match event {
        StreamEvent::PayloadChunk(chunk) => {
            let Some(payload) = chunk.payload else {
                return Ok(None);
            };
            frames.push(&payload.into_inner());
            frames.drain(progress, meter)
        }
        // The invocation failed — timed out, out of memory, throttled — as
        // distinct from the push being rejected, which arrives as a frame.
        StreamEvent::InvokeComplete(done) => match done.error_code {
            Some(code) => Err(Error::from(anyhow::anyhow!(
                "ingest worker failed: {code}: {}",
                done.error_details.unwrap_or_default()
            ))),
            None => Ok(None),
        },
        // A variant added to the API since this was built.
        _ => Ok(None),
    }
}

/// Take every whole line out of `pending`, leaving any partial one buffered: a
/// chunk boundary lands wherever the network put it, not on a frame.
fn drain_frames(
    pending: &mut Vec<u8>,
    progress: ProgressSink<'_>,
    meter: &Arc<Meter>,
) -> Result<Option<Ingested>, Error> {
    while let Some(at) = pending.iter().position(|b| *b == b'\n') {
        let line: Vec<u8> = pending.drain(..=at).collect();
        // `at` indexed the newline, so dropping the last byte drops exactly it.
        let Some(payload) = line.split_last().map(|(_newline, rest)| rest) else {
            continue;
        };
        match parse_frame(payload) {
            Some(wire::Frame::Progress(stage)) => {
                if let Some(stage) = stage.to_git() {
                    progress(stage);
                }
            }
            Some(wire::Frame::Done { ingested, cost }) => {
                charge(meter, cost);
                return Ok(Some(convert::ingested_from_wire(ingested)));
            }
            Some(wire::Frame::Failed { message, cost }) => {
                // Charged before the error propagates: a push that died in the
                // worker still spent what it spent getting there.
                charge(meter, cost);
                return Err(Error::from(anyhow::anyhow!("ingest failed: {message}")));
            }
            // A worker a version ahead, or a blank keepalive line.
            Some(wire::Frame::Unknown) | None => {}
        }
    }
    Ok(None)
}

/// Fold what a worker reported into the push's meter.
///
/// A worker too old to report anything leaves it untouched, undercounting
/// rather than failing the push.
fn charge(meter: &Arc<Meter>, cost: Option<wire::Cost>) {
    if let Some(cost) = cost {
        meter.add(convert::cost_from_wire(cost));
    }
}

/// `None` for a blank or unparseable line: a worker a version ahead must not
/// fail an otherwise fine push.
fn parse_frame(line: &[u8]) -> Option<wire::Frame> {
    if line.iter().all(u8::is_ascii_whitespace) {
        return None;
    }
    match serde_json::from_slice::<wire::Frame>(line) {
        Ok(frame) => Some(frame),
        Err(e) => {
            tracing::warn!(error = %e, "unparseable frame from ingest worker, skipping");
            None
        }
    }
}

impl IngestWorker for LambdaIngestWorker {
    fn ingest<'a>(
        &'a self,
        request: IngestRequest,
        pack: IncomingPack,
        progress: ProgressSink<'a>,
        meter: &'a Arc<Meter>,
    ) -> Pin<Box<dyn Future<Output = Result<Ingested, Error>> + Send + 'a>> {
        Box::pin(async move {
            // The caller's meter, seen through the handoff bucket, so what
            // staging this push cost is charged to it and not to whichever
            // pushes are being dispatched alongside it.
            let staging = CountingStore::wrap(
                Arc::clone(&self.staging),
                Arc::clone(meter),
                StoreRole::Handoff,
            );

            // Keyed by repo and a fresh id, so two pushes cannot collide. The
            // `.push` sibling shares the id but not the key: a staged push may
            // name a staged pack, and one key cannot hold both.
            let id = uuid::Uuid::new_v4();
            let storage_key = request.repo.storage_key;
            let pack_key = format!("packs/{storage_key}/{id}.pack");

            // Set outside `convert`, which is a pure mapping and has no
            // business reading ambient trace state.
            let traceparent = crate::trace::outgoing_traceparent();
            // The hint is of no use here: `deliver` counts what it forwards,
            // and that count is what the worker is told.
            let delivered = self.deliver(&staging, &pack_key, pack.reader).await?;
            // The invoke below can cold-start the worker before it reports
            // anything of its own, and `deliver` returning means the client's
            // own meter has released the line this would land on.
            progress(IngestProgress::Dispatching);
            let push = convert::request_to_wire(request, delivered);
            let body = self
                .encode_call(
                    &staging,
                    &format!("packs/{storage_key}/{id}.push"),
                    traceparent,
                    push,
                )
                .await?;

            let response = self.invoke(body).await?;
            Self::collect(response, progress, meter).await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::sync::Mutex;

    use enroute_git_ingest::IngestProgress;
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;

    use super::*;

    const KEY: &str = "packs/test/one.pack";

    /// A worker whose Lambda client is never called — every test here stops at
    /// delivering the pack.
    ///
    /// `staging` is handed over so a test can read back what was uploaded; a
    /// real one builds it from `config.staging` and cannot disagree.
    fn worker(staging: Arc<dyn ObjectStore>, max_pack_bytes: u64) -> LambdaIngestWorker {
        let conf = aws_sdk_lambda::Config::builder()
            .behavior_version(aws_sdk_lambda::config::BehaviorVersion::latest())
            .region(aws_sdk_lambda::config::Region::new("eu-central-1"))
            .build();
        LambdaIngestWorker {
            staging,
            lambda: aws_sdk_lambda::Client::from_conf(conf),
            config: LambdaConfig {
                function: "test".to_string(),
                qualifier: None,
                max_pack_bytes,
                objects: "memory:///objects".parse().expect("a store URI"),
                staging: Bucket {
                    uri: "memory:///staging".parse().expect("a store URI"),
                    credentials: BTreeMap::new(),
                },
            },
        }
    }

    /// A one-ref push carrying `pack`.
    fn request_with(pack: wire::Pack) -> wire::Request {
        wire::Request {
            repo: wire::Repo {
                id: 1,
                storage_key: uuid::Uuid::nil().to_string(),
                default_branch: "refs/heads/main".to_string(),
            },
            existing: BTreeMap::new(),
            updates: vec![wire::RefUpdate {
                refname: "refs/heads/main".to_string(),
                old_id: "0".repeat(40),
                new_id: "a".repeat(40),
            }],
            pack,
        }
    }

    /// Deliver `pack` and assert the bucket holds exactly it, returning what
    /// the call would have carried.
    async fn staged_whole(pack: &[u8]) -> wire::Pack {
        let staging: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let delivered = worker(staging.clone(), u64::MAX)
            .deliver(&staging, KEY, reader(pack.to_vec()))
            .await
            .expect("a pack over the threshold should stage");

        let stored = staging
            .get(&StorePath::from(KEY))
            .await
            .expect("the pack should be in the bucket")
            .bytes()
            .await
            .expect("reading it back");
        assert_eq!(stored.as_ref(), pack);
        delivered
    }

    /// Bytes that differ position to position, so a mis-ordered or truncated
    /// write cannot compare equal.
    fn pack_of(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| u8::try_from(i % 251).unwrap_or_default())
            .collect()
    }

    fn reader(bytes: Vec<u8>) -> PackReader {
        Box::new(Cursor::new(bytes))
    }

    /// The optimization itself: a pack this small never reaches the bucket.
    #[tokio::test]
    async fn a_small_pack_travels_in_the_call() {
        let staging: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pack = vec![7u8; 4096];

        let delivered = worker(staging.clone(), u64::MAX)
            .deliver(&staging, KEY, reader(pack.clone()))
            .await
            .expect("a small pack should deliver");

        let wire::Pack::Inline { bytes } = delivered else {
            panic!("expected an inline pack, got {delivered:?}");
        };
        assert_eq!(bytes.as_ref(), pack.as_slice());
        assert!(
            staging.get(&StorePath::from(KEY)).await.is_err(),
            "an inline pack must not have been written to the bucket"
        );
    }

    /// A pack read to the end exactly at the threshold is still whole, so the
    /// boundary belongs to the inline path.
    #[tokio::test]
    async fn a_pack_exactly_at_the_threshold_still_travels_in_the_call() {
        let staging: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let delivered = worker(staging.clone(), u64::MAX)
            .deliver(&staging, KEY, reader(vec![7u8; INLINE_MAX]))
            .await
            .expect("a pack at the threshold should deliver");

        assert!(
            matches!(delivered, wire::Pack::Inline { .. }),
            "got {delivered:?}"
        );
    }

    /// One byte past it, the bytes already read must end up in the bucket
    /// rather than being dropped on the floor.
    #[tokio::test]
    async fn a_pack_past_the_threshold_is_staged_whole() {
        // Past `UPLOAD_CHUNK` too, so the buffered head and a later read both
        // have to land.
        let pack = pack_of(UPLOAD_CHUNK + 1024);
        let wire::Pack::Staged(at) = staged_whole(&pack).await else {
            panic!("expected a staged pack");
        };
        assert_eq!(at.key, KEY);
        assert_eq!(at.len, u64::try_from(pack.len()).unwrap_or(u64::MAX));
    }

    /// Between the threshold and a whole part, where `fill` grows to exactly
    /// `UPLOAD_CHUNK` and then meets the pack's end inside it.
    #[tokio::test]
    async fn a_pack_between_the_threshold_and_a_part_is_staged_whole() {
        let delivered = staged_whole(&pack_of(INLINE_MAX + 1024)).await;
        assert!(
            matches!(delivered, wire::Pack::Staged(_)),
            "got {delivered:?}"
        );
    }

    /// A push too large to send goes to the bucket whole, and the call that
    /// replaces it is small enough that no push can overrun the quota.
    #[tokio::test]
    async fn a_push_too_large_to_send_goes_to_the_bucket() {
        const PUSH_KEY: &str = "packs/test/one.push";
        const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

        // Refs enough that the push cannot be described in one payload, which
        // is the only way this path is reached.
        let mut push = request_with(wire::Pack::Inline {
            bytes: bytes::Bytes::new(),
        });
        push.updates = (0..30_000)
            .map(|i| wire::RefUpdate {
                refname: format!("refs/heads/agent/{i:0100}"),
                old_id: "0".repeat(40),
                new_id: "a".repeat(40),
            })
            .collect();
        assert!(
            serde_json::to_vec(&push).expect("encoding").len() > PAYLOAD_MAX,
            "the fixture has to exceed the quota or it tests the other branch"
        );

        let staging: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let body = worker(staging.clone(), u64::MAX)
            .encode_call(
                &staging,
                PUSH_KEY,
                Some(TRACEPARENT.to_string()),
                push.clone(),
            )
            .await
            .expect("encoding the call");

        // What travels is a key, not a push — so the quota cannot be the thing
        // that fails, however many refs the push touched.
        assert!(body.len() < PAYLOAD_MAX, "{} bytes", body.len());
        let pointer: wire::Call = serde_json::from_slice(&body).expect("decoding the pointer");
        assert_eq!(
            pointer.traceparent.as_deref(),
            Some(TRACEPARENT),
            "the trace must survive the detour"
        );
        let wire::Push::Staged(at) = pointer.push else {
            panic!("expected a staged push");
        };
        assert_eq!(at.key, PUSH_KEY);

        // The worker reads exactly this back, so it has to be the push itself
        // and the length has to describe it.
        let stored = staging
            .get(&StorePath::from(PUSH_KEY))
            .await
            .expect("the push should be in the bucket")
            .bytes()
            .await
            .expect("reading it back");
        assert_eq!(at.len, u64::try_from(stored.len()).unwrap_or(u64::MAX));
        let back: wire::Request = serde_json::from_slice(&stored).expect("decoding the push");
        assert_eq!(back.updates.len(), push.updates.len());
        assert_eq!(back.updates[0].refname, push.updates[0].refname);
    }

    /// Only a hint, but too small costs the realloc-and-copy it exists to
    /// avoid — so it has to bound the real thing.
    ///
    /// Varies refnames too: a constant-per-ref estimate would undershoot on
    /// this application's long generated names.
    #[test]
    fn the_body_estimate_is_never_under_the_encoded_call() {
        for pack_bytes in [0usize, 1, 1024, INLINE_MAX] {
            for (refs, name_len) in [(1usize, 4usize), (1, 400), (2_000, 200), (20_000, 60)] {
                let mut push = request_with(wire::Pack::Inline {
                    bytes: bytes::Bytes::from(vec![7u8; pack_bytes]),
                });
                push.updates = (0..refs)
                    .map(|i| wire::RefUpdate {
                        refname: format!("refs/heads/{i:0name_len$}"),
                        old_id: "0".repeat(40),
                        new_id: "a".repeat(40),
                    })
                    .collect();
                push.existing = push
                    .updates
                    .iter()
                    .map(|u| (u.refname.clone(), u.old_id.clone()))
                    .collect();

                let encoded = serde_json::to_vec(&push).expect("encoding").len();
                assert!(
                    body_capacity(&push) >= encoded,
                    "estimated {} for {encoded} bytes: pack {pack_bytes}, {refs} refs of {name_len}",
                    body_capacity(&push),
                );
            }
        }
    }

    /// The cap is a policy, and configuration may put it below `INLINE_MAX` —
    /// where nothing streams and so nothing else would check it.
    #[tokio::test]
    async fn the_cap_still_refuses_a_pack_small_enough_to_inline() {
        let staging: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let err = worker(staging.clone(), 1024)
            .deliver(&staging, KEY, reader(vec![7u8; 4096]))
            .await
            .expect_err("2048 bytes over the cap");

        assert!(err.to_string().contains("exceeds 1024 bytes"), "{err}");
    }

    type Drained = (Vec<IngestProgress>, Option<Ingested>);

    /// Drive a stream with exactly these chunks and nothing added, so a test
    /// can place the prelude itself — or leave it out.
    fn feed(chunks: &[&[u8]]) -> Result<Drained, Error> {
        let seen = Mutex::new(Vec::new());
        let record = |stage: IngestProgress| {
            seen.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(stage);
        };

        let mut stream = FrameStream::default();
        let mut outcomes = None;
        let meter = Meter::new();
        for chunk in chunks {
            stream.push(chunk);
            outcomes = outcomes.or(stream.drain(&record, &meter)?);
        }
        Ok((seen.into_inner().unwrap_or_default(), outcomes))
    }

    /// A stream shaped like a real one: every invocation opens with a prelude,
    /// so a test about frames shouldn't have to say so.
    fn drain_bytes(chunks: &[&[u8]]) -> Drained {
        let opening = prelude();
        let mut all: Vec<&[u8]> = vec![&opening];
        all.extend_from_slice(chunks);
        feed(&all).expect("frames should parse")
    }

    fn drain(chunks: &[&str]) -> Drained {
        let bytes: Vec<&[u8]> = chunks.iter().map(|c| c.as_bytes()).collect();
        drain_bytes(&bytes)
    }

    /// What a real invocation puts in front of the frames.
    fn prelude() -> Vec<u8> {
        let mut out =
            br#"{"statusCode":200,"headers":{"content-type":"application/x-ndjson"},"cookies":[]}"#
                .to_vec();
        out.extend_from_slice(&PRELUDE_END);
        out
    }

    /// A frame split across two reads must still be one frame, and a
    /// partial tail must not be parsed as though it were complete.
    #[test]
    fn a_frame_split_across_chunks_is_still_one_frame() {
        let whole = r#"{"kind":"progress","stage":"checking_connectivity"}"#;
        let (at, rest) = whole.split_at(20);

        let (partial, outcomes) = drain(&[at]);
        assert!(partial.is_empty(), "half a frame is not a frame");
        assert!(outcomes.is_none());

        let (seen, _) = drain(&[at, &format!("{rest}\n")]);
        assert_eq!(seen, vec![IngestProgress::CheckingConnectivity]);
    }

    /// Several frames in one chunk, plus the blank lines a keepalive could add.
    #[test]
    fn one_chunk_can_carry_several_frames() {
        let (seen, outcomes) = drain(&[concat!(
            r#"{"kind":"progress","stage":"updating_references"}"#,
            "\n\n",
            r#"{"kind":"progress","stage":"checking_connectivity"}"#,
            "\n",
            r#"{"kind":"done","ingested":{"rejected":[],"screened":[]}}"#,
            "\n",
        )]);

        assert_eq!(
            seen,
            vec![
                IngestProgress::UpdatingReferences,
                IngestProgress::CheckingConnectivity
            ]
        );
        let ingested = outcomes.expect("done should have been seen");
        assert!(ingested.rejected.is_empty(), "{ingested:?}");
        assert!(ingested.screened.is_empty(), "{ingested:?}");
    }

    /// Lambda's prelude ends in eight NULs, no newline — a `Done` first
    /// must not lose its outcomes.
    #[test]
    fn the_metadata_prelude_does_not_swallow_the_first_frame() {
        let mut chunk = prelude();
        chunk.extend_from_slice(br#"{"kind":"done","ingested":{"rejected":[],"screened":[]}}"#);
        chunk.push(b'\n');

        let (seen, outcomes) = feed(&[&chunk]).expect("frames should parse");
        assert!(seen.is_empty(), "a delete-only push reports no progress");
        let ingested = outcomes.expect("done should have been seen");
        assert!(ingested.rejected.is_empty(), "{ingested:?}");
        assert!(ingested.screened.is_empty(), "{ingested:?}");
    }

    /// The delimiter straddles a chunk boundary like anything else on the wire.
    #[test]
    fn a_prelude_split_mid_delimiter_still_yields_the_first_frame() {
        let whole = prelude();
        let (head, tail) = whole.split_at(whole.len() - 5);

        let mut rest = tail.to_vec();
        rest.extend_from_slice(br#"{"kind":"progress","stage":"checking_connectivity"}"#);
        rest.push(b'\n');

        let (seen, _) = feed(&[head, &rest]).expect("frames should parse");
        assert_eq!(seen, vec![IngestProgress::CheckingConnectivity]);
    }

    /// Only the payload is frames — anything ahead of the delimiter is
    /// Lambda's metadata.
    #[test]
    fn nothing_before_the_delimiter_is_read_as_a_frame() {
        let (seen, outcomes) = feed(&[
            br#"{"kind":"progress","stage":"checking_connectivity"}"#,
            b"\n",
        ])
        .expect("a prelude that hasn't ended yet is not an error");
        assert!(seen.is_empty(), "still inside the prelude");
        assert!(outcomes.is_none());
    }

    /// Without a bound this buffers the whole response and then fails as a
    /// missing outcome, naming neither the cause nor the place.
    #[test]
    fn a_stream_with_no_delimiter_is_refused_by_its_own_name() {
        let filler = vec![b'x'; PRELUDE_MAX + 1];
        let err = feed(&[&filler]).expect_err("no delimiter can ever arrive");
        assert!(
            err.to_string().contains("no metadata prelude delimiter"),
            "unexpected error: {err}"
        );
    }

    /// A worker a version ahead must not fail a push it otherwise ran fine.
    #[test]
    fn unparseable_and_unknown_frames_are_skipped() {
        let (seen, outcomes) = drain(&[concat!(
            "not json at all\n",
            r#"{"kind":"invented_later"}"#,
            "\n",
            r#"{"kind":"progress","stage":"updating_references"}"#,
            "\n",
        )]);
        assert_eq!(seen, vec![IngestProgress::UpdatingReferences]);
        assert!(outcomes.is_none());
    }
}
