//! Simulated-latency wrapper for an [`object_store::ObjectStore`].
//!
//! The fast in-memory backends the harnesses run against answer instantly, so
//! this delays each call to give a run timings shaped like production S3.
//! Counting is [`enroute_git_cost::CountingStore`]'s, stacked outside this.
#![allow(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    reason = "byte counts and part sizes in a benchmark run stay far under 2^52, \
              so u64/usize-to-f64 conversions for the latency math are exact"
)]

use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    path::Path as StorePath,
};
use rand_distr::{Distribution, LogNormal};

use enroute_git_cost::StoreUnits;

/// A single kind of S3 request, tracked separately because each has its own
/// latency characteristics even when several share a billing tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Operation {
    /// `GetObject` and other body-returning reads.
    Get,
    /// `HeadObject` (and any `GetObject` issued with `head: true`) — no body transfer.
    Head,
    /// `PutObject` and `CopyObject`.
    Put,
    /// `ListObjectsV2` (including delimited listings).
    List,
    /// `DeleteObject`.
    Delete,
}

/// Fixed per-request latency and per-byte transfer time for one [`Operation`].
///
/// Modelled as `fixed + bytes / throughput`: `fixed` is a log-normal sample
/// parameterised by a median/p99, and `throughput` applies only to `Get`/`Put`.
struct OpLatency {
    fixed: LogNormal<f64>,
    throughput_bytes_per_sec: Option<f64>,
}

impl OpLatency {
    /// Builds from a median/p99 pair (milliseconds) and an optional
    /// sustained throughput (bytes/sec) for operations that move a body.
    #[expect(
        clippy::unwrap_used,
        reason = "median/p99 pairs below are fixed positive constants; \
                  LogNormal::new only fails on non-finite or non-positive input"
    )]
    fn new(median_ms: f64, p99_ms: f64, throughput_bytes_per_sec: Option<f64>) -> Self {
        // For a log-normal distribution, the median is exp(mu), and the p99
        // is exp(mu + z * sigma) where z ~= 2.3263 (the 99th percentile of
        // the standard normal distribution).
        const Z_99: f64 = 2.326_347_874;
        let mu = median_ms.ln();
        let sigma = (p99_ms.ln() - mu) / Z_99;
        Self {
            fixed: LogNormal::new(mu, sigma).unwrap(),
            throughput_bytes_per_sec,
        }
    }

    fn sample(&self, bytes: u64) -> Duration {
        let fixed_ms = self.fixed.sample(&mut rand::rng());
        let transfer_ms = self
            .throughput_bytes_per_sec
            .map_or(0.0, |bps| bytes as f64 / bps * 1000.0);
        Duration::from_secs_f64((fixed_ms + transfer_ms).max(0.0) / 1000.0)
    }

    /// Sample only the fixed TTFB component (no transfer term).
    fn sample_ttfb(&self) -> Duration {
        let fixed_ms = self.fixed.sample(&mut rand::rng());
        Duration::from_secs_f64(fixed_ms.max(0.0) / 1000.0)
    }
}

/// Latency model for every [`Operation`], used to delay calls on a
/// [`LatencyStore`] so a fast in-memory backend behaves like real S3.
///
/// The figures are illustrative same-region S3 numbers, not a guarantee
/// about any bucket — they give the benchmark a *shape*, not a precise oracle.
pub struct LatencyProfile {
    get: OpLatency,
    head: OpLatency,
    put: OpLatency,
    list: OpLatency,
    delete: OpLatency,
}

impl Debug for LatencyProfile {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatencyProfile").finish_non_exhaustive()
    }
}

impl LatencyProfile {
    /// Same-region S3 Standard latency/throughput profile.
    ///
    /// TTFB derived by fitting `latency = TTFB + size/throughput` to
    /// 1KB–10MB same-region benchmarks (~85 MB/s, ~20ms median TTFB).
    #[must_use]
    pub fn production() -> Self {
        const DOWNLOAD_BYTES_PER_SEC: f64 = 85_000_000.0;
        const UPLOAD_BYTES_PER_SEC: f64 = 75_000_000.0;
        Self {
            get: OpLatency::new(20.0, 80.0, Some(DOWNLOAD_BYTES_PER_SEC)),
            head: OpLatency::new(15.0, 60.0, None),
            put: OpLatency::new(25.0, 100.0, Some(UPLOAD_BYTES_PER_SEC)),
            list: OpLatency::new(25.0, 100.0, None),
            delete: OpLatency::new(15.0, 60.0, None),
        }
    }

    /// Same-AZ S3 Express One Zone latency/throughput profile.
    ///
    /// AWS claims consistent single-digit-millisecond first-byte latency;
    /// independent benchmarks put the median at 2–5ms and p99 at ~8ms.
    #[must_use]
    pub fn express() -> Self {
        const DOWNLOAD_BYTES_PER_SEC: f64 = 300_000_000.0;
        const UPLOAD_BYTES_PER_SEC: f64 = 200_000_000.0;
        Self {
            get: OpLatency::new(3.0, 8.0, Some(DOWNLOAD_BYTES_PER_SEC)),
            head: OpLatency::new(2.0, 5.0, None),
            put: OpLatency::new(3.0, 10.0, Some(UPLOAD_BYTES_PER_SEC)),
            list: OpLatency::new(4.0, 15.0, None),
            delete: OpLatency::new(2.0, 7.0, None),
        }
    }

    fn sample(&self, op: Operation, bytes: u64) -> Duration {
        match op {
            Operation::Get => self.get.sample(bytes),
            Operation::Head => self.head.sample(bytes),
            Operation::Put => self.put.sample(bytes),
            Operation::List => self.list.sample(bytes),
            Operation::Delete => self.delete.sample(bytes),
        }
    }

    fn sample_ttfb(&self, op: Operation) -> Duration {
        match op {
            Operation::Get => self.get.sample_ttfb(),
            Operation::Head => self.head.sample_ttfb(),
            Operation::Put => self.put.sample_ttfb(),
            Operation::List => self.list.sample_ttfb(),
            Operation::Delete => self.delete.sample_ttfb(),
        }
    }

    fn throughput(&self, op: Operation) -> Option<f64> {
        match op {
            Operation::Get => self.get.throughput_bytes_per_sec,
            Operation::Head => self.head.throughput_bytes_per_sec,
            Operation::Put => self.put.throughput_bytes_per_sec,
            Operation::List => self.list.throughput_bytes_per_sec,
            Operation::Delete => self.delete.throughput_bytes_per_sec,
        }
    }
}

/// The units in `now` that weren't already in `earlier` (`now - earlier`).
///
/// For isolating one phase of a run, e.g. the clone loop apart from the seed
/// push that set it up.
#[must_use]
pub fn units_since(now: StoreUnits, earlier: StoreUnits) -> StoreUnits {
    StoreUnits {
        get_class: now.get_class.saturating_sub(earlier.get_class),
        put_class: now.put_class.saturating_sub(earlier.put_class),
        deletes: now.deletes.saturating_sub(earlier.deletes),
        bytes_read: now.bytes_read.saturating_sub(earlier.bytes_read),
        bytes_written: now.bytes_written.saturating_sub(earlier.bytes_written),
    }
}

/// Wraps an [`ObjectStore`], delaying every call by a [`LatencyProfile`] to
/// mimic production S3.
pub struct LatencyStore {
    inner: Arc<dyn ObjectStore>,
    /// Shared rather than owned so a multipart upload, which outlives the
    /// call that created it, can delay its parts by the same model.
    latency: Arc<LatencyProfile>,
}

impl Debug for LatencyStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LatencyStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Display for LatencyStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "LatencyStore({})", self.inner)
    }
}

impl LatencyStore {
    /// Delays `inner` by `latency`, or hands it back untouched when there is
    /// no profile — `--no-latency` wants the real store, not a zeroed model.
    #[must_use]
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        latency: Option<LatencyProfile>,
    ) -> Arc<dyn ObjectStore> {
        match latency {
            None => inner,
            Some(profile) => Arc::new(Self {
                inner,
                latency: Arc::new(profile),
            }),
        }
    }

    async fn delay(&self, op: Operation, bytes: u64) {
        tokio::time::sleep(self.latency.sample(op, bytes)).await;
    }
}

/// Simulated network chunk size: bytes per "packet" for progressive delivery.
///
/// A few TCP segments — small enough to let `StreamReader` callers
/// interleave work, large enough not to spin the event loop on big objects.
const STREAM_CHUNK_BYTES: usize = 16 * 1024;

/// Split `bytes` into `STREAM_CHUNK_BYTES`-sized slices without copying.
fn split_into_chunks(bytes: Bytes) -> impl Iterator<Item = Bytes> {
    let len = bytes.len();
    (0..len)
        .step_by(STREAM_CHUNK_BYTES)
        .map(move |start| bytes.slice(start..(start + STREAM_CHUNK_BYTES).min(len)))
}

/// Sleep for the time it would take to receive `chunk` at `throughput`, then
/// yield the chunk.
async fn timed_chunk(chunk: Bytes, throughput_bytes_per_sec: f64) -> object_store::Result<Bytes> {
    let delay_secs = chunk.len() as f64 / throughput_bytes_per_sec;
    if delay_secs > 0.0 {
        tokio::time::sleep(Duration::from_secs_f64(delay_secs)).await;
    }
    Ok(chunk)
}

/// Wraps `payload` so each logical chunk is split into `STREAM_CHUNK_BYTES`
/// pieces, each delayed proportionally to its size at `throughput_bytes_per_sec`.
///
/// Makes the in-memory store behave like a real streaming GET, so
/// `StreamReader` callers can begin processing before the body arrives.
fn inject_transfer_delay(
    payload: GetResultPayload,
    throughput_bytes_per_sec: f64,
) -> GetResultPayload {
    match payload {
        GetResultPayload::Stream(stream) => {
            let delayed = stream
                .flat_map(move |chunk| match chunk {
                    Err(e) => futures::stream::once(async move { Err(e) }).left_stream(),
                    Ok(bytes) => futures::stream::iter(split_into_chunks(bytes))
                        .then(move |piece| timed_chunk(piece, throughput_bytes_per_sec))
                        .right_stream(),
                })
                .boxed();
            GetResultPayload::Stream(delayed)
        }
        // File variant is never produced when the `aws` feature is active;
        // pass it through without delay rather than panicking.
        other @ GetResultPayload::File(..) => other,
    }
}

#[async_trait]
impl ObjectStore for LatencyStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let len = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        self.delay(Operation::Put, len).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        // `CreateMultipartUpload` is a round trip of its own, before any part.
        self.delay(Operation::Put, 0).await;
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(SlowMultipartUpload {
            inner,
            latency: Arc::clone(&self.latency),
        }))
    }

    async fn get_opts(&self, location: &StorePath, options: GetOptions) -> StoreResult<GetResult> {
        let op = if options.head {
            Operation::Head
        } else {
            Operation::Get
        };
        let result = self.inner.get_opts(location, options).await?;
        let profile = &self.latency;

        // TTFB fires immediately, ahead of any bytes, regardless of whether
        // the caller buffers or streams.
        tokio::time::sleep(profile.sample_ttfb(op)).await;

        // Injected into the payload itself so streaming callers see bytes
        // arrive progressively at the modelled throughput; buffered callers
        // see the same correct total either way.
        if let Some(throughput) = profile.throughput(op) {
            Ok(GetResult {
                payload: inject_transfer_delay(result.payload, throughput),
                ..result
            })
        } else {
            Ok(result)
        }
    }

    /// Delayed per object the store reported on.
    ///
    /// The profile has always carried a delete latency; nothing applied it
    /// while this type was still counting them instead.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<StorePath>>,
    ) -> BoxStream<'static, StoreResult<StorePath>> {
        let latency = Arc::clone(&self.latency);
        self.inner
            .delete_stream(locations)
            .then(move |deleted| {
                let latency = Arc::clone(&latency);
                async move {
                    tokio::time::sleep(latency.sample(Operation::Delete, 0)).await;
                    deleted
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    /// Forwarded rather than left to the trait default.
    ///
    /// The default filters client-side over `list`, which would charge the
    /// counter outside for every key it then skipped.
    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> StoreResult<ListResult> {
        self.delay(Operation::List, 0).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        // CopyObject is latency-shaped like Put.
        self.delay(Operation::Put, 0).await;
        self.inner.copy_opts(from, to, options).await
    }
}

/// Wraps a [`MultipartUpload`], delaying each part by the time its bytes
/// would have taken, and the completing round trip by its own.
#[derive(Debug)]
struct SlowMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    latency: Arc<LatencyProfile>,
}

#[async_trait]
impl MultipartUpload for SlowMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let len = u64::try_from(data.content_length()).unwrap_or(u64::MAX);
        let delay = self.latency.sample(Operation::Put, len);
        let upload = self.inner.put_part(data);
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            upload.await
        })
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        // `CompleteMultipartUpload` is another round trip, and a slow one.
        tokio::time::sleep(self.latency.sample(Operation::Put, 0)).await;
        self.inner.complete().await
    }

    async fn abort(&mut self) -> StoreResult<()> {
        self.inner.abort().await
    }
}
