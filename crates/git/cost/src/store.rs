//! The [`ObjectStore`] decorator that does the counting.

use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult, path::Path,
};

use crate::{Meter, StoreRole};

/// Wraps an [`ObjectStore`], charging every request it serves to a [`Meter`]
/// under one [`StoreRole`].
///
/// Only the six required trait methods are overridden — `ObjectStoreExt`'s
/// conveniences are all defined in terms of them, so nothing bypasses a counter.
pub struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    meter: Arc<Meter>,
    role: StoreRole,
}

impl Debug for CountingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountingStore")
            .field("inner", &self.inner)
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

/// `ObjectStore` requires `Display`, and errors quote the store that raised
/// them — so this has to name the store underneath rather than itself.
impl Display for CountingStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl CountingStore {
    /// Charge `inner`'s requests to `meter` as `role`.
    ///
    /// Returns the trait object rather than `Self`, a decorator being useful
    /// only behind the trait it decorates.
    #[must_use]
    pub fn wrap(
        inner: Arc<dyn ObjectStore>,
        meter: Arc<Meter>,
        role: StoreRole,
    ) -> Arc<dyn ObjectStore> {
        Arc::new(Self { inner, meter, role })
    }

    fn counters(&self) -> &crate::StoreCounters {
        self.meter.store(self.role)
    }

    fn put_class(&self) {
        self.counters().put_class.fetch_add(1, Ordering::Relaxed);
    }

    fn get_class(&self) {
        self.counters().get_class.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let len = u64::try_from(payload.content_length()).unwrap_or(u64::MAX);
        self.put_class();
        self.counters()
            .bytes_written
            .fetch_add(len, Ordering::Relaxed);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        // `CreateMultipartUpload` is a billed round trip of its own, before
        // any part is sent.
        self.put_class();
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingUpload {
            inner,
            meter: Arc::clone(&self.meter),
            role: self.role,
            bytes: 0,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> StoreResult<GetResult> {
        // A `head: true` GET answers with no body, but `result.range` still
        // reports the range it *would* return — counting it would invent transfer.
        let head = options.head;
        self.get_class();
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            // The response body, which for a ranged read is the range and not
            // the object: what a retrieval tier would charge for.
            let bytes = result.range.end.saturating_sub(result.range.start);
            self.counters()
                .bytes_read
                .fetch_add(bytes, Ordering::Relaxed);
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        let meter = Arc::clone(&self.meter);
        let role = self.role;
        // Counted on the way out, one per path the store actually reported
        // on. Deliberately per *object*, not per request: S3 batches deletes
        // up to a thousand at a time, but neither vendor bills for them, and
        // how much staging is being reclaimed is the useful number.
        self.inner
            .delete_stream(locations)
            .inspect(move |_| {
                meter.store(role).deletes.fetch_add(1, Ordering::Relaxed);
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        // One per call, so a listing that pages counts as one where S3 would
        // bill per thousand keys. Left as is because nothing on the metered
        // git paths lists — it is the janitor and the staging sweep that do.
        self.put_class();
        self.inner.list(prefix)
    }

    /// Forwarded, not left to the trait default — `AmazonS3` pushes the
    /// offset down server-side, unlike the default's client-side filter.
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.put_class();
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.put_class();
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        // CopyObject is billed in the same class as a PUT, and moves its
        // bytes server-side — so it costs a request and no transfer.
        self.put_class();
        self.inner.copy_opts(from, to, options).await
    }
}

/// Wraps a [`MultipartUpload`], holding bytes back until `complete()`
/// succeeds — an aborted upload's parts were never stored.
#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn MultipartUpload>,
    meter: Arc<Meter>,
    role: StoreRole,
    bytes: u64,
}

#[async_trait]
impl MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let len = u64::try_from(data.content_length()).unwrap_or(u64::MAX);
        self.bytes = self.bytes.saturating_add(len);
        self.meter
            .store(self.role)
            .put_class
            .fetch_add(1, Ordering::Relaxed);
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        // `CompleteMultipartUpload` is another billed round trip.
        self.meter
            .store(self.role)
            .put_class
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.complete().await?;
        self.meter
            .store(self.role)
            .bytes_written
            .fetch_add(self.bytes, Ordering::Relaxed);
        Ok(result)
    }

    async fn abort(&mut self) -> StoreResult<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt as _, memory::InMemory};

    use super::*;

    fn metered() -> (Arc<dyn ObjectStore>, Arc<Meter>) {
        let meter = Meter::new();
        let store = CountingStore::wrap(
            Arc::new(InMemory::new()),
            Arc::clone(&meter),
            StoreRole::Primary,
        );
        (store, meter)
    }

    /// Every `ObjectStoreExt` convenience must land on a counter through the
    /// required method it delegates to.
    #[tokio::test]
    async fn the_convenience_methods_are_all_counted() {
        let (store, meter) = metered();
        let path = Path::from("obj");

        store.put(&path, vec![7u8; 64].into()).await.unwrap();
        store.get(&path).await.unwrap().bytes().await.unwrap();
        store.head(&path).await.unwrap();
        store.get_range(&path, 0..16).await.unwrap();
        store.delete(&path).await.unwrap();

        let units = meter.units().primary;
        assert_eq!(units.put_class, 1);
        // get + head + get_range.
        assert_eq!(units.get_class, 3);
        assert_eq!(units.deletes, 1);
        assert_eq!(units.bytes_written, 64);
        // The whole object once, then 16 bytes of it — a head moves none.
        assert_eq!(units.bytes_read, 64 + 16);
    }

    /// Ranged reads are how a fetch actually reads segments, and a retrieval
    /// tier bills the range rather than the object it came from.
    #[tokio::test]
    async fn a_ranged_read_counts_only_the_range() {
        let (store, meter) = metered();
        let path = Path::from("obj");
        store.put(&path, vec![0u8; 4096].into()).await.unwrap();

        store.get_range(&path, 100..200).await.unwrap();

        assert_eq!(meter.units().primary.bytes_read, 100);
    }

    #[tokio::test]
    async fn an_aborted_upload_stores_no_bytes() {
        let (store, meter) = metered();
        let mut upload = store.put_multipart(&Path::from("obj")).await.unwrap();

        upload
            .put_part(vec![0u8; 5 * 1024 * 1024].into())
            .await
            .unwrap();
        upload.abort().await.unwrap();

        let units = meter.units().primary;
        assert_eq!(units.bytes_written, 0);
        // Create and the part still happened, and are still billed.
        assert_eq!(units.put_class, 2);
    }

    #[tokio::test]
    async fn a_completed_upload_counts_every_round_trip() {
        let (store, meter) = metered();
        let mut upload = store.put_multipart(&Path::from("obj")).await.unwrap();

        let part = 5 * 1024 * 1024;
        upload.put_part(vec![0u8; part].into()).await.unwrap();
        upload.put_part(vec![0u8; part].into()).await.unwrap();
        upload.complete().await.unwrap();

        let units = meter.units().primary;
        // Create, two parts, complete.
        assert_eq!(units.put_class, 4);
        assert_eq!(units.bytes_written, u64::try_from(part * 2).unwrap());
    }

    /// A meter is shared by every handle working for one operation, and the
    /// roles must not bleed into each other.
    #[tokio::test]
    async fn roles_are_counted_apart() {
        let meter = Meter::new();
        let primary = CountingStore::wrap(
            Arc::new(InMemory::new()),
            Arc::clone(&meter),
            StoreRole::Primary,
        );
        let handoff = CountingStore::wrap(
            Arc::new(InMemory::new()),
            Arc::clone(&meter),
            StoreRole::Handoff,
        );

        primary
            .put(&Path::from("a"), vec![0u8; 8].into())
            .await
            .unwrap();
        handoff
            .put(&Path::from("b"), vec![0u8; 16].into())
            .await
            .unwrap();

        let units = meter.units();
        assert_eq!(units.primary.bytes_written, 8);
        assert_eq!(units.handoff.bytes_written, 16);
    }
}
