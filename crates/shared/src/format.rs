//! The bucket's format at `cluster/format`: the release that created the bucket,
//! which every node checks before it opens anything. Only ledger nodes write
//! it, once, on a new bucket; compute nodes only read it.

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::store::Objects;
use crate::version::{self, Version};

/// Where the format is. Under `cluster/`, which compute nodes can read.
const FORMAT: &str = "cluster/format";

const UPGRADES: &str = "https://github.com/enroute-sh/enroute/blob/HEAD/docs/operate/upgrades.md";

/// The object at [`FORMAT`].
#[derive(Serialize, Deserialize)]
struct Format {
    /// The release that created the bucket.
    version: Version,
}

/// Refuse a bucket this release cannot read, and stamp an empty one when `write`.
///
/// # Errors
///
/// Returns why the bucket cannot be read, or an error if the store fails.
pub async fn check(objects: &Objects, write: bool) -> Result<()> {
    // List before reading: a ledger writes the format before any data, so
    // data the list sees means the read finds the format another ledger wrote.
    let empty = objects.is_empty().await?;
    if let Some(data) = objects.get_object(FORMAT).await? {
        let format: Format =
            serde_json::from_slice(&data).with_context(|| format!("reading {FORMAT}"))?;
        return version::check(format.version)
            .with_context(|| format!("the bucket was created by release {}", format.version));
    }
    anyhow::ensure!(
        empty,
        "the bucket holds data but no {FORMAT}: Enroute 0.3 or earlier wrote it, and this \
         release cannot read it. Migrate it to a new bucket and new data directories: \
         {UPGRADES}"
    );
    if write {
        let data = serde_json::to_vec(&Format {
            version: version::BINARY,
        })?;
        // Losing the race means another ledger node wrote the same object.
        objects.put_if(FORMAT, data.into(), None).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::memory::InMemory;

    use super::*;
    use crate::store::{IoConfig, IoLimiter, Store};

    fn bucket() -> Arc<Objects> {
        let io = IoLimiter::new(IoConfig::default()).unwrap();
        Store::new(Arc::new(InMemory::new())).objects_in(&io, None)
    }

    #[tokio::test]
    async fn a_ledger_stamps_a_new_bucket_and_a_compute_does_not() {
        let objects = bucket();
        check(&objects, false).await.unwrap();
        assert!(objects.get_object(FORMAT).await.unwrap().is_none());
        check(&objects, true).await.unwrap();
        check(&objects, false).await.unwrap();
        let stamped = objects.get_object(FORMAT).await.unwrap().unwrap();
        let expected = format!(r#"{{"version":"{}"}}"#, version::BINARY);
        assert_eq!(stamped, expected.as_bytes());
    }

    #[tokio::test]
    async fn a_bucket_of_0_3_or_of_a_newer_release_is_refused() {
        let objects = bucket();
        let state = Bytes::from_static(b"x");
        objects.put_object("repos/acme/state", state).await.unwrap();
        for write in [false, true] {
            let error = check(&objects, write).await.unwrap_err().to_string();
            assert!(error.contains("0.3 or earlier"), "{error}");
        }
        assert!(objects.get_object(FORMAT).await.unwrap().is_none());

        let objects = bucket();
        let newer = Bytes::from_static(br#"{"version":"99.0"}"#);
        objects.put_object(FORMAT, newer).await.unwrap();
        let error = format!("{:#}", check(&objects, true).await.unwrap_err());
        assert!(error.contains("created by release 99.0"), "{error}");
    }

    /// What a ledger does first: check the bucket, then write data.
    async fn start_ledger(objects: Arc<Objects>, n: usize) -> Result<()> {
        check(&objects, true).await?;
        let path = format!("cluster/nodes/{n}");
        objects.put_object(&path, Bytes::from_static(b"x")).await
    }

    /// Ledgers that start together on an empty bucket all start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ledgers_starting_together_all_start() {
        for _ in 0..200 {
            let objects = bucket();
            let starts = (0..4).map(|n| tokio::spawn(start_ledger(objects.clone(), n)));
            let started = futures::future::try_join_all(starts).await.unwrap();
            started.into_iter().collect::<Result<Vec<()>>>().unwrap();
        }
    }
}
