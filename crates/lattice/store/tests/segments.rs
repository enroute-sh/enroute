//! Writing, reading and compacting, against a catalog in a map.

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use ulid::Ulid;

use enroute_lattice_core::{Key, KeyRange, Policy};
use enroute_lattice_store::{
    Counters, Error, MemoryCatalog, Report, Scope, SegmentId, Segments, Sweep, conformance, inspect,
};

/// The one scope these tests hold a value in.
fn scope() -> Scope {
    Scope {
        id: 7,
        prefix: Path::from("scopes/7"),
    }
}

/// A store whose graduation threshold `graduation_bytes` sets.
///
/// Twelve bytes an entry, so the threshold is stated in entries at the call
/// site rather than being a number nothing explains.
fn store(graduation_bytes: u64) -> (Segments<Counters>, Arc<InMemory>) {
    let bucket = Arc::new(InMemory::new());
    let policy = Policy {
        fanout: 4,
        max_inputs: 8,
        max_input_bytes: 1 << 20,
        graduation_bytes,
        inline_ceiling: 64,
    };
    let store = Segments::new(Arc::new(MemoryCatalog::new()), bucket.clone(), policy);
    (store, bucket)
}

fn everything() -> KeyRange {
    KeyRange::EVERYTHING
}

async fn objects(bucket: &InMemory) -> usize {
    use futures::StreamExt;
    bucket.list(None).count().await
}

#[tokio::test]
async fn a_written_value_reads_back() {
    let (store, _) = store(u64::MAX);
    let value = Counters::of(&[(1, 10), (5, 50)]);

    assert!(store.write(&scope(), &value).await.unwrap().is_some());
    assert_eq!(
        store.read(&scope(), everything()).await.unwrap(),
        Some(value)
    );
}

#[tokio::test]
async fn separate_writes_compose_on_read() {
    let (store, _) = store(u64::MAX);
    store.write(&scope(), &Counters::run(0, 4)).await.unwrap();
    store.write(&scope(), &Counters::run(4, 4)).await.unwrap();

    let read = store.read(&scope(), everything()).await.unwrap();
    assert_eq!(read, Some(Counters::run(0, 8)));
}

// The read is a range read, so it must not pay for segments it cannot use.
#[tokio::test]
async fn a_read_composes_only_what_its_range_touches() {
    let (store, _) = store(u64::MAX);
    store.write(&scope(), &Counters::run(0, 10)).await.unwrap();
    store
        .write(&scope(), &Counters::run(100, 10))
        .await
        .unwrap();

    let range = KeyRange::new(Key::ZERO, Key::new(20)).unwrap();
    assert_eq!(
        store.read(&scope(), range).await.unwrap(),
        Some(Counters::run(0, 10))
    );
}

#[tokio::test]
async fn an_empty_value_is_not_a_segment() {
    let (store, _) = store(u64::MAX);
    assert_eq!(
        store.write(&scope(), &Counters::default()).await.unwrap(),
        None
    );
    assert_eq!(store.read(&scope(), everything()).await.unwrap(), None);
    assert_eq!(
        inspect::counts(store.catalog().as_ref(), &scope()).await,
        (0, 0)
    );
}

#[tokio::test]
async fn reading_a_scope_with_nothing_in_it_is_nothing() {
    let (store, _) = store(u64::MAX);
    assert_eq!(store.read(&scope(), everything()).await.unwrap(), None);
}

// The medium follows the size, and that is the whole of the rule.
#[tokio::test]
async fn a_small_segment_inlines_and_a_large_one_graduates() {
    let (store, bucket) = store(12 * 5);

    store.write(&scope(), &Counters::run(0, 2)).await.unwrap();
    assert_eq!(
        inspect::counts(store.catalog().as_ref(), &scope()).await,
        (1, 1)
    );
    assert_eq!(
        objects(&bucket).await,
        0,
        "a small push stays off the bucket"
    );

    store.write(&scope(), &Counters::run(10, 9)).await.unwrap();
    assert_eq!(
        inspect::counts(store.catalog().as_ref(), &scope()).await,
        (2, 1)
    );
    assert_eq!(objects(&bucket).await, 1);

    let read = store.read(&scope(), everything()).await.unwrap().unwrap();
    let mut expected = Counters::run(0, 2);
    expected.entries.extend(Counters::run(10, 9).entries);
    assert_eq!(read, expected, "both media read back the same way");
}

#[tokio::test]
async fn compaction_merges_a_tier_into_the_next() {
    let (store, _) = store(u64::MAX);
    for index in 0..4 {
        store
            .write(&scope(), &Counters::run(index * 10, 10))
            .await
            .unwrap();
    }
    assert_eq!(
        inspect::tiers(store.catalog().as_ref(), &scope()).await,
        vec![0, 0, 0, 0]
    );

    let report = store.compact(&scope()).await.unwrap();
    assert_eq!(report.merged, 4);
    assert_eq!(
        inspect::tiers(store.catalog().as_ref(), &scope()).await,
        vec![1]
    );
    assert_eq!(
        store.read(&scope(), everything()).await.unwrap(),
        Some(Counters::run(0, 40)),
        "compaction changes the cost of a read, never its answer"
    );
}

#[tokio::test]
async fn compaction_reclaims_the_bucket_objects_it_superseded() {
    let (store, bucket) = store(0);
    for index in 0..4 {
        store
            .write(&scope(), &Counters::run(index * 10, 10))
            .await
            .unwrap();
    }
    assert_eq!(objects(&bucket).await, 4);

    let report = store.compact(&scope()).await.unwrap();
    assert_eq!(report.graduated, 1);
    assert_eq!(report.orphaned, 0);
    assert_eq!(objects(&bucket).await, 1, "the four it replaced are gone");
    assert_eq!(
        store.read(&scope(), everything()).await.unwrap(),
        Some(Counters::run(0, 40))
    );
}

#[tokio::test]
async fn compaction_with_nothing_due_does_nothing() {
    let (store, _) = store(u64::MAX);
    store.write(&scope(), &Counters::run(0, 10)).await.unwrap();
    assert_eq!(store.compact(&scope()).await.unwrap(), Report::default());
    assert_eq!(
        inspect::counts(store.catalog().as_ref(), &scope()).await,
        (1, 1)
    );
}

// The property the whole design rests on: allocation order and write order
// diverge, so a segment can land inside a range compaction already merged.
#[tokio::test]
async fn a_segment_arriving_inside_an_already_merged_range_still_composes() {
    let (store, _) = store(u64::MAX);
    for index in 0..4 {
        store
            .write(&scope(), &Counters::run(index * 10, 10))
            .await
            .unwrap();
    }
    store.compact(&scope()).await.unwrap();

    // A push that was slow to commit, holding keys inside what just merged.
    store
        .write(&scope(), &Counters::of(&[(15, 900)]))
        .await
        .unwrap();

    let read = store.read(&scope(), everything()).await.unwrap().unwrap();
    let mut expected = Counters::run(0, 40);
    expected.entries.insert(15, 900);
    assert_eq!(read, expected);
}

// Running it again must not be a second answer, only a cheaper one.
#[tokio::test]
async fn compaction_is_safe_to_run_again() {
    let (store, _) = store(u64::MAX);
    for index in 0..9 {
        store
            .write(&scope(), &Counters::run(index * 10, 10))
            .await
            .unwrap();
    }
    let before = store.read(&scope(), everything()).await.unwrap();

    for _ in 0..4 {
        store.compact(&scope()).await.unwrap();
        assert_eq!(store.read(&scope(), everything()).await.unwrap(), before);
    }
    let (rows, _) = inspect::counts(store.catalog().as_ref(), &scope()).await;
    assert!(rows < 9, "repeated passes still make progress: {rows} rows");
}

// The index is a source of truth, so this has to be loud rather than empty.
#[tokio::test]
async fn bytes_that_are_not_a_segment_are_reported() {
    let (store, _) = store(u64::MAX);
    store.write(&scope(), &Counters::run(0, 4)).await.unwrap();
    inspect::corrupt(store.catalog().as_ref(), &scope()).await;

    let read = store.read(&scope(), everything()).await;
    assert!(matches!(read, Err(Error::Corrupt { .. })), "{read:?}");
}

// The in-memory catalog is only evidence about a real one if both answer to
// one contract; this is that contract, and the Postgres catalog runs it too.
#[tokio::test]
async fn the_in_memory_catalog_meets_the_contract() {
    conformance::check(&MemoryCatalog::new(), &scope())
        .await
        .unwrap();
}

#[tokio::test]
async fn with_bucket_reads_and_writes_through_the_new_bucket() {
    // Zero, so every segment graduates and the bucket is what a read must
    // reach — an inlined one would pass this without proving anything.
    let (store, original) = store(0);
    store.write(&scope(), &Counters::run(0, 4)).await.unwrap();

    let replacement = Arc::new(InMemory::new());
    let swapped = store.with_bucket(replacement.clone());
    swapped.write(&scope(), &Counters::run(4, 4)).await.unwrap();

    assert_eq!(objects(&original).await, 1, "the first write stayed put");
    assert_eq!(objects(&replacement).await, 1, "the second went elsewhere");

    // The catalog is shared, so the swapped store lists both segments and
    // fails on the one whose bytes its bucket does not hold.
    assert!(matches!(
        swapped.read(&scope(), everything()).await,
        Err(Error::Bucket { .. })
    ));
}

/// An hour in the milliseconds a sweep compares.
const HOUR_MS: u64 = 60 * 60 * 1000;

/// A name stamped at `ms`, so a test can age an object without waiting.
///
/// A ULID is its own clock, which is what the grace window reads.
fn named_at(ms: u64) -> SegmentId {
    SegmentId::from(Ulid::from_parts(ms, 42).0)
}

/// Puts `bytes` under the scope's prefix as `name`, naming no row.
async fn plant(
    bucket: &InMemory,
    name: &str,
    bytes: &[u8],
) -> object_store::Result<object_store::PutResult> {
    bucket
        .put(&scope().prefix.join(name), PutPayload::from(bytes.to_vec()))
        .await
}

/// What the bucket holds, by name, in an order a test can read.
async fn names(bucket: &InMemory) -> Vec<String> {
    use futures::StreamExt;
    let mut found: Vec<String> = bucket
        .list(None)
        .filter_map(|object| async move {
            object
                .ok()
                .and_then(|meta| meta.location.filename().map(str::to_owned))
        })
        .collect()
        .await;
    found.sort();
    found
}

/// A graduated segment's object is put before the row naming it, so an
/// unnamed object is either an orphan or a write still in flight.
#[tokio::test]
async fn a_sweep_deletes_only_what_no_row_names_past_the_window() {
    let (store, bucket) = store(0);
    store.write(&scope(), &Counters::run(0, 4)).await.unwrap();

    let now = 100 * HOUR_MS;
    let orphan = named_at(now - 3 * HOUR_MS).to_string();
    let in_flight = named_at(now - 60_000).to_string();
    plant(&bucket, &orphan, b"orphaned bytes").await.unwrap();
    plant(&bucket, &in_flight, b"a push still running")
        .await
        .unwrap();
    plant(&bucket, "notes.txt", b"somebody else's")
        .await
        .unwrap();

    let swept = store
        .sweep_bucket(
            &scope(),
            Sweep {
                now_ms: now,
                grace_secs: 3600,
                dry_run: false,
            },
        )
        .await
        .unwrap();

    assert_eq!(swept.orphans, 1, "only the aged unnamed object went");
    assert_eq!(swept.bytes, 14, "and its size is what it freed");
    assert_eq!(swept.scanned, 3, "the name that is not a segment's is not");

    let left = names(&bucket).await;
    assert!(!left.contains(&orphan), "the orphan went: {left:?}");
    assert!(left.contains(&in_flight), "the in-flight write stayed");
    assert!(
        left.contains(&"notes.txt".to_owned()),
        "and so did what this store did not write"
    );
    assert_eq!(left.len(), 3, "the segment a row names is still there");
}

/// Whoever runs a sweep should be able to see what it would take first.
#[tokio::test]
async fn a_dry_run_reports_what_it_would_delete_and_deletes_nothing() {
    let (store, bucket) = store(0);
    let now = 100 * HOUR_MS;
    let aged = named_at(now - 3 * HOUR_MS).to_string();
    plant(&bucket, &aged, b"x").await.unwrap();

    let sweep = |dry_run| Sweep {
        now_ms: now,
        grace_secs: 3600,
        dry_run,
    };
    let planned = store.sweep_bucket(&scope(), sweep(true)).await.unwrap();
    assert_eq!(planned.orphans, 1);
    assert_eq!(objects(&bucket).await, 1, "a dry run deletes nothing");

    let swept = store.sweep_bucket(&scope(), sweep(false)).await.unwrap();
    assert_eq!(
        swept, planned,
        "a dry run reports exactly what the sweep takes"
    );
    assert_eq!(objects(&bucket).await, 0);
}

/// What compaction supersedes it deletes itself; what a failed delete left
/// behind is what this is for, so a pass over a merged scope takes nothing.
#[tokio::test]
async fn a_sweep_after_compaction_finds_nothing_to_take() {
    let (store, bucket) = store(0);
    for index in 0..9 {
        store
            .write(&scope(), &Counters::run(index * 10, 10))
            .await
            .unwrap();
    }
    store.compact(&scope()).await.unwrap();
    let before = objects(&bucket).await;

    let swept = store
        .sweep_bucket(
            &scope(),
            Sweep {
                now_ms: u64::MAX,
                grace_secs: 0,
                dry_run: false,
            },
        )
        .await
        .unwrap();

    assert_eq!(swept.orphans, 0, "every object left is one a row names");
    assert_eq!(objects(&bucket).await, before);
    assert_eq!(
        store.read(&scope(), everything()).await.unwrap(),
        Some(Counters::run(0, 90))
    );
}
