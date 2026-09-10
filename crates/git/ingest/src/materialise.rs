//! Pass 3 of ingest: produce the bytes the plan asks for that the wire pack
//! cannot supply verbatim.
//!
//! Everything staged here is rebuilt from the pack (or read from the store)
//! and compressed. A pair is produced once no matter how many commits' packs
//! name it, and an encoding that isn't smaller than the object is stored
//! whole instead.

use bytes::Bytes;
use futures::StreamExt as _;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{Error, ObjectHashMap, ObjectHashSet};
use enroute_git_cost::count;
use enroute_git_graph::ObjectRefs;
use enroute_git_packfile::encode_delta;
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::delta_plan::{DeltaPlan, EntrySource};
use crate::pack::WirePacks;
use crate::pool::{deflate_into, on_pool};
use crate::progress::{IngestProgress, ProgressSink};
use crate::rebuild::Rebuilder;
use crate::staging::Staging;
use crate::timing::{AtomicDuration, as_u64, record_ms};

/// Items rebuilt and encoded concurrently before the batch is staged, at
/// most.
///
/// Enough overlap to keep the pool fed; [`BATCH_BYTES`] is the real bound.
const BATCH: usize = 256;

/// Roughly how much object content one batch may hold — the real bound.
///
/// An item holds target and base resident at once, either up to
/// `MAX_OBJECT_BYTES`. An oversized item still goes alone.
const BATCH_BYTES: u64 = 256 * 1024 * 1024;

/// One encoding the plan asks for.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Want {
    oid: ObjectId,
    kind: Kind,
    /// The version to encode against, or `None` for the object's own bytes.
    base: Option<ObjectId>,
}

/// Where this pass' time goes, summed across concurrent tasks rather than
/// spanned per item.
///
/// [`Self::fetch`] and [`Self::pool`] exceed the phase's wall time; only
/// their ratio means anything.
#[derive(Default)]
struct EncodeCost {
    /// Reading a batch's pre-existing objects, wall time rather than summed.
    prefetch: AtomicDuration,
    /// Rebuilding the versions involved, or reading them from the store.
    fetch: AtomicDuration,
    /// Encoding and deflating on the worker pool — queue wait included, so
    /// this grows with how far the pool is behind.
    pool: AtomicDuration,
}

/// A finished encoding, waiting to be staged.
struct Encoded {
    oid: ObjectId,
    kind: Kind,
    /// What the bytes encode against, `None` whenever they are the object
    /// itself.
    base: Option<ObjectId>,
    compressed: Vec<u8>,
    /// Decompressed length of what was compressed: the delta stream for a
    /// delta, the object for anything else.
    length: usize,
}

/// Compress a delta only if it beats storing the object whole.
///
/// Compared before compressing: deflating both to find out would double
/// this pass' CPU.
fn encode_pair(want: Want, target: &[u8], base: &[u8]) -> Result<Encoded, Error> {
    let delta = encode_delta(base, target).map_err(|e| anyhow::anyhow!("encode delta: {e}"))?;
    if delta.len() >= target.len() {
        return encode_whole(want, target);
    }
    let mut compressed = Vec::new();
    deflate_into(&delta, &mut compressed)?;
    Ok(Encoded {
        oid: want.oid,
        kind: want.kind,
        base: want.base,
        compressed,
        length: delta.len(),
    })
}

fn encode_whole(want: Want, target: &[u8]) -> Result<Encoded, Error> {
    let mut compressed = Vec::new();
    deflate_into(target, &mut compressed)?;
    Ok(Encoded {
        oid: want.oid,
        kind: want.kind,
        base: None,
        compressed,
        length: target.len(),
    })
}

/// Produce and stage every encoding `plan` calls for.
///
/// # Errors
/// Returns an error if an object or one of its bases can't be rebuilt or
/// read, or if a staging write fails.
#[tracing::instrument(
    name = "enroute_git_ingest::encode",
    skip_all,
    fields(
        items = tracing::field::Empty,
        deltas = tracing::field::Empty,
        declined = tracing::field::Empty,
        staged_bytes = tracing::field::Empty,
        prefetch_ms = tracing::field::Empty,
        fetch_ms = tracing::field::Empty,
        pool_ms = tracing::field::Empty,
    )
)]
pub(crate) async fn materialise_plan(
    plan: &DeltaPlan,
    wire: &WirePacks,
    resolved: &ObjectHashMap<ObjectRefs>,
    staging: &mut Staging,
    state: &Storage,
    repo: &RepoMetadata,
    progress: ProgressSink<'_>,
) -> Result<(), Error> {
    // Before `wanted`, which reads pre-existing entries back and is
    // itself long enough to look like a stall.
    progress(IngestProgress::CompressingObjects { done: 0, total: 0 });
    let wants = wanted(plan, resolved, state, repo).await?;
    let span = tracing::Span::current();
    span.record("items", count(wants.len()));
    span.record(
        "deltas",
        count(wants.iter().filter(|w| w.base.is_some()).count()),
    );
    if wants.is_empty() {
        return Ok(());
    }

    let mut declined = 0usize;
    let mut staged_bytes = 0u64;
    let cost = EncodeCost::default();
    let mut done = 0usize;
    let total = as_u64(wants.len());
    // Outlives the batches so a chain walked for one is not walked again
    // for the next.
    let rebuilder = Rebuilder::new();

    for batch in batches(&wants, |oid| wire.size_of(oid)) {
        // Per item, no two reads can be seen to be adjacent; the batch
        // boundary is what makes them visible together.
        let involved: Vec<ObjectId> = batch
            .iter()
            .flat_map(|want| std::iter::once(want.oid).chain(want.base))
            .collect();
        let at = std::time::Instant::now();
        let prefetched = rebuilder
            .prefetch(&involved, wire, staging, state, repo)
            .await?;
        cost.prefetch.add(at.elapsed());

        // The shared borrows of `staging` all end with this block, which is
        // what lets the staging writes below take it by `&mut`.
        let encoded: Vec<Encoded> = {
            let encoder = Encoder {
                rebuilder: &rebuilder,
                prefetched: &prefetched,
                cost: &cost,
            };
            let staging = &*staging;
            futures::stream::iter(batch.iter().copied())
                .map(|want| async move { encode_one(want, &encoder, wire, staging, state, repo).await })
                .buffer_unordered(BATCH)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, Error>>()?
        };

        for item in encoded {
            done += 1;
            if item.base.is_none() {
                declined += 1;
            }
            staged_bytes += as_u64(item.compressed.len());
            stage(item, staging, repo).await?;
        }
        progress(IngestProgress::CompressingObjects {
            done: as_u64(done),
            total,
        });
    }

    // "Stored whole", not "asked for a delta and got none": the `deltas`
    // field already reports how the plan asked.
    span.record("declined", count(declined));
    span.record("staged_bytes", count(staged_bytes));
    record_ms(&[
        ("prefetch_ms", cost.prefetch.get()),
        ("fetch_ms", cost.fetch.get()),
        ("pool_ms", cost.pool.get()),
    ]);
    Ok(())
}

/// Every distinct encoding this push has to produce.
///
/// A re-included delta gets a whole copy: its original base need not be
/// reachable from the commit that re-includes it.
async fn wanted(
    plan: &DeltaPlan,
    resolved: &ObjectHashMap<ObjectRefs>,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<Vec<Want>, Error> {
    let mut seen: std::collections::HashSet<(ObjectId, Option<ObjectId>)> =
        std::collections::HashSet::new();
    let mut wants = Vec::new();
    let mut push = |want: Want, wants: &mut Vec<Want>| {
        if seen.insert((want.oid, want.base)) {
            wants.push(want);
        }
    };

    for (&oid, refs) in resolved {
        let kind = match refs {
            // A commit whose own entry is kept from the wire needs
            // nothing produced for it.
            ObjectRefs::Commit { .. }
                if !matches!(plan.commits.get(&oid), Some(EntrySource::Wire(_))) =>
            {
                Kind::Commit
            }
            // A tag is read back whole at promotion to be rewritten as a
            // loose object, so its content has to be somewhere staging
            // can reach.
            ObjectRefs::Tag(_) => Kind::Tag,
            // Trees and blobs are stored only where a pack names them.
            _ => continue,
        };
        push(
            Want {
                oid,
                kind,
                base: None,
            },
            &mut wants,
        );
    }

    let mut foreign: ObjectHashSet = ObjectHashSet::default();
    for entry in plan.packs.values().flatten() {
        // Kept from the client's pack: promotion copies those bytes
        // straight out of it.
        if matches!(entry.source, EntrySource::Wire(_)) {
            continue;
        }
        let in_push = resolved.contains_key(&entry.oid);
        if !in_push {
            foreign.insert(entry.oid);
        }
        // An object older than this push that the plan stores whole is
        // already in the store: promotion forwards its stored entry
        // rather than restaging identical bytes.
        if entry.base.is_none() && !in_push {
            continue;
        }
        push(
            Want {
                oid: entry.oid,
                kind: entry.kind,
                base: entry.base,
            },
            &mut wants,
        );
    }

    for (oid, kind) in foreign_deltas(&foreign, state, repo).await? {
        push(
            Want {
                oid,
                kind,
                base: None,
            },
            &mut wants,
        );
    }
    Ok(wants)
}

/// Which of `foreign` are stored as deltas — the ones promotion may not
/// forward verbatim.
async fn foreign_deltas(
    foreign: &ObjectHashSet,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<Vec<(ObjectId, Kind)>, Error> {
    if foreign.is_empty() {
        return Ok(Vec::new());
    }
    let oids: Vec<ObjectId> = foreign.iter().copied().collect();
    let metas = enroute_git_retrieve::metas(state, repo.id, &oids).await?;
    Ok(metas
        .iter()
        .filter(|(_, meta)| {
            meta.location
                .as_ref()
                .is_some_and(|loc| loc.image.base.is_some())
        })
        .map(|(&oid, meta)| (oid, meta.kind))
        .collect())
}

async fn stage(item: Encoded, staging: &mut Staging, repo: &RepoMetadata) -> Result<(), Error> {
    match item.base {
        Some(base) => {
            staging
                .encoding(
                    item.oid,
                    item.kind,
                    base,
                    &item.compressed,
                    item.length,
                    repo,
                )
                .await
        }
        None => {
            staging
                .whole(item.oid, item.kind, &item.compressed, item.length, repo)
                .await
        }
    }
}

/// Split `wants` into batches bounded by both count and resident bytes.
///
/// Sizes come from the pack the entry arrived in where there is one, and
/// from zero otherwise, since sizing an older object costs a read.
fn batches(wants: &[Want], size_of: impl Fn(ObjectId) -> u64) -> Vec<&[Want]> {
    let mut batches = Vec::new();
    let (mut start, mut bytes) = (0usize, 0u64);
    for (at, want) in wants.iter().enumerate() {
        let cost = size_of(want.oid) + want.base.map_or(0, &size_of);
        let full = at - start >= BATCH || (at > start && bytes.saturating_add(cost) > BATCH_BYTES);
        if full {
            batches.extend(wants.get(start..at));
            (start, bytes) = (at, 0);
        }
        bytes = bytes.saturating_add(cost);
    }
    batches.extend(wants.get(start..).filter(|rest| !rest.is_empty()));
    batches
}

/// This pass' own state, shared by every item of a batch: the chain cache,
/// what the batch already read, and where its time is going.
#[derive(Clone, Copy)]
struct Encoder<'a> {
    rebuilder: &'a Rebuilder,
    prefetched: &'a ObjectHashMap<Bytes>,
    cost: &'a EncodeCost,
}

/// Rebuild whatever one item needs and encode it, taking a pre-existing
/// object from what the batch already read rather than reading it again.
async fn encode_one(
    want: Want,
    encoder: &Encoder<'_>,
    wire: &WirePacks,
    staging: &Staging,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<Encoded, Error> {
    let Encoder {
        rebuilder,
        prefetched,
        cost,
    } = *encoder;
    let at = std::time::Instant::now();
    let target = rebuilder
        .content_or(want.oid, prefetched, wire, staging, state, repo)
        .await?;
    let base = match want.base {
        Some(base) => Some(
            rebuilder
                .content_or(base, prefetched, wire, staging, state, repo)
                .await?,
        ),
        None => None,
    };
    cost.fetch.add(at.elapsed());

    let at = std::time::Instant::now();
    let out = on_pool(move || match base {
        Some(base) => encode_pair(want, &target, &base),
        None => encode_whole(want, &target),
    })
    .await;
    cost.pool.add(at.elapsed());
    out?
}

#[cfg(test)]
mod tests {
    use gix_object::Kind;

    use enroute_git_core::oid;

    use super::{BATCH, BATCH_BYTES, Want, batches};

    fn want(n: u8) -> Want {
        Want {
            oid: oid(n),
            kind: Kind::Blob,
            base: None,
        }
    }

    /// Whatever the sizes, every item must land in exactly one batch and no
    /// batch may be empty — a lost item is an object that never gets stored.
    #[test]
    fn every_item_is_batched_exactly_once() {
        let wants: Vec<Want> = (0..200u8).map(want).collect();
        for size in [0, 1, BATCH_BYTES / 4, BATCH_BYTES, BATCH_BYTES * 4] {
            let batches = batches(&wants, |_| size);
            assert!(batches.iter().all(|b| !b.is_empty()), "size {size}");
            assert_eq!(
                batches.iter().map(|b| b.len()).sum::<usize>(),
                wants.len(),
                "size {size}"
            );
        }
    }

    /// The point of the byte bound: a run of large objects must cut sooner
    /// than the item count would.
    #[test]
    fn a_run_of_large_objects_cuts_before_the_count_does() {
        // Fewer than one batch's worth by count, so only the bytes can cut.
        let wants: Vec<Want> = (0..40u8).map(want).collect();
        assert!(wants.len() < BATCH);
        let batches = batches(&wants, |_| BATCH_BYTES / 4);
        assert!(
            batches.len() > 1,
            "four items should fill the budget, not all {}",
            wants.len()
        );
        assert!(batches.iter().all(|b| b.len() <= 5), "{batches:?}");
    }

    /// An object bigger than the whole budget still has to go somewhere.
    #[test]
    fn an_oversized_item_goes_alone_rather_than_nowhere() {
        let wants: Vec<Want> = (0..3u8).map(want).collect();
        let batches = batches(&wants, |_| BATCH_BYTES * 2);
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|b| b.len() == 1));
    }
}
