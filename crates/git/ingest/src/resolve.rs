//! Pass 2 of ingest: work out what every entry of the pack *is*.
//!
//! Each entry is inflated, its delta applied, and the result hashed and
//! parsed for graph edges, then dropped — the wire pack keeps the bytes, and
//! this pass produces only the index over it that classification and
//! assembly work from.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{Error, ObjectHashMap, hash_loose};
use enroute_git_cost::count;
use enroute_git_graph::{ObjectRefs, object_refs};
use enroute_git_packfile::{apply_delta, inflate_entry};
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::pack::{EntryBase, ScannedEntry, read_span};
use crate::pool::{on_pool, pool};
use crate::progress::{IngestProgress, ProgressSink};
use crate::staging::Staging;
use crate::timing::{as_u64, as_usize, record_ms, timed};

/// Entries dispatched to the pool at once.
///
/// Bounds no memory on its own — [`IN_FLIGHT_BYTES`] is the real budget —
/// this only caps the queue enough to keep every thread fed.
const IN_FLIGHT_ENTRIES: usize = 64;

/// Roughly how much object content may be in flight at once.
///
/// The real bound: entry sizes vary by orders of magnitude, so a fixed
/// entry count bounds nothing. One oversized entry is admitted alone.
const IN_FLIGHT_BYTES: usize = 256 * 1024 * 1024;

/// Everything a worker needs to identify one entry, owned outright.
struct Job {
    /// The entry as it sits in the pack, header included.
    raw: Bytes,
    entry: ScannedEntry,
    base: JobBase,
}

/// What identifying one entry adds at its peak: the object it becomes.
///
/// A delta's result isn't known ahead of time, so its base stands in,
/// charged once. Its own bytes don't count: a view into a resident pack.
fn cost_of(entry: &ScannedEntry, base: &JobBase) -> usize {
    base.content.as_ref().map_or(0, Bytes::len) + as_usize(entry.decompressed_size)
}

/// What an entry becomes itself from: its own bytes, or a base to delta
/// against.
///
/// Carries the kind either way, since a delta inherits its base's.
struct JobBase {
    kind: Kind,
    /// `None` for a whole object, which needs nothing but itself.
    content: Option<Bytes>,
}

/// One identified object, and everything the driver has to record about it.
struct Done {
    sha: ObjectId,
    kind: Kind,
    /// Kept only so dependents can be applied against it; dropped once the
    /// last of them has run.
    content: Bytes,
    refs: ObjectRefs,
}

/// The whole of an entry's CPU cost: inflate, apply, hash, parse.
///
/// Touches no I/O and shares nothing across threads, so it needs no
/// coordination.
fn identify(job: Job, queued_at: Instant) -> Result<(Done, Timings), Error> {
    let mut timings = Timings {
        queued: queued_at.elapsed(),
        ..Timings::default()
    };

    let body = job
        .raw
        .get(job.entry.header_len..)
        .ok_or_else(|| anyhow::anyhow!("pack entry shorter than its header"))?;

    let inflated = timed(&mut timings.inflate, || {
        inflate_entry(body, job.entry.decompressed_size)
    })?;

    let kind = job.base.kind;
    let content = match job.base.content {
        None => inflated,
        Some(base) => timed(&mut timings.delta, || apply_delta(&base, &inflated))?.into(),
    };

    let sha = timed(&mut timings.hash, || hash_loose(kind, &content))?;
    let refs = timed(&mut timings.refs, || object_refs(kind, &content))?;

    Ok((
        Done {
            sha,
            kind,
            content,
            refs,
        },
        timings,
    ))
}

/// Identify one entry, tagged with what the driver needs back.
///
/// `at` and `cost` are the driver's own bookkeeping and never the worker's
/// business, so they ride alongside rather than through it.
async fn identify_entry(
    job: Job,
    at: usize,
    cost: usize,
) -> (usize, usize, Result<(Done, Timings), Error>) {
    let queued_at = Instant::now();
    let done = on_pool(move || identify(job, queued_at))
        .await
        .and_then(|r| r);
    (at, cost, done)
}

/// What one worker spent on one entry.
///
/// Returned rather than reported: a rayon thread carries no span context.
/// The driver sums them, so totals are worker-time, not wall.
#[derive(Default)]
struct Timings {
    /// Between being handed to the pool and a thread picking it up — near
    /// zero means the pool is idle; large means it's the constraint.
    queued: Duration,
    inflate: Duration,
    delta: Duration,
    hash: Duration,
    refs: Duration,
}

impl std::ops::AddAssign for Timings {
    fn add_assign(&mut self, rhs: Self) {
        self.queued += rhs.queued;
        self.inflate += rhs.inflate;
        self.delta += rhs.delta;
        self.hash += rhs.hash;
        self.refs += rhs.refs;
    }
}

/// Where this pass' time went, so a slow phase can be attributed rather than
/// argued about.
#[derive(Default)]
struct Waits {
    /// Driver with nothing to do but wait for the next entry to finish — its
    /// only wait, now that reading one is a slice of memory it already holds.
    stalled: Duration,
    /// Summed across entries.
    workers: Timings,
}

impl Waits {
    fn record(&self) {
        record_ms(&[
            ("stall_ms", self.stalled),
            ("queue_wait_ms", self.workers.queued),
            ("inflate_ms", self.workers.inflate),
            ("delta_ms", self.workers.delta),
            ("hash_ms", self.workers.hash),
            ("refs_ms", self.workers.refs),
        ]);
    }
}

/// What the scan left the driver to work from: every entry it located, the
/// pack they sit in, and which entries each one unblocks.
struct Scanned<'a> {
    entries: &'a [ScannedEntry],
    pack: &'a Bytes,
    /// For each entry, the entries that delta against it by position.
    by_index: &'a [Vec<usize>],
}

/// An entry whose base is in hand, waiting for a worker.
struct Pending {
    at: usize,
    base: JobBase,
}

impl Pending {
    /// An entry that is already whole.
    fn whole(at: usize, kind: Kind) -> Self {
        Self {
            at,
            base: JobBase {
                kind,
                content: None,
            },
        }
    }

    /// An entry that deltas against `content`, which it inherits the kind of.
    fn delta(at: usize, kind: Kind, content: Bytes) -> Self {
        Self {
            at,
            base: JobBase {
                kind,
                content: Some(content),
            },
        }
    }
}

/// Identify every entry the scan located, returning each object id
/// positionally and recording its outgoing refs in `resolved`.
///
/// No bytes are recorded — the wire pack keeps those.
///
/// # Errors
/// Returns an error if the pack is inconsistent — a delta whose base never
/// arrives — or if reading a pre-existing base fails.
#[tracing::instrument(
    name = "enroute_git_ingest::resolve",
    skip_all,
    fields(
        entries = count(entries.len()),
        pool_threads = count(pool().map_or(0, rayon::ThreadPool::current_num_threads)),
        // The span's own idle time cannot say whether this phase is fed
        // too slowly or computed too slowly; these can.
        stall_ms = tracing::field::Empty,
        // Summed across workers, so these exceed wall time when the pool
        // is doing its job. Their shape is what says where the CPU went.
        queue_wait_ms = tracing::field::Empty,
        inflate_ms = tracing::field::Empty,
        delta_ms = tracing::field::Empty,
        hash_ms = tracing::field::Empty,
        refs_ms = tracing::field::Empty,
    )
)]
pub(crate) async fn identify_entries(
    entries: &[ScannedEntry],
    pack: &Bytes,
    resolved: &mut ObjectHashMap<ObjectRefs>,
    staging: &Staging,
    state: &Storage,
    repo: &RepoMetadata,
    progress: ProgressSink<'_>,
) -> Result<Vec<ObjectId>, Error> {
    // Two ways an entry can be blocked, and both unblock the same way.
    // `OFS_DELTA` names its base by position, so it hangs off an index;
    // `REF_DELTA` names one by id, which may belong to this pack or to
    // the repo, and is only distinguishable by looking.
    let by_index = dependents_by_index(entries);
    let mut by_id: ObjectHashMap<Vec<usize>> = ObjectHashMap::default();
    let mut ready: VecDeque<Pending> = VecDeque::new();

    // Before any seeding, because asking per entry in the loop below
    // awaits each answer in turn — a react `push --all` at 1.8 in flight.
    let named: Vec<ObjectId> = entries
        .iter()
        .filter_map(|entry| match entry.base {
            EntryBase::Ref(base_id) => Some(base_id),
            _ => None,
        })
        .collect();
    let bases = staging.fetch_many(&named, state, repo).await?;

    for (at, entry) in entries.iter().enumerate() {
        match entry.base {
            EntryBase::Whole(kind) => ready.push_back(Pending::whole(at, kind)),
            // An `OFS_DELTA` is never seeded: it hangs off its base, and
            // becomes runnable when that base lands.
            EntryBase::InPack(_) => {}
            EntryBase::Ref(base_id) => match bases.get(&base_id) {
                // Already in the repo: a thin-pack base, so this is a
                // root of the pass.
                Some((kind, content)) => {
                    ready.push_back(Pending::delta(at, *kind, content.clone()));
                }
                // Not yet anywhere, so it is another entry of this pack
                // and this becomes runnable when that one lands.
                None => by_id.entry(base_id).or_default().push(at),
            },
        }
    }
    // Seeded entries hold what they need: this frees a base with the last
    // of them rather than at the end of the pass.
    drop(bases);

    // Recorded before the `?`: a pass that dies after five minutes is
    // exactly the one whose timings are worth having, and `drain` keeps
    // them in the caller's hands so the error path still reports them.
    let mut waits = Waits::default();
    let scanned = Scanned {
        entries,
        pack,
        by_index: &by_index,
    };
    let outcome = drain(&scanned, &mut by_id, ready, resolved, progress, &mut waits).await;
    waits.record();
    outcome
}

/// Run `ready` and everything it transitively unblocks to completion,
/// returning each entry's object id.
async fn drain(
    scanned: &Scanned<'_>,
    by_id: &mut ObjectHashMap<Vec<usize>>,
    mut ready: VecDeque<Pending>,
    resolved: &mut ObjectHashMap<ObjectRefs>,
    progress: ProgressSink<'_>,
    waits: &mut Waits,
) -> Result<Vec<ObjectId>, Error> {
    let Scanned {
        entries,
        pack,
        by_index,
    } = *scanned;
    let mut in_flight = FuturesUnordered::new();
    let mut in_flight_bytes = 0usize;
    let mut identified = 0usize;
    let mut oids = vec![ObjectId::null(gix_hash::Kind::Sha1); entries.len()];

    loop {
        while in_flight.len() < IN_FLIGHT_ENTRIES
            && (in_flight.is_empty() || in_flight_bytes < IN_FLIGHT_BYTES)
            && let Some(pending) = ready.pop_front()
        {
            let entry = entries
                .get(pending.at)
                .ok_or_else(|| anyhow::anyhow!("entry index out of range"))?;
            let cost = cost_of(entry, &pending.base);
            in_flight_bytes = in_flight_bytes.saturating_add(cost);
            let job = Job {
                raw: read_span(pack, entry.entry)?,
                entry: *entry,
                base: pending.base,
            };
            // Nothing is awaited here, so filling the window hands the
            // pool a batch instead of walking one entry at a time.
            in_flight.push(identify_entry(job, pending.at, cost));
        }

        let started = Instant::now();
        let next = in_flight.next().await;
        waits.stalled += started.elapsed();
        let Some((at, cost, done)) = next else {
            return all_identified(identified, entries.len()).map(|()| oids);
        };
        let (done, timings) = done?;
        waits.workers += timings;
        identified += 1;
        in_flight_bytes = in_flight_bytes.saturating_sub(cost);
        progress(IngestProgress::ResolvingObjects {
            done: as_u64(identified),
            total: as_u64(entries.len()),
        });

        if let Some(slot) = oids.get_mut(at) {
            *slot = done.sha;
        }
        resolved.insert(done.sha, done.refs);

        // Pushed to the front so a chain is followed to its end before
        // the next root starts, which keeps only one chain's worth of
        // bases alive rather than a whole level's.
        for &blocked in by_index.get(at).map(Vec::as_slice).unwrap_or_default() {
            ready.push_front(Pending::delta(blocked, done.kind, done.content.clone()));
        }
        for blocked in by_id.remove(&done.sha).unwrap_or_default() {
            ready.push_front(Pending::delta(blocked, done.kind, done.content.clone()));
        }
    }
}

/// Nothing left to run means every entry should have been identified; short
/// of that, the pack names a base that is neither in it nor already stored.
fn all_identified(resolved: usize, entries: usize) -> Result<(), Error> {
    if resolved == entries {
        return Ok(());
    }
    Err(Error::Invalid(format!(
        "identified {resolved} of {entries} pack entries; the rest delta against \
         a base that is neither in the pack nor already stored"
    )))
}

/// For each entry, the entries that delta against it by position.
fn dependents_by_index(entries: &[ScannedEntry]) -> Vec<Vec<usize>> {
    let mut dependents = vec![Vec::new(); entries.len()];
    for (at, entry) in entries.iter().enumerate() {
        if let EntryBase::InPack(base) = entry.base
            && let Some(slot) = dependents.get_mut(base)
        {
            slot.push(at);
        }
    }
    dependents
}

#[cfg(test)]
mod tests {
    use gix_hash::ObjectId;
    use gix_object::Kind;

    use super::dependents_by_index;
    use crate::pack::{EntryBase, RawSpan, ScannedEntry};

    fn entry(base: EntryBase) -> ScannedEntry {
        ScannedEntry {
            base,
            decompressed_size: 0,
            entry: RawSpan { offset: 0, len: 0 },
            header_len: 0,
        }
    }

    /// The dependency edges are what make the schedule correct: every delta
    /// must appear under the entry it deltifies against, and nothing else may.
    #[test]
    fn dependents_are_indexed_by_the_entry_they_delta_against() {
        let entries = vec![
            entry(EntryBase::Whole(Kind::Blob)),                         // 0
            entry(EntryBase::InPack(0)),                                 // 1
            entry(EntryBase::InPack(0)),                                 // 2
            entry(EntryBase::InPack(2)),                                 // 3
            entry(EntryBase::Ref(ObjectId::null(gix_hash::Kind::Sha1))), // 4
        ];

        let dependents = dependents_by_index(&entries);

        assert_eq!(dependents[0], vec![1, 2], "both deltas hang off the root");
        assert!(dependents[1].is_empty());
        assert_eq!(dependents[2], vec![3], "chains nest");
        assert!(dependents[3].is_empty());
        assert!(
            dependents[4].is_empty(),
            "an entry deltifying against an id outside the pack is a root, not a dependent"
        );
    }

    #[test]
    fn an_entry_never_depends_on_itself_or_a_later_one() {
        let entries = vec![
            entry(EntryBase::Whole(Kind::Tree)),
            entry(EntryBase::InPack(0)),
            entry(EntryBase::InPack(1)),
        ];
        for (at, deps) in dependents_by_index(&entries).iter().enumerate() {
            assert!(
                deps.iter().all(|&d| d > at),
                "entry {at} has a dependent that is not later in the pack: {deps:?}"
            );
        }
    }
}
