//! Recording what a push brought: first what it is called, then what is held.
//!
//! Here rather than in storage because it is the push that knows what a push
//! is: identity numbers what arrived, the commit graph takes the parents and
//! the object index the records, and the ordering that makes them agree is
//! this path's own.
//!
//! # Two writes
//! Numbering commits alone, so two pushes of one object collide before either
//! writes an entry; the loser re-reads and takes the number that won. Nothing
//! reads an object through its number, so a push is invisible until the
//! journal lands, and one that stops between the two is finished by the next.

use anyhow::{Context, Result};
use gix_hash::ObjectId;
use gix_object::Kind;
use roaring::RoaringTreemap;

use enroute_git_core::{
    NewCommit, NewObject, ObjectHashMap, ObjectHashSet, ObjectSeq, ObjectSeqs, PackImageLocation,
    Ulid, topo_order,
};
use enroute_git_graph_store::{Pack, Recorded as RecordedCommit, RepoGraph};
use enroute_git_journal::{Journal, Ledger};
use enroute_git_metadata::{Identity, Raced, RepoRows};
use enroute_git_objects::{Location, Recorded as RecordedObject, RepoObjects};

use crate::object_io::KnownIdentities;

/// The layers one push records into.
///
/// Passed as one value because every caller holds them as one: they are the
/// same [`Storage`](enroute_git_retrieve::Storage) a read is served from.
#[derive(Clone, Copy)]
pub struct Engine<'a> {
    /// What every object of this repository is called.
    pub ids: RepoRows<'a>,
    /// What a commit points at, and where its pack image is.
    pub graph: RepoGraph<'a>,
    /// Where a tree or blob is stored, and what a tree contains.
    pub objects: RepoObjects<'a>,
    /// Where the index rows land, all of them or none.
    pub ledger: &'a dyn Ledger,
}

impl core::fmt::Debug for Engine<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Engine")
            .field("repo", &self.graph.repo_id())
            .finish_non_exhaustive()
    }
}

/// What one commit's pack stores, split by kind.
#[derive(Default)]
struct PackBitmaps {
    trees: RoaringTreemap,
    blobs: RoaringTreemap,
}

/// An identity as the space it belongs to, or `None` for a commit.
///
/// A commit is numbered like everything else and read back like everything
/// else; it is only the segments above that hold it in a space of its own.
fn numbered(identity: Identity) -> Option<ObjectSeq> {
    ObjectSeq::of(identity.kind, u64::try_from(identity.seq).ok()?)
}

fn unresolved_parent(parent: ObjectId, child: ObjectId) -> anyhow::Error {
    anyhow::anyhow!("metadata store: unresolved parent {parent} for {child}")
}

/// The segment objects this push's fresh packs were appended to, deduplicated.
///
/// Registered in the write that references them, so a segment object with no
/// row is a failed push's orphan and never the other way round.
fn fresh_segments(to_insert: &[ObjectId], new_commits: &ObjectHashMap<NewCommit>) -> Vec<Ulid> {
    let mut seen: std::collections::HashSet<Ulid> = std::collections::HashSet::new();
    to_insert
        .iter()
        .filter_map(|oid| new_commits.get(oid))
        .map(|commit| commit.segment.id)
        .filter(|id| seen.insert(*id))
        .collect()
}

/// This push's commits, as object planning needs to see them.
struct PushCommits<'a> {
    to_insert: &'a [ObjectId],
    new_commits: &'a ObjectHashMap<NewCommit>,
}

/// What a push brought about its non-commit objects, resolved.
struct Objects<'a> {
    /// Objects with work this push, paired with their fresh locations.
    relevant: Vec<(&'a NewObject, Vec<&'a PackImageLocation>)>,
    /// Every oid this push touches, resolved to a seq.
    seq_of: ObjectHashMap<ObjectSeq>,
    /// Which of them are appearing here for the first time.
    fresh: ObjectHashSet,
    /// Which of them this push numbered, and so must write an identity for.
    ///
    /// Not the same as `fresh`: a re-recorded object keeps the number it has,
    /// and inserting that number again is a unique violation.
    numbered_here: ObjectHashSet,
}

/// A seq for every relevant object that has none yet.
///
/// One block per kind, because each kind is counted on its own: a tree and a
/// blob may hold the same number and be two different objects.
async fn allocate_object_seqs(
    ids: RepoRows<'_>,
    relevant: &[(&NewObject, Vec<&PackImageLocation>)],
    seq_of: &mut ObjectHashMap<ObjectSeq>,
) -> Result<ObjectHashSet> {
    let missing: Vec<(ObjectId, Kind)> = relevant
        .iter()
        .map(|(obj, _)| (obj.oid, obj.kind))
        .filter(|(oid, _)| !seq_of.contains_key(oid))
        .collect();
    for kind in [Kind::Tree, Kind::Blob, Kind::Tag] {
        let taking: Vec<ObjectId> = missing
            .iter()
            .filter(|(_, held)| *held == kind)
            .map(|(oid, _)| *oid)
            .collect();
        if taking.is_empty() {
            continue;
        }
        let count = u64::try_from(taking.len()).context("object batch size overflow")?;
        let start = ids.allocate(kind, count).await?;
        for (offset, oid) in taking.iter().enumerate() {
            let offset = i64::try_from(offset).context("object batch index overflow")?;
            let seq = numbered(Identity {
                seq: start.saturating_add(offset),
                kind,
            })
            .with_context(|| format!("metadata store: {oid} of kind {kind} is not counted"))?;
            seq_of.insert(*oid, seq);
        }
    }
    Ok(missing.into_iter().map(|(oid, _)| oid).collect())
}

/// Resolve this push's relevant non-commit objects and allocate a seq for
/// the genuinely-new ones.
///
/// Only work attached to a **freshly-created pack** is emitted: a pack whose
/// commit already existed has its objects and bitmaps already recorded.
async fn plan_objects<'a>(
    ids: RepoRows<'_>,
    objects: RepoObjects<'_>,
    new_objects: &'a [NewObject],
    commits: &PushCommits<'_>,
    known_seqs: &KnownIdentities,
) -> Result<Objects<'a>> {
    // A pack is fresh when this push is the one recording its commit, which
    // is exactly `to_insert` — not a seq range, since a commit re-recorded
    // under the number it already had falls below anything this push allocated.
    let recording: ObjectHashSet = commits.to_insert.iter().copied().collect();
    let is_fresh_pack = |pack_sha: &ObjectId| recording.contains(pack_sha);
    let relevant: Vec<(&NewObject, Vec<&PackImageLocation>)> = new_objects
        .iter()
        .filter_map(|obj| {
            let fresh: Vec<&PackImageLocation> = obj
                .locations
                .iter()
                .filter(|loc| is_fresh_pack(&loc.pack_sha))
                .collect();
            (!fresh.is_empty() || obj.locations.is_empty()).then_some((obj, fresh))
        })
        .collect();

    // Every oid this push has to resolve: the objects themselves, whatever
    // their entries delta against, and whatever a tree names as a child.
    let mut wanted: ObjectHashSet = ObjectHashSet::default();
    for (obj, locs) in &relevant {
        wanted.insert(obj.oid);
        wanted.extend(obj.children.iter().copied());
        wanted.extend(locs.iter().filter_map(|loc| loc.base));
    }
    // A commit's root tree needs a seq too, and it is often an object an
    // older push recorded rather than one this push brought.
    wanted.extend(
        commits
            .to_insert
            .iter()
            .filter_map(|oid| commits.new_commits.get(oid))
            .map(|commit| commit.root_tree),
    );

    // A stale absence is safe only for an object this push would insert:
    // losing that race is a unique violation, which `append` retries against
    // a fresh read. For one it merely names — a child, a base, a root tree —
    // believing it strands whatever named it, with nothing to retry on.
    let insertable: ObjectHashSet = relevant.iter().map(|(obj, _)| obj.oid).collect();
    let mut seq_of: ObjectHashMap<ObjectSeq> = ObjectHashMap::default();
    let mut unknown: Vec<ObjectId> = Vec::new();
    for oid in &wanted {
        match known_seqs.get(oid) {
            Some(identity) => {
                if let Some(held) = numbered(identity) {
                    seq_of.insert(*oid, held);
                }
            }
            None if known_seqs.covers(oid) && insertable.contains(oid) => {}
            None => unknown.push(*oid),
        }
    }
    if !unknown.is_empty() {
        for (oid, identity) in ids.identify(&unknown).await? {
            if let Some(held) = numbered(identity) {
                seq_of.insert(oid, held);
            }
        }
    }

    // Which of these the index already holds, asked before anything is
    // allocated: a seq handed out a moment ago names nothing yet, and asking
    // about it would read every new object as one already recorded. A tag is
    // recorded by identity alone, which is what `is_stored` says of one.
    let held: Vec<(ObjectId, Identity)> = relevant
        .iter()
        .filter_map(|(obj, _)| {
            let seq = seq_of.get(&obj.oid)?;
            Some((
                obj.oid,
                Identity {
                    seq: i64::try_from(seq.seq()).ok()?,
                    kind: obj.kind,
                },
            ))
        })
        .collect();
    let recorded: ObjectHashSet = objects
        .lookup(&held)
        .await?
        .into_iter()
        .filter(|(_, meta)| meta.is_stored())
        .map(|(oid, _)| oid)
        .collect();

    let numbered_here = allocate_object_seqs(ids, &relevant, &mut seq_of).await?;

    let fresh: ObjectHashSet = relevant
        .iter()
        .map(|(obj, _)| obj.oid)
        .filter(|oid| !recorded.contains(oid))
        .collect();

    Ok(Objects {
        relevant,
        seq_of,
        fresh,
        numbered_here,
    })
}

/// The locations one object gained this push, as the index records them.
fn locations_of(
    obj: &NewObject,
    fresh: &[&PackImageLocation],
    new_commits: &ObjectHashMap<NewCommit>,
    commit_seq_of: &ObjectHashMap<i64>,
    seq_of: &ObjectHashMap<ObjectSeq>,
) -> Result<Vec<Location>> {
    let mut locations = Vec::with_capacity(fresh.len());
    for loc in fresh {
        let (Some(&pack_seq), Some(pack)) = (
            commit_seq_of.get(&loc.pack_sha),
            new_commits.get(&loc.pack_sha),
        ) else {
            continue;
        };
        // A base is numbered where the entry is, since git never deltas
        // across kinds. One that is not would read back as another object.
        let base_seq = loc
            .base
            .map(|base| {
                let numbered = seq_of.get(&base).copied().with_context(|| {
                    format!(
                        "metadata store: entry base {base} of {} has no seq",
                        obj.oid
                    )
                })?;
                if numbered.kind() == obj.kind {
                    Ok(numbered.seq())
                } else {
                    Err(anyhow::anyhow!(
                        "metadata store: {} of kind {} deltas against {base} of kind {}",
                        obj.oid,
                        obj.kind,
                        numbered.kind()
                    ))
                }
            })
            .transpose()?;
        locations.push(Location {
            pack_seq,
            pack_oid: loc.pack_sha,
            segment: pack.segment,
            offset: loc.offset,
            entry_len: loc.entry_len,
            base_seq,
        });
    }
    Ok(locations)
}

/// A tree's direct entries, as the seqs the index holds them by.
fn children_of(obj: &NewObject, seq_of: &ObjectHashMap<ObjectSeq>) -> Result<ObjectSeqs> {
    let mut children = ObjectSeqs::default();
    if obj.kind != Kind::Tree {
        return Ok(children);
    }
    for child in &obj.children {
        let numbered = seq_of.get(child).copied().with_context(|| {
            format!("metadata store: tree {} child {child} has no seq", obj.oid)
        })?;
        match numbered {
            ObjectSeq::Tree(seq) => children.trees.insert(seq),
            ObjectSeq::Blob(seq) => children.blobs.insert(seq),
            ObjectSeq::Tag(_) => {
                return Err(anyhow::anyhow!(
                    "metadata store: tree {} names {child}, which is a tag",
                    obj.oid
                ));
            }
        };
    }
    Ok(children)
}

/// What each fresh pack stores, split by kind, keyed by its commit.
fn accumulate_pack_bitmaps(objects: &Objects<'_>) -> Result<ObjectHashMap<PackBitmaps>> {
    let mut bitmaps: ObjectHashMap<PackBitmaps> = ObjectHashMap::default();
    for (obj, fresh_locs) in &objects.relevant {
        if fresh_locs.is_empty() {
            continue;
        }
        let seq = *objects
            .seq_of
            .get(&obj.oid)
            .ok_or_else(|| anyhow::anyhow!("metadata store: object {} lost its seq", obj.oid))?;
        for loc in fresh_locs {
            let pack = bitmaps.entry(loc.pack_sha).or_default();
            match seq {
                ObjectSeq::Tree(seq) => pack.trees.insert(seq),
                ObjectSeq::Blob(seq) => pack.blobs.insert(seq),
                ObjectSeq::Tag(_) => {
                    return Err(anyhow::anyhow!(
                        "metadata store: tag {} has a pack location",
                        obj.oid
                    ));
                }
            };
        }
    }
    Ok(bitmaps)
}

/// One commit as the graph store records it.
fn build_commit(
    oid: ObjectId,
    new_commits: &ObjectHashMap<NewCommit>,
    commit_seq_of: &ObjectHashMap<i64>,
    object_seq_of: &ObjectHashMap<ObjectSeq>,
    bitmaps: &ObjectHashMap<PackBitmaps>,
) -> Result<RecordedCommit> {
    let commit = new_commits
        .get(&oid)
        .ok_or_else(|| anyhow::anyhow!("metadata store: missing NewCommit metadata for {oid}"))?;

    let mut parents = Vec::with_capacity(commit.parents.len());
    for parent in &commit.parents {
        parents.push(
            *commit_seq_of
                .get(parent)
                .ok_or_else(|| unresolved_parent(*parent, oid))?,
        );
    }

    let pack = bitmaps.get(&oid);
    Ok(RecordedCommit {
        seq: *commit_seq_of
            .get(&oid)
            .ok_or_else(|| anyhow::anyhow!("metadata store: no seq allocated for commit {oid}"))?,
        oid,
        parents,
        root_tree_seq: match object_seq_of.get(&commit.root_tree) {
            Some(ObjectSeq::Tree(seq)) => *seq,
            _ => {
                return Err(anyhow::anyhow!(
                    "metadata store: unresolved root tree {} for {oid}",
                    commit.root_tree
                ));
            }
        },
        committer_date: commit.committer_date,
        pack: Pack {
            entry_len: commit.entry_len,
            blob_offset: commit.blob_offset,
            segment: commit.segment,
            trees: pack.map(|p| p.trees.clone()).unwrap_or_default(),
            blobs: pack.map(|p| p.blobs.clone()).unwrap_or_default(),
        },
    })
}

/// Every commit this push adds, and the seq of every commit it names.
///
/// "Adds" is decided by the graph rather than by identity: a number says what
/// a commit is called, and only an entry says the repository holds it.
async fn plan_commits(
    ids: RepoRows<'_>,
    graph: RepoGraph<'_>,
    new_commits: &ObjectHashMap<NewCommit>,
    known: &KnownIdentities,
) -> Result<(Vec<ObjectId>, ObjectHashMap<i64>, ObjectHashSet)> {
    let parents_map: ObjectHashMap<Vec<ObjectId>> = new_commits
        .iter()
        .map(|(&oid, c)| (oid, c.parents.clone()))
        .collect();
    let order = topo_order(&parents_map)?;

    // This push's commits and the parents they build on. Attribution already
    // asked about both, so an ordinary push resolves them out of what it read
    // and reaches the database for nothing at all.
    let order_set: ObjectHashSet = order.iter().copied().collect();
    let mut seq_of: ObjectHashMap<i64> = ObjectHashMap::default();
    let mut asking: Vec<ObjectId> = Vec::new();
    for oid in order.iter().copied().chain(
        new_commits
            .values()
            .flat_map(|c| c.parents.iter().copied())
            .filter(|p| !order_set.contains(p)),
    ) {
        if known.covers(&oid) {
            if let Some(identity) = known.get(&oid).filter(|held| held.kind == Kind::Commit) {
                seq_of.insert(oid, identity.seq);
            }
        } else {
            asking.push(oid);
        }
    }
    if !asking.is_empty() {
        seq_of.extend(ids.seqs_of(Kind::Commit, &asking).await?);
    }

    // Recorded, not merely numbered. A commit whose number was handed out but
    // whose graph entry never landed has to be written, or nothing would ever
    // write it: identity is permanent, so every later push would skip it too.
    let named: Vec<(ObjectId, Identity)> = seq_of
        .iter()
        .map(|(&oid, &seq)| {
            (
                oid,
                Identity {
                    seq,
                    kind: Kind::Commit,
                },
            )
        })
        .collect();
    let already: ObjectHashSet = graph
        .locations_of(&named)
        .await?
        .into_iter()
        .filter(|(_, meta)| meta.is_stored())
        .map(|(oid, _)| oid)
        .collect();

    let to_insert: Vec<ObjectId> = order
        .iter()
        .copied()
        .filter(|oid| !already.contains(oid))
        .collect();
    if to_insert.is_empty() {
        return Ok((to_insert, seq_of, ObjectHashSet::default()));
    }

    // A commit that already has a number keeps it. Reusing it is what makes a
    // retry write the entry that was missing rather than strand the old number
    // and take a new one, and the index composes the same either way.
    let taking: Vec<ObjectId> = to_insert
        .iter()
        .copied()
        .filter(|oid| !seq_of.contains_key(oid))
        .collect();
    if !taking.is_empty() {
        let batch = u64::try_from(taking.len()).context("commit batch size overflow")?;
        let start = ids.allocate(Kind::Commit, batch).await?;
        for (offset, &oid) in taking.iter().enumerate() {
            let offset = i64::try_from(offset).context("commit batch index overflow")?;
            seq_of.insert(oid, start + offset);
        }
    }
    Ok((to_insert, seq_of, taking.into_iter().collect()))
}

/// One attempt at [`append`].
async fn append_once(
    engine: Engine<'_>,
    new_commits: &ObjectHashMap<NewCommit>,
    new_objects: &[NewObject],
    known_seqs: &KnownIdentities,
) -> Result<()> {
    let Engine {
        ids,
        graph,
        objects,
        ledger,
    } = engine;
    let (to_insert, commit_seq_of, commits_numbered_here) =
        plan_commits(ids, graph, new_commits, known_seqs).await?;

    let planned = plan_objects(
        ids,
        objects,
        new_objects,
        &PushCommits {
            to_insert: &to_insert,
            new_commits,
        },
        known_seqs,
    )
    .await?;
    let bitmaps = accumulate_pack_bitmaps(&planned)?;

    let mut fresh_objects = Vec::new();
    let mut added_packs = Vec::new();
    for (obj, fresh_locs) in &planned.relevant {
        let seq = *planned
            .seq_of
            .get(&obj.oid)
            .ok_or_else(|| anyhow::anyhow!("metadata store: object {} lost its seq", obj.oid))?;
        let locations = locations_of(
            obj,
            fresh_locs,
            new_commits,
            &commit_seq_of,
            &planned.seq_of,
        )?;
        if planned.fresh.contains(&obj.oid) {
            fresh_objects.push(RecordedObject {
                seq: seq.seq(),
                oid: obj.oid,
                kind: obj.kind,
                locations,
                children: children_of(obj, &planned.seq_of)?,
            });
        } else {
            added_packs.extend(locations.into_iter().map(|loc| (seq.seq(), obj.kind, loc)));
        }
    }

    let commits: Vec<RecordedCommit> = to_insert
        .iter()
        .map(|&oid| build_commit(oid, new_commits, &commit_seq_of, &planned.seq_of, &bitmaps))
        .collect::<Result<_>>()?;

    // What this push *numbered*, of every kind, in one go: the table does not
    // care which, and neither does the race that retries it. Only what was
    // numbered here — an object being re-recorded under the number it already
    // holds already has its row, and writing it again is a unique violation.
    let mut named: Vec<(ObjectId, Identity)> = commits
        .iter()
        .filter(|commit| commits_numbered_here.contains(&commit.oid))
        .map(|commit| {
            (
                commit.oid,
                Identity {
                    seq: commit.seq,
                    kind: Kind::Commit,
                },
            )
        })
        .collect();
    for object in &fresh_objects {
        if !planned.numbered_here.contains(&object.oid) {
            continue;
        }
        named.push((
            object.oid,
            Identity {
                seq: i64::try_from(object.seq).context("an object seq past a bigint")?,
                kind: object.kind,
            },
        ));
    }

    // What this push's boundary parents rank at, read out here: a read inside
    // the transaction would be a second connection held behind the first.
    let mine: Vec<i64> = commits.iter().map(|c| c.seq).collect();
    let parents: Vec<i64> = commits
        .iter()
        .flat_map(|c| c.parents.iter().copied())
        .collect();
    let ranks = graph.ranks_of(&mine, &parents).await?;

    // Numbering first and alone: two pushes of one object collide here, before
    // either wrote an entry, so the loser has nothing to undo. Every read path
    // asks the index, so until the journal lands the push is simply not there.
    ids.record(&named).await?;

    // Then the index, as one journal: a segment is encoded and put while this
    // is built, so nothing holds a transaction open across a bucket round trip.
    // `commit_segments` is registered here and not with the numbering, since it
    // is what stops the sweep reclaiming an image the index now points into.
    let mut journal = Journal::new();
    journal.register(fresh_segments(&to_insert, new_commits));
    objects.record(&mut journal, &fresh_objects).await?;
    objects.relocate(&mut journal, &added_packs).await?;
    graph.record(&mut journal, &commits, &ranks).await?;

    ledger.commit(graph.repo_id(), &journal).await?;
    Ok(())
}

/// Whether `err` is the numbering race [`append`]'s retry exists for.
///
/// What the store reported rather than what its driver called it, so a store
/// that is not a database says the same thing and is retried the same way.
fn is_duplicate_race(err: &anyhow::Error) -> bool {
    err.downcast_ref::<Raced>().is_some()
}

/// Retry cap for [`append`]'s identity race — generous headroom over the one
/// retry it actually needs.
///
/// A retry finishes the work rather than redoing it: the loser reuses the
/// number that won and writes the index entry against it.
const MAX_APPEND_ATTEMPTS: u32 = 5;

/// Record a push's commits and non-commit objects.
///
/// Numbering before the index, so a push that stops between the two leaves
/// numbers nothing reads. Parents outside `new_commits` are taken as recorded.
///
/// # Errors
/// A commit whose parent is neither recorded nor in `new_commits`, a
/// location naming a pack absent from it, or whatever storage said.
pub async fn append(
    engine: Engine<'_>,
    new_commits: &ObjectHashMap<NewCommit>,
    new_objects: &[NewObject],
    known_seqs: &KnownIdentities,
) -> Result<()> {
    if new_commits.is_empty() && new_objects.is_empty() {
        return Ok(());
    }

    // A retry is a push that lost a race, so what it read before it started
    // is exactly what it must not trust the second time: an oid it was told
    // was absent is the one the winner just recorded. Reading nothing is how
    // it reads again.
    let reread = KnownIdentities::default();
    let mut result = Ok(());
    for attempt in 0..MAX_APPEND_ATTEMPTS {
        let known = if attempt == 0 { known_seqs } else { &reread };
        result = append_once(engine, new_commits, new_objects, known).await;
        match &result {
            Err(err) if is_duplicate_race(err) => {}
            _ => return result,
        }
    }
    result
}
