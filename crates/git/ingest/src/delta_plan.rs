//! What each commit's pack stores, and what each entry deltas against.
//!
//! A commit's pack carries one entry per path whose content differs from its
//! lattice base, which lets a chain of bases climb the lattice in
//! `popcount(depth)` hops instead of walking version by version.

use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{CommitPackLocation, Error, ObjectHashMap, ObjectHashSet, topo_order};
use enroute_git_graph::{ObjectRefs, TreeChild, TreeReader, diff_trees, object_refs as parse_tree};
use enroute_git_metadata::Identity;
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::pack::WireRef;
use crate::progress::{IngestProgress, ProgressSink};
use crate::timing::as_u64;

/// Where an entry's bytes come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntrySource {
    /// The client's own bytes, at that entry of the pack it sent — copied
    /// into the commit pack without being inflated or recompressed.
    Wire(WireRef),
    /// Produced by [`crate::materialise`] and held in staging until
    /// promotion.
    Encoded,
}

/// One entry of a commit's pack: an object, the version it is encoded
/// against, and where those bytes come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DeltaEntry {
    pub(crate) oid: ObjectId,
    /// Tree or blob.
    pub(crate) kind: Kind,
    /// The version this entry deltas against, or `None` when stored whole —
    /// no counterpart at the base, or a commit rooting its own line.
    pub(crate) base: Option<ObjectId>,
    pub(crate) source: EntrySource,
}

/// What each commit's pack holds.
#[derive(Debug, Default)]
pub(crate) struct DeltaPlan {
    /// Pack contents per commit, `commit → trees → blobs` order — the
    /// commit's own entry, always whole, is not here.
    pub(crate) packs: ObjectHashMap<Vec<DeltaEntry>>,
    /// Where each commit's own entry comes from — always whole, so only its
    /// source is in question.
    pub(crate) commits: ObjectHashMap<EntrySource>,
}

/// Longest chain an entry may sit at the end of, per kind; reaching it stores
/// the object whole, bounding a reader's round trips.
///
/// Trees are held shorter: the push path reads them every push and they
/// deltify well (6.4x on react vs. blobs' 3.5x), so whole copies are cheap.
const MAX_TREE_CHAIN: u32 = 8;
const MAX_BLOB_CHAIN: u32 = 32;

const fn max_chain(kind: Kind) -> u32 {
    if matches!(kind, Kind::Tree) {
        MAX_TREE_CHAIN
    } else {
        MAX_BLOB_CHAIN
    }
}

/// Trees already in hand: this push's own, plus whatever an earlier round
/// read — shared across commits, since consecutive commits diff similar trees.
struct Trees<'a> {
    in_push: &'a ObjectHashMap<ObjectRefs>,
    fetched: &'a ObjectHashMap<Option<Vec<TreeChild>>>,
}

impl TreeReader for Trees<'_> {
    fn children(&mut self, oid: ObjectId) -> Option<Vec<TreeChild>> {
        if let Some(ObjectRefs::Tree(children)) = self.in_push.get(&oid) {
            return Some(children.clone());
        }
        self.fetched.get(&oid)?.clone()
    }
}

/// Every in-push commit's parents — this push's own commit graph, which is
/// what makes a parent missing from it a boundary.
pub(crate) fn commit_parents(
    object_refs: &ObjectHashMap<ObjectRefs>,
) -> ObjectHashMap<Vec<ObjectId>> {
    object_refs
        .iter()
        .filter_map(|(&oid, refs)| match refs {
            ObjectRefs::Commit { parents, .. } => Some((oid, parents.clone())),
            _ => None,
        })
        .collect()
}

/// Place every commit of this push on the lattice and work out what each
/// one's pack stores.
///
/// Every entry asks to be encoded against its predecessor;
/// [`crate::classify`] may replace that.
///
/// # Errors
/// A base commit or a pre-existing tree can't be read.
#[tracing::instrument(skip_all)]
pub(crate) async fn plan_push(
    object_refs: &ObjectHashMap<ObjectRefs>,
    state: &Storage,
    repo: &RepoMetadata,
    progress: ProgressSink<'_>,
) -> Result<DeltaPlan, Error> {
    // Before the boundary read below, which is a store round trip on a push
    // whose parents are already stored.
    progress(IngestProgress::PreparingPacks { done: 0, total: 0 });
    let commits: Vec<(ObjectId, ObjectId, Vec<ObjectId>)> = object_refs
        .iter()
        .filter_map(|(&oid, refs)| match refs {
            ObjectRefs::Commit {
                root_tree, parents, ..
            } => Some((oid, *root_tree, parents.clone())),
            _ => None,
        })
        .collect();

    let root_of: ObjectHashMap<ObjectId> = commits
        .iter()
        .map(|(oid, root_tree, _)| (*oid, *root_tree))
        .collect();

    // The immediate predecessor along each first-parent line. A parent this
    // push carries is already in hand; the rest are one batch, deduplicated,
    // since a fan of branches shares its boundary.
    let boundary: Vec<ObjectId> = commits
        .iter()
        .filter_map(|(_, _, parents)| parents.first().copied())
        .filter(|parent| !root_of.contains_key(parent))
        .collect::<ObjectHashSet>()
        .into_iter()
        .collect();
    let boundary_roots = root_trees_of(&boundary, state, repo).await?;

    let mut pending: Vec<Planning> = commits
        .iter()
        .map(|(commit, root_tree, parents)| Planning {
            commit: *commit,
            root_tree: *root_tree,
            base_root: parents
                .first()
                .and_then(|parent| root_of.get(parent).or_else(|| boundary_roots.get(parent)))
                .copied(),
            entries: None,
            unplannable: false,
        })
        .collect();

    let total = as_u64(pending.len());
    let mut fetched: ObjectHashMap<Option<Vec<TreeChild>>> = ObjectHashMap::default();
    loop {
        let round = diff_round(&mut pending, object_refs, &fetched)?;
        // Per round, not per commit: the read between rounds is the slow part.
        let settled = pending.iter().filter(|item| item.settled()).count();
        progress(IngestProgress::PreparingPacks {
            done: as_u64(settled),
            total,
        });
        if round.is_empty() {
            break;
        }
        fetched.extend(read_trees(&round, state, repo).await?);
    }

    Ok(DeltaPlan {
        packs: pending
            .into_iter()
            .filter_map(|item| Some((item.commit, item.entries?)))
            .collect(),
        commits: ObjectHashMap::default(),
    })
}

/// One commit's place in the walk: the diff it is running, and the answer
/// once it has one.
struct Planning {
    commit: ObjectId,
    root_tree: ObjectId,
    base_root: Option<ObjectId>,
    entries: Option<Vec<DeltaEntry>>,
    /// The diff reached a tree the store doesn't hold, so this commit gets no
    /// pack — see [`plan_push`] on why that isn't an error here.
    unplannable: bool,
}

impl Planning {
    /// Reached an answer: a plan, or that there can't be one — one spelling,
    /// so the filter and progress count can't drift apart.
    fn settled(&self) -> bool {
        self.entries.is_some() || self.unplannable
    }
}

/// Advance every unfinished commit's diff by one round, returning the trees
/// the whole round is missing.
fn diff_round(
    pending: &mut [Planning],
    object_refs: &ObjectHashMap<ObjectRefs>,
    fetched: &ObjectHashMap<Option<Vec<TreeChild>>>,
) -> Result<Vec<ObjectId>, Error> {
    let mut round: ObjectHashSet = ObjectHashSet::default();
    for item in pending.iter_mut().filter(|item| !item.settled()) {
        let mut trees = Trees {
            in_push: object_refs,
            fetched,
        };
        let diff = diff_trees(&mut trees, item.base_root, item.root_tree);
        if diff.is_complete() {
            item.entries = Some(order_entries(item.root_tree, item.base_root, diff.changes));
            continue;
        }
        for oid in diff.unreadable {
            match fetched.get(&oid) {
                Some(None) => {
                    item.unplannable = true;
                    break;
                }
                Some(Some(_)) => {
                    return Err(anyhow::anyhow!("tree {oid} unreadable after being fetched").into());
                }
                None => {
                    round.insert(oid);
                }
            }
        }
    }
    Ok(round.into_iter().collect())
}

/// Read a round's missing trees.
///
/// `None` against an oid is a tree the store doesn't have — an unplannable
/// commit, not a failure; see [`plan_push`].
async fn read_trees(
    round: &[ObjectId],
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<ObjectHashMap<Option<Vec<TreeChild>>>, Error> {
    let metas = enroute_git_retrieve::metas(state, repo.id, round).await?;
    let wanted: Vec<(ObjectId, CommitPackLocation)> = round
        .iter()
        .filter_map(|oid| Some((*oid, metas.get(oid)?.location?)))
        .collect();
    let contents = enroute_git_retrieve::known(state, repo, &wanted).await?;

    // Everything asked for gets an answer, so a round always advances. A tree
    // the index can't place has no bytes to diff against either.
    let mut read: ObjectHashMap<Option<Vec<TreeChild>>> =
        round.iter().map(|&oid| (oid, None)).collect();
    for (oid, content) in contents {
        let children = parse_tree(Kind::Tree, &content)?
            .into_tree_children()
            .ok_or_else(|| anyhow::anyhow!("object {oid} is not a tree"))?;
        read.insert(oid, Some(children));
    }
    Ok(read)
}

/// Walk the plan parents-first, giving every entry a chain depth and cutting
/// any past its kind's limit, or with an earlier entry in the walk.
///
/// The second rule keeps the delta graph acyclic: deltifying only against a
/// strictly earlier base makes every edge run backwards, which no cycle can.
///
/// # Errors
/// The commit graph can't be topologically ordered, or the store can't
/// derive an older base's depth.
pub(crate) async fn bound_chains(
    packs: &mut ObjectHashMap<Vec<DeltaEntry>>,
    parents_of: &ObjectHashMap<Vec<ObjectId>>,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<(), Error> {
    let in_push: ObjectHashSet = packs
        .values()
        .flat_map(|entries| entries.iter().map(|e| e.oid))
        .collect();
    let mut seen: ObjectHashSet = ObjectHashSet::default();
    let older: Vec<ObjectId> = packs
        .values()
        .flat_map(|entries| entries.iter().filter_map(|e| e.base))
        .filter(|base| !in_push.contains(base) && seen.insert(*base))
        .collect();
    let older_depths = state
        .objects
        .repo(repo.id)
        .chain_depths(&older, MAX_BLOB_CHAIN)
        .await?;

    // Only an entry whose object already exists can close a loop. Absent from
    // the store means this push introduces it, which nothing older reaches.
    let known: Vec<ObjectId> = packs
        .values()
        .flat_map(|entries| entries.iter())
        .filter(|entry| entry.base.is_some())
        .map(|entry| entry.oid)
        .chain(older.iter().copied())
        .collect::<ObjectHashSet>()
        .into_iter()
        .collect();
    let stored_at = state.objects.repo(repo.id).first_packs(&known).await?;

    let mut depth: ObjectHashMap<u32> = ObjectHashMap::default();
    // Rank by first appearance in the walk, which places the objects this
    // push introduces; `stored_at` places the rest.
    let mut rank: ObjectHashMap<u32> = ObjectHashMap::default();
    let mut next_rank: u32 = 0;
    for commit in topo_order(parents_of)? {
        let Some(entries) = packs.get_mut(&commit) else {
            continue;
        };
        for entry in entries {
            // A base that doesn't strictly precede this object could loop.
            if let Some(base) = entry.base
                && let Some(own) = placed(entry.oid, &stored_at, &rank, true)
                && placed(base, &stored_at, &rank, false).is_some_and(|base| base >= own)
            {
                cut(entry);
            }
            // An unknown base is one nothing stores yet, so nothing can be
            // rebuilt through it.
            let reached = entry.base.map_or(0, |base| {
                depth
                    .get(&base)
                    .or_else(|| older_depths.get(&base))
                    .map_or(u32::MAX, |d| d.saturating_add(1))
            });
            let reached = if reached > max_chain(entry.kind) {
                cut(entry);
                0
            } else {
                reached
            };
            depth.entry(entry.oid).or_insert(reached);
            rank.entry(entry.oid).or_insert_with(|| {
                let r = next_rank;
                next_rank += 1;
                r
            });
        }
    }
    Ok(())
}

/// Where an object falls in resolution order — every stored object precedes
/// every object this push introduces, letting one comparison span both.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Placed {
    /// The pack seq of the object's canonical entry.
    Stored(i64),
    /// Where the object's first entry falls in this push's walk.
    InPush(u32),
}

/// Where to treat `oid` as sitting, or `None` when nothing holds it — so no
/// chain can lead back through it.
///
/// `prefer_stored` for the object being stored, at the earliest place it can
/// be; its base takes its in-push copy, which it could reach through.
fn placed(
    oid: ObjectId,
    stored_at: &ObjectHashMap<i64>,
    rank: &ObjectHashMap<u32>,
    prefer_stored: bool,
) -> Option<Placed> {
    let stored = || stored_at.get(&oid).map(|&seq| Placed::Stored(seq));
    let in_push = || rank.get(&oid).map(|&at| Placed::InPush(at));
    if prefer_stored {
        stored().or_else(in_push)
    } else {
        in_push().or_else(stored)
    }
}

/// Store this entry's object whole.
///
/// A client's delta stream is built for one base and reconstructs nothing
/// without it, so dropping the base drops the bytes with it.
fn cut(entry: &mut DeltaEntry) {
    if entry.base.is_some() && matches!(entry.source, EntrySource::Wire(_)) {
        entry.source = EntrySource::Encoded;
    }
    entry.base = None;
}

/// Turn a diff into pack entries: root tree first, then trees, then blobs.
///
/// The root tree is added here since a diff never names its own operands;
/// entries dedup by oid since one object can sit at several changed paths.
fn order_entries(
    root_tree: ObjectId,
    base_root: Option<ObjectId>,
    changes: Vec<enroute_git_graph::Change>,
) -> Vec<DeltaEntry> {
    // An unchanged root tree means an unchanged commit tree: the base's pack
    // already holds every object, and the diff is empty.
    if changes.is_empty() && base_root == Some(root_tree) {
        return Vec::new();
    }
    let mut seen: ObjectHashSet = ObjectHashSet::default();
    let mut trees = vec![DeltaEntry {
        oid: root_tree,
        kind: Kind::Tree,
        base: base_root,
        source: EntrySource::Encoded,
    }];
    seen.insert(root_tree);
    let mut blobs = Vec::new();
    for change in changes {
        if !seen.insert(change.new) {
            continue;
        }
        let entry = DeltaEntry {
            oid: change.new,
            kind: change.kind,
            base: change.old,
            source: EntrySource::Encoded,
        };
        if change.kind == Kind::Tree {
            trees.push(entry);
        } else {
            blobs.push(entry);
        }
    }
    trees.extend(blobs);
    trees
}

/// The root trees of commits this push doesn't carry — the lattice bases its
/// boundary commits diff against.
///
/// One batch, since a fan of branches off one trunk gives every boundary
/// commit the same parent, and asking per commit pays for it twice over.
async fn root_trees_of(
    commits: &[ObjectId],
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<ObjectHashMap<ObjectId>, Error> {
    // These are commits, so only the commit graph is asked: a `batch_lookup`
    // would read the object index too, and throw the answer away.
    let held = state
        .rows
        .repo(repo.id)
        .identify(commits)
        .await
        .map_err(|e| anyhow::anyhow!("identifying lattice bases: {e:#}"))?;
    let mut named: Vec<(ObjectId, Identity)> = Vec::with_capacity(held.len());
    for oid in commits {
        // An oid the repository does not hold has no base to diff against,
        // and is skipped as it always was; one it holds as something else is
        // a bug, and says so here rather than after fetching its bytes.
        let Some(identity) = held.get(oid).copied() else {
            continue;
        };
        if identity.kind != Kind::Commit {
            return Err(anyhow::anyhow!("lattice base {oid} is not a commit").into());
        }
        named.push((*oid, identity));
    }

    let metas = state
        .graph
        .repo(repo.id)
        .locations_of(&named)
        .await
        .map_err(|e| anyhow::anyhow!("commit graph read for lattice bases: {e:#}"))?;
    let wanted: Vec<(ObjectId, CommitPackLocation)> = metas
        .into_iter()
        .filter_map(|(oid, meta)| Some((oid, meta.location?)))
        .collect();

    enroute_git_retrieve::known(state, repo, &wanted)
        .await?
        .into_iter()
        .map(|(oid, content)| match parse_tree(Kind::Commit, &content)? {
            ObjectRefs::Commit { root_tree, .. } => Ok((oid, root_tree)),
            _ => Err(anyhow::anyhow!("lattice base {oid} is not a commit").into()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{DeltaEntry, EntrySource, order_entries};
    use enroute_git_core::oid;
    use enroute_git_graph::Change;
    use gix_object::Kind;

    fn change(path: &str, kind: Kind, old: Option<u8>, new: u8) -> Change {
        Change {
            path: path.as_bytes().to_vec(),
            kind,
            old: old.map(oid),
            new: oid(new),
        }
    }

    /// `blob_section_offset` reads the boundary off the entry order, so trees
    /// must all precede blobs and the root tree must lead.
    #[test]
    fn root_tree_leads_and_blobs_trail() {
        let entries = order_entries(
            oid(1),
            Some(oid(2)),
            vec![
                change("a", Kind::Blob, Some(10), 11),
                change("d", Kind::Tree, Some(50), 51),
                change("d/f", Kind::Blob, None, 12),
            ],
        );
        let kinds: Vec<Kind> = entries.iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            vec![Kind::Tree, Kind::Tree, Kind::Blob, Kind::Blob],
            "{entries:?}"
        );
        assert_eq!(entries[0].oid, oid(1));
        assert_eq!(entries[0].base, Some(oid(2)));
    }

    #[test]
    fn a_changed_path_carries_its_predecessor_as_the_base() {
        let entries = order_entries(oid(1), None, vec![change("a", Kind::Blob, Some(10), 11)]);
        assert!(entries.contains(&DeltaEntry {
            oid: oid(11),
            kind: Kind::Blob,
            base: Some(oid(10)),
            source: EntrySource::Encoded,
        }));
    }

    /// One object at two changed paths is one pack entry: either path's base
    /// rebuilds the same bytes, and a pack holds one entry per object.
    #[test]
    fn an_object_at_two_paths_gets_one_entry() {
        let entries = order_entries(
            oid(1),
            None,
            vec![
                change("a", Kind::Blob, Some(10), 20),
                change("b", Kind::Blob, Some(11), 20),
            ],
        );
        assert_eq!(entries.iter().filter(|e| e.oid == oid(20)).count(), 1);
    }

    /// A commit whose tree is its base's stores nothing: every object is
    /// already in the base's pack.
    #[test]
    fn an_unchanged_tree_stores_nothing() {
        assert!(order_entries(oid(1), Some(oid(1)), vec![]).is_empty());
    }

    /// With no base every path is new, so the root tree is stored whole.
    #[test]
    fn a_root_commit_stores_its_tree_whole() {
        let entries = order_entries(oid(1), None, vec![change("a", Kind::Blob, None, 10)]);
        assert_eq!(entries[0].base, None);
    }
}
