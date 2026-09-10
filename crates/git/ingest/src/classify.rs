//! Deciding, for every entry the plan holds, whether the client already sent
//! bytes we can store as they are.
//!
//! Git clients send packs that are mostly deltas, and their delta search is
//! both better than ours and already paid for. What stops us keeping one is
//! never its quality: the client picked a base for *its* pack, which need
//! not sit anywhere a reader of ours will look. Each kept delta is checked
//! against the storage format's one invariant — a base lives in the pack of
//! an ancestor of the entry's own commit — and whatever fails falls back to
//! being encoded here, which costs CPU rather than correctness. An object
//! the client sent whole stays whole.

use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{Error, ObjectHashMap, ObjectHashSet};
use enroute_git_cost::count;
use enroute_git_graph::{ObjectRefs, Query};
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::ancestry;
use crate::delta_plan::{DeltaEntry, DeltaPlan, EntrySource};
use crate::pack::{EntryBase, WirePacks, WireRef};

/// Rewrite `plan` to keep whatever of the client's own encodings a reader
/// could resolve.
///
/// # Errors
/// Returns an error if the pack index is inconsistent, or if resolving a
/// pre-existing base's home fails.
#[tracing::instrument(
    name = "enroute_git_ingest::classify",
    skip_all,
    fields(
        entries = tracing::field::Empty,
        kept = tracing::field::Empty,
        copied = tracing::field::Empty,
        queries = tracing::field::Empty,
    )
)]
pub(crate) async fn classify(
    plan: &mut DeltaPlan,
    wire: &WirePacks,
    resolved: &ObjectHashMap<ObjectRefs>,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<(), Error> {
    let parents_of = crate::delta_plan::commit_parents(resolved);
    let homes = homes(plan);
    // Walked once and reused: every question below is about the same set
    // of entries.
    let wire_deltas = wire_deltas(wire, plan)?;
    let older_homes = older_homes(&wire_deltas, &homes, state, repo).await?;
    let queries = queries(&wire_deltas, &homes, &older_homes);
    let span = tracing::Span::current();
    span.record("queries", count(queries.len()));
    let ancestry = ancestry::prove(&queries, &parents_of, state, repo).await?;

    let (mut entries, mut kept, mut copied) = (0usize, 0usize, 0usize);
    for (&commit, planned) in &mut plan.packs {
        for entry in planned.iter_mut() {
            entries += 1;
            let Some(at) = wire.entry_of(entry.oid) else {
                continue;
            };
            match delta_base(wire, at)? {
                // No proof needed: an entry with no base can't reach
                // outside its own bytes.
                None => {
                    entry.base = None;
                    entry.source = EntrySource::Wire(at);
                    copied += 1;
                }
                Some(base) => {
                    if home_of(base, &homes, &older_homes).any(|home| {
                        ancestry.holds(Query {
                            target: commit,
                            home,
                        })
                    }) {
                        entry.base = Some(base);
                        entry.source = EntrySource::Wire(at);
                        kept += 1;
                    }
                }
            }
        }
        dependency_order(planned);
    }

    // A commit's entry is always whole, so a client that deltified one
    // leaves nothing to keep.
    plan.commits = parents_of
        .keys()
        .map(|&commit| {
            let source = match wire.entry_of(commit) {
                Some(at) if delta_base(wire, at).is_ok_and(|base| base.is_none()) => {
                    EntrySource::Wire(at)
                }
                _ => EntrySource::Encoded,
            };
            (commit, source)
        })
        .collect();

    span.record("entries", count(entries));
    span.record("kept", count(kept));
    span.record("copied", count(copied));
    Ok(())
}

/// What the client encoded this entry against, or `None` if it sent the
/// object itself.
fn delta_base(wire: &WirePacks, at: WireRef) -> Result<Option<ObjectId>, Error> {
    match wire.entry(at)?.base {
        EntryBase::Whole(_) => Ok(None),
        EntryBase::InPack(base) => wire
            .oid(WireRef {
                pack: at.pack,
                at: base,
            })
            .map(Some),
        EntryBase::Ref(base) => Ok(Some(base)),
    }
}

/// Every planned entry the client itself sent as a delta: the commit whose
/// pack will hold it, and the version it was encoded against.
fn wire_deltas(wire: &WirePacks, plan: &DeltaPlan) -> Result<Vec<(ObjectId, ObjectId)>, Error> {
    let mut deltas = Vec::new();
    for (&commit, planned) in &plan.packs {
        for entry in planned {
            // Anything this push never received was encoded by nobody.
            let Some(at) = wire.entry_of(entry.oid) else {
                continue;
            };
            deltas.extend(delta_base(wire, at)?.map(|base| (commit, base)));
        }
    }
    Ok(deltas)
}

/// Which commit's pack already holds each base this push didn't store
/// itself.
///
/// Its introducing pack, which every point read of it resolves through.
async fn older_homes(
    wire_deltas: &[(ObjectId, ObjectId)],
    homes: &ObjectHashMap<Vec<ObjectId>>,
    state: &Storage,
    repo: &RepoMetadata,
) -> Result<ObjectHashMap<ObjectId>, Error> {
    let older: ObjectHashSet = wire_deltas
        .iter()
        .map(|&(_, base)| base)
        .filter(|base| !homes.contains_key(base))
        .collect();
    if older.is_empty() {
        return Ok(ObjectHashMap::default());
    }

    let oids: Vec<ObjectId> = older.into_iter().collect();
    let metas = enroute_git_retrieve::metas(state, repo.id, &oids).await?;
    Ok(metas
        .iter()
        .filter_map(|(&oid, meta)| meta.location.as_ref().map(|loc| (oid, loc.image.pack_sha)))
        .collect())
}

/// Every proof the plan needs, deduplicated.
fn queries(
    wire_deltas: &[(ObjectId, ObjectId)],
    homes: &ObjectHashMap<Vec<ObjectId>>,
    older_homes: &ObjectHashMap<ObjectId>,
) -> Vec<Query> {
    let mut seen: std::collections::HashSet<Query> = std::collections::HashSet::new();
    let mut queries = Vec::new();
    for &(commit, base) in wire_deltas {
        let asked = home_of(base, homes, older_homes)
            .map(|home| Query {
                target: commit,
                home,
            })
            .filter(|q| q.home != q.target && seen.insert(*q));
        queries.extend(asked);
    }
    queries
}

/// Put every entry after the one it deltas against, where both are in the
/// same pack.
///
/// Reading needs none of this — entries resolve in any order.
/// [`crate::delta_plan::bound_chains`] is what needs a chain in order.
fn dependency_order(entries: &mut Vec<DeltaEntry>) {
    let (trees, blobs): (Vec<DeltaEntry>, Vec<DeltaEntry>) =
        entries.iter().partition(|e| e.kind == Kind::Tree);
    let mut ordered = section_order(&trees);
    ordered.extend(section_order(&blobs));
    *entries = ordered;
}

/// One kind's entries, bases first.
fn section_order(section: &[DeltaEntry]) -> Vec<DeltaEntry> {
    let at: ObjectHashMap<usize> = section
        .iter()
        .enumerate()
        .map(|(at, entry)| (entry.oid, at))
        .collect();
    // Iterative rather than recursive: a section can be one long chain, and a
    // client's may run to git's own depth of 50.
    let mut state = vec![Visit::Fresh; section.len()];
    let mut ordered = Vec::with_capacity(section.len());
    let mut stack: Vec<usize> = Vec::new();

    for start in 0..section.len() {
        if !mark(&mut state, start, Visit::OnWalk) {
            continue;
        }
        stack.push(start);
        while let Some(&top) = stack.last() {
            // An entry already on the walk is a cycle — impossible within the
            // client's own pack, but a mixed plan is not the client's pack.
            // Leaving the edge unsatisfied hands it to `bound_chains`.
            let next = section
                .get(top)
                .and_then(|entry| entry.base)
                .and_then(|base| at.get(&base).copied())
                .filter(|&base| state.get(base).is_some_and(|s| *s == Visit::Fresh));
            if let Some(base) = next {
                mark(&mut state, base, Visit::OnWalk);
                stack.push(base);
                continue;
            }
            stack.pop();
            mark(&mut state, top, Visit::Emitted);
            ordered.extend(section.get(top).copied());
        }
    }
    ordered
}

/// How far the ordering walk has got with one entry.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Visit {
    Fresh,
    OnWalk,
    Emitted,
}

/// Move `at` to `to`, reporting whether it had yet to be visited.
fn mark(state: &mut [Visit], at: usize, to: Visit) -> bool {
    let Some(slot) = state.get_mut(at) else {
        return false;
    };
    let fresh = *slot == Visit::Fresh;
    *slot = to;
    fresh
}

/// Which commits' packs will hold each object this push stores.
///
/// More than one is normal — the same content is stored wherever a path
/// changes to it.
fn homes(plan: &DeltaPlan) -> ObjectHashMap<Vec<ObjectId>> {
    let mut homes: ObjectHashMap<Vec<ObjectId>> = ObjectHashMap::default();
    for (&commit, planned) in &plan.packs {
        for entry in planned {
            homes.entry(entry.oid).or_default().push(commit);
        }
    }
    homes
}

/// Every commit whose pack could supply `base`.
fn home_of<'a>(
    base: ObjectId,
    homes: &'a ObjectHashMap<Vec<ObjectId>>,
    older_homes: &'a ObjectHashMap<ObjectId>,
) -> impl Iterator<Item = ObjectId> + 'a {
    homes
        .get(&base)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .copied()
        .chain(older_homes.get(&base).copied())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use gix_object::Kind;

    use enroute_git_retrieve::RefUpdate;
    use enroute_git_test_support::{
        PackEntry, all_literal_delta, commit, make_pack_of, make_state, tree_with_blob,
    };

    use crate::session::IngestSession;
    use crate::test_helpers::staging_store;

    fn blob(content: &str) -> (gix_hash::ObjectId, Vec<u8>) {
        enroute_git_test_support::blob(content.as_bytes())
    }

    fn tree(name: &str, blob_oid: gix_hash::ObjectId) -> (gix_hash::ObjectId, Vec<u8>) {
        tree_with_blob(name, blob_oid)
    }

    /// Content that deltas well: a long shared prefix and a distinguishing
    /// tail, so the client's encoder has something to find.
    fn versioned(tag: &str) -> String {
        format!("{}{tag}", "x".repeat(400))
    }

    /// The entry header the store ended up holding for `oid`.
    async fn stored_entry(
        state: &enroute_git_retrieve::Storage,
        repo: &enroute_git_retrieve::RepoMetadata,
        oid: gix_hash::ObjectId,
    ) -> enroute_git_store::PackEntryHeader {
        let meta = enroute_git_retrieve::meta(state, repo.id, oid)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{oid} is not indexed"));
        let loc = meta.location.expect("an indexed object has a location");
        let bytes = state
            .store
            .get_segment_slice(
                repo,
                loc.segment.id,
                loc.segment_offset(),
                Some(loc.image.entry_len),
            )
            .await
            .unwrap()
            .expect("the segment the index points at");
        enroute_git_store::decode_pack_entry_header(&bytes)
            .unwrap()
            .0
    }

    /// What the store ended up holding for `oid`, base included.
    async fn stored_base(
        state: &enroute_git_retrieve::Storage,
        repo: &enroute_git_retrieve::RepoMetadata,
        oid: gix_hash::ObjectId,
    ) -> Option<gix_hash::ObjectId> {
        let meta = enroute_git_retrieve::meta(state, repo.id, oid)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{oid} is not indexed"));
        meta.location
            .expect("an indexed object has a location")
            .image
            .base
    }

    /// Push one pack and apply `updates`, asserting every ref landed.
    async fn push(
        state: &enroute_git_retrieve::Storage,
        repo: &enroute_git_retrieve::RepoMetadata,
        entries: &[PackEntry<'_>],
        updates: &[RefUpdate],
    ) {
        let mut session = IngestSession::new(state.clone(), staging_store(), repo.clone());
        session
            .ingest_pack(
                Cursor::new(make_pack_of(entries)),
                None,
                &crate::noop_progress,
            )
            .await
            .unwrap();
        let existing = state.rows.repo(repo.id).refs_for(repo).await.unwrap();
        for outcome in session
            .finalize_and_apply(
                &existing,
                updates,
                &crate::noop_progress,
                &crate::hooks::NoHooks,
            )
            .await
            .unwrap()
        {
            assert!(outcome.result.is_ok(), "{outcome:?}");
        }
    }

    fn update(refname: &str, new_id: gix_hash::ObjectId) -> RefUpdate {
        RefUpdate {
            refname: refname.to_string(),
            old_id: gix_hash::ObjectId::null(gix_hash::Kind::Sha1),
            new_id,
        }
    }

    /// The point of the whole pass: a client base our own planner would never
    /// have picked, but readable from the new commit, is what gets stored.
    #[tokio::test]
    async fn a_client_delta_on_a_readable_base_is_the_one_stored() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let (v1, v1_bytes) = blob(&versioned("1"));
        let (v2, v2_bytes) = blob(&versioned("2"));
        let (v3, v3_bytes) = blob(&versioned("3"));
        let (t1, t1_bytes) = tree("f", v1);
        let (t2, t2_bytes) = tree("f", v2);
        let (t3, t3_bytes) = tree("f", v3);
        let (c1, c1_bytes) = commit(t1, None);
        let (c2, c2_bytes) = commit(t2, Some(c1));
        let (c3, c3_bytes) = commit(t3, Some(c2));

        push(
            &state,
            &repo,
            &[
                PackEntry::whole(Kind::Blob, &v1_bytes),
                PackEntry::whole(Kind::Blob, &v2_bytes),
                // Deltified against v1, where the predecessor scheme would
                // have picked v2 — the version live at c3's parent.
                PackEntry::delta(Kind::Blob, &v3_bytes, (v1, v1_bytes.as_slice())),
                PackEntry::whole(Kind::Tree, &t1_bytes),
                PackEntry::whole(Kind::Tree, &t2_bytes),
                PackEntry::whole(Kind::Tree, &t3_bytes),
                PackEntry::whole(Kind::Commit, &c1_bytes),
                PackEntry::whole(Kind::Commit, &c2_bytes),
                PackEntry::whole(Kind::Commit, &c3_bytes),
            ],
            &[update("refs/heads/main", c3)],
        )
        .await;

        assert_eq!(
            stored_base(&state, &repo, v3).await,
            Some(v1),
            "the client's own base should have been kept"
        );
        let (kind, content) = enroute_git_retrieve::object(&state, &repo, v3)
            .await
            .unwrap();
        assert_eq!(kind, Kind::Blob);
        assert_eq!(content.as_ref(), versioned("3").as_bytes());
    }

    /// A client's base can sit on a branch this commit's readers never
    /// receive.
    ///
    /// Keeping one would store an entry nothing can rebuild.
    #[tokio::test]
    async fn a_client_delta_on_a_base_from_another_branch_is_not_kept() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let (v0, v0_bytes) = blob(&versioned("0"));
        let (v1, v1_bytes) = blob(&versioned("1"));
        let (v2, v2_bytes) = blob(&versioned("2"));
        let (root_tree, root_tree_bytes) = tree("f", v0);
        let (main_tree, main_tree_bytes) = tree("f", v1);
        let (side_tree, side_tree_bytes) = tree("g", v2);
        let (root_commit, root_commit_bytes) = commit(root_tree, None);
        let (main_commit, main_commit_bytes) = commit(main_tree, Some(root_commit));
        let (side_commit, side_commit_bytes) = commit(side_tree, Some(root_commit));

        push(
            &state,
            &repo,
            &[
                PackEntry::whole(Kind::Blob, &v0_bytes),
                PackEntry::whole(Kind::Blob, &v1_bytes),
                // v1 is introduced on main; the side branch cannot reach it.
                PackEntry::delta(Kind::Blob, &v2_bytes, (v1, v1_bytes.as_slice())),
                PackEntry::whole(Kind::Tree, &root_tree_bytes),
                PackEntry::whole(Kind::Tree, &main_tree_bytes),
                PackEntry::whole(Kind::Tree, &side_tree_bytes),
                PackEntry::whole(Kind::Commit, &root_commit_bytes),
                PackEntry::whole(Kind::Commit, &main_commit_bytes),
                PackEntry::whole(Kind::Commit, &side_commit_bytes),
            ],
            &[
                update("refs/heads/main", main_commit),
                update("refs/heads/side", side_commit),
            ],
        )
        .await;

        assert_ne!(
            stored_base(&state, &repo, v2).await,
            Some(v1),
            "a base on the other side of a fork must not be stored"
        );
        let (_, content) = enroute_git_retrieve::object(&state, &repo, v2)
            .await
            .unwrap();
        assert_eq!(content.as_ref(), versioned("2").as_bytes());
    }

    /// The thin-pack case: a second push deltifies against a version the
    /// repo already holds, so nothing in this push says where it lives.
    ///
    /// Proving it means walking back from the old ref tip into store history.
    #[tokio::test]
    async fn a_delta_on_a_base_from_an_earlier_push_is_kept() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let (v1, v1_bytes) = blob(&versioned("1"));
        let (tree1, tree1_bytes) = tree("f", v1);
        let (commit1, commit1_bytes) = commit(tree1, None);
        push(
            &state,
            &repo,
            &[
                PackEntry::whole(Kind::Blob, &v1_bytes),
                PackEntry::whole(Kind::Tree, &tree1_bytes),
                PackEntry::whole(Kind::Commit, &commit1_bytes),
            ],
            &[update("refs/heads/main", commit1)],
        )
        .await;

        // Second push: v1 is not in this pack at all, only named as a base.
        let (v2, v2_bytes) = blob(&versioned("2"));
        let (tree2, tree2_bytes) = tree("f", v2);
        let (commit2, commit2_bytes) = commit(tree2, Some(commit1));
        push(
            &state,
            &repo,
            &[
                PackEntry::delta(Kind::Blob, &v2_bytes, (v1, v1_bytes.as_slice())),
                PackEntry::whole(Kind::Tree, &tree2_bytes),
                PackEntry::whole(Kind::Commit, &commit2_bytes),
            ],
            &[RefUpdate {
                refname: "refs/heads/main".to_string(),
                old_id: commit1,
                new_id: commit2,
            }],
        )
        .await;

        assert_eq!(
            stored_base(&state, &repo, v2).await,
            Some(v1),
            "a base the earlier push stored is readable from this commit"
        );
        let (_, content) = enroute_git_retrieve::object(&state, &repo, v2)
            .await
            .unwrap();
        assert_eq!(content.as_ref(), versioned("2").as_bytes());
    }

    /// A kept delta and a re-encoded one are both git's format, so only
    /// their contents tell them apart.
    ///
    /// Sends a deliberately wasteful delta and checks the same stream comes
    /// back.
    #[tokio::test]
    async fn a_kept_delta_is_the_client_s_own_bytes_and_not_a_re_encoding() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let (v1, v1_bytes) = blob(&versioned("1"));
        let (v2, v2_bytes) = blob(&versioned("2"));
        let (tree1, tree1_bytes) = tree("f", v1);
        let (tree2, tree2_bytes) = tree("f", v2);
        let (commit1, commit1_bytes) = commit(tree1, None);
        let (commit2, commit2_bytes) = commit(tree2, Some(commit1));
        let wasteful = all_literal_delta(&v1_bytes, &v2_bytes);

        push(
            &state,
            &repo,
            &[
                PackEntry::whole(Kind::Blob, &v1_bytes),
                PackEntry::given_delta(Kind::Blob, &v2_bytes, v1, wasteful.clone()),
                PackEntry::whole(Kind::Tree, &tree1_bytes),
                PackEntry::whole(Kind::Tree, &tree2_bytes),
                PackEntry::whole(Kind::Commit, &commit1_bytes),
                PackEntry::whole(Kind::Commit, &commit2_bytes),
            ],
            &[update("refs/heads/main", commit2)],
        )
        .await;

        let stored = stored_entry(&state, &repo, v2).await;
        assert_eq!(stored.base, Some(v1));
        assert_eq!(
            stored.length,
            u64::try_from(wasteful.len()).unwrap(),
            "the stored entry should be the client's own delta stream, byte for byte"
        );
        let (_, content) = enroute_git_retrieve::object(&state, &repo, v2)
            .await
            .unwrap();
        assert_eq!(content.as_ref(), v2_bytes.as_slice());
    }

    /// A `REF_DELTA` entry can name, by oid, the very object it reconstructs.
    ///
    /// Rebuilding one must not chase itself. Driven from an identity delta,
    /// the shortest way to make an entry hash to its own base.
    #[tokio::test]
    async fn an_entry_that_names_itself_as_its_base_does_not_chase_its_own_tail() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let (v1, v1_bytes) = blob(&versioned("1"));
        let (tree1, tree1_bytes) = tree("f", v1);
        let (commit1, commit1_bytes) = commit(tree1, None);
        push(
            &state,
            &repo,
            &[
                PackEntry::whole(Kind::Blob, &v1_bytes),
                PackEntry::whole(Kind::Tree, &tree1_bytes),
                PackEntry::whole(Kind::Commit, &commit1_bytes),
            ],
            &[update("refs/heads/main", commit1)],
        )
        .await;

        // Resends v1 as a delta against v1 itself, which the repo now has.
        let (tree2, tree2_bytes) = tree("g", v1);
        let (commit2, commit2_bytes) = commit(tree2, Some(commit1));
        push(
            &state,
            &repo,
            &[
                PackEntry::given_delta(
                    Kind::Blob,
                    &v1_bytes,
                    v1,
                    all_literal_delta(&v1_bytes, &v1_bytes),
                ),
                PackEntry::whole(Kind::Tree, &tree2_bytes),
                PackEntry::whole(Kind::Commit, &commit2_bytes),
            ],
            &[RefUpdate {
                refname: "refs/heads/main".to_string(),
                old_id: commit1,
                new_id: commit2,
            }],
        )
        .await;

        let (_, content) = enroute_git_retrieve::object(&state, &repo, v1)
            .await
            .unwrap();
        assert_eq!(content.as_ref(), v1_bytes.as_slice());
    }

    /// An object the client sent whole stays whole: its delta window already
    /// saw what ours would have proposed.
    #[tokio::test]
    async fn an_object_sent_whole_is_not_deltified_against_its_predecessor() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;

        let (v1, v1_bytes) = blob(&versioned("1"));
        let (v2, v2_bytes) = blob(&versioned("2"));
        let (t1, t1_bytes) = tree("f", v1);
        let (t2, t2_bytes) = tree("f", v2);
        let (c1, c1_bytes) = commit(t1, None);
        let (c2, c2_bytes) = commit(t2, Some(c1));

        push(
            &state,
            &repo,
            &[
                PackEntry::whole(Kind::Blob, &v1_bytes),
                PackEntry::whole(Kind::Blob, &v2_bytes),
                PackEntry::whole(Kind::Tree, &t1_bytes),
                PackEntry::whole(Kind::Tree, &t2_bytes),
                PackEntry::whole(Kind::Commit, &c1_bytes),
                PackEntry::whole(Kind::Commit, &c2_bytes),
            ],
            &[update("refs/heads/main", c2)],
        )
        .await;

        assert_eq!(
            stored_base(&state, &repo, v2).await,
            None,
            "the client declined to deltify this; so should we"
        );
    }
}
