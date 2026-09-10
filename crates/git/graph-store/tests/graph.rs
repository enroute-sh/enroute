//! The commit graph over rows and catalogs in a map, which needs nothing
//! installed.
//!
//! What only a database can answer for is asserted where that database is:
//! the identity copy in `enroute-git-metadata`, and the journal transaction
//! in `enroute-git-journal`.

use std::sync::Arc;

use anyhow::Result;

use gix_hash::ObjectId;
use gix_object::Kind;
use object_store::memory::InMemory;
use roaring::RoaringTreemap;

use enroute_git_core::{RepoId, SegmentLocation, Ulid, oid};
use enroute_git_graph_store::{BandPolicy, CommitGraph, Pack, Recorded, WalkSeeds};
use enroute_git_journal::{Index, Journal, Ledger as _, MemoryLedger};
use enroute_git_metadata::{Identity, Memory, Rows};
use enroute_lattice_core::Policy;

/// `let (store, ledger) = store!();` — an empty graph.
///
/// `store!(n)` gives back the bucket too, and graduates past `n` bytes — the
/// one place a commit segment crosses the medium boundary.
macro_rules! store {
    () => {{
        let (store, _bucket, ledger) = store!(u64::MAX);
        (store, ledger)
    }};
    ($bytes:expr) => {{
        let policy = Policy {
            fanout: 8,
            max_inputs: 16,
            max_input_bytes: 1 << 24,
            graduation_bytes: $bytes,
            inline_ceiling: 1 << 20,
        };
        let bucket = Arc::new(InMemory::new());
        let memory = Arc::new(Memory::new());
        let ids = Rows::over_memory(Arc::clone(&memory));
        let ledger = MemoryLedger::in_memory(memory);
        let store = CommitGraph::new(
            ledger.catalog(Index::CommitGraph),
            ledger.catalog(Index::CommitPacks),
            ids,
            bucket.clone(),
            policy,
        );
        (store, bucket, ledger)
    }};
}

fn repo() -> RepoId {
    RepoId::new(1)
}

fn pack(seed: u64) -> Pack {
    Pack {
        entry_len: seed + 1,
        blob_offset: seed + 2,
        segment: SegmentLocation {
            id: Ulid(u128::from(seed) + 3),
            base_offset: seed + 4,
            image_len: seed + 5,
        },
        trees: RoaringTreemap::from_iter([seed]),
        blobs: RoaringTreemap::new(),
    }
}

/// `count` commits in a line, each the child of the last, seqs from `first`.
fn chain(first: i64, count: u8, byte: u8) -> Vec<Recorded> {
    (0..count)
        .map(|step| {
            let seq = first + i64::from(step);
            Recorded {
                seq,
                oid: oid(byte + step),
                parents: if step == 0 { Vec::new() } else { vec![seq - 1] },
                root_tree_seq: seq.unsigned_abs() + 1000,
                committer_date: 1_700_000_000 + i64::from(step),
                pack: pack(seq.unsigned_abs()),
            }
        })
        .collect()
}

/// Records `commits` and their identities in a transaction of their own.
///
/// What a commit is called is another store's; this one holds only what it
/// points at, so a test wanting both writes both.
async fn record(store: &CommitGraph, ledger: &MemoryLedger, commits: &[Recorded]) -> Result<()> {
    let mine: Vec<i64> = commits.iter().map(|commit| commit.seq).collect();
    let parents: Vec<i64> = commits
        .iter()
        .flat_map(|commit| commit.parents.iter().copied())
        .collect();
    let ranks = store.repo(repo()).ranks_of(&mine, &parents).await?;
    let named: Vec<(ObjectId, Identity)> = commits
        .iter()
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

    store.ids().repo(repo()).record(&named).await?;

    let mut journal = Journal::new();
    store
        .repo(repo())
        .record(&mut journal, commits, &ranks)
        .await?;
    ledger.commit(repo(), &journal).await?;
    Ok(())
}

/// A counter, and six commits in a line.
async fn seeded(store: &CommitGraph, ledger: &MemoryLedger) -> Result<Vec<Recorded>> {
    store.ids().repo(repo()).create_counters().await?;

    let commits = chain(
        store.ids().repo(repo()).allocate(Kind::Commit, 6).await?,
        6,
        1,
    );
    record(store, ledger, &commits).await?;
    Ok(commits)
}

#[tokio::test]
async fn a_commit_reads_back_by_oid_and_by_seq() {
    let (store, ledger) = store!();
    let commits = seeded(&store, &ledger).await.expect("a seeded graph");
    let graph = store.repo(repo());

    let oids: Vec<ObjectId> = commits.iter().map(|commit| commit.oid).collect();
    let seqs = graph.seqs_of(&oids).await.expect("resolving oids");
    assert_eq!(seqs.len(), commits.len());
    for commit in &commits {
        assert_eq!(seqs.get(&commit.oid), Some(&commit.seq));
    }

    let back = graph
        .oids_of(&commits.iter().map(|c| c.seq).collect::<Vec<_>>())
        .await
        .expect("resolving seqs");
    for commit in &commits {
        assert_eq!(back.get(&commit.seq), Some(&commit.oid));
    }

    assert!(graph.contains(commits[0].oid).await.expect("a lookup"));
    assert!(!graph.contains(oid(200)).await.expect("a lookup"));
}

// A commit the graph does not hold has no ancestry, rather than an error.
#[tokio::test]
async fn an_unknown_oid_resolves_to_nothing() {
    let (store, ledger) = store!();
    seeded(&store, &ledger).await.expect("a seeded graph");
    let graph = store.repo(repo());

    assert!(
        !graph
            .is_ancestor(oid(200), oid(201))
            .await
            .expect("a lookup")
    );
    assert!(
        graph
            .merge_bases(oid(200), oid(201), 1000)
            .await
            .expect("a lookup")
            .bases
            .is_empty()
    );
    assert!(
        graph
            .first_parent_page(oid(200), 10)
            .await
            .expect("a lookup")
            .is_empty()
    );
}

#[tokio::test]
async fn ancestry_runs_off_the_index() {
    let (store, ledger) = store!();
    let commits = seeded(&store, &ledger).await.expect("a seeded graph");
    let graph = store.repo(repo());

    let root = commits[0].oid;
    let tip = commits[5].oid;
    assert!(graph.is_ancestor(root, tip).await.expect("a walk"));
    assert!(!graph.is_ancestor(tip, root).await.expect("a walk"));
    assert!(graph.is_ancestor(tip, tip).await.expect("a walk"));

    let bases = graph
        .merge_bases(tip, commits[3].oid, 1000)
        .await
        .expect("a walk");
    assert_eq!(
        bases.bases,
        vec![commits[3].oid],
        "one is an ancestor of the other"
    );
}

#[tokio::test]
async fn a_history_page_follows_first_parents() {
    let (store, ledger) = store!();
    let commits = seeded(&store, &ledger).await.expect("a seeded graph");
    let graph = store.repo(repo());

    let page = graph
        .first_parent_page(commits[5].oid, 3)
        .await
        .expect("a page");
    assert_eq!(
        page,
        vec![commits[5].oid, commits[4].oid, commits[3].oid],
        "newest first, and no more than asked for"
    );

    let whole = graph
        .first_parent_page(commits[5].oid, 100)
        .await
        .expect("a page");
    assert_eq!(whole.len(), 6, "a page longer than the history stops at it");
}

#[tokio::test]
async fn ancestors_among_reports_only_the_candidates_it_reached() {
    let (store, ledger) = store!();
    let commits = seeded(&store, &ledger).await.expect("a seeded graph");
    let graph = store.repo(repo());

    let found = graph
        .ancestors_among(
            commits[3].oid,
            &[commits[1].oid, commits[4].oid, oid(200)],
            1000,
        )
        .await
        .expect("a walk");
    assert!(found.contains(&commits[1].oid), "below it");
    assert!(!found.contains(&commits[4].oid), "above it");
    assert!(!found.contains(&oid(200)), "not in the graph at all");
}

// The whole point: a fetch's paint walk, answered from segments.
#[tokio::test]
async fn a_paint_walk_answers_with_oids_and_pack_facts() {
    let (store, ledger) = store!();
    let commits = seeded(&store, &ledger).await.expect("a seeded graph");
    let graph = store.repo(repo());
    let seeds = WalkSeeds {
        wants: vec![commits[5].seq],
        haves: vec![commits[2].seq],
        shallow: vec![],
    };

    let needed = graph
        .needed_commits(&seeds, BandPolicy::production())
        .await
        .expect("a walk");

    let names: Vec<ObjectId> = needed.commits.iter().map(|one| one.oid).collect();
    assert_eq!(
        names,
        vec![commits[3].oid, commits[4].oid, commits[5].oid],
        "everything above the have, want-only"
    );
    assert_eq!(
        needed.commits[0].blob_offset, commits[3].pack.blob_offset,
        "the pack facts come back with it"
    );
    assert_eq!(needed.commits[0].segment, commits[3].pack.segment);
    assert_eq!(
        needed.trees,
        commits[3..].iter().fold(RoaringTreemap::new(), |all, one| {
            all | one.pack.trees.clone()
        }),
        "and the bitmaps are unioned across the walk"
    );
}

// A band is a schedule and not an answer, so the narrowest one has to reach
// what a band wide enough for the whole graph reaches.
#[tokio::test]
async fn narrow_bands_walk_to_the_same_needed_set() {
    let (store, ledger) = store!();
    let commits = seeded(&store, &ledger).await.expect("a seeded graph");
    let graph = store.repo(repo());
    let seeds = WalkSeeds {
        wants: vec![commits[5].seq],
        haves: vec![commits[1].seq],
        shallow: vec![],
    };

    let narrow = graph
        .needed_commits(&seeds, BandPolicy::new(1))
        .await
        .expect("a walk");
    let wide = graph
        .needed_commits(&seeds, BandPolicy::production())
        .await
        .expect("a walk");

    let names = |needed: &enroute_git_graph_store::NeededCommits| -> Vec<ObjectId> {
        let mut all: Vec<ObjectId> = needed.commits.iter().map(|one| one.oid).collect();
        all.sort_unstable();
        all
    };
    assert_eq!(names(&narrow), names(&wide));
    assert_eq!(narrow.trees, wide.trees);
}

// A later push builds on a tip written by an earlier one, so the generation
// has to come out of the segment the first push left behind.
#[tokio::test]
async fn a_second_push_ranks_against_the_first() {
    let (store, ledger) = store!();
    let first = seeded(&store, &ledger).await.expect("a seeded graph");
    let graph = store.repo(repo());

    let next = store
        .ids()
        .repo(repo())
        .allocate(Kind::Commit, 2)
        .await
        .expect("two seqs");
    let more = vec![
        Recorded {
            seq: next,
            oid: oid(50),
            parents: vec![first[5].seq],
            root_tree_seq: 2000,
            committer_date: 1_700_000_100,
            pack: pack(50),
        },
        Recorded {
            seq: next + 1,
            oid: oid(51),
            parents: vec![next],
            root_tree_seq: 2001,
            committer_date: 1_700_000_200,
            pack: pack(51),
        },
    ];
    record(&store, &ledger, &more).await.expect("a second push");

    assert!(
        graph
            .is_ancestor(first[0].oid, oid(51))
            .await
            .expect("a walk"),
        "the two pushes compose into one history"
    );

    let page = graph.first_parent_page(oid(51), 100).await.expect("a page");
    assert_eq!(
        page.len(),
        8,
        "six from the first push and two from the second"
    );
}

// A parent nothing has recorded is a bug, and has to be loud rather than a
// commit silently ranked below its own history.
#[tokio::test]
async fn a_parent_that_was_never_recorded_is_refused() {
    let (store, ledger) = store!();
    seeded(&store, &ledger).await.expect("a seeded graph");

    let next = store
        .ids()
        .repo(repo())
        .allocate(Kind::Commit, 1)
        .await
        .expect("one seq");

    let refused = store.repo(repo()).ranks_of(&[next], &[9_999]).await;
    assert!(refused.is_err(), "{refused:?}");
}

// A deleted repository leaves nothing behind in the rows, and identity being
// another store's makes that two calls rather than one. Its bucket objects
// outlive it, as a failed push's orphan already does.
#[tokio::test]
async fn purging_a_repository_leaves_no_rows() {
    let (store, ledger) = store!();
    let commits = seeded(&store, &ledger).await.expect("a seeded graph");

    ledger.erase(repo()).await.expect("erasing");

    let graph = store.repo(repo());
    assert!(!graph.contains(commits[0].oid).await.expect("a lookup"));
    assert!(
        graph
            .seqs_of(&[commits[0].oid])
            .await
            .expect("a lookup")
            .is_empty()
    );
    assert!(
        store
            .ids()
            .repo(repo())
            .allocate(Kind::Commit, 1)
            .await
            .is_err(),
        "and the counter is gone with them"
    );
}

/// Graduation, compaction and the sweep are `lattice-store`'s, and its own
/// tests hold them; this is the only place commit bytes make that round trip.
#[tokio::test]
async fn compaction_merges_commit_segments_and_keeps_every_commit() {
    let (store, _bucket, ledger) = store!(0);

    // One segment a push per shelf, and the policy's fanout is eight — so
    // fewer pushes than this merges nothing and proves nothing.
    let mut pushed: Vec<Recorded> = Vec::new();
    for round in 0..9_u8 {
        let batch = chain(i64::from(round) * 3, 3, 40 + round * 3);
        record(&store, &ledger, &batch).await.expect("a push");
        pushed.extend(batch);
    }

    let report = store.repo(repo()).compact().await.expect("a pass");
    assert!(report.merged >= 2, "at least one merge happened");

    // Every commit of every push still answers, from whatever holds it now.
    let graph = store.repo(repo());
    for commit in &pushed {
        let seqs = graph.seqs_of(&[commit.oid]).await.expect("a lookup");
        assert_eq!(
            seqs.get(&commit.oid),
            Some(&commit.seq),
            "commit {} survived the merge",
            commit.seq
        );
    }

    // And a pass over what a pass already merged is not an error.
    store.repo(repo()).compact().await.expect("a second pass");
}
