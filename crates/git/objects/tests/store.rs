//! The object index over rows and catalogs in a map, which needs nothing
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

use enroute_git_core::{ObjectMeta, ObjectSeq, ObjectSeqs, RepoId, SegmentLocation, Ulid, oid};
use enroute_git_journal::{Index, Journal, Ledger as _, MemoryLedger};
use enroute_git_metadata::{Identity, Memory, Rows};
use enroute_git_objects::{Location, Objects, Recorded, Shelf};
use enroute_lattice_core::Policy;

/// `let (store, ledger) = store!();` — an empty index.
///
/// `store!(n)` gives back the bucket too, and graduates past `n` bytes — the
/// one place a tree or a blob segment crosses the medium boundary.
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
        let store = Objects::new(
            ledger.catalog(Index::Trees),
            ledger.catalog(Index::Blobs),
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

fn location(pack_seq: i64, base_seq: Option<u64>) -> Location {
    let seed = pack_seq.unsigned_abs();
    Location {
        pack_seq,
        pack_oid: oid(u8::try_from(pack_seq % 251).unwrap_or(0)),
        segment: SegmentLocation {
            id: Ulid(u128::from(seed) + 1),
            base_offset: seed + 2,
            image_len: seed + 3,
        },
        offset: seed + 4,
        entry_len: seed + 5,
        base_seq,
    }
}

fn recorded(seq: u64, byte: u8, kind: Kind, pack_seq: i64, base: Option<u64>) -> Recorded {
    Recorded {
        seq,
        oid: oid(byte),
        kind,
        locations: vec![location(pack_seq, base)],
        children: ObjectSeqs::default(),
    }
}

/// Look oids up the way the metadata store does: identity first, then the
/// half that answers for the kinds it found.
async fn lookup(store: &Objects, oids: &[ObjectId]) -> Result<Vec<(ObjectId, ObjectMeta)>> {
    let named: Vec<(ObjectId, Identity)> = store
        .ids()
        .repo(repo())
        .identify(oids)
        .await?
        .into_iter()
        .collect();
    store.repo(repo()).lookup(&named).await
}

/// A counter per kind, a tree with two children, those two blobs, and a tag.
///
/// Each kind is allocated from its own space, so the tree and the first blob
/// hold the same number and are two different objects.
async fn seeded(store: &Objects, ledger: &MemoryLedger) -> Result<Vec<Recorded>> {
    store.ids().repo(repo()).create_counters().await?;

    // Identity counts in the width the column holds; a record here counts in
    // the width its bitmaps do.
    let ids = store.ids().repo(repo());
    let tree_seq = u64::try_from(ids.allocate(Kind::Tree, 1).await?)?;
    let blob_seq = u64::try_from(ids.allocate(Kind::Blob, 2).await?)?;
    let tag_seq = u64::try_from(ids.allocate(Kind::Tag, 1).await?)?;

    let mut tree = recorded(tree_seq, 10, Kind::Tree, 1, None);
    tree.children = ObjectSeqs {
        trees: RoaringTreemap::new(),
        blobs: [blob_seq, blob_seq + 1].into_iter().collect(),
    };

    let objects = vec![
        tree,
        recorded(blob_seq, 11, Kind::Blob, 1, None),
        recorded(blob_seq + 1, 12, Kind::Blob, 1, Some(blob_seq)),
        recorded(tag_seq, 13, Kind::Tag, 1, None),
    ];

    // What an object is *called* is the rows layer's, and this store
    // holds only what it points at — so a test that wants both writes both.
    let named: Vec<(ObjectId, Identity)> = objects
        .iter()
        .map(|object| {
            Ok((
                object.oid,
                Identity {
                    seq: i64::try_from(object.seq)?,
                    kind: object.kind,
                },
            ))
        })
        .collect::<Result<_>>()?;

    ids.record(&named).await?;

    let mut journal = Journal::new();
    store.repo(repo()).record(&mut journal, &objects).await?;
    ledger.commit(repo(), &journal).await?;
    Ok(objects)
}

#[tokio::test]
async fn an_object_reads_back_with_its_kind_and_location() {
    let (store, ledger) = store!();
    let objects = seeded(&store, &ledger).await.expect("a seeded index");

    let found = lookup(&store, &objects.iter().map(|o| o.oid).collect::<Vec<_>>())
        .await
        .expect("looking up");
    assert_eq!(found.len(), 4);

    for (oid, meta) in &found {
        let written = objects.iter().find(|o| o.oid == *oid).expect("one of ours");
        assert_eq!(meta.kind, written.kind);
        assert_eq!(
            meta.object_seq,
            ObjectSeq::of(written.kind, written.seq),
            "the seq comes back in the space it was handed out from"
        );
        if written.kind == Kind::Tag {
            assert!(meta.location.is_none(), "a tag is never packed");
        } else {
            let location = meta.location.expect("a packed object has a location");
            assert_eq!(location.image.entry_len, written.locations[0].entry_len);
            assert_eq!(location.segment, written.locations[0].segment);
        }
    }
}

// The delta base is reported as an oid, which means resolving it back out of
// seq space before an answer can be built.
#[tokio::test]
async fn a_delta_reports_the_oid_of_its_base() {
    let (store, ledger) = store!();
    let objects = seeded(&store, &ledger).await.expect("a seeded index");

    let found = lookup(&store, &[objects[2].oid]).await.expect("looking up");
    let (_, meta) = found.first().expect("the delta");
    assert_eq!(
        meta.location.expect("a location").image.base,
        Some(objects[1].oid),
        "the base's oid, not its seq"
    );
}

// The whole point of two shelves: a tree walk reads trees and nothing else.
#[tokio::test]
async fn a_tree_walk_reads_only_trees() {
    let (store, ledger) = store!();
    let objects = seeded(&store, &ledger).await.expect("a seeded index");
    let index = store.repo(repo());

    let children = index
        .children_of(&objects.iter().map(|o| o.seq).collect::<Vec<_>>())
        .await
        .expect("reading children");
    assert_eq!(children.len(), 1, "only the tree has entries");
    let entries = children.get(&objects[0].seq).expect("the tree's entries");
    assert!(entries.trees.is_empty(), "the tree names no tree");
    assert_eq!(
        entries.blobs,
        [objects[1].seq, objects[2].seq]
            .into_iter()
            .collect::<RoaringTreemap>()
    );
}

#[tokio::test]
async fn an_unknown_oid_is_absent_rather_than_an_error() {
    let (store, ledger) = store!();
    seeded(&store, &ledger).await.expect("a seeded index");
    let index = store.repo(repo());

    assert!(
        lookup(&store, &[oid(200)])
            .await
            .expect("a lookup")
            .is_empty()
    );
    assert!(index.packs_of(oid(200)).await.expect("a lookup").is_empty());
    assert!(
        index
            .chain(oid(200), 64)
            .await
            .expect("a lookup")
            .is_empty()
    );
}

// A re-inclusion adds a pack to an object already recorded, and the join is
// what makes the second segment agree with the first.
#[tokio::test]
async fn a_re_inclusion_adds_a_pack_without_replacing_one() {
    let (store, ledger) = store!();
    let objects = seeded(&store, &ledger).await.expect("a seeded index");
    let index = store.repo(repo());

    let blob = &objects[1];
    let mut journal = Journal::new();
    index
        .relocate(&mut journal, &[(blob.seq, Kind::Blob, location(9, None))])
        .await
        .expect("recording a re-inclusion");
    ledger.commit(repo(), &journal).await.expect("committing");

    let packs = index.packs_of(blob.oid).await.expect("its packs");
    assert_eq!(packs.len(), 2, "both packs, not the newer one alone");
    assert_eq!(
        index
            .first_packs(&[blob.oid])
            .await
            .expect("first packs")
            .get(&blob.oid),
        Some(&1),
        "the introducing pack is still the lowest"
    );
}

#[tokio::test]
async fn a_delta_chain_walks_hop_by_hop() {
    let (store, ledger) = store!();
    let objects = seeded(&store, &ledger).await.expect("a seeded index");
    let index = store.repo(repo());

    let chain = index.chain(objects[2].oid, 64).await.expect("a chain");
    assert_eq!(chain.len(), 2, "the delta and the whole entry under it");
    assert!(
        chain.iter().all(|hop| hop.image.base.is_none()),
        "a reader takes each base from the entry's own header"
    );

    let depths = index
        .chain_depths(&[objects[1].oid, objects[2].oid], 64)
        .await
        .expect("depths");
    assert_eq!(depths.get(&objects[1].oid), Some(&0), "a whole entry");
    assert_eq!(depths.get(&objects[2].oid), Some(&1), "one hop deep");
}

#[tokio::test]
async fn seqs_resolve_back_to_oids_and_kinds() {
    let (store, ledger) = store!();
    let objects = seeded(&store, &ledger).await.expect("a seeded index");
    let index = store.repo(repo());

    let numbered: Vec<ObjectSeq> = objects
        .iter()
        .filter_map(|o| ObjectSeq::of(o.kind, o.seq))
        .collect();
    let by_seq = index.locations_by_seq(&numbered).await.expect("by seq");
    assert_eq!(by_seq.len(), 3, "the tag is on no shelf to be read from");

    for (shelf, kind) in [(Shelf::Trees, Kind::Tree), (Shelf::Blobs, Kind::Blob)] {
        let wanted: Vec<u64> = objects
            .iter()
            .filter(|o| o.kind == kind)
            .map(|o| o.seq)
            .collect();
        let oids = index.oids_of(shelf, &wanted).await.expect("oids");
        for object in objects.iter().filter(|o| o.kind == kind) {
            assert_eq!(oids.get(&object.seq), Some(&object.oid));
        }
    }
}

// The whole point of counting each kind apart: the same number is two
// objects, and asking in the wrong space must not answer with the other.
#[tokio::test]
async fn one_number_in_two_spaces_is_two_objects() {
    let (store, ledger) = store!();
    let objects = seeded(&store, &ledger).await.expect("a seeded index");
    let index = store.repo(repo());

    let (tree, blob) = (&objects[0], &objects[1]);
    assert_eq!(tree.seq, blob.seq, "the seeding relies on them colliding");

    assert_eq!(
        index
            .oids_of(Shelf::Trees, &[tree.seq])
            .await
            .expect("the tree space")
            .get(&tree.seq),
        Some(&tree.oid)
    );
    assert_eq!(
        index
            .oids_of(Shelf::Blobs, &[blob.seq])
            .await
            .expect("the blob space")
            .get(&blob.seq),
        Some(&blob.oid)
    );
}

// Identity is another store's, so purging is two calls; what this holds to
// is that neither leaves a row the other would have to explain.
#[tokio::test]
async fn purging_a_repository_leaves_no_rows() {
    let (store, ledger) = store!();
    let objects = seeded(&store, &ledger).await.expect("a seeded index");

    ledger.erase(repo()).await.expect("erasing");

    assert!(
        lookup(&store, &[objects[0].oid])
            .await
            .expect("a lookup")
            .is_empty()
    );
    assert!(
        store
            .ids()
            .repo(repo())
            .allocate(Kind::Blob, 1)
            .await
            .is_err(),
        "and the counter is gone with them"
    );
}

/// Records `count` blobs and the tree naming them, oids from `byte`.
///
/// One push is one segment a shelf, which is what a merge needs several of.
async fn push_objects(
    store: &Objects,
    ledger: &MemoryLedger,
    count: u8,
    byte: u8,
) -> Result<Vec<Recorded>> {
    let ids = store.ids().repo(repo());
    let first = u64::try_from(ids.allocate(Kind::Blob, u64::from(count)).await?)?;
    let mut objects: Vec<Recorded> = (0..count)
        .map(|step| recorded(first + u64::from(step), byte + step, Kind::Blob, 1, None))
        .collect();

    let tree_seq = u64::try_from(ids.allocate(Kind::Tree, 1).await?)?;
    let mut tree = recorded(tree_seq, byte + 100, Kind::Tree, 1, None);
    tree.children = ObjectSeqs {
        trees: RoaringTreemap::new(),
        blobs: (first..first + u64::from(count)).collect(),
    };
    objects.push(tree);

    let named: Vec<(ObjectId, Identity)> = objects
        .iter()
        .map(|object| {
            Ok((
                object.oid,
                Identity {
                    seq: i64::try_from(object.seq)?,
                    kind: object.kind,
                },
            ))
        })
        .collect::<Result<_>>()?;

    ids.record(&named).await?;

    let mut journal = Journal::new();
    store.repo(repo()).record(&mut journal, &objects).await?;
    ledger.commit(repo(), &journal).await?;
    Ok(objects)
}

/// Graduation and compaction are `lattice-store`'s, and its own tests hold
/// them; this is the only place object bytes make that round trip.
#[tokio::test]
async fn compaction_merges_object_segments_and_keeps_every_object() {
    let (store, _bucket, ledger) = store!(0);
    store
        .ids()
        .repo(repo())
        .create_counters()
        .await
        .expect("counters");

    // One segment a push per shelf, and the policy's fanout is eight — so
    // fewer pushes than this merges nothing and proves nothing.
    let mut pushed: Vec<Recorded> = Vec::new();
    for round in 0..9_u8 {
        let batch = push_objects(&store, &ledger, 3, 40 + round * 3)
            .await
            .expect("a push");
        pushed.extend(batch);
    }

    let report = store.repo(repo()).compact().await.expect("a pass");
    assert!(report.merged >= 2, "at least one merge happened");

    // Every object of every push still answers, from whatever holds it now.
    let oids: Vec<ObjectId> = pushed.iter().map(|object| object.oid).collect();
    let found = lookup(&store, &oids).await.expect("a lookup");
    assert_eq!(found.len(), pushed.len(), "every object survived the merge");

    // Only the tree shelf encodes entries, so a merge that dropped them
    // would still keep every blob.
    let tree = pushed
        .iter()
        .find(|object| object.kind == Kind::Tree)
        .expect("the first push's tree");
    let children = store
        .repo(repo())
        .children_of(&[tree.seq])
        .await
        .expect("reading the merged tree back out of the bucket");
    assert_eq!(children.get(&tree.seq), Some(&tree.children));

    // And a second pass over merged segments is not an error.
    store.repo(repo()).compact().await.expect("a second pass");
}
