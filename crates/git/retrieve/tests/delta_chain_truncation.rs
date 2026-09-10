//! What a read does when the index's delta chain is corrupt.
//!
//! `resolve_chain` used to join out for each hop's pack location, and that
//! join was an inner one: a hop naming a missing commit dropped out
//! silently, from the middle, leaving the deltas above it applying to the
//! wrong base. The object index is segments now, so there is no row to drop
//! and no join to drop it in — but a segment can still go missing, and a
//! chain that stops early still must not read back as content. That is what
//! these hold to. An integration test, not a unit one, since it drives a
//! push through `enroute-git-ingest`.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use gix_hash::ObjectId;
use gix_object::Kind;
use object_store::memory::InMemory;

use enroute_git_core::RepoId;
use enroute_git_ingest::{IncomingPack, IngestRequest, IngestWorker as _, LocalIngestWorker};
use enroute_git_journal::Index;
use enroute_git_retrieve::{RefUpdate, RefsMap, RepoMetadata, Storage};
use enroute_git_store::Store;
use enroute_lattice_store::Scope;

/// A state with nothing installed, plus a repo to push into.
async fn state_and_repo() -> Result<(Storage, RepoMetadata)> {
    let state = Storage::in_memory(
        Arc::new(InMemory::new()),
        Arc::new(Store::new(Arc::new(InMemory::new()))),
    );
    let repo = state.rows.create(None).await?;
    Ok((state, repo))
}

/// Fixed length across versions, so a misaligned base still satisfies
/// `apply_delta`'s base-size check.
///
/// Only v0 differs in its leading region: applying a later delta over v0
/// yields wrong bytes rather than tripping any check.
fn versioned(i: u8) -> Vec<u8> {
    let mut content = vec![if i == 0 { b'A' } else { b'B' }; 100];
    content.extend(std::iter::repeat_n(b'x', 1899));
    content.push(b'0' + i);
    content
}

/// What `oid`'s stored entry deltas against, `None` if it holds the object
/// itself — read off the entry header, the way the fetch path does.
async fn stored_base(
    state: &Storage,
    repo: &RepoMetadata,
    oid: ObjectId,
) -> Result<Option<ObjectId>> {
    let meta = enroute_git_retrieve::meta(state, repo.id, oid)
        .await?
        .ok_or_else(|| anyhow!("{oid} is not indexed"))?;
    let loc = meta
        .location
        .ok_or_else(|| anyhow!("{oid} has no location"))?;
    let entry = state
        .store
        .get_segment_slice(
            repo,
            loc.segment.id,
            loc.segment_offset(),
            Some(loc.image.entry_len),
        )
        .await?
        .ok_or_else(|| anyhow!("{oid}'s segment is missing"))?;
    Ok(enroute_git_store::decode_pack_entry_header(&entry)
        .map_err(|e| anyhow!("decode header: {e}"))?
        .0
        .base)
}

/// Pushes `n` versions of one path as a linear history, each blob a delta
/// against the one before, so the stored chain is deep.
///
/// Returns the blob oids in version order.
async fn push_chain(state: &Storage, repo: &RepoMetadata, n: u8) -> Result<Vec<ObjectId>> {
    let mut commits = Vec::new();
    let mut parent: Option<String> = None;
    for i in 0..n {
        let tc = enroute_git_test_support::linear_commit(
            &versioned(i),
            parent.as_deref(),
            u64::from(i),
            "c",
        );
        parent = Some(tc.commit_sha.clone());
        commits.push(tc);
    }
    let tip = commits
        .last()
        .ok_or_else(|| anyhow!("no commits"))?
        .commit_oid;
    let blobs: Vec<ObjectId> = commits.iter().map(|tc| tc.blob_oid).collect();

    let mut entries = Vec::new();
    for (i, tc) in commits.iter().enumerate() {
        let parts = tc.pack_entries();
        let (blob_kind, blob_bytes) = *parts.first().ok_or_else(|| anyhow!("no blob entry"))?;
        match i.checked_sub(1).and_then(|prev| commits.get(prev)) {
            None => entries.push(enroute_git_test_support::PackEntry::whole(
                blob_kind, blob_bytes,
            )),
            Some(prev) => {
                let prev_bytes = prev
                    .pack_entries()
                    .first()
                    .ok_or_else(|| anyhow!("no blob entry"))?
                    .1;
                entries.push(enroute_git_test_support::PackEntry::delta(
                    blob_kind,
                    blob_bytes,
                    (prev.blob_oid, prev_bytes),
                ));
            }
        }
        for (kind, bytes) in parts.into_iter().skip(1) {
            entries.push(enroute_git_test_support::PackEntry::whole(kind, bytes));
        }
    }
    let pack = enroute_git_test_support::make_pack_of(&entries);

    let worker = LocalIngestWorker::new(state.clone(), Arc::new(InMemory::new()));
    let ingested = worker
        .ingest(
            IngestRequest {
                repo: repo.clone(),
                existing: RefsMap::new(),
                updates: vec![RefUpdate {
                    refname: "refs/heads/main".to_string(),
                    old_id: ObjectId::null(gix_hash::Kind::Sha1),
                    new_id: tip,
                }],
            },
            IncomingPack {
                reader: Box::new(std::io::Cursor::new(pack.clone())),
                len_hint: u64::try_from(pack.len()).ok(),
            },
            &enroute_git_ingest::noop_progress,
            &enroute_git_cost::Meter::new(),
        )
        .await?;
    // Only the objects matter here: this test is about how they were stored,
    // and no ref has to move to inspect that.
    if ingested.rejected.is_empty() && ingested.screened.is_empty() {
        Ok(blobs)
    } else {
        Err(anyhow!("push did not store what it brought: {ingested:?}"))
    }
}

/// A blob stored three deep, as `(top, middle, root)` — `top` deltas against
/// `middle`, which deltas against `root`, which is whole.
async fn three_deep(
    state: &Storage,
    repo: &RepoMetadata,
    blobs: &[ObjectId],
) -> Result<(ObjectId, ObjectId, ObjectId)> {
    for &top in blobs.iter().rev() {
        let Some(middle) = stored_base(state, repo, top).await? else {
            continue;
        };
        let Some(root) = stored_base(state, repo, middle).await? else {
            continue;
        };
        if stored_base(state, repo, root).await?.is_none() {
            return Ok((top, middle, root));
        }
    }
    Err(anyhow!("this push stored no three-deep delta chain"))
}

/// Drop every blob segment, as a botched compaction would.
///
/// The identity rows stay, so an object still resolves to a seq; what is
/// gone is the record saying where its bytes are.
async fn lose_the_blob_segments(state: &Storage, repo_id: RepoId) -> Result<()> {
    let blobs = state.ledger.catalog(Index::Blobs);
    let scope = Scope {
        id: repo_id.as_i64(),
        prefix: object_store::path::Path::default(),
    };
    let entries = blobs
        .entries(&scope)
        .await
        .map_err(anyhow::Error::from_boxed)?;
    if entries.is_empty() {
        return Err(anyhow!("expected to drop at least one blob segment"));
    }
    blobs
        .purge(repo_id.as_i64())
        .await
        .map_err(anyhow::Error::from_boxed)?;
    Ok(())
}

/// A healthy chain reads back the bytes that were written.
///
/// The baseline the two truncation tests below are measured against: if this
/// ever fails, they are testing the setup rather than the corruption.
#[tokio::test]
async fn a_healthy_chain_reads_back_what_was_written() {
    let (state, repo) = state_and_repo().await.unwrap();

    let blobs = push_chain(&state, &repo, 8).await.unwrap();
    let (top, middle, root) = three_deep(&state, &repo, &blobs).await.unwrap();
    assert_ne!(middle, root, "the chain needs a distinct middle hop");

    let top_index = blobs.iter().position(|b| *b == top).unwrap();
    let truth = versioned(u8::try_from(top_index).unwrap());

    let chain = state.objects.repo(repo.id).chain(top, 64).await.unwrap();
    assert_eq!(chain.len(), 3, "expected top → middle → root");

    let (kind, content) = enroute_git_retrieve::object(&state, &repo, top)
        .await
        .unwrap();
    assert_eq!(kind, Kind::Blob);
    assert_eq!(content.as_ref(), truth.as_slice());
}

/// A lost segment truncates the chain, and the read fails rather than
/// rebuilding from whatever is left.
///
/// The index cannot dangle a pointer any more, but it can be missing the
/// record a pointer names, which stops the walk on an entry still a delta.
#[tokio::test]
async fn a_lost_segment_is_caught_rather_than_rebuilt_around() {
    let (state, repo) = state_and_repo().await.unwrap();

    let blobs = push_chain(&state, &repo, 8).await.unwrap();
    let (top, _middle, _root) = three_deep(&state, &repo, &blobs).await.unwrap();
    let top_index = blobs.iter().position(|b| *b == top).unwrap();
    let truth = versioned(u8::try_from(top_index).unwrap());

    lose_the_blob_segments(&state, repo.id).await.unwrap();

    match enroute_git_retrieve::object(&state, &repo, top).await {
        Err(_) => {}
        Ok((_, content)) => assert_ne!(
            content.as_ref(),
            truth.as_slice(),
            "a truncated chain must not read back as content"
        ),
    }
}

/// The same truncation through the batched entry point.
///
/// Rebuilding from entries already in hand must check "the innermost hop is
/// whole" explicitly, or that delta decodes as the base as content.
#[tokio::test]
async fn a_batched_read_rejects_a_chain_that_stops_on_a_delta() {
    let (state, repo) = state_and_repo().await.unwrap();

    let blobs = push_chain(&state, &repo, 8).await.unwrap();
    let (top, _middle, _root) = three_deep(&state, &repo, &blobs).await.unwrap();
    let loc = enroute_git_retrieve::meta(&state, repo.id, top)
        .await
        .unwrap()
        .expect("the top of the chain is indexed")
        .location
        .expect("a packed object has a location");

    // Healthy first, so a later failure is the corruption and not the setup.
    let ok = enroute_git_retrieve::known(&state, &repo, &[(top, loc)])
        .await
        .unwrap();
    assert!(ok.contains_key(&top));

    lose_the_blob_segments(&state, repo.id).await.unwrap();

    match enroute_git_retrieve::known(&state, &repo, &[(top, loc)]).await {
        Err(_) => {}
        Ok(found) => assert!(
            !found.contains_key(&top),
            "a truncated chain must not read back as content"
        ),
    }
}
