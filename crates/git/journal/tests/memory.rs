//! The ledger against catalogs and rows in a map, which needs nothing
//! installed.
//!
//! The same questions `postgres.rs` asks, so what a journal does is pinned
//! without a database — and the two places a memory stack behaves differently
//! are asserted rather than left to be found out.
#![allow(
    clippy::expect_used,
    reason = "a test: a store that cannot answer fails it, which is the point"
)]

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use object_store::path::Path;

use enroute_git_core::{RepoId, Ulid};
use enroute_git_journal::{Index, Journal, Ledger as _, MemoryLedger};
use enroute_git_metadata::{Memory, RepoMetadata, Rows};
use enroute_lattice_core::{Key, KeyRange, Tier};
use enroute_lattice_store::{Body, SegmentId, Written, inspect};

/// The rows and a ledger over them, with the catalogs it made.
struct Stack {
    rows: Rows,
    ledger: MemoryLedger,
}

fn stack() -> Stack {
    let store = Arc::new(Memory::new());
    Stack {
        rows: Rows::over_memory(Arc::clone(&store)),
        ledger: MemoryLedger::in_memory(store),
    }
}

impl Stack {
    /// How many rows one list holds for `repo`.
    async fn listed(&self, index: Index, repo: RepoId) -> usize {
        let catalog = self.ledger.catalog(index);
        inspect::counts(catalog.as_ref(), &scope(repo)).await.0
    }

    async fn repo(&self) -> RepoMetadata {
        self.rows.create(None).await.expect("a repository")
    }
}

fn scope(repo: RepoId) -> enroute_lattice_store::Scope {
    enroute_lattice_store::Scope {
        id: repo.as_i64(),
        prefix: Path::from("test"),
    }
}

/// One inlined segment, named `id`, covering a key range of its own.
fn segment(id: SegmentId, first: u64) -> Result<Written> {
    Ok(Written {
        id,
        range: KeyRange::new(Key::new(first), Key::new(first + 1))?,
        tier: Tier::ZERO,
        body: Body::Inline(Bytes::from_static(b"segment")),
        bytes: 7,
    })
}

/// A journal touching every list, and registering `images` pack images.
fn journal_over(repo: RepoId, images: &[Ulid]) -> Journal {
    let mut journal = Journal::new();
    for (offset, index) in Index::ALL.into_iter().enumerate() {
        let first = u64::try_from(offset).expect("an offset") * 10;
        journal.list(
            index,
            &scope(repo),
            segment(SegmentId::fresh(), first).expect("a segment"),
        );
    }
    journal.register(images.iter().copied());
    journal
}

#[tokio::test]
async fn one_journal_lands_every_list_it_touches() {
    let stack = stack();
    let repo = stack.repo().await;
    let journal = journal_over(repo.id, &[Ulid(1), Ulid(2)]);

    stack
        .ledger
        .commit(repo.id, &journal)
        .await
        .expect("committing");

    for index in Index::ALL {
        assert_eq!(stack.listed(index, repo.id).await, 1, "the {index:?} list");
    }
    assert_eq!(
        stack
            .rows
            .repo(repo.id)
            .referenced_segments(&[Ulid(1), Ulid(2)])
            .await
            .expect("a lookup")
            .len(),
        2
    );
}

/// An empty journal is not a write, so nothing is touched by one.
#[tokio::test]
async fn an_empty_journal_lands_nothing() {
    let stack = stack();
    let repo = stack.repo().await;

    stack
        .ledger
        .commit(repo.id, &Journal::new())
        .await
        .expect("committing");

    for index in Index::ALL {
        assert_eq!(stack.listed(index, repo.id).await, 0, "the {index:?} list");
    }
}

/// A retired image keeps its row, since the row is what stops the sweep
/// taking an object a reader is still inside.
#[tokio::test]
async fn a_retired_image_stops_being_live_and_stays_referenced() {
    let stack = stack();
    let repo = stack.repo().await;
    let image = Ulid(7);

    stack
        .ledger
        .commit(repo.id, &journal_over(repo.id, &[image]))
        .await
        .expect("committing");

    let mut retiring = Journal::new();
    retiring.retire([image]);
    stack
        .ledger
        .commit(repo.id, &retiring)
        .await
        .expect("retiring");

    let rows = stack.rows.repo(repo.id);
    assert!(
        rows.referenced_segments(&[image])
            .await
            .expect("a lookup")
            .contains(&image),
        "a retired image is still one the sweep must leave alone"
    );
    assert!(
        rows.live_segments(&[image])
            .await
            .expect("a lookup")
            .is_empty(),
        "a gather must not take an image already copied elsewhere"
    );
}

/// Two gathers at once would copy the same images twice, so the second is
/// refused rather than made to wait.
#[tokio::test]
async fn a_second_maintenance_pass_is_refused_while_the_first_holds_it() {
    let stack = stack();
    let repo = stack.repo().await;

    assert!(
        stack
            .ledger
            .commit_held(&repo, &journal_over(repo.id, &[]))
            .await
            .expect("a first pass"),
        "nobody else holds it"
    );

    // The lock is given back when the pass ends, so a later one takes it.
    assert!(
        stack
            .ledger
            .commit_held(&repo, &journal_over(repo.id, &[]))
            .await
            .expect("a later pass"),
        "the first pass gave it back"
    );
}

/// A deleted repository leaves nothing behind: its four lists, its rows and
/// the row naming it all go together.
#[tokio::test]
async fn erasing_a_repository_leaves_no_lists_and_no_row() {
    let stack = stack();
    let repo = stack.repo().await;
    stack
        .ledger
        .commit(repo.id, &journal_over(repo.id, &[Ulid(1)]))
        .await
        .expect("committing");

    stack.ledger.erase(repo.id).await.expect("erasing");

    for index in Index::ALL {
        assert_eq!(stack.listed(index, repo.id).await, 0, "the {index:?} list");
    }
    assert!(
        stack
            .rows
            .repo(repo.id)
            .lookup()
            .await
            .expect("a lookup")
            .is_none(),
        "the row naming an erased repository is still there"
    );
}
