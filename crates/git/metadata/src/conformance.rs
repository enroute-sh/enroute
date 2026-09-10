//! What the engine asks of a metadata store, as cases over whichever one it
//! is handed.
//!
//! In the library rather than in a test file because two crates run them: the
//! memory store here, and the one that speaks to a database in the crate that
//! owns the database — so the two cannot drift apart unnoticed.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::print_stdout,
    reason = "cases, not helpers of the library: a store that cannot answer \
              fails the test, and the harness shows what it printed first"
)]

use anyhow::Result;
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::oid;

use crate::{Identity, Raced, RefUpdate, RefUpdateRejection, RepoMetadata, Rows};

/// Runs every case against `rows`, in order.
///
/// One entry point rather than one per case, so a store cannot be held to
/// some of them: whoever runs these runs all of them.
pub async fn check(rows: &Rows) {
    println!("case: a_created_repository_reads_back");
    a_created_repository_reads_back(rows).await;
    println!("case: a_fresh_repository_offers_head_alone");
    a_fresh_repository_offers_head_alone(rows).await;
    println!("case: each_kind_is_counted_in_a_space_of_its_own");
    each_kind_is_counted_in_a_space_of_its_own(rows).await;
    println!("case: a_numbered_object_reads_back_both_ways");
    a_numbered_object_reads_back_both_ways(rows).await;
    println!("case: a_second_writer_of_one_oid_is_told_it_raced");
    a_second_writer_of_one_oid_is_told_it_raced(rows).await;
    println!("case: a_batch_that_loses_the_race_leaves_nothing_behind");
    a_batch_that_loses_the_race_leaves_nothing_behind(rows).await;
    println!("case: a_branch_is_guarded_by_its_current_value");
    a_branch_is_guarded_by_its_current_value(rows).await;
    println!("case: a_tag_is_guarded_by_its_current_value");
    a_tag_is_guarded_by_its_current_value(rows).await;
    println!("case: a_ref_outside_refs_heads_is_not_asked_for_a_commit");
    a_ref_outside_refs_heads_is_not_asked_for_a_commit(rows).await;
    println!("case: a_null_new_id_deletes_the_ref");
    a_null_new_id_deletes_the_ref(rows).await;
    println!("case: a_refname_git_would_refuse_is_refused");
    a_refname_git_would_refuse_is_refused(rows).await;
    println!("case: one_refusal_leaves_the_rest_of_a_batch_alone");
    one_refusal_leaves_the_rest_of_a_batch_alone(rows).await;
    println!("case: only_the_refs_asked_for_come_back");
    only_the_refs_asked_for_come_back(rows).await;
    println!("case: two_reasons_to_refuse_one_write_are_reported_in_one_order");
    two_reasons_to_refuse_one_write_are_reported_in_one_order(rows).await;
    println!("case: a_deleted_repository_stops_being_listed");
    a_deleted_repository_stops_being_listed(rows).await;
    println!("case: a_grace_window_of_nothing_is_still_a_window");
    a_grace_window_of_nothing_is_still_a_window(rows).await;
    println!("case: a_summary_reports_the_ids_it_still_has_in_order");
    a_summary_reports_the_ids_it_still_has_in_order(rows).await;
}

/// No ref: as an `old_id` it guards on absence, as a `new_id` it deletes.
fn null() -> ObjectId {
    ObjectId::null(gix_hash::Kind::Sha1)
}

/// One update of `refname`, from `old` to `new`.
fn update(refname: &str, old: ObjectId, new: ObjectId) -> RefUpdate {
    RefUpdate {
        refname: refname.to_owned(),
        old_id: old,
        new_id: new,
    }
}

/// A repository with counters, as `create` leaves one.
async fn repo(rows: &Rows) -> Result<RepoMetadata> {
    rows.create(None).await
}

/// `oid`, numbered as a commit, so a branch may point at it.
async fn commit(rows: &Rows, repo: &RepoMetadata, at: u8) -> Result<ObjectId> {
    let named = oid(at);
    let seq = rows.repo(repo.id).allocate(Kind::Commit, 1).await?;
    rows.repo(repo.id)
        .record(&[(
            named,
            Identity {
                seq,
                kind: Kind::Commit,
            },
        )])
        .await?;
    Ok(named)
}

/// A repository exists once created, with the default branch it was given.
async fn a_created_repository_reads_back(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");

    let found = rows
        .repo(made.id)
        .lookup()
        .await
        .expect("a lookup")
        .expect("the repository");
    assert_eq!(found.id, made.id);
    assert_eq!(found.default_branch, "refs/heads/main");
    assert!(
        rows.all()
            .await
            .expect("a listing")
            .iter()
            .any(|one| one.id == made.id)
    );

    let named = rows
        .create(Some("refs/heads/trunk"))
        .await
        .expect("a repository");
    assert_eq!(named.default_branch, "refs/heads/trunk");
    assert_ne!(named.id, made.id);
    assert_ne!(
        named.storage_key, made.storage_key,
        "a name is not what keeps two repositories apart"
    );
}

/// Nothing points anywhere until a push, and `HEAD` is made rather than kept.
async fn a_fresh_repository_offers_head_alone(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");

    let refs = rows.repo(made.id).refs_for(&made).await.expect("its refs");
    assert_eq!(refs.get("HEAD"), Some(&"ref: refs/heads/main".to_owned()));
    assert_eq!(refs.len(), 1);
}

/// A counter hands out dense ranges, and each kind counts on its own —
/// the same number under two kinds is two different objects.
async fn each_kind_is_counted_in_a_space_of_its_own(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let ids = rows.repo(made.id);

    assert_eq!(ids.allocate(Kind::Commit, 4).await.expect("a range"), 0);
    assert_eq!(
        ids.allocate(Kind::Commit, 2).await.expect("a range"),
        4,
        "a counter hands out dense ranges"
    );
    assert_eq!(
        ids.allocate(Kind::Tree, 1).await.expect("a range"),
        0,
        "a space of its own"
    );
}

/// An object numbered once answers by oid and by seq alike.
async fn a_numbered_object_reads_back_both_ways(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let named = commit(rows, &made, 7).await.expect("a commit");
    let ids = rows.repo(made.id);

    let held = ids.identify(&[named]).await.expect("what it is");
    assert_eq!(held.get(&named).map(|one| one.kind), Some(Kind::Commit));

    let seq = held.get(&named).expect("what it is called").seq;
    assert_eq!(
        ids.oids_of(Kind::Commit, &[seq])
            .await
            .expect("a lookup")
            .get(&seq),
        Some(&named)
    );
    assert!(
        ids.oids_of(Kind::Tree, &[seq])
            .await
            .expect("a lookup")
            .is_empty(),
        "a seq means nothing without the kind beside it"
    );
}

/// The race a push retries against, which every store must report as one
/// rather than as whatever its driver calls a duplicate.
async fn a_second_writer_of_one_oid_is_told_it_raced(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    commit(rows, &made, 9).await.expect("a commit");

    let err = commit(rows, &made, 9)
        .await
        .expect_err("a second numbering of one oid");

    assert!(
        err.downcast_ref::<Raced>().is_some(),
        "a lost race reads as a failed push: {err:?}"
    );
}

/// Nothing lands when one of a batch is already numbered, since the copy
/// this stands in for is one statement.
async fn a_batch_that_loses_the_race_leaves_nothing_behind(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let held = commit(rows, &made, 3).await.expect("a commit");
    let ids = rows.repo(made.id);

    let fresh = oid(4);
    ids.record(&[
        (
            fresh,
            Identity {
                seq: 40,
                kind: Kind::Commit,
            },
        ),
        (
            held,
            Identity {
                seq: 41,
                kind: Kind::Commit,
            },
        ),
    ])
    .await
    .expect_err("a batch naming one already numbered");

    assert!(
        ids.identify(&[fresh]).await.expect("a lookup").is_empty(),
        "half a batch landed"
    );
}

/// A branch may only point at a commit the repository numbers, and the
/// CAS on it is what makes a non-fast-forward a refusal.
async fn a_branch_is_guarded_by_its_current_value(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let first = commit(rows, &made, 1).await.expect("a commit");
    let second = commit(rows, &made, 2).await.expect("a commit");
    let ids = rows.repo(made.id);

    let apply = async |old, new| {
        ids.update_refs(&[update("refs/heads/main", old, new)])
            .await
            .expect("an update")[0]
            .result
    };

    assert_eq!(
        apply(null(), oid(200)).await,
        Err(RefUpdateRejection::UnknownCommit),
        "a branch may not point at an object nothing numbered"
    );
    assert_eq!(apply(null(), first).await, Ok(()));
    assert_eq!(
        apply(null(), second).await,
        Err(RefUpdateRejection::AlreadyExists)
    );
    assert_eq!(
        apply(second, second).await,
        Err(RefUpdateRejection::NonFastForward),
        "the guard is the value it holds now"
    );
    assert_eq!(
        ids.refs_for(&made)
            .await
            .expect("its refs")
            .get("refs/heads/main"),
        Some(&first.to_string()),
        "a refused write leaves the branch where it was"
    );
    assert_eq!(apply(first, second).await, Ok(()));

    let refs = ids.refs_for(&made).await.expect("its refs");
    assert_eq!(refs.get("refs/heads/main"), Some(&second.to_string()));
    assert_eq!(
        refs.get("HEAD"),
        Some(&"ref: refs/heads/main".to_owned()),
        "HEAD is synthesized, not stored"
    );
}

/// A tag is guarded like a branch, but takes the write path that asks for no
/// commit — so its compare-and-set is proved on its own.
async fn a_tag_is_guarded_by_its_current_value(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let ids = rows.repo(made.id);

    let apply = async |old, new| {
        ids.update_refs(&[update("refs/tags/v1", old, new)])
            .await
            .expect("an update")[0]
            .result
    };

    // None of these is a numbered commit, which only a branch must be.
    assert_eq!(apply(null(), oid(1)).await, Ok(()));
    assert_eq!(
        apply(null(), oid(2)).await,
        Err(RefUpdateRejection::AlreadyExists)
    );
    assert_eq!(
        apply(oid(2), oid(3)).await,
        Err(RefUpdateRejection::NonFastForward),
        "the guard is the value it holds now"
    );
    assert_eq!(apply(oid(1), oid(2)).await, Ok(()));

    let refs = ids.refs_for(&made).await.expect("its refs");
    assert_eq!(refs.get("refs/tags/v1"), Some(&oid(2).to_string()));
}

/// A ref outside `refs/heads/*` needs no numbered commit, since what walks
/// the objects is what proves an object is there.
///
/// Which namespaces a deployment allows is its application's business, and
/// no table of this store's.
async fn a_ref_outside_refs_heads_is_not_asked_for_a_commit(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let ids = rows.repo(made.id);

    let results = ids
        .update_refs(&[
            update("refs/tags/v1", null(), oid(210)),
            update("refs/notes/commits", null(), oid(211)),
            update("refs/merge-requests/42/head", null(), oid(212)),
        ])
        .await
        .expect("an update");
    assert_eq!(
        results.iter().map(|one| one.result).collect::<Vec<_>>(),
        vec![Ok(()), Ok(()), Ok(())]
    );

    let refs = ids.refs_for(&made).await.expect("its refs");
    assert_eq!(refs.get("refs/notes/commits"), Some(&oid(211).to_string()));
    assert_eq!(
        refs.get("refs/merge-requests/42/head"),
        Some(&oid(212).to_string())
    );
}

/// A delete is a write of the null id, guarded like any other — and an
/// unguarded delete of a ref that is not there has nothing to do.
async fn a_null_new_id_deletes_the_ref(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let tip = commit(rows, &made, 1).await.expect("a commit");
    let ids = rows.repo(made.id);
    ids.update_refs(&[
        update("refs/heads/main", null(), tip),
        update("refs/tags/v1", null(), oid(210)),
    ])
    .await
    .expect("an update");

    let results = ids
        .update_refs(&[
            update("refs/heads/main", tip, null()),
            update("refs/tags/v1", oid(210), null()),
            update("refs/heads/never-there", null(), null()),
        ])
        .await
        .expect("an update");
    assert_eq!(
        results.iter().map(|one| one.result).collect::<Vec<_>>(),
        vec![Ok(()), Ok(()), Ok(())],
        "a delete of what is not there is nothing to do"
    );

    let refs = ids.refs_for(&made).await.expect("its refs");
    assert!(!refs.contains_key("refs/heads/main"));
    assert!(!refs.contains_key("refs/tags/v1"));
}

/// Refused here rather than left to a caller: an application reaching the
/// contract runs no `receive-pack` screen, and `HEAD` is not a ref to write.
async fn a_refname_git_would_refuse_is_refused(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let ids = rows.repo(made.id);

    let results = ids
        .update_refs(&[update("HEAD", null(), oid(1))])
        .await
        .expect("an update");
    assert_eq!(results[0].result, Err(RefUpdateRejection::InvalidRefname));

    assert_eq!(
        ids.refs_for(&made).await.expect("its refs").get("HEAD"),
        Some(&"ref: refs/heads/main".to_owned()),
        "and the synthesized HEAD is untouched"
    );
}

/// Each update in a batch stands on its own guard: one refusal neither
/// blocks the rest nor rolls them back.
async fn one_refusal_leaves_the_rest_of_a_batch_alone(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let tip = commit(rows, &made, 1).await.expect("a commit");
    let moved_to = commit(rows, &made, 2).await.expect("a commit");
    let ids = rows.repo(made.id);
    ids.update_refs(&[update("refs/heads/main", null(), tip)])
        .await
        .expect("an update");

    let results = ids
        .update_refs(&[
            update("refs/heads/main", oid(9), moved_to),
            update("refs/tags/v1", null(), oid(210)),
        ])
        .await
        .expect("an update");
    assert_eq!(
        results.iter().map(|one| one.result).collect::<Vec<_>>(),
        vec![Err(RefUpdateRejection::NonFastForward), Ok(())],
        "answered in the order it was asked, whatever order it wrote in"
    );

    let refs = ids.refs_for(&made).await.expect("its refs");
    assert_eq!(refs.get("refs/heads/main"), Some(&tip.to_string()));
    assert_eq!(refs.get("refs/tags/v1"), Some(&oid(210).to_string()));
}

/// A caller that names the refs it wants is answered with those and nothing
/// else — no other branch, and no `HEAD` it did not ask for.
async fn only_the_refs_asked_for_come_back(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let main_tip = commit(rows, &made, 1).await.expect("a commit");
    let other_tip = commit(rows, &made, 2).await.expect("a commit");
    let ids = rows.repo(made.id);
    ids.update_refs(&[
        update("refs/heads/main", null(), main_tip),
        update("refs/heads/other", null(), other_tip),
        update("refs/tags/v1", null(), oid(210)),
    ])
    .await
    .expect("an update");

    let refs = ids
        .refs_matching(&["refs/heads/main", "refs/tags/v1"])
        .await
        .expect("its refs");
    assert_eq!(refs.get("refs/heads/main"), Some(&main_tip.to_string()));
    assert_eq!(refs.get("refs/tags/v1"), Some(&oid(210).to_string()));
    assert_eq!(refs.len(), 2, "neither another ref nor a HEAD");
}

/// The window a repository is erased after, which the janitor reads.
async fn a_deleted_repository_stops_being_listed(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");

    assert!(rows.repo(made.id).mark_deleted().await.expect("a delete"));
    assert!(
        !rows.repo(made.id).mark_deleted().await.expect("a delete"),
        "only the call that did it says so"
    );
    assert!(
        rows.repo(made.id)
            .lookup()
            .await
            .expect("a lookup")
            .is_none()
    );
    assert!(
        !rows
            .all()
            .await
            .expect("a listing")
            .iter()
            .any(|one| one.id == made.id)
    );
    assert!(
        rows.deleted(0)
            .await
            .expect("a listing")
            .iter()
            .any(|one| one.id == made.id),
        "a repository past its window is the janitor's"
    );
}

/// Stamps are finer than a second, so a window of none still tells two
/// writes apart — which is what the other store's `timestamptz` does.
async fn a_grace_window_of_nothing_is_still_a_window(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    rows.repo(made.id).mark_deleted().await.expect("a delete");

    assert!(
        rows.deleted(3600).await.expect("a listing").is_empty(),
        "an hour has not passed"
    );
}

/// The ids still there, in order, each with when it last took a push.
///
/// A gone id is not an error: a listing pages a ledger it does not lock.
async fn a_summary_reports_the_ids_it_still_has_in_order(rows: &Rows) {
    let quiet = repo(rows).await.expect("a repository");
    let pushed = repo(rows).await.expect("a repository");
    let gone = repo(rows).await.expect("a repository");
    let tip = commit(rows, &pushed, 3).await.expect("a commit");
    rows.repo(pushed.id)
        .update_refs(&[update("refs/heads/main", null(), tip)])
        .await
        .expect("an update");
    rows.repo(gone.id).mark_deleted().await.expect("a delete");

    // Handed newest first: the order must be the store's, not the caller's.
    let found = rows
        .summarize(&[gone.id, pushed.id, quiet.id])
        .await
        .expect("a summary");

    let ids: Vec<i64> = found.iter().map(|one| one.repo.id.as_i64()).collect();
    assert_eq!(
        ids,
        vec![quiet.id.as_i64(), pushed.id.as_i64()],
        "oldest first, and a deleted one is not there"
    );
    assert!(
        found[0].last_push_unix_seconds.is_none(),
        "nothing has been pushed to it"
    );
    assert!(
        found[1].last_push_unix_seconds.is_some(),
        "its default branch has moved"
    );
    assert!(
        rows.summarize(&[]).await.expect("a summary").is_empty(),
        "asking for none reads none"
    );
}

/// Which refusal a write that fails both ways is given.
///
/// One statement decides both, so the order is the store's rather than a
/// caller's, and a caller reading the reason must get the same one either way.
async fn two_reasons_to_refuse_one_write_are_reported_in_one_order(rows: &Rows) {
    let made = repo(rows).await.expect("a repository");
    let tip = commit(rows, &made, 1).await.expect("a commit");
    let ids = rows.repo(made.id);

    let apply = async |old, new| {
        ids.update_refs(&[update("refs/heads/main", old, new)])
            .await
            .expect("an update")[0]
            .result
    };

    assert_eq!(apply(null(), tip).await, Ok(()));
    assert_eq!(
        apply(null(), oid(200)).await,
        Err(RefUpdateRejection::UnknownCommit),
        "creating over a branch that exists, with a tip nothing numbered"
    );
    assert_eq!(
        apply(oid(199), oid(200)).await,
        Err(RefUpdateRejection::NonFastForward),
        "a stale guard is a non-fast-forward whatever it was pointed at"
    );
}
