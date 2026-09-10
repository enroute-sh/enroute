//! Reading refs, and moving one onto objects the repository already has.
//!
//! Moving one is how an application merges: a fast-forward introduces no
//! objects, so the whole update is a ref move with no packfile anywhere in it.

use enroute_api::api::v1alpha1::RefUpdate;
use enroute_git_test_support::linear_commit;

use crate::contract::{Client, hex, oid};
use crate::support::seed_pack;

use super::support::{absent_oid, hook_backed_repo, pushed_repo, status_of};

const TRUNK: &str = "refs/heads/main";
const BRANCH: &str = "refs/heads/feature";

/// `ListRefs` dates each ref, so a caller can build a branch listing
/// without a second call or a clock of the server's.
#[tokio::test]
async fn refs_carry_when_they_last_moved() {
    let (client, id, _tmp, _local) = pushed_repo("body\n").await;

    let refs = client.list_refs(&id).await.unwrap();
    let main = refs
        .refs
        .iter()
        .find(|r| r.name == "refs/heads/main")
        .expect("the pushed branch is missing");
    let moved = main
        .updated_at
        .map(|at| at.seconds)
        .expect("a branch that was just pushed has moved");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(
        moved > 0 && now.saturating_sub(u64::try_from(moved).unwrap()) < 300,
        "a just-pushed branch is dated {moved}, now is {now}"
    );
}

/// The merge, end to end: a branch holds the objects, and trunk is walked
/// onto them by a call that carries none.
#[tokio::test]
async fn a_ref_moves_onto_stored_objects() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    // Two commits, so the second move is a real fast-forward rather than
    // another create.
    let first = linear_commit(b"one\n", None, 1, "first");
    let second = linear_commit(b"two\n", Some(&first.commit_sha), 2, "second");

    // Everything arrives on the branch, as a push would put it there.
    // Trunk is untouched, and every object trunk will later need is now in
    // permanent storage.
    seed_pack(&state, BRANCH, &[&first, &second]).await;

    // Creating trunk at the first commit.
    let outcomes = client
        .update_refs(
            &repo,
            vec![RefUpdate {
                refname: TRUNK.to_string(),
                old_object_id: None,
                new_object_id: oid(first.commit_sha.clone()),
            }],
        )
        .await
        .unwrap();
    assert!(
        outcomes.iter().all(|outcome| outcome.rejection.is_none()),
        "a create onto a stored object must land: {outcomes:?}"
    );
    assert_eq!(
        tip(&client, &repo, TRUNK).await.as_deref(),
        Some(first.commit_sha.as_str())
    );

    // And the fast-forward itself, guarded on where trunk was — the compare-
    // and-swap an application merges with.
    let outcomes = client
        .update_refs(
            &repo,
            vec![RefUpdate {
                refname: TRUNK.to_string(),
                old_object_id: oid(first.commit_sha.clone()),
                new_object_id: oid(second.commit_sha.clone()),
            }],
        )
        .await
        .unwrap();
    assert!(
        outcomes.iter().all(|outcome| outcome.rejection.is_none()),
        "a fast-forward onto a stored object must land: {outcomes:?}"
    );
    assert_eq!(
        tip(&client, &repo, TRUNK).await.as_deref(),
        Some(second.commit_sha.as_str()),
        "trunk has to be where the merge put it"
    );
}

/// A ref pointed at an object nobody has pushed is refused.
///
/// Both namespaces, since the ref store guards a branch with a lookup of
/// its own and a tag with nothing.
#[tokio::test]
async fn a_tip_that_is_not_stored_is_refused() {
    let (_state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    // Never pushed anywhere, so the repository has never seen it.
    let stranger = linear_commit(b"unsent\n", None, 1, "unsent");

    for refname in [TRUNK, "refs/tags/v1"] {
        let outcomes = client
            .update_refs(
                &repo,
                vec![RefUpdate {
                    refname: refname.to_string(),
                    old_object_id: None,
                    new_object_id: oid(stranger.commit_sha.clone()),
                }],
            )
            .await
            .unwrap();

        assert!(
            outcomes.iter().any(|outcome| outcome.rejection.is_some()),
            "{refname} may not be pointed at an object nobody pushed: {outcomes:?}"
        );
        assert_eq!(
            tip(&client, &repo, refname).await,
            None,
            "and {refname} must not exist"
        );
    }
}

/// Two merges racing: the one that read trunk before the other moved it is
/// refused rather than served.
///
/// A merge queue's whole concurrency safety: an application reads the refusal
/// as "trunk moved, try again", so it must refuse rather than overwrite.
#[tokio::test]
async fn a_stale_old_id_is_refused() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    let first = linear_commit(b"one\n", None, 1, "first");
    let second = linear_commit(b"two\n", Some(&first.commit_sha), 2, "second");

    seed_pack(&state, TRUNK, &[&first, &second]).await;

    // Trunk is at `second`; this asks as though it were still at `first`.
    let outcomes = client
        .update_refs(
            &repo,
            vec![RefUpdate {
                refname: TRUNK.to_string(),
                old_object_id: oid(first.commit_sha.clone()),
                new_object_id: oid(first.commit_sha.clone()),
            }],
        )
        .await
        .unwrap();

    assert!(
        outcomes.iter().any(|outcome| outcome.rejection.is_some()),
        "an update guarded on the wrong tip must be refused: {outcomes:?}"
    );
    assert_eq!(
        tip(&client, &repo, TRUNK).await.as_deref(),
        Some(second.commit_sha.as_str()),
        "and must not have moved trunk"
    );
}

/// Enroute answers the ancestry question and holds no opinion about it.
///
/// An application has no commit graph, the same reason `pre-receive` is handed
/// `force`. What to do with the answer stays the application's.
#[tokio::test]
async fn ancestry_is_answered_and_not_enforced() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    let first = linear_commit(b"one\n", None, 1, "first");
    let second = linear_commit(b"two\n", Some(&first.commit_sha), 2, "second");
    seed_pack(&state, TRUNK, &[&first, &second]).await;

    assert!(
        client
            .is_ancestor(&repo, &first.commit_sha, &second.commit_sha)
            .await
            .unwrap(),
        "the second commit descends from the first"
    );
    assert!(
        !client
            .is_ancestor(&repo, &second.commit_sha, &first.commit_sha)
            .await
            .unwrap(),
        "and the first does not descend from the second"
    );

    // Which is what an application would refuse on. Enroute does not: trunk is
    // at `second`, the old id is correct, and moving it back to `first` lands.
    let outcomes = client
        .update_refs(
            &repo,
            vec![RefUpdate {
                refname: TRUNK.to_string(),
                old_object_id: oid(second.commit_sha.clone()),
                new_object_id: oid(first.commit_sha.clone()),
            }],
        )
        .await
        .unwrap();
    assert_eq!(outcomes[0].rejection, None, "{outcomes:?}");
    assert_eq!(
        tip(&client, &repo, TRUNK).await.as_deref(),
        Some(first.commit_sha.as_str())
    );
}

async fn tip(client: &Client, repo: &str, refname: &str) -> Option<String> {
    client
        .list_refs(repo)
        .await
        .unwrap()
        .refs
        .into_iter()
        .find(|r| r.name == refname)
        .map(|r| hex(r.object_id.as_ref()))
}

/// A repository one call old has no refs, and says so rather than failing.
///
/// The default branch is still reported: it is where `HEAD` points, which is
/// true before anything is pushed to it.
#[tokio::test]
async fn an_empty_repository_lists_no_refs() {
    let (_state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    let listed = client.list_refs(&repo).await.unwrap();
    assert!(listed.refs.is_empty(), "{:?}", listed.refs);
    assert_eq!(listed.default_branch, TRUNK);
}

/// A prefix narrows the listing, which is how a branch picker is drawn
/// without reading every tag in the repository.
#[tokio::test]
async fn a_prefix_narrows_the_listing() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    let commit = linear_commit(b"one\n", None, 1, "first");
    seed_pack(&state, TRUNK, &[&commit]).await;
    let landed = client
        .update_refs(
            &repo,
            vec![RefUpdate {
                refname: "refs/tags/v1".to_string(),
                old_object_id: None,
                new_object_id: oid(commit.commit_sha.clone()),
            }],
        )
        .await
        .unwrap();
    assert_eq!(landed[0].rejection, None, "{landed:?}");

    let branches = client
        .list_refs_under(&repo, vec!["refs/heads/".to_string()])
        .await
        .unwrap();
    assert_eq!(
        branches
            .refs
            .iter()
            .map(|r| r.name.clone())
            .collect::<Vec<_>>(),
        vec![TRUNK.to_string()]
    );

    let tags = client
        .list_refs_under(&repo, vec!["refs/tags/".to_string()])
        .await
        .unwrap();
    assert_eq!(
        tags.refs.iter().map(|r| r.name.clone()).collect::<Vec<_>>(),
        vec!["refs/tags/v1".to_string()]
    );

    let both = client
        .list_refs_under(
            &repo,
            vec!["refs/heads/".to_string(), "refs/tags/".to_string()],
        )
        .await
        .unwrap();
    assert_eq!(both.refs.len(), 2, "{:?}", both.refs);
}

/// Several refs move in one call, and each one answers for itself.
///
/// One outcome per update, in the order asked — a refusal of one is data
/// about that ref rather than a failure of the call.
#[tokio::test]
async fn several_refs_move_in_one_call() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    let first = linear_commit(b"one\n", None, 1, "first");
    let second = linear_commit(b"two\n", Some(&first.commit_sha), 2, "second");
    seed_pack(&state, BRANCH, &[&first, &second]).await;
    let stranger = linear_commit(b"unsent\n", None, 3, "unsent");

    let outcomes = client
        .update_refs(
            &repo,
            vec![
                RefUpdate {
                    refname: TRUNK.to_string(),
                    old_object_id: None,
                    new_object_id: oid(second.commit_sha.clone()),
                },
                RefUpdate {
                    refname: "refs/heads/nope".to_string(),
                    old_object_id: None,
                    new_object_id: oid(stranger.commit_sha.clone()),
                },
                RefUpdate {
                    refname: "refs/tags/v1".to_string(),
                    old_object_id: None,
                    new_object_id: oid(first.commit_sha.clone()),
                },
            ],
        )
        .await
        .unwrap();

    assert_eq!(outcomes.len(), 3, "{outcomes:?}");
    assert_eq!(outcomes[0].refname, TRUNK);
    assert_eq!(outcomes[0].rejection, None, "{outcomes:?}");
    assert!(
        outcomes[1].rejection.is_some(),
        "a tip nobody pushed may not land: {outcomes:?}"
    );
    assert_eq!(outcomes[2].rejection, None, "{outcomes:?}");

    assert_eq!(
        tip(&client, &repo, TRUNK).await.as_deref(),
        Some(second.commit_sha.as_str())
    );
    assert_eq!(tip(&client, &repo, "refs/heads/nope").await, None);
    assert_eq!(
        tip(&client, &repo, "refs/tags/v1").await.as_deref(),
        Some(first.commit_sha.as_str())
    );
}

/// A ref is deleted by moving it to the null id, which is how git says it too.
#[tokio::test]
async fn a_ref_moved_to_the_null_id_is_deleted() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    let commit = linear_commit(b"one\n", None, 1, "first");
    seed_pack(&state, BRANCH, &[&commit]).await;
    assert_eq!(
        tip(&client, &repo, BRANCH).await.as_deref(),
        Some(commit.commit_sha.as_str())
    );

    let outcomes = client
        .update_refs(
            &repo,
            vec![RefUpdate {
                refname: BRANCH.to_string(),
                old_object_id: oid(commit.commit_sha.clone()),
                new_object_id: None,
            }],
        )
        .await
        .unwrap();
    assert_eq!(outcomes[0].rejection, None, "{outcomes:?}");
    assert_eq!(
        tip(&client, &repo, BRANCH).await,
        None,
        "the branch survived its own deletion"
    );
}

/// A commit is its own ancestor, and one nobody pushed is nobody's.
///
/// Both are answers rather than errors: the question is well formed either way,
/// and an application asking about a stale commit gets `false`.
#[tokio::test]
async fn ancestry_answers_for_the_edges_too() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    let commit = linear_commit(b"one\n", None, 1, "first");
    seed_pack(&state, TRUNK, &[&commit]).await;
    let absent = absent_oid();

    assert!(
        client
            .is_ancestor(&repo, &commit.commit_sha, &commit.commit_sha)
            .await
            .unwrap(),
        "a commit descends from itself"
    );
    assert!(
        !client
            .is_ancestor(&repo, &absent, &commit.commit_sha)
            .await
            .unwrap(),
        "a commit nobody pushed is nobody's ancestor"
    );
    assert!(
        !client
            .is_ancestor(&repo, &commit.commit_sha, &absent)
            .await
            .unwrap(),
        "and nothing descends from it either"
    );
}

/// A malformed object id in an update is refused before anything moves.
#[tokio::test]
async fn a_malformed_id_in_an_update_moves_nothing() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    let commit = linear_commit(b"one\n", None, 1, "first");
    seed_pack(&state, BRANCH, &[&commit]).await;

    let error = client
        .update_refs(
            &repo,
            vec![
                RefUpdate {
                    refname: TRUNK.to_string(),
                    old_object_id: None,
                    new_object_id: oid(commit.commit_sha.clone()),
                },
                RefUpdate {
                    refname: "refs/heads/other".to_string(),
                    old_object_id: None,
                    new_object_id: oid("not-an-object-id".to_string()),
                },
            ],
        )
        .await
        .expect_err("an update carrying a malformed id is not a request");
    assert_eq!(
        status_of(&error).code(),
        tonic::Code::InvalidArgument,
        "{error:?}"
    );
    assert_eq!(
        tip(&client, &repo, TRUNK).await,
        None,
        "the well-formed half of a refused call must not have landed"
    );
}
