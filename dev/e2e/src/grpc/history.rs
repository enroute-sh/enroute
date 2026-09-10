//! What a commit changed, and what landed before it.

use enroute_git_test_support::linear_commit;

use crate::contract::{Client, hex, hexes};
use crate::support::{
    E2E_TENANT, front_door_for, git, make_isolated_state, seed_pack, spawn_contract_without_hooks,
};

use super::support::{absent_oid, hook_backed_repo, pushed_repo, pushed_repo_serving, status_of};

/// Where a merge base is measured against, as the other tests here use it.
const TRUNK: &str = "refs/heads/main";

/// What a commit view is drawn from: the paths a commit touched, and the
/// blob on each side of every one of them.
///
/// Add, change and delete together, because the three are one walk and a
/// deletion is the half the push path never has to report.
#[tokio::test]
async fn a_commit_diffs_against_its_parent() {
    // The door is kept up, because this pushes more than once.
    let (client, id, _tmp, local, _servers) = pushed_repo_serving("first\n").await;

    // A second commit doing all three, in a subdirectory as well as at
    // the root: a path is only a path once something is above it.
    std::fs::create_dir_all(local.join("dir")).unwrap();
    std::fs::write(local.join("dir/kept.txt"), "kept\n").unwrap();
    std::fs::write(local.join("dir/gone.txt"), "gone\n").unwrap();
    git(&["add", "."], Some(&local)).await;
    git(&["commit", "-m", "second"], Some(&local)).await;
    git(&["push", "origin", "main"], Some(&local)).await;

    std::fs::write(local.join("a.txt"), "second\n").unwrap();
    std::fs::write(local.join("added.txt"), "new\n").unwrap();
    std::fs::remove_file(local.join("dir/gone.txt")).unwrap();
    git(&["add", "-A"], Some(&local)).await;
    git(&["commit", "-m", "third"], Some(&local)).await;
    git(&["push", "origin", "main"], Some(&local)).await;

    let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
    let changes = client.diff_commit(&id, head.trim(), "").await.unwrap();

    let paths: Vec<&str> = changes.iter().map(|change| change.path.as_str()).collect();
    assert_eq!(paths, vec!["a.txt", "added.txt", "dir/gone.txt"]);
    // Which side each id is on is the whole answer: it says what the
    // change was without a word for it.
    assert!(changes[0].old_object_id.is_some() && changes[0].new_object_id.is_some());
    assert!(changes[1].old_object_id.is_none() && changes[1].new_object_id.is_some());
    assert!(changes[2].old_object_id.is_some() && changes[2].new_object_id.is_none());

    // Against git's own answer, so the two agree about what changed.
    let expected = git(
        &[
            "diff-tree",
            "-r",
            "--name-only",
            "--no-commit-id",
            head.trim(),
        ],
        Some(&local),
    )
    .await;
    let mut named: Vec<&str> = expected.lines().filter(|line| !line.is_empty()).collect();
    named.sort_unstable();
    assert_eq!(paths, named);

    // The new side is the blob git has at that path, not merely some id.
    let blob = git(&["rev-parse", "HEAD:a.txt"], Some(&local)).await;
    assert_eq!(hex(changes[0].new_object_id.as_ref()), blob.trim());
}

/// A commit with no parent has nothing to diff against, so everything in
/// it is an add — git's own answer for a root commit.
#[tokio::test]
async fn a_root_commit_adds_every_path_in_it() {
    let (client, id, _tmp, local) = pushed_repo("first\n").await;
    let head = git(&["rev-parse", "HEAD"], Some(&local)).await;

    let changes = client.diff_commit(&id, head.trim(), "").await.unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].path, "a.txt");
    assert!(
        changes[0].old_object_id.is_none(),
        "a root commit has no side to have changed from"
    );
}

/// A diff is of a commit, and a tree is refused.
///
/// A tree has no parent, so asking what it changed is a question with no
/// answer rather than one whose answer is empty.
#[tokio::test]
async fn diffing_a_tree_is_refused() {
    let (client, id, _tmp, local) = pushed_repo("first\n").await;
    let tree = git(&["rev-parse", "HEAD^{tree}"], Some(&local)).await;

    let error = client
        .diff_commit(&id, tree.trim(), "")
        .await
        .expect_err("a tree is not a commit");
    assert_eq!(
        status_of(&error).code(),
        tonic::Code::InvalidArgument,
        "wrong status for diffing a tree: {error:?}"
    );
}

/// A repository whose `main` is `count` commits deep, newest last.
///
/// Returns the client, the repository id, the checkout, and every oid in
/// the order git made them.
async fn pushed_history(count: usize) -> (Client, String, tempfile::TempDir, Vec<String>) {
    let state = make_isolated_state().await;
    let enroute = spawn_contract_without_hooks(state.clone()).await;
    let client = Client::connect(format!("http://{enroute}"), E2E_TENANT)
        .await
        .unwrap();
    let repo = client.create_repository("").await.unwrap();
    let id = repo.repo.clone().unwrap().key;

    // Held for as long as the push needs it: every commit is made before
    // the one push, so the front door outlives the only call that uses it.
    let (front_door, _servers) = front_door_for(state).await;
    let tmp = tempfile::tempdir().unwrap();
    let local = tmp.path().join("work");
    std::fs::create_dir_all(&local).unwrap();
    let url = format!("http://{front_door}/anything.git");
    git(&["init", "-b", "main"], Some(&local)).await;
    git(&["remote", "add", "origin", &url], Some(&local)).await;
    for n in 0..count {
        std::fs::write(local.join("a.txt"), format!("body {n}\n")).unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", &format!("commit {n}")], Some(&local)).await;
    }
    git(&["push", "origin", "main"], Some(&local)).await;

    let listed = git(&["rev-list", "--reverse", "HEAD"], Some(&local)).await;
    let oids = listed.lines().map(str::to_owned).collect();
    (client, id, tmp, oids)
}

/// The history reads back newest first, and reads back what git says it
/// is — the ordering is the whole point of the call.
#[tokio::test]
async fn a_history_lists_newest_first() {
    let (client, id, _tmp, oids) = pushed_history(5).await;
    let head = oids.last().unwrap();

    let (commits, next) = client.list_commits(&id, head, 0, "").await.unwrap();
    let listed: Vec<String> = commits.iter().map(|c| hex(c.commit_id.as_ref())).collect();
    let expected: Vec<String> = oids.iter().rev().cloned().collect();
    assert_eq!(listed, expected, "a history came back out of order");
    assert!(
        next.is_empty(),
        "a whole history still named a page after it: {next}"
    );

    let newest = commits.first().expect("a page of five has a first commit");
    assert_eq!(newest.summary, "commit 4");
    assert_eq!(
        hexes(&newest.parent_commit_ids).first().copied(),
        Some(oids[3].as_str()),
        "a commit named the wrong parent"
    );
}

/// Paging resumes exactly where the page before it stopped, with nothing
/// repeated and nothing skipped across the seam.
#[tokio::test]
async fn paging_a_history_neither_repeats_nor_skips() {
    let (client, id, _tmp, oids) = pushed_history(5).await;
    let head = oids.last().unwrap();

    let (first, next) = client.list_commits(&id, head, 2, "").await.unwrap();
    assert_eq!(first.len(), 2);
    assert!(!next.is_empty(), "a partial history named no page after it");

    let (second, last) = client.list_commits(&id, head, 3, &next).await.unwrap();
    assert_eq!(second.len(), 3);
    assert!(
        last.is_empty(),
        "the last page named a page after it: {last}"
    );

    let walked: Vec<String> = first
        .iter()
        .chain(second.iter())
        .map(|c| hex(c.commit_id.as_ref()))
        .collect();
    let expected: Vec<String> = oids.iter().rev().cloned().collect();
    assert_eq!(walked, expected, "paging did not reassemble the history");

    // And an unset limit from the same token takes whatever is left, which is
    // what a caller reading the tail of a history asks for.
    let (rest, none) = client.list_commits(&id, head, 0, &next).await.unwrap();
    assert!(
        none.is_empty(),
        "the last page named a page after it: {none}"
    );
    assert_eq!(
        rest.iter()
            .map(|c| hex(c.commit_id.as_ref()))
            .collect::<Vec<_>>(),
        expected[2..],
        "an unset limit did not take the rest of the history"
    );
}

/// A history starts at a commit, and a tree is a well-formed question with
/// the answer no — not an internal error.
#[tokio::test]
async fn a_history_from_a_tree_is_refused() {
    let (client, id, _tmp, local) = pushed_repo("body\n").await;
    let tree = git(&["rev-parse", "HEAD^{tree}"], Some(&local)).await;

    let error = client
        .list_commits(&id, tree.trim(), 0, "")
        .await
        .expect_err("a tree has no history");
    assert_eq!(
        status_of(&error).code(),
        tonic::Code::InvalidArgument,
        "wrong status for a history from a tree: {error:?}"
    );
}

/// And one nobody pushed is still a `NotFound`, as reading it would be.
#[tokio::test]
async fn a_history_from_an_unknown_object_is_not_found() {
    let (client, id, _tmp, _local) = pushed_repo("body\n").await;
    let absent = absent_oid();

    let error = client
        .list_commits(&id, &absent, 0, "")
        .await
        .expect_err("an object that was never pushed has no history");
    assert_eq!(
        status_of(&error).code(),
        tonic::Code::NotFound,
        "wrong status for a history from a missing object: {error:?}"
    );
}

/// A merge is read against its first parent, in both of the calls a commit
/// view is drawn from: what it changed, and what landed before it.
///
/// One repository for both, since a merge is what each is about and building
/// one costs a checkout, a branch, and a push.
#[tokio::test]
async fn a_merge_reads_against_its_first_parent() {
    let (client, id, _tmp, local, _servers) = pushed_repo_serving("root\n").await;
    git(&["checkout", "-b", "side"], Some(&local)).await;
    std::fs::write(local.join("side.txt"), "side\n").unwrap();
    git(&["add", "."], Some(&local)).await;
    git(&["commit", "-m", "on the side"], Some(&local)).await;

    git(&["checkout", "main"], Some(&local)).await;
    std::fs::write(local.join("trunk.txt"), "trunk\n").unwrap();
    git(&["add", "."], Some(&local)).await;
    git(&["commit", "-m", "on trunk"], Some(&local)).await;
    git(
        &["merge", "--no-ff", "-m", "merge the side in", "side"],
        Some(&local),
    )
    .await;
    git(&["push", "origin", "main"], Some(&local)).await;
    let merge = git(&["rev-parse", "HEAD"], Some(&local)).await;
    let first_parents = git(&["rev-list", "--first-parent", "HEAD"], Some(&local)).await;

    let changes = client.diff_commit(&id, merge.trim(), "").await.unwrap();
    let paths: Vec<&str> = changes.iter().map(|change| change.path.as_str()).collect();
    assert_eq!(
        paths,
        vec!["side.txt"],
        "a merge's diff is what the side brought, not what trunk already had"
    );

    let (commits, _next) = client.list_commits(&id, merge.trim(), 0, "").await.unwrap();
    let listed: Vec<String> = commits.iter().map(|c| hex(c.commit_id.as_ref())).collect();
    assert_eq!(
        listed,
        first_parents.lines().map(str::to_owned).collect::<Vec<_>>(),
        "the history is not git's own first-parent walk"
    );
}

/// A commit carries what a listing is drawn from: its summary, its body, and
/// who wrote it — so a caller draws one without reading the object.
#[tokio::test]
async fn a_commit_carries_its_summary_and_author() {
    let (client, id, _tmp, local, _servers) = pushed_repo_serving("body\n").await;
    std::fs::write(local.join("a.txt"), "again\n").unwrap();
    git(&["add", "."], Some(&local)).await;
    git(
        &["commit", "-m", "a summary", "-m", "and a body of its own"],
        Some(&local),
    )
    .await;
    git(&["push", "origin", "main"], Some(&local)).await;
    let written = git(
        &["log", "-1", "--format=%H%n%T%n%P%n%an <%ae>"],
        Some(&local),
    )
    .await;
    let [head, tree, parent, author] = <[&str; 4]>::try_from(written.lines().collect::<Vec<_>>())
        .expect("four lines, one per format directive");

    let (commits, _next) = client.list_commits(&id, head, 0, "").await.unwrap();
    let commit = commits.first().expect("a history has its own tip in it");
    assert_eq!(hex(commit.commit_id.as_ref()), head);
    assert_eq!(commit.summary, "a summary");
    // Trailing newline and all: the body is the commit's own bytes after the
    // summary, not a trimmed rendering of them.
    assert_eq!(commit.body, "and a body of its own\n");
    assert_eq!(hex(commit.tree_id.as_ref()), tree);
    assert_eq!(hexes(&commit.parent_commit_ids), vec![parent],);
    let identity = commit.author.as_ref().expect("a commit has an author");
    assert_eq!(
        format!("{} <{}>", identity.name, identity.email),
        author,
        "the commit does not report the author git wrote"
    );
}

/// Where two branches last agreed, which only the commit graph knows.
///
/// An application reads what a branch changed against this commit, not against
/// the other branch's tip — everything landed there since would read as undone.
#[tokio::test]
async fn a_merge_base_is_the_commit_two_branches_forked_at() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    // One root, then a commit on each side of it.
    let root = linear_commit(b"root\n", None, 1, "root");
    let mine = linear_commit(b"mine\n", Some(&root.commit_sha), 2, "mine");
    let yours = linear_commit(b"yours\n", Some(&root.commit_sha), 3, "yours");
    seed_pack(&state, TRUNK, &[&root, &mine]).await;
    seed_pack(&state, "refs/heads/yours", &[&root, &yours]).await;

    let found = client
        .find_merge_bases(&repo, &mine.commit_sha, &yours.commit_sha)
        .await
        .unwrap();
    assert_eq!(
        hexes(&found.base_commit_ids),
        vec![root.commit_sha.as_str()],
        "the two forked at the root"
    );
    assert!(
        !found.exhausted,
        "a walk this small must not report itself cut short"
    );
    // Symmetric, and a commit's base with itself is itself.
    assert_eq!(
        hexes(
            &client
                .find_merge_bases(&repo, &yours.commit_sha, &mine.commit_sha)
                .await
                .unwrap()
                .base_commit_ids
        ),
        vec![root.commit_sha.as_str()],
    );
    assert_eq!(
        hexes(
            &client
                .find_merge_bases(&repo, &mine.commit_sha, &mine.commit_sha)
                .await
                .unwrap()
                .base_commit_ids
        ),
        vec![mine.commit_sha.as_str()],
    );
}

/// A branch against a trunk, which is the pair a merge request is drawn from.
///
/// Every commit here writes the same path, so which base was used shows in
/// the blob on the old side rather than in the paths.
#[tokio::test]
async fn a_diff_reads_against_the_base_it_is_given() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;

    // Trunk moves on after the branch is cut, so the two have different ideas
    // of what the file says.
    let root = linear_commit(b"root\n", None, 1, "root");
    let landed = linear_commit(b"landed\n", Some(&root.commit_sha), 2, "landed on trunk");
    let mine = linear_commit(b"mine\n", Some(&root.commit_sha), 3, "mine");
    seed_pack(&state, TRUNK, &[&root, &landed]).await;
    seed_pack(&state, "refs/heads/mine", &[&root, &mine]).await;

    let older = |changes: Vec<enroute_api::api::v1alpha1::FileChange>| {
        assert_eq!(changes.len(), 1, "one path, written by every commit here");
        hex(changes[0].old_object_id.as_ref())
    };

    // The base the two forked at, which `FindMergeBases` names and the test
    // above already holds it to.
    let against_base = client
        .diff_commit(&repo, &mine.commit_sha, &root.commit_sha)
        .await
        .unwrap();
    assert_eq!(
        older(against_base),
        root.blob_oid.to_hex().to_string(),
        "what the branch changed, read from where it forked"
    );

    // Against trunk's tip the old side is trunk's own work, so the diff
    // claims the branch undid it.
    let against_tip = client
        .diff_commit(&repo, &mine.commit_sha, &landed.commit_sha)
        .await
        .unwrap();
    assert_eq!(older(against_tip), landed.blob_oid.to_hex().to_string());

    // Empty still means the first parent, which for this commit is the root.
    let against_parent = client
        .diff_commit(&repo, &mine.commit_sha, "")
        .await
        .unwrap();
    assert_eq!(older(against_parent), root.blob_oid.to_hex().to_string());
}

/// A base that is not a commit is refused, as a diff of a tree is.
#[tokio::test]
async fn a_diff_against_a_base_that_is_not_a_commit_is_refused() {
    let (state, client, repo, _servers) = hook_backed_repo(TRUNK).await;
    let only = linear_commit(b"only\n", None, 1, "only");
    seed_pack(&state, TRUNK, &[&only]).await;

    let refused = client
        .diff_commit(&repo, &only.commit_sha, &only.tree_sha)
        .await
        .expect_err("a tree is not a base");
    assert_eq!(status_of(&refused).code(), tonic::Code::InvalidArgument);
}
