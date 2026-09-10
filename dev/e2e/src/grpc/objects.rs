//! Granular reads: naming one object instead of moving a whole history.

use enroute_api::api::v1alpha1::ObjectKind;

use crate::support::git;

use super::support::{absent_oid, contract_with_repo, pushed_repo, pushed_repo_serving, status_of};

/// The point of the whole primitive: read a file's contents by object id,
/// without a packfile and without speaking git.
#[tokio::test]
async fn a_blob_reads_back_byte_for_byte() {
    let (client, id, _tmp, local) = pushed_repo("hello from a blob\n").await;
    let oid = git(&["rev-parse", "HEAD:a.txt"], Some(&local)).await;

    let object = client.get_object(&id, oid.trim()).await.unwrap();
    assert_eq!(object.kind, ObjectKind::Blob);
    assert_eq!(object.size, 18);

    let bytes = object.collect().await.unwrap();
    assert_eq!(&bytes[..], b"hello from a blob\n");
}

/// A commit reads back as the bytes git itself hashes, so the two agree
/// about what the object is — `cat-file` is the reference answer.
#[tokio::test]
async fn a_commit_reads_back_as_git_stores_it() {
    let (client, id, _tmp, local) = pushed_repo("body\n").await;
    let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
    let expected = git(&["cat-file", "commit", head.trim()], Some(&local)).await;

    let object = client.get_object(&id, head.trim()).await.unwrap();
    assert_eq!(object.kind, ObjectKind::Commit);

    let bytes = object.collect().await.unwrap();
    assert_eq!(String::from_utf8(bytes).unwrap(), expected);
}

/// A tree too, which is what a directory listing will be built from.
#[tokio::test]
async fn a_tree_reads_back_with_its_kind() {
    let (client, id, _tmp, local) = pushed_repo("body\n").await;
    let tree = git(&["rev-parse", "HEAD^{tree}"], Some(&local)).await;

    let object = client.get_object(&id, tree.trim()).await.unwrap();
    assert_eq!(object.kind, ObjectKind::Tree);
    let size = object.size;
    let bytes = object.collect().await.unwrap();
    assert_eq!(u64::try_from(bytes.len()).unwrap(), size);
    // A tree entry is `<mode> <name>\0<20-byte oid>`; the name is the
    // only part readable without parsing it.
    assert!(
        bytes.windows(5).any(|w| w == b"a.txt"),
        "the tree does not mention its only file"
    );
}

/// An object nobody pushed is a `NotFound`, not an internal error: the
/// caller asked a well-formed question and the answer is simply no.
#[tokio::test]
async fn an_unknown_object_is_not_found() {
    let (client, id, _tmp, _local) = pushed_repo("body\n").await;
    let absent = absent_oid();

    let error = client
        .get_object(&id, &absent)
        .await
        .expect_err("an object that was never pushed cannot be read");
    // The gRPC code rather than the message: what a caller acts on is the
    // status, and a message is free to be reworded.
    assert_eq!(
        status_of(&error).code(),
        tonic::Code::NotFound,
        "wrong status for a missing object: {error:?}"
    );
}

/// A blob past one message's worth of bytes, which is the case the
/// streaming response exists for.
#[tokio::test]
async fn a_large_blob_crosses_in_several_chunks() {
    let body = "0123456789abcdef".repeat(100_000);
    let (client, id, _tmp, local) = pushed_repo(&body).await;
    let oid = git(&["rev-parse", "HEAD:a.txt"], Some(&local)).await;

    let mut object = client.get_object(&id, oid.trim()).await.unwrap();
    assert_eq!(object.size, 1_600_000);

    let mut chunks = 0;
    let mut total = 0;
    while let Some(chunk) = object.next_chunk().await.unwrap() {
        chunks += 1;
        total += chunk.len();
    }
    assert_eq!(total, 1_600_000);
    assert!(chunks > 1, "a 1.6 MB blob arrived in {chunks} chunk(s)");
}

/// An annotated tag is an object like any other, and reads back as one.
///
/// The one kind no push path has to create, so nothing else here covers it.
#[tokio::test]
async fn an_annotated_tag_reads_back_with_its_kind() {
    let (client, id, _tmp, local, _servers) = pushed_repo_serving("body\n").await;
    git(&["tag", "-a", "v1", "-m", "the first one"], Some(&local)).await;
    git(&["push", "origin", "v1"], Some(&local)).await;
    let oid = git(&["rev-parse", "v1"], Some(&local)).await;

    let object = client.get_object(&id, oid.trim()).await.unwrap();
    assert_eq!(object.kind, ObjectKind::Tag);

    let bytes = object.collect().await.unwrap();
    let text = String::from_utf8(bytes).unwrap();
    assert!(
        text.contains("the first one"),
        "the tag does not carry its message:\n{text}"
    );
}

/// An object id that is not one is the caller's own string, so it comes back
/// as `InvalidArgument` rather than as a repository that has no such object.
#[tokio::test]
async fn a_malformed_object_id_is_an_invalid_argument() {
    let (client, id) = contract_with_repo().await;

    for bad in ["", "zz", &"g".repeat(40), &"0".repeat(41)] {
        let error = client
            .get_object(&id, bad)
            .await
            .expect_err("a malformed object id names nothing");
        assert_eq!(
            status_of(&error).code(),
            tonic::Code::InvalidArgument,
            "wrong status for the object id {bad:?}"
        );
    }
}

/// The listing primitive: every path under a tree, directories included.
///
/// Nested, because a path is only a path once something is above it, and an
/// empty directory would otherwise be invisible.
#[tokio::test]
async fn a_tree_lists_every_path_under_it() {
    let (client, id, _tmp, local, _servers) = pushed_repo_serving("root\n").await;
    std::fs::create_dir_all(local.join("dir/deeper")).unwrap();
    std::fs::write(local.join("dir/one.txt"), "one\n").unwrap();
    std::fs::write(local.join("dir/deeper/two.txt"), "two\n").unwrap();
    git(&["add", "."], Some(&local)).await;
    git(&["commit", "-m", "nested"], Some(&local)).await;
    git(&["push", "origin", "main"], Some(&local)).await;
    let head = git(&["rev-parse", "HEAD"], Some(&local)).await;

    let (entries, truncated) = client.list_tree(&id, head.trim()).await.unwrap();
    assert!(!truncated, "a three-file tree is not a truncated walk");

    let mut paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        vec![
            "a.txt",
            "dir",
            "dir/deeper",
            "dir/deeper/two.txt",
            "dir/one.txt"
        ],
        "the walk did not list the tree git pushed"
    );

    let kind = |path: &str| {
        entries
            .iter()
            .find(|entry| entry.path == path)
            .map(enroute_api::api::v1alpha1::TreeEntry::kind)
            .expect("the walk listed this path")
    };
    assert_eq!(kind("dir"), ObjectKind::Tree);
    assert_eq!(kind("dir/one.txt"), ObjectKind::Blob);
}

/// A commit resolves to its root tree, so a caller holding a branch tip needs
/// no round trip to turn one into the other.
#[tokio::test]
async fn a_commit_lists_as_its_root_tree() {
    let (client, id, _tmp, local) = pushed_repo("body\n").await;
    let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
    let tree = git(&["rev-parse", "HEAD^{tree}"], Some(&local)).await;

    let (from_commit, _) = client.list_tree(&id, head.trim()).await.unwrap();
    let (from_tree, _) = client.list_tree(&id, tree.trim()).await.unwrap();

    let paths = |entries: &[enroute_api::api::v1alpha1::TreeEntry]| {
        entries.iter().map(|e| e.path.clone()).collect::<Vec<_>>()
    };
    assert_eq!(paths(&from_commit), paths(&from_tree));
    assert_eq!(paths(&from_commit), vec!["a.txt".to_string()]);
}

/// A blob has nothing under it, so listing one is a question with no answer
/// rather than one whose answer is empty.
#[tokio::test]
async fn listing_a_blob_is_refused() {
    let (client, id, _tmp, local) = pushed_repo("body\n").await;
    let blob = git(&["rev-parse", "HEAD:a.txt"], Some(&local)).await;

    let error = client
        .list_tree(&id, blob.trim())
        .await
        .expect_err("a blob is not a tree");
    assert_eq!(
        status_of(&error).code(),
        tonic::Code::InvalidArgument,
        "wrong status for listing a blob: {error:?}"
    );
}

/// And a tree nobody pushed is a `NotFound`, as reading it would be.
#[tokio::test]
async fn listing_an_unknown_object_is_not_found() {
    let (client, id) = contract_with_repo().await;
    let absent = absent_oid();

    let error = client
        .list_tree(&id, &absent)
        .await
        .expect_err("an object that was never pushed has no listing");
    assert_eq!(status_of(&error).code(), tonic::Code::NotFound, "{error:?}");
}
