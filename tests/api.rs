//! Server-side commits through the API, and how they meet git.

#![expect(
    clippy::too_many_lines,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use common::{Git, Server, api, commit, delete, expecting, put, put_as};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_side_commits_interoperate_with_git() {
    let server = Server::start_with(|c| {
        c.compute.merge_factor = 4;
        c.compute.flush_interval = std::time::Duration::from_millis(200);
    })
    .await;
    let git = Git::new();
    let mut c = server.api_client();
    c.create_repo("api").await.unwrap();

    // Create a repo and history purely through the API.
    let init = commit(
        "api",
        "main",
        "init",
        vec![
            put("README.md", "hello\n"),
            put("src/lib.rs", "pub fn f() {}\n"),
            put("src/nested/deep/x.txt", "x\n"),
            put_as(
                "bin/run.sh",
                "#!/bin/sh\necho hi\n",
                api::FileMode::Executable,
            ),
        ],
    );
    let r = c.create_commit(init).await.unwrap();
    assert!(r.parent_commit_id.is_none());
    let first = r.commit_id.unwrap().hex;
    let edit = commit(
        "api",
        "main",
        "edit",
        vec![
            put("src/lib.rs", "pub fn f() { g() }\n"),
            delete("src/nested/deep/x.txt"),
            put("data.bin", vec![0u8, 1, 2, 3, 4]),
        ],
    );
    let r = c.create_commit(expecting(&first, edit)).await.unwrap();
    assert_eq!(r.parent_commit_id.unwrap().hex, first);

    // Stale CAS is rejected.
    let stale = expecting(&first, commit("api", "main", "stale", vec![put("a", "a")]));
    let e = c.create_commit(stale).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e}");
    // So are invalid requests.
    let e = c
        .create_commit(commit("api", "main", "bad", vec![put("../a", "a")]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
    // A name longer than git's fsck takes, which a push of the same tree
    // would also be refused for.
    let name = "n".repeat(5000);
    let e = c
        .create_commit(commit("api", "main", "long", vec![put(&name, "a")]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e}");
    assert!(e.message().contains("excessively large pathname"), "{e}");

    let url = server.repo_url("api");
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(
        std::fs::read_to_string(dst.join("src/lib.rs")).unwrap(),
        "pub fn f() { g() }\n"
    );
    assert!(!dst.join("src/nested").exists());
    assert_eq!(
        std::fs::read(dst.join("data.bin")).unwrap(),
        vec![0, 1, 2, 3, 4]
    );
    let mode = git.run(&dst, &["ls-tree", "HEAD", "bin/run.sh"]).await;
    assert!(mode.starts_with("100755"), "{mode}");
    let log = git.run(&dst, &["log", "--format=%s"]).await;
    assert_eq!(log, "edit\ninit\n");

    // git push on top of API commits, then an API commit on top of the push.
    git.commit_file(&dst, "from-git.txt", "git\n", "from git")
        .await;
    git.run(&dst, &["push", "-q", "origin", "main"]).await;
    c.create_commit(commit(
        "api",
        "main",
        "after push",
        vec![put("from-git.txt", "git + api\n")],
    ))
    .await
    .unwrap();
    git.run(&dst, &["pull", "-q", "--ff-only"]).await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(
        std::fs::read_to_string(dst.join("from-git.txt")).unwrap(),
        "git + api\n"
    );

    // Reads.
    assert_eq!(
        c.read_file("api", "main", "from-git.txt").await.unwrap(),
        b"git + api\n"
    );
    assert_eq!(
        c.read_file("api", &first, "src/lib.rs").await.unwrap(),
        b"pub fn f() {}\n"
    );
    let refs = c.refs("api").await.unwrap();
    assert_eq!(refs["refs/heads/main"], git.rev(&dst, "HEAD").await);
    let e = c.refs("missing").await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound, "{e}");

    // Many small API commits: compaction flushes inline layers to storage.
    for i in 0..10 {
        c.create_commit(commit(
            "api",
            "main",
            &format!("bump {i}"),
            vec![put("counter.txt", format!("{i}\n"))],
        ))
        .await
        .unwrap();
    }
    let inline = || {
        server
            .node
            .consensus()
            .unwrap()
            .repo_meta("api")
            .unwrap()
            .layers
            .iter()
            .filter(|l| !l.is_stored())
            .count()
    };
    let layers = || {
        server
            .node
            .consensus()
            .unwrap()
            .repo_meta("api")
            .unwrap()
            .layers
            .len()
    };
    common::wait_until(|| layers() <= 4 && inline() < 4, 10).await;
    assert!(inline() < 4, "inline layers were not flushed");
    git.run(&dst, &["pull", "-q", "--ff-only"]).await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(
        std::fs::read_to_string(dst.join("counter.txt")).unwrap(),
        "9\n"
    );
}

/// The identical commit on two new branches moves only the second branch.
///
/// Same tree, no parent, same second, same message: it has no new objects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_commits_on_two_branches() {
    let server = Server::start_with(|c| c.compute.merge_factor = 0).await;
    let mut c = server.api_client();
    c.create_repo("same").await.unwrap();
    let layers = || {
        server
            .node
            .consensus()
            .unwrap()
            .repo_meta("same")
            .unwrap()
            .layers
            .len()
    };
    let a = c
        .create_commit(commit("same", "a", "same", vec![put("f", "x\n")]))
        .await
        .unwrap();
    let before = layers();
    let b = c
        .create_commit(commit("same", "b", "same", vec![put("f", "x\n")]))
        .await
        .unwrap();
    if a.commit_id == b.commit_id {
        assert_eq!(layers(), before, "no objects, so no layer");
    }
    let refs = c.refs("same").await.unwrap();
    assert_eq!(refs["refs/heads/a"], a.commit_id.unwrap().hex);
    assert_eq!(refs["refs/heads/b"], b.commit_id.unwrap().hex);
}
