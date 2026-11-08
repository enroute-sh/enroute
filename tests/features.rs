//! Git features through a node: partial and shallow clones, concurrent
//! pushes and the older protocol versions.

#![expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use common::{Git, Server, text};

async fn seeded(
    server: &Server,
    git: &Git,
    name: &str,
    commits: u64,
) -> (std::path::PathBuf, String) {
    let src = git.init(&format!("{name}-src")).await;
    for i in 0..commits {
        git.commit_file(
            &src,
            &format!("d{}/f{}.txt", i % 3, i % 5),
            &text(i, 50 + i as usize),
            &format!("c{i}"),
        )
        .await;
    }
    let url = server.repo_url(name);
    git.run(&src, &["push", "-q", &url, "main"]).await;
    (src, url)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_clone_blobless() {
    let server = Server::start().await;
    let git = Git::new();
    let (src, url) = seeded(&server, &git, "p", 12).await;
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            "--filter=blob:none",
            "--no-checkout",
            &url,
            dst.to_str().unwrap(),
        ],
    )
    .await;
    // Only commits and trees were transferred.
    let missing = git
        .run(&dst, &["rev-list", "--objects", "--all", "--missing=print"])
        .await;
    assert!(
        missing.lines().any(|l| l.starts_with('?')),
        "expected missing blobs"
    );
    // Checkout lazily fetches blobs from the promisor remote.
    git.run(&dst, &["checkout", "-q", "main"]).await;
    git.run(&dst, &["fsck"]).await;
    assert_eq!(git.rev(&src, "HEAD").await, git.rev(&dst, "HEAD").await);
    let a = std::fs::read_to_string(src.join("d0/f0.txt")).unwrap();
    let b = std::fs::read_to_string(dst.join("d0/f0.txt")).unwrap();
    assert_eq!(a, b);

    // Treeless too.
    let dst2 = git.dir("dst2");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            "--filter=tree:0",
            &url,
            dst2.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(git.rev(&src, "HEAD").await, git.rev(&dst2, "HEAD").await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partial_clone_by_blob_size() {
    let server = Server::start().await;
    let git = Git::new();
    let src = git.init("l-src").await;
    git.commit_file(&src, "small", "abc", "small").await;
    git.commit_file(&src, "exact", "abcd", "exact").await;
    let url = server.repo_url("l");
    git.run(&src, &["push", "-q", &url, "main"]).await;
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            "--filter=blob:limit=4",
            "--no-checkout",
            &url,
            dst.to_str().unwrap(),
        ],
    )
    .await;
    // As git serves it: a blob of exactly the limit is left out.
    let missing = git
        .run(&dst, &["rev-list", "--objects", "--all", "--missing=print"])
        .await;
    let small = git.rev(&src, "HEAD:small").await;
    let exact = git.rev(&src, "HEAD:exact").await;
    assert!(missing.contains(&format!("?{exact}")), "{missing}");
    assert!(!missing.contains(&format!("?{small}")), "{missing}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shallow_clone_and_deepen() {
    let server = Server::start().await;
    let git = Git::new();
    let (src, url) = seeded(&server, &git, "s", 10).await;
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", "--depth=1", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck"]).await;
    let count = git.run(&dst, &["rev-list", "--count", "HEAD"]).await;
    assert_eq!(count.trim(), "1");
    assert_eq!(
        std::fs::read_to_string(src.join("d0/f0.txt")).unwrap(),
        std::fs::read_to_string(dst.join("d0/f0.txt")).unwrap()
    );

    git.run(&dst, &["fetch", "-q", "--deepen=3"]).await;
    git.run(&dst, &["fsck"]).await;
    let count = git.run(&dst, &["rev-list", "--count", "HEAD"]).await;
    assert_eq!(count.trim(), "4");

    // New commits fetched into the shallow clone.
    git.commit_file(&src, "new.txt", "new\n", "new").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    git.run(&dst, &["pull", "-q", "--ff-only"]).await;
    git.run(&dst, &["fsck"]).await;
    assert_eq!(git.rev(&src, "HEAD").await, git.rev(&dst, "HEAD").await);

    // A second shallow clone of the new tip mixes cached re-encoded entries
    // (unchanged objects) with freshly re-encoded ones (the new commit's).
    let dst2 = git.dir("dst2");
    git.run(
        git.home.path(),
        &["clone", "-q", "--depth=1", &url, dst2.to_str().unwrap()],
    )
    .await;
    git.run(&dst2, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&src, "HEAD").await, git.rev(&dst2, "HEAD").await);
    assert_eq!(
        std::fs::read_to_string(dst2.join("new.txt")).unwrap(),
        "new\n"
    );

    git.run(&dst, &["fetch", "-q", "--unshallow"]).await;
    git.run(&dst, &["fsck", "--strict"]).await;
    let count = git.run(&dst, &["rev-list", "--count", "HEAD"]).await;
    assert_eq!(count.trim(), "11");
}

/// A depth that lands exactly on the root makes the root a shallow
/// boundary, as git's does, and deepening past it unshallows it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shallow_boundary_at_the_root() {
    let server = Server::start().await;
    let git = Git::new();
    let (src, url) = seeded(&server, &git, "r", 3).await;
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", "--depth=3", &url, dst.to_str().unwrap()],
    )
    .await;
    let root = git
        .run(&src, &["rev-list", "--max-parents=0", "HEAD"])
        .await;
    let shallow = std::fs::read_to_string(dst.join(".git/shallow")).unwrap();
    assert_eq!(shallow, root);
    git.run(&dst, &["fetch", "-q", "--deepen=1"]).await;
    let is = git
        .run(&dst, &["rev-parse", "--is-shallow-repository"])
        .await;
    assert_eq!(is.trim(), "false");
    git.run(&dst, &["fsck", "--strict"]).await;
}

/// A push may not delete the branch `HEAD` names, as git's default
/// `receive.denyDeleteCurrent` refuses it; other branches it may.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_cannot_delete_the_branch_head_names() {
    let server = Server::start().await;
    let git = Git::new();
    let (src, url) = seeded(&server, &git, "dc", 2).await;
    git.run(&src, &["push", "-q", &url, "main:refs/heads/other"])
        .await;
    let (ok, _, err) = git.try_run(&src, &["push", &url, ":refs/heads/main"]).await;
    assert!(!ok);
    assert!(
        err.contains("deletion of the current branch prohibited"),
        "{err}"
    );
    git.run(&src, &["push", "-q", &url, ":refs/heads/other"])
        .await;
    let refs = git.run(&src, &["ls-remote", &url]).await;
    assert!(refs.contains("refs/heads/main"), "{refs}");
    assert!(!refs.contains("refs/heads/other"), "{refs}");
}

/// A have does not cover the history past the client's shallow boundary, so
/// tags there must come with their commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shallow_clone_fetches_tags_past_its_boundary() {
    let server = Server::start().await;
    let git = Git::new();
    let (src, url) = seeded(&server, &git, "st", 10).await;
    git.run(&src, &["tag", "-a", "old", "-m", "tag", "HEAD~5"])
        .await;
    git.run(&src, &["tag", "light", "HEAD~7"]).await;
    git.run(&src, &["push", "-q", &url, "--tags"]).await;
    // Both walks: the plain one, and the one of `deepen-relative`.
    let fetches: [&[&str]; 2] = [&["--tags"], &["--tags", "--deepen=1"]];
    for (version, fetch) in ["2", "0"].into_iter().flat_map(|v| fetches.map(|f| (v, f))) {
        let pv = format!("protocol.version={version}");
        let dst = git.dir(&format!("st{version}{}", fetch.len()));
        git.run(
            git.home.path(),
            &[
                "-c",
                &pv,
                "clone",
                "-q",
                "--no-tags",
                "--depth=1",
                &url,
                dst.to_str().unwrap(),
            ],
        )
        .await;
        let mut args = vec!["-c", &pv, "fetch", "-q"];
        args.extend(fetch);
        args.push("origin");
        git.run(&dst, &args).await;
        git.run(&dst, &["fsck"]).await;
        for tag in ["old", "light"] {
            assert_eq!(git.rev(&src, tag).await, git.rev(&dst, tag).await);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_pushes_to_different_branches() {
    let server = Server::start().await;
    let git = Git::new();
    let (_src, url) = seeded(&server, &git, "c", 3).await;
    let mut clones = Vec::new();
    for i in 0..8 {
        let d = git.dir(&format!("w{i}"));
        git.run(git.home.path(), &["clone", "-q", &url, d.to_str().unwrap()])
            .await;
        git.run(&d, &["checkout", "-q", "-b", &format!("b{i}")])
            .await;
        for j in 0..3 {
            git.commit_file(
                &d,
                &format!("w{i}.txt"),
                &text(i * 10 + j, 40),
                &format!("w{i} {j}"),
            )
            .await;
        }
        clones.push(d);
    }
    let pushes = clones.iter().enumerate().map(|(i, d)| {
        let git = &git;
        let url = url.clone();
        async move { git.run(d, &["push", "-q", &url, &format!("b{i}")]).await }
    });
    futures::future::join_all(pushes).await;
    let check = git.dir("check");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, check.to_str().unwrap()],
    )
    .await;
    git.run(&check, &["fsck", "--strict"]).await;
    for (i, d) in clones.iter().enumerate() {
        assert_eq!(
            git.rev(d, "HEAD").await,
            git.rev(&check, &format!("origin/b{i}")).await
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_pushes_same_branch_one_wins() {
    let server = Server::start().await;
    let git = Git::new();
    let (_src, url) = seeded(&server, &git, "x", 2).await;
    let mut clones = Vec::new();
    for i in 0..4 {
        let d = git.dir(&format!("w{i}"));
        git.run(git.home.path(), &["clone", "-q", &url, d.to_str().unwrap()])
            .await;
        git.commit_file(&d, "f.txt", &text(i, 10), "x").await;
        clones.push(d);
    }
    let results = futures::future::join_all(clones.iter().map(|d| {
        let git = &git;
        let url = url.clone();
        async move { git.try_run(d, &["push", "-q", &url, "main"]).await.0 }
    }))
    .await;
    assert!(results.iter().filter(|ok| **ok).count() >= 1);
    let check = git.dir("check");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, check.to_str().unwrap()],
    )
    .await;
    git.run(&check, &["fsck", "--strict"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocol_v0_and_v1_clients() {
    let server = Server::start().await;
    let git = Git::new();
    let (src, url) = seeded(&server, &git, "old", 10).await;
    git.run(&src, &["tag", "-a", "v1", "-m", "tag"]).await;
    git.run(&src, &["push", "-q", &url, "v1"]).await;
    for version in ["0", "1"] {
        let pv = format!("protocol.version={version}");
        let dst = git.dir(&format!("v{version}"));
        git.run(
            git.home.path(),
            &["-c", &pv, "clone", "-q", &url, dst.to_str().unwrap()],
        )
        .await;
        git.run(&dst, &["fsck", "--strict"]).await;
        assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "HEAD").await);
        assert_eq!(git.rev(&src, "v1").await, git.rev(&dst, "v1").await);

        // Incremental fetch with negotiation (haves).
        git.commit_file(&src, &format!("v{version}.txt"), &text(77, 30), "more")
            .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
        git.run(&dst, &["-c", &pv, "pull", "-q", "--ff-only"]).await;
        git.run(&dst, &["fsck", "--strict"]).await;
        assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "HEAD").await);

        // Shallow clone and deepen.
        let sh = git.dir(&format!("sh{version}"));
        git.run(
            git.home.path(),
            &[
                "-c",
                &pv,
                "clone",
                "-q",
                "--depth=1",
                &url,
                sh.to_str().unwrap(),
            ],
        )
        .await;
        assert_eq!(
            git.run(&sh, &["rev-list", "--count", "HEAD"]).await.trim(),
            "1"
        );
        git.run(&sh, &["-c", &pv, "fetch", "-q", "--deepen=2"])
            .await;
        assert_eq!(
            git.run(&sh, &["rev-list", "--count", "HEAD"]).await.trim(),
            "3"
        );
        git.run(&sh, &["fsck"]).await;

        // Blobless clone with lazy fetch.
        let pc = git.dir(&format!("pc{version}"));
        git.run(
            git.home.path(),
            &[
                "-c",
                &pv,
                "clone",
                "-q",
                "--filter=blob:none",
                &url,
                pc.to_str().unwrap(),
            ],
        )
        .await;
        git.run(&pc, &["fsck"]).await;
        assert_eq!(git.rev(&src, "main").await, git.rev(&pc, "HEAD").await);
    }
}
