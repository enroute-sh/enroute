//! The data commands and `diagnose`, run as the binary against a node in
//! this process.

mod common;

use common::{Git, Server, local_store};

/// Run `enroute` with `args`; its success, stdout and stderr.
async fn enroute(args: &[&str]) -> (bool, String, String) {
    let out = tokio::process::Command::new(env!("CARGO_BIN_EXE_enroute"))
        .args(args)
        .env_remove("ENROUTE_API")
        .env_remove("ENROUTE_CONFIG")
        .env("RUST_LOG", "warn")
        .output()
        .await
        .unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Run `enroute` against `server`'s API, and fail unless it succeeds.
async fn ok(server: &Server, args: &[&str]) -> String {
    let mut all = args.to_vec();
    all.extend(["--api", server.api.as_str()]);
    let (ok, out, err) = enroute(&all).await;
    assert!(ok, "enroute {args:?} failed:\n{err}\n{out}");
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repositories_are_made_read_and_deleted() {
    let server = Server::start().await;

    let made = ok(&server, &["repo", "create", "acme/widgets"]).await;
    assert!(
        made.starts_with("acme/widgets\trefs/heads/main\tnever pushed"),
        "{made}"
    );
    ok(
        &server,
        &["repo", "create", "acme/gears", "--default-branch", "trunk"],
    )
    .await;
    ok(&server, &["repo", "create", "other"]).await;

    let listed = ok(&server, &["repo", "list", "--prefix", "acme/"]).await;
    let keys: Vec<&str> = listed
        .lines()
        .filter_map(|l| l.split('\t').next())
        .collect();
    assert_eq!(keys, ["acme/gears", "acme/widgets"], "{listed}");

    let json: serde_json::Value =
        serde_json::from_str(&ok(&server, &["repo", "list", "--json"]).await).unwrap();
    assert_eq!(json.as_array().unwrap().len(), 3, "{json}");
    let got = ok(&server, &["repo", "get", "acme/gears", "--json"]).await;
    let got: serde_json::Value = serde_json::from_str(&got).unwrap();
    assert_eq!(got["default_branch"], "refs/heads/trunk");

    ok(&server, &["repo", "delete", "other"]).await;
    let (found, _, err) = enroute(&["repo", "get", "other", "--api", &server.api]).await;
    assert!(!found);
    assert!(err.contains("NotFound"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_lands_where_git_sees_it() {
    let server = Server::start().await;
    let git = Git::new();
    let local = git.dir("payload.bin");
    std::fs::write(&local, [0u8, 1, 2]).unwrap();
    ok(&server, &["repo", "create", "acme/site"]).await;

    let made = ok(
        &server,
        &[
            "commit",
            "acme/site",
            "--branch",
            "main",
            "-m",
            "first",
            "--author",
            "Ada <ada@example.com>",
            "--expect-absent",
            "--put",
            "README.md=hello",
            "--put",
            &format!("data.bin=@{}", local.display()),
            "--put",
            "run.sh=echo",
            "--exec",
            "run.sh",
            "--symlink",
            "link=README.md",
        ],
    )
    .await;
    let oid = made
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("commit "))
        .unwrap()
        .to_string();
    assert!(made.contains("parent none"), "{made}");

    let refs = ok(&server, &["refs", "acme/site"]).await;
    assert_eq!(refs.trim(), format!("{oid}\trefs/heads/main"));
    // A stale expectation is refused.
    let (moved, _, err) = enroute(&[
        "commit",
        "acme/site",
        "--branch",
        "main",
        "-m",
        "x",
        "--author",
        "A <a@b>",
        "--expect-absent",
        "--delete",
        "README.md",
        "--api",
        &server.api,
    ])
    .await;
    assert!(!moved);
    assert!(err.contains("FailedPrecondition"), "{err}");

    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &[
            "clone",
            "-q",
            &server.repo_url("acme/site"),
            dst.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(dst.join("README.md")).unwrap(),
        "hello"
    );
    assert_eq!(std::fs::read(dst.join("data.bin")).unwrap(), [0, 1, 2]);
    let tree = git.run(&dst, &["ls-tree", "HEAD"]).await;
    assert!(
        tree.contains("100755 blob") && tree.contains("120000 blob"),
        "{tree}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diagnose_reports_a_reachable_store() {
    let dir = tempfile::tempdir().unwrap();
    let _server = Server::start_config(enroute::node::Config::local(
        dir.path().join("data"),
        local_store(dir.path()),
    ))
    .await;
    let store = common::store_uri(dir.path());

    let (fine, out, err) = enroute(&["diagnose", "--bucket", &store]).await;
    assert!(fine, "{err}\n{out}");
    assert!(out.contains("ok    object store: reachable"), "{out}");

    let (fine, out, _) = enroute(&["diagnose", "--bucket", &store, "--json"]).await;
    assert!(fine);
    let rows: serde_json::Value = serde_json::from_str(&out).unwrap();
    assert!(
        rows.as_array().unwrap().iter().all(|r| r["ok"] == true),
        "{rows}"
    );

    let (fine, _, err) = enroute(&["diagnose"]).await;
    assert!(!fine);
    assert!(err.contains("--bucket"), "{err}");
}
