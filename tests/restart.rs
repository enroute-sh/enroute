//! Restarts: local disk does not outlive the process, so a restarted node
//! starts as a fresh one.

#![expect(
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use common::{Git, Server, copy_dir, local_store, own_copies, push_commits, text, wait_stored};
use enroute::node::Config;

/// Push a repository with enough history to span many chunks, and wait
/// until its layers are in the object store.
async fn seed(
    git: &Git,
    root: &std::path::Path,
    store: &enroute::node::Store,
) -> std::path::PathBuf {
    let src = git.init("src").await;
    for i in 0..20u64 {
        git.commit_file(
            &src,
            &format!("d{}/f{}.txt", i % 3, i % 5),
            &text(i, 400),
            &format!("c{i}"),
        )
        .await;
    }
    let mut config = own_copies(Config::local(root.join("seed"), store.clone()));
    config.compute.flush_interval = std::time::Duration::from_millis(200);
    let server = Server::start_config(config).await;
    git.run(&src, &["push", "-q", &server.repo_url("r"), "main"])
        .await;
    wait_stored(&server, "r").await;
    // Stopped, so its stores can be copied.
    server.node.shutdown().await;
    src
}

fn files_with(dir: &std::path::Path, suffix: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            if e.file_type().unwrap().is_dir() {
                stack.push(e.path());
            } else if e.path().to_string_lossy().ends_with(suffix) {
                out.push(e.path());
            }
        }
    }
    out
}

/// A restart empties the local disk: local copies, scratch and the
/// catalog of older versions go, and layers are fetched again.
///
/// The old files are not read, so damage to them does not matter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_starts_with_an_empty_local_disk() {
    let git = Git::new();
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let src = seed(&git, root.path(), &store).await;
    let data = root.path().join("seed");
    let local = data.join("local");
    let layers = files_with(&local, ".layer");
    assert!(!layers.is_empty(), "local copies kept on disk");
    for l in &layers {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new().write(true).open(l).unwrap();
        f.write_all_at(&[0x5a; 64], 200).unwrap();
    }
    std::fs::write(local.join("tmp/junk"), b"x").unwrap();

    let server = Server::start_config(own_copies(Config::local(data, store))).await;
    assert!(!local.join("tmp/junk").exists(), "scratch removed");
    assert!(
        layers.iter().all(|l| !l.exists()),
        "old local copies moved aside"
    );
    let wiped = || {
        std::fs::read_dir(&local)
            .unwrap()
            .flatten()
            .all(|e| !e.file_name().to_string_lossy().starts_with("wiped-"))
    };
    assert!(
        common::wait_until(wiped, 10).await,
        "old local files deleted"
    );
    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &server.repo_url("r"), dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "HEAD").await);
    let stats = server.node.compute().unwrap().storage().status();
    assert!(
        stats["local_copies"]["remote_opens"].as_u64().unwrap() > 0,
        "{stats}"
    );
}

/// A fresh node with only the log rebuilds its private state from the store.
///
/// It serves layers by range at once, and with no disk budget copies them
/// whole in the background.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_node_copies_layers_in_the_background() {
    let git = Git::new();
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let src = seed(&git, root.path(), &store).await;
    let data = root.path().join("fresh");
    copy_dir(&root.path().join("seed/log"), &data.join("log"));
    copy_dir(&root.path().join("seed/state"), &data.join("state"));
    let server = Server::start_config(own_copies(Config::local(data, store))).await;
    common::clone_matches(&git, &src, &server.repo_url("r"), "dst").await;
    let stat = |k: &str| {
        server.node.compute().unwrap().storage().status()["local_copies"][k]
            .as_u64()
            .unwrap()
    };
    let opened = stat("remote_opens");
    assert!(opened > 0);
    assert!(
        common::wait_until(|| stat("hydrations") == opened, 10).await,
        "{} of {opened} copies completed",
        stat("hydrations")
    );
}

/// The inode and the length of each file under `dir` that ends in `.layer`.
fn layer_files(dir: &std::path::Path) -> Vec<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    files_with(dir, ".layer")
        .iter()
        .map(|p| {
            let m = std::fs::metadata(p).unwrap();
            (m.ino(), m.len())
        })
        .collect()
}

/// With a `file://` store on the same filesystem as the node's data, the
/// local copy of each layer is a hard link to the stored file.
///
/// That holds for flushed and compacted layers, and after a restart (which
/// links them again), so the layers are on disk once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_copies_are_links_to_a_local_store() {
    use std::os::unix::fs::MetadataExt;
    let git = Git::new();
    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let data = root.path().join("node");
    let config = || {
        let mut c = Config::local(data.clone(), store.clone());
        c.compute.merge_factor = 2;
        c.compute.flush_interval = std::time::Duration::from_millis(200);
        c
    };
    let src = git.init("src").await;
    let live = |server: &Server| {
        let meta = server.node.consensus().unwrap().repo_meta("r").unwrap();
        assert!(
            meta.layers
                .iter()
                .all(enroute_shared::proto::LayerMeta::is_stored)
        );
        meta.layers.into_iter().map(|l| l.id).collect::<Vec<_>>()
    };
    let linked = |ids: &[String]| {
        for id in ids {
            let local = data.join(format!("local/layers/r/{id}.layer"));
            let stored = root
                .path()
                .join(format!("store/data/repos/r/layers/{id}.layer"));
            let (local, stored) = (
                std::fs::metadata(&local).unwrap(),
                std::fs::metadata(&stored).unwrap(),
            );
            assert_eq!(local.ino(), stored.ino(), "layer {id} is on disk twice");
        }
    };
    {
        let server = Server::start_config(config()).await;
        push_commits(&git, &src, &server.repo_url("r"), 0, 30).await;
        let compacted = || {
            let meta = server.node.consensus().unwrap().repo_meta("r").unwrap();
            meta.layers
                .iter()
                .all(enroute_shared::proto::LayerMeta::is_stored)
                && meta.layers.iter().any(|l| l.bytes >= 4 << 10)
        };
        assert!(common::wait_until(compacted, 20).await, "no compaction");
        common::clone_matches(&git, &src, &server.repo_url("r"), "one").await;
        linked(&live(&server));
    }
    let server = Server::start_config(config()).await;
    common::clone_matches(&git, &src, &server.repo_url("r"), "two").await;
    let ids = live(&server);
    linked(&ids);
    let links = server.node.compute().unwrap().storage().status()["local_copies"]["links"]
        .as_u64()
        .unwrap();
    assert!(links > 0);

    // Every local copy is a name of a stored file, so it adds no bytes.
    let stored = layer_files(&root.path().join("store"));
    let local = layer_files(&data.join("local"));
    let mut unique: Vec<(u64, u64)> = stored.iter().chain(&local).copied().collect();
    unique.sort_unstable();
    unique.dedup();
    let bytes = |files: &[(u64, u64)]| files.iter().map(|(_, len)| len).sum::<u64>();
    assert!(bytes(&local) > 0);
    assert_eq!(
        bytes(&unique),
        bytes(&stored),
        "local copies of {} bytes are not all links",
        bytes(&local)
    );
}
