//! The layer sweep: stale publishes are republished, and only files that
//! nothing names are deleted.

mod common;

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use common::{Git, Server, text, wait_until};
use enroute_shared::state::{Command, Commit, UPLOAD_GEN_WINDOW, layer_gen};

/// The ids of `repo`'s layer files in the (file) object store under `root`.
fn files(root: &Path, repo: &str) -> HashSet<String> {
    let dir = root.join("store/data/repos").join(repo).join("layers");
    std::fs::read_dir(dir)
        .map(|d| {
            d.flatten()
                .filter_map(|e| {
                    e.file_name()
                        .to_str()?
                        .strip_suffix(".layer")
                        .map(String::from)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn config(root: &Path) -> enroute::node::Config {
    let mut c = enroute::node::Config::local(root.join("data"), common::local_store(root));
    c.compute.inline_layer_bytes = 0; // every push uploads a layer
    c
}

/// A slow publish whose upload generation expired is refused, then republished.
///
/// A copy within the object store moves it to the current generation, and
/// the stale copy is left to the sweep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_publishes_are_republished() {
    let root = tempfile::tempdir().unwrap();
    let mut c = config(root.path());
    c.compute.merge_factor = 0;
    c.compute.sweep_interval = Duration::from_secs(3600);
    let server = Server::start_config(c).await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("slow");
    git.commit_file(&src, "a.txt", &text(1, 20), "c1").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    let compute = server.node.compute().unwrap();
    let pushed = compute.state("slow").unwrap().layers[0].clone();

    // A layer published now (generation 0): the pushed layer's bytes again.
    let path = root
        .path()
        .join("store/data/repos/slow/layers")
        .join(format!("{}.layer", pushed.id));
    let sealed = enroute_compute::testing::Sealed::of(std::fs::read(path).unwrap().into()).unwrap();
    let layer = compute
        .publish_layer("slow", sealed, pushed.objects)
        .await
        .unwrap();
    assert_eq!(layer_gen(&layer.id), Some(0));
    // The log moves past its window before the commit.
    for _ in 0..=UPLOAD_GEN_WINDOW {
        assert!(
            compute
                .propose(Command::advance_upload_gen("slow"))
                .await
                .unwrap()
                .error
                .is_none()
        );
    }
    let out = compute
        .commit(
            "slow",
            compute.state("slow").unwrap().epoch,
            None,
            Commit {
                add_layers: vec![layer.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .outcome;
    assert!(out.error.is_none(), "{out:?}");
    // A linearizable read: the commit is acknowledged once the ledger
    // answers, maybe before this node applies it.
    let repo = compute.repo_for_read("slow", true).await.unwrap().unwrap();
    let meta = repo.snapshot().meta.clone();
    let added = meta.layers.last().unwrap();
    assert_ne!(added.id, layer.id);
    assert_eq!(layer_gen(&added.id), Some(UPLOAD_GEN_WINDOW + 1));
    assert!(
        files(root.path(), "slow").contains(&layer.id),
        "the stale copy waits for the sweep"
    );

    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
}

/// The sweep deletes what nothing names, and nothing the manifest names.
///
/// Replaced layers and uncommitted publishes go once out of the window and
/// past the retention.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sweep_deletes_only_unnamed_files() {
    let root = tempfile::tempdir().unwrap();
    let mut c = config(root.path());
    c.compute.merge_factor = 2;
    c.compute.sweep_interval = Duration::from_millis(100);
    c.compute.sweep_retention = Duration::from_millis(300);
    let server = Server::start_config(c).await;
    let git = Git::new();
    let src = git.init("src").await;
    let url = server.repo_url("swept");
    for i in 0..6 {
        git.commit_file(
            &src,
            &format!("f{}.txt", i % 3),
            &text(i, 20),
            &format!("c{i}"),
        )
        .await;
        git.run(&src, &["push", "-q", &url, "main"]).await;
    }
    let compute = server.node.compute().unwrap();
    // A publish that never committed: a copy of a layer under a fresh id.
    let some = compute.state("swept").unwrap().layers[0].id.clone();
    let orphan = enroute_compute::testing::new_layer_id(compute.state("swept").unwrap().upload_gen);
    compute
        .storage()
        .copy_layer("swept", &some, &orphan)
        .await
        .unwrap();
    assert!(files(root.path(), "swept").contains(&orphan));

    // Everything left is what the manifest names (compaction settled, the
    // sweep caught up).
    let settled = || {
        let named: HashSet<String> = compute
            .state("swept")
            .unwrap()
            .layers
            .iter()
            .map(|l| l.id.clone())
            .collect();
        named.len() <= 2 && files(root.path(), "swept") == named
    };
    assert!(
        wait_until(settled, 30).await,
        "unnamed files were not swept: {:?}",
        files(root.path(), "swept")
    );
    assert!(
        compute.state("swept").unwrap().upload_gen > UPLOAD_GEN_WINDOW,
        "the sweep advanced the generation"
    );

    // An idle repository with nothing to delete costs no log entries.
    let version = compute.state("swept").unwrap().version;
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(compute.state("swept").unwrap().version, version);

    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &url, dst.to_str().unwrap()],
    )
    .await;
    git.run(&dst, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&src, "main").await, git.rev(&dst, "main").await);
}
