//! Packs for a push to another git server (`query::pack_for_push`), taken by
//! a real `git receive-pack`.

#![expect(
    clippy::format_push_string,
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::io::Write as _;
use std::path::Path;
use std::sync::{Arc, Mutex};

use common::{Git, Server, copy_dir, text};
use enroute_compute::testing::{Error, PushPack, pack_for_push};
use enroute_shared::oid::Oid;

/// The pack's bytes.
async fn bytes(pack: PushPack) -> Vec<u8> {
    let out = Arc::new(Mutex::new(Vec::new()));
    let sink = out.clone();
    pack.write(Box::new(move |b| {
        sink.lock().unwrap().extend_from_slice(b);
        Ok(())
    }))
    .await
    .unwrap();
    Arc::try_unwrap(out).unwrap().into_inner().unwrap()
}

/// One pkt-line.
fn pkt(line: &str) -> Vec<u8> {
    let mut v = format!("{:04x}", line.len() + 4).into_bytes();
    v.extend_from_slice(line.as_bytes());
    v
}

/// Run git in `dir` with `input` on stdin; whether it succeeded, and its
/// output.
async fn git_with_input(dir: &Path, args: &[&str], input: Vec<u8>) -> (bool, String) {
    let mut c = Git::cmd(dir, args);
    c.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    tokio::task::spawn_blocking(move || {
        let mut child = c.spawn().unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let writer = std::thread::spawn(move || {
            // receive-pack may stop reading early after an error.
            drop(stdin.write_all(&input));
        });
        let out = child.wait_with_output().unwrap();
        writer.join().unwrap();
        let text = String::from_utf8_lossy(&out.stdout).into_owned()
            + &String::from_utf8_lossy(&out.stderr);
        (out.status.success(), text)
    })
    .await
    .unwrap()
}

/// Push `pack` with `commands` (old, new, ref) to the bare repository at
/// `remote` through `git receive-pack`, which checks every object.
async fn receive(remote: &Path, commands: &[(&str, &str, &str)], pack: &[u8]) {
    let mut input = Vec::new();
    for (i, (old, new, name)) in commands.iter().enumerate() {
        let caps = if i == 0 { "\0report-status" } else { "" };
        input.extend(pkt(&format!("{old} {new} {name}{caps}\n")));
    }
    input.extend_from_slice(b"0000");
    input.extend_from_slice(pack);
    let args = ["-c", "receive.fsckObjects=true", "receive-pack", "."];
    let (ok, out) = git_with_input(remote, &args, input).await;
    assert!(ok, "receive-pack failed:\n{out}");
    assert!(out.contains("unpack ok"), "{out}");
    for (_, _, name) in commands {
        assert!(out.contains(&format!("ok {name}")), "{name}: {out}");
    }
}

fn oid(hex: &str) -> Oid {
    Oid::from_hex(hex).unwrap()
}

/// A remote holding an older state takes the pack for the newer one, and
/// ends up with the same refs and a sound repository.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_takes_the_pack_for_a_push() {
    let server = Server::start().await;
    let git = Git::new();
    let url = server.repo_url("pushed");
    let src = git.init("src").await;
    let mut big = text(1, 400);
    for i in 0..3 {
        git.commit_file(&src, &format!("d/f{i}"), &text(10 + i, 30), "base")
            .await;
    }
    git.commit_file(&src, "big", &big, "big").await;
    git.run(&src, &["tag", "-a", "v1", "-m", "v1"]).await;
    git.run(&src, &["push", "-q", &url, "main", "v1"]).await;
    let old = git.rev(&src, "main").await;
    let v1 = git.rev(&src, "v1").await;

    // The remote: a bare copy of that state.
    let remote = git.dir("remote");
    git.run(
        git.home.path(),
        &["clone", "-q", "--bare", &url, remote.to_str().unwrap()],
    )
    .await;

    // Small edits of a large file: deltas against what the remote holds.
    for i in 0..3 {
        big.push_str(&format!("more {i}\n"));
        git.commit_file(&src, "big", &big, &format!("edit {i}"))
            .await;
    }
    git.commit_file(&src, "d/new", "new\n", "new").await;
    git.run(&src, &["tag", "-a", "v2", "-m", "v2"]).await;
    git.run(&src, &["push", "-q", &url, "main", "v2"]).await;
    let new = git.rev(&src, "main").await;
    let v2 = git.rev(&src, "v2").await;
    let expected = git
        .run(
            &src,
            &["rev-list", "--objects", "main", "v2", "--not", &old],
        )
        .await
        .lines()
        .count();

    let snap = server
        .node
        .compute()
        .unwrap()
        .repo_for_read("pushed", true)
        .await
        .unwrap()
        .unwrap()
        .snapshot();
    // The remote advertises a tag, and a tip this repository lacks.
    let haves = [oid(&old), oid(&v1), Oid::sha1([9; 20])];
    let wants = [oid(&new), oid(&v2)];
    let zero = "0".repeat(40);
    let pack = pack_for_push(&snap, &wants, &haves).await.unwrap();
    assert_eq!(pack.objects(), expected);
    let data = bytes(pack).await;
    assert_eq!(data[8..12], u32::try_from(expected).unwrap().to_be_bytes());
    // No delta has a base outside the pack.
    let empty = git.dir("empty");
    git.run(
        git.home.path(),
        &["init", "-q", "--bare", empty.to_str().unwrap()],
    )
    .await;
    let (ok, out) = git_with_input(&empty, &["index-pack", "--stdin"], data.clone()).await;
    assert!(ok, "index-pack in an empty repository:\n{out}");
    let target = git.dir("remote-pushed");
    copy_dir(&remote, &target);
    let commands = [
        (old.as_str(), new.as_str(), "refs/heads/main"),
        (zero.as_str(), v2.as_str(), "refs/tags/v2"),
    ];
    receive(&target, &commands, &data).await;
    git.run(&target, &["fsck", "--strict"]).await;
    assert_eq!(git.rev(&target, "main").await, new);
    assert_eq!(git.rev(&target, "refs/tags/v2").await, v2);

    // What the remote holds already needs no objects.
    let pack = pack_for_push(&snap, &[oid(&old)], &haves).await.unwrap();
    assert_eq!(pack.objects(), 0);
    let data = bytes(pack).await;
    let target = git.dir("remote-same");
    copy_dir(&remote, &target);
    receive(
        &target,
        &[(zero.as_str(), old.as_str(), "refs/heads/same")],
        &data,
    )
    .await;
    assert_eq!(git.rev(&target, "same").await, old);

    // An object this repository does not hold.
    let unknown = Oid::sha1([7; 20]);
    assert!(matches!(
        pack_for_push(&snap, &[unknown], &haves).await,
        Err(Error::Missing(o)) if o == unknown
    ));
}

/// Two branches edit one file the remote holds: one new version goes whole,
/// the other as a delta against it, so the pack needs nothing outside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn versions_on_one_held_base_delta_against_each_other() {
    let server = Server::start().await;
    let git = Git::new();
    let url = server.repo_url("siblings");
    let src = git.init("src").await;
    let big = text(3, 400);
    git.commit_file(&src, "big", &big, "base").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    let old = git.rev(&src, "main").await;
    for b in ["a", "b"] {
        git.run(&src, &["checkout", "-q", "-b", b, &old]).await;
        git.commit_file(&src, "big", &format!("{big}{b}\n"), b)
            .await;
    }
    git.run(&src, &["push", "-q", &url, "a", "b"]).await;
    let (a, b) = (git.rev(&src, "a").await, git.rev(&src, "b").await);

    let snap = server
        .node
        .compute()
        .unwrap()
        .repo_for_read("siblings", true)
        .await
        .unwrap()
        .unwrap()
        .snapshot();
    let pack = pack_for_push(&snap, &[oid(&a), oid(&b)], &[oid(&old)])
        .await
        .unwrap();
    let data = bytes(pack).await;
    let empty = git.dir("empty");
    git.run(
        git.home.path(),
        &["init", "-q", "--bare", empty.to_str().unwrap()],
    )
    .await;
    let (ok, out) = git_with_input(&empty, &["index-pack", "--stdin"], data).await;
    assert!(ok, "index-pack in an empty repository:\n{out}");

    let idx = std::fs::read_dir(empty.join("objects/pack"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "idx"))
        .unwrap();
    let verify = git
        .run(&empty, &["verify-pack", "-v", idx.to_str().unwrap()])
        .await;
    let blob_a = git.rev(&src, "a:big").await;
    let blob_b = git.rev(&src, "b:big").await;
    let line = |o: &str| {
        verify
            .lines()
            .find(|l| l.starts_with(o))
            .map(|l| l.split_whitespace().skip(5).collect::<Vec<_>>().join(" "))
            .unwrap()
    };
    let (la, lb) = (line(&blob_a), line(&blob_b));
    // verify-pack shows "<depth> <base>" for a delta, nothing for a whole object.
    assert!(
        (la.is_empty() && lb == format!("1 {blob_a}"))
            || (lb.is_empty() && la == format!("1 {blob_b}")),
        "a: {la:?}, b: {lb:?}\n{verify}"
    );
}
