//! Push hooks, by [`PreReceiveMode`].
//!
//! `Commands` decides before a one-entry commit, `Objects` between the
//! objects and the refs, and no mode never calls `pre_receive`.

#![expect(
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use common::{Git, Server};
use enroute::node::Node;
use enroute_compute::{
    HookCommand, HostHooks, Judgement, PreReceiveMode, PushContext, PushHooks, RefJudgement,
    RepoState,
};
use enroute_shared::oid::Oid;
use enroute_shared::state::{Commit, RefUpdate};

/// What `pre_receive` saw.
#[derive(Clone, Debug)]
struct Pre {
    commands: Vec<HookCommand>,
    /// Per command with a new value: readable through a linearizable read,
    /// as the API reads.
    readable: Vec<bool>,
    options: Vec<String>,
}

/// A hook that allows everything but what the test says.
#[derive(Default, Debug)]
struct TestHook {
    node: OnceLock<Weak<Node>>,
    /// Refname to refusal reason.
    refuse: Mutex<HashMap<String, String>>,
    /// Refnames left unjudged.
    skip: Mutex<HashSet<String>>,
    /// In `pre_receive`, move this ref to this commit first.
    move_ref: Mutex<Option<(String, Oid)>>,
    /// Lines `post_receive` returns.
    lines: Mutex<Vec<String>>,
    pre: Mutex<Vec<Pre>>,
    post: Mutex<Vec<Vec<HookCommand>>>,
    /// The push options each `post_receive` saw.
    post_options: Mutex<Vec<Vec<String>>>,
    mode: Option<PreReceiveMode>,
}

#[async_trait::async_trait]
impl PushHooks for TestHook {
    fn pre_receive_mode(&self) -> Option<PreReceiveMode> {
        self.mode
    }

    async fn pre_receive(
        &self,
        _ctx: &PushContext,
        repo: &str,
        commands: &[HookCommand],
        options: &[String],
    ) -> anyhow::Result<Vec<RefJudgement>> {
        assert!(self.mode.is_some(), "pre_receive with no mode");
        let node = self.node.get().and_then(Weak::upgrade).unwrap();
        let compute = node.compute().unwrap();
        let snap = compute.repo_for_read(repo, true).await?.unwrap().snapshot();
        let readable = commands
            .iter()
            .filter_map(|c| c.new)
            .map(|n| snap.read_oid(&n).unwrap().is_some())
            .collect();
        self.pre.lock().unwrap().push(Pre {
            commands: commands.to_vec(),
            readable,
            options: options.to_vec(),
        });
        let moved = self.move_ref.lock().unwrap().take();
        if let Some((name, to)) = moved {
            let old = snap.meta.refs.get(&name).copied();
            let update = RefUpdate {
                name,
                old,
                new: Some(to),
            };
            let commit = Commit {
                updates: vec![update],
                ..Default::default()
            };
            let out = compute
                .commit(repo, snap.meta.epoch, None, commit)
                .await?
                .outcome;
            assert!(out.error.is_none() && out.ref_errors().all(|e| e.is_none()));
        }
        let refuse = self.refuse.lock().unwrap().clone();
        let skip = self.skip.lock().unwrap().clone();
        Ok(commands
            .iter()
            .filter(|c| !skip.contains(&c.refname))
            .map(|c| RefJudgement {
                refname: c.refname.clone(),
                judgement: match refuse.get(&c.refname) {
                    Some(r) => Judgement::Refuse(r.clone()),
                    None => Judgement::Allow,
                },
            })
            .collect())
    }

    async fn post_receive(
        &self,
        _ctx: &PushContext,
        _repo: &str,
        commands: &[HookCommand],
        options: &[String],
    ) -> anyhow::Result<Vec<String>> {
        self.post_options.lock().unwrap().push(options.to_vec());
        self.post.lock().unwrap().push(commands.to_vec());
        Ok(self.lines.lock().unwrap().clone())
    }
}

async fn start(mode: Option<PreReceiveMode>) -> (Server, Arc<TestHook>) {
    let hook = Arc::new(TestHook {
        mode,
        ..TestHook::default()
    });
    let hooks = HostHooks {
        push: Some(hook.clone()),
        ..HostHooks::default()
    };
    let server = Server::start_hooked(hooks, |c| {
        // The tests count the log entries of a push: a flush or a
        // compaction would add its own.
        c.compute.flush_interval = std::time::Duration::from_secs(3600);
        c.compute.merge_factor = 0;
    })
    .await;
    hook.node.set(Arc::downgrade(&server.node)).unwrap();
    (server, hook)
}

fn oid(hex: &str) -> Oid {
    Oid::from_hex(hex).unwrap()
}

/// The remote's value of `refname`, if any.
async fn remote(git: &Git, cwd: &Path, url: &str, refname: &str) -> Option<String> {
    let ls = git.run(cwd, &["ls-remote", url, refname]).await;
    ls.lines()
        .find(|l| l.ends_with(&format!("\t{refname}")))
        .map(|l| l.split('\t').next().unwrap().to_string())
}

/// The repository's replicated state, as the compute follows it.
///
/// A push is acknowledged before this node follows it, so catch up first.
async fn state(server: &Server, repo: &str) -> Arc<RepoState> {
    let compute = server.node.compute().unwrap();
    compute.repo_for_read(repo, true).await.unwrap();
    compute.state(repo).unwrap()
}

async fn version(server: &Server, repo: &str) -> u64 {
    state(server, repo).await.version
}

/// The layer count and the version of `repo`.
async fn layers_and_version(server: &Server, repo: &str) -> (usize, u64) {
    let state = state(server, repo).await;
    (state.layers.len(), state.version)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn allow_all_lands_and_post_receive_sees_it() {
    let (server, hook) = start(Some(PreReceiveMode::Objects)).await;
    *hook.lines.lock().unwrap() = vec!["open a merge request".into()];
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    let c1 = git.commit_file(&src, "f", "1\n", "1").await;
    let (ok, _, err) = git.try_run(&src, &["push", &url, "main"]).await;
    assert!(ok, "{err}");
    assert!(err.contains("remote: open a merge request"), "{err}");
    assert_eq!(
        remote(&git, &src, &url, "refs/heads/main").await,
        Some(c1.clone())
    );
    let pre = hook.pre.lock().unwrap().clone();
    assert_eq!(pre.len(), 1);
    // The objects were committed (not only local) before any ref moved.
    assert_eq!(pre[0].readable, vec![true]);
    let want = HookCommand {
        refname: "refs/heads/main".into(),
        old: None,
        new: Some(oid(&c1)),
        force: false,
    };
    assert_eq!(pre[0].commands, vec![want.clone()]);
    assert_eq!(*hook.post.lock().unwrap(), vec![vec![want]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_options_reach_both_hooks() {
    let (server, hook) = start(Some(PreReceiveMode::Commands)).await;
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    git.commit_file(&src, "f", "1\n", "1").await;
    let push = ["push", "-o", "ci.skip", "-o", "title=Fix it", &url, "main"];
    let (ok, _, err) = git.try_run(&src, &push).await;
    assert!(ok, "{err}");
    let sent = ["ci.skip", "title=Fix it"];
    assert_eq!(hook.pre.lock().unwrap()[0].options, sent);
    assert_eq!(hook.post_options.lock().unwrap()[0], sent);
}

/// The hook and the command of a second push of `main` in `mode`.
///
/// The second push, new objects and a ref move, must commit in one entry and
/// reach `post_receive`.
async fn second_push_in_one_entry(mode: Option<PreReceiveMode>) -> (Arc<TestHook>, HookCommand) {
    let (server, hook) = start(mode).await;
    *hook.lines.lock().unwrap() = vec!["build queued".into()];
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    let c1 = git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;

    let v = version(&server, "r").await;
    let c2 = git.commit_file(&src, "f", "2\n", "2").await;
    let (ok, _, err) = git.try_run(&src, &["push", &url, "main"]).await;
    assert!(ok, "{err}");
    assert_eq!(version(&server, "r").await - v, 1);
    assert!(err.contains("remote: build queued"), "{err}");
    let want = HookCommand {
        refname: "refs/heads/main".into(),
        old: Some(oid(&c1)),
        new: Some(oid(&c2)),
        force: false,
    };
    let last = hook.post.lock().unwrap().last().cloned();
    assert_eq!(last, Some(vec![want.clone()]));
    (hook, want)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_commits_in_one_entry() {
    let (hook, want) = second_push_in_one_entry(Some(PreReceiveMode::Commands)).await;
    let pre = hook.pre.lock().unwrap().clone();
    assert_eq!(pre.len(), 2);
    assert_eq!(pre[1].commands, vec![want]);
    // The hook saw the repository before the push.
    assert_eq!(pre[1].readable, vec![false]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn off_commits_in_one_entry_and_skips_pre_receive() {
    let (hook, _) = second_push_in_one_entry(None).await;
    assert!(hook.pre.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_refusal_commits_nothing() {
    let (server, hook) = start(Some(PreReceiveMode::Commands)).await;
    hook.refuse
        .lock()
        .unwrap()
        .insert("refs/heads/b".into(), "b is protected".into());
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    let before = layers_and_version(&server, "r").await;

    git.run(&src, &["checkout", "-q", "-b", "b"]).await;
    let c2 = git.commit_file(&src, "f", "2\n", "2").await;
    let (ok, _, err) = git.try_run(&src, &["push", &url, "b"]).await;
    assert!(!ok);
    assert!(
        err.contains("[remote rejected] b -> b (b is protected)"),
        "{err}"
    );
    assert_eq!(hook.pre.lock().unwrap().len(), 2);
    // No entry, so no layer: the refused objects were never stored.
    assert_eq!(layers_and_version(&server, "r").await, before);
    let repo = server
        .node
        .compute()
        .unwrap()
        .repo_for_read("r", true)
        .await
        .unwrap();
    let snap = repo.unwrap().snapshot();
    assert!(snap.read_oid(&oid(&c2)).unwrap().is_none());
}

/// Commands a push should show the hook: refname, old, new, force.
type Expect<'a> = Vec<(&'a str, Option<&'a String>, Option<&'a String>, bool)>;

/// `force` from the pushed commits, in one entry per push.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_force_flags() {
    let (server, hook) = start(Some(PreReceiveMode::Commands)).await;
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    let c1 = git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    let mut want = vec![("refs/heads/main".to_string(), None, Some(oid(&c1)), false)];
    let mut push = async |args: &[&str], expect: Expect<'_>| {
        let v = version(&server, "r").await;
        let mut full = vec!["push", "-q"];
        full.extend_from_slice(args);
        git.run(&src, &full).await;
        assert_eq!(version(&server, "r").await - v, 1, "{args:?}");
        want.extend(
            expect
                .into_iter()
                .map(|(r, o, n, f)| (r.to_string(), o.map(|o| oid(o)), n.map(|n| oid(n)), f)),
        );
    };

    // Several commits at once: a fast-forward.
    git.commit_file(&src, "f", "2\n", "2").await;
    git.commit_file(&src, "f", "3\n", "3").await;
    let c4 = git.commit_file(&src, "f", "4\n", "4").await;
    push(
        &[&url, "main"],
        vec![("refs/heads/main", Some(&c1), Some(&c4), false)],
    )
    .await;
    // A side branch, then a merge that pushes both parents' new commits.
    git.run(&src, &["checkout", "-q", "-b", "side", &c1]).await;
    let s1 = git.commit_file(&src, "s", "1\n", "s1").await;
    push(
        &[&url, "side"],
        vec![("refs/heads/side", None, Some(&s1), false)],
    )
    .await;
    git.commit_file(&src, "s", "2\n", "s2").await;
    git.run(&src, &["checkout", "-q", "main"]).await;
    let c5 = git.commit_file(&src, "f", "5\n", "5").await;
    git.run(&src, &["merge", "-q", "--no-edit", "side"]).await;
    let m = git.rev(&src, "HEAD").await;
    assert_ne!(m, c5);
    push(
        &[&url, "main", "main:refs/heads/side"],
        vec![
            ("refs/heads/main", Some(&c4), Some(&m), false),
            ("refs/heads/side", Some(&s1), Some(&m), false),
        ],
    )
    .await;
    // An unrelated root commit, then main's history forced over it.
    git.run(&src, &["checkout", "-q", "--orphan", "lone"]).await;
    let l1 = git.commit_file(&src, "l", "1\n", "l1").await;
    push(
        &[&url, "lone"],
        vec![("refs/heads/lone", None, Some(&l1), false)],
    )
    .await;
    git.run(&src, &["checkout", "-q", "main"]).await;
    let c6 = git.commit_file(&src, "f", "6\n", "6").await;
    push(
        &["--force", &url, "main:refs/heads/lone"],
        vec![("refs/heads/lone", Some(&l1), Some(&c6), true)],
    )
    .await;
    // Rewritten history: reset, commit, push with --force.
    git.run(&src, &["reset", "-q", "--hard", &c4]).await;
    let c7 = git.commit_file(&src, "f", "7\n", "7").await;
    push(
        &["--force", &url, "main"],
        vec![("refs/heads/main", Some(&m), Some(&c7), true)],
    )
    .await;
    // Moved back to an existing, older commit.
    push(
        &["--force", &url, &format!("{c1}:refs/heads/main")],
        vec![("refs/heads/main", Some(&c7), Some(&c1), true)],
    )
    .await;
    // Forward again to an existing commit.
    push(
        &[&url, &format!("{c4}:refs/heads/main")],
        vec![("refs/heads/main", Some(&c1), Some(&c4), false)],
    )
    .await;
    // Delete.
    push(
        &[&url, ":refs/heads/lone"],
        vec![("refs/heads/lone", Some(&c6), None, false)],
    )
    .await;

    let got: Vec<_> = hook
        .pre
        .lock()
        .unwrap()
        .iter()
        .flat_map(|p| p.commands.clone())
        .map(|c| (c.refname, c.old, c.new, c.force))
        .collect();
    assert_eq!(got, want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refuses_one_of_two() {
    let (server, hook) = start(Some(PreReceiveMode::Commands)).await;
    hook.refuse
        .lock()
        .unwrap()
        .insert("refs/heads/b".into(), "b is protected".into());
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    let c1 = git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["branch", "b"]).await;
    let (ok, _, err) = git.try_run(&src, &["push", &url, "main", "b"]).await;
    assert!(!ok);
    assert!(
        err.contains("[remote rejected] b -> b (b is protected)"),
        "{err}"
    );
    assert_eq!(remote(&git, &src, &url, "refs/heads/main").await, Some(c1));
    assert_eq!(remote(&git, &src, &url, "refs/heads/b").await, None);
    let post = hook.post.lock().unwrap().clone();
    assert_eq!(post.len(), 1);
    assert_eq!(post[0].len(), 1);
    assert_eq!(post[0][0].refname, "refs/heads/main");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_refusal_lands_nothing_and_keeps_objects() {
    let (server, hook) = start(Some(PreReceiveMode::Objects)).await;
    hook.refuse
        .lock()
        .unwrap()
        .insert("refs/heads/b".into(), "b is protected".into());
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    let c1 = git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["branch", "b"]).await;
    let (ok, _, err) = git
        .try_run(&src, &["push", "--atomic", &url, "main", "b"])
        .await;
    assert!(!ok);
    assert!(err.contains("(b is protected)"), "{err}");
    assert!(err.contains("(atomic push failure)"), "{err}");
    assert_eq!(remote(&git, &src, &url, "refs/heads/main").await, None);
    assert_eq!(remote(&git, &src, &url, "refs/heads/b").await, None);
    assert!(hook.post.lock().unwrap().is_empty());
    // Documented: a refused push's objects stay stored, readable by oid,
    // until compaction drops them as unreachable.
    let repo = server
        .node
        .compute()
        .unwrap()
        .repo_for_read("r", true)
        .await
        .unwrap();
    let snap = repo.unwrap().snapshot();
    assert!(snap.read_oid(&oid(&c1)).unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ref_moved_between_entries_is_stale() {
    let (server, hook) = start(Some(PreReceiveMode::Objects)).await;
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    git.commit_file(&src, "f", "1\n", "1").await;
    let c2 = git.commit_file(&src, "f", "2\n", "2").await;
    git.run(
        &src,
        &[
            "push",
            "-q",
            &url,
            "HEAD~1:refs/heads/main",
            "HEAD:refs/heads/other",
        ],
    )
    .await;
    git.commit_file(&src, "f", "3\n", "3").await;
    // While the hook runs, another writer moves main to c2.
    *hook.move_ref.lock().unwrap() = Some(("refs/heads/main".into(), oid(&c2)));
    let (ok, _, err) = git.try_run(&src, &["push", &url, "main"]).await;
    assert!(!ok);
    assert!(err.contains("stale info"), "{err}");
    assert_eq!(remote(&git, &src, &url, "refs/heads/main").await, Some(c2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unjudged_command_fails_the_push() {
    let (server, hook) = start(Some(PreReceiveMode::Commands)).await;
    hook.skip.lock().unwrap().insert("refs/heads/b".into());
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["branch", "b"]).await;
    let (ok, _, err) = git.try_run(&src, &["push", &url, "main", "b"]).await;
    assert!(!ok);
    assert!(
        err.contains("main -> main (pre-receive hook did not judge refs/heads/b)"),
        "{err}"
    );
    assert_eq!(remote(&git, &src, &url, "refs/heads/main").await, None);
    assert!(hook.post.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn force_and_entry_count() {
    let (server, hook) = start(Some(PreReceiveMode::Objects)).await;
    let git = Git::new();
    let url = server.repo_url("r");
    let src = git.init("src").await;
    let c1 = git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    // Fast-forward, with new objects: two entries.
    let v = version(&server, "r").await;
    let c2 = git.commit_file(&src, "f", "2\n", "2").await;
    git.run(&src, &["push", "-q", &url, "main"]).await;
    assert_eq!(version(&server, "r").await - v, 2);
    // Rewrite history: non-fast-forward.
    git.run(&src, &["reset", "-q", "--hard", &c1]).await;
    let c3 = git.commit_file(&src, "f", "3\n", "3").await;
    git.run(&src, &["push", "-q", "--force", &url, "main"])
        .await;
    // Create a branch at an existing commit: no new objects, one entry.
    let v = version(&server, "r").await;
    git.run(&src, &["push", "-q", &url, "main:refs/heads/b"])
        .await;
    assert_eq!(version(&server, "r").await - v, 1);
    // Delete it: one entry.
    let v = version(&server, "r").await;
    git.run(&src, &["push", "-q", &url, ":refs/heads/b"]).await;
    assert_eq!(version(&server, "r").await - v, 1);

    let pre = hook.pre.lock().unwrap().clone();
    let got: Vec<_> = pre
        .iter()
        .map(|p| {
            let c = &p.commands[0];
            (c.refname.as_str(), c.old, c.new, c.force)
        })
        .collect();
    let (c1, c2, c3) = (oid(&c1), oid(&c2), oid(&c3));
    assert_eq!(
        got,
        vec![
            ("refs/heads/main", None, Some(c1), false),
            ("refs/heads/main", Some(c1), Some(c2), false),
            ("refs/heads/main", Some(c2), Some(c3), true),
            ("refs/heads/b", None, Some(c3), false),
            ("refs/heads/b", Some(c3), None, false),
        ]
    );
    assert_eq!(remote(&git, &src, &url, "refs/heads/b").await, None);
}
