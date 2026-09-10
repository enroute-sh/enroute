//! Differential state-machine test: two simulated clients alternate staging,
//! committing, and pushing, each through a enroute repo and a `git daemon` oracle.
//!
//! `git daemon` is ground truth, so the reference model only needs to track
//! which files currently exist, never to be a content oracle itself.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "e2e verification binary, never compiled into the production build"
)]

use std::sync::OnceLock;

use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
use tokio::runtime::{Handle, Runtime};

/// One process-wide runtime, shared by every `init_test()` call via
/// [`shared_runtime_handle`].
///
/// A fresh per-`Sut` runtime dropped at test-case end can strand the shared
/// Postgres pool's later runtimes without connections.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn shared_runtime_handle() -> Handle {
    RUNTIME
        .get_or_init(|| Runtime::new().expect("creating shared e2e runtime"))
        .handle()
        .clone()
}

mod contract;
mod daemon;
mod git_http;
mod grpc;
mod hooks;
mod maintenance;
mod model;
mod password_hint;
mod pre_receive;
mod support;
mod visible_refs;
use daemon::OracleDaemon;
use enroute::maintenance::Maintenance;
use model::{
    ClientId, ModifyOp, OracleStateMachine, Patience, RefState, ResyncKind, SyncKind, Transition,
    WorkTree, copy_destination,
};
use support::{
    ALICE, git, init_tracing, make_state, read_graph, read_missing_objects, read_tags, spawn_server,
};

/// Apply a `ModifyOp` to the file at `path`, reading and rewriting whatever
/// content is already there.
fn apply_modify_op(path: &std::path::Path, op: &ModifyOp) {
    match op {
        ModifyOp::Set(data) => std::fs::write(path, data).unwrap(),
        ModifyOp::Append(data) => {
            let mut existing = std::fs::read(path).unwrap();
            existing.extend_from_slice(data);
            std::fs::write(path, existing).unwrap();
        }
        ModifyOp::Splice { at, len, data } => {
            let mut existing = std::fs::read(path).unwrap();
            let at = if existing.is_empty() {
                0
            } else {
                at % existing.len()
            };
            let len = (*len).min(existing.len() - at);
            existing.splice(at..at + len, data.iter().copied());
            std::fs::write(path, existing).unwrap();
        }
    }
}

/// Create the directories leading to `path`, since a generated path may name
/// a directory no earlier transition happened to create.
fn ensure_parent(path: &std::path::Path) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
}

/// Drop the directories above a path that has just been removed, for as long
/// as they are empty.
///
/// Git tracks no empty directory, so leaving one would go unnoticed until a
/// later transition put a *file* at the freed name and hit it on disk.
fn prune_empty_dirs(removed: &std::path::Path, root: &std::path::Path) {
    let mut dir = removed.parent();
    while let Some(current) = dir {
        if current == root || !current.starts_with(root) || std::fs::remove_dir(current).is_err() {
            return;
        }
        dir = current.parent();
    }
}

/// Copy a directory recursively, preserving content exactly so the copy's
/// trees hash to the same oids as the original's.
fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Clone `url` into `at`, for a repository the reference model does not
/// track — a read of the whole history, checked by git as it arrives.
async fn clone_into(at: &std::path::Path, url: &str) {
    git(
        &[
            "-c",
            "protocol.version=2",
            "clone",
            url,
            at.to_str().unwrap(),
        ],
        None,
    )
    .await;
}

/// The real git branch name for a reference-model branch id.
fn branch_name(branch_id: u32) -> String {
    format!("wip-{branch_id}")
}

/// A short, timing-report-friendly name for a `SyncKind`.
///
/// Composed from its three independent axes rather than hand-matched per
/// combination, so the count doesn't double every time one gains a variant.
fn sync_op_name(sync: SyncKind) -> String {
    let resync = match sync.resync {
        ResyncKind::Pull => "pull",
        ResyncKind::Fresh => "fresh",
    };
    let filter = if sync.filter.is_some() {
        "_filtered"
    } else {
        ""
    };
    let depth = if sync.depth.is_some() { "_shallow" } else { "" };
    format!("sync_{resync}{filter}{depth}")
}

// System under test: two clients, each with its own clone of both enroute and
// the real `git daemon` — four local repos total, fully isolated from each other.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Remote {
    Enroute,
    Oracle,
}

impl Remote {
    const ALL: [Remote; 2] = [Remote::Enroute, Remote::Oracle];

    fn name(self) -> &'static str {
        match self {
            Remote::Enroute => "enroute",
            Remote::Oracle => "oracle",
        }
    }
}

struct Sut {
    rt: Handle,
    /// Where every clone of this case lives, kept alive so the paths below
    /// keep pointing at something.
    tmp: tempfile::TempDir,
    a_enroute: std::path::PathBuf,
    a_oracle: std::path::PathBuf,
    b_enroute: std::path::PathBuf,
    b_oracle: std::path::PathBuf,
    enroute_url: String,
    oracle_daemon: OracleDaemon,
    /// Stopped when this case ends.
    ///
    /// Each pins the Postgres connection its `pg_temp` tenancy lives in.
    _servers: support::Servers,
    current: std::cell::Cell<ClientId>,
    /// Paths currently holding a submodule gitlink, staged directly via
    /// `git update-index` rather than the working tree.
    ///
    /// `commit()` excludes them from `git add -A` so a gitlink with nothing
    /// on disk is never misread as a deleted path.
    submodule_paths: std::cell::RefCell<std::collections::BTreeSet<String>>,
    /// What a maintenance pass runs against, and which repository is this
    /// case's — the store is shared with every other case in the run.
    storage: enroute::Storage,
    repo: enroute_git_retrieve::RepoMetadata,
    /// How many verification clones this case has taken, so each gets a
    /// directory of its own.
    verifications: std::cell::Cell<u32>,
}

impl Sut {
    fn dir(&self, client: ClientId, remote: Remote) -> &std::path::Path {
        match (client, remote) {
            (ClientId::A, Remote::Enroute) => &self.a_enroute,
            (ClientId::A, Remote::Oracle) => &self.a_oracle,
            (ClientId::B, Remote::Enroute) => &self.b_enroute,
            (ClientId::B, Remote::Oracle) => &self.b_oracle,
        }
    }

    fn url(&self, remote: Remote) -> &str {
        match remote {
            Remote::Enroute => &self.enroute_url,
            Remote::Oracle => &self.oracle_daemon.url,
        }
    }

    /// Clear a submodule's checked-out directory at `name`, if there is one.
    ///
    /// A gitlink checks out as an empty directory, so every op that writes,
    /// moves or removes a file there has to displace it first.
    fn displace_submodule(&self, name: &str) {
        self.submodule_paths.borrow_mut().remove(name);
        let client = self.current.get();
        for remote in Remote::ALL {
            let path = self.dir(client, remote).join(name);
            if path.is_dir() {
                std::fs::remove_dir(&path).unwrap();
            }
        }
    }

    /// Stage the creation of a brand-new file for the current client, applied
    /// identically in both of its per-remote repos.
    fn create_file(&self, name: &str, content: &[u8]) {
        self.displace_submodule(name);
        let client = self.current.get();
        for remote in Remote::ALL {
            let path = self.dir(client, remote).join(name);
            ensure_parent(&path);
            std::fs::write(path, content).unwrap();
        }
    }

    /// Stage a change to an existing file's content, resolved against
    /// whatever is actually on disk right now.
    ///
    /// `name` may be a submodule path again, since reverting a file that was
    /// once a gitlink checks the gitlink back out as an empty directory.
    fn modify_file(&self, name: &str, op: &ModifyOp) {
        let displaced = self
            .dir(self.current.get(), Remote::Enroute)
            .join(name)
            .is_dir();
        self.displace_submodule(name);
        let client = self.current.get();
        for remote in Remote::ALL {
            let path = self.dir(client, remote).join(name);
            // A displaced gitlink leaves no file, and the op reads the empty
            // base it would read for a file with nothing in it.
            if displaced {
                std::fs::write(&path, b"").unwrap();
            }
            apply_modify_op(&path, op);
        }
    }

    /// Stage `name` as it stood before its most recent change.
    ///
    /// A file with no earlier version stages nothing, absorbed by an empty commit.
    #[tracing::instrument(skip_all)]
    fn revert_file(&self, name: &str) {
        let client = self.current.get();
        let run = |remote: Remote| async move {
            let local = self.dir(client, remote);
            let touched = git(&["log", "--format=%H", "-2", "--", name], Some(local)).await;
            let Some(previous) = touched.lines().nth(1).map(str::to_owned) else {
                return;
            };
            let listed = git(
                &["ls-tree", "--name-only", &previous, "--", name],
                Some(local),
            )
            .await;
            if listed.trim().is_empty() {
                return;
            }
            git(&["checkout", &previous, "--", name], Some(local)).await;
        };
        self.rt
            .block_on(async { tokio::join!(run(Remote::Enroute), run(Remote::Oracle)) });
    }

    /// The current client's copy of `name` on disk.
    ///
    /// Only scenarios read one back; the state machine's oracle is `git daemon`.
    #[cfg(test)]
    fn read_file(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.dir(self.current.get(), Remote::Enroute).join(name)).unwrap()
    }

    /// Stage the deletion of an existing file in the working tree.
    ///
    /// `name` may be a submodule path again, for the reason
    /// [`Self::modify_file`] gives, so disk decides which removal this is.
    ///
    /// # Asked once
    /// [`Self::displace_submodule`] is already both remotes', so calling it
    /// from in here would take the second one's copy away before its turn.
    fn delete_file(&self, name: &str) {
        let client = self.current.get();
        let displaced = self.dir(client, Remote::Enroute).join(name).is_dir();
        self.submodule_paths.borrow_mut().remove(name);
        for remote in Remote::ALL {
            let root = self.dir(client, remote);
            let path = root.join(name);
            if displaced {
                std::fs::remove_dir(&path).unwrap();
            } else {
                std::fs::remove_file(&path).unwrap();
            }
            prune_empty_dirs(&path, root);
        }
    }

    /// Stage a copy of a whole directory at a new path.
    ///
    /// The copy is byte-identical, so git stores one tree under both paths.
    #[tracing::instrument(skip_all)]
    fn copy_dir(&self, from: &str, to: &str) {
        let client = self.current.get();
        for remote in Remote::ALL {
            let root = self.dir(client, remote);
            copy_tree(&root.join(from), &root.join(to));
        }
    }

    /// Stage the rename of an existing file in the working tree.
    ///
    /// `from` may be a submodule path again, and moving the empty directory
    /// a gitlink checks out as would store nothing at `to`.
    fn move_file(&self, from: &str, to: &str) {
        self.submodule_paths.borrow_mut().remove(to);
        let client = self.current.get();
        let displaced = self.dir(client, Remote::Enroute).join(from).is_dir();
        self.displace_submodule(from);
        for remote in Remote::ALL {
            let local = self.dir(client, remote);
            let source = local.join(from);
            // What moves is the empty file a displaced gitlink reads as, so
            // `to` is the file the model says it is.
            if displaced {
                std::fs::write(&source, b"").unwrap();
            }
            let target = local.join(to);
            ensure_parent(&target);
            if target.is_dir() {
                std::fs::remove_dir(&target).unwrap();
            }
            std::fs::rename(&source, target).unwrap();
            prune_empty_dirs(&source, local);
        }
    }

    /// Stage a submodule gitlink at `path` pointing at `oid`.
    ///
    /// A mode-160000 tree entry staged via `git update-index`; `oid` never has
    /// to resolve to a stored object, which push connectivity must tolerate.
    fn submodule_set(&self, path: &str, oid: &str) {
        self.submodule_paths.borrow_mut().insert(path.to_string());
        let client = self.current.get();
        for remote in Remote::ALL {
            let root = self.dir(client, remote);
            let local = root.join(path);
            if local.is_file() {
                std::fs::remove_file(&local).unwrap();
                prune_empty_dirs(&local, root);
            }
        }
        let cacheinfo = format!("160000,{oid},{path}");
        let run = |remote: Remote| {
            let cacheinfo = &cacheinfo;
            async move {
                let local = self.dir(client, remote);
                git(
                    &["update-index", "--add", "--cacheinfo", cacheinfo],
                    Some(local),
                )
                .await;
            }
        };
        self.rt
            .block_on(async { tokio::join!(run(Remote::Enroute), run(Remote::Oracle)) });
    }

    /// Stage the removal of an existing submodule gitlink.
    fn submodule_remove(&self, path: &str) {
        self.submodule_paths.borrow_mut().remove(path);
        let client = self.current.get();
        let run = |remote: Remote| async move {
            let local = self.dir(client, remote);
            git(&["update-index", "--force-remove", path], Some(local)).await;
        };
        self.rt
            .block_on(async { tokio::join!(run(Remote::Enroute), run(Remote::Oracle)) });
    }

    /// Commit whatever is staged for the current client, in both per-remote
    /// repos, not pushed yet.
    ///
    /// Every `submodule_paths` entry is excluded from `add -A` so a gitlink
    /// with nothing on disk isn't silently un-staged as deleted.
    #[tracing::instrument(skip_all)]
    fn commit(&self, commit_id: Option<u32>) {
        let client = self.current.get();
        let message = match commit_id {
            Some(id) => format!("turn\n\nCommit-Id: {id}"),
            None => "turn".to_string(),
        };
        let exclusions: Vec<String> = self
            .submodule_paths
            .borrow()
            .iter()
            .map(|path| format!(":!{path}"))
            .collect();
        let run = |remote: Remote| {
            let message = &message;
            let exclusions = &exclusions;
            async move {
                let local = self.dir(client, remote);
                let mut add_args = vec!["add", "-A", "--", "."];
                add_args.extend(exclusions.iter().map(String::as_str));
                git(&add_args, Some(local)).await;
                git(&["commit", "--allow-empty", "-m", message], Some(local)).await;
            }
        };
        // The two remotes are independent working trees, so commit to both
        // concurrently instead of paying for four sequential git spawns.
        self.rt
            .block_on(async { tokio::join!(run(Remote::Enroute), run(Remote::Oracle)) });
    }

    /// Branch off the current client's `main` HEAD and check it out in both
    /// per-remote repos, so subsequent transitions land on it.
    ///
    /// `branch_id` names the real branch, so the SUT needs no naming state.
    #[tracing::instrument(skip_all)]
    fn branch_start(&self, branch_id: u32) {
        let client = self.current.get();
        let name = branch_name(branch_id);
        let run = |remote: Remote| {
            let name = &name;
            async move {
                let local = self.dir(client, remote);
                git(&["checkout", "-b", name], Some(local)).await;
            }
        };
        self.rt
            .block_on(async { tokio::join!(run(Remote::Enroute), run(Remote::Oracle)) });
    }

    /// Merge the branch identified by `branch_id` back into `main` with
    /// `--no-ff`, then delete the branch.
    ///
    /// Nothing else touches `main` while the branch is checked out, so this
    /// can never hit a real merge conflict.
    #[tracing::instrument(skip_all)]
    fn branch_merge(&self, branch_id: u32, commit_id: Option<u32>) {
        let client = self.current.get();
        let name = branch_name(branch_id);
        let message = match commit_id {
            Some(id) => format!("merge\n\nCommit-Id: {id}"),
            None => "merge".to_string(),
        };
        let run = |remote: Remote| {
            let name = &name;
            let message = &message;
            async move {
                let local = self.dir(client, remote);
                git(&["checkout", "main"], Some(local)).await;
                git(&["merge", "--no-ff", "-m", message, name], Some(local)).await;
                git(&["branch", "-d", name], Some(local)).await;
            }
        };
        self.rt
            .block_on(async { tokio::join!(run(Remote::Enroute), run(Remote::Oracle)) });
    }

    /// Tag an existing commit, resolved by its `Commit-Id: N` trailer rather
    /// than by walking to a specific ref.
    ///
    /// The commit may not be checked out, or reachable except via `main`,
    /// if made on a since-merged or since-deleted wip branch.
    #[tracing::instrument(skip_all)]
    fn create_tag(&self, commit_id: u32, name: &str, annotated: bool) {
        let client = self.current.get();
        let run = |remote: Remote| async move {
            let local = self.dir(client, remote);
            let grep = format!("^Commit-Id: {commit_id}$");
            let oid = git(
                &["log", "--all", "--format=%H", "--grep", &grep, "-n1"],
                Some(local),
            )
            .await;
            let oid = oid.trim();
            assert!(
                !oid.is_empty(),
                "no commit found with Commit-Id: {commit_id}"
            );
            if annotated {
                git(&["tag", "-a", name, "-m", "tag", oid], Some(local)).await;
            } else {
                git(&["tag", name, oid], Some(local)).await;
            }
        };
        self.rt
            .block_on(async { tokio::join!(run(Remote::Enroute), run(Remote::Oracle)) });
    }

    /// Push the current client's commits to each remote, then hand the turn
    /// to the other client, who resyncs both repos first, per `sync`.
    #[tracing::instrument(skip_all)]
    fn end_turn(&self, sync: SyncKind) {
        let client = self.current.get();
        let push = |remote: Remote| async move {
            let local = self.dir(client, remote);
            git(
                &[
                    "-c",
                    "protocol.version=2",
                    "push",
                    "origin",
                    "HEAD:refs/heads/main",
                    // Wildcard refspec pushes every local tag; a no-op if
                    // none exist yet, so safe to include unconditionally.
                    "refs/tags/*:refs/tags/*",
                ],
                Some(local),
            )
            .await;
        };
        self.rt
            .block_on(async { tokio::join!(push(Remote::Enroute), push(Remote::Oracle)) });

        let next = client.other();
        self.current.set(next);
        self.rt.block_on(async {
            tokio::join!(
                self.sync(next, Remote::Enroute, sync),
                self.sync(next, Remote::Oracle, sync)
            )
        });
    }

    /// Sync one (client, remote) repo per `sync`: an incremental fetch +
    /// fast-forward merge, or a from-scratch wipe-and-clone.
    ///
    /// Uses `fetch` + `merge --ff-only` rather than `git pull`, since
    /// filter/depth passthrough is only guaranteed on `fetch`.
    #[tracing::instrument(skip_all, fields(op = sync_op_name(sync)))]
    async fn sync(&self, client: ClientId, remote: Remote, sync: SyncKind) {
        let local = self.dir(client, remote);
        let filter_arg = sync
            .filter
            .map(|filter| format!("--filter={}", filter.spec()));
        let depth_arg = sync.depth.map(|depth| format!("--depth={depth}"));

        match sync.resync {
            ResyncKind::Pull => {
                let mut args = vec!["-c", "protocol.version=2", "fetch", "--tags"];
                if let Some(filter_arg) = &filter_arg {
                    args.push(filter_arg);
                }
                if let Some(depth_arg) = &depth_arg {
                    args.push(depth_arg);
                }
                args.push("origin");
                args.push("main");
                git(&args, Some(local)).await;
                git(&["merge", "--ff-only", "FETCH_HEAD"], Some(local)).await;
            }
            ResyncKind::Fresh => {
                std::fs::remove_dir_all(local).unwrap();
                // `--no-tags`, then an explicit `fetch --tags`: enroute doesn't
                // implement `clone`'s `include-tag` auto-bundling, so a plain
                // tagged clone would silently miss the tag.
                let mut args = vec!["-c", "protocol.version=2", "clone", "--no-tags"];
                if let Some(filter_arg) = &filter_arg {
                    args.push(filter_arg);
                }
                if let Some(depth_arg) = &depth_arg {
                    args.push(depth_arg);
                }
                args.push(self.url(remote));
                args.push(local.to_str().unwrap());
                git(&args, None).await;
                git(
                    &["-c", "protocol.version=2", "fetch", "--tags", "origin"],
                    Some(local),
                )
                .await;
            }
        }
    }

    /// Run a maintenance pass over this case's repository, then read the whole
    /// repository back from scratch and check it against the oracle.
    ///
    /// The clients' clones already hold every object, so only a clone into a
    /// fresh directory re-reads what a pass rewrote.
    #[tracing::instrument(skip_all)]
    fn maintain(&self, patience: Patience) {
        let config = match patience {
            Patience::Patient => Maintenance::default(),
            // Nothing is left for a later pass: retired rows are dropped and
            // their objects swept in this one. Safe here because this case's
            // repository is its own, and the state machine pushes nothing
            // while a pass runs.
            Patience::Impatient => Maintenance {
                grace_secs: 0,
                deleted_grace_secs: 0,
                dry_run: false,
            },
        };

        let scratch = self.tmp.path().join(format!(
            "verify-{}",
            self.verifications.replace(self.verifications.get() + 1)
        ));
        let enroute_at = scratch.join("enroute");
        let oracle_at = scratch.join("oracle");

        self.rt.block_on(async {
            enroute::maintenance::run_repo(&self.storage, &self.repo, config)
                .await
                .expect("a maintenance pass");

            // Two clones of the same history, one from each remote, neither
            // of them a repository the model tracks.
            tokio::join!(
                clone_into(&enroute_at, &self.enroute_url),
                clone_into(&oracle_at, &self.oracle_daemon.url),
            );
        });

        let (enroute, oracle) = self.rt.block_on(async {
            tokio::join!(
                async { (read_graph(&enroute_at).await, read_tags(&enroute_at).await,) },
                async { (read_graph(&oracle_at).await, read_tags(&oracle_at).await) },
            )
        });

        assert_eq!(
            enroute.0, oracle.0,
            "a clone after maintenance disagreed with the oracle about the commit graph"
        );
        assert_eq!(
            enroute.1, oracle.1,
            "a clone after maintenance disagreed with the oracle about tags"
        );
    }

    /// Assert each client's enroute repo agrees with that same client's oracle
    /// repo — checked per client, not "all four equal" across one reference.
    ///
    /// Per-client scoping is what `SyncKind::depth` requires: two clients can
    /// independently reach different, equally correct depths.
    #[tracing::instrument(skip_all)]
    fn assert_converged(&self) {
        let read = |client: ClientId, remote: Remote| async move {
            let dir = self.dir(client, remote);
            let (graph, tags, missing) =
                tokio::join!(read_graph(dir), read_tags(dir), read_missing_objects(dir));
            RepoSnapshot {
                graph,
                tags,
                missing,
            }
        };

        // All four repos are read-only here and independent of each other,
        // so fetch their commit graphs, tags, and missing-object sets
        // concurrently.
        let (a_enroute, a_oracle, b_enroute, b_oracle) = self.rt.block_on(async {
            tokio::join!(
                read(ClientId::A, Remote::Enroute),
                read(ClientId::A, Remote::Oracle),
                read(ClientId::B, Remote::Enroute),
                read(ClientId::B, Remote::Oracle),
            )
        });

        let assert_pair_matches =
            |client: ClientId, enroute: &RepoSnapshot, oracle: &RepoSnapshot| {
                assert_eq!(
                    enroute.graph, oracle.graph,
                    "{client:?}'s enroute commit graph diverged from its own oracle clone"
                );
                assert_eq!(
                    enroute.tags, oracle.tags,
                    "{client:?}'s enroute tags diverged from its own oracle clone"
                );
                assert_eq!(
                    enroute.missing, oracle.missing,
                    "{client:?}'s enroute-backed repo and oracle-backed repo disagree \
                 on which objects are missing locally"
                );
            };
        assert_pair_matches(ClientId::A, &a_enroute, &a_oracle);
        assert_pair_matches(ClientId::B, &b_enroute, &b_oracle);
    }
}

/// One repo's state as read by `Sut::assert_converged`.
struct RepoSnapshot {
    graph: Vec<String>,
    tags: Vec<String>,
    missing: std::collections::BTreeSet<String>,
}

impl StateMachineTest for OracleStateMachine {
    type SystemUnderTest = Sut;
    type Reference = Self;

    #[tracing::instrument(skip_all)]
    fn init_test(_ref_state: &RefState) -> Self::SystemUnderTest {
        let rt = shared_runtime_handle();
        let tmp = tempfile::tempdir().unwrap();

        // Unique per call so concurrent/repeated `init_test` calls against the
        // shared schema (`support::make_state`) never collide on `(owner, name)`.
        let repo_name = format!("repo-{}", uuid::Uuid::new_v4());
        let (enroute_addr, alice_token, _landed, servers, storage, repo) = rt.block_on(async {
            let state = make_state().await;
            let repo = state.rows.create(None).await.unwrap();
            let (addr, token, landed, servers) =
                spawn_server(state.clone(), &[(&repo_name, repo.id)]).await;
            (addr, token, landed, servers, state, repo)
        });
        // Credentials embedded in the URL: git and reqwest both read a
        // `user:pass@host` authority as Basic auth without further setup.
        // No namespace segment in the path — the server reads that from the
        // Host header instead (see `support::git`'s `http.extraHeader`).
        let enroute_url = format!("http://{ALICE}:{alice_token}@{enroute_addr}/{repo_name}.git");

        let oracle = OracleDaemon::spawn(tmp.path());

        // Both remotes are empty here, so this also exercises cloning an
        // empty repo — both advertise their unborn HEAD as `refs/heads/main`,
        // so the clone lands on `main` regardless of `init.defaultBranch`.
        let repo_path = |client: ClientId, remote: Remote| {
            tmp.path()
                .join(format!("{client:?}-{}", remote.name()).to_lowercase())
        };
        let clone_repo = |local: std::path::PathBuf, url: String| async move {
            git(
                &[
                    "-c",
                    "protocol.version=2",
                    "clone",
                    &url,
                    local.to_str().unwrap(),
                ],
                None,
            )
            .await;
            local
        };
        let a_enroute = repo_path(ClientId::A, Remote::Enroute);
        let a_oracle = repo_path(ClientId::A, Remote::Oracle);
        let b_enroute = repo_path(ClientId::B, Remote::Enroute);
        let b_oracle = repo_path(ClientId::B, Remote::Oracle);
        // The four initial clones are of independent repos, so run them
        // concurrently rather than paying for four sequential git spawns.
        let (a_enroute, a_oracle, b_enroute, b_oracle) = rt.block_on(async {
            tokio::join!(
                clone_repo(a_enroute, enroute_url.clone()),
                clone_repo(a_oracle, oracle.url.clone()),
                clone_repo(b_enroute, enroute_url.clone()),
                clone_repo(b_oracle, oracle.url.clone()),
            )
        });

        Sut {
            rt,
            tmp,
            a_enroute,
            a_oracle,
            b_enroute,
            b_oracle,
            enroute_url,
            oracle_daemon: oracle,
            _servers: servers,
            current: std::cell::Cell::new(ClientId::A),
            submodule_paths: std::cell::RefCell::new(std::collections::BTreeSet::new()),
            storage,
            repo,
            verifications: std::cell::Cell::new(0),
        }
    }

    fn apply(
        state: Self::SystemUnderTest,
        _ref_state: &RefState,
        transition: Transition,
    ) -> Self::SystemUnderTest {
        match &transition {
            Transition::Create { name, content } => state.create_file(name, content),
            Transition::Modify { name, op } => state.modify_file(name, op),
            Transition::Revert { name } => state.revert_file(name),
            Transition::Delete(name) => state.delete_file(name),
            Transition::Move { from, to } => state.move_file(from, to),
            Transition::CopyDir { from, copy_id } => {
                state.copy_dir(from, &copy_destination(from, *copy_id));
            }
            Transition::SubmoduleAdd { path, oid } | Transition::SubmoduleUpdate { path, oid } => {
                state.submodule_set(path, oid);
            }
            Transition::SubmoduleRemove(path) => state.submodule_remove(path),
            Transition::Commit { commit_id } => state.commit(Some(*commit_id)),
            Transition::EndTurn { sync } => {
                state.end_turn(*sync);
                state.assert_converged();
            }
            Transition::BranchStart { branch_id } => state.branch_start(*branch_id),
            Transition::BranchMerge {
                commit_id,
                branch_id,
            } => state.branch_merge(*branch_id, Some(*commit_id)),
            Transition::CreateTag {
                commit_id,
                name,
                annotated,
            } => state.create_tag(*commit_id, name, *annotated),
            Transition::Maintain { patience } => state.maintain(*patience),
        }
        state
    }

    /// A run may end mid-turn: an unmerged branch open, staged changes, or
    /// unpushed commits.
    ///
    /// Flush it all so every generated sequence ends with a check.
    fn teardown(state: Self::SystemUnderTest, ref_state: RefState) {
        let staged = matches!(ref_state.work, WorkTree::Staged | WorkTree::BranchStaged);
        let on_branch = matches!(ref_state.work, WorkTree::Branch | WorkTree::BranchStaged);
        if staged {
            state.commit(None);
        }
        if on_branch {
            let branch_id = ref_state
                .open_branch
                .expect("on_branch implies a branch is open");
            state.branch_merge(branch_id, None);
        }
        if staged || on_branch || ref_state.turn_dirty {
            state.end_turn(SyncKind {
                resync: ResyncKind::Pull,
                filter: None,
                depth: None,
            });
        }
        // A run with no transitions at all leaves every repo with an unborn
        // `main` (no commits ever made), which `git log main` can't read —
        // there's nothing to converge in that case anyway.
        if ref_state.history.any_commits() {
            state.assert_converged();
        }
    }
}

/// Parse `name` from the environment, falling back to `default` if unset or
/// unparseable.
fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let timing = init_tracing();
    let cases: u32 = env_or("ENROUTE_E2E_CASES", 32);
    let max_transitions: usize = env_or("ENROUTE_E2E_MAX_TRANSITIONS", 20);
    println!(
        "enroute_matches_real_git_daemon: cases={cases} max_transitions={max_transitions} \
         (override with ENROUTE_E2E_CASES / ENROUTE_E2E_MAX_TRANSITIONS)"
    );

    let config = proptest::test_runner::Config {
        cases,
        // Outside a `#[test]`, proptest has no `file!()` to derive a
        // regressions path from; pin one explicitly instead.
        failure_persistence: Some(Box::new(
            proptest::test_runner::FileFailurePersistence::Direct(
                "dev/e2e/e2e.proptest-regressions",
            ),
        )),
        ..proptest::test_runner::Config::default()
    };
    let mut runner = proptest::test_runner::TestRunner::new(config.clone());
    let strategy = OracleStateMachine::sequential_strategy(1..max_transitions);

    let result = runner.run(&strategy, |(initial_state, transitions, seen_counter)| {
        OracleStateMachine::test_sequential(
            config.clone(),
            initial_state,
            transitions,
            seen_counter,
        );
        Ok(())
    });

    timing.print();

    match result {
        Ok(()) => println!("enroute_matches_real_git_daemon: ok"),
        Err(e) => {
            eprintln!("enroute_matches_real_git_daemon: FAILED\n{e}");
            std::process::exit(1);
        }
    }
}

/// Regression tests pinning down specific bugs found via the differential
/// suite above.
///
/// Deterministic, minimal reproductions that run on every `cargo test`
/// rather than depending on the fuzzer randomly generating the shape again.
#[cfg(test)]
mod regression {
    use super::*;
    use model::FilterKind;

    /// A revert landing in a later push than the change it undoes, cloned
    /// back from scratch: two stored entries for one object.
    ///
    /// Does not pin the delta-cycle itself down — a real git client gives no
    /// purchase on that ordering; `a_full_clone_never_ships_a_delta_cycle` does.
    #[test]
    fn revert_in_a_later_push_still_clones() {
        let sut = OracleStateMachine::init_test(&RefState::default());
        for i in 0..9 {
            sut.create_file(&format!("f{i}"), format!("filler {i}").as_bytes());
        }
        // Barely changed and incompressible, so the delta beats the whole
        // object — two whole entries could never point at each other.
        let original: Vec<u8> = (0..600u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13).to_le_bytes()[0])
            .collect();
        let mut edited = original.clone();
        edited.splice(200..210, *b"CHANGED!!!");
        sut.create_file("x", &original);
        sut.commit(Some(0));
        sut.modify_file("x", &ModifyOp::Set(edited));
        sut.commit(Some(1));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Pull,
            filter: None,
            depth: None,
        });

        // The other client holds the turn and has pulled both commits, so this
        // revert is a second push over an object the first already stored.
        sut.revert_file("x");
        assert_eq!(
            sut.read_file("x"),
            original,
            "the revert has to bring the first version's bytes back, or there \
             is no second entry for that object and nothing here is tested"
        );
        sut.commit(Some(2));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Fresh,
            filter: None,
            depth: None,
        });
        sut.assert_converged();
    }

    /// Minimal repro: a file that was once a gitlink, reverted, then deleted.
    ///
    /// The revert checks the gitlink back out as an empty directory, so what
    /// the delete meets on disk is a directory where the model says file.
    #[test]
    fn deleting_a_file_that_reverted_to_a_gitlink_still_works() {
        let sut = OracleStateMachine::init_test(&RefState::default());
        sut.submodule_set("p", "a0aaaaa000aaaa0a00aaa0aa000a00a00aa0aaaa");
        sut.commit(Some(0));
        sut.create_file("p", b"now a file");
        sut.commit(Some(1));

        // Back to the gitlink, which is an empty directory on disk.
        sut.revert_file("p");
        sut.delete_file("p");
        sut.commit(Some(2));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Pull,
            filter: None,
            depth: None,
        });
        sut.assert_converged();
    }

    /// Minimal repro: a file that was once a gitlink, reverted, moved, then
    /// deleted after a fresh clone.
    ///
    /// Moving the empty directory a gitlink checks out as would put nothing
    /// at the new path that git stores, so the clone brings nothing back.
    #[test]
    fn moving_a_file_that_reverted_to_a_gitlink_stores_something() {
        let sut = OracleStateMachine::init_test(&RefState::default());
        sut.submodule_set("p", "a0aaaaa000aaaa0a00aaa0aa000a00a00aa0aaaa");
        sut.commit(Some(0));
        sut.create_file("p", b"now a file");
        sut.commit(Some(1));
        sut.revert_file("p");
        sut.move_file("p", "q");
        sut.commit(Some(2));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Fresh,
            filter: None,
            depth: None,
        });
        // The model calls `q` a file. If the move carried an empty directory
        // there, git stored nothing and a fresh clone brings nothing back.
        sut.delete_file("q");
        sut.commit(Some(3));
        sut.assert_converged();
    }

    /// Minimal repro: a directory copied to a second path, pushed, then both
    /// copies changed in one later commit.
    ///
    /// The diff needs one stored tree under two paths at once, which used to
    /// read back as a tree the fetch had failed to supply.
    #[test]
    fn a_copied_directory_diverging_on_both_sides_still_pushes() {
        let sut = OracleStateMachine::init_test(&RefState::default());
        sut.create_file("one/f", b"original");
        sut.copy_dir("one", "one_copy");
        sut.commit(Some(0));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Pull,
            filter: None,
            depth: None,
        });

        // The turn has changed hands and the shared tree is stored, so this
        // second push is the one that must fetch it — once, for both paths.
        sut.modify_file("one/f", &ModifyOp::Set(b"left".to_vec()));
        sut.modify_file("one_copy/f", &ModifyOp::Set(b"right".to_vec()));
        sut.commit(Some(1));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Pull,
            filter: None,
            depth: None,
        });
        sut.assert_converged();
    }

    /// Minimal repro: A adds a file and pushes, B pulls with `blob:none`.
    ///
    /// B's checkout must lazily backfill the filtered blob via a direct
    /// `want`; `assert_converged` catches enroute dropping it.
    #[test]
    fn create_file_commit_endturn_pull_filtered() {
        let sut = OracleStateMachine::init_test(&RefState::default());
        sut.create_file("a", b"hello world");
        sut.commit(Some(0));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Pull,
            filter: Some(FilterKind::BlobNone),
            depth: None,
        });
        sut.assert_converged();
    }

    /// Minimal repro: commit 0 is tagged `v1`, commit 1 (its child) is the
    /// branch tip; B pulls with `--depth=1`, which implies `--tags`.
    ///
    /// The tag target is also the tip's parent, so a boundary check phrased
    /// as "has a parent outside the sent set" wrongly clears the tip's flag.
    #[test]
    fn tag_on_ancestor_commit_endturn_pull_shallow() {
        let sut = OracleStateMachine::init_test(&RefState::default());
        sut.create_file("a", b"one");
        sut.commit(Some(0));
        sut.create_tag(0, "v1", false);
        sut.create_file("b", b"two");
        sut.commit(Some(1));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Pull,
            filter: None,
            depth: Some(1),
        });
        sut.assert_converged();
    }

    /// Minimal repro: B pulls with `--depth=1`, then commits and pushes from
    /// that shallow clone.
    ///
    /// Such a push prefixes the command list with `shallow <oid>` lines;
    /// `parse_ref_updates` once assumed capabilities rode on `lines[0]`.
    #[test]
    fn push_from_shallow_clone_is_accepted() {
        let sut = OracleStateMachine::init_test(&RefState::default());
        sut.create_file("a", b"one");
        sut.commit(Some(0));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Pull,
            filter: None,
            depth: Some(1),
        });
        // B is now shallow at depth 1; commit and push from it.
        sut.create_file("b", b"two");
        sut.commit(Some(1));
        sut.end_turn(SyncKind {
            resync: ResyncKind::Pull,
            filter: None,
            depth: None,
        });
        sut.assert_converged();
    }
}
