//! The reference state machine: a pure, in-memory model of what a valid
//! sequence of git operations looks like, with no git or tokio dependency.
//!
//! Kept separate from `main.rs` so unit tests can replay transition
//! sequences against just this model, in milliseconds, with no real servers.

use std::collections::{BTreeMap, BTreeSet};

use proptest::collection::vec as pvec;
use proptest::prelude::*;
use proptest::strategy::Union;
use proptest_state_machine::ReferenceStateMachine;

/// What kind of tree entry a tracked path currently is.
///
/// A path can flip between kinds (`Create`/`SubmoduleAdd` on the same path),
/// both ordinary git operations, so the model overwrites rather than forbids it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PathKind {
    File,
    Submodule,
}

/// Which client currently holds the turn: a run of local commits capped off
/// by `EndTurn`, which pushes and hands the turn to the other client.
///
/// Not testing ref-update races or merge conflicts, but that two clone
/// lineages pulling and pushing in alternation stay consistent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ClientId {
    #[default]
    A,
    B,
}

impl ClientId {
    pub(crate) fn other(self) -> Self {
        match self {
            ClientId::A => ClientId::B,
            ClientId::B => ClientId::A,
        }
    }
}

/// State of the working tree relative to `main`.
///
/// A branch must be fully created, worked on, and merged back into `main`
/// within the same turn, so merging with `--no-ff` can never conflict.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum WorkTree {
    /// Checked out on `main`, nothing staged.
    #[default]
    Clean,
    /// Checked out on `main`, staged changes not yet committed.
    Staged,
    /// Checked out on an unmerged branch, nothing staged.
    Branch,
    /// Checked out on an unmerged branch, staged changes not yet committed.
    BranchStaged,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RefState {
    /// `git daemon` is ground truth for content, so the model only needs
    /// which paths exist and what kind each is.
    ///
    /// A submodule's gitlink oid is never tracked: minted fresh at
    /// generation time and never looked back up.
    paths: BTreeMap<String, PathKind>,
    current: ClientId,
    pub(crate) work: WorkTree,
    /// Whether the current turn has any commits to push.
    pub(crate) turn_dirty: bool,
    /// How far `main` has got: unborn, committed locally, or pushed.
    pub(crate) history: History,
    /// Opaque ids of every commit made so far, regardless of which client.
    ///
    /// Every branch is merged before its turn ends, so any id here is
    /// guaranteed reachable from `main` — safe for `CreateTag` to pick from.
    commits: Vec<u32>,
    next_commit_id: u32,
    /// Tag names already in use, so the generator never proposes a
    /// duplicate.
    tags: BTreeSet<String>,
    /// Whether the currently open branch has at least one commit on it.
    ///
    /// `--no-ff` refuses to fabricate a merge commit when the branch is
    /// identical to `main`, so `BranchMerge` must wait for divergence.
    branch_has_commits: bool,
    /// Id of the currently open branch, if any, minted from
    /// `next_branch_id` the same way `Commit`/`BranchMerge` mint commit ids.
    pub(crate) open_branch: Option<u32>,
    next_branch_id: u32,
    /// Minted into each `CopyDir`'s destination name, so no two copies can
    /// ever propose the same path (see [`Transition::CopyDir`]).
    next_copy_id: u32,
    /// Each client's current shallow depth on its enroute/oracle repo pair —
    /// `None` for full history, `Some(n)` for a real `n`-deep boundary.
    ///
    /// Keeps `EndTurn`'s generated `SyncKind::depth` realistic: real git's
    /// `--depth` can't reach back further than an existing shallow boundary.
    depths: BTreeMap<ClientId, Option<u32>>,
}

impl RefState {
    /// Move `work` to its "something staged" counterpart, keeping whether
    /// we're on `main` or an open branch.
    fn mark_staged(&mut self) {
        self.work = match self.work {
            WorkTree::Clean | WorkTree::Staged => WorkTree::Staged,
            WorkTree::Branch | WorkTree::BranchStaged => WorkTree::BranchStaged,
        };
    }

    /// `client`'s current shallow depth (see `depths`), defaulting to full
    /// history (`None`) for a client that has never resynced.
    fn depth_of(&self, client: ClientId) -> Option<u32> {
        self.depths.get(&client).copied().flatten()
    }
}

/// A change to an existing file's content, applied against whatever is on
/// disk at apply time.
///
/// The model only tracks filenames, not content, so this is defined relative
/// to real prior bytes rather than anything the model knows.
#[derive(Clone, Debug)]
pub(crate) enum ModifyOp {
    /// Replace the file's content wholesale.
    Set(Vec<u8>),
    /// Append bytes to the file's content.
    Append(Vec<u8>),
    /// Replace a byte range with `data`.
    ///
    /// `at`/`len` are taken modulo/clamped to the file's actual length at
    /// apply time, since the model doesn't know it up front.
    Splice {
        at: usize,
        len: usize,
        data: Vec<u8>,
    },
}

/// A partial-clone filter-spec git supports, exercised end-to-end via
/// `SyncKind::filter`.
///
/// `blob:none` is the only variant — see `PackFilter`'s doc for why `tree:0`
/// isn't implemented.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FilterKind {
    BlobNone,
}

impl FilterKind {
    /// The `--filter=<spec>` value real git expects.
    pub(crate) fn spec(self) -> &'static str {
        match self {
            FilterKind::BlobNone => "blob:none",
        }
    }
}

/// Whether the next client resyncs incrementally or from scratch (see
/// `SyncKind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResyncKind {
    /// An incremental `git fetch` + fast-forward merge.
    Pull,
    /// Wipe the repo and do a from-scratch full `git clone`, exercising full
    /// pack generation rather than just fetch deltas.
    Fresh,
}

/// How the next client resyncs each of its per-remote repos at the end of a
/// turn.
///
/// `filter` exercises lazy blob backfill; `depth` exercises shallow fetch,
/// but `depth` depends on prior state, unlike `filter`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SyncKind {
    pub(crate) resync: ResyncKind,
    pub(crate) filter: Option<FilterKind>,
    pub(crate) depth: Option<u32>,
}

#[derive(Clone, Debug)]
pub(crate) enum Transition {
    /// Stage the creation of a brand-new file.
    Create { name: String, content: Vec<u8> },
    /// Stage a change to an existing file's content.
    Modify { name: String, op: ModifyOp },
    /// Stage `name`'s content from before its most recent change.
    ///
    /// The only transition that puts back a blob the repo already stores —
    /// every other invents content that never lands on an existing oid.
    Revert { name: String },
    /// Stage the deletion of an existing file.
    Delete(String),
    /// Stage the rename of an existing file to a currently-unused name.
    Move { from: String, to: String },
    /// Stage a copy of a whole directory at a new path, leaving the original
    /// in place — the only transition that puts one tree under two paths.
    ///
    /// `copy_id` names the destination through [`copy_destination`]; a freely
    /// drawn one collides often enough in this name space to exhaust proptest.
    CopyDir { from: String, copy_id: u32 },
    /// Commit everything currently staged, for the client whose turn it is
    /// (not pushed yet).
    ///
    /// `commit_id` is threaded through to the SUT, embedded as a
    /// `Commit-Id: N` trailer so `CreateTag` can resolve it to a real oid.
    Commit { commit_id: u32 },
    /// Push the current client's local commits, then hand the turn to the
    /// other client.
    ///
    /// The other client resyncs each of its per-remote repos per `sync` (see `SyncKind`).
    EndTurn { sync: SyncKind },
    /// Branch off the current client's `main` HEAD and check it out.
    ///
    /// `branch_id` identifies this branch to the SUT, which derives a name
    /// like `wip-{branch_id}` from it, needing no naming state of its own.
    BranchStart { branch_id: u32 },
    /// Merge the currently open branch back into `main` with `--no-ff`, then
    /// delete the branch.
    ///
    /// `branch_id` identifies which branch, opened by the matching `BranchStart`.
    BranchMerge { commit_id: u32, branch_id: u32 },
    /// Tag an existing commit, not necessarily HEAD or on `main`.
    ///
    /// `annotated` picks between a lightweight ref-only tag and a real
    /// annotated tag object, since those exercise different read paths.
    CreateTag {
        commit_id: u32,
        name: String,
        annotated: bool,
    },
    /// Stage a brand-new submodule gitlink: a mode-160000 tree entry whose
    /// oid is never resolved against this repo's object store.
    ///
    /// A synthetic oid staged via `git update-index --add --cacheinfo` is
    /// indistinguishable from one a real `git submodule add` would produce.
    SubmoduleAdd { path: String, oid: String },
    /// Stage moving an existing submodule's gitlink to point at a
    /// different (equally synthetic) commit oid.
    SubmoduleUpdate { path: String, oid: String },
    /// Stage removing an existing submodule's gitlink entry.
    SubmoduleRemove(String),
    /// Run a maintenance pass over the repository, and read it all back.
    ///
    /// The model does nothing with this, which is the whole claim: a pass
    /// must be invisible to every git client.
    Maintain { patience: Patience },
}

/// How far `main` has got, which decides what is legal next.
///
/// A commit sits in one client's clone until a turn ends, so neither remote
/// has a `main` to clone before a push lands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum History {
    /// No commit anywhere: `main` is a name nothing resolves.
    #[default]
    Unborn,
    /// Committed in a client's own clone, and pushed nowhere yet.
    Local,
    /// On both remotes, so a fresh clone reads it back.
    Pushed,
}

impl History {
    /// Whether a commit exists at all, wherever it is.
    pub(crate) fn any_commits(self) -> bool {
        self != Self::Unborn
    }
}

/// How long a generated maintenance pass waits before reclaiming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Patience {
    /// A deployment's own windows: what a pass does hour to hour.
    Patient,
    /// No window at all, so one pass runs every stage of a reclaim at once.
    Impatient,
}

/// `EndTurn`'s `sync` argument for a target client currently at `current_depth`.
///
/// `depth` depends on `resync` and prior state (see `SyncKind`'s doc), so
/// this branches on `resync` first.
fn sync_kind(current_depth: Option<u32>) -> BoxedStrategy<SyncKind> {
    let filter = prop_oneof![
        3 => Just(None),
        1 => Just(Some(FilterKind::BlobNone)),
    ];

    // `Fresh` re-clones from scratch, so any depth is safe. Kept small so
    // depth cuts land inside these runs' short histories instead of always
    // exceeding them (degenerating to a full clone).
    let fresh_depth = prop_oneof![
        3 => Just(None),
        1 => (1u32..=3).prop_map(Some),
    ];
    let fresh = (filter.clone(), fresh_depth)
        .prop_map(|(filter, depth)| SyncKind {
            resync: ResyncKind::Fresh,
            filter,
            depth,
        })
        .boxed();

    // `Pull`'s depth must be a genuine deepen (see `SyncKind`'s doc). A
    // client at full history (`current_depth: None`) can only go shallow via
    // `Fresh`; one already shallow at `d` may deepen to `d..=d+2` or
    // plain-fetch (`None`, preserving its existing boundary).
    let pull_depth: BoxedStrategy<Option<u32>> = match current_depth {
        None => Just(None).boxed(),
        Some(d) => prop_oneof![
            3 => Just(None),
            1 => (d..=d.saturating_add(2)).prop_map(Some),
        ]
        .boxed(),
    };
    let pull = (filter, pull_depth)
        .prop_map(|(filter, depth)| SyncKind {
            resync: ResyncKind::Pull,
            filter,
            depth,
        })
        .boxed();

    prop_oneof![4 => pull, 1 => fresh].boxed()
}

/// A tiny pool so the same directory recurs across files and commits.
///
/// Freely-drawn segments would nest every path in its own tree, giving a flat repo with longer names.
const DIR_SEGMENTS: [&str; 3] = ["one", "two", "three"];

/// Also a small pool: two directories share a tree only when they hold the
/// same names as well as the same content.
fn leaf_name() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => proptest::sample::select(vec!["f", "g", "h"]).prop_map(String::from),
        1 => "[a-z][a-z0-9_]{0,10}",
    ]
}

/// A path zero to two directories deep.
///
/// Depth is what puts more than one tree in a commit, for a recursive diff to descend through.
fn file_name() -> impl Strategy<Value = String> {
    (
        pvec(proptest::sample::select(DIR_SEGMENTS.to_vec()), 0..3),
        leaf_name(),
    )
        .prop_map(|(dirs, leaf)| {
            let mut path = String::new();
            for dir in dirs {
                path.push_str(dir);
                path.push('/');
            }
            path.push_str(&leaf);
            path
        })
}

/// Whether `inner` sits somewhere under directory `outer`.
fn nests(inner: &str, outer: &str) -> bool {
    inner
        .strip_prefix(outer)
        .is_some_and(|rest| rest.starts_with('/'))
}

/// Whether `candidate` can't coexist with the paths in `existing`.
///
/// Git holds a path as a file or a directory, never both, so `a` and `a/b`
/// rule each other out.
fn path_conflicts<S: AsRef<str>>(mut existing: impl Iterator<Item = S>, candidate: &str) -> bool {
    existing.any(|path| {
        let path = path.as_ref();
        nests(path, candidate) || nests(candidate, path)
    })
}

fn tag_name() -> impl Strategy<Value = String> {
    "v[a-z0-9_]{0,10}"
}

/// A syntactically valid (40 lowercase hex chars) commit oid for a
/// submodule's checked-out commit.
///
/// It never needs to resolve to a real object — see `Transition::SubmoduleAdd`.
fn submodule_oid() -> impl Strategy<Value = String> {
    "[0-9a-f]{40}"
}

/// Mostly a few bytes from a tiny alphabet, occasionally free random bytes.
///
/// Repeated content means colliding blobs, which lets two directories share
/// a tree — `Revert` exists to cover that case; this narrows it at the source.
fn file_content() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        3 => pvec(proptest::sample::select(vec![b'a', b'b', b'c']), 0..4),
        1 => pvec(any::<u8>(), 0..64),
    ]
}

/// A change to an existing file: a full overwrite, an append, or a splice
/// into the middle.
///
/// Append and splice are small, delta-friendly edits, interesting for pack generation.
fn modify_op() -> impl Strategy<Value = ModifyOp> {
    prop_oneof![
        2 => file_content().prop_map(ModifyOp::Set),
        3 => file_content().prop_map(ModifyOp::Append),
        3 => (0usize..128, 0usize..64, file_content())
            .prop_map(|(at, len, data)| ModifyOp::Splice { at, len, data }),
    ]
}

/// Where a copy of `from` lands.
///
/// A minted id makes the name unique, so a destination never needs a
/// collision check, and the suffix keeps it a sibling of the source.
pub(crate) fn copy_destination(from: &str, copy_id: u32) -> String {
    format!("{from}_copy{copy_id}")
}

/// Every directory that can be copied (see [`Transition::CopyDir`]).
///
/// Every level counts — `one/two/f` contributes both `one` and `one/two`.
fn copyable_directories(state: &RefState) -> Vec<String> {
    let mut dirs = BTreeSet::new();
    for path in state.paths.keys() {
        let mut at = path.as_str();
        while let Some((parent, _)) = at.rsplit_once('/') {
            dirs.insert(parent.to_string());
            at = parent;
        }
    }
    dirs.retain(|dir| is_copyable(state, dir));
    dirs.into_iter().collect()
}

/// Whether `dir` holds something and nothing under it is a submodule.
///
/// Lifted out of [`copyable_directories`] so a precondition on one directory
/// doesn't rebuild the whole set.
fn is_copyable(state: &RefState, dir: &str) -> bool {
    let mut under = state
        .paths
        .iter()
        .filter(|(path, _)| nests(path, dir))
        .peekable();
    under.peek().is_some() && under.all(|(_, kind)| *kind == PathKind::File)
}

/// A path nothing already claims.
///
/// The only place the file-vs-directory rule is applied at generation time.
fn fresh_path(state: &RefState) -> impl Strategy<Value = String> + use<> {
    let existing = state.paths.clone();
    file_name().prop_filter("new path must not collide with a directory", move |name| {
        !path_conflicts(existing.keys(), name)
    })
}

/// Every currently-tracked path of the given kind, for transitions that pick
/// an existing file or submodule to act on.
fn paths_of_kind(state: &RefState, kind: PathKind) -> Vec<String> {
    state
        .paths
        .iter()
        .filter(|(_, k)| **k == kind)
        .map(|(name, _)| name.clone())
        .collect()
}

/// Every transition that can legally stage something into the index right now.
///
/// `Create`/`SubmoduleAdd` are always legal; the rest need an existing path
/// of the matching kind to pick from.
fn stage_op_branches(state: &RefState) -> BoxedStrategy<Transition> {
    let create = (fresh_path(state), file_content())
        .prop_map(|(name, content)| Transition::Create { name, content });
    let submodule_add = (fresh_path(state), submodule_oid())
        .prop_map(|(path, oid)| Transition::SubmoduleAdd { path, oid });
    let mut branches: Vec<(u32, BoxedStrategy<Transition>)> =
        vec![(3, create.boxed()), (1, submodule_add.boxed())];

    let existing_files = paths_of_kind(state, PathKind::File);
    if !existing_files.is_empty() {
        let existing_for_move = state.paths.clone();
        let delete = proptest::sample::select(existing_files.clone()).prop_map(Transition::Delete);
        let modify = (
            proptest::sample::select(existing_files.clone()),
            modify_op(),
        )
            .prop_map(|(name, op)| Transition::Modify { name, op });
        let revert = proptest::sample::select(existing_files.clone())
            .prop_map(|name| Transition::Revert { name });
        let move_op = (proptest::sample::select(existing_files), file_name())
            .prop_filter("move destination must be unused", move |(from, to)| {
                to != from
                    && !existing_for_move.contains_key(to)
                    && !path_conflicts(existing_for_move.keys().filter(|p| *p != from), to)
            })
            .prop_map(|(from, to)| Transition::Move { from, to });
        branches.push((2, modify.boxed()));
        branches.push((2, revert.boxed()));
        branches.push((1, delete.boxed()));
        branches.push((1, move_op.boxed()));
    }

    let dirs = copyable_directories(state);
    if !dirs.is_empty() {
        let copy_id = state.next_copy_id;
        let copy_dir = proptest::sample::select(dirs)
            .prop_map(move |from| Transition::CopyDir { from, copy_id });
        branches.push((2, copy_dir.boxed()));
    }

    let existing_submodules = paths_of_kind(state, PathKind::Submodule);
    if !existing_submodules.is_empty() {
        let submodule_update = (
            proptest::sample::select(existing_submodules.clone()),
            submodule_oid(),
        )
            .prop_map(|(path, oid)| Transition::SubmoduleUpdate { path, oid });
        let submodule_remove =
            proptest::sample::select(existing_submodules).prop_map(Transition::SubmoduleRemove);
        branches.push((1, submodule_update.boxed()));
        branches.push((1, submodule_remove.boxed()));
    }

    Union::new_weighted(branches).boxed()
}

pub(crate) struct OracleStateMachine;

impl ReferenceStateMachine for OracleStateMachine {
    type State = RefState;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RefState::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        let stage_ops = stage_op_branches(state);

        let base = match state.work {
            WorkTree::Staged | WorkTree::BranchStaged => {
                let commit = Just(Transition::Commit {
                    commit_id: state.next_commit_id,
                });
                prop_oneof![3 => stage_ops, 1 => commit].boxed()
            }
            WorkTree::Branch if state.branch_has_commits => {
                let branch_merge = Just(Transition::BranchMerge {
                    commit_id: state.next_commit_id,
                    branch_id: state.open_branch.unwrap(),
                });
                prop_oneof![3 => stage_ops, 1 => branch_merge].boxed()
            }
            WorkTree::Branch => stage_ops,
            WorkTree::Clean if state.turn_dirty => {
                let branch_start = Just(Transition::BranchStart {
                    branch_id: state.next_branch_id,
                });
                let next_depth = state.depth_of(state.current.other());
                let end_turn = sync_kind(next_depth).prop_map(|sync| Transition::EndTurn { sync });
                prop_oneof![3 => stage_ops, 1 => end_turn, 1 => branch_start].boxed()
            }
            WorkTree::Clean => {
                let branch_start = Just(Transition::BranchStart {
                    branch_id: state.next_branch_id,
                });
                prop_oneof![3 => stage_ops, 1 => branch_start].boxed()
            }
        };

        // Maintenance is orthogonal to everything: it is legal in any state,
        // changes nothing the model holds, and is worth generating only once
        // there is something in the repository to maintain. Rare, because it
        // reads the whole repository back and a run should mostly be pushing.
        let base = if state.history == History::Pushed {
            let maintain = prop_oneof![
                1 => Just(Transition::Maintain { patience: Patience::Patient }),
                1 => Just(Transition::Maintain { patience: Patience::Impatient }),
            ]
            .boxed();
            Union::new_weighted(vec![(12, base), (1, maintain)]).boxed()
        } else {
            base
        };

        // Tagging is orthogonal to the WorkTree state machine above — it just
        // needs a commit from the ever-growing `commits` pool, so it's
        // available alongside whatever else is legal right now.
        if state.commits.is_empty() {
            base
        } else {
            let commit_ids = state.commits.clone();
            let existing_tags = state.tags.clone();
            let tag_op = (
                proptest::sample::select(commit_ids),
                tag_name(),
                any::<bool>(),
            )
                .prop_filter("tag name must be unused", move |(_, name, _)| {
                    !existing_tags.contains(name)
                })
                .prop_map(|(commit_id, name, annotated)| Transition::CreateTag {
                    commit_id,
                    name,
                    annotated,
                })
                .boxed();
            Union::new_weighted(vec![(4, base), (1, tag_op)]).boxed()
        }
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::Create { name, .. } => {
                state.paths.insert(name.clone(), PathKind::File);
                state.mark_staged();
            }
            Transition::Modify { .. }
            | Transition::Revert { .. }
            | Transition::SubmoduleUpdate { .. } => {
                state.mark_staged();
            }
            Transition::Delete(name) => {
                state.paths.remove(name);
                state.mark_staged();
            }
            Transition::Move { from, to } => {
                state.paths.remove(from);
                state.paths.insert(to.clone(), PathKind::File);
                state.mark_staged();
            }
            Transition::CopyDir { from, copy_id } => {
                let to = copy_destination(from, *copy_id);
                let copies: Vec<(String, PathKind)> = state
                    .paths
                    .iter()
                    .filter_map(|(path, kind)| {
                        path.strip_prefix(from.as_str())
                            .filter(|rest| rest.starts_with('/'))
                            .map(|rest| (format!("{to}{rest}"), *kind))
                    })
                    .collect();
                state.paths.extend(copies);
                debug_assert_eq!(*copy_id, state.next_copy_id);
                state.next_copy_id += 1;
                state.mark_staged();
            }
            Transition::SubmoduleAdd { path, .. } => {
                state.paths.insert(path.clone(), PathKind::Submodule);
                state.mark_staged();
            }
            Transition::SubmoduleRemove(path) => {
                state.paths.remove(path);
                state.mark_staged();
            }
            // Nothing. A pass moves bytes between objects and rewrites an
            // index; a git client must not be able to tell it ran, and the
            // model holding still is how that is asserted.
            Transition::Maintain { .. } => {}
            Transition::Commit { commit_id } => {
                let was_branch_staged = state.work == WorkTree::BranchStaged;
                state.work = match state.work {
                    WorkTree::Staged => WorkTree::Clean,
                    WorkTree::BranchStaged => WorkTree::Branch,
                    WorkTree::Clean | WorkTree::Branch => state.work,
                };
                state.turn_dirty = true;
                state.history = state.history.max(History::Local);
                if was_branch_staged {
                    state.branch_has_commits = true;
                }
                debug_assert_eq!(*commit_id, state.next_commit_id);
                state.next_commit_id += 1;
                state.commits.push(*commit_id);
            }
            Transition::EndTurn { sync } => {
                state.history = state.history.max(History::Pushed);
                let next = state.current.other();
                // `Fresh` always replaces the tracked depth. `Pull` only
                // replaces it on a genuine deepen (`sync_kind` never
                // generates a shallower one); `None` is a plain fetch.
                let replaces_depth = sync.resync == ResyncKind::Fresh || sync.depth.is_some();
                if replaces_depth {
                    state.depths.insert(next, sync.depth);
                }
                state.current = next;
                state.turn_dirty = false;
            }
            Transition::BranchStart { branch_id } => {
                state.work = WorkTree::Branch;
                state.branch_has_commits = false;
                debug_assert_eq!(*branch_id, state.next_branch_id);
                state.open_branch = Some(*branch_id);
                state.next_branch_id += 1;
            }
            Transition::BranchMerge {
                commit_id,
                branch_id,
            } => {
                state.work = WorkTree::Clean;
                state.turn_dirty = true;
                debug_assert_eq!(*commit_id, state.next_commit_id);
                state.next_commit_id += 1;
                state.commits.push(*commit_id);
                debug_assert_eq!(state.open_branch, Some(*branch_id));
                state.open_branch = None;
            }
            Transition::CreateTag { name, .. } => {
                state.tags.insert(name.clone());
                // The tag lives locally until the next push; mark the turn
                // dirty so teardown calls end_turn and pushes it before
                // assert_converged compares all four repos.
                state.turn_dirty = true;
            }
        }
        state
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        match transition {
            Transition::Create { name, .. } => !path_conflicts(state.paths.keys(), name),
            Transition::SubmoduleAdd { path, .. } => !path_conflicts(state.paths.keys(), path),
            Transition::Modify { name, .. } | Transition::Delete(name) => {
                state.paths.get(name) == Some(&PathKind::File)
            }
            // Needs a commit to reach back to; whether a given file has an
            // earlier version is the SUT's business.
            Transition::Revert { name } => {
                state.history.any_commits() && state.paths.get(name) == Some(&PathKind::File)
            }
            Transition::Move { from, to } => {
                from != to
                    && state.paths.get(from) == Some(&PathKind::File)
                    && !state.paths.contains_key(to)
                    && !path_conflicts(state.paths.keys().filter(|p| *p != from), to)
            }
            Transition::CopyDir { from, .. } => is_copyable(state, from),
            Transition::SubmoduleUpdate { path, .. } | Transition::SubmoduleRemove(path) => {
                state.paths.get(path) == Some(&PathKind::Submodule)
            }
            Transition::Commit { .. } => {
                matches!(state.work, WorkTree::Staged | WorkTree::BranchStaged)
            }
            Transition::EndTurn { .. } => state.turn_dirty && state.work == WorkTree::Clean,
            Transition::BranchStart { .. } => {
                state.work == WorkTree::Clean && state.history.any_commits()
            }
            Transition::BranchMerge { .. } => {
                state.work == WorkTree::Branch && state.branch_has_commits
            }
            Transition::CreateTag {
                commit_id, name, ..
            } => state.commits.contains(commit_id) && !state.tags.contains(name),
            // Legal at any point, including mid-turn with work staged: a
            // pass runs against what has landed, and what a client has not
            // pushed yet is no business of it.
            Transition::Maintain { .. } => state.history == History::Pushed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::test_runner::{TestCaseError, TestRunner};

    fn with_any_commits() -> RefState {
        RefState {
            history: History::Local,
            ..Default::default()
        }
    }

    /// Invariants the model must maintain regardless of the transitions
    /// applied to reach `state`.
    ///
    /// Checked after every transition, so a violation points at the exact one that broke it.
    fn check_invariants(state: &RefState) {
        let commit_count = u32::try_from(state.commits.len()).unwrap();
        assert_eq!(
            commit_count, state.next_commit_id,
            "commits pool must have exactly one entry per minted id"
        );
        assert!(
            state.commits.iter().copied().eq(0..state.next_commit_id),
            "commit ids must be dense and assigned in order: {:?}",
            state.commits
        );
        if !state.commits.is_empty() {
            assert!(
                state.history.any_commits(),
                "a commit must move the history past unborn"
            );
        }
    }

    /// Checked against every transition before it's applied, along every
    /// generated sequence.
    fn check_transition(state: &RefState, transition: &Transition) -> Result<(), TestCaseError> {
        if let Transition::BranchMerge { .. } = transition {
            prop_assert!(
                state.branch_has_commits,
                "BranchMerge generated with nothing committed on the branch"
            );
        }
        if let Transition::CreateTag { commit_id, .. } = transition {
            prop_assert!(
                state.commits.contains(commit_id),
                "CreateTag generated for a commit id not yet minted: {commit_id}"
            );
        }
        if matches!(transition, Transition::EndTurn { .. }) {
            prop_assert_eq!(state.work, WorkTree::Clean);
        }
        Ok(())
    }

    /// Replays random transition sequences against the reference model
    /// alone, checking invariants after every step.
    ///
    /// Catches bugs like `BranchMerge` with nothing to merge, `EndTurn`
    /// firing mid-branch, or `CreateTag` on an unminted commit id.
    #[test]
    fn reference_model_invariants_hold_along_generated_sequences() {
        let mut runner = TestRunner::default();
        let strategy = OracleStateMachine::sequential_strategy(1..40);
        runner
            .run(&strategy, |(init_state, transitions, _)| {
                let mut state = init_state;
                check_invariants(&state);
                for transition in transitions {
                    check_transition(&state, &transition)?;
                    state = OracleStateMachine::apply(state, &transition);
                    check_invariants(&state);
                }
                Ok(())
            })
            .unwrap();
    }

    /// Regression: `CreateTag` after `EndTurn` must set `turn_dirty`, so
    /// teardown pushes the local tag before comparing all four repos.
    ///
    /// Otherwise `assert_converged` sees A's repos (no tag) diverge from B's (tagged).
    #[test]
    fn create_tag_marks_turn_dirty() {
        let state = RefState::default();
        let state = OracleStateMachine::apply(
            state,
            &Transition::Create {
                name: "a".to_string(),
                content: vec![],
            },
        );
        let state = OracleStateMachine::apply(state, &Transition::Commit { commit_id: 0 });
        let state = OracleStateMachine::apply(
            state,
            &Transition::EndTurn {
                sync: SyncKind {
                    resync: ResyncKind::Pull,
                    filter: None,
                    depth: None,
                },
            },
        );
        assert!(!state.turn_dirty, "EndTurn must clear turn_dirty");
        let state = OracleStateMachine::apply(
            state,
            &Transition::CreateTag {
                commit_id: 0,
                name: "v".to_string(),
                annotated: false,
            },
        );
        assert!(
            state.turn_dirty,
            "CreateTag must set turn_dirty so teardown pushes the tag"
        );
        assert!(
            OracleStateMachine::preconditions(
                &state,
                &Transition::EndTurn {
                    sync: SyncKind {
                        resync: ResyncKind::Pull,
                        filter: None,
                        depth: None
                    }
                }
            ),
            "EndTurn must be legal after CreateTag so the tag can be pushed"
        );
    }

    #[test]
    fn commit_precondition_rejects_clean_worktree() {
        let state = RefState::default();
        assert!(!OracleStateMachine::preconditions(
            &state,
            &Transition::Commit { commit_id: 0 }
        ));
    }

    #[test]
    fn branch_merge_precondition_rejects_freshly_started_branch() {
        let state = OracleStateMachine::apply(
            with_any_commits(),
            &Transition::BranchStart { branch_id: 0 },
        );
        assert_eq!(state.work, WorkTree::Branch);
        assert!(!OracleStateMachine::preconditions(
            &state,
            &Transition::BranchMerge {
                commit_id: 0,
                branch_id: 0
            }
        ));
    }

    #[test]
    fn branch_merge_precondition_accepts_branch_with_a_commit() {
        let state = OracleStateMachine::apply(
            with_any_commits(),
            &Transition::BranchStart { branch_id: 0 },
        );
        let state = OracleStateMachine::apply(
            state,
            &Transition::Create {
                name: "a".to_string(),
                content: vec![],
            },
        );
        let state = OracleStateMachine::apply(state, &Transition::Commit { commit_id: 0 });
        assert!(state.branch_has_commits);
        assert!(OracleStateMachine::preconditions(
            &state,
            &Transition::BranchMerge {
                commit_id: 1,
                branch_id: 0
            }
        ));
    }
}
