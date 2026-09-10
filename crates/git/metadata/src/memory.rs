//! The same rows, in a map behind a mutex.
//!
//! For a test that is about what a push does rather than about Postgres, and
//! for a local stack with nothing to install. It holds what the schema holds
//! and answers the same questions the same way — the CAS on a ref, a counter
//! that hands out dense ranges, a duplicate oid reported as [`Raced`] — since
//! a double that answers differently is worse than no double at all.
//!
//! # What it is not
//! Nothing here is durable, and nothing is shared between processes. One lock
//! covers the whole store, which is what a transaction bought in the other.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{ObjectHashMap, RepoId, StorageKey, Ulid, is_funny_refname, kind_to_u8};

use crate::store::{Identity, Raced};
use crate::{
    RefEntry, RefUpdate, RefUpdateRejection, RefUpdateResult, RefsMap, RepoMetadata, RepoSummary,
    is_branch_refname,
};

/// Microseconds since the epoch, which is what a `timestamptz` holds.
///
/// Not seconds: two writes in one second are ordered in the other store, and
/// a stand-in that cannot tell them apart answers a grace window differently.
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
        })
}

/// One microsecond count as the seconds a caller is answered in.
const fn secs(micros: i64) -> i64 {
    micros / 1_000_000
}

/// A grace window back from now, in the same units the stamps are in.
fn cutoff(grace_secs: u64) -> i64 {
    let window = i64::try_from(grace_secs)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000);
    now().saturating_sub(window)
}

/// One ref, and when it last moved.
#[derive(Debug, Clone)]
struct Ref {
    oid: ObjectId,
    updated: i64,
}

/// One repository's rows.
#[derive(Debug, Default)]
struct Repo {
    storage_key: Option<StorageKey>,
    default_branch: String,
    deleted_at: Option<i64>,
    /// Next seq per kind, by the number the kind column holds.
    counters: HashMap<u8, i64>,
    identities: ObjectHashMap<Identity>,
    /// The same rows read the other way, which the oid column's index is.
    by_seq: HashMap<(u8, i64), ObjectId>,
    branches: BTreeMap<String, Ref>,
    others: BTreeMap<String, Ref>,
    /// Pack images this repository names, and when each was retired.
    segments: HashMap<Ulid, Option<i64>>,
    /// Whether a maintenance pass holds it.
    gathering: bool,
}

impl Repo {
    /// What a resolved repository is, for one that exists.
    fn meta(&self, id: RepoId) -> Option<RepoMetadata> {
        Some(RepoMetadata {
            id,
            storage_key: self.storage_key?,
            default_branch: self.default_branch.clone(),
        })
    }

    /// The refs of one namespace.
    fn table(&mut self, branch: bool) -> &mut BTreeMap<String, Ref> {
        if branch {
            &mut self.branches
        } else {
            &mut self.others
        }
    }

    /// Whether `oid` is numbered as a commit, which a branch tip must be.
    fn numbers_commit(&self, oid: ObjectId) -> bool {
        self.identities
            .get(&oid)
            .is_some_and(|held| held.kind == Kind::Commit)
    }
}

/// Everything the store holds, under one lock.
#[derive(Debug, Default)]
struct State {
    next_id: i64,
    repos: HashMap<RepoId, Repo>,
}

impl State {
    /// One repository's rows, created empty if this is the first mention.
    ///
    /// Absent and empty answer every question here alike, which is what the
    /// other store does for an id no `repositories` row names.
    fn repo(&mut self, id: RepoId) -> &mut Repo {
        self.repos.entry(id).or_default()
    }

    /// One repository's rows as they stand, minting none.
    ///
    /// What a read wants: a question about a repository must not bring one
    /// into being, or an erased one comes back as an empty one.
    fn held(&self, id: RepoId) -> Option<&Repo> {
        self.repos.get(&id)
    }
}

/// The engine's rows, in memory.
#[derive(Debug, Default)]
pub struct Memory {
    state: Mutex<State>,
}

impl Memory {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The state, taking a poisoned lock as the state it was left in.
    ///
    /// A panic in one test must not fail every later one with a poisoning.
    fn locked(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// This repository, or `None` if it is absent or deleted.
    ///
    /// Inherent as well as a trait method, since a summary asks it per id
    /// and nothing about that question is asynchronous.
    fn lookup(&self, id: RepoId) -> Option<RepoMetadata> {
        let state = self.locked();
        let repo = state.repos.get(&id)?;
        if repo.deleted_at.is_some() {
            return None;
        }
        repo.meta(id)
    }

    /// When its default branch last moved.
    fn last_push(&self, repo: &RepoMetadata) -> Option<i64> {
        let state = self.locked();
        let held = state.held(repo.id)?;
        Some(secs(held.branches.get(&repo.default_branch)?.updated))
    }

    /// Keeps these pack images alive against the sweep.
    pub fn register_segments(&self, id: RepoId, ids: &[Ulid]) {
        let mut state = self.locked();
        let repo = state.repo(id);
        for held in ids {
            repo.segments.entry(*held).or_default();
        }
    }

    /// Stamps these pack images as copied elsewhere, leaving the rows.
    pub fn retire_segments(&self, id: RepoId, ids: &[Ulid]) {
        let at = now();
        let mut state = self.locked();
        let repo = state.repo(id);
        for held in ids {
            if let Some(retired) = repo.segments.get_mut(held) {
                retired.get_or_insert(at);
            }
        }
    }

    /// Whether this caller now holds the repository's maintenance lock.
    ///
    /// Refused rather than waited on, as the advisory lock it stands in for
    /// is: a gather told `false` takes its copy back instead of making one.
    pub fn hold_maintenance(&self, id: RepoId) -> bool {
        !std::mem::replace(&mut self.locked().repo(id).gathering, true)
    }

    /// Gives the lock back, which the transaction ending is in the other.
    pub fn release_maintenance(&self, id: RepoId) {
        self.locked().repo(id).gathering = false;
    }

    /// Takes a repository apart, the row naming it included.
    pub fn erase(&self, id: RepoId) {
        self.locked().repos.remove(&id);
    }
}

#[async_trait::async_trait]
impl crate::metadata::Metadata for Memory {
    async fn create(&self, default_branch: Option<&str>) -> Result<RepoMetadata> {
        let mut state = self.locked();
        state.next_id += 1;
        let id = RepoId::new(state.next_id);
        let meta = RepoMetadata {
            id,
            storage_key: StorageKey::new_v4(),
            default_branch: default_branch.unwrap_or("refs/heads/main").to_owned(),
        };
        let repo = state.repo(id);
        repo.storage_key = Some(meta.storage_key);
        repo.default_branch.clone_from(&meta.default_branch);
        counters(repo);
        Ok(meta)
    }

    async fn all(&self) -> Result<Vec<RepoMetadata>> {
        let state = self.locked();
        let mut found: Vec<RepoMetadata> = state
            .repos
            .iter()
            .filter(|(_, repo)| repo.deleted_at.is_none())
            .filter_map(|(id, repo)| repo.meta(*id))
            .collect();
        found.sort_by_key(|repo| repo.id.as_i64());
        Ok(found)
    }

    async fn deleted(&self, grace_secs: u64) -> Result<Vec<RepoMetadata>> {
        let cutoff = cutoff(grace_secs);
        let state = self.locked();
        let mut found: Vec<RepoMetadata> = state
            .repos
            .iter()
            .filter(|(_, repo)| repo.deleted_at.is_some_and(|at| at < cutoff))
            .filter_map(|(id, repo)| repo.meta(*id))
            .collect();
        found.sort_by_key(|repo| repo.id.as_i64());
        Ok(found)
    }

    async fn create_counters(&self, repo: RepoId) -> Result<()> {
        counters(self.locked().repo(repo));
        Ok(())
    }

    async fn allocate(&self, repo: RepoId, kind: Kind, count: u64) -> Result<i64> {
        if count == 0 {
            return Err(anyhow!("a push allocating no {kind} seqs"));
        }
        let count =
            i64::try_from(count).map_err(|_wide| anyhow!("an object batch past a bigint"))?;
        let mut state = self.locked();
        let held = state.repo(repo);
        let next = held
            .counters
            .get_mut(&kind_to_u8(kind))
            .ok_or_else(|| anyhow!("repository {repo} has no {kind} counter"))?;
        let first = *next;
        *next = next.saturating_add(count);
        Ok(first)
    }

    async fn record(&self, repo: RepoId, named: &[(ObjectId, Identity)]) -> Result<()> {
        let mut state = self.locked();
        let held = state.repo(repo);
        // Checked before anything lands, so a batch that loses the race
        // leaves nothing behind — as the copy it stands in for does.
        if named
            .iter()
            .any(|(oid, _)| held.identities.contains_key(oid))
        {
            return Err(Raced.into());
        }
        for (oid, identity) in named {
            held.identities.insert(*oid, *identity);
            held.by_seq
                .insert((kind_to_u8(identity.kind), identity.seq), *oid);
        }
        Ok(())
    }

    async fn identify(&self, repo: RepoId, oids: &[ObjectId]) -> Result<ObjectHashMap<Identity>> {
        let state = self.locked();
        let Some(held) = state.held(repo) else {
            return Ok(ObjectHashMap::default());
        };
        Ok(oids
            .iter()
            .filter_map(|oid| Some((*oid, *held.identities.get(oid)?)))
            .collect())
    }

    async fn oids_of(
        &self,
        repo: RepoId,
        kind: Kind,
        seqs: &[i64],
    ) -> Result<HashMap<i64, ObjectId>> {
        let state = self.locked();
        let Some(held) = state.held(repo) else {
            return Ok(HashMap::new());
        };
        Ok(seqs
            .iter()
            .filter_map(|seq| Some((*seq, *held.by_seq.get(&(kind_to_u8(kind), *seq))?)))
            .collect())
    }

    async fn lookup(&self, repo: RepoId) -> Result<Option<RepoMetadata>> {
        Ok(Self::lookup(self, repo))
    }

    async fn summarize(&self, repos: &[RepoId]) -> Result<Vec<RepoSummary>> {
        let mut found: Vec<RepoSummary> = repos
            .iter()
            .filter_map(|id| {
                let repo = self.lookup(*id)?;
                let last_push_unix_seconds = self.last_push(&repo);
                Some(RepoSummary {
                    repo,
                    last_push_unix_seconds,
                })
            })
            .collect();
        found.sort_by_key(|summary| summary.repo.id.as_i64());
        Ok(found)
    }

    async fn mark_deleted(&self, repo: RepoId) -> Result<bool> {
        let mut state = self.locked();
        let held = state.repo(repo);
        if held.deleted_at.is_some() {
            return Ok(false);
        }
        held.deleted_at = Some(now());
        Ok(true)
    }

    async fn ref_listing(&self, repo: RepoId) -> Result<Vec<RefEntry>> {
        let state = self.locked();
        let Some(held) = state.held(repo) else {
            return Ok(Vec::new());
        };
        let mut found: Vec<RefEntry> = held
            .branches
            .iter()
            .chain(held.others.iter())
            .map(|(refname, one)| RefEntry {
                refname: refname.clone(),
                oid: one.oid,
                updated_unix_seconds: secs(one.updated),
            })
            .collect();
        found.sort_by(|a, b| a.refname.cmp(&b.refname));
        Ok(found)
    }

    async fn referenced_segments(&self, repo: RepoId, ids: &[Ulid]) -> Result<HashSet<Ulid>> {
        let state = self.locked();
        let Some(held) = state.held(repo) else {
            return Ok(HashSet::new());
        };
        Ok(ids
            .iter()
            .filter(|one| held.segments.contains_key(one))
            .copied()
            .collect())
    }

    async fn live_segments(&self, repo: RepoId, ids: &[Ulid]) -> Result<HashSet<Ulid>> {
        let state = self.locked();
        let Some(held) = state.held(repo) else {
            return Ok(HashSet::new());
        };
        Ok(ids
            .iter()
            .filter(|one| held.segments.get(one).is_some_and(Option::is_none))
            .copied()
            .collect())
    }

    async fn drop_retired_segments(&self, repo: RepoId, grace_secs: u64) -> Result<u64> {
        let cutoff = cutoff(grace_secs);
        let mut state = self.locked();
        let held = state.repo(repo);
        let before = held.segments.len();
        held.segments
            .retain(|_, retired| !retired.is_some_and(|at| at < cutoff));
        Ok(u64::try_from(before.saturating_sub(held.segments.len())).unwrap_or(u64::MAX))
    }

    async fn refs_for(&self, repo: &RepoMetadata) -> Result<RefsMap> {
        let state = self.locked();
        let mut refs: RefsMap = state
            .held(repo.id)
            .into_iter()
            .flat_map(|held| held.branches.iter().chain(held.others.iter()))
            .map(|(refname, one)| (refname.clone(), one.oid.to_string()))
            .collect();
        refs.insert("HEAD".to_owned(), format!("ref: {}", repo.default_branch));
        Ok(refs)
    }

    async fn refs_matching(&self, repo: RepoId, refnames: &[&str]) -> Result<RefsMap> {
        let state = self.locked();
        let Some(held) = state.held(repo) else {
            return Ok(RefsMap::new());
        };
        Ok(refnames
            .iter()
            .filter_map(|refname| {
                let one = held
                    .branches
                    .get(*refname)
                    .or_else(|| held.others.get(*refname))?;
                Some(((*refname).to_owned(), one.oid.to_string()))
            })
            .collect())
    }

    async fn update_refs(
        &self,
        repo: RepoId,
        updates: &[RefUpdate],
    ) -> Result<Vec<RefUpdateResult>> {
        let mut state = self.locked();
        let held = state.repo(repo);
        // Under one lock, as the other applies them in one transaction.
        Ok(updates.iter().map(|update| apply(held, update)).collect())
    }
}

/// One counter per kind, leaving any that are already there.
fn counters(repo: &mut Repo) {
    for kind in [Kind::Commit, Kind::Tree, Kind::Blob, Kind::Tag] {
        repo.counters.entry(kind_to_u8(kind)).or_insert(0);
    }
}

/// Applies one update, dispatching on namespace as the statements do.
fn apply(repo: &mut Repo, update: &RefUpdate) -> RefUpdateResult {
    let null = ObjectId::null(gix_hash::Kind::Sha1);
    if is_funny_refname(&update.refname, update.new_id == null) {
        return crate::refs::reject_result(&update.refname, RefUpdateRejection::InvalidRefname);
    }
    let branch = is_branch_refname(&update.refname);
    if update.new_id == null {
        return delete(repo, update, null, branch);
    }
    write(repo, update, null, branch)
}

/// The `new_id == null` arm: an unguarded delete of a missing ref is a no-op,
/// and a guarded one matching nothing is non-fast-forward.
fn delete(repo: &mut Repo, update: &RefUpdate, null: ObjectId, branch: bool) -> RefUpdateResult {
    let table = repo.table(branch);
    if update.old_id == null {
        table.remove(&update.refname);
        return crate::refs::ok_result(&update.refname);
    }
    let held = table.get(&update.refname).map(|one| one.oid);
    if held == Some(update.old_id) {
        table.remove(&update.refname);
        return crate::refs::ok_result(&update.refname);
    }
    crate::refs::reject_result(&update.refname, RefUpdateRejection::NonFastForward)
}

/// The create and move arms, which differ only in what they compare against.
fn write(repo: &mut Repo, update: &RefUpdate, null: ObjectId, branch: bool) -> RefUpdateResult {
    // A branch must point at a commit the repository numbers; any other ref
    // is checked only by what walks the objects.
    let numbered = !branch || repo.numbers_commit(update.new_id);
    let at = now();
    let creating = update.old_id == null;
    let table = repo.table(branch);
    let held = table.get(&update.refname).map(|one| one.oid);
    let rejection = if !creating && held != Some(update.old_id) {
        // Decided before the tip is: a stale guard is a non-fast-forward
        // whatever it was pointed at, which is what a compare-and-set that
        // names the tip in the same statement answers first.
        Some(RefUpdateRejection::NonFastForward)
    } else if !numbered {
        Some(RefUpdateRejection::UnknownCommit)
    } else if creating && held.is_some() {
        Some(RefUpdateRejection::AlreadyExists)
    } else {
        None
    };
    if let Some(rejection) = rejection {
        return crate::refs::reject_result(&update.refname, rejection);
    }
    table.insert(
        update.refname.clone(),
        Ref {
            oid: update.new_id,
            updated: at,
        },
    );
    crate::refs::ok_result(&update.refname)
}
