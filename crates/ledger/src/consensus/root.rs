//! The root log: one replicated log, hosted by every ledger node, whose
//! state machine is the ledger's directory: which repositories exist, in
//! which incarnation, where each one's log is (its term, sequencer and
//! replicas), and who the members are. It is what `CockroachDB`'s meta
//! ranges and Kafka's controller quorum are to them, kept small.
//!
//! An incarnation starts when its `Create` entry is applied, on the replicas
//! its proposer placed it on (or, if those no longer fit, the least loaded
//! members), with its proposer as its first sequencer, from an initial state
//! that follows from the entry: every replica derives the same one, so none
//! needs it from anywhere else. An `Appoint` entry hands a log to a new
//! sequencer (and maybe new replicas), by compare-and-swap on its term, as
//! Vertical Paxos's master does (see `consensus::repo`). A `Delete` entry
//! ends an incarnation once its log committed the tombstone; nodes then drop
//! its log, and a newer incarnation never shares its log id.
//!
//! The directory is applied by the engine's loop itself (not the apply
//! thread), and kept in memory and in the root's region of the log store,
//! written with the round that applies it: one key per repository, and one
//! for the rest ([`RootMeta`]). Raft snapshots carry it whole.
//!
//! The root starts at index 1, not 0 (as `TiKV`'s regions start at 5): a node
//! joining it has an empty log, and can then only be caught up by a
//! snapshot, which carries the root's membership, never by entries that
//! would assume the membership it started with.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use prost::Message as _;
use raft::prelude::ConfState;
use raft_engine::LogBatch;

use enroute_shared::state::{RepoMeta, UPLOAD_GEN_WINDOW};
use enroute_shared::version::{self, Version};

use crate::proto::{
    Activation, Appointment, Create, Delete, Directory as Whole, Incarnation, Member,
    NamedIncarnation, RootEntry, RootMeta, Upgrade, root_entry,
};

/// How many replicas a repository's log has (fewer if there are fewer
/// members).
pub(crate) const REPLICAS: usize = 3;

/// The root log's region in the log store: even, so never a repository's
/// log (see `logdb::is_log`), and not the store's identity (2).
pub(crate) const ROOT: u64 = 4;

/// The root's key for its [`RootMeta`].
const META_KEY: &[u8] = b"root";
/// The root's keys for repositories: this prefix, then the name.
const REPO_PREFIX: &[u8] = b"repo/";
/// Just past every repository key (`/` + 1 is `0`).
const REPO_END: &[u8] = b"repo0";

/// The index (and term) the root starts at.
pub(super) const ROOT_START: u64 = 1;

/// The root log's state: the directory, which only the engine's loop
/// writes, shared with the engine's readers and the root's raft storage
/// (for snapshots); and whether this node leads the root log.
#[derive(Default)]
pub(crate) struct RootState {
    pub(crate) dir: Directory,
    pub(crate) leading: bool,
    /// The nodes alive, as this node last saw (see `control::Liveness`).
    pub(crate) alive: Vec<u64>,
    /// The nodes that answer pings at their address in the directory.
    pub(crate) answering: Vec<u64>,
}

/// The root log's state, shared.
pub(crate) type SharedRoot = std::sync::Arc<parking_lot::RwLock<RootState>>;

/// The ledger's directory: the root log's state machine.
#[derive(Default, Clone, Debug)]
pub(crate) struct Directory {
    pub(crate) meta: RootMeta,
    pub(crate) repos: BTreeMap<String, Incarnation>,
    /// Derived (not stored): the live repositories each node sequences, and
    /// how many logs each node is a replica of (see `reindex`).
    pub(crate) sequencing: HashMap<u64, BTreeSet<String>>,
    pub(crate) placed: HashMap<u64, usize>,
}

/// What applying a root entry changed.
#[derive(Debug, PartialEq)]
pub(super) enum Change {
    /// Nothing: the entry no longer applied.
    None,
    /// Incarnation `epoch` of `repo` started.
    Created(String, u64),
    /// Incarnation `epoch` of `repo` ended.
    Deleted(String, u64),
    /// The log of incarnation `epoch` of `repo` has a new term (sequencer,
    /// replicas), or its replica change ended.
    Appointed(String, u64),
    /// The members changed.
    Members,
}

impl Change {
    /// The repository it changed, if any.
    pub(super) fn repo(&self) -> Option<&str> {
        match self {
            Change::Created(r, _) | Change::Deleted(r, _) | Change::Appointed(r, _) => Some(r),
            Change::None | Change::Members => None,
        }
    }
}

/// Whether `node` hosts incarnation `inc`'s log: one of its replicas, or of
/// those it is changing from.
pub(crate) fn hosts(inc: &Incarnation, node: u64) -> bool {
    !inc.deleted && (inc.replicas.contains(&node) || inc.joint.contains(&node))
}

/// A tie-break between nodes for `repo`, the same on every node and every
/// build (the root's state depends on it).
fn spread(repo: &str, node: u64) -> u64 {
    enroute_shared::nodes::weight(repo, &node.to_le_bytes())
}

impl Directory {
    /// The directory of a new ledger, with `me` its only member, at this
    /// binary's release.
    pub(super) fn bootstrap(me: u64) -> Directory {
        Directory {
            meta: RootMeta {
                applied_index: ROOT_START,
                applied_term: ROOT_START,
                voters: vec![me],
                members: vec![me],
                version: Some(version::BINARY.into()),
                ..Default::default()
            },
            ..Directory::default()
        }
    }

    /// The cluster's version; None in a root an older release wrote.
    pub(super) fn version(&self) -> Option<Version> {
        self.meta.version.map(Into::into)
    }

    /// The root log's membership as of the last entry applied.
    pub(super) fn conf(&self) -> ConfState {
        ConfState {
            voters: self.meta.voters.clone(),
            learners: self.meta.learners.clone(),
            ..Default::default()
        }
    }

    /// The root log's membership is now `conf`.
    pub(super) fn set_conf(&mut self, conf: &ConfState) {
        self.meta.voters.clone_from(&conf.voters);
        self.meta.learners.clone_from(&conf.learners);
    }

    /// `repo`'s live incarnation, if it has one.
    pub(crate) fn live(&self, repo: &str) -> Option<&Incarnation> {
        self.repos.get(repo).filter(|i| !i.deleted)
    }

    /// Whether incarnation `epoch` of `repo` ended: deleted, or superseded
    /// by a newer one. (Not an incarnation this node has yet to hear of.)
    #[cfg(test)]
    pub(crate) fn ended(&self, repo: &str, epoch: u64) -> bool {
        self.repos
            .get(repo)
            .is_some_and(|i| i.epoch > epoch || i.epoch == epoch && i.deleted)
    }

    /// Apply a root entry: what it changed.
    pub(super) fn apply(&mut self, entry: &RootEntry) -> Vec<Change> {
        let changes = match &entry.change {
            Some(root_entry::Change::Appoint(a)) => {
                return a.appointments.iter().map(|a| self.appoint(a)).collect();
            }
            Some(root_entry::Change::Activate(a)) => {
                return a.activations.iter().map(|a| self.activate(a)).collect();
            }
            _ => self.apply_one(entry),
        };
        vec![changes]
    }

    fn apply_one(&mut self, entry: &RootEntry) -> Change {
        match &entry.change {
            Some(root_entry::Change::Create(c)) => self.create(c),
            Some(root_entry::Change::Delete(d)) => self.delete(d),
            Some(root_entry::Change::AddMember(peer)) => {
                let m = &mut self.meta;
                if m.removed.contains(&peer.id) || m.peers.contains(peer) {
                    return Change::None;
                }
                m.peers.retain(|p| p.id != peer.id);
                m.peers.push(peer.clone());
                m.peers.sort_unstable_by_key(|p| p.id);
                if !m.members.contains(&peer.id) {
                    m.members.push(peer.id);
                    m.members.sort_unstable();
                }
                Change::Members
            }
            Some(root_entry::Change::RemoveMember(id)) => {
                let m = &mut self.meta;
                // Never the last member: nothing could commit afterwards.
                if m.removed.contains(id) || m.members == [*id] {
                    return Change::None;
                }
                m.members.retain(|x| x != id);
                m.removed.push(*id);
                Change::Members
            }
            Some(root_entry::Change::Upgrade(u)) => {
                self.upgrade(u);
                Change::None
            }
            Some(root_entry::Change::Appoint(_) | root_entry::Change::Activate(_)) | None => {
                Change::None
            }
        }
    }

    /// Only forward, and only from the version the proposer saw.
    fn upgrade(&mut self, u: &Upgrade) {
        if let (Some(from), Some(to)) = (u.from, u.to)
            && self.meta.version == Some(from)
            && Version::from(to) > from.into()
        {
            self.meta.version = Some(to);
        }
    }

    /// The replicas for a log of `repo`: `keep` (those that are members),
    /// then the least placed of `among` in zones none has yet, then the
    /// least placed of the rest: up to [`REPLICAS`].
    pub(crate) fn choose(&self, repo: &str, keep: &[u64], among: &[u64]) -> Vec<u64> {
        let mut chosen: Vec<u64> = Vec::with_capacity(REPLICAS);
        for &n in keep {
            if self.meta.members.contains(&n) && !chosen.contains(&n) {
                chosen.push(n);
            }
        }
        chosen.truncate(REPLICAS);
        let mut nodes: Vec<u64> = among
            .iter()
            .copied()
            .filter(|n| self.meta.members.contains(n) && !chosen.contains(n))
            .collect();
        nodes.sort_by_key(|&n| (self.placed(n), spread(repo, n)));
        let mut zones: BTreeSet<&str> = chosen.iter().filter_map(|&n| self.zone(n)).collect();
        let (mut fresh, mut rest) = (Vec::new(), Vec::new());
        for n in nodes {
            match self.zone(n) {
                Some(z) if !zones.insert(z) => rest.push(n),
                _ => fresh.push(n),
            }
        }
        let room = REPLICAS - chosen.len();
        chosen.extend(fresh.into_iter().chain(rest).take(room));
        chosen.sort_unstable();
        chosen
    }

    /// `replicas` with one that shares a zone moved to a node of `among` in
    /// a zone they lack, as [`Self::choose`] picks it, if there is one: the
    /// sequencer stays, and one not `alive`, or else the most placed, moves.
    pub(crate) fn respread(
        &self,
        repo: &str,
        replicas: &[u64],
        sequencer: u64,
        among: &[u64],
        alive: impl Fn(u64) -> bool,
    ) -> Option<Vec<u64>> {
        let gone = (replicas.iter().copied())
            .filter(|&n| n != sequencer && self.shares_zone(n, replicas))
            .max_by_key(|&n| (!alive(n), self.placed(n), n))?;
        let kept: Vec<u64> = replicas.iter().copied().filter(|&n| n != gone).collect();
        let zoned: Vec<u64> = (among.iter().copied())
            .filter(|&n| self.zone(n).is_some())
            .collect();
        let moved = self.choose(repo, &kept, &zoned);
        let to = moved.iter().copied().find(|n| !kept.contains(n))?;
        let lacked = self
            .zone(to)
            .filter(|z| kept.iter().all(|&k| self.zone(k) != Some(*z)));
        lacked.map(|_| moved)
    }

    /// Whether two of `replicas` are in the same zone.
    pub(crate) fn crowded(&self, replicas: &[u64]) -> bool {
        replicas.iter().any(|&n| self.shares_zone(n, replicas))
    }

    /// Whether `node` is in a zone another of `replicas` is in.
    fn shares_zone(&self, node: u64, replicas: &[u64]) -> bool {
        let zone = self.zone(node);
        zone.is_some() && replicas.iter().any(|&m| m != node && self.zone(m) == zone)
    }

    /// Node `node`'s zone, if it named one.
    pub(crate) fn zone(&self, node: u64) -> Option<&str> {
        self.node(node)
            .map(|p| p.zone.as_str())
            .filter(|z| !z.is_empty())
    }

    /// Whether `replicas` are a valid placement: distinct members, as many as
    /// there can be.
    fn fits(&self, replicas: &[u64]) -> bool {
        let want = REPLICAS.min(self.meta.members.len());
        let distinct: BTreeSet<u64> = replicas.iter().copied().collect();
        distinct.len() == replicas.len()
            && replicas.len() == want
            && replicas.iter().all(|n| self.meta.members.contains(n))
    }

    fn create(&mut self, c: &Create) -> Change {
        let prior = self.repos.get(&c.repo);
        if prior.is_some_and(|i| !i.deleted) || self.meta.members.is_empty() {
            return Change::None;
        }
        // As placed by the proposer, unless that no longer fits.
        let (replicas, sequencer, boot) =
            if self.fits(&c.replicas) && c.replicas.contains(&c.sequencer) {
                (c.replicas.clone(), c.sequencer, c.boot)
            } else {
                let members = self.meta.members.clone();
                let replicas = self.choose(&c.repo, &[c.sequencer], &members);
                let sequencer = if replicas.contains(&c.sequencer) {
                    c.sequencer
                } else {
                    replicas
                        .iter()
                        .copied()
                        .min_by_key(|&n| self.sequenced(n))
                        .unwrap_or(0)
                };
                // Not the proposer's boot: the controller appoints again.
                let boot = if sequencer == c.sequencer { c.boot } else { 0 };
                (replicas, sequencer, boot)
            };
        let inc = Incarnation {
            epoch: prior.map_or(1, |p| p.epoch + 1),
            head: c.head.clone(),
            base_version: prior.map_or(0, |p| p.end_version),
            base_upload_gen: prior.map_or(0, |p| p.end_upload_gen),
            term: 1,
            sequencer,
            boot,
            replicas,
            object_format: c.object_format,
            ..Default::default()
        };
        let epoch = inc.epoch;
        self.index(&c.repo, &inc, true);
        self.repos.insert(c.repo.clone(), inc);
        Change::Created(c.repo.clone(), epoch)
    }

    fn delete(&mut self, d: &Delete) -> Change {
        match self.repos.get(&d.repo).cloned() {
            Some(mut i) if i.epoch == d.epoch && !i.deleted => {
                self.index(&d.repo, &i, false);
                i.deleted = true;
                (i.end_version, i.end_upload_gen) = (d.version, d.upload_gen);
                self.repos.insert(d.repo.clone(), i);
                Change::Deleted(d.repo.clone(), d.epoch)
            }
            _ => Change::None,
        }
    }

    /// Hand a log to a new term, if it is still at the appointment's base
    /// term, its sequencer a member and one of its replicas, and (for new
    /// replicas) no other replica change under way.
    fn appoint(&mut self, a: &Appointment) -> Change {
        let Some(old) = self.repos.get(&a.repo).cloned() else {
            return Change::None;
        };
        if old.epoch != a.epoch || old.deleted || old.term != a.base_term {
            return Change::None;
        }
        let replicas = if a.replicas.is_empty() {
            old.replicas.clone()
        } else {
            a.replicas.clone()
        };
        if !self.meta.members.contains(&a.sequencer) || !replicas.contains(&a.sequencer) {
            return Change::None;
        }
        let mut inc = old.clone();
        if replicas != old.replicas {
            if !old.joint.is_empty() || !self.fits(&replicas) {
                return Change::None;
            }
            inc.joint.clone_from(&old.replicas);
        }
        self.index(&a.repo, &old, false);
        (inc.term, inc.sequencer, inc.boot, inc.replicas) =
            (a.base_term + 1, a.sequencer, a.boot, replicas);
        self.index(&a.repo, &inc, true);
        self.repos.insert(a.repo.clone(), inc);
        Change::Appointed(a.repo.clone(), a.epoch)
    }

    /// End a replica change once its term committed on the new replicas.
    fn activate(&mut self, a: &Activation) -> Change {
        let Some(old) = self.repos.get(&a.repo).cloned() else {
            return Change::None;
        };
        if old.epoch != a.epoch || old.deleted || old.term != a.term || old.joint.is_empty() {
            return Change::None;
        }
        let mut inc = old.clone();
        inc.joint.clear();
        self.index(&a.repo, &old, false);
        self.index(&a.repo, &inc, true);
        self.repos.insert(a.repo.clone(), inc);
        Change::Appointed(a.repo.clone(), a.epoch)
    }

    /// Count (`add`) or uncount a live incarnation in the derived indexes.
    fn index(&mut self, repo: &str, inc: &Incarnation, add: bool) {
        if inc.deleted {
            return;
        }
        let seq = self.sequencing.entry(inc.sequencer).or_default();
        if add {
            seq.insert(repo.to_string());
        } else {
            seq.remove(repo);
        }
        let hosts: BTreeSet<u64> = inc.replicas.iter().chain(&inc.joint).copied().collect();
        for n in hosts {
            let c = self.placed.entry(n).or_default();
            *c = if add { *c + 1 } else { c.saturating_sub(1) };
        }
    }

    /// Rebuild the derived indexes.
    fn reindex(&mut self) {
        (self.sequencing, self.placed) = (HashMap::new(), HashMap::new());
        let repos = std::mem::take(&mut self.repos);
        for (repo, inc) in &repos {
            self.index(repo, inc, true);
        }
        self.repos = repos;
    }

    /// How many logs `node` is a replica of.
    pub(crate) fn placed(&self, node: u64) -> usize {
        self.placed.get(&node).copied().unwrap_or(0)
    }

    /// How loaded `node` is as a sequencer.
    pub(crate) fn sequenced(&self, node: u64) -> usize {
        self.sequencing.get(&node).map_or(0, BTreeSet::len)
    }

    /// The members, and the nodes removed.
    pub(super) fn members(&self) -> (Vec<u64>, Vec<u64>) {
        (self.meta.members.clone(), self.meta.removed.clone())
    }

    /// Where the members are and their token keys, by id.
    pub(crate) fn peers(&self) -> impl Iterator<Item = &Member> {
        let m = &self.meta;
        (m.peers.iter()).filter(|p| m.members.contains(&p.id))
    }

    /// Node `id` as it last joined, member or removed since: its key
    /// checks the tokens of a removed node that does not know it yet.
    pub(crate) fn node(&self, id: u64) -> Option<&Member> {
        self.meta.peers.iter().find(|p| p.id == id)
    }

    /// Whether node `id` was removed.
    pub(crate) fn removed(&self, id: u64) -> bool {
        self.meta.removed.contains(&id)
    }

    /// Whether node `id` was removed and holds no log any more: its logs
    /// moved off it, so its peers no longer take its messages.
    pub(crate) fn departed(&self, id: u64) -> bool {
        self.removed(id) && self.placed(id) == 0
    }

    /// The nodes this node exchanges messages with: the members, and the
    /// removed nodes whose logs are still moving off them.
    pub(crate) fn reachable(&self) -> impl Iterator<Item = &Member> {
        self.meta.peers.iter().filter(|p| !self.departed(p.id))
    }

    /// As a snapshot carries it.
    pub(super) fn encode(&self) -> Vec<u8> {
        Whole {
            meta: Some(self.meta.clone()),
            repos: self
                .repos
                .iter()
                .map(|(repo, i)| NamedIncarnation {
                    repo: repo.clone(),
                    incarnation: Some(i.clone()),
                })
                .collect(),
        }
        .encode_to_vec()
    }

    /// From a snapshot.
    pub(super) fn decode(data: &[u8]) -> Result<Directory> {
        let w = Whole::decode(data)?;
        let mut dir = Directory {
            meta: w.meta.unwrap_or_default(),
            repos: w
                .repos
                .into_iter()
                .map(|n| (n.repo, n.incarnation.unwrap_or_default()))
                .collect(),
            ..Directory::default()
        };
        dir.reindex();
        Ok(dir)
    }

    /// Read from the log store; None if this node has no root yet.
    pub(super) fn load(db: &raft_engine::Engine) -> Result<Option<Directory>> {
        let Some(meta) = db.get(ROOT, META_KEY) else {
            return Ok(None);
        };
        let mut dir = Directory {
            meta: RootMeta::decode(meta.as_slice())?,
            ..Directory::default()
        };
        let mut bad = None;
        db.scan_raw_messages(ROOT, Some(REPO_PREFIX), Some(REPO_END), false, |k, v| {
            let name = String::from_utf8_lossy(k.get(REPO_PREFIX.len()..).unwrap_or_default())
                .into_owned();
            match Incarnation::decode(v) {
                Ok(i) => {
                    dir.repos.insert(name, i);
                    true
                }
                Err(e) => {
                    bad = Some(e);
                    false
                }
            }
        })?;
        if let Some(e) = bad {
            return Err(e.into());
        }
        dir.reindex();
        Ok(Some(dir))
    }

    /// Write `repo`'s entry (or its absence) to `batch`.
    pub(super) fn put_repo(&self, batch: &mut LogBatch, repo: &str) -> Result<()> {
        let key = [REPO_PREFIX, repo.as_bytes()].concat();
        match self.repos.get(repo) {
            Some(i) => batch.put(ROOT, key, i.encode_to_vec())?,
            None => batch.delete(ROOT, key),
        }
        Ok(())
    }

    /// Write the rest to `batch`.
    pub(super) fn put_meta(&self, batch: &mut LogBatch) -> Result<()> {
        batch.put(ROOT, META_KEY.to_vec(), self.meta.encode_to_vec())?;
        Ok(())
    }

    /// Write what differs from `old` to `batch`.
    pub(super) fn put_all(&self, batch: &mut LogBatch, old: &Directory) -> Result<()> {
        for repo in old.repos.keys().filter(|r| !self.repos.contains_key(*r)) {
            self.put_repo(batch, repo)?;
        }
        for (repo, i) in &self.repos {
            if old.repos.get(repo) != Some(i) {
                self.put_repo(batch, repo)?;
            }
        }
        self.put_meta(batch)
    }
}

/// The initial state of incarnation `inc`: empty, with its HEAD if given
/// (kept, see `keep_head`). After a deleted incarnation, versions go on
/// increasing, and upload generations start past the window of every file
/// an older incarnation could name, so none is ever committed here.
pub(crate) fn initial(inc: &Incarnation) -> RepoMeta {
    let mut m = RepoMeta::new();
    m.epoch = inc.epoch;
    m.object_format = inc.object_format;
    if let Some(head) = &inc.head {
        (m.head, m.keep_head) = (head.clone(), true);
    }
    if inc.epoch > 1 {
        m.version = inc.base_version + 1;
        m.upload_gen = inc.base_upload_gen + UPLOAD_GEN_WINDOW + 1;
    }
    m
}

/// Whether the node with log store `db` has a root log (started one, or
/// was caught up by the root's leader).
pub(crate) fn exists(db: &raft_engine::Engine) -> bool {
    db.get(ROOT, META_KEY).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: u64) -> Member {
        Member {
            id,
            url: format!("http://n{id}"),
            key: String::new(),
            zone: String::new(),
        }
    }

    fn create(repo: &str) -> RootEntry {
        root_entry::Change::Create(Create {
            repo: repo.into(),
            sequencer: 1,
            ..Default::default()
        })
        .into()
    }

    fn delete(repo: &str, epoch: u64, version: u64, upload_gen: u64) -> RootEntry {
        root_entry::Change::Delete(Delete {
            repo: repo.into(),
            epoch,
            version,
            upload_gen,
        })
        .into()
    }

    /// The cluster's version moves only forward, from the version the
    /// proposer saw.
    #[test]
    fn upgrades_only_go_forward_from_the_version_seen() {
        let v = |minor| Some(enroute_shared::proto::Version { major: 0, minor });
        let up = |from, to| RootEntry::from(root_entry::Change::Upgrade(Upgrade { from, to }));
        let mut d = Directory::bootstrap(1);
        d.meta.version = v(4);
        for (from, to) in [(3, 5), (4, 4), (4, 3)] {
            one(&mut d, &up(v(from), v(to)));
            assert_eq!(d.meta.version, v(4));
        }
        one(&mut d, &up(v(4), v(5)));
        assert_eq!(d.meta.version, v(5));
    }

    fn one(d: &mut Directory, e: &RootEntry) -> Change {
        let mut c = d.apply(e);
        assert_eq!(c.len(), 1);
        c.remove(0)
    }

    fn appoint(repo: &str, base_term: u64, sequencer: u64, replicas: Vec<u64>) -> RootEntry {
        root_entry::Change::Appoint(crate::proto::Appoint {
            appointments: vec![Appointment {
                repo: repo.into(),
                epoch: 1,
                base_term,
                sequencer,
                replicas,
                boot: 5,
            }],
        })
        .into()
    }

    #[test]
    fn incarnations_follow_creates_and_deletes() {
        let mut d = Directory::bootstrap(7);
        assert_eq!(one(&mut d, &create("a")), Change::Created("a".into(), 1));
        assert_eq!(one(&mut d, &create("a")), Change::None);
        let a = d.live("a").unwrap();
        // The proposer (1) is not a member: placed on the members.
        assert_eq!(
            (a.replicas.as_slice(), a.sequencer, a.term),
            (&[7][..], 7, 1)
        );
        // Only the live incarnation ends.
        assert_eq!(one(&mut d, &delete("a", 2, 5, 9)), Change::None);
        assert_eq!(
            one(&mut d, &delete("a", 1, 5, 9)),
            Change::Deleted("a".into(), 1)
        );
        assert!(d.live("a").is_none() && d.ended("a", 1) && !d.ended("a", 2));
        assert_eq!(one(&mut d, &create("a")), Change::Created("a".into(), 2));
        let m = initial(d.live("a").unwrap());
        assert_eq!((m.epoch, m.version), (2, 6));
        assert_eq!(m.upload_gen, 9 + UPLOAD_GEN_WINDOW + 1);
        assert!(d.ended("a", 1) && !d.ended("a", 2));
    }

    #[test]
    fn removed_members_never_come_back() {
        use root_entry::Change::{AddMember, RemoveMember};
        let mut d = Directory::bootstrap(1);
        assert_eq!(one(&mut d, &RemoveMember(1).into()), Change::None);
        assert_eq!(one(&mut d, &AddMember(peer(2)).into()), Change::Members);
        assert_eq!(one(&mut d, &AddMember(peer(2)).into()), Change::None);
        // A member back at another address.
        let moved = Member {
            url: "http://elsewhere".into(),
            ..peer(2)
        };
        assert_eq!(
            one(&mut d, &AddMember(moved.clone()).into()),
            Change::Members
        );
        assert_eq!(d.peers().find(|p| p.id == 2), Some(&moved));
        assert_eq!(d.meta.members, [1, 2]);
        assert_eq!(one(&mut d, &RemoveMember(2).into()), Change::Members);
        assert!(d.peers().all(|p| p.id != 2));
        assert_eq!(d.node(2), Some(&moved), "a removed node's key is kept");
        assert_eq!(one(&mut d, &AddMember(peer(2)).into()), Change::None);
        assert_eq!(
            (d.meta.members.as_slice(), d.meta.removed.as_slice()),
            (&[1][..], &[2][..])
        );
        // A new incarnation's replicas are members.
        d.apply(&AddMember(peer(3)).into());
        d.apply(&create("r"));
        assert_eq!(d.live("r").unwrap().replicas, [1, 3]);
    }

    #[test]
    fn placement_spreads_logs_over_the_least_placed() {
        use root_entry::Change::AddMember;
        let mut d = Directory::bootstrap(1);
        for n in 2..=6 {
            d.apply(&AddMember(peer(n)).into());
        }
        for i in 0..60 {
            d.apply(&create(&format!("r{i}")));
        }
        for n in 1..=6 {
            let placed = d.placed(n);
            // Node 1 proposed every one, and is on every one.
            let want = if n == 1 { 60 } else { 24 };
            assert_eq!(placed, want, "node {n}");
        }
        assert_eq!(d.sequenced(1), 60);
    }

    /// A directory of members 1..=n, member `i` in `zones[i - 1]`.
    fn zoned(zones: &[&str]) -> Directory {
        use root_entry::Change::AddMember;
        let mut d = Directory::bootstrap(1);
        for (id, z) in (1..).zip(zones) {
            let zone = (*z).to_string();
            d.apply(&AddMember(Member { zone, ..peer(id) }).into());
        }
        d
    }

    #[test]
    fn placement_puts_replicas_in_distinct_zones_first() {
        // Two nodes in a, two in b, one in c: every log gets a, b and c.
        let mut d = zoned(&["a", "a", "b", "b", "c"]);
        let zone = |d: &Directory, n| d.zone(n).unwrap().to_string();
        for i in 0..40 {
            let repo = format!("r{i}");
            let proposer = [1, 2, 3, 4, 5][i % 5];
            let replicas = d.choose(&repo, &[proposer], &[1, 2, 3, 4, 5]);
            assert!(replicas.contains(&proposer));
            let zones: BTreeSet<String> = replicas.iter().map(|&n| zone(&d, n)).collect();
            assert_eq!(zones.len(), 3, "{repo}: {replicas:?}");
            d.apply(
                &root_entry::Change::Create(Create {
                    repo,
                    replicas,
                    sequencer: proposer,
                    ..Default::default()
                })
                .into(),
            );
        }
        // Zone c's one node is on every log; a and b split theirs.
        assert_eq!(d.placed(5), 40);
        for n in 1..=4 {
            assert_eq!(d.placed(n), 20, "node {n}");
        }
        // With c down, a log still gets its two zones, then the least placed.
        let replicas = d.choose("down", &[], &[1, 2, 3, 4]);
        let zones: BTreeSet<String> = replicas.iter().map(|&n| zone(&d, n)).collect();
        assert_eq!((replicas.len(), zones.len()), (3, 2));
    }

    #[test]
    fn placement_without_zones_is_by_load() {
        let d = zoned(&["", "", "", "", ""]);
        assert!(!d.crowded(&[1, 2, 3]));
        assert_eq!(d.respread("r", &[1, 2, 3], 1, &[4, 5], |_| true), None);
        assert_eq!(d.choose("r", &[1], &[1, 2, 3, 4, 5]).len(), 3);
    }

    #[test]
    fn a_crowded_log_moves_one_replica_to_a_zone_it_lacks() {
        let d = zoned(&["a", "a", "b", "b", "c"]);
        assert!(d.crowded(&[1, 2, 3]) && !d.crowded(&[1, 3, 5]));
        // The sequencer (1) stays; its zone partner moves to c.
        assert_eq!(
            d.respread("r", &[1, 2, 3], 1, &[4, 5], |_| true),
            Some(vec![1, 3, 5])
        );
        assert_eq!(
            d.respread("r", &[1, 2, 3], 2, &[4, 5], |_| true),
            Some(vec![2, 3, 5])
        );
        // Of two sharing a zone, a down one moves first.
        let d = zoned(&["a", "a", "a", "b"]);
        assert_eq!(
            d.respread("r", &[1, 2, 3], 1, &[4], |n| n != 3),
            Some(vec![1, 2, 4])
        );
        // Nowhere better: c is not up (not among the candidates).
        let d = zoned(&["a", "a", "b", "b", "c"]);
        assert_eq!(d.respread("r", &[1, 2, 3], 1, &[4], |_| true), None);
        assert_eq!(d.respread("r", &[1, 3, 5], 1, &[2, 4], |_| true), None);
    }

    #[test]
    fn appointments_compare_and_swap_terms() {
        use root_entry::Change::{AddMember, RemoveMember};
        let mut d = Directory::bootstrap(1);
        d.apply(&AddMember(peer(2)).into());
        d.apply(&AddMember(peer(3)).into());
        d.apply(&create("r"));
        assert_eq!(d.live("r").unwrap().replicas, [1, 2, 3]);
        // Not from the current term, or not a replica: refused.
        assert_eq!(one(&mut d, &appoint("r", 0, 2, vec![])), Change::None);
        assert_eq!(one(&mut d, &appoint("r", 1, 9, vec![])), Change::None);
        assert_eq!(
            one(&mut d, &appoint("r", 1, 2, vec![])),
            Change::Appointed("r".into(), 1)
        );
        let i = d.live("r").unwrap();
        assert_eq!((i.term, i.sequencer, i.boot), (2, 2, 5));
        assert_eq!((d.sequenced(1), d.sequenced(2)), (0, 1));
        // A replica change keeps the old replicas until activated.
        d.apply(&AddMember(peer(4)).into());
        d.apply(&RemoveMember(3).into());
        assert_eq!(
            one(&mut d, &appoint("r", 2, 2, vec![1, 2, 4])),
            Change::Appointed("r".into(), 1)
        );
        let i = d.live("r").unwrap();
        assert_eq!(
            (i.replicas.as_slice(), i.joint.as_slice()),
            (&[1, 2, 4][..], &[1, 2, 3][..])
        );
        assert!(hosts(i, 3) && hosts(i, 4));
        // One change at a time.
        assert_eq!(
            one(&mut d, &appoint("r", 3, 2, vec![1, 2, 5])),
            Change::None
        );
        let activate = |term| {
            root_entry::Change::Activate(crate::proto::Activate {
                activations: vec![Activation {
                    repo: "r".into(),
                    epoch: 1,
                    term,
                }],
            })
            .into()
        };
        assert_eq!(one(&mut d, &activate(2)), Change::None);
        assert_eq!(one(&mut d, &activate(3)), Change::Appointed("r".into(), 1));
        let i = d.live("r").unwrap();
        assert!(i.joint.is_empty() && !hosts(i, 3));
        assert_eq!(d.placed.get(&3).copied().unwrap_or(0), 0);
    }

    #[test]
    fn round_trips_through_snapshots() {
        let mut d = Directory::bootstrap(1);
        d.apply(&create("x"));
        d.apply(&create("y"));
        let back = Directory::decode(&d.encode()).unwrap();
        assert_eq!(back.meta, d.meta);
        assert_eq!(back.repos, d.repos);
        assert_eq!(back.placed, d.placed);
        assert_eq!(back.sequencing, d.sequencing);
    }
}
