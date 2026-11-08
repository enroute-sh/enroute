//! The root log's driver: its raft-rs node, the proposals and reads
//! waiting on it, how it recovers, and how each round collects, applies
//! and compacts it. The root is the one log that elects its own leader,
//! so it is the one raft-rs runs; the repository logs are
//! [`super::super::shardlog`]'s.

use enroute_shared::version;

use super::*;

/// The root log, as this node runs it.
pub(super) struct RootLog {
    pub(super) raw: RawNode<LogStore>,
    /// The last entry applied (the directory's), as durable.
    pub(super) durable: u64,
    /// Proposals in flight, by id: answered once applied here.
    pub(super) pending: HashMap<u64, (oneshot::Sender<Result<()>>, Instant)>,
    pub(super) reads: HashMap<u64, RootRead>,
    /// Whether it has work.
    pub(super) dirty: bool,
}

/// A root read index request issued to raft.
pub(super) struct RootRead {
    pub(super) reader: Reader,
    pub(super) issued: Instant,
    pub(super) deadline: Instant,
}

pub(super) fn raft_config(node_id: u64, applied: u64) -> RaftCfg {
    RaftCfg {
        id: node_id,
        election_tick: ELECTION_TICKS,
        heartbeat_tick: HEARTBEAT_TICKS,
        applied,
        pre_vote: true,
        check_quorum: true,
        max_size_per_msg: 1 << 20,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

/// Whether this binary runs the cluster at `dir`'s version.
fn supported(dir: &Directory) -> Result<()> {
    let v = dir
        .version()
        .context("the root log has no cluster version: an older release wrote it")?;
    version::check(v)
}

/// A committed root entry's payload, if it has one; an error if this
/// release cannot read it, as when a newer release wrote it.
fn readable(e: &Entry) -> Result<Option<RootEntry>> {
    if e.get_entry_type() != EntryType::EntryNormal || e.data.is_empty() {
        return Ok(None);
    }
    let entry = RootEntry::decode(e.data.as_slice()).with_context(|| {
        format!(
            "root entry {} does not decode: a newer release wrote it?",
            e.index
        )
    })?;
    anyhow::ensure!(
        entry.change.is_some(),
        "root entry {} is of a kind this release does not know: a newer release wrote it",
        e.index
    );
    Ok(Some(entry))
}

/// Recover the root log, or start it: as its only voter if `bootstrap` (a
/// new ledger; written at once), else empty, for the root's leader to catch
/// up with a snapshot once it adds this node.
pub(super) fn recover_root(
    db: &LogDb,
    node: u64,
    logger: &slog::Logger,
    root: &SharedRoot,
    bootstrap: bool,
) -> Result<RootLog> {
    let found = Directory::load(db)?;
    let (snapshot, entries, hard_state) = db.read(read_root)?;
    let mut meta = LogMeta::default();
    meta.hard_state = hard_state.unwrap_or_default();
    meta.snapshot = snapshot.unwrap_or_default();
    let dir = match found {
        Some(dir) => {
            supported(&dir)?;
            meta.conf = dir.conf();
            meta.append(entries.iter().map(|e| (e.index, e.term)));
            let applied = dir.meta.applied_index;
            anyhow::ensure!(
                applied >= meta.snapshot.get_metadata().index,
                "the root's directory is at entry {applied}, before its log's start"
            );
            if meta.conf.voters == [node] {
                meta.hard_state.commit = meta.hard_state.commit.max(meta.last_index());
            }
            meta.hard_state.commit = meta.hard_state.commit.max(applied);
            dir
        }
        None if bootstrap => {
            let dir = Directory::bootstrap(node);
            meta.conf = dir.conf();
            meta.snapshot = meta.snapshot_at(ROOT_START, ROOT_START);
            (meta.hard_state.term, meta.hard_state.commit) = (ROOT_START, ROOT_START);
            let mut batch = LogBatch::default();
            put_snapshot(&mut batch, ROOT, &meta.snapshot);
            batch.put_value::<Prost, _>(ROOT, HARD_STATE_KEY.to_vec(), &meta.hard_state)?;
            dir.put_meta(&mut batch)?;
            db.write(&mut batch, true)?;
            tracing::info!(node, "started a new ledger");
            dir
        }
        None => Directory::default(),
    };
    let applied = dir.meta.applied_index;
    root.write().dir = dir;
    let store = LogStore::new(ROOT, db.clone(), meta, root.clone());
    Ok(RootLog {
        raw: RawNode::new(&raft_config(node, applied), store, logger)?,
        durable: applied,
        pending: HashMap::new(),
        reads: HashMap::new(),
        dirty: true,
    })
}

/// The root log as stored: where it starts (its snapshot), its entries
/// after that, and its hard state.
pub(super) type RootStored = (Option<Snapshot>, Vec<Entry>, Option<HardState>);

pub(super) fn read_root(db: &raft_engine::Engine) -> raft_engine::Result<RootStored> {
    let snapshot = db.get_value::<Snapshot, Prost>(ROOT, SNAPSHOT_KEY)?;
    let hard_state = db.get_value::<HardState, Prost>(ROOT, HARD_STATE_KEY)?;
    let start = snapshot.as_ref().map_or(0, |s| s.get_metadata().index);
    let entries = match db.last_index(ROOT).filter(|&l| l > start) {
        Some(last) => fetch(db, ROOT, start + 1, last + 1, None)?,
        None => Vec::new(),
    };
    Ok((snapshot, entries, hard_state))
}

/// Apply a committed root membership change to the raft core and its
/// storage view: the voters and learners after it.
pub(super) fn apply_conf_change(
    raw: &mut RawNode<LogStore>,
    cc: &ConfChangeV2,
) -> Option<ConfState> {
    let cs = raw.apply_conf_change(cc).ok()?;
    tracing::info!(voters = ?cs.voters, learners = ?cs.learners, "root membership changed");
    raw.store().meta.write().conf = cs.clone();
    Some(cs)
}

/// A membership change entry's change (only `ConfChangeV2`s are proposed).
pub(super) fn conf_change(e: &Entry) -> Option<ConfChangeV2> {
    use raft_prost::Message as _;
    match e.get_entry_type() {
        EntryType::EntryConfChangeV2 => ConfChangeV2::decode(e.data.as_slice()).ok(),
        _ => None,
    }
}

/// What the root's storage view takes once a round is durable: a
/// snapshot, the entries' (index, term), and the hard state.
pub(super) type RootPersisted = (Option<Snapshot>, Vec<(u64, u64)>, Option<HardState>);

/// Add a log's snapshot to a write.
#[expect(
    clippy::expect_used,
    reason = "invariant: the snapshot key is not reserved"
)]
pub(super) fn put_snapshot(batch: &mut LogBatch, id: u64, snap: &Snapshot) {
    batch
        .put_value::<Prost, _>(id, SNAPSHOT_KEY.to_vec(), snap)
        .expect("snapshot key is not reserved");
}

impl Loop {
    /// Propose a root entry nobody waits for (it is proposed again if it
    /// is lost).
    pub(super) fn propose_root_quietly(&mut self, entry: &RootEntry) {
        let (resp, _) = oneshot::channel();
        self.next_id += 1;
        let pid = self.next_id;
        self.wait(
            ROOT,
            Wait::Root {
                pid,
                data: entry.encode_to_vec(),
                resp,
            },
        );
    }

    pub(super) fn retry_root(&mut self, w: Waiting, now: Instant) {
        if w.deadline < now {
            reject(
                w.kind,
                anyhow::anyhow!("no leader for the root log (timed out)"),
            );
            return;
        }
        let r = &self.root_log.raw.raft;
        let (leading, no_leader) = (r.state == StateRole::Leader, r.leader_id == 0);
        // A new leader, until it commits an entry of its term, may not have
        // applied all the log committed (and raft-rs silently drops its
        // read index requests).
        if no_leader || (leading && !r.commit_to_current_term()) {
            if no_leader && r.prs().conf().voters().ids().len() == 1 {
                drop(self.root_log.raw.campaign());
            }
            self.root_log.dirty = true;
            self.waiting.push(w);
            return;
        }
        match w.kind {
            Wait::Read(reader) => {
                self.next_id += 1;
                let rid = self.next_id;
                self.root_log.raw.read_index(rid.to_le_bytes().to_vec());
                self.root_log.reads.insert(
                    rid,
                    RootRead {
                        reader,
                        issued: now,
                        deadline: w.deadline,
                    },
                );
            }
            Wait::Root { pid, data, resp } => {
                // A follower forwards it to the leader. Dropped (no leader
                // just now): tried again.
                if self
                    .root_log
                    .raw
                    .propose(pid.to_le_bytes().to_vec(), data.clone())
                    .is_err()
                {
                    self.waiting.push(Waiting {
                        log: ROOT,
                        deadline: w.deadline,
                        kind: Wait::Root { pid, data, resp },
                    });
                    return;
                }
                self.root_log.pending.insert(pid, (resp, w.deadline));
            }
            Wait::Evaluate(p) => drop(p.resp.send(Err(anyhow::anyhow!("not a repository log")))),
        }
        self.root_log.dirty = true;
    }

    /// If this node leads the root log and its voters are not the members,
    /// take the next step: add a member as a learner, make a caught-up
    /// learner a voter, make a removed voter a learner (the leader too: it
    /// hands off first), or remove a removed learner once it holds no log.
    pub(super) fn reconcile_root(&mut self) {
        let raw = &mut self.root_log.raw;
        let raft = &raw.raft;
        // One change at a time, after an entry in this term (the fix to
        // single-server changes).
        if raft.state != StateRole::Leader
            || raft.has_pending_conf()
            || !raft.commit_to_current_term()
        {
            return;
        }
        let conf = raw.store().meta.read().conf.clone();
        let (want, removed) = (&self.members, &self.removed);
        let floor = raft
            .raft_log
            .committed
            .saturating_sub(CATCH_UP)
            .max(raft.pending_conf_index);
        let caught_up = |n: u64| raft.prs().get(n).is_some_and(|p| p.matched >= floor);
        let unknown = |n: &&u64| !conf.voters.contains(n) && !conf.learners.contains(n);
        // A removed node stays a learner while its logs move off it, so that
        // it learns of each move.
        let root = self.root.read();
        let change = if let Some(&n) = want.iter().find(unknown) {
            Some((ConfChangeType::AddLearnerNode, n))
        } else if let Some(&n) =
            (conf.learners.iter()).find(|&&n| want.contains(&n) && caught_up(n))
        {
            Some((ConfChangeType::AddNode, n))
        } else if let Some(&n) = (conf.voters.iter()).find(|&&n| removed.contains(&n)) {
            Some((ConfChangeType::AddLearnerNode, n))
        } else {
            (conf.learners.iter())
                .find(|&&n| root.dir.departed(n))
                .map(|&n| (ConfChangeType::RemoveNode, n))
        };
        drop(root);
        let Some((ty, node)) = change else { return };
        if ty != ConfChangeType::AddNode && node == self.node_id {
            self.leave_root(&conf.voters, floor);
            return;
        }
        let cc = ConfChangeV2 {
            changes: vec![ConfChangeSingle {
                node_id: node,
                change_type: ty.into(),
            }],
            ..Default::default()
        };
        tracing::info!(node, change = ?ty, "changing the root's membership");
        if let Err(e) = raw.propose_conf_change(Vec::new(), cc) {
            tracing::warn!("proposing a root membership change: {e:#}");
            return;
        }
        self.root_log.dirty = true;
    }

    /// A removed root leader would go on leading a group it is leaving: it
    /// hands leadership to a caught-up voter first, and the next leader
    /// makes it a learner.
    pub(super) fn leave_root(&mut self, voters: &[u64], floor: u64) {
        let raw = &mut self.root_log.raw;
        let me = self.node_id;
        let to = (voters.iter()).copied().find(|&n| {
            n != me
                && !self.removed.contains(&n)
                && raw.raft.prs().get(n).is_some_and(|p| p.matched >= floor)
        });
        if let Some(to) = to
            && raw.raft.lead_transferee.is_none()
        {
            tracing::info!(to, "handing the root's leadership off before leaving");
            raw.transfer_leader(to);
            self.root_log.dirty = true;
        }
    }

    /// Collect the root log's `Ready` into the round.
    pub(super) fn collect_root(
        &mut self,
        batch: &mut LogBatch,
        work: &mut RoundWork,
    ) -> Result<()> {
        let l = &mut self.root_log;
        if !l.raw.has_ready() {
            return Ok(());
        }
        let mut rd = l.raw.ready();
        let msgs = rd.take_messages();
        let mut snapshot = (!rd.snapshot().is_empty()).then(|| rd.snapshot().clone());
        let mut root_snapshot = None;
        if let Some(snap) = snapshot.as_mut() {
            root_snapshot = Some(std::mem::take(&mut snap.data));
            self.stats.root_snapshots += 1;
            put_snapshot(batch, ROOT, snap);
            let last = l.raw.store().meta.read().last_index();
            let index = snap.get_metadata().index.max(last) + 1;
            batch.add_command(ROOT, raft_engine::Command::Compact { index });
        }
        batch.add_entries_with::<Entries, Prost>(ROOT, rd.entries())?;
        let terms = rd.entries().iter().map(|e| (e.index, e.term)).collect();
        let hs = rd.hs().cloned();
        if let Some(hs) = &hs {
            batch.put_value::<Prost, _>(ROOT, HARD_STATE_KEY.to_vec(), hs)?;
        }
        let snap_index = snapshot.as_ref().map(|s| s.get_metadata().index);
        work.root_persisted = Some((snapshot, terms, hs));
        for rs in rd.take_read_states() {
            let rid = ctx_id(&rs.request_ctx);
            if let Some(RootRead { reader, .. }) = l.reads.remove(&rid) {
                self.read_applied.push((ROOT, rs.index, reader));
            }
        }
        let leading = l.raw.raft.state == StateRole::Leader;
        if self.root.read().leading != leading {
            self.root.write().leading = leading;
        }
        let committed = rd.take_committed_entries();
        work.root_held = rd.take_persisted_messages();
        work.root_round = Some(rd.number());
        l.raw.advance_append_async(rd);
        // A snapshot is applied with this round (`install_root`): raft-rs
        // must know, or its applied index stays below the log's start, and
        // a scan of the entries after it (as on becoming leader) fails.
        if let Some(index) = snap_index {
            l.raw.advance_apply_to(index);
        }
        let mut confs = HashMap::new();
        if let Some(last) = committed.last().map(|e| e.index) {
            for (e, cc) in committed.iter().filter_map(|e| Some((e, conf_change(e)?))) {
                confs.extend(apply_conf_change(&mut l.raw, &cc).map(|cs| (e.index, cs)));
            }
            l.raw.advance_apply_to(last);
        }
        if let Some(data) = root_snapshot {
            self.install_root(&data, batch)?;
        }
        self.apply_root(&committed, &confs, batch)?;
        for m in msgs {
            let to = m.to;
            self.send(to, root_frame(&m));
        }
        if self.root_log.raw.has_ready() {
            self.root_log.dirty = true;
        }
        Ok(())
    }

    /// Apply committed root entries, here in the loop: the directory's
    /// changes go into this round's `batch`, their effects on the hosted
    /// logs follow, and their proposers are answered once it is durable.
    pub(super) fn apply_root(
        &mut self,
        entries: &[Entry],
        confs: &HashMap<u64, ConfState>,
        batch: &mut LogBatch,
    ) -> Result<()> {
        let Some(last) = entries.last().map(|e| (e.index, e.term)) else {
            return Ok(());
        };
        for e in entries {
            let pid = ctx_id(&e.context);
            if let Some((resp, _)) = self.root_log.pending.remove(&pid) {
                self.replies.push(resp);
            }
        }
        let mut changes = Vec::new();
        let mut root = self.root.write();
        let dir = &mut root.dir;
        for e in entries {
            if let Some(cs) = confs.get(&e.index) {
                dir.set_conf(cs);
                continue;
            }
            let Some(entry) = readable(e)? else {
                continue;
            };
            let applied = dir.apply(&entry);
            // Before anything past it is applied, or seen by readers.
            supported(dir)?;
            for repo in applied.iter().filter_map(Change::repo) {
                dir.put_repo(batch, repo)?;
            }
            changes.extend(applied);
        }
        (dir.meta.applied_index, dir.meta.applied_term) = last;
        dir.put_meta(batch)?;
        drop(root);
        // Durable with this round's write, before any compaction's.
        self.root_log.durable = last.0;
        for change in changes {
            self.on_root_change(change);
        }
        self.maybe_snapshot(ROOT);
        Ok(())
    }

    /// Take the directory from a snapshot of the root (in this round's
    /// `batch`, with the log's reset to it): drop the logs it says are not
    /// this node's, host those it says are, and take its members.
    pub(super) fn install_root(&mut self, data: &[u8], batch: &mut LogBatch) -> Result<()> {
        let dir = Directory::decode(data)?;
        supported(&dir)?;
        let applied = dir.meta.applied_index;
        // Only the loop writes the directory: readers wait just for the swap.
        dir.put_all(batch, &self.root.read().dir)?;
        let repos: Vec<String> = {
            let mut root = self.root.write();
            root.dir = dir;
            let hosted = self.hosted.read();
            hosted
                .keys()
                .cloned()
                .chain(
                    (root.dir.repos.iter())
                        .filter(|(_, i)| hosts(i, self.node_id))
                        .map(|(r, _)| r.clone()),
                )
                .collect()
        };
        self.root_log.durable = applied;
        for repo in repos {
            self.sync_repo(&repo);
        }
        self.members_changed();
        let root = self.root.read();
        self.controller.note_all(&root.dir);
        Ok(())
    }

    /// The root log's snapshot at its durably applied index, now its start
    /// in memory (None if that is its start already).
    pub(super) fn compact_root(&self) -> Option<Snapshot> {
        let store = self.root_log.raw.store();
        let index = self.root_log.durable;
        let snap = {
            let m = store.meta.read();
            if index <= m.snapshot.get_metadata().index {
                return None;
            }
            m.snapshot_at(index, m.term(index)?)
        };
        store.meta.write().compact(snap.clone());
        Some(snap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(data: Vec<u8>) -> Entry {
        let mut e = Entry::default();
        e.set_entry_type(EntryType::EntryNormal);
        e.index = 7;
        e.data = data;
        e
    }

    /// A root entry this release cannot read stops the node rather than
    /// being skipped.
    #[test]
    fn unreadable_root_entries_are_errors() {
        assert!(readable(&entry(Vec::new())).unwrap().is_none());
        let known = RootEntry::from(root_entry::Change::RemoveMember(2));
        assert!(readable(&entry(known.encode_to_vec())).unwrap().is_some());
        // Field 8 of the oneof, which this release does not have.
        let unknown = readable(&entry(vec![0x40, 0x01]));
        assert!(unknown.unwrap_err().to_string().contains("does not know"));
        readable(&entry(vec![0xff, 0xff, 0xff])).unwrap_err();
    }
}
