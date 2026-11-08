//! The replicated state machine, in two halves. A proposer *evaluates* a
//! command against the state it sees ([`evaluate`]): every rule lives
//! there (compare-and-swaps, refusals, id assignment), and the result is
//! the command's outcome and the change it makes. The log carries the
//! change, as an [`Effect`], and every replica *applies* it
//! ([`apply_effect`]): a patch, written only if the state is still the one
//! it was evaluated against, so replicas never re-run the rules and cannot
//! disagree on them. Subscribers apply the same changes
//! ([`enroute_shared::state::RepoMeta::apply_change`]).
//!
//! As the leaseholder in `CockroachDB` evaluates a command and replicates its
//! effect, guarded by a lease index, the log's leader evaluates here. The
//! guard is the version (and evaluation chain) the effect was evaluated
//! against, as in execute-order-validate (Hyperledger Fabric): an old
//! leader's effects may still commit after a new leader took over.
//!
//! It reads and writes the state through [`State`]: the node's state store
//! ([`crate::state::stored`]), or, in tests, a `RepoMeta` in memory. It reads only what the
//! command needs (the refs it compare-and-swaps, the layers a replacement
//! removes), so a command costs the same however big the repository.
//!
//! The manifest is in id order, and every layer has at least one object
//! id, so first ids are unique and keep it in order (the store keys it by
//! them).

use anyhow::{Context, Result};

use enroute_shared::oid::{ObjectFormat, Oid};
use enroute_shared::state::{
    ATOMIC_FAILURE, Change, ChangeTarget, Command, Commit, LayerEdit, LayerMeta, Outcome,
    REF_EXISTS, REF_STALE, RefChange, RefResult, RefUpdate, Refusal, RemovedLayer, Scalars,
    UPLOAD_GEN_WINDOW, apply_change as shared_apply, command, layer_gen,
};

use crate::proto::Effect;

pub(crate) use crate::state::store::Counters;

/// A repository's state, as the state machine sees it. Reads see the
/// command's own writes; the manifest is only read before a command
/// changes it.
pub(crate) trait State {
    fn get_ref(&mut self, name: &str) -> Result<Option<Oid>>;
    // `updated_ms`: when the ref is set (0: unknown).
    fn set_ref(&mut self, name: &str, value: Option<Oid>, updated_ms: u64) -> Result<()>;
    /// The first ref starting with `prefix`, in name order.
    fn first_ref(&mut self, prefix: &str) -> Result<Option<String>>;
    fn head(&mut self) -> Result<String>;
    fn set_head(&mut self, head: &str) -> Result<()>;
    fn counters(&mut self) -> Result<Counters>;
    fn set_counters(&mut self, c: Counters) -> Result<()>;
    /// The layer of the manifest with this first id.
    fn layer(&mut self, first_id: u32) -> Result<Option<LayerMeta>>;
    /// The first layer of the manifest whose first id is at least
    /// `first_id`.
    fn next_layer(&mut self, first_id: u32) -> Result<Option<LayerMeta>>;
    /// Add a layer to the manifest (in its place by id).
    fn add_layer(&mut self, layer: &LayerMeta) -> Result<()>;
    /// Remove a layer of the manifest.
    fn remove_layer(&mut self, layer: &LayerMeta) -> Result<()>;
    /// Drop every ref and layer (inline bytes and pins too).
    fn clear(&mut self) -> Result<()>;
}

/// The outcome of `cmd` if it needs no evaluation: refusals that hold
/// whatever the log does next (the incarnation is over, or it is another
/// one's), and malformed commands.
pub(crate) fn settled(c: &Counters, cmd: &Command) -> Option<Outcome> {
    if c.deleted {
        return Some(on_deleted(cmd, c.version));
    }
    let mut out = Outcome {
        repo_version: c.version,
        ..Default::default()
    };
    if cmd.epoch != 0 && cmd.epoch != c.epoch {
        out.refuse(Refusal::OtherIncarnation);
        return Some(out);
    }
    if cmd.kind.is_none() {
        out.refuse_invalid("empty command");
        return Some(out);
    }
    None
}

/// Evaluate a command in evaluation chain `chain` against `state`, writing
/// its change there too (so the next command evaluated sees it): its
/// outcome, and the change, if it makes one. Fails only if the state
/// cannot be read or written.
pub(crate) fn evaluate(
    state: &mut impl State,
    cmd: &Command,
    chain: u64,
) -> Result<(Outcome, Option<Change>)> {
    let mut c = state.counters()?;
    if let Some(out) = settled(&c, cmd) {
        return Ok((out, None));
    }
    c.chain = chain;
    let out = Outcome {
        repo_version: c.version,
        ..Default::default()
    };
    match &cmd.kind {
        None => Ok((out, None)),
        Some(command::Kind::Delete(_)) => evaluate_delete(state, c, out),
        Some(command::Kind::AdvanceUploadGen(_)) => evaluate_advance(state, c, out),
        Some(command::Kind::Commit(commit)) => evaluate_commit(state, c, out, commit),
    }
}

/// Whether `effect` applies to a state with counters `c`: the one it was
/// evaluated against.
pub(crate) fn applies(c: &Counters, effect: &Effect) -> bool {
    (c.version, c.chain) == (effect.base, effect.base_chain)
}

/// Apply a log entry's effect to `state`: its change, if it applies (see
/// [`applies`]). Whether it did. It reads only what the patch needs.
pub(crate) fn apply_effect(state: &mut impl State, effect: &Effect) -> Result<bool> {
    let c = state.counters()?;
    if !applies(&c, effect) {
        return Ok(false);
    }
    if let Some(change) = &effect.change {
        apply_change(state, c, change)?;
    }
    Ok(true)
}

/// Write `change` over a state with counters `c`.
fn apply_change(state: &mut impl State, c: Counters, change: &Change) -> Result<()> {
    shared_apply(&mut Applying { state, c }, change)
}

/// A state with its counters, as a change writes to them.
struct Applying<'a, S> {
    state: &'a mut S,
    c: Counters,
}

impl<S: State> ChangeTarget for Applying<'_, S> {
    type Error = anyhow::Error;

    fn clear(&mut self) -> Result<()> {
        self.state.clear()?;
        self.c.layers = 0;
        self.c.deleted = true;
        Ok(())
    }

    fn set_ref(&mut self, name: &str, value: Option<Oid>, updated_ms: u64) -> Result<()> {
        self.state.set_ref(name, value, updated_ms)
    }

    fn set_head(&mut self, head: &str) -> Result<()> {
        self.state.set_head(head)
    }

    fn edit_layers(&mut self, e: &LayerEdit) -> Result<()> {
        for r in &e.remove {
            self.state.remove_layer(&LayerMeta {
                id: r.id.clone(),
                first_id: r.first_id,
                ..Default::default()
            })?;
        }
        for l in &e.insert {
            self.state.add_layer(l)?;
        }
        self.c.layers = self.c.layers - count(e.remove.len()) + count(e.insert.len());
        Ok(())
    }

    fn set_scalars(&mut self, s: &Scalars) -> Result<()> {
        let c = &mut self.c;
        s.apply([
            &mut c.next_id,
            &mut c.upload_gen,
            &mut c.last_push_ms,
            &mut c.version,
            &mut c.chain,
        ]);
        self.state.set_counters(*c)
    }
}

/// The outcome of `cmd` on a deleted repository at `version`.
pub(crate) fn on_deleted(cmd: &Command, version: u64) -> Outcome {
    let mut out = Outcome {
        repo_version: version,
        ..Default::default()
    };
    // Deleting again is not an error: deletion is idempotent.
    if !matches!(cmd.kind, Some(command::Kind::Delete(_))) {
        out.refuse(Refusal::Deleted);
    }
    out
}

/// The number of items as a counter (a length always fits in 64 bits).
pub(crate) fn count(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

fn evaluate_delete(
    state: &mut impl State,
    mut c: Counters,
    mut out: Outcome,
) -> Result<(Outcome, Option<Change>)> {
    state.clear()?;
    c.layers = 0;
    c.deleted = true;
    c.version += 1;
    state.set_counters(c)?;
    out.repo_version = c.version;
    Ok((
        out,
        Some(Change {
            version: c.version,
            chain: c.chain,
            deleted: true,
            ..Default::default()
        }),
    ))
}

fn evaluate_advance(
    state: &mut impl State,
    mut c: Counters,
    mut out: Outcome,
) -> Result<(Outcome, Option<Change>)> {
    c.upload_gen += 1;
    c.version += 1;
    state.set_counters(c)?;
    out.repo_version = c.version;
    Ok((
        out,
        Some(Change {
            version: c.version,
            chain: c.chain,
            upload_gen: Some(c.upload_gen),
            ..Default::default()
        }),
    ))
}

fn evaluate_commit(
    state: &mut impl State,
    mut c: Counters,
    mut out: Outcome,
    commit: &Commit,
) -> Result<(Outcome, Option<Change>)> {
    let Some(plan) = plan_layers(state, &c, commit, &mut out)? else {
        return Ok((out, None));
    };
    let errors = check_refs(state, c.format, &commit.updates, commit.atomic)?;
    let mut change = Change::default();
    if !plan.added.is_empty() || !commit.remove_layers.is_empty() {
        for l in &plan.removed {
            state.remove_layer(l)?;
        }
        for l in &plan.added {
            state.add_layer(l)?;
        }
        c.layers = c.layers - count(plan.removed.len()) + count(plan.added.len());
        change.layers = Some(LayerEdit {
            remove: commit.remove_layers.clone(),
            insert: plan.added,
        });
    }
    if plan.next_id != c.next_id {
        c.next_id = plan.next_id;
        change.next_id = Some(plan.next_id);
    }
    for (u, r) in commit.updates.iter().zip(&errors) {
        if r.is_some() {
            continue;
        }
        let updated_at_ms = if u.new.is_some() {
            commit.updated_at_ms
        } else {
            0
        };
        state.set_ref(&u.name, u.new, updated_at_ms)?;
        change.refs.push(RefChange {
            name: u.name.clone(),
            value: u.new,
            updated_at_ms,
        });
    }
    if commit.pushed_at_ms > 0 && !change.refs.is_empty() {
        // Monotonic, whatever the proposers' clocks.
        c.last_push_ms = c.last_push_ms.max(commit.pushed_at_ms);
        change.last_push_ms = Some(c.last_push_ms);
    }
    if !c.keep_head {
        change.head = fix_head(state)?;
    }
    out.ref_results = errors
        .into_iter()
        .map(|error| RefResult { error })
        .collect();
    if change.layers.is_none() && change.refs.is_empty() && change.head.is_none() {
        return Ok((out, None));
    }
    c.version += 1;
    state.set_counters(c)?;
    out.repo_version = c.version;
    (change.version, change.chain) = (c.version, c.chain);
    Ok((out, Some(change)))
}

/// What a commit does to the manifest.
struct LayerPlan {
    /// The layers to add, with their ids.
    added: Vec<LayerMeta>,
    /// The layers they replace.
    removed: Vec<LayerMeta>,
    /// The next free object id once they are added.
    next_id: u64,
}

/// The manifest edit of `commit`, or None if it is refused (`out` then says
/// why).
fn plan_layers(
    state: &mut impl State,
    c: &Counters,
    commit: &Commit,
    out: &mut Outcome,
) -> Result<Option<LayerPlan>> {
    if commit
        .add_layers
        .iter()
        .any(|l| !check_generation(c.upload_gen, l, out))
    {
        return Ok(None);
    }
    let mut added = commit.add_layers.clone();
    if !commit.remove_layers.is_empty() {
        // The manifest is read for replacements only.
        let Some(removed) = plan_replacement(state, &added, &commit.remove_layers, out)? else {
            return Ok(None);
        };
        return Ok(Some(LayerPlan {
            added,
            removed,
            next_id: c.next_id,
        }));
    }
    if added.iter().any(|l| l.objects == 0) {
        out.refuse_invalid("a layer without objects");
        return Ok(None);
    }
    // New layers: fresh ids.
    let mut next_id = c.next_id;
    for l in &mut added {
        l.first_id = u32::try_from(next_id).unwrap_or(u32::MAX);
        l.id_span = l.objects;
        next_id += u64::from(l.objects);
    }
    if next_id > u64::from(u32::MAX) {
        out.refuse_invalid("repository object id space exhausted");
        return Ok(None);
    }
    Ok(Some(LayerPlan {
        added,
        removed: Vec::new(),
        next_id,
    }))
}

/// What the layers `remove_layers` (flush, compaction) of the manifest are,
/// if `added` replaces them: consecutive layers, keeping their ids. None if
/// it is refused (`out` then says why). Only those layers are read: a range
/// read would also step over every layer deleted since the store last
/// compacted.
fn plan_replacement(
    state: &mut impl State,
    added: &[LayerMeta],
    remove_layers: &[RemovedLayer],
    out: &mut Outcome,
) -> Result<Option<Vec<LayerMeta>>> {
    let mut removed = Vec::with_capacity(remove_layers.len());
    for r in remove_layers {
        match state.layer(r.first_id)? {
            Some(l) if l.id == r.id => removed.push(l),
            _ => {
                out.refuse_invalid("layers to replace are no longer present");
                return Ok(None);
            }
        }
    }
    for w in removed.windows(2) {
        let [prev, next] = w else { continue };
        if !are_consecutive(state, prev, next)? {
            out.refuse_invalid("layers to replace are not consecutive");
            return Ok(None);
        }
    }
    let first = removed.first().context("no layers to replace")?;
    let mut at = u64::from(first.first_id);
    // In id order, as checked above.
    let end = removed.last().map_or(at, LayerMeta::end_id);
    for l in added {
        if u64::from(l.first_id) < at || l.end_id() > end {
            out.refuse_invalid("replacement layers' ids are not within the replaced layers' range");
            return Ok(None);
        }
        if l.id_span == 0 {
            out.refuse_invalid("a replacement layer without ids");
            return Ok(None);
        }
        at = l.end_id();
    }
    Ok(Some(removed))
}

/// Whether `next` follows `prev` directly in the manifest. Ranges are
/// disjoint, so a layer between them starts in the gap between their ranges.
fn are_consecutive(state: &mut impl State, prev: &LayerMeta, next: &LayerMeta) -> Result<bool> {
    Ok(match u64::from(next.first_id).cmp(&prev.end_id()) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => true,
        std::cmp::Ordering::Greater => state
            .next_layer(u32::try_from(prev.end_id())?)?
            .is_some_and(|l| l.id == next.id),
    })
}

/// The compare-and-swap check of each ref update, all against the state
/// before the command: None for a ref that can change, else why not. A
/// `format` repository's refs hold ids of that format only.
fn check_refs(
    state: &mut impl State,
    format: ObjectFormat,
    updates: &[RefUpdate],
    atomic: bool,
) -> Result<Vec<Option<String>>> {
    let mut errors = Vec::with_capacity(updates.len());
    for u in updates {
        if let Some(bad) = [u.old, u.new]
            .into_iter()
            .flatten()
            .find(|o| o.format() != format)
        {
            errors.push(Some(format!("{bad} is not a {format} object id")));
            continue;
        }
        let cur = state.get_ref(&u.name)?;
        errors.push((cur != u.old).then(|| {
            if u.old.is_none() {
                REF_EXISTS
            } else {
                REF_STALE
            }
            .to_string()
        }));
    }
    if atomic && errors.iter().any(Option::is_some) {
        for r in &mut errors {
            r.get_or_insert_with(|| ATOMIC_FAILURE.into());
        }
        // Layers are still published: their objects are valid and
        // are reclaimed by compaction if unreachable.
    }
    Ok(errors)
}

/// Whether a repository of upload generation `upload_gen` can take the
/// layer `l` now; if not, `out` says why. A stored layer's upload
/// generation must be within the window (see `RepoMeta.upload_gen`). Inline
/// layers are in the log, not the object store, and have no file to sweep.
fn check_generation(upload_gen: u64, l: &LayerMeta, out: &mut Outcome) -> bool {
    if !l.is_stored() {
        return true;
    }
    match layer_gen(&l.id) {
        None => out.refuse_invalid(format!("layer id {:?} has no upload generation", l.id)),
        Some(g) if g > upload_gen => {
            out.refuse_invalid(format!("layer {} is of a future upload generation", l.id));
        }
        Some(g) if g + UPLOAD_GEN_WINDOW < upload_gen => out.refuse(Refusal::StaleUpload),
        Some(_) => return true,
    }
    false
}

/// If HEAD points at a branch that does not exist, point it at a sensible
/// existing branch (main, master, or the first one). The new HEAD, if it
/// moved. Not for a HEAD set at creation (`keep_head`): that one names the
/// repository's default branch, which stays even when the branch is deleted.
fn fix_head(state: &mut impl State) -> Result<Option<String>> {
    let head = state.head()?;
    if state.get_ref(&head)?.is_some() {
        return Ok(None);
    }
    let mut pick = None;
    for c in ["refs/heads/main", "refs/heads/master"] {
        if state.get_ref(c)?.is_some() {
            pick = Some(c.to_string());
            break;
        }
    }
    let Some(pick) = pick.map_or_else(|| state.first_ref("refs/heads/"), |p| Ok(Some(p)))? else {
        return Ok(None);
    };
    if pick == head {
        return Ok(None);
    }
    state.set_head(&pick)?;
    Ok(Some(pick))
}

/// A repository's state in memory, which the tests check a state store's
/// state against.
#[cfg(test)]
impl State for enroute_shared::state::RepoMeta {
    fn get_ref(&mut self, name: &str) -> Result<Option<Oid>> {
        Ok(self.refs.get(name).copied())
    }

    fn set_ref(&mut self, name: &str, value: Option<Oid>, updated_ms: u64) -> Result<()> {
        self.set_ref_at(name, value, updated_ms);
        Ok(())
    }

    fn first_ref(&mut self, prefix: &str) -> Result<Option<String>> {
        Ok(self
            .refs
            .range(prefix.to_string()..)
            .next()
            .map(|(k, _)| k)
            .filter(|k| k.starts_with(prefix))
            .cloned())
    }

    fn head(&mut self) -> Result<String> {
        Ok(self.head.clone())
    }

    fn set_head(&mut self, head: &str) -> Result<()> {
        self.head = head.to_string();
        Ok(())
    }

    fn counters(&mut self) -> Result<Counters> {
        Ok(Counters::of(self))
    }

    fn set_counters(&mut self, c: Counters) -> Result<()> {
        c.apply_to(self);
        Ok(())
    }

    fn layer(&mut self, first_id: u32) -> Result<Option<LayerMeta>> {
        Ok(self
            .next_layer(first_id)?
            .filter(|l| l.first_id == first_id))
    }

    fn next_layer(&mut self, first_id: u32) -> Result<Option<LayerMeta>> {
        let at = self.layers.partition_point(|l| l.first_id < first_id);
        Ok(self.layers.get(at).cloned())
    }

    fn add_layer(&mut self, layer: &LayerMeta) -> Result<()> {
        let at = self.layers.partition_point(|l| l.first_id < layer.first_id);
        self.layers.insert(at, layer.clone());
        Ok(())
    }

    fn remove_layer(&mut self, layer: &LayerMeta) -> Result<()> {
        self.layers.retain(|l| l.id != layer.id);
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        self.refs.clear();
        self.ref_updated_ms.clear();
        self.layers.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use enroute_shared::oid::Oid;
    use enroute_shared::state::{LayerMeta, RefUpdate, RepoMeta, layer_id};

    use super::*;

    /// A stored layer's id: `name` (as a number) of upload generation 0.
    fn update(name: &str, old: Option<Oid>, new: Option<Oid>) -> RefUpdate {
        RefUpdate {
            name: name.into(),
            old,
            new,
        }
    }

    fn sid(name: &str) -> String {
        layer_id(name.bytes().fold(0u128, |n, b| n * 256 + u128::from(b)), 0)
    }

    /// Stored layer `name`, at `first_id`, to remove.
    fn removed(name: &str, first_id: u32) -> RemovedLayer {
        RemovedLayer {
            id: sid(name),
            first_id,
        }
    }

    const LOG: u64 = 1;

    /// A repository's state twice: in memory and in a state store, each
    /// command applied to both.
    struct Twin {
        m: RepoMeta,
        store: crate::state::store::StateStore,
        /// The log: what pinned inline layers are read from.
        log: std::collections::HashMap<u64, Effect>,
        _dir: tempfile::TempDir,
    }

    impl std::ops::Deref for Twin {
        type Target = RepoMeta;

        fn deref(&self) -> &RepoMeta {
            &self.m
        }
    }

    impl Twin {
        fn new() -> Twin {
            let dir = tempfile::tempdir().unwrap();
            let store = crate::state::store::StateStore::open(dir.path(), 16 << 20).unwrap();
            let m = RepoMeta::new();
            let root = crate::state::stored::root("r", 1, 0, 0);
            store
                .install(LOG, root, crate::state::stored::entries(&m).unwrap())
                .unwrap();
            Twin {
                m,
                store,
                log: std::collections::HashMap::default(),
                _dir: dir,
            }
        }

        /// Evaluate the command against both states, which must agree on
        /// its outcome and change, then apply its effect at log index
        /// `index` to both, which must then agree on the state.
        fn apply(&mut self, index: u64, cmd: &Command) -> (Outcome, Option<Change>) {
            let (mem, effect) = run(&mut self.m, cmd);
            let mut round = self.store.round();
            let stored = evaluate(
                &mut crate::state::stored::Stored::new(&mut round, LOG, 0),
                cmd,
                CHAIN,
            )
            .unwrap();
            drop(round);
            assert_eq!(
                stored, mem,
                "the store and memory agree on the outcome and change"
            );
            let mut round = self.store.round();
            let applied =
                crate::state::stored::apply(&mut round, LOG, index, 1, Some(&effect)).unwrap();
            round.commit().unwrap();
            assert_eq!(
                applied,
                Some(true),
                "an effect applies where it was evaluated"
            );
            self.log.insert(index, effect);
            let (root, loaded) =
                crate::state::stored::load(&self.store, LOG, |i| Ok(self.log[&i].clone()))
                    .unwrap()
                    .unwrap();
            assert_eq!(loaded, self.m, "the store and memory agree on the state");
            assert_eq!((root.applied_index, root.version), (index, self.m.version));
            assert_eq!(
                self.store.round().counters(LOG).unwrap().layers,
                count(self.m.layers.len())
            );
            mem
        }

        fn set_next_id(&mut self, next_id: u64) {
            self.m.next_id = next_id;
            let mut round = self.store.round();
            let c = round.counters(LOG).unwrap();
            round.set_counters(LOG, Counters { next_id, ..c });
            round.commit().unwrap();
        }
    }

    /// The chain the tests' effects are evaluated in.
    const CHAIN: u64 = 7;

    /// Evaluate `cmd` against `m`, then apply its effect: its outcome and
    /// change, and the effect. Applying writes what evaluating did.
    fn run(m: &mut RepoMeta, cmd: &Command) -> ((Outcome, Option<Change>), Effect) {
        let mut evaluated = m.clone();
        let done = evaluate(&mut evaluated, cmd, CHAIN).unwrap();
        let effect = Effect {
            base: m.version,
            base_chain: m.chain,
            chain: CHAIN,
            change: done.1.clone(),
        };
        assert!(apply_effect(m, &effect).unwrap());
        assert_eq!(
            evaluated, *m,
            "applying the effect writes what evaluating did"
        );
        (done, effect)
    }

    /// Apply a command, and check that its change takes a follower (who
    /// had the state before it) to the same state.
    fn apply_followed(m: &mut Twin, index: u64, cmd: &Command) -> Outcome {
        let mut follower = m.m.clone();
        let (out, change) = m.apply(index, cmd);
        if let Some(c) = change {
            follower.apply_change(&c);
        }
        assert_eq!(
            follower, m.m,
            "a follower applying the change reaches the same state"
        );
        out
    }

    #[test]
    fn cas_and_compaction() {
        let mut m = Twin::new();
        let (a, b) = (Oid::sha1([1; 20]), Oid::sha1([2; 20]));
        let layer = |id: &str| LayerMeta {
            id: sid(id),
            objects: 1,
            bytes: 1,
            ..Default::default()
        };
        let commit = |add: Vec<LayerMeta>, remove: Vec<&str>, updates: Vec<RefUpdate>| {
            Command::commit(
                "r",
                Commit {
                    add_layers: add,
                    // The first ids the layers below get.
                    remove_layers: remove
                        .into_iter()
                        .map(|id| removed(id, u32::from(id == "l2")))
                        .collect(),
                    updates,
                    ..Default::default()
                },
            )
        };
        let first = commit(
            vec![layer("l1")],
            vec![],
            vec![update("refs/heads/x", None, Some(a))],
        );
        let out = apply_followed(&mut m, 1, &first);
        assert_eq!(out.ref_errors().collect::<Vec<_>>(), vec![None]);
        assert_eq!(m.head, "refs/heads/x");
        let second = commit(
            vec![layer("l2")],
            vec![],
            vec![
                update("refs/heads/x", Some(b), Some(a)),
                update("refs/heads/y", None, Some(b)),
            ],
        );
        let out = apply_followed(&mut m, 2, &second);
        assert!(out.ref_results[0].error.is_some() && out.ref_results[1].error.is_none());
        let merged = LayerMeta {
            first_id: 0,
            id_span: 2,
            ..layer("c")
        };
        let replace = commit(vec![merged.clone()], vec!["l1", "l2"], vec![]);
        let out = apply_followed(&mut m, 3, &replace);
        assert!(out.error.is_none(), "{out:?}");
        assert_eq!(m.layers, vec![merged]);
        // An output without ids would share its first id with a neighbour.
        let no_ids = LayerMeta {
            id_span: 0,
            ..layer("e")
        };
        let out = apply_followed(&mut m, 4, &commit(vec![no_ids], vec!["c"], vec![]));
        assert!(out.error.is_some());
        let out = apply_followed(&mut m, 4, &commit(vec![layer("d")], vec!["l1"], vec![]));
        assert!(out.error.is_some());
        assert_eq!(m.version, 4);
        let empty = LayerMeta {
            objects: 0,
            ..layer("f")
        };
        let out = apply_followed(&mut m, 4, &commit(vec![empty], vec![], vec![]));
        assert!(out.error.is_some(), "a new layer without objects");
        // Deleting a ref, and a commit that changes nothing.
        let delete = commit(vec![], vec![], vec![update("refs/heads/y", Some(b), None)]);
        apply_followed(&mut m, 5, &delete);
        assert!(!m.refs.contains_key("refs/heads/y"));
        let stale = commit(vec![], vec![], vec![update("refs/heads/x", Some(b), None)]);
        let (_, change) = m.apply(6, &stale);
        assert!(change.is_none(), "a rejected update changes nothing");
    }

    /// Two proposers evaluate against the same state: the effect that
    /// lands first applies, and the other is skipped, and so is everything
    /// its chain evaluated after it, even where the version matches again.
    #[test]
    fn effects_apply_only_to_the_state_they_were_evaluated_against() {
        let mut m = RepoMeta::new();
        let (a, b) = (Oid::sha1([1; 20]), Oid::sha1([2; 20]));
        let effect = |state: &mut RepoMeta, chain, cmd: &Command| Effect {
            base: state.version,
            base_chain: state.chain,
            chain,
            change: evaluate(state, cmd, chain).unwrap().1,
        };
        // Chain 1 evaluates two commands, the second after the first.
        let mut one = m.clone();
        let a1 = effect(&mut one, 1, &set_ref("refs/heads/x", None, Some(a)));
        let a2 = effect(&mut one, 1, &set_ref("refs/heads/x", Some(a), Some(b)));
        // Chain 2 evaluates one, against the same state.
        let mut two = m.clone();
        let b1 = effect(&mut two, 2, &set_ref("refs/heads/x", None, Some(b)));
        assert!(apply_effect(&mut m, &b1).unwrap());
        assert!(
            !apply_effect(&mut m, &a1).unwrap(),
            "evaluated against an older state"
        );
        assert_eq!(m.version, a2.base, "the version matches a2's");
        assert!(!apply_effect(&mut m, &a2).unwrap(), "but not the state");
        assert_eq!(m.refs["refs/heads/x"], b);
    }

    #[test]
    fn layers_get_id_ranges_that_replacements_keep() {
        let mut m = Twin::new();
        let push = |id: &str, objects: u32| {
            // Ids a pushing compute sends are ignored.
            let l = LayerMeta {
                id: sid(id),
                objects,
                first_id: 99,
                id_span: 99,
                ..Default::default()
            };
            Command::commit(
                "r",
                Commit {
                    add_layers: vec![l],
                    ..Default::default()
                },
            )
        };
        let replace = |remove: &[&str], add: &[(&str, u32, u32)]| {
            let add_layers = add.iter().map(|&(id, first_id, id_span)| LayerMeta {
                id: sid(id),
                first_id,
                id_span,
                ..Default::default()
            });
            // The first ids the pushes below get.
            let first_id = |id: &str| match id {
                "a" => 0,
                "b" | "x" => 3,
                "c" => 5,
                "y" => 6,
                _ => 9,
            };
            Command::commit(
                "r",
                Commit {
                    add_layers: add_layers.collect(),
                    remove_layers: remove.iter().map(|&id| removed(id, first_id(id))).collect(),
                    ..Default::default()
                },
            )
        };
        let ranges = |m: &Twin| {
            m.layers
                .iter()
                .map(|l| (l.id.clone(), l.first_id, l.id_span))
                .collect::<Vec<_>>()
        };
        let r = |id: &str, first: u32, span: u32| (sid(id), first, span);
        for (i, (id, n)) in [("a", 3), ("b", 2), ("c", 4), ("d", 1)]
            .into_iter()
            .enumerate()
        {
            apply_followed(&mut m, count(i) + 1, &push(id, n));
        }
        assert_eq!(
            ranges(&m),
            vec![r("a", 0, 3), r("b", 3, 2), r("c", 5, 4), r("d", 9, 1)]
        );
        assert_eq!(m.next_id, 10);

        // Not consecutive, or outside the replaced range: rejected.
        let o = apply_followed(&mut m, 5, &replace(&["a", "c"], &[("x", 0, 9)]));
        assert!(o.error.is_some());
        let o = apply_followed(&mut m, 6, &replace(&["b", "c"], &[("x", 2, 7)]));
        assert!(o.error.is_some());
        let o = apply_followed(
            &mut m,
            7,
            &replace(&["b", "c"], &[("x", 3, 4), ("y", 6, 3)]),
        );
        assert!(o.error.is_some(), "overlapping outputs");

        // Two outputs within b..c, with a hole between them.
        let o = apply_followed(
            &mut m,
            8,
            &replace(&["b", "c"], &[("x", 3, 2), ("y", 6, 3)]),
        );
        assert!(o.error.is_none(), "{o:?}");
        assert_eq!(
            ranges(&m),
            vec![r("a", 0, 3), r("x", 3, 2), r("y", 6, 3), r("d", 9, 1)]
        );
        assert_eq!(m.next_id, 10, "replacements take no new ids");

        // Across the hole: x is between a and y, so a and y are not
        // consecutive; x and y are.
        let o = apply_followed(&mut m, 9, &replace(&["a", "y"], &[("z", 0, 9)]));
        assert!(o.error.is_some());
        let o = apply_followed(&mut m, 9, &replace(&["x", "y"], &[("z", 3, 6)]));
        assert!(o.error.is_none(), "{o:?}");
        assert_eq!(ranges(&m), vec![r("a", 0, 3), r("z", 3, 6), r("d", 9, 1)]);
        apply_followed(&mut m, 10, &push("e", 5));
        assert_eq!(ranges(&m).last(), Some(&r("e", 10, 5)));

        // The id space is bounded.
        m.set_next_id(u64::from(u32::MAX) - 2);
        let o = apply_followed(&mut m, 11, &push("f", 3));
        assert!(o.error.is_some() && m.layers.len() == 4);
    }

    /// Stored layers are added only from the last few upload generations
    /// (and not from future ones); inline layers have no file and are
    /// never refused for it. Replacements are checked too.
    #[test]
    fn stored_layers_must_be_of_a_recent_upload_generation() {
        let mut m = Twin::new();
        let stored = |generation: u64, n: u128| LayerMeta {
            id: layer_id(n, generation),
            objects: 1,
            bytes: 1,
            ..Default::default()
        };
        let add = |l: LayerMeta| {
            Command::commit(
                "r",
                Commit {
                    add_layers: vec![l],
                    ..Default::default()
                },
            )
        };
        let mut index = 0;
        let mut step = |m: &mut Twin, cmd: &Command| {
            index += 1;
            apply_followed(m, index, cmd)
        };
        assert!(step(&mut m, &add(stored(0, 1))).error.is_none());
        assert!(
            step(&mut m, &add(stored(1, 2)))
                .error
                .as_deref()
                .is_some_and(|e| e.contains("future"))
        );
        for g in 1..=3 {
            let o = step(&mut m, &Command::advance_upload_gen("r"));
            assert!(o.error.is_none());
            assert_eq!((m.upload_gen, o.repo_version), (g, m.version));
        }
        // Generation 3: 1..=3 are in the window, 0 is not.
        assert_eq!(
            step(&mut m, &add(stored(0, 3))).refused(),
            Some(Refusal::StaleUpload)
        );
        assert!(step(&mut m, &add(stored(1, 4))).error.is_none());
        assert!(step(&mut m, &add(stored(3, 5))).error.is_none());
        let inline = LayerMeta {
            id: "inline".into(),
            objects: 1,
            data: Some(b"x".to_vec().into()),
            ..Default::default()
        };
        assert!(
            step(&mut m, &add(inline)).error.is_none(),
            "inline layers have no generation"
        );
        let o = step(
            &mut m,
            &add(LayerMeta {
                id: "no-generation".into(),
                objects: 1,
                ..Default::default()
            }),
        );
        assert!(
            o.error
                .as_deref()
                .is_some_and(|e| e.contains("no upload generation")),
            "{o:?}"
        );
        // A compaction output from an expired generation is refused, and
        // the layers it would replace stay.
        let before = m.layers.clone();
        let (first, last) = (&m.layers[1], &m.layers[2]);
        let replace = |l: LayerMeta| {
            Command::commit(
                "r",
                Commit {
                    add_layers: vec![l],
                    remove_layers: vec![first.into(), last.into()],
                    ..Default::default()
                },
            )
        };
        let merged = |generation: u64| LayerMeta {
            first_id: first.first_id,
            id_span: u32::try_from(last.end_id()).unwrap() - first.first_id,
            ..stored(generation, 6)
        };
        let (stale, fresh) = (replace(merged(0)), replace(merged(2)));
        assert_eq!(step(&mut m, &stale).refused(), Some(Refusal::StaleUpload));
        assert_eq!(m.layers, before);
        assert!(step(&mut m, &fresh).error.is_none());
    }

    /// Many commands applied in one round of the store (as the apply thread
    /// does) reach the same state as one at a time in memory: a push, its
    /// flush (reading the manifest the round itself wrote) and a HEAD move.
    #[test]
    fn one_round_applies_many_commands() {
        let mut m = Twin::new();
        let a = Oid::sha1([1; 20]);
        let inline = LayerMeta {
            id: "i1".into(),
            objects: 2,
            bytes: 3,
            data: Some(b"abc".to_vec().into()),
            ..Default::default()
        };
        let flushed = LayerMeta {
            id: sid("f"),
            objects: 2,
            bytes: 3,
            first_id: 0,
            id_span: 2,
            ..Default::default()
        };
        let up = |name: &str, old: Option<Oid>, new: Option<Oid>| RefUpdate {
            name: name.into(),
            old,
            new,
        };
        let cmds = [
            Command::commit(
                "r",
                Commit {
                    add_layers: vec![inline],
                    updates: vec![up("refs/heads/main", None, Some(a))],
                    ..Default::default()
                },
            ),
            Command::commit(
                "r",
                Commit {
                    add_layers: vec![flushed],
                    remove_layers: vec![RemovedLayer {
                        id: "i1".into(),
                        first_id: 0,
                    }],
                    ..Default::default()
                },
            ),
            Command::commit(
                "r",
                Commit {
                    updates: vec![
                        up("refs/heads/b", None, Some(a)),
                        up("refs/heads/main", Some(a), None),
                    ],
                    ..Default::default()
                },
            ),
        ];
        let mut round = m.store.round();
        for (i, cmd) in cmds.iter().enumerate() {
            let index = count(i) + 1;
            let (_, effect) = run(&mut m.m, cmd);
            let applied =
                crate::state::stored::apply(&mut round, LOG, index, 1, Some(&effect)).unwrap();
            assert_eq!(applied, Some(true));
        }
        round.commit().unwrap();
        let (_, loaded) =
            crate::state::stored::load(&m.store, LOG, |_| anyhow::bail!("no pins left"))
                .unwrap()
                .unwrap();
        assert_eq!(loaded, m.m);
        assert_eq!((m.head.as_str(), m.layers.len()), ("refs/heads/b", 1));
        assert_eq!(m.store.oldest_pin(LOG).unwrap(), None);
    }

    /// A deletion drops the refs, the manifest and the pins of inline
    /// layers (so the log is not held back), and every later command is
    /// refused; deleting again is not an error.
    #[test]
    fn a_deletion_is_a_tombstone() {
        let mut m = Twin::new();
        let a = Oid::sha1([1; 20]);
        let push = Command::commit(
            "r",
            Commit {
                add_layers: vec![LayerMeta {
                    id: "i1".into(),
                    objects: 1,
                    data: Some(b"abc".to_vec().into()),
                    ..Default::default()
                }],
                updates: vec![RefUpdate {
                    name: "refs/heads/main".into(),
                    old: None,
                    new: Some(a),
                }],
                ..Default::default()
            },
        );
        apply_followed(&mut m, 1, &push);
        assert_eq!(m.store.oldest_pin(LOG).unwrap(), Some(1));
        let o = apply_followed(&mut m, 2, &Command::delete("r"));
        assert!(o.error.is_none());
        assert!(m.deleted && m.refs.is_empty() && m.layers.is_empty());
        assert_eq!(m.store.oldest_pin(LOG).unwrap(), None);
        let version = m.version;
        let o = apply_followed(&mut m, 3, &push);
        assert_eq!(o.refused(), Some(Refusal::Deleted));
        let o = apply_followed(&mut m, 4, &Command::advance_upload_gen("r"));
        assert_eq!(o.refused(), Some(Refusal::Deleted));
        let o = apply_followed(&mut m, 5, &Command::delete("r"));
        assert!(o.error.is_none(), "deleting again succeeds");
        assert_eq!(m.version, version, "and changes nothing");
    }

    /// A command prepared against another incarnation is refused.
    #[test]
    fn commands_for_another_incarnation_are_refused() {
        let mut m = Twin::new();
        let mut round = m.store.round();
        let c = round.counters(LOG).unwrap();
        round.set_counters(LOG, Counters { epoch: 3, ..c });
        round.commit().unwrap();
        m.m.epoch = 3;
        let cmd = |epoch| Command::advance_upload_gen("r").at(epoch);
        assert_eq!(
            apply_followed(&mut m, 1, &cmd(2)).refused(),
            Some(Refusal::OtherIncarnation)
        );
        assert_eq!(
            apply_followed(&mut m, 2, &Command::delete("r").at(4)).refused(),
            Some(Refusal::OtherIncarnation)
        );
        assert!(!m.deleted);
        assert!(apply_followed(&mut m, 3, &cmd(3)).error.is_none());
        assert!(apply_followed(&mut m, 4, &cmd(0)).error.is_none());
        assert_eq!(m.upload_gen, 2);
    }

    /// A push's time is the last push's once a ref moves; a push whose
    /// refs are all refused leaves it. It never goes back.
    #[test]
    fn pushes_stamp_their_time() {
        let mut m = Twin::new();
        let (a, b) = (Oid::sha1([1; 20]), Oid::sha1([2; 20]));
        let push = |old: Option<Oid>, new: Oid, at: u64| {
            Command::commit(
                "r",
                Commit {
                    updates: vec![RefUpdate {
                        name: "refs/heads/main".into(),
                        old,
                        new: Some(new),
                    }],
                    pushed_at_ms: at,
                    ..Default::default()
                },
            )
        };
        apply_followed(&mut m, 1, &push(None, a, 1000));
        assert_eq!(m.last_push_ms, 1000);
        let o = apply_followed(&mut m, 2, &push(None, b, 2000));
        assert!(o.ref_results[0].error.is_some());
        assert_eq!(m.last_push_ms, 1000, "a refused push");
        apply_followed(&mut m, 3, &push(Some(a), b, 500));
        assert_eq!(m.last_push_ms, 1000, "an earlier clock");
        apply_followed(&mut m, 4, &push(Some(b), a, 0));
        assert_eq!(m.last_push_ms, 1000, "not a push");
        apply_followed(&mut m, 5, &push(Some(a), b, 3000));
        assert_eq!(m.last_push_ms, 3000);
    }

    /// Each ref keeps the time of the commit that last set it; a refused
    /// update, or another ref's, leaves it; a delete drops it.
    #[test]
    fn refs_keep_their_update_time() {
        let mut m = Twin::new();
        let (a, b) = (Oid::sha1([1; 20]), Oid::sha1([2; 20]));
        let set = |name: &str, old: Option<Oid>, new: Option<Oid>, at: u64| {
            Command::commit(
                "r",
                Commit {
                    updates: vec![RefUpdate {
                        name: name.into(),
                        old,
                        new,
                    }],
                    updated_at_ms: at,
                    ..Default::default()
                },
            )
        };
        let time = |m: &Twin, name: &str| m.ref_updated_ms.get(name).copied();
        apply_followed(&mut m, 1, &set("refs/heads/main", None, Some(a), 1000));
        apply_followed(&mut m, 2, &set("refs/tags/t", None, Some(b), 2000));
        assert_eq!(time(&m, "refs/heads/main"), Some(1000));
        assert_eq!(time(&m, "refs/tags/t"), Some(2000));
        let o = apply_followed(&mut m, 3, &set("refs/heads/main", None, Some(b), 3000));
        assert!(o.ref_results[0].error.is_some());
        assert_eq!(time(&m, "refs/heads/main"), Some(1000), "a refused update");
        apply_followed(&mut m, 4, &set("refs/heads/main", Some(a), Some(b), 4000));
        assert_eq!(time(&m, "refs/heads/main"), Some(4000));
        assert_eq!(time(&m, "refs/tags/t"), Some(2000), "another ref");
        apply_followed(&mut m, 5, &set("refs/heads/main", Some(b), Some(a), 0));
        assert_eq!(time(&m, "refs/heads/main"), None, "an unknown time");
        apply_followed(&mut m, 6, &set("refs/tags/t", Some(b), None, 6000));
        assert_eq!(time(&m, "refs/tags/t"), None, "a delete");
        apply_followed(&mut m, 7, &Command::delete("r"));
        assert!(m.ref_updated_ms.is_empty());
    }

    /// A HEAD set at creation stays while its branch is unborn, and after
    /// the branch is deleted.
    #[test]
    fn a_kept_head_stays() {
        let mut m = Twin::new();
        let mut round = m.store.round();
        round.set_head(LOG, b"refs/heads/dev");
        let c = round.counters(LOG).unwrap();
        round.set_counters(
            LOG,
            Counters {
                keep_head: true,
                ..c
            },
        );
        round.commit().unwrap();
        (m.m.head, m.m.keep_head) = ("refs/heads/dev".into(), true);
        let a = Oid::sha1([1; 20]);
        apply_followed(&mut m, 1, &set_ref("refs/heads/main", None, Some(a)));
        assert_eq!(m.head, "refs/heads/dev");
        apply_followed(&mut m, 2, &set_ref("refs/heads/dev", None, Some(a)));
        apply_followed(&mut m, 3, &set_ref("refs/heads/dev", Some(a), None));
        assert_eq!(m.head, "refs/heads/dev", "after its branch is deleted");
    }

    /// A HEAD not set at creation moves to another branch once its own is
    /// deleted.
    #[test]
    fn an_implicit_head_is_repaired() {
        let mut m = Twin::new();
        let a = Oid::sha1([1; 20]);
        apply_followed(&mut m, 1, &set_ref("refs/heads/main", None, Some(a)));
        apply_followed(&mut m, 2, &set_ref("refs/heads/dev", None, Some(a)));
        assert_eq!(m.head, "refs/heads/main");
        apply_followed(&mut m, 3, &set_ref("refs/heads/main", Some(a), None));
        assert_eq!(m.head, "refs/heads/dev");
    }

    /// A commit of one ref update.
    fn set_ref(name: &str, old: Option<Oid>, new: Option<Oid>) -> Command {
        Command::commit(
            "r",
            Commit {
                updates: vec![RefUpdate {
                    name: name.into(),
                    old,
                    new,
                }],
                ..Default::default()
            },
        )
    }

    /// An inline layer's bytes stay in its log entry, pinned until a flush
    /// replaces it; HEAD follows the branches (the first one, by name, once
    /// main is gone).
    #[test]
    fn inline_layers_are_pinned_until_flushed() {
        let mut m = Twin::new();
        let a = Oid::sha1([1; 20]);
        let inline = |id: &str| LayerMeta {
            id: id.into(),
            objects: 2,
            bytes: 3,
            data: Some(b"abc".to_vec().into()),
            ..Default::default()
        };
        let push = |l: LayerMeta, name: &str| {
            Command::commit(
                "r",
                Commit {
                    add_layers: vec![l],
                    updates: vec![RefUpdate {
                        name: name.into(),
                        old: None,
                        new: Some(a),
                    }],
                    ..Default::default()
                },
            )
        };
        apply_followed(&mut m, 1, &push(inline("i1"), "refs/heads/main"));
        apply_followed(&mut m, 2, &push(inline("i2"), "refs/heads/zz"));
        apply_followed(&mut m, 3, &push(inline("i3"), "refs/heads/b"));
        assert_eq!(m.store.oldest_pin(LOG).unwrap(), Some(1));
        // A flush of the first two.
        let flushed = LayerMeta {
            id: sid("f"),
            objects: 4,
            bytes: 6,
            first_id: 0,
            id_span: 4,
            ..Default::default()
        };
        let flush = Command::commit(
            "r",
            Commit {
                add_layers: vec![flushed],
                remove_layers: m.layers[..2].iter().map(Into::into).collect(),
                ..Default::default()
            },
        );
        assert!(apply_followed(&mut m, 4, &flush).error.is_none());
        assert_eq!(
            m.store.oldest_pin(LOG).unwrap(),
            Some(3),
            "the log may go up to the third push"
        );
        let unmain = Command::commit(
            "r",
            Commit {
                updates: vec![RefUpdate {
                    name: "refs/heads/main".into(),
                    old: Some(a),
                    new: None,
                }],
                ..Default::default()
            },
        );
        apply_followed(&mut m, 5, &unmain);
        assert_eq!(m.head, "refs/heads/b");
    }
}
