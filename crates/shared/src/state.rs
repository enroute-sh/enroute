//! A repository's replicated state (refs and layer manifest), the commands
//! that change it and the changes they make: the protocol's types
//! (generated from `proto/state.proto`), with what both roles do with them.
//! Applying commands (the state machine) is the ledger's; a follower applies
//! the changes it is sent ([`RepoMeta::apply_change`]).

use crate::oid;

pub use crate::proto::{
    AdvanceUploadGen, Change, Command, Commit, Delete, LayerEdit, LayerMeta, Outcome, RefChange,
    RefResult, RefUpdate, Refusal, RemovedLayer, RepoMeta, command,
};

impl From<crate::proto::ObjectFormat> for oid::ObjectFormat {
    fn from(f: crate::proto::ObjectFormat) -> oid::ObjectFormat {
        match f {
            crate::proto::ObjectFormat::Sha1 => oid::ObjectFormat::Sha1,
            crate::proto::ObjectFormat::Sha256 => oid::ObjectFormat::Sha256,
        }
    }
}

impl From<oid::ObjectFormat> for crate::proto::ObjectFormat {
    fn from(f: oid::ObjectFormat) -> crate::proto::ObjectFormat {
        match f {
            oid::ObjectFormat::Sha1 => crate::proto::ObjectFormat::Sha1,
            oid::ObjectFormat::Sha256 => crate::proto::ObjectFormat::Sha256,
        }
    }
}

/// How many upload generations before the current one a stored layer may
/// still be added from (see `RepoMeta.upload_gen`): a publish has about
/// this many layer sweeps' time between its upload and its commit.
pub const UPLOAD_GEN_WINDOW: u64 = 2;

/// A layer id: `random` and the upload `generation`, in hex. The
/// generation is last, so ids (and object-store keys) spread evenly.
#[must_use]
pub fn layer_id(random: u128, generation: u64) -> String {
    format!("{random:032x}-{generation:016x}")
}

/// The upload generation in a layer id (None if it has none).
#[must_use]
pub fn layer_gen(id: &str) -> Option<u64> {
    let (_, generation) = id.rsplit_once('-')?;
    if generation.len() != 16 || !generation.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(generation, 16).ok()
}

impl LayerMeta {
    /// In the object store (not inline).
    pub fn is_stored(&self) -> bool {
        self.data.is_none()
    }

    /// The end of the layer's id range.
    pub fn end_id(&self) -> u64 {
        u64::from(self.first_id) + u64::from(self.id_span)
    }
}

impl From<&LayerMeta> for RemovedLayer {
    fn from(l: &LayerMeta) -> Self {
        RemovedLayer {
            id: l.id.clone(),
            first_id: l.first_id,
        }
    }
}

impl Command {
    /// A command that commits `commit` to `repo`'s log.
    #[must_use]
    pub fn commit(repo: &str, commit: Commit) -> Command {
        Command {
            repo: repo.to_string(),
            kind: Some(command::Kind::Commit(commit)),
            ..Default::default()
        }
    }

    /// The size of the largest layer whose bytes the command carries.
    #[must_use]
    pub fn largest_inline_layer(&self) -> Option<u64> {
        match &self.kind {
            Some(command::Kind::Commit(c)) => c
                .add_layers
                .iter()
                .filter_map(|l| l.data.as_ref())
                .map(|d| u64::try_from(d.len()).unwrap_or(u64::MAX))
                .max(),
            _ => None,
        }
    }

    /// A command that advances `repo`'s upload generation.
    #[must_use]
    pub fn advance_upload_gen(repo: &str) -> Command {
        Command {
            repo: repo.to_string(),
            kind: Some(command::Kind::AdvanceUploadGen(AdvanceUploadGen {})),
            ..Default::default()
        }
    }

    /// A command that deletes `repo`.
    #[must_use]
    pub fn delete(repo: &str) -> Command {
        Command {
            repo: repo.to_string(),
            kind: Some(command::Kind::Delete(Delete {})),
            ..Default::default()
        }
    }

    /// For incarnation `epoch` only (see `Command.epoch`).
    #[must_use]
    pub fn at(self, epoch: u64) -> Command {
        Command { epoch, ..self }
    }
}

/// A ref update's error when a create finds the ref present.
pub const REF_EXISTS: &str = "reference already exists";

/// A ref update's error when the ref does not hold the expected value.
pub const REF_STALE: &str = "stale info (ref changed)";

/// A ref update's error when another update of the same atomic commit failed.
pub const ATOMIC_FAILURE: &str = "atomic push failure";

impl Outcome {
    /// Per ref update: None if applied, else why not.
    pub fn ref_errors(&self) -> impl Iterator<Item = Option<String>> + '_ {
        self.ref_results.iter().map(|r| r.error.clone())
    }

    /// Refuse the whole command for `why`.
    pub fn refuse(&mut self, why: Refusal) {
        self.error = Some(why.text().to_string());
        self.set_refusal(why);
    }

    /// Refuse the whole command as invalid, showing `text`.
    pub fn refuse_invalid(&mut self, text: impl Into<String>) {
        self.error = Some(text.into());
        self.set_refusal(Refusal::Invalid);
    }

    /// Why the whole command was refused (None: it was not).
    #[must_use]
    pub fn refused(&self) -> Option<Refusal> {
        self.error.as_ref()?;
        Some(match self.refusal() {
            Refusal::Unspecified => Refusal::Invalid,
            why => why,
        })
    }
}

impl Refusal {
    /// What a refusal shows when it has no text of its own.
    #[must_use]
    pub fn text(self) -> &'static str {
        match self {
            Refusal::Unspecified | Refusal::Invalid => "command refused",
            Refusal::Deleted => "repository deleted",
            Refusal::OtherIncarnation => "repository deleted (and maybe created again) since",
            Refusal::StaleUpload => "layer uploaded in an expired generation; publish it again",
        }
    }
}

/// Apply a layer edit to `layers`: drop the removed layers, and insert the
/// new ones where the first removed one was (at the end if none was).
pub fn apply_layer_edit(layers: &mut Vec<LayerMeta>, e: &LayerEdit) {
    let remove: std::collections::HashSet<&str> = e.remove.iter().map(|r| r.id.as_str()).collect();
    let at = layers
        .iter()
        .position(|l| remove.contains(l.id.as_str()))
        .unwrap_or(layers.len());
    layers.retain(|l| !remove.contains(l.id.as_str()));
    layers.splice(at..at, e.insert.iter().cloned());
}

/// The HEAD of a new repository.
pub const DEFAULT_HEAD: &str = "refs/heads/main";

impl RepoMeta {
    /// A freshly created repository.
    #[must_use]
    pub fn new() -> RepoMeta {
        RepoMeta {
            head: DEFAULT_HEAD.to_string(),
            version: 1,
            ..Default::default()
        }
    }

    /// How the repository names its objects. A format this binary does not
    /// know reads as SHA-1; the ledger never creates one.
    #[must_use]
    pub fn format(&self) -> oid::ObjectFormat {
        self.object_format().into()
    }

    /// Set (or, with None, delete) a ref, set at `updated_ms` (0: unknown).
    pub fn set_ref_at(&mut self, name: &str, value: Option<oid::Oid>, updated_ms: u64) {
        match value {
            Some(v) => self.refs.insert(name.to_string(), v),
            None => self.refs.remove(name),
        };
        if value.is_some() && updated_ms > 0 {
            self.ref_updated_ms.insert(name.to_string(), updated_ms);
        } else {
            self.ref_updated_ms.remove(name);
        }
    }

    /// Apply a change (as made by the ledger applying a command; see
    /// `Watch` in `ledger.proto`).
    pub fn apply_change(&mut self, c: &Change) {
        let Ok(()) = apply_change(self, c);
    }
}

impl ChangeTarget for RepoMeta {
    type Error = std::convert::Infallible;

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.refs.clear();
        self.ref_updated_ms.clear();
        self.layers.clear();
        self.deleted = true;
        Ok(())
    }

    fn set_ref(
        &mut self,
        name: &str,
        value: Option<oid::Oid>,
        updated_ms: u64,
    ) -> Result<(), Self::Error> {
        self.set_ref_at(name, value, updated_ms);
        Ok(())
    }

    fn set_head(&mut self, head: &str) -> Result<(), Self::Error> {
        self.head = head.to_string();
        Ok(())
    }

    fn edit_layers(&mut self, edit: &LayerEdit) -> Result<(), Self::Error> {
        apply_layer_edit(&mut self.layers, edit);
        Ok(())
    }

    fn set_scalars(&mut self, s: &Scalars) -> Result<(), Self::Error> {
        s.apply([
            &mut self.next_id,
            &mut self.upload_gen,
            &mut self.last_push_ms,
            &mut self.version,
            &mut self.chain,
        ]);
        Ok(())
    }
}

/// A repository's state that a [`Change`] is applied to, wherever it is
/// kept: [`apply_change`] is the one place that says what a change does.
pub trait ChangeTarget {
    /// What a write to the state can fail with.
    type Error;

    /// Drops every ref and layer, and marks the state deleted.
    ///
    /// # Errors
    ///
    /// The state cannot be written.
    fn clear(&mut self) -> Result<(), Self::Error>;
    /// Sets a ref (deletes it if `value` is None), set at `updated_ms`
    /// (0: unknown).
    ///
    /// # Errors
    ///
    /// The state cannot be written.
    fn set_ref(
        &mut self,
        name: &str,
        value: Option<oid::Oid>,
        updated_ms: u64,
    ) -> Result<(), Self::Error>;
    /// Points HEAD at `head`.
    ///
    /// # Errors
    ///
    /// The state cannot be written.
    fn set_head(&mut self, head: &str) -> Result<(), Self::Error>;
    /// Replaces the layers `edit` removes with those it inserts.
    ///
    /// # Errors
    ///
    /// The state cannot be written.
    fn edit_layers(&mut self, edit: &LayerEdit) -> Result<(), Self::Error>;
    /// Sets the scalars of the state.
    ///
    /// # Errors
    ///
    /// The state cannot be written.
    fn set_scalars(&mut self, s: &Scalars) -> Result<(), Self::Error>;
}

/// A change's scalars: those that it moves, and its version and chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Scalars {
    /// `RepoMeta.next_id`, if it moved.
    pub next_id: Option<u64>,
    /// `RepoMeta.upload_gen`, if it moved.
    pub upload_gen: Option<u64>,
    /// `RepoMeta.last_push_ms`, if it moved.
    pub last_push_ms: Option<u64>,
    /// The version after the change.
    pub version: u64,
    /// The evaluation chain that made it.
    pub chain: u64,
}

impl Scalars {
    /// Write these over the fields `next_id`, `upload_gen` and
    /// `last_push_ms`, and `version` and `chain` (in that order).
    pub fn apply(&self, [next_id, upload_gen, last_push_ms, version, chain]: [&mut u64; 5]) {
        *next_id = self.next_id.unwrap_or(*next_id);
        *upload_gen = self.upload_gen.unwrap_or(*upload_gen);
        *last_push_ms = self.last_push_ms.unwrap_or(*last_push_ms);
        (*version, *chain) = (self.version, self.chain);
    }
}

/// Apply change `c` to `state`: a deletion first, then the refs in order,
/// HEAD, the layers, and the scalars.
///
/// # Errors
///
/// Whatever a write to `state` fails with.
pub fn apply_change<T: ChangeTarget + ?Sized>(state: &mut T, c: &Change) -> Result<(), T::Error> {
    if c.deleted {
        state.clear()?;
    }
    for r in &c.refs {
        state.set_ref(&r.name, r.value, r.updated_at_ms)?;
    }
    if let Some(head) = &c.head {
        state.set_head(head)?;
    }
    if let Some(e) = &c.layers {
        state.edit_layers(e)?;
    }
    state.set_scalars(&Scalars {
        next_id: c.next_id,
        upload_gen: c.upload_gen,
        last_push_ms: c.last_push_ms,
        version: c.version,
        chain: c.chain,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oid::Oid;

    #[test]
    fn states_roundtrip_through_protobuf() {
        use prost::Message;
        let mut m = RepoMeta::new();
        let layer = LayerMeta {
            id: "x".into(),
            objects: 2,
            bytes: 3,
            data: Some(bytes::Bytes::from_static(b"inline")),
            first_id: 5,
            id_span: 2,
            root: bytes::Bytes::from_static(&[9; 32]),
            meta_bytes: 4,
        };
        m.layers.push(layer);
        m.refs.insert("refs/heads/main".into(), Oid::sha1([7; 20]));
        let back = RepoMeta::decode(m.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back, m);
        assert_eq!(back.refs["refs/heads/main"], Oid::sha1([7; 20]));
    }

    #[test]
    fn a_refusal_without_a_reason_is_invalid() {
        let mut out = Outcome::default();
        assert_eq!(out.refused(), None);
        out.error = Some("something".into());
        assert_eq!(out.refused(), Some(Refusal::Invalid));
        out.refuse(Refusal::StaleUpload);
        assert_eq!(out.refused(), Some(Refusal::StaleUpload));
    }

    #[test]
    fn layer_ids_carry_their_generation() {
        let id = layer_id(u128::MAX - 5, 0x2a);
        assert_eq!(id.len(), 32 + 1 + 16);
        assert_eq!(layer_gen(&id), Some(0x2a));
        assert_eq!(layer_gen(&layer_id(0, u64::MAX)), Some(u64::MAX));
        for bad in [
            "",
            "abc",
            "x-2a",
            "a-+00000000000002a",
            "0123456789abcdef0123456789abcdef",
            "a-000000000000002g",
        ] {
            assert_eq!(layer_gen(bad), None, "{bad}");
        }
    }
}
