//! A repository's state as a compute holds it: [`RepoMeta`] with its refs in
//! persistent maps, so a copy shares them.
//!
//! A follower changes the state while readers still hold the last one it
//! published, so each batch of changes copies it. With a plain map that copy
//! costs every ref, which on a busy repository with many refs took most of
//! the follower's time; a persistent map copies in O(1) and each change
//! costs O(log refs).

use rpds::RedBlackTreeMapSync;

use enroute_shared::{
    oid::{self, Oid},
    proto::{Change, LayerEdit, LayerMeta, RepoMeta},
    state::{ChangeTarget, Scalars, apply_change, apply_layer_edit},
};

/// Refs (or their times) by name, in name order, as git lists them.
pub(crate) type Map<V> = RedBlackTreeMapSync<String, V>;

/// A repository's state ([`RepoMeta`], whose fields these mirror).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RepoState {
    /// Every ref's value.
    pub refs: Map<Oid>,
    /// HEAD's target.
    pub head: String,
    /// The layers, in id order.
    pub layers: Vec<LayerMeta>,
    /// Increases with every change.
    pub version: u64,
    /// The first object id not yet assigned to a layer.
    pub next_id: u64,
    /// The current upload generation.
    pub upload_gen: u64,
    /// The incarnation.
    pub epoch: u64,
    /// The state is a tombstone.
    pub deleted: bool,
    /// HEAD stays on the branch it was created on.
    pub keep_head: bool,
    /// The time of the last git push that moved a ref (Unix milliseconds).
    pub last_push_ms: u64,
    /// When each ref was last set (Unix milliseconds), where known.
    pub ref_updated_ms: Map<u64>,
    /// The ledger's evaluation chain of the last change applied.
    pub chain: u64,
    /// The incarnation's object format.
    pub object_format: i32,
}

impl From<RepoMeta> for RepoState {
    fn from(m: RepoMeta) -> RepoState {
        RepoState {
            refs: m.refs.into_iter().collect(),
            head: m.head,
            layers: m.layers,
            version: m.version,
            next_id: m.next_id,
            upload_gen: m.upload_gen,
            epoch: m.epoch,
            deleted: m.deleted,
            keep_head: m.keep_head,
            last_push_ms: m.last_push_ms,
            ref_updated_ms: m.ref_updated_ms.into_iter().collect(),
            chain: m.chain,
            object_format: m.object_format,
        }
    }
}

impl RepoState {
    /// A freshly created repository (as [`RepoMeta::new`]).
    #[must_use]
    pub fn new() -> RepoState {
        RepoMeta::new().into()
    }

    /// How the repository names its objects (as [`RepoMeta::format`]).
    #[must_use]
    pub fn format(&self) -> oid::ObjectFormat {
        RepoMeta {
            object_format: self.object_format,
            ..RepoMeta::default()
        }
        .format()
    }

    /// Apply a change, as [`RepoMeta::apply_change`] does.
    pub fn apply_change(&mut self, c: &Change) {
        let Ok(()) = apply_change(self, c);
    }
}

impl ChangeTarget for RepoState {
    type Error = std::convert::Infallible;

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.refs = Map::new_sync();
        self.ref_updated_ms = Map::new_sync();
        self.layers.clear();
        self.deleted = true;
        Ok(())
    }

    fn set_ref(
        &mut self,
        name: &str,
        value: Option<Oid>,
        updated_ms: u64,
    ) -> Result<(), Self::Error> {
        match value {
            Some(v) => self.refs.insert_mut(name.to_string(), v),
            None => _ = self.refs.remove_mut(name),
        }
        if value.is_some() && updated_ms > 0 {
            self.ref_updated_ms.insert_mut(name.to_string(), updated_ms);
        } else {
            self.ref_updated_ms.remove_mut(name);
        }
        Ok(())
    }

    fn set_head(&mut self, head: &str) -> Result<(), Self::Error> {
        head.clone_into(&mut self.head);
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

#[cfg(test)]
mod tests {
    use enroute_shared::proto::{LayerEdit, RefChange};

    use super::*;

    /// Changes leave the same state on both, and an earlier copy keeps its
    /// refs.
    #[test]
    fn changes_match_repo_meta() {
        let mut meta = RepoMeta::new();
        meta.set_ref_at("refs/heads/main", Some(Oid::sha1([1; 20])), 5);
        let mut state = RepoState::from(meta.clone());
        let before = state.clone();
        let changes = [
            Change {
                refs: vec![
                    RefChange {
                        name: "refs/heads/a".into(),
                        value: Some(Oid::sha1([2; 20])),
                        updated_at_ms: 7,
                    },
                    RefChange {
                        name: "refs/heads/main".into(),
                        value: None,
                        updated_at_ms: 0,
                    },
                ],
                head: Some("refs/heads/a".into()),
                layers: Some(LayerEdit {
                    insert: vec![LayerMeta {
                        id: "l1".into(),
                        ..LayerMeta::default()
                    }],
                    remove: vec![],
                }),
                version: 3,
                chain: 9,
                ..Change::default()
            },
            Change {
                deleted: true,
                version: 4,
                ..Change::default()
            },
        ];
        for c in &changes {
            meta.apply_change(c);
            state.apply_change(c);
            assert_eq!(state, RepoState::from(meta.clone()));
        }
        assert_eq!(before.refs.size(), 1);
    }
}
