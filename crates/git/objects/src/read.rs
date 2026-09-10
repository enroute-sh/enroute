//! What a reader asks of the object index, answered from segments.

use std::collections::HashMap;

use anyhow::Result;
use futures::{StreamExt as _, TryStreamExt as _};
use gix_hash::ObjectId;
use gix_object::Kind;
use roaring::RoaringTreemap;

use enroute_git_core::{
    CommitPackLocation, ObjectHashMap, ObjectMeta, ObjectSeq, ObjectSeqs, PackImageLocation,
};
use enroute_git_metadata::Identity;

use crate::index::{Location, ObjectIndex};
use crate::store::{Held, RepoObjects, Shelf};

/// How many delta chains a plan walks at once.
///
/// Every hop is a database round trip, so this queues on the pool rather
/// than on a store — the same reason the fetch path bounds its own.
const CHAIN_CONCURRENCY: usize = 16;

/// What one shelf's read produced: the index, and its bases resolved to oids.
///
/// A base is numbered in the same space as the entry deltaing against it, so
/// resolving one is a shelf-local question rather than a global one.
#[derive(Default)]
struct Read {
    index: ObjectIndex,
    base_oids: HashMap<u64, ObjectId>,
}

impl RepoObjects<'_> {
    /// What the index knows about each named object.
    ///
    /// Takes what identity already said rather than asking again, and reads
    /// past the commits in it: those are the graph's to answer for.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn lookup(
        &self,
        named: &[(ObjectId, Identity)],
    ) -> Result<Vec<(ObjectId, ObjectMeta)>> {
        let held: Vec<(ObjectId, Held)> = named
            .iter()
            .filter(|(_, identity)| identity.kind != Kind::Commit)
            .map(|(oid, identity)| Ok((*oid, Held::of(*identity)?)))
            .collect::<Result<_>>()?;
        self.assemble(&held).await
    }

    /// The same, for a caller already working in seq space.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn locations_by_seq(
        &self,
        seqs: &[ObjectSeq],
    ) -> Result<Vec<(ObjectId, ObjectMeta)>> {
        let mut named: Vec<(ObjectId, Held)> = Vec::with_capacity(seqs.len());
        for shelf in Shelf::BOTH {
            let wanted = shelved(seqs, shelf);
            if wanted.is_empty() {
                continue;
            }
            for (seq, oid) in self.oids_of(shelf, &wanted).await? {
                named.push((
                    oid,
                    Held {
                        seq,
                        kind: shelf.kind(),
                    },
                ));
            }
        }
        self.assemble(&named).await
    }

    /// Builds one `ObjectMeta` per identity, reading each shelf once.
    async fn assemble(&self, named: &[(ObjectId, Held)]) -> Result<Vec<(ObjectId, ObjectMeta)>> {
        let mut shelves: HashMap<Shelf, Read> = HashMap::new();
        for shelf in Shelf::BOTH {
            let wanted = held_on(named, shelf);
            if wanted.is_empty() {
                continue;
            }
            let index = self.read(shelf, &wanted).await?;
            // A delta base is reported as an oid, so whatever the introducing
            // entries name has to be resolved before any of them are built.
            let bases: Vec<u64> = wanted
                .iter()
                .filter_map(|seq| index.get(*seq)?.introducing()?.base_seq)
                .collect();
            let base_oids = self.oids_of(shelf, &bases).await?;
            shelves.insert(shelf, Read { index, base_oids });
        }

        let mut found = Vec::with_capacity(named.len());
        for (oid, identity) in named {
            let read = identity.shelf().and_then(|shelf| shelves.get(&shelf));
            let location = read
                .and_then(|read| Some((read, read.index.get(identity.seq)?.introducing()?)))
                .map(|(read, location)| resolved(location, &read.base_oids));
            found.push((
                *oid,
                ObjectMeta {
                    kind: identity.kind,
                    location,
                    object_seq: identity.numbered(),
                },
            ));
        }
        Ok(found)
    }

    /// The direct entries of every tree in `seqs`, empty ones omitted.
    ///
    /// Reads the tree shelf alone, which is the point of there being two.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn children_of(&self, seqs: &[u64]) -> Result<HashMap<u64, ObjectSeqs>> {
        let index = self.read(Shelf::Trees, seqs).await?;
        let mut found = HashMap::new();
        for seq in seqs {
            if let Some(object) = index.get(*seq)
                && !object.children.is_empty()
            {
                found.insert(*seq, object.children.clone());
            }
        }
        Ok(found)
    }

    /// Every distinct tree and blob reachable from `roots`, by a leveled BFS
    /// over [`Self::children_of`].
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn tree_closure(&self, roots: &[u64]) -> Result<ObjectSeqs> {
        let mut seen = ObjectSeqs {
            trees: roots.iter().copied().collect(),
            blobs: RoaringTreemap::new(),
        };
        let mut frontier: Vec<u64> = roots.to_vec();
        while !frontier.is_empty() {
            let batch = self.children_of(&frontier).await?;
            frontier.clear();
            for children in batch.into_values() {
                // Only trees go back on the frontier. A blob has no entries,
                // so asking for its children is a round trip whose answer is
                // known.
                let fresh = children.trees.iter().filter(|seq| seen.trees.insert(*seq));
                frontier.extend(fresh.collect::<Vec<u64>>());
                seen.blobs |= children.blobs;
            }
        }
        Ok(seen)
    }

    /// Every pack storing `oid`, introducing pack first.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn packs_of(&self, oid: ObjectId) -> Result<Vec<ObjectId>> {
        let Some(identity) = self.identify(&[oid]).await?.get(&oid).copied() else {
            return Ok(Vec::new());
        };
        let Some(held) = identity.shelf() else {
            return Ok(Vec::new());
        };
        Ok(self
            .read(held, &[identity.seq])
            .await?
            .get(identity.seq)
            .map(|object| object.locations.iter().map(|l| l.pack_oid).collect())
            .unwrap_or_default())
    }

    /// The introducing pack seq of every object in `oids`.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn first_packs(&self, oids: &[ObjectId]) -> Result<ObjectHashMap<i64>> {
        let identities = self.identify(oids).await?;
        let named: Vec<(ObjectId, Held)> = identities.into_iter().collect();

        let mut shelves: HashMap<Shelf, ObjectIndex> = HashMap::new();
        for shelf in Shelf::BOTH {
            let wanted = held_on(&named, shelf);
            if !wanted.is_empty() {
                shelves.insert(shelf, self.read(shelf, &wanted).await?);
            }
        }

        let mut found = ObjectHashMap::default();
        for (oid, identity) in named {
            if let Some(location) = identity
                .shelf()
                .and_then(|shelf| shelves.get(&shelf))
                .and_then(|index| index.get(identity.seq)?.introducing())
            {
                found.insert(oid, location.pack_seq);
            }
        }
        Ok(found)
    }

    /// Every entry rebuilding `oid` costs, innermost last.
    ///
    /// A hop at a time rather than one recursive query, reloading only when
    /// a base falls outside what is already composed.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn chain(&self, oid: ObjectId, max_hops: u32) -> Result<Vec<CommitPackLocation>> {
        let Some(identity) = self.identify(&[oid]).await?.get(&oid).copied() else {
            return Ok(Vec::new());
        };
        // Every hop is on the shelf the object itself is on: git never
        // deltas across kinds, and each kind is numbered on its own, so a
        // base seq means nothing anywhere else.
        let Some(held) = identity.shelf() else {
            return Ok(Vec::new());
        };

        let mut hops: Vec<Location> = Vec::new();
        let mut index = self.read(held, &[identity.seq]).await?;
        let mut at = Some(identity.seq);
        let max_hops = usize::try_from(max_hops).unwrap_or(usize::MAX);
        while let Some(seq) = at.filter(|_| hops.len() <= max_hops) {
            if index.get(seq).is_none() {
                index = self.read(held, &[seq]).await?;
            }
            let Some(location) = index.get(seq).and_then(|object| object.introducing()) else {
                break;
            };
            hops.push(*location);
            at = location.base_seq;
        }

        Ok(hops
            .into_iter()
            .map(|location| CommitPackLocation {
                image: PackImageLocation {
                    pack_sha: location.pack_oid,
                    offset: location.offset,
                    entry_len: location.entry_len,
                    // The reader takes each entry's base from its own header
                    // bytes; the seq only exists so the walk can recurse.
                    base: None,
                },
                segment: location.segment,
            })
            .collect())
    }

    /// How many deltas rebuilding each of `oids` costs today.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn chain_depths(
        &self,
        oids: &[ObjectId],
        max_hops: u32,
    ) -> Result<ObjectHashMap<u32>> {
        // Each chain is its own walk of up to `max_hops` reads, and one at a
        // time made a push wait out the sum of them. Bounded, not unbounded:
        // every hop is a database round trip. Collected before it is streamed
        // because borrowing `self` into `buffer_unordered` breaks callers.
        let walks: Vec<_> = oids
            .iter()
            .map(|oid| async move {
                // An empty chain is an object with no delta recorded, and one
                // hop is a depth of zero.
                let hops = self.chain(*oid, max_hops).await?;
                anyhow::Ok(
                    hops.len()
                        .checked_sub(1)
                        .map(|deltas| (*oid, u32::try_from(deltas).unwrap_or(u32::MAX))),
                )
            })
            .collect();
        let walked: Vec<Option<(ObjectId, u32)>> = futures::stream::iter(walks)
            .buffer_unordered(CHAIN_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(walked.into_iter().flatten().collect())
    }
}

/// One location, with its base named as the oid a reader expects.
fn resolved(location: &Location, base_oids: &HashMap<u64, ObjectId>) -> CommitPackLocation {
    CommitPackLocation {
        image: PackImageLocation {
            pack_sha: location.pack_oid,
            offset: location.offset,
            entry_len: location.entry_len,
            base: location
                .base_seq
                .and_then(|base| base_oids.get(&base).copied()),
        },
        segment: location.segment,
    }
}

/// The seqs of `seqs` that are numbered in `shelf`'s space.
fn shelved(seqs: &[ObjectSeq], shelf: Shelf) -> Vec<u64> {
    seqs.iter()
        .filter(|seq| Shelf::for_kind(seq.kind()) == Some(shelf))
        .map(|seq| seq.seq())
        .collect()
}

/// The seqs of the identities held on `shelf`.
fn held_on(named: &[(ObjectId, Held)], shelf: Shelf) -> Vec<u64> {
    named
        .iter()
        .filter(|(_, identity)| identity.shelf() == Some(shelf))
        .map(|(_, identity)| identity.seq)
        .collect()
}
