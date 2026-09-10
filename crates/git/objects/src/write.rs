//! The push path: hand out seqs, and record what each object is and where.

use std::collections::HashMap;

use anyhow::{Context, Result};
use gix_hash::ObjectId;
use gix_object::Kind;

use enroute_git_core::{ObjectSeqs, SegmentLocation, Ulid};
use enroute_git_journal::{Index, Journal};
use enroute_lattice_store::{Reclaimed, Report, Sweep, Swept};

use crate::index::{Builder, Location};
use crate::store::{RepoObjects, Shelf};

/// One object a push is recording.
#[derive(Debug, Clone)]
pub struct Recorded {
    /// The seq it was allocated.
    pub seq: u64,
    /// What it is called.
    pub oid: ObjectId,
    /// What kind of object it is.
    pub kind: Kind,
    /// Every pack this push knows it to be in.
    pub locations: Vec<Location>,
    /// A tree's direct entries; empty for anything else.
    pub children: ObjectSeqs,
}

/// Which list a shelf's segments are catalogued in.
const fn index(shelf: Shelf) -> Index {
    match shelf {
        Shelf::Trees => Index::Trees,
        Shelf::Blobs => Index::Blobs,
    }
}

/// The same entry, in the segment its image was copied into.
fn at(location: Location, (into, shift): (Ulid, u64)) -> Location {
    Location {
        segment: SegmentLocation {
            id: into,
            base_offset: location.segment.base_offset.saturating_add(shift),
            image_len: location.segment.image_len,
        },
        ..location
    }
}

impl RepoObjects<'_> {
    /// Adds `objects` to `journal`, so they land with whatever else the push
    /// writes.
    ///
    /// A tree and a blob go to different shelves, and a tag to neither: it
    /// is never packed, so identity is all there is to say about one.
    ///
    /// # Errors
    /// Whatever the bucket said.
    pub async fn record(&self, journal: &mut Journal, objects: &[Recorded]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }

        for held in Shelf::BOTH {
            let mut builder = Builder::new();
            let mut any = false;
            for object in objects
                .iter()
                .filter(|o| Shelf::for_kind(o.kind) == Some(held))
            {
                any = true;
                shelve(&mut builder, object);
            }
            if !any {
                continue;
            }

            let scope = self.scope(held);
            // Encoded, and put in the bucket if large enough, before the row
            // that names it: a rollback then leaves an orphan rather than a
            // row pointing at nothing.
            if let Some(segment) = self.shelf(held).prepare(&scope, &builder.build()).await? {
                journal.list(index(held), &scope, segment);
            }
        }

        Ok(())
    }

    /// Adds packs to objects the repository already holds.
    ///
    /// A re-inclusion writes no identity and no children, so this is the
    /// grow-only half of a record on its own.
    ///
    /// # Errors
    /// Whatever the bucket said.
    pub async fn relocate(
        &self,
        journal: &mut Journal,
        added: &[(u64, Kind, Location)],
    ) -> Result<()> {
        for shelf in Shelf::BOTH {
            let mut builder = Builder::new();
            let mut any = false;
            for (seq, _, location) in added
                .iter()
                .filter(|(_, k, _)| Shelf::for_kind(*k) == Some(shelf))
            {
                any = true;
                builder.locate(*seq, *location);
            }
            if !any {
                continue;
            }

            let scope = self.scope(shelf);
            if let Some(segment) = self.shelf(shelf).prepare(&scope, &builder.build()).await? {
                journal.list(index(shelf), &scope, segment);
            }
        }
        Ok(())
    }

    /// Every location of `objects` that names a segment `moved` lists, as it
    /// would read after the move.
    ///
    /// Separate from [`RepoObjects::relocate`] because it takes a connection,
    /// and asking for one while holding a transaction deadlocks a pool.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn moved_locations(
        &self,
        objects: &ObjectSeqs,
        moved: &HashMap<Ulid, (Ulid, u64)>,
    ) -> Result<Vec<(u64, Kind, Location)>> {
        let mut found = Vec::new();
        for shelf in Shelf::BOTH {
            let wanted: Vec<u64> = match shelf {
                Shelf::Trees => objects.trees.iter().collect(),
                Shelf::Blobs => objects.blobs.iter().collect(),
            };
            if wanted.is_empty() {
                continue;
            }

            let index = self.read(shelf, &wanted).await?;
            found.extend(
                wanted
                    .iter()
                    .filter_map(|seq| Some((*seq, index.get(*seq)?)))
                    .flat_map(|(seq, object)| object.locations.iter().map(move |one| (seq, one)))
                    .filter_map(|(seq, location)| {
                        let moved = moved.get(&location.segment.id)?;
                        Some((seq, shelf.kind(), at(*location, *moved)))
                    }),
            );
        }
        Ok(found)
    }

    /// Merge this repository's object segments down a tier where the policy
    /// says to.
    ///
    /// Safe to repeat and safe to interrupt: a pass that dies part way leaves
    /// what it already merged merged, and the next one carries on.
    ///
    /// # Errors
    /// Whatever the catalog, the bucket or a segment's bytes said.
    pub async fn compact(&self) -> Result<Report> {
        let mut total = Report::default();
        for held in Shelf::BOTH {
            let one = self
                .shelf(held)
                .compact(&self.scope(held))
                .await
                .with_context(|| format!("compacting the {held:?} index"))?;
            total = total.plus(one);
        }
        Ok(total)
    }

    /// Delete every bucket object this repository's segments hold.
    ///
    /// Before the rows go, since the rows are what name the keys — and a
    /// repository being erased has no reader left to see the gap.
    ///
    /// # Errors
    /// Whatever the catalog said.
    pub async fn purge_bucket(&self) -> Result<Reclaimed> {
        let mut total = Reclaimed::default();
        for held in Shelf::BOTH {
            let one = self
                .shelf(held)
                .purge_bucket(&self.scope(held))
                .await
                .with_context(|| format!("purging the {held:?} index's bucket objects"))?;
            total = total.plus(one);
        }
        Ok(total)
    }

    /// Delete this repository's index objects that no segment row names.
    ///
    /// # Errors
    /// Whatever the bucket or the catalog said.
    pub async fn sweep_bucket(&self, sweep: Sweep) -> Result<Swept> {
        let mut total = Swept::default();
        for held in Shelf::BOTH {
            let one = self
                .shelf(held)
                .sweep_bucket(&self.scope(held), sweep)
                .await
                .with_context(|| format!("sweeping the {held:?} index's bucket objects"))?;
            total = total.plus(one);
        }
        Ok(total)
    }
}

/// Puts one object's locations and entries into the builder for its shelf.
fn shelve(builder: &mut Builder, object: &Recorded) {
    for location in &object.locations {
        builder.locate(object.seq, *location);
    }
    if !object.children.is_empty() {
        builder.children(object.seq, &object.children);
    }
}
