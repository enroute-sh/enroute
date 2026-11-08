//! Resolving a pack's deltas from each base outward, as git's index-pack
//! does, with a bound on the base contents held.

use std::ops::Range;
use std::sync::Arc;

use anyhow::{Context, Result};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::git::delta;
use crate::git::oid::{Kind, Oid, try_hash_object};
use crate::storage::{TempTable, Workspace};

use super::{Base, BaseLookup, ContentCache, PackIndex, Visitor, ix};

/// Delta roots per block of parallel work. A root can carry thousands of
/// deltas, so blocks are small for work stealing.
const ROOTS: usize = 16;
/// Roots per window; windows bound the memory held by results in flight.
const WINDOW: usize = 4096 * ROOTS;

/// An `OFS_DELTA` entry and the entry of its base.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct OfsChild {
    base: u32,
    entry: u32,
}

/// A `REF_DELTA` entry and the oid of its base.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub(super) struct RefChild {
    pub(super) base: Oid,
    pub(super) entry: u32,
}

/// The deltas of a pack, sorted by base.
pub(super) struct Children {
    by_ofs: TempTable<OfsChild>,
    pub(super) by_ref: TempTable<RefChild>,
}

impl Children {
    pub(super) fn build(idx: &PackIndex, deltas: &[u32], ws: &Workspace) -> Children {
        let mut by_ofs = TempTable::new(ws);
        let mut by_ref = TempTable::new(ws);
        for &entry in deltas {
            match idx.base(idx.entries.get(ix(entry))) {
                Base::Ofs(b) => by_ofs.push(OfsChild {
                    base: u32::try_from(b).unwrap_or(u32::MAX),
                    entry,
                }),
                Base::Ref(base) => by_ref.push(RefChild { base, entry }),
                Base::None => {}
            }
        }
        by_ofs.sort_by_key(|c| c.base);
        by_ref.sort_by_key(|c| c.base);
        Children { by_ofs, by_ref }
    }

    /// Positions in `by_ofs` of the `OFS_DELTA`s on entry `entry`.
    fn ofs(&self, entry: Option<usize>) -> Range<usize> {
        let Some(e) = entry.and_then(|e| u32::try_from(e).ok()) else {
            return 0..0;
        };
        let start = self.by_ofs.partition_point(|c| c.base < e);
        start..self.by_ofs.partition_point(|c| c.base <= e)
    }

    /// Positions in `by_ref` of the `REF_DELTA`s on the object `oid`.
    fn refs(&self, oid: &Oid) -> Range<usize> {
        let start = self.by_ref.partition_point(|c| c.base < *oid);
        start..self.by_ref.partition_point(|c| c.base <= *oid)
    }

    pub(super) fn has(&self, entry: Option<usize>, oid: &Oid) -> bool {
        !self.ofs(entry).is_empty() || !self.refs(oid).is_empty()
    }
}

/// A walk from delta roots to every delta on them.
pub(super) struct Walk<'a, 'v> {
    pub(super) kids: Children,
    pub(super) cache: &'a ContentCache,
    pub(super) visit: &'a Visitor<'v>,
    pub(super) external: &'a BaseLookup<'v>,
}

impl Walk<'_, '_> {
    /// Resolve each of `roots` and the deltas on it, in parallel, a window
    /// at a time. `root_of` gives a root's base, or `None` if it has none.
    pub(super) fn run<T: Sync>(
        &self,
        idx: &mut PackIndex,
        roots: &[T],
        root_of: impl Fn(&PackIndex, &T) -> Result<Option<Root>> + Sync,
    ) -> Result<()> {
        for window in roots.chunks(WINDOW) {
            let shared = &*idx;
            let results = crate::util::par_blocks(window.len().div_ceil(ROOTS), |b| {
                let block = window.chunks(ROOTS).nth(b).unwrap_or_default();
                self.block(shared, block, &root_of)
            });
            idx.store(results)?;
        }
        Ok(())
    }

    fn block<T>(
        &self,
        idx: &PackIndex,
        roots: &[T],
        root_of: &impl Fn(&PackIndex, &T) -> Result<Option<Root>>,
    ) -> Result<Resolved> {
        let mut out = Vec::new();
        for r in roots {
            if let Some(root) = root_of(idx, r)? {
                self.from(idx, root, &mut out)?;
            }
        }
        Ok(out)
    }

    /// Resolve every delta that has `root` at the end of its chain, depth
    /// first, so that only one path of contents is held at a time. Bases
    /// go to the cache for the later lazy reads.
    fn from(
        &self,
        idx: &PackIndex,
        (entry, oid, kind, content): Root,
        out: &mut Resolved,
    ) -> Result<()> {
        if let Some(i) = entry {
            out.push((i, oid, kind));
        }
        let mut stack = Stack::default();
        stack.push(&self.kids, entry, &oid, &content, true);
        drop(content);
        while let Some(p) = stack.pop() {
            let base = match p.base {
                Held::Content(b) => b,
                Held::Entry(e) => idx.content(e, self.external, self.cache)?,
                Held::External(o) => (self.external)(&o)?.context("thin-pack base missing")?.1,
            };
            let content = Arc::new(delta::apply(&base, &idx.inflate(p.entry)?)?);
            drop(base);
            let oid = try_hash_object(idx.format(), kind, &content)?;
            (self.visit)(&oid, kind, &content)?;
            out.push((p.entry, oid, kind));
            if stack.push(&self.kids, Some(p.entry), &oid, &content, p.rereadable) {
                self.cache.insert(p.entry, content);
            }
        }
        Ok(())
    }
}

/// Bytes of base content a walk holds for the deltas it has yet to apply.
/// Past this it drops bases that it can read again, as git's index-pack
/// does past `core.deltaBaseCacheLimit`. Unit tests hold almost none, so
/// that they read bases again.
const HELD_BYTES: usize = if cfg!(test) { 1 } else { 64 << 20 };

/// The base of a delta that a walk has yet to apply: its content, or where
/// to read it again.
enum Held {
    Content(Arc<Vec<u8>>),
    Entry(usize),
    External(Oid),
}

/// A delta that a walk has yet to apply.
struct Pending {
    entry: usize,
    base: Held,
    /// Whether [`PackIndex::content`] can read this entry again while
    /// deltas are resolved: its chain has no `REF_DELTA` on an object of
    /// the pack, as those are found by oid only after resolution.
    rereadable: bool,
    /// Bytes held for its group, on the group's last delta to pop.
    release: usize,
}

/// The deltas a walk has yet to apply, last pushed first.
#[derive(Default)]
struct Stack {
    items: Vec<Pending>,
    held: usize,
}

impl Stack {
    /// Push the deltas on the object `oid` (entry `entry`, else a thin
    /// pack's base). Its content is held unless that would pass
    /// [`HELD_BYTES`] and it is `rereadable`. Returns whether any were pushed.
    fn push(
        &mut self,
        kids: &Children,
        entry: Option<usize>,
        oid: &Oid,
        content: &Arc<Vec<u8>>,
        rereadable: bool,
    ) -> bool {
        let keep = !rereadable || self.held + content.len() <= HELD_BYTES;
        let base = || match (keep, entry) {
            (true, _) => Held::Content(content.clone()),
            (false, Some(e)) => Held::Entry(e),
            (false, None) => Held::External(*oid),
        };
        let first = self.items.len();
        let ofs = kids
            .ofs(entry)
            .map(|k| (kids.by_ofs.get(k).entry, rereadable));
        // Only a thin pack's base is found again by oid during resolution.
        let refs = kids
            .refs(oid)
            .map(|k| (kids.by_ref.get(k).entry, entry.is_none()));
        self.items
            .extend(ofs.chain(refs).map(|(k, rereadable)| Pending {
                entry: ix(k),
                base: base(),
                rereadable,
                release: 0,
            }));
        let Some(last_to_pop) = self.items.get_mut(first) else {
            return false;
        };
        if keep {
            last_to_pop.release = content.len();
            self.held += content.len();
        }
        true
    }

    fn pop(&mut self) -> Option<Pending> {
        let p = self.items.pop()?;
        self.held -= p.release;
        Some(p)
    }
}

/// Objects resolved in one block of roots: entry, oid, kind.
pub(super) type Resolved = Vec<(usize, Oid, Kind)>;

/// The base a walk starts from: its entry in the pack (none for a thin
/// pack's base), oid, kind and content.
pub(super) type Root = (Option<usize>, Oid, Kind, Arc<Vec<u8>>);
