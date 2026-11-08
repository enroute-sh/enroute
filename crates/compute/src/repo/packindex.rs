//! Indexing an incoming pack: every object resolved (deltas included),
//! hashed and handed to a visitor, with lazy, cached access afterwards.
//!
//! Per-object metadata lives in temp tables ([`TempTable`]s) and the oid
//! lookup is a sorted [`OidIndex`], so indexing a huge pack needs memory for
//! the content cache, not for every object. The pack is read once in order
//! ([`PackReader`]), as it arrives, then through [`ReadAt`] for positional
//! reads of entries while resolving deltas.

use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::git::delta;
use crate::git::io::{self, ReadAt};
use crate::git::oid::{Kind, ObjectFormat, Oid, try_hash_object};
use crate::git::pack::{EntryBase, PackReader, encode_entry_header};
use crate::git::zlib;
use crate::storage::{OidIndex, TempTable, Workspace};
use crate::util::ix;

use super::view::must;

mod walk;
use walk::{Children, Resolved, Walk};

/// One pack entry, as a fixed-size record (see [`crate::storage::spill`]).
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
struct EntryRec {
    data_start: u64,
    /// Inflated size of the entry data (for deltas: of the delta).
    size: u64,
    data_len: u32,
    /// `NO_BASE`, an entry index (`OFS_DELTA`), or `REF_BASE | slot` into
    /// `ref_bases` (`REF_DELTA`).
    base: u32,
    type_code: u32,
    _pad: u32,
}

const NO_BASE: u32 = u32::MAX;
const REF_BASE: u32 = 1 << 31;

/// The object kind of entry `e`.
fn entry_kind(e: &EntryRec) -> Result<Kind> {
    u8::try_from(e.type_code)
        .ok()
        .and_then(Kind::from_code)
        .context("bad object type")
}

#[derive(Debug, Clone, Copy)]
enum Base {
    None,
    Ofs(usize), // entry index
    Ref(Oid),
}

/// External object lookup for thin packs: oid -> (kind, content).
pub(crate) type BaseLookup<'a> = dyn Fn(&Oid) -> Result<Option<(Kind, Arc<Vec<u8>>)>> + Sync + 'a;

/// Decoded objects of a pack, by entry index.
pub(crate) type ContentCache = crate::util::ByteCache<usize>;

/// Callback for every resolved object: (oid, kind, content).
pub(crate) type Visitor<'a> = dyn Fn(&Oid, Kind, &[u8]) -> Result<()> + Sync + 'a;

/// An indexed incoming pack. Objects are resolved on demand.
///
/// Per-object metadata lives in [`TempTable`]s and the oid lookup is a sorted
/// [`OidIndex`], so indexing a huge pack needs memory for the content
/// cache, not for every object.
pub(crate) struct PackIndex {
    format: ObjectFormat,
    data: Arc<dyn ReadAt>,
    entries: TempTable<EntryRec>,
    ref_bases: TempTable<Oid>,
    oids: TempTable<Oid>,
    kinds: TempTable<u8>,
    by_oid: OidIndex,
    /// Entries in the order the resolver walked them: each base right
    /// before the deltas on it.
    order: TempTable<u32>,
}

/// A pack after the sequential pass ([`PackIndex::scan`]): entry
/// boundaries known, full objects hashed, deltas still to resolve.
pub(crate) struct Scanned {
    format: ObjectFormat,
    entries: TempTable<EntryRec>,
    ref_bases: TempTable<Oid>,
    oids: TempTable<Oid>,
    kinds: TempTable<u8>,
    remaining: Vec<u32>,
}

impl Scanned {
    /// Every `REF_DELTA` base named by the pack (some may be in the pack
    /// itself; the others must come from elsewhere: a thin pack).
    pub(crate) fn ref_bases(&self) -> impl Iterator<Item = Oid> + '_ {
        self.ref_bases.iter()
    }
}

impl PackIndex {
    /// Pass 1 walks the pack sequentially (entry boundaries are only known
    /// after inflating) and hashes full objects on the spot, calling
    /// `visit(oid, kind, content)` for each object (possibly from several
    /// threads); pass 2, [`Scanned::resolve`], resolves the deltas from each
    /// base outward, consulting `external` for thin-pack bases.
    ///
    /// `reader` reads the pack in arrival order, so this can run while it
    /// is received; pass 2 reads entries from the stored pack.
    pub(crate) fn scan(
        mut reader: PackReader<'_>,
        visit: &Visitor<'_>,
        ws: &Workspace,
    ) -> Result<Scanned> {
        let count = reader.count();
        ensure!(count < REF_BASE, "pack has too many objects");
        let count = ix(count);
        let format = reader.format();
        let mut idx = Scanned {
            format,
            entries: TempTable::new(ws),
            ref_bases: TempTable::new(ws),
            oids: TempTable::filled(ws, count, Oid::ZERO),
            kinds: TempTable::filled(ws, count, 0),
            remaining: Vec::new(),
        };
        // Entry offsets, ascending: OFS_DELTA bases are found by binary search.
        let mut offsets: TempTable<u64> = TempTable::new(ws);
        let mut i = 0u32;
        while let Some(e) = reader.next_entry()? {
            let base = match e.base {
                EntryBase::Ofs(base_off) => {
                    let b = offsets.partition_point(|&o| o < base_off);
                    ensure!(
                        b < offsets.len() && offsets.get(b) == base_off,
                        "ofs-delta base not at entry boundary"
                    );
                    u32::try_from(b).context("pack has too many objects")?
                }
                EntryBase::Ref(o) => {
                    let slot =
                        u32::try_from(idx.ref_bases.len()).context("pack has too many objects")?;
                    idx.ref_bases.push(o);
                    REF_BASE | slot
                }
                EntryBase::None => NO_BASE,
            };
            offsets.push(e.offset);
            idx.entries.push(EntryRec {
                data_start: e.data_start,
                size: e.size,
                data_len: e.data_len.try_into().context("pack entry too large")?,
                base,
                type_code: u32::from(e.type_code),
                _pad: 0,
            });
            if base == NO_BASE {
                let kind = Kind::from_code(e.type_code).context("bad object type")?;
                let oid = try_hash_object(format, kind, &e.content)?;
                idx.oids.set(ix(i), oid);
                idx.kinds.set(ix(i), kind.code());
                visit(&oid, kind, &e.content)?;
            } else {
                idx.remaining.push(i);
            }
            i += 1;
        }
        Ok(idx)
    }
}

impl Scanned {
    /// Pass 2: resolve every delta, base first, as git's index-pack does:
    /// each delta is inflated and applied once, while its base is still at
    /// hand.
    ///
    /// # Errors
    ///
    /// Returns an error if a delta or its base is corrupt, a base is
    /// missing, or `visit` fails.
    pub(crate) fn resolve(
        self,
        data: Arc<dyn ReadAt>,
        external: &BaseLookup<'_>,
        cache: &ContentCache,
        visit: &Visitor<'_>,
        ws: &Workspace,
    ) -> Result<PackIndex> {
        let Scanned {
            format,
            entries,
            ref_bases,
            oids,
            kinds,
            remaining,
        } = self;
        let mut idx = PackIndex {
            format,
            data,
            entries,
            ref_bases,
            oids,
            kinds,
            by_oid: OidIndex::build(ws, std::iter::empty()),
            order: TempTable::new(ws),
        };
        let count = u32::try_from(idx.entries.len()).context("pack has too many objects")?;
        let walk = Walk {
            kids: Children::build(&idx, &remaining, ws),
            cache,
            visit,
            external,
        };
        let (roots, leaves): (Vec<u32>, Vec<u32>) = (0..count)
            .filter(|&i| idx.entries.get(ix(i)).base == NO_BASE)
            .partition(|&i| walk.kids.has(Some(ix(i)), &idx.oids.get(ix(i))));
        walk.run(&mut idx, &roots, |idx, &i| {
            let i = ix(i);
            let kind = entry_kind(&idx.entries.get(i))?;
            let content = Arc::new(idx.inflate(i)?);
            cache.insert(i, content.clone());
            Ok(Some((Some(i), idx.oids.get(i), kind, content)))
        })?;

        // The `REF_DELTA` bases not in the pack: a thin pack's.
        let mut bases: Vec<Oid> = walk
            .kids
            .by_ref
            .iter()
            .filter(|c| idx.oids.get(ix(c.entry)).is_zero())
            .map(|c| c.base)
            .collect();
        bases.dedup();
        walk.run(&mut idx, &bases, |_, oid| {
            Ok(external(oid)?.map(|(kind, content)| (None, *oid, kind, content)))
        })?;

        if let Some(&i) = remaining.iter().find(|&&i| idx.oids.get(ix(i)).is_zero()) {
            let e = idx.entries.get(ix(i));
            bail!(
                "unresolvable delta at data offset {} (missing base {:?})",
                e.data_start,
                idx.base(e)
            );
        }
        idx.order.extend_from_slice(&leaves);
        idx.by_oid = OidIndex::build(ws, (0..count).map(|i| (idx.oids.get(ix(i)), i)));
        Ok(idx)
    }
}

impl PackIndex {
    fn base(&self, e: EntryRec) -> Base {
        match e.base {
            NO_BASE => Base::None,
            b if b & REF_BASE != 0 => Base::Ref(self.ref_bases.get(ix(b & !REF_BASE))),
            b => Base::Ofs(ix(b)),
        }
    }

    /// Record the resolved objects of one window, in walk order.
    fn store(&mut self, results: Vec<Result<Resolved>>) -> Result<()> {
        for r in results {
            for (i, oid, kind) in r? {
                self.oids.set(i, oid);
                self.kinds.set(i, kind.code());
                self.order
                    .push(u32::try_from(i).context("pack has too many objects")?);
            }
        }
        Ok(())
    }

    fn inflate(&self, i: usize) -> Result<Vec<u8>> {
        let e = self.entries.get(i);
        let size = usize::try_from(e.size).context("pack entry too large")?;
        zlib::decompress(&io::read(&*self.data, e.data_start, ix(e.data_len))?, size)
    }

    /// Every entry, each base right before the deltas on it: the order in
    /// which reading entries rebuilds each delta chain only once.
    pub(crate) fn walk_order(&self) -> impl Iterator<Item = u32> + '_ {
        self.order.iter()
    }

    /// The format of the pack's ids.
    pub(crate) fn format(&self) -> ObjectFormat {
        self.format
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn oid(&self, i: usize) -> Oid {
        self.oids.get(i)
    }

    /// The kind of entry `i`, which is resolved.
    pub(crate) fn kind(&self, i: usize) -> Kind {
        must(Kind::from_code(self.kinds.get(i)), "resolved entry")
    }

    /// Size of object `i` if the pack sends it whole.
    pub(crate) fn whole_size(&self, i: usize) -> Option<u64> {
        let e = self.entries.get(i);
        (e.base == NO_BASE).then_some(e.size)
    }

    /// Entry `i`, sent whole, as a stored entry with the zlib data of the
    /// pack. The sequential pass inflated that data to its stream end, so
    /// it is valid.
    pub(crate) fn whole_entry(&self, i: usize) -> Result<Vec<u8>> {
        let e = self.entries.get(i);
        ensure!(e.base == NO_BASE, "entry {i} is a delta");
        let len = ix(e.data_len);
        let code = u8::try_from(e.type_code).context("bad object type")?;
        let mut out = Vec::with_capacity(len + 10);
        encode_entry_header(code, e.size, &mut out);
        let h = out.len();
        out.resize(h + len, 0);
        let body = must(out.get_mut(h..), "entry buffer");
        self.data.read_at(e.data_start, body)?;
        Ok(out)
    }

    /// Entry index of `oid` in this pack (the first, if duplicated).
    pub(crate) fn lookup(&self, oid: &Oid) -> Option<usize> {
        self.by_oid.get(oid).map(ix)
    }

    /// Content of entry `i`. Walks its chain down to a cached or whole
    /// object, then applies the deltas back up, caching each result.
    pub(crate) fn content(
        &self,
        i: usize,
        external: &BaseLookup<'_>,
        cache: &ContentCache,
    ) -> Result<Arc<Vec<u8>>> {
        let mut chain = Vec::new();
        let mut cur = i;
        let mut content = loop {
            if let Some(c) = cache.get(&cur) {
                break c;
            }
            let base = match self.base(self.entries.get(cur)) {
                Base::None => {
                    let c = Arc::new(self.inflate(cur)?);
                    cache.insert(cur, c.clone());
                    break c;
                }
                Base::Ofs(b) => Ok(b),
                Base::Ref(oid) => self.lookup(&oid).ok_or(oid),
            };
            chain.push(cur);
            ensure!(chain.len() < 10_000, "delta chain too deep");
            match base {
                Ok(b) => cur = b,
                // Not in the pack: a thin pack's base.
                Err(oid) => break external(&oid)?.context("delta base missing")?.1,
            }
        };
        while let Some(d) = chain.pop() {
            let out = Arc::new(delta::apply(&content, &self.inflate(d)?)?);
            cache.insert(d, out.clone());
            content = out;
        }
        Ok(content)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::git::io::Incoming;
    use crate::git::oid::hash_object;
    use crate::git::pack::{full_entry, ref_delta_entry};

    #[test]
    fn index_simple_pack() {
        let base = b"hello world, this is a base object with some content".repeat(4);
        let mut target = base.clone();
        target.extend_from_slice(b"more");
        let base_oid = hash_object(Kind::Blob, &base);
        let d = delta::encode(&base, &target, usize::MAX).unwrap();
        let entries = [
            full_entry(Kind::Blob, &base),
            ref_delta_entry(&base_oid, &d),
        ];
        let pack = crate::git::pack::pack_of(ObjectFormat::Sha1, &entries);
        let cache = crate::util::byte_cache(1 << 20);
        let none = |_: &Oid| Ok(None);
        let tmp = tempfile::tempdir().unwrap();
        let src = Arc::new(io::Owned(pack));
        let ws = Workspace::at(tmp.path());
        let idx = PackIndex::scan(
            PackReader::new(&mut Incoming::of(src.clone()), ObjectFormat::Sha1, u64::MAX).unwrap(),
            &|_, _, _| Ok(()),
            &ws,
        )
        .unwrap()
        .resolve(src, &none, &cache, &|_, _, _| Ok(()), &ws)
        .unwrap();
        assert_eq!(idx.oid(1), hash_object(Kind::Blob, &target));
        assert_eq!(*idx.content(1, &none, &cache).unwrap(), target);
    }

    /// Chains of `OFS_DELTA`s and `REF_DELTA`s, on bases in the pack and on
    /// a thin pack's base, with a branch.
    #[test]
    fn resolve_mixed_chains() {
        use crate::git::pack::{encode_entry_header, encode_ofs};
        enum Spec {
            Full(usize),
            Ref(usize, usize),
            /// Base entry, base version, version.
            Ofs(usize, usize, usize),
        }
        let versions: Vec<Vec<u8>> = (0..9u8)
            .map(|n| {
                let mut c = b"line of a file that changes a little each time\n".repeat(8);
                c.extend(std::iter::repeat_n(b'a' + n, usize::from(n) + 1));
                c
            })
            .collect();
        let oid = |n: usize| hash_object(Kind::Blob, &versions[n]);
        let diff = |from: usize, to: usize| {
            delta::encode(&versions[from], &versions[to], usize::MAX).unwrap()
        };
        // Entry n holds version n + 1; version 0 comes from outside.
        let specs = [
            Spec::Ref(0, 1),
            Spec::Ofs(0, 1, 2),
            Spec::Ofs(1, 2, 3),
            Spec::Ofs(1, 2, 4),
            Spec::Full(5),
            Spec::Ref(3, 6),
            Spec::Ofs(4, 5, 7),
            Spec::Ofs(6, 7, 8),
        ];
        let (mut entries, mut offsets, mut end) = (Vec::new(), Vec::new(), 12u64);
        for spec in specs {
            let e = match spec {
                Spec::Full(to) => full_entry(Kind::Blob, &versions[to]),
                Spec::Ref(from, to) => ref_delta_entry(&oid(from), &diff(from, to)),
                Spec::Ofs(base, from, to) => {
                    let delta = diff(from, to);
                    let mut e = Vec::new();
                    encode_entry_header(6, u64::try_from(delta.len()).unwrap(), &mut e);
                    encode_ofs(end - offsets[base], &mut e);
                    zlib::compress(&delta, &mut e);
                    e
                }
            };
            offsets.push(end);
            end += u64::try_from(e.len()).unwrap();
            entries.push(e);
        }
        let pack = crate::git::pack::pack_of(ObjectFormat::Sha1, &entries);

        let base = Arc::new(versions[0].clone());
        let thin = |o: &Oid| Ok((*o == oid(0)).then(|| (Kind::Blob, base.clone())));
        // The walk reads bases again (see `HELD_BYTES`), through the cache
        // or, with no cache, from the pack.
        for cache_bytes in [1 << 20, 0] {
            let cache = crate::util::byte_cache(cache_bytes);
            let tmp = tempfile::tempdir().unwrap();
            let ws = Workspace::at(tmp.path());
            let src = Arc::new(io::Owned(pack.clone()));
            let idx = PackIndex::scan(
                PackReader::new(&mut Incoming::of(src.clone()), ObjectFormat::Sha1, u64::MAX)
                    .unwrap(),
                &|_, _, _| Ok(()),
                &ws,
            )
            .unwrap()
            .resolve(src, &thin, &cache, &|_, _, _| Ok(()), &ws)
            .unwrap();
            for n in 0..8 {
                assert_eq!(idx.oid(n), oid(n + 1));
                assert_eq!(idx.kind(n), Kind::Blob);
                assert_eq!(*idx.content(n, &thin, &cache).unwrap(), versions[n + 1]);
            }
        }
    }
}
