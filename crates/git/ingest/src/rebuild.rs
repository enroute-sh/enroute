//! Rebuilding an object out of the pack it arrived in: walk the entry's delta
//! chain back to a root, then apply forward.
//!
//! A byte-budgeted cache of recently rebuilt objects keeps a chain from
//! being re-walked once per hop — `git index-pack`'s `delta_base_cache`
//! strategy.

use std::collections::HashSet;
use std::sync::Mutex;

use bytes::Bytes;
use gix_hash::ObjectId;

use enroute_git_core::{Error, ObjectHashMap, ObjectHashSet, object_hash_map_with_capacity};
use enroute_git_packfile::{apply_delta, inflate_entry};
use enroute_git_retrieve::{RepoMetadata, Storage};

use crate::pack::{EntryBase, WirePacks, WireRef};
use crate::pool::on_pool;
use crate::staging::Staging;

/// How much rebuilt content the cache may hold.
///
/// Sized to be worth having on a deep chain without competing with the
/// resolve pass' own budget.
const CACHE_BYTES: usize = 128 * 1024 * 1024;

/// Objects above this never enter the cache: one of them would evict
/// everything else for a hit nothing is likely to want twice.
const MAX_CACHED_BYTES: usize = CACHE_BYTES / 8;

/// Recently rebuilt objects, bounded by their total size rather than their
/// count — entries differ by orders of magnitude, so a count bounds nothing.
struct ByteCache {
    lru: lru::LruCache<ObjectId, Bytes>,
    bytes: usize,
}

impl ByteCache {
    fn new() -> Self {
        Self {
            // Unbounded by count; `bytes` is the real limit.
            lru: lru::LruCache::unbounded(),
            bytes: 0,
        }
    }

    fn get(&mut self, oid: ObjectId) -> Option<Bytes> {
        self.lru.get(&oid).cloned()
    }

    fn put(&mut self, oid: ObjectId, content: &Bytes) {
        if content.len() > MAX_CACHED_BYTES {
            return;
        }
        if let Some(old) = self.lru.put(oid, content.clone()) {
            self.bytes = self.bytes.saturating_sub(old.len());
        }
        self.bytes = self.bytes.saturating_add(content.len());
        while self.bytes > CACHE_BYTES {
            let Some((_, evicted)) = self.lru.pop_lru() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(evicted.len());
        }
    }
}

/// One hop of a delta chain: the compressed instructions and what they
/// inflate to.
struct Hop {
    oid: ObjectId,
    body: Bytes,
    decompressed_size: u64,
}

/// Where a chain walk goes next: another entry of this push, or an object
/// only the repo has.
enum Next {
    Entry(WireRef),
    Repo(ObjectId),
}

/// Where a chain bottoms out.
enum Root {
    /// A whole entry of the pack: inflating its body *is* the object.
    Entry(Hop),
    /// Content already in hand — a cache hit, or an object from the store.
    Content(Bytes),
}

/// Rebuilds objects on demand, remembering what it recently rebuilt.
///
/// Holds no borrow of the staging area: callers alternate between reading
/// content and staging it, and staging takes it by `&mut`.
pub(crate) struct Rebuilder {
    cache: Mutex<ByteCache>,
}

impl Rebuilder {
    pub(crate) fn new() -> Self {
        Self {
            cache: Mutex::new(ByteCache::new()),
        }
    }

    /// `oid`'s content: from the cache, rebuilt from the pack it arrived in,
    /// or read from wherever the repo already keeps it.
    ///
    /// # Errors
    /// Returns an error if the object is nowhere, or if inflating or applying
    /// its chain fails.
    pub(crate) async fn content(
        &self,
        oid: ObjectId,
        wire: &WirePacks,
        staging: &Staging,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> Result<Bytes, Error> {
        if let Some(hit) = self.cached(oid) {
            return Ok(hit);
        }
        match wire.entry_of(oid) {
            Some(at) => self.rebuild(at, wire, staging, state, repo).await,
            None => self.outside(oid, staging, state, repo).await,
        }
    }

    /// [`content`](Self::content), preferring what a batch already read.
    ///
    /// # Errors
    /// As [`content`](Self::content).
    pub(crate) async fn content_or(
        &self,
        oid: ObjectId,
        prefetched: &ObjectHashMap<Bytes>,
        wire: &WirePacks,
        staging: &Staging,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> Result<Bytes, Error> {
        match prefetched.get(&oid) {
            Some(hit) => Ok(hit.clone()),
            None => self.content(oid, wire, staging, state, repo).await,
        }
    }

    /// Read everything in `oids` that this push never received, in one batch,
    /// for the caller to hold while it works through them.
    ///
    /// Returned as well as cached: the cache can refuse or evict an entry,
    /// either of which would cost a second read otherwise.
    ///
    /// # Errors
    /// Returns an error if a lookup or read fails.
    pub(crate) async fn prefetch(
        &self,
        oids: &[ObjectId],
        wire: &WirePacks,
        staging: &Staging,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> Result<ObjectHashMap<Bytes>, Error> {
        // Deduped by the set: a base shared across the batch is probed once.
        let wanted: Vec<ObjectId> = oids
            .iter()
            .copied()
            .collect::<ObjectHashSet>()
            .into_iter()
            .filter(|&oid| wire.entry_of(oid).is_none() && self.cached(oid).is_none())
            .collect();
        let mut read = object_hash_map_with_capacity(wanted.len());
        for (oid, (_, content)) in staging.fetch_many(&wanted, state, repo).await? {
            self.remember(oid, &content);
            read.insert(oid, content);
        }
        Ok(read)
    }

    fn cached(&self, oid: ObjectId) -> Option<Bytes> {
        self.cache.lock().ok()?.get(oid)
    }

    fn remember(&self, oid: ObjectId, content: &Bytes) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(oid, content);
        }
    }

    /// Walk `at`'s chain back to something whole, then apply forward.
    async fn rebuild(
        &self,
        at: WireRef,
        wire: &WirePacks,
        staging: &Staging,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> Result<Bytes, Error> {
        // Newest first: the walk pushes as it descends, and applying runs
        // back up the same list.
        let mut chain: Vec<Hop> = Vec::new();
        let mut walked: HashSet<WireRef> = HashSet::new();
        let mut cursor = at;
        let root = loop {
            walked.insert(cursor);
            let hop = hop(wire, cursor)?;
            let next = match wire.entry(cursor)?.base {
                EntryBase::Whole(_) => break Root::Entry(hop),
                EntryBase::InPack(base) => Next::Entry(WireRef {
                    pack: cursor.pack,
                    at: base,
                }),
                EntryBase::Ref(base_oid) => wire
                    .entry_of(base_oid)
                    .filter(|at| !walked.contains(at))
                    .map_or(Next::Repo(base_oid), Next::Entry),
            };
            chain.push(hop);
            cursor = match next {
                Next::Entry(at) => at,
                // A thin-pack base, or an in-pack entry naming one of its own
                // ancestors: the resolve pass reads both from the repo, and
                // following the latter here would not terminate.
                Next::Repo(oid) => {
                    break Root::Content(self.outside(oid, staging, state, repo).await?);
                }
            };
            // A base already rebuilt ends the walk wherever it is found.
            if let Some(hit) = self.cached(wire.oid(cursor)?) {
                break Root::Content(hit);
            }
        };

        let Rebuilt {
            target,
            along_the_way,
        } = on_pool(move || apply_chain(root, chain)).await??;
        for (oid, content) in &along_the_way {
            self.remember(*oid, content);
        }
        Ok(target)
    }

    /// An object this push never received, from wherever the repo keeps it.
    async fn outside(
        &self,
        oid: ObjectId,
        staging: &Staging,
        state: &Storage,
        repo: &RepoMetadata,
    ) -> Result<Bytes, Error> {
        if let Some(hit) = self.cached(oid) {
            return Ok(hit);
        }
        let (_, content) = staging
            .fetch(oid, state, repo)
            .await?
            .ok_or_else(|| anyhow::anyhow!("{oid} is in neither this push nor the store"))?;
        self.remember(oid, &content);
        Ok(content)
    }
}

/// One hop of a chain, ready to hand to a worker.
fn hop(wire: &WirePacks, at: WireRef) -> Result<Hop, Error> {
    Ok(Hop {
        oid: wire.oid(at)?,
        body: wire.body(at)?,
        decompressed_size: wire.entry(at)?.decompressed_size,
    })
}

/// What one chain walk produced.
struct Rebuilt {
    target: Bytes,
    /// The versions passed through on the way, for the cache.
    ///
    /// Skips whatever the cache would refuse anyway, so large objects don't
    /// stay resident only to be dropped.
    along_the_way: Vec<(ObjectId, Bytes)>,
}

/// Apply `chain` (newest first) onto `root`.
fn apply_chain(root: Root, chain: Vec<Hop>) -> Result<Rebuilt, Error> {
    let mut along_the_way: Vec<(ObjectId, Bytes)> = Vec::new();
    let mut keep = |oid: ObjectId, content: &Bytes| {
        if content.len() <= MAX_CACHED_BYTES {
            along_the_way.push((oid, content.clone()));
        }
    };

    let mut content = match root {
        Root::Entry(hop) => {
            let inflated = inflate_entry(&hop.body, hop.decompressed_size)?;
            keep(hop.oid, &inflated);
            inflated
        }
        Root::Content(content) => content,
    };
    for hop in chain.into_iter().rev() {
        let delta = inflate_entry(&hop.body, hop.decompressed_size)?;
        content = Bytes::from(apply_delta(&content, &delta)?);
        keep(hop.oid, &content);
    }
    Ok(Rebuilt {
        target: content,
        along_the_way,
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use enroute_git_core::oid;

    use super::ByteCache;

    /// The cache is bounded by bytes, not entries: a run of large objects
    /// must evict, or a push's peak memory would follow its object count.
    #[test]
    fn the_cache_evicts_by_size() {
        let mut cache = ByteCache::new();
        let chunk = super::MAX_CACHED_BYTES;
        for i in 0..12u8 {
            cache.put(oid(i), &Bytes::from(vec![i; chunk]));
        }
        assert!(cache.bytes <= super::CACHE_BYTES, "{}", cache.bytes);
        assert!(
            cache.get(oid(11)).is_some(),
            "the most recent entry must survive"
        );
        assert!(cache.get(oid(0)).is_none(), "the oldest must not");
    }

    #[test]
    fn an_oversized_object_is_never_cached() {
        let mut cache = ByteCache::new();
        cache.put(oid(1), &Bytes::from(vec![1u8; super::MAX_CACHED_BYTES + 1]));
        assert!(cache.get(oid(1)).is_none());
        assert_eq!(cache.bytes, 0);
    }
}
