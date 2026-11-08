# Read path

This page describes how a compute node serves a fetch: planning on metadata,
streaming stored bytes, and the node-private views that make both fast. It also
covers why local disk does not outlive a node's process.

**The key idea.** A fetch is planned on metadata alone (the commit graph and the
list of objects that each commit introduced) and then served by copying stored
pack entries as they are, without decompressing or re-encoding them. Each fetch
reads one immutable snapshot, so it never blocks a push and never sees half of
one.

## Fetch

Serving has two stages: **planning** on metadata, then **streaming** bytes.

**Planning** produces the object list and its count before any bytes are sent.
It reads objects only to walk trees (shallow boundaries, wanted trees) and to
peel tags. Those reads are [machines](storage.md#machines). The rest (commit
walk, locations, delta decisions) works on local metadata on the blocking pool.

1. **Commit walk.** Compute `wants − haves` with a generation-number priority
   queue. Uninteresting marks propagate from the haves. They stop at the
   shallow commits of the client, because the client does not have the history
   past its boundary. The walk stops once nothing interesting is queued.
2. **Object set.** Union the per-commit **segments** (sorted arrays of dense
   ids), in parents-first order, deduplicated by a bitmap. The union of segments
   over the ancestry of a commit is exactly its reachable closure, because each
   segment is "reachable from this commit's tree minus what is at the same paths
   in its parents".
3. **Explicit wants** of trees and blobs (promisor fetches) are never filtered.
4. **Shallow clones.** Boundary commits contribute their full tree closure
   instead of a segment. The walk covers all boundaries together, so it walks a
   subtree that they share once.
5. **Delta base availability.** The node sends a stored delta as is in two
   cases. The base is in the pack. Or the pack is thin, the fetch is not shallow
   or filtered, and the **base context** commit is either sent or known to the
   client (uninteresting). Otherwise the node **re-encodes** the object as a
   full entry. This is safe because the tree of the base context contains the
   base. If the client has that commit, it has the base.
6. **Locations and order.** The node sorts the planned objects by id, which is
   table order and **storage order**: layer by layer, each front to back. It
   reads the table record of every planned object once, in that order (a
   sequential pass). The record gives where the entry is (offset, length) and
   its delta base and base context, which step 5 uses. The node sends the
   objects in the same order, so every base in the pack comes before its deltas
   and goes as `OFS_DELTA` when the client allows it. Streaming then reads the
   data region sequentially and never goes back to the table. Each page loads
   about once. Random record reads on a large layer would miss the pool again
   and again.
7. **Re-encoding by replay.** A shallow fetch re-encodes the boundary trees:
   every object stored as a delta whose base is not sent.
   - Deltas are forward (a base is an older version), and storage order puts
     every base before its deltas. Ingest writes topologically, and merges copy
     in storage order.
   - Large re-encode sets (10k objects or more) are rebuilt in bulk by
     replay. One sequential scan of
     the tables yields every base link. The chains are marked in memory. One
     sequential pass over the data rebuilds them all, in groups bounded to 128
     MiB of content per pass.
   - Groups and compression run as tasks on the shared CPU pool. A group starts
     once the bytes of its chain members are local (the next groups load
     meanwhile) and its size is reserved from a node-wide budget (512 MiB;
     waiting for budget is asynchronous too). The pack writer emits entries as
     they finish.
   - The node builds the base-link array once per snapshot and shares it among
     all requests.
   - Smaller sets walk each chain through the base links of the records.
   - Boundary commits share one walked-tree set, so a shallow fetch across many
     merges walks each subtree once.

**CPU pool.** CPU-bound work (ingest encoding and segment diffing, merges,
replay, compression) runs on one process-wide pool (`node.cpu_threads`, default
one per core). Concurrent requests share the cores instead of spawning threads
each. Tasks on the pool never wait on clients, the object store or memory budget
(replay waits for budget asynchronously).

**OFS_DELTA.** Stored deltas name their base by object id (REF_DELTA, 20 bytes).
When the client accepts `ofs-delta` and the base is already in this pack, the
node rewrites the entry on the fly to name the base by distance (1-4 bytes), as
Git's own packs do.

**Streaming** writes the pack header, copies entries verbatim from layer pages,
and hashes the output for the trailer. It also writes re-encoded full entries,
produced in parallel per window of 1024 objects once their delta chains are
local. All output is inside side-band-64k framing. The writer is a machine:
entries go out in plan order as their bytes are local, with loads for the next
32 MiB of entries in flight. It runs on blocking threads that feed a bounded
channel, so backpressure reaches the producer.

**Staleness.** The view of a compute node can lag a write made through another
node, so every ref read takes a read barrier first: `ls-refs`, the v0
`git-upload-pack` advertisement, and the advertisement for a push. A `fetch`
reads objects by id from the view as it is, and takes the barrier only when the
view does not have a want yet. A push also checks against the view as it is,
and takes the barrier only when the view does not have an object that the push
needs. The ledger refuses a ref update whose old value is stale.

## Node-private views

A *snapshot* is an immutable pair of refs and layers at one replicated version,
published atomically. **Readers
never block writers**, and a fetch sees a consistent version from planning
through streaming.

### Materialization

Applying a log entry only records the latest repository metadata. A
per-repository materializer then builds the next snapshot asynchronously and
coalesces bursts.

- It reuses the view of every layer that is still in the manifest with the same
  id range, and is still inline or still stored, wherever it now sits. It builds views only for the others:
  new pushes, and flush and compaction outputs. A view does not depend on the
  layers before it, because the ids it resolved references and duplicates to are
  stable ([Stable object numbering](storage.md#stable-object-numbering)). A
  single inline layer flushed as is becomes a new file, under a new id, and gets
  a new view.
- A flush or compaction output stores its commit graph in ids, which the merge
  writes from the graphs of its sources: a merge keeps every id and only
  canonical copies, so their nodes hold as they are. Its view copies that
  graph, with no lookups. Indexing a big layer again would take seconds, and
  every request waits for the snapshot.
- The current views and the manifest are both in id order, so one walk pairs
  them. Per-layer work is only for layers without a view. Under load a snapshot
  has many layers and a build adds only a few, so per-layer work such as hashing
  ids and polling a future would dominate a build.
- The inline layers of new pushes start opening as their commands apply, each in
  a task of its own, ahead of the materializer. The materializer builds one
  snapshot at a time per repository.
- When the incarnation of a repository ends here, the node drops its view.
- A stored layer that no view or reader holds any more closes, and storage lets
  its local copy go a while later, unless it is open again by then.

### Dense ids

Each layer covers a range of ids from its manifest entry. The ids are assigned
through the log and are the same on every node: the first id plus the local
index for a pushed layer, the id runs for a merged one. Duplicate oids across
layers map to the oldest copy (the smallest id), so each oid has one dense id
per snapshot. Finding an object by id is a binary search over the ranges of the
layers, then, in a merged layer, a binary search over its id runs in memory. The
state machine rejects pushes past the 32-bit id space.

### Commit graph

Parents, tree, generation number, commit time and the segment (a sorted array of
dense ids) are flat arrays per layer ([Pages and
memory](storage.md#pages-and-memory)). A flush or compaction output stores
them in its file. A node builds them for a pushed layer when the layer loads.

### Caches

- **Object cache.** A sharded, byte-bounded cache of decoded objects, **keyed by
  object id**. Content-addressed entries never go stale. They survive compaction
  (which copies entries verbatim) and are shared by duplicate copies across
  layers. Resolving a delta chain caches the result plus a checkpoint every 16
  steps, not every intermediate version. Otherwise one read of a deep chain
  evicts up to 50 other objects, and repeated shallow clones of a large tree
  keep rebuilding their tips.
- **Entry cache.** Self-contained (full) pack entries for objects that had to be
  re-encoded, also keyed by object id. A shallow clone sends the whole tree of
  the tip, mostly stored as deltas against versions that the client lacks. For
  hot tips (CI) the node re-encodes once, and later clones only copy bytes. A
  new push changes few objects, so only those miss. `node.cache` is split 3:1
  between decoded objects and entries.
- **Bloom filters.** Each layer has a Bloom filter over its object ids (about 10
  bits per object, stored in the layer). Checking a layer for an object costs a
  few bit probes.

### Lookups

A lookup of an object by id returns its oldest copy. It goes through two
structures, cheapest first. A lookup checks the
inline index, then the stored layers in order, so the oldest copy wins.

- **The inline index.** Each snapshot indexes the objects of its inline layers
  in a hash map (object id to canonical dense id and kind). Its hash is seeded
  per map, because object ids come from pushes. A busy repository has hundreds
  of inline layers, one per push until a flush, and walking them would be most
  of the lookup work. The dense id of an entry already names the oldest copy,
  older stored layers included, because the view of the inline layer resolved it
  when it was built. Canonical ids never change, so an entry never goes wrong.
  The index is copied from the previous snapshot and extended. Entries below the
  first id of the oldest inline layer are dropped, because their objects are now
  in stored layers, where lookups find them.
- **Stored layers** are checked one by one, oldest first: a Bloom filter
  check each, and a table search if the filter allows the object. Compaction
  keeps them few. Layer sizes grow geometrically, so a repository has fewer
  than `layers.merge_factor` stored layers per size class
  ([Compaction](maintenance.md#compaction)).

## Local disk is a cache

A compute node's local disk lives as long as its process. At startup the node
moves its old local copies and scratch aside and deletes them in the
background, so a restarted node starts like a fresh one: it attaches to the
log and serves layers by range from the object store at once. Only fresh
starts need to be fast, and nothing on disk needs to be recorded, checked or
reconciled after a crash. A node also starts fresh on a new machine, after
losing its disk and when scaling out, so this is the one start path.

| Class | Examples | Lives in | Integrity and lifetime |
|---|---|---|---|
| Authoritative shared | refs, layer manifest, inline layers | the log; layer bytes in the object store | consensus |
| Node-private | local copies, their chunk bitmaps, views | local disk and memory | the process; loss costs a re-fetch |
| Scratch | spooled bodies, temp tables | temp directory | the operation |

Authoritative state needs agreement. Derived state does not. The layers carry
what can be computed when they are written: the commit section, the segments,
and the per-chunk hashes (SHA-256 of each 1 MiB chunk, in the index region). The
hashes verify ranged reads from the object store and linked copies.

**Local copies.** A local copy of a stored layer is a sparse file of the
layer's length, and its chunk bitmap is in memory. Nothing is written per load
or per eviction besides the chunks themselves.

- Inline layers have no local copies, because the node serves them from their
  bytes in the log.
- A layer that closes keeps its local copy, open, for two minutes, so a
  reopen finds the chunks it loaded. Then the copy is deleted.
- WarpStream agents and Snowflake warehouses treat local disk the same way. A
  restart costs downloads, never correctness.
