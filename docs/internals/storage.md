# Storage

This page describes how a compute node stores and reads repository data: the
layer file format, object ids, the storage service, the buffer pool, and the
rule that no thread waits on the object store.

**The key idea.** A layer is an immutable file, written once and never
changed, so any node can cache any part of it without coordination. Reads go
through a buffer pool over local copies, and a read that needs the object
store waits as a task, never as a blocked thread.

## Layers

A layer is an immutable file:

```
data region      pack entries (full or REF_DELTA) in storage order: a pushed
                 layer groups them by kind (commits | trees | blobs | tags),
                 topological order within a kind; a merged layer keeps each
                 source's order, sources oldest first
object table     fixed 52-byte records, one per entry, in storage order:
                 oid, offset, length, size, base ref, base-context ref, depth
oid index        (oid, local index), 24 bytes each, sorted by (kind, oid), 170
                 to each 4 KiB page so that no entry crosses a page
external oids    oids referenced but stored in other layers
commit section   commits in topological order: tree, parents, time, segment members
commit graph     merged layers only: the commit graph in ids, fixed-width arrays
                 by commit rank
index region     every 256th oid per kind of the oid index, kind runs, id runs,
                 Bloom filter over all oids, then the SHA-256 of every chunk of
                 the file before the index region
footer           section offsets, per-kind counts, run counts, the number of ids
                 the layer covers, the object format (1 SHA-1, 2 SHA-256),
                 crc32 of the index region and footer, "ENRLAYR2"
```

The chunk hashes check everything before the index
region, and the crc32 checks the rest.

- **One order.** The local index of an object is its position in the object
  table, and the table is in storage order. Id order is the same order (see
  [Object ids](#stable-object-numbering)), and a delta's base always comes before
  the delta. So a pass in id order reads the table and the data region from
  front to back, and a pack sent in id order can name each base by offset.
- **Splitting by type.** Kinds occupy runs of the table and the data region:
  four in a pushed layer, up to four per source in a merged layer. The kind runs
  stay in memory, so the kind of an object costs no read. A blobless clone never
  touches blob bytes, and the commit graph is a small, separate section.
- **Lookup reads one page.** Oid index entries are fixed-size and sorted by
  (kind, oid). A layer keeps every 256th oid in memory. A lookup binary-searches
  that sample, then reads one index page in the [buffer
  pool](#pages-and-memory): oids are uniform, so interpolation finds the page and
  a short scan finds the entry, which gives the local index. A caller that needs
  the record then reads it by local index. The object table, oid index and
  external oids are paged separately in 4 KiB pages (data uses 64 KiB), so a
  random lookup loads little beyond the entry it needs.
- **Metadata is always local.** Everything after the data region (table,
  external oids, commit section, commit graph, index region, footer) is loaded
  when a layer opens. The manifest entry records the file size and the metadata
  size, so a remote open takes one ranged read. The metadata stays pinned while
  the layer is open, so lookups and table scans never wait for bytes. Only entry reads can. The
  samples and a Bloom filter (about 10 bits per object) stay in memory.
- **Chunk hashes** verify any chunk on its own. They check bytes read by range
  from the object store, chunks loaded before the hashes were known, and the
  chunks of a linked copy, before first use. A chunk that fails is dropped and
  fetched again.
- **Refs inside a layer** are layer-local indexes or indexes into the external
  oid table.
- **Object ids.** Every object has an id, the same on every node, from the range
  of its layer in the manifest ([Stable object
  numbering](#stable-object-numbering)). The file holds ids relative to the
  start of the range, as id runs (in memory). A pushed layer's file is written
  before its range is assigned and has one run, so each id is the first id of
  the range plus the local index. A merged layer keeps the ids of its sources,
  in their storage order, so its ids also go up with the local index. Its runs
  mark where ids skip a hole. Views number objects by these ids.
- **Entries are stored as pack entries**, so serving copies them verbatim.

## Stable object numbering

A layer covers a contiguous range of object ids. The ids are assigned through
the log, so they are the same on every node.

**Why.** A view must not depend on the position of its layer. Without stable
ids, a layer's dense ids start where the previous layer's end. When a flush
replaces a run of inline layers with one merged layer, or a compaction replaces
stored layers, every view after them is wrong and must be rebuilt. Under heavy
writes into one repository, these rebuilds would be the biggest wait of a push. A longer inline
tail does not help, because it makes every snapshot build costlier.

**What numbering must keep.** Dense ids are dense (about 0 to N). Arrays are
indexed by them (delta-base links), and segments and roaring bitmaps are compact
sets of them. They are not hashes (sparse) and not ranks in sorted order (every
new object would shift later ids). Ids that each node assigns itself would
depend on the history of a node, so nothing derived from them could be shared.

### Assignment

- The replicated repository state keeps a counter of the next free id. A commit
  entry that adds a pushed layer gives it one id per object from that counter
  (the first id is in its manifest entry) and advances the counter. A layer
  keeps its ids for as long as it lives.
- Ranges follow log order. The layer list is ordered by id, and ranges do not
  overlap. A merge (flush or compaction) takes a contiguous run of layers. Its
  output covers the range of the source layers. Duplicates that the merge drops
  (from concurrent pushes) leave holes in the range. A hole is never
  referenced, because views resolve every reference to the canonical copy.
- A merge copies the source layers in
  order, so every object keeps its id. Id order is a topological order: a push
  refers only to its own objects and to the snapshot it was ingested against,
  both with smaller ids, and deduplication keeps the oldest, smallest-id copy.
  So a layer refers only to itself and older layers. Within a layer, entries
  stay in commit order.
- For a pushed layer, the id is the first id plus the local index. A merged
  layer's local order is also id order, with holes. It stores id runs: the first
  local index of each run of consecutive ids, and its id (one run, plus one per
  hole). Going from a local index to an id is an addition, or a binary search
  over the runs and an addition. Finding the layer of an id is a binary search
  over the ranges of the layers, then the same. Neither reads the file.
- The state machine rejects a replacement of layers that are not consecutive, or
  whose outputs' ranges are not ascending, disjoint and within the range of the
  replaced layers. It also rejects pushes past the 32-bit id space. A manifest
  entry carries the first id and the length of the range (a merged layer's is
  more than its object count where duplicates were dropped).

### Effects

- **No rebuilds after a flush or compaction.** Views are built against ids and
  do not depend on position. The outputs of a merge cover exactly the range of
  the layers they replace. Swapping them in replaces a stretch of the layer list
  of the snapshot and leaves every other view as it is. The view of a layer takes
  its range from the manifest, not from its predecessor.
- **Duplicates resolve to the smallest id.** Ranges follow log order and merges
  keep the oldest copy. The smallest id, the oldest copy and the copy in the
  earliest layer are one and the same. The rule is structural. The lookup order
  does not have to maintain it.
- **The inline index holds ids and kinds.** The layer of an id comes from a
  search over the layer ranges.
- **Id space.** Holes come only from duplicates of concurrent pushes. The id
  space therefore grows with the objects ever added, not with the objects
  present. A 32-bit id per repository allows about 4 billion ids. A commit
  entry that would overflow it fails.

The view of a merged layer comes from its commit graph section (see
[Materialization](read-path.md#materialization)).

## Storage service

**Rule.** Whatever is persisted, shared and authoritative goes through the log.
The log's state stays on the ledger's disks, which move with their nodes
([Freezing the ledger](ledger-and-compute.md#freezing-the-ledger)). Derivable data may be
persisted. Scratch data lives for one operation.

| Class | Examples | Lives in | Rules |
|---|---|---|---|
| **Shared** | layer bytes, the layer manifest, refs | the log (with inline layers' bytes); immutable files in the object store, each key written once | Part of shared state only through a log entry. Bytes are written first and referenced second: a crash leaves at most an unreferenced orphan (swept), never a dangling reference. |
| **Derived** | local layer copies, pool pages, caches | local disk | Rebuildable from shared state; never read by another node; correctness never depends on it. |
| **Scratch** | spooled request bodies, ingest temp tables, builder spill files | memory or local disk | Lives as long as the operation that made it. |

### Layering

Dependencies point down only:

```
protocol   the git wire protocol; hands request bodies over as opaque byte sources
engine     ingest (with the pack indexer), serving, layer maintenance, views
storage    layers over immutable files, tiered reads, the buffer pool, temp tables,
           the object store
git        pure formats and codecs (objects, zlib, deltas, pack entries); no I/O
```

The Git formats read bytes through a positional-read interface, and know
nothing else. The engine never sees paths, pages, files, durability mechanics or
the object store. It asks storage for these things:

- **Scratch** for one operation: temp tables, spooled request bodies, and layer
  builders. A finished layer is sealed with its root, in memory when small or in
  a scratch file that publishing moves into place.
- **Publish:** storage puts a sealed layer in the object store and keeps a local
  copy. Whether a layer goes inline instead is a decision of the compute: it is
  a property of the log entry, not of storage.
- **Read:** storage opens a stored layer wherever it is, or an inline layer from
  its bytes in the log, after it checks them against the root. Each read says
  whether it is a point read or a scan. That hint goes down to pool pages and
  disk chunks.
- **Load:** a read of bytes that are not local fails. The engine loads them
  first, and storage keeps them on local disk until the reader is done
  ([Never waiting on the object store](#never-waiting-on-the-object-store)).
- **Cleanup:** storage lists and deletes layer files for the [layer
  sweep](maintenance.md#the-layer-sweep), which decides what goes. A local copy
  can go a while after its layer closes.

### Files and tiers

Inside storage, a layer file is named by its repository and id. Upload, local
copies and deletion work on these names. A file is read in tiers: pool pages,
then local disk, then the object store. Storage keeps opened layers for reuse,
with one opener per layer.

**Every local copy is one sparse file and a chunk bitmap** in memory. Local
disk does not outlive the process ([Local disk is a
cache](read-path.md#local-disk-is-a-cache)), so every file on it was written by
this process. Opening a copy reuses its open source if it has one, else finds
what it is (its length, whether it is linked, and whether this process wrote it
whole), then opens it once.

- A copy that this node wrote (a layer it published) has every chunk.
- A file opened from the object store starts with no chunks. It is served at
  once, and loads fill it in ([Never waiting on the object
  store](#never-waiting-on-the-object-store)).

Without a disk budget, the node loads the rest of an incomplete copy in the
background (`hydrations` in `/status`). With a budget, it loads only what is
read. A node that lost its disk, or one that was just handed a compacted layer,
serves right away instead of downloading whole layers first.

### Linked local copies

When the object store is a `file://` directory, the local copy of a layer is a
hard link to its file in the store. Enroute makes the link on publish and on
every open (linked under scratch, then renamed into place). Without links, every
stored layer is on disk twice under `enroute dev` or a single-volume container:
the file of the store and the local copy.

- A link shares the inode of the stored file. Stored files are written once (to
  a temporary name, then renamed) and never changed, so sharing is safe as long
  as nothing writes through the link.
- A linked copy is opened read-only, has every chunk, and is
  outside the disk budget, so no eviction punches the stored file.
- A chunk that fails its hash is not repaired. The bytes of the store are the
  same bytes, so the load fails its hash check before it writes, and the
  read-only descriptor refuses a write anyway.
- Releasing, sweeping and deleting are unchanged. Each removes a name, and the
  space is freed when the stored file and the link are both gone.
- A link that fails falls back to a copy. `EXDEV` (the store on another
  filesystem) turns linking off for the process. If a link cannot be made again
  but the old one shares the inode of the stored file, the node opens it as
  linked, never for writing.
- `links` in `/status` counts them.

## Pages and memory

Only storage touches files, as in the classic DBMS architecture (executor,
access methods, buffer manager, disk space manager). The disk space manager
writes immutable files once and moves them into place, and it gives positional
and sequential reads and scratch files. The pool is the buffer manager. A layer
is the access method. Request handling, ingest and serving talk
to access methods, never to files or pages. The ledger's log and state stores
are the other direct disk users. Pages stay internal.

Everything that is read or worked on goes through the pool:

- layers as normal pages;
- spooled pushes as scan pages (first to be evicted);
- temp tables as writable pages;
- uploads read scratch files directly.

**Why a pool.** Memory has several independent budgets: the object cache (3/4 of
`node.cache`), the entry cache (1/4), a 128 MiB cache per concurrent ingest,
and the serving views, which are built in memory. A pool gives one global limit
and keeps ingest footprints stable.

### Design

1. **One buffer pool for immutable pages**, with a global byte budget, CLOCK
   eviction and pin guards. Everything paged is immutable, so there is no
   dirty-page writeback, logging or latching beyond pin counts. Page loads are
   synchronous `pread`s of local disk. Remote bytes reach local disk only
   through asynchronous loads ([Never waiting on the object
   store](#never-waiting-on-the-object-store)). A node can serve a layer it has
   not fully downloaded, which gives faster cold starts and no whole-layer
   downloads after compaction.
2. **What is paged:** layer metadata (object table, commit section, external
   oids), through the pool (`node.page_cache`, default 256 MiB). The object
   table, oid index and external oids use 4 KiB pages, and the data region uses
   64 KiB pages. A page table per open file has one atomic slot per page, so a hit
   borrows the page with an atomic load, without touching a shared reference
   count.
   A global CLOCK ring skips pinned pages and prunes closed files lazily. Scans
   (full clones, compaction) insert pages cold. An I/O error fails the read and
   does not stop the process. Planning and streaming read each record once, in id order, which is table
   order.
3. **What is not paged:** object payloads stay in the object and entry caches
   (variable size, keyed by object id), but they are charged to the same budget.
   Temp tables and spooled packs are pool pages too: writable, and written back
   to a scratch file when evicted, like the temporary buffers of DuckDB. Nothing
   is memory-mapped. Memory that the OS page cache would hold uncounted is
   counted and bounded by `node.page_cache`.
4. **No mmap for layers.** A fault on a cold page blocks whatever thread touches
   it, including async runtime workers. A disk error is `SIGBUS` instead of an
   error. There is no control over eviction, and resident memory is opaque. See
   Crotty, Leis and Pavlo, *Are You Sure You Want to Use MMAP in Your DBMS?*
   (CIDR 2022).

The commit graph is flat arrays per layer (tree, generation, time, parent and
segment ranges into shared arrays). Segments are sorted and deduplicated, so
pack order is unchanged. The view takes about 4 bytes per segment member plus
about 32 bytes per commit. A flush or compaction output stores its graph in
the file. A node builds the graph of a pushed layer in memory when the layer
loads. Enroute persists no other node-private index file.

## Never waiting on the object store

**Rule: no thread ever waits on a remote tier.** Layer bytes may live only in
the object store (a cold node, a freshly compacted layer, a disk budget).
Synchronous code (page loads, codecs, everything on the blocking and CPU pools)
only ever reads local disk. Reading bytes that are not local fails. There is no fallback: a path that reads data it did not load is a
bug, and it fails.

### Residency

Residency is local disk, not memory. A file opened remotely gets a sparse local
file and a bitmap of the chunks present in it (1 MiB per chunk). A load fetches
the missing chunks:

- adjacent chunks are coalesced into ranged requests of up to 16 MiB (object
  stores serve 8-16 MiB ranges most efficiently);
- up to 8 requests run concurrently;
- each chunk is fetched once, however many readers ask for it.

Once local, bytes stay local, so the pool can evict pages freely. Hydration
(without a disk budget) is a load of the whole file in the background.
A reader can find out, without reading, which chunks a read would need. The
answer is page-aligned, because page loads read whole pages.

### Machines

Code that reads layer data does one of two things. It loads what it will read
before it runs: merges load their inputs, ingest loads the bases of the thin
pack and then every snapshot delta base, and replay groups load their chain
members. Or it is a resumable state machine, when what it
reads depends on what it has read. Tree walks, tag peeling, segment diffs and
the pack writer are machines.

- A run of the machine does all the work it can with local bytes. An item that needs other
  bytes is *parked* under a ticket with everything it needs: an object's entry
  and its whole delta chain, computed from the table records (always local). A
  chain costs one round trip, not one per step. Other items go on.
- A ticket can also wait on any event (a replay group finishing, budget being
  freed).
- The driver spawns each wait as its own task, so loads progress while the
  machine computes. Waiters for the same bytes share one fetch of each chunk. It resumes
  a ticket as soon as all of its waits are done, without waiting for the others.
  Nothing is restarted or redone.
- The machine runs on the blocking pool. Parallel parts use the CPU pool.

### Disk budget

The disk budget is `node.disk_cache` (default unbounded). With a budget,
every local copy of an object-store file is loaded only as read. Inline layers
have no local copy. A linked copy uses no disk of its own and is outside the
budget. When the node is over budget, it evicts chunks by CLOCK, by punching
holes in the files.

- **Intent steers eviction.** Point reads (and pool misses for them) mark chunks
  as referenced, so they survive a sweep. Scans do not. Scans are pack streaming
  of large fetches, replay groups and merges. A full clone cycles through
  without displacing the working set.
- **Pins.** Pinned chunks are never evicted: layer metadata while the layer is
  open, and whatever a reader has loaded but not yet read. The driver pins the
  bytes of each ticket as it loads them, and hands the pins to the machine.
  The machine drops them once it has read the bytes (the pack writer once it is
  past a read-ahead batch). Phases that load first and read later (merges,
  ingest, replay groups) hold their pins until done. Without pins, a request
  whose working set exceeds the budget evicts its own loads before it reads
  them, so it thrashes or never finishes. With pins it finishes, and what is
  pinned can exceed the budget.
- **Races.** An eviction claims the chunk (refetches wait), clears its bit and
  bumps the epoch of the file before it punches. A read rechecks the epoch after
  its `pread`, so it never returns a punched hole.

Every operation works on a node with no local copies and a budget that never
evicts, so that nothing loads in the background: ls-refs, API reads and
commits, pushes, shallow and deepening fetches, and partial and full clones.
Each one loads what it reads first.
