# Maintenance

This page describes the background work on a repository's layers: flushing,
compaction, and the layer sweep. All of it runs on the compute node that
maintains the repository ([Maintainers](ledger-and-compute.md#maintainers)).

**The key idea.** Upkeep is an LSM tree's: small layers merge into larger ones
by size, and a file leaves storage only after nothing has named it for a
retention period. Every change is a compare-and-swap on the layer list, so two
nodes doing the same work at once waste effort but never corrupt state, and no
lock or lease is needed.

## Compaction

The background task that flushes inline layers ([Merge before
upload](write-path.md#merge-before-upload)) also merges stored layers. Each
flush and compaction replaces its own layers, so they run independently.

- **Policy (log-structured, by size class).** Merges take adjacent layers only,
  because a merge must keep the id order ([Stable object
  numbering](storage.md#stable-object-numbering)). Lucene's log merge policy has
  the same limit for segments in document order, and Enroute uses the same
  method. From the oldest stored layer, a stretch goes to the last layer that is
  at least 1/`layers.merge_factor` of the largest layer left. The layers of the
  stretch merge at least `layers.merge_factor` (default 8) at a time. A backlog
  merges in wider runs, up to `layers.merge_factor` times the size of the
  largest layer of the stretch, so a burst of small layers does not drain only
  `layers.merge_factor` - 1 layers for each merge. Smaller layers between them
  join their merges, so no layer stays behind. Then the next stretch starts.
  Layers under 64 KiB count as 64 KiB: exact sizes of small flushes are not
  important.
- **Cost.** A merge makes a layer about `layers.merge_factor` times larger. So
  each byte is rewritten about once per size class: log₈(repository size / 64
  KiB) times at the default. A repository has fewer than `layers.merge_factor`
  stored layers per size class, which is the number of layers that a lookup
  checks ([Lookups](read-path.md#lookups)).
- **No largest size.** Every stored layer can merge again. A merge of the
  largest layer rewrites most of the repository, but only when the repository
  has grown by about that size. Every node that follows the repository then
  loads that layer again into its cache. Its view comes from the commit graph
  that it stores ([Materialization](read-path.md#materialization)), so the swap
  does not index it again.
- **Concurrency.** Compactions of disjoint id ranges run at the same time: at
  most 2 for each repository and 4 on each node. The newest due run goes first,
  so small layers continue to merge next to long merges.
- **Merging** copies stored entries **verbatim**, with no recompression, in
  storage order, which is also table order, so it needs no plan. It loads 64
  MiB of source entries at a time (small layers together), and loads the next
  64 MiB while it copies, so memory and local disk do not depend on the size of
  the merge. An output of 256 MiB or more
  goes directly into its multipart upload; a smaller one goes through a local
  file that the node keeps. The merge drops objects that have an older copy, and
  removes a run that holds only such objects.
- **Swap.** One commit entry that adds the merged layer and removes its sources
  acts as a compare-and-swap on the manifest. Concurrent pushes are never
  blocked, and losing the race only wastes the merge. A flush or compaction
  keeps its layers marked as in flight until the state of this node shows its
  swap. If the node freed them at once, the next flush would pick the same
  layers from a state that had not caught up, merge and upload them again, and
  lose.
- **Replaced layers stay where they are.** Compaction and flushes delete
  nothing. Nodes evict replaced layers from their local cache once no snapshot
  references them. The layer sweep deletes the copies in the object store.
- **Bounded object-store concurrency.** Each node caps concurrent object-store
  requests (64). Unbounded fan-out (a burst of uploads, or one delete per
  retired layer) opens hundreds of TLS connections at once, and connects to S3
  time out.

Compaction copies entries verbatim, so it does not re-optimize delta chains
across layers.

## The layer sweep

The layer sweep deletes
layer files that nothing names and nothing will. These are replaced layers,
outputs of lost races, and files of crashed or abandoned publishes.

The maintainer runs it every `layers.sweep_interval` (1 h). It lists the layers
of the repository, then reads the live manifest through a read barrier. It
deletes a listed file only if both conditions hold.

1. **No manifest names it, and none ever will.** The live manifest does not name
   it, and its upload generation (in its id) is out of the window of that state.
   The state machine adds a stored layer only if its generation is at least the
   current upload generation minus a window of 2. Nothing can add an older file
   later.
   Ids are never reused, so a replaced layer cannot come back. These are facts
   about the log. A sweeper with a skewed clock, or one far behind on the log,
   can delay a deletion but never delete a file that something names.
2. **It has gone unnamed for `layers.sweep_retention`** (6 h) **by the clock of
   the object store.** Readers of older states can then finish: compute nodes
   that serve older snapshots, and long queries that read the object store
   directly. The sweep record (`data/repos/<repo>/sweep`, replaced by
   compare-and-swap) keeps the files seen unnamed and since when. The time is
   the last-modified time of the record write that first listed them, filled in
   by the next sweep. That sweep also takes the last-modified time of the record
   as "now". Both err late. Getting this wrong fails a read, never loses a
   referenced file. The retention also bounds how old the volume snapshots an
   operator restores the ledger from may be
   ([Recovery](ledger-and-compute.md#recovery)): a state no older than the
   retention names only files the sweep has kept.

The sweep advances the upload generation (a log entry) only when a file that
nothing names is still in the window. A repository with nothing to delete
therefore costs no log entries.

A publish has about two sweep intervals between its upload and its commit. A
publish that took longer is refused as stale. Pushes and API commits then copy
the file within the object store under the current generation and propose again.
Compactions simply lose the race.

### Deletes

Deletes are bulk requests of 200 keys, 200 ms apart. S3 throttles bursts of
deletes against one prefix (`503 SlowDown`). Ids put the random part first and the
generation last, so the keys of a repository spread over its prefix.

Unfinished multipart uploads never appear in a listing. A bucket lifecycle rule
(`AbortIncompleteMultipartUpload`) removes them.

A repository that no compute follows is not swept until one follows it again.
The exception is the files of deleted incarnations
([Lifecycle](lifecycle.md#reclaiming-layer-files)).

## Garbage collection

Enroute does not collect unreachable objects. Compaction drops duplicate
objects, and the layer sweep deletes layer files that nothing names.
Unreachable objects are kept, and Enroute cannot purge a specific object (a
leaked secret, a takedown).

**Design rule:** objects can disappear, and every layer can be rewritten.
Nothing (caches, per-node indexes, bitmaps) assumes that an object or a layer
lives forever. Everything is keyed by layer id, which is immutable and never
reused. So removing objects needs no format change.

