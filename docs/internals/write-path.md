# Write path

This page describes how a push becomes durable state: ingest, validation, delta
encoding, and the log entry that commits the result. It also covers server-side
commits and the two-entry form of a push when the host installs hooks.

**The key idea.** The expensive work of a push (indexing, checking, and delta
encoding) runs in parallel and without locks, also for pushes to the same
repository. Only the last step is serialized: one log entry that adds the new
layer and moves the refs with a compare-and-swap. A small layer travels inside
that entry, so the push waits for the ledger disks and never for the object
store.

```text
compute node                          ledger (3 replicas)        object store
------------                          -------------------        ------------
spool the pack to disk
index, check, delta-encode (parallel)
build the layer
  layer > 1 MiB? ------------------------------------------------> upload
propose: add layer, CAS refs -------> sequencer appends
                                      majority syncs to disk
<------------------------------------ committed
report to the client
...later, as maintainer:
merge the inline run, upload ------------------------------------> PUT
swap the run for the file ----------> manifest CAS
```

## Push

1. Spool the request body to local disk, gunzipping if needed.
2. **Index** the pack. Pushes under 16 MiB stay in memory throughout.
   - Pass 1 is sequential. It finds entry boundaries and hashes full objects on
     the spot.
   - The node then loads the REF_DELTA bases that the pack names and that are in
     the current snapshot (those of a thin pack).
   - Pass 2 resolves delta chains **in parallel**.
3. **Validate** while indexing. Parse every object (fsck-lite) and check that
   every referenced object exists with the expected kind. Ref targets must
   exist, and branches must point at commits.
4. **Segments.** For each new commit, path-diff its tree against the trees of
   its parents. Prune identical subtrees. The changed objects are the *segment*
   of the commit. Each diff is split into frames, one tree level each, which run
   in parallel. A frame whose parent trees are not local waits for them (see
   [machines](storage.md#machines)) while the others go on. The output of a
   window is flattened in the depth-first order of each commit and merged in
   topological order. The first commit that introduces an object therefore
   assigns its base, exactly as a recursive pass would.
5. **Delta re-encoding.** Each changed object gets the version of the first
   parent at the same path as its delta base. The node loads bases in the
   snapshot before encoding starts. That parent is recorded as the **base
   context**. The chain depth is capped at 50 for blobs and 100 for trees. Every
   version of a path deltas against the previous one, so a chain ends in a whole
   copy. Trees have the higher cap, because whole copies of large, often-changed
   trees would be most of all tree bytes at depth 50. Encoding runs in parallel
   with work stealing, window by window, into the layer builder (which spills to
   disk). Encoded entries are never all in memory.
   - **No recompression of whole objects.** For an object that the pack sends
     whole and the node stores whole, the node keeps the zlib data of the
     pack. Deltas are compressed again, because they are small. An entry
     under 64 bytes goes in an uncompressed block, because deflate makes it
     larger.
6. Commit the **L0 layer** plus the ref compare-and-swaps as **one log entry**.
   The layer is inline (its bytes in the entry), or it is uploaded first if it
   is large ([Durability without waiting for
   S3](#durability-without-waiting-for-s3)).
7. Report status once the ledger answers. The node does not wait for its own
   view: every read takes a read barrier, so it sees the push through any node
   ([Read path](read-path.md)).

### Bounded memory

Ingest works in bounded memory. The per-object tables are temp tables of
fixed-size records: pack entries, object ids, kinds, references, commits, base
assignments, and the records of the layer builder. Each table moves into the
buffer pool as writable pages once it passes 16 MiB. The pool charges them to
its budget and writes them back to a scratch file when it evicts them. Lookups
by object id use sorted tables with a fan-out (like a pack `.idx`) instead of
hash maps. Sorting a large table is an external merge sort.

Accesses stay page-friendly. Base assignments are applied per window in index
order. The layer builder sorts its records rather than looking them up by
position. What remains outside the pool is the 128 MiB decoded-object cache plus
a few bytes per object and per commit.

### Invariant

**Every ref points into indexed storage.** There are no staged packs.

Object ids use the repository's format. SHA-1 ids hash with collision
detection, so content addressing stays sound against known SHA-1 collision
attacks: Enroute rejects objects that carry such an attack, as Git does. Pack
trailers are integrity checksums, not addresses, so they use plain SHA-1, or
SHA-256 in a SHA-256 repository. Every layer holds ids of one format, which
its footer names; the layout is the same for both, with 20- or 32-byte ids.

## Durability without waiting for S3

Objects are content-addressed. Layer files are not. The id of a layer is a name:
128 random bits, given when the file is built and never reused. The file's root
sits next to it in the manifest entry, and inline layers are checked against it.
The root is the SHA-256 of the file's index region and footer, which hold the
SHA-256 of each chunk before them. Every object-store key is therefore written by exactly one upload. A
layer that left the manifest can never come back under its name, so a pending
deletion can never remove a file that something references again. A
content-addressed id would let a retired layer's key be written and referenced
again while its deletion was pending.

Only refs and the manifest need consensus. Layer bytes only need to be durable
before anything refers to them. A layer is durable in one of two places, and
there is no third:

- **Inline, in the log** (layers up to `layers.inline_layer`, default 1 MiB).
  The layer file travels inside the log entry that commits it. The push costs
  one replicated, group-committed log write and no object store round trip.
  Every compute node that follows the repository writes the bytes to its local
  disk (checked against the id) and reads them like any other layer. This is the
  split of Neon: the log takes small writes, and everything at rest is in the
  object store.
- **In the object store** (larger layers, and every flush and compaction
  output). The upload finishes before the entry that names the layer is
  proposed.

**Local copies** are never the durable copy. The node writes them without fsync.
They are evictable, and the node deletes them at startup: local disk does
not outlive the process ([Local disk is a
cache](read-path.md#local-disk-is-a-cache)).
Inline layers have no local copy: the node serves them from their bytes in the
log. With a `file://` store on the same filesystem, a local copy is a hard link
to the stored file ([Linked local copies](storage.md#linked-local-copies)).

### Merge before upload

Inline layers are not uploaded one by one. The maintainer of the repository
([Maintainers](ledger-and-compute.md#maintainers)) takes each contiguous run of
inline layers when either condition holds:

- its oldest layer is `layers.flush_interval` old (default 30 s);
- the run reaches 32 MiB or `layers.flush_max_layers` (default 256).

It merges the run, uploads the result once, and swaps the run for it with a
single manifest compare-and-swap, like the flush of an LSM memtable. A run of
one layer is uploaded as is. The object store sees one PUT per run, not one per
push.

- Flushes never wait for compactions, because they replace disjoint layers.
- Up to 4 flushes run at once, on disjoint runs. A finished flush looks for the
  next at once. One flush at a time behind compactions would let the inline tail
  grow very long under heavy pushes into one repository.
- Longer runs are not cheaper overall. Every layer left inline makes snapshot
  builds and the snapshots of the ledger costlier.
- Every follower has the bytes, so a new maintainer simply continues. A flush
  that loses a race or crashes leaves only a file that nothing references, for
  the [layer sweep](maintenance.md#the-layer-sweep).

### Backpressure

A repository holds at most `layers.max_inline` (64 MiB) of inline layers. Beyond
that, the node uploads new layers first. This bounds the replicated state while
the object store is slow or down. Inline layers are part of that state
(snapshots), and their entries stay in the log until they are flushed.

### Deletes never race with references

Layer ids are never reused, so once a layer leaves the manifest nothing can
reference it again. Each id also ends in the upload generation in which it was
published. The state machine checks it, so a file that missed its window can
never be committed. Both are facts about the log. The [layer
sweep](maintenance.md#the-layer-sweep) relies on them to delete files, never on
a clock. A flush or compaction that loses its race leaves its output to the
sweep.

### Durability window

Until it is flushed, an inline layer is as durable as the log: it is on the
disks of a majority of nodes. This is the guarantee that Neon's safekeepers
give. If a majority of the replicas lose their logs, an operator restores the
ledger from volume snapshots ([Recovery](ledger-and-compute.md#recovery)).

## Server-side writes

`CreateCommit` builds blobs, trees (in canonical Git order) and the commit on
the server. Enroute ingests the new objects into a layer like those of a push,
and commits it the same way. The layer is inline in the log when it is small
(the usual case, no object store round trip). Otherwise the node uploads it
first.

## Pushes with hooks

A push calls `pre_receive` before any ref moves, and `post_receive` after. The
setting `hooks.pre_receive_mode` sets how many log entries the push costs.

**One entry (`commands`, the default).** `pre_receive` decides from the commands, not
from the objects.

1. **Ingest.** The node checks the pack and builds the layer on local disk.
   During the same pass, it computes `force` for each command
   ([Force](#force)).
2. **`pre_receive`.** The hook gets the commands. The pushed objects are not
   stored yet, so the hook cannot read them. The node fails any command
   without a judgement, and fails the whole push on an error. In an atomic
   push, one refusal turns every command into `atomic push failure`.
3. **Commit.** The node commits the layer and the allowed refs in one entry.
   If no command is allowed, it proposes nothing: the layer is not stored.
4. **`post_receive`.** Once the ledger answers, the node tells the hook what
   landed. A read that the hook makes then sees it, through any node. An error
   is only logged, because the refs have already moved.

This is cheap because the hook adds no log entry and no object store write: a
push costs what it costs with no hook.

**Two entries (`objects`).** For a hook that reads the pushed
objects through the API before it decides
([Serving](serving.md#push-hooks)).

1. **Objects.** The node commits the layer alone, in an entry with no ref
   updates. Once the ledger answers, the objects are readable by object id
   through the API on every compute node, and no ref has moved. The node
   commits no layer when every command has already failed its own checks.
2. **`pre_receive`.** The hook gets the commands, and reads the objects through
   the API. Judgements apply as in the default.
3. **Refs.** The node commits the allowed refs in a second entry, with the
   same incarnation check and the atomic flag.
4. **`post_receive`.** As in the default.

The split makes the objects visible to the hook without a node-local overlay,
at the cost of a second log entry per push. If the hook refuses a ref, its
objects stay in the manifest. Compaction keeps them, and nothing collects them
([Garbage collection](maintenance.md#garbage-collection)).

### Force

`force` is true when the old value is not an ancestor of the new value. In one
entry, the snapshot does not hold the pushed commits, so the node computes
`force` during ingest. It walks back from the new value through the parents of
the pushed commits, which ingest already keeps sorted by object id. Each path
stops at the old value or at a commit that the snapshot holds. From those
commits, the node walks the snapshot's commit graph, and stops below the
generation of the old value. The first part reads each pushed commit at most
once, from tables that spill to disk for a large push. Annotated tags are
peeled first. A new value that is not a commit, or an old value that the node
does not know, counts as a force.
