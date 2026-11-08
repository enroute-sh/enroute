# Storage

Enroute embeds a storage engine built like a modern database. A repository's
objects are immutable files, called layers, in an object store such as S3.
Its refs and its list of layers are in a replicated log on the ledger nodes.
There is no external database.

This page follows one push and one clone through that engine. For the design
in depth, see [Architecture](../internals/README.md).

## Nodes

An Enroute process is a node. A node runs one of three roles:

- **Ledger.** Holds the replicated logs on a durable local disk. Each
  repository's log is on three ledger nodes, its replicas. With fewer than
  three ledger nodes, it is on all of them.
- **Compute.** Does the Git work: receives pushes, serves clones and fetches,
  and answers the API. It keeps only caches that it can rebuild.
- **Both.** The default. One process runs the ledger and the compute.

A compute node finds the ledger nodes through the object store.

## Life of a push

A push of a few commits to `acme/widgets`:

1. **The compute node checks the pack.** It indexes the objects, checks that
   each one is well formed and that everything it names exists, and builds a
   new layer from them. Many pushes, to the same repository too, do this at
   the same time without locks.
2. **One log entry commits it.** The compute node proposes one entry to the
   repository's log: add this layer, and move these refs if they still hold
   the values that the client saw. A small layer, up to 1 MiB by default,
   travels inside the entry. A larger one is uploaded to the object store
   first.
3. **A majority syncs it to disk.** Two of the log's three replicas write the
   entry and sync their disks. Only then does Enroute answer the client. The
   sync time of the ledger disks is the commit time of a push.
4. **The layer is flushed later.** Within 30 seconds by default, a compute
   node copies small layers from the log to the object store, in one upload
   for many pushes. Until then, the log is their only copy.
5. **Layers are merged in the background.** Small layers are merged into
   larger ones, as an LSM tree does. A layer file that nothing names any more
   is removed after 6 hours, so that slow readers can finish.

Two pushes to different branches do not conflict. Two pushes that move the same
branch from the same commit race at step 2. The first one lands, and the
second is rejected, as in any Git server.

## Life of a clone

1. **The refs are current.** The compute node asks the ledger for the
   repository's latest state before it answers. The refs that a client sees
   include every push that finished before the request, through any compute
   node.
2. **The pack comes from a snapshot.** The compute node reads one immutable
   version of the repository from start to end. A push that lands during the
   clone does not change what the clone gets, and the clone does not block
   the push.
3. **Layers come from local disk.** The compute node keeps local copies of the
   layers that it reads, and fetches a layer from the object store only on a
   miss. A compute node drops its view of a repository that nobody has used
for 15 minutes (`node.evict_after`).

The API reads work the same way: every read sees every write that finished
before it began.

## What survives what

An acknowledged write is on the disks of a majority of its repository's
ledger replicas. What it survives depends on the deployment:

| Deployment | An acknowledged write survives | Writes continue while |
| --- | --- | --- |
| One node | A restart of the process or the machine | The node is up |
| One ledger node and compute nodes | The loss of any compute node | The ledger node is up |
| Three or more ledger nodes | The permanent loss of one of a repository's three ledger nodes | Two of each repository's three ledger nodes are up |

Losing more than that needs a restore from volume snapshots, which loses the
writes after the snapshots. See [Deployment profiles](../operate/deployment-profiles.md)
for each model in full, and [Maintenance](../operate/maintenance.md) for the
procedures.

## Deleting data

`DeleteRepository` frees the key at once, and Enroute reclaims the layers in
the background after the same 6-hour delay. Enroute does not garbage-collect
objects that a deleted branch leaves unreachable. To remove them, delete the
repository.

Enroute does not encrypt data at rest. Configure encryption in the object
store and on the ledger disks. See
[Storage backends](../operate/storage-backends.md).
