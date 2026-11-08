# Deployment profiles

Enroute supports three deployment models. Each model uses the same binary and
the same `bucket`. Only `[node]` changes. You can move a live deployment from
one model to the next without data loss.

| Model | Nodes | Survives the loss of |
| --- | --- | --- |
| [Single node](#single-node) | One `both` node | Nothing. Writes and reads stop |
| [One ledger, scale-out compute nodes](#one-ledger-scale-out-compute-nodes) | One `ledger` node, and N `compute` nodes | A compute node |
| [High availability](#high-availability) | Three or more `ledger` nodes, and N `compute` nodes. Or three `both` nodes | One ledger node at a time, and any compute node |

For the roles, see [Hosting](hosting.md#node-roles). For each key, see
[Configuration keys](../reference/configuration-keys.md#node).

## What every model shares

- **An acknowledged write is on the ledger disk.** Enroute acknowledges a push
  or an API commit after the ledger writes it to the log and syncs the disk.
  In the replicated models, a majority of the repository's replicas do this.
- **Large layers go to the bucket** before Enroute acknowledges the write. Small
  ones follow in the background.
- **Volume snapshots are the last resort.** A ledger node that restarts keeps
  every acknowledged write on its disk. Only when the single ledger node, or
  two of a repository's three ledger nodes, lose their disks do you restore
  from volume snapshots, and lose the writes after them. See
  [Maintenance](maintenance.md#recover-from-losing-a-majority).
- **Compute disks hold only caches.** A compute node that loses its disk rebuilds
  its caches from the ledger and `bucket`.
- **The ledger scales out.** Each repository is on three ledger nodes, which
  apply its writes. More ledger nodes hold more repositories. The sync time of the disk is the commit time, so give ledger nodes
  fast disks with low sync latency, such as local NVMe or provisioned-IOPS
  volumes.
- **Compute nodes scale out.** Compute nodes do the CPU work: they receive and
  check pushes, build layers, and serve fetches and clones. Add compute nodes for
  more Git and API traffic.

The examples use an S3 bucket. A single node can also use a `file://`
bucket. All nodes of a deployment use the same `bucket`, and each `bucket`
holds one deployment.

## Single node

One node runs both roles. `enroute dev` runs this model.

**Use it when** you develop, test, or run a small deployment that can accept
downtime.

```toml
[bucket]
uri = "s3://enroute-objects/prod"

[node]
data_dir = "/var/lib/enroute"
```

The node needs no `advertise_url`. Without it, the node is the only replica of
its logs, and it is in no cluster.

On a machine with no object store, such as a VPS, use a
[local directory](storage-backends.md#local-directory) as the bucket:

```toml
[bucket]
uri = "file:///var/lib/enroute/objects"

[node]
data_dir = "/var/lib/enroute/data"
```

**Durability.** An acknowledged write is on the node disk. If the process
stops or the machine restarts, the node starts again on the same disk with
every acknowledged write. If the disk is lost, you restore it from a backup
or volume snapshot, and lose the writes after it. With a `file://` bucket on
the same disk, the layer files are lost too, so back up the whole disk.

**Availability.** While the node is down, Git and the API are not available.

**Scaling.** Give the node more CPU, memory, and a faster disk.

**Disk.** The node keeps local copies of layers under `data_dir`, up to
`node.disk_cache` (by default, no limit). With a `file://` bucket on the same
filesystem as `data_dir`, as in `enroute dev`, a local copy is a hard link to
the file in the bucket. The layers then use disk space one time, not two.

**Move to the next model.** See
[From a single node](#from-a-single-node-to-one-ledger). The next models put
nodes on more than one machine, so they need an object store as the bucket. To
move from a `file://` bucket to an object store,
[migrate](upgrades.md#migrate-to-a-new-deployment) the repositories to a new
deployment.

## One ledger, scale-out compute nodes

One ledger node keeps the logs on a durable disk. Compute nodes serve Git and the
API behind a load balancer.

**Use it when** Git or API traffic needs more CPU than one node has, and you
can accept a write outage while the ledger node restarts.

The ledger node:

```toml
[bucket]
uri = "s3://enroute-objects/prod"

[node]
role = "ledger"
data_dir = "/var/lib/enroute"
internal_listen = "0.0.0.0:7400"
advertise_url = "http://ledger-0.internal:7400"
```

Each compute node:

```toml
[bucket]
uri = "s3://enroute-objects/prod"

[node]
role = "compute"
data_dir = "/var/lib/enroute"
```

Put the `[hooks]`, `[listen]`, and `[git]` tables of your application on each
compute node. A compute node finds the ledger node in the bucket. Other nodes
never connect to a compute node, so a compute node needs no `advertise_url`.

**Durability.** An acknowledged write is on the ledger node disk. If the
ledger disk is lost, you restore it from a volume snapshot, and lose the writes
after it.

**Availability.**

- If a compute node stops, the load balancer sends its traffic to the other compute
  nodes. Nothing is lost.
- If the ledger node stops, pushes, API writes, and repository creation stop
  until it starts again. Fetches of a repository that a compute node already
  follows continue for a few seconds, from the state that the compute node has.
  Then the compute node stops following the repository. Fetches fail with
  `no ledger node is alive` until the ledger node is back.

**Scaling.** Add compute nodes for more traffic. Give the ledger node more CPU,
memory, and a faster disk.

**Move to the next model.** See
[From one ledger](#from-one-ledger-to-a-replicated-ledger).

## High availability

Each repository's log has three replicas among the ledger nodes (all of
them with three ledger nodes). A write commits when a majority of the log's
replicas has it on disk.

**Use it when** writes must continue when one ledger node, or its disk, is
lost.

Each ledger node has its own `advertise_url`, and the `zone` it runs in:

```toml
[bucket]
uri = "s3://enroute-objects/prod"

[node]
role = "ledger"
data_dir = "/var/lib/enroute"
internal_listen = "0.0.0.0:7400"
advertise_url = "http://ledger-1.internal:7400"
zone = "eu-central-1a"
```

Compute nodes use the same configuration as in the previous model.

For a smaller deployment, run three `both` nodes behind the load balancer.
Each one uses the ledger node configuration without `role`, with your
application tables added.

**Durability.** An acknowledged write is on the disks of at least two of the
repository's three ledger nodes, so it survives the permanent loss of any one
ledger node. After you remove a lost node, each of its logs gets a new replica
and tolerates another loss. See
[Maintenance](maintenance.md#recover-from-losing-a-majority) for the case of
two lost disks at once.

**Availability.** Writes to every repository continue while one ledger node is
down. Each repository's log is on three ledger nodes, and a write needs two of
them. If a second node goes down before the first comes back or is removed,
the repositories whose logs are on both nodes stop taking writes until one of
the two returns. Other repositories continue.

Creating and deleting repositories, failing a log over to another node, and
moving a log to new nodes need a majority of all ledger nodes, because the
ledger's directory is on every ledger node. With five ledger nodes, these
continue while two are down. An even number of ledger nodes adds no fault
tolerance to the directory: four nodes also continue with only one down.

A node that stops and starts again on its disk catches up by itself. A node
that loses its disk comes back as a new node, and the old one counts against
the majority of its logs until you remove it. See
[Replace a node that lost its disk](maintenance.md#replace-a-node-that-lost-its-disk).

**Scaling.** Add compute nodes for more traffic, and ledger nodes for more
repositories. Give ledger nodes fast disks with low sync latency.

### Spread ledger nodes over zones

Run the ledger nodes in at least three failure domains, such as availability
zones, and set `node.zone` on each ledger node. Each repository's log then
puts its three replicas in three different zones, so a write needs nodes in
two zones, and the loss of one zone stops no repository. With five ledger
nodes in three zones, the directory also continues without one zone, because
no zone holds a majority of the nodes.

Enroute places a new log in distinct zones first, and then on the nodes that
hold the fewest logs. If a log was placed while a zone was down, so that two
of its replicas share a zone, the ledger moves one of them when a node in a
missing zone is up, one log at a time. Without `node.zone`, or with fewer
zones than replicas, placement goes by load alone.

## Move between models

These procedures keep every repository, ref, and object, and every
acknowledged write. The test suite runs each of them.

### From a single node to one ledger

1. Stop the node.
2. Change its `[node]` table to the ledger node configuration: add
   `role = "ledger"`, `internal_listen`, and `advertise_url`. Keep `data_dir`
   and `bucket`.
3. Start the node. It registers in `bucket` as the only ledger member, and it
   stays the only replica of every repository's log. `enroute members` lists its
   ID.
4. Start the compute nodes with the same `bucket`, and send Git and API traffic
   to them.

Git and the API are not available from step 1 until the compute nodes run. Then
every repository, ref, and object is available through the compute nodes. Pushes,
API commits, and new repositories work.

You cannot move back. After the node has an `advertise_url`, do not remove it.

### From one ledger to a replicated ledger

1. Start two new ledger nodes with the same `bucket`, each with its own
   `advertise_url` and an empty `data_dir`.
2. Each new node adds itself to the ledger members. `enroute members` lists
   three IDs.
3. Each repository's log grows onto the new nodes, one at a time: its
   sequencer catches the new replica up, and the change ends once the new
   replicas hold its new term. Logs that nobody writes change too.

The compute nodes need no change.

Wait until the move is complete before you rely on it. Enroute has no command
that shows the replicas of each log yet. To see progress, compare
`consensus.repos` in `GET /status` of each new node with the count on the
first node, and check that `consensus.engine.lagging_logs` is 0.

After the move, the first ledger node is an ordinary member. Writes continue
if it stops.

To get five ledger nodes, start four new nodes in place of two.
