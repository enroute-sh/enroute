# Ledger and compute

Enroute splits a node into two roles: the ledger and the compute. This page
describes the roles, how they talk, and how the ledger keeps one owner for each
repository. Enroute does not place compute nodes or add them with load.

**Goal:** the compute scales with load, in both directions, without touching any
repository's log. At most one incarnation of a repository can acknowledge writes
at any time.

**The key idea.** The ledger holds the only state that needs consensus: refs,
the layer list, and which repositories exist. A compute node holds nothing that
it cannot rebuild from the ledger and the object store, so compute nodes can
be added, replaced, or lost at any time.

## Roles

Nodes run one binary with a role (`node.role`). A small deployment gives every
node both roles. The ledger keeps the record of what happened and in what order.
The compute (the usual word for Git hosting) does the Git work.

- **Ledger**: a small, stable set of nodes with durable disks, like TiKV's
  stores or Neon's safekeepers. Each repository's log has three of the members
  as its replicas. The members hold the logs and the state machines (refs and
  manifest), and little else.
- **Compute**: everything else, as many nodes as the load needs. A compute node
  that serves a repository **subscribes** to its log. A ledger node streams it
  the state of the repository, then every committed change, inline layers
  included, as Neon's pageservers stream WAL from its safekeepers. Compute nodes
  are not members of any log. They come and go without a single entry being
  written.

Compute nodes are subscribers, not replicas, for three reasons. A replica
join or leave would be a change in the log of every repository the node
serves. Replicas must persist entries before they acknowledge them, so every
compute node would need a durable log. The sequencer alone would track and feed
every replica. A subscription is a stream from any ledger node that hosts
the log (every replica applies committed entries). The fan-out spreads over the
ledger, and a compute node keeps no consensus state.

### What runs where

| | Ledger | Compute |
|---|---|---|
| Disk | the log store (entries, inline layer bytes until flushed: ≤ 64 MiB backlog per repository) and the state store (each repository's refs, manifest and counters) | local copies of layers, under the disk budget |
| CPU | applying entries (ref and manifest compare-and-swaps), replication | receiving and validating packs, building and uploading layers, serving fetches and clones, flushes and compaction |
| Memory | the state store's budget (block cache and memtables), and the terms of each log it hosts | views, the buffer pool |
| Network | inline bytes in from the compute and to the other ledger nodes, out to every subscriber | layers from and to the object store; pack data to and from clients |

The number of repositories, their refs (on disk) and the inline backlog set the
size of the ledger. Data does not. The state store's budget sets its memory,
which should hold the refs that are written. The fsync latency of the ledger is
the commit latency, so ledger nodes want fast disks.

## Protocol between nodes

Nodes talk gRPC on their own port. This is `node.internal_listen`, the address
that peers find among the members ([Finding the ledger](#finding-the-ledger)). It also serves `GET /status`. It is separate
from the smart HTTP port (`listen.git`) and the API port (`listen.api`, also
gRPC), so each can be exposed or firewalled on its own. Expose the internal port
only to cluster members.

Protobuf messages are the contract between the roles. They are the types of
the state machine too. Commands, repository state and outcomes are protobuf
messages in the log, in the stream to subscribers, in the RPCs and in the state
objects in the object store. Inline layer bytes travel as bytes, not base64.

Three services run between nodes:

- **The membership service**, which earns a node its token (see
  [Joining the ledger](#joining-the-ledger)).
- **The ledger service**, which the compute uses:
  - Activate makes the log of a repository live. It creates the log if asked,
    with a HEAD for an explicit create.
  - Delete, and describe (the HEAD and last push of a repository, after a read
    barrier).
  - List returns the repositories that a ledger node hosts, deleted ones
    included ([Lifecycle](lifecycle.md#listing)).
  - Propose sends a command and answers with its outcome once applied.
  - Read barrier waits for everything committed so far.
  - Watch follows many repositories on one stream, as etcd's Watch does. The
    compute subscribes and cancels by id, and each subscription streams the
    state of a repository, then every change to it: refs set or deleted, HEAD,
    a layer edit, the version, the end of the incarnation.

  Subscribers patch their state with the changes. They need neither the
  commands, nor log indices, nor the rules of the state machine
  (compare-and-swaps, rejections). These stay with the ledger. A node that runs
  both roles calls its own ledger in-process through the same interface. A
  compute keeps one connection per core to each ledger node, because one task
  drives one HTTP/2 connection, and every call to that node would wait on it.
  Each connection carries one Watch stream for all the repositories followed
  on it. A stream per repository would hit the HTTP/2 limit of concurrent
  streams per connection (200 by default) at a few thousand repositories. A
  ledger node that stops ends its Watch streams, so its computes subscribe
  on the other nodes.
  Messages can be as large as a state with its inline layers, in both
  directions. A subscription that breaks is followed again from a new state. A
  compute that falls 256 updates behind keeps its subscription: the ledger merges
  the changes it misses (each ref's latest value, and one layer edit from the
  layers the compute holds to the current ones) and, once the compute has
  drained half its queue, sends them as one change. Only a deletion in the gap
  makes it send the whole state instead.
- **The `Peer` service**, between ledger nodes: batches of frames for many logs
  per request (the root log's Raft messages as opaque bytes, the repository
  logs' appends, acknowledgements, seals and checks) and liveness pings
  ([Consensus](consensus.md#batched-transport)).

## Joining the ledger

Every call on a ledger node's internal port needs a token, an
`authorization: Bearer` header, except the calls that earn one and the
`Cluster` service's `Join` and `Members` (see [Membership](#membership)). A node earns a
token by joining (the `Membership` service), which shows that it can read the
object store and holds an Ed25519 key. The steps copy ACME's key authorization
(RFC 8555):

1. The node generates a key when it starts, and asks a live ledger node for a
   challenge for it.
2. The ledger node writes a single-use nonce to
   `data/challenges/<random id>` in the object store, and answers with the id
   and the path.
3. The node reads the nonce, and signs `hex(nonce) "." thumbprint` with
   its key, where the thumbprint is the SHA-256 of the public key in hex.
4. The ledger node checks the signature against the key the challenge was
   for, deletes the nonce, and answers with a token.

A wrong answer spends the challenge too. Each challenge lasts 60 s, and a node
holds at most 4096 unanswered ones.

The token is a JWT signed with `EdDSA`. It names the node (`sub`, the key's
thumbprint) and the ledger node that signed it (`kid` and `iss`), and lasts an
hour. Each ledger node signs with a key of its own, kept in its data directory
(`token-key`). The public half is in the root log's members, so every ledger node
can check every token without a shared secret. A ledger node checks a token
once and caches it until it expires, so a call costs a lookup.

A node joins again when a third of its token's life is left. Taking away a
node's read access to the object store therefore takes it out of the ledger
within an hour. A `Watch` stream outlives its token: the compute sends each
renewed token on it, and the ledger ends a stream whose token expired without
one.

Ledger nodes sign tokens for themselves for their own calls to each other.
`Peer` calls and `Cluster.Remove` need such a token, so a node that joined
reaches the Ledger service, never `Peer`. A node running both roles calls its own ledger
in-process, without tokens.

## What the compute does with it

- **Pushes** are received, validated and turned into a layer on a compute node.
  The connectivity check needs the view of the compute node, which follows the
  log. The node takes a read barrier for the ref advertisement and proposes the
  commit entry ([Write path](write-path.md)).
- **Maintenance** (flushes and compaction) runs on one compute node per
  repository ([Maintenance](maintenance.md)).
- **Checkpoints** need only the applied state. The state compare-and-swap
  protects them. They stay on the sequencer, which is a ledger node. There is no
  log upload.

### Maintainers

Maintenance is safe from any node, because each swap is a manifest
compare-and-swap in the log. Two maintainers at once only waste a merge. So the
maintainer needs no election or lease. It is the first live compute node in the
rendezvous order of the repository, as for ledger nodes. The live compute nodes
are those that called `Pending` in the last 10 s: each ledger node keeps them
in memory, and answers each call with those it heard from, which the compute
merges. The id is in `compute-id` in the data directory, so a restarted node
gets its repositories back. When a compute node stops, its repositories move to
the next node in their order after 10 s.

The sequencer of each log tracks which repositories may need work: those whose
layers changed, with the version it saw then. It knows nothing of the merge
policy, which is the compute's alone, so either side can change without the
other. Every 2 s, each compute node asks every member (`Pending`). It gets back
the repositories that it maintains and that are not settled. It follows those it does not follow yet, and keeps
following them until their work is done. When the compute finds nothing to do
on a repository from a state at least as new as that version, it tells the
ledger (in `settled` of its next `Pending`). The repository stays settled
until its layers change. So a repository is maintained even if no node serves
its requests, for example after the node that took the pushes stopped.

The maintenance tick (every 100 ms) looks only at repositories in that answer
whose state changed, whose oldest inline layer is due to flush, or whose job
just ended. An idle node does no work per followed repository, and a busy one
does none per pending repository that is waiting. A repository with
new inline layers starts its flush age when the next answer lists it, at most
2 s later.

## Finding the ledger

The members are in the root log, each with its id, its `node.advertise_url`,
its `node.zone`, and the public key that checks its tokens
([Membership](#membership)). The
object store only lets nodes find them, under `cluster/`, which only ledger
nodes write:

- `cluster/members` names the members, with their URLs and keys. The first
  node of a cluster creates it ([Starting a ledger](#starting-a-ledger)), and
  the root log's leader rewrites it when the members change. It is never
  rewritten otherwise, so nothing in the store is written while nothing
  changes.
- `cluster/nodes/<id>` is a ledger node's own announcement, with its URL and
  key, written once each time it starts. It is what a join asks for: only a
  ledger node could have written it.

Node ids are random store ids, so they cannot be configured. A ledger node's
transport resolves an id to a URL through the root log's members. A node that
is joining does not have the root log yet, so it takes the members' URLs and
keys from `cluster/members` until it does.

The compute is a client of the whole ledger, not of one address: it reads
`cluster/members` when it starts and every 10 s after, and calls the members
directly, as a Kafka client fetches the brokers' metadata. A node that runs
both roles takes the live members from its own root log instead. Any ledger
node that hosts a repository's log answers its requests, except proposals,
deletions, read barriers, activations and descriptions, which only the log's
sequencer takes. A compute follows each repository on its first live ledger
node in the repository's rank, and the subscription names the sequencer, so the
compute sends proposals straight there (the sequencer is named by the URL it
advertises, so the compute needs no ids); a node that refuses one names the
sequencer too (see [consensus](consensus.md#proposals)). Other requests go to
the members in turn. `List` and `Pending` ask every member and merge the
answers, since a node may lag a create or a deletion and only a log's sequencer
tracks its maintenance. A compute moves on while a node is unreachable or does
not host the log, and tries a node it could not reach last for the next 2 s. A
node that runs both roles asks its own ledger first.

Safety never depends on `cluster/`. The members change only through the root
log, and the replicas and sequencer of a log only through the root log. If the
object store is unreachable, running incarnations are unaffected, because
addresses already learned are kept, and so are creates and deletions.

Liveness is the ledger's own. Ledger nodes ping each other
([Failover](consensus.md#failover)), and a ledger node notes each compute that
calls `Pending` (every 2 s) in memory; a compute not heard from for 10 s is not
alive ([Maintainers](#maintainers)).

## Membership

The members are part of the root log's state, with the nodes removed from it,
as etcd keeps its member list in its own log. Each repository's log has three
members as its replicas (all of them, if fewer), placed when the root applies
its creation: the node that creates it, which sequences it, and the live
members holding the fewest logs.

- **Adding a node** is starting it. It announces itself in
  `cluster/nodes/<id>` and asks a member it finds in `cluster/members` to let
  it join (`Cluster.Join`, with its id). The member reads the announcement and
  proposes the node, with its URL and key, to the root log. A join is refused
  for a removed node, and for an id whose member answers pings at another URL:
  two copies of one log store must never run at once. A node that moved (the
  same disk at a new address) joins again with its new URL once the old one
  stops answering. A new member hosts the root log from the start, empty, and
  the root's leader catches it up with a snapshot. New repositories are placed
  on it, and logs with fewer than three replicas grow onto it.
- **Removing a node** is a decision of the operator (`enroute members --remove
  <id>`), because a node that is down looks the same as a node that is gone.
  The command runs on a ledger node: it signs a token with that node's key
  (`token-key` in its data directory), and asks a member to remove the node
  (`Cluster.Remove`, which takes only a ledger node's own token). The member
  proposes the removal to the root log. It is refused if the members left would
  not have a live majority, unless forced. Removal is for good. The removed id
  never rejoins, so a node that lost its disk, or one being retired, comes back
  only as a new node with a wiped data directory. A removed node that is still
  running keeps its logs until they move off it: it stays a learner of the
  root log, so it sees each move, and its peers take its messages until it
  holds no log. Only then is it out of the root, and a peer refuses its
  messages as a removed node's (as in etcd); it then drops whatever it still
  hosts, as when it was down while its logs moved. A removed root leader hands
  the root's leadership to another member first.
- **Each log follows the members** through the controller on the root's leader
  ([Replica changes](consensus.md#replica-changes)). A removed replica is
  replaced by a live member, one change per log at a time, idle logs too, and
  the new replica is caught up by the log's sequencer.

To replace a node that lost its disk, start the new one, then remove the old
one: its logs move to the others. The compute never touches the replicas of a
log.

### Starting a ledger

The first node of a new cluster starts the root log, with itself as its only
member. It is the node that creates `cluster/members`, a conditional create
that names it; every other node finds the object there, and joins once the
root's leader has written the members into it. A standalone node starts its own
root log.

A standalone node becomes the first member of a cluster when it restarts on its
data directory with `node.advertise_url`. Its id is its store id, in a
cluster or not, and it is already the only member of its root log and the
only replica of every log. It creates `cluster/members` if it is missing, so no
other node starts a root. The restart changes no replicas or terms of any log.
Every entry it committed alone is on its disk. Logs grow onto ledger nodes that
start later, one change at a time.

Two limits apply. The bucket must hold no other ledger (and a bucket reused for
a new ledger needs its `cluster/` prefix emptied first). The move is one way:
restarted without `node.advertise_url`, a node whose logs have other replicas
cannot reach them, and it commits nothing.

The ledger scales out: each member holds the logs it is a replica of, so more
members hold more repositories.

## One owner at a time

Within an incarnation, one sequencer at a time can commit: an appointment seals
a majority of the replicas at a higher term first, and every commit needs a
majority at its own term ([Why an appointed
sequencer](consensus.md#why-an-appointed-sequencer)). Between incarnations:

- **Incarnations start** as the root log applies their creation, and only when
  the directory has no live incarnation of the repository. Of concurrent creates
  exactly one starts an incarnation; the others change nothing.
- **Incarnations end only by deletion**, through their own log (a tombstone),
  then through the root log, never by timeouts. A replica that missed the end
  can never write again, because any later sequencer has the tombstone
  ([Lifecycle](lifecycle.md)), and it drops the log as its root catches up. The
  next incarnation starts only after the root ended the old one.
- **An incarnation that did not end** is never taken over by another: its
  replicas resume it under a new sequencer, and a minority of them may be down.
- **Losing a majority of a log's replicas** is a disaster, as in any
  replicated log ([Recovery](#recovery)).
- **Freezing the ledger** is stopping its nodes and moving their disks (see
  below).

Other systems end old logs the same way. Logs replaced by new ones through a
metadata store: BookKeeper ledgers (Pulsar), the virtual log of Delos, the log
generations of FoundationDB, and in theory Vertical Paxos. All of these end the
old log through a quorum of its own members (fence, seal, lock), never by
timeouts.

## Freezing the ledger

Every repository stays on its replicas while it exists. A log that nobody
writes sends nothing ([Quiet logs](consensus.md#quiet-logs)).

Enroute does not move idle logs to the object store. Only a majority of the
replicas sees a log end, so a replica that fell behind could keep the ended
incarnation and serve it as live, and the resident cost it saves is small.

**Freezing a ledger**, to move it to other machines or to stop it for a while,
is done with nodes, not repositories:

1. Stop the ledger nodes.
2. Copy the data directory of each one (its log store and state store, together:
   a volume snapshot, or an archive in the object store).
3. Later, start a node on each copy.

The identity of a node is its log store. A copy started at a new address joins
again with it ([Membership](#membership)), so the copies are the same nodes at
new addresses. Every repository resumes on the same replicas with its state
intact.

Two rules apply. A store must never run twice, because two copies could answer
twice in one seal or commit. A join is refused while the member with that id
answers pings at its other address. This is a check that catches mistakes. It
is not a guarantee. A node must also never be restored from
an older copy than it last ran with. It would forget fences and acknowledgements,
and its manifests could name layers that the [layer
sweep](maintenance.md#the-layer-sweep) has since deleted. Compute nodes hold
nothing that needs this. They leave and come back whenever they like.

### Layout

The object store splits by who writes. The ledger writes under `cluster/`, and
compute nodes write only under `data/`, where the ledger also leaves trash
markers and join challenges for them. A bucket policy can then keep compute
nodes from writing the ledger's objects, which they only read (see
[Security](../operate/security.md#object-store-permissions-by-role)).

```text
cluster/format                  the release that created the bucket (see Upgrades
                                and migrations)
cluster/members                 the members' addresses and token keys, and the node
                                that started the root log
cluster/nodes/<id>              a ledger node's address and token key, as it started
data/repos/<repo>/layers/       the layer files
data/repos/<repo>/sweep         the layer sweep's record (see Maintenance)
data/trash/<repo>/<E>           incarnation E was deleted; its layer files may remain (see
                                Lifecycle)
data/challenges/<id>            a nonce a joining node reads (see Joining the ledger)
```

Which repositories exist, and the state of each one, is the ledger's alone (the
root log, and each repository's log): nothing in the object store says so.

### Activation

A write, or a linearizable read, activates a repository that a node does not
host. The node takes a read barrier on the root log, so its directory has every
create and deletion committed before the request. Then:

- **No live incarnation, and the request creates it:** the node proposes the
  repository's next incarnation to the root log (a follower forwards it to the
  root's leader), with itself as the sequencer and two more live members as
  replicas. As each replica applies the entry, it hosts the incarnation, from
  the initial state the directory implies: empty, past the versions and upload
  generations of the incarnation before it.
- **A live incarnation that this node is a replica of:** it hosts it, and the
  sequencer catches it up.
- **A live incarnation on other nodes, or this node is not a member:** the node
  refuses, naming the sequencer, and the compute asks there.

A node that hosts the live incarnation skips the barrier. One that missed its
end keeps it only until its root catches up, and could serve nothing from it
meanwhile: the others dropped the log, so it has no majority for a read or a
write.

### The root log

One more log, a Raft group hosted by every node, holds the directory: which
repositories exist, in which incarnation (with the initial state each started
from, and its replicas, sequencer and term), and the members. Its entries are
creates, deletions, member changes, appointments and activations. Each changes nothing if it no longer applies, so a proposer that timed
out proposes it again. The engine applies them itself, writing the directory to
the log store with the round that applies them: one key per repository, and one
for the rest. Its snapshots carry the whole directory. It starts at index 1, so
a node that joins it is always caught up by a snapshot, never by entries that
assume a membership it does not know. It plays the part of CockroachDB's meta
ranges, small: a repository costs one key in it, and only creates, deletions and
failovers write to it.

### Recovery

If a majority of a log's replicas lose their disks, its acknowledged writes may
be gone. The protocol cannot recover from that. The ledger nodes' data directories, backed
up as volume snapshots, are what an operator restores from (all of them, from
one point in time, as when [freezing the ledger](#freezing-the-ledger)). The
[layer sweep](maintenance.md#the-layer-sweep) keeps unnamed files for
`layers.sweep_retention`, so restored states find their layers if the snapshots
are no older than that. Forcing a log with no majority to go on from the replicas
left, as CockroachDB's and TiKV's loss-of-quorum recovery does, is not built.

### Costs and limits

- Creating or deleting a repository is a root log entry: one more commit, with
  a majority of the ledger up. Neither needs the object store (but a deletion
  writes a trash marker).
- Each repository is on three ledger nodes. Adding ledger nodes adds capacity:
  new repositories go to the nodes holding the fewest. Existing logs move only
  when a replica is removed.
- A deleted repository costs one key in the root log's directory, for good: its
  next incarnation starts past it.
