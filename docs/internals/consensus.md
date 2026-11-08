# Consensus

Each active repository has its own replicated log. This page describes the log,
its storage, and the engine that drives all logs on a node. For the roles that
host the logs, see [Ledger and compute](ledger-and-compute.md).

**The key idea.** A repository's refs and layer list are a small state machine,
so each repository gets its own log instead of sharing one. A root Raft log
appoints one sequencer for each log, and the log's entries commit on a majority
of its three replicas. All logs on a node share one group-committed log store,
so a thousand busy repositories cost about one disk sync each round, and an
idle log costs nothing.

## One log per repository

Every repository has its own replicated log. The state machine of the log is the
refs and the layer manifest of the repository. Three members of the ledger (all
of them, if fewer) are its *replicas*, and one of them is its *sequencer*: the
only node that appends to it. One more log, the root log, hosted by every node,
records which repositories exist, the replicas and sequencer of each, and who
the members are ([The root log](ledger-and-compute.md#the-root-log)).

A log is an *incarnation* of its repository. Its log id comes from the
repository and an epoch. Incarnation 1 starts at creation. The next one starts
when the repository is created again after a deletion
([Lifecycle](lifecycle.md)). Both start as the root log applies the create. A
node keeps the identity of a log with its state, so a recovering node knows
which log is which.

One log for each repository has these properties:

1. **One physical log per node, many logical logs.** All logs on a node share
   one log store. One group-committed fsync covers every repository written in a
   batch. Without this, many small logs would destroy group commit, and
   throughput rests on group commit.
2. **Zero idle cost.** A log has no timers, heartbeats, elections or leases. A
   repository that nobody writes sends nothing. Failure detection is per node,
   not per log.
3. **A standing sequencer.** A write is one round trip from the sequencer to a
   majority of the replicas. When the sequencer fails, the root's leader appoints
   another ([Failover](#failover)).
4. **Cheap per-repository state.** The state machine lives in the state store on
   disk, so a node can host millions of mostly idle repositories.
5. **Linearizable reads without a log write**, through a check with a majority
   of the replicas, per repository, on demand.
6. **No coupling between repositories.** A hot repository does not slow others,
   because it shares no log with them.
7. **Scale-out.** A node holds only the logs it is a replica of, so adding nodes
   adds capacity.

Replication and flushing are node-level. A node does one log store write per
fsync. It sends one RPC per peer, which carries entries and acknowledgements for
many repositories, as TiKV batches its Raft messages.

Ref updates to *different* refs never contend. They share only log throughput,
which group commit amortizes. Updates to the *same* ref serialize, as Git
semantics require.

### Why an appointed sequencer

A repository's log replicates as Raft does, but its leader is appointed, not
elected. The root log is a Raft group. Its leader runs a *controller* that
appoints each log's sequencer, as Kafka's controller appoints partition leaders,
and as Vertical Paxos and PacificA reconfigure through a master. Raft's election
machinery per log (timers, votes, pre-vote, hibernation, leader balancing,
membership changes one log at a time) is what made many logs expensive, and the
root already decides everything else about a log.

The protocol is the ledger's `consensus::shardlog` module. It is checked with Stateright over
every interleaving of sequencers, appointments, reads, trims and faults within
small bounds, and with random walks within larger ones, which the weekly soak
workflow runs from new seeds each time. Most rules have a planted bug, planted from the model,
that the checker must catch, and the walks must find each within minutes:

- **Appends** carry the sequencer's term, the position and term they follow
  (Raft's log matching), and the commit point. A replica takes an append unless
  its fence is higher, and truncates a mismatching suffix, never a committed
  position.
- **Commit.** The commit point moves to the highest position a majority holds,
  but only for a position of the sequencer's own term. A new term starts with a
  no-op, which commits everything before it. This is Raft's rule against its
  Figure 8.
- **Failover.** The controller *seals* the log at term + 1 on its replicas. A
  seal raises the replica's fence and answers where its log ends. Once a
  majority answered, the controller appoints one of the most up to date of
  those answers, with a compare-and-swap on the root (`Appointment`). A seal
  majority meets every commit majority, so the new sequencer holds every
  committed entry.
- **Reads** take the sequencer's commit point, then check with a majority that
  its term is still open, once its no-op has committed.

The root log stays Raft (raft-rs), because it is the one log that must choose
its own leader.

## Commands

- **Commit.** One entry atomically adds layers, removes layers, and applies ref
  compare-and-swaps, with an optional atomic flag. Removing layers makes it a
  manifest compare-and-swap, which flushes and compaction use. A Git push stamps
  its time, which becomes the last push time if a ref moves. This is the
  proposer's clock, so applying stays deterministic, and the time never goes
  back. Every commit with ref updates stamps the update time of each ref it
  sets ([Ref update times](lifecycle.md#ref-update-times)).
- **Advance the upload generation.** The layer sweep's next upload generation
  ([Maintenance](maintenance.md#the-layer-sweep)).
- **Delete.** The tombstone ([Lifecycle](lifecycle.md)).

Every command may name the incarnation it was prepared against. The state
machine refuses a command for another incarnation.

Versions increase across incarnations. A change sets the version to its log
index + 1, or to one more than the previous version if that is larger. A new
incarnation's log starts again from 1.

## The engine

The root log runs raft-rs, in the etcd/raft design. The repository logs run the
`shardlog` protocol, through the same code the model checks: each hosted log is
a `shardlog::Node`, which hides the replica, the sequencer and the read rounds,
and each failover is a `shardlog::Reconfigure`. The engine feeds them messages,
proposals and timer fires, carries out the effects (sends, commits, reads that
ended), keeps their store over the log store, and syncs it; answers that need
their writes on disk go only through `Node::synced`. It never reads or writes a
fence, a commit point or a replica's progress itself. Enroute owns the storage,
transport and scheduling of both.

### Log store

Each node has one log store, built for many logs, as TiKV's is.

- Each log is a group under its log id. The group holds its entries, its hard
  state, and where it starts. The hard state and the start are key-value
  pairs. A repository log's entries, hard state (its replica's fence and
  commit point) and start are the ledger's own records (`proto/log.proto`).
  The root log's are raft-rs's, with the metadata of its latest snapshot as
  its start.
- The engine keeps the terms, the hard state and that metadata in memory.
  Entries are read from the store, or from a cache of recent ones.
- Values are protobuf messages.
- The store group-commits concurrent writes. On macOS its syncs flush the drive
  cache with `F_FULLFSYNC`, like the other durable writes of Enroute. It recovers after a crash and purges old files in
  the background.
- A log that holds old files back is compacted on request, as far as its durably
  applied state and pinned inline layers allow. A log that is no longer hosted
  is dropped.

### Event loop

One event loop drives all logs. Each round it:

1. takes the pending inputs (proposals, batched frames from peers, read
   barriers, ticks);
2. collects the work of every touched log: the root's `Ready`, and each
   repository log's accepted entries and hard state;
3. sends what does not need durability (a sequencer's appends go out before its
   own fsync, as Raft allows), and hands the apply thread entries that are
   already committed and written here: the round's write holds at most their
   commit point, which a restart recovers from the applied state;
4. persists the round with one log store write;
5. then acknowledges the appends it wrote, counts its own writes toward each
   commit point, and hands committed entries to the apply thread.

Inputs that arrive during the fsync go into the next round. The loop does not
overlap the fsync with the next round. An entry goes to the apply thread only
once it is committed and written on this node.

### State store and apply thread

Every log's state machine is in the node's *state store*. It is one LSM
tree next to the log store, as
TiKV keeps the data of its regions. It has no write-ahead log: the log
already holds every applied entry, so recovery replays what the tree did not
flush.

- **Keys.** Each ref, the HEAD, each layer of the manifest and the counters are
  keys under the log. A layer's key is its first id, which is unique because
  every layer has at least one object id. A root key holds the identity of the
  log, its applied index and its version.
- **Batches.** The apply thread applies committed entries in atomic batches. A
  batch also records the entries as applied, unsynced. A command reads only the
  refs it compare-and-swaps, and the manifest for replacements. Its cost does
  not grow with the repository, and nothing of a repository's state is resident.
- **Apply thread.** Applying runs on a thread of its own, as TiKV splits its
  raft and apply pools. The loop hands it each log's committed entries in order.
  It also hands over snapshots, new logs' states, deletions and new subscribers.
  The loop then goes on with consensus. The apply thread evaluates proposals
  ([Proposals](#proposals)), answers the proposers of the entries and notifies
  subscribers.
- **Run of entries.** Each run is one write to the store. Before the write, the
  thread reads every ref and counter that the run touches in one go, from
  several threads. Entries apply against what was read, not against one disk
  read at a time.
- **Sync.** The writes collect in the WAL buffer of the store, which is written
  out and synced every 100 ms. After each sync the thread reports what is
  durable. Log truncation goes by this report only. The store does not write
  each batch out to the OS on its own, because on macOS a write waits behind the
  full fsync of the log store on another thread.
- **Snapshots.** Installs are made from the store when a replica needs
  one. Whole states (a new subscriber's) are read from it.
- **Versions.** The apply thread publishes each repository's version once per
  batch. It answers proposers only after it publishes.

Three rules keep the two stores in step:

- A log is truncated only up to an entry that the state store has durably
  applied. It is not truncated past an inline layer that is not flushed at that
  point. The apply thread reports both after a sync.
- A state installed by a sequencer, or the state of a new log, is durable in the state
  store before the log is reset to it. The loop waits for the apply thread
  before that write.
- A restart replays each log's committed entries after the applied index of its
  state.

### Inline layers stay in the log

An inline layer's bytes travel in the log entry of its push. The state store
only *pins* that entry. The log is not truncated past the oldest pinned entry
until the layer is flushed. Copying the bytes into the state store would make
them most of its writes and make it stall, so the bytes are written once.

While the object store is down, pins hold logs back. The inline cap bounds this.
The log store then rewrites the held entries out of old files itself, instead of
keeping the files. An install carries the bytes, because the replica has no
such entry. The replica keeps them in its store until the flush.

### Node identity and discovery

The id of a node is its **store id**. The node creates it at random with its log
store and keeps it there. A node that loses its disk comes back as a new node.
It can never answer in a log after it forgot what it accepted. Safety rests on
that state surviving.

Each start of a node's engine also picks a random *boot* id. A restart loses the
unsynced appends of the node's sequencers, so their terms end: the controller
appoints a sequencer again, preferring the same node.

Nodes find each other through the root log's members, which hold each one's
URL and token key ([Finding the
ledger](ledger-and-compute.md#finding-the-ledger)). A node without
`node.advertise_url` is standalone: it is the only replica of its logs, and it
is in no cluster. A log with one replica commits what it writes.

### Failover

Every node pings every member every 250 ms, and its event loop answers, so a
node whose loop is stuck counts as down too. A node unheard of for 1 s is down.
Pongs carry the boot id.

The controller runs on the root's leader. It looks at a log again when its
sequencer is down, restarted, or no longer a member. It seals the log at the
next term and appoints a sequencer from the answers, preferring the old
sequencer, then the node that sequences the fewest logs. The appointments of
every log ready in a tick go into one root entry. A seal refused by a higher
term, or an appointment whose compare-and-swap fails, means another took over,
and the controller looks at the log again. A failover not done in 3 s starts
over.

The appointee starts its term with a no-op. Proposals that the old sequencer
had not committed fail as not led, and the compute sends them to the new one.

A controller can lose the root's leadership between its seal and its
appointment, and its seal can still land later, after a partition heals. The
replicas are then fenced at a term that the root never gave anyone, and the
old sequencer is deposed while the root still names it. That sequencer tells
the root's leader, every second until a new term comes, and the leader's
controller fails the log over as if its sequencer were down. Jepsen's ledger
partitions found this.

### Replica changes

A log follows the members through its replicas
([Membership](ledger-and-compute.md#membership)). When a replica is removed, a
log has fewer than three replicas and more members are up, or two replicas
share a zone and a member in a zone the log lacks is up, the controller
changes the replicas with an appointment, one change at a time:

- The seal goes to the old replicas and needs a majority of them, as any
  failover's does. The new sequencer is one of the most up-to-date answers that
  stays a replica. The new replica holds nothing to lose.
- A removed sequencer first hands the log to another replica, in a term on the
  same replicas, as a lease moves before its replica does. It stays a replica of
  that term, so it learns the outcome of each proposal it appended and answers
  it. Its replica moves next. A node that drops a log with a proposal still
  unanswered (it was down, or lagged) answers that its outcome is unknown, never
  that the proposal missed the log: the other replicas may still commit it.
- Until the change ends, the log is in a *joint* configuration: the root records
  the replicas it changes from, and a failover seals a majority of both sets.
- The new sequencer catches the new replica up (with a snapshot if it is behind
  the log's start). Once its term's no-op has committed, it proposes an
  `Activation` to the root, which drops the old set.

A node hosts a log while the root names it in either set.

### Batched transport

All frames for all logs that go to one peer travel in one call per batch. The
call is on the `Peer` gRPC service, on the internal port, over one HTTP/2
connection per peer. A frame is a root log message, a repository log's append,
install, acknowledgement, seal or check, or a ping.

The transport addresses peers by id and resolves them through the root log's
members. A
sequencer sends only to its log's replicas. The transport drops a frame for
another id, such as a node that had the address before and lost its disk.

### Quiet logs

A repository log has no timers. A sequencer resends only to replicas that lag
and are up. A log that nobody writes sends nothing, so the idle cost is the
pings, per node and not per log.

On a 3-node cluster, 200 idle repositories exchange **zero messages**.

A quiet log costs no CPU. Its state is in the state store, and what is in memory
is its terms and hard state. Memory is the budget of the state store
(`ledger.state_memory`, 512 MiB by default: block cache and memtables). It is
not a cost per ref.

### Proposals

The log holds effects, not commands, as in CockroachDB's proposer-evaluated KV
and Hyperledger Fabric's execute-order-validate. Only the sequencer evaluates a
command. It checks the compare-and-swaps and builds the `Change` that applying
makes: the refs to set, the layers to add and remove, the new version. The
entry is an `Effect`: the change, the version it was evaluated against (its
base), and the evaluation chain it belongs to. Replicas apply the change as it
is, so a new release can change how commands evaluate without the replicas
agreeing on it.

- **Evaluation.** It runs on the apply thread, after every entry handed to it,
  against the applied state plus the effects the sequencer proposed that have
  not applied yet (its in-flight chain). So proposals to one log pipeline: the
  next one does not wait for the last to commit. A new sequencer evaluates only
  once its no-op has committed, and so it applied the whole log.
- **The check at apply.** An effect applies only if the state is still the one
  it was evaluated against: the same version, made by the same chain (versions
  only grow, but two chains can each reach one). Otherwise it is skipped, as CockroachDB skips a command past its
  MaxLeaseIndex, and the sequencer evaluates the command again, up to 8 times.
  A skip happens when an old sequencer's entries commit after a new one took
  over, or when a new term truncated an entry of the chain.
- **Refusals.** A command that changes nothing (a failed compare-and-swap, a
  deleted repository) evaluated with no change in flight needs no entry. The
  sequencer confirms it with a read check: if the version is unchanged once that
  read has applied, the answer is linearizable. Otherwise it evaluates again.
- **Routing.** Only the sequencer takes a proposal or a deletion. Every `Watch`
  subscription names the log's sequencer, after the state and again whenever it
  changes, and the compute sends proposals there. Any other node refuses one
  as `Unavailable`, naming the sequencer the root names (`enroute-sequencer`
  metadata), and the compute sends it there at once. While a failover runs, or
  the named node cannot answer, the compute asks again for up to 5 s. Ledger
  nodes never send requests on to each other, so a proposal (and its inline
  layer) crosses the network once. This is how CockroachDB's range cache and
  TiKV's client route to the leaseholder or leader.
- **Placement.** The node that creates a repository sequences its log, and the
  other replicas are live members in zones (`node.zone`) the log has no
  replica in yet, then live members holding the fewest logs. A zone move keeps
  the sequencer, and moves a replica that is down before one that is up.
  After a failover the controller prefers the replica that sequences the
  fewest logs, so sequencing spreads across the survivors.

Proposals wait in the sequencer by the position and term of their entry, and
are answered when that entry applies. An entry that a new term truncated, or
whose term no longer matches, fails its proposal as not led, and the compute
retries it at the new sequencer.

### Hosting a new incarnation

Members learn of a new incarnation from the root log. The replicas named in the
create host it as they apply it, from the initial state the directory implies,
before the sequencer's appends arrive (they follow the root's commit). A replica
that missed the create, or a new replica of a replica change, hosts the log
when its root applies the change that names it.

### Read barriers

A read barrier takes the sequencer's commit point, then checks with a majority
of the replicas that its term is still open (no seal raised their fence). It is
a read confirmed by a quorum, not a lease: a quiet sequencer cannot detect that
it was deposed. The engine answers the read as soon as that commit point is
applied, without waiting for the fsync of the round.

A new sequencer serves reads only once its no-op has committed, so its commit
point covers every entry acknowledged before it. Reads that arrive together
share one check. A check unanswered after 300 ms is sent again.

### Recovery

On start, the engine reads every log in the log store: where it starts, its hard
state, and its entries after that. It also reads the applied index of the state.
It then:

1. applies the committed entries after the applied index, synchronously, so the
   [materializer](read-path.md#materialization) sees current states;
2. resumes normal operation. A log whose node sequenced it waits for the
   controller to appoint it again (its boot id changed).

A state ahead of its log (a crash between installing a state and resetting the
log) resets the log. A state behind the start of the log would mean that
entries were lost, and the node refuses to start. The node drops incarnations
that its root log says ended, and later those its root learns ended as it
catches up. A log with one replica counts every durable entry as committed.

The root log recovers from its snapshot and entries, but its directory is in the
log store, at the index it was applied to; Raft hands over the committed entries
after it again. Layer maintenance runs on the compute node that maintains the
repository ([Maintainers](ledger-and-compute.md#maintainers)).
