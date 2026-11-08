# Repository lifecycle

This page describes how a repository is created and deleted, how a deleted name
is reused safely, and how Enroute reclaims the files of a deleted repository.

**The key idea.** Each creation of a repository is a new incarnation, with its
own number in the root log. A key that is deleted and created again names a new
incarnation, so a node or a command that still holds the old one can never
write into the new repository.

The API creates a repository with a default branch (`CreateRepository`), gets
it with its last push time (`GetRepository`), lists (`ListRepositories`) and
deletes it (`DeleteRepository`, and the name is free at once). Between the
roles, the ledger service carries these as calls to activate with a HEAD,
describe, list and delete.

## Creation

`CreateRepository` activates the repository with the create option and a HEAD.
If the repository does not exist (or is deleted), the node proposes its next
incarnation to the root log, as for an implicit create
([Activation](ledger-and-compute.md#activation)). HEAD names the default branch,
and the state marks HEAD as kept.

A kept HEAD is the fixed default branch of the repository. HEAD stays on its
branch while that branch is unborn, as Git does (`ls-refs unborn`; an empty
clone takes it). It also stays on its branch after the branch is deleted. A
repository that a push created is different: Enroute repairs its HEAD to an
existing branch (main, then master, then the first) when its branch is gone.

Creating an existing repository changes nothing and answers with what it has. An
explicit create takes a read barrier on the root log even when the node hosts
the repository, so a replica that missed a deletion cannot answer with the
deleted repository.
Names are at most 256 bytes, so every Enroute key is one.

## Deletion

`DeleteRepository` asks a ledger node to do two things.

1. Host the live incarnation E (as the root log has it) and propose a delete for
   E. The state machine drops the refs and the manifest, and the pins of inline
   layers so the log is not held back. It marks the state deleted. Every later
   command in E is refused as deleted, on the node before it reaches the log,
   and by the state machine for any that did. Deleting a missing or deleted
   repository succeeds.
2. After the tombstone is applied there (the proposer is answered after
   applying), write the trash marker `data/trash/<repo>/<E>` with the last upload
   generation of E. Then propose the end of E to the root log, with the last
   version and upload generation of E. Every member drops the log of E as it
   applies that. A crash between the two steps is finished by the next
   activation of a node that applied the tombstone.

When the call returns, the root log says that E ended. Activation then answers
not found unless it creates. A create starts E + 1 through the root log, as for
incarnation 1.

- The initial state of the new incarnation is empty.
- Its version is one more than the last version of E, so views and subscribers
  never mistake it for an older state.
- Its upload generation is the last one of E plus the upload window, plus 1.

### Why a lagging replica can never write again

The tombstone is committed. A majority of the replicas of E have it, so any
later sequencer of E has it (an appointment seals a majority and picks the most
up to date), and a sequencer refuses every command. The root ends E only after
that commit. Whoever reads "ended" therefore knows that any later sequencer of E
has the tombstone. A replica that was down keeps E untombstoned, but it cannot
commit without a majority, and the others dropped the log. It drops E as its
root log catches up (at startup, if its root already says so).

A node never hosts an ended incarnation again: it hosts only the live one its
directory names. A node that dropped its log would answer in E again from a
fresh state, although it may have answered in E before.

Reads without a barrier may see the old state of E on a lagging node, as any
view can lag the log ([Read path](read-path.md#fetch)). An API commit, a
describe call, the ref reads of a fetch and the receive-pack advertisement use a
read barrier, which brings the node up to the tombstone. A push can check
against the old state, and the next section explains why it cannot land.

### Commands prepared against a deleted incarnation

A push is checked against a snapshot of E. Suppose E is deleted and the name is
created again. The push could then land in E + 1, with references to objects
that only E had. So every compute command names the incarnation that its
snapshot came from. The state machine refuses it in any other incarnation, and
the compute answers not found. Stored layers of E cannot be committed into E + 1
either, because their upload generation is below its window.

### Compute nodes

A deletion reaches the subscription of a compute as a change that marks the
repository deleted. The compute drops the view and stops following. Later
requests activate the repository again and find nothing, or the new incarnation.
A subscription that ends without that change (E dropped on its ledger node) is
followed again. A state of another incarnation replaces the view instead of
updating it. The compute that asked for the deletion drops its view at once.

## Reclaiming layer files

The files of a deleted incarnation stay under `data/repos/<repo>/layers/`. Enroute
reclaims them after `layers.sweep_retention`, so that readers of older snapshots
can finish.

- **Created again:** the layer sweep of the new incarnation finds the files
  unnamed and out of its window. Their generation is at most the last one of E,
  which is below the start of the new window. The sweep deletes them like any
  other file.
- **Deleted, or not followed:** the compute reclaims trash markers every
  `layers.sweep_interval`, on the maintainer of the repository. The ledger
  writes the marker once the tombstone is committed, so nothing names a file
  whose generation is at most the one in the marker, and nothing ever will. The compute deletes the
  file after the retention, timed in the marker as a sweep record does. The
  marker goes once no such file is left.

`data/repos/<repo>/sweep` carries over between incarnations. A sweep record only
delays deletions: it lists files seen unnamed, which the sweep of the new
incarnation checks again.

## Listing

The directory of the root log, which every ledger node has in memory in name
order, is the source, with no object-store requests. A live repository the node
hosts is described from its state; another only by name and incarnation. The
list of a node can lag a create or a deletion, until its root log applies it.

The compute asks every live ledger node and merges the answers. For each name,
the most advanced answer wins: the newest incarnation, deleted over live, then
the newest version. The node that answered a create hosts it, and the node that
answered a deletion applied it. A listing after the call therefore agrees with
it while that node is up.

Each node returns at most one page of names, deleted ones too, and says if more
follow. The merged page stops at the smallest last name among nodes that had
more, so pages never skip a name. A page can be short, because deleted names
take places.

## Last push

The compute stamps the commit entry of a Git push with its clock. The state
machine records it as the last push time if a ref moved, and never moves it
back. A refused push leaves it. API commits and maintenance do not stamp it. A
describe call returns it with HEAD.

## Ref update times

Each ref has the time it was last set (created or moved, also to the same
value), as `ListRefs` of Enroute reports it. The compute stamps every commit
entry that updates refs with its clock, so applying stays deterministic. The
state machine gives that time to each ref that the commit sets, and drops it
with the ref. A refused update leaves it.

The repository state keeps the time, and each ref change carries it to
followers. The state store keeps it after the object id in the
value of the ref (8 bytes), so snapshots carry it with no new keys. Times come from the clocks of proposers, so they can go back when clocks differ.

## Limits

A deleted repository keeps a key in the root log's directory: its next
incarnation starts past the last one.
