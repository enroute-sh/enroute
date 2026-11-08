# Jepsen tests

A [Jepsen](https://jepsen.io) suite checks what Git clients see over smart HTTP
while nodes fail.

## What it checks

Each branch is a list: its first-parent chain. A push appends one commit to
each ref that it moves, and a ref read returns every list of a repository.
[Elle](https://github.com/jepsen-io/elle) then checks the history against
these guarantees:

| Guarantee | Requests | Elle model |
| --- | --- | --- |
| A ref read sees every push that completed before it started, through any compute node | `ls-refs`, the v0 `git-upload-pack` advertisement, and the `git-receive-pack` advertisement. Each takes a read barrier | `:strict-serializable` |
| One compute node never serves an older state than it served before, and serves each push that it acknowledged | `ls-refs` and the v0 `git-upload-pack` advertisement, on one compute node | `:strong-session-serializable`, and a per-node check |
| The refs that land from a push that is not atomic apply together | Pushes without `atomic` | All of the above |

The suite also checks the following:

- Every acknowledged push survives to the end of the run.
- No read returns a commit that no client wrote.
- A fetch of tips that a read returned gets every object, from any compute
  node.

## Faults

- Partitions between ledger nodes: one node alone, majority and minority,
  ring, and bridge.
- Partitions between a compute node and some ledger nodes, and a lagging
  ledger node that is the only one a compute node can reach.
- Kills (`SIGKILL`) of ledger nodes, on
  [LazyFS](https://github.com/dsrhaslab/lazyfs) so that a kill also loses
  the writes that the node did not sync.
- Pauses (`SIGSTOP`) of the leader of a repository's log. The suite finds the
  leader with `GET /status/repos/<repo>` on each ledger node.
- Kills and pauses of compute nodes.
- Member changes, with the other ledger faults: a ledger node loses its disk
  and is replaced, or a running ledger node is retired and joins again as a
  new node. Each follows the steps in
  [Maintenance](../operate/maintenance.md).

The suite runs the nodes as local processes on one machine, with 3 ledger
nodes and 2 compute nodes, 5 ledger nodes and 2 compute nodes, or 3 nodes with
both roles. Only with 5 ledger nodes does a member change move a log's
replicas. It does not test the object store under faults, or zones.
