# Jepsen tests for git push and ref reads

This suite tests what git clients see over smart HTTP: ref updates from
`git-receive-pack`, and ref reads from `ls-refs` and the ref advertisements.
[Jepsen tests](../../docs/internals/jepsen.md) describes what it checks. It
runs every week in CI (`.github/workflows/soak.yml`, with random walks
of the shard log model).

The suite runs all nodes as local processes on one Linux machine, on
`127.0.0.1`. It does not use Docker or SSH.

## Requirements

- Linux, as root. The suite uses `iptables`, `setpriv`, and `useradd`.
- Java 17 or later, and [Leiningen](https://leiningen.org).
- `git`, for the fetch checks.
- [Graphviz](https://graphviz.org) (`dot`). Elle draws each anomaly that it
  finds. Without `dot`, a run with an anomaly reports `:unknown`, not the
  anomaly.
- Optional: [LazyFS](https://github.com/dsrhaslab/lazyfs), so that a ledger
  kill also loses the writes that were not synced. See
  [Build LazyFS](#build-lazyfs).

## Set up the machine

1.  Build Enroute:

    ```sh
    cargo build --release --bin enroute
    ```

2.  Make one Linux user for each node. Each node runs as its own user, so that
    `iptables` can drop the packets of one node on the loopback interface.

    ```sh
    for i in 1 2 3 4 5 6 7; do useradd -M -s /usr/sbin/nologin n$i; done
    ```

## Run a test

From this directory:

```sh
lein run test --workload w1 --nemesis kill-ledger --time-limit 60 --concurrency 10
```

The test starts the cluster, runs the workload for `--time-limit` seconds, ends
all faults, waits `--quiet` seconds, reads every key on each compute node, and
then checks the history. Jepsen writes the history, the node logs, and the
results to `store/`.

To run each workload with each fault, and then with all faults together:

```sh
BIN=../../target/release/enroute TIME=60 ./run.sh
```

After the last run, `run.sh` prints one line for each run and appends the
lines to `summary.txt`. It exits with a non-zero status if a run is not valid
or did not finish. `WORKLOADS`, `FAULTS`, and `TOPOLOGY` select the runs, as
in the weekly workflow. To print the line for runs in the store:

```sh
lein run -m jepsen.enroute.summary store/latest
```

To see the results in a browser, run `lein run serve` and open
`http://localhost:8080`.

### Options

| Option | Default | Purpose |
| --- | --- | --- |
| `--workload` | `w1` | `w1`, `w1-v0`, `w2`, `w3`, or `w4`. See [Workloads](#workloads) |
| `--topology` | `split` | `split`: 3 ledger nodes and 2 compute nodes. `both`: 3 nodes with both roles. `wide`: 5 ledger nodes and 2 compute nodes |
| `--nemesis` | none | Faults, separated by commas. See [Faults](#faults) |
| `--interval` | `10` | Seconds between fault changes |
| `--quiet` | `30` | Seconds without faults before the final reads |
| `--rate` | `100` | Client operations per second |
| `--key-count` | `8` | Keys (branches) in each repository |
| `--per-key` | `200` | Appends to each key before the test uses a new repository |
| `--repos` | `1` | Repositories written at once: half the operations go to one, the rest to any of them |
| `--binary` | `../../target/release/enroute` | The `enroute` binary |
| `--dir` | `/var/tmp/enroute-jepsen` | Parent of the per-test directories for node data and the bucket |
| `--lazyfs` | none | Path of the LazyFS binary. Puts each ledger data directory on LazyFS |
| `--inline-layer-kb` | none | `layers.inline_layer`, in KiB. `0` makes every push upload its layer to the bucket first |
| `--rust-log` | `warn` | `RUST_LOG` of the nodes |

Jepsen's own options, such as `--concurrency` and `--test-count`, also apply.

## The cluster

Node `nI` listens on these ports:

| Port | Listener |
| --- | --- |
| `18000 + 10 × I` | `listen.git` |
| `18001 + 10 × I` | `listen.api` |
| `18002 + 10 × I` | `node.internal_listen`, and `advertise_url` on ledger nodes |

In the `split` topology, `n1` to `n3` have `role = "ledger"`, and `n4` and `n5`
have `role = "compute"`. In the `wide` topology, `n1` to `n5` are ledger nodes
and `n6` and `n7` are compute nodes. Each log has 3 replicas, so only in `wide`
can a member change move a replica to another node. All nodes use one
`file://` bucket. The configuration
has no `[hooks]` table, so pushes and reads are open, and a push creates its
repository.

### How the partitions work

All nodes share the loopback interface, so an address cannot identify a node.
The suite tags connections instead:

1. A rule in the `mangle` table marks each new TCP connection with the index of
   the user that opened it (`CONNMARK`).
2. To cut node A off from node B, a rule in the `filter` table drops packets
   of connections with the mark of A, to or from the internal port of B. This
   stops both directions of new and open connections.

The rules are in the chain `ENROUTE_JEPSEN` of both tables. If a test crashes,
remove them:

```sh
iptables -F ENROUTE_JEPSEN; iptables -t mangle -F ENROUTE_JEPSEN
```

## The client

The client speaks smart HTTP with the Java HTTP client. It does not use `git`
or gRPC, except for the fetch checks.

Each key is a branch. Key `k` is branch `k<k mod key-count>` of repository
`jepsen-<k / key-count>`. Each append is a commit of the empty tree, with the
old tip as parent and the value in the message. The client computes each
commit ID, and keeps a map of commit ID to parent and value. Thus it decodes a
tip to a list without a fetch. A tip that is not in the map is a phantom.

| Operation | Request | Elle transaction |
| --- | --- | --- |
| Read | `ls-refs` (protocol v2), the v0 `git-upload-pack` advertisement, or the `git-receive-pack` advertisement | `[:r k list]` for each key of the repository |
| Write | Read the tips from the `git-receive-pack` advertisement, then push one commit for each append | `[:r k list] [:append k v]` for each ref that landed |

A landed ref proves that the ref still held the tip that the client read, so
the write also reads that list.

The client maps the push outcome as follows:

| `report-status` | Outcome |
| --- | --- |
| `ok` for each ref | `:ok` |
| `ok` for some refs (a push that is not atomic) | `:ok`, with only the landed refs |
| `ng` for each ref, or `unpack` failed | `:fail` |
| No report, a 5xx status, a timeout, or a broken connection | `:info` |
| The tip read failed, or the connection was refused | `:fail`. Nothing was sent |

## Workloads

| Workload | Pushes | Reads | Checks |
| --- | --- | --- | --- |
| `w1` | Atomic | `ls-refs` | Elle `:strict-serializable` and `:strong-session-serializable`, and monotonic reads on each node |
| `w1-v0` | Atomic | v0 `git-upload-pack` advertisement | As `w1` |
| `w2` | Not atomic | `ls-refs` | As `w1` |
| `w3` | Atomic or not, at random | `git-receive-pack` advertisement | Elle `:strict-serializable` |
| `w4` | Atomic | `ls-refs` on a random compute node, then a fetch of each tip from the other compute node | Elle `:strict-serializable`, and no fetch fails with `not our ref` |

In `w1` to `w3`, each Jepsen process sends all requests to one compute node.
In `w4`, each request goes to a random compute node.

About 1% of operations, and one operation per process at the end, run
`git fetch` of all branches and `git fsck --connectivity-only`.

## Checkers

| Checker | Fails when |
| --- | --- |
| `elle` | Elle finds an anomaly that the consistency models of the workload forbid. A push that is not atomic and timed out first keeps only the appends that the final reads show landed |
| `node-monotonic` | On one compute node, a read returns an older list than a read that ended before it started, or misses an append that ended before it started |
| `durability` | A final read misses an append that was `:ok` |
| `phantoms` | A read returns a commit that the client did not write |
| `fetches` | A fetch of tips that a read returned fails with `not our ref`, or its pack misses objects, or `git fsck` fails |

Elle's `:strong-session-serializable` treats each Jepsen process as a session.
The `node-monotonic` checker adds the guarantee across the processes of one
compute node.

## Faults

| Fault | What it does |
| --- | --- |
| `partition-ledger` | Partitions the ledger nodes: one node alone, majority and minority, ring, or bridge |
| `partition-compute` | Cuts one compute node off from some of the ledger nodes |
| `partition-lagging` | Cuts one ledger node off from the other ledger nodes, so that it falls behind. Cuts one compute node off from all ledger nodes but that one |
| `kill-ledger` | Kills (`SIGKILL`) one ledger node, then starts it again. With `--lazyfs`, it also drops the writes the node did not sync |
| `kill-leader` | As `kill-ledger`, but kills the leader of the current repository. See [Find the leader](#find-the-leader) |
| `pause-leader` | Pauses (`SIGSTOP`) the leader of the current repository, then resumes it |
| `pause-compute` | Pauses one compute node, then resumes it |
| `kill-compute` | Kills one compute node, then starts it again |
| `replace-ledger` | A ledger node loses its disk and starts again at the same address, with a new ID. Then the old ID is removed with `enroute members --remove` |
| `retire-ledger` | Removes a running ledger node, waits until its logs move off it, and stops it. Then it starts on an empty disk and joins as a new node. Needs `wide` |

Each cycle waits `--interval` seconds, starts one or two faults, waits
`--interval` seconds, and ends them.

The member changes follow the steps in
[Maintenance](../../docs/operate/maintenance.md), and never use `--force`. If
the ledger refuses to remove an old ID, for example while a partition holds,
the suite tries again when the cycle ends and before the final reads.
`run.sh` names the set of member changes and other ledger faults
`membership`.

### Find the leader

`kill-leader` and `pause-leader` ask each ledger node for
`GET /status/repos/<repo>`, and pick the first of these:

1. The leader that a majority of the log's 3 replicas name, at the highest
   term.
2. The node that says it leads, at the highest term. This can be a leader
   that a partition deposed.
3. Any ledger node, if no node hosts the repository.

The nemesis op records which rule it used: `:majority`, `:claim`, or
`:random`.

## Build LazyFS

LazyFS needs `libfuse3-dev`, `fuse3`, `cmake`, and a C++ compiler. Its build
downloads `spdlog` and `toml11`.

```sh
git clone https://github.com/dsrhaslab/lazyfs.git
cd lazyfs/libs/libpcache && ./build.sh && cd ../../lazyfs && ./build.sh
```

Then pass `--lazyfs <path>/lazyfs/build/lazyfs`. `run.sh` uses
`/opt/lazyfs/lazyfs` if it exists.

## Limitations

- All nodes run on one machine, without Docker. The bucket is a local
  directory, so there is no object store fault.
- The ledger has 3 or 5 nodes, and the nodes have no `node.zone`.
