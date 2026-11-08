# Overview

This page is the one-page picture of Enroute. For deployment storage, see
[Storage](../concepts/storage.md). Each topic here has its own page. The
[index](README.md) gives a reading order.

Enroute is a Git server that follows modern database principles to get high
throughput and high scalability.

Most Git hosting starts short-lived `git` processes against on-disk
repositories. Compared with a database, this has two limits. It has no
long-lived in-memory data structures for fast operations. It also couples
compute and storage, which modern databases decouple.

Enroute instead:

- keeps repositories as **immutable layers in an object store** (S3, Google
  Cloud Storage, Azure Blob Storage, or a local directory), LSM-style, with
  background compaction;
- keeps each repository's **refs and layer manifest in its own replicated log**
  (a sequencer appointed by a root Raft log, replicating over one shared,
  group-committed log store per node, with the state machines on disk in an
  LSM tree; idle logs cost nothing; no external database);
- serves fetches from **long-lived in-memory snapshots** (commit graph with
  generation numbers, per-commit object segments as roaring bitmaps), streaming
  stored pack entries verbatim;
- ingests pushes with **parallel indexing and delta encoding**, publishing
  objects and ref updates atomically;
- offers a **fast write path** for server-side commits that skips the object
  store entirely (inline layers).

## Goals

- **Fully compatible with stock Git clients.** No reliance on default-off client
  features (for example `packfile-uris` or `bundle-uri`). Enroute must be fast
  out of the box on a stock client config.
- **High throughput per repository**: many concurrent pushes and many concurrent
  clones and fetches against the same repository.
- **Scales in both directions**: millions of small repositories, and very large
  monorepos.
- **Storage and compute are disaggregated** in the style of Neon: the object
  store is the durable store, and nodes are replaceable.
- **No external database dependency.** Consensus for mutable state is built in
  (a shard log per repository, and a Raft root log).

## Non-goals

- Forks and shared object storage across repositories. **The repository is the
  storage unit.**
- Protocols other than smart HTTP. The design does not depend on the
  transport.
- Placement: routing requests to the compute nodes that serve a repository, and
  adding and removing compute nodes with load. Enroute does not do this. Compute
  nodes can come and go freely.
- Authentication and authorization policy. The host decides, through hooks: an
  authorizer on each Git request names the repository and the caller, and a ref visibility filter runs on each ref advertisement. The
  push hooks get the same caller. See [Serving](serving.md).

## Architecture

A node runs a **ledger**, a **compute**, or both. A small deployment gives every
node both roles.

```
         stock git clients             API clients
               │ smart HTTP (git port)       │ gRPC (API port)
  ┌────────────▼─────────────────────────────▼──────────────┐
  │ compute: git protocol (upload-pack, receive-pack), API  │
  │   ingest   serving   maintenance (as maintainer)        │
  │   views: immutable snapshots, node-private state        │
  │   followed repositories   storage: files, buffer pool   │──┐
  └───────────────┬─────────────────────────────────────────┘  │ layers
  ledger service  │ activate, propose, read barrier,           │
    (in-process   │ follow (the internal port)                 │
     or remote)   ▼                                            ▼
  ┌───────────────────────────────────────────────────────┐   object store
  │ ledger: consensus (a shard log per repository,        │   (S3, GCS, Azure,
  │ state machine: refs + manifest + inline layers),      │──▶ or a directory):
  │ root log: incarnations, members                       │    members list,
  └───────────────┬───────────────────────────────────────┘    trash markers
       Peer (gRPC,│ batched, the internal port)
                  ▼
          other ledger nodes
```

- The **ledger** keeps one replicated log per repository. The state machine of a log
  is the refs, the `HEAD` branch, and the layer manifest of that repository.
  Logs of all repositories share one group-committed log store on each node.
  Each log has up to three replicas among the ledger nodes, one of them its
  sequencer, which the root log names. See [Consensus](consensus.md) and
  [Ledger and compute](ledger-and-compute.md).
- The **compute** does the Git work. It follows the logs, builds in-memory views
  of each repository, and serves Git and the gRPC API from them.
- A repository's data is a set of immutable LSM layers in the object store. A
  layer holds objects, a commit graph, and an index. See [Storage](storage.md).

### Writes

A push indexes the pack and writes it as a new layer. The compute then commits
the layer and the ref changes as one log entry. A small layer travels inside the
entry, so the push costs one replicated log write. A large layer is uploaded to
the object store first. A background task on one compute node per repository
flushes inline layers to the object store and merges layers. See [Write
path](write-path.md) and [Maintenance](maintenance.md).

A push with `--atomic` sets the atomic flag on the commit, so every ref lands or
none does. `UpdateRefs` sets it when the request asks for `atomic`. Otherwise
its updates share one log entry, but each one succeeds or fails on its own.

### Reads

A compute serves a fetch from its in-memory view of the repository. The view
follows the log, so it can lag the ledger briefly. The ref reads of a fetch
and gRPC reads use a read barrier, which brings the view up to date first. A
push takes the barrier only when its view does not have an object it needs. See [Read path](read-path.md).

## Protocol

- **Fetch** is protocol v2 `upload-pack` over smart HTTP: `ls-refs` (with
  `unborn`, `peel`, `symrefs`) and `fetch`. `fetch` supports `thin-pack`,
  `include-tag`, `filter` (`blob:none`, `blob:limit=<n>`, `tree:0`), `shallow`,
  `deepen <n>`, `deepen-relative` and `--unshallow`. Protocol v0 and v1
  `upload-pack` (stateless RPC, as some clients such as go-git still use) is
  served from the same planner and pack writer.
- **Push** uses v0 `receive-pack` over smart HTTP, because v2 does not define
  push. Supported: `report-status`, `side-band-64k`, `atomic`, `delete-refs`,
  `quiet`, thin packs, and gzip request bodies. Enroute accepts pushes from
  shallow clones when the result stays connected.
- **Server-side writes** go through the API, Enroute's contract
  `enroute.api.v1alpha1` on its own port: `CommitService.CreateCommit` edits
  files and creates a commit on the server. Errors are gRPC status codes:
  INVALID_ARGUMENT, NOT_FOUND, FAILED_PRECONDITION (the branch moved), ABORTED
  (other writers kept moving it).
- **Repositories** are created by a Git push to a missing one
  (`git.create_on_push`, on by default; off, a push answers not found and
  creates nothing), or explicitly with `CreateRepository`. The API also lists
  and deletes them ([Lifecycle](lifecycle.md)). An API commit to a missing
  repository answers not found.

[Serving](serving.md) describes what the compute exposes in more detail.

## State: three tiers

| Tier | Where | Source of truth? | Contents |
|---|---|---|---|
| Replicated | Shard log + state machine, while a repository is active | Yes | Per repo: refs, HEAD, layer manifest, inline layers; the root log: which repositories exist, the members |
| Durable shared | Object store | Yes | Immutable layer files; the [members list](ledger-and-compute.md#finding-the-ledger) |
| Replica-private | Node memory + local disk | **No**: derived, rebuildable | Layer cache (local files read through the buffer pool), dense ids, commit graph, segments, object and entry caches |

A node can lose its entire local cache. It rebuilds from the replicated state
plus the object store. Losing replica-private data costs
performance, never correctness. A repository's latest state is in its log and
state store on its replicas' disks, which move with their nodes ([Freezing the
ledger](ledger-and-compute.md#freezing-the-ledger)). If a majority of them are
lost, an operator restores the ledger from volume snapshots
([Recovery](ledger-and-compute.md#recovery)).

## Components

Enroute has three parts. Dependencies point down only:

- The server: the configuration, the node assembly, telemetry, and the
  `enroute` command. It is the top part and can use all others.
- The storage engine: the ledger and the compute. The compute serves Git and
  the gRPC API, calls the hook endpoint, and pushes to remotes. The ledger and
  the compute do not depend on each other. They meet only at the ledger
  service.
- The public contract: the API and the hook signatures.
