# Architecture

These pages tell how Enroute works inside: how it stores, replicates, and
serves repositories, and how it is tested. You do not need them to run Enroute
or to build on it.

Read the pages in the order below. Each one describes the system as built.

| Page | Covers |
| --- | --- |
| [Overview](overview.md) | Goals, the three tiers of state, protocol, and components |
| [Consensus](consensus.md) | One replicated log per repository (shardlog), the root Raft log, the log store, the state store, and recovery |
| [Ledger and compute](ledger-and-compute.md) | The two roles, subscriptions, membership, the root log, and ownership |
| [Storage](storage.md) | Layers, object ids, the storage service, the buffer pool, and tiered reads |
| [Write path](write-path.md) | Push, durability without waiting for the object store, and hooked pushes |
| [Read path](read-path.md) | Fetch planning and streaming, node-private views, and local disk as a cache |
| [Serving](serving.md) | What the compute exposes: smart HTTP, hooks, and the API |
| [Lifecycle](lifecycle.md) | Create, delete, incarnations, and reclaiming files |
| [Maintenance](maintenance.md) | Compaction, the layer sweep, and garbage collection |
| [Jepsen tests](jepsen.md) | The consistency that Git clients see under faults, and how the suite checks it |
