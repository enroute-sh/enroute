# Concepts

These pages explain the boundaries between Enroute, Git clients, and your
application.

Enroute serves Git over HTTP, stores Git data, and exposes a gRPC API. Your
application owns repository names, users, access control, and product policy.
Start with the data model and interfaces, then read Hooks before implementing
an endpoint.

| Page | Covers |
| --- | --- |
| [Data model](model.md) | Repository keys, refs, objects, and tenants |
| [Clients and interfaces](two-callers.md) | Git, gRPC, hooks, and network boundaries |
| [Hooks](hooks.md) | Application policy decisions and hook limits |
| [Storage](storage.md) | Object storage, Postgres, indexes, and ingestion |
