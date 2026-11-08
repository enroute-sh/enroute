# Concepts

These pages explain the boundaries between Enroute, Git clients, and your
application.

Enroute serves Git over HTTP, stores Git data, and exposes a gRPC API. Your
application owns repository names, users, access control, and product policy.
Start with the data model and interfaces, then read Hooks before implementing
an endpoint.

| Page | Covers |
| --- | --- |
| [Data model](model.md) | What your application holds, what Enroute holds, and the key that joins them |
| [Clients and interfaces](two-callers.md) | Git, the API, and hooks, and which one a feature uses |
| [Hooks](hooks.md) | A push step by step, and what each hook decides |
| [Storage](storage.md) | A push and a clone through the storage engine, and what survives what |
