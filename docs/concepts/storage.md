# Storage

Enroute keeps Git data in an object store and transactional metadata in
Postgres. Serving processes are stateless; they do not keep repository disks.

## Object store

Git objects are stored in pack images. Reads use an index to locate a byte
range in the object store. Active pushes use separate staging storage until
ingestion completes.

## Postgres

Postgres stores repository ownership, refs, object-ID-to-sequence mappings,
and the index catalog. Enroute applies database migrations at startup.

Each Git object type has a separate sequence-number space. A sequence number
is meaningful only with its object type.

## Indexes

The commit graph and object index are segmented. Small segments are stored in
Postgres; larger segments are stored in the object store. Writes append a
segment, reads compose the needed segments, and maintenance merges segments.

The commit graph supports ancestry, merge-base, and history queries. The
object index locates objects and records tree entries. See
[Architecture](../internals/architecture.md).

## Ingestion and retention

The server keeps Git connections open, calls hooks, and updates refs.
Ingestion stores uploaded objects and builds indexes. It can run locally or in
Lambda. See [Storage backends](../operate/storage-backends.md).

Enroute does not garbage-collect objects made unreachable by branch deletion.
Delete the repository to remove its data. Configure encryption at rest in
Postgres and the object store; Enroute does not provide it. Fetches that
include `have` lines receive a complete pack rather than a thin pack.
