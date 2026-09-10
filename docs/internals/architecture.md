# Architecture

This page describes Enroute internals. For deployment storage, see
[Storage](../concepts/storage.md).

## Segmented indexes

Repository indexes are ordered sets of segments. Small segments live in their
catalog row; large segments live in the object store. Writes append segments,
reads compose the required range, and maintenance compacts older segments.

Composition is associative, commutative, and idempotent. Compaction can group
segments in any order, retry safely, and merge late segments without
coordination. This state-based CRDT design gives the `lattice` layer its name.

The commit graph stores commit parents, root trees, and generations by commit
sequence number. Separate segmented values store pack bitmaps and locations.
The object index records object locations and tree entries as sorted sequence
pairs. Multiple pack locations form a union; newer segment IDs resolve
placement conflicts.

## Ingestion and cleanup

A push updates several catalog values. Enroute prepares a journal, uploads
segments, then commits the catalog rows through a ledger in one atomic step.
No database transaction remains open during object-store writes.

Objects are uploaded before catalog rows reference them and catalog rows are
removed before their objects. Failures therefore create orphans rather than
dangling references. Maintenance lists each prefix and removes unreferenced
keys after a grace period.

## Boundaries

Postgres holds strict metadata and the migration ledger. Only
`crates/server/postgres` depends on a database driver; the engine uses traits
for rows, ledgers, and segment catalogs. Migrations are checksummed history
applied under an advisory lock.

- `crates/api/*`: public wire contract and hook signatures.
- `crates/lattice/*`: segmented storage independent of Git.
- `crates/git/*`: Git functionality on the storage layer.
- `crates/server/*`: services, configuration, Postgres, and Lambda.

`cargo xtask lint` enforces these dependency boundaries. Application code is
outside this repository; `dev/e2e` contains only a hook stub for tests.
