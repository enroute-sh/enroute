# Configuration keys

See [Configuration](../operate/configuration.md) for loading, environment
expansion, and tenant refresh. Keys without defaults are required.

## Main configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `bucket.uri` | — | Permanent object and index store |
| `bucket.credentials` | none | Store access key and secret |
| `database.url` | — | Postgres URL |
| `database.max_connections` | `10` | Connection-pool limit |
| `database.migrate` | `auto` | `off` blocks automatic migrations |
| `hooks.signing_key` | — | PEM Ed25519 private key |
| `hooks.timeout_secs` | `10` | Hook response timeout in seconds |
| `tenants.uri` | — | Tenant-file URL |
| `tenants.refresh_secs` | `30` | Tenant reload and revocation window |
| `tenants.header` | `x-enroute-tenant` | API tenant header |
| `listen.api` | `0.0.0.0:50051` | gRPC listener |
| `listen.git` | `0.0.0.0:8080` | Git HTTP listener |
| `maintenance.grace_secs` | `21600` | Orphan retention window |
| `maintenance.deleted_grace_secs` | `86400` | Deleted repository retention window |
| `sync.allow_private_remotes` | `false` | Permit HTTP and private remote addresses |

## Implementation choices

Configure exactly one table in each group.

| Group | Keys |
| --- | --- |
| Ingestion | `ingest.local.scratch` or `ingest.lambda.function`, `.region`, `.handoff.uri`, `.handoff.credentials`, `.max_pack_bytes`, `.qualifier` |
| Maintenance | `maintenance.run.in-process.interval_secs` or empty `maintenance.run.off` |
| Telemetry | `telemetry.endpoint`, `.headers`, `.sample_ratio` |

`tenants.<id>` configures a tenant ID, its `hook_endpoint_url`, and exact or
wildcard `domains`. Tenant files are limited to 1 MiB.

Lambda ingestion reads `ENROUTE_SECRET_ID`, `DATABASE_MAX_CONNECTIONS`, and
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`. Its referenced secret contains
`DATABASE_URL`, `STORAGE_ACCESS_KEY_ID`, `STORAGE_SECRET_ACCESS_KEY`, and
`OTEL_EXPORTER_OTLP_HEADERS`.

| Flag | Program | Purpose |
| --- | --- | --- |
| `--config` | all | Configuration URL; also `ENROUTE_CONFIG` |
| `--dry-run` | schema and maintenance tools | Report work without changes |
| `--every` | maintenance tool | Run at this interval |
