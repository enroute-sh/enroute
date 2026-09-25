# Configuration keys

See [Configuration](../operate/configuration.md) for loading, environment
expansion. Keys without defaults are required.

## Main configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `bucket.uri` | — | Permanent object and index store |
| `bucket.credentials` | none | Store access key and secret |
| `database.url` | — | Postgres URL |
| `database.max_connections` | `10` | Connection-pool limit |
| `database.migrate` | `auto` | `off` blocks automatic migrations |
| `hooks.endpoint_url` | — | URL your application answers hooks on |
| `hooks.signing_key` | — | PEM Ed25519 private key |
| `hooks.timeout_secs` | `10` | Hook response timeout in seconds |
| `listen.api` | `0.0.0.0:50051` | gRPC listener |
| `listen.git` | `0.0.0.0:8080` | Git HTTP listener |
| `maintenance.grace_secs` | `21600` | Orphan retention window |
| `maintenance.deleted_grace_secs` | `86400` | Deleted repository retention window |
| `sync.allow_private_remotes` | `false` | Permit HTTP and private remote addresses |

Enroute orders and ranges repository keys with the database's own collation.
`ListRepositories` compares a `prefix` byte for byte, so a database collating
`text` in byte order — `C` or `POSIX` — is what makes a prefix mean exactly the
keys starting with it. A linguistic collation may order punctuation against
letters differently and narrow a page to the wrong set. Enroute does not set
this; it is chosen when the database is created.

## Implementation choices

Configure exactly one table in each group.

| Group | Keys |
| --- | --- |
| Ingestion | `ingest.local.scratch` or `ingest.lambda.function`, `.region`, `.handoff.uri`, `.handoff.credentials`, `.max_pack_bytes`, `.database_max_connections`, `.objects_credentials`, `.handoff_credentials`, `.qualifier`, `.tenant` |
| Maintenance | `maintenance.run.in-process.interval_secs` or empty `maintenance.run.off` |
| Telemetry | `telemetry.endpoint`, `.headers`, `.sample_ratio` |

Lambda ingestion reads no configuration of its own. Everything it needs
arrives on the invocation: `database.url` with `.database_max_connections`,
both buckets' credentials, and `telemetry.endpoint` with the
`telemetry.headers` its spans carry. Set `.tenant` only on a function shared
between deployments, which has to be one created in tenant isolation mode.
`.database_max_connections` caps one push's fan-out, where `database
.max_connections` caps this server's concurrent pushes.

`.objects_credentials` and `.handoff_credentials` say where the function takes
each bucket's credentials from, and take `sent` or `environment`. `sent`
carries that bucket's `credentials` on every call and the function reaches it
with those alone. `environment` leaves the function to reach it as itself, with
the one role every deployment on it shares, so use it for a bucket the
function's operator holds for all of them. `sent` for a bucket configured with
no credentials is refused.

| Flag | Program | Purpose |
| --- | --- | --- |
| `--config` | all | Configuration URL; also `ENROUTE_CONFIG` |
| `--dry-run` | schema and maintenance tools | Report work without changes |
| `--every` | maintenance tool | Run at this interval |
