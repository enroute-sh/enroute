# Configuration

Enroute loads one main TOML file at startup. Set its URL with `--config` or
`ENROUTE_CONFIG`:

```sh
enroute --config file:///etc/enroute/enroute.toml
enroute --config s3://my-config/enroute.toml
```

Any store supported by `bucket` can store the file. See
[Configuration keys](../reference/configuration-keys.md) for the complete
schema and `dev/enroute.example.toml` for an annotated example.

## Secrets

String values can reference an environment variable:

```toml
[database]
url = "${DATABASE_URL}"

[hooks]
signing_key = "${ENROUTE_HOOK_SIGNING_KEY}"
```

Expansion occurs after TOML parsing. Numbers and booleans cannot expand. A
missing variable stops loading, `$$` produces `$`, and bare `$NAME` is
rejected. Expansion is single-pass. Credentials are redacted in logs, but any
process that can inspect Enroute's environment can read them.

## Validation and reloads

Unknown keys are rejected. Configure exactly one ingestion table
(`ingest.local` or `ingest.lambda`) and exactly one maintenance runner table
(`maintenance.run.in-process` or `maintenance.run.off`). A main-file change
requires a restart.

The tenant file is the exception: Enroute reloads it while running.

## Tenants

`tenants.uri` points to a second TOML file:

```toml
[tenants.acme]
hook_endpoint_url = "https://acme.example.com/api/enroute/hooks"
domains = ["acme.enroute.sh", "git.acme.com", "*.acme.com"]
```

The table name is the tenant ID: 1–64 lowercase letters, digits, `-`, or `_`.
It is permanent because repositories are owned by that ID. Use an opaque ID,
not a mutable display name.

`hook_endpoint_url` is the exact URL Enroute signs for hook requests.
`domains` maps Git hostnames to a tenant. Exact domains win over wildcards;
among wildcard suffixes, the longest match wins. `*` is a fallback and may be
claimed by only one tenant. An unclaimed hostname returns 404.

The tenant file does not authenticate API clients. Authenticate them before
the API listener; see [Security](security.md).

### Refresh behavior

Enroute checks for tenant-file changes every `tenants.refresh_secs` (30 seconds
by default). The interval is also the maximum revocation delay. If a reload
fails, Enroute retains the last valid tenant list and logs an error. The first
load must succeed. The file limit is 1 MiB.

Write a replacement beside the current file and rename it into place. In
Docker, mount the containing directory rather than the file so the container
can observe the replacement inode.

## Schema and maintenance tools

The image includes `enroute-schema`, which reads `[database]` and applies
pending migrations. `--dry-run` lists them. `enroute-maintenance` is built
from source. Both use the normal configuration URL:

```sh
enroute-schema --config file:///etc/enroute/enroute.toml --dry-run
enroute-maintenance --config file:///etc/enroute/enroute.toml --every 900
```

`database.migrate = "auto"` applies migrations at startup, each in a
transaction under an advisory lock. With `"off"`, startup refuses an outdated
database until you run `enroute-schema`. Migrations use the connection's
`search_path`; create a separate schema first and set it in the database URL.

## Lambda ingestion

The ingest function receives `bucket.uri` and `ingest.lambda.handoff.uri` in
its invocation, not the configuration file. It reads credentials from Secrets
Manager so they are not retained in invocation payloads.

| Variable | Purpose |
| --- | --- |
| `ENROUTE_SECRET_ID` | Secrets Manager secret |
| `DATABASE_MAX_CONNECTIONS` | Per-invocation connection limit |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | OTLP traces endpoint; unset disables export |

The secret contains `DATABASE_URL`, `STORAGE_ACCESS_KEY_ID`,
`STORAGE_SECRET_ACCESS_KEY`, and `OTEL_EXPORTER_OTLP_HEADERS`.
