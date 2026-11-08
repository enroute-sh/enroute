# Configuration

Enroute loads one main TOML file at startup. Set its URL with `--config` or
`ENROUTE_CONFIG`:

```sh
enroute --config file:///etc/enroute/enroute.toml
enroute --config s3://my-config/enroute.toml
```

`enroute` and `enroute serve` are the same command. `enroute members` and
`enroute diagnose` read the same file. See [Maintenance](maintenance.md).
Every command also reads `ENROUTE_*` values from `.env` files. See
[`.env` files](../reference/configuration-keys.md#env-files).

Any store supported by `bucket` can store the file. See
[Configuration keys](../reference/configuration-keys.md) for the complete
schema and `dev/enroute.example.toml` for an annotated example.

## A production configuration

A replicated deployment has two kinds of node, and each kind has its own
file. The ledger nodes share one file except for `advertise_url`. The compute
nodes share one file.

A compute node serves Git and the API to your application, so it holds the
`[hooks]`, `[listen]`, and `[git]` tables:

```toml
# A compute node: serves Git and the API, and keeps only caches.
[bucket]
uri = "s3://enroute-objects/prod?region=eu-central-1"

[node]
role = "compute"
data_dir = "/var/lib/enroute"
internal_listen = "0.0.0.0:7400"

[listen]
git = "0.0.0.0:8080"         # Public, behind your TLS terminator.
api = "10.0.1.15:50051"      # A private address that only your application reaches.

[hooks]
enabled = ["authorize", "visible_refs", "pre_receive", "post_receive"]
endpoint_url = "https://app.example.com/api/enroute/hooks"
signing_key = "${ENROUTE_HOOK_SIGNING_KEY}"

[git]
create_on_push = false       # Create repositories only with CreateRepository.

[telemetry]
endpoint = "https://otlp.example.com/v1/traces"
sample_ratio = 0.1

[telemetry.headers]
authorization = "Bearer ${OTEL_TOKEN}"
```

A ledger node serves only `internal_listen`, so it needs no `[hooks]`,
`[listen]`, or `[git]`:

```toml
# A ledger node: holds the replicated logs on a durable disk.
[bucket]
uri = "s3://enroute-objects/prod?region=eu-central-1"

[node]
role = "ledger"
data_dir = "/var/lib/enroute"            # On a durable disk with low sync latency.
internal_listen = "0.0.0.0:7400"
advertise_url = "http://ledger-1.internal:7400"   # Different on each ledger node.

[telemetry]
endpoint = "https://otlp.example.com/v1/traces"
sample_ratio = 0.1

[telemetry.headers]
authorization = "Bearer ${OTEL_TOKEN}"
```

Both files leave out `[bucket.credentials]`, so the nodes use the instance's
workload identity. Give each role its own object store permissions; see
[Security](security.md#object-store-permissions-by-role). For the other
deployment models, see [Deployment profiles](deployment-profiles.md).

| Table | Compute node | Ledger node | Purpose |
| --- | --- | --- | --- |
| `[bucket]` | Yes | Yes | The object store. Every node of a deployment uses the same one |
| `[node]` | Yes | Yes | The role, the data directory, the internal listener, and cache sizes |
| `[listen]` | Yes | No | The Git and API listeners |
| `[hooks]` | Yes | No | Your application's hook endpoint |
| `[git]` | Yes | No | Repository creation, object format, object size, and remotes |
| `[telemetry]` | Optional | Optional | Where traces go. See [Observability](observability.md) |
| `[layers]`, `[store_io]` | Optional | Optional | Engine tuning for layers and object store requests |
| `[ledger]` | No | Optional | Engine tuning for the ledger, and `allow_sha256` |

## Secrets

String values can reference an environment variable:

```toml
[bucket.credentials]
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"

[hooks]
signing_key = "${ENROUTE_HOOK_SIGNING_KEY}"
```

Expansion occurs after TOML parsing. Numbers and booleans cannot expand. A
missing variable stops loading, `$$` produces `$`, and bare `$NAME` is
rejected. Expansion is single-pass. Credentials are redacted in logs, but any
process that can inspect Enroute's environment can read them.

## Validation and reloads

Unknown keys are rejected. Configuration is read once, so any change requires a
restart.

## Your application

A deployment serves one application. List the hooks it answers:

```toml
[hooks]
enabled = ["authorize", "visible_refs", "pre_receive", "post_receive"]
endpoint_url = "https://acme.example.com/api/enroute/hooks"
signing_key = "${ENROUTE_HOOK_SIGNING_KEY}"
```

Every hook is opt-in. Without the `[hooks]` table, Enroute calls no hook:
anyone who can reach `listen.git` can read and push. See
[Enable hooks](../reference/hooks.md#enable-hooks) for the default of each
hook.

`endpoint_url` is the exact URL Enroute signs and sends every hook to. It is
validated at startup, so a URL that will not parse stops the process rather
than failing the first Git request.

Enroute reads nothing from the Git request's `Host` header. Every hostname that
reaches `listen.git` is served by this one application, which decides in
`authorize` whether the path it was given names a repository.

This does not authenticate API clients. Nothing does; see
[Security](security.md).

## The node

The `[node]` table sets the role, the data directory, and the cache sizes. See
[Hosting](hosting.md) for roles and [Storage backends](storage-backends.md) for
the data directory.

To replicate the ledger, set `node.internal_listen` and `node.advertise_url`.
Other nodes dial `advertise_url`, which must reach `internal_listen`. Without
`advertise_url`, the node runs alone. Every node serves `GET /status` on
`internal_listen`; see [Observability](observability.md).

## Engine tuning

The `[layers]` and `[ledger]` tables tune how the engine writes, merges,
and deletes data. Every key is optional. A key that you leave out
takes the engine's own default. Change these only to fix a measured problem.
