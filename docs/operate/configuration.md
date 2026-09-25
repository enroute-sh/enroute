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
(`maintenance.run.in-process` or `maintenance.run.off`). Configuration is read
once, so any change requires a restart.

## Your application

A deployment serves one application:

```toml
[hooks]
endpoint_url = "https://acme.example.com/api/enroute/hooks"
signing_key = "${ENROUTE_HOOK_SIGNING_KEY}"
```

`endpoint_url` is the exact URL Enroute signs and sends every hook to. It is
validated at startup, so a URL that will not parse stops the process rather
than failing the first Git request.

Enroute reads nothing from the Git request's `Host` header. Every hostname that
reaches `listen.git` is served by this one application, which decides in
`authorize` whether the path it was given names a repository.

This does not authenticate API clients. Nothing does; see
[Security](security.md).

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

The ingest function receives `bucket.uri`, `ingest.lambda.handoff.uri`, the
credentials for both, `database.url` with its connection cap, and where to
export spans — all in its invocation, not a configuration file. It reads no
configuration of its own, so one function can serve deployments whose
databases and buckets have nothing in common. The only variables it reads are
the ones Lambda sets for every function, and `RUST_LOG`.

Say where the function takes each bucket's credentials from:
`ingest.lambda.objects_credentials` for `bucket` and `.handoff_credentials`
for the handoff bucket. `sent` carries that bucket's credentials on every call,
and the function reaches it with those and nothing else. `environment` leaves
the function to reach it as itself, using the role it runs under.

A deployment has a function to itself by default, and that function serves
nobody else. To share one between deployments, create it in tenant isolation
mode and give each deployment an `ingest.lambda.tenant` of its own. Lambda
keeps an execution environment to the one tenant it first served, so no two
deployments share a `/tmp` or a connection pool. Set the mode when you create
the function: it cannot be turned on later, and Lambda refuses a tenant id for
a function without it.

A shared function runs under one role, which every deployment on it reaches a
bucket as. Use `environment` there only for a bucket the function's operator
holds for all of them, and `sent` for a bucket that belongs to one deployment.

It holds no credentials, so it needs no secret and no storage permissions.
Give its execution role only what Lambda requires to run and to write logs.
