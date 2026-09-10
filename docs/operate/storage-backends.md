# Storage backends

Enroute uses object-store URIs such as `s3://`, `gs://`, `az://`, `https://`,
`file://`, and `memory://`.

```toml
[bucket]
uri = "s3://enroute-objects/prefix"

[bucket.credentials]
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
```

Prefer workload credentials, such as an instance role or IRSA, over explicit
credentials. Put backend options in the URI query. Credentials in a URI query
are rejected.

## Permanent and staging storage

`bucket.uri` stores permanent Git objects and index segments. A push stages
data elsewhere before ingestion completes.

With `[ingest.local]`, `scratch` must use `file://` or `memory://`. Do not
share it between instances; each instance removes keys not held by its own
active sessions.

With `[ingest.lambda]`, `handoff.uri` is a shared bucket for the server and
function. It can use separate credentials from the permanent bucket.

Staging data is temporary. Ingestion writes completed data and its catalog to
`bucket.uri`.

## Production requirements

Production deployments require Postgres and an S3-compatible object store.
Use `file://` only for local development. Configure encryption at rest and
access control in the storage providers; Enroute does not provide them.
