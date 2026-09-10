# Hosting

Enroute is currently self-hosted. Managed hosting is planned.

## Image

Images are published at `ghcr.io/enroute-sh/enroute` for `linux/amd64` and
`linux/arm64`. Releases use `0.x` version tags.

| Tag | Meaning |
| --- | --- |
| `latest` | Current release; moves with each one |
| `<version>` | One release; not rebuilt |
| `sha-<commit>` | A specific source commit; never reused |

The `latest` tag moves and may be rebuilt. A version tag names one image, so
use `<version>` or `sha-<commit>` when you need an immutable image. The image
contains the server, `enroute-schema`, and proto files in
`/usr/share/enroute/proto`. Build `enroute-maintenance` from source.

## Upgrades

There is no backwards compatibility between `0.x` releases. Read the release
commit before you move, and plan an upgrade as a deployment change rather than
a restart.

- Database tables migrate forward when the server starts. A migration is
  transactional and forward-only, so a snapshot taken before the upgrade is
  the only way back.
- The object store does not migrate. A release that changes the format reads
  nothing an earlier release wrote, and the repositories must be pushed again.
- The `v1alpha1` contract can change. Generate your client again from the
  proto files the new image carries.
- Configuration keys can change. Start the new image against your
  configuration before you send traffic to it.

## Deployment model

Run Enroute on stateless compute with Postgres for metadata and an object store
for Git data. Multiple server instances can share those services behind a load
balancer. Lambda ingestion is optional.

Configure [storage](storage-backends.md), [security](security.md),
[maintenance](maintenance.md), and [observability](observability.md) before
deploying.

## Operator responsibilities

- Terminate TLS for the Git listener.
- Authenticate API callers and set the tenant header.
- Protect and rotate the hook signing key.
- Apply quotas, rate limits, and concurrency limits.

Review [Limitations](../reference/limitations.md) before production use.
