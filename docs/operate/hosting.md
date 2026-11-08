# Hosting

Enroute is currently self-hosted. Managed hosting is planned.

## Image

Images are published at `ghcr.io/enroute-sh/enroute` for `linux/amd64` and
`linux/arm64`. Releases use `0.x` version tags, such as `0.4.0`.

| Tag | Meaning |
| --- | --- |
| `latest` | Current release; moves with each one |
| `<version>` | One release; not rebuilt |
| `sha-<commit>` | A specific source commit; never reused |

The `latest` tag moves and may be rebuilt. A version tag names one image, so
use `<version>` or `sha-<commit>` when you need an immutable image. The image
contains the server and the proto files in `/usr/share/enroute/proto`. It
declares `/var/lib/enroute` as a volume for the node's data. The entrypoint is
the server.

A minimal start reads the configuration from a mounted directory:

```sh
docker run -d -p 8080:8080 -p 50051:50051 \
  -v /srv/enroute/config:/etc/enroute:ro -v enroute-data:/var/lib/enroute \
  ghcr.io/enroute-sh/enroute:<version> --config file:///etc/enroute/enroute.toml
```

Set `node.data_dir` under `/var/lib/enroute`, so that the node's data is on
the volume. Mount the directory, not the file. An editor that saves by renaming replaces
the inode, and a single-file bind mount keeps the old one. To run a local
node for development instead, see [Run the image](../quickstart.md#run-the-image).

## Upgrades

Until Enroute 1.0, a `0.x` release can change the storage format, the
configuration keys, and the `v1alpha1` contract. The notes of each release say
what changed. A release that reads your storage is an in-place upgrade: replace
the image on every node and restart. A release that does not read it needs a
migration to a new deployment, which copies each repository and keeps the old
deployment available until the cutover. See
[Upgrades and migrations](upgrades.md).

## Node roles

A node runs one of three roles, set with `node.role`:

| Role | Runs | Needs |
| --- | --- | --- |
| `both` | The ledger and the compute. This is the default | A durable disk, and the object store |
| `ledger` | The replicated logs only | A durable disk, `node.advertise_url`, and the object store |
| `compute` | Git and the gRPC API only | The object store, and a disk for caches |

Every deployment uses the same binary and the same bucket, and only the
`[node]` table changes. [Deployment profiles](deployment-profiles.md) gives
the three supported models: a configuration for each node, what each model
survives, how each one scales, and how to move a live deployment from one
model to the next.

Before you deploy, configure [storage](storage-backends.md),
[security](security.md), and [observability](observability.md), and read
[Maintenance](maintenance.md) for the tasks that a replicated ledger needs.

## Operator responsibilities

- Terminate TLS for the Git listener.
- Keep the API listener reachable only by your application.
- Protect and rotate the hook signing key.
- Keep the disk of each ledger node durable.
- Apply quotas, rate limits, and concurrency limits.

Review [Limitations](../reference/limitations.md) before production use.
