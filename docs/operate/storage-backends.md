# Storage backends

Enroute keeps data in two places: an object store and the local disk of each
ledger node.

## Object store

Enroute reaches object stores through the Rust
[`object_store`](https://crates.io/crates/object_store) crate. The scheme of
`bucket.uri` selects the backend:

| Backend | URI | Production |
| --- | --- | --- |
| Amazon S3 | `s3://bucket/prefix` | Supported |
| S3-compatible stores, such as MinIO or Cloudflare R2 | `s3://bucket/prefix?endpoint=...` | Supported if the store has conditional writes |
| Google Cloud Storage | `gs://bucket/prefix` | Supported |
| Azure Blob Storage | `az://container/prefix?account_name=...` | Supported |
| Local directory | `file:///path` | Supported, for nodes on one machine only |
| HTTP (WebDAV) | `https://...` | Not supported. It has no conditional writes |
| Memory | `memory:///` | Not supported. Use it only for development |

Enroute uses the same code for S3, Google Cloud Storage, and Azure Blob
Storage. Only the `object_store` client for the backend is different.

### What Enroute needs from an object store

Enroute writes repository state and the ledger's members with conditional
writes:

- Create an object only if it does not exist (`If-None-Match: *`).
- Replace an object only if it did not change since Enroute read it
  (`If-Match` on the ETag, or the object generation on Google Cloud Storage).

S3, Google Cloud Storage, and Azure Blob Storage have both operations. Some
S3-compatible stores do not. Make sure that your store rejects a conditional
write that does not match. Do not set the S3 option `conditional_put` to
`disabled`.

### Configuration

The query of `bucket.uri` holds the backend options, by their `object_store`
names. `bucket.credentials` holds the secrets. Enroute rejects a credential in
the URI query. A `file://` bucket takes no query.

Each backend also reads its usual environment variables, such as
`AWS_REGION`, `GOOGLE_APPLICATION_CREDENTIALS`, or
`AZURE_STORAGE_ACCOUNT_NAME`. Prefer workload credentials, such as an
instance role, IRSA, GKE Workload Identity, or an Azure managed identity, over
explicit credentials.

Amazon S3:

```toml
[bucket]
uri = "s3://enroute-objects/prefix?region=eu-central-1"

[bucket.credentials]
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
```

Google Cloud Storage:

```toml
[bucket]
uri = "gs://enroute-objects/prefix"

[bucket.credentials]
service_account_key = "${GCS_SERVICE_ACCOUNT_KEY}"
```

Azure Blob Storage:

```toml
[bucket]
uri = "az://enroute-objects/prefix?account_name=mystorageaccount"

[bucket.credentials]
access_key = "${AZURE_STORAGE_ACCOUNT_KEY}"
```

With a workload identity, leave out `bucket.credentials`.

### Shared bucket

The bucket holds repository layers, and the members list that nodes use to
find the ledger. All nodes of a deployment must
share one bucket.

### Incomplete uploads

Enroute uploads large layers in parts. A crash can interrupt an upload and
leave parts that no listing shows. Enroute does not remove them.

- On S3, add a lifecycle rule that aborts incomplete multipart uploads
  (`AbortIncompleteMultipartUpload`, after one day).
- On Google Cloud Storage, add a lifecycle rule with the action
  `AbortIncompleteMultipartUpload` and an age of one day.
- On Azure Blob Storage, no step is necessary. Azure deletes uncommitted
  blocks after seven days.

## Local directory

A `file://` bucket keeps the objects in a directory on the node. Use it to run
Enroute on one machine, such as a small server or a VPS, with no object store.

```toml
[bucket]
uri = "file:///var/lib/enroute/objects"

[node]
data_dir = "/var/lib/enroute/data"
```

- All nodes that use the bucket must run on the same machine. Do not put the
  directory on a network filesystem, such as NFS.
- Enroute syncs each object to disk before a write completes.
- If the bucket and `data_dir` are on the same filesystem, a local copy of a
  layer is a hard link to its file in the bucket. The layers then use disk
  space one time.
- The bucket and `data_dir` are the only copies of your data. If you lose the
  disk, you lose every repository. Make backups of the disk, for example with
  volume snapshots.

## Node disk

`node.data_dir` holds the replicated logs and the state of a ledger node. Put
it on a disk that persists, such as a cloud volume. In the image, put it under
`/var/lib/enroute`, which is a volume.

A node that loses this directory starts again as a new node. It does not
recover its old identity. Do not run two nodes from copies of one directory.

Compute nodes keep only caches in `data_dir`. Enroute rebuilds them when they are
lost.

## Production requirements

Production deployments need a persistent disk for every ledger node, and one of
these buckets:

- Amazon S3, Google Cloud Storage, or Azure Blob Storage.
- An S3-compatible object store with conditional writes. See
  [What Enroute needs from an object store](#what-enroute-needs-from-an-object-store).
- A [local directory](#local-directory) on a persistent disk, for a deployment
  on one machine.

Use `memory://` only for development.
Configure encryption at rest and access control in the storage providers;
Enroute does not provide them.
