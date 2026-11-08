# Limitations

Enroute is a `0.x` release with the following current limits.

## Compatibility

- The `v1alpha1` contract is unstable until Enroute 1.0. Messages, fields, and
  services can change in any release.
- The storage format and the configuration keys can change in any `0.x`
  release. A release that changes the storage format cannot read what an
  earlier release wrote. Move to it with a
  [migration](../operate/upgrades.md#migrate-to-a-new-deployment), which copies
  each repository over Git.
- All nodes of a deployment must run the same release. There is no rolling
  upgrade and no supported downgrade.

## Transport and synchronization

- Git smart HTTP is supported; SSH and `git://` are not.
- Enroute does not terminate TLS or authenticate API callers.
- Fetches and `PushToRemote` can send a delta before its base. A receiving
  Git that asked for a thin pack must be version 2.50 or later. An earlier
  version can reject the pack with `REF_DELTA at offset N already resolved`.
- `PushToRemote` and `FetchFromRemote` support `https` remotes with basic or
  bearer credentials. They do not use SSH, persist remotes, or follow
  redirects. `FetchFromRemote` needs a remote that speaks protocol v2.
- Remote pushes and fetches are synchronous: one hour total and one minute
  without remote output. Progress is not reported.

## Data model and storage

- Production requires a durable disk for each ledger node, and a bucket on
  Amazon S3, Google Cloud Storage, Azure Blob Storage, an S3-compatible store
  with conditional writes, or a `file://` bucket. A `file://` bucket serves
  only nodes on one machine. See
  [Storage backends](../operate/storage-backends.md#production-requirements).
- A deployment cannot move from a `file://` bucket to an object store in place.
  Migrate its repositories to a new deployment on the object store instead. See
  [Upgrades and migrations](../operate/upgrades.md#migrate-to-a-new-deployment).
- Enroute does not encrypt data at rest or collect unreachable branch objects.
- A repository's object format is fixed at creation: there is no conversion
  between SHA-1 and SHA-256, and a push that creates a repository creates
  it in `git.default_object_format`, so a push in the other format to a missing
  repository is refused. `FetchFromRemote` and pushes to a remote need a remote of the same
  format. Enroute creates no merge commits.
- `visible_refs` hides ref names, not objects.
- Reads are bounded: 100 repositories or commits per page and 50,000 tree or
  diff entries.
- `ListRepositories` compares a `prefix` byte for byte and returns keys in byte
  order.
- `updated_at` in `ListRefs` comes from the clock of the node that proposed the
  change. It can go back when node clocks differ.

## Operations

- There are no quotas, rate limits, or per-caller concurrency limits.
- Background work (flushes, compaction, the layer sweep) is not traced. Only
  its failures are visible, as `ERROR` log lines.
- Each repository's log is on three ledger nodes, whatever the size of the
  ledger. A repository stops taking writes while two of its three nodes are
  down, even in a ledger of five nodes or more. Remove a lost node promptly, so
  that its logs get a new replica.
- A push is acknowledged when a small layer is in the replicated log on a
  majority of the repository's ledger replicas. The log is the only copy until
  Enroute flushes the layer to the object store.
- Losing a majority of ledger nodes needs a restore from volume snapshots of
  the ledger disks, which loses the writes after them. There is no tool yet that
  forces the remaining nodes to go on without a majority.
- Removing a ledger node is a manual step, `enroute members --remove`. See
  [Maintenance](../operate/maintenance.md).
- The `latest` image tag moves and can be rebuilt. A version tag names one
  image and is not rebuilt.
