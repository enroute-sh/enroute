# Upgrades and migrations

This page explains how to move a deployment to a new release of Enroute, and
how to move repositories from one deployment to another. The procedures do not
depend on a particular pair of releases. Notes that apply to one release are in
that release's notes.

## Overview

### Choose a method

There are two ways to move to a new release:

| Method | What you do | Use it when |
| --- | --- | --- |
| [Upgrade in place](#upgrade-in-place) | Replace the binary on every node, and restart the nodes on their disks and bucket. | The new release reads the storage that your release wrote. |
| [Migrate to a new deployment](#migrate-to-a-new-deployment) | Start a new deployment, copy each repository into it, then send traffic to it. | The new release does not read your storage, or you change the bucket. |

An in-place upgrade stops Git and the API while the nodes restart. A migration
keeps the old deployment available while it copies. Each repository refuses
writes only during its last copy, which takes seconds for most repositories.

A migration also moves a deployment to a different bucket, object store, or
cloud, for example from a `file://` bucket to Amazon S3. For moves between
deployment models on the same bucket, see
[Deployment profiles](deployment-profiles.md#move-between-models).

### Release compatibility

Until Enroute 1.0, any `0.x` release can change the storage format, the
configuration keys, and the `v1alpha1` contract. The notes of each release say
which of these changed, and which method the upgrade needs.

- **Storage format.** If the format changed, the new release cannot read what
  the earlier release wrote. Migrate to a new deployment. The bucket's
  `cluster/format` object names the release that created it. A node refuses to
  start on a bucket that a later release created, and on a 0.3 bucket, which
  has no such object.
- **Configuration keys.** Start the new release against your configuration
  before you send traffic to it. A node refuses to start with a key it does not
  know.
- **The `v1alpha1` contract.** Generate your clients again from the proto files
  in the new image, in `/usr/share/enroute/proto`, and test your application
  against the new release.

Run every node of a deployment on the same release. Enroute does not yet test
nodes of different releases together, so it has no rolling upgrade, and there
is no supported downgrade.

## Before you upgrade

1. Read the notes of every release between your release and the new one.
2. Check that the deployment is healthy. `enroute diagnose` must exit with
   status 0, and `enroute members` must list every ledger node that you run.
3. Take volume snapshots of every ledger node's data directory. If an in-place
   upgrade fails, the snapshots are how you go back. Take all of them at one
   point in time, and restore them within `layers.sweep_retention` (6 hours by
   default), so the layer files that they name still exist.
4. Test the upgrade on a staging deployment first, with a copy of real
   repositories and your application.

## Upgrade in place

The new release starts on the ledger disks and the bucket of the old release.
Git and the API are not available from step 2 until step 5.

1. Pull the new image, or install the new binary, on every node.
2. Stop the compute nodes. In a deployment of `both` nodes, stop every node.
3. Stop the ledger nodes.
4. Start the ledger nodes with the new release. Each node starts from its disk
   and catches up with the other ledger nodes.
5. Start the compute nodes with the new release.
6. Run `enroute diagnose`, and clone and push to a test repository.

To roll back, stop every node, restore the volume snapshots of the ledger disks,
and start the earlier release. Writes after the snapshots are lost. Do not
start the earlier release on disks that the new release has written to: it
might not read them.

## Migrate to a new deployment

A migration copies each repository over Git. The new deployment calls
[`FetchFromRemote`](../reference/fetch-from-a-remote.md) with the old
deployment's Git URL. The old deployment serves the fetch as it serves any
clone. A second fetch copies only what is new, so you can copy every
repository while the old deployment takes writes, and then copy each one again
at cutover.

### Requirements

- The new deployment runs a release that has `FetchFromRemote` (0.4.0 or
  later).
- The old deployment serves Git protocol v2. Every release from 0.3.0 does.
- The new deployment can reach the old deployment's Git listener. If the
  listener has an `http` URL or a private address, set
  `git.allow_private_remotes = true` on the new deployment's compute nodes for
  the migration.
- Your application gives the migration a credential that reads every
  repository on the old deployment. If you use hooks, `authorize` must grant it
  read access, and `visible_refs` must show it every ref. A hidden ref is not
  copied.

If the new release has no `FetchFromRemote`, copy each repository with
`git clone --mirror` from the old deployment and `git push --mirror` to the new
one. The rest of the procedure is the same.

### What a migration copies

A migration copies, for each repository:

- The repository key, the default branch, and the object format, which you
  give to `CreateRepository`.
- Every ref under `refs/`, and every object that the refs reach.

It does not copy:

- Objects that no ref reaches.
- `last_push` and the `updated_at` time of each ref. The new deployment sets
  them when the copy lands.

`FetchFromRemote` runs no hooks on the new deployment, so a copy does not start
your `post_receive` work, such as CI.

### Copy the repositories

1. Start the new deployment with a new, empty bucket and empty ledger disks.
   A bucket holds one deployment. Use the same hook configuration as the old
   deployment, but send no traffic to it yet.
2. If any repository uses SHA-256, set `ledger.allow_sha256 = true` on the new
   deployment.
3. List the repositories on the old deployment:

   ```sh
   enroute repo list --api http://old-api.internal:50051 --json
   ```

4. For each repository, create it on the new deployment with the same key,
   default branch, and object format:

   ```sh
   enroute repo create acme/widgets --api http://new-api.internal:50051 \
     --default-branch refs/heads/main --object-format sha1
   ```

5. For each repository, fetch every ref from the old deployment:

   ```sh
   grpcurl -plaintext -d @ new-api.internal:50051 \
     enroute.api.v1alpha1.SyncService/FetchFromRemote <<'EOF'
   {
     "repo": { "key": "acme/widgets" },
     "remote": {
       "url": "http://old-git.internal:8080/acme/widgets.git",
       "basic": { "username": "migration", "password": "<credential>" }
     },
     "prefixes": ["refs/"]
   }
   EOF
   ```

   The response lists each ref that changed. A ref with `STATUS_REJECTED`
   failed a check on the new release. Its message says which one.

Repeat step 5 as often as you like. Each call fetches only what changed since
the last one, and retrying a call that failed is safe.

### Cut over

Cut over one repository at a time, or all of them together:

1. Stop writes to the repository on the old deployment. For example, have your
   `authorize` hook refuse pushes to it, and stop your application's API writes
   to it. Without hooks, take the old deployment out of the load balancer.
2. Fetch the repository once more, as in step 5 of
   [Copy the repositories](#copy-the-repositories).
3. Compare the refs on both deployments. The two outputs must be identical:

   ```sh
   enroute refs acme/widgets --api http://old-api.internal:50051
   enroute refs acme/widgets --api http://new-api.internal:50051
   ```

4. Send Git and API traffic for the repository to the new deployment, and
   allow writes again.

After the cutover of every repository, keep the old deployment, without
traffic, until you are sure you do not need to go back. Then delete it, with
its bucket and ledger disks.

### Roll back a migration

Before a repository's cutover, the old deployment is unchanged: delete the
copy, or leave it. After the cutover, send the repository's traffic back to the
old deployment. Writes that landed on the new deployment after the cutover are
not on the old one. To keep them, copy them back the same way, with
`FetchFromRemote` on the old deployment if its release has it, or with
`git push --mirror`.

## Notes for specific releases

### From 0.3 to 0.4

- 0.4.0 changes the storage format. [Migrate](#migrate-to-a-new-deployment)
  to a new deployment. A 0.4.0 node refuses to start on a 0.3 bucket.
- Every 0.3 repository is SHA-1. Create each one as SHA-1 on the new
  deployment.
- 0.3 has no `FetchFromRemote`. To copy writes back to 0.3 after a cutover,
  use `git push --mirror`.
- Remove `ledger.checkpoint_age` and `ledger.checkpoint_concurrency` from the
  configuration. 0.4.0 refuses them.
- The default of `layers.flush_interval` is now `"30s"`, not `"1s"`. Small
  layers stay in the ledger's log for up to 30 seconds before a compute node
  uploads them to the bucket, so a deployment with many active repositories
  makes far fewer object store requests. To keep the 0.3 behavior, set
  `flush_interval = "1s"` in `[layers]`.
- Compute nodes read the ledger's members from `cluster/members`, which 0.3
  kept in `ledger/members`. If you give compute nodes their own object store
  credentials, allow them to read `cluster/*`. See
  [Object store permissions by role](security.md#object-store-permissions-by-role).
- `UpdateRefs` lands its updates as a `git push` with no pack does, and
  follows the same rules:
  - A delete needs `old_object_id`. A delete without it fails the call with
    `INVALID_ARGUMENT`. In 0.3, it deleted the ref whatever it held.
  - A call that updates the same ref twice is rejected with
    `REASON_REPEATED_REF`.
  - A delete of the branch that `HEAD` points to is rejected with
    `REASON_DELETE_CURRENT`.
  - An update that loses a race with another writer is rejected with
    `REASON_NON_FAST_FORWARD`. Enroute does not retry it, and the call no
    longer fails with `ABORTED`.

  A Git push is also rejected when it updates a ref twice or deletes the
  branch that `HEAD` points to, as Git's own server rejects it. See
  [`UpdateRefs`](../reference/the-contract.md#updaterefs).
- New keys are optional. If your ledger nodes run in more than one
  availability zone, set `node.zone` on each of them, so that each
  repository's log keeps its replicas in distinct zones. See
  [Spread ledger nodes over zones](deployment-profiles.md#spread-ledger-nodes-over-zones).
- The `upgrade-enroute` agent skill walks a coding agent through this
  migration. Install it with `npx skills add enroute-sh/enroute`.

## Troubleshooting

- **`FetchFromRemote` answers `PERMISSION_DENIED`.** The old deployment's
  `authorize` hook refused the migration credential.
- **`FetchFromRemote` answers `FAILED_PRECONDITION`.** The old deployment does
  not speak protocol v2, or the repository on the new deployment has a
  different object format.
- **The refs differ after the last fetch.** A write reached the old deployment
  after step 1 of the cutover, or `visible_refs` hides refs from the migration
  credential. Check both, and fetch again.
- **A node refuses to start after an in-place upgrade.** Read the error. A
  removed or renamed configuration key is the most common cause. The release
  notes name the replacement.
