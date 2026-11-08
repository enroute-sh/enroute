---
name: upgrade-enroute
description: "Upgrade a running Enroute deployment to a new release, or move its repositories to a new deployment: an in-place restart when the new release reads the existing storage, or a migration that copies every repository over Git with FetchFromRemote, catches up, and cuts over one repository at a time. Also moves a deployment to another bucket or object store. Use this skill when an operator wants to upgrade Enroute (for example from 0.3 to 0.4), migrate or move a deployment, or copy repositories between Enroute deployments. Not for building an application on Enroute."
---

# Upgrade Enroute

This skill moves a running Enroute deployment to a new release. It follows the
docs page [Upgrades and migrations], which is the reference. Read the notes of
every release between the user's release and the new one before you start.

There are two methods. Pick one with the user before anything else:

| Method | Use it when |
| --- | --- |
| [Upgrade in place](#upgrade-in-place) | The new release reads the storage that the old release wrote. |
| [Migrate to a new deployment](#migrate-to-a-new-deployment) | The new release does not read it (its notes say the storage format changed), or the user moves to another bucket or object store. |

From 0.3 to 0.4 is a migration. See [Notes for 0.3 to 0.4](#notes-for-03-to-04).

Every node of a deployment runs the same release. There is no rolling upgrade
and no supported downgrade.

## Before either method

Check these with the user. Do not continue while one fails.

1. `enroute diagnose --config <URI>` exits with status 0, and
   `enroute members --config <URI>` lists every ledger node they run. A
   single node with no `node.advertise_url` lists none.
2. They have volume snapshots of every ledger node's data directory, taken at
   one point in time. For an in-place upgrade, the snapshots are the only way
   back.
3. Their configuration starts on the new release. A node refuses a key it does
   not know, so start one node of the new release against the configuration
   on a staging machine first.
4. Their application works with the new release's contract. Generate clients
   again from the proto files in the new image, `/usr/share/enroute/proto`.

## Upgrade in place

Git and the API stop from step 2 until step 5. Tell the user before step 2.

1. Install the new image or binary on every node.
2. Stop the compute nodes. With `both` nodes, stop every node.
3. Stop the ledger nodes.
4. Start the ledger nodes on the new release.
5. Start the compute nodes on the new release.
6. Run `enroute diagnose`, then clone and push a test repository.

To go back: stop every node, restore the snapshots, and start the old release.
Writes after the snapshots are lost. Never start the old release on disks that
the new release has written to.

## Migrate to a new deployment

The new deployment copies each repository over Git: `CreateRepository` with the
same key, default branch, and object format, then `FetchFromRemote` from the old
deployment's Git URL under `refs/`. The old deployment serves that as it serves
any clone, so it keeps working the whole time. A second fetch copies only what
changed, so copy everything while the old deployment takes writes, then freeze
each repository, fetch it once more, compare, and switch it.

`FetchFromRemote` runs no hooks on the new deployment, so a copy does not start
the user's `post_receive` work, such as CI. It does not copy unreachable
objects, `last_push`, or the `updated_at` of each ref.

### What the user decides or provides

Ask in one message, then stop asking:

1. **The new deployment.** It runs the new release with a new, empty bucket and
   empty ledger disks, and the same hook configuration as the old one. A bucket
   holds one deployment, so never point it at the old bucket. Send it no
   traffic yet. If any repository is SHA-256, set `ledger.allow_sha256 = true`.
2. **Three URLs.** The old deployment's API, the new deployment's API, and the
   Git URL of one repository on the old deployment, with `{key}` where the key
   goes, for example `http://old-git.internal:8080/{key}.git`.
3. **A migration credential.** If the old deployment has hooks enabled, its
   `authorize` hook must give this credential read access to every repository,
   and resolve the Git path in the URL above to the key. `visible_refs` must
   show it every ref: a hidden ref is not copied. Offer to add this to the
   user's endpoint, scoped to the migration, and to remove it afterwards.
4. **How writes stop at cutover.** For example, `authorize` refuses pushes to a
   repository on the old deployment once its cutover starts, and the
   application stops its own API writes to it. Without hooks, take the old
   deployment out of the load balancer.

If the old Git URL is `http` or a private address, set
`git.allow_private_remotes = true` on the new deployment's compute nodes for
the migration.

### Copy

Write the copy as a small program or script in whatever the user already runs:
their application's language with its generated client, or a shell script with
the `enroute` command and `grpcurl`. It fits their URLs, credentials, and
repository list better than a generic tool. For each repository:

1. Read it on the old deployment: `GetRepository`, or `ListRepositories` page by
   page for all of them (`enroute repo list --api <old> --json`). Keep `key`,
   `default_branch`, and `object_format`.
2. `CreateRepository` on the new deployment with the same three values
   (`enroute repo create <key> --api <new> --default-branch <ref>
   --object-format <fmt>`). It is idempotent, so a rerun is safe.
3. `FetchFromRemote` on the new deployment, with `repo.key`, the old Git URL of
   that repository in `remote.url`, the migration credential in `remote.basic`
   (leave `remote` credentials out if the old deployment has no hooks), and
   `prefixes: ["refs/"]`. The `enroute` command has no fetch subcommand, so call
   it through the generated client or `grpcurl` (the server answers
   reflection).
4. Report each repository's result. `outcomes` lists only refs that changed, so
   an empty list means it was already in step. Show the user every
   `STATUS_REJECTED` outcome with its `message`: a check on the new release
   refused that ref. A call that fails as a whole changes nothing; record it,
   go on with the next repository, and report the failures at the end.

Make the program take a list of keys, so the same program does the cutover of
one repository. Rerun it until most repositories report no changes. Each run
fetches only what changed, and retrying a failed call is safe.

Verification compares the refs on both deployments. `enroute refs <key> --api
<url> --json` prints `default_branch` and `refs` (`name`, `object_id`). Sort the
refs by name and compare both outputs, and treat a repository missing on the
new deployment as a difference.

Pitfalls seen in practice:

- `grpcurl -d @` reads the request from standard input. Redirecting its input
  elsewhere, for example `</dev/null` inside a loop, sends an empty request,
  and the server answers `no repository given`.
- Build the request JSON with a JSON tool such as `jq`, not string
  concatenation, so a password with quotes cannot break it.
- In a `while read` loop over repositories, keep other commands from reading
  the loop's input.

### Cut over

For each repository, or for all of them together:

1. Stop writes to it on the old deployment, as the user decided.
2. Copy it once more.
3. Compare its refs on both deployments. They must be identical. If not, a write got through,
   or `visible_refs` hides a ref. Fix that, and go back to step 2.
4. The user sends its Git and API traffic to the new deployment, and allows
   writes again.

Keep the old deployment, without traffic, until the user is sure they will not
go back. To go back after a cutover, send traffic to the old deployment again.
Writes that landed on the new one since then are not there. Copy them back with
`FetchFromRemote` on the old deployment if its release has it, or with
`git clone --mirror` from the new deployment and `git push --mirror` to the old
one.

### Troubleshooting

- **`PERMISSION_DENIED`.** The old deployment's `authorize` refused the
  migration credential.
- **`FAILED_PRECONDITION`.** The old deployment does not speak Git protocol v2,
  or the repository on the new deployment has another object format.
- **`UNAVAILABLE`.** The new deployment cannot reach the old Git URL, or needs
  `git.allow_private_remotes`.

## Notes for 0.3 to 0.4

- 0.4.0 cannot read what 0.3 wrote, and refuses to start on a 0.3 bucket.
  Migrate.
- 0.3 serves Git protocol v2, which `FetchFromRemote` needs. 0.3 has no
  `FetchFromRemote` of its own, so a copy back to 0.3 uses `git push --mirror`.
- Every 0.3 repository is SHA-1. Create each one as SHA-1 on 0.4.
- 0.4 refuses two configuration keys that 0.3 had: `ledger.checkpoint_age` and
  `ledger.checkpoint_concurrency`. Remove them from the new configuration.
- A `ledger` node no longer binds the Git and API ports, and
  `layers.flush_interval` defaults to `30s`. Compare the configuration with the
  0.4 [configuration keys].

[Upgrades and migrations]: https://github.com/enroute-sh/enroute/blob/HEAD/docs/operate/upgrades.md
[configuration keys]: https://github.com/enroute-sh/enroute/blob/HEAD/docs/reference/configuration-keys.md
