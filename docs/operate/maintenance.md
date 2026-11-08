# Maintenance

Enroute has no scheduler to set up. Each node runs its upkeep in the
background. The tasks on this page change the ledger's members: you add a
node, replace one that lost its disk, retire one, or restore the ledger after
a larger failure.

For what each deployment model survives, see
[Deployment profiles](deployment-profiles.md).

## Automatic work

For each repository, one compute node runs this work:

1. Flushes small layers from the replicated log to the object store.
2. Merges layers of a similar size into a larger one. A large layer merges
   again only after the repository grows by about its size.
3. Removes layer files that no repository state names, once they have been
   unused for `layers.sweep_retention` (6 hours). The delay lets slow readers
   finish.

When you delete a repository, the key is free at once. The sweep reclaims the
layer files in the background after the same delay.

Merging layers does not repack objects or reclaim objects from deleted
branches. Delete the repository to reclaim that data.

The keys in `[layers]` and `[ledger]` tune this work. The defaults suit most
deployments. See [Configuration keys](../reference/configuration-keys.md).

## Before you begin

The tasks below use two commands:

- `enroute members` lists the ledger's members, and removes one.
- `enroute diagnose` checks the object store, each member, and the `/status`
  of each live node. It exits with status 1 if a check fails.

Run them on a ledger node, with that node's configuration file. The commands
read only the `[bucket]` table, but the whole file must load, so set the
variables that it names. `enroute members --remove` signs with the key in the
node's `data_dir`, so it works only on a ledger node.

```sh
enroute members --config file:///etc/enroute/enroute.toml
```

In a container, run the commands with `docker exec`:

```sh
docker exec <container> enroute members --config file:///etc/enroute/enroute.toml
```

A node ID is a number. `enroute members`, `--remove`, and `GET /status` give
it in decimal, such as `3810549716048109`. `enroute diagnose` gives the same ID
in hexadecimal, padded to 16 digits: `000d89acbe3958ed`. Match a node by its
URL in the `diagnose` output, or convert the ID:

```sh
printf '%016x\n' 3810549716048109
```

## Check the ledger

Run `enroute diagnose` before and after each task. A healthy ledger of three
nodes reports this:

```text
ok    object store: reachable
ok    members: members [830222279395061, 3810549716048109, 6484904094913937], removed [], 3 alive
ok    ledger node 0002f315300c46f5: alive http://ledger-2.internal:7400
ok    ledger node 000d89acbe3958ed: alive http://ledger-1.internal:7400
ok    ledger node 001709fc64fad591: alive http://ledger-3.internal:7400
ok    node 0002f315300c46f5 /status: http://ledger-2.internal:7400 answered
ok    node 000d89acbe3958ed /status: http://ledger-1.internal:7400 answered
ok    node 001709fc64fad591 /status: http://ledger-3.internal:7400 answered
```

To see which nodes hold one repository's log, ask each ledger node for
`GET /status/repos/<key>`. A node that holds the log answers with its state.
A node that does not answers `404`.

```sh
curl http://ledger-1.internal:7400/status/repos/acme/widgets
```

```json
{"applied":4,"epoch":1,"leading":true,"node":3810549716048109,"repo":"acme/widgets","sequencer":3810549716048109,"term":2,"version":3}
```

Each log has three replicas. In `GET /status`, `consensus.repos` is the
number of logs that a node holds, and `consensus.engine.lagging_logs` is the
number that are not caught up. See [Observability](observability.md#node-status).

## Add a ledger node

Add a node for capacity: new repositories are placed on it.

1. Give the node a durable disk with an empty `data_dir`.
2. Give it the ledger node configuration of the other nodes, with the same
   `bucket`, its own `advertise_url`, and its `zone`. See
   [Deployment profiles](deployment-profiles.md#high-availability).
3. Start the node. It finds the ledger in the bucket and adds itself as a
   member.
4. Verify that `enroute members` lists one more ID, and that
   `enroute diagnose` reports the node `alive`.

```text
members: 830222279395061, 3810549716048109, 6484904094913937, 7789025947870438
removed: none
```

A log that has fewer than three replicas grows onto the new node. A log that
has three stays where it is, unless two of them share a zone and the new node
is in a zone the log lacks: one of those two moves to it.

## Replace a node that lost its disk

A node that stops and starts again on its disk catches up by itself, and
needs nothing from you. A node that loses its `data_dir` comes back as a new
node, with a new ID. The old ID stays a member until you remove it, and it
counts against the majority of each of its logs. While it is a member, each of
its logs runs on two replicas, and one more loss among them stops that log's
writes. Replace the node promptly.

1. Start the node again with an empty `data_dir`. It can keep its
   `advertise_url`. It joins as a new member.
2. Find the old ID. `enroute members` lists both, and `enroute diagnose`
   reports the old one as `not answering`:

   ```text
   ok    members: members [830222279395061, 3810549716048109, 4172151754867845, 6484904094913937], removed [], 3 alive
   FAIL  ledger node 0002f315300c46f5: not answering http://ledger-2.internal:7400
   ```

   Here the old ID is `0002f315300c46f5`, which is `830222279395061` in
   decimal.
3. Remove the old ID:

   ```sh
   enroute members --config file:///etc/enroute/enroute.toml --remove 830222279395061
   ```

   ```text
   members: 3810549716048109, 4172151754867845, 6484904094913937
   removed: 830222279395061
   ```

4. Verify. `enroute diagnose` reports every member `alive`, and each log that
   was on the old node gets a new replica, usually within seconds. Check that
   `consensus.engine.lagging_logs` is `0` on each ledger node, and that three
   nodes answer `GET /status/repos/<key>` for a repository that was on the old
   node.

The command refuses to remove a node if the members that remain would not have
a live majority. `--force` removes it anyway. Use `--force` only when the
remaining nodes cannot form a majority again, and read
[Recover from losing a majority](#recover-from-losing-a-majority) first.

## Retire a ledger node

To take a working node out of the ledger, remove it while it runs, so that
its logs move off it first.

1. Make sure that at least three members remain after the removal. If the
   ledger has three nodes, [add one](#add-a-ledger-node) first.
2. Remove the node, from any ledger node:

   ```sh
   enroute members --config file:///etc/enroute/enroute.toml --remove 4172151754867845
   ```

3. Wait until the node holds no logs: in its `GET /status`,
   `consensus.repos` is `0`. Each of its logs gets a new replica on another
   node.
4. Stop the node.

A removed node cannot join again. If it starts on its old `data_dir`, it logs
`node <id> was removed from the ledger` and retries. To use the machine again,
start it with an empty `data_dir`. It then joins as a new node.

## Recover from losing a majority

A repository loses its majority only if two of its three ledger nodes lose
their disks before you replace the first one. Creating and deleting
repositories also needs a majority of all ledger nodes. Without a majority, the
affected repositories cannot accept writes. There is no tool yet that forces
the remaining nodes to go on without a majority.

The recovery restores the whole ledger from volume snapshots of the ledger
nodes' `data_dir`, all taken at one point in time. The writes after the
snapshots are lost.

1. Stop every ledger node.
2. Restore each ledger node's `data_dir` from the snapshots of one point in
   time.
3. Start every ledger node.
4. Verify with `enroute diagnose`, and read back a repository that was written
   to recently, for example with `enroute refs <key>`.

The restored state names layer files in the bucket. The sweep keeps an
unnamed file for `layers.sweep_retention` (6 hours), so take the snapshots
more often than that: a restore from an older snapshot can name files that are
gone.

To make this unlikely, keep ledger disks durable, spread the nodes over
failure domains, and replace a lost node promptly.

## Monitoring

Enroute is observed through its OpenTelemetry traces, its logs, and
`enroute diagnose`. See [What to watch](observability.md#what-to-watch).
