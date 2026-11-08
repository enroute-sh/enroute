# Configuration keys

See [Configuration](../operate/configuration.md) for loading and environment
expansion. Keys without defaults are required. Enroute refuses a key it does
not know.

A size is a string with a unit, such as `"512 MiB"` or `"2 GiB"`. A bare number
is bytes. `MiB` and `GiB` are powers of 1024, and `MB` and `GB` are powers of
1000. A duration is a string with a unit, such as `"300ms"`, `"15m"`, or
`"6h"`.

## Main configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `bucket.uri` | — | Permanent object store. A `file://` bucket takes no query |
| `bucket.credentials` | none | Store access key and secret |
| `git.allow_private_remotes` | `false` | Permit HTTP and private remote addresses for `PushToRemote` and `FetchFromRemote` |
| `git.create_on_push` | `true` | A push to a repository that does not exist creates it. `false` means only `CreateRepository` creates repositories |
| `git.default_object_format` | `"sha1"` | `"sha1"` or `"sha256"`: the object format of a repository a push creates, and of one `CreateRepository` creates without `object_format`. `"sha256"` needs `ledger.allow_sha256` |
| `git.max_object_size` | `"100 MiB"` | A push or API commit with a larger object fails, and so does a delta that builds one. A push gets the reason in its report |
| `hooks.enabled` | `[]` | Hooks Enroute calls: any of `authorize`, `visible_refs`, `pre_receive`, `post_receive` |
| `hooks.endpoint_url` | none | URL your application answers hooks on. Required when `enabled` is not empty |
| `hooks.pre_receive_mode` | `"commands"` | `"commands"` or `"objects"`. `"objects"` makes the pushed objects readable through the API during `pre_receive`, at the cost of a second log entry per push. Ignored unless `pre_receive` is enabled. See [Read the pushed objects](hooks.md#read-the-pushed-objects) |
| `hooks.signing_key` | none | PEM Ed25519 private key, PKCS#8. Required when `enabled` is not empty |
| `hooks.timeout` | `"10s"` | Hook response timeout. Longer than zero |
| `listen.api` | `0.0.0.0:50051` | gRPC listener. A `ledger` node does not bind it |
| `listen.git` | `0.0.0.0:8080` | Git HTTP listener. A `ledger` node does not bind it |
| `telemetry.endpoint` | none | OTLP trace endpoint. Without `[telemetry]`, spans go to the console only |
| `telemetry.headers` | none | Headers each export carries |
| `telemetry.sample_ratio` | `1.0` | Fraction of traces to keep, from `0.0` to `1.0` |

Every hook is opt-in. Without `[hooks]`, or with `enabled = []`, Enroute calls
no hook, and each hook takes its default. Enroute refuses an unknown name in
`enabled`. See [Enable hooks](hooks.md#enable-hooks).

## Node

The `[node]` table configures the node that runs in the Enroute process. The
node keeps its objects in `bucket`, and its log and state in `data_dir`.

| Key | Default | Purpose |
| --- | --- | --- |
| `node.data_dir` | — | Local directory for the node's logs, state, and layer copies |
| `node.role` | `both` | `both`, `ledger`, or `compute` |
| `node.internal_listen` | `0.0.0.0:7400` | Listener for other nodes and for `GET /status` |
| `node.advertise_url` | none | URL of `internal_listen` for other nodes. Set it to replicate |
| `node.zone` | none | Failure domain of a ledger node, such as an availability zone. Each repository's log puts its replicas in distinct zones first. See [Spread ledger nodes over zones](../operate/deployment-profiles.md#spread-ledger-nodes-over-zones) |
| `node.cpu_threads` | `0` | Workers for CPU-bound work. `0` means one for each core |
| `node.cache` | `"512 MiB"` | Object and entry cache size |
| `node.page_cache` | `"256 MiB"` | Layer page cache size |
| `node.disk_cache` | `0` | Local layer cache size. `0` means no limit |
| `node.max_open_files` | none | Open files for layer storage: layer files and scratch files (push spools, spill files, layers being built), split 3:1. A scratch file waits up to 30 s for a free slot, then fails with a retry error. If unset, the open-file limit less a reserve (the larger of 256 and a quarter of the limit). A higher value is lowered to that ceiling |
| `node.evict_after` | `"15m"` | Time before the node drops an unused repository view |
| `node.console_buffer_lines` | `128000` | Console log lines that can wait for stdout. When the buffer is full, new lines are dropped and the node logs how many |

Keep `data_dir` on a disk that persists. A node that loses this directory
starts again as a new node. The image declares `/var/lib/enroute` as a volume
for it.

A `both` node runs the replicated ledger and serves Git and the API. A `ledger`
node runs only the ledger, needs `advertise_url`, and serves only
`internal_listen`. A `compute` node serves Git and the API, and finds the
ledger's members in the bucket.

## Store IO

A node has one pool of concurrent requests to `bucket`. Each kind of work has
a share of it. A kind can always use its `reserve`, even when the other kinds
hold every other request. It never holds more than its `limit`. Requests that
are not reserved go to the kinds that wait, in proportion to their `weight`.
`GET /status` shows each kind's requests in flight, its waiters, and a
histogram of how long its requests waited (`store_io`).

| Key | Default | Purpose |
| --- | --- | --- |
| `store_io.slots` | `256` | Concurrent requests to `bucket` for the whole node. The reserves together must not be more than this |
| `store_io.commit` | `{ reserve = 16, limit = 128, weight = 4 }` | Writes that a push or a deletion waits on |
| `store_io.foreground` | `{ reserve = 8, limit = 128, weight = 2 }` | Reads that a request waits on |
| `store_io.background` | `{ reserve = 4, limit = 128, weight = 1 }` | Hydration of local copies, compaction, flushes and sweeps |

A kind's table names all three keys, for example
`background = { reserve = 4, limit = 64, weight = 1 }`. A kind left out keeps
its default.

## Layers

The `[layers]` table sets how a compute node writes, merges, and deletes layers.
A small push goes into the replicated log as an inline layer. The node later
flushes inline layers to `bucket`, and merges small layers into larger ones.
The sweep deletes layer files that no repository state names.

| Key | Default | Purpose |
| --- | --- | --- |
| `layers.merge_factor` | `8` | Number of stored layers of about one size that a merge takes. `0` means never. Not `1` |
| `layers.inline_layer` | `"1 MiB"` | Largest layer that goes into the log. `0` means every push uploads first |
| `layers.max_inline` | `"64 MiB"` | Size of inline layers a repository can hold before a push uploads first |
| `layers.flush_interval` | `"30s"` | Flush inline layers when the oldest is this old |
| `layers.flush_max_layers` | `256` | Flush inline layers when there are this many. At least `1` |
| `layers.sweep_interval` | `"1h"` | Time between sweeps of each repository |
| `layers.sweep_retention` | `"6h"` | Time to keep a layer file that nothing names |

## Ledger

The `[ledger]` table applies to nodes that run the ledger.

| Key | Default | Purpose |
| --- | --- | --- |
| `ledger.state_memory` | `"512 MiB"` | Block cache and memtables of the state store |
| `ledger.max_inline_layer` | `"1 MiB"` | Largest inline layer to accept. A push that carries a larger layer is refused. `0` refuses every inline layer, so no repository content goes through the ledger. Keep `layers.inline_layer` on every compute node at most this |
| `ledger.allow_sha256` | `false` | Create SHA-256 repositories. Turn it on once every node runs a release that reads them; an older node cannot |

The engine defines these defaults. A key that you leave out takes the engine's
value.

## Command line

| Command | Purpose |
| --- | --- |
| `enroute [serve] --config <URI>` | Run the node. `serve` is the default |
| `enroute dev [--hooks <URL>] [--enable <HOOK>]` | Run a local node with a generated configuration. See [Quickstart](../quickstart.md) |
| `enroute members --config <URI>` | List the ledger members in `bucket` |
| `enroute members --config <URI> --remove <ID> [--force]` | Remove a ledger member for good |
| `enroute diagnose --config <URI>` | Check the object store, the ledger members and which answer, and the `/status` of each live node. Exits non-zero if a check fails |
| `enroute repo create\|get\|list\|delete` | Manage repositories over the API |
| `enroute refs <KEY> [--prefix <P>]` | Print each ref as `<oid>\t<refname>` |
| `enroute commit <KEY> --branch <REF> -m <MSG> --author <ID>` | Make a commit with `CreateCommit`. Takes `--put`, `--delete`, `--exec`, `--symlink`, `--expect`, and `--expect-absent` |

`--config` also reads `ENROUTE_CONFIG`, and goes before or after the
subcommand. `diagnose` also takes `--bucket <URI>` in place of `--config`.

The `repo`, `refs`, and `commit` commands call the API at `--api`, or
`ENROUTE_API`, by default `http://127.0.0.1:50051`. `--json` prints JSON.
Each command prints tab-separated lines:

```console
$ enroute repo create acme/gadgets --default-branch refs/heads/trunk
acme/gadgets	refs/heads/trunk	never pushed
$ enroute repo get acme/widgets
acme/widgets	refs/heads/main	pushed 2026-11-06T13:53:20Z
$ enroute refs acme/widgets --prefix refs/heads/
355317f0a3db29fdca481531c12b2f3fff247229	refs/heads/feature
ade924f34031005a3d4e72b70b509502576dd01f	refs/heads/main
$ enroute commit acme/widgets --branch main -m "Add notes" \
    --author "Ada Lovelace <ada@example.com>" --put notes.md="hello"
commit 41511accabd3ad65408dbf16233ff5d1dd21953b
tree   6cce16521601999b0a295480206f906d74514724
parent 9b2b9b51c3d9fb42dd5a02293565b96862bb7cea
```

A failed call prints the gRPC status and its message, such as
`Error: NotFound: no such repository`, and exits with status 1. The statuses
are in the [API contract](the-contract.md).

### `enroute dev`

| Option | Environment | Default | Purpose |
| --- | --- | --- | --- |
| `--host <ADDR>` | `ENROUTE_HOST` | `127.0.0.1` | Bind Git and the API to this address. A container needs `0.0.0.0` |
| `--port-base <N>` | `ENROUTE_DEV_PORT_BASE` | none | Listen on `N` (Git), `N+1` (API), and `N+2` (internal) in place of `8080`, `50051`, and `7400` |
| `--dir <PATH>` | `ENROUTE_DEV_DIR` | `.enroute/dev` | Directory for the node data and objects |
| `--clean` | none | off | Delete the directory first. Only a directory with a `.enroute-dev` marker file |
| `--hooks <URL>` | `ENROUTE_HOOKS_URL` | none | Call hooks at this URL, signed with a generated key |
| `--enable <HOOK>` | `ENROUTE_HOOKS_ENABLE` | all four | Call only these hooks. Repeat the option or separate names with commas |
| `--pre-receive-mode <MODE>` | `ENROUTE_PRE_RECEIVE_MODE` | `commands` | Set `hooks.pre_receive_mode` to `commands` or `objects`. Requires `--hooks` |

The internal listener binds to `127.0.0.1`, and `git.allow_private_remotes` is
on.

### `.env` files

Every command reads `ENROUTE_*` values from `.env` and `.env.local` in the
current directory, for example `ENROUTE_CONFIG` and `ENROUTE_API`. A flag wins
over the environment, the environment wins over `.env.local`, and `.env.local`
wins over `.env`.
