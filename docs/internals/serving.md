# Serving

This page describes what the compute role exposes to clients and to the host
application, and how the API is organized. For the exact contracts, read the
[API contract](../reference/the-contract.md), the
[hook reference](../reference/hooks.md) and
[Push to a remote](../reference/push-to-a-remote.md). This page does not repeat
them.

The compute serves two ports. The Git port speaks smart HTTP. The API port speaks gRPC. Both read from the same snapshots
([Read path](read-path.md)).

## Smart HTTP

The Git router serves three paths per repository: `info/refs`,
`git-upload-pack` and `git-receive-pack`. A request for dumb HTTP gets `403`.
The ref advertisement for a push is a write access, so a reader cannot learn
which refs it could push to.

### Fetch

Protocol v2 is the default when the client asks for it. Enroute advertises
`ls-refs=unborn`, `fetch=shallow filter` and the repository's `object-format`
(`sha1` or `sha256`). A request in another format is refused, as Git refuses
it.

- `ls-refs` supports `symrefs`, `peel`, `unborn` and `ref-prefix`.
- `fetch` supports `want`, `have`, `done`, `thin-pack`, `ofs-delta`,
  `include-tag`, `filter`, `shallow`, `deepen`, `deepen-relative` and
  `wait-for-done`. It accepts `no-progress` and `sideband-all` and ignores
  them.
- `fetch` rejects `deepen-since`, `deepen-not`, `want-ref` and `packfile-uris`
  with an error.
- A request body can be gzip. The size limit is 256 MiB, before and after
  decompression.

A client that does not ask for v2 gets protocol v0 or v1.
The advertisement offers `multi_ack_detailed`, `side-band-64k`, `thin-pack`,
`ofs-delta`, `shallow`, `deepen-relative`, `no-progress`, `include-tag`,
`allow-tip-sha1-in-want`, `allow-reachable-sha1-in-want` and `filter`. The v0
path uses the same planner and pack writer as v2.

### Push

The advertisement of `git-receive-pack` offers `report-status`,
`delete-refs`, `side-band-64k`, `quiet`, `atomic`, `ofs-delta` and the
repository's `object-format`.

- With `atomic`, one refused command fails every command of the push
  ([Write path](write-path.md#pushes-with-hooks)).
- The node keeps a request body under 16 MiB in memory. It spools a larger
  body to a scratch file. Gzip request bodies work.
- A push to a missing repository creates it when `git.create_on_push` is on (the
  default). When it is off, the advertisement and the push answer not found,
  and nothing is created. An API commit to a missing repository always answers
  not found.

## Access control

Enroute authenticates nothing. The host decides, through hooks that the
compute calls before it serves anything. The compute makes each hook call as a
signed HTTP `POST` to one endpoint. The
[hook reference](../reference/hooks.md) lists the messages.

Every hook is opt-in. The compute calls only the hooks that `hooks.enabled`
lists. A hook that is off takes its default:

| Hook | Default when off |
|---|---|
| `authorize` | Allow read and write. The URL path, without `.git`, is the repository key. A path that is not a valid key is a `400`. |
| `visible_refs` | Every ref is visible. |
| `pre_receive` | Every command is allowed. |
| `post_receive` | No call. |

### Authorize to repository key

With `authorize` on, the compute passes the path as the client spelled it, the
headers and the access (read or write). It rejects a path that is empty, longer
than 1024 bytes, or unsafe before it asks the host. The host answers with a
repository key and an actor, or with a denial (`401` with a challenge, `403`,
`404`). The key replaces the path, so a URL can name a repository that has
another key. The grant also carries opaque context that the compute replays to
the later hooks of the same request. Enroute reads neither the actor nor the
context.

### Visibility hides names, not objects

`visible_refs` runs before each ref advertisement, and before a fetch that uses
`include-tag`. The host returns the refs that the caller may see. The compute
hides the others. It hides `HEAD` together with the branch that `HEAD` names.
It does not check wants against hidden refs, so a client that knows the object
id of a hidden tip can still fetch it, as with `uploadpack.hideRefs` in Git. A
secret that must survive a guessed object id needs a repository of its own.

### Push hooks

`pre_receive` runs before any ref moves. `post_receive` runs after the refs
moved. By default, a push commits its objects and refs in one log entry, after
`pre_receive`, and the hook cannot read the pushed objects. With
`hooks.pre_receive_mode = "objects"`, the push commits the objects first, so the
hook can read them through the API on any node, and the refs in a second entry
([Pushes with hooks](write-path.md#pushes-with-hooks)). Each command carries a
`force` flag. The compute works it out during ingest, from the pushed commits
and the snapshot, and treats anything that it cannot prove to be a
fast-forward as a force. `UpdateRefs` on the API runs no hooks, because the
caller there is the application.

## The API port

The API is `enroute.api.v1alpha1`: the ref, repository, object, commit and
sync services, with gRPC reflection. It has two parts.

- The core reads a snapshot and answers with plain values: refs, objects,
  trees, history, diffs, ref updates and server-side commits. It does not
  depend on gRPC.
- The gRPC surface turns messages into calls to the core, and the results into
  messages. The hooks use the same conversions and the same rules for
  repository keys, so the API and the hooks always agree.

Every API call takes a read barrier first, so a caller reads what it wrote.
The API port authenticates nothing. Keep it on a network that only the
application can reach ([Security](../operate/security.md)). Engine errors reach
the caller as `Internal` without detail. The log carries the detail.

## Push to a remote

`SyncService.PushToRemote` makes Enroute a Git client. The compute builds the pack
with the code that answers a fetch, from a snapshot. It keeps nothing about the
remote: each call brings the URL, the
credentials and the refs.

- It checks that the remote offers `report-status`, and `atomic` if the call
  asks for it. Otherwise the call fails as unsupported.
- It works out each ref before it sends anything. A ref that the remote
  already holds at the requested value is answered locally.
- It dials only remotes that the deployment allows: `https`, public
  addresses, no redirects. `git.allow_private_remotes` lifts the first two
  limits. The [reference](../reference/push-to-a-remote.md) lists the limits
  and the timeouts.
