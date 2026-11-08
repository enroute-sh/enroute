# API contract

Your application calls Enroute over gRPC on `listen.api` (`:50051` by default).
This page describes each service and RPC: its fields, its rules, the statuses
it returns, and an example call with its output.

For the hooks that Enroute calls on your application, see the
[hook reference](hooks.md). For the proto files, see
[Protocol Buffers](proto.md).

## Transport and authentication

Enroute serves plain HTTP/2 on `listen.api`. Terminate TLS in front of it.

Enroute authenticates nothing on this listener. Your application is its only
caller, and any client that can reach the port can act as your application.
Keep it unreachable from anywhere else. See [Security](../operate/security.md).

The listener serves gRPC reflection, so a tool such as [`grpcurl`] can call it
without the proto files.

[`grpcurl`]: https://github.com/fullstorydev/grpcurl

## Services

| Service | RPCs | Purpose |
| --- | --- | --- |
| [`RepositoryService`](#repositoryservice) | `CreateRepository`, `GetRepository`, `ListRepositories`, `DeleteRepository` | Create, inspect, list, and delete repositories |
| [`RefService`](#refservice) | `ListRefs`, `UpdateRefs`, `IsAncestor` | Read refs, move them, and ask about ancestry |
| [`ObjectService`](#objectservice) | `GetObject`, `ListTree`, `ListCommits`, `DiffCommit`, `FindMergeBases` | Read objects, trees, history, and diffs |
| [`CommitService`](#commitservice) | `CreateCommit` | Make a commit from file changes |
| [`SyncService`](#syncservice) | `PushToRemote`, `FetchFromRemote` | Send refs to, or bring refs from, another Git server |

Every service is in the `enroute.api.v1alpha1` package. `v1alpha1` can change
in any release before 1.0.

## How to read the examples

The examples call a local `enroute dev` node with `grpcurl`. They use one
repository, `acme/widgets`, that holds two branches:

| Ref | Commit | History |
| --- | --- | --- |
| `refs/heads/main` | `ade924f…` | "Start the widgets library", then "Add the widget function" |
| `refs/heads/feature` | `355317f…` | `main`, then "Return the answer" |

A call looks like this:

```sh
grpcurl -plaintext -d '{"repo": {"key": "acme/widgets"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RefService/ListRefs
```

The requests and responses are JSON in the canonical
[ProtoJSON mapping][protojson]:

| Rule | Result |
| --- | --- |
| Field names | `lowerCamelCase`, so `old_object_id` prints as `oldObjectId` |
| Enums | The value name as a string, such as `"OBJECT_KIND_BLOB"` |
| `bytes` | Base64 |
| 64-bit integers | Strings, such as `"19"` |
| Unset fields, `false`, zero, and empty strings | Omitted |

The last rule matters in this contract. An unset field is how the contract
spells absence, so a missing key in a response is information. For example,
`{}` from `IsAncestor` means `isAncestor` is `false`.

[protojson]: https://protobuf.dev/programming-guides/json/

## Conventions

These rules hold for every RPC on this page.

### Repository keys

Repository keys are application-owned, opaque identifiers. A key is at most
256 bytes, in one or more segments separated by `/`, such as `acme/widgets`.
Each segment holds ASCII letters, digits, `-`, `_`, or `.`, and starts and ends
with a letter or digit. A key holds no empty, `.`, or `..` segment. Keys are
case-sensitive and compared byte for byte. A key is unique across the
deployment.

If your application renames or moves repositories, use a stable ID such as a
UUID, ULID, or database ID as the key. The `authorize` hook maps Git URLs to
keys. Without `authorize`, the key is the URL path without `.git`.

A push to a repository that does not exist creates it, under the key that
`authorize` granted, or under the path key when `authorize` is not enabled.
A fetch, clone, or `ls-remote` of a missing repository is not found. Set
`git.create_on_push = false` to create repositories only with
`CreateRepository`.

### Object IDs

An `ObjectId` is a message with one field, `hex`: 40 lowercase hexadecimal
characters in a SHA-1 repository, and 64 in a SHA-256 one. An optional object
ID is an unset field, never forty zeros and never an empty string. A `hex`
that is not an object ID fails the call with `INVALID_ARGUMENT`.

### Consistency

Every read sees every write that finished before the read began, through any
compute node. A call that returns after a write, such as `CreateCommit` or
`UpdateRefs`, is followed by reads that see it.

### Pages and partial results

`ListRepositories` and `ListCommits` return pages. Send `next_page_token` back
as `page_token` until it is empty. Do not treat a short page as the last page.
A page token is opaque, and it belongs to the request that produced it.

`ListTree`, `DiffCommit`, and `FindMergeBases` stop at a server limit and say
so with a flag. Check the flag before you present a result as complete.

| RPC | Limit | Signal |
| --- | --- | --- |
| `ListRepositories` | 100 repositories per page | `next_page_token` |
| `ListCommits` | 100 commits per page | `next_page_token` |
| `ListTree` | 50,000 entries | `truncated` |
| `DiffCommit` | 50,000 changes, or 10,000 tree reads | `truncated` |
| `FindMergeBases` | 250,000 commits walked | `exhausted` |
| `CreateCommit` | 32 MiB per request | `OUT_OF_RANGE` |

### Statuses

A call fails with a gRPC status. These statuses are common to every RPC; each
RPC lists the others that it returns.

| Status | Cause |
| --- | --- |
| `INVALID_ARGUMENT` | The request has no `repo`, a key that breaks the rules above, a malformed object ID, or a field that breaks a rule of the RPC. The message says which |
| `NOT_FOUND` | The repository does not exist (`no such repository`), or an object that the request names is not in it (`no object <id>`) |
| `INTERNAL` | Storage or the ledger failed. The message is `internal error`, and the node logs the cause at `ERROR` |
| `UNAVAILABLE` | The compute node cannot reach the ledger (`no ledger node is alive`), or the connection to Enroute failed. Retry with backoff |

A refusal that is part of an answer, such as a rejected ref update, is not a
status. The RPC succeeds and the response describes the refusal.

## `RepositoryService`

Creates, reads, lists, and deletes repositories. Enroute stores a repository
under the key that you give it and holds no owner, name, or permission for it.

### `Repository`

| Field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | The key of the repository. |
| `default_branch` | `string` | The branch that `HEAD` points to, such as `refs/heads/main`. |
| `last_push` | `Timestamp` | The time of the most recent push, in whole seconds. Unset before the first push. |
| `object_format` | `ObjectFormat` | `OBJECT_FORMAT_SHA1` or `OBJECT_FORMAT_SHA256`. Fixed at creation. |

### `CreateRepository`

Creates a repository, or returns the existing one with the same key.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `default_branch` | `string` | The branch `HEAD` names, under `refs/heads/`. Empty means `refs/heads/main`. |
| `object_format` | `ObjectFormat` | `OBJECT_FORMAT_SHA1` or `OBJECT_FORMAT_SHA256`. Unspecified means `git.default_object_format`. |

The response holds the `Repository`.

- **Idempotent.** A second call with the same key returns the existing
  repository and changes nothing. A call that races an identical one gets the
  same answer, so a retry needs no bookkeeping.
- **Creation only.** `default_branch` and `object_format` are read only by the
  call that creates the repository. A later call that names different values
  does not fail and does not change the repository. Compare the response with
  the request if the difference matters.
- **Before the first push.** To give a repository a default branch other than
  `refs/heads/main`, call `CreateRepository` before the first push creates it.

| Status | Cause |
| --- | --- |
| `INVALID_ARGUMENT` | `default_branch` is not under `refs/heads/` |
| `FAILED_PRECONDITION` | `object_format` is SHA-256 and the ledger does not allow it (`ledger.allow_sha256`) |

```sh
grpcurl -plaintext \
  -d '{"repo": {"key": "acme/gadgets"}, "defaultBranch": "refs/heads/trunk"}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RepositoryService/CreateRepository
```

```json
{
  "repository": {
    "repo": {
      "key": "acme/gadgets"
    },
    "defaultBranch": "refs/heads/trunk",
    "objectFormat": "OBJECT_FORMAT_SHA1"
  }
}
```

### `GetRepository`

Returns one repository.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |

The response holds the `Repository`. A missing repository is `NOT_FOUND`.

```sh
grpcurl -plaintext -d '{"repo": {"key": "acme/widgets"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RepositoryService/GetRepository
```

```json
{
  "repository": {
    "repo": {
      "key": "acme/widgets"
    },
    "defaultBranch": "refs/heads/main",
    "lastPush": "2026-11-06T13:53:20Z",
    "objectFormat": "OBJECT_FORMAT_SHA1"
  }
}
```

### `ListRepositories`

Lists repositories in key order, one page at a time.

| Request field | Type | Description |
| --- | --- | --- |
| `limit` | `uint32` | The most repositories to return. Zero, or more than 100, means 100. |
| `page_token` | `string` | The `next_page_token` of the previous page. Empty starts at the first key. |
| `prefix` | `string` | Return only keys that start with these bytes. It need not end at a segment boundary. Empty returns every key. |

| Response field | Type | Description |
| --- | --- | --- |
| `repositories` | `Repository[]` | The page, in byte order of the keys. |
| `next_page_token` | `string` | Where the next page starts. Empty only on the last page. |

- **Byte order.** Keys are sorted and compared byte for byte, so `Acme/x` comes
  before `acme/x`.
- **Stable across pages.** A repository created during a walk appears on a
  later page. It does not move one that you have already read.
- **Short pages.** A deleted repository can leave a page short. Follow
  `next_page_token` until it is empty.
- **One prefix per walk.** A page token belongs to the prefix that produced
  it.

| Status | Cause |
| --- | --- |
| `INVALID_ARGUMENT` | `page_token` is not a token, `prefix` holds a character that no key holds, or the token is outside `prefix` |

```sh
grpcurl -plaintext -d '{"prefix": "acme/", "limit": 1}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RepositoryService/ListRepositories
```

```json
{
  "repositories": [
    {
      "repo": {
        "key": "acme/gadgets"
      },
      "defaultBranch": "refs/heads/trunk",
      "objectFormat": "OBJECT_FORMAT_SHA1"
    }
  ],
  "nextPageToken": "acme/gadgets"
}
```

### `DeleteRepository`

Deletes a repository.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |

The response is empty.

- **Idempotent.** Deleting a key that does not exist succeeds. The response
  does not say whether a repository was there.
- **The key is free at once.** When the call returns, the key names no
  repository. `CreateRepository` with the same key, or a push to it, makes a
  new, empty repository. It does not bring back the old one.
- **Storage is reclaimed later.** Enroute removes the layer files in the
  background, after `layers.sweep_retention` (6 hours by default). See
  [Maintenance](../operate/maintenance.md).

```sh
grpcurl -plaintext -d '{"repo": {"key": "acme/gadgets"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RepositoryService/DeleteRepository
```

```json
{}
```

## `RefService`

Reads refs and moves them. A ref can only point to an object that the
repository already stores, so these calls carry no pack. To add objects, push
them over Git or make a commit with [`CreateCommit`](#createcommit).

### `ListRefs`

Returns the refs of a repository and the branch that `HEAD` points to.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `prefixes` | `string[]` | Return only refs that start with one of these. Empty returns every ref. |

| Response field | Type | Description |
| --- | --- | --- |
| `refs` | `Ref[]` | The refs, sorted by name. |
| `default_branch` | `string` | The branch `HEAD` points to. `HEAD` is not one of `refs`. |

| `Ref` field | Type | Description |
| --- | --- | --- |
| `name` | `string` | The full refname, such as `refs/heads/main`. |
| `object_id` | `ObjectId` | The object the ref points to. |
| `updated_at` | `Timestamp` | When the ref last moved, by the clock of the node that proposed the change. Times can go back when node clocks differ. |

`ListRefs` returns every ref, whatever `visible_refs` would hide from a Git
client: the caller is your application.

```sh
grpcurl -plaintext -d '{"repo": {"key": "acme/widgets"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RefService/ListRefs
```

```json
{
  "refs": [
    {
      "name": "refs/heads/feature",
      "objectId": {
        "hex": "355317f0a3db29fdca481531c12b2f3fff247229"
      },
      "updatedAt": "2026-11-06T13:53:20Z"
    },
    {
      "name": "refs/heads/main",
      "objectId": {
        "hex": "ade924f34031005a3d4e72b70b509502576dd01f"
      },
      "updatedAt": "2026-11-06T13:53:20Z"
    }
  ],
  "defaultBranch": "refs/heads/main"
}
```

### `UpdateRefs`

Moves, creates, and deletes refs. Each update is a compare-and-swap on the
ref's current value.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `updates` | `RefUpdate[]` | The updates. Outcomes come back in this order. |
| `atomic` | `bool` | Land all updates or none, as `git push --atomic` does. |

| `RefUpdate` field | Type | Description |
| --- | --- | --- |
| `refname` | `string` | The full refname. |
| `old_object_id` | `ObjectId` | What the ref must hold now. Unset means the ref must not exist. |
| `new_object_id` | `ObjectId` | Where the ref moves. Unset deletes the ref. |

The three shapes of an update:

| Update | `old_object_id` | `new_object_id` | Lands when |
| --- | --- | --- | --- |
| Create | Unset | Set | The ref does not exist |
| Move | Set | Set | The ref holds `old_object_id` |
| Delete | Set | Unset | The ref holds `old_object_id` |

The response holds one `RefUpdateOutcome` for each update, in request order.
An outcome has the `refname`, and a `rejection` if the update did not land. A
rejection is an outcome, not a call failure: the RPC succeeds when some
updates land and others are rejected.

| Rejection `reason` | Cause |
| --- | --- |
| `REASON_NON_FAST_FORWARD` | The ref does not hold `old_object_id`. Another writer moved it first. Enroute does not retry |
| `REASON_ALREADY_EXISTS` | A create found the ref already there |
| `REASON_MISSING_OBJECTS` | `new_object_id` is not in the repository, or a branch's new target is not a commit |
| `REASON_INVALID_REFNAME` | `refname` is not a refname that Git accepts |
| `REASON_REPEATED_REF` | The call updates the same ref more than once |
| `REASON_DELETE_CURRENT` | The update deletes the branch that `HEAD` points to |
| `REASON_ATOMIC_FAILURE` | The call is `atomic`, and another update in it was rejected |
| `REASON_UNSPECIFIED` | Another refusal. `message` holds the reason |

- **Compare-and-swap, not ancestry.** `REASON_NON_FAST_FORWARD` means the ref
  no longer holds `old_object_id`. When it does, the move lands even if
  `new_object_id` is not a descendant: Enroute moves a ref backwards when told
  to. Ask [`IsAncestor`](#isancestor) first if your policy needs a
  fast-forward.
- **No hooks.** `UpdateRefs` runs no `pre_receive` and no `post_receive`. The
  caller is your application, so apply your policy before the call.
- **Git's rules.** Enroute lands the updates as a Git push with no pack. A ref
  under `refs/heads/` must point to a commit. Other refs can point to any
  stored object. Enroute stores any valid refname, including `refs/notes/*`
  and namespaces that your application invents.

| Status | Cause |
| --- | --- |
| `INVALID_ARGUMENT` | A delete has no `old_object_id`. Nothing changes |

In this call, the create of `refs/heads/release` lands. The move of `main` is
rejected because `main` does not hold the `old_object_id` that the call names:

```sh
grpcurl -plaintext -d '{
  "repo": {"key": "acme/widgets"},
  "updates": [
    {"refname": "refs/heads/release",
     "newObjectId": {"hex": "ade924f34031005a3d4e72b70b509502576dd01f"}},
    {"refname": "refs/heads/main",
     "oldObjectId": {"hex": "355317f0a3db29fdca481531c12b2f3fff247229"},
     "newObjectId": {"hex": "355317f0a3db29fdca481531c12b2f3fff247229"}}
  ]}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RefService/UpdateRefs
```

```json
{
  "outcomes": [
    {
      "refname": "refs/heads/release"
    },
    {
      "refname": "refs/heads/main",
      "rejection": {
        "reason": "REASON_NON_FAST_FORWARD"
      }
    }
  ]
}
```

The same two kinds of update with `"atomic": true`, after `release` exists:
the create is rejected, so the move of `main` is rejected with it, and no ref
moves:

```json
{
  "outcomes": [
    {
      "refname": "refs/heads/main",
      "rejection": {
        "reason": "REASON_ATOMIC_FAILURE"
      }
    },
    {
      "refname": "refs/heads/release",
      "rejection": {
        "reason": "REASON_ALREADY_EXISTS"
      }
    }
  ]
}
```

### `IsAncestor`

Reports whether one commit is in the history of another.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `ancestor_commit_id` | `ObjectId` | Required. The possible ancestor. |
| `descendant_commit_id` | `ObjectId` | Required. The possible descendant. |

| Response field | Type | Description |
| --- | --- | --- |
| `is_ancestor` | `bool` | `true` if `descendant_commit_id` has `ancestor_commit_id` in its history. A commit is its own ancestor. |

- **Unknown commits are `false`.** If either commit is not in the repository,
  the answer is `false`, not `NOT_FOUND`.
- **Safe to ask separately.** Commits are immutable, so the answer cannot
  change between this call and the `UpdateRefs` that it informs. Guard the
  move itself with `old_object_id`.

```sh
grpcurl -plaintext -d '{
  "repo": {"key": "acme/widgets"},
  "ancestorCommitId": {"hex": "ade924f34031005a3d4e72b70b509502576dd01f"},
  "descendantCommitId": {"hex": "355317f0a3db29fdca481531c12b2f3fff247229"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RefService/IsAncestor
```

```json
{
  "isAncestor": true
}
```

With the two commits swapped, the response is `{}`: `isAncestor` is `false`,
and ProtoJSON omits it.

## `ObjectService`

Reads objects, trees, history, and diffs, without a Git client. Every RPC
reads; none changes the repository.

A `TreeEntry` and a `FileChange` hold a path as a `string`. Git stores a path
as bytes that need not be UTF-8. A path that is not valid UTF-8 arrives with
the invalid sequences replaced.

### `GetObject`

Streams one object: a header, then its bytes.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `object_id` | `ObjectId` | Required. The object. Any kind. |

The response is a stream. The first message holds `header`; every later
message holds `data`.

| Field | Type | Description |
| --- | --- | --- |
| `header.kind` | `ObjectKind` | `OBJECT_KIND_COMMIT`, `OBJECT_KIND_TREE`, `OBJECT_KIND_BLOB`, or `OBJECT_KIND_TAG`. |
| `header.size` | `uint64` | The size of the object in bytes, uncompressed. The `data` chunks add up to it. |
| `data` | `bytes` | The next chunk of the object, up to 512 KiB. |

Read `header.size` before you read on, for example to refuse to render a
large file. The bytes are the object's content as Git stores it, without
Git's `<kind> <size>` prefix.

```sh
grpcurl -plaintext -d '{
  "repo": {"key": "acme/widgets"},
  "objectId": {"hex": "0e489d4c5ae27b6e6c9e48f7ebda4889cad31b72"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.ObjectService/GetObject
```

```json
{
  "header": {
    "kind": "OBJECT_KIND_BLOB",
    "size": "19"
  }
}
{
  "data": "cHViIGZuIHdpZGdldCgpIHt9Cg=="
}
```

The `data` is `pub fn widget() {}` and a newline, in base64.

### `ListTree`

Streams every path under a tree, recursively.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `object_id` | `ObjectId` | Required. A commit or a tree. A commit means its root tree. |

The response is a stream of batches.

| Field | Type | Description |
| --- | --- | --- |
| `entries` | `TreeEntry[]` | The next batch of entries. |
| `truncated` | `bool` | The walk stopped at 50,000 entries. The entries sent are a part of the tree. |

| `TreeEntry` field | Type | Description |
| --- | --- | --- |
| `path` | `string` | The path from the root of the tree, `/`-separated, with no leading `/`. |
| `kind` | `ObjectKind` | `OBJECT_KIND_BLOB` for a file or a symbolic link, `OBJECT_KIND_TREE` for a directory. |
| `object_id` | `ObjectId` | The blob or the tree. |

- **Breadth first.** Every entry at one depth arrives before any entry below
  it. Order within a depth is not defined.
- **Directories too.** A directory is an entry as well as the files in it.
- **No submodules.** A submodule entry (a gitlink) is left out.
- **No mode and no size.** `ListTree` reads trees only, so its cost grows with
  the number of directories, not files. Read a file's size with `GetObject`.

| Status | Cause |
| --- | --- |
| `INVALID_ARGUMENT` | `object_id` is a blob or a tag |

```sh
grpcurl -plaintext -d '{
  "repo": {"key": "acme/widgets"},
  "objectId": {"hex": "ade924f34031005a3d4e72b70b509502576dd01f"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.ObjectService/ListTree
```

```json
{
  "entries": [
    {
      "path": "README.md",
      "kind": "OBJECT_KIND_BLOB",
      "objectId": {
        "hex": "cd7f97a073a7146364987c8afb05c89a4ae1a7f1"
      }
    },
    {
      "path": "src",
      "kind": "OBJECT_KIND_TREE",
      "objectId": {
        "hex": "e8362855c0db716c31259e8d57ac897038e06b5f"
      }
    }
  ]
}
{
  "entries": [
    {
      "path": "src/lib.rs",
      "kind": "OBJECT_KIND_BLOB",
      "objectId": {
        "hex": "0e489d4c5ae27b6e6c9e48f7ebda4889cad31b72"
      }
    }
  ]
}
```

### `ListCommits`

Streams the history of a commit, newest first, along first parents.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `commit_id` | `ObjectId` | Required. The newest commit of the walk, such as a branch tip from `ListRefs`. |
| `limit` | `uint32` | The most commits to return. Zero, or more than 100, means 100. |
| `page_token` | `string` | The `next_page_token` of the previous page. Empty starts at `commit_id`. |

| Response field | Type | Description |
| --- | --- | --- |
| `commits` | `Commit[]` | The commits of this page, newest first. |
| `next_page_token` | `string` | Where the next page starts. Empty when the walk reached a commit with no parent. |

| `Commit` field | Type | Description |
| --- | --- | --- |
| `commit_id` | `ObjectId` | The commit. |
| `parent_commit_ids` | `ObjectId[]` | Every parent, first parent first. Empty for a root commit. |
| `tree_id` | `ObjectId` | The root tree. |
| `author`, `committer` | `Identity` | `name`, `email`, `timestamp`, and `utc_offset_seconds`, the zone that the author's clock reported. |
| `summary` | `string` | The first line of the message. |
| `body` | `string` | The rest of the message, without the blank line after the summary. Empty when there is none. |

- **First parents.** The walk follows first parents only: the commits that
  landed on a branch, not every commit that a merge brought in. Every parent
  is still reported.
- **Identities are as stored.** A name or an email that is not valid UTF-8
  arrives with the invalid sequences replaced.

| Status | Cause |
| --- | --- |
| `INVALID_ARGUMENT` | `commit_id` is not a commit, or `page_token` is not a token |

```sh
grpcurl -plaintext -d '{
  "repo": {"key": "acme/widgets"},
  "commitId": {"hex": "355317f0a3db29fdca481531c12b2f3fff247229"},
  "limit": 1}' \
  127.0.0.1:50051 enroute.api.v1alpha1.ObjectService/ListCommits
```

```json
{
  "commits": [
    {
      "commitId": {
        "hex": "355317f0a3db29fdca481531c12b2f3fff247229"
      },
      "parentCommitIds": [
        {
          "hex": "ade924f34031005a3d4e72b70b509502576dd01f"
        }
      ],
      "treeId": {
        "hex": "b4ef7b81f4f00d50d40051ef327f78de080d6db5"
      },
      "author": {
        "name": "Ada Lovelace",
        "email": "ada@example.com",
        "timestamp": "2026-11-04T13:00:00Z",
        "utcOffsetSeconds": 3600
      },
      "committer": {
        "name": "Ada Lovelace",
        "email": "ada@example.com",
        "timestamp": "2026-11-04T13:00:00Z",
        "utcOffsetSeconds": 3600
      },
      "summary": "Return the answer"
    }
  ],
  "nextPageToken": "ade924f34031005a3d4e72b70b509502576dd01f"
}
```

The next call sends `"pageToken": "ade924f34031005a3d4e72b70b509502576dd01f"`
and gets "Add the widget function", whose `body` is
`"The first public item of the crate.\n"`.

### `DiffCommit`

Streams the paths that differ between a commit and a base, with the blobs on
each side.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `commit_id` | `ObjectId` | Required. The commit to diff. |
| `base_commit_id` | `ObjectId` | The commit to diff against. Unset means the first parent of `commit_id`. |

The response is a stream of batches.

| Field | Type | Description |
| --- | --- | --- |
| `changes` | `FileChange[]` | The next batch of changes, ordered by path across the whole response. |
| `truncated` | `bool` | The diff stopped at a limit. The changes sent are the first part of the answer, in path order. |

| `FileChange` field | Type | Description |
| --- | --- | --- |
| `path` | `string` | The path from the root of the tree. |
| `old_object_id` | `ObjectId` | The blob in the base. Unset when the commit adds the path. |
| `new_object_id` | `ObjectId` | The blob in the commit. Unset when the commit deletes the path. |

- **What a commit changed.** With no `base_commit_id`, the diff is against the
  first parent. A merge then shows what it landed on the branch, not
  everything that it brought in. A root commit diffs against nothing, so every
  path is an add.
- **What a branch changed.** Diff the branch tip against its merge base with
  the target, from [`FindMergeBases`](#findmergebases). A diff against the
  target's tip shows the target's newer commits as reversed changes.
- **No rename detection.** A rename is a delete and an add.
- **Content only.** A change of mode alone, such as making a file executable,
  and a change to a submodule are not reported.
- **File to directory.** A path that changed between a file and a directory
  is a delete and an add of the paths under it.
- **No contents.** Read each blob with `GetObject`, and compute the line diff
  in your application.

| Status | Cause |
| --- | --- |
| `INVALID_ARGUMENT` | `commit_id` or `base_commit_id` is not a commit |

```sh
grpcurl -plaintext -d '{
  "repo": {"key": "acme/widgets"},
  "commitId": {"hex": "355317f0a3db29fdca481531c12b2f3fff247229"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.ObjectService/DiffCommit
```

```json
{
  "changes": [
    {
      "path": "README.md",
      "oldObjectId": {
        "hex": "cd7f97a073a7146364987c8afb05c89a4ae1a7f1"
      },
      "newObjectId": {
        "hex": "acb56cde0f3dc1337a9dfdf9cb036b1f0eb24f7f"
      }
    },
    {
      "path": "src/lib.rs",
      "oldObjectId": {
        "hex": "0e489d4c5ae27b6e6c9e48f7ebda4889cad31b72"
      },
      "newObjectId": {
        "hex": "e098b53098d7e5bd0d592585ce0a989840c259ad"
      }
    }
  ]
}
```

### `FindMergeBases`

Returns the commits where two histories last met, as
`git merge-base --all` does.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `commit_id_a`, `commit_id_b` | `ObjectId` | Required. The two commits, in either order. |

| Response field | Type | Description |
| --- | --- | --- |
| `base_commit_ids` | `ObjectId[]` | The merge bases, as a set in no defined order. |
| `exhausted` | `bool` | The walk stopped at its limit before it finished. `base_commit_ids` is then not the answer. |

- **Usually one base.** A criss-cross history has more than one. Choosing
  between them is your application's decision.
- **Empty means unrelated, or unknown.** The set is empty when the two
  commits share no history, or when either is not in the repository.
- **`exhausted` means unknown.** Two unrelated histories cost a walk of both.
  On a large repository the walk can stop at its limit. Do not draw a diff
  from an exhausted answer.

```sh
grpcurl -plaintext -d '{
  "repo": {"key": "acme/widgets"},
  "commitIdA": {"hex": "ade924f34031005a3d4e72b70b509502576dd01f"},
  "commitIdB": {"hex": "355317f0a3db29fdca481531c12b2f3fff247229"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.ObjectService/FindMergeBases
```

```json
{
  "baseCommitIds": [
    {
      "hex": "ade924f34031005a3d4e72b70b509502576dd01f"
    }
  ]
}
```

## `CommitService`

### `CreateCommit`

Makes one commit on a branch from file changes, and moves the branch to it.
Enroute makes the blobs, the trees, and the commit. Your application does not
need a checkout or a Git client.

| Request field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | Required. The key of the repository. |
| `branch` | `string` | Required. The full refname of the branch, under `refs/heads/`. |
| `expected_old_commit` | `ExpectedOldCommit` | What the branch must hold: `commit_id` for that commit, or `absent` for no branch. Unset means no check. |
| `message` | `string` | The commit message. |
| `author` | `Identity` | Required. Who wrote the change. |
| `committer` | `Identity` | Who made the commit. Unset means the author. |
| `changes` | `PathChange[]` | Required. At least one change, and each path at most once. |

| `PathChange` field | Type | Description |
| --- | --- | --- |
| `path` | `string` | The path from the root of the tree, such as `src/lib.rs`. |
| `put.content` | `bytes` | Write a file with these bytes, or a symbolic link to this target. |
| `put.mode` | `FileMode` | `FILE_MODE_REGULAR` (the default), `FILE_MODE_EXECUTABLE`, or `FILE_MODE_SYMLINK`. |
| `delete` | `DeleteFile` | Remove the file at `path`. An empty message. |

| Response field | Type | Description |
| --- | --- | --- |
| `commit_id` | `ObjectId` | The new commit, where the branch now points. |
| `tree_id` | `ObjectId` | Its root tree. |
| `parent_commit_id` | `ObjectId` | Where the branch pointed before. Unset when the commit started the branch. |

- **The parent.** The parent of the new commit is the commit that the branch
  points to. If the branch does not exist, the call creates it, and the
  commit has no parent. A commit has at most one parent; Enroute makes no
  merge commits.
- **Compare-and-swap.** With `expected_old_commit`, the call fails with
  `FAILED_PRECONDITION` if the branch does not match, and nothing changes.
  Without it, Enroute makes the commit on whatever the branch holds. If
  another writer moves the branch during the call, Enroute makes the commit
  again on the new tip. If this continues, the call fails with `ABORTED`.
- **Changes.** A put creates the directories in its path. A delete of a path
  that holds no file does nothing. A directory that a delete leaves empty is
  removed. A put cannot replace a directory or go through a file, and a
  delete cannot remove a directory.
- **Paths.** A path is relative and `/`-separated. It has no empty, `.`, or
  `..` component, no component that Git reads as `.git` in any case, and no
  `\` or NUL. A `.gitmodules` cannot be a symbolic link.
- **Identities.** A name and an email must not be empty and must not hold
  `<`, `>`, a newline, or NUL. If `timestamp` is unset, Enroute uses the time
  that the call arrives. `utc_offset_seconds` must be whole minutes, at most
  99:59 in each direction.
- **Message.** Enroute stores the message as given and adds a final newline
  if there is none. The message cannot hold NUL.
- **Size.** A request is at most 32 MiB, encoded. To add more data, push it
  over Git.
- **No hooks.** `CreateCommit` runs no `pre_receive` and no `post_receive`.
  Apply your policy before the call.

| Status | Cause |
| --- | --- |
| `INVALID_ARGUMENT` | A field breaks a rule above. Nothing changes |
| `FAILED_PRECONDITION` | The branch does not match `expected_old_commit`. The message names where it is |
| `ABORTED` | Other writers kept moving a branch that had no `expected_old_commit` |
| `OUT_OF_RANGE` | The request is larger than 32 MiB |

```sh
grpcurl -plaintext -d '{
  "repo": {"key": "acme/widgets"},
  "branch": "refs/heads/main",
  "expectedOldCommit": {"commitId": {"hex": "ade924f34031005a3d4e72b70b509502576dd01f"}},
  "message": "Add a changelog",
  "author": {"name": "Ada Lovelace", "email": "ada@example.com",
             "timestamp": "2026-11-05T09:00:00Z", "utcOffsetSeconds": 3600},
  "changes": [{"path": "CHANGELOG.md",
               "put": {"content": "V2lkZ2V0cyBmb3IgZXZlcnlvbmUuCg=="}}]}' \
  127.0.0.1:50051 enroute.api.v1alpha1.CommitService/CreateCommit
```

```json
{
  "commitId": {
    "hex": "9b2b9b51c3d9fb42dd5a02293565b96862bb7cea"
  },
  "treeId": {
    "hex": "a39cd0ceeb286242d882e0452c7a0e42f1ff07c9"
  },
  "parentCommitId": {
    "hex": "ade924f34031005a3d4e72b70b509502576dd01f"
  }
}
```

The same call again fails, because `main` has moved:

```text
ERROR:
  Code: FailedPrecondition
  Message: refs/heads/main is at 9b2b9b51c3d9fb42dd5a02293565b96862bb7cea
```

## `SyncService`

`PushToRemote` sends refs from a repository to another Git server, and
`FetchFromRemote` brings refs from another Git server into a repository.
Enroute acts as the Git client. Each call carries its remote URL and
credentials, and Enroute discards them when the call ends.

Each has its own page: [Push to a remote](push-to-a-remote.md) and
[Fetch from a remote](fetch-from-a-remote.md).
