# API contract

Your application calls Enroute over gRPC on `listen.api` (`:50051` by default).
Enroute serves plain HTTP/2; terminate TLS before it.

## Tenant header

Every RPC needs the `x-enroute-tenant` header by default. Authenticate callers
at a proxy, gateway, or mesh, and replace this header with the caller's tenant.
Missing, duplicate, and unknown tenant headers return `UNAUTHENTICATED`.

## Services

| Service | RPCs |
| --- | --- |
| `RepositoryService` | `CreateRepository`, `GetRepository`, `ListRepositories`, `DeleteRepository` |
| `RefService` | `ListRefs`, `UpdateRefs`, `IsAncestor` |
| `ObjectService` | `GetObject`, `ListTree`, `ListCommits`, `DiffCommit`, `FindMergeBases` |
| `SyncService` | `PushToRemote` |

The proto files define all fields. See [Protocol Buffers](proto.md).

## Repository keys

Repository keys are application-owned, opaque identifiers. They are 1–256
ASCII letters, digits, `-`, `_`, or `.`, start and end with a letter or digit,
and are case-sensitive. A key is unique only within a tenant.

Use a stable ID such as a UUID, ULID, or database ID. Do not use a mutable
path or display name. The `authorize` hook maps Git URLs to keys.

`CreateRepository` is idempotent. `default_branch` is used only when creating
the repository and defaults to `refs/heads/main`. `ListRepositories` is
key-ordered and paginated at 100 items; follow `next_page_token` until empty.
Do not treat a short page as the last page. `GetRepository` includes the
repository key, default branch, and the time of the most recent push; the
timestamp is unset before the first push.
`DeleteRepository` is idempotent, releases the key immediately, and removes
stored data later through maintenance.

## Refs and objects

`ListRefs` returns refs, their object IDs, update times, and `HEAD`.
`UpdateRefs` applies updates atomically. Results are returned in request order;
a rejected update is an outcome rather than an RPC failure. Unset
`old_object_id` creates a ref, and unset `new_object_id` deletes one.

Updates can only target commits already uploaded through Git. `UpdateRefs`
does not run `pre_receive`; the gRPC caller is your application. `IsAncestor`
returns `false` if either commit is unknown.

All object reads stream. `ListTree` is breadth-first, `ListCommits` follows
first parents newest-first, `DiffCommit` compares against a first parent or
specified base, and `FindMergeBases` returns merge bases newest-first.

| RPC | Limit | Completion signal |
| --- | --- | --- |
| `ListCommits` | 100 commits per page | `next_page_token` |
| `ListTree` | 50,000 entries | `truncated` |
| `DiffCommit` | 50,000 changes or 10,000 tree reads | `truncated` |
| `FindMergeBases` | Walk budget | `exhausted` |

`truncated` and `exhausted` results are partial. Check the signal before
presenting a tree or diff as complete.

## Synchronization

`PushToRemote` sends refs to another Git server. See [Push to a remote](push-to-a-remote.md).
