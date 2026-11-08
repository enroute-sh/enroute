# Clients and interfaces

Enroute has two callers, Git clients and your application, and it calls one
back: your application's hook endpoint.

```text
Git client -- Git smart HTTP --> Enroute <-- gRPC -- application
                                    |
                                    +-- signed HTTP hooks --> application
```

| Interface | Caller | Use | Exposure |
| --- | --- | --- | --- |
| `listen.git` | Git clients | Clone, fetch, and push over Git smart HTTP | Public, behind TLS |
| `listen.api` | Your application | Create repositories, read Git data, and move refs over gRPC | Private: it authenticates nobody |
| Hook endpoint | Enroute | Ask your application for identity and policy decisions | Reachable from the compute nodes |

## One feature, three interfaces

A typical feature uses all three. Take a merge request in a code-hosting
application:

1. **A developer pushes a branch** over Git. Enroute calls `authorize` to learn
   who they are, and `pre_receive` to check that they may push to it. After the
   push, `post_receive` tells your application which branch moved, and your
   application prints a link to open a merge request.
2. **A reviewer opens the merge request page.** Your application calls the API:
   `FindMergeBases` for the commit where the branch started, `DiffCommit` for
   the changed files, and `GetObject` for their contents. It renders the page
   itself. Enroute serves no HTML.
3. **The reviewer merges.** Your application calls `UpdateRefs` to move the
   target branch, guarded by the commit that the reviewer saw. No hook runs,
   because your application is the caller.

Your application never parses the Git protocol, holds a connection open for a
clone, or runs `git`. Enroute never decides who a user is or what they may do.
The [Merge requests](../patterns/merge-requests.md) pattern builds this
feature in full.

## Which interface to use

| To | Use |
| --- | --- |
| Let a developer or CI clone and push | Git, on `listen.git` |
| Show files, history, and diffs in your product | The API: `ListTree`, `ListCommits`, `DiffCommit`, `GetObject` |
| Commit a change from your product, such as an edit in a browser | The API: `CreateCommit` |
| Merge, or move a branch on a user's behalf | The API: `UpdateRefs` |
| Decide who may read or push, and what may land | Hooks: `authorize`, `visible_refs`, `pre_receive` |
| Start work after a push, such as CI | Hooks: `post_receive` |
| Copy refs to or from another Git server | The API: `PushToRemote`, `FetchFromRemote` |

## The trust boundary

The Git listener is public, and `authorize` is its only access control. The
API listener trusts every caller: any client that can reach it can read,
write, and delete every repository. Keep it reachable only by your application.
Hooks are signed, so your application can tell that a request came from
Enroute. See [Security](../operate/security.md).

For the RPCs, see the [API contract](../reference/the-contract.md). For the
hooks, see [Hooks](hooks.md).
