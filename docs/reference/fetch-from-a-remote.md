# Fetch from a remote

`SyncService.FetchFromRemote` makes refs in an Enroute repository match the
refs on another Git server. Enroute acts as the Git client and speaks
protocol v2.

Each call supplies its remote URL and either basic or bearer credentials, in
the same `Remote` message as `PushToRemote`. Credentials are sent for the call
and then discarded; remotes are not stored.

`prefixes` cannot be empty, and each prefix must start with `refs/`. Under each
prefix, refs are created, moved, or deleted to match the remote. A move does
not have to be a fast-forward: a rewound branch on the remote rewinds here.
Refs outside the prefixes are not read or changed.

The call does not change the repository's default branch. To mirror a remote
whose default branch is not `main`, create the repository with the remote's
default branch, for example `refs/heads/master`. Otherwise a clone finds `HEAD`
pointing at a ref that does not exist, and checks out nothing.

A Git host that hides private repositories answers a missing repository the
same way as a bad credential, so a wrong URL can also fail with
`PERMISSION_DENIED`.

Fetched objects pass the same checks as a push, including the object checks.
All ref changes land as one log entry. No hooks run, the same as `UpdateRefs`
and `CreateCommit`: the caller is the application.

`outcomes` lists only refs that changed or tried to change, in name order. An
empty list means the repository already matched. `STATUS_UPDATED` and
`STATUS_DELETED` are successful outcomes. `STATUS_REJECTED` includes the
message from the check that refused the ref; other refs still land.

The call fails and changes no refs when:

- The remote cannot be reached (`UNAVAILABLE`).
- The remote refuses the credentials (`PERMISSION_DENIED`).
- The remote does not speak protocol v2 with `ls-refs` and `fetch`, or uses
  another object format than the repository (`FAILED_PRECONDITION`).
- The pack the remote sends is refused (`ABORTED`).

The call has a 30-second ref-discovery limit, a one-hour total limit, and a
one-minute no-output timeout. Retrying is safe: a second call fetches only what
is still missing.

## URL restrictions

The rules are the same as for [Push to a remote](push-to-a-remote.md#url-restrictions).
