# Hook reference

Enroute sends a signed HTTP `POST` to `hook_endpoint_url` whenever a Git
request needs an application decision.

This reference lists each hook message and field. To implement an endpoint,
follow [Build a code hosting application](../build/README.md).

## Transport

| Part | Value |
| --- | --- |
| Method | `POST` |
| URL | `hook_endpoint_url` from the tenant file |
| `Content-Type` | `application/x-protobuf` |
| Request body | A binary `HookRequest` |
| Success response | `200` with a binary `HookResponse` |
| Timeout | `hooks.timeout_secs`, 10 seconds by default |

`HookRequest.call` is a `oneof` with four members, and it selects the call. Set
the matching member of `HookResponse.answer`. Because the body selects the
call, your application mounts one route for all four.

The messages are defined in `proto/enroute/hook/v1alpha1/hook.proto`, which
declares no service. See [Protocol Buffers](proto.md).

## Read the raw body

The signature covers exactly the bytes that arrived, so a framework that
parses, re-encodes, or normalises the body first breaks every call. Read the
body once, and use the same bytes for the digest check and the protobuf decode.

| Framework | Use |
| --- | --- |
| Next.js route handler | `new Uint8Array(await request.arrayBuffer())` |
| Express | `express.raw({ type: '*/*' })`, then `req.body` is a Buffer |
| Fastify | An `application/x-protobuf` content type parser that returns the Buffer |
| FastAPI or Starlette | `await request.body()` |
| Flask | `request.get_data()` |
| Django | `request.body` |
| Go `net/http` | `io.ReadAll(r.Body)` |
| Rails | `request.raw_post` |
| Spring | `@RequestBody byte[] body` |
| ASP.NET | Read `Request.Body` to a `byte[]` |

## Choose the route path

Do not choose a path that a repository could take. A code hosting platform
serves repositories at paths named after repositories, and a repository can be
named anything a user types, including the path your hooks are on.

Put the route in the namespace your framework keeps for machine traffic:
`/api/…` under Next.js, Nuxt, or SvelteKit; a router or blueprint under its own
prefix in Express, FastAPI, or Flask; a route outside the resource routes in
Rails or Phoenix. `/enroute/hooks` is the wrong choice, because a repository
named `enroute` takes it. `/api/enroute/hooks` is a safe one.

## How to read the examples

Bodies on this page are shown as JSON so that you can read them. **On the wire
they are binary protobuf.** Do not build an endpoint that reads or writes JSON.

The JSON follows the canonical [ProtoJSON mapping][protojson], which is what
your generated code produces when you print a message:

| Rule | Result |
| --- | --- |
| Field names | `lowerCamelCase`, so `old_object_id` prints as `oldObjectId` |
| Enums | The value name as a string, such as `"ACCESS_WRITE"` |
| `bytes` | Base64 |
| Unset fields | Omitted |

The last row matters more here than anywhere else. An unset field is how this
contract spells absence, so a missing key in an example is information. See
[`pre_receive`](#pre_receive), where a missing `oldObjectId` means a branch is
being created.

[protojson]: https://protobuf.dev/programming-guides/json/

## A call in full

Every hook arrives in the following shape. This example is `authorize`; the
other three differ only in the body.

```http
POST /api/enroute/hooks HTTP/1.1
Host: example.com
Content-Type: application/x-protobuf
Content-Digest: sha-256=:<digest of the body>:
Signature-Input: sig1=("@method" "@authority" "@path" "content-digest");created=1700000000;keyid="<thumbprint>"
Signature: sig1=:<Ed25519 signature>:

{"authorize": {"repoPath": "acme/widgets.git", "access": "ACCESS_WRITE"}}
```

```http
HTTP/1.1 200 OK
Content-Type: application/x-protobuf

{"authorize": {"granted": {"repo": {"key": "repo-4f2a1c"}, "actor": "user-8471"}}}
```

Verify the signature before you read the body. See
[Verify the signature](verify-the-signature.md).

## The four calls

| Call | Enroute asks | When |
| --- | --- | --- |
| [`authorize`](#authorize) | Which repository is this, and who is asking? | Once per Git request, before anything is served |
| [`visible_refs`](#visible_refs) | Which of these refs may this caller see? | Before each ref advertisement |
| [`pre_receive`](#pre_receive) | Which of these ref updates may land? | After a push is stored, before any ref moves |
| [`post_receive`](#post_receive) | — | After the refs moved |

Every call fails closed. If your endpoint returns a status other than `200`, an
unreadable body, an empty `answer`, or nothing within the timeout, Enroute
fails the Git request. A refusal is a field inside a `200` response, never a
status code.

`post_receive` is the exception, because nothing is left to decide. Enroute
ignores an empty answer there and does not retry.

## Shared messages

These appear in more than one call.

### `RepoKey`

| Field | Type | Description |
| --- | --- | --- |
| `key` | `string` | The key your application passed to `CreateRepository`. |

A key is 1 to 256 bytes of ASCII letters, digits, `-`, `_`, and `.`, starting
and ending with a letter or digit. Enroute compares keys byte for byte. See
[Repository keys](the-contract.md#repository-keys).

### `ObjectId`

| Field | Type | Description |
| --- | --- | --- |
| `hex` | `string` | 40 lowercase hexadecimal characters. |

An object ID is a message rather than a string so that a generated client
cannot pass a refname where an ID belongs. An optional ID is an unset field,
never 40 zeros and never an empty string.

### `Header`

| Field | Type | Description |
| --- | --- | --- |
| `name` | `string` | The header name. |
| `value` | `string` | The header value. |

Headers are repeated rather than a map, because a header can appear more than
once.

### `Access`

| Value | Meaning |
| --- | --- |
| `ACCESS_UNSPECIFIED` | Not a decision. Enroute never sends this. |
| `ACCESS_READ` | A fetch, and the ref advertisement before it. |
| `ACCESS_WRITE` | A push, and the ref advertisement before it. |

## `authorize`

Enroute asks which repository a Git URL names and who is asking. This is the
first call of every Git request, and nothing is served until it is answered.

The request carries the Git request roughly as it arrived, because what a
credential looks like and what a path means are your application's decisions.

### `AuthorizeRequest`

| Field | Type | Description |
| --- | --- | --- |
| `repoPath` | `string` | The repository path exactly as the client spelled it, at any depth, including a `.git` suffix if the client sent one. |
| `headers` | `Header[]` | The request headers as they arrived, including the credential. |
| `access` | `Access` | What the route about to run does to the repository. |

Enroute rejects some paths before it calls you. A path that is empty, longer
than 1024 bytes, or that holds an empty segment, a `.` or `..` segment, an
encoded slash, or a control character is a `404` to the client, and no hook is
sent.

The `.git` suffix means nothing in the Git protocol. Whether `acme/widgets` and
`acme/widgets.git` are one repository is your decision.

### `AuthorizeResponse`

Set exactly one of `granted` or `denied`.

#### `Granted`

| Field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | The repository to serve. Enroute resolves it within the tenant the request arrived for. A key the tenant does not hold is a `404` to the client. |
| `actor` | `string` | Who is asking, as an ID in your own namespace. |
| `context` | `bytes` | Up to 8 KiB replayed on the later hooks of this request. Optional. |

Enroute records `actor` on the request's traces and in usage data, so it must
be safe to keep. Use an ID. Never send a credential and never send an email
address.

Set `actor` from your own records rather than from the request. The Git client
is the one party that must not be trusted about who it is.

`context` is opaque. Enroute never parses it, never logs it, and hands it back
byte for byte on `visible_refs`, `pre_receive`, and `post_receive` for the same
Git request. Because it is captured now and replayed when the hooks run, which
on a large push is minutes later, check anything that can change in between. A
`context` over 8 KiB fails the Git request.

#### `Denied`

| Field | Type | Description |
| --- | --- | --- |
| `denial` | `Denial` | Why the request is refused. |
| `challenge` | `Challenge` | How to authenticate. Read only for `DENIAL_UNAUTHORIZED`, and required there. |

| `Denial` | HTTP status | Meaning |
| --- | --- | --- |
| `DENIAL_UNSPECIFIED` | — | Not a decision. Do not send this. |
| `DENIAL_UNAUTHORIZED` | `401` | No usable credential was presented. |
| `DENIAL_FORBIDDEN` | `403` | A valid credential that does not grant this access. |
| `DENIAL_NOT_FOUND` | `404` | No such repository, or one this caller may not be told about. |

Answering `DENIAL_NOT_FOUND` before `DENIAL_UNAUTHORIZED` tells an anonymous
caller which repositories exist. Which one you send for a repository that must
stay hidden is your decision, not an order Enroute imposes.

#### `Challenge`

| Field | Type | Description |
| --- | --- | --- |
| `wwwAuthenticate` | `string` | The `WWW-Authenticate` header value. |
| `help` | `string` | A plain-text body, which Git prints as `remote:` lines. Include the trailing newline. |

Git's credential handling, such as netrc, credential helpers, and a prompted
retry, engages only from `wwwAuthenticate`. A `401` without it leaves the
person behind the client with no way forward.

### Examples

A push is granted:

```json
{
  "authorize": {
    "repoPath": "acme/widgets.git",
    "headers": [
      { "name": "authorization", "value": "Basic eDpzZWNyZXQtdG9rZW4=" },
      { "name": "user-agent", "value": "git/2.43.0" }
    ],
    "access": "ACCESS_WRITE"
  }
}
```

```json
{
  "authorize": {
    "granted": {
      "repo": { "key": "repo-4f2a1c" },
      "actor": "user-8471",
      "context": "eyJzZXNzaW9uIjoic2Vzcy0yMTkifQ=="
    }
  }
}
```

No usable credential. The challenge is what makes the Git client ask for one:

```json
{
  "authorize": {
    "denied": {
      "denial": "DENIAL_UNAUTHORIZED",
      "challenge": {
        "wwwAuthenticate": "Basic realm=\"acme\"",
        "help": "Create a token at https://example.com/settings/tokens\n"
      }
    }
  }
}
```

A valid credential without write access:

```json
{
  "authorize": {
    "denied": { "denial": "DENIAL_FORBIDDEN" }
  }
}
```

## `visible_refs`

Enroute asks which refs this caller may see, before each ref advertisement. It
sends the refnames it would otherwise advertise, and advertises the ones you
return.

This is where a private branch, a hidden `refs/pull/` namespace, or a ref only
its author may see comes from.

### `VisibleRefsRequest`

| Field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | The repository. |
| `actor` | `string` | Who is asking, as `Granted.actor` named them. |
| `access` | `Access` | Whether the advertisement is for a fetch or a push. |
| `refnames` | `string[]` | Every refname Enroute would advertise, excluding `HEAD`. |
| `context` | `bytes` | `Granted.context`, byte for byte. |

The same refs are asked about twice, once for a fetch and once for a push,
because what a caller may read and what it may be told is writable are not the
same question.

`HEAD` is not in the list. It names no ref of its own, and Enroute hides it
automatically when it hides what it points at.

### `VisibleRefsResponse`

| Field | Type | Description |
| --- | --- | --- |
| `refnames` | `string[]` | The refnames to advertise. |

A name that Enroute sent and you do not return is hidden. A name you return
that was not sent is ignored, because this answers which refs may be seen and
not which exist.

The refs are sent rather than described by a pattern, because visibility is
your question to answer. A pattern language would only cover the policies
somebody thought to spell.

> **Note:** This hides refnames. It does not make objects unreachable. A client
> that already knows a hidden tip's object ID can still fetch it, as with Git's
> own `uploadpack.hideRefs`. Anything whose secrecy must survive a guessed
> object ID needs a repository of its own.

### Example

A contributor fetches. They may not see the security branch or another user's
draft:

```json
{
  "visibleRefs": {
    "repo": { "key": "repo-4f2a1c" },
    "actor": "user-8471",
    "access": "ACCESS_READ",
    "refnames": [
      "refs/heads/main",
      "refs/heads/security/embargo-2419",
      "refs/heads/draft/user-9002/spike",
      "refs/tags/v1.2.0"
    ],
    "context": "eyJzZXNzaW9uIjoic2Vzcy0yMTkifQ=="
  }
}
```

```json
{
  "visibleRefs": {
    "refnames": ["refs/heads/main", "refs/tags/v1.2.0"]
  }
}
```

To hide nothing, return the list you were sent.

## `pre_receive`

Enroute asks which ref updates may land. This is Git's `pre-receive` hook for a
repository whose hooks run in another process. The objects of the push are
already stored, and no ref has moved.

Protected branches, merge queues, and review gates live here. Your application
can read the pushed commits over the API before it answers, because the objects
are already stored.

### `PreReceiveRequest`

| Field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | The repository. |
| `actor` | `string` | Who is pushing, as `Granted.actor` named them. |
| `commands` | `RefCommand[]` | Every update the push still asks for, in `receive-pack` order. |
| `context` | `bytes` | `Granted.context`, byte for byte. |

Whoever reaches this call already has write access. Use `actor` for policy that
cares *which* writer, not whether.

`commands` excludes anything Enroute already refused on its own, such as a
non-fast-forward or an object that never arrived. What you are sent is still
live, so a judgement you return is never for something already dead.

#### `RefCommand`

| Field | Type | Description |
| --- | --- | --- |
| `refname` | `string` | The full refname, such as `refs/heads/main`. |
| `oldObjectId` | `ObjectId` | What the ref points at now. **Unset means the command creates the ref.** |
| `newObjectId` | `ObjectId` | What the ref would point at. **Unset means the command deletes the ref.** |
| `force` | `bool` | Whether `oldObjectId` is *not* an ancestor of `newObjectId`. |

Read the command type from which IDs are set:

| `oldObjectId` | `newObjectId` | The command |
| --- | --- | --- |
| Unset | Set | Creates the ref |
| Set | Set | Moves the ref |
| Set | Unset | Deletes the ref |

`force` answers what a hook running next to the repository would work out with
`git merge-base --is-ancestor`. Enroute answers it because your endpoint holds
no commit graph.

> **Caution:** `force` is only ever true for a command that has both IDs, so a
> delete arrives with `force` false. A rule that refuses a force-push and
> nothing else leaves a protected branch that anyone can delete. Test for a
> delete before you test `force`.

### `PreReceiveResponse`

| Field | Type | Description |
| --- | --- | --- |
| `judgements` | `RefJudgement[]` | One judgement per command, matched by refname. |

#### `RefJudgement`

| Field | Type | Description |
| --- | --- | --- |
| `refname` | `string` | Which command this judges, spelled as `RefCommand.refname` spelled it. |
| `judgement` | `Judgement` | Whether the command may land. |
| `reason` | `string` | What to tell whoever pushed. Read only for `JUDGEMENT_REFUSE`. |

| `Judgement` | Meaning |
| --- | --- |
| `JUDGEMENT_UNSPECIFIED` | Not a decision. A command judged this way is a command nobody judged. |
| `JUDGEMENT_ALLOW` | This command may land. |
| `JUDGEMENT_REFUSE` | This command may not land, for `reason`. |

**Judge every command you are sent.** There is no default. A command with no
judgement fails the whole push, so a policy that misses a ref stops the push
instead of accepting it. Enroute cannot tell an application that meant to allow
a ref from one whose policy never reached it.

Enroute ignores a judgement that names a refname the request did not carry. A
refname judged more than once is refused if any of those judgements refuses it.

`reason` reaches the client as the rejection message, the way a hook's stderr
does. It is all the person gets, so name the ref and say what to do instead.

> **Note:** `UpdateRefs` on the API does not run this hook. There, the caller is
> your own application.

### Example

One push asks for three updates: it moves `main`, creates a branch, and deletes
a tag.

```json
{
  "preReceive": {
    "repo": { "key": "repo-4f2a1c" },
    "actor": "user-8471",
    "commands": [
      {
        "refname": "refs/heads/main",
        "oldObjectId": { "hex": "9f2c1b7e4a8d3c5f6b0e2a1d4c7f8b3e5a6d9c0f" },
        "newObjectId": { "hex": "2d4f6a8c0e1b3d5f7a9c2e4b6d8f0a1c3e5b7d9f" }
      },
      {
        "refname": "refs/heads/feature/checkout",
        "newObjectId": { "hex": "7b1e3d5a9c2f4b6d8e0a1c3f5b7d9e2a4c6f8b0d" }
      },
      {
        "refname": "refs/tags/v1.2.0",
        "oldObjectId": { "hex": "4c6e8a0b2d4f6a8c0e2b4d6f8a0c2e4b6d8f0a2c" }
      }
    ],
    "context": "eyJzZXNzaW9uIjoic2Vzcy0yMTkifQ=="
  }
}
```

`refs/heads/feature/checkout` has no `oldObjectId`, so it is a create.
`refs/tags/v1.2.0` has no `newObjectId`, so it is a delete. Neither carries
`force`, because `false` is the default value and ProtoJSON omits it.

The trunk is protected and release tags cannot be deleted, so two of the three
are refused:

```json
{
  "preReceive": {
    "judgements": [
      {
        "refname": "refs/heads/main",
        "judgement": "JUDGEMENT_REFUSE",
        "reason": "main is protected: open a merge request"
      },
      {
        "refname": "refs/heads/feature/checkout",
        "judgement": "JUDGEMENT_ALLOW"
      },
      {
        "refname": "refs/tags/v1.2.0",
        "judgement": "JUDGEMENT_REFUSE",
        "reason": "release tags cannot be deleted"
      }
    ]
  }
}
```

The person who pushed sees the following. Enroute offers a Git client no
`atomic` capability, so the allowed ref lands even though two were refused:

```text
To http://127.0.0.1:8080/acme/widgets.git
 * [new branch]      feature/checkout -> feature/checkout
 ! [remote rejected] main -> main (main is protected: open a merge request)
 ! [remote rejected] v1.2.0 -> v1.2.0 (release tags cannot be deleted)
error: failed to push some refs to 'http://127.0.0.1:8080/acme/widgets.git'
```

To allow everything, judge every command `JUDGEMENT_ALLOW`. That is what having
no `pre-receive` hook installed has always meant.

## `post_receive`

Enroute reports what landed. This is Git's `post-receive` hook. The refs have
moved and nothing can be refused.

This is where CI belongs. Your application learns that a branch has a new tip at
the moment it does, rather than by listing refs on a timer.

### `PostReceiveRequest`

| Field | Type | Description |
| --- | --- | --- |
| `repo` | `RepoKey` | The repository. |
| `actor` | `string` | Who pushed, as `Granted.actor` named them. |
| `commands` | `RefCommand[]` | Every command that landed, and only those. |
| `context` | `bytes` | `Granted.context`, byte for byte. |

`commands` uses the same [`RefCommand`](#refcommand) as `pre_receive`. A push
whose commands were all refused sends no call at all, rather than a call with an
empty list.

`actor` is always a Git client. An application that moves a ref over the API
runs no hooks, so its own writes never arrive here.

### `PostReceiveResponse`

| Field | Type | Description |
| --- | --- | --- |
| `messages` | `string[]` | Lines to print to whoever pushed. |

Enroute sends `messages` as sideband progress frames before the report-status
body, so they reach the client before its `ok <ref>` lines. This is where "open a
merge request at ..." comes from, and there is nowhere else it could come from:
your application is not on the connection.

A client that negotiated no sideband has no channel to receive them on and is
sent none, which is also what Git does.

> **Important:** Delivery is best-effort and at most once. Enroute sends this
> call after the push has already succeeded, so it cannot retry without lying to
> a client that is gone. An unreachable application receives no event.
> Treat this as latency, not as a guarantee. Anything whose correctness depends
> on being told needs its own record.

Enroute waits for the answer and ignores it, the way `receive-pack` waits for
`post-receive` and ignores its exit code. A failure here undoes nothing, and the
push is not failed for it.

### Example

The branch landed, so the application offers a merge request:

```json
{
  "postReceive": {
    "repo": { "key": "repo-4f2a1c" },
    "actor": "user-8471",
    "commands": [
      {
        "refname": "refs/heads/feature/checkout",
        "newObjectId": { "hex": "7b1e3d5a9c2f4b6d8e0a1c3f5b7d9e2a4c6f8b0d" }
      }
    ],
    "context": "eyJzZXNzaW9uIjoic2Vzcy0yMTkifQ=="
  }
}
```

```json
{
  "postReceive": {
    "messages": [
      "Open a merge request:",
      "  https://example.com/acme/widgets/merge/new?from=feature/checkout"
    ]
  }
}
```

The person who pushed sees the following:

```text
remote: Open a merge request:
remote:   https://example.com/acme/widgets/merge/new?from=feature/checkout
To http://127.0.0.1:8080/acme/widgets.git
 * [new branch]      feature/checkout -> feature/checkout
```

To say nothing, answer with an empty `messages` list.

## Failure reference

What the person pushing or fetching sees when something goes wrong, and what to
check.

| Condition | Result | Check |
| --- | --- | --- |
| Endpoint unreachable | Git request fails | Your endpoint is running, and `hook_endpoint_url` points at it |
| No answer within the timeout | Git request fails | `hooks.timeout_secs`, and slow lookups in your handler |
| Status other than `200` | Git request fails | Deny inside the body with a `200`, not with a status code |
| Body is not a `HookResponse` | Git request fails | You set a member of `answer`, and you write binary protobuf |
| `answer` is empty | Git request fails, except on `post_receive` | The `answer` member matches the `call` member |
| Signature rejected by your endpoint | Git request fails | You verify against the configured URL's authority and path, not the arriving `Host` |
| `Granted.repo` names no repository | `404` to the client | The key exists in this tenant |
| `Granted.context` over 8 KiB | Git request fails | Store the data and send a reference to it |
| A command with no judgement | The whole push fails | You judge every command in `commands` |

## See also

- [Build a code hosting platform](../build/README.md) — write the endpoint that answers these
  calls.
- [Verify the signature](verify-the-signature.md) — the check to run
  before you read a body.
- [Hooks](../concepts/hooks.md) — why Enroute asks, and what you control.
- [Protocol Buffers](proto.md) — the proto files and how to get them.
