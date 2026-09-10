# The endpoint: answering Enroute

One HTTP route. Enroute calls it each time a git request needs a decision that
only the user's application can make.

| Part           | Value                                  |
| -------------- | -------------------------------------- |
| Method         | `POST`                                 |
| URL            | Whatever was registered for the tenant |
| `Content-Type` | `application/x-protobuf`               |
| Request body   | A binary `HookRequest`                 |
| Response       | `200` with a binary `HookResponse`     |

`HookRequest.call` is a `oneof` with four members. Read which one arrived, and
set the matching member of `HookResponse.answer`. The body selects the call,
not the URL, so a new call later does not ask the user to mount a new route.

The types come from `proto/enroute/hook/v1alpha1/hook.proto`. It declares no
service on purpose.

## Four rules

**Answer the call that was asked.** An empty `answer` means "I do not
implement this", and Enroute refuses the git request rather than proceed on
an answer nobody gave. `post_receive` is the exception: the refs have already
moved, so Enroute notes an empty answer there and ignores it.

**Answer inside the timeout of the deployment.** It is `hooks.timeout_secs`
and defaults to 10 seconds. A slower answer fails the git request. The default
allows a cold serverless start.

**Deny with `200`.** A refusal is a field in the answer. Any other status, an
unreadable body, or a timeout is a *fault*: Enroute fails the git request
closed. It never reads a fault as a refusal.

**Never trust the request for identity.** The git client is the one party you
must not believe about who it is. `Granted.actor` is the user's answer, and
Enroute hands every later call that answer rather than anything the client
said.

## Reading the raw body

The signature covers exactly the bytes that arrived. A framework that parses,
re-encodes, or normalises the body first breaks every call.

| Framework             | Use                                                                    |
| --------------------- | ---------------------------------------------------------------------- |
| Next.js route handler | `new Uint8Array(await request.arrayBuffer())`                          |
| Express               | `express.raw({ type: '*/*' })`, then `req.body` is a Buffer            |
| Fastify               | An `application/x-protobuf` content type parser that returns the Buffer |
| FastAPI / Starlette   | `await request.body()`                                                 |
| Flask                 | `request.get_data()`                                                   |
| Django                | `request.body`                                                         |
| Go net/http           | `io.ReadAll(r.Body)`                                                   |
| Rails                 | `request.raw_post`                                                     |
| Spring                | `@RequestBody byte[] body`                                             |
| ASP.NET               | Read `Request.Body` to a `byte[]`                                      |

Read the body **once**, keep the bytes, and use the same bytes for the digest
check and the protobuf decode.

## The four calls

### authorize

Enroute asks this once per git request, before anything is served.

```
AuthorizeRequest { repo_path, headers[], access }
```

- `repo_path` is the URL path, exactly as the client spelled it. It nests to
  any depth, so `owner/repo` and `team/group/repo` both arrive whole. The user
  decides what it is relative to: a path prefix, a namespace in the hostname,
  or nothing.

  It keeps any `.git` on the end. The protocol gives the suffix no meaning, so
  the application decides whether the two spellings are one repository:

  ```ts
  const name = req.repoPath.replace(/\.git$/, "");
  ```

  Enroute refuses a path before it asks: an empty path, an empty segment, a
  `.` or `..` segment, an encoded `/`, a control character, or a path over
  1024 bytes. Such a request is a 404, and it never reaches this call.
- `headers` holds the request headers as they arrived, so the credential is
  in there. Git sends Basic by default. Accept `Authorization: Bearer <token>`,
  and `Basic <base64>` where the password is the token and the username is
  ignored: git needs *some* username before it sends a password, and each
  client picks its own placeholder.
- `access` is `ACCESS_READ` for a fetch, `ACCESS_WRITE` for a push.

Answer `Granted` or `Denied`.

```
Granted { repo: RepoKey, actor: string, context: bytes }
```

- `repo` is the **key** the application passed to `CreateRepository`. Look the
  path up in the user's table, as the application does to render a page, and
  answer with that row's id. Enroute resolves the key within the tenant the
  request arrived for. A key the tenant does not hold is a 404 to the client.
- `actor` is who is asking, in the user's namespace. Enroute records it on
  spans and on what the request cost, so put an id there. Never a credential,
  and never an email address.
- `context` is the user's bytes, which Enroute plays back on the later calls.
  See below.

```
Denied { denial, challenge }
```

`denial` is `DENIAL_UNAUTHORIZED`, `DENIAL_FORBIDDEN`, or `DENIAL_NOT_FOUND`.

Set `challenge` for `DENIAL_UNAUTHORIZED`, and only there. Git's credential
handling (netrc, credential helpers, a prompted retry) starts from the
`WWW-Authenticate` value the challenge carries, so a `401` without one leaves
the person behind the client with no way forward. `challenge.help` reaches
them as `remote:` lines, and needs its trailing newline.

Check permission before existence. If you answer `NOT_FOUND` first, you tell
a caller with no credential which repositories exist.

### visible_refs

Enroute asks this before every ref advertisement.

```
VisibleRefsRequest  { repo, actor, access, refnames[], context }
VisibleRefsResponse { refnames[] }
```

Enroute sends the refnames it would advertise. Answer with the ones the caller
may be told about. A name you drop is hidden. A name you add that was not sent
is ignored. `HEAD` is not in the list. Enroute hides it when it hides what
`HEAD` points at.

`access` says whether the advertisement is for a fetch or a push. What a
caller may read and what it may be told is writable are two questions.

This hides refs, not objects. A client that knows the object id of a hidden
tip can still fetch it, as under git's `uploadpack.hideRefs`. Anything whose
secrecy must survive a guessed object id needs a repository of its own.

### pre_receive

This is git's `pre-receive` hook, for a repository whose hooks live in another
process. The objects are stored and no ref has moved.

```
PreReceiveRequest  { repo, actor, commands[], context }
RefCommand         { refname, old_object_id, new_object_id, force }
PreReceiveResponse { judgements[] }
RefJudgement       { refname, judgement, reason }
Judgement          = JUDGEMENT_ALLOW | JUDGEMENT_REFUSE
```

- `commands` holds what the push still asks for. Enroute has already removed
  what it refused on its own.
- `old_object_id` is unset for a create. `new_object_id` is unset for a
  delete. Unset, not forty zeroes. Each is an `ObjectId { hex }`, not a bare
  string.
- `force` is true when `old_object_id` is **not** an ancestor of
  `new_object_id`. The caller holds no commit graph, so Enroute answers this,
  and only for a command that has both ids.

Answer with one judgement per command, matched to it by refname. Judge every
command you were sent: a command you leave out fails the whole push, the way a
call you do not answer does. `reason` is read only for a refusal, and reaches
the git client verbatim, the way the stderr of a hook does.

Enroute ignores a judgement for a refname the request did not carry, and
refuses a refname judged twice if either judgement refuses it.

`visibleRefs` above answers with a plain list of names, and this one does not,
because a refusal carries a reason and a list of allowed names has nowhere to
put it. Both fail closed; only the grammar differs.

**Judge in the loop, not after it.** The shape that fails here is a policy
written over one namespace and a `continue` for everything else. Walk
`req.commands` once and push a judgement on every pass, so a rule that does
not apply says `JUDGEMENT_ALLOW` rather than saying nothing. There is no
default: a ref nobody judged fails the push it is in.

Protected branches, merge queues, and review gates live here. The user's code
may call the contract before it answers, to read what was pushed.

### post_receive

The refs have moved. Nothing can be refused.

```
PostReceiveRequest  { repo, actor, commands[], context }
PostReceiveResponse { messages[] }
```

`commands` holds only what landed. A push whose commands were all refused
sends no call at all.

Enroute prints `messages` to whoever pushed, above the `ok <ref>` lines. A
message such as "Open a merge request at ..." comes from here, and from
nowhere else: the application is not on the connection.

This is where CI belongs. You learn that a branch has a new tip at the moment
it does, not by listing refs on a timer.

Delivery is best effort and at most once. Enroute sends this after the push
succeeded, so it cannot retry without lying to a client that has gone. It
does not tell an application that was unreachable. Treat this as latency, not
as a guarantee. Anything whose correctness depends on being told needs its own
record.

Enroute waits for the answer and ignores it, the way `receive-pack` ignores the
exit code of `post-receive`. A failure here undoes nothing.

## The context field

`Granted.context` is opaque bytes. Enroute plays them back on `visible_refs`,
`pre_receive`, and `post_receive` for the same git request, byte for byte. It
never parses or logs them, so a session id or a scoped token is safe there.

The limit is 8 KiB. A longer one is a fault, not a denial.

Enroute captures it at `authorize` and plays it back when the hooks run. On a
large push that is minutes later. Check again anything that may have changed.

An empty context is fine. Use it when the later calls can look everything up
from `repo` and `actor`, which is the common case.

## Order of work

Write it in this order, and test each part before the next:

1. The route, reading raw bytes, answering `401` to anything unsigned.
2. Signature verification. See `signature.md`.
3. `authorize`, granting one hard-coded repository. Then a `git clone` works.
4. `pre_receive` allowing every command it is sent, and `post_receive` doing
   nothing. Then a `git push` works.
5. `visible_refs` returning what it was sent. Then everything works.
6. Replace each stub with the user's real policy.

Steps 3 to 5 are the smallest endpoint that serves git. Get there first.
