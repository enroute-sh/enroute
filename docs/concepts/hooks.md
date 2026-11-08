# Hooks

Enroute knows Git, and your application knows everything else: who a user is,
which repository a URL means, and what a user may do. Hooks are how Enroute
asks. At four points in a Git request, Enroute sends a signed HTTP `POST` to
your application and waits for the answer.

| Hook | Enroute asks | When |
| --- | --- | --- |
| `authorize` | Which repository is this URL, and who is asking? May they read or write? | At the start of every Git request |
| `visible_refs` | Which of these refs may this caller see? | Before refs are advertised |
| `pre_receive` | Which of these ref updates may land? | After a push has uploaded its objects, before any ref moves |
| `post_receive` | Nothing. It reports what landed | After the refs moved |

## A push, step by step

A `git push` is two HTTP requests. The first asks which refs the repository
has. The second sends the objects and the ref updates. This is a push of
`main` by `user-8471` to `https://git.example.com/acme/widgets.git`, with all
four hooks enabled:

```text
git client              Enroute (compute node)                 your application
----------              ----------------------                 ----------------
GET .../info/refs  ---> authorize(path "acme/widgets.git",  ---> look up the path and
                         access WRITE, headers)                  the token
                        <--- granted: key "repo-4f2a1c", actor "user-8471"
                        visible_refs(actor, refnames)       ---> drop refs this user
                        <--- the refs to show                    may not see
                   <--- the ref advertisement

POST .../git-receive-pack (pack + "update main old -> new")
                   ---> authorize(...)                      ---> the same decision
                        <--- granted                             as above
                        index and check the pack
                        pre_receive(actor, commands, force) ---> protected branch?
                        <--- judgement for each command          review required?
                        one log entry: the objects and the
                        allowed ref updates, on the ledger
                        post_receive(actor, landed commands)---> start CI, record
                        <--- messages for the client             the event
                   <--- "ok refs/heads/main", and the messages
```

Some facts follow from this order:

- **`authorize` runs on every request.** The two requests of one push are
  authorized separately. The `context` that `authorize` returns goes to the
  later hooks of the same request only.
- **The ref advertisement is filtered.** A ref that `visible_refs` drops is
  never shown to that caller, for a fetch or a push.
- **`pre_receive` runs before anything is stored.** By default, it decides
  from the request alone: the actor, each refname, the object IDs, and whether
  each update is a force. Enroute then stores the objects and moves the
  allowed refs in one log entry. If your policy must read the pushed commits
  first, set `hooks.pre_receive_mode = "objects"`. Enroute then stores the
  objects before the call, which costs a second log entry per push. See
  [Read the pushed objects](../reference/hooks.md#read-the-pushed-objects).
- **`post_receive` runs after the refs moved.** Its messages reach the client
  before the `ok` lines, so it is where "open a merge request at …" comes
  from. It cannot undo the push.

A fetch or a clone also runs `authorize` on each of its requests, with
`access` set to read, and `visible_refs` before it is shown refs. It never
runs `pre_receive` or `post_receive`.

The API runs no hooks. [`UpdateRefs`](../reference/the-contract.md#updaterefs)
and [`CreateCommit`](../reference/the-contract.md#createcommit) are your
application acting on its own authority, so it applies its rules before it
calls them.

## Every hook is opt-in

Enroute calls only the hooks that `hooks.enabled` lists, and each hook works
alone. A hook that is not enabled takes a default:

| Hook | Default when not enabled |
| --- | --- |
| `authorize` | Allow read and write. The key is the URL path without `.git`. |
| `visible_refs` | Every ref is visible. |
| `pre_receive` | Every command is allowed. |
| `post_receive` | No call. |

With no hooks, Enroute is an open Git server: use it on loopback, or for a
first test. Enable `authorize` before you expose `listen.git`. See
[Security](../operate/security.md).

## What a hook can and cannot do

| Hook | Can do | Cannot do |
| --- | --- | --- |
| `authorize` | Map a URL to a repository, identify an actor, deny with `401`, `403`, or `404`, and attach up to 8 KiB of context | Inspect pushed objects, rewrite the URL, or run again during the same request |
| `visible_refs` | Filter advertised ref names by actor and access type | Hide objects behind a hidden ref, or hide `HEAD` independently of its target |
| `pre_receive` | Accept or refuse each ref command, and read the uploaded objects if `hooks.pre_receive_mode` is `"objects"` | Modify a command, make a push all-or-nothing, or run for `UpdateRefs` |
| `post_receive` | Send messages to the client and start work in your application | Refuse a completed update, or provide reliable event delivery |

## Failures fail closed

An enabled hook must return a valid answer. A transport error, a status other
than `200`, or no answer within `hooks.timeout` fails the Git request. To deny,
return a denial inside a `200` response: the denial is a decision, and an error
is not.

`pre_receive` must judge every command that it receives. A missing judgement
fails the whole push, so an incomplete answer cannot let a ref update through.

`post_receive` is informational. Enroute sends it after the push has
succeeded, ignores its answer, and does not retry it. If your application must
act on every push, keep its own record, for example by comparing `ListRefs`
with what it last saw.

## Identity and trust

Git clients do not supply a trusted identity. The identity for a request is
the `actor` that `authorize` returns, which Enroute records in traces and
passes to the later hooks. Use an ID from your own records, never a credential
or an email address.

A large push can take minutes between `authorize` and `pre_receive`. Check
anything that can change in that time, such as a revoked token, in
`pre_receive`.

Verify every hook request before you read its body. Enroute signs each one
with an Ed25519 HTTP Message Signature. See
[Verify hook signatures](../reference/verify-the-signature.md).

## Where hooks run

A compute node calls the hooks, stores the uploaded objects, and moves the
refs. Ledger nodes never call your application and never hold the hook signing
key.

For every field of every hook, see the [hook reference](../reference/hooks.md).
To build an endpoint, follow [Handle hooks](../build/03-handle-hooks.md).
