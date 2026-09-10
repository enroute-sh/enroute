# Hooks

Hooks let your application make decisions that Enroute cannot make from Git
data alone. Enroute sends a signed HTTP `POST` to the tenant's configured
endpoint and waits for the response.

| Hook | Timing | Application decision |
| --- | --- | --- |
| `authorize` | Once at the start of a Git request | Resolve the URL, identify the actor, and grant or deny access |
| `visible_refs` | Before advertising refs | Select the refs the actor may discover |
| `pre_receive` | After object upload and before ref updates | Accept or reject each requested ref update |
| `post_receive` | After ref updates | Handle updates that succeeded |

For request and response fields, see the [hook reference](../reference/hooks.md).

## Hook limits

| Hook | Can do | Cannot do |
| --- | --- | --- |
| `authorize` | Map a URL to a repository, identify an actor, deny with `401`, `403`, or `404`, and attach up to 8 KiB of context | Inspect pushed objects, rewrite the URL, or run again during the same request |
| `visible_refs` | Filter advertised ref names by actor and access type | Hide objects behind a hidden ref or hide `HEAD` independently of its target |
| `pre_receive` | Accept or refuse individual ref commands after reading uploaded objects | Modify a command, make a push all-or-nothing, or run for `UpdateRefs` |
| `post_receive` | Send client messages and start application work | Refuse a completed update or provide reliable event delivery |

## Responsibilities

Enroute does not store access rules, branch protection, or review policy.
Your application supplies those decisions from its identity system and its own
data.

Required hooks fail closed. A required hook must return a valid response;
transport and server errors are failures, not policy denials. Return a denial
inside a `200` response when the application made the decision to deny.

`pre_receive` must include a judgment for every command it receives. A missing
judgment rejects the push. This prevents an incomplete policy response from
silently allowing a ref update.

`post_receive` is informational. Enroute sends it after a successful push,
ignores its response, and does not retry it. Record or reconcile state
separately if correctness depends on processing an event.

## Request context

`authorize` returns a repository key, actor ID, and optional context. Enroute
resolves the key within the request tenant, records the actor in traces and
usage data, and passes the context unchanged to later hooks for that request.
It does not parse or log the context.

Large pushes can take minutes between authorization and `pre_receive`. Recheck
time-sensitive permissions or state in the later hook.

## Trust boundary

Verify every hook request before reading its body. Enroute signs hooks with an
Ed25519 HTTP Message Signature. Git clients do not supply a trusted identity;
the identity for the request is the `actor` returned by `authorize`.

See [Verify hook signatures](../reference/verify-the-signature.md) for the
verification procedure.

## Ingestion

The server process runs hooks and moves refs. Ingestion stores uploaded
objects and builds indexes, and can run separately. It does not resolve
tenants, move refs, or use the hook signing key. See [Storage](storage.md).
