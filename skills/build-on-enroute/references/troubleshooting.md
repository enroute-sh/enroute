# Troubleshooting

Symptom, cause, fix. Read the section for the phase that failed.

## The stack will not start

**`Bind for 0.0.0.0:5433 failed: port is already allocated`.** A Postgres
already uses that port. Nothing outside the compose network needs it, so stop
publishing it. The stack is the user's copy, so edit `compose.yaml` and
comment out the `ports` block under `postgres`:

```yaml
services:
  postgres:
    # ports:
    #   - "5433:5432"
```

For `psql` from the host, publish it on another host port instead:
`"5434:5432"`.

**Port 8080 or 50051 already allocated.** Change the left side of the
published port in `compose.yaml`, and use the new one everywhere. The
container side must stay, because the configuration names it.

**Something starts compiling Rust.** The `up` ran in the repository's own
compose file, which builds from source. `stack/compose.yaml` names no build.

**`denied` or `401 Unauthorized` on the pull.** The package is private. Run
`docker login ghcr.io` with a **classic** token holding `read:packages`. A
fine-grained token fails the same way: the registry does not accept one, and
it says only `denied`.

**`manifest unknown`.** The tag does not exist. A bare `docker pull` means
`:latest`, which names the current release; to hold this stack on the
release it was built against, name the tag `stack/compose.yaml` names.

**`no matching manifest for linux/...`.** The tag is a per-architecture digest,
not the manifest list. Use `latest`, a version, or `sha-<commit>`.

**`enroute` restarts in a loop.** Read `docker compose logs enroute`. The usual
cause is a database that is not ready, which `depends_on` already handles. The
other cause is a database a different image built. The log names the
migration step it stopped on. Run `docker compose down -v`, then `up` again.

**`step ... was applied and has changed since`.** One image built the
database, and another that spells a step differently now serves it. The
stack starts one release, so the cause is a volume that outlived a skill
somebody reinstalled. Run `docker compose down -v`, then `up` again.

## The contract refuses

**`UNAUTHENTICATED: no tenant was named`.** The header is missing, doubled, or
misspelled, or the tenant is not in the list. Every one of these reads alike
on purpose. Against the local stack the header is `x-enroute-tenant: dev`.
The reason is in the server log at `debug`:
`RUST_LOG=debug docker compose up -d enroute`. Enroute refuses a repeated
header rather than guess at it.

**`NOT_FOUND` for a repository that exists.** The key belongs to another
tenant, or was never created in this one. One deployment serves many tenants,
and a key resolves only within its own.

**`INVALID_ARGUMENT` on a key.** A key is 1 to 256 bytes of ASCII letters,
digits, `-`, `_`, and `.`, starting and ending with a letter or digit. A `/`
is refused. See `contract.md`.

**The client cannot connect.** The contract is plain HTTP/2, with no TLS. Use
the "insecure" or "plaintext" credentials the library offers. A TLS client
against a plaintext port reports a handshake failure, not a refusal.

**`protoc` cannot find an import.** Keep the directory structure. The file
must be at `<import path>/enroute/api/v1alpha1/repository.proto`, and the
import path is the directory that *contains* `enroute/`.

## Git will not clone or push

**Every git request fails, and the endpoint logs nothing.** Nothing stands in
for the endpoint. Until it answers, git cannot serve. This is expected before
phase 4.

**`repository not found`, or a 404.** Enroute found no tenant for the
hostname, or the endpoint answered `DENIAL_NOT_FOUND`, or the key it granted
is not one this tenant holds. The local tenant claims every hostname, so
suspect the endpoint. Log what `authorize` was asked and what it answered.

**A 404 the endpoint never sees.** Count the `authorize` calls, not only their
answers. Zero calls for one path and one call for another means Enroute
refused the path itself: an empty segment, a `.` or `..` segment, an encoded
`/`, a control character, or a path over 1024 bytes. One call and a
`DENIAL_NOT_FOUND` is the opposite problem: the path arrived and the endpoint
did not know it. Check whether the row is keyed without the `.git` the clone
URL carries, because Enroute passes the path through as the client spelled it.

**A password prompt, or a 401.** The credential is whatever the endpoint's
`authorize` accepts. Put it in the URL for a local loop:
`http://user:token@127.0.0.1:8080/<name>.git`. Git needs some username before
it sends a password, so accept any username and read the password.

**`fatal: unable to access ... 500`.** The endpoint faulted. Enroute fails a
git request closed when the endpoint is unreachable, answers a non-200,
answers a body that will not decode, or takes longer than `hooks.timeout_secs`.
`docker compose logs enroute` names which one.

**`the hooks did not answer the authorize call`.** The endpoint answered `200`
with an empty `HookResponse.answer`, or set the wrong member of the `oneof`.
Answer the call that was asked. The same message names `pre-receive` and
`visible-refs` for those calls.

**The push is refused with a reason.** That is the endpoint working. The
reason is the `RefJudgement.reason` it answered a `JUDGEMENT_REFUSE` with,
printed verbatim.

**`the pre-receive hook judged 2 of 3 commands`.** The endpoint answered about
some of the push and not the rest, so Enroute failed all of it. The message
names what was left out, and names any refname you judged that the push never
asked for — which is where a misspelt refname shows up. Push a judgement on
every pass of the loop over `req.commands`, including the passes where no rule
applies.

**`the hooks judged refs/heads/... neither way`.** The endpoint sent a
`RefJudgement` with no `judgement`, which proto3 cannot tell from a field
nobody set. Set `JUDGEMENT_ALLOW` or `JUDGEMENT_REFUSE`.

**`warning: You appear to have cloned an empty repository`.** Expected. The
repository was just created and nothing has been pushed.

## Every hook call is a 401

One cause almost every time: **the URL Enroute signs over is not the URL the
endpoint verifies against.**

Enroute signs `@authority` and `@path` from the URL the tenant was registered
with. The endpoint must verify against the same string, from its own
configuration and not from the arriving request.

Check both sides. Enroute's half is `hook_endpoint_url` in
`config/tenants.toml` in the stack directory. Then check the configured public
URL of the endpoint. Look for a trailing slash, `localhost` against
`127.0.0.1`, a missing or present port, `http` against `https`, and a path that
differs by one segment. Any one of them fails every call.

**Enroute still calls the old URL after an edit to the tenants file.** Enroute
reads the file on a timer, so wait two seconds. Then check
`docker compose logs enroute` for an `ERROR` that says the edit would not
load, which leaves the previous tenants serving.

**The digest does not match.** A middleware parsed or re-encoded the body
before the handler saw it. Read the raw bytes, once, and use those bytes for
both the digest and the decode.

**The signature verifies locally but not in the stack.** Check the skew. The
limit is 300 seconds. A container clock that drifted, or a laptop resumed
from sleep, fails every call. A restart of Docker synchronizes it.

## Enroute cannot reach the endpoint

**On Linux, `host.docker.internal` does not resolve.** `stack/compose.yaml`
carries this, so check that it survived any edit to the `enroute` service:

```yaml
services:
  enroute:
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

**The endpoint listens on loopback only.** A service bound to `127.0.0.1` on
the host is not reachable from a container. Bind it to `0.0.0.0`.

**The endpoint runs in a container of its own.** Put it on the same compose
network and name it by its service name: `http://my-app:3000/api/enroute/hooks`.
`host.docker.internal` is only for a process on the host.

## Tools

**`openssl pkey` says "unsupported algorithm" for Ed25519.** macOS ships
LibreSSL as `openssl`. Install OpenSSL, or read the key with the language
runtime in use.

**`grpcurl` is not installed.** Run it as a container. See `contract.md`.

**`grpcurl` cannot list the services.** The server answers reflection, so the
usual cause is the wrong address, or a TLS client against a plaintext port:
pass `-plaintext`. You can also skip reflection by passing `-import-path` and
`-proto` for the copied proto files.

## Starting over

```sh
docker compose down -v
docker compose up -d
```

This drops the volumes, so the schema and every object come back fresh. It
costs a pull at most. The tenants survive it, because they are a file beside
the compose file, and a change to them never needed a reset.
