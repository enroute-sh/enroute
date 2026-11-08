# Troubleshooting

Symptom, cause, fix. Read the section for the phase that failed.

## The stack will not start

**Port 8080 or 50051 already allocated.** Under the image, change the left
side of `-p`, for example `-p 18080:8080`, and use the new port everywhere.
The container side must stay, because `enroute dev` listens on it. Under
`enroute dev`, pass `--port-base N` to listen on N, N+1, and N+2.

**`--name enroute` is already in use.** An earlier container is still
running. `docker stop enroute`, then run again.

**`denied` or `401 Unauthorized` on the pull.** The package is private. Run
`docker login ghcr.io` with a **classic** token holding `read:packages`. A
fine-grained token fails the same way: the registry does not accept one, and
it says only `denied`.

**`manifest unknown`.** The tag does not exist. A bare `docker pull` means
`:latest`, which names the current release; to hold this stack on the
release it was built against, name the tag `local-stack.md` names.

**`no matching manifest for linux/...`.** The tag is a per-architecture digest,
not the manifest list. Use `latest`, a version, or `sha-<commit>`.

**The container stops at once.** Run it without `-d` to see the error. The
usual cause is a volume that a different release wrote, which outlived a skill
somebody reinstalled. Run `docker volume rm enroute-dev`, then start it again.

## The contract refuses

**`NOT_FOUND` for a repository that exists.** The key was never created in
this deployment, or it was created in another one. Keys are unique across a
deployment and mean nothing outside it. `ListRepositories` shows what is
actually here.

**`INVALID_ARGUMENT` on a key.** A key is at most 256 bytes of segments
joined by `/`, such as `acme/widgets`. A segment is ASCII letters, digits, `-`,
`_`, and `.`, and starts and ends with a letter or digit. An empty segment is
refused: a leading, trailing, or doubled `/`. See `contract.md`.

**The client cannot connect.** The contract is plain HTTP/2, with no TLS. Use
the "insecure" or "plaintext" credentials the library offers. A TLS client
against a plaintext port reports a handshake failure, not a refusal.

**`protoc` cannot find an import.** Keep the directory structure. The file
must be at `<import path>/enroute/api/v1alpha1/repository.proto`, and the
import path is the directory that *contains* `enroute/`.

## Git will not clone or push

**Every Git request fails, and the endpoint logs nothing.** Nothing stands in
for an enabled hook. Until the endpoint answers, Git cannot serve. This is
expected before phase 4.

**`repository not found`, or a 404.** The endpoint answered
`DENIAL_NOT_FOUND`, or the key it granted names no repository here. Enroute
reads nothing from the hostname, so suspect the endpoint. Log what `authorize`
was asked and what it answered.

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
Git request closed when the endpoint is unreachable, answers a non-200,
answers a body that will not decode, or takes longer than `hooks.timeout`.
The `enroute dev` terminal, or `docker logs enroute`, names which one.

**`the hooks did not answer the authorize call`.** The endpoint answered `200`
with an empty `HookResponse.answer`, or set the wrong member of the `oneof`.
Answer the call that was asked. The same message names `pre-receive` and
`visible-refs` for those calls.

**An empty repository clones, and one with refs does not.** `visible_refs` is
enabled and the endpoint does not answer it. Enroute does not call it for a
repository with no refs. Answer it, or leave it out of the enabled hooks
until the endpoint does.

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

Enroute signs `@authority` and `@path` from its configured
`hooks.endpoint_url`. The endpoint must verify against the same string, from
its own configuration and not from the arriving request.

Check both sides. Enroute's half is the `--hooks` URL of `enroute dev`, in
the terminal or in the container. Then check the configured public URL of the
endpoint. Look for a trailing slash, `localhost` against
`127.0.0.1`, a missing or present port, `http` against `https`, and a path that
differs by one segment. Any one of them fails every call.

**Enroute still calls the old URL.** The URL is read once, at startup, so a
new one needs a restart with the new `--hooks`. Under the image, run
`docker stop enroute`, then `docker run` again. The banner in
`docker logs enroute` shows the URL it started with.

**The digest does not match.** A middleware parsed or re-encoded the body
before the handler saw it. Read the raw bytes, once, and use those bytes for
both the digest and the decode.

**The signature verifies locally but not in the stack.** Check the skew. The
limit is 300 seconds. A container clock that drifted, or a laptop resumed
from sleep, fails every call. A restart of Docker synchronizes it.

## Enroute cannot reach the endpoint

**On Linux, `host.docker.internal` does not resolve.** Add this flag to
`docker run`:

```sh
--add-host=host.docker.internal:host-gateway
```

**The endpoint listens on loopback only.** A service bound to `127.0.0.1` on
the host is not reachable from a container. Bind it to `0.0.0.0`.

**The endpoint runs in a container of its own.** Put both containers on one
user-defined network (`docker network create`, then `--network` on each), and
name the endpoint by its container name:
`http://my-app:3000/api/enroute/hooks`. `host.docker.internal` is only for a
process on the host.

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

Under `enroute dev`, run `enroute dev --clean` with the same flags. Under the
image:

```sh
docker stop enroute
docker volume rm enroute-dev
```

Then run the same `docker run` again. This drops the volume, so the node's
log, every object, and the signing key come back fresh. Give the endpoint the
new public key.
