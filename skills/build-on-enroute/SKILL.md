---
name: build-on-enroute
description: "Start a local Enroute git-hosting node with `enroute dev`, installed or from the Docker image, then build a minimal integration against it in the language and framework the user chose: a gRPC client of the contract, and the one HTTP route Enroute calls back on. Holds recipes for the patterns built on top: protected branches, merge requests, a merge queue, checks and CI, private repositories, and mirroring to another Git host. Use this skill when someone wants to try Enroute, develop against it locally, connect an application to the Git API and hooks of Enroute, or build one of those patterns on it."
---

# Build on Enroute

Enroute hosts Git. It terminates the Git connection, stores the objects, and
knows a repository only by the key the application gave it. It does not know
what a repository is called, who owns it, or who may read it. The application
you are about to build answers those questions.

Your task: start a local Enroute, then write the smallest program that works
against it, in the stack the user chose.

## What the user ends up with

Two pieces of their own code. Neither one speaks Git.

| Piece    | Direction      | What it is                                                               |
| -------- | -------------- | ------------------------------------------------------------------------ |
| Client   | user → Enroute | A generated gRPC client. Creates repositories, reads objects, moves refs. |
| Endpoint | Enroute → user | One HTTP POST route. Verifies a signature, then answers four calls.       |

They also get a local Enroute: `enroute dev`, from the `enroute` command when
it is installed, or else from the published image.

Two facts shape the whole build:

- **Every hook is opt-in.** Enroute calls only the hooks that are enabled,
  in `hooks.enabled` or with `enroute dev --hooks`. A hook that is not
  enabled takes its default: `authorize` allows read and write and uses the
  URL path without `.git` as the key, `visible_refs` shows every ref,
  `pre_receive` allows every command, and `post_receive` is not called. This
  skill enables all four.
- **A key may contain `/`**, as in `acme/widgets`. Each segment is ASCII
  letters, digits, `-`, `_`, and `.`. Still key by an id that never changes,
  because a rename must not move a repository.

Everything this needs ships with the skill: the contract, the references, and
the recipes. Build from these files only. Do not read an
Enroute checkout, a published SDK, or another integration on the machine. Each
of those is a different version of a different thing, and the build must match
the contract the running stack serves.

## Work in this order

Do not skip a phase. Each phase ends in a checkpoint that proves the phase
before it, so a failure in phase 4 is never also a question about phase 2.

1. **Agree the shape.** One round of questions, then stop asking.
2. **Start the stack.** Enroute answers before any of the user's code exists.
3. **Write the client.** Create a repository over the contract.
4. **Write the endpoint.** Verify the signature, then answer the four calls.
5. **Run the whole loop.** Clone, push, and prove each hook reaches the client.

## Phase 1: agree the shape

Ask the user, in one message:

- **Language and framework.** For example TypeScript on Next.js, Python on
  FastAPI, Go on net/http, or Rust on axum. You need both. The language decides
  the protobuf tooling. The framework decides how you read a raw request body.
- **Where the code goes.** An existing project, or a new directory.

Do not ask about storage, tenancy, authentication, or deployment. The loop
works without them, and the user can decide them later.

Then **choose the URL of the endpoint yourself.** Under `enroute dev`, the host
is `localhost`. Under the image, the host is `host.docker.internal`, so the
container can reach the host. The port is the
development server of the framework: `3000` for Next.js, `8000` for FastAPI or
Django, `4000` for Phoenix.

Choose the path with care. A forge serves repositories at paths named after
repositories, and a repository can have any name a user types, including the
path the hook is mounted on. Put the route in the namespace the framework keeps
for machine traffic: `/api/…` under Next.js, Nuxt, or SvelteKit; a router or
blueprint under its own prefix in Express, FastAPI, or Flask; a route outside
the resource routes in Rails or Phoenix. `/enroute/hooks` is wrong, because a
repository named `enroute` takes it.

Phase 2 configures the URL before the endpoint exists. Choose it now, write it
down, and serve the route there in phase 4.

Then check the tools:

```sh
git --version
enroute --version    # if this fails, use the image
docker version       # needed only for the image
```

Git is required, and one of `enroute` or Docker. Everything else depends on
the language.

## Phase 2: start the stack

Read `references/local-stack.md` and follow it.

If `enroute` is installed, run `enroute dev` in the user's project directory,
with the endpoint URL. It enables all four hooks and generates a signing key
in `.enroute/dev`:

```sh
enroute dev --hooks http://localhost:3000/api/enroute/hooks
```

Otherwise, run `enroute dev` from the published image, so nothing is cloned
and nothing compiles. Give it the endpoint URL:

```sh
docker run --rm -d --name enroute \
  -p 8080:8080 -p 50051:50051 -v enroute-dev:/data \
  --add-host=host.docker.internal:host-gateway \
  ghcr.io/enroute-sh/enroute:0.4.0 dev --dir /data --host 0.0.0.0 \
  --hooks http://host.docker.internal:3000/api/enroute/hooks
```

The `enroute-dev` volume keeps the data and the signing key. The endpoint gets
the public key from the banner (`docker logs enroute`) or with
`docker cp enroute:/data/hooks-signing-key.pub.pem .`.

Git does not serve until the endpoint answers, which is phase 4. Nothing stands
in for an enabled hook. Tell the user this now, rather than let a failed clone
tell them.

These values are fixed. None of them is a secret:

| Thing           | Value                                                                       |
| --------------- | --------------------------------------------------------------------------- |
| Git             | `http://127.0.0.1:8080`                                                     |
| Contract (gRPC) | `127.0.0.1:50051`, plaintext HTTP/2                                         |
| Signing key     | `.enroute/dev/hooks-signing-key.pub.pem`, or `/data/hooks-signing-key.pub.pem` in the container |

The contract authenticates nobody, and needs no header. This stack puts nothing
in front of the listener, so anything that can reach 50051 can read, write and
delete every repository.

That is correct on a laptop and wrong in a deployment, where the port must be
reachable only by the user's application. Tell the user this, so they do not
ship the local shape.

**Checkpoint.** The `enroute dev` banner, or `docker logs enroute`, shows the four hooks and a `signing key id`. Do not continue until it does.
`references/troubleshooting.md` covers the failures.

## Phase 3: write the client

Read `references/contract.md`. It gives the codegen command for each language,
the services, and the rules that catch people out.

The contract ships with this skill. Copy it into the user's project from the
`proto/` directory beside this file:

```sh
mkdir -p <the user's project>/proto
cp -R <this skill>/proto/enroute <the user's project>/proto/
```

**Copy all seven files, `hook/v1alpha1/hook.proto` among them, and generate all
seven.** That file declares no service, so a codegen command written around the
five services skips it without a message. Phase 4 cannot be written without
the types in it.

Copy rather than generate from the skill in place. The next upgrade replaces
the skill, and the contract of a project must not move when that happens.

Then generate a client and write the smallest program that:

1. Calls `RepositoryService.CreateRepository` with a key of the user's own
   choosing.
2. Prints the key the answer carries.

Use a key the application already has for a repository: a row id, a UUID, a
ULID. Never a name a user can rename. `references/contract.md` gives the rules.

**Checkpoint.** The program prints the key it sent, and `ListRefs` on that key
answers with no refs rather than an error. Call the create a second time with
the same key: it answers with the same repository and makes nothing new. Then
check the generated code holds `HookRequest` and `HookResponse`. A codegen run
that skipped `hook.proto` fails here, not two phases later.

Stop here if the key does not come back. That key proves the contract and the
database both work. After it, every failure is in the user's
code, not the stack.

No endpoint answers yet, so Git has nothing to ask and a clone fails. The Git
loop is phase 5.

## Phase 4: write the endpoint

Read `references/endpoint.md` for the four calls, and `references/signature.md`
for the verification. Write them in that order: the signature first, the calls
second.

The route is one POST. It receives a binary `HookRequest` and answers `200`
with a binary `HookResponse`. Four things go wrong more than anything else, so
handle each one on purpose:

- **Read the raw body bytes.** The signature covers exactly what arrived. A
  framework that parses or re-serializes the body first breaks every call.
  `references/endpoint.md` names the call to use for each framework.
- **Verify against the URL Enroute is configured with**, not the arriving
  `Host` header and path. A proxy rewrites those.
- **Answer the call that was asked.** An empty answer makes Enroute refuse the
  Git request. That is deliberate: it means "not implemented", not "allow".
- **Deny with `200`.** A refusal is a field in the answer. A non-200 is a
  fault, and Enroute fails the Git request closed.

There is no table from a name to an id of Enroute's. The key the application
chose at `CreateRepository` is what every call names. Key by the primary key of
the project's repositories table, never by the path a repository is served
under, because a rename moves the path. `authorize` looks the path up as the
project already does to render a page, and answers with that row's id.

**Checkpoint.** Serve the route locally and check that it refuses an unsigned
`POST` with `401` and nothing else. An endpoint that answers an unsigned call
passes every test in phase 5 and is useless in production.

## Phase 5: run the whole loop

The stack already names the user's endpoint, from phase 2. Start the endpoint
and go to the checkpoint.

If the endpoint ended up on a different URL, stop `enroute dev` and start it
again with the new `--hooks` URL. For the image, run `docker stop enroute`,
then the same `docker run` with the new URL. The volume keeps the data and
the key. Configuration is read once, at startup.

Enroute signs over that exact authority and path. If it differs by one byte
from what the endpoint verifies against, every call is a `401` with no reason
given.

With the image on Linux, `host.docker.internal` does not resolve without
`--add-host`. See `references/troubleshooting.md`.

**Checkpoint.** Run the whole loop against the user's endpoint:

1. Create a repository over the contract, and name it in the user's table.
2. `git clone` it by that name, with a credential the endpoint accepts. This
   is the first Git traffic, so it is the first proof that `authorize` works.
3. Commit and push. It lands.
4. Make the endpoint refuse one refname from `pre_receive`. Push it, and watch
   Git print the reason.
5. Make the endpoint drop one refname from `visible_refs`. Fetch, and watch
   the ref not be advertised.
6. Make `post_receive` answer with a message. Push, and watch Git print it.

Steps 4 to 6 tell a working endpoint apart from one that returns `200` and
decides nothing. Run all three.

## Then stop

The loop above is the whole integration. Do not build a UI, a permission
model, a database schema, or CI hooks unless the user asks. Report what runs,
how to start it again, and which of the four calls are still stubs.

Point the user at [the docs] for the reference version of everything here.

## When the user asks for a pattern

The patterns are protected branches, merge requests, a merge queue, checks,
private repositories, CI on push, a mirror to GitHub, a repository browser, a
file tree, and a diff on a page. A request as broad as "build the UI" is those
last three. They are recipes, not something to invent. `references/recipes.md`
is the index: the rules every recipe assumes, and one line per recipe. Read
it, then open the one file the user asked for. Never the whole directory.

Build a recipe only when the user asks for it, and only after phase 5 passes.
A pattern built on a loop that does not work is two failures you cannot tell
apart.

## References

Read these.

| File                            | Read it for                                                                    |
| ------------------------------- | ------------------------------------------------------------------------------ |
| `references/local-stack.md`     | `enroute dev`, installed or from the image, hook defaults, resetting it.       |
| `references/contract.md`        | Codegen per language, the RPCs, the rules.                                     |
| `references/endpoint.md`        | The four calls, and reading a raw body per framework.                          |
| `references/signature.md`       | RFC 9421 verification, with a test vector.                                     |
| `references/troubleshooting.md` | Every failure with a known cause.                                              |
| `references/recipes.md`         | The index of patterns on top of the loop. One file each under `references/recipes/`. |

## What ships with this skill

Copy this out. The next upgrade replaces it, so a project that reads it in
place moves when the skill does.

| Directory        | What it is                                                        | Copy it to                      |
| ---------------- | ----------------------------------------------------------------- | ------------------------------- |
| `proto/enroute/` | The contract. Seven files, `hook/v1alpha1/hook.proto` among them. | The user's project, in phase 3. |

[the docs]: https://github.com/enroute-sh/enroute/blob/HEAD/docs/README.md
