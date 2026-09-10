# The contract: calling Enroute

The contract is gRPC. The service files under `proto/enroute/api/v1alpha1/`
define it: `repository.proto`, `ref.proto`, `object.proto`, and `sync.proto`,
one per service. They import `enroute/common/v1alpha1/common.proto`, which
holds `RepoKey` and `ObjectId`. A sixth file, `enroute/hook/v1alpha1/hook.proto`,
holds what the endpoint answers with and declares no service. Copy the whole
`enroute/` tree into the user's project and generate every file in it.

`v1alpha1` is not a typo for `v1`. The suffix tracks the major version of the
server, which is `0`, and a `0.y.z` API may change at any time. It becomes
`v1` when the server reaches `1.0.0`.

## Connecting

| Setting | Local value                                                                |
| ------- | -------------------------------------------------------------------------- |
| Address | `127.0.0.1:50051`                                                          |
| TLS     | None. Plain HTTP/2, so use the "insecure" credentials your library offers. |
| Header  | `x-enroute-tenant: dev`                                                    |

The contract authenticates nobody. The header names which tenant a call is
for. A deployment has a proxy in front that authenticates the caller and sets
the header. The local stack has no proxy, so send the header yourself.

Send the header on every call. A call with no header, more than one, or an
unknown tenant is `UNAUTHENTICATED`. Enroute resolves every repository key
within that tenant, so another tenant's key is `NOT_FOUND`.

A deployment puts TLS in front of the same port. Nothing else changes.

## Smoke test without writing code

`grpcurl` needs no install if Docker is running. The server answers
reflection, so you can list the services with no proto files at all. The call
below names the copied proto files instead, which works the same way:

```sh
docker run --rm --network <compose-project>_default \
  -v "$PWD/proto:/proto:ro" fullstorydev/grpcurl:latest \
  -plaintext -import-path /proto -proto enroute/api/v1alpha1/repository.proto \
  -H "x-enroute-tenant: dev" \
  -d '{"repo": {"key": "repo-4f2a1c"}}' \
  enroute:50051 enroute.api.v1alpha1.RepositoryService/CreateRepository
```

`<compose-project>` is the directory the stack was copied to. For
`enroute-stack` the network is `enroute-stack_default`, unless `-p` was given.
`docker network ls` shows it. A call from the user's code works the same way,
and that is the point of phase 3.

It answers:

```json
{ "repository": { "repo": { "key": "repo-4f2a1c" }, "defaultBranch": "refs/heads/main" } }
```

## The services

| Service             | RPC                | Answers                                                        |
| ------------------- | ------------------ | -------------------------------------------------------------- |
| `RepositoryService` | `CreateRepository` | The repository under the key you gave. Idempotent on the key.  |
|                     | `GetRepository`    | The default branch, and the last push time.                    |
|                     | `ListRepositories` | Every repository of the tenant, in key order, a page at a time. |
|                     | `DeleteRepository` | Nothing. Idempotent, and the storage is reclaimed later.       |
| `RefService`        | `ListRefs`         | Every ref, or the ones under given prefixes.                   |
|                     | `UpdateRefs`       | One outcome per update, atomically applied.                    |
|                     | `IsAncestor`       | Whether one commit is in another's history.                    |
| `ObjectService`     | `GetObject`        | A stream: one header, then the bytes.                          |
|                     | `ListTree`         | A stream of paths under a commit or tree.                      |
|                     | `ListCommits`      | A stream of commits, newest first, paged.                      |
|                     | `DiffCommit`       | A stream of changed paths, as blob id pairs.                   |
|                     | `FindMergeBases`   | Where two histories last agreed.                               |
| `SyncService`       | `PushToRemote`     | One outcome per ref sent to another git server.                |

## Rules that catch people out

**A repository key is yours.** Enroute has no column for a name or an owner.
It stores the key the application passed to `CreateRepository`, compares it
byte for byte, and reads no meaning out of it. There is no id of Enroute's to
keep beside the id the application already has. A key is 1 to 256 bytes of
ASCII letters, digits, `-`, `_`, and `.`, starting and ending with a letter or
digit. It holds no `/`. Use a row id, a UUID, or a ULID. Never `owner/name`,
because a rename would then point at a different repository.

**`CreateRepository` is idempotent on the key.** A second create with the same
key answers with the same repository and makes nothing new. A retry needs no
bookkeeping. `default_branch` is read only by the create that makes the
repository.

**`ListRepositories` is the one read that starts from no key.** It exists for
an application whose own table went missing, and for finding a repository
nothing names. Send `next_page_token` back until it is empty. A short page is
not the last page.

**Object ids are lowercase hex strings**, not raw bytes.

**A rejection is an outcome, not a failure.** `UpdateRefs` can land some
updates and refuse others, and the RPC still returns `OK`. Read
`UpdateRefsResponse.outcomes`, which come back in the order the updates were
sent. An outcome with no `rejection` landed.

**`UpdateRefs` cannot introduce objects.** Every tip it names must already be
a commit the repository stores. Push over git first, then move the ref. That
is what makes `UpdateRefs` a merge and not a write path.

**`UpdateRefs` runs no `pre-receive`.** The application is the caller there,
so there is nobody to ask. The user's policy runs in their own code before the
call.

**Enroute moves a ref backwards if told to.** It reports ancestry with
`IsAncestor` and holds no opinion about it. The user decides what may be
rewritten.

**Diff against the merge base, not the tip.** To see what a branch changed
against a trunk, call `FindMergeBases` first and pass that commit as
`DiffCommit.base_commit_id`. Against the tip of the trunk, everything that
landed there since reads as a reversion.

**Check `truncated` and `exhausted`.** `ListTree` and `DiffCommit` set
`truncated` when they stopped at a server limit, and what arrived is then the
first part of the answer. `FindMergeBases` sets `exhausted` to mean "unknown",
not "no shared history".

## Generating a client

For every language, the input is the copied `proto/enroute/` tree. It lives in
three places, and they hold the same six files:

| Where                                          | When to take it from there                                                                     |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `proto/enroute/` beside this skill             | Always, unless one of the rows below applies. Phase 3 copies from here.                        |
| `/usr/share/enroute/proto` in the deploy image | The user pinned an Enroute this skill does not match.                                          |
| `proto/enroute/` in a checkout                 | The user has one and does not want to pull an image.                                           |

The copy in this skill is the contract at the time the skill was installed.
That is the right one while the user runs a matching Enroute, which the
`docker compose` in phase 2 gives them. If they pinned a different version,
take it out of the image they run:

```sh
docker create --name enroute-proto ghcr.io/enroute-sh/enroute:<version>
mkdir -p proto && docker cp enroute-proto:/usr/share/enroute/proto/enroute proto/
docker rm enroute-proto
```

A mismatch shows up as a field that is not there, never as a wrong answer. The
packages are `v1alpha1` against a `0.y.z` server, so the contract may break
between versions.

Generate from that copy, not from reflection. The server answers reflection
too, with the whole contract and its comments. `grpcurl -plaintext
127.0.0.1:50051 list` is the fastest way to check the stack by hand, and
`describe enroute.hook.v1alpha1.HookRequest` reaches the hook file that `list`
does not show. Use reflection to look, not to build. What comes back is a
descriptor set, and the user's project wants files it can commit.

**TypeScript / Node.** `@bufbuild/protobuf` with `@bufbuild/protoc-gen-es`,
and `@connectrpc/connect-node` for the transport. `ts-proto` with
`@grpc/grpc-js` is the other common pair. The gRPC transport of Connect speaks
HTTP/2 to `http://127.0.0.1:50051`.

**Python.** `pip install grpcio grpcio-tools`, then:

```sh
python -m grpc_tools.protoc -I proto \
  --python_out=. --grpc_python_out=. --pyi_out=. \
  proto/enroute/*/v1alpha1/*.proto
```

Connect with `grpc.insecure_channel` and send the header as metadata.

**Go.** `protoc-gen-go` and `protoc-gen-go-grpc`. Dial with
`grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))`
and attach the header with `metadata.AppendToOutgoingContext`.

**Rust.** `tonic-build` in a `build.rs`, and `tonic` for the client.

**Java / Kotlin.** `protobuf-gradle-plugin` with the `grpc-java` plugin, and
`usePlaintext()` on the channel builder.

**Ruby.** `grpc-tools`, then `grpc_tools_ruby_protoc`.

**C#.** The `Grpc.Tools` package generates on build from a `<Protobuf>` item.

**A codegen plugin is found on `PATH`, not by the package manager.** Every
command above calls a plugin binary by name. A plugin installed into a
directory of its own is invisible until that directory is on `PATH`. The
error says the plugin is not installed, when it is.

**Build the project before writing anything on top of the generated code.**
The codegen defaults are chosen for the language, not for the project that
compiles the result. Three settings go wrong most often: the module format,
the import specifiers, and where the output may live. All three fail in the
build of the project, not in the generate. One build after the first generate
turns a mystery two phases later into a flag on the generate command.

**Generate every file in the tree, `hook.proto` included.** It declares no
service, so a command written around the four services skips it, and phase 4
then has no `HookRequest` or `HookResponse` to decode with. Name the whole
tree, `proto/enroute/*/v1alpha1/*.proto` or the equivalent, rather than the
service files one by one.

**An object id is a message, not a string.** `ObjectId { hex }` appears
everywhere an id appears, so a client cannot pass a refname where an id
belongs. Where an id is optional, the old side of a create or the new side of
a delete, the field is unset. Not empty, and not forty zeroes.
