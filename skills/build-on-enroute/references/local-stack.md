# The local stack

Postgres and Enroute, in containers, from the published image. Nothing
compiles.

## The published image

Every release is pushed to the GitHub Container Registry as
`ghcr.io/enroute-sh/enroute`.

| Tag            | What it names                                  |
| -------------- | ---------------------------------------------- |
| `latest`       | The current release. Moves with each one.      |
| `<version>`    | One release, for example `0.1.0`. Not rebuilt. |
| `sha-<commit>` | One commit. The tag that is never reused.      |

The `latest` tag moves with each release. A version tag does not, so name one
to hold a stack on the release it was built against.

The image is a manifest list for `linux/amd64` and `linux/arm64`, so it runs
on an Intel or an Apple Silicon machine without emulation.

The image holds `enroute` and `enroute-schema`. `enroute` applies its own
database tables at startup, so the local stack never runs the second one.

The image also carries the contract it serves, at `/usr/share/enroute/proto`.
That copy is for a project that generates a client against a different
Enroute. This skill ships its own copy, and `contract.md` says which to use.

**The image and this skill are in step.** `stack/compose.yaml` names the exact
release this skill shipped beside, so there is nothing to look up and nothing
to pin. A skill installed a year ago still starts the Enroute its recipes and
its proto were written against.

There is no variable to point the stack at another version, and doing so is
not supported. The recipes, the proto, and the server are one release, and the
contract is not stable between `0.x` releases, so a newer server is a mismatch
and not an upgrade. **Install the skill again to move both:**

```sh
npx skills add enroute-sh/enroute
```

### While the package is private

```sh
docker login ghcr.io -u <github-username>
```

The password is a GitHub personal access token with `read:packages`. It must
be a **classic** token. The registry refuses a fine-grained one, and its
refusal does not say so. An anonymous pull answers `401` until the package is
public.

Access to the image follows access to the repository: whoever can read the
source can pull the image.

## Start it

Everything the stack needs is in the `stack/` directory beside this skill: a
compose file and the two configuration files it mounts.

```sh
cp -R <this skill>/stack enroute-stack
cd enroute-stack
$EDITOR config/tenants.toml    # hook_endpoint_url is the user's
docker compose up -d
```

Copy it rather than run it in place. `docker compose` names the project after
the directory, the volumes belong to that project, and an upgrade replaces the
skill directory, which would strand them.

`hook_endpoint_url` in `config/tenants.toml` is the route the user's endpoint
will serve. It does not have to exist yet. Enroute calls it when a git request
arrives, and not before. Enroute reads the file again every two seconds, so a
wrong value costs an edit and not a restart.

Git does not serve until that endpoint answers. Until then the contract is all
that works, and that is the checkpoint for this phase.

### Against a checkout instead

A person who has the repository runs `docker compose up -d` in it and gets the
same two services, built from source. That is the path for a contributor, and
it compiles the workspace. `docs/internals/development.md` in the repository
describes it.

## What is running

| Service    | Port               | What it is                              |
| ---------- | ------------------ | --------------------------------------- |
| `postgres` | `5433` on the host | Every table Enroute owns, in one schema. |
| `enroute`  | `8080`, `50051`    | Git on 8080, the contract on 50051.      |

Nobody applies anything to Postgres first. Enroute creates its tables and, at
every start, brings them up to what the image reads. It applies only what is
new, so an upgrade is a new image tag and nothing else.

`postgres` publishes `5433` and not `5432`, because a developer machine
usually has one on `5432`. If `5433` is taken too, see `troubleshooting.md`.

`config/` is mounted read-only at `/etc/enroute` and holds both files.
`enroute.toml` is the deployment: where objects go, where a push stages, what
each listener binds to. `--config` is the only flag the server takes. Enroute
reads the file once, so an edit needs `docker compose restart enroute`.
[`dev/enroute.example.toml`] in the repository gives the full format with
comments.

There is no tenant to register and no command that registers one. The tenants
are in `config/tenants.toml`, which Enroute reads again every two seconds. To
add one is an edit and no restart. [`dev/tenants.example.toml`] gives that
format with comments.

Enroute refuses a file that does not load rather than serve it. For the
tenants, the server logs at `ERROR` and keeps serving what it read last. For
the configuration, the first read must succeed or the server does not start.
Mount the *directory*, never the file: a bind mount of one file pins one
inode, and a tenants file is replaced by a rename.

## Fixed values

None of these is a secret. They are fixed so a caller does not have to read a
container's logs to find them.

| Thing            | Value                                           |
| ---------------- | ----------------------------------------------- |
| Git base URL     | `http://127.0.0.1:8080`                         |
| Repository URL   | `http://127.0.0.1:8080/<path>.git`              |
| Contract address | `127.0.0.1:50051`, plaintext HTTP/2, no TLS     |
| Contract tenant  | `x-enroute-tenant: dev`                         |
| Tenant id        | `dev`, claiming `*`, so any hostname reaches it |
| Signing key      | RFC 9421's published `test-key-ed25519`         |
| Key id           | `poqkLGiymh_W0uP6PZFw-dvez3QJT5SolqXBCW38r0U`   |

The contract authenticates nobody. A call names its tenant in a header, and
this stack puts nothing in front of the listener to set it. That is fine on a
laptop and is not how to deploy.

The user's endpoint decides what a git client presents as its credential. It
is whatever `authorize` accepts, so pick something in phase 4 and use it.

The public half of the signing key, which the endpoint verifies against:

```
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAJrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=
-----END PUBLIC KEY-----
```

Derive it from the private key in `config/enroute.toml` at any time:

```sh
openssl pkey -pubout <<'EOF'
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJ+DYvh6SEqVTm50DFtMDoQikTmiCqirVv9mWG9qfSnF
-----END PRIVATE KEY-----
EOF
```

macOS ships LibreSSL as `openssl`, and LibreSSL refuses Ed25519 with
"unsupported algorithm". Use `brew install openssl` and the `openssl` in its
`bin`, or read the key with the language runtime already in use.

Generate a key of your own for anything that is not a local stack:

```sh
openssl genpkey -algorithm ed25519 -out hook-signing-key.pem
```

Name it in the configuration as `signing_key` under `[hooks]`, usually as
`"${ENROUTE_HOOK_SIGNING_KEY}"` so the file holds no key, and give the
endpoint the public half.

## Change the endpoint URL

The URL is `hook_endpoint_url` in `config/tenants.toml`. The stack reads that
file again every two seconds:

```sh
$EDITOR config/tenants.toml
```

Enroute signs over that exact authority and path, and the endpoint verifies
against the URL it was configured with. The two strings must match byte for
byte.

## Reset it

```sh
docker compose down -v
```

`down -v` drops the volumes, so the tables and the objects come back fresh.
Enroute creates the tables again on the next start. Without `-v` they are
kept. There is no tenant to put back: the tenants are a file beside the
compose file, not state in a volume.

## Logs

```sh
docker compose logs -f enroute   # the service, every call it makes, and the
                                 # tables it brought up at startup
```

`RUST_LOG=debug docker compose up -d` raises the level on Enroute.

[`dev/enroute.example.toml`]: https://github.com/enroute-sh/enroute/blob/HEAD/dev/enroute.example.toml
[`dev/tenants.example.toml`]: https://github.com/enroute-sh/enroute/blob/HEAD/dev/tenants.example.toml
