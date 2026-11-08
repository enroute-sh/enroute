# The local stack

Two ways run Enroute locally. Both run `enroute dev`, and both serve Git on
`8080` and the contract on `50051`.

| Way                 | Use it when                                  |
| ------------------- | -------------------------------------------- |
| `enroute dev`       | The `enroute` command is installed.          |
| The published image | Docker runs, and `enroute` is not installed. |

Check with `enroute --version`. If it is not installed, use the image. Ask the
user before you install it with `curl -fsSL https://enroute.sh/install | sh`.

## Hooks are opt-in

Enroute calls only the hooks that are enabled. A hook that is not enabled
takes its default:

| Hook           | Default when not enabled                                                |
| -------------- | ----------------------------------------------------------------------- |
| `authorize`    | Allow read and write. The key is the URL path without `.git`.           |
| `visible_refs` | Every ref is visible.                                                   |
| `pre_receive`  | Every command is allowed.                                               |
| `post_receive` | No call.                                                                |

For this skill, enable all four. `--hooks` with no `--enable` does that.

## `enroute dev`

Run it in the user's project directory, with the endpoint URL from phase 1:

```sh
enroute dev --hooks http://localhost:3000/api/enroute/hooks
```

- `--hooks` enables all four hooks. `--enable authorize,pre_receive` enables
  only those.
- `--pre-receive-mode objects` makes the pushed objects readable through the
  contract during `pre_receive`. Add it only if the endpoint reads them there.
- All listeners bind to `127.0.0.1`. The endpoint runs on the same host, so
  the URL names `localhost`, not `host.docker.internal`. `--host` binds Git
  and the contract to a different address.
- The node keeps its data in `./.enroute/dev`, and writes a `.gitignore` in
  `.enroute/` so that Git ignores it.
- On the first run, it generates an Ed25519 signing key and keeps it in that
  directory. The endpoint verifies with the public half,
  `.enroute/dev/hooks-signing-key.pub.pem`. The banner prints that key, its
  `keyid`, and both paths.
- It reads `ENROUTE_*` values from `.env` and `.env.local` in the current
  directory: `ENROUTE_HOOKS_URL` and `ENROUTE_HOOKS_ENABLE`, for example. A
  flag wins over the environment, and the environment wins over the files.
- Ctrl-C stops it. To change the URL, stop it and start it again with the new
  `--hooks`.
- `enroute dev --clean` deletes the directory first: the repositories, the
  objects, and the signing key. The next start makes a new key.
- The logs go to the terminal. `RUST_LOG=debug enroute dev …` raises the level.

**Checkpoint.** The banner shows `hooks` with the four names, the URL, and a
`signing key id`.

The same binary answers the contract from the shell:

```sh
enroute repo create repo-4f2a1c
enroute repo list
enroute refs repo-4f2a1c
```

## The published image

The image runs the same `enroute dev`, in a container. Nothing compiles.

### Tags

Every release is pushed to the GitHub Container Registry as
`ghcr.io/enroute-sh/enroute`.

| Tag            | What it names                                  |
| -------------- | ---------------------------------------------- |
| `latest`       | The current release. Moves with each one.      |
| `<version>`    | One release, for example `0.4.0`. Not rebuilt. |
| `sha-<commit>` | One commit. The tag that is never reused.      |

The image is a manifest list for `linux/amd64` and `linux/arm64`, so it runs
on an Intel or an Apple Silicon machine without emulation.

The image also carries the contract it serves, at `/usr/share/enroute/proto`.
That copy is for a project that generates a client against a different
Enroute. This skill ships its own copy, and `contract.md` says which to use.

**The image and this skill are in step.** The command below names the exact
release this skill shipped beside, so there is nothing to look up and nothing
to pin. A skill installed a year ago still starts the Enroute its recipes and
its proto were written against.

Do not change the tag. The recipes, the proto, and the server are one release,
and the contract is not stable between `0.x` releases, so a newer server is a
mismatch and not an upgrade. **Install the skill again to move both:**

```sh
npx skills add enroute-sh/enroute
```

#### While the package is private

```sh
docker login ghcr.io -u <github-username>
```

The password is a GitHub personal access token with `read:packages`. It must
be a **classic** token. The registry refuses a fine-grained one, and its
refusal does not say so. An anonymous pull answers `401` until the package is
public.

Access to the image follows access to the repository: whoever can read the
source can pull the image.

### Start it

Use the endpoint URL from phase 1. Its host is `host.docker.internal`, which
is the host machine as a container sees it:

```sh
docker run --rm -d --name enroute \
  -p 8080:8080 -p 50051:50051 -v enroute-dev:/data \
  --add-host=host.docker.internal:host-gateway \
  ghcr.io/enroute-sh/enroute:0.4.0 dev --dir /data --host 0.0.0.0 \
  --hooks http://host.docker.internal:3000/api/enroute/hooks
```

- `--host 0.0.0.0` binds Git and the contract to every interface in the
  container, because the host cannot reach the loopback address of a
  container. The internal listener stays on loopback.
- `-v enroute-dev:/data` keeps the repositories, the objects, and the signing
  key in a named volume. They outlive the container.
- `--add-host` makes `host.docker.internal` resolve on Linux. Docker Desktop
  resolves it without the flag, and the flag does no harm there.
- `--hooks` enables all four hooks. Add `--enable authorize,pre_receive` to
  enable only those.
- `-d` runs it in the background. `--rm` deletes the container when it stops,
  but not the volume.

A release can change the storage format, and then it cannot read a volume that
an earlier release wrote. A local volume holds only test data, so remove it
when you move to a new release: `docker volume rm enroute-dev`.

The endpoint does not have to answer yet. Enroute calls it when a Git request
arrives, and not before. Git does not serve until that endpoint answers. Until
then the contract is all that works, and that is the checkpoint for this
phase.

**Checkpoint.** `docker logs enroute` shows the banner: `hooks` with the four
names, the URL, and a `signing key id`.

### The signing key

On the first start, `enroute dev` generates an Ed25519 signing key and keeps
it in the volume: `/data/hooks-signing-key.pem`, and the public half in
`/data/hooks-signing-key.pub.pem`. The key stays the same until the volume is
removed.

The endpoint runs on the host and cannot read the volume. Get the public key
in one of these ways:

- Read the banner, which prints the public key and its `keyid`:
  `docker logs enroute`.
- Copy the file into the user's project:

  ```sh
  docker cp enroute:/data/hooks-signing-key.pub.pem .
  ```

Generate a key of your own for anything that is not a local stack:

```sh
openssl genpkey -algorithm ed25519 -out hook-signing-key.pem
```

Name it in the configuration as `signing_key` under `[hooks]`, usually as
`"${ENROUTE_HOOK_SIGNING_KEY}"` so the file holds no key, and give the
endpoint the public half. [`dev/enroute.example.toml`] in the repository gives
the full format with comments.

### Change the endpoint URL

The URL is a flag, read once at startup. Stop the container, then run the same
`docker run` command with the new `--hooks` URL:

```sh
docker stop enroute
```

The volume keeps the repositories and the signing key.

Enroute signs over that exact authority and path, and the endpoint verifies
against the URL it was configured with. The two strings must match byte for
byte.

### Reset it

```sh
docker stop enroute
docker volume rm enroute-dev
```

Removing the volume deletes the repositories, the objects, and the signing
key. The next start makes a new key.

### Logs

```sh
docker logs -f enroute   # the node; RUST_LOG=debug adds every call
```

Add `-e RUST_LOG=debug` to `docker run` to raise the level.

## Fixed values

None of these is a secret. They are fixed so a caller does not have to read
logs to find them.

| Thing            | Value                                         |
| ---------------- | --------------------------------------------- |
| Git base URL     | `http://127.0.0.1:8080`                       |
| Repository URL   | `http://127.0.0.1:8080/<path>.git`            |
| Contract address | `127.0.0.1:50051`, plaintext HTTP/2, no TLS   |
| Signing key      | Generated by `enroute dev` on the first start |

The contract authenticates nobody and needs no header. Nothing stands in front
of the listener, so anything that can reach 50051 can read, write and delete
every repository. That is fine on a laptop and is not how to deploy.

The user's endpoint decides what a Git client presents as its credential. It
is whatever `authorize` accepts, so pick something in phase 4 and use it.

[`dev/enroute.example.toml`]: https://github.com/enroute-sh/enroute/blob/HEAD/dev/enroute.example.toml
