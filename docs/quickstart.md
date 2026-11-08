# Quickstart

In about five minutes, you run Enroute on your machine, push a repository to
it, read the repository back over Git and over the API, and turn on the first
hook. You need Git, and Linux or macOS on Apple silicon.

## Install

Run the installer:

```sh
curl -fsSL https://enroute.sh/install | sh
```

It downloads the latest release for your platform, checks it against
`SHA256SUMS`, and puts `enroute` in `~/.local/bin`. To install a specific
release, set `ENROUTE_VERSION`:

```sh
curl -fsSL https://enroute.sh/install | ENROUTE_VERSION=0.4.0 sh
```

To install from an archive, build from source, or use the container image, see
[Other ways to install](#other-ways-to-install).

## Start Enroute

In an empty directory, run:

```sh
enroute dev
```

```text
enroute dev is running. Press Ctrl-C to stop.

  git      http://127.0.0.1:8080
  api      http://127.0.0.1:50051
  dir      /home/you/project/.enroute/dev
  hooks    off (every request may read and push; pass --hooks URL to opt in)

Try:
  git push http://127.0.0.1:8080/hello.git main
  enroute repo list
```

`enroute dev` runs one node with a generated configuration. It keeps its data
in `.enroute/dev`, and writes a `.gitignore` in `.enroute` so that Git ignores
it. Every listener binds to `127.0.0.1`. No hook is enabled, so every request
may read and push.

Leave it running, and open a second terminal for the next steps.

## Push a repository

Make a repository with one commit, and push it:

```sh
git init -b main hello && cd hello
echo "# hello" > README.md
git add README.md && git commit -m "First commit"
git push http://127.0.0.1:8080/hello.git main
```

```text
To http://127.0.0.1:8080/hello.git
 * [new branch]      main -> main
```

The push created the repository. Nothing had to exist first: with no
`authorize` hook, the URL path without `.git` is the repository key, here
`hello`.

List the repositories:

```sh
enroute repo list
```

```text
hello	refs/heads/main	pushed 2026-11-06T14:19:32Z
```

Each line is the key, the default branch, and the time of the last push.

## Read it back

Clone the repository into a second directory:

```sh
cd .. && git clone http://127.0.0.1:8080/hello.git hello-copy
```

```text
Cloning into 'hello-copy'...
```

Now make a commit without a checkout. `enroute commit` calls the
`CreateCommit` RPC, as your application would:

```sh
enroute commit hello --branch main -m "Add notes" \
  --author "You <you@example.com>" --put notes.md="hello"
```

```text
commit cd11fc866fc0f7b3d614fe666ae944e8fa650016
tree   396194106f8aa1ebd739119f51e48c40f90f5d79
parent 30fbadfa87dba7ee2d3d34385315a675d98ed191
```

Your IDs differ, because they include the time of the commit. Pull the commit
into the clone:

```sh
cd hello-copy && git pull
```

```text
Fast-forward
 notes.md | 1 +
 1 file changed, 1 insertion(+)
 create mode 100644 notes.md
```

The branch now points to the new commit, whichever way you read it:

```sh
enroute refs hello
```

```text
cd11fc866fc0f7b3d614fe666ae944e8fa650016	refs/heads/main
```

`enroute repo`, `enroute refs`, and `enroute commit` call the gRPC API on port
`50051`. You can call it directly with any gRPC client, such as
[`grpcurl`](https://github.com/fullstorydev/grpcurl). The server answers
reflection, so you need no proto files to explore it. See the
[API contract](reference/the-contract.md).

## Turn on a hook

So far, anyone who can reach port `8080` can read and push. Hooks let your
application decide. Stop `enroute dev` with Ctrl-C, and start it again with a
hook URL and the `authorize` hook:

```sh
enroute dev --hooks http://localhost:3000/api/enroute/hooks --enable authorize
```

```text
  hooks    authorize -> http://localhost:3000/api/enroute/hooks
           signing key id UDb6dVdOEUZzCnZ0-HqsSkgGpEM8vdvEfdGvIg6rI70
           private key /home/you/project/.enroute/dev/hooks-signing-key.pem
           public key /home/you/project/.enroute/dev/hooks-signing-key.pub.pem:
```

On the first run, Enroute generates a signing key and keeps it in
`.enroute/dev`. Your application verifies each hook with the public key.
Leave out `--enable` to turn on all four hooks.

Nothing answers at that URL yet, so every Git request now fails:

```sh
git ls-remote http://127.0.0.1:8080/hello.git
```

```text
remote: authorization failed
fatal: unable to access 'http://127.0.0.1:8080/hello.git/': The requested URL returned error: 500
```

That is the intended behaviour. An enabled hook fails closed: if your
application does not answer, Enroute serves nothing.

## What just happened

- **Enroute served Git, and nothing else.** It took a push, served a clone,
  and made a commit, but it does not know who you are. Users, repository
  names, and permissions belong to your application, which Enroute asks
  through hooks.
- **Two interfaces reached one repository.** Git clients use port `8080`. Your
  application uses the gRPC API on port `50051`, which reads files, history,
  and diffs, and writes commits, without a Git client. See
  [Clients and interfaces](concepts/two-callers.md).
- **Each write was durable when it returned.** The push and the commit were
  each one entry in the repository's log, synced to disk before Enroute
  answered. On one node, the log and the objects are both in `.enroute/dev`.
  In production, the objects go to an object store and the log is replicated.
  See [Storage](concepts/storage.md).

## Next steps

- [Build a code-hosting application](build/README.md): answer the hooks, and
  show repositories, files, history, and diffs in a web application.
- [Concepts](concepts/README.md): what your application holds, what Enroute
  holds, and how a push moves through both.
- [Deployment profiles](operate/deployment-profiles.md): run Enroute on more
  than one machine.

## Other ways to install

### From an archive

Download the archive for your platform from the
[releases page](https://github.com/enroute-sh/enroute/releases), and put
`enroute` on your `PATH`:

| Platform | Archive |
| --- | --- |
| Linux, x86-64 | `enroute-<version>-x86_64-unknown-linux-gnu.tar.gz` |
| Linux, ARM64 | `enroute-<version>-aarch64-unknown-linux-gnu.tar.gz` |
| macOS, Apple silicon | `enroute-<version>-aarch64-apple-darwin.tar.gz` |

```sh
tar -xzf enroute-<version>-<target>.tar.gz
sudo install enroute-<version>-<target>/enroute /usr/local/bin/
```

`SHA256SUMS` on the same page has the checksum of each archive. The Linux
binaries need glibc 2.28 or later. The macOS binary is not signed. If you
download it with a browser, macOS blocks it. To let it run, enter
`xattr -d com.apple.quarantine /usr/local/bin/enroute`.

### From source

For another platform, build `enroute` from a checkout of the repository. You
need Rust:

```sh
cargo install --path .
```

### Run the image

You can run `enroute dev` from the published image. Replace `<version>` with
a release, for example `0.4.0`:

```sh
docker run --rm -p 8080:8080 -p 50051:50051 -v enroute-dev:/data \
  ghcr.io/enroute-sh/enroute:<version> dev --dir /data --host 0.0.0.0
```

`--host 0.0.0.0` binds Git and the API to all interfaces in the container,
because the host cannot reach the loopback address of a container. With no
`authorize` hook, Enroute logs a warning that anyone who can reach Git can
read and push. Publish the ports only to a machine that you trust. The
`enroute-dev` volume keeps the data. To delete it, run
`docker volume rm enroute-dev`.

To send hooks to an application on the host, name `host.docker.internal` in
the hook URL:

```sh
docker run --rm -p 8080:8080 -p 50051:50051 -v enroute-dev:/data \
  --add-host=host.docker.internal:host-gateway \
  ghcr.io/enroute-sh/enroute:<version> dev --dir /data --host 0.0.0.0 \
  --hooks http://host.docker.internal:3000/api/enroute/hooks --enable authorize
```

`--add-host` is necessary on Linux. Docker Desktop resolves
`host.docker.internal` without it. The public signing key is in the volume, at
`/data/hooks-signing-key.pub.pem`, and the banner prints it. If the container
runs in the background, read the banner with `docker logs <container>`.
