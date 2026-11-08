<img src="https://github.com/enroute-sh/enroute/blob/assets/banner.png?raw=true"></img>

<p align="center">
  <a href="https://github.com/enroute-sh/enroute/blob/HEAD/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache_2.0-E11311.svg" alt="Apache 2.0 License">
  </a>
</p>

**Enroute** is a _fast_, _programmable_ Git server that is _easy to operate_.

Databases have changed a lot in the last decade. Git servers haven't. With
Enroute we're asking the question: What would a Git server look like if we
built it like a modern database? Our answer:

1. **Fast.** Over 7,500 pushes per second to a single repository, with every
   push on the disks of a majority of its ledger replicas before Enroute acknowledges
   it.
2. **Programmable.** Enroute terminates Git client connections and provides a
   gRPC API. Hooks let your code decide who can read, what they see, and which
   pushes land. Each hook is opt-in. No pre-baked opinions, just code.
3. **Easy to operate.** Repositories live in object storage such as S3:
   cheap, durable, and no repository disks to fill or replicate. One binary,
   no external database and no maintenance jobs. Start with one node and grow
   to a replicated cluster without pushing a single repository again.

## How does it work?

Most Git servers keep each repository as a directory of files on the disk of
one machine, as Git itself does. To scale, they shard repositories across
machines, replicate them by copying files, and run `git gc` to keep them
healthy.

We've rethought the Git server with the design principles of modern databases,
and rebuilt it from the ground up in Rust. At its core is a
[Git storage engine](docs/internals/overview.md) built on these principles:

- **Separate compute and storage.** **Compute** nodes do the Git work: they
  receive pushes and serve clones and fetches. They keep only caches that
  Enroute can rebuild, so you can add or replace them at any time. **Ledger**
  nodes hold the state that matters. One process can run both roles.
- **Object storage as the storage tier.** Each repository is a set of
  immutable layers in an object store such as S3, Google Cloud Storage or
  Azure Blob Storage. The object store gives cheap, durable storage that grows
  with your data.
- **A replicated log for each repository.** Ledger nodes keep the refs and
  the layer list of each repository in its own replicated log, on the durable
  local disks of three ledger nodes. A small push commits once its log entry is on the ledger disks,
  without waiting for the object store.
- **Readers work on snapshots.** A clone or fetch reads one immutable version
  of the repository from start to end. Readers never block writers.
- **Optimistic writes.** A compute node does the expensive work of a push
  without locks: it indexes and validates the pack and builds a new layer.
  Many pushes, also to the same repository, do this at the same time. Only
  the last step is serialized, through the log of the repository: one entry
  that adds the layer and updates the refs with a compare-and-swap. Pushes to
  different branches do not conflict.
- **Log-structured layers.** New pushes add layers, and Enroute merges them
  and removes unused ones in the background, as an LSM tree does. There is no
  `git repack` to schedule.

There is no external database. The only requirements are an object store (on
a single machine, a local directory works) and a disk that persists for each
ledger node.

This design is also what makes Enroute fast. Enroute merges small pushes
before it uploads them, and serves clones from local copies of the layers. A
repository that nobody writes to costs almost nothing.

While currently focused purely on Git storage, the vision is that this will
eventually form the basis for a broader suite of primitives to support you in
automating your software development lifecycle end-to-end, including CI/CD,
code reviews and much more.

## I'm intrigued, how can I try it out?

Install `enroute` on Linux or on macOS with Apple silicon, start a local node,
and push to it:

```sh
curl -fsSL https://enroute.sh/install | sh
enroute dev
```

In another terminal:

```sh
git push http://127.0.0.1:8080/hello.git main
enroute repo list
```

The push creates the repository. Every hook is opt-in, so this node lets
everyone read and push. See the [Quickstart](docs/quickstart.md).

To run it in a container instead, use the published image:

```sh
docker run --rm -p 8080:8080 -p 50051:50051 -v enroute-dev:/data \
  ghcr.io/enroute-sh/enroute:0.4.0 dev --dir /data --host 0.0.0.0
```

To build an application on Enroute with a coding agent, add our agent skills:

```
npx skills add enroute-sh/enroute
```

This will give your coding agent everything it needs to spin up a local
environment and help you build your dream platform.

## How does it grow with me?

Every deployment uses the same binary and the same bucket. Only the `[node]`
section of the configuration changes:

1. **One node** on a single machine, with the bucket in a local directory or
   an object store.
2. **Separate compute nodes** behind a load balancer when you need more
   capacity for Git and API traffic.
3. **Three or more ledger nodes** when writes must continue after the loss of
   a machine or an availability zone. Each repository's log is replicated to
   three of them.

You can move a deployment from one step to the next without data loss. To move
from a local directory to an object store, copy the repositories to a new
deployment. See [Deployment profiles](docs/operate/deployment-profiles.md) and
[Upgrades and migrations](docs/operate/upgrades.md).

## What's currently supported?

Enroute handles the full Git smart HTTP protocol for you, terminating Git client
connections so your application doesn't need to deal with long running requests.

In turn, Enroute supports `hooks`: it calls your application back at specific
points so you can influence the behavior of the Git transactions. The following
hooks are supported:

- Authentication: Control who has access to which repositories, and whether they
  can read or write.
- Visibility: Limit which branches or tags a client can see. You can use this to
  hide internal details, implement finer grained permissions or speed up clients
  by archiving old branches.
- Pre-receive: Decide which ref updates are allowed. Protect branches against
  force pushes or require PRs for changes to main.
- Post-receive: Schedule actions to run after a successful push like running CI,
  deploying code, or mirroring your code to another repository.

On top of the hooks, Enroute exposes a rich gRPC API for you to programmatically
interact with any repository. See the [API reference](docs/reference/proto.md).

## What's on the roadmap?

In the near future, our focus is on increasing the reliability and performance
of Enroute. This includes benchmarks against other Git servers, and moving a
single machine from a local directory to an object store in place.

In addition, we are planning the following features:

- More transports like SSH and `git://`.
- More gRPC capabilities to enable a broader set of use-cases.
- Fast checkout: Specific features aimed towards use in latency-sensitive,
  ephemeral environments like agent sandboxes.
- First-class support for the [`git-meta`](https://git-meta.com/) standard.

## Where can I learn more?

[The docs](docs/) start here. The short version:

- [Quickstart](docs/quickstart.md) — a repository served in a few minutes.
- [Build a code-hosting application](docs/build/README.md) — build one on Enroute,
  chapter by chapter, from an empty project to code and diffs on a page.
- [Hook reference](docs/reference/hooks.md) — every field of every hook, with
  the payload of each call.
- [Concepts](docs/concepts/README.md) — the model, the clients and interfaces, the hooks
  and the storage, in four short pages.
- [Operate a deployment](docs/operate/README.md) — configuration,
  storage, security, maintenance, telemetry and upgrades.
- [Limitations](docs/reference/limitations.md) — what does not work yet, stated
  plainly.
- [Architecture](docs/internals/README.md) — how Enroute stores,
  replicates and serves repositories, and how it is tested.

## What's the status?

Enroute is **early**. We use it ourselves, but read
[limitations](docs/reference/limitations.md) and
[security](docs/operate/security.md) before you serve anything you care about.

**Until 1.0, any `0.x` release can change the storage format, the configuration
and the API.** When the storage format changes, you move your repositories to a
new deployment rather than push them again. See
[Upgrades and migrations](docs/operate/upgrades.md).

## How can I contribute?

Pull requests are disabled. Coding agents make it too easy to send a large,
low-context change that costs maintainers more time than it saves. Thoughtful
contributions are welcome; please understand the code, keep the patch focused,
and respect the review time you are asking for.

Send a `git format-patch` attachment to
[patches@enroute.sh](mailto:patches@enroute.sh).

Contributor License Agreement: By emailing a patch, you certify that you have
the right to submit it and assign to OpenCanopy GmbH all rights in the patch
that you can assign. Where a right cannot be assigned, you grant OpenCanopy GmbH
a perpetual, irrevocable, worldwide, royalty-free, transferable, sublicensable
license to use, modify, combine, relicense, redistribute, or publish the patch,
in whole or in part, with or without attribution.

## What's the license?

Enroute is licensed under [Apache-2.0](LICENSE). The [NOTICE](NOTICE) file has
the attribution notice.
