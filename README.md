<p align="center">
  <a href="https://enroute.sh"><img src="https://github.com/enroute-sh/enroute/blob/assets/banner.png?raw=true" alt="Enroute" width="100%" /></a>
</p>

<p align="center">
  <strong>Git infrastructure for the most demanding workloads</strong>
</p>

<p align="center">
  <a href="https://enroute.sh/docs">Docs</a> &nbsp;|&nbsp;
  <a href="https://enroute.sh/docs/quickstart">Quickstart</a> &nbsp;|&nbsp;
  <a href="https://enroute.sh/docs/reference">API reference</a> &nbsp;|&nbsp;
  <a href="https://enroute.sh/blog">Blog</a>
</p>

<p align="center">
  <a href="https://github.com/enroute-sh/enroute/releases/latest"><img src="https://img.shields.io/github/v/release/enroute-sh/enroute?style=flat-square&color=E11311&label=release" alt="Latest release" /></a>
  <a href="https://github.com/enroute-sh/enroute/blob/HEAD/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-E11311?style=flat-square" alt="Apache 2.0 License" /></a>
  <a href="https://discord.gg/Dhzzu3vpUh"><img src="https://img.shields.io/badge/Discord-Join-5865F2?style=flat-square&logo=discord&logoColor=white" alt="Join us on Discord" /></a>
  <a href="https://x.com/enroute_sh"><img src="https://img.shields.io/badge/X-Follow-000000?style=flat-square&logo=x&logoColor=white" alt="Follow us on X" /></a>
  <a href="https://www.linkedin.com/company/enroute-sh/"><img src="https://custom-icon-badges.demolab.com/badge/LinkedIn-Follow-0A66C2?style=flat-square&logo=linkedin-white&logoColor=fff" alt="Follow us on LinkedIn" /></a>
</p>

---

**Enroute** is Git infrastructure for the most demanding workloads out there:
thousands of pushes per second at millisecond latency, programmable, open
source, with APIs built for machines.

We are building the Git backend for the next decade of software development:

1. **Scalable.** Over 7,600 pushes per second to a single repository, and
   single-digit ms latency for most operations. Clusters can hold 300,000+
   repositories, compute scales horizontally with load and can even be
   co-located with sandboxes or CI runners.
2. **Programmable.** Enroute not only terminates Git client connections, but
   provides a rich gRPC API to interact programmatically with repositories.
   Hooks let your code decide who can read, what they see, and which pushes
   land.
3. **Infinite storage.** Repositories live in object storage such as S3 (or
   compatible APIs), Google Cloud Storage or Azure Blob Storage: dirt cheap,
   extremely durable, and virtually unlimited.

## How does it work?

Most Git servers keep each repository as a directory of files on the disk of one
machine. To scale, they shard repositories across machines, replicate them by
copying files, and run blocking gc and repack tasks to keep them healthy.

We've rethought the Git server with the design principles of modern databases,
and rebuilt it from the ground up in Rust. At its core is a [Git storage
engine](docs/internals/overview.md) built on these principles:

- **Separate compute and storage.** Compute nodes do the heavy lifting: they
  receive pushes and serve clones and fetches. They keep only caches that
  Enroute can rebuild, so you can add or replace them at any time. Ledger nodes
  keep track of and persist metadata.
- **Object storage as the storage tier.** Each repository is a set of immutable
  layers in an object store such as S3, Google Cloud Storage or Azure Blob
  Storage. The object store provides cheap, durable storage that grows with your
  data.
- **A replicated log for each repository.** Ledger nodes keep the refs and the
  layer list of each repository in its own replicated log, which can be deployed
  in an HA configuration across failure domains. The ledger leader ensures load
  remains evenly balanced and repositories stay available even when nodes go
  down.
- **Readers work on snapshots.** Lightweight MVCC snapshots allow each request
  to work off an immutable version of the repository from start to end. This
  way, readers never block writers, and vice-versa.
- **Optimistic concurrency control.** The expensive work of a push happens
  without locks: indexing and validating the pack files happens concurrently for
  many pushes at the same time. Metadata in the ledger is updated via
  compare-and-swap. Pushes to different branches do not conflict.
- **Log-structured layers.** New pushes add layers, and Enroute merges them and
  removes unused ones in the background, as an LSM tree does. There is no `git
  repack` to schedule. Objects are grouped by the commit that changed them, so a
  fetch reads a few ranges of a layer instead of walking the tree.

There is no external database. The only requirements are an object store (on a
single machine, even a local directory works) and a disk that persists for each
ledger node.

This design is also what makes Enroute fast. Small, frequent pushes are merged
before being uploaded to object storage, and clones are served from local copies
of the layers. A repository that nobody writes to costs almost nothing.

On top of that, Enroute is headless: it takes care of the complicated Git work
and leaves the rest to you. The API and hooks provide the right primitives to
integrate Git hosting seamlessly into whatever you have in mind.

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
  ghcr.io/enroute-sh/enroute:0.4.1 dev --dir /data --host 0.0.0.0
```

To build an application on Enroute with a coding agent, add our agent skills:

```
npx skills add enroute-sh/enroute
```

This will give your coding agent everything it needs to spin up a local
environment and help you build your dream platform.

## How does it grow with me?

We know that different setups have different requirements. Enroute does not
force you to overcomplicate yours if you don't want to. It can run in three
different configurations:

1. **One node** on a single machine, with the storage in a local directory or an
   object store.
2. **Separate compute nodes** behind a load balancer when you need more capacity
   for Git and API traffic.
3. **Three or more ledger nodes** when you need high availability across
   multiple machines, availability zones or regions.

Check out [Deployment profiles](docs/operate/deployment-profiles.md) and
[Upgrades and migrations](docs/operate/upgrades.md) for more information.

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

On top of the hooks, Enroute exposes a gRPC API to create commits on the server,
read files, history and diffs, and push to or fetch from other Git hosts. See
the [API reference](docs/reference/proto.md).

## Where can I learn more?

[The docs](docs/) start here. The short version:

- [Quickstart](docs/quickstart.md) — a repository served in a few minutes.
- [Build a code-hosting application](docs/build/README.md) — build one on
  Enroute, chapter by chapter, from an empty project to code and diffs on a
  page.
- [Hook reference](docs/reference/hooks.md) — every field of every hook, with
  the payload of each call.
- [Concepts](docs/concepts/README.md) — the model, the clients and interfaces,
  the hooks and the storage, in four short pages.
- [Operate a deployment](docs/operate/README.md) — configuration, storage,
  security, maintenance, telemetry and upgrades.
- [Limitations](docs/reference/limitations.md) — what does not work yet, stated
  plainly.
- [Architecture](docs/internals/README.md) — how Enroute stores, replicates and
  serves repositories, and how it is tested.

## What's the status?

Enroute is **early**. We use it ourselves, but read
[limitations](docs/reference/limitations.md) and
[security](docs/operate/security.md) before you serve anything you care about.

**Until 1.0, any `0.x` release can change the storage format, the configuration
and the API.** When the storage format changes, you move your repositories to a
new deployment rather than push them again. See [Upgrades and
migrations](docs/operate/upgrades.md).

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

Enroute is licensed under [Apache-2.0](LICENSE). Hosted Enroute is in early
access at [enroute.sh](https://enroute.sh).
