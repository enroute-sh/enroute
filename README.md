<img src="https://github.com/enroute-sh/enroute/blob/assets/banner.png?raw=true"></img>

<p align="center">
  <a href="https://github.com/enroute-sh/enroute/blob/HEAD/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache_2.0-E11311.svg" alt="Apache 2.0 License">
  </a>
</p>

**Enroute** is a git platform rebuilt from the ground up to be _hackable_.

If you're also fed up with the limitations of existing git hosting setups, tired
of clicking through endless settings pages, walking through YAML hell just to
configure a few  simple automations, abusing tags and comments on issues and PRs
for semantic meaning, or building OAuth apps and handling webhooks, then Enroute
might be for you.

With Enroute we're asking the question: What if git hosting was an API instead
of a SaaS? So far we have three answers:

1. Headless: Enroute terminates git client connections and provides a gRPC API.
   Everything else is on you. No restrictions or pre-baked opinions, just code.
2. Composable: Hooks provide a way to customize authentication, ref visibility,
   pre-receive admission control and post-receive handling.
3. Simple to operate: Stateless compute and object storage instead of fickle
   SSDs. No backups to schedule, replicas to synchronize or shards to route.

## I'm intrigued, how can I try it out?

The simplest way to try out Enroute is via our agent skills:

```
npx skills add enroute-sh/enroute
```

This will give your coding agent everything it needs to spin up a local
environment and help you build your dream platform.

## How does it work?

We've rebuilt the git backend from the ground up in Rust on top of object
storage. The key win of this is that the git backend becomes fully stateless,
so it is quite scalable while being easy to operate.

To achieve the best performance, we package this into a client-server model with
a gRPC interface, with the only hard dependencies being a PostgreSQL database
for metadata storage, and access to an object store like S3, Google Cloud Storage
or Azure Blob Storage.

While currently focussed purely on git storage, the vision is that this will
eventually form the basis for a broader suite of primitives to support you in
automating your software development lifecycle end-to-end, including CI/CD,
code reviews and much more.

## What's currently supported?

Enroute handles the full git smart http protocol for you, terminating git client
connections so your application doesn't need to deal with long running requests.

In turn, Enroute supports `hooks`: It calls your application back at specific
points so you can influence the behaviour of the git transactions. The following
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
interact with any repository. Check out the `proto` directory for details.

## What's on the roadmap?

In the near future, our focus is on increasing the reliability and performance
of Enroute. For this, we will be publishing benchmarks soon.

In addition, we are planning the following features:

- More transports like SSH and `git://`.
- More gRPC capabilities to enable a broader set of use-cases.
- Fast checkout: Specific features aimed towards use in latency-sensitive,
  ephemeral environments like agent sandboxes.
- First-class support for the [`git-meta`](https://git-meta.com/) standard.

## Where can I learn more?

[The docs](docs/) start here. The short version:

- [Quickstart](docs/quickstart.md) — a repository served in about ten minutes.
- [Build a code hosting platform](docs/build/README.md) — build one on Enroute,
  chapter by chapter, from an empty project to code and diffs on a page.
- [Hook reference](docs/reference/hooks.md) — every field of every hook, with
  the payload of each call.
- [Concepts](docs/README.md#concepts) — the model, the two callers, the hooks
  and the storage, in four short pages.
- [Operate a deployment](docs/README.md#run-enroute) — configuration,
  storage, security, maintenance and telemetry.
- [Limitations](docs/reference/limitations.md) — what does not work yet, stated
  plainly.
- [Internals](docs/README.md#internals) — how it is built, tested, and
  structured internally.

## What's the status?

Enroute is **early**. We use it ourselves, but read
[limitations](docs/reference/limitations.md) and
[security](docs/operate/security.md) before you serve anything you care about.

**There is no backwards compatibility between `0.x` releases.** Especially the
storage format can change in any release, requiring a clean slate and re-push.

Releases are `0.x` and published as `ghcr.io/enroute-sh/enroute:latest`. A
version tag names one image and is not rebuilt, so `:0.1.0` is enough to pin.
What a release changed is the message of its commit on the channel branch, so
`git log main` is the changelog.

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

Enroute is licensed under [Apache-2.0](LICENSE). See [NOTICE](NOTICE) for the
third-party code that's included.
