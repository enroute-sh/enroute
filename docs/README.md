# Enroute documentation

Enroute is a Git server for applications that host code. It serves Git over
HTTP, stores repositories in an object store, and gives your application a
gRPC API to read and change them. Your application keeps what Enroute does
not: users, repository names, permissions, and policy, which Enroute asks for
through signed hooks.

```text
Git client -- Git smart HTTP --> Enroute <-- gRPC -------- your application
                                  |   |
                                  |   +-- signed HTTP hooks --> your application
                                  |
                   ledger nodes (replicated logs) + object store (layers)
```

Start with the [Quickstart](quickstart.md): Enroute on your machine, a push,
and a commit through the API, in about five minutes.

## Build on Enroute

You write an application that hosts repositories: a code-hosting product, a CI
system, or a platform for coding agents.

1. [Concepts](concepts/README.md): what your application holds, what Enroute
   holds, and how a push moves through both.
2. [Build a code-hosting application](build/README.md): a seven-chapter
   TypeScript tutorial, from an empty project to files, history, and diffs on a
   page.
3. [Patterns](patterns/README.md): protected branches, merge requests, merge
   queues, CI, mirroring, and more, each built on the tutorial's application.
4. [API contract](reference/the-contract.md) and
   [Hook reference](reference/hooks.md): every RPC and every hook, field by
   field.

## Run Enroute

You deploy and maintain Enroute for an application.

1. [Deployment profiles](operate/deployment-profiles.md): one node, one ledger
   with compute nodes, or a replicated ledger, and what each survives.
2. [Operate Enroute](operate/README.md): configuration, storage, security,
   observability, and the maintenance procedures.
3. [Upgrades and migrations](operate/upgrades.md): moving to a new release.
4. [Limitations](reference/limitations.md): what does not work yet.

## Understand how it works

You want to know how Enroute stores, replicates, and serves repositories, and
how it is tested.

- [Architecture](internals/README.md): the storage engine, consensus, the
  write and read paths, and maintenance, with
  [Jepsen tests](internals/jepsen.md) for consistency under faults.
