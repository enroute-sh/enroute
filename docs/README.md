# Enroute documentation

Enroute is a Git server for applications that need to own their repository
model. It handles Git over HTTP, Git data, and a gRPC API. Your application
owns repository names, users, permissions, and policy.

## Choose a starting point

| If you need to… | Read… |
| --- | --- |
| Run Enroute locally | [Quickstart](quickstart.md) |
| Build a code-hosting application | [Build a code hosting platform](build/README.md) |
| Understand responsibilities and boundaries | [Concepts](#concepts) |
| Assess the current limitations | [Limitations](reference/limitations.md) |

## Guides

[Build a code hosting platform](build/README.md) is a seven-part TypeScript
and Next.js tutorial. It creates an application that can create repositories,
serve Git traffic, and display files, history, and diffs. The same API and hook
contract work with any stack that supports gRPC and HTTP.

After the tutorial, use [Patterns](patterns/README.md) for common product
features such as branch protection, merge requests, CI, and mirroring.

## Concepts

See [Concepts](concepts/README.md) for the data model, interfaces, hooks, and
storage design.

## Operating Enroute

See [Operate Enroute](operate/README.md) for deployment, configuration,
security, maintenance, and observability.

## Reference

See [Reference](reference/README.md) for API, hook, protocol, configuration,
and limitation details.

## Internals

See [Internals](internals/README.md) for contributor development, testing, and
architecture notes.
