# Data model

Enroute stores Git repositories and nothing about them that Git does not
store. Your application holds everything else: what a repository is called,
who owns it, who may read it, and what may land on it. This page shows where
that line falls, using the application from the
[Build guide](../build/README.md) as the example.

## What each side holds

A code-hosting application has users, repositories with names and owners, and
rules about who can do what. Enroute has repositories, refs, and objects.

| Fact | Held by | Where |
| --- | --- | --- |
| A user, their name and their token | Your application | Your `users` table |
| A repository's name, such as `acme/widgets` | Your application | `repos.path` |
| A repository's owner and members | Your application | `repos.owner_id`, and your own tables |
| Who may read or push | Your application | Decided in the [`authorize` hook](hooks.md) |
| Which pushes may land | Your application | Decided in the [`pre_receive` hook](hooks.md) |
| The repository itself | Enroute | Under the key that your application chose |
| Branches, tags, and other refs | Enroute | In the repository's replicated log |
| Commits, trees, and file contents | Enroute | In layers in the object store |

The two sides meet at one value: the **repository key**. In the guide, the
key is the primary key of the `repos` row:

```text
your database                         Enroute
--------------------------------      --------------------------------
repos                                 repository "repo-4f2a1c"
  id        = "repo-4f2a1c"   ----->    HEAD     -> refs/heads/main
  path      = "acme/widgets"            refs     refs/heads/main -> ade924f…
  owner_id  = "user-8471"                        refs/heads/feature -> 355317f…
                                        objects  commits, trees, blobs
```

Enroute stores no ID of its own, so there is nothing to keep in step. Every
later call names the repository by the key that you already have.

## Repository keys

A key is opaque to Enroute. It reads nothing out of it: a `/` in a key is a
character, not an owner or a group. Keys are unique across the deployment.

Use an identity that does not change, such as a row ID, as the key, and keep
the name in your own table. A user who renames `acme/widgets` then changes one
column, and the repository keeps its key. If the name were the key, a rename
would point every later push at a different repository.

The `authorize` hook connects the two. A Git client asks for a URL path, such
as `acme/widgets.git`. Your application looks up the path and answers with the
key. Without `authorize`, the key is the URL path without `.git`, which suits
a first test and not much more.

The rules for a key are in [Repository keys](../reference/the-contract.md#repository-keys).
To group repositories by team or tenant, see
[Namespacing repositories](../patterns/namespacing.md).

## Refs and objects

A repository holds what any Git repository holds: objects (commits, trees,
blobs, and tags) and refs that name them. Enroute adds no records of its own,
such as pull requests or issues.

Objects arrive in two ways: a Git push, or a
[`CreateCommit`](../reference/the-contract.md#createcommit) call that makes one
commit from file changes. Refs move in three ways: a push,
[`UpdateRefs`](../reference/the-contract.md#updaterefs), and `CreateCommit`.
Only a push runs the hooks. The API calls are your application acting on its
own authority, so it applies its rules before it calls.

Enroute enforces Git's rules and no others. A branch must point to a commit,
and a ref can only point to an object that the repository has. Enroute takes
a force-push, a deleted branch, or a ref in a namespace that you invent,
unless your `pre_receive` hook refuses it. It reports facts, such as whether
an update is a fast-forward, so that your application can decide.

A repository is SHA-1 or SHA-256, chosen when it is created and fixed for
good. A Git client must use the same format.

## What Enroute does not do

- **Authenticate anyone.** A Git client's credentials go to your `authorize`
  hook, and the API listener trusts every caller. See
  [Security](../operate/security.md).
- **Store policy.** Branch protection, review rules, and access lists are your
  application's data, applied in hooks.
- **Create objects on its own.** Every object comes from a push or a
  `CreateCommit` call. Enroute makes no merge commits.
- **Share objects between repositories.** Each repository stores its own
  objects. There are no forks with shared storage.

For the current limits, see [Limitations](../reference/limitations.md).
