# Namespacing repositories

Namespacing groups repositories under an owner, a team, or a project, so that
two groups can each have a `backend` and nobody has to see the other's.

Enroute serves one application and stores repositories under flat keys that are
unique across the deployment. It has no concept of a group, an owner, or a
path. This page is how to build one anyway, and what each way costs.

## Use this pattern when

People want `acme/backend` and `globex/backend` to be two repositories. Almost
every forge works this way, so almost every application built on Enroute needs
an answer.

Read this before you pick one: **a namespace here is naming, not isolation.**
Enroute enforces nothing about it. Your application already decides who may
reach which repository, in `authorize`, and that answer is what keeps one
group out of another's. A namespace makes the answer easier to write. It does
not make a wrong answer safe.

If you need a boundary that holds when your application is wrong, a namespace
is the wrong tool. Run a second Enroute: its own configuration, its own
database, and its own bucket. Nothing in one deployment can name anything in
another, because there is no shared table for a name to resolve against.

## The URL is already yours

Start here, because it is the part most people expect to be hard and is not.

Enroute's Git front door takes a repository path of any depth and reads nothing
from it. The path is relayed to your `authorize` hook, which answers with the
repository key it means. So this works today, with no configuration:

```
git clone https://git.example.com/acme/backend.git
```

Your `authorize` implementation receives `repo_path` as `/acme/backend.git`,
looks it up in your own table, and returns whatever key that repository has.
The key does not have to resemble the path, and should not — see below.

Namespaced clone URLs therefore need nothing from this page. What follows is
only about the *key*.

## Implement the pattern

Keep the namespace in your database and keep the key opaque.

Your application already has a table mapping a URL path to a repository. Add
the namespace to that table as a column, and let the Enroute key stay an ID you
allocate:

```ts
// repos: id (uuid), namespace, name, enroute_key
// UNIQUE (namespace, name)
export async function create(namespace: string, name: string, ownerId: string) {
  const key = randomUUID();
  await createRepository(key);
  await db.insert(repos).values({ namespace, name, ownerId, enrouteKey: key });
  return key;
}
```

`UNIQUE (namespace, name)` is where two groups each get a `backend`. Enroute
never sees either string.

This is the recommendation, and it is the same advice as
[Repository lifecycle](repository-lifecycle.md): do not put a name in a key.
A namespace is a name, and names move. An organisation renames itself, a
project transfers between teams, a personal account becomes an organisation.
Each of those rewrites `acme/backend`, and a key that spelled it would have to
be rewritten too — which Enroute has no operation for, because a key is what a
repository *is*. A UUID survives every one of those events untouched.

## Spelling the namespace into the key

You may want the key readable anyway: in a log line, in a metrics label, in a
storage listing, when someone is trying to work out which repository is filling
a bucket. The cost is the instability above, and it is a real cost.

If you accept it, the key charset already allows it. A key is ASCII letters and
digits, `-`, `_` and `.`, starting and ending with a letter or digit. So:

```
acme.backend
acme-backend
```

Pick one separator and never use it inside either half, or `acme.backend.v2`
becomes ambiguous. A key holds no `/`: a key is one path segment, so it needs
no escaping wherever it is written.

If you do this, treat the key as frozen at creation. When the namespace is
renamed, your table changes and the key does not. The key stops describing the
repository accurately, which is the price of having had it describe the
repository at all.

## Listing one namespace

`ListRepositories` takes a `prefix` and reports only the keys starting with
it, byte for byte:

```ts
const { repositories } = await listRepositories({ prefix: "acme." });
```

Send the separator. `acme` matches `acmex` as readily as `acme.web`, because
Enroute reads nothing out of a key beyond the bytes you asked about — where a
namespace ends is your convention, not something inferred here.

The walk is a half-open range over the key index, from `acme.` up to `acme/`,
so a page costs what the group costs rather than what the deployment does. A
page token belongs to the prefix that minted it; sending one back under a
different prefix is refused rather than resumed somewhere else.

With keys as UUIDs you do not need this: your own table answers "what is in
`acme`" with an index, and answers it with the display names too. Reach for it
when the keys themselves carry the grouping, and for reconciliation — finding
what Enroute holds that your database has forgotten, or the reverse. Run that
on a schedule, not in a request.
