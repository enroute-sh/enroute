# Repository lifecycle

Repository lifecycle management covers repository names, owners, and the work
required when either changes.

[Chapter 2](../build/02-create-repositories.md) created a repository and stopped
there. This adds renaming, deletion, the sweep that finds what the two got out
of step, and the option of making a repository by pushing to it.

## Use this pattern when

People want repositories that have a name, belong to somebody, and can be
renamed or thrown away. Enroute stores a repository under the key you gave it
and holds nothing else: no name, no owner, and no column that could become
one. The map from a name to a key is your table, and it is the only thing that
knows what a repository is called.

Do not use the repository name as the Enroute key. A rename changes the URL
path but must continue to resolve to the same repository key.

The other cost is in the order of two writes. A create is a row in your
database and a repository in Enroute. If the process stops between the two,
one of them is an orphan, and which one decides whether a name breaks forever
or storage sits unused.

## Implement the pattern

Key by an ID you allocate and never change, which is what the guide's `repos`
table already does. Make the repository first and write the row second. A
rename does not touch Enroute.

```ts
import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { db } from "@/lib/db";
import { repos } from "@/lib/db/schema";
import { createRepository, deleteRepository } from "@/lib/enroute";
import { repoByPath } from "@/lib/store";

export async function create(path: string, ownerId: string) {
  const id = `repo-${randomUUID().slice(0, 8)}`;
  await createRepository(id);                     // Enroute first
  await db.insert(repos).values({ id, path, ownerId });
  return id;
}

export async function rename(from: string, to: string) {
  await db.update(repos).set({ path: to }).where(eq(repos.path, from));
}

export async function remove(path: string) {
  const repo = await repoByPath(path);
  if (!repo) return;
  await db.delete(repos).where(eq(repos.id, repo.id));   // Git stops resolving it
  await deleteRepository(repo.id);
}
```

A rename is one row. Nothing tells Enroute, because there is nothing to tell:
no key changes, no object moves, and no clone breaks. Moving a repository to
another owner costs the same one `UPDATE`.

**The key is the row's ID, never its path.** A key is what a repository *is*
to Enroute, so a name a rename moves is the wrong thing to use. `authorize` is
where a path becomes a key: look it up, and answer with the row's ID.

**The create is idempotent on the key.** A second `CreateRepository` with the
same key answers with the same repository and makes nothing new, so a retry
after a crash needs no check first.

`deleteRepository` is the one call the guide has not needed yet. Add it to
`lib/enroute.ts` beside `createRepository`, against `RepositoryService`.

### Order both so the orphan is in Enroute

The two orderings are opposite, for the same reason. A repository nothing
names costs storage. A row that names a repository that does not exist breaks
every request for that name.

- **To create**, make the repository *first* and insert the row second. A stop
  between them leaves a repository nothing names.
- **To delete**, drop the row *first* and call `DeleteRepository` second. The
  drop is what makes the name unreachable, because `authorize` can no longer
  resolve it. A stop between them leaves storage to reclaim on a retry, not a
  repository still serving Git. `DeleteRepository` is idempotent, so the retry
  is free.

`ListRepositories` finds the orphans. It walks every key the tenant holds, a
page at a time, and a key no row names is one to delete:

```ts
export async function sweepOrphans() {
  let pageToken = "";
  do {
    const page = await listRepositories({ limit: 0, pageToken });
    for (const r of page.repositories) {
      if (!(await repoById(r.repo!.key))) {
        await deleteRepository(r.repo!.key);
      }
    }
    pageToken = page.nextPageToken;
  } while (pageToken !== "");
}
```

A short page is not the last page. Stop only when `nextPageToken` comes back
empty. See [The contract](../reference/the-contract.md#repositories).

The same ordering settles a race between two creates of one path. Each caller
allocates its own ID and makes its own repository. Let the unique constraint
on `path` decide, and have the loser delete what it made:

```ts
export async function create(path: string, ownerId: string) {
  const id = `repo-${randomUUID().slice(0, 8)}`;
  await createRepository(id);
  try {
    await db.insert(repos).values({ id, path, ownerId });
    return id;
  } catch (err) {
    if (!isUniqueViolation(err)) throw err;
    await deleteRepository(id);          // or leave it for the sweep
    const winner = await repoByPath(path);
    return winner!.id;
  }
}
```

### Make a repository by pushing to it

The push that first needs a repository can create it, in `authorize`. The path
is not in the table, so make it, and grant what was made.

```ts
const path = req.repoPath.replace(/\.git$/, "");
let repo = await repoByPath(path);

if (!repo) {
  if (req.access !== Access.ACCESS_WRITE) {
    return { denied: { denial: Denial.DENIAL_NOT_FOUND, challenge: undefined } };
  }
  const id = await create(path, user.id);
  repo = { id, path, ownerId: user.id };
}

// ... grant { repo: { key: repo.id }, ... } as in chapter 4
```

**Only on write.** A fetch of a path nothing holds must still be
`DENIAL_NOT_FOUND`. A mistyped clone must say so, where a mistyped push at
worst leaves an empty repository behind.

**A push asks before it pushes.** Enroute calls `authorize` with write access
for the ref advertisement too, so a push somebody then abandons has already
made the repository. That is the cost of the pattern, and it is why the create
must be cheap and idempotent, and must not also send mail.

Refusing an unknown path is the other choice, and the one to prefer once the
table is the set of repositories that exist.

## Scope

**It stores no name anywhere Enroute can see.** `CreateRepository` takes a key
and a default branch, and nothing else. Every listing by name, every search,
and every "who owns this" is a query against your own table.
`ListRepositories` answers in key order, and that order means nothing to a
person.

## See also

- [Private repositories](private-repositories.md) — the rest of `authorize`,
  and who may read what this table names.
- [Protected branches](protected-branches.md) — policy on the refs inside one
  of these.
- [The contract](../reference/the-contract.md) — `RepositoryService`, and the
  paging rules the sweep depends on.
