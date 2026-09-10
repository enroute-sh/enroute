# Creating, renaming and deleting repositories

The name of a repository, its owner, and what has to happen when either of
them changes.

## The problem

People want repositories that have a name, belong to somebody, and can be
renamed or thrown away. Enroute stores a repository under the key the
application gave it and holds nothing else: no name, no owner, and no column
that could become one. The map from a name to a key is a table of the
application, and it is the only thing that knows what a repository is called.

The obvious key is the name. It is unique, it is what the URL carries, and it
is what `authorize` receives. Then somebody renames the project, and the key
does not move with it: every clone URL now names a different repository, or
none. Nothing errors at the rename. It fails at the next push.

The other cost is in the order of two writes. A create is a row in the
application and a repository in Enroute. If the process stops between the two,
one of them is an orphan, and which one decides whether a name breaks forever
or storage sits unused.

## The solution

Key by an id the application allocates and never changes. Make the repository
first and write the row second. A rename does not touch Enroute.

```ts
async function create(name: string, owner: string): Promise<string> {
  const id = newId();                                       // a ULID, a UUID, a row id
  await repos.createRepository({ repo: { key: id } });      // Enroute first
  await db.insert("repos", { id, name, owner });
  return id;
}

async function rename(from: string, to: string): Promise<void> {
  await db.update("repos", { name: to }, { name: from });
}

async function remove(name: string): Promise<void> {
  const row = await repoByName(name);
  if (!row) return;
  await db.delete("repos", { name });                       // git stops resolving it
  await repos.deleteRepository({ repo: { key: row.id } });
}
```

A rename is one row. Nothing tells Enroute, because there is nothing to tell:
no key changes, no object moves, and no clone breaks. To move a repository to
another owner costs the same one `UPDATE`.

**The key is the row's id, never its name.** A key is what a repository *is*
to Enroute, so [a name a rename moves is the wrong thing to use](../recipes.md).
`authorize` is where a name becomes a key: look the path up, and answer with
the id of the row.

**The create is idempotent on the key.** A second `createRepository` with the
same key answers with the same repository and makes nothing new. A retry of
`create` after a crash needs no check first.

## Order both so the orphan is in Enroute

The two orderings are opposite, for the same reason. A repository nothing
names costs storage. A row that names a repository that does not exist breaks
every request for that name.

- **To create**, make the repository *first* and insert the row second. A stop
  between them leaves a repository nothing names.
- **To delete**, drop the row *first* and call `deleteRepository` second. The
  drop of the row is what makes the name unreachable, because `authorize` can
  no longer resolve it. A stop between them leaves storage to reclaim on a
  retry, not a repository still serving git. `DeleteRepository` is idempotent,
  so the retry is free.

`listRepositories` finds the orphans. It walks every key the tenant holds, a
page at a time, and a key no row names is one to delete:

```ts
async function sweepOrphans(): Promise<void> {
  let pageToken = "";
  do {
    const page = await repos.listRepositories({ limit: 0, pageToken });
    for (const r of page.repositories) {
      if (!(await repoById(r.repo!.key))) {
        await repos.deleteRepository({ repo: r.repo });
      }
    }
    pageToken = page.nextPageToken;
  } while (pageToken !== "");
}
```

A short page is not the last page. Stop only when `nextPageToken` comes back
empty.

The same ordering settles a race between two creates of one name. Each caller
allocates its own id and makes its own repository. Let the unique constraint
on the name decide, and have the loser delete what it made:

```ts
async function create(name: string): Promise<string> {
  const id = newId();
  await repos.createRepository({ repo: { key: id } });
  try {
    await db.insert("repos", { id, name });
    return id;
  } catch (err) {
    if (!isUniqueViolation(err)) throw err;
    await repos.deleteRepository({ repo: { key: id } });   // or leave it for the sweep
    const winner = await repoByName(name);
    return winner!.id;
  }
}
```

## Making a repository by pushing to it

The push that first needs a repository can create it, in `authorize`. The
name is not in the table, so make it, and grant what was made.

```ts
async function authorize(req: AuthorizeRequest): Promise<AuthorizeResponse> {
  // Both spellings of the URL name one repository here, so the row is keyed
  // without the `.git` that a clone URL carries by convention.
  const name = req.repoPath.replace(/\.git$/, "");
  let row = await repoByName(name);
  if (!row) {
    if (req.access !== Access.WRITE) return deny(Denial.NOT_FOUND);
    row = { id: await create(name), name };
  }
  // ... grant { repo: { key: row.id }, ... }, as in private-repositories.md
}
```

**Only on write.** A fetch of a name nothing holds must still be `NOT_FOUND`.
A mistyped clone must say so, where a mistyped push at worst leaves an empty
repository behind.

**A push asks before it pushes.** Enroute calls `authorize` with write access
for the ref advertisement too, so a push the person then abandons has already
made the repository. That is the cost of the pattern, and it is why the create
must be cheap and idempotent, and must not also send mail.

To refuse an unknown name is the other choice, and the one to prefer once the
table is the set of repositories that exist.

## What it does not do

**It stores no name anywhere Enroute can see.** `CreateRepository` takes a key
and a default branch, and nothing else. Every listing by name, every search,
and every "who owns this" is a query against the application's table.
`listRepositories` answers in key order, and that order means nothing to a
person.

## See also

- [private-repositories.md](private-repositories.md): the rest of `authorize`,
  and who may read what this table names.
- [protected-branches.md](protected-branches.md): policy on the refs inside
  one of these.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read,
  including the key this one chooses.
