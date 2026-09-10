# Protected branches

Protected branches cannot be force-pushed, deleted, or written to directly by
people who can otherwise push.

This builds on the app from [Build a code hosting
platform](../build/README.md), which leaves `pre_receive` allowing every
command. Read that first: this page changes one function in it.

## Use this pattern when

### What Enroute refuses on its own

Enroute refuses a push whose objects never arrived, whose refname Git would not
accept, and whose `oldObjectId` no longer matches what the ref holds. It
refuses nothing else.

A force-push over the trunk, a deleted trunk, and a branch nobody reviewed each
land unless your app says otherwise. Enroute holds no opinion about which
refs may be rewritten, which is why `pre_receive` exists.

### Reject deletion as well as force pushes

Checking only `force` does not protect deletion:

```ts
if (command.force) return refuse("main is protected");   // not enough
```

A delete carries no `newObjectId`, and `force` is only ever true for a command
that has *both* IDs, because a command with one ID rewrites no history. So a
delete arrives with `force` false and sails past that test.

Without a deletion check, a protected branch can still be removed.

**Test the delete before the force.**

## Implement the pattern

### Store protection rules

Read the rule from a table rather than a literal, so that protecting a second
branch is a row and not a second hook. Add it to `lib/db/schema.ts`:

```ts
export const protections = pgTable("protections", {
  id: serial("id").primaryKey(),
  repoId: text("repo_id").notNull().references(() => repos.id),
  // An exact refname, or a prefix ending in `*`.
  pattern: text("pattern").notNull(),
  allowDirectPush: boolean("allow_direct_push").notNull().default(false),
});
```

```sh
npx drizzle-kit push
```

And the query, in `lib/store.ts`:

```ts
export function protectionsFor(repoId: string) {
  return db.select().from(protections).where(eq(protections.repoId, repoId));
}
```

Give the repository two rows. Add them to `scripts/seed.ts`:

```ts
import { protections } from "../lib/db/schema";

await db
  .insert(protections)
  .values([
    { repoId: "repo-4f2a1c", pattern: "refs/heads/main", allowDirectPush: false },
    { repoId: "repo-4f2a1c", pattern: "refs/tags/*", allowDirectPush: true },
  ])
  .onConflictDoNothing();
```

Then replace `preReceive` and `judge` in `lib/hooks/receive.ts`:

```ts
import { protectionsFor } from "@/lib/store";

type Rule = Awaited<ReturnType<typeof protectionsFor>>[number];

function matches(pattern: string, refname: string) {
  return pattern.endsWith("*")
    ? refname.startsWith(pattern.slice(0, -1))
    : refname === pattern;
}

export async function preReceive(req: PreReceiveRequest) {
  // One query for the push, not one per ref.
  const rules = await protectionsFor(req.repo!.key);

  const judgements = [];
  for (const command of req.commands) {
    judgements.push({ refname: command.refname, ...judge(rules, command) });
  }
  return { judgements };
}

function judge(rules: Rule[], command: RefCommand) {
  const rule = rules.find((r) => matches(r.pattern, command.refname));
  if (!rule) return allow;

  // A delete first: it carries no newObjectId, so force is false for it.
  if (!command.newObjectId) {
    return refuse(`${command.refname} is protected and cannot be deleted`);
  }

  // A create has no old side. Somebody has to be able to make the branch that
  // does not exist yet, and there is no history to overwrite.
  if (!command.oldObjectId) return allow;

  if (command.force) {
    return refuse(`${command.refname} is protected: no force-push`);
  }

  if (!rule.allowDirectPush) {
    return refuse(`${command.refname} is protected: open a merge request`);
  }

  return allow;
}
```

### Why it is shaped like this

**`force` means what Git means.** It is true when `oldObjectId` is not an
ancestor of `newObjectId`, which is a rewrite. Enroute answers it because your
app holds no commit graph and cannot work it out. A ref that moves forward
over a stale value is a different thing, and Enroute refuses that before it
asks you.

**`reason` is all the person gets.** Their Git client prints it the way it
prints the stderr of a hook. Name the ref and say what to do instead.

**A refusal is per ref.** Enroute offers a Git client no `atomic` capability,
so one refused ref in a push of three does not hold back the other two. Say
which ref you refused and why, in each refusal.

**Let the first push create it.** An unset `oldObjectId` is a create. Without
that exception a new repository could never get a trunk. The exception is a
state that stops existing, not an identity that keeps working.

**Judge every command.** `judge` selects the applicable rule, and the handler
returns a judgment for each command. Branches without a matching rule are
explicitly allowed.

**Read the rules once per push.** Load rules before processing commands. A
push can update many refs, so querying policy per ref adds unnecessary work.

### Gate landing, not working

Refuse pushes to the trunk and let every other branch land freely. If you gate
feature branches, you ask a contributor to earn permission to *work*, when what
needs earning is permission to *land*. Pushing a branch costs nothing and
blocks nobody.

Two things this hook must not do:

- **Do not consult a review.** A push to a branch under review is how somebody
  addresses feedback, not a violation.
- **Do not exempt whatever lands the merges.** The strongest form of this rule
  refuses *every* push to the trunk, from everybody, and needs no exception: a
  merge goes over the API with `UpdateRefs`, which runs no `pre_receive` hook.
  A hook that admits one privileged actor instead is two rules that have to
  agree, and the trunk is then only as safe as the guarantee that no Git client
  can claim that name.

## Check your work

A push to a feature branch still lands:

```sh
cd widgets
git switch -c feature/pricing
git commit --allow-empty -m "start pricing"
git push origin feature/pricing
```

A direct push to `main` is refused:

```sh
git switch main
git commit --allow-empty -m "straight to main"
git push origin main
```

```text
 ! [remote rejected] main -> main (refs/heads/main is protected: open a merge request)
error: failed to push some refs to 'http://127.0.0.1:8080/acme/widgets.git'
```

Deleting the trunk is refused, and this is the check the force-only rule would
have failed:

```sh
git push origin --delete main
```

```text
 ! [remote rejected] main (refs/heads/main is protected and cannot be deleted)
```

The repository page still shows `main` at its old tip, because nothing moved.

## Result

- A trunk that cannot be rewritten, removed, or written to directly.
- A rule read from a table, so a namespace is a row rather than a second hook.
- Proof that your app decides something, rather than returning `200` and
  waving pushes through.

## Scope

**It does not keep the policy safe.** A rule a pusher can change by pushing is
not a rule. Everything a merge is gated on belongs where the thing being gated
cannot reach it: your app's own configuration, reviewed and deployed like the
rest of it, never a file in the branch under test.

**It does not hide anything.** A refused push still told the pusher that the
ref exists. Hiding a ref is the `visible_refs` hook.

**It does not land anything.** With direct pushes closed, something has to
merge. That is `UpdateRefs` over the API, which runs no hook because there the
caller is your app.

## See also

- [Merge requests](merge-requests.md) — the door left open once this one
  closes the trunk.
- [Merge queue](merge-queue.md) — what lands through that door, and on what
  evidence.
- [`pre_receive`](../reference/hooks.md#pre_receive) — every field this hook
  carries, with payloads.
- [The contract](../reference/the-contract.md#refs) — `UpdateRefs`, which is
  how a merge lands once direct pushes are closed.
