# Checks

Each row records a result for one commit. A merge compares those rows.

Enroute does not appear in this pattern, except that the commit ID came from a
push it announced. It holds no job, no verdict, and no notion of a check, so
your app alone answers "may this land".

## Use this pattern when

Use this pattern when a merge requires test results or review results for its
proposed commit.

One approach builds a runner into your app: take the push, start a job,
hold the job, poll it, and let the merge wait on the result. That makes your
app the owner of one live run per push. A thousand pushers is a thousand
pieces of in-flight state to schedule, to recover after a restart, to time
out, and to explain when it stalls. That state exists only because you decided
to run the work here.

It is also usually the second time the work has been done. Where the things
pushing are agents, they ran the suite in their own sandbox before they
pushed. Running it again pays twice for one answer, and the second answer is
the late one.

## Implement the pattern

The system that runs a check posts its result. This pattern does not include a
runner, queue, or job state.

```ts
export const checks = pgTable("checks", {
  repoId: text("repo_id").notNull().references(() => repos.id),
  commitId: text("commit_id").notNull(),
  name: text("name").notNull(),
  verdict: text("verdict").notNull(),          // "pass" | "fail"
  reporter: text("reporter").notNull(),
  at: timestamp("at").notNull().defaultNow(),
}, (t) => ({ pk: primaryKey({ columns: [t.repoId, t.commitId, t.name] }) }));
```

```ts
// POST /api/repos/:id/checks
// {"commit": "<40 hex>", "check": "test", "verdict": "pass"}

export async function report(
  repoId: string,
  commitId: string,
  name: string,
  verdict: string,
  reporter: string,
) {
  await db
    .insert(checks)
    .values({ repoId, commitId, name, verdict, reporter })
    .onConflictDoUpdate({
      target: [checks.repoId, checks.commitId, checks.name],
      set: { verdict, reporter, at: new Date() },
    });

  return { merged: await tryToLandEverythingOpen(repoId) };
}
```

The route that records a check answers with what that check let through. That
makes the workflow synchronous for the reporter: it posts the last verdict,
and in the same response it learns the branch is on the trunk.

**Upsert on `(repo, commit, name)`.** A second run replaces the row rather
than adding one, because what a merge asks is what the evidence says *now*. If
you want the history of attempts, put it in a second table and leave this one
as the current answer.

**Name the reporter from its credential, never from the body.** A reporter
that can name itself is not evidence of anything.

**Bind a check to a commit.** Then a branch that moved has no evidence at its
new tip, and staleness needs no expiry pass, no invalidation hook, and no rule
about which pushes clear what. The stronger version binds a check to the
*tree* the commit names, so evidence survives a rebase that changed no
content. Bound to a commit is the small version, and the one to write first.

**Use one result format for checks and reviews.** Both are named verdicts for
a commit. A common format keeps merge evaluation simple.

**The reporter can be any external system.** It fetches the branch, evaluates
it, and reports a verdict. The merge policy only depends on the stored result.

**Fail closed.** A refusal, a truncated answer, or a missing verdict must all
block the merge. A gate that fails open is not a gate.

### Make the predicate one comparison

```ts
export function outstanding(required: string[], reported: Check[]) {
  const passing = new Set(
    reported.filter((c) => c.verdict === "pass").map((c) => c.name),
  );
  return required.filter((name) => !passing.has(name));
}
```

A check that failed and a check nobody reported come back in one list. To a
person reading the page they are different things. To a merge they are the
same thing: neither is permission to land. One list keeps admission a
comparison instead of a state machine.

## Scope

**It runs nothing and schedules nothing.** If no reporter posts, no check is
"failed". It is absent, and absent blocks. That is the intended behaviour, and
it is why the required set belongs in your app's configuration and not
anywhere a branch can reach.

**It does not know what a check means.** `pass` is a string you chose. Enroute
never sees any of this.

## See also

- [Merge queue](merge-queue.md) — the caller of `outstanding`, and what lands
  once nothing is outstanding.
- [CI on push](ci-on-push.md) — the other shape, where your app starts the
  work instead of receiving the verdict.
- [Protected branches](protected-branches.md) — why a push to a branch under
  review clears no evidence.
