# Checks: evidence instead of CI

One row that says something ran, against one commit, and came out a
particular way. A merge is a comparison over those rows.

## The problem

Nothing may land on the trunk unless the tests passed and somebody looked
at it. What it usually costs is a CI system.

Enroute is not in this recipe, except that the commit id came from a push it
announced. It holds no job, no verdict, and no notion of a check, so the
application alone answers "may this land".

The obvious answer is to build a runner into the application: take the push,
start a job, hold the job, poll it, and let the merge wait on the result. That
makes the application the owner of one live run per push. A thousand pushers
is a thousand pieces of in-flight state to schedule, to recover after a
restart, to time out, and to explain when it stalls. That state exists only
because you decided to run the work here.

It is also usually the second time the work has been done. Where the things
pushing are agents, they ran the suite in their own sandbox before they
pushed. To run it again pays twice for one answer, and the second answer is
the late one.

## The solution

Whoever *ran* the thing posts the result. There is no runner here, no queue,
and no job state.

```ts
// POST /repos/<name>/checks
// {"commit": "<40 hex>", "check": "test", "verdict": "pass"}

async function report(
  repo: string,
  commit: string,
  name: string,
  verdict: Verdict,
  reporter: string,
): Promise<{ merged: string[] }> {
  await db.upsert(
    "checks",
    { repo, commit, name },                    // the key it replaces on
    { verdict, reporter, at: new Date() },
  );
  return { merged: await tryToLandEverythingOpen(repo) };
}
```

The route that records a check answers with what that check let through. That
makes the workflow synchronous for the reporter: it posts the last verdict,
and in the same response it learns that the branch is on the trunk.

**Upsert on `(repo, commit, name)`.** A second run replaces the row instead of
adding one, because what a merge asks is what the evidence says *now*. A forge
that wants the history of attempts puts it in a second table and leaves this
one as the current answer.

**Name the reporter from its credential, never from the body.** A reporter
that can name itself is not evidence of anything.

**Bind a check to a commit.** Then a branch that moved has no evidence at its
new tip, and staleness needs no expiry pass, no invalidation hook, and no
rule about which pushes clear what. The stronger version binds a check to the
*tree* the commit names, so evidence survives a rebase that changed no
content. Bound to a commit is the small version, and the one to write first.

**A test result and a review are one row.** Both say "somebody ran something
and it came out this way". A table of its own for review adds a second thing
to consult before a merge, and no second thing to know.

**The runner is one program.** It fetches the branch, runs a command against
it, and reports the verdict. A reviewing agent is that program with a
different command: it reads the branch against the trunk, asks a model whether
it may land, and exits 0 or 1. Nothing about it is privileged. Swap the
model, the provider, or the whole program for a person, and nothing else
changes.

**Fail closed.** A refusal, a truncated answer, or a missing verdict line must
all block the merge. A gate that fails open is not a gate.

## The predicate is one comparison

```ts
function outstanding(required: string[], reported: Check[]): string[] {
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

## What it does not do

**It runs nothing and schedules nothing.** If no reporter posts, no check is
"failed". It is absent, and absent blocks. That is the intended behaviour,
and it is why the required set belongs in the application's configuration and
not anywhere a branch can reach.

**It does not know what a check means.** `pass` is a string the application
chose. Enroute never sees any of this.

## See also

- [merge-queue.md](merge-queue.md): the caller of `outstanding`, and what
  lands once nothing is outstanding.
- [ci-on-push.md](ci-on-push.md): the other shape, where the application
  starts the work instead of receiving the verdict.
- [protected-branches.md](protected-branches.md): why a push to a branch under
  review clears no evidence.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
