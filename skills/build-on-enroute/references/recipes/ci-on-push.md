# CI on push

How to start a build at the moment a push lands, and how to read what it
changed with no checkout.

## The problem

Something has to notice that a branch has a new tip. The alternative to being
told is to list refs on a timer and compare them with what you saw last,
which is slow when it is cheap and expensive when it is quick.

`postReceive` tells you, with two limits that decide the whole design.
Enroute waits for the answer, and you have ten seconds to give one, so the
work cannot happen there. And delivery is best effort and **at most once**:
the push has already succeeded when Enroute makes the call, so Enroute cannot
retry it without lying to a client that has gone.

The obvious answer treats the call as a guarantee. It is not one. Nothing
tells an application that was unreachable for those few seconds, nothing
retries, and that commit is never built. There is no error, no failed job in
a list, and no queue depth to alarm on. The branch sits there looking built,
and the first person to notice is whoever wonders why the artifact is old.

Before you reach for this at all, check whether the work has already been
done. [checks.md](checks.md) is the other shape: the application starts
nothing, and whoever ran something reports the verdict. Prefer that one where
the things pushing already run the suite themselves, and this one where the
push is the only thing that knows a build is due.

## The solution

Put the work on a queue in the hook, and keep a record so you can recover
from a missed call.

```ts
async function postReceive(
  req: PostReceiveRequest,
): Promise<PostReceiveResponse> {
  for (const c of req.commands) {
    if (!c.newObjectId) continue;             // a delete builds nothing
    await enqueueBuild(req.repo, c.refname, c.newObjectId.hex, req.actor);
  }
  return { messages: [] };
}
```

**Enqueue, never build.** The hook has ten seconds, and Enroute waits for the
answer. Whatever the build is, it belongs behind a queue.

**Reconcile against the refs.** Keep one row per ref, holding the tip you
built last, and compare it with `listRefs` on a timer or when somebody opens
the page. The hook makes a build prompt. The record makes it certain.
Anything whose correctness depends on being told needs its own record.

**Read what changed with no clone.** `diffCommit` gives the paths a push
touched, as blob id pairs, and it streams:

```ts
async function changedPaths(repo: RepoKey, c: RefCommand): Promise<string[]> {
  const paths: string[] = [];
  for await (const page of objects.diffCommit({
    repo,
    commitId: c.newObjectId,
    baseCommitId: c.oldObjectId,   // unset on a create: see below
  })) {
    paths.push(...page.changes.map((change) => change.path));
  }
  return paths;
}
```

A create carries no `oldObjectId`. If you pass it through unset, the diff is
against the first parent of the commit, which is what that commit changed and
not what the branch did. For the latter, pass the merge base against the
trunk. One config file is a `listTree` and a `getObject`, and a build that
needs no working tree needs no checkout.

## What it does not do

**It reports nothing back.** Nothing a build learns reaches Enroute, and
nothing Enroute holds is gated on it. To turn a verdict into permission to
merge, see [checks.md](checks.md).

**It does not see a refused push.** `postReceive` carries the commands that
landed, and only those. A push whose commands were all refused sends nothing,
not an empty list.

## See also

- [checks.md](checks.md): the inverse shape, where the runner reports and the
  application runs nothing.
- [mirroring.md](mirroring.md): the same hook, the same at-most-once problem,
  and the same answer of reconciling against state.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
