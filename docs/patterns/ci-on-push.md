# CI on push

This pattern starts a build when a push lands and reads the changes without a
checkout.

## Use this pattern when

Something has to notice that a branch has a new tip. The alternative to being
told is to list refs on a timer and compare them with what you saw last, which
is slow when it is cheap and expensive when it is quick.

`post_receive` tells you, with two limits that decide the whole design.
Enroute waits for the answer, and you have ten seconds to give one, so the
work cannot happen there. And delivery is best-effort and at most once:
the push has already succeeded when Enroute makes the call, so it cannot retry
without lying to a client that has gone.

Treating the call as a guarantee fails. Nothing
tells an app that was unreachable for those few seconds, nothing retries, and
that commit is never built. There is no error, no failed job in a list, and no
queue depth to alarm on. The branch sits there looking built, and the first
person to notice is whoever wonders why the artifact is old.

Before reaching for this at all, check whether the work has already been done.
[Checks](checks.md) is the other shape: your app starts nothing, and whoever
ran something reports the verdict. Prefer that where the things pushing
already run the suite themselves, and this one where the push is the only
thing that knows a build is due.

## Implement the pattern

Put the work on a queue in the hook, and keep a record so you can recover from
a missed call.

```ts
export async function postReceive(req: PostReceiveRequest) {
  for (const command of req.commands) {
    if (!command.newObjectId) continue;          // a delete builds nothing
    await enqueueBuild({
      repoId: req.repo!.key,
      refname: command.refname,
      commitId: command.newObjectId.hex,
      actor: req.actor,
    });
  }
  return { messages: [] };
}
```

**Enqueue, never build.** The hook has ten seconds and Enroute waits for the
answer. Whatever the build is, it belongs behind a queue.

**Reconcile against the refs.** Keep one row per ref holding the tip you built
last, and compare it with `ListRefs` on a timer, or when somebody opens the
page. The hook makes a build prompt. The record makes it certain. Anything
whose correctness depends on being told needs its own record.

**Read what changed with no clone.** `DiffCommit` gives the paths a push
touched, as object ID pairs, and it streams:

```ts
export async function changedPaths(repoId: string, command: RefCommand) {
  const stream = objects.diffCommit(
    {
      repo: { key: repoId },
      commitId: command.newObjectId,
      baseCommitId: command.oldObjectId,
    },
    tenant(),
  );

  const paths: string[] = [];
  for await (const page of stream as AsyncIterable<DiffCommitResponse>) {
    paths.push(...page.changes.map((change) => change.path));
  }
  return paths;
}
```

A create carries no `oldObjectId`. Passed through unset, the diff is against
the first parent of the commit, which is what *that commit* changed and not
what the branch did. For the latter, pass the merge base against the trunk —
see [Merge requests](merge-requests.md). One config file is a `ListTree` and a
`GetObject`, and a build that needs no working tree needs no checkout.

## Scope

**It reports nothing back.** Nothing a build learns reaches Enroute, and
nothing Enroute holds is gated on it. To turn a verdict into permission to
merge, see [Checks](checks.md).

**It does not see a refused push.** `post_receive` carries the commands that
landed, and only those. A push whose commands were all refused sends nothing
at all, not an empty list.

## See also

- [Checks](checks.md) — the inverse shape, where the runner reports and your
  app runs nothing.
- [Mirroring](mirroring.md) — the same hook, the same at-most-once problem,
  and the same answer of reconciling against state.
- [`post_receive`](../reference/hooks.md#post_receive) — what the call
  guarantees, and what it does not.
