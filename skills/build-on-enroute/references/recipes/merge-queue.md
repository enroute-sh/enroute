# A merge queue

Landing open merge requests one at a time, each on evidence gathered at the
exact tip that becomes the trunk.

## The problem

Two things have to hold at once. Only a branch with passing evidence may
land, and two merges at the same moment must not both win. The second would
overwrite the first, or land a branch that was never tested against it.

Enroute moves refs and answers ancestry questions. It holds no queue, no lock,
and no lease, so it appears that the application has to serialize the
writers.

The obvious answer is the one every merge queue is built from: a worker
process, a lock it holds while it works, a lease it renews so a crashed worker
does not hold the lock forever, and a record of what is in flight so a restart
can continue. That is four pieces of state, and every one exists only to stop
two writers colliding.

They are not needed here. `UpdateRefs` compares `oldObjectId` against what the
ref holds, so it is already a compare-and-swap. Two callers racing produce one
winner and one refusal, decided by the ref store and not by anything the
application arranged. The lock prevents a collision the contract already made
impossible, and the lease, the worker, and the in-flight record exist only to
look after the lock.

## The solution

Read the state, decide, move the ref, and return. Nothing runs between passes.

```ts
import {
  RefUpdateOutcome_Rejection_Reason as Reason,
} from "./gen/enroute/api/v1alpha1/ref_pb";

async function land(
  mr: MergeRequest,
  trunkTip: string,
  reported: Check[],
): Promise<Landing> {
  // Something already landed these commits.
  if (mr.tip === trunkTip) return { merged: mr.tip };

  const { isAncestor } = await refs.isAncestor({
    repo: mr.repo,
    ancestorCommitId: { hex: trunkTip },
    descendantCommitId: { hex: mr.tip },
  });
  if (!isAncestor) {
    return { blocked: "behind the trunk: rebase and push again" };
  }

  const waiting = outstanding(mr.requiredChecks, reported);
  if (waiting.length > 0) {
    return { blocked: `waiting on ${waiting.join(", ")}` };
  }

  const { outcomes } = await refs.updateRefs({
    repo: mr.repo,
    updates: [{
      refname: mr.target,
      oldObjectId: { hex: trunkTip },
      newObjectId: { hex: mr.tip },
    }],
  });

  // The trunk moved between the read and the move. Nothing is wrong, and the
  // next pass measures against the new tip.
  if (outcomes[0].rejection?.reason === Reason.NON_FAST_FORWARD) {
    return { retry: true };
  }
  return { merged: mr.tip };
}
```

The order matters. Require the branch to hold the current trunk *before* you
require the evidence, because a branch that does not hold it is not what
anything was run against. The tip that passes is exactly what the trunk
becomes, which is why the queue tests the branch and never a merge of it.

**The compare-and-swap is the queue.** Nothing has to be locked, and no pass
has to be the only one running. An update whose `oldObjectId` is stale comes
back refused, so the loser is refused and not served.

**A rejection is an outcome, not an error.** The RPC returns `OK`. Read
`outcomes`, which arrive in the order the updates were sent, and treat an
absent `rejection` as landed.

**`Reason.NON_FAST_FORWARD` needs no repair.** It means only that the trunk
moved between the read and the write. Show it differently on the page: "the
trunk moved, will retry" is not the same message as "rebase your branch". If
you tell a contributor to act on a race, you waste their time.

**A repository with no trunk needs a create.** Leave `oldObjectId` unset,
which is [how the contract spells an absent id everywhere](../recipes.md).
That does not waive a check. It is a different one. A second caller that got
there first comes back `Reason.ALREADY_EXISTS`, so the create is its own race
protection.

## One pass over every open request

```ts
async function run(repo: RepoKey): Promise<void> {
  const { refs: tips } = await refs.listRefs({ repo });  // every branch
  const pending = await openRequests(repo);              // oldest first
  const reported = await checksAtAny(pending.map((mr) => mr.tip));

  let trunk = tips.find((r) => r.name === TRUNK)?.objectId?.hex;
  if (!trunk) return;                          // no trunk yet. See above

  for (const mr of pending) {
    const outcome = await land(mr, trunk, reported.get(mr.tip) ?? []);
    if (outcome.merged) trunk = outcome.merged;
  }
}
```

**Carry the trunk through the loop** instead of reading it again. A request
that just landed moved the trunk. Measure the next request against where the
trunk is *now*, so a branch cut from the old tip is told to rebase in this
pass instead of being refused by Enroute in the next one.

**Read in batches.** One `ListRefs` for every tip, and one query for the
checks of every request. This runs while somebody waits on an HTTP response,
so avoid a round trip per request.

## Run it on the events, and sweep as a safety net

Land what can be landed at the moments something changed: a check reported, a
request opened. The merge then happens during that call instead of on a later
sweep, and in the same response the reporter learns that the branch is on the
trunk.

Keep a timer as well, but as a net and not as the mechanism. It catches the
merges nothing triggered: two requests that raced, where the loser was told
the trunk moved and has nothing left to report, or a request opened before its
branch was pushed. It can be infrequent, because nobody waits on it.

In a forge where merges happen *only* on a timer, the period of the timer is
how long everybody waits.

```ts
async function sweep(req: Request): Promise<Response> {
  const secret = process.env.CRON_SECRET;
  if (!secret) return new Response("not configured", { status: 503 });
  if (!constantTimeEqual(bearer(req), secret)) {
    return new Response("unauthorized", { status: 401 });
  }

  for (const repo of await allRepos()) {
    try {
      await run(repo);
    } catch (err) {
      log.error({ repo, err });          // and go on to the next
    }
  }
  return new Response("ok");
}
```

You write `constantTimeEqual`. The Node function `crypto.timingSafeEqual`
throws when the two buffers differ in length, so it needs a length check that
does not itself leak. The usual way is to compare a digest of each side.

**Refuse when the secret is unset**, instead of reading unset as "no check
needed". A missing secret must never mean that everything is allowed.

**A failure in one repository must not stop the rest.** A sweep is the thing
that recovers from a bad state, so it is the last place to give up on the
first problem it meets.

## What it does not do

**It does not build a merge.** The tip that lands is the tip that was tested,
unchanged. Whoever owns a branch that has fallen behind rebases it. The shapes
available are in [merge-requests.md](merge-requests.md).

**It orders only by age.** Oldest first is a policy, not a property.
Priorities, batching several requests into one trunk move, and speculative
testing of a queue are all things this shape can grow, and none of them is
here.

## See also

- [merge-requests.md](merge-requests.md): the record this queue reads, and the
  merge it performs.
- [checks.md](checks.md): where `outstanding` comes from.
- [protected-branches.md](protected-branches.md): closing the trunk, which is
  what makes this queue its only writer.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
