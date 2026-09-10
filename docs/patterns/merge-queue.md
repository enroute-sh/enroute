# Merge queue

A merge queue processes open merge requests in order and requires checks for
the commit that will become the trunk tip.

## Use this pattern when

Two things have to hold at once. Only a branch with passing evidence may land,
and two merges at the same moment must not both win. The second would
overwrite the first, or land a branch that was never tested against it.

Enroute moves refs and answers ancestry questions. It holds no queue, no lock,
and no lease, so it appears that your app has to serialize the writers.

A common approach uses a worker
process, a lock it holds while it works, a lease it renews so a crashed worker
does not hold the lock forever, and a record of what is in flight so a restart
can continue. That is four pieces of state, and every one exists only to stop
two writers colliding.

`UpdateRefs` compares `oldObjectId` with the current ref value, so it provides
the required compare-and-swap behavior. Concurrent callers produce one
successful update and one refusal without an application lock.

## Implement the pattern

Read the state, evaluate the request, update the ref, and return.

```ts
import { RefUpdateOutcome_Rejection_Reason as Reason } from "@/lib/gen/enroute/api/v1alpha1/ref";
import { isAncestor, updateRefs } from "@/lib/enroute";
import { outstanding } from "@/lib/checks";

export async function land(mr: MergeRequest, trunkTip: string, reported: Check[]) {
  // Something already landed these commits.
  if (mr.tip === trunkTip) return { merged: mr.tip };

  if (!(await isAncestor(mr.repoId, trunkTip, mr.tip))) {
    return { blocked: "behind the trunk: rebase and push again" };
  }

  const waiting = outstanding(mr.requiredChecks, reported);
  if (waiting.length > 0) {
    return { blocked: `waiting on ${waiting.join(", ")}` };
  }

  const { outcomes } = await updateRefs(mr.repoId, [{
    refname: mr.target,
    oldObjectId: { hex: trunkTip },
    newObjectId: { hex: mr.tip },
  }]);

  // The trunk moved between the read and the move. Nothing is wrong, and the
  // next pass measures against the new tip.
  if (outcomes[0].rejection?.reason === Reason.REASON_NON_FAST_FORWARD) {
    return { retry: true as const };
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
back refused, so the loser is refused rather than served.

**A rejection is an outcome, not an error.** The RPC returns `OK`. Read
`outcomes`, which arrive in the order the updates were sent, and treat an
absent `rejection` as landed. See
[The contract](../reference/the-contract.md#refs).

**`REASON_NON_FAST_FORWARD` needs no repair.** It means only that the trunk
moved between the read and the write. Show it differently on the page: "the
trunk moved, will retry" is not the same message as "rebase your branch". If
you tell a contributor to act on a race, you waste their time.

**A repository with no trunk needs a create.** Leave `oldObjectId` unset,
which is how this contract spells an absent ID everywhere. That does not waive
a check; it is a different one. A second caller that got there first comes
back `REASON_ALREADY_EXISTS`, so the create is its own race protection.

### Make one pass over every open request

```ts
export async function run(repoId: string) {
  const { refs } = await listRefs(repoId);            // every branch
  const pending = await openRequests(repoId);         // oldest first
  const reported = await checksAtAny(pending.map((mr) => mr.tip));

  let trunk = refs.find((r) => r.name === TRUNK)?.objectId?.hex;
  if (!trunk) return;                                  // no trunk yet

  for (const mr of pending) {
    const outcome = await land(mr, trunk, reported.get(mr.tip) ?? []);
    if (outcome.merged) trunk = outcome.merged;
  }
}
```

Carry the updated trunk tip through the iteration. Evaluate the next request
against that value so an out-of-date branch is blocked before its ref update.

**Read in batches.** One `ListRefs` for every tip, and one query for the
checks of every request. This runs while somebody waits on an HTTP response,
so avoid a round trip per request.

### Run it on events and sweep as a safety net

Land what can be landed at the moments something changed: a check reported, a
request opened. The merge then happens during that call rather than on a later
sweep, and in the same response the reporter learns the branch is on the
trunk.

Keep a timer as well, but as a net and not as the mechanism. It catches the
merges nothing triggered: two requests that raced, where the loser was told
the trunk moved and has nothing left to report, or a request opened before its
branch was pushed. It can be infrequent, because nobody waits on it.

Where merges happen *only* on a timer, the period of the timer is how long
everybody waits.

```ts
// app/api/cron/merge-queue/route.ts
export async function POST(request: Request) {
  const secret = process.env.CRON_SECRET;
  if (!secret) return new Response("not configured", { status: 503 });
  if (!constantTimeEqual(bearer(request), secret)) {
    return new Response("unauthorized", { status: 401 });
  }

  for (const repo of await allRepos()) {
    try {
      await run(repo.id);
    } catch (err) {
      console.error({ repo: repo.id, err });     // and go on to the next
    }
  }
  return new Response("ok");
}
```

You write `constantTimeEqual`. Node's `crypto.timingSafeEqual` throws when the
two buffers differ in length, so it needs a length check that does not itself
leak. The usual way is to compare a digest of each side.

**Refuse when the secret is unset**, rather than reading unset as "no check
needed". A missing secret must never mean everything is allowed.

**A failure in one repository must not stop the rest.** A sweep is the thing
that recovers from a bad state, so it is the last place to give up on the
first problem it meets.

## Scope

**It does not build a merge.** The tip that lands is the tip that was tested,
unchanged. Whoever owns a branch that has fallen behind rebases it. The shapes
available are in [Merge requests](merge-requests.md).

**It orders only by age.** Oldest first is a policy, not a property.
Priorities, batching several requests into one trunk move, and speculative
testing of a queue are all things this shape can grow, and none of them is
here.

## See also

- [Merge requests](merge-requests.md) — the record this queue reads, and the
  merge it performs.
- [Checks](checks.md) — where `outstanding` comes from.
- [Protected branches](protected-branches.md) — closing the trunk, which is
  what makes this queue its only writer.
