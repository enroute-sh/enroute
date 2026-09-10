# Merge requests

Merge requests propose a branch, show its changes, and land it on the trunk.

[Chapter 5](../build/05-process-pushes.md) already prints an invitation from
`post_receive`. This is the record behind it, the diff a reviewer reads, and
the ref move that lands it.

## Use this pattern when

A merge request is a record that a branch wants to become part of another one,
plus a view of what that would do. Enroute holds neither. It holds the commits
and answers questions about them, so the record is yours, and you assemble
what the reviewer sees from a few reads.

Compute the diff from the merge base, not the current trunk tip. A diff against
the current trunk includes target-only commits as reversed changes. Use
`FindMergeBases` to find the common base, then compare the source branch to it.

## Implement the pattern

Keep the request as a row, and merge by moving a ref, with the tip the
reviewer was shown as the guard.

```ts
import { RefUpdateOutcome_Rejection_Reason as Reason } from "@/lib/gen/enroute/api/v1alpha1/ref";
import { updateRefs } from "@/lib/enroute";

export async function merge(mr: MergeRequest, targetTip: string) {
  const { outcomes } = await updateRefs(mr.repoId, [{
    refname: mr.target,
    oldObjectId: { hex: targetTip },     // the tip the reviewer was shown
    newObjectId: { hex: mr.tip },
  }]);

  // The trunk moved while the page was open. Re-read it and ask again.
  if (outcomes[0].rejection?.reason === Reason.REASON_NON_FAST_FORWARD) {
    return { stale: true as const };
  }
  return { tip: mr.tip };
}
```

**Pass the tip the reviewer was shown.** Leaving `oldObjectId` unset asks for
a *create*, not an unguarded write, so a trunk that exists comes back
`REASON_ALREADY_EXISTS`. Setting it makes this a compare-and-swap, and that is
the only thing between two reviewers who press Merge in the same second.

**The swap compares the old value, not ancestry.** `REASON_NON_FAST_FORWARD`
here means "the ref no longer holds `oldObjectId`". If the ref does hold it,
the move lands, whether or not `newObjectId` is a descendant. Enroute moves a
ref backwards when told to, and reports ancestry rather than imposing it. So
the following ancestry check is not a nicety for the reviewer: it is what stops a
merge of a stale branch from resetting the trunk.

**Close the front door in `pre_receive`.** Refuse a direct push to the trunk,
per [Protected branches](protected-branches.md), and the merge request is the
only way in. `UpdateRefs` runs no `pre_receive`, because the caller there is
your app, so the rule that refuses every person pushing to the trunk does not
refuse this merge. There is no exemption to write, and no way for a Git client
to borrow one.

### A push invites a request; it does not open one

Somebody pushes a branch many times before it is finished, so do not open a
request on every push. Use `post_receive` to tell them how to ask, at the one
moment you have their attention. Your app holds no connection to them and will
not have one again.

```ts
export async function postReceive(req: PostReceiveRequest) {
  const repo = await repoById(req.repo!.key);          // the key carries no name
  if (!repo) return { messages: [] };

  const pushed = req.commands
    .filter((c) => c.newObjectId && c.refname !== TRUNK)
    .map((c) => c.refname);

  const already = await openRequestsFor(repo.id, pushed);   // one query

  return {
    messages: pushed
      .filter((refname) => !already.has(refname))
      .map((refname) => {
        const branch = refname.slice("refs/heads/".length);
        return `Open a merge request: https://example.com/r/${repo.path}/merge/new?from=${branch}`;
      }),
  };
}
```

Skip the trunk, which arrives here only when the first push creates it, and
skip a branch that already has an open request. Use one query for the whole
push rather than one per ref: this runs while a person's terminal waits.

### Read the change

Read against the merge base, never against the tip of the trunk. `DiffCommit`
and `ListCommits` both stream, so fold each one as it arrives.

```ts
export async function show(mr: MergeRequest) {
  const { refs } = await listRefs(mr.repoId, [mr.target]);
  const targetTip = refs[0]?.objectId?.hex;
  if (!targetTip) return { unknown: true as const };

  const bases = await findMergeBases(mr.repoId, mr.tip, targetTip);
  if (bases.exhausted) return { unknown: true as const };   // not "no shared history"
  const base = bases.baseCommitIds[0];

  const { changes, truncated } = await diffCommitAgainst(mr.repoId, mr.tip, base);
  return { base, changes, truncated };
}
```

`diffCommitAgainst` is `diffCommit` from [chapter
7](../build/07-read-commits-and-diffs.md) with `baseCommitId` set instead of left
unset.

`ListCommits` walks first parents, which is the history the branch reads as.
It does not stop at the merge base on its own, so read pages until a commit ID
matches `base`, and drop the rest. Check `truncated` on the diff: a set that
stopped at a server limit is only the first part of the answer.

`FindMergeBases` answers with every base in `baseCommitIds`, because a
criss-cross history has more than one. One is the ordinary answer.
`exhausted` means the walk stopped before it had the answer, which is not the
same as "no shared history" — draw no diff and say why.

### Offer the reviewer the right information

Two ancestry questions decide it.

```ts
// The branch already holds the trunk, so it fast-forwards. False: it is
// behind, and wants a rebase and another push.
const ready = await isAncestor(mr.repoId, targetTip, mr.tip);

// It already landed. Close the merge request.
const landed = await isAncestor(mr.repoId, mr.tip, targetTip);
```

`IsAncestor` is also false when the repository does not know either commit.
Act on that answer the same way: this is not a history the repository can
vouch for.

## Scope

**Enroute makes no merge commit.** It cannot create a Git object outside a
packfile, so these are the shapes on offer:

| Shape | How it works |
| --- | --- |
| Fast-forward | Require the branch to hold the trunk, then move the ref. Nothing to build. |
| Rebase, then fast-forward | The rewritten commits must be *pushed* first, by the contributor or by a worker with a checkout. Then this process is the same as the fast-forward row. |
| A true merge commit | A worker with a checkout builds it and pushes it over Git. Merging it is then a ref move again. |

**The constraint decides the workflow.** Choose fast-forward-only even where a
merge commit is available. The trunk stays linear, and what was tested is what
landed, because the commits on the trunk are the exact commits the evidence
was reported for — see [Checks](checks.md). Whoever owns a branch that has
fallen behind rebases it and pushes again.

## See also

- [Merge queue](merge-queue.md) — this pattern with the evidence required in
  front of the ref move.
- [Protected branches](protected-branches.md) — the rule that makes this the
  only way onto the trunk.
- [The contract](../reference/the-contract.md#refs) — `UpdateRefs`, its
  outcomes, and `IsAncestor`.
