# Merge requests

Proposing a branch, showing what it changes, and landing it on the trunk.

## The problem

A merge request is a record that a branch wants to become part of another
one, plus a view of what that would do. Enroute holds neither. It holds the
commits and answers questions about them, so the record belongs to the
application, and the application assembles what the reviewer sees from a few
reads.

The one that goes wrong is the diff. The natural way to show a branch is to
compare it with the trunk, whose tip you already read, so you diff the branch
against that tip. The page renders. The hunks look like hunks. But everything
that landed on the trunk since the branch left it appears in that diff
*backwards*, as though the branch reverted work it has not caught up with. A
reviewer sees deletions nobody wrote and cannot tell them from the ones
somebody did, and the longer the branch is open the worse it reads. Nothing
errors, and the answer is wrong.

You have to read the change against the commit where the two histories last
agreed, and only the commit graph knows which one that is.

## The solution

Keep the request as a row, and merge by moving a ref, with the tip the
reviewer was shown as the guard.

```ts
import {
  RefUpdateOutcome_Rejection_Reason as Reason,
} from "./gen/enroute/api/v1alpha1/ref_pb";

async function merge(mr: MergeRequest, targetTip: string): Promise<Merged> {
  const { outcomes } = await refs.updateRefs({
    repo: mr.repo,
    updates: [{
      refname: mr.target,
      oldObjectId: { hex: targetTip },   // the tip the reviewer was shown
      newObjectId: { hex: mr.tip },
    }],
  });

  // The trunk moved while the page was open. Re-read it and ask again.
  if (outcomes[0].rejection?.reason === Reason.NON_FAST_FORWARD) {
    return { stale: true };
  }
  return { tip: mr.tip };
}
```

**Pass the tip the reviewer was shown.** If you leave `oldObjectId` unset, you
ask for a create, not an unguarded write, so a trunk that exists comes back
`Reason.ALREADY_EXISTS`. To set it is a compare-and-swap, and it is the only
thing between two reviewers who press Merge in the same second.

**The swap compares the old value, not ancestry.** `Reason.NON_FAST_FORWARD`
here means "the ref no longer holds `oldObjectId`". If the ref does hold it,
the move lands, whether or not `newObjectId` is a descendant. Enroute moves a
ref backwards when told to, and reports ancestry instead of imposing it. So
the ancestry check below is not a nicety for the reviewer. It is what stops a
merge of a stale branch from resetting the trunk.

**Close the front door in `preReceive`.** Refuse a direct push to the trunk,
per [protected-branches.md](protected-branches.md), and the merge request is
the only way in. `UpdateRefs` runs no `pre-receive`, because the caller there
is the application, so the rule that refuses every person pushing to the trunk
does not refuse this merge. There is no exemption to write, and no way for a
git client to borrow one.

## A push invites a request; it does not open one

Somebody pushes a branch many times before it is finished, so do not open a
request on every push. Use `postReceive` to tell the person how to ask, at the
one moment the application has their attention. It holds no connection to
them and will not have one again.

```ts
async function postReceive(
  req: PostReceiveRequest,
): Promise<PostReceiveResponse> {
  const name = await nameOf(req.repo.key);  // the key carries no name
  const pushed = req.commands
    .filter((c) => c.newObjectId && !isTrunk(c.refname))
    .map((c) => c.refname);

  const already = await openRequestsFor(req.repo, pushed);   // one query

  return {
    messages: pushed
      .filter((b) => !already.has(b))
      .map((b) =>
        `Open a merge request for ${short(b)}:\n` +
        `  curl -X POST ${FORGE_URL}/repos/${name}/merge-requests` +
        ` -d '{"branch": "${b}"}'\n`
      ),
  };
}
```

Git prints those lines to whoever pushed, above the `ok <ref>` lines.

Skip the trunk, which arrives here only when the first push creates it, and
skip a branch that already has an open request. Use one query for the whole
push and not one per ref: this runs while a person's terminal waits.

## Reading the change

Read against the merge base, never against the tip of the trunk. `diffCommit`
and `listCommits` both stream, so fold each one as it arrives.

```ts
async function show(mr: MergeRequest) {
  const { refs: tips } = await refs.listRefs({
    repo: mr.repo, prefixes: [mr.target],
  });
  const targetTip = tips[0].objectId!.hex;

  const bases = await objects.findMergeBases({
    repo: mr.repo,
    commitIdA: { hex: mr.tip },
    commitIdB: { hex: targetTip },
  });
  if (bases.exhausted) return { unknown: true };  // not "no shared history"
  const base = bases.baseCommitIds[0];

  const changes: FileChange[] = [];
  let truncated = false;
  for await (const page of objects.diffCommit({
    repo: mr.repo,
    commitId: { hex: mr.tip },
    baseCommitId: base,
  })) {
    changes.push(...page.changes);
    truncated ||= page.truncated;
  }

  return { base, changes, truncated };
}
```

`listCommits` walks first parents, which is the history the branch reads as.
It does not stop at the merge base on its own, so read pages until a commit id
matches `base`, and drop the rest. Check `truncated` on the diff: a set that
stopped at a server limit is only the first part of the answer.

`findMergeBases` answers with every base in `baseCommitIds`, because a
criss-cross history has more than one. One is the ordinary answer.
[rendering.md](rendering.md) is how those changes become a diff on a page.

## What to offer the reviewer

Two ancestry questions decide it.

```ts
// The branch already holds the trunk, so it fast-forwards. False: it is
// behind, and wants a rebase and another push.
const { isAncestor: ready } = await refs.isAncestor({
  repo: mr.repo,
  ancestorCommitId: { hex: targetTip },
  descendantCommitId: { hex: mr.tip },
});

// It already landed. Close the merge request.
const { isAncestor: landed } = await refs.isAncestor({
  repo: mr.repo,
  ancestorCommitId: { hex: mr.tip },
  descendantCommitId: { hex: targetTip },
});
```

`isAncestor` is also false when the repository does not know either commit.
Act on that answer the same way: this is not a history the repository can
vouch for.

## What it does not do

**Enroute makes no merge commit.** It cannot create a git object outside a
packfile, so these are the shapes on offer:

| Shape | How it works |
| --- | --- |
| Fast-forward | Require the branch to hold the trunk, then move the ref. Nothing to build. |
| Rebase, then fast-forward | The rewritten commits must be *pushed* first, by the contributor or by a worker with a checkout. Then it is the row above. |
| A true merge commit | A worker with a checkout builds it and pushes it over git. Merging it is then a ref move again. |

**The constraint decides the workflow.** Choose fast-forward-only even where a
merge commit is available. The trunk stays linear. What was tested is what
landed, because the commits on the trunk are the exact commits the evidence
was reported for, per [checks.md](checks.md). Whoever owns a branch that has
fallen behind rebases it and pushes again. Where the things pushing are
agents, which rewrite history constantly, this is the history they were going
to ask for anyway.

## See also

- [merge-queue.md](merge-queue.md): this recipe with the evidence required in
  front of the ref move.
- [protected-branches.md](protected-branches.md): the rule that makes this the
  only way onto the trunk.
- [rendering.md](rendering.md): turning `diffCommit` into a diff on a page.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
