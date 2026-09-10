# Protected branches

A branch that cannot be force-pushed, deleted, or written to directly by the
people who can otherwise push.

## The problem

A trunk is the branch everything else is measured against, so it is the one
branch nobody may rewrite under everybody else. Enroute will not
stop them. It refuses a push whose objects never arrived, a push whose refname
git would not accept, and a push whose `oldObjectId` no longer matches what
the ref holds. It refuses nothing else. A force-push over the trunk, a deleted
trunk, or a branch nobody reviewed each lands unless the application says
otherwise. Enroute holds no opinion about which refs may be rewritten.

The obvious hook checks whether the push is a force, and refuses it if it is.
That hook protects nothing. A delete carries no `newObjectId`, and a command
with only one id rewrites no history, so a delete arrives with `force` false
and passes the test. The branch nobody can rewrite is still one anybody can
remove, and the hook looks correct until somebody removes it.

## The solution

Test the delete before the force, and read the rule from a table instead of a
literal.

```ts
async function preReceive(req: PreReceiveRequest): Promise<PreReceiveResponse> {
  const judgements: RefJudgement[] = [];

  for (const c of req.commands) {
    judgements.push({ refname: c.refname, ...(await judge(req, c)) });
  }

  return { judgements };
}

const allow = { judgement: Judgement.ALLOW, reason: "" };
const refuse = (reason: string) => ({ judgement: Judgement.REFUSE, reason });

async function judge(req: PreReceiveRequest, c: RefCommand) {
  const rule = await protectionFor(req.repo, c.refname);  // your own table
  if (!rule) return allow;

  if (!c.newObjectId) {
    return refuse(`${c.refname} is protected and cannot be deleted`);
  }
  if (!c.oldObjectId) {
    return allow;                                 // a create. See below
  }
  if (c.force) {
    return refuse(`${c.refname} is protected: no force-push`);
  }
  if (rule.noDirectPush) {
    return refuse("push a branch and open a merge request");
  }
  return allow;
}
```

**A delete is not a force.** `force` is only ever true for a command that has
both ids, so a delete arrives with `force` false and no `newObjectId`. Test the
delete first, or whoever cannot rewrite a protected branch can still remove it.

**`force` means what git means.** It is true when `oldObjectId` is not an
ancestor of `newObjectId`, which is a rewrite. A ref that moves forward over a
stale value is a different thing, and Enroute refuses that before it asks the
hook.

**`reason` reaches the person.** Their git client prints it, the way it prints
the stderr of a hook. Name the ref and say what to do instead. That text is
all they get.

**Judge every command.** Enroute matches a judgement to a command by refname,
and a command you leave unjudged fails the whole push. That is what puts the
loop and the rule in two functions here: a judgement is pushed on every pass,
so a branch no rule covers is allowed out loud rather than by omission. Write
it as one loop with a `continue`, and a table that grows a row it does not
cover becomes a table that protects nothing.

**A refusal is per ref.** Enroute offers a git client no `atomic` capability,
so [one refused ref in a push of three does not hold back the other
two](../recipes.md). Say which ref you refused and why, in each refusal.

**Let the first push create it.** An unset `oldObjectId` is a create, and
somebody has to be able to create a protected branch that does not exist yet.
There is no history to overwrite and no evidence to bypass, and without the
exception a new repository could never get a trunk. After that create, the
rule applies to every push. The exception is a state that stops existing, not
an identity that keeps working.

**Do not exempt whatever lands the merges.** The strongest form of this rule
refuses *every* push to the trunk, from everybody, and it needs no exception:
a merge goes over the contract, which runs no `pre-receive`. A hook that
admits one privileged actor instead is two rules that have to agree, and the
trunk is then only as safe as the guarantee that no git client can claim that
name. [A rule you must exempt yourself from is at the wrong
door](../recipes.md).

To protect a namespace instead of a branch, use the same code with a different
`protectionFor`. `refs/tags/*` closed to rewrites, and `refs/heads/release/*`
closed to direct pushes, are two rows and not two hooks.

## Gate landing, not working

Refuse pushes to the trunk and let every other branch land freely. If you gate
feature branches, you ask a contributor to earn permission to *work*, when
what needs earning is permission to *land*. A push of a branch costs nothing
and blocks nobody.

Two things this hook must not do:

- **Do not consult the merge request.** A push to a branch under review is how
  somebody addresses feedback, not a violation.
- **Do not invalidate evidence.** Bind a check to a commit, per
  [checks.md](checks.md), and a branch that moved has no evidence at its new
  tip. Nothing has to be expired.

## What it does not do

**It does not keep the policy safe.** A rule a pusher can change by pushing is
not a rule. Everything a merge is gated on lives where the thing being gated
cannot reach it: the application's configuration, reviewed and deployed like
the rest of it, never a file in the branch under test. Read the rule through a
lookup instead of a literal, and an application that outgrows a map hands it a
query and changes nothing else.

**It does not hide anything.** A refused push still told the pusher the ref
exists. To hide a ref, see [private-repositories.md](private-repositories.md).

## See also

- [merge-requests.md](merge-requests.md): the door left open once this one
  closes the trunk.
- [checks.md](checks.md): what a branch has to show before it lands.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
