# Chapter 5: Process pushes

This chapter implements the two hooks used during a push: one decides ref
updates and one handles updates that succeeded.

## The two halves of a push

Enroute stores the objects of a push before it asks anything, then calls
`pre_receive` while no ref has moved yet. Whatever you allow, it applies, and
then calls `post_receive` to say what landed.

| Call | When | Can it refuse? |
| --- | --- | --- |
| `pre_receive` | Objects stored, no ref moved | Yes, per ref, with a message |
| `post_receive` | Refs moved | No |

`pre_receive` applies ref-update policy. `post_receive` starts follow-up work.

## Judge every command

`pre_receive` carries `commands`, one per ref the push asks to change. Each has
a refname and two optional object IDs:

| `oldObjectId` | `newObjectId` | The command |
| --- | --- | --- |
| Unset | Set | Creates the ref |
| Set | Set | Moves the ref |
| Set | Unset | Deletes the ref |

An absent ID is an unset field, never 40 zeros and never an empty string.

Return one judgment per command, matched by refname. A missing judgment fails
the push, which prevents an incomplete policy response from allowing a ref.

Start by allowing everything, which is what having no `pre-receive` hook
installed has always meant. Create `lib/hooks/receive.ts`:

```ts
import { Judgement } from "@/lib/gen/enroute/hook/v1alpha1/hook";
import type {
  PreReceiveRequest, RefCommand,
} from "@/lib/gen/enroute/hook/v1alpha1/hook";

const allow = { judgement: Judgement.JUDGEMENT_ALLOW, reason: "" };
const refuse = (reason: string) => ({
  judgement: Judgement.JUDGEMENT_REFUSE,
  reason,
});

export async function preReceive(req: PreReceiveRequest) {
  const judgements = [];
  for (const command of req.commands) {
    judgements.push({ refname: command.refname, ...(await judge(req, command)) });
  }
  return { judgements };
}

async function judge(req: PreReceiveRequest, command: RefCommand) {
  return allow;
}
```

The handler returns a judgment for every command. A ref with no matching rule
is explicitly allowed. Replace the body of `judge` to apply policy; see
[Protected branches](../patterns/protected-branches.md).

## Say what landed

`post_receive` carries only the commands that landed. A push whose commands
were all refused sends no call at all.

Nothing here can be refused, and `messages` is the one way your app can speak
to the person at the terminal. Enroute sends those lines on the sideband, before
the `ok <ref>` lines, so "open a merge request at ..." comes from here and from
nowhere else: your app is not on the connection.

```ts
import type { PostReceiveRequest } from "@/lib/gen/enroute/hook/v1alpha1/hook";
import { repoById } from "@/lib/store";

export async function postReceive(req: PostReceiveRequest) {
  const repo = await repoById(req.repo!.key);
  if (!repo) return { messages: [] };

  const branches = req.commands
    .filter((c) => c.refname.startsWith("refs/heads/") && c.newObjectId)
    .map((c) => c.refname.slice("refs/heads/".length));

  const messages = branches
    .filter((branch) => branch !== "main")
    .flatMap((branch) => [
      "Open a merge request:",
      `  http://127.0.0.1:3000/r/${repo.path}/merge/new?from=${branch}`,
    ]);

  return { messages };
}
```

Add both to `dispatch`:

```ts
if (req.preReceive) {
  return HookResponse.fromPartial({ preReceive: await preReceive(req.preReceive) });
}
if (req.postReceive) {
  return HookResponse.fromPartial({ postReceive: await postReceive(req.postReceive) });
}
```

## What `post_receive` promises, and what it does not

This is where CI belongs. Your app learns that a branch has a new tip at the
moment it does, rather than by listing refs on a timer and comparing them with
what it saw last.

> **Important:** Delivery is best-effort and at most once. Enroute sends the
> call after the push has already succeeded, so it cannot retry without lying
> to a client that is gone. An app that was unreachable is simply not told.
> Treat this as latency, not as a guarantee: anything whose correctness depends
> on being told needs its own record, reconciled separately.

Enroute waits for your answer and ignores it, the way `receive-pack` waits for
`post-receive` and ignores its exit code. A failure here undoes nothing.

## Check your work

Push to the repository you created in chapter 2:

```sh
cd widgets
git commit --allow-empty -m "first"
git push origin main
```

The push lands. The message excludes `main`, so nothing is
printed yet.

Now push a branch:

```sh
git switch -c feature/checkout
git commit --allow-empty -m "start checkout"
git push origin feature/checkout
```

Your `post_receive` message appears before the `ok` line, which proves the
sideband works end to end:

```text
remote: Open a merge request:
remote:   http://127.0.0.1:3000/r/acme/widgets/merge/new?from=feature/checkout
To http://127.0.0.1:8080/acme/widgets.git
 * [new branch]      feature/checkout -> feature/checkout
```

Confirm the refs are really there:

```sh
git ls-remote http://ada:tok_ada@127.0.0.1:8080/acme/widgets.git
```

### Prove that it decides something

An app that returns `200` and judges nothing passes both preceding checks. Refuse
one ref so you can distinguish the two outcomes. Give `judge` a body:

```ts
async function judge(req: PreReceiveRequest, command: RefCommand) {
  if (command.refname === "refs/heads/main") {
    return refuse("main is protected: push a branch instead");
  }
  return allow;
}
```

`main` already exists, so this next push moves it rather than creating it:

```sh
git switch main
git commit --allow-empty -m "straight to main"
git push origin main
```

Git prints your reason, and the push does not land:

```text
 ! [remote rejected] main -> main (main is protected: push a branch instead)
error: failed to push some refs to 'http://127.0.0.1:8080/acme/widgets.git'
```

That is the whole mechanism. What belongs in `judge` — which refs, which
actors, what a force-push means, what a delete means — is policy, and
[Protected branches](../patterns/protected-branches.md) works it through.
Return `allow` for everything again before the next chapter, or keep the rule
and push branches instead of `main`.

## Result

- A `pre_receive` that judges every command it is sent, and can refuse one.
- A `post_receive` that speaks to whoever pushed.
- An app that is now a working Git host: clone, push, a message back, and a
  refusal that reaches the person who caused it.

You can now host Git. The next two chapters let people browse the repository.

Next: [Browse repositories](06-browse-repositories.md).
