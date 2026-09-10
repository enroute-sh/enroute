# Deploy keys and bot actors

Deploy keys are credentials that let an automated system clone one repository
and nothing else.

This branches off the `authorize` from [chapter
4](../build/04-authenticate-git-clients.md), before the user lookup.

## Use this pattern when

A build server, a release bot, or an agent needs to clone one repository.
Enroute asks one question about it — who is calling — and takes one answer. It
has no notion of a machine account, no credential scoped to a repository, and
no read-only flag. Every one of those is yours. To Enroute, the build server
is the same kind of caller as a person with a browser session.

One approach gives the machine a token that belongs to whoever set it
up. That token reaches every repository its owner reaches, so a compromised
runner is a compromised account. It stops working the day that person leaves,
which is a build break nobody predicts. And Enroute records every push it
makes against that person, so the one question an audit asks — who did this —
has the wrong answer for as long as the record is kept.

## Implement the pattern

Give machine credentials a table of their own:

```ts
export const deployKeys = pgTable("deploy_keys", {
  id: text("id").primaryKey(),
  repoId: text("repo_id").notNull().references(() => repos.id),
  token: text("token").notNull().unique(),
  readOnly: boolean("read_only").notNull().default(true),
});
```

Look the token up there *before* the users table, and grant against the one
repository the key names:

```ts
import { deployKeyByToken, repoByPath, userByToken } from "@/lib/store";

export async function authorize(req: AuthorizeRequest) {
  const token = tokenFrom(req.headers);
  if (!token) {
    return { denied: { denial: Denial.DENIAL_UNAUTHORIZED, challenge: CHALLENGE } };
  }

  const key = await deployKeyByToken(token);
  if (!key) return authorizeUser(req, token);        // a person's, as in chapter 4

  const repo = await repoByPath(req.repoPath.replace(/\.git$/, ""));
  if (!repo || repo.id !== key.repoId) {
    return { denied: { denial: Denial.DENIAL_NOT_FOUND, challenge: undefined } };
  }
  if (req.access === Access.ACCESS_WRITE && key.readOnly) {
    return { denied: { denial: Denial.DENIAL_FORBIDDEN, challenge: undefined } };
  }

  return {
    granted: {
      repo: { key: repo.id },
      actor: `deploy-key:${key.id}`,
      context: new TextEncoder().encode(JSON.stringify({ deployKey: key.id })),
    },
  };
}
```

**Answer `DENIAL_NOT_FOUND` for a key pointed at the wrong repository.** A key
that is valid elsewhere must not be able to test which repositories exist
here, so it gets the same answer a stranger gets.

**Name the actor for what it is.** `deploy-key:42` and `bot:release` are
actors in your namespace, exactly as a user ID is. Enroute records the actor
on spans and against what the request cost, so the traffic of a machine is
separable from the traffic of a person. Never put the token there.

**`context` is bytes, and yours.** Enroute replays it on the later hooks and
never parses or logs it, so what goes in is your business: JSON here, and
anything at all in practice. The limit is 8 KiB. See
[`Granted`](../reference/hooks.md#granted).

**An agent acting for a person is two facts.** `actor` is one string and
Enroute never splits it, so the second fact goes in `context`. An auth model
is yours, and no field Enroute designed could hold every shape of one.

## See also

- [Private repositories](private-repositories.md) — the `authorize` this one
  branches off, and per-caller ref visibility.
- [Mirroring](mirroring.md) — the credential in the other direction, for a
  push out to somebody else's host.
- [`authorize`](../reference/hooks.md#authorize) — every field, with payloads.
