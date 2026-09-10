# Deploy keys and bot actors

A credential that clones one repository and nothing else, held by something
that is not a person.

## The problem

A build server, a release bot, or an agent needs to clone one repository.
Enroute asks one question about it, who is calling, and takes one answer. It
has no notion of a machine account, no credential scoped to a repository, and
no read-only flag. Every one of those belongs to the application. To Enroute,
the build server is the same kind of caller as a person with a browser
session.

The obvious answer gives the machine a token that belongs to whoever set it
up. That token reaches every repository its owner reaches, so a compromised
runner is a compromised account. It stops working the day that person leaves,
which is a build break nobody predicts. And Enroute records every push it
makes against that person, so the one question an audit asks, who did this,
has the wrong answer for as long as the record is kept.

## The solution

Look the token up in the deploy-key table before the session table, and grant
against the one repository the key names.

```ts
const deny = (denial: Denial): AuthorizeResponse => ({
  outcome: { case: "denied", value: { denial } },
});

async function authorize(req: AuthorizeRequest): Promise<AuthorizeResponse> {
  const token = bearerOrBasicPassword(req.headers);

  const key = await deployKeyByToken(token);
  if (!key) return authorizeSession(req, token);   // a person's, as elsewhere

  const row = await repoByName(req.repoPath.replace(/\.git$/, ""));
  if (!row || row.id !== key.repoId) return deny(Denial.NOT_FOUND);
  if (req.access === Access.WRITE && key.readOnly) {
    return deny(Denial.FORBIDDEN);
  }

  return {
    outcome: {
      case: "granted",
      value: {
        repo: { key: row.id },
        actor: `deploy-key:${key.id}`,
        context: new TextEncoder().encode(JSON.stringify({ key: key.id })),
      },
    },
  };
}
```

**Answer `NOT_FOUND` for a key pointed at the wrong repository.** A key that
is valid elsewhere must not be able to test which repositories exist here, so
it gets the same answer a stranger gets.

**Name the actor for what it is.** `deploy-key:42` and `bot:release` are
actors in [the application's namespace](../recipes.md), as a user id is.
Enroute records the actor on spans and against what the request cost, so the
traffic of a machine is separable from the traffic of a person. Never put the
token there.

**`context` is bytes, and yours.** Enroute plays it back on the later hooks
and never parses or logs it, so what goes in belongs to the application: JSON
here, and anything at all in practice. The limit is 8 KiB.

**An agent acting for a person is two facts.** `actor` is one id and Enroute
never splits it, so the second fact goes in `context`. An auth model belongs
to the application, and no field Enroute designed could hold every shape of
one.

## See also

- [private-repositories.md](private-repositories.md): the `authorize` this
  one branches off, and per-caller ref visibility.
- [mirroring.md](mirroring.md): the credential in the other direction, for a
  push out to somebody else's host.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
