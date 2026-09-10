# Private repositories

Who may reach a repository at all, and which of its refs they are told about.

## The problem

Enroute knows a repository by the key the application gave it. It has no
users, no teams, no permissions, and no notion that a repository might be
private, so every one of those questions comes back to the application.
Twice, because they are two questions. The first is whether the request may
happen at all. The second is what the caller may be told about: a caller can
be entitled to clone a repository and still have no business knowing that a
particular branch exists.

The obvious answer to the first is the one every HTTP tutorial teaches: `404`
when the repository does not exist, `403` when it exists but is not yours.
Each answer is correct on its own, and together they are a directory listing.
Anyone can send a list of guesses and sort the answers: a refusal that says
*forbidden* has confirmed the name, and a refusal that says *not found* has
ruled it out. Nothing is breached and nothing looks wrong. The private
repositories announce which of them are real, one probe at a time.

## The solution

Answer the same denial for "no such repository" and "not yours", and separate
authentication from authorization by which denial you send.

```ts
const deny = (denial: Denial, challenge?: Challenge): AuthorizeResponse => ({
  outcome: { case: "denied", value: { denial, challenge } },
});

async function authorize(req: AuthorizeRequest): Promise<AuthorizeResponse> {
  const token = bearerOrBasicPassword(req.headers);

  const caller = await lookupSession(token);
  if (!caller) {
    return deny(Denial.UNAUTHORIZED, {
      wwwAuthenticate: 'Basic realm="forge"',
      help: "Create a token at https://forge.example.com/tokens\n",
    });
  }

  // `.git` is a convention of the client and means nothing in the protocol.
  // Both spellings arrive, and this forge serves them as one repository.
  const row = await repoByName(req.repoPath.replace(/\.git$/, ""));
  if (!row || !canRead(caller, row)) return deny(Denial.NOT_FOUND);
  if (req.access === Access.WRITE && !canWrite(caller, row)) {
    return deny(Denial.FORBIDDEN);
  }

  return {
    outcome: {
      case: "granted",
      value: {
        repo: { key: row.id },                 // the row's id is the key
        actor: caller.id,
        context: encodeContext({ session: caller.id, perms: caller.perms }),
      },
    },
  };
}
```

**Check permission before existence.** One `Denial.NOT_FOUND` for both "no
such repository" and "not yours" is what stops a caller with no credential
from learning which private repositories exist. `Denial.FORBIDDEN` is then
safe for the write check, because a caller that reached it has already proved
it may read.

**Set `challenge` only on `UNAUTHORIZED`, and always there.** Git's credential
handling (netrc, a credential helper, a prompted retry) starts from the
`challenge.wwwAuthenticate` value, so a `401` without one leaves the person
behind the client with no way forward. `challenge.help` reaches them as
`remote:` lines, and needs its trailing newline.

**Accept Basic as well as Bearer.** Git sends Basic by default. Read the
password as the token and ignore the username. Git needs *some* username
before it sends a password, and each client picks its own placeholder.

**Answer with the key, not the name.** `repo` is the id the row holds, which
is [what `createRepository` was given](../recipes.md). The name is what the
URL carries, and a rename moves it.

## Which refs the caller is told about

Enroute asks `visibleRefs` before every ref advertisement, with the refs it
would otherwise send.

```ts
async function visibleRefs(
  req: VisibleRefsRequest,
): Promise<VisibleRefsResponse> {
  const perms = decodeContext(req.context);
  return { refnames: req.refnames.filter((r) => maySee(perms, r, req.access)) };
}
```

`access` says whether this advertisement is for a fetch or a push. What a
caller may read and what it may be told is writable are two questions. A name
you drop is hidden. A name you add that was not sent is ignored. `HEAD` is not
in the list, and it hides itself when what it points at is hidden.

**The context is a snapshot.** Enroute captures it at `authorize` and plays it
back on the later calls, which on a large push is minutes later. In
`preReceive`, check again anything that may have been revoked since.

## What it does not do

**It hides refs, not objects.** A client that knows the object id of a hidden
tip can still fetch it, as under git's `uploadpack.hideRefs`. Anything whose
secrecy must survive a guessed object id belongs in a repository of its own.

**It does not decide what a token is.** Enroute hands over the headers as they
arrived and reads none of them. Sessions, expiry, and rotation belong to the
application.

## See also

- [deploy-keys.md](deploy-keys.md): the same `authorize`, answering for
  something that is not a person.
- [repository-lifecycle.md](repository-lifecycle.md): the table `repoByName`
  reads, and where its ids come from.
- [../recipes.md](../recipes.md): the rules every recipe here takes as read.
