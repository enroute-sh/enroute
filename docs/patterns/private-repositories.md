# Private repositories

Private repository rules control who can reach a repository and which refs they
can discover.

[Chapter 4](../build/04-authenticate-git-clients.md) granted any user with a token and
checked the owner only for writes. This makes a repository private, and hides
individual refs inside a repository somebody may otherwise read.

## Use this pattern when

Enroute knows a repository by the key you gave it. It has no users, no teams,
no permissions, and no notion that a repository might be private, so every one
of those questions comes back to your app. Twice, because they are two
questions. The first is whether the request may happen at all. The second is
what the caller may be *told about*: somebody can be entitled to clone a
repository and still have no business knowing that a particular branch exists.

One common response to the first question is `404`
when the repository does not exist, `403` when it exists but is not yours.
Each answer is correct on its own, and together they are a directory listing.
Anyone can send a list of guesses and sort the answers: a refusal that says
*forbidden* has confirmed the name, and a refusal that says *not found* has
ruled it out. Together, those responses let a caller enumerate private
repository paths.

## Implement the pattern

Answer the same denial for "no such repository" and "not yours", and separate
authentication from authorization by which denial you send.

Give the schema somewhere to hold it, in `lib/db/schema.ts`:

```ts
export const repos = pgTable("repos", {
  id: text("id").primaryKey(),
  path: text("path").notNull().unique(),
  ownerId: text("owner_id").notNull().references(() => users.id),
  isPrivate: boolean("is_private").notNull().default(false),
  createdAt: timestamp("created_at").notNull().defaultNow(),
});

export const members = pgTable("members", {
  repoId: text("repo_id").notNull().references(() => repos.id),
  userId: text("user_id").notNull().references(() => users.id),
  canWrite: boolean("can_write").notNull().default(false),
});
```

Then replace `authorize` in `lib/hooks/authorize.ts`:

```ts
import { Access, Denial } from "@/lib/gen/enroute/hook/v1alpha1/hook";
import { membership, repoByPath, userByToken } from "@/lib/store";

const CHALLENGE = {
  wwwAuthenticate: 'Basic realm="codehost"',
  help: "Create a token at https://example.com/settings/tokens\n",
};

export async function authorize(req: AuthorizeRequest) {
  const token = tokenFrom(req.headers);
  const user = token ? await userByToken(token) : undefined;
  if (!user) {
    return { denied: { denial: Denial.DENIAL_UNAUTHORIZED, challenge: CHALLENGE } };
  }

  const repo = await repoByPath(req.repoPath.replace(/\.git$/, ""));
  const member = repo ? await membership(repo.id, user.id) : undefined;

  // One answer for "no such repository" and "not yours".
  const mayRead = repo && (!repo.isPrivate || repo.ownerId === user.id || member);
  if (!mayRead) {
    return { denied: { denial: Denial.DENIAL_NOT_FOUND, challenge: undefined } };
  }

  const mayWrite = repo.ownerId === user.id || member?.canWrite;
  if (req.access === Access.ACCESS_WRITE && !mayWrite) {
    return { denied: { denial: Denial.DENIAL_FORBIDDEN, challenge: undefined } };
  }

  return {
    granted: {
      repo: { key: repo.id },
      actor: user.id,
      context: new TextEncoder().encode(
        JSON.stringify({ userId: user.id, mayWrite: Boolean(mayWrite) }),
      ),
    },
  };
}
```

**Check permission before existence.** One `DENIAL_NOT_FOUND` for both "no
such repository" and "not yours" is what stops a caller with no credential
from learning which private repositories exist. `DENIAL_FORBIDDEN` is then
safe for the write check, because a caller that reached it has already proved
it may read.

**Set `challenge` only on `DENIAL_UNAUTHORIZED`, and always there.** Git's
credential handling starts from `wwwAuthenticate`, so a `401` without one
leaves the person behind the client with no way forward. See
[`Challenge`](../reference/hooks.md#challenge).

**Answer with the key, not the path.** `repo.id` is what `CreateRepository`
was given. The path is what the URL carries, and a rename moves it.

### Control which refs the caller can discover

Enroute asks `visible_refs` before every ref advertisement, with the refs it
would otherwise send. Chapter 5 left this answering with everything.

```ts
export async function visibleRefs(req: VisibleRefsRequest) {
  const { mayWrite } = JSON.parse(new TextDecoder().decode(req.context));

  return {
    refnames: req.refnames.filter((refname) => {
      // A draft namespace only its author and writers are told about.
      if (refname.startsWith("refs/heads/draft/")) {
        return mayWrite || refname.startsWith(`refs/heads/draft/${req.actor}/`);
      }
      return true;
    }),
  };
}
```

`access` says whether this advertisement is for a fetch or a push: what a
caller may read and what it may be told is writable are two questions. A name
you drop is hidden. A name you add that was not sent is ignored. `HEAD` is not
in the list, and it is hidden automatically when what it points at is.

**The context is a snapshot.** Enroute captures it at `authorize` and replays
it on the later calls, which on a large push is minutes later. In
`pre_receive`, re-read anything that may have been revoked since.

## Scope

**It hides refs, not objects.** A client that knows the object ID of a hidden
tip can still fetch it, as under Git's `uploadpack.hideRefs`. Anything whose
secrecy must survive a guessed object ID belongs in a repository of its own.

**It does not decide what a token is.** Enroute hands over the headers as they
arrived and reads none of them. Sessions, expiry, and rotation are yours.

## See also

- [Deploy keys](deploy-keys.md) — the same `authorize`, answering for
  something that is not a person.
- [Repository lifecycle](repository-lifecycle.md) — the table `repoByPath`
  reads, and where its IDs come from.
- [`visible_refs`](../reference/hooks.md#visible_refs) — every field, with
  payloads.
