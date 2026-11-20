# Chapter 4: Authenticate Git clients

Enroute identifies repositories by opaque keys and does not manage users. This
chapter maps a Git URL to a repository and authenticates the caller.

At the end of this chapter, token-authenticated clients can clone a repository.

## What `authorize` is for

`authorize` runs once per Git request, before Enroute serves data. It carries
the request headers and path so the application can apply its credential and
path rules.

| Field | What it holds |
| --- | --- |
| `repoPath` | The path the client sent, at any depth, with `.git` if the client sent one |
| `headers` | The request headers as they arrived, credential included |
| `access` | `ACCESS_READ` for a fetch, `ACCESS_WRITE` for a push |

You answer `granted` with a repository key and an actor, or `denied` with a
reason. See [`authorize`](../reference/hooks.md#authorize) for every field.

## Read the credential

Git sends Basic authentication by default, with the token as the password and
a placeholder username that each client picks for itself. Accept both spellings
and ignore the username.

Create `lib/hooks/authorize.ts`:

```ts
import type { Header } from "@/lib/gen/enroute/hook/v1alpha1/hook";

export function tokenFrom(headers: Header[]): string | undefined {
  const auth = headers.find((h) => h.name.toLowerCase() === "authorization");
  if (!auth) return undefined;

  const bearer = auth.value.match(/^Bearer (.+)$/i);
  if (bearer) return bearer[1];

  const basic = auth.value.match(/^Basic (.+)$/i);
  if (!basic) return undefined;

  // Git needs some username before it sends a password, so the token is the
  // password and the username is whatever that client chose.
  const [, password] = Buffer.from(basic[1], "base64").toString().split(":", 2);
  return password || undefined;
}
```

## Answer the call

Add `authorize` to the same file:

```ts
import { Access, Denial } from "@/lib/gen/enroute/hook/v1alpha1/hook";
import type { AuthorizeRequest } from "@/lib/gen/enroute/hook/v1alpha1/hook";
import { repoByPath, userByToken } from "@/lib/store";

const CHALLENGE = {
  wwwAuthenticate: 'Basic realm="codehost"',
  // Git prints this as remote: lines. It needs the trailing newline.
  help: "Use a token as the password. See http://127.0.0.1:3000/settings/tokens\n",
};

export async function authorize(req: AuthorizeRequest) {
  // The .git suffix is a convention of the URL a person typed. The protocol
  // gives it no meaning, so serving both spellings as one repository is ours
  // to choose.
  const path = req.repoPath.replace(/\.git$/, "");

  const token = tokenFrom(req.headers);
  const user = token ? await userByToken(token) : undefined;
  if (!user) {
    return { denied: { denial: Denial.DENIAL_UNAUTHORIZED, challenge: CHALLENGE } };
  }

  // Look the path up exactly as your app does to render a page.
  const repo = await repoByPath(path);
  if (!repo) {
    return { denied: { denial: Denial.DENIAL_NOT_FOUND, challenge: undefined } };
  }

  if (req.access === Access.ACCESS_WRITE && repo.ownerId !== user.id) {
    return { denied: { denial: Denial.DENIAL_FORBIDDEN, challenge: undefined } };
  }

  return {
    granted: {
      repo: { key: repo.id },
      actor: user.id,
      context: new TextEncoder().encode(JSON.stringify({ userId: user.id })),
    },
  };
}
```

Then import it and dispatch to it, in `app/api/enroute/hooks/route.ts`:

```ts
import { authorize } from "@/lib/hooks/authorize";

async function dispatch(req: HookRequest): Promise<HookResponse> {
  if (req.authorize) {
    return HookResponse.fromPartial({ authorize: await authorize(req.authorize) });
  }
  return HookResponse.fromPartial({});
}
```

`authorize` is the only hook that chapter 1 enabled, so `dispatch` answers only
it. Enroute does not call the other three hooks, and each one takes its
default: every ref is visible, and every ref update is allowed. The empty
answer at the end is for a hook that is enabled but not implemented. Enroute
refuses the Git request for it.

## Authorization decisions

**Answer with your key, never the path.** `repo.id` is what you passed to
`CreateRepository`. A path is a name, and a name can be renamed; if the key
followed the name, a rename would silently point every push at a different
repository.

**`actor` is an ID, never a credential and never an email address.** Enroute
records it on the request's traces and in usage data, so it must be safe to
keep. It also comes from *your* lookup rather than from the request, because
the Git client is the one party that must not be trusted about who it is.
Enroute hands this same actor to every later hook of the request.

**Check the credential before the path.** This example answers `401` to an
anonymous caller before it looks the path up, so a caller without a token
cannot learn which repositories exist. To hide private repositories from
signed-in users as well, see
[Private repositories](../patterns/private-repositories.md).

**A `401` without a challenge is a dead end.** Git's credential handling —
netrc, credential helpers, a prompted retry — engages only from the
`WWW-Authenticate` value. Without it the person behind the client is told no
and given no way to fix it.

## The context field

`Granted.context` is up to 8 KiB of your own bytes. Enroute never parses or
logs it, and hands it back byte for byte on every later hook of the same Git
request.

It is captured now and replayed when the hooks run, which on a large push is
minutes later. Anything that can change in between should be checked again
rather than trusted from here. An empty context is fine, and common: later
chapters can look everything up from `repo` and `actor`.

## Check your work

A clone with a token succeeds:

```sh
git clone http://ada:tok_ada@127.0.0.1:8080/acme/widgets.git
```

The repository is empty, so Git warns that you cloned nothing. That warning is
success: `authorize` granted, and Enroute served the repository your key named.

A clone with no credential asks for one, rather than failing outright:

```sh
git clone http://127.0.0.1:8080/acme/widgets.git
```

A clone of a path your app does not know is a `404`:

```sh
git clone http://ada:tok_ada@127.0.0.1:8080/acme/nothing.git
```

A clone by somebody who is not the owner succeeds:

```sh
git clone http://linus:tok_linus@127.0.0.1:8080/acme/widgets.git
```

Reading is allowed for everyone here. Only writing checks the owner, so a push
from this clone is refused.

## Result

- A route that turns a Git URL into one of your repository keys.
- Authentication from a token, with a challenge that lets Git ask for one.
- An actor that every later hook will carry.

Git can now read, and the owner can push. No push policy applies yet, because
`pre_receive` and `post_receive` are not enabled. Chapter 5 adds them.

Next: [Process pushes](05-process-pushes.md).
