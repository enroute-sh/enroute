# Chapter 3: Handle hooks

Enroute asks your app a question every time a Git client does something. This
chapter builds the route that receives those questions and proves they came
from Enroute.

At the end of this chapter your app accepts a signed hook and rejects an
unsigned one. It does not answer anything yet.

## One route, four calls

Enroute sends every hook as a `POST` to the URL you configured in chapter 1.
The body is a binary `HookRequest`, and `HookRequest.call` says which of the
four calls it is.

| Part | Value |
| --- | --- |
| Method | `POST` |
| `Content-Type` | `application/x-protobuf` |
| Request body | A binary `HookRequest` |
| Success response | `200` with a binary `HookResponse` |
| Timeout | 10 seconds by default |

The body selects the call, not the URL, so you mount one route rather than
four. A call added to the contract later then asks you to mount nothing new.

## Read the raw bytes

The signature covers exactly the bytes that arrived. Anything that parses,
re-encodes, or normalises the body first breaks every call.

In a Next.js route handler, `request.arrayBuffer()` gives you those bytes.
Read the body once and use the same bytes for the digest check and the
protobuf decode.

Create `app/api/enroute/hooks/route.ts`:

```ts
import { HookRequest, HookResponse } from "@/lib/gen/enroute/hook/v1alpha1/hook";
import { verifyHook } from "@/lib/hooks/signature";

// grpc-js needs Node, and so does the signature check.
export const runtime = "nodejs";

export async function POST(request: Request) {
  const body = new Uint8Array(await request.arrayBuffer());

  if (!verifyHook(request.headers, body)) {
    return new Response(null, { status: 401 });
  }

  const req = HookRequest.decode(body);
  const res = await dispatch(req);

  return new Response(HookResponse.encode(res).finish(), {
    headers: { "content-type": "application/x-protobuf" },
  });
}

async function dispatch(req: HookRequest): Promise<HookResponse> {
  // Chapters 4 and 5 fill this in. Answering nothing is deliberate: it means
  // "not implemented", and Enroute refuses the Git request rather than guess.
  return HookResponse.fromPartial({});
}
```

Four rules govern every answer this route will ever give:

- **Answer the call that was asked.** Set the `HookResponse.answer` member that
  matches the `HookRequest.call` member. An empty answer means "I do not
  implement this", and Enroute refuses the Git request rather than proceed on
  an answer nobody gave.
- **Deny with `200`.** A refusal is a field inside the answer. Any other
  status, an unreadable body, or a timeout is a *fault*, and Enroute fails the
  Git request closed. It never reads a fault as a refusal.
- **Answer within the timeout.** The default of 10 seconds allows a cold
  serverless start.
- **Never trust the request for identity.** The Git client is the one party you
  must not believe about who it is.

## Verify the signature

Enroute signs each hook with an Ed25519 HTTP Message Signature
([RFC 9421](https://www.rfc-editor.org/rfc/rfc9421.html)). Do not implement the
signature base by hand: use a library, and spend your attention on the two
checks a library does not do for you.

```sh
npm install http-message-signatures
```

Create `lib/hooks/signature.ts`:

```ts
import { createHash, createPublicKey, verify as verifyEd25519 } from "node:crypto";
import { httpbis } from "http-message-signatures";

// The public half of the key the deployment signs with. Ask the operator for
// it. This value pairs with the RFC 9421 test key the local stack uses.
const PUBLIC_KEY = createPublicKey(
  process.env.ENROUTE_HOOK_PUBLIC_KEY ??
    `-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAJrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=
-----END PUBLIC KEY-----`,
);

// Exactly the hook_endpoint_url configured for the tenant.
const ENDPOINT =
  process.env.ENROUTE_HOOK_URL ??
  "http://host.docker.internal:3000/api/enroute/hooks";

const KEYID = process.env.ENROUTE_HOOK_KEYID ?? "poqkLGiymh_W0uP6PZFw-dvez3QJT5SolqXBCW38r0U";
const MAX_SKEW_SECONDS = 300;

const key = {
  id: KEYID,
  algs: ["ed25519"],
  verify: async (data: Buffer, signature: Buffer) =>
    verifyEd25519(null, data, PUBLIC_KEY, signature),
};

export async function verifyHook(headers: Headers, body: Uint8Array) {
  // The signature covers the content-digest *header*, not the bytes. Without
  // this line the header can describe a body that never arrived.
  const digest = `sha-256=:${createHash("sha256").update(body).digest("base64")}:`;
  if (headers.get("content-digest") !== digest) return false;

  try {
    return await httpbis.verifyMessage(
      {
        keyLookup: async ({ keyid }) => (keyid === KEYID ? key : null),
        // Reject a created time more than five minutes out, either way.
        requiredParams: ["created"],
        maxAge: MAX_SKEW_SECONDS,
        tolerance: MAX_SKEW_SECONDS,
      },
      // The URL you registered, never the one that arrived.
      { method: "POST", url: ENDPOINT, headers: Object.fromEntries(headers) },
    );
  } catch {
    return false;
  }
}
```

Two things in there are load-bearing.

**Check the digest against the body yourself.** The signature covers the
`content-digest` header. It says that header was not altered; it says nothing
about the bytes that arrived. This library's request type has no body field at
all, so it cannot make this check for you, and without it somebody can replace
the body and leave the header and signature untouched. It is the one part of
verification no library will do.

`Content-Digest` is its own specification, [RFC 9530], which is why a signature
library has no opinion about it. Enroute sends a single `sha-256` entry, so
comparing the whole header value works. The field is a dictionary and may carry
several algorithms in principle; if that ever happens this comparison starts
refusing valid requests rather than accepting bad ones, which is the direction
to be wrong in.

[RFC 9530]: https://www.rfc-editor.org/rfc/rfc9530.html

**Verify against the URL you registered**, not the `Host` header and path that
arrived. A proxy rewrites those, and Enroute signed the string you configured
in chapter 1. This is why `url` is passed explicitly rather than taken from the
request. If the two differ by one byte, every call is a `401` and nothing says
why.

> **Note:** `@authority` includes a port only when the URL names one. Enroute
> signs over the configured URL, so `host.docker.internal:3000` carries its
> port and a public `https://example.com` does not.

Before you trust any library, run the
[test vector](../reference/verify-the-signature.md#test-vector) through it. That page also
gives the signature base in full, for a language with no library to reach for.

## Check your work

An unsigned request is refused:

```sh
curl -i -X POST --data-binary '' http://127.0.0.1:3000/api/enroute/hooks
```

It answers `401`. An endpoint that answers anything else to an unsigned request
passes every remaining check in this guide and is unsafe in production.

A signed request gets through. Enroute sends one as soon as a Git client asks
for anything:

```sh
git clone http://127.0.0.1:8080/acme/widgets.git
```

The clone still fails, because `dispatch` answers nothing. That is the right
failure: Enroute asked, your app did not answer, and Enroute refused rather
than guess. Your dev server log shows the request arriving and passing
verification.

If instead you see a `401` in the log, the signature check failed. The cause is
almost always that `ENROUTE_HOOK_URL` and `hook_endpoint_url` disagree.

## Result

- One route that receives every hook.
- A verifier that proves a request came from Enroute, checked against the URL
  you registered rather than the one that arrived.

Next: [Authenticate Git clients](04-authenticate-git-clients.md), where the clone starts
working.
