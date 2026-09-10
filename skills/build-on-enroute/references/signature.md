# Verifying the signature

Enroute signs every endpoint call with Ed25519, as an
[RFC 9421 HTTP Message Signature](https://www.rfc-editor.org/rfc/rfc9421.html).
The user's application holds only the public half, so it can check a call and
cannot make one.

Write this before the four calls, and test it against the vector at the
bottom before you serve anything.

## What arrives

Three headers:

```
content-digest: sha-256=:<base64 of the SHA-256 of the body>:
signature-input: sig1=("@method" "@authority" "@path" "content-digest");created=<unix seconds>;keyid="<thumbprint>"
signature: sig1=:<base64 of 64 signature bytes>:
```

The label is `sig1` here, but any label is legal. Read whichever one arrives.

`Content-Digest` is the field from
[RFC 9530](https://www.rfc-editor.org/rfc/rfc9530.html), the companion to
9421. The colons around the value are syntax: they delimit a byte sequence in
a structured field. The value to compare is the whole of `sha-256=:<base64>:`,
colons included. A verifier that strips them compares the wrong string.

## Verify in this order

Refuse the call at the first step that fails, and answer `401`.

1. **Read `Signature-Input`.** It is `<label>=<parameters>`. Split on the
   first `=`. The parameters begin with the component list in parentheses,
   then `;name=value` pairs.

2. **Check that the component list is exactly**
   `("@method" "@authority" "@path" "content-digest")`. Refuse any other list.
   Do not verify whatever list the caller names. If you trust that list, the
   caller can drop `content-digest` and sign nothing about the body.

3. **Check `created`.** Refuse it if it is more than **300 seconds** from now,
   in either direction.

4. **Find the key by `keyid`.** Ignore any `alg` parameter. The algorithm
   comes from the key, never from the message. If the caller chooses `alg`,
   the caller can reduce the scheme to one it can forge.

5. **Check the digest.** Compute the SHA-256 of the raw body, base64 it, and
   build `sha-256=:<that>:`. Compare it to the `Content-Digest` header. Refuse
   if they differ. Never trust the digest the header carries.

6. **Build the signature base.** Below.

7. **Verify.** Base64-decode the value between the colons in `Signature`, and
   verify those 64 bytes over the base with the Ed25519 public key.

## The signature base

One line per covered component, then the parameters. Each line is the
component name in double quotes, then `": "`, then the value, then `\n`. The
last line has **no trailing newline**.

```
"@method": POST
"@authority": forge.example.com
"@path": /enroute/hooks
"content-digest": sha-256=:LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=:
"@signature-params": ("@method" "@authority" "@path" "content-digest");created=1700000000;keyid="poqkLGiymh_W0uP6PZFw-dvez3QJT5SolqXBCW38r0U"
```

`@signature-params` is everything after the label in `Signature-Input`,
copied through unchanged. Build it from what arrived and not from a template.
A parameter string you format again is a different base.

`content-digest` is the header value as it arrived.

### Take the authority and path from configuration

`@authority` and `@path` come from the URL registered for the tenant, **not**
from the arriving `Host` header and request path. A proxy rewrites both, and
Enroute signs over what it was configured with.

`@authority` carries a port only when the URL does: `forge.example.com` for
port 443 over https, `localhost:3000` for a local endpoint.

Configure the endpoint with its own public URL, and check that it is the same
string the tenant was registered with. One byte of difference makes every call
a `401` with no reason given. That is the most common failure in this
integration.

## The public key

Get it from whoever operates Enroute. For the local stack it is the published
test key of RFC 9421:

```
-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAJrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=
-----END PUBLIC KEY-----
```

Its `keyid` is `poqkLGiymh_W0uP6PZFw-dvez3QJT5SolqXBCW38r0U`.

`keyid` is the [RFC 7638](https://www.rfc-editor.org/rfc/rfc7638.html) JWK
thumbprint of the key: the base64url, unpadded, SHA-256 of
`{"crv":"Ed25519","kty":"OKP","x":"<base64url of the 32 raw bytes>"}` with the
members in that order and no whitespace. It is derived, not configured. To
rotate a key you hold two of them, and nothing has to agree on a name first.

Hold a **list** of keys and select by `keyid`. That is what makes a rotation
possible with no outage.

## Whether to use a library

Write it yourself. A SHA-256, an Ed25519 verify, and the string above are the
whole of it. The per-language notes below are the entire crypto.

RFC 9421 libraries exist. As of **August 2026** none of them is mature: across
Node, Python, Go, Rust, and Java, every one is pre-1.0, written against a
draft rather than the published RFC, or years without a commit. Check again
if you read this much later. The lasting reason is different: three of the
seven steps above stay yours with any library, and each of the three is a way
to pass every test while verifying nothing:

- **The component list.** A library verifies what the message says it covers.
  Step 2 refuses every list but the expected one, which is what stops a caller
  dropping `content-digest`.
- **The digest.** The signature covers the `Content-Digest` *header*, never
  the bytes, so something still has to hash the raw body and compare.
- **The authority and the path.** A library takes these off the request
  object, which is the `Host` and path a proxy just rewrote. They come from
  configuration instead.

A library that did all three would still leave the smaller half of the job to
you.

## Per-language notes

**Node.** `crypto.createPublicKey(pem)` reads the SPKI PEM directly.
`crypto.verify(null, Buffer.from(base), key, sig)` verifies Ed25519. The
algorithm argument is `null`, because the key carries it.

**Python.** `cryptography`: `serialization.load_pem_public_key(pem)` gives an
`Ed25519PublicKey`, and `key.verify(sig, base.encode())` raises
`InvalidSignature` on a mismatch.

**Go.** `x509.ParsePKIXPublicKey` on the bytes of the PEM block gives an
`ed25519.PublicKey`, and `ed25519.Verify(key, base, sig)` returns a bool.

**Rust.** `ed25519-dalek` with `DecodePublicKey::from_public_key_pem`.
[`crates/api/signature`] in the Enroute repository is the reference
implementation of both halves, and the stub in `dev/e2e/src/hooks.rs` is a
verifier to read.

**Java.** JDK 15 and later: `Signature.getInstance("Ed25519")`, with the key
read by `KeyFactory.getInstance("Ed25519")` from an `X509EncodedKeySpec`.

**C#.** The base class library has no Ed25519. Use `NSec.Cryptography` or
BouncyCastle.

## Test vector

Check the verifier against this before you serve anything. Enroute's signing
code generated it.

| Part      | Value                                |
| --------- | ------------------------------------ |
| Key       | RFC 9421's `test-key-ed25519`, above |
| Method    | `POST`                               |
| Authority | `forge.example.com`                  |
| Path      | `/enroute/hooks`                     |
| Body      | The five ASCII bytes `hello`         |
| `created` | `1700000000`                         |

The base is the one shown above, and the headers are:

```
content-digest: sha-256=:LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=:
signature-input: sig1=("@method" "@authority" "@path" "content-digest");created=1700000000;keyid="poqkLGiymh_W0uP6PZFw-dvez3QJT5SolqXBCW38r0U"
signature: sig1=:rZK1LaKQdmo1Sq9YvFhmnooiI81NeGj9dSHXwST7R7bVCP5QUJar7zjc1gRqFKLWzxeCj1syqPMmJGFyszwcAw==:
```

Test three things, not one:

1. The vector verifies.
2. A changed body fails, at the digest step.
3. A `created` of `0` fails, at the skew step.

The skew check needs the clock, so pass the current time into the verifier
instead of reading it inside. That is what makes the vector testable.

[`crates/api/signature`]: https://github.com/enroute-sh/enroute/tree/release/crates/api/signature
