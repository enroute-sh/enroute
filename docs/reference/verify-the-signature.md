# Verify hook signatures

Enroute signs hooks with Ed25519 HTTP Message Signatures (RFC 9421). Obtain
the deployment public key and verify a request before reading its body.

## Procedure

1. Parse `Signature-Input`. Enroute covers `@method`, `@authority`, `@path`,
   and `content-digest`.
2. Reject a `created` timestamp more than 300 seconds from the current time.
3. Select the public key by `keyid`; the key determines the algorithm.
4. Compute the body SHA-256 and compare it with the `Content-Digest` SHA-256
   entry (RFC 9530).
5. Build the RFC 9421 signature base and verify `Signature` with Ed25519.

Use `@authority` and `@path` from the configured `hook_endpoint_url`, not the
URL as delivered by a proxy. The configured authority includes a port only when
the URL includes one.

`keyid` is the RFC 7638 JWK thumbprint of the public key. During rotation,
accept both keys until all signers use the replacement.

Use an RFC 9421 library to parse and verify the signature, but independently
check `Content-Digest` against the received bytes. The signature covers the
digest header; a signature library does not validate the body for you.

## Test vector

The local stack uses the RFC 9421 `test-key-ed25519` key. For a `POST` with
body `hello` to `https://example.com/api/enroute/hooks` and
`created=1700000000`, it produces:

```text
content-digest: sha-256=:LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=:
signature-input: sig1=("@method" "@authority" "@path" "content-digest");created=1700000000;keyid="poqkLGiymh_W0uP6PZFw-dvez3QJT5SolqXBCW38r0U"
signature: sig1=:szerawmIvPHHZZJI/5wM+7a5l9CSpl34E2JgSeQFLfZIrF9MgJ28y7BGGG6sbYQineXZm83p3K3SKMkhu7g6DQ==:
```

Validate the vector with the library before using it in production.
