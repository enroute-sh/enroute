# Security

Enroute is unaudited. Security fixes go out in the next release, and there are
no backports to earlier ones. Report vulnerabilities to
<security@enroute.sh>.

Your application authenticates users and supplies product policy through
hooks. Its security controls are part of the deployment boundary.

## API access

| Listener | Intended exposure |
| --- | --- |
| `listen.git` | Public, behind TLS termination |
| `listen.api` | Private; application callers only |

Enroute does not terminate TLS or authenticate gRPC callers. The tenant header
selects the tenant. Any caller that can reach the API listener and set that
header can access every tenant.

Place a proxy, gateway, or service mesh in front of the API listener. It must
authenticate callers and replace, rather than append, the tenant header.
Enroute rejects requests with multiple tenant headers. gRPC reflection is
available before tenant validation; do not expose the listener if its API shape
is confidential.

## Hook signatures

Enroute signs each hook with Ed25519 HTTP Message Signatures (RFC 9421).
Distribute the public key to each application and verify every request before
reading its body. Generate a separate key for each environment:

```sh
openssl genpkey -algorithm ed25519 -out hook-signing-key.pem
```

Reference the private key through `ENROUTE_HOOK_SIGNING_KEY`. The Quickstart
uses a public RFC test key and is only suitable for local development. See
[Verify hook signatures](../reference/verify-the-signature.md).

## Other boundaries

`PushToRemote` discards credentials after each call. It rejects credentials in
URLs and redirects, and permits only public `https` hosts unless
`sync.allow_private_remotes` is enabled.

Repository ownership is scoped by tenant ID. A key owned by another tenant is
not found. Tenant-file removals take effect at the next refresh; restart to
revoke immediately or deny access in your application.

Enroute validates Git paths, tenant ownership, hook-context size, and ref
targets. It does not provide rate limits, quotas, per-caller concurrency
limits, encryption at rest, or protection against a compromised application.
