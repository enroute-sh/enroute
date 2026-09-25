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

Enroute does not terminate TLS or authenticate gRPC callers. Any caller that
can reach the API listener can read, write, and delete every repository in the
deployment.

Reachability is therefore the whole of the control. Bind `listen.api` to a
private interface, and restrict it further with a network policy, a security
group, or a service mesh. gRPC reflection is served on the same listener, so do
not expose it if your API shape is confidential.

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

A deployment holds one application's repositories. Enroute enforces no
boundary inside that set: every key is reachable by whoever reaches the API
listener, and by whatever your `authorize` hook grants. To serve parties that
must not reach each other's repositories, run a deployment per party, each with
its own database and bucket.

Enroute validates Git paths, repository keys, hook-context size, and ref
targets. It does not provide rate limits, quotas, per-caller concurrency
limits, encryption at rest, or protection against a compromised application.
