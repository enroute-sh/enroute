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

The internal listener (`node.internal_listen`) authenticates its callers. A
node gets a token by showing a ledger node that it can read the object store
(see [Joining the ledger](../internals/ledger-and-compute.md#joining-the-ledger)),
so access to the bucket decides who may call the ledger. `Peer` calls take only a
ledger node's own token. The internal listener has no TLS yet: keep it on a
private network.

## Object store permissions by role

When ledger and compute nodes run separately, give each role its own
credentials. Enroute keeps what each role writes under its own prefix:

| Prefix | Ledger nodes | Compute nodes |
| --- | --- | --- |
| `cluster/` | read, write | read |
| `data/` | read, write, delete | read, write, delete, list |

`cluster/` holds the ledger's members, with the URLs and public keys of the
ledger nodes: compute nodes read it to find the ledger, and nothing in it is
secret. Compute nodes must not be able to write `cluster/`. The keys there
check the tokens ledger nodes sign, and a join is proven by a write there, so a
node that could write it could join the ledger or sign tokens of its own,
including the ones `Peer` calls take.

`enroute members --remove` signs with a ledger node's own key, so run it on a
ledger node.

On S3, for example, a compute node's policy allows:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::BUCKET"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::BUCKET/data/*", "arn:aws:s3:::BUCKET/cluster/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::BUCKET/data/*"
    }
  ]
}
```

Ledger nodes get the same statements with `GetObject`, `PutObject` and
`DeleteObject` on `BUCKET/*`. On GCS, grant the compute nodes' service account
`storage.objects.list` on the bucket (a custom role: a list request names the
bucket, so an object-name condition would refuse it), and
`roles/storage.objectUser` with the IAM condition
`resource.name.startsWith("projects/_/buckets/BUCKET/objects/data/")`.

Some providers scope credentials per bucket only, among them Cloudflare R2
(without temporary credentials) and DigitalOcean Spaces. There, compute nodes'
credentials can write the whole bucket, and the ledger's tokens keep out only
nodes that cannot read it.

Reachability is therefore the whole of the control. Bind `listen.api` to a
private interface, and restrict it further with a network policy, a security
group, or a service mesh. gRPC reflection is served on the same listener, so do
not expose it if your API shape is confidential.

## Git access

The `authorize` hook is the only access control on `listen.git`. If you do not
enable it, anyone who can reach the listener can read and push every
repository, and a push creates new ones. Enroute logs a warning at startup when
`authorize` is off and `listen.git` is not bound to a loopback address.

Never expose `listen.git` with `authorize` off. Enable it in `hooks.enabled`
before you bind Git to any interface other than loopback. See
[Enable hooks](../reference/hooks.md#enable-hooks).

## Hook signatures

Enroute signs each hook with Ed25519 HTTP Message Signatures (RFC 9421).
Distribute the public key to each application and verify every request before
reading its body. Generate a separate key for each environment:

```sh
openssl genpkey -algorithm ed25519 -out hook-signing-key.pem
```

Reference the private key through `ENROUTE_HOOK_SIGNING_KEY`. `enroute dev`
keeps a generated key in its directory. Use that key only for local
development. See
[Verify hook signatures](../reference/verify-the-signature.md).

## Other boundaries

`PushToRemote` and `FetchFromRemote` discard credentials after each call. They
reject credentials in URLs and redirects, and permits only public `https` hosts unless
`git.allow_private_remotes` is enabled.

A deployment holds one application's repositories. Enroute enforces no
boundary inside that set: every key is reachable by whoever reaches the API
listener, and by whatever your `authorize` hook grants. To serve parties that
must not reach each other's repositories, run a deployment per party, each with
its own bucket and node disks.

Enroute validates Git paths, repository keys, hook-context size, and ref
targets. It does not provide rate limits, quotas, per-caller concurrency
limits, encryption at rest, or protection against a compromised application.
