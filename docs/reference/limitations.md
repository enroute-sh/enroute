# Limitations

Enroute is a `0.x` release with the following current limits.

## Compatibility

- There is no backwards compatibility between `0.x` releases.
- The `v1alpha1` contract is unstable until Enroute 1.0. Messages, fields, and
  services can change in any release.
- The object store format and the configuration keys can change in any
  release.
- Database tables migrate forward at startup. The object store does not
  migrate, so a format change needs new storage and a new push.
- There is no supported downgrade.

## Transport and synchronization

- Git smart HTTP is supported; SSH and `git://` are not.
- Enroute does not terminate TLS or authenticate API callers.
- Fetches with `have` lines use complete packs rather than thin packs.
- `PushToRemote` supports push-only `https` remotes with basic or bearer
  credentials. It does not fetch, use SSH, persist remotes, or follow redirects.
- Remote pushes are synchronous: one hour total and one minute without remote
  output. Progress is not reported.

## Data model and storage

- Production requires Postgres and an S3-compatible object store. `file://`
  storage is for local development.
- Enroute does not encrypt data at rest or collect unreachable branch objects.
- SHA-1 is the only object format. Enroute creates no merge commits.
- `visible_refs` hides ref names, not objects.
- Reads are bounded: 100 repositories or commits per page and 50,000 tree or
  diff entries.

## Operations

- Tenants are file-managed; removal takes effect on the next refresh.
- Tenant IDs cannot be renamed without losing repository ownership.
- There are no quotas, rate limits, per-caller concurrency limits, or
  maintenance metrics.
- `enroute-maintenance` is not included in the image.
- Migrations are forward-only and transactional; concurrent index creation is
  not available.
- The `latest` image tag moves and can be rebuilt. A version tag names one
  image and is not rebuilt.
