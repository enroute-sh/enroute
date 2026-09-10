# Quickstart

This guide runs Enroute from the published image, creates a repository over
gRPC, and shows what a Git push needs. You need Docker, Git, and [`grpcurl`].
You do not need a source checkout.

[`grpcurl`]: https://github.com/fullstorydev/grpcurl

## Create the project

Make a directory with a `config` subdirectory:

```sh
mkdir -p enroute-local/config && cd enroute-local
```

Create `compose.yaml`. It starts Postgres and the published image:

```yaml
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: enroute
      POSTGRES_PASSWORD: enroute
      POSTGRES_DB: enroute
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U enroute -d enroute"]
      interval: 2s
      timeout: 3s
      retries: 30
    volumes: ["pgdata:/var/lib/postgresql/data"]

  enroute:
    image: ghcr.io/enroute-sh/enroute:latest
    command: ["--config=file:///etc/enroute/enroute.toml"]
    ports: ["8080:8080", "50051:50051"]
    volumes:
      - "objects:/data"
      - "./config:/etc/enroute:ro"
    extra_hosts: ["host.docker.internal:host-gateway"]
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pgdata:
  objects:
```

Create `config/enroute.toml`:

```toml
[bucket]
uri = "file:///data/objects"

[database]
url = "postgres://enroute:enroute@postgres:5432/enroute"

[hooks]
# The public test key from RFC 9421. Generate your own key for any other use.
signing_key = """
-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIJ+DYvh6SEqVTm50DFtMDoQikTmiCqirVv9mWG9qfSnF
-----END PRIVATE KEY-----
"""

[tenants]
uri = "file:///etc/enroute/tenants.toml"
refresh_secs = 2

[ingest.local]
scratch = "file:///data/scratch"
```

Create `config/tenants.toml`. The endpoint URL identifies the HTTP route that
will answer hooks:

```toml
[tenants.dev]
hook_endpoint_url = "http://host.docker.internal:3000/api/enroute/hooks"
domains = ["*"]
```

## Start Enroute

```sh
docker compose up -d
```

Enroute creates its database tables at startup. Git listens on port `8080`.
The API listens on port `50051`.

## Create a repository

Every API call names its tenant in a header. This stack has one tenant, `dev`,
and nothing in front of the API to set the header, so you set it yourself.
For local use, set the header directly. In a deployment, an authenticated
proxy must set it; see [Security](operate/security.md).

You choose the repository key. Use an id your application already allocated
and will never change, not a name a user can rename:

```sh
grpcurl -plaintext -H 'x-enroute-tenant: dev' \
  -d '{"repo": {"key": "repo-4f2a1c"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RepositoryService/CreateRepository
```

The call is idempotent. Run it twice and you get the same repository, so a
retry needs no bookkeeping. See
[Repository keys](reference/the-contract.md#repository-keys) for the rules a key
must follow.

Read the repository back, and list every repository the tenant has:

```sh
grpcurl -plaintext -H 'x-enroute-tenant: dev' \
  -d '{"repo": {"key": "repo-4f2a1c"}}' \
  127.0.0.1:50051 enroute.api.v1alpha1.RepositoryService/GetRepository

grpcurl -plaintext -H 'x-enroute-tenant: dev' \
  127.0.0.1:50051 enroute.api.v1alpha1.RepositoryService/ListRepositories
```

The API works with no application connected. Git does not, which the next
section explains.

## Serve it over Git

Enroute does not know which URL names which repository, and it does not know
who may push. It asks your application through the `authorize` hook on every
Git request. Until an endpoint answers that hook, every Git request fails.

Your endpoint receives one `POST` per hook at the URL in `config/tenants.toml`.
For a push of `http://127.0.0.1:8080/acme/widgets.git`, the `authorize` call
carries the path `acme/widgets.git`. Your application looks that path up in its
own tables and answers with the repository key, `repo-4f2a1c`. The URL and the
key are separate: a path can change, but a key stays stable.

Two ways to get an endpoint:

- Follow [Build a code hosting platform](build/README.md) to create a client
  and hook endpoint.

Edit `hook_endpoint_url` to match where your endpoint listens. Enroute re-reads
the tenant file every two seconds in this stack, so no restart is needed.

With an endpoint that grants access, this push lands:

```sh
git init widgets && cd widgets
git commit --allow-empty -m "first"
git remote add origin http://127.0.0.1:8080/acme/widgets.git
git push origin main
```

## Reset

```sh
docker compose down -v
```

This removes the database and object volumes. Your configuration files stay.

## Next steps

- [Build a code hosting application](build/README.md) — build an application
  on Enroute.
- [Hook reference](reference/hooks.md) — every field of every hook, with
  payloads.
- [Concepts](README.md#concepts)
- [Configuration](operate/configuration.md)
- [Limitations](reference/limitations.md)
