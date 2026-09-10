# Development

Use this page when changing Enroute itself. See [Quality assurance](quality-assurance.md)
for test coverage.

## Prerequisites

`rust-toolchain.toml` specifies the Rust version. Install Git, Docker, `buf`,
and `cargo-machete`:

```sh
cargo install cargo-machete
```

## Local stack

```sh
docker compose up --build
docker compose down -v
```

The root compose file builds the image and runs it with Postgres on host port
`5433`. The server reads `dev/config/enroute.toml` and its tenant file. Point
the configured hook URL at an application endpoint. Git requests require that
endpoint; the API does not. `down -v` removes local database and object data.

## Database migrations

Add migrations under `crates/server/postgres/migrations/` and register them in
the crate's `schema` module. Do not modify an applied migration: Enroute
checksums its contents. Migrations run transactionally under an advisory lock,
so they cannot include `CREATE INDEX CONCURRENTLY`.

`enroute-schema --config <uri>` applies migrations outside the server, and
`--dry-run` lists pending work.

## Checks

```sh
cargo build
cargo test
cargo clippy
cargo fmt
cargo xtask lint
cargo run -p e2e
```

`cargo xtask lint` checks layering, imports, dead code, documentation,
dependencies, and proto rules. Run `e2e` after changes to fetch or push paths.

## Protocol changes

Change `proto/enroute/` first. Keep its copies synchronized in the image,
`skills/build-on-enroute/proto/`, and `crates/api/types` descriptors.
`cargo xtask lint` detects drift and validates skill frontmatter.
