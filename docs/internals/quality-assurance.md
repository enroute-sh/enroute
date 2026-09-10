# Quality assurance

Tests cover Git compatibility, push atomicity, and hook validation.

| Change | Command |
| --- | --- |
| General Rust code | `cargo test --workspace --exclude e2e` |
| Postgres code | `cargo test --workspace --features enroute/postgres-tests` |
| Fetch or push behavior | `cargo run -p e2e` |
| Dependencies, API, and documentation rules | `cargo xtask lint` |

## Git compatibility

`dev/e2e` generates histories with a `proptest` state machine and compares
Enroute with `git daemon` after each operation. It checks commit graphs, tags,
and missing objects. The default run uses 32 cases and up to 20 transitions:

```sh
cargo run -p e2e
ENROUTE_E2E_CASES=200 ENROUTE_E2E_MAX_TRANSITIONS=40 cargo run -p e2e
```

Reduced failures are saved in `dev/e2e/e2e.proptest-regressions` and replayed
first. The generated operations include branches, merges, tags, reverts,
shallow and blobless clones, deepening fetches, submodule gitlinks, and
maintenance passes.

## API and signatures

`dev/e2e/src/grpc/` covers repository, ref, object, history, sync, tenancy,
and reflection behavior. Some cases use real Git clients. Signature tests use
the RFC 9421 vector and a fixed fixture to detect changes in covered
components or JWK thumbprints.

Postgres tests require a server:

```sh
docker compose up -d postgres
export DATABASE_URL=postgresql://enroute:enroute@localhost:5433/enroute
cargo test --workspace --features enroute/postgres-tests
```

Connections use `pg_temp` schemas. In-memory and Postgres catalogs both run
the `enroute_git_metadata::conformance` suite.

## Known gaps

- No fault-injection coverage for node loss, object-store loss, or corrupt segments.
- No read-during-maintenance differential test.
- No CI load or soak test.
- The ref-update protocol is not formally specified or model checked.
