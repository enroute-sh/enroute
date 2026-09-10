# Maintenance

Maintenance runs work that should not delay Git requests. Each pass:

1. Removes repositories deleted longer than `deleted_grace_secs`.
2. Combines pack images.
3. Merges index segments.
4. Removes old orphaned pack images and segments.

Combining pack images does not repack objects or reclaim objects from deleted
branches. Delete the repository to reclaim that data.

## Running maintenance

`[maintenance.run.in-process]` runs a pass in the server at `interval_secs`
(900 seconds by default). `[maintenance.run.off]` disables it so an external
scheduler can run `enroute-maintenance`. Running both is safe. Steps are
idempotent; pack gathering uses a per-repository lock and skips busy work.

Build the CLI from source:

```sh
cargo build --release -p enroute --bin enroute-maintenance
enroute-maintenance --config file:///etc/enroute/enroute.toml
enroute-maintenance --config file:///etc/enroute/enroute.toml --every 900
```

Without `--every`, the command runs once. `--dry-run` reports eligible work
without changing data.

## Retention windows

| Key | Default | Purpose |
| --- | --- | --- |
| `maintenance.grace_secs` | `21600` | Minimum age before an unreferenced object is removed |
| `maintenance.deleted_grace_secs` | `86400` | Time before a deleted repository is erased |

Keep `grace_secs` longer than the longest push. Uploaded bytes are
unreferenced until their catalog transaction commits.

A deleted repository can be restored before `deleted_grace_secs` by clearing
`repositories.deleted_at`, provided its key has not been reused.

## Monitoring

Maintenance has no metrics. It logs every pass; alert on stalled or failed
passes.
