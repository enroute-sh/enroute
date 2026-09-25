# Operate Enroute

Use these pages to deploy, configure, and maintain Enroute.

An Enroute deployment needs stateless server compute, Postgres, and object
storage. Expose the Git listener behind TLS. Keep the API listener reachable
only by your application.

| Page | Use it for |
| --- | --- |
| [Hosting](hosting.md) | Deployment model, image tags, and operator responsibilities |
| [Configuration](configuration.md) | Main configuration, migrations, and Lambda ingestion |
| [Storage backends](storage-backends.md) | Object storage and staging storage |
| [Security](security.md) | Listener exposure, hook signatures, and what Enroute does not isolate |
| [Maintenance](maintenance.md) | Compaction, cleanup, retention, and scheduled maintenance |
| [Observability](observability.md) | Traces, logs, and usage data |

For a production deployment, read Hosting, Storage backends, and Security
before Configuration. Then set up maintenance and observability. Review
[Limitations](../reference/limitations.md) before deploying.
