# Observability

Enroute exports OpenTelemetry traces over OTLP when `[telemetry]` provides an
endpoint. Without that table, spans are written only to the console.

```toml
[telemetry]
endpoint = "https://otlp.example.com/v1/traces"
sample_ratio = 1.0

[telemetry.headers]
authorization = "Bearer ${OTEL_TOKEN}"
"x-dataset" = "enroute"
```

| Key | Default | Meaning |
| --- | --- | --- |
| `telemetry.endpoint` | none | Full OTLP traces URL |
| `telemetry.headers` | none | Collector headers; values are secret |
| `telemetry.sample_ratio` | `1.0` | Head-sampling ratio from `0.0` to `1.0` |

Invalid ratios or header names stop startup. `RUST_LOG` controls console logs;
exported spans retain an `INFO` minimum level. Lambda ingestion uses
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and `OTEL_EXPORTER_OTLP_HEADERS`.

Git operation spans record object-store request counts and bytes by storage
role. The `actor` from `authorize` is attached to the span, so use an internal
identifier rather than a credential or email address. Scratch storage is not
metered.

Maintenance has no metric, and tenant-refresh failures only produce `ERROR`
logs. Alert on those logs.
