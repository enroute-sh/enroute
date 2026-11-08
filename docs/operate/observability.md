# Observability

Enroute's observability is distributed tracing. Each node exports
OpenTelemetry traces over OTLP to the collector that `[telemetry]` names, and
one trace follows a request across the compute node, the ledger nodes, and your
hook endpoint. Rates, latencies, and errors come from those spans in your
tracing backend, so Enroute exports no separate metrics. Without a
`[telemetry]` table, spans are written only to the console.

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
exported spans retain an `INFO` minimum level. A background thread writes
console logs to stdout, so a slow terminal or pipe does not slow requests. If
that thread falls behind by a full buffer of lines, new lines are dropped.

The `actor` from `authorize` is attached to the span, so use an internal
identifier rather than a credential or email address.

## Traces

Each Git request is one trace. Its root span, `enroute::git::request`, has the
`repo`, the `git.service` and the response status. A fetch's span ends when
its pack is sent. Under the root, a push has a span for each stage:

| Span | What it covers |
| --- | --- |
| `enroute::push::receive` | The spool of the body and the pack's first pass |
| `enroute::push::ingest` | The pack checked against the repository |
| `enroute::hooks::pre_receive` | The pre-receive hook |
| `enroute::push::land` | The commit of the ref updates |
| `enroute::commit::publish_layer` | The upload of the new layer |
| `enroute::commit::propose` | The proposal to the ledger |
| `enroute::ledger::call` | A call to the ledger, with its `rpc.method` |
| `enroute::hooks::post_receive` | The post-receive hook |

A fetch has `enroute::fetch::plan` and `enroute::fetch::write_pack`.

Calls from a compute to a ledger node, and to the hook endpoint, carry W3C
trace context (`traceparent`). The ledger node continues the trace in
`enroute::grpc::server`, `enroute::ledger::propose` and
`enroute::ledger::replicate`, so one trace shows a push on both nodes. A
`traceparent` that a proxy sends with a Git request makes the request part of
the proxy's trace. Calls between ledger nodes are not traced.

Each node names itself with these resource attributes:

| Attribute | Value |
| --- | --- |
| `service.name` | `enroute` |
| `service.version` | The Enroute version |
| `service.instance.id` | `node.advertise_url`, or else `node.internal_listen` |
| `enroute.role` | `both`, `ledger` or `compute` |

Use `service.version` to compare the nodes of each version during an upgrade.
`OTEL_RESOURCE_ATTRIBUTES` adds more attributes. At a stop, a node exports the
spans that it has not sent yet.

Background work, such as flushes, compaction, and the layer sweep, is not
traced. Enroute logs its failures. See [What to watch](#what-to-watch).

## Timing logs

To see where a request spends its time, turn on debug logs for these targets.
Each line gives the times in microseconds (`*_us`). The fields can change in
any release.

| Target | What it times |
| --- | --- |
| `enroute::request_timing` | Each Git request, and the spool of a push body |
| `enroute::push_timing` | The stages of a push |
| `enroute::read_timing` | Linearizable reads: the follow, the read barrier and the catch-up |
| `enroute::ledger_call` | Each call from a compute to the ledger, split into network and handler time |
| `enroute::ledger_timing` | Rounds and apply batches on a ledger node |

```sh
RUST_LOG=info,enroute::push_timing=debug
```

With `enroute::ledger_call` on, calls to the ledger carry `x-enroute-*-us`
metadata so that the two nodes can split the time.

## Node status

Each node serves `GET /status` on its internal listener, `node.internal_listen`
(`0.0.0.0:7400` by default). The response is JSON diagnostics of each role
that the node runs: the compute's repository views and storage, and the
consensus engine under `consensus`. The fields are for people and can change
in any release. Do not parse them in automation.

```sh
curl http://127.0.0.1:7400/status
```

A ledger node also serves `GET /status/repos/<repo>`: what the node knows of
the repository's log. The response has the node ID (`node`), the sequencer
that the node knows of (`sequencer`, or `null`), the log's `term`, whether the
node sequences the log (`leading`), and the `version` of the state that the
node has.
A node that does not host the log answers `404`.

```sh
curl http://127.0.0.1:7400/status/repos/acme/widgets
```

Each node answers from its own view. A paused or partitioned sequencer still
says that it leads. To find the sequencer, ask each ledger node, and take the
`sequencer` that a majority names at the highest `term`.

The Git and API listeners do not serve `/status`. The internal listener also
carries the gRPC between ledger nodes, so keep it reachable only inside the
cluster. For a liveness check from outside, use `GET /healthz` on the Git
listener.

## What to watch

Watch requests through their traces, and the rest of the system through the
signals that follow:

| Signal | Where | Act when |
| --- | --- | --- |
| Request rate, errors, and latency | The `enroute::git::request` spans (`http.response.status_code`) and the `enroute::grpc::*` spans, in your tracing backend | Errors rise, or latency rises. The child spans show which stage is slow |
| `ERROR` log lines | Each node's console | Any appear. A failed flush, merge, sweep, or API call logs one, with the cause |
| `enroute diagnose` exit status | A ledger node, on a schedule | It exits with status 1. Its output names the check that failed |
| A member that is not `alive` | `enroute diagnose` | A node is down. If its disk is lost, [replace it](maintenance.md#replace-a-node-that-lost-its-disk) |
| `consensus.engine.lagging_logs` | `GET /status` of each ledger node, read by hand | It stays above `0` after a node change. A replica is not catching up |
| Push latency | The `enroute::push::land` and `enroute::commit::propose` spans | It rises. A commit waits for the ledger disks to sync, so check their sync latency |
| `GET /healthz` | Each compute node's Git listener | It does not answer `200`. Use it for the load balancer's health check |
| Ledger disk space and sync latency | Your host or cloud metrics | Space runs low, or sync latency rises |
| Object store errors and throttling | Your provider's metrics | Errors or throttled requests rise |

`/healthz` says only that the process serves HTTP. It does not check the
ledger or the object store: a compute node that cannot reach the ledger still
answers `200`. Use `enroute diagnose` for those.

Run `enroute diagnose` from automation and use its exit status. Do not parse
its output or the `/status` fields: they are for people, and they can change
in any release.
