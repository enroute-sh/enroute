//! Traces of Git requests across a compute-only node and a ledger-only node.
//!
//! Each span is the child of the span that caused it, on either node.

#![expect(
    clippy::panic,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use common::{Git, Server, clone_matches, cluster, local_store, push_commits};
use enroute::node::{Config, Role};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

/// The value of `span`'s attribute `key`, as text.
fn attribute(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.to_string())
}

/// The first span called `name`.
fn find_named<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no {name} span"))
}

/// The first span called `name` whose attribute `key` is `value`.
fn find<'a>(spans: &'a [SpanData], name: &str, key: &str, value: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|s| s.name == name && attribute(s, key).as_deref() == Some(value))
        .unwrap_or_else(|| panic!("no {name} span with {key} {value}"))
}

/// The parent of `child` among `spans`.
fn parent<'a>(spans: &'a [SpanData], child: &SpanData) -> &'a SpanData {
    spans
        .iter()
        .find(|s| s.span_context.span_id() == child.parent_span_id)
        .unwrap_or_else(|| panic!("the parent of {} is not among the spans", child.name))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_is_one_trace_across_compute_and_ledger() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")))
        .init();
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let root = tempfile::tempdir().unwrap();
    let store = local_store(root.path());
    let _ledger = cluster(root.path(), &store, 1, |_, c| c.role = Role::Ledger).await;
    let mut c = Config::local(root.path().join("compute"), store.clone());
    c.role = Role::Compute;
    let compute = Server::start_config(c).await;
    let git = Git::new();
    let src = git.init("src").await;
    push_commits(&git, &src, &compute.repo_url("r"), 0, 1).await;
    provider.force_flush().unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let push = find(
        &spans,
        "enroute::git::request",
        "git.service",
        "git-receive-pack",
    );
    assert_eq!(attribute(push, "repo").as_deref(), Some("r"));
    let trace = push.span_context.trace_id();
    let traced: Vec<SpanData> = spans
        .into_iter()
        .filter(|s| s.span_context.trace_id() == trace)
        .collect();
    // The name of the parent of the span called `name`.
    let parent_of = |name: &str| parent(&traced, find_named(&traced, name)).name.to_string();

    assert_eq!(parent_of("enroute::push::ingest"), "enroute::git::request");
    assert_eq!(parent_of("enroute::push::land"), "enroute::git::request");
    assert_eq!(parent_of("enroute::commit::propose"), "enroute::push::land");
    let call = find(&traced, "enroute::ledger::call", "rpc.method", "propose");
    assert_eq!(parent(&traced, call).name, "enroute::commit::propose");
    // The ledger's spans, on the other node, joined through the gRPC call.
    assert_eq!(parent_of("enroute::grpc::server"), "enroute::ledger::call");
    assert_eq!(
        parent_of("enroute::ledger::propose"),
        "enroute::grpc::server"
    );
    assert_eq!(
        parent_of("enroute::ledger::replicate"),
        "enroute::ledger::propose"
    );

    // A fetch's pack is written after its handler returns, still in its
    // request's span.
    clone_matches(&git, &src, &compute.repo_url("r"), "clone").await;
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let pack = find_named(&spans, "enroute::fetch::write_pack");
    let request = parent(&spans, pack);
    assert_eq!(request.name, "enroute::git::request");
    assert_eq!(
        attribute(request, "git.service").as_deref(),
        Some("git-upload-pack")
    );
    assert!(
        request.end_time >= pack.end_time,
        "the request span ended before its pack"
    );
}
