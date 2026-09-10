//! Tracing for the worker: one subscriber, built on the first invocation
//! because the export's headers hold a token only `INVOKE` can reach.
//!
//! Do not put the export layer behind a `reload::Layer` slot to recover a
//! console for the init window: `reload` refuses `downcast_raw`, which is how
//! `set_parent` locates the layer, so every push would silently become two
//! traces. Nothing logs before the first invocation anyway.

use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::Context as _;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig as _, WithHttpConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::registry::Registry;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer};

/// Kept so [`flush`] can reach it.
///
/// Batching is what makes that flush load-bearing, and is still right.
static PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Whether [`install`] has run, so later invocations skip it.
static INSTALLED: OnceLock<()> = OnceLock::new();

/// Console logging, and span export too when `headers` is `Some`.
///
/// Call on the first invocation, before any span meant to be exported — a
/// subscriber installs once, so later calls do nothing.
pub fn install(headers: Option<&str>) {
    if INSTALLED.set(()).is_err() {
        return;
    }

    // Filtered per-layer, matching `enroute::telemetry`, so quieting `RUST_LOG`
    // cannot silently stop spans being exported. INFO rather than the empty
    // filter an unset `RUST_LOG` gives: CloudWatch is the only console here, and
    // nobody can re-run a Lambda with `RUST_LOG` set.
    let fmt = tracing_subscriber::fmt::layer().json().with_filter(
        EnvFilter::try_from_default_env().unwrap_or_else(|_unset| EnvFilter::new("info")),
    );
    let registry = Registry::default().with(fmt);

    let Some(export) = headers.and_then(|headers| match build_provider(headers) {
        Ok(provider) => Some(provider),
        Err(e) => {
            // Reaches nobody yet, no subscriber being installed — kept against
            // a future ordering where one is.
            tracing::error!(error = %e, "span export disabled");
            None
        }
    }) else {
        drop(registry.try_init());
        return;
    };

    // Its own floor rather than `RUST_LOG`, or crates instrumenting at DEBUG
    // (h2, sqlx, rustls) would all be exported.
    let layer = tracing_opentelemetry::layer()
        .with_tracer(export.tracer("enroute-ingest-lambda"))
        .with_filter(LevelFilter::INFO);
    drop(registry.with(layer).try_init());
    drop(PROVIDER.set(export));
}

/// Export whatever is queued, at stream completion rather than handler return.
///
/// Lambda freezes the environment once the response completes, so without
/// this a batch escapes only if a later invocation flushes it.
pub async fn flush() {
    let Some(provider) = PROVIDER.get() else {
        return;
    };
    // `force_flush` blocks on the exporter's HTTP round trip.
    let flushed = tokio::task::spawn_blocking(|| provider.force_flush()).await;
    match flushed {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "span flush failed"),
        Err(e) => tracing::warn!(error = %e, "span flush panicked"),
    }
}

/// The exporter `headers` and the environment describe.
///
/// The front door is configured by a file and this is not, so it reads OTLP's
/// own variables — the one vocabulary both ends share with every collector.
fn build_provider(headers: &str) -> Result<SdkTracerProvider, anyhow::Error> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .context("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")?;
    let exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(endpoint)
        .with_headers(parse_headers(headers))
        .build()?;

    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name("enroute-ingest-lambda")
                .with_attributes([
                    KeyValue::new("cloud.region", env_or_empty("AWS_REGION")),
                    KeyValue::new("faas.instance", env_or_empty("AWS_LAMBDA_LOG_STREAM_NAME")),
                    KeyValue::new(
                        "faas.max_memory",
                        env_or_empty("AWS_LAMBDA_FUNCTION_MEMORY_SIZE"),
                    ),
                ])
                .build(),
        )
        .build())
}

fn env_or_empty(key: &str) -> String {
    std::env::var(key).unwrap_or_default()
}

/// `key=value,key=value`, as `OTEL_EXPORTER_OTLP_HEADERS` is specified.
///
/// A pair without an `=` is dropped rather than refused: this runs before the
/// subscriber exists, and a push should not fail over a telemetry header.
fn parse_headers(raw: &str) -> HashMap<String, String> {
    raw.split(',')
        .filter_map(|pair| pair.split_once('='))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .filter(|(name, _)| !name.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use opentelemetry::propagation::TextMapPropagator as _;
    use opentelemetry::trace::TraceContextExt as _;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    use super::*;

    #[test]
    fn headers_are_read_the_way_otlp_specifies_them() {
        let parsed = parse_headers("authorization=Bearer abc,x-dataset=enroute");

        assert_eq!(parsed["authorization"], "Bearer abc");
        assert_eq!(parsed["x-dataset"], "enroute");
    }

    /// Whitespace around a pair is a deployment writing it readably, and a
    /// pair with no `=` is dropped rather than failing a push over telemetry.
    #[test]
    fn a_malformed_pair_costs_only_itself() {
        let parsed = parse_headers(" a = 1 ,nonsense, =2,b=3");

        assert_eq!(parsed["a"], "1");
        assert_eq!(parsed["b"], "3");
        assert_eq!(parsed.len(), 2);
    }

    /// `reload::Layer` refuses `downcast_raw`, which `set_parent` needs, so
    /// wrapping the layer in one would silently root a new trace instead.
    #[test]
    fn a_span_adopts_a_caller_trace_through_the_installed_stack() {
        let provider = SdkTracerProvider::builder().build();
        let subscriber = Registry::default().with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("test"))
                .with_filter(LevelFilter::INFO),
        );

        let caller = "0af7651916cd43dd8448eb211c80319c";
        let mut headers = HashMap::new();
        headers.insert(
            "traceparent".to_string(),
            format!("00-{caller}-b7ad6b7169203331-01"),
        );

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("probe");
            span.set_parent(TraceContextPropagator::new().extract(&headers))
                .expect("the layer must be reachable by downcast");
            let adopted = span.context().span().span_context().trace_id().to_string();
            assert_eq!(adopted, caller, "span did not join the caller's trace");
        });
    }
}
