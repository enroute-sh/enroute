//! Tracing subscriber setup: console output always, OTLP export when a
//! deployment names somewhere to send spans.

use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use anyhow::Result;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracerProvider},
};
use tracing_appender::non_blocking::{
    DEFAULT_BUFFERED_LINES_LIMIT, ErrorCounter, NonBlockingBuilder, WorkerGuard,
};
use tracing_subscriber::{
    EnvFilter, Layer, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

use crate::config::{Node, Telemetry};

/// Flushes the spans not yet exported and the console lines not yet written
/// when it drops, so that the last ones before a stop are not lost.
#[derive(Debug)]
#[must_use = "spans and console lines not yet written are lost when this drops"]
pub struct Flush {
    provider: Option<SdkTracerProvider>,
    // Dropped after `drop` runs, so a warning from the span flush still
    // reaches the console.
    _console: WorkerGuard,
}

impl Drop for Flush {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(e) = provider.shutdown()
        {
            tracing::warn!("flushing spans: {e}");
        }
    }
}

/// Initializes the global tracing subscriber.
///
/// Console logging (via `RUST_LOG`, `console` when unset) is always on, and
/// drops lines a full buffer behind. With `telemetry`, spans export too.
///
/// # Errors
///
/// Returns an error if a subscriber is already installed, if a header name is
/// not one, or if building the OTLP exporter fails.
pub fn init(telemetry: Option<&Telemetry>, node: Option<&Node>, console: &str) -> Result<Flush> {
    // Filtered per-layer: quieting the console (RUST_LOG) must not silently
    // drop exported spans, so the otel layer gets its own INFO floor.
    let fmt_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(console));
    // Written from a thread of their own: a write on the thread that logs
    // takes stdout's lock, and with debug timing logs on, the async workers
    // stalled on each other and on the pipe. Dropping lines past a full
    // buffer keeps that wait off the workers.
    let lines = node.map_or(DEFAULT_BUFFERED_LINES_LIMIT, |node| {
        node.console_buffer_lines.get()
    });
    let (stdout, console) = NonBlockingBuilder::default()
        .buffered_lines_limit(lines)
        .thread_name("console")
        .finish(std::io::stdout());
    report_drops(stdout.error_counter())?;
    let registry = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_writer(stdout)
            .with_filter(fmt_filter),
    );

    let Some(telemetry) = telemetry else {
        registry.try_init()?;
        return Ok(Flush {
            provider: None,
            _console: console,
        });
    };
    let provider = build_provider(telemetry, node)?;
    registry
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("enroute"))
                // Nothing reads busy and idle times, and a push body's poll
                // per chunk would pay for them.
                .with_tracked_inactivity(false)
                .with_filter(LevelFilter::INFO),
        )
        .try_init()?;
    // Only with an exporter: else calls to other nodes carry no headers.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    Ok(Flush {
        provider: Some(provider),
        _console: console,
    })
}

/// How often [`report_drops`] looks.
const DROPS_EVERY: Duration = Duration::from_secs(10);

/// Logs how many console lines were dropped, at most every [`DROPS_EVERY`].
///
/// A dropped line leaves no trace on its own, so a gap in the console would
/// otherwise read as a quiet stretch.
fn report_drops(dropped: ErrorCounter) -> Result<()> {
    thread::Builder::new()
        .name("console-drops".into())
        .spawn(move || {
            let mut reported = 0;
            loop {
                thread::sleep(DROPS_EVERY);
                let total = dropped.dropped_lines();
                if total > reported {
                    tracing::warn!(
                        dropped = total - reported,
                        "console lines dropped: stdout fell a full buffer behind"
                    );
                    reported = total;
                }
            }
        })?;
    Ok(())
}

/// Builds the OTLP/HTTP tracer provider `telemetry` describes, for `node`.
///
/// Protobuf over HTTP, which is the encoding every collector accepts; the
/// endpoint is dialed as written, and the headers are how one knows a caller.
fn build_provider(telemetry: &Telemetry, node: Option<&Node>) -> Result<SdkTracerProvider> {
    // Names were checked where they were written, so this only copies.
    let headers: HashMap<String, String> = telemetry
        .headers
        .iter()
        .map(|(name, value)| (name.clone(), value.expose().to_string()))
        .collect();

    let exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(&telemetry.endpoint)
        .with_headers(headers)
        .build()?;

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            telemetry.sample_ratio,
        ))))
        .with_resource(
            Resource::builder()
                .with_service_name("enroute")
                .with_attributes(resource(node))
                .build(),
        )
        .build();

    Ok(provider)
}

/// What tells one node's spans from another's: during an upgrade, the
/// version; across the cluster, the role and the address.
fn resource(node: Option<&Node>) -> Vec<KeyValue> {
    let mut attributes = vec![KeyValue::new("service.version", env!("CARGO_PKG_VERSION"))];
    if let Some(node) = node {
        let instance = node
            .advertise_url
            .as_ref()
            .map_or_else(|| node.internal_listen.to_string(), ToString::to_string);
        attributes.push(KeyValue::new("service.instance.id", instance));
        attributes.push(KeyValue::new("enroute.role", node.role.name()));
    }
    attributes
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::{layer::Context, registry::LookupSpan};

    use super::*;

    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<String>>>);

    impl<S> Layer<S> for Recorder
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: Context<'_, S>,
        ) {
            self.0.lock().unwrap().push(attrs.metadata().name().into());
        }
    }

    // An unset `RUST_LOG` must not silently drop spans from export: an INFO
    // span must reach a layer with its own INFO filter past a stricter one.
    #[test]
    fn info_filtered_layer_still_sees_spans_past_a_stricter_sibling() {
        let recorder = Recorder::default();

        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::new("error")))
            .with(recorder.clone().with_filter(LevelFilter::INFO));

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("root");
            let _guard = span.enter();
        });

        assert_eq!(*recorder.0.lock().unwrap(), vec!["root".to_string()]);
    }
}
