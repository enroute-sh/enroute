//! Tracing subscriber setup: console output always, OTLP export when a
//! deployment names somewhere to send spans.

use std::collections::HashMap;

use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    trace::{Sampler, SdkTracerProvider, Tracer},
};
use tracing_subscriber::{
    EnvFilter, Layer, filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt,
};

use enroute_config::Telemetry;

/// Initializes the global tracing subscriber.
///
/// Console logging (via `RUST_LOG`) is always on. With `telemetry`, spans also
/// export over OTLP — see [`build_tracer`].
///
/// # Errors
///
/// Returns an error if a subscriber is already installed, if a header name is
/// not one, or if building the OTLP exporter fails.
pub fn init(telemetry: Option<&Telemetry>) -> Result<()> {
    // Filtered per-layer: quieting the console (RUST_LOG) must not silently
    // drop exported spans, so the otel layer gets its own INFO floor.
    let fmt_filter = EnvFilter::from_default_env();
    let registry = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(fmt_filter));

    match telemetry {
        Some(telemetry) => registry
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(build_tracer(telemetry)?)
                    .with_filter(LevelFilter::INFO),
            )
            .try_init()?,
        None => registry.try_init()?,
    }

    Ok(())
}

/// Builds the OTLP/HTTP tracer `telemetry` describes.
///
/// Protobuf over HTTP, which is the encoding every collector accepts; the
/// endpoint is dialled as written, and the headers are how one knows a caller.
fn build_tracer(telemetry: &Telemetry) -> Result<Tracer> {
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
        .with_resource(Resource::builder().with_service_name("enroute").build())
        .build();

    Ok(provider.tracer("enroute"))
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
