//! W3C trace context on calls between nodes, so that the spans of one push or
//! fetch on a compute and on the ledger are one trace.
//!
//! The propagator is the process's global one, which does nothing until
//! telemetry installs it: without an exporter, calls carry no extra headers.

use opentelemetry_http::{HeaderExtractor, HeaderInjector};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// Write the current span's trace context into `headers`.
pub fn inject(headers: &mut http::HeaderMap) {
    let cx = tracing::Span::current().context();
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(&cx, &mut HeaderInjector(headers));
    });
}

/// Whether `headers` carry a trace context.
#[must_use]
pub fn carried(headers: &http::HeaderMap) -> bool {
    headers.contains_key("traceparent")
}

/// Make `span`, which must not have started, a child of the trace context in
/// `headers`. Without one, `span` stays a root.
pub fn adopt(span: &tracing::Span, headers: &http::HeaderMap) {
    let cx =
        opentelemetry::global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(headers)));
    // An error means no exporter, or a span the filter disabled: nothing to
    // join then.
    drop(span.set_parent(cx));
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{TraceContextExt as _, TracerProvider as _};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    #[test]
    fn an_injected_context_is_adopted_on_the_other_side() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let provider = SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        tracing::subscriber::with_default(subscriber, || {
            let caller = tracing::info_span!("caller");
            let mut headers = http::HeaderMap::new();
            caller.in_scope(|| inject(&mut headers));
            assert!(carried(&headers));

            let handler = tracing::info_span!("handler");
            adopt(&handler, &headers);
            assert_eq!(
                handler.context().span().span_context().trace_id(),
                caller.context().span().span_context().trace_id(),
            );
        });
    }
}
