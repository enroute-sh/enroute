//! Carrying the caller's trace context across the invoke, so a push is one
//! trace rather than two.
//!
//! W3C `traceparent`, as a field of the call rather than a header: a direct
//! invoke has none, and a payload field says trace context is part of the
//! protocol.

use std::collections::HashMap;

use opentelemetry::propagation::TextMapPropagator as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

/// The current span's `traceparent`, to put on an outgoing call.
///
/// `None` when nothing is being traced, in which case the worker roots its
/// own trace.
#[must_use]
#[cfg(feature = "client")]
pub(crate) fn outgoing_traceparent() -> Option<String> {
    let mut carrier = HashMap::new();
    // Built here rather than `global::text_map_propagator()`, which means
    // nothing unless the binary called `set_text_map_propagator`.
    TraceContextPropagator::new().inject_context(&tracing::Span::current().context(), &mut carrier);
    carrier.remove("traceparent")
}

/// Make the calling push's trace the parent of `span`, if it carried one.
///
/// A no-op without a `traceparent`, leaving `span` to root its own trace — which
/// is what a manual invoke should do.
pub fn adopt_caller(span: &tracing::Span, traceparent: Option<&str>) {
    let Some(traceparent) = traceparent else {
        return;
    };
    let carrier = HashMap::from([("traceparent".to_string(), traceparent.to_string())]);

    // Reported rather than dropped: this fails when the layer can't be reached
    // by `downcast_raw`, which silently costs every push its connected trace.
    if let Err(e) = span.set_parent(TraceContextPropagator::new().extract(&carrier)) {
        tracing::warn!(error = %e, "could not adopt the caller's trace");
    }
}
