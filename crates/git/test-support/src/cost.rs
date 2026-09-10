//! Capturing the cost fields an operation records on its span.
//!
//! `Span::record` on a name the span never declared is a silent no-op, so the
//! field names an `#[instrument]` attribute declares and the ones `enroute_git_cost`
//! writes are a join the compiler cannot check. Drift there makes cost quietly
//! stop arriving — indistinguishable, in a dashboard, from an operation that
//! stopped spending. These helpers are what hold the two lists together.

use std::fmt::Debug;
use std::sync::{Arc, Mutex, PoisonError};

use tracing::field::{Field, Visit};
use tracing::span::{Id, Record};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt as _};

/// Collects the values recorded on spans *after* creation, which is where
/// cost lands.
///
/// An operation declares those fields `tracing::field::Empty` and fills
/// them in once it knows what it spent.
#[derive(Clone, Debug, Default)]
pub struct RecordedFields(Arc<Mutex<Vec<(String, i64)>>>);

impl RecordedFields {
    /// The value recorded under `name`, or `None` if it never arrived.
    pub fn get(&self, name: &str) -> Option<i64> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| *value)
    }

    /// Assert every one of `names` reached a span, naming the missing one.
    ///
    /// What did arrive is in the message: it separates fields that drifted
    /// from a span that was never enabled at all.
    pub fn assert_all_recorded(&self, names: &[&str]) {
        for name in names {
            assert!(
                self.get(name).is_some(),
                "`{name}` never reached a span: the #[instrument] declaration and \
                 the recorded names have drifted apart. recorded so far: {:?}",
                self.0.lock().unwrap_or_else(PoisonError::into_inner),
            );
        }
    }
}

impl Visit for RecordedFields {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((field.name().to_string(), value));
    }

    /// Every cost field is an `i64`; whatever else these spans carry is not
    /// what this is watching.
    fn record_debug(&mut self, _: &Field, _: &dyn Debug) {}
}

impl<S: tracing::Subscriber> Layer<S> for RecordedFields {
    fn on_record(&self, _span: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        values.record(&mut self.clone());
    }
}

/// Install a [`RecordedFields`] as the current thread's subscriber, returning
/// it with the guard that keeps it installed.
///
/// The guard must be held across the awaits under test: a span records
/// against the subscriber it was *created* under.
#[must_use]
pub fn capture_recorded_fields() -> (RecordedFields, DefaultGuard) {
    keep_callsites_enabled();
    let recorded = RecordedFields::default();
    let subscriber = tracing_subscriber::registry().with(recorded.clone());
    let guard = tracing::subscriber::set_default(subscriber);
    (recorded, guard)
}

/// Callsite enablement is cached once per process, but the subscriber above
/// is installed per thread.
///
/// A sibling test dropping its guard can get that cache stuck on "never" and
/// silently disable a span a different thread is still measuring.
fn keep_callsites_enabled() {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        drop(tracing::subscriber::set_global_default(
            tracing_subscriber::registry(),
        ));
    });
}
