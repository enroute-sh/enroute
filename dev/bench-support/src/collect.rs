//! Reads a run's own instrumentation back out, in the process that made it.
//!
//! Enroute records where its time goes as fields on tracing spans, and the
//! only other consumer is the OTLP exporter — so a harness that could not
//! read them here would need a deploy and a trace backend to see a number.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// What one key accumulated over a single run.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct SpanTotals {
    /// How many spans closed under this key.
    pub count: u64,
    /// Summed creation-to-close, so it exceeds the run's wall time for spans
    /// that ran concurrently — that ratio is how much concurrency they got.
    #[serde(rename = "wall_ms", serialize_with = "whole_millis")]
    pub wall: Duration,
    /// Summed rather than last-wins: phase counters are per-span totals that
    /// add across repeats, and a once-per-run span sums a single value.
    pub fields: BTreeMap<String, i64>,
}

impl SpanTotals {
    /// [`Self::wall`] in whole milliseconds.
    #[must_use]
    pub fn wall_ms(&self) -> u64 {
        millis(self.wall)
    }
}

/// How a closing span is filed.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum KeyedBy {
    /// The span's name, which is the phase for instrumented library code.
    #[default]
    Name,
    /// The span's `op` field, and its name where it carries none.
    Op,
}

/// Aggregates every span that closes while it is installed.
///
/// Cloneable and shared, so the subscriber's layer and the harness's handle
/// are one map.
#[derive(Debug, Clone)]
pub struct Collector {
    keyed_by: KeyedBy,
    totals: Arc<Mutex<BTreeMap<String, SpanTotals>>>,
    queries: Arc<AtomicU64>,
}

impl Collector {
    /// A collector filing each span under `keyed_by`.
    #[must_use]
    pub fn keyed_by(keyed_by: KeyedBy) -> Self {
        Self {
            keyed_by,
            totals: Arc::default(),
            queries: Arc::default(),
        }
    }

    /// Statements sent to Postgres since the last [`Self::reset`].
    #[must_use]
    pub fn queries(&self) -> u64 {
        self.queries.load(Ordering::Relaxed)
    }

    /// Forget everything measured so far, for the next run.
    pub fn reset(&self) {
        self.totals().clear();
        self.queries.store(0, Ordering::Relaxed);
    }

    /// A snapshot of what has closed since the last [`Self::reset`].
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<String, SpanTotals> {
        self.totals().clone()
    }

    fn totals(&self) -> MutexGuard<'_, BTreeMap<String, SpanTotals>> {
        // The map is only ever inserted into, so a poisoned lock loses
        // measurements, not correctness.
        self.totals.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<S> Layer<S> for Collector
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut recorded = Recorded::default();
        attrs.record(&mut recorded);
        let mut extensions = span.extensions_mut();
        extensions.insert(Started(Instant::now()));
        extensions.insert(recorded);
    }

    /// `sqlx` logs one event per statement, so its target is the query count.
    ///
    /// `COPY FROM STDIN` bodies aren't logged, so an append-heavy push moves
    /// more rows than this count implies.
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target().starts_with("sqlx::query") {
            self.queries.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut extensions = span.extensions_mut();
        if let Some(recorded) = extensions.get_mut::<Recorded>() {
            values.record(recorded);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let extensions = span.extensions();
        let Some(started) = extensions.get::<Started>() else {
            return;
        };
        let elapsed = started.0.elapsed();
        let recorded = extensions.get::<Recorded>();

        let key = match self.keyed_by {
            KeyedBy::Name => None,
            KeyedBy::Op => recorded.and_then(|r| r.op.clone()),
        };
        let mut totals = self.totals();
        let entry = totals
            .entry(key.unwrap_or_else(|| span.name().to_owned()))
            .or_default();
        entry.count += 1;
        entry.wall += elapsed;
        for (name, value) in recorded.into_iter().flat_map(|r| r.numbers.iter()) {
            *entry.fields.entry(name.clone()).or_default() += *value;
        }
    }
}

/// Integer span fields, and the `op` label [`KeyedBy::Op`] files under.
///
/// Every measurement is encoded as an `i64` so it does not arrive as a
/// string; anything else is a label.
#[derive(Debug, Default)]
struct Recorded {
    numbers: BTreeMap<String, i64>,
    op: Option<String>,
}

impl Visit for Recorded {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.numbers.insert(field.name().to_owned(), value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.numbers.insert(
            field.name().to_owned(),
            i64::try_from(value).unwrap_or(i64::MAX),
        );
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "op" {
            // A `Display` value arrives here too, and quotes a plain string.
            self.op = Some(format!("{value:?}").trim_matches('"').to_string());
        }
    }
}

#[derive(Debug)]
struct Started(Instant);

fn millis(wall: Duration) -> u64 {
    u64::try_from(wall.as_millis()).unwrap_or(u64::MAX)
}

/// Report a duration as the whole milliseconds it used to be counted in.
fn whole_millis<S: serde::Serializer>(wall: &Duration, out: S) -> Result<S::Ok, S::Error> {
    out.serialize_u64(millis(*wall))
}
