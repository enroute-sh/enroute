//! Unit cost accounting for the git operations `git` serves.
//!
//! A [`Meter`] accumulates the billable resources one `upload-pack` or
//! `receive-pack` consumes, and records them on its span. Deliberately units
//! only, never prices, since a rate belongs to whoever queries the spans. A
//! meter rides inside the store handle ([`CountingStore`]), not a
//! task-local, since a push's work spans spawned tasks, a `JoinSet`, and a
//! rayon pool that a task-local would silently fail to follow.

mod store;

pub use store::CountingStore;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Which store a request went to, and so which set of counters it lands in.
///
/// Named for the role rather than the product: one deployment's `Primary` and
/// another's can be buckets at different vendors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreRole {
    /// Permanent objects — segments and tags.
    Primary,
    /// The push handoff: the pack, and the push document when it doesn't fit
    /// in the call, written by the front door for the ingest worker to read.
    ///
    /// Not `enroute_git_ingest`'s local-disk `StagingStore` — mixing that
    /// free scratch into a billed tally would produce a number no rate fits.
    Handoff,
}

/// The billable request classes and byte volumes for one store.
///
/// A snapshot, not live counters — [`Meter::units`] produces it, and it is
/// what crosses the wire back from a worker.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreUnits {
    /// GET and HEAD requests, which an S3-compatible store bills as one class.
    pub get_class: u64,
    /// PUT, COPY and LIST requests, likewise one billed class — LIST sits
    /// with the writes because that is how a vendor prices it.
    pub put_class: u64,
    /// DELETE requests, free at most vendors — counted because an unreclaimed
    /// handoff bucket shows up here first.
    pub deletes: u64,
    /// Bytes in response bodies: free in a standard storage class, billed as
    /// data retrieval in an infrequent-access one.
    pub bytes_read: u64,
    /// Bytes in request bodies.
    pub bytes_written: u64,
}

/// What an operation spent on Lambda.
///
/// Only a `receive-pack` has any: it is the one operation that runs work off
/// the front door.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LambdaUnits {
    /// Invocations — one per push that carries a pack, since a delete-only
    /// push never reaches the worker.
    pub invocations: u64,
    /// Configured memory in MB times billed duration in ms.
    ///
    /// Exact in integers, unlike GB-seconds — divide by 1,024,000 to get them.
    pub mb_millis: u64,
}

/// Everything one git operation consumed, as a snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Units {
    /// Requests against the permanent object store.
    pub primary: StoreUnits,
    /// Requests against the push handoff bucket.
    pub handoff: StoreUnits,
    /// Lambda, for the pushes whose ingestion runs there.
    pub lambda: LambdaUnits,
}

impl Units {
    /// Record every unit on `span`, under the field names an operation's
    /// `#[instrument]` attribute declares as `Empty`.
    ///
    /// `Span::record` on an undeclared name is a silent no-op, so the
    /// `#[instrument]` list and [`FIELD_NAMES`] must agree.
    pub fn record_on(&self, span: &tracing::Span) {
        span.record(PRIMARY_GET_CLASS, count(self.primary.get_class));
        span.record(PRIMARY_PUT_CLASS, count(self.primary.put_class));
        span.record(PRIMARY_DELETES, count(self.primary.deletes));
        span.record(PRIMARY_BYTES_READ, count(self.primary.bytes_read));
        span.record(PRIMARY_BYTES_WRITTEN, count(self.primary.bytes_written));
        span.record(HANDOFF_GET_CLASS, count(self.handoff.get_class));
        span.record(HANDOFF_PUT_CLASS, count(self.handoff.put_class));
        span.record(HANDOFF_DELETES, count(self.handoff.deletes));
        span.record(HANDOFF_BYTES_READ, count(self.handoff.bytes_read));
        span.record(HANDOFF_BYTES_WRITTEN, count(self.handoff.bytes_written));
        span.record(LAMBDA_INVOCATIONS, count(self.lambda.invocations));
        span.record(LAMBDA_MB_MILLIS, count(self.lambda.mb_millis));
    }
}

/// The span field names [`Units::record_on`] writes, which every operation
/// that reports cost must declare as `tracing::field::Empty`.
///
/// `#[instrument]` must spell these as literals — `tracing`'s macro takes no
/// constant — so renaming one here is not a compile error.
pub const PRIMARY_GET_CLASS: &str = "cost_primary_get_class";
/// See [`PRIMARY_GET_CLASS`].
pub const PRIMARY_PUT_CLASS: &str = "cost_primary_put_class";
/// See [`PRIMARY_GET_CLASS`].
pub const PRIMARY_DELETES: &str = "cost_primary_deletes";
/// See [`PRIMARY_GET_CLASS`].
pub const PRIMARY_BYTES_READ: &str = "cost_primary_bytes_read";
/// See [`PRIMARY_GET_CLASS`].
pub const PRIMARY_BYTES_WRITTEN: &str = "cost_primary_bytes_written";
/// See [`PRIMARY_GET_CLASS`].
pub const HANDOFF_GET_CLASS: &str = "cost_handoff_get_class";
/// See [`PRIMARY_GET_CLASS`].
pub const HANDOFF_PUT_CLASS: &str = "cost_handoff_put_class";
/// See [`PRIMARY_GET_CLASS`].
pub const HANDOFF_DELETES: &str = "cost_handoff_deletes";
/// See [`PRIMARY_GET_CLASS`].
pub const HANDOFF_BYTES_READ: &str = "cost_handoff_bytes_read";
/// See [`PRIMARY_GET_CLASS`].
pub const HANDOFF_BYTES_WRITTEN: &str = "cost_handoff_bytes_written";
/// See [`PRIMARY_GET_CLASS`].
pub const LAMBDA_INVOCATIONS: &str = "cost_lambda_invocations";
/// See [`PRIMARY_GET_CLASS`].
pub const LAMBDA_MB_MILLIS: &str = "cost_lambda_mb_millis";

/// Every field name [`Units::record_on`] writes, for the tests that check an
/// operation declared all of them.
pub const FIELD_NAMES: [&str; 12] = [
    PRIMARY_GET_CLASS,
    PRIMARY_PUT_CLASS,
    PRIMARY_DELETES,
    PRIMARY_BYTES_READ,
    PRIMARY_BYTES_WRITTEN,
    HANDOFF_GET_CLASS,
    HANDOFF_PUT_CLASS,
    HANDOFF_DELETES,
    HANDOFF_BYTES_READ,
    HANDOFF_BYTES_WRITTEN,
    LAMBDA_INVOCATIONS,
    LAMBDA_MB_MILLIS,
];

/// A count as `i64` — wider integers fall through to `Debug` and arrive as
/// a *string*, which `OpenTelemetry` cannot treat as a measurement.
pub fn count<T: TryInto<i64>>(n: T) -> i64 {
    n.try_into().unwrap_or(i64::MAX)
}

/// Milliseconds as `i64`, for the same reason as [`count`].
///
/// `OpenTelemetry`'s visitor implements `record_i64` but not `record_u64`.
#[must_use]
pub fn millis(d: std::time::Duration) -> i64 {
    count(d.as_millis())
}

/// Live counters for one operation, shared by every store handle and worker
/// working on its behalf.
///
/// Cheap enough to be unconditional: a handful of relaxed atomic adds against
/// work measured in object-store round trips.
#[derive(Debug, Default)]
pub struct Meter {
    primary: StoreCounters,
    handoff: StoreCounters,
    lambda_invocations: AtomicU64,
    lambda_mb_millis: AtomicU64,
}

impl Meter {
    /// A fresh meter, at zero.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn store(&self, role: StoreRole) -> &StoreCounters {
        match role {
            StoreRole::Primary => &self.primary,
            StoreRole::Handoff => &self.handoff,
        }
    }

    /// Fold in units another process reported, so the front door's total
    /// covers what its worker spent as well as what it spent itself.
    pub fn add(&self, units: Units) {
        self.primary.add(units.primary);
        self.handoff.add(units.handoff);
        self.lambda_invocations
            .fetch_add(units.lambda.invocations, Ordering::Relaxed);
        self.lambda_mb_millis
            .fetch_add(units.lambda.mb_millis, Ordering::Relaxed);
    }

    /// Read the counters as they stand.
    #[must_use]
    pub fn units(&self) -> Units {
        Units {
            primary: self.primary.units(),
            handoff: self.handoff.units(),
            lambda: LambdaUnits {
                invocations: self.lambda_invocations.load(Ordering::Relaxed),
                mb_millis: self.lambda_mb_millis.load(Ordering::Relaxed),
            },
        }
    }
}

/// The live form of [`StoreUnits`], relaxed throughout — independent tallies
/// read once at the end, with no ordering between them worth paying for.
#[derive(Debug, Default)]
struct StoreCounters {
    get_class: AtomicU64,
    put_class: AtomicU64,
    deletes: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

impl StoreCounters {
    fn units(&self) -> StoreUnits {
        StoreUnits {
            get_class: self.get_class.load(Ordering::Relaxed),
            put_class: self.put_class.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
        }
    }

    fn add(&self, units: StoreUnits) {
        self.get_class.fetch_add(units.get_class, Ordering::Relaxed);
        self.put_class.fetch_add(units.put_class, Ordering::Relaxed);
        self.deletes.fetch_add(units.deletes, Ordering::Relaxed);
        self.bytes_read
            .fetch_add(units.bytes_read, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(units.bytes_written, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A push spends on both sides of the wire: the front door meters its own
    /// handoff writes as they happen, then folds in what the worker reports.
    #[test]
    fn adding_a_workers_units_sums_both_ends() {
        let meter = Meter::new();
        // What the front door spent staging the pack, counted locally.
        meter.add(Units {
            handoff: StoreUnits {
                put_class: 2,
                bytes_written: 512,
                ..StoreUnits::default()
            },
            ..Units::default()
        });
        // What the worker reported once it had run.
        meter.add(Units {
            primary: StoreUnits {
                put_class: 3,
                bytes_written: 100,
                ..StoreUnits::default()
            },
            lambda: LambdaUnits {
                invocations: 1,
                mb_millis: 2048,
            },
            ..Units::default()
        });

        let units = meter.units();
        assert_eq!(units.handoff.put_class, 2);
        assert_eq!(units.handoff.bytes_written, 512);
        assert_eq!(units.primary.put_class, 3);
        assert_eq!(units.primary.bytes_written, 100);
        assert_eq!(units.lambda.invocations, 1);
        assert_eq!(units.lambda.mb_millis, 2048);
    }
}
