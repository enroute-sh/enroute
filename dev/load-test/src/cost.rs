//! Turns [`StoreUnits`] into dollar figures, using hardcoded AWS pricing
//! (us-east-1).
//!
//! S3 Standard: PUT/COPY/POST/LIST $0.005/1,000; GET/HEAD $0.0004/1,000;
//!   storage $0.023/GB-month; no per-GB data transfer charge.
//! S3 Express One Zone: PUT $0.00113/1,000; GET $0.00003/1,000;
//!   $0.0032/GB uploaded; $0.0006/GB downloaded; storage $0.11/GB-month.
//! Aurora `PostgreSQL` (Standard configuration, on-demand):
//!   storage $0.10/GB-month; I/O $0.20/million requests.
//! All figures are published list prices, not negotiated rates, and drift
//!   over time — treat results as order-of-magnitude, not an invoice.

use std::fmt::{self, Display, Formatter};

use enroute_git_cost::StoreUnits;

// ── pricing ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub(crate) enum Tier {
    Standard,
    ExpressOneZone,
}

struct TierPricing {
    put_class_per_request: f64,
    get_class_per_request: f64,
    storage_per_gb_month: f64,
    upload_per_gb: f64,
    download_per_gb: f64,
}

impl Tier {
    fn pricing(self) -> TierPricing {
        match self {
            Tier::Standard => TierPricing {
                put_class_per_request: 0.005 / 1_000.0,
                get_class_per_request: 0.0004 / 1_000.0,
                storage_per_gb_month: 0.023,
                upload_per_gb: 0.0,
                download_per_gb: 0.0,
            },
            Tier::ExpressOneZone => TierPricing {
                put_class_per_request: 0.00113 / 1_000.0,
                get_class_per_request: 0.00003 / 1_000.0,
                storage_per_gb_month: 0.11,
                upload_per_gb: 0.0032,
                download_per_gb: 0.0006,
            },
        }
    }
}

const BYTES_PER_GB: f64 = 1_000_000_000.0;

// ── single-tier S3 cost ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub(crate) struct CostReport {
    pub units: StoreUnits,
    pub get_class_cost: f64,
    pub put_class_cost: f64,
    pub transfer_cost: f64,
    tier: Tier,
}

impl CostReport {
    #[must_use]
    pub(crate) fn compute(units: StoreUnits, tier: Tier) -> Self {
        let p = tier.pricing();
        let uploaded_gb = units.bytes_written as f64 / BYTES_PER_GB;
        let downloaded_gb = units.bytes_read as f64 / BYTES_PER_GB;
        Self {
            units,
            tier,
            get_class_cost: units.get_class as f64 * p.get_class_per_request,
            put_class_cost: units.put_class as f64 * p.put_class_per_request,
            transfer_cost: uploaded_gb * p.upload_per_gb + downloaded_gb * p.download_per_gb,
        }
    }

    #[must_use]
    pub(crate) fn request_cost(&self) -> f64 {
        self.get_class_cost + self.put_class_cost + self.transfer_cost
    }
}

// ── Aurora (Postgres) cost ─────────────────────────────────────────────────────

const AURORA_STORAGE_PER_GB_MONTH: f64 = 0.10;
const AURORA_READ_PER_MILLION_REQUESTS: f64 = 0.20;

/// Storage cost for the Aurora-backed metadata store, measured directly
/// from Postgres rather than derived from request counts like S3 above.
///
/// `bytes` is already net of the empty schema's fixed DDL overhead (see
/// `baseline_bytes` in `main.rs`).
#[derive(Debug, Clone, Copy)]
pub(crate) struct AuroraStorageReport {
    pub bytes: u64,
}

impl AuroraStorageReport {
    fn cost_per_month(self) -> f64 {
        self.bytes as f64 / BYTES_PER_GB * AURORA_STORAGE_PER_GB_MONTH
    }
}

/// Read-side I/O cost for the Aurora-backed metadata store, derived from
/// `pg_statio_user_tables` block-read counters against this run's schema.
///
/// Deliberately read-only, excluding the WAL-driven writes Aurora also
/// bills — not obtainable per-schema, so not folded into [`DataReport`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct AuroraReadsReport {
    pub reads: u64,
}

impl AuroraReadsReport {
    fn cost(self) -> f64 {
        self.reads as f64 / 1_000_000.0 * AURORA_READ_PER_MILLION_REQUESTS
    }

    /// Average this report's reads over `clones`, for a per-clone estimate
    /// alongside the other backends' [`SectionReport::per_clone`].
    #[must_use]
    pub(crate) fn per_clone(self, clones: u64) -> Option<AveragedAuroraReadsReport> {
        if clones == 0 {
            return None;
        }
        Some(AveragedAuroraReadsReport {
            reads: self.reads as f64 / clones as f64,
        })
    }
}

impl Display for AuroraReadsReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Widths hand-tuned to line up the `$` column with SectionReport's rows.
        write!(
            f,
            "  {:<26} {:>10} physical reads{:>18}  ${:.6}",
            "Aurora Reads:",
            self.reads,
            "",
            self.cost(),
        )
    }
}

/// Per-clone average of [`AuroraReadsReport`], mirroring [`AveragedSectionReport`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct AveragedAuroraReadsReport {
    reads: f64,
}

impl AveragedAuroraReadsReport {
    fn cost(self) -> f64 {
        self.reads / 1_000_000.0 * AURORA_READ_PER_MILLION_REQUESTS
    }
}

impl Display for AveragedAuroraReadsReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // Wider than AuroraReadsReport's to match AveragedSectionReport's GET/PUT columns.
        write!(
            f,
            "  {:<26} {:>10.1} physical reads{:>22}  ${:.6}",
            "Aurora Reads:",
            self.reads,
            "",
            self.cost(),
        )
    }
}

// ── section report (seed / clone) ─────────────────────────────────────────────

/// One section of the cost report (seed or clone): three backends + total row.
///
/// Only covers request/transfer costs; storage costs live in [`DataReport`].
pub(crate) struct SectionReport {
    primary: CostReport,
    staging: CostReport,
}

impl SectionReport {
    pub(crate) fn new(primary: StoreUnits, staging: StoreUnits) -> Self {
        Self {
            primary: CostReport::compute(primary, Tier::Standard),
            staging: CostReport::compute(staging, Tier::ExpressOneZone),
        }
    }

    fn total_request_cost(&self) -> f64 {
        self.primary.request_cost() + self.staging.request_cost()
    }

    pub(crate) fn per_clone(&self, clones: u64) -> Option<AveragedSectionReport> {
        if clones == 0 {
            return None;
        }
        let n = clones as f64;
        Some(AveragedSectionReport {
            primary: AveragedCostReport::from(&self.primary, n),
            staging: AveragedCostReport::from(&self.staging, n),
            total_request_cost: self.total_request_cost() / n,
        })
    }
}

impl Display for SectionReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", S3Line(&self.primary))?;
        writeln!(f, "{}", S3Line(&self.staging))?;
        write!(
            f,
            "  {:<26} {:>43}  ${:.6}",
            "total:",
            "",
            self.total_request_cost()
        )
    }
}

// ── averaged section report (per clone) ──────────────────────────────────────

pub(crate) struct AveragedSectionReport {
    primary: AveragedCostReport,
    staging: AveragedCostReport,
    total_request_cost: f64,
}

impl Display for AveragedSectionReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.primary)?;
        writeln!(f, "{}", self.staging)?;
        write!(
            f,
            "  {:<26} {:>47}  ${:.6}",
            "total:", "", self.total_request_cost
        )
    }
}

// ── data report (storage per month) ──────────────────────────────────────────

pub(crate) struct DataReport {
    pub s3_standard: u64,
    pub s3_express: u64,
    pub aurora: AuroraStorageReport,
}

impl DataReport {
    fn s3_standard_cost(&self) -> f64 {
        self.s3_standard as f64 / BYTES_PER_GB * Tier::Standard.pricing().storage_per_gb_month
    }

    fn s3_express_cost(&self) -> f64 {
        self.s3_express as f64 / BYTES_PER_GB * Tier::ExpressOneZone.pricing().storage_per_gb_month
    }

    fn total_cost(&self) -> f64 {
        self.s3_standard_cost() + self.s3_express_cost() + self.aurora.cost_per_month()
    }
}

impl Display for DataReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "  {:<26} {:>10}{:>47}  ${:.6}/mo",
            "S3 Standard:",
            format_bytes(self.s3_standard as f64),
            "",
            self.s3_standard_cost(),
        )?;
        writeln!(
            f,
            "  {:<26} {:>10}{:>47}  ${:.6}/mo",
            "S3 Express One Zone:",
            format_bytes(self.s3_express as f64),
            "",
            self.s3_express_cost(),
        )?;
        writeln!(
            f,
            "  {:<26} {:>10}{:>47}  ${:.6}/mo",
            "Aurora (Postgres):",
            format_bytes(self.aurora.bytes as f64),
            "",
            self.aurora.cost_per_month(),
        )?;
        write!(
            f,
            "  {:<26} {:>57}  ${:.6}/mo",
            "total:",
            "",
            self.total_cost()
        )
    }
}

// ── formatting helpers ────────────────────────────────────────────────────────

struct S3Line<'a>(&'a CostReport);

impl Display for S3Line<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let r = self.0;
        let u = &r.units;
        let label = match r.tier {
            Tier::Standard => "S3 Standard:",
            Tier::ExpressOneZone => "S3 Express One Zone:",
        };
        write!(
            f,
            "  {:<26} {:>4} GET {:>4} PUT {:>10} ↑ {:>10} ↓  ${:.6}",
            label,
            u.get_class,
            u.put_class,
            format_bytes(u.bytes_written as f64),
            format_bytes(u.bytes_read as f64),
            r.request_cost(),
        )
    }
}

struct AveragedCostReport {
    tier: Tier,
    get_requests: f64,
    put_requests: f64,
    bytes_uploaded: f64,
    bytes_downloaded: f64,
    request_cost: f64,
}

impl AveragedCostReport {
    fn from(r: &CostReport, n: f64) -> Self {
        let u = &r.units;
        Self {
            tier: r.tier,
            get_requests: u.get_class as f64 / n,
            put_requests: u.put_class as f64 / n,
            bytes_uploaded: u.bytes_written as f64 / n,
            bytes_downloaded: u.bytes_read as f64 / n,
            request_cost: r.request_cost() / n,
        }
    }
}

impl Display for AveragedCostReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let label = match self.tier {
            Tier::Standard => "S3 Standard:",
            Tier::ExpressOneZone => "S3 Express One Zone:",
        };
        write!(
            f,
            "  {:<26} {:>6.1} GET {:>6.1} PUT {:>10} ↑ {:>10} ↓  ${:.6}",
            label,
            self.get_requests,
            self.put_requests,
            format_bytes(self.bytes_uploaded),
            format_bytes(self.bytes_downloaded),
            self.request_cost,
        )
    }
}

fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    let Some(suffix) = UNITS.get(unit) else {
        return format!("{bytes} B");
    };
    format!("{value:.2} {suffix}")
}
