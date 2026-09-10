//! The algebra a segmented store rests on: a join, a key range, a framing
//! and a compaction plan.
//!
//! Pure — no database, no bucket, no git. One value is held as segments,
//! each covering a range of a dense integer key space; a reader composes the
//! segments touching the range it wants, and a compactor joins segments into
//! larger ones. Both are the same [`Join`], and requiring it to be
//! associative, commutative and idempotent is what removes coordination from
//! everything above: compaction is correct under any grouping, correct when
//! retried, and correct against a segment that arrives late inside a range
//! already merged. Where a segment's bytes live is a consequence of its size
//! rather than a separate design — [`plan`] says what to merge and
//! [`Policy::residence_for`] says where the result belongs. [`frame`] is the
//! shape the bytes take: a record per key, and a [`tail`] for what varies.
//! See [`conformance`] for checking a new segment type against the whole
//! contract, and [`laws`] for the three laws alone.

pub mod conformance;
pub mod frame;
mod join;
mod key;
pub mod laws;
mod plan;
mod segment;
pub mod tail;

pub use frame::{Frame, Header, Layout};
pub use join::{Join, compose};
pub use key::{EmptyRange, Key, KeyRange, cluster};
pub use plan::{Merge, Placement, Policy, Residence, Tier, plan};
pub use segment::Segment;
