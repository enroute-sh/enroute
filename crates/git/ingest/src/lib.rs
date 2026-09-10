//! Push/write-path orchestration: staged pack ingestion, commit attribution,
//! connectivity checking, and ref-update classification/application.
//!
//! Depends on `enroute-git-store`/`enroute-git-metadata` so `enroute-git-graph`
//! can stay a pure in-memory algorithm crate.

mod ancestry;
mod append;
mod classify;
mod concurrency;
mod connectivity;
mod delta_plan;
mod hooks;
mod materialise;
mod object_io;
mod pack;
mod pool;
mod progress;
mod rebuild;
mod ref_updates;
mod resolve;
mod session;
mod staging;
#[cfg(test)]
mod test_helpers;
mod timing;
mod upload;
mod worker;

pub use progress::{IngestProgress, ProgressSink, noop_progress};
pub use ref_updates::{Applied, Ingested, PushRejection, RefUpdateOutcome, apply_ref_updates};

pub use append::{Engine, append};
pub use hooks::{Actor, NoHooks, ReceiveHooks, RefCommand, RefJudgement, Verdict};
pub use object_io::KnownIdentities;
pub use worker::{IncomingPack, IngestRequest, IngestWorker, LocalIngestWorker, PackReader};
