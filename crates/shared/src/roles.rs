//! The interface between the roles is the `Ledger` gRPC service
//! (`proto/ledger.proto`): the ledger implements its generated trait
//! ([`ledger_server::Ledger`]); in a node running both roles the compute
//! calls it in-process, and a compute-only node through gRPC (an
//! implementation of the same trait that forwards to ledger nodes). Following
//! a repository is [`Follow`], one subscription of the service's `Watch`
//! call. This module has the trait object the compute holds, and plain-typed
//! calls on it.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;

use crate::oid::ObjectFormat;
use crate::proto::{
    ActivateRequest, DeleteRequest, DescribeRequest, ListRequest, ListResponse, PendingRepo,
    PendingRequest, PendingResponse, ReadBarrierRequest, RepoSummary, Update, WatchEvent,
    ledger_server,
};
use crate::state::{Command, Outcome};

/// One subscription's updates.
pub type FollowStream = BoxStream<'static, Result<Update, tonic::Status>>;

/// The response stream of the service's `Watch` call.
pub type WatchStream = BoxStream<'static, Result<WatchEvent, tonic::Status>>;

/// Follow one repository, as one subscription of the `Watch` call.
///
/// The compute calls it on the ledger in its own process directly, and a
/// remote ledger subscribes on a shared `Watch` stream.
#[tonic::async_trait]
pub trait Follow: Send + Sync + 'static {
    /// The state of `repo` (its log must be live), then every change to it.
    async fn follow(&self, repo: &str) -> Result<FollowStream, tonic::Status>;
}

/// The `Ledger` service, and [`Follow`].
pub trait Role: ledger_server::Ledger<WatchStream = WatchStream> + Follow {}

impl<T: ledger_server::Ledger<WatchStream = WatchStream> + Follow> Role for T {}

/// A ledger, in-process or remote.
pub type DynLedger = dyn Role;

/// A subscription's updates (see [`follow`]).
pub type Updates = BoxStream<'static, anyhow::Result<Update>>;

/// The error of a request for a repository whose log the ledger node asked
/// does not host, or for whose incarnation it is not a replica: another node
/// may serve it.
#[derive(Debug, Clone, Copy)]
pub struct NotHosted;

impl std::fmt::Display for NotHosted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("repository is not active on this node")
    }
}

impl std::error::Error for NotHosted {}

/// The status code of [`NotHosted`], so a caller across gRPC can tell it.
const NOT_HOSTED: tonic::Code = tonic::Code::FailedPrecondition;

/// An error as a status (its message, with causes).
#[must_use]
#[expect(
    clippy::needless_pass_by_value,
    reason = "used as a `map_err` function, which passes its error by value"
)]
pub fn status(e: anyhow::Error) -> tonic::Status {
    let code = if not_hosted(&e) {
        NOT_HOSTED
    } else {
        tonic::Code::Internal
    };
    tonic::Status::new(code, format!("{e:#}"))
}

/// Whether the status says the node asked does not host the repository.
#[must_use]
pub fn not_hosted_status(s: &tonic::Status) -> bool {
    s.code() == NOT_HOSTED
}

/// The metadata key of a [`not_sequencer_status`]: the sequencer's internal URL.
const SEQUENCER: &str = "enroute-sequencer";

/// The refusal of a request only the sequencer of the repository's log serves
/// (a proposal, a deletion), by another node: unavailable (the request did
/// not reach the log), naming the internal URL of the `sequencer` this node
/// knows (empty: none).
#[must_use]
pub fn not_sequencer_status(message: String, sequencer: &str) -> tonic::Status {
    let mut s = tonic::Status::unavailable(message);
    if let Ok(v) = sequencer.parse() {
        s.metadata_mut().insert(SEQUENCER, v);
    }
    s
}

/// The sequencer a [`not_sequencer_status`] names (empty: none known); None for
/// any other status.
#[must_use]
pub fn sequencer_named(s: &tonic::Status) -> Option<String> {
    Some(s.metadata().get(SEQUENCER)?.to_str().ok()?.to_string())
}

/// Whether `e` says the ledger node asked does not host the repository,
/// in this process ([`NotHosted`]) or across gRPC (its status).
#[must_use]
pub fn not_hosted(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.is::<NotHosted>()
            || c.downcast_ref::<tonic::Status>()
                .is_some_and(not_hosted_status)
    })
}

/// A status as an error: its message, keeping the status (for callers that
/// look at its code).
#[must_use]
pub fn error(s: tonic::Status) -> anyhow::Error {
    let message = s.message().to_string();
    anyhow::Error::new(s).context(message)
}

/// Make `repo`'s log live, waking it from the object store or creating it
/// (`create`). False: it does not exist.
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn activate(
    ledger: &DynLedger,
    repo: &str,
    create: Option<ObjectFormat>,
) -> anyhow::Result<bool> {
    let req = ActivateRequest {
        repo: repo.to_string(),
        create: create.is_some(),
        head: None,
        object_format: crate::proto::ObjectFormat::from(create.unwrap_or_default()).into(),
    };
    Ok(ledger
        .activate(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner()
        .exists)
}

/// Create `repo` with HEAD `head`, its objects named in `format`, unless it
/// exists (then it is left as it is).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn create(
    ledger: &DynLedger,
    repo: &str,
    head: &str,
    format: ObjectFormat,
) -> anyhow::Result<()> {
    let req = ActivateRequest {
        repo: repo.to_string(),
        create: true,
        head: Some(head.to_string()),
        object_format: crate::proto::ObjectFormat::from(format).into(),
    };
    ledger
        .activate(tonic::Request::new(req))
        .await
        .map_err(error)?;
    Ok(())
}

/// Delete `repo` (see `Delete` in `ledger.proto`).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn delete(ledger: &DynLedger, repo: &str) -> anyhow::Result<()> {
    let req = DeleteRequest {
        repo: repo.to_string(),
    };
    ledger
        .delete(tonic::Request::new(req))
        .await
        .map_err(error)?;
    Ok(())
}

/// `repo` as of everything committed before the call (None: it does not
/// exist).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn describe(ledger: &DynLedger, repo: &str) -> anyhow::Result<Option<RepoSummary>> {
    let req = DescribeRequest {
        repo: repo.to_string(),
    };
    Ok(ledger
        .describe(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner()
        .repo)
}

/// Repositories named with `prefix`, after `after`, at most `limit` (see
/// `List` in `ledger.proto`).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn list(
    ledger: &DynLedger,
    prefix: &str,
    after: &str,
    limit: u32,
) -> anyhow::Result<ListResponse> {
    let req = ListRequest {
        prefix: prefix.to_string(),
        after: after.to_string(),
        limit,
    };
    Ok(ledger
        .list(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner())
}

/// Repositories that may need maintenance and that compute node `compute`
/// maintains, after settling `settled`, and the live computes (see
/// `Pending` in `ledger.proto`).
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn pending(
    ledger: &DynLedger,
    compute: u64,
    settled: Vec<PendingRepo>,
) -> anyhow::Result<PendingResponse> {
    let req = PendingRequest { compute, settled };
    Ok(ledger
        .pending(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner())
}

/// Order and apply `cmd` in its repository's log; resolves with the outcome
/// once applied.
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn propose(ledger: &DynLedger, cmd: Command) -> anyhow::Result<Outcome> {
    Ok(ledger
        .propose(tonic::Request::new(cmd))
        .await
        .map_err(error)?
        .into_inner())
}

/// Wait until everything committed to `repo`'s log before the call has been
/// applied on the ledger; returns the repository's version then, which a
/// follower waits for its own state to reach.
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn read_barrier(ledger: &DynLedger, repo: &str) -> anyhow::Result<u64> {
    let req = ReadBarrierRequest {
        repo: repo.to_string(),
    };
    Ok(ledger
        .read_barrier(tonic::Request::new(req))
        .await
        .map_err(error)?
        .into_inner()
        .version)
}

/// Follow `repo` (its log must be live): its state, then every change to it.
///
/// # Errors
///
/// Returns an error if the ledger request fails.
pub async fn follow(ledger: &DynLedger, repo: &str) -> anyhow::Result<Updates> {
    let stream = Follow::follow(ledger, repo).await.map_err(error)?;
    Ok(stream.map(|u| u.map_err(error)).boxed())
}

/// A shared ledger.
pub type LedgerRef = Arc<DynLedger>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_hosted_keeps_its_type_across_grpc() {
        let e = anyhow::Error::new(NotHosted).context("not a replica of incarnation 3");
        assert!(not_hosted(&e));
        let s = status(e);
        assert!(not_hosted_status(&s));
        assert!(s.message().contains("not a replica"), "{s:?}");
        assert!(not_hosted(&error(s)));
        let s = not_sequencer_status("not the sequencer".into(), "http://l2:7400");
        assert_eq!(s.code(), tonic::Code::Unavailable);
        assert_eq!(sequencer_named(&s).as_deref(), Some("http://l2:7400"));
        assert_eq!(sequencer_named(&tonic::Status::unavailable("down")), None);
        let other = status(anyhow::anyhow!("disk full"));
        assert_eq!(other.code(), tonic::Code::Internal);
        assert!(!not_hosted(&error(other)));
    }
}
