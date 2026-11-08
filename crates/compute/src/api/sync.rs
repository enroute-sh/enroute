//! `SyncService`: the compute's git client speaks to the remote, from one
//! snapshot.

use tonic::{Code, Request, Response, Status};

use enroute_api::api::v1alpha1::sync_service_server::SyncService;
use enroute_api::api::v1alpha1::{
    FetchFromRemoteRequest, FetchFromRemoteResponse, PushToRemoteRequest, PushToRemoteResponse,
    RefFetchOutcome, RefPushOutcome, RefSpec as WireRefSpec, Remote as WireRemote,
    ref_fetch_outcome::Status as WireFetchStatus, ref_push_outcome::Status as WirePushStatus,
    remote::Credentials,
};

use crate::contract::wire::wire_oid_or_unset;
use crate::protocol::receive_pack::PushCommand;
use crate::push::Refusal;
use crate::remote::{self, Error};

use super::Api;

#[tonic::async_trait]
impl SyncService for Api {
    #[tracing::instrument(name = "enroute::grpc::push_to_remote", skip(self, request))]
    async fn push_to_remote(
        &self,
        request: Request<PushToRemoteRequest>,
    ) -> Result<Response<PushToRemoteResponse>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        let push = parse_push(request)?;
        let outcomes = self
            .remotes
            .push(&snap, &push)
            .await
            .map_err(|error| sync_failed(&error))?;
        Ok(Response::new(PushToRemoteResponse {
            outcomes: outcomes.iter().map(to_wire_push_outcome).collect(),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::fetch_from_remote", skip(self, request))]
    async fn fetch_from_remote(
        &self,
        request: Request<FetchFromRemoteRequest>,
    ) -> Result<Response<FetchFromRemoteResponse>, Status> {
        let request = request.into_inner();
        let (key, snap) = self.snapshot(request.repo.as_ref()).await?;
        let fetch = remote::FetchRequest {
            remote: parse_remote(
                request
                    .remote
                    .ok_or_else(|| Status::invalid_argument("no remote given"))?,
            ),
            prefixes: request.prefixes,
        };
        let remote::Fetched {
            changes,
            pack,
            failure,
        } = self
            .remotes
            .fetch(&snap, &fetch)
            .await
            .map_err(|error| sync_failed(&error))?;
        if changes.is_empty() {
            return Ok(Response::new(FetchFromRemoteResponse::default()));
        }

        let landed = self
            .compute
            .land_commands(key.as_str(), snap.format(), changes, false, pack)
            .await;
        // A pack cut off by the remote fails the landing too; the remote's
        // error is the one the caller can act on.
        if let Some(error) = failure.take() {
            return Err(sync_failed(&error));
        }
        let landed = match landed {
            Ok(Ok(landed)) => landed,
            Ok(Err(why)) => {
                return Err(Status::aborted(format!(
                    "this repository refused the remote's pack: {why}"
                )));
            }
            Err(error) => return Err(super::internal(&error)),
        };
        Ok(Response::new(FetchFromRemoteResponse {
            outcomes: landed
                .into_iter()
                .map(|(change, refusal)| to_wire_fetch_outcome(change, refusal))
                .collect(),
        }))
    }
}

/// The push a call asks for.
fn parse_push(request: PushToRemoteRequest) -> Result<remote::PushRequest, Status> {
    let remote = request
        .remote
        .ok_or_else(|| Status::invalid_argument("no remote given"))?;

    Ok(remote::PushRequest {
        remote: parse_remote(remote),
        refs: request.refs.into_iter().map(parse_refspec).collect(),
        atomic: request.atomic,
    })
}

fn parse_remote(remote: WireRemote) -> remote::Remote {
    remote::Remote {
        url: remote.url,
        credentials: remote.credentials.map(|credentials| match credentials {
            Credentials::Basic(basic) => remote::Credentials::Basic {
                username: basic.username,
                password: basic.password,
            },
            Credentials::BearerToken(token) => remote::Credentials::Bearer(token),
        }),
    }
}

fn parse_refspec(spec: WireRefSpec) -> remote::RefSpec {
    remote::RefSpec {
        source: spec.source,
        destination: spec.destination,
        force: spec.force,
    }
}

/// One ref's outcome on the remote.
///
/// A ref the remote refused is data, not a failure: a push can land some refs
/// and be refused others.
fn to_wire_push_outcome(outcome: &remote::RefOutcome) -> RefPushOutcome {
    let (status, message) = match outcome.status {
        remote::PushStatus::Updated => (WirePushStatus::Updated, String::new()),
        remote::PushStatus::Deleted => (WirePushStatus::Deleted, String::new()),
        remote::PushStatus::UpToDate => (WirePushStatus::UpToDate, String::new()),
        remote::PushStatus::Rejected(ref why) => (WirePushStatus::Rejected, why.clone()),
    };
    RefPushOutcome {
        destination: outcome.destination.clone(),
        old_object_id: wire_oid_or_unset(outcome.old_id),
        new_object_id: wire_oid_or_unset(outcome.new_id),
        status: status.into(),
        message,
    }
}

/// One ref's outcome here.
fn to_wire_fetch_outcome(change: PushCommand, refusal: Option<Refusal>) -> RefFetchOutcome {
    let (status, message) = match refusal {
        Some(why) => (WireFetchStatus::Rejected, why.text().to_string()),
        None if change.new.is_zero() => (WireFetchStatus::Deleted, String::new()),
        None => (WireFetchStatus::Updated, String::new()),
    };
    RefFetchOutcome {
        name: change.name,
        old_object_id: wire_oid_or_unset(change.old),
        new_object_id: wire_oid_or_unset(change.new),
        status: status.into(),
        message,
    }
}

/// A push or a fetch that never got as far as an outcome.
///
/// The remote's own words reach the caller, who holds the relationship with
/// it — unlike a storage failure, which [`super::internal`] keeps.
fn sync_failed(error: &Error) -> Status {
    let code = match error {
        Error::Request(_) => Code::InvalidArgument,
        Error::Remote(_) | Error::Protocol(_) | Error::Unsupported(_) => Code::FailedPrecondition,
        Error::Unauthorized => Code::PermissionDenied,
        Error::PackRefused(_) => Code::Aborted,
        // `Unknown` on purpose: the push reached the remote, so what landed
        // is not something this call can say either way any more.
        Error::ReportUnread(_) => Code::Unknown,
        Error::Transport(_) => Code::Unavailable,
        Error::Storage(_) => {
            tracing::error!(%error, "sync with a remote failed");
            return Status::internal("internal error");
        }
    };
    Status::new(code, error.to_string())
}
