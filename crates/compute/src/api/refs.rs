//! `RefService`.

use std::time::{Duration, UNIX_EPOCH};

use tonic::{Request, Response, Status};

use enroute_api::api::v1alpha1::ref_service_server::RefService;
use enroute_api::api::v1alpha1::ref_update_outcome::Rejection;
use enroute_api::api::v1alpha1::ref_update_outcome::rejection::Reason;
use enroute_api::api::v1alpha1::{
    IsAncestorRequest, IsAncestorResponse, ListRefsRequest, ListRefsResponse, Ref,
    RefUpdateOutcome, UpdateRefsRequest, UpdateRefsResponse,
};

use crate::access::Hidden;
use crate::contract::wire::{optional_oid, require_oid, wire_oid, wire_system_time};
use crate::protocol::receive_pack::PushCommand;
use crate::push::{BadCommand, Refusal};
use crate::query::{self, RefView};
use crate::remote;

use super::Api;

#[tonic::async_trait]
impl RefService for Api {
    #[tracing::instrument(name = "enroute::grpc::list_refs", skip(self, request))]
    async fn list_refs(
        &self,
        request: Request<ListRefsRequest>,
    ) -> Result<Response<ListRefsResponse>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        // The API's caller is the application, which sees every ref.
        let view = RefView::new(&snap.meta, &Hidden::default(), &request.prefixes);
        let refs = view
            .refs
            .into_iter()
            .map(|(name, oid)| Ref {
                name: name.to_string(),
                object_id: Some(wire_oid(oid)),
                updated_at: wire_system_time(
                    snap.meta
                        .ref_updated_ms
                        .get(name)
                        .map(|ms| UNIX_EPOCH + Duration::from_millis(*ms)),
                ),
            })
            .collect();
        Ok(Response::new(ListRefsResponse {
            refs,
            default_branch: snap.meta.head.clone(),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::update_refs", skip(self, request))]
    async fn update_refs(
        &self,
        request: Request<UpdateRefsRequest>,
    ) -> Result<Response<UpdateRefsResponse>, Status> {
        let request = request.into_inner();
        // A missing repository wins over a malformed update.
        let (key, snap) = self.snapshot(request.repo.as_ref()).await?;
        let format = snap.format();
        let commands = request
            .updates
            .into_iter()
            .map(|update| {
                let old = optional_oid(update.old_object_id.as_ref())?;
                let new = optional_oid(update.new_object_id.as_ref())?;
                if old.is_none() && new.is_none() {
                    return Err(Status::invalid_argument(format!(
                        "deleting {} needs its old_object_id",
                        update.refname
                    )));
                }
                Ok(PushCommand {
                    name: update.refname,
                    old: old.unwrap_or(format.zero()),
                    new: new.unwrap_or(format.zero()),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let landed = self
            .compute
            .land_commands(
                key.as_str(),
                format,
                commands,
                request.atomic,
                None::<remote::PackStream>,
            )
            .await
            .map_err(|error| super::internal(&error))?
            .map_err(|why| Status::internal(format!("no pack, yet refused: {why}")))?;
        // A repository deleted while the call landed is not found, as for any
        // other call.
        if landed
            .iter()
            .any(|(_, refusal)| matches!(refusal, Some(Refusal::Gone(_))))
        {
            return Err(query::Error::NotFound.into());
        }
        Ok(Response::new(UpdateRefsResponse {
            outcomes: landed
                .into_iter()
                .map(|(command, refusal)| RefUpdateOutcome {
                    refname: command.name,
                    rejection: refusal.as_ref().map(rejection),
                })
                .collect(),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::is_ancestor", skip(self, request))]
    async fn is_ancestor(
        &self,
        request: Request<IsAncestorRequest>,
    ) -> Result<Response<IsAncestorResponse>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        let ancestor = require_oid(request.ancestor_commit_id.as_ref(), "ancestor_commit_id")?;
        let descendant = require_oid(
            request.descendant_commit_id.as_ref(),
            "descendant_commit_id",
        )?;
        let is_ancestor = query::is_ancestor(&snap, ancestor, descendant)
            .await
            .map_err(query::Error::from)?;
        Ok(Response::new(IsAncestorResponse { is_ancestor }))
    }
}

/// A refusal as the API reports it.
fn rejection(refusal: &Refusal) -> Rejection {
    let reason = match refusal {
        Refusal::Bad(BadCommand::Name) => Reason::InvalidRefname,
        Refusal::Bad(BadCommand::Target(_)) => Reason::MissingObjects,
        Refusal::Bad(BadCommand::Repeated) => Reason::RepeatedRef,
        Refusal::Bad(BadCommand::DeleteCurrent) => Reason::DeleteCurrent,
        Refusal::Exists => Reason::AlreadyExists,
        Refusal::Stale => Reason::NonFastForward,
        Refusal::Atomic => Reason::AtomicFailure,
        Refusal::Gone(_) | Refusal::Said(_) => Reason::Unspecified,
    };
    Rejection {
        reason: reason.into(),
        message: if reason == Reason::Unspecified {
            refusal.text().to_string()
        } else {
            String::new()
        },
    }
}
