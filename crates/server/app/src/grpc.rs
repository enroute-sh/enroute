//! The wire contract, served.
//!
//! Every method is a thin translation to the engine: this layer knows
//! repositories by the key the application gave them, and the engine by a
//! `RepoId` that never reaches a caller. [`Api::resolve`] is the only thing
//! that turns one into the other. Nothing here authenticates, the application
//! being the only caller: whether it may reach this port is the deployment's
//! to answer — see docs/operate/security.md.

use std::pin::Pin;
use std::sync::Arc;

use futures::StreamExt as _;
use gix_hash::ObjectId;
use tonic::{Request, Response, Status};

use enroute_api::api::v1alpha1::object_service_server::{ObjectService, ObjectServiceServer};
use enroute_api::api::v1alpha1::ref_service_server::{RefService, RefServiceServer};
use enroute_api::api::v1alpha1::repository_service_server::{
    RepositoryService, RepositoryServiceServer,
};
use enroute_api::api::v1alpha1::sync_service_server::{SyncService, SyncServiceServer};
use enroute_api::api::v1alpha1::{
    Commit as WireCommit, CreateRepositoryRequest, CreateRepositoryResponse,
    DeleteRepositoryRequest, DeleteRepositoryResponse, DiffCommitRequest, DiffCommitResponse,
    FileChange, FindMergeBasesRequest, FindMergeBasesResponse, GetObjectRequest, GetObjectResponse,
    GetRepositoryRequest, GetRepositoryResponse, Identity as WireIdentity, IsAncestorRequest,
    IsAncestorResponse, ListCommitsRequest, ListCommitsResponse, ListRefsRequest, ListRefsResponse,
    ListRepositoriesRequest, ListRepositoriesResponse, ListTreeRequest, ListTreeResponse,
    ObjectHeader, ObjectKind, Ref, RefUpdate as WireRefUpdate, RefUpdateOutcome as WireOutcome,
    Repository, TreeEntry, UpdateRefsRequest, UpdateRefsResponse, get_object_response,
    ref_update_outcome::Rejection, ref_update_outcome::rejection::Reason,
};
use enroute_api::api::v1alpha1::{
    PushToRemoteRequest, PushToRemoteResponse, RefPushOutcome, RefSpec as WireRefSpec,
    Remote as WireRemote, ref_push_outcome::Status as WirePushStatus, remote::Credentials,
};
use enroute_api::common::v1alpha1::RepoKey as WireRepoKey;
use enroute_git_core::{ObjectHashMap, RepoId};
use enroute_git_graph::{CommitDetails, DiffOptions, Identity, TreeDiff, commit_details};
use enroute_git_retrieve::{
    RefUpdate, RefUpdateRejection, RefUpdateResult, RepoMetadata, Storage, TreeError, TreeItem,
    TreeWalk,
};

use crate::repo_key::RepoKey;
use crate::wire::{
    oid_or_null, parse_hex, parse_oid, parse_repo, require_oid, wire_oid, wire_oid_or_unset,
    wire_time,
};

/// A server-streamed response.
///
/// Boxed because the streams these return are built from combinators whose
/// types are not nameable.
type ResponseStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send>>;

/// Everything Enroute serves on the contract port.
///
/// Named so a binary holding one can spell its type rather than infer it.
pub type Services = (
    RefServiceServer<Api>,
    RepositoryServiceServer<Api>,
    ObjectServiceServer<Api>,
    SyncServiceServer<Api>,
);

/// [`Services`] added to a `tonic` server, with reflection, ready to serve.
///
/// Here, not at each of the binary's/harness's callers: a service added to
/// the contract and not to this list would be served by some and not others.
///
/// # Errors
///
/// If the compiled-in descriptors do not resolve, which is a build fault and
/// not a deployment's.
pub fn router(
    services: Services,
) -> Result<tonic::transport::server::Router, tonic_reflection::server::Error> {
    let (refs, repositories, objects, sync) = services;

    let reflection = || {
        tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(enroute_api::FILE_DESCRIPTOR_SET)
    };

    Ok(tonic::transport::Server::builder()
        .add_service(refs)
        .add_service(repositories)
        .add_service(objects)
        .add_service(sync)
        // Two service names rather than one with a version negotiated: a
        // client too old to ask for `v1` asks `v1alpha`, and a server serving
        // only the other answers `Unimplemented`.
        .add_service(reflection().build_v1()?)
        .add_service(reflection().build_v1alpha()?))
}

/// The `enroute.api.v1alpha1` services, ready to be added to a `tonic` server.
///
/// Arguments, not things installed later: whoever reaches the contract is the
/// application, and which remotes a sync may dial the deployment's answer.
#[must_use]
pub fn services(state: Storage, remotes: enroute_git_remote::Client) -> Services {
    let api = Api {
        state,
        // Shared rather than cloned: the four services are one client's
        // connection pool, and `Client` is not `Clone` in any case.
        remotes: Arc::new(remotes),
    };
    (
        RefServiceServer::new(api.clone()),
        RepositoryServiceServer::new(api.clone()),
        ObjectServiceServer::new(api.clone()),
        SyncServiceServer::new(api),
    )
}

/// Everything the contract is served out of.
///
/// One type behind all four services rather than four alike ones, which
/// would be four copies of the same engine to keep in step.
#[derive(Debug, Clone)]
pub struct Api {
    state: Storage,
    remotes: Arc<enroute_git_remote::Client>,
}

impl Api {
    /// Turns the key a call arrived with into the repository the engine holds,
    /// failing the call if it names nothing here.
    ///
    /// One read: the key is a column on the repository it names.
    async fn resolve(&self, repo: Option<&WireRepoKey>) -> Result<RepoMetadata, Status> {
        self.held(&parse_repo(repo)?)
            .await?
            .ok_or_else(|| Status::not_found("no such repository"))
    }

    /// The same read for a caller that has an answer for `None` — a create
    /// repeated, or a delete of something already gone.
    async fn held(&self, key: &RepoKey) -> Result<Option<RepoMetadata>, Status> {
        self.state
            .rows
            .by_key(key.external())
            .await
            .map_err(|error| internal(&error))
    }
}

#[tonic::async_trait]
impl RefService for Api {
    #[tracing::instrument(name = "enroute::grpc::list_refs", skip(self, request))]
    async fn list_refs(
        &self,
        request: Request<ListRefsRequest>,
    ) -> Result<Response<ListRefsResponse>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;

        // `HEAD` is not a ref in the contract: it is derivable from
        // `default_branch`, and sending both would let them disagree. So this
        // reads the listing rather than the advertisement map, which
        // synthesizes one.
        let refs = self
            .state
            .rows
            .repo(repo.id)
            .ref_listing()
            .await
            .map_err(|error| internal(&error))?;

        let refs = refs
            .into_iter()
            .filter(|entry| matches(&request.prefixes, &entry.refname))
            .map(|entry| Ref {
                name: entry.refname,
                object_id: Some(wire_oid(entry.oid)),
                updated_at: Some(wire_time(entry.updated_unix_seconds)),
            })
            .collect();

        Ok(Response::new(ListRefsResponse {
            refs,
            default_branch: repo.default_branch,
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::update_refs", skip(self, request))]
    async fn update_refs(
        &self,
        request: Request<UpdateRefsRequest>,
    ) -> Result<Response<UpdateRefsResponse>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;
        let updates = parse_updates(&request.updates)?;

        // No pack, and no `pre-receive`: the application authored that hook, so
        // asking it to authorize its own call would catch only its own bugs.
        // What it cannot waive still holds — the ref store admits only objects
        // this repository has, so git remains the only way one arrives.
        let outcomes = enroute_git_retrieve::move_refs(&self.state, repo.id, &updates)
            .await
            .map_err(|error| internal(&error))?;

        Ok(Response::new(UpdateRefsResponse {
            outcomes: outcomes.iter().map(to_wire_outcome).collect(),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::is_ancestor", skip(self, request))]
    async fn is_ancestor(
        &self,
        request: Request<IsAncestorRequest>,
    ) -> Result<Response<IsAncestorResponse>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;

        let ancestor = require_oid(request.ancestor_commit_id.as_ref(), "ancestor_commit_id")?;
        let descendant = require_oid(
            request.descendant_commit_id.as_ref(),
            "descendant_commit_id",
        )?;

        let is_ancestor = self
            .state
            .graph
            .repo(repo.id)
            .is_ancestor(ancestor, descendant)
            .await
            .map_err(|error| internal(&error))?;

        Ok(Response::new(IsAncestorResponse { is_ancestor }))
    }
}

#[tonic::async_trait]
impl SyncService for Api {
    #[tracing::instrument(name = "enroute::grpc::push_to_remote", skip(self, request))]
    async fn push_to_remote(
        &self,
        request: Request<PushToRemoteRequest>,
    ) -> Result<Response<PushToRemoteResponse>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;
        let push = parse_push(request)?;

        let outcomes = self
            .remotes
            .push(&self.state, &repo, &push)
            .await
            .map_err(|error| push_failed(&error))?;

        Ok(Response::new(PushToRemoteResponse {
            outcomes: outcomes.iter().map(to_wire_push_outcome).collect(),
        }))
    }
}

/// The push a call asks for.
fn parse_push(request: PushToRemoteRequest) -> Result<enroute_git_remote::PushRequest, Status> {
    let remote = request
        .remote
        .ok_or_else(|| Status::invalid_argument("no remote given"))?;

    Ok(enroute_git_remote::PushRequest {
        remote: parse_remote(remote),
        refs: request.refs.into_iter().map(parse_refspec).collect(),
        atomic: request.atomic,
    })
}

fn parse_remote(remote: WireRemote) -> enroute_git_remote::Remote {
    enroute_git_remote::Remote {
        url: remote.url,
        credentials: remote.credentials.map(|credentials| match credentials {
            Credentials::Basic(basic) => enroute_git_remote::Credentials::Basic {
                username: basic.username,
                password: basic.password,
            },
            Credentials::BearerToken(token) => enroute_git_remote::Credentials::Bearer(token),
        }),
    }
}

fn parse_refspec(spec: WireRefSpec) -> enroute_git_remote::RefSpec {
    enroute_git_remote::RefSpec {
        source: spec.source,
        destination: spec.destination,
        force: spec.force,
    }
}

/// One ref's outcome on the remote.
///
/// A ref the remote refused is data, not a failure, for the reason
/// [`to_wire_outcome`] gives.
fn to_wire_push_outcome(outcome: &enroute_git_remote::RefOutcome) -> RefPushOutcome {
    let (status, message) = match outcome.status {
        enroute_git_remote::PushStatus::Updated => (WirePushStatus::Updated, String::new()),
        enroute_git_remote::PushStatus::Deleted => (WirePushStatus::Deleted, String::new()),
        enroute_git_remote::PushStatus::UpToDate => (WirePushStatus::UpToDate, String::new()),
        enroute_git_remote::PushStatus::Rejected(ref why) => {
            (WirePushStatus::Rejected, why.clone())
        }
    };
    RefPushOutcome {
        destination: outcome.destination.clone(),
        old_object_id: wire_oid_or_unset(outcome.old_id),
        new_object_id: wire_oid_or_unset(outcome.new_id),
        status: status.into(),
        message,
    }
}

/// A push that never got as far as an outcome.
///
/// The remote's own words reach the caller, who holds the relationship with
/// it — unlike an engine failure, which [`internal`] keeps.
fn push_failed(error: &enroute_git_remote::Error) -> Status {
    use enroute_git_remote::Error;

    match *error {
        Error::Request(ref why) => Status::invalid_argument(why.clone()),
        Error::Remote(ref why) | Error::Protocol(ref why) => {
            Status::failed_precondition(why.clone())
        }
        Error::Unauthorized => Status::permission_denied("the remote refused the credentials"),
        Error::Unsupported(ref what) => {
            Status::failed_precondition(format!("the remote does not offer {what}"))
        }
        Error::PackRefused(ref why) => Status::aborted(format!("the remote stored nothing: {why}")),
        // `Unknown` on purpose: the push reached the remote, so what landed
        // is not something this call can say either way any more.
        Error::ReportUnread(ref why) => Status::unknown(format!(
            "the push was sent and the remote's answer was not readable: {why}"
        )),
        Error::Transport(ref error) => Status::unavailable(format!("reaching the remote: {error}")),
        Error::Pack(_) | Error::Storage(_) => {
            tracing::error!(%error, "push to remote failed");
            Status::internal("internal error")
        }
    }
}

/// One ref's outcome.
///
/// A rejection is data, not a failure — a call can land some updates and
/// reject others, so this never becomes a `Status`.
fn to_wire_outcome(outcome: &RefUpdateResult) -> WireOutcome {
    let rejection = outcome.result.as_ref().err().map(|why| {
        let reason = match why {
            RefUpdateRejection::NonFastForward => Reason::NonFastForward,
            RefUpdateRejection::AlreadyExists => Reason::AlreadyExists,
            RefUpdateRejection::UnknownCommit => Reason::MissingObjects,
            RefUpdateRejection::InvalidRefname => Reason::InvalidRefname,
        };
        Rejection {
            reason: reason.into(),
            message: String::new(),
        }
    });

    WireOutcome {
        refname: outcome.refname.clone(),
        rejection,
    }
}

/// The ref updates a push asks for, in the order its outcomes come back in.
fn parse_updates(updates: &[WireRefUpdate]) -> Result<Vec<RefUpdate>, Status> {
    updates
        .iter()
        .map(|update| {
            Ok(RefUpdate {
                refname: update.refname.clone(),
                old_id: oid_or_null(update.old_object_id.as_ref())?,
                new_id: oid_or_null(update.new_object_id.as_ref())?,
            })
        })
        .collect()
}

/// Whether `name` is under any of `prefixes`.
///
/// No prefixes means every ref: proto3 cannot tell "unset" from "empty".
fn matches(prefixes: &[String], name: &str) -> bool {
    prefixes.is_empty() || prefixes.iter().any(|prefix| name.starts_with(prefix))
}

impl Api {
    /// A repository the ledger has just named, as the contract reports one.
    ///
    /// Reads `last_push`, unlike the answer to a fresh create: a repository
    /// reached by key may have been pushed to for months.
    async fn describe(&self, id: RepoId, key: &RepoKey) -> Result<Repository, Status> {
        let summary = self
            .state
            .rows
            .summarize(&[id])
            .await
            .map_err(|error| internal(&error))?
            .into_iter()
            .next()
            .ok_or_else(|| Status::not_found("no such repository"))?;
        Ok(described(
            &summary.repo,
            key.to_string(),
            summary.last_push_unix_seconds,
        ))
    }
}

#[tonic::async_trait]
impl RepositoryService for Api {
    #[tracing::instrument(name = "enroute::grpc::create_repository", skip(self, request))]
    async fn create_repository(
        &self,
        request: Request<CreateRepositoryRequest>,
    ) -> Result<Response<CreateRepositoryResponse>, Status> {
        let request = request.into_inner();
        let key = parse_repo(request.repo.as_ref())?;
        let default_branch = request.default_branch;
        // `HEAD` is advertised as a symbolic ref to this name, so a value
        // that is not a branch would produce an advertisement no client can
        // resolve. Refused here rather than stored and puzzled over later.
        let default_branch = match default_branch.as_str() {
            "" => None,
            branch if enroute_git_retrieve::is_branch_refname(branch) => Some(branch),
            other => {
                return Err(Status::invalid_argument(format!(
                    "default_branch must be under refs/heads/: {other}"
                )));
            }
        };

        // One write, and the key is part of it: the repository and the name it
        // is reached by land together or not at all. A key already taken is
        // answered with the repository holding it, so a create repeated after
        // a lost answer is the same create rather than a second repository.
        let repo = self
            .state
            .rows
            .create(default_branch, key.external())
            .await
            .map_err(|error| internal(&error))?;

        // Described rather than reported from what `create` returned, since a
        // repeat is answering for a repository that may have been pushed to.
        Ok(Response::new(CreateRepositoryResponse {
            repository: Some(self.describe(repo.id, &key).await?),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::get_repository", skip(self, request))]
    async fn get_repository(
        &self,
        request: Request<GetRepositoryRequest>,
    ) -> Result<Response<GetRepositoryResponse>, Status> {
        let request = request.into_inner();
        let key = parse_repo(request.repo.as_ref())?;
        let repo = self
            .held(&key)
            .await?
            .ok_or_else(|| Status::not_found("no such repository"))?;
        Ok(Response::new(GetRepositoryResponse {
            repository: Some(self.describe(repo.id, &key).await?),
        }))
    }

    /// Every repository, in key order.
    #[tracing::instrument(name = "enroute::grpc::list_repositories", skip(self, request))]
    async fn list_repositories(
        &self,
        request: Request<ListRepositoriesRequest>,
    ) -> Result<Response<ListRepositoriesResponse>, Status> {
        let request = request.into_inner();

        // A token is one this call minted, so a malformed one says only that.
        // Which rule a key broke is for `CreateRepository`, where the key is
        // the caller's to fix.
        let after = match request.page_token.as_str() {
            "" => None,
            token => Some(
                token
                    .parse::<RepoKey>()
                    .map_err(|_not_a_key| Status::invalid_argument("not a page token"))?,
            ),
        };
        let limit = page_limit(request.limit, REPOSITORY_LIMIT);

        // Refused rather than ignored: a prefix of what no key holds would
        // report nothing, which reads like an empty deployment.
        let prefix = request.prefix.as_str();
        crate::repo_key::check_prefix(prefix)
            .map_err(|bad| Status::invalid_argument(bad.to_string()))?;

        // A token is the last key of a page this call minted, so one from
        // outside the prefix is two walks confused for one.
        if let Some(after) = after.as_ref()
            && !after.as_str().starts_with(prefix)
        {
            return Err(Status::invalid_argument(
                "the page token is not inside the prefix",
            ));
        }

        // One past the page, so a next one is known without a second query.
        // A key starts with a letter or digit, so the empty string sorts
        // before every one of them and starts the walk.
        let after = after.as_ref().map_or("", RepoKey::as_str);
        let mut held = self
            .state
            .rows
            .page_by_key(prefix, after, limit + 1)
            .await
            .map_err(|error| internal(&error))?;

        let page = usize::try_from(limit).unwrap_or(usize::MAX);
        let more = held.len() > page;
        held.truncate(page);
        let next_page_token = match held.last() {
            Some((_, key)) if more => key.to_string(),
            _ => String::new(),
        };

        Ok(Response::new(ListRepositoriesResponse {
            repositories: held
                .into_iter()
                .map(|(summary, key)| {
                    described(&summary.repo, key.into(), summary.last_push_unix_seconds)
                })
                .collect(),
            next_page_token,
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::delete_repository", skip(self, request))]
    async fn delete_repository(
        &self,
        request: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        // Idempotent: a never-existed id and one deleted a moment ago answer
        // alike. Somebody else's repository answers alike too and deletes
        // nothing — refusing would say it exists.
        let request = request.into_inner();
        let key = parse_repo(request.repo.as_ref())?;
        // An error answers alike as well: this call cannot say why it did
        // nothing without saying the repository is there.
        let Ok(Some(held)) = self.held(&key).await else {
            return Ok(Response::new(DeleteRepositoryResponse {}));
        };

        // One write, which is also what frees the key: it is unique among
        // repositories that are not deleted, so this hands the name back
        // without waiting for maintenance to reclaim the row.
        let deleted = self
            .state
            .rows
            .repo(held.id)
            .mark_deleted()
            .await
            .map_err(|error| internal(&error))?;
        tracing::info!(
            repo_id = %held.id,
            %key,
            deleted,
            "delete_repository"
        );
        Ok(Response::new(DeleteRepositoryResponse {}))
    }
}

/// How many commits a merge-base walk reads before it gives up.
///
/// How much work a request may spend is this side's to decide, as
/// `ancestors_among`'s cap is its caller's rather than the walk's.
const MERGE_BASE_MAX_COMMITS: u64 = 250_000;

/// How much of an object's bytes ride in one message.
///
/// Well under gRPC's default 4 MiB ceiling, so a blob of any size streams
/// rather than failing at the limit.
const OBJECT_CHUNK_BYTES: usize = 512 * 1024;

#[tonic::async_trait]
impl ObjectService for Api {
    type GetObjectStream = ResponseStream<GetObjectResponse>;
    type ListTreeStream = ResponseStream<ListTreeResponse>;
    type ListCommitsStream = ResponseStream<ListCommitsResponse>;
    type DiffCommitStream = ResponseStream<DiffCommitResponse>;

    #[tracing::instrument(name = "enroute::grpc::get_object", skip(self, request))]
    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;
        let oid = require_oid(request.object_id.as_ref(), "object_id")?;

        // Read whole, then chunk. The engine reconstructs an object by
        // following its delta chain, which has no partial answer to give: the
        // bytes do not exist until the last hop is applied.
        let (kind, bytes) = enroute_git_retrieve::object(&self.state, &repo, oid)
            .await
            .map_err(|error| object_failed(&oid, &error))?;

        let header = GetObjectResponse {
            chunk: Some(get_object_response::Chunk::Header(ObjectHeader {
                kind: object_kind(kind).into(),
                size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            })),
        };
        let body = futures::stream::iter(
            chunks(bytes)
                .map(|chunk| {
                    Ok(GetObjectResponse {
                        chunk: Some(get_object_response::Chunk::Data(chunk)),
                    })
                })
                .collect::<Vec<_>>(),
        );

        Ok(Response::new(Box::pin(
            futures::stream::once(async move { Ok(header) }).chain(body),
        )))
    }

    /// Every path under a commit's tree.
    ///
    /// Here rather than in a caller because it is a walk of storage: a level of
    /// directories at a time, in parallel, instead of a round trip each.
    async fn list_tree(
        &self,
        request: Request<ListTreeRequest>,
    ) -> Result<Response<Self::ListTreeStream>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;
        let oid = require_oid(request.object_id.as_ref(), "object_id")?;

        let walk = enroute_git_retrieve::tree(&self.state, &repo, oid, TREE_ENTRY_LIMIT)
            .await
            .map_err(|error| tree_failed(error, "a listing starts at a commit or a tree"))?;

        Ok(Response::new(Box::pin(futures::stream::iter(
            tree_batches(walk).into_iter().map(Ok),
        ))))
    }

    /// What landed before a commit, newest first.
    ///
    /// First parent only, which is the history a branch reads as: what landed
    /// on it, rather than everything a merge brought along.
    async fn list_commits(
        &self,
        request: Request<ListCommitsRequest>,
    ) -> Result<Response<Self::ListCommitsStream>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;

        // A page token names where to resume, and is the only thing read when
        // it is set: a caller that pages is walking one history, and taking
        // the start from both would let the two disagree.
        let tip = if request.page_token.is_empty() {
            require_oid(request.commit_id.as_ref(), "commit_id")?
        } else {
            parse_hex(&request.page_token, "a page token")?
        };

        let limit = page_limit(request.limit, COMMIT_LIMIT);

        // One commit past the page, so where to resume is known without a
        // second walk — and without reading a commit the caller never asked
        // for, since only the page's own oids are fetched.
        let walked = self
            .state
            .graph
            .repo(repo.id)
            .first_parent_page(tip, limit + 1)
            .await
            .map_err(|error| metadata_failed(&error))?;

        if walked.is_empty() {
            return Err(
                not_a_commit(&self.state, &repo, tip, "a history starts at a commit").await,
            );
        }

        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        // All of it when the walk ran out first, which is the whole history
        // rather than a page of it.
        let page = walked.get(..limit).unwrap_or(walked.as_slice());

        // Every oid up front, so the page is one batched read instead of one
        // round trip per commit chained on the last one's bytes.
        let fetched = enroute_git_retrieve::objects(&self.state, &repo, page)
            .await
            .map_err(|error| object_failed(&tip, &error))?;

        let mut commits = Vec::with_capacity(page.len());
        for &oid in page {
            commits.push(wire_commit(oid, &commit_read(&fetched, oid)?));
        }

        // Empty when the walk ran out of first parents, which is the whole
        // history rather than a page of it.
        let next_page_token = walked
            .get(limit)
            .map(|oid| oid.to_hex().to_string())
            .unwrap_or_default();
        let response = ListCommitsResponse {
            commits,
            next_page_token,
        };
        Ok(Response::new(Box::pin(futures::stream::once(async move {
            Ok(response)
        }))))
    }

    /// Which paths lie between two commits, or one and its first parent.
    ///
    /// Here rather than in a caller for the reason `list_tree` is: working it
    /// out means reading trees, and a caller doing that is a round trip each.
    async fn diff_commit(
        &self,
        request: Request<DiffCommitRequest>,
    ) -> Result<Response<Self::DiffCommitStream>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;
        let oid = require_oid(request.commit_id.as_ref(), "commit_id")?;
        let base = request.base_commit_id.as_ref().map(parse_oid).transpose()?;

        // One round trip either way. A named base needs both oids proven to be
        // commits and nothing else; the first parent is an edge only the graph
        // holds, so that one walks rather than looks up.
        let older = if let Some(base) = base {
            let known = enroute_git_retrieve::metas(&self.state, repo.id, &[oid, base])
                .await
                .map_err(|error| metadata_failed(&error))?;
            let named = [
                (oid, "a diff is of a commit"),
                (base, "a diff is against a commit"),
            ];
            for (of, wanted) in named {
                // Recorded as well as named, since a diff reads the bytes and
                // a number alone cannot produce them.
                let usable = known
                    .get(&of)
                    .is_some_and(|meta| meta.kind == gix_object::Kind::Commit && meta.is_stored());
                if !usable {
                    return Err(not_a_commit(&self.state, &repo, of, wanted).await);
                }
            }
            Some(base)
        } else {
            let walked = self
                .state
                .graph
                .repo(repo.id)
                .first_parent_page(oid, 2)
                .await
                .map_err(|error| metadata_failed(&error))?;
            let Some((_, parent)) = walked.split_first() else {
                return Err(not_a_commit(&self.state, &repo, oid, "a diff is of a commit").await);
            };
            parent.first().copied()
        };

        let reading: Vec<ObjectId> = std::iter::once(oid).chain(older).collect();
        let fetched = enroute_git_retrieve::objects(&self.state, &repo, &reading)
            .await
            .map_err(|error| object_failed(&oid, &error))?;
        let root_tree = |of: ObjectId| commit_read(&fetched, of).map(|read| read.root_tree);

        // A commit with no parent is diffed against nothing, which makes every
        // path in it an add — git's own answer for a root commit.
        let old_root = older.map(root_tree).transpose()?;
        let options = DiffOptions { removals: true };
        let diff = enroute_git_retrieve::diff_trees(
            &self.state,
            &repo,
            old_root,
            root_tree(oid)?,
            options,
            DIFF_TREE_READ_LIMIT,
        )
        .await
        .map_err(|error| internal(&error))?;
        let batches = changed_files(diff);

        Ok(Response::new(Box::pin(futures::stream::iter(
            batches.into_iter().map(Ok),
        ))))
    }

    /// Where two commits' histories last agreed.
    ///
    /// Here rather than in a caller for the reason `list_commits` is: the
    /// walk is over parent edges only this side holds.
    #[tracing::instrument(name = "enroute::grpc::find_merge_bases", skip(self, request))]
    async fn find_merge_bases(
        &self,
        request: Request<FindMergeBasesRequest>,
    ) -> Result<Response<FindMergeBasesResponse>, Status> {
        let request = request.into_inner();
        let repo = self.resolve(request.repo.as_ref()).await?;

        let a = require_oid(request.commit_id_a.as_ref(), "commit_id_a")?;
        let b = require_oid(request.commit_id_b.as_ref(), "commit_id_b")?;

        let found = self
            .state
            .graph
            .repo(repo.id)
            .merge_bases(a, b, MERGE_BASE_MAX_COMMITS)
            .await
            .map_err(|error| internal(&error))?;

        Ok(Response::new(FindMergeBasesResponse {
            base_commit_ids: found.bases.iter().copied().map(wire_oid).collect(),
            exhausted: found.exhausted,
        }))
    }
}

/// One commit out of a batch already fetched, parsed.
///
/// Both readings are the commit graph disagreeing with the store, which is
/// this side's bug and nothing a caller can act on — so both are internal.
fn commit_read(
    fetched: &ObjectHashMap<(gix_object::Kind, bytes::Bytes)>,
    oid: ObjectId,
) -> Result<CommitDetails, Status> {
    let (_, bytes) = fetched.get(&oid).ok_or_else(|| {
        tracing::error!(%oid, "the commit graph named a commit the store does not hold");
        Status::internal("internal error")
    })?;
    commit_details(bytes).map_err(|error| {
        tracing::error!(%oid, %error, "a commit object did not parse as one");
        Status::internal("internal error")
    })
}

/// What `oid` was, when the commit graph's walk from it came back empty.
///
/// The walk holds only commits, so the object index says which of the other
/// things it is — in its own terms, without reading bytes to find out.
async fn not_a_commit(state: &Storage, repo: &RepoMetadata, oid: ObjectId, wanted: &str) -> Status {
    let found = match enroute_git_retrieve::meta(state, repo.id, oid).await {
        Ok(found) => found,
        Err(error) => return metadata_failed(&error),
    };
    match found {
        None => Status::not_found(format!("no object {oid}")),
        // Numbered and never recorded is an answer rather than a fault: the
        // graph is right to hold nothing for it.
        Some(meta) if !meta.is_stored() => Status::not_found(format!("no object {oid}")),
        Some(meta) if meta.kind == gix_object::Kind::Commit => {
            tracing::error!(%oid, "a commit the commit graph does not hold");
            Status::internal("internal error")
        }
        Some(meta) => Status::invalid_argument(format!("{oid} is a {}, and {wanted}", meta.kind)),
    }
}

/// The blobs a diff touched, in path order, as the wire carries them.
///
/// Trees are dropped: a caller rebuilds a directory from the paths under it,
/// and the pair of ids on one buys nothing that its children do not say.
fn changed_files(diff: TreeDiff) -> Vec<DiffCommitResponse> {
    let complete = diff.is_complete();
    let blobs = |kind| kind == gix_object::Kind::Blob;
    let added = diff
        .changes
        .into_iter()
        .filter(|change| blobs(change.kind))
        .map(|change| FileChange {
            path: wire_path(change.path),
            old_object_id: change.old.map(wire_oid),
            new_object_id: Some(wire_oid(change.new)),
        });
    let gone = diff
        .removals
        .into_iter()
        .filter(|removal| blobs(removal.kind))
        .map(|removal| FileChange {
            path: wire_path(removal.path),
            old_object_id: Some(wire_oid(removal.old)),
            new_object_id: None,
        });

    // Sorted, and truncated after: what a caller is handed is then a prefix of
    // the answer rather than an arbitrary part of it, and no client has to sort
    // what it was already given.
    let mut changes: Vec<FileChange> = added.chain(gone).collect();
    changes.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    let truncated = changes.len() > DIFF_CHANGE_LIMIT || !complete;
    changes.truncate(DIFF_CHANGE_LIMIT);

    let mut batches = Vec::new();
    while !changes.is_empty() {
        let rest = changes.split_off(changes.len().min(DIFF_BATCH_CHANGES));
        batches.push(DiffCommitResponse {
            changes,
            truncated: false,
        });
        changes = rest;
    }
    // On the last batch, so a caller learns it only once it holds everything
    // that was worked out — and on one of its own when there is nothing else
    // to carry it.
    if truncated {
        match batches.last_mut() {
            Some(last) => last.truncated = true,
            None => batches.push(DiffCommitResponse {
                changes: Vec::new(),
                truncated: true,
            }),
        }
    }
    batches
}

/// A path as the wire spells it, keeping the bytes where they are already a
/// string.
///
/// Git stores a name as bytes and does not require them to be UTF-8; an
/// invalid one is replaced rather than failing the diff, as a listing does.
fn wire_path(path: Vec<u8>) -> String {
    String::from_utf8(path)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

/// How many changed paths ride in one message.
///
/// A bound on the size of a message rather than on the answer, which
/// `DIFF_CHANGE_LIMIT` is.
const DIFF_BATCH_CHANGES: usize = 1_000;

/// How many changed paths one diff reports before it says it stopped.
const DIFF_CHANGE_LIMIT: usize = TREE_ENTRY_LIMIT;

/// How many trees one diff reads before it stops and reports what it has.
///
/// A bound on the reads one request may cost, as `COMMIT_LIMIT` is — and not
/// the same question as how many paths come back, which a diff prunes to.
const DIFF_TREE_READ_LIMIT: usize = 10_000;

/// How many commits one call walks before it stops and says where it stopped.
///
/// A page size rather than a cost ceiling: the edges come from the commit
/// graph, so the reads are one batch whatever this is.
const COMMIT_LIMIT: u32 = 100;

/// How many repositories one listing reports before it says where it stopped.
///
/// A page size, as `COMMIT_LIMIT` is: a page is two queries whatever this is.
const REPOSITORY_LIMIT: u32 = 100;

/// The page size a request asked for, capped by `max`.
///
/// A zero asks for `max`, which is what a caller sends when it has no opinion.
fn page_limit(asked: u32, max: u32) -> u32 {
    if asked == 0 { max } else { max.min(asked) }
}

/// One commit on the wire.
///
/// Every string git stored is bytes, and none of them is required to be UTF-8;
/// invalid sequences are replaced rather than failing the listing.
fn wire_commit(oid: ObjectId, details: &CommitDetails) -> WireCommit {
    fn identity(identity: &Identity) -> WireIdentity {
        WireIdentity {
            name: String::from_utf8_lossy(&identity.name).into_owned(),
            email: String::from_utf8_lossy(&identity.email).into_owned(),
            timestamp: Some(wire_time(identity.seconds)),
            utc_offset_seconds: identity.offset_seconds,
        }
    }

    WireCommit {
        commit_id: Some(wire_oid(oid)),
        parent_commit_ids: details.parents.iter().copied().map(wire_oid).collect(),
        tree_id: Some(wire_oid(details.root_tree)),
        author: Some(identity(&details.author)),
        committer: Some(identity(&details.committer)),
        summary: String::from_utf8_lossy(&details.summary).into_owned(),
        body: String::from_utf8_lossy(&details.body).into_owned(),
    }
}

/// How many entries one listing carries before it stops and says so.
///
/// A bound on what a caller can be handed rather than on what a repository may
/// hold. Whoever wants more than this wants a different call.
const TREE_ENTRY_LIMIT: usize = 50_000;

/// A walk's levels as the wire carries them, one response per depth.
///
/// The engine says whether it stopped short; the wire says so on the last
/// response, so a caller learns it holding everything that was worked out.
fn tree_batches(walk: TreeWalk) -> Vec<ListTreeResponse> {
    let mut batches: Vec<ListTreeResponse> = walk
        .levels
        .into_iter()
        .map(|level| ListTreeResponse {
            entries: level.into_iter().map(tree_entry).collect(),
            truncated: false,
        })
        .collect();
    if walk.truncated
        && let Some(last) = batches.last_mut()
    {
        last.truncated = true;
    }
    batches
}

/// One found entry as the wire spells it.
fn tree_entry(item: TreeItem) -> TreeEntry {
    TreeEntry {
        path: wire_path(item.path),
        kind: if item.is_tree {
            ObjectKind::Tree
        } else {
            ObjectKind::Blob
        }
        .into(),
        object_id: Some(wire_oid(item.oid)),
    }
}

/// A tree walk that gave no listing.
///
/// `Missing` and `NotTreeish` are the caller's own argument, with `wanted`
/// saying what this call takes; everything else is ours.
fn tree_failed(error: TreeError, wanted: &str) -> Status {
    match error {
        TreeError::Missing(oid) => Status::not_found(format!("no object {oid}")),
        TreeError::NotTreeish { oid, kind } => {
            Status::invalid_argument(format!("{oid} is a {kind}, and {wanted}"))
        }
        TreeError::Failed(error) => internal(&error),
    }
}

/// The commit graph would not answer.
///
/// Never the caller's fault: a repository this Enroute stores has an index of
/// it, and a missing answer is this service's problem to fix.
fn metadata_failed(error: &anyhow::Error) -> Status {
    tracing::error!(%error, "reading the commit graph failed");
    Status::internal("internal error")
}

/// Split `bytes` into wire-sized pieces without copying: each is a view onto
/// the same allocation.
fn chunks(bytes: bytes::Bytes) -> impl Iterator<Item = bytes::Bytes> {
    let mut rest = bytes;
    std::iter::from_fn(move || {
        if rest.is_empty() {
            return None;
        }
        Some(rest.split_to(OBJECT_CHUNK_BYTES.min(rest.len())))
    })
}

fn object_kind(kind: gix_object::Kind) -> ObjectKind {
    match kind {
        gix_object::Kind::Commit => ObjectKind::Commit,
        gix_object::Kind::Tree => ObjectKind::Tree,
        gix_object::Kind::Blob => ObjectKind::Blob,
        gix_object::Kind::Tag => ObjectKind::Tag,
    }
}

/// A read that found no such object.
///
/// Everything else is ours: a chain that will not rebuild is a storage
/// fault, not the caller's.
fn object_failed(oid: &ObjectId, error: &enroute_git_core::Error) -> Status {
    if matches!(error, enroute_git_core::Error::Missing(_)) {
        return Status::not_found(format!("no object {oid}"));
    }
    tracing::error!(%oid, %error, "reading an object failed");
    Status::internal("internal error")
}

fn described(repo: &RepoMetadata, key: String, last_push_unix_seconds: Option<i64>) -> Repository {
    Repository {
        repo: Some(WireRepoKey { key }),
        default_branch: repo.default_branch.clone(),
        last_push: last_push_unix_seconds.map(wire_time),
    }
}

/// Engine failures reach a caller as `Internal` with no detail: what went
/// wrong inside is not theirs to act on, and the span already carries it.
fn internal(error: &anyhow::Error) -> Status {
    tracing::error!(%error, "grpc call failed");
    Status::internal("internal error")
}

#[cfg(test)]
mod reflection {
    use prost::Message as _;

    /// Both reflection services resolve the descriptors compiled into them.
    ///
    /// The one way this fails is a build that produced a set with a dangling
    /// import, which nothing else here would notice.
    #[test]
    fn the_services_build() {
        let configure = || {
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(enroute_api::FILE_DESCRIPTOR_SET)
        };

        configure().build_v1().expect("the v1 reflection service");
        configure()
            .build_v1alpha()
            .expect("the v1alpha reflection service");
    }

    /// The hook contract is in the descriptors, though it declares no service.
    ///
    /// Reflection walks services to their imports and would never reach it,
    /// so an integrator can ask for it only because it is registered here.
    #[test]
    fn the_descriptors_carry_the_hook_contract() {
        let set = prost_types::FileDescriptorSet::decode(enroute_api::FILE_DESCRIPTOR_SET)
            .expect("the compiled-in descriptors");
        let files: Vec<&str> = set.file.iter().filter_map(|f| f.name.as_deref()).collect();

        for wanted in [
            "enroute/common/v1alpha1/common.proto",
            "enroute/api/v1alpha1/repository.proto",
            "enroute/api/v1alpha1/ref.proto",
            "enroute/api/v1alpha1/object.proto",
            "enroute/api/v1alpha1/sync.proto",
            "enroute/hook/v1alpha1/hook.proto",
        ] {
            assert!(
                files.contains(&wanted),
                "{wanted} is not in the descriptors"
            );
        }
    }
}
