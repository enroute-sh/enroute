//! `RepositoryService`: the repository key is the repository name, and
//! the compute's lifecycle calls answer each RPC.

use tonic::{Request, Response, Status};

use enroute_api::api::v1alpha1::repository_service_server::RepositoryService;
use enroute_api::api::v1alpha1::{
    CreateRepositoryRequest, CreateRepositoryResponse, DeleteRepositoryRequest,
    DeleteRepositoryResponse, GetRepositoryRequest, GetRepositoryResponse, ListRepositoriesRequest,
    ListRepositoriesResponse, ObjectFormat as WireObjectFormat, Repository,
};
use enroute_api::common::v1alpha1::RepoKey as WireRepoKey;

use crate::compute::RepoInfo;
use crate::contract::repo_key::{self, RepoKey};
use crate::contract::wire::{parse_repo, wire_system_time};
use crate::git::oid::ObjectFormat;
use crate::query::{self, page_limit};

use super::{Api, internal};

/// How many repositories one listing reports.
const REPOSITORY_LIMIT: u32 = 100;

/// The ledger's refusal of a create it is set up not to make (a SHA-256
/// repository where those are off), as the caller sees it.
fn refused(error: &anyhow::Error) -> Option<Status> {
    error
        .chain()
        .filter_map(|c| c.downcast_ref::<Status>())
        .find(|s| s.code() == tonic::Code::FailedPrecondition)
        .map(|s| Status::failed_precondition(s.message()))
}

/// A repository as the contract reports one.
fn described(info: RepoInfo) -> Repository {
    let format = match info.format {
        ObjectFormat::Sha1 => WireObjectFormat::Sha1,
        ObjectFormat::Sha256 => WireObjectFormat::Sha256,
    };
    Repository {
        repo: Some(WireRepoKey { key: info.name }),
        default_branch: info.head,
        last_push: wire_system_time(info.last_push),
        object_format: format.into(),
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
        // Enroute takes a bare branch name too, and the contract does not.
        let branch = request.default_branch.as_str();
        if !branch.is_empty() && !branch.starts_with("refs/heads/") {
            return Err(Status::invalid_argument(format!(
                "default_branch must be under refs/heads/: {branch}"
            )));
        }
        let format = match request.object_format() {
            WireObjectFormat::Unspecified => self.compute.config.default_object_format,
            WireObjectFormat::Sha1 => ObjectFormat::Sha1,
            WireObjectFormat::Sha256 => ObjectFormat::Sha256,
        };
        // An existing repository is left as it is, so a repeat is the same
        // create.
        let info = self
            .compute
            .create_repo_in(key.as_str(), branch, format)
            .await
            .map_err(|error| refused(&error).unwrap_or_else(|| internal(&error)))?;
        Ok(Response::new(CreateRepositoryResponse {
            repository: Some(described(info)),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::get_repository", skip(self, request))]
    async fn get_repository(
        &self,
        request: Request<GetRepositoryRequest>,
    ) -> Result<Response<GetRepositoryResponse>, Status> {
        let request = request.into_inner();
        let key = parse_repo(request.repo.as_ref())?;
        let info = self
            .compute
            .get_repo(key.as_str())
            .await
            .map_err(query::Error::from)?
            .ok_or(query::Error::NotFound)?;
        Ok(Response::new(GetRepositoryResponse {
            repository: Some(described(info)),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::list_repositories", skip(self, request))]
    async fn list_repositories(
        &self,
        request: Request<ListRepositoriesRequest>,
    ) -> Result<Response<ListRepositoriesResponse>, Status> {
        let request = request.into_inner();
        let after = match request.page_token.as_str() {
            "" => None,
            token => Some(
                token
                    .parse::<RepoKey>()
                    .map_err(|_not_a_key| Status::invalid_argument("not a page token"))?,
            ),
        };
        let prefix = request.prefix.as_str();
        repo_key::check_prefix(prefix).map_err(|bad| Status::invalid_argument(bad.to_string()))?;
        if let Some(after) = after.as_ref()
            && !after.as_str().starts_with(prefix)
        {
            return Err(Status::invalid_argument(
                "the page token is not inside the prefix",
            ));
        }
        let after = after.as_ref().map_or("", RepoKey::as_str);
        let limit = page_limit(request.limit, REPOSITORY_LIMIT);

        // A page can be short, because deleted repositories take places.
        let page = self
            .compute
            .list_repos(prefix, after, limit)
            .await
            .map_err(|error| internal(&error))?;
        Ok(Response::new(ListRepositoriesResponse {
            repositories: page.repos.into_iter().map(described).collect(),
            next_page_token: page.next.unwrap_or_default(),
        }))
    }

    #[tracing::instrument(name = "enroute::grpc::delete_repository", skip(self, request))]
    async fn delete_repository(
        &self,
        request: Request<DeleteRepositoryRequest>,
    ) -> Result<Response<DeleteRepositoryResponse>, Status> {
        let request = request.into_inner();
        let key = parse_repo(request.repo.as_ref())?;
        // Idempotent: Enroute deletes a missing repository too, so the answer
        // does not say whether one was there.
        self.compute
            .delete_repo(key.as_str())
            .await
            .map_err(|error| internal(&error))?;
        tracing::info!(%key, "deleted a repository");
        Ok(Response::new(DeleteRepositoryResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_repository_reports_its_last_push_in_seconds() {
        let at = std::time::UNIX_EPOCH + std::time::Duration::from_millis(1_700_000_000_500);
        let wire = described(RepoInfo {
            name: "repo-1".into(),
            head: "refs/heads/main".into(),
            last_push: Some(at),
            format: ObjectFormat::Sha1,
        });
        assert_eq!(wire.last_push.map(|t| t.seconds), Some(1_700_000_000));
        assert_eq!(wire.default_branch, "refs/heads/main");

        let never = described(RepoInfo {
            name: "repo-1".into(),
            head: "refs/heads/main".into(),
            last_push: None,
            format: ObjectFormat::Sha1,
        });
        assert!(never.last_push.is_none());
    }
}
