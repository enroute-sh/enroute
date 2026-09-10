//! Calling the contract from a test.
//!
//! The generated `tonic` clients with a bearer token attached and the
//! `Option`s unwrapped. A test convenience and not a client library: this
//! repository ships none, and one that grew here would be a client with a
//! dependency on the engine.

#![allow(
    dead_code,
    reason = "compiled separately per test binary, so not every helper is used by every one"
)]

use anyhow::{Context as _, Result};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use enroute_api::api::v1alpha1::object_service_client::ObjectServiceClient;
use enroute_api::api::v1alpha1::ref_service_client::RefServiceClient;
use enroute_api::api::v1alpha1::repository_service_client::RepositoryServiceClient;
use enroute_api::api::v1alpha1::sync_service_client::SyncServiceClient;
use enroute_api::api::v1alpha1::{
    Commit, CreateRepositoryRequest, DeleteRepositoryRequest, DiffCommitRequest, FileChange,
    FindMergeBasesRequest, FindMergeBasesResponse, GetObjectRequest, GetRepositoryRequest,
    IsAncestorRequest, ListCommitsRequest, ListRefsRequest, ListRefsResponse,
    ListRepositoriesRequest, ListTreeRequest, ObjectKind, PushToRemoteRequest, RefPushOutcome,
    RefSpec, RefUpdate, Remote, Repository, TreeEntry, UpdateRefsRequest, get_object_response,
};
use enroute_api::common::v1alpha1::{ObjectId, RepoKey};

/// The hex out of an object id the contract sent.
///
/// Unset reads as empty, which is what a test asserting a side of a diff is
/// absent compares against.
pub(crate) fn hex(id: Option<&ObjectId>) -> String {
    id.map(|id| id.hex.clone()).unwrap_or_default()
}

/// The hex out of every id in a list, for comparing against git's own.
pub(crate) fn hexes(ids: &[ObjectId]) -> Vec<&str> {
    ids.iter().map(|id| id.hex.as_str()).collect()
}

/// A hex object id as the contract carries it, or unset when empty.
///
/// Empty is how a test spells "no object", the way the contract spells it
/// with an unset field.
pub(crate) fn oid(hex: impl Into<String>) -> Option<ObjectId> {
    let hex = hex.into();
    (!hex.is_empty()).then_some(ObjectId { hex })
}

/// A repository key as the contract carries it.
///
/// A sibling of [`oid`], so no call site builds the wire wrapper by hand.
pub(crate) fn repo_key(key: impl Into<String>) -> RepoKey {
    RepoKey { key: key.into() }
}

/// A channel naming its tenant on every call.
///
/// An interceptor rather than a header per call, so no call can forget. What a
/// deployment's proxy would set, since nothing here is behind one.
type Authenticated = InterceptedService<Channel, Names>;

#[derive(Clone)]
pub(crate) struct Names(Option<MetadataValue<Ascii>>);

impl tonic::service::Interceptor for Names {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        // `None` sends no header at all, which is what a call nothing
        // authenticated looks like — an empty one would name a tenant instead.
        if let Some(tenant) = self.0.clone() {
            drop(
                request
                    .metadata_mut()
                    .insert(crate::support::TENANT_HEADER, tenant),
            );
        }
        Ok(request)
    }
}

/// Everything these tests ask Enroute for.
pub(crate) struct Client {
    refs: RefServiceClient<Authenticated>,
    repositories: RepositoryServiceClient<Authenticated>,
    objects: ObjectServiceClient<Authenticated>,
    sync: SyncServiceClient<Authenticated>,
}

impl Client {
    /// Connect to Enroute at `endpoint`, calling as `tenant`.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint is unusable or unreachable, or the
    /// tenant cannot be sent as a header.
    pub(crate) async fn connect(endpoint: String, tenant: &str) -> Result<Self> {
        let names = Names(Some(
            tenant
                .parse()
                .context("the tenant is not sendable as a header")?,
        ));
        Self::naming(endpoint, names).await
    }

    /// The two connect paths' common half.
    async fn naming(endpoint: String, bearer: Names) -> Result<Self> {
        let channel = Channel::from_shared(endpoint)
            .context("not a usable endpoint URL")?
            .connect()
            .await
            .context("connecting to the contract")?;
        Ok(Self {
            refs: RefServiceClient::with_interceptor(channel.clone(), bearer.clone()),
            repositories: RepositoryServiceClient::with_interceptor(
                channel.clone(),
                bearer.clone(),
            ),
            objects: ObjectServiceClient::with_interceptor(channel.clone(), bearer.clone()),
            sync: SyncServiceClient::with_interceptor(channel, bearer),
        })
    }

    /// Push refs to a git server somewhere else.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails, which is a push that never got as
    /// far as an outcome to report.
    pub(crate) async fn push_to_remote(
        &self,
        repo: &str,
        url: &str,
        refs: Vec<RefSpec>,
    ) -> Result<Vec<RefPushOutcome>> {
        Ok(self
            .sync
            .clone()
            .push_to_remote(PushToRemoteRequest {
                repo: Some(repo_key(repo)),
                remote: Some(Remote {
                    url: url.to_string(),
                    credentials: None,
                }),
                refs,
                atomic: false,
            })
            .await
            .context("calling push_to_remote")?
            .into_inner()
            .outcomes)
    }

    /// Connect presenting no token at all.
    ///
    /// What an unauthenticated caller is, which is not the same as one whose
    /// token nobody minted.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint is unusable or unreachable.
    pub(crate) async fn connect_anonymously(endpoint: String) -> Result<Self> {
        Self::naming(endpoint, Names(None)).await
    }

    /// Create a repository under a key no other test will pick.
    ///
    /// An empty `default_branch` means `refs/heads/main`.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails or answers with no repository.
    ///
    /// Most tests are not about the key and only need a repository; the ones
    /// that are call [`Client::create_repository_as`].
    pub(crate) async fn create_repository(&self, default_branch: &str) -> Result<Repository> {
        let key = format!("e2e-{}", uuid::Uuid::new_v4());
        self.create_repository_as(&key, default_branch).await
    }

    /// Creates one under the key the test chose.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails or answers with no repository.
    pub(crate) async fn create_repository_as(
        &self,
        key: &str,
        default_branch: &str,
    ) -> Result<Repository> {
        self.repositories
            .clone()
            .create_repository(CreateRepositoryRequest {
                default_branch: default_branch.to_string(),
                repo: Some(repo_key(key)),
            })
            .await?
            .into_inner()
            .repository
            .context("create returned no repository")
    }

    /// # Errors
    ///
    /// Returns an error if the call fails or answers with no repository.
    pub(crate) async fn get_repository(&self, repo: &str) -> Result<Repository> {
        self.repositories
            .clone()
            .get_repository(GetRepositoryRequest {
                repo: Some(repo_key(repo)),
            })
            .await?
            .into_inner()
            .repository
            .context("get returned no repository")
    }

    /// One page of the caller's repositories, and where to resume.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn list_repositories(
        &self,
        limit: u32,
        page_token: &str,
    ) -> Result<(Vec<Repository>, String)> {
        let page = self
            .repositories
            .clone()
            .list_repositories(ListRepositoriesRequest {
                limit,
                page_token: page_token.to_string(),
            })
            .await?
            .into_inner();
        Ok((page.repositories, page.next_page_token))
    }

    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn delete_repository(&self, repo: &str) -> Result<()> {
        self.repositories
            .clone()
            .delete_repository(DeleteRepositoryRequest {
                repo: Some(repo_key(repo)),
            })
            .await?;
        Ok(())
    }

    /// Every ref, whatever its prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn list_refs(&self, repo: &str) -> Result<ListRefsResponse> {
        self.list_refs_under(repo, Vec::new()).await
    }

    /// The refs under `prefixes`, or every one of them when it is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn list_refs_under(
        &self,
        repo: &str,
        prefixes: Vec<String>,
    ) -> Result<ListRefsResponse> {
        Ok(self
            .refs
            .clone()
            .list_refs(ListRefsRequest {
                repo: Some(repo_key(repo)),
                prefixes,
            })
            .await?
            .into_inner())
    }

    /// Whether `descendant` has `ancestor` in its history.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn is_ancestor(
        &self,
        repo: &str,
        ancestor: &str,
        descendant: &str,
    ) -> Result<bool> {
        Ok(self
            .refs
            .clone()
            .is_ancestor(IsAncestorRequest {
                repo: Some(repo_key(repo)),
                ancestor_commit_id: oid(ancestor),
                descendant_commit_id: oid(descendant),
            })
            .await
            .context("calling is_ancestor")?
            .into_inner()
            .is_ancestor)
    }

    /// Where two commits' histories last agreed, and whether the walk
    /// proved it — both, so a test can hold the contract to either.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn find_merge_bases(
        &self,
        repo: &str,
        a: &str,
        b: &str,
    ) -> Result<FindMergeBasesResponse> {
        Ok(self
            .objects
            .clone()
            .find_merge_bases(FindMergeBasesRequest {
                repo: Some(repo_key(repo)),
                commit_id_a: oid(a),
                commit_id_b: oid(b),
            })
            .await
            .context("calling find_merge_bases")?
            .into_inner())
    }

    /// Ask for `updates`, and read back what each one did.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn update_refs(
        &self,
        repo: &str,
        updates: Vec<RefUpdate>,
    ) -> Result<Vec<RefOutcome>> {
        let response = self
            .refs
            .clone()
            .update_refs(UpdateRefsRequest {
                repo: Some(repo_key(repo)),
                updates,
            })
            .await
            .context("calling update_refs")?
            .into_inner();

        Ok(response
            .outcomes
            .into_iter()
            .map(|outcome| RefOutcome {
                refname: outcome.refname,
                rejection: outcome.rejection.map(|rejection| rejection.message),
            })
            .collect())
    }

    /// One page of history from `start`, and the token for the page after it.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails, or the stream carries no page.
    pub(crate) async fn list_commits(
        &self,
        repo: &str,
        start: &str,
        limit: u32,
        page_token: &str,
    ) -> Result<(Vec<Commit>, String)> {
        let mut stream = self
            .objects
            .clone()
            .list_commits(ListCommitsRequest {
                repo: Some(repo_key(repo)),
                commit_id: oid(start),
                limit,
                page_token: page_token.to_string(),
            })
            .await
            .context("calling list_commits")?
            .into_inner();

        let page = stream
            .message()
            .await
            .context("reading a history page")?
            .context("list_commits carried no page")?;
        Ok((page.commits, page.next_page_token))
    }

    /// Which files a commit changed, in the path order Enroute sends.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn diff_commit(
        &self,
        repo: &str,
        object_id: &str,
        base: &str,
    ) -> Result<Vec<FileChange>> {
        let mut stream = self
            .objects
            .clone()
            .diff_commit(DiffCommitRequest {
                repo: Some(repo_key(repo)),
                commit_id: oid(object_id),
                base_commit_id: oid(base),
            })
            .await
            .context("calling diff_commit")?
            .into_inner();

        let mut changes = Vec::new();
        while let Some(batch) = stream.message().await? {
            changes.extend(batch.changes);
        }
        Ok(changes)
    }

    /// Every path under a commit or a tree, and whether the walk was cut short.
    ///
    /// The batches are concatenated and their `truncated` flags folded into
    /// one: a caller drawing a listing wants the walk, not how it arrived.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub(crate) async fn list_tree(
        &self,
        repo: &str,
        object_id: &str,
    ) -> Result<(Vec<TreeEntry>, bool)> {
        let mut stream = self
            .objects
            .clone()
            .list_tree(ListTreeRequest {
                repo: Some(repo_key(repo)),
                object_id: oid(object_id),
            })
            .await
            .context("calling list_tree")?
            .into_inner();

        let mut entries = Vec::new();
        let mut truncated = false;
        while let Some(batch) = stream.message().await? {
            entries.extend(batch.entries);
            truncated |= batch.truncated;
        }
        Ok((entries, truncated))
    }

    /// One object's kind and size, and a handle on its bytes.
    ///
    /// The header arrives first and the bytes follow, so this reads only the
    /// header, leaving the rest to be pulled — making chunking observable.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails, or the stream does not open with a
    /// header.
    pub(crate) async fn get_object(&self, repo: &str, object_id: &str) -> Result<Object> {
        let mut stream = self
            .objects
            .clone()
            .get_object(GetObjectRequest {
                repo: Some(repo_key(repo)),
                object_id: oid(object_id),
            })
            .await?
            .into_inner();

        let first = stream
            .message()
            .await?
            .and_then(|message| message.chunk)
            .context("the object stream ended before its header")?;
        let get_object_response::Chunk::Header(header) = first else {
            anyhow::bail!("the object stream did not open with a header");
        };

        Ok(Object {
            kind: header.kind(),
            size: header.size,
            stream,
        })
    }
}

/// What one requested ref update did.
#[derive(Debug)]
pub(crate) struct RefOutcome {
    pub(crate) refname: String,
    /// `None` if it landed, else what the client is told — which for a hook's
    /// refusal is the reason the hook gave, verbatim.
    pub(crate) rejection: Option<String>,
}

/// A resolved repository, as the contract describes it.
///
/// `Debug` reports the header rather than the stream, so a test asserting a
/// call failed can print what it got instead.
pub(crate) struct Object {
    pub(crate) kind: ObjectKind,
    /// What Enroute says the object is, before any of it has been read.
    pub(crate) size: u64,
    stream: tonic::Streaming<enroute_api::api::v1alpha1::GetObjectResponse>,
}

impl std::fmt::Debug for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Object")
            .field("kind", &self.kind)
            .field("size", &self.size)
            .finish_non_exhaustive()
    }
}

impl Object {
    /// The next chunk of bytes, or `None` at the end.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream fails mid-object.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        while let Some(message) = self.stream.message().await? {
            if let Some(get_object_response::Chunk::Data(data)) = message.chunk {
                return Ok(Some(data.to_vec()));
            }
        }
        Ok(None)
    }

    /// Every remaining byte, for a caller that wanted the whole object.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream fails mid-object.
    pub(crate) async fn collect(mut self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(chunk) = self.next_chunk().await? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }
}
