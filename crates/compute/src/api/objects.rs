//! `ObjectService`.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use enroute_api::api::v1alpha1::object_service_server::ObjectService;
use enroute_api::api::v1alpha1::{
    Commit, DiffCommitRequest, DiffCommitResponse, FileChange, FindMergeBasesRequest,
    FindMergeBasesResponse, GetObjectRequest, GetObjectResponse, Identity, ListCommitsRequest,
    ListCommitsResponse, ListTreeRequest, ListTreeResponse, ObjectHeader, ObjectKind, TreeEntry,
    get_object_response,
};

use crate::contract::wire::{parse_hex, parse_oid, require_oid, wire_oid, wire_time};
use crate::git::oid::Kind;
use crate::query;

use super::{Api, ResponseStream, streamed};

/// How much of an object's bytes go in one message.
const OBJECT_CHUNK_BYTES: usize = 512 * 1024;

/// How many changed paths go in one message.
const DIFF_BATCH_CHANGES: usize = 1_000;

/// Git's raw bytes as the contract's string.
fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn object_kind(kind: Kind) -> ObjectKind {
    match kind {
        Kind::Commit => ObjectKind::Commit,
        Kind::Tree => ObjectKind::Tree,
        Kind::Blob => ObjectKind::Blob,
        Kind::Tag => ObjectKind::Tag,
    }
}

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
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        let oid = require_oid(request.object_id.as_ref(), "object_id")?;
        let (kind, data) = query::get_object(&snap, oid).await?;
        let header = GetObjectResponse {
            chunk: Some(get_object_response::Chunk::Header(ObjectHeader {
                kind: object_kind(kind).into(),
                size: u64::try_from(data.len()).unwrap_or(u64::MAX),
            })),
        };
        let mut rest = bytes::Bytes::from(Arc::unwrap_or_clone(data));
        let mut messages = vec![header];
        while !rest.is_empty() {
            let chunk = rest.split_to(OBJECT_CHUNK_BYTES.min(rest.len()));
            messages.push(GetObjectResponse {
                chunk: Some(get_object_response::Chunk::Data(chunk)),
            });
        }
        Ok(Response::new(streamed(messages)))
    }

    async fn list_tree(
        &self,
        request: Request<ListTreeRequest>,
    ) -> Result<Response<Self::ListTreeStream>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        let oid = require_oid(request.object_id.as_ref(), "object_id")?;
        let walk = query::list_tree(&snap, oid).await?;
        let mut batches: Vec<ListTreeResponse> = walk
            .levels
            .into_iter()
            .map(|level| ListTreeResponse {
                entries: level
                    .into_iter()
                    .map(|item| TreeEntry {
                        path: lossy(&item.path),
                        kind: if item.is_tree {
                            ObjectKind::Tree
                        } else {
                            ObjectKind::Blob
                        }
                        .into(),
                        object_id: Some(wire_oid(item.oid)),
                    })
                    .collect(),
                truncated: false,
            })
            .collect();
        if walk.truncated
            && let Some(last) = batches.last_mut()
        {
            last.truncated = true;
        }
        Ok(Response::new(streamed(batches)))
    }

    async fn list_commits(
        &self,
        request: Request<ListCommitsRequest>,
    ) -> Result<Response<Self::ListCommitsStream>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        // A page token, when set, is the only start read.
        let tip = if request.page_token.is_empty() {
            require_oid(request.commit_id.as_ref(), "commit_id")?
        } else {
            parse_hex(&request.page_token, "a page token")?
        };
        let page = query::list_commits(&snap, tip, request.limit).await?;
        let identity = |who: &query::Identity| Identity {
            name: lossy(&who.name),
            email: lossy(&who.email),
            timestamp: Some(wire_time(who.seconds)),
            utc_offset_seconds: who.offset_seconds,
        };
        let response = ListCommitsResponse {
            commits: page
                .commits
                .iter()
                .map(|commit| Commit {
                    commit_id: Some(wire_oid(commit.id)),
                    parent_commit_ids: commit.parents.iter().copied().map(wire_oid).collect(),
                    tree_id: Some(wire_oid(commit.tree)),
                    author: Some(identity(&commit.author)),
                    committer: Some(identity(&commit.committer)),
                    summary: lossy(&commit.summary),
                    body: lossy(&commit.body),
                })
                .collect(),
            next_page_token: page.next.map_or_default(|oid| oid.to_hex()),
        };
        Ok(Response::new(streamed(vec![response])))
    }

    async fn diff_commit(
        &self,
        request: Request<DiffCommitRequest>,
    ) -> Result<Response<Self::DiffCommitStream>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        let oid = require_oid(request.commit_id.as_ref(), "commit_id")?;
        let base = request.base_commit_id.as_ref().map(parse_oid).transpose()?;
        let diff = query::diff_commit(&snap, oid, base).await?;
        let mut batches: Vec<DiffCommitResponse> = diff
            .changes
            .chunks(DIFF_BATCH_CHANGES)
            .map(|chunk| DiffCommitResponse {
                changes: chunk
                    .iter()
                    .map(|change| FileChange {
                        path: lossy(&change.path),
                        old_object_id: change.old.map(wire_oid),
                        new_object_id: change.new.map(wire_oid),
                    })
                    .collect(),
                truncated: false,
            })
            .collect();
        if diff.truncated {
            match batches.last_mut() {
                Some(last) => last.truncated = true,
                None => batches.push(DiffCommitResponse {
                    changes: Vec::new(),
                    truncated: true,
                }),
            }
        }
        Ok(Response::new(streamed(batches)))
    }

    #[tracing::instrument(name = "enroute::grpc::find_merge_bases", skip(self, request))]
    async fn find_merge_bases(
        &self,
        request: Request<FindMergeBasesRequest>,
    ) -> Result<Response<FindMergeBasesResponse>, Status> {
        let request = request.into_inner();
        let (_, snap) = self.snapshot(request.repo.as_ref()).await?;
        let a = require_oid(request.commit_id_a.as_ref(), "commit_id_a")?;
        let b = require_oid(request.commit_id_b.as_ref(), "commit_id_b")?;
        let found = query::merge_bases(&snap, a, b)
            .await
            .map_err(query::Error::from)?;
        Ok(Response::new(FindMergeBasesResponse {
            base_commit_ids: found.bases.into_iter().map(wire_oid).collect(),
            exhausted: found.exhausted,
        }))
    }
}
