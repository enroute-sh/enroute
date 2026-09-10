//! What an object is, and where its bytes are.
//!
//! One identity read says what kind each oid is; the commit graph and the
//! object index then answer for their own kinds in parallel. Neither can
//! answer the other's, and a caller with a bare oid does not know which to
//! ask — which is the whole reason this is not a method on either.

use anyhow::Result;
use gix_hash::ObjectId;

use enroute_git_core::{ObjectHashMap, ObjectMeta, RepoId};
use enroute_git_metadata::Identity;

use crate::Storage;

/// What the repository holds `oid` as, and where its bytes are.
///
/// # Errors
/// Whatever any of them said.
#[tracing::instrument(name = "enroute_git_retrieve::meta", skip(storage), fields(repo_id = %repo_id, oid = %oid))]
pub async fn meta(storage: &Storage, repo_id: RepoId, oid: ObjectId) -> Result<Option<ObjectMeta>> {
    Ok(metas(storage, repo_id, &[oid]).await?.remove(&oid))
}

/// The same for many oids: one identity read, then both index halves
/// in parallel.
///
/// # Errors
/// Whatever any of them said.
#[tracing::instrument(name = "enroute_git_retrieve::metas", skip(storage, oids), fields(repo_id = %repo_id, oid_count = oids.len()))]
pub async fn metas(
    storage: &Storage,
    repo_id: RepoId,
    oids: &[ObjectId],
) -> Result<ObjectHashMap<ObjectMeta>> {
    // One question, then both homes read together: identity answers for
    // every kind at once, and a commit's pack facts are the graph store's
    // where an object's are the index's.
    let named: Vec<(ObjectId, Identity)> = storage
        .rows
        .repo(repo_id)
        .identify(oids)
        .await?
        .into_iter()
        .collect();
    let graph = storage.graph.repo(repo_id);
    let objects = storage.objects.repo(repo_id);
    let (commits, objects) = tokio::try_join!(graph.locations_of(&named), objects.lookup(&named))?;
    Ok(commits.into_iter().chain(objects).collect())
}
