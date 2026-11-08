//! One contract primitive apiece, proved against a running Enroute.
//!
//! A test here reaches whichever door the fact needs — most only the gRPC one,
//! a few a real `git` as well, since what an application may reach is decided
//! at both.

#![cfg(test)]

mod commit;
mod fetch;
mod history;
mod objects;
mod reflection;
mod refs;
mod repo_keys;
mod repository;
mod sync;

mod support {
    use crate::contract::Client;
    use crate::support::{Stack, git};

    /// The path the stub application resolves to a test's one repository.
    pub(super) const PATH: &str = "anything";

    /// A repository with one commit, pushed over git.
    ///
    /// Returns the client, the repository id, and a checkout to read oids out
    /// of. A test that pushes again wants [`pushed_repo_serving`].
    pub(super) async fn pushed_repo(
        body: &str,
    ) -> (Client, String, tempfile::TempDir, std::path::PathBuf) {
        let (client, id, tmp, local, stack) = pushed_repo_serving(body).await;
        (client.holding(stack), id, tmp, local)
    }

    /// [`pushed_repo`], with the deployment handed back.
    ///
    /// The deployment lives as long as the returned [`Stack`], which a
    /// second push needs.
    pub(super) async fn pushed_repo_serving(
        body: &str,
    ) -> (Client, String, tempfile::TempDir, std::path::PathBuf, Stack) {
        let stack = Stack::start().await;
        let client = stack.client().await;
        let repo = client.create_repository("").await.unwrap();
        let id = repo.repo.clone().unwrap().key;
        stack.repos.name(PATH, &id);

        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        let url = stack.url(PATH);
        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;
        std::fs::write(local.join("a.txt"), body).unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;
        git(&["push", "origin", "main"], Some(&local)).await;
        (client, id, tmp, local, stack)
    }

    /// Where `repo` holds `refname`, as hex.
    pub(super) async fn tip(client: &Client, repo: &str, refname: &str) -> Option<String> {
        client
            .list_refs(repo)
            .await
            .unwrap()
            .refs
            .into_iter()
            .find(|r| r.name == refname)
            .map(|r| crate::contract::hex(r.object_id.as_ref()))
    }

    /// An object id no repository holds, and none ever will.
    pub(super) fn absent_oid() -> String {
        "0".repeat(39) + "1"
    }

    /// The `tonic` status inside an `anyhow` error.
    ///
    /// What a caller acts on is the code; a message is free to be reworded.
    pub(super) fn status_of(error: &anyhow::Error) -> &tonic::Status {
        error
            .downcast_ref::<tonic::Status>()
            .expect("a gRPC status")
    }

    /// A repository on a deployment with a stub application registered
    /// behind it.
    ///
    /// The deployment comes back too, since a test moving a ref has to seed
    /// the objects it moves onto, which only a push can introduce.
    pub(super) async fn hook_backed_repo(default_branch: &str) -> (Stack, Client, String) {
        let stack = Stack::start().await;
        let client = stack.client().await;
        let created = client.create_repository(default_branch).await.unwrap();
        let repo = created.repo.expect("a created repository has a key").key;
        (stack, client, repo)
    }

    /// A client for a deployment where nothing has been pushed.
    ///
    /// What a test wants when nothing in it has to push.
    pub(super) async fn contract_only() -> Client {
        let stack = Stack::start().await;
        let client = stack.client().await;
        client.holding(stack)
    }

    /// [`contract_only`], with a repository already created on it.
    pub(super) async fn contract_with_repo() -> (Client, String) {
        let client = contract_only().await;
        let id = client
            .create_repository("")
            .await
            .unwrap()
            .repo
            .expect("a created repository has a key")
            .key;
        (client, id)
    }
}
