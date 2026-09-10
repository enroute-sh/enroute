//! One contract primitive apiece, proved against a running Enroute.
//!
//! A test here reaches whichever door the fact needs — most only the gRPC one,
//! a few a real `git` as well, since what an application may reach is decided
//! at both.

#![cfg(test)]

mod history;
mod objects;
mod reflection;
mod refs;
mod repo_keys;
mod repository;
mod sync;
mod tenancy;

mod support {
    use enroute_git_retrieve::Storage;

    use crate::contract::Client;
    use crate::support::{
        E2E_TENANT, front_door_for, git, make_isolated_state, spawn_contract_with_hooks,
        spawn_contract_without_hooks,
    };

    /// A repository with one commit, pushed through the contract.
    ///
    /// Returns the client, the repository id, and a checkout to read oids out
    /// of. A test that pushes again wants [`pushed_repo_serving`].
    pub(super) async fn pushed_repo(
        body: &str,
    ) -> (Client, String, tempfile::TempDir, std::path::PathBuf) {
        let (client, id, tmp, local, _servers) = pushed_repo_serving(body).await;
        (client, id, tmp, local)
    }

    /// [`pushed_repo`], with the front door left up.
    ///
    /// The servers live as long as the guard, which is what a second push
    /// needs: dropping it takes the door down with it.
    pub(super) async fn pushed_repo_serving(
        body: &str,
    ) -> (
        Client,
        String,
        tempfile::TempDir,
        std::path::PathBuf,
        crate::support::Servers,
    ) {
        let state = make_isolated_state().await;
        let enroute = spawn_contract_without_hooks(state.clone()).await;
        let client = Client::connect(format!("http://{enroute}"), E2E_TENANT)
            .await
            .unwrap();
        let repo = client.create_repository("").await.unwrap();
        let id = repo.repo.clone().unwrap().key;

        let (front_door, servers) = front_door_for(state).await;
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        let url = format!("http://{front_door}/anything.git");
        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;
        std::fs::write(local.join("a.txt"), body).unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;
        git(&["push", "origin", "main"], Some(&local)).await;
        (client, id, tmp, local, servers)
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

    /// A repository on a contract with a stub application registered behind it.
    ///
    /// The state comes back too, since a test moving a ref has to seed the
    /// objects it moves onto — which no contract call can introduce.
    pub(super) async fn hook_backed_repo(
        default_branch: &str,
    ) -> (Storage, Client, String, crate::support::Servers) {
        let state = make_isolated_state().await;
        let (addr, token, servers) = spawn_contract_with_hooks(state.clone()).await;
        let client = Client::connect(format!("http://{addr}"), &token)
            .await
            .unwrap();
        let created = client.create_repository(default_branch).await.unwrap();
        let repo = created.repo.expect("a created repository has a key").key;
        (state, client, repo, servers)
    }

    /// A client for a Enroute that serves no git.
    ///
    /// What a test wants when nothing in it has to push.
    pub(super) async fn contract_only() -> Client {
        let state = make_isolated_state().await;
        let enroute = spawn_contract_without_hooks(state).await;
        Client::connect(format!("http://{enroute}"), E2E_TENANT)
            .await
            .unwrap()
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
