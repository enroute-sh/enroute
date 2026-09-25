//! The key an application names a repository by.
//!
//! Enroute allocates no id of its own, so these are the facts an integration
//! rests on: the key you gave is the key you get back, creating twice with one
//! key is one repository, and a key released by a delete is free again.

use crate::contract::Client;
use crate::grpc::support::{contract_only, status_of};
use crate::support::{make_isolated_state, spawn_enroute};

/// The key comes back exactly as it was given.
///
/// Byte-exact, so an application can hold one identifier rather than ours
/// beside its own.
#[tokio::test]
async fn a_key_is_returned_as_it_was_given() {
    let client = contract_only().await;

    for key in [
        "Repo",
        "3f1b9a4e-0000-4000-8000-000000000000",
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "1",
        "repo_1.2",
    ] {
        let made = client.create_repository_as(key, "").await.unwrap();
        assert_eq!(made.repo.expect("a key").key, key);

        let read = client.get_repository(key).await.unwrap();
        assert_eq!(read.repo.expect("a key").key, key);
    }
}

/// Creating twice with one key is one repository.
///
/// The whole of what makes a create safe to retry: an application that never
/// recorded the first answer, or never saw it, may simply ask again.
#[tokio::test]
async fn creating_twice_with_one_key_is_idempotent() {
    let state = make_isolated_state().await;
    let enroute = spawn_enroute(state.clone()).await;
    let client = Client::connect(format!("http://{enroute}")).await.unwrap();

    let first = client.create_repository_as("once", "").await.unwrap();
    let again = client.create_repository_as("once", "").await.unwrap();
    assert_eq!(
        first.repo.expect("a key").key,
        again.repo.expect("a key").key
    );

    // One repository, not two: the engine made nothing the second time.
    let held = state.rows.all().await.unwrap();
    assert_eq!(held.len(), 1, "a repeated create made a second repository");
}

/// A repeat naming a different default branch does not move `HEAD`.
///
/// `default_branch` belongs to the create that made the repository, so a retry
/// carrying something else is answered with what is there rather than refused.
#[tokio::test]
async fn a_repeat_does_not_change_the_default_branch() {
    let client = contract_only().await;

    let first = client
        .create_repository_as("trunked", "refs/heads/trunk")
        .await
        .unwrap();
    assert_eq!(first.default_branch, "refs/heads/trunk");

    let again = client
        .create_repository_as("trunked", "refs/heads/other")
        .await
        .unwrap();
    assert_eq!(
        again.default_branch, "refs/heads/trunk",
        "a repeat moved the default branch"
    );
}

/// A deleted key is free, and creating with it again is a new repository.
#[tokio::test]
async fn a_deleted_key_can_be_used_again() {
    let client = contract_only().await;

    client.create_repository_as("recycled", "").await.unwrap();
    client.delete_repository("recycled").await.unwrap();

    let error = client
        .get_repository("recycled")
        .await
        .expect_err("a deleted key still resolves");
    assert_eq!(status_of(&error).code(), tonic::Code::NotFound);

    client
        .create_repository_as("recycled", "")
        .await
        .expect("a released key was not free again");
}

/// A listing narrowed to one grouping, over the contract.
///
/// The one thing an application cannot do for itself with a flat key space:
/// walk a group without reading every repository in the deployment.
#[tokio::test]
async fn a_listing_can_be_narrowed_to_a_prefix() {
    let client = contract_only().await;

    for key in ["acme.backend", "acme.web", "acmex", "globex.api"] {
        client.create_repository_as(key, "").await.unwrap();
    }

    let keys = |page: Vec<enroute_api::api::v1alpha1::Repository>| {
        let mut held: Vec<String> = page
            .into_iter()
            .map(|one| one.repo.expect("a key").key)
            .collect();
        held.sort();
        held
    };

    let (page, next) = client
        .list_repositories_under("acme.", 50, "")
        .await
        .unwrap();
    assert_eq!(keys(page), ["acme.backend", "acme.web"]);
    assert!(next.is_empty());

    // The separator is the caller's: without it, the neighbour is in.
    let (page, _) = client
        .list_repositories_under("acme", 50, "")
        .await
        .unwrap();
    assert_eq!(keys(page), ["acme.backend", "acme.web", "acmex"]);

    let (page, _) = client
        .list_repositories_under("nobody.", 50, "")
        .await
        .unwrap();
    assert!(page.is_empty());
}

/// Paging inside a prefix stays inside it, and a token from another walk is
/// refused rather than quietly resumed somewhere else.
#[tokio::test]
async fn a_page_token_belongs_to_the_prefix_that_minted_it() {
    let client = contract_only().await;

    for key in ["acme.backend", "acme.web", "globex.api"] {
        client.create_repository_as(key, "").await.unwrap();
    }

    let (first, next) = client
        .list_repositories_under("acme.", 1, "")
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert!(!next.is_empty(), "a page of one of two reports a next");

    let (second, last) = client
        .list_repositories_under("acme.", 50, &next)
        .await
        .unwrap();
    assert_eq!(second.len(), 1, "the rest of the group");
    assert!(last.is_empty());

    // The same token against another prefix is two walks confused for one.
    let error = client
        .list_repositories_under("globex.", 50, &next)
        .await
        .expect_err("a token from another prefix");
    assert_eq!(status_of(&error).code(), tonic::Code::InvalidArgument);
}

/// A prefix holding what no key holds is the caller's to fix, and saying so
/// beats reporting an empty deployment.
#[tokio::test]
async fn a_prefix_that_could_start_no_key_is_refused() {
    let client = contract_only().await;

    for bad in ["acme/backend", "a b", "acme%2F"] {
        let error = client
            .list_repositories_under(bad, 50, "")
            .await
            .expect_err(bad);
        assert_eq!(
            status_of(&error).code(),
            tonic::Code::InvalidArgument,
            "{bad}"
        );
    }
}

/// A key the rules refuse is an `InvalidArgument`, and says which rule.
///
/// Unlike a key that names nothing, this is the caller's own to fix: they
/// chose it, so telling them why costs nothing and saves a guess.
#[tokio::test]
async fn a_key_the_rules_refuse_says_so() {
    let client = contract_only().await;

    let too_long = "k".repeat(257);
    for key in [
        "",
        "a\nb",
        "a b",
        "ünïcode",
        "-leading",
        "trailing.",
        "acme/backend",
        "a?b",
        too_long.as_str(),
    ] {
        let error = client
            .create_repository_as(key, "")
            .await
            .expect_err("a key the rules refuse was taken");
        assert_eq!(
            status_of(&error).code(),
            tonic::Code::InvalidArgument,
            "wrong status for the key {key:?}"
        );
    }
}
