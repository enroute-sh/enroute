//! The key an application names a repository by.
//!
//! Enroute allocates no id of its own, so these are the facts an integration
//! rests on: the key you gave is the key you get back, creating twice with one
//! key is one repository, and a key means nothing outside the tenant that
//! chose it.

use crate::contract::Client;
use crate::grpc::support::{contract_only, status_of};
use crate::support::{E2E_TENANT, make_isolated_state, spawn_contract_without_hooks};

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
    let enroute = spawn_contract_without_hooks(state.clone()).await;
    let client = Client::connect(format!("http://{enroute}"), E2E_TENANT)
        .await
        .unwrap();

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

/// Two tenants may each call a repository the same thing.
///
/// Why a key needs no unguessability: scoped to whoever chose it, one tenant's
/// keys neither collide with another's nor reach them.
#[tokio::test]
async fn one_key_names_a_different_repository_for_each_tenant() {
    let (acme, other) = crate::grpc::tenancy::two_tenants().await;

    let mine = acme.create_repository_as("shared-name", "").await.unwrap();
    let theirs = other
        .create_repository_as("shared-name", "")
        .await
        .expect("another tenant's key blocked this one");

    // The same string, and two repositories: neither tenant can tell the
    // other's exists, let alone reach it.
    assert_eq!(mine.repo.expect("a key").key, "shared-name");
    assert_eq!(theirs.repo.expect("a key").key, "shared-name");

    acme.delete_repository("shared-name").await.unwrap();
    // Deleting one leaves the other alone, which a shared row would not.
    other
        .get_repository("shared-name")
        .await
        .expect("deleting one tenant's repository took another's");
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
