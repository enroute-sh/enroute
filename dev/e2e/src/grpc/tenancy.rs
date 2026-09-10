//! One tenant reaching for another's repository, over the contract.
//!
//! The tenancy unit tests cover the queries; this covers the wiring against
//! a running server, with two tokens differing only in who minted them.

use crate::contract::Client;
use crate::support::{
    git_allowing_failure, listed_tenancy, make_isolated_state, refreshing_tenancy, rewrite,
    spawn_contract_without_hooks, tenants_file,
};

use super::support::status_of;

/// Two tenants on one Enroute, and a client for each.
///
/// Neither claims a hostname: nothing here serves git, so nothing has to
/// resolve, and the tokens are the whole of what tells them apart.
pub(super) async fn two_tenants() -> (Client, Client) {
    let state = make_isolated_state().await;
    let tenants = listed_tenancy(&tenants_file(
        &[
            ("acme", "http://127.0.0.1:1/never-called"),
            ("other", "http://127.0.0.1:1/never-called"),
        ],
        "",
    ))
    .await;

    let addr = crate::support::spawn_enroute(state, tenants).await;
    let connect = async |token: &str| {
        Client::connect(format!("http://{addr}"), token)
            .await
            .expect("connecting to the contract")
    };
    (connect("acme").await, connect("other").await)
}

#[tokio::test]
async fn a_tenant_cannot_read_another_tenants_repository() {
    let (acme, other) = two_tenants().await;
    let repo = acme.create_repository("").await.unwrap();
    let id = repo.repo.clone().unwrap().key;

    // Its owner can, which is what makes the refusals below mean
    // something rather than the id being broken.
    acme.get_repository(&id).await.expect("its own repository");
    acme.list_refs(&id).await.expect("its own refs");

    let refused = other
        .get_repository(&id)
        .await
        .expect_err("another tenant's repository");
    assert!(
        format!("{refused}").contains("no such repository"),
        "a cross-tenant read must not say what it found: {refused}"
    );
    other
        .list_refs(&id)
        .await
        .expect_err("another tenant's refs");
    other
        .get_object(&id, "0000000000000000000000000000000000000000")
        .await
        .expect_err("another tenant's objects");
}

/// A listing is the one read that names no key, so it is the one place a
/// tenant could be handed another's repositories by accident.
///
/// Both tenants use one key, which each is free to: a key is unique within a
/// tenant, so a listing scoped by anything less would report the wrong one.
#[tokio::test]
async fn a_listing_reports_only_the_callers_repositories() {
    let (acme, other) = two_tenants().await;
    acme.create_repository_as("widgets", "").await.unwrap();
    other.create_repository_as("widgets", "").await.unwrap();
    other.create_repository_as("gadgets", "").await.unwrap();

    let (listed, token) = acme.list_repositories(0, "").await.unwrap();
    let keys: Vec<String> = listed
        .iter()
        .map(|one| one.repo.clone().unwrap().key)
        .collect();

    assert_eq!(keys, ["widgets"], "a tenant sees theirs and only theirs");
    assert!(token.is_empty(), "one repository is one page");
}

/// Deleting somebody else's repository answers as though it were already
/// gone, and leaves it there.
///
/// Refusing would say it exists, indistinguishable from having deleted it.
#[tokio::test]
async fn a_tenant_cannot_delete_another_tenants_repository() {
    let (acme, other) = two_tenants().await;
    let repo = acme.create_repository("").await.unwrap();
    let id = repo.repo.clone().unwrap().key;

    other
        .delete_repository(&id)
        .await
        .expect("a delete of somebody else's repository reads as done");
    acme.get_repository(&id)
        .await
        .expect("and leaves the repository alone");

    acme.delete_repository(&id).await.expect("its own");
    acme.get_repository(&id)
        .await
        .expect_err("its own, once deleted");
}

/// A repository is its creator's from the moment it exists — there is no
/// window in which it belongs to nobody and anybody may have it.
#[tokio::test]
async fn a_new_repository_belongs_to_whoever_created_it() {
    let (acme, other) = two_tenants().await;
    let theirs = other.create_repository("").await.unwrap();
    let id = theirs.repo.clone().unwrap().key;

    other.get_repository(&id).await.expect("its own");
    acme.get_repository(&id)
        .await
        .expect_err("the other tenant's");
}

/// An application naming a repository that is not its tenant's, over git.
///
/// Enroute asks which repository a URL means and takes the answer — trusted
/// outright, an application could serve another tenant's.
#[tokio::test]
async fn an_application_cannot_grant_another_tenants_repository() {
    use crate::support::{endpoint_signing_key, spawn_git, spawn_hooks};

    let state = make_isolated_state().await;
    let repo = state.rows.create(None).await.unwrap();

    // Two tenants: the one the repository really belongs to, and one whose
    // application cheerfully names it anyway and which every git request
    // reaches.
    let (hooks, token, _landed, _hook_servers) = spawn_hooks(&[("hello", repo.id)]).await;
    // Both claim every hostname; only the first to be read keeps it, and
    // which one that is does not matter — the attacker is reached by its
    // own token either way.
    let toml = tenants_file(
        &[(
            "attacker",
            &bench_support::hooks::endpoint_url(&format!("http://{hooks}")),
        )],
        "domains = [\"*\"]\n",
    ) + &tenants_file(&[("victim", "http://127.0.0.1:1/never-called")], "");
    let tenants = listed_tenancy(&toml).await;

    let victim = tenants.by_id("victim").unwrap();
    // The victim's, under the very key the attacker's application answers
    // with: the same string means one repository for them and none at all for
    // anybody else.
    tenants
        .claim(
            &victim,
            repo.id,
            &crate::support::key_for("hello").parse().unwrap(),
        )
        .await
        .unwrap();

    let hooks = std::sync::Arc::new(
        enroute::hooks::Hooks::new(
            tenants,
            endpoint_signing_key(),
            std::time::Duration::from_secs(10),
        )
        .unwrap(),
    );
    let (front_door, _servers) = spawn_git(state, hooks.clone(), hooks.clone(), hooks).await;

    // `ls-remote` rather than `clone`: the advertisement is the first
    // thing authorization gates, so a refusal lands before any pack.
    let tmp = tempfile::tempdir().unwrap();
    let url = format!("http://alice:{token}@{front_door}/hello.git");
    let (ok, output) = git_allowing_failure(&["ls-remote", &url], tmp.path()).await;
    assert!(!ok, "a hooks granted another tenant's repository: {output}");
}

/// A call naming a tenant this deployment does not serve.
///
/// The same answer a call naming nobody gets, so a caller cannot learn which
/// ids exist by trying them.
#[tokio::test]
async fn a_tenant_this_serves_nobody_by_reaches_nothing() {
    let state = make_isolated_state().await;
    let tenants = listed_tenancy(&tenants_file(
        &[("acme", "http://127.0.0.1:1/never-called")],
        "",
    ))
    .await;
    let addr = crate::support::spawn_enroute(state, tenants).await;

    let stranger = Client::connect(format!("http://{addr}"), "nobody")
        .await
        .expect("connecting is not being served");
    let refused = stranger
        .create_repository("")
        .await
        .expect_err("a tenant this does not serve");
    // The message Enroute sends, not the code's own description: naming a
    // tenant nobody serves and naming none are meant to read identically, and
    // this is the text that says so.
    assert!(
        format!("{refused}").contains("no tenant was named"),
        "{refused}"
    );
}

/// A caller presenting no token at all reaches nothing.
///
/// Distinct from a token nobody minted: this one never says who it is, and is
/// turned away by the layer before a handler is reached.
#[tokio::test]
async fn a_caller_with_no_token_reaches_nothing() {
    let state = make_isolated_state().await;
    let addr = spawn_contract_without_hooks(state).await;
    let client = Client::connect_anonymously(format!("http://{addr}"))
        .await
        .unwrap();

    let error = client
        .create_repository("")
        .await
        .expect_err("an unauthenticated caller creates nothing");
    assert_eq!(
        status_of(&error).code(),
        tonic::Code::Unauthenticated,
        "{error:?}"
    );
}

/// Two tenants, as a tenants list names them.
fn before() -> String {
    tenants_file(
        &[
            ("acme", "https://acme.example/enroute/hooks"),
            ("spare", "https://spare.example/enroute/hooks"),
        ],
        "",
    )
}

/// The same, with one tenant added and one taken away.
fn after() -> String {
    tenants_file(
        &[
            ("spare", "https://spare.example/enroute/hooks"),
            ("other", "https://other.example/enroute/hooks"),
        ],
        "",
    )
}

/// Wait for something a timer will do, rather than for the timer.
///
/// A refresh happens on its own schedule, so a test that asserted once would
/// be asserting on that schedule instead of on the refresh.
async fn eventually(what: &str, mut ready: impl AsyncFnMut() -> bool) {
    for _ in 0..100 {
        if ready().await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("{what} did not happen within five seconds");
}

/// Editing the tenants is what onboarding is, and a running server picks the
/// edit up without a restart and without being signalled.
#[tokio::test]
async fn a_refresh_changes_who_may_call() {
    let state = make_isolated_state().await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("tenants.toml");
    rewrite(&file, &before());

    let tenants = refreshing_tenancy(&file, std::time::Duration::from_millis(50)).await;
    let addr = crate::support::spawn_enroute(state, tenants).await;
    let connect = async |token: &str| {
        Client::connect(format!("http://{addr}"), token)
            .await
            .expect("connecting to the contract")
    };

    // Before: the list names acme, and does not name other.
    let acme = connect("acme").await;
    let id = acme.create_repository("").await.unwrap().repo.unwrap().key;
    connect("other")
        .await
        .create_repository("")
        .await
        .expect_err("a signer the list does not name yet");

    rewrite(&file, &after());

    eventually("the added tenant starts serving", async || {
        connect("other").await.create_repository("").await.is_ok()
    })
    .await;

    // And the tenant the edit removed stops being served, which is the half
    // that has to work for a list to be a way to revoke anything.
    eventually("the removed tenant stops being served", async || {
        connect("acme").await.get_repository(&id).await.is_err()
    })
    .await;

    // A tenant the edit left alone is untouched by any of it.
    connect("spare")
        .await
        .create_repository("")
        .await
        .expect("a tenant the edit did not name");
}

/// A file that will not load leaves the tenants that were already serving.
///
/// The failure worth refusing: a bad edit must not empty the directory and
/// take every customer offline until somebody notices.
#[tokio::test]
async fn a_broken_edit_keeps_the_tenants_that_were_serving() {
    let state = make_isolated_state().await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("tenants.toml");
    rewrite(&file, &before());

    let tenants = refreshing_tenancy(&file, std::time::Duration::from_millis(50)).await;
    let addr = crate::support::spawn_enroute(state, tenants).await;
    let acme = Client::connect(format!("http://{addr}"), "acme")
        .await
        .unwrap();
    let id = acme.create_repository("").await.unwrap().repo.unwrap().key;

    rewrite(
        &file,
        "[[tenants]]\nid = \"acme\"\ntoken = \"not a field\"\n",
    );

    // Long enough for several refreshes to have read it and refused it.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    acme.get_repository(&id)
        .await
        .expect("the tenants that were serving are still serving");
}

/// The example tenants file loads, and says what its comments say it says.
///
/// `include_str!`, so moving the file breaks the build rather than leaving a
/// documented example nothing checks.
#[test]
fn the_example_tenants_file_is_one_that_would_serve() {
    let example = include_str!("../../../tenants.example.toml");
    let directory =
        enroute::tenancy::Directory::from_toml(example).expect("dev/tenants.example.toml");

    let listing = directory.listing();
    let [acme, dev] = listing.as_slice() else {
        panic!("the example names two tenants");
    };
    // The two halves the file is there to show: a tenant reached by its own
    // hostnames, and one claiming whatever nothing else did.
    assert_eq!(acme.tenant.id.as_str(), "acme");
    assert_eq!(
        acme.domains,
        ["*.acme.com", "acme.enroute.sh", "git.acme.com"]
    );

    assert_eq!(dev.tenant.id.as_str(), "dev");
    assert_eq!(dev.domains, ["*"]);
}
