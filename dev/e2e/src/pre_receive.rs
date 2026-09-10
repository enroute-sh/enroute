//! Wire-level smoke tests for the `pre-receive` hook: a rejection reaches a
//! real `git push`, and does not reach the application's own calls.
//!
//! Both doors, because they are not the same door. The hook is how an
//! application decides what a *git client* may land; the contract is the
//! application itself calling, and asking it to authorize its own call would
//! only catch its own bugs.

#[cfg(test)]
mod tests {
    use crate::support::{git, git_allowing_failure, spawn_smoke_repo};

    #[tokio::test]
    async fn a_hook_rejection_reaches_the_pusher() {
        let (_tmp, local, _landed, _servers) = spawn_smoke_repo().await;
        let local = local.as_path();

        // A branch the stub application has no opinion about lands, so the
        // failure below is the hook's doing and not a broken push path.
        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["push", "origin", "main"], Some(local)).await;

        // The one refname the stub refuses. A real application decides this
        // from what it knows; the stub decides by name, which is all a harness
        // needs to prove the answer travels.
        let protected = crate::hooks::PROTECTED
            .strip_prefix("refs/heads/")
            .expect("the stub protects a branch");
        git(&["checkout", "-b", protected], Some(local)).await;
        git(&["commit", "--allow-empty", "-m", "second"], Some(local)).await;

        let (ok, out) = git_allowing_failure(&["push", "origin", protected], local).await;
        assert!(!ok, "a push the hook refused must fail:\n{out}");
        assert!(
            out.contains("is protected by the stub application"),
            "the push output is missing the hook's reason:\n{out}"
        );
    }

    /// A command the application judged neither way fails the push.
    ///
    /// An application that answers for part of a push has decided nothing
    /// about the rest, and silence read as consent lands what its policy missed.
    #[tokio::test]
    async fn a_command_the_application_left_unjudged_fails_the_push() {
        let (_tmp, local, _landed, _servers) = spawn_smoke_repo().await;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;

        let unjudged = crate::hooks::UNJUDGED
            .strip_prefix("refs/heads/")
            .expect("the stub leaves a branch unjudged");
        git(&["checkout", "-b", unjudged], Some(local)).await;
        git(&["commit", "--allow-empty", "-m", "second"], Some(local)).await;

        let (ok, out) = git_allowing_failure(&["push", "origin", unjudged], local).await;
        assert!(!ok, "a push nobody judged must not land:\n{out}");
        // Legibility to whoever has to fix the endpoint is the whole reason
        // for failing the push rather than guessing at it.
        assert!(
            out.contains("judged"),
            "the push output does not say what was left unjudged:\n{out}"
        );
    }

    /// What `post-receive` says reaches the person who pushed.
    ///
    /// A `remote:` line, and the only way an application can reach them at all:
    /// it holds no connection, and by here nothing is left to refuse.
    #[tokio::test]
    async fn what_post_receive_says_reaches_the_pusher() {
        let (_tmp, local, _landed, _servers) = spawn_smoke_repo().await;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        let out = git(&["push", "origin", "main"], Some(local)).await;

        assert!(
            out.contains(&format!("{} refs/heads/main", crate::hooks::LANDED_PREFIX)),
            "the push output is missing what the application said:\n{out}"
        );
    }

    /// What `authorize` said about the pusher reaches the hooks unchanged.
    ///
    /// The context is opaque to Enroute by contract, so the only thing there is
    /// to assert is that the bytes arrive as the application wrote them.
    #[tokio::test]
    async fn the_actor_and_its_context_are_played_back_to_the_application() {
        let (_tmp, local, landed, _servers) = spawn_smoke_repo().await;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["push", "origin", "main"], Some(local)).await;

        let seen = landed.seen();
        let push = seen.first().expect("the application was told what landed");
        assert_eq!(push.actor, crate::hooks::ACTOR);
        assert_eq!(push.context, crate::hooks::CONTEXT);
    }

    /// The refused branch alone fails.
    ///
    /// Git reports per ref: an application objecting to one has not objected to
    /// the rest.
    #[tokio::test]
    async fn a_rejection_does_not_take_the_rest_of_the_push_with_it() {
        let (_tmp, local, _landed, _servers) = spawn_smoke_repo().await;
        let local = local.as_path();

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        let protected = crate::hooks::PROTECTED
            .strip_prefix("refs/heads/")
            .expect("the stub protects a branch");
        git(&["branch", protected], Some(local)).await;
        git(&["branch", "fine"], Some(local)).await;

        let (ok, out) = git_allowing_failure(&["push", "origin", protected, "fine"], local).await;
        assert!(!ok, "the refused branch still fails the command:\n{out}");

        // What landed is the question, and the remote is what answers it.
        let refs = git(&["ls-remote", "--heads", "origin"], Some(local)).await;
        assert!(refs.contains("refs/heads/fine"), "{refs}");
        assert!(
            !refs.contains(crate::hooks::PROTECTED),
            "the refused branch must not be there:\n{refs}"
        );
    }
}

/// The same refusal, over the contract rather than over git.
///
/// The contract's `UpdateRefs` runs the same hook through the same
/// `apply_ref_updates`; only the answer's spelling differs.
#[cfg(test)]
mod contract_tests {
    use enroute_api::api::v1alpha1::RefUpdate;
    use enroute_git_test_support::linear_commit;

    use crate::contract::{Client, oid};
    use crate::support::{make_isolated_state, seed_pack, spawn_contract_with_hooks};

    /// The contract does not run the hook, and the same refname proves it.
    ///
    /// `PROTECTED` is the name the stub refuses, and the test above pushes it
    /// and fails. Here it lands, because the caller wrote that rule.
    #[tokio::test]
    async fn the_contract_is_not_held_to_the_git_door_s_hook() {
        let state = make_isolated_state().await;
        let (addr, token, _servers) = spawn_contract_with_hooks(state.clone()).await;
        let client = Client::connect(format!("http://{addr}"), &token)
            .await
            .unwrap();

        let created = client.create_repository("refs/heads/main").await.unwrap();
        let repo = created.repo.expect("a created repository has a key").key;

        let commit = linear_commit(b"hello\n", None, 1, "first");

        // The objects have to be stored before a ref can name them: a push is
        // still the only way one arrives, which is what the contract enforces
        // in place of a hook.
        seed_pack(&state, "refs/heads/seed", &[&commit]).await;

        let updates = vec![RefUpdate {
            refname: crate::hooks::PROTECTED.to_string(),
            old_object_id: None,
            new_object_id: oid(commit.commit_sha.clone()),
        }];

        let outcomes = client.update_refs(&repo, updates).await.unwrap();
        let protected = outcomes
            .iter()
            .find(|outcome| outcome.refname == crate::hooks::PROTECTED)
            .expect("an outcome for every requested update");
        assert_eq!(protected.rejection, None, "{outcomes:?}");

        let refs = client.list_refs(&repo).await.unwrap();
        let names: Vec<&str> = refs.refs.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&crate::hooks::PROTECTED), "{names:?}");
    }

    /// An object nobody pushed cannot be named, however the contract is called.
    ///
    /// The one rule the contract keeps, and the reason it needs no hook to
    /// keep it: git stays the only way an object arrives.
    #[tokio::test]
    async fn the_contract_cannot_name_an_object_that_never_arrived() {
        let state = make_isolated_state().await;
        let (addr, token, _servers) = spawn_contract_with_hooks(state.clone()).await;
        let client = Client::connect(format!("http://{addr}"), &token)
            .await
            .unwrap();

        let created = client.create_repository("refs/heads/main").await.unwrap();
        let repo = created.repo.expect("a created repository has a key").key;

        let updates = vec![RefUpdate {
            refname: "refs/heads/invented".to_string(),
            old_object_id: None,
            new_object_id: oid("1".repeat(40)),
        }];

        let outcomes = client.update_refs(&repo, updates).await.unwrap();
        assert!(
            outcomes[0].rejection.is_some(),
            "a ref pointing at nothing must not land: {outcomes:?}"
        );
    }
}
