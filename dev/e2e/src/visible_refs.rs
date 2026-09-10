//! Wire-level smoke tests for the visibility hook: a ref the application does
//! not admit to never reaches a real git client.
//!
//! Over git alone, because that is the only door it guards. Whoever reaches the
//! contract *is* the application, and hiding a repository's refs from the party
//! that decided to hide them would answer nobody's question.

#[cfg(test)]
mod tests {
    use crate::support::{git, spawn_smoke_repo};

    /// The refs a clone is offered are the ones the application admitted to.
    #[tokio::test]
    async fn a_hidden_ref_is_not_advertised_to_a_clone() {
        let (_tmp, local, _landed, _servers) = spawn_smoke_repo().await;
        let local = local.as_path();

        let hidden = crate::hooks::HIDDEN
            .strip_prefix("refs/heads/")
            .expect("the stub hides a branch");

        git(&["commit", "--allow-empty", "-m", "first"], Some(local)).await;
        git(&["branch", hidden], Some(local)).await;
        git(&["push", "origin", "main", hidden], Some(local)).await;

        // The push landed both — hiding a ref is not refusing a write, and a
        // ref that never landed would prove nothing about the advertisement.
        let listed = git(&["ls-remote", "--heads", "origin"], Some(local)).await;
        assert!(listed.contains("refs/heads/main"), "{listed}");
        assert!(
            !listed.contains(crate::hooks::HIDDEN),
            "the hidden branch reached the client:\n{listed}"
        );

        // And the same through a clone, which is `ls-refs` reaching the thing
        // a person actually notices.
        let elsewhere = tempfile::tempdir().unwrap();
        let url = git(&["remote", "get-url", "origin"], Some(local)).await;
        git(&["clone", url.trim(), "clone"], Some(elsewhere.path())).await;
        let branches = git(&["branch", "-r"], Some(&elsewhere.path().join("clone"))).await;
        assert!(branches.contains("origin/main"), "{branches}");
        assert!(
            !branches.contains(hidden),
            "the hidden branch was cloned:\n{branches}"
        );
    }
}
