//! Proves the 401 hint reaches a real `git` client.
//!
//! Git only prints an error body (as `remote:` lines) when it is
//! `text/plain`, so this asserts the user-visible outcome, not the shape.

#[cfg(test)]
mod tests {
    use crate::support::{ALICE, make_isolated_state, spawn_server, with_host_override};

    #[tokio::test]
    async fn a_password_gets_a_usable_explanation_over_the_wire() {
        let state = make_isolated_state().await;
        let repo = format!("repo-{}", uuid::Uuid::new_v4());
        let created = state.rows.create(None).await.unwrap();
        let (addr, _token, _landed, _servers) = spawn_server(state, &[(&repo, created.id)]).await;

        let tmp = tempfile::tempdir().unwrap();
        let mut cmd = tokio::process::Command::new("git");
        cmd.env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .arg("clone")
            .arg(format!("http://{ALICE}:hunter2@{addr}/{repo}.git"))
            .arg(tmp.path().join("clone"));
        with_host_override(&mut cmd, ALICE);
        let out = cmd.output().await.unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();

        assert!(
            stderr.contains("remote: this server authenticates git with an access token"),
            "git did not surface the hint:\n{stderr}"
        );
    }
}
