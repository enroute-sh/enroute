//! The paths a real git client takes, proved against a real repository.
//!
//! Enroute terminates git itself, so these reach storage with no second
//! process in the way — only real `git clone` verifies the packfile checksum.

#[cfg(test)]
mod fetch {
    use crate::support::{ALICE, Servers, front_door_for, git, make_isolated_state, spawn_server};

    /// A repository with `commits` commits on one branch, plus the two
    /// servers in front of it.
    ///
    /// Returns the front door's address and a checkout to run `git` from.
    pub(crate) async fn spawn_slice_with(
        commits: usize,
    ) -> (tempfile::TempDir, std::net::SocketAddr, String, Servers) {
        let state = make_isolated_state().await;
        let name = format!("repo-{}", uuid::Uuid::new_v4());
        let repo = state.rows.create(None).await.unwrap();

        // Seeded through an application front door of its own, because these
        // tests are about the read path rather than about who may reach it.
        let (legacy, token, _landed, seeding) =
            spawn_server(state.clone(), &[(&name, repo.id)]).await;
        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().to_path_buf();
        git(&["init", "-b", "main"], Some(&local)).await;
        git(
            &[
                "remote",
                "add",
                "origin",
                &format!("http://{ALICE}:{token}@{legacy}/{name}.git"),
            ],
            Some(&local),
        )
        .await;
        for n in 0..commits {
            // Real content, so the pack carries blobs and trees rather than
            // commits alone — an empty pack would prove much less.
            std::fs::write(
                local.join(format!("file-{n}.txt")),
                format!("contents {n}\n"),
            )
            .unwrap();
            git(&["add", "."], Some(&local)).await;
            git(&["commit", "-m", &format!("commit {n}")], Some(&local)).await;
        }
        git(&["push", "origin", "main"], Some(&local)).await;

        let (front_door, servers) = front_door_for(state).await;
        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
        // The seeding servers go on living until the caller drops these: a
        // test that reads back what it pushed needs both ends up.
        (
            tmp,
            front_door,
            head.trim().to_string(),
            servers.and(seeding),
        )
    }

    /// An empty repository with nothing seeded into it.
    ///
    /// Push tests must reach it through git alone, or prove nothing about the write path.
    pub(crate) async fn front_door_only() -> (tempfile::TempDir, std::net::SocketAddr, Servers) {
        let state = make_isolated_state().await;
        state.rows.create(None).await.unwrap();
        let (front_door, servers) = front_door_for(state).await;
        (tempfile::tempdir().unwrap(), front_door, servers)
    }

    /// The slice that matters most: a whole repository, fetched through the
    /// front door and reassembled by real git.
    ///
    /// `git clone` verifies the pack's checksum and every object in it.
    #[tokio::test]
    async fn clone_through_the_front_door() {
        let (tmp, front_door, head, _servers) = spawn_slice_with(3).await;
        let url = format!("http://{front_door}/anything.git");
        let into = tmp.path().join("cloned");

        git(&["clone", &url, into.to_str().unwrap()], Some(tmp.path())).await;

        let cloned_head = git(&["rev-parse", "HEAD"], Some(&into)).await;
        assert_eq!(
            cloned_head.trim(),
            head,
            "clone landed on a different commit"
        );
        let log = git(&["log", "--oneline"], Some(&into)).await;
        assert_eq!(log.lines().count(), 3, "history is short:\n{log}");
        // Proves the blobs arrived, not only the commits pointing at them.
        assert_eq!(
            std::fs::read_to_string(into.join("file-2.txt")).unwrap(),
            "contents 2\n"
        );
        // git checks the pack trailer itself, but only reports it here.
        let status = git(&["status", "--porcelain"], Some(&into)).await;
        assert!(status.trim().is_empty(), "working tree is dirty:\n{status}");
    }

    /// A second clone from an existing checkout negotiates: git sends its
    /// haves, and the server has to recognise them.
    #[tokio::test]
    async fn fetch_into_an_existing_checkout_negotiates() {
        let (tmp, front_door, head, _servers) = spawn_slice_with(2).await;
        let url = format!("http://{front_door}/anything.git");
        let into = tmp.path().join("cloned");
        git(&["clone", &url, into.to_str().unwrap()], Some(tmp.path())).await;

        // Nothing new to send: the interesting part is that it succeeds
        // rather than hanging or resending the whole history.
        git(&["fetch", "origin"], Some(&into)).await;

        let cloned_head = git(&["rev-parse", "HEAD"], Some(&into)).await;
        assert_eq!(cloned_head.trim(), head);
    }

    /// `blob:none` is the one filter the server honors exactly.
    #[tokio::test]
    async fn a_blobless_clone_omits_blobs() {
        let (tmp, front_door, _head, _servers) = spawn_slice_with(2).await;
        let url = format!("http://{front_door}/anything.git");
        let into = tmp.path().join("blobless");

        git(
            &[
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                &url,
                into.to_str().unwrap(),
            ],
            Some(tmp.path()),
        )
        .await;

        let kinds = git(
            &[
                "cat-file",
                "--batch-all-objects",
                "--batch-check=%(objecttype)",
            ],
            Some(&into),
        )
        .await;
        assert!(
            !kinds.lines().any(|kind| kind.trim() == "blob"),
            "blobs came through a blob:none clone:\n{kinds}"
        );
    }

    #[tokio::test]
    async fn ls_remote_through_the_front_door() {
        let (tmp, front_door, head, _servers) = spawn_slice_with(1).await;
        let url = format!("http://{front_door}/anything.git");

        let refs = git(&["ls-remote", &url], Some(tmp.path())).await;

        assert!(
            refs.contains(&format!("{head}\tHEAD")),
            "HEAD missing or wrong:\n{refs}"
        );
        assert!(
            refs.contains(&format!("{head}\trefs/heads/main")),
            "branch missing or wrong:\n{refs}"
        );
    }

    /// go-git and friends never send the `Git-Protocol` header, so the v0
    /// advertisement is a path real clients take, not a legacy one.
    #[tokio::test]
    async fn ls_remote_over_protocol_v0() {
        let (tmp, front_door, head, _servers) = spawn_slice_with(1).await;
        let url = format!("http://{front_door}/anything.git");

        let refs = git(
            &["-c", "protocol.version=0", "ls-remote", &url],
            Some(tmp.path()),
        )
        .await;

        assert!(
            refs.contains(&format!("{head}\trefs/heads/main")),
            "v0 advertisement missing the branch:\n{refs}"
        );
    }
}

/// The write path, which the read tests could not reach.
///
/// `git push` all the way through the front door, with nothing between the
/// pkt-lines and storage.
#[cfg(test)]
mod push {
    use crate::support::git;

    use super::fetch::{front_door_only, spawn_slice_with};

    /// A push into an empty repository.
    ///
    /// No advertisement to fast-forward from, and every object in the pack is new.
    #[tokio::test]
    async fn push_into_an_empty_repository() {
        let (tmp, front_door, _servers) = front_door_only().await;
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        let url = format!("http://{front_door}/anything.git");

        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;
        std::fs::write(local.join("a.txt"), "hello\n").unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;

        git(&["push", "origin", "main"], Some(&local)).await;

        let refs = git(&["ls-remote", &url], Some(&local)).await;
        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
        assert!(
            refs.contains(head.trim()),
            "pushed commit is not advertised:\n{refs}"
        );
    }

    /// A clone URL that holds a namespace, which is the shape a forge wants
    /// and the one a single path segment refused before anyone was asked.
    #[tokio::test]
    async fn a_nested_path_pushes_and_clones_back() {
        let (tmp, front_door, _servers) = front_door_only().await;
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        let url = format!("http://{front_door}/enroute-sh/enroute.git");

        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;
        std::fs::write(local.join("a.txt"), "hello\n").unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;
        git(&["push", "origin", "main"], Some(&local)).await;

        let back = tmp.path().join("clone");
        git(&["clone", &url, back.to_str().unwrap()], None).await;
        assert_eq!(
            git(&["rev-parse", "HEAD"], Some(&local)).await,
            git(&["rev-parse", "HEAD"], Some(&back)).await,
        );
    }

    /// Git appends no suffix, so both spellings are URLs a client may send
    /// and both must reach the same repository behind one answer.
    #[tokio::test]
    async fn a_path_with_no_dot_git_is_served_too() {
        let (tmp, front_door, _servers) = front_door_only().await;
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();

        git(&["init", "-b", "main"], Some(&local)).await;
        std::fs::write(local.join("a.txt"), "hello\n").unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;
        git(
            &[
                "push",
                &format!("http://{front_door}/team/group/repo"),
                "main",
            ],
            Some(&local),
        )
        .await;

        let refs = git(
            &["ls-remote", &format!("http://{front_door}/team/group/repo")],
            Some(&local),
        )
        .await;
        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
        assert!(
            refs.contains(head.trim()),
            "pushed commit is not advertised:\n{refs}"
        );
    }

    /// Push, then clone it back.
    ///
    /// The round trip proves the pack was stored, not merely accepted.
    #[tokio::test]
    async fn a_pushed_repository_clones_back() {
        let (tmp, front_door, _servers) = front_door_only().await;
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        let url = format!("http://{front_door}/anything.git");

        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;
        for n in 0..3 {
            std::fs::write(local.join(format!("f{n}.txt")), format!("body {n}\n")).unwrap();
            git(&["add", "."], Some(&local)).await;
            git(&["commit", "-m", &format!("c{n}")], Some(&local)).await;
        }
        git(&["push", "origin", "main"], Some(&local)).await;
        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;

        let back = tmp.path().join("back");
        git(&["clone", &url, back.to_str().unwrap()], Some(tmp.path())).await;

        assert_eq!(
            git(&["rev-parse", "HEAD"], Some(&back)).await.trim(),
            head.trim()
        );
        assert_eq!(
            std::fs::read_to_string(back.join("f2.txt")).unwrap(),
            "body 2\n"
        );
    }

    /// A second push on top of the first.
    ///
    /// The client now has an advertisement to compute `old-id` from, so this
    /// exercises the fast-forward check rather than a create.
    #[tokio::test]
    async fn a_second_push_fast_forwards() {
        let (tmp, front_door, _servers) = front_door_only().await;
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        let url = format!("http://{front_door}/anything.git");

        git(&["init", "-b", "main"], Some(&local)).await;
        git(&["remote", "add", "origin", &url], Some(&local)).await;
        std::fs::write(local.join("a.txt"), "one\n").unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "first"], Some(&local)).await;
        git(&["push", "origin", "main"], Some(&local)).await;

        std::fs::write(local.join("a.txt"), "two\n").unwrap();
        git(&["add", "."], Some(&local)).await;
        git(&["commit", "-m", "second"], Some(&local)).await;
        git(&["push", "origin", "main"], Some(&local)).await;

        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;
        let refs = git(&["ls-remote", &url], Some(&local)).await;
        assert!(
            refs.contains(head.trim()),
            "second push did not land:\n{refs}"
        );
    }

    /// A push that seeded a branch, then a push that deletes it.
    ///
    /// Delete-only pushes carry no packfile, so ingest meets EOF where a header would be.
    #[tokio::test]
    async fn a_delete_only_push_carries_no_pack() {
        let (tmp, front_door, _head, _servers) = spawn_slice_with(1).await;
        let url = format!("http://{front_door}/anything.git");
        let local = tmp.path().join("work");
        git(&["clone", &url, local.to_str().unwrap()], Some(tmp.path())).await;
        git(&["push", "origin", "main:doomed"], Some(&local)).await;
        assert!(
            git(&["ls-remote", &url], Some(&local))
                .await
                .contains("doomed"),
            "setup did not create the branch"
        );

        git(&["push", "origin", ":doomed"], Some(&local)).await;

        let refs = git(&["ls-remote", &url], Some(&local)).await;
        assert!(
            !refs.contains("doomed"),
            "branch survived the delete:\n{refs}"
        );
        assert!(
            refs.contains("refs/heads/main"),
            "delete took a neighbour:\n{refs}"
        );
    }
}
