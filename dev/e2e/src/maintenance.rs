//! The maintenance pass against a repository that was really pushed to.
//!
//! Compaction rewrites the index a fetch reads, so the thing worth checking
//! is not the report but the clone afterwards: every commit, every blob, and
//! a packfile git verifies itself.

#[cfg(test)]
mod tests {
    use enroute::maintenance::{Maintenance, run};
    use enroute_git_retrieve::{STARTING_COALESCE, coalesce};

    use crate::support::{ALICE, front_door_for, git, make_isolated_state, spawn_server};

    /// The policy's fanout is eight, so fewer pushes than this merges nothing
    /// and would prove nothing about a merged index.
    const PUSHES: usize = 9;

    #[tokio::test]
    async fn a_pass_merges_the_index_and_the_repository_still_clones() {
        let state = make_isolated_state().await;
        let name = format!("repo-{}", uuid::Uuid::new_v4());
        let repo = state.rows.create(None).await.unwrap();
        let (door, token, _landed, servers) =
            spawn_server(state.clone(), &[(&name, repo.id)]).await;

        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        git(&["init", "-b", "main"], Some(&local)).await;
        git(
            &[
                "remote",
                "add",
                "origin",
                &format!("http://{ALICE}:{token}@{door}/{name}.git"),
            ],
            Some(&local),
        )
        .await;

        // One push a commit, since a segment is written per push and a merge
        // needs several of them to have anything to join.
        for n in 0..PUSHES {
            std::fs::write(
                local.join(format!("file-{n}.txt")),
                format!("contents {n}\n"),
            )
            .unwrap();
            git(&["add", "."], Some(&local)).await;
            git(&["commit", "-m", &format!("commit {n}")], Some(&local)).await;
            git(&["push", "origin", "main"], Some(&local)).await;
        }
        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;

        let pass = run(&state, Maintenance::default()).await.unwrap();
        assert!(pass.merged.merged >= 2, "the pass merged nothing: {pass:?}");
        assert_eq!(pass.indexes.orphans, 0, "a live index lost an object");
        assert_eq!(pass.packs.orphans, 0, "a live push lost its pack image");

        // The whole point: what a merge rewrote is what a fetch reads.
        let (front_door, _front) = front_door_for(state).await;
        let into = tmp.path().join("cloned");
        git(
            &[
                "clone",
                &format!("http://{front_door}/anything.git"),
                into.to_str().unwrap(),
            ],
            Some(tmp.path()),
        )
        .await;

        assert_eq!(
            git(&["rev-parse", "HEAD"], Some(&into)).await.trim(),
            head.trim(),
            "the clone landed on a different commit"
        );
        let log = git(&["log", "--oneline"], Some(&into)).await;
        assert_eq!(log.lines().count(), PUSHES, "history is short:\n{log}");
        let status = git(&["status", "--porcelain"], Some(&into)).await;
        assert!(status.trim().is_empty(), "working tree is dirty:\n{status}");
        drop(servers);
    }

    /// Running it again must be cheap and harmless, since that is what a
    /// timer and a scheduler both do.
    #[tokio::test]
    async fn a_second_pass_over_a_quiet_deployment_takes_nothing() {
        let state = make_isolated_state().await;
        let config = Maintenance::default();

        let first = run(&state, config).await.unwrap();
        let second = run(&state, config).await.unwrap();

        assert_eq!(first.purged.repos, 0, "nothing was deleted to erase");
        assert_eq!(second.indexes.orphans, 0);
        assert_eq!(second.packs.orphans, 0);
    }

    /// Copying pack images moves every byte a fetch reads, so the check is a
    /// clone git verifies itself rather than a count in a report.
    #[tokio::test]
    async fn a_coalesce_moves_the_images_and_the_repository_still_clones() {
        let state = make_isolated_state().await;
        let name = format!("repo-{}", uuid::Uuid::new_v4());
        let repo = state.rows.create(None).await.unwrap();
        let (door, token, _landed, servers) =
            spawn_server(state.clone(), &[(&name, repo.id)]).await;

        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        git(&["init", "-b", "main"], Some(&local)).await;
        git(
            &[
                "remote",
                "add",
                "origin",
                &format!("http://{ALICE}:{token}@{door}/{name}.git"),
            ],
            Some(&local),
        )
        .await;

        // A push a commit, so each lands in a segment object of its own and
        // there is something to gather.
        for n in 0..PUSHES {
            std::fs::write(
                local.join(format!("file-{n}.txt")),
                format!("contents {n}\n"),
            )
            .unwrap();
            git(&["add", "."], Some(&local)).await;
            git(&["commit", "-m", &format!("commit {n}")], Some(&local)).await;
            git(&["push", "origin", "main"], Some(&local)).await;
        }
        let head = git(&["rev-parse", "HEAD"], Some(&local)).await;

        let stored = state.rows.repo(repo.id).lookup().await.unwrap().unwrap();
        let before = state.store.list_segments(&stored).await.unwrap().len();
        assert!(before >= PUSHES, "each push wrote a segment: {before}");

        let coalesced = coalesce(&state, &stored, STARTING_COALESCE).await.unwrap();
        assert!(coalesced.segments >= 2, "nothing was merged: {coalesced:?}");
        assert!(coalesced.images > 0, "no image moved: {coalesced:?}");
        assert!(coalesced.locations > 0, "no location moved: {coalesced:?}");

        // Every source is still referenced, retired rather than dropped: a
        // clone that composed the index a moment ago is still reading ranges
        // out of those objects, and the sweep keys off these rows.
        let referenced = state
            .rows
            .repo(repo.id)
            .referenced_segments(
                &state
                    .store
                    .list_segments(&stored)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        assert_eq!(
            referenced.len(),
            before + 1,
            "a source stopped being referenced the moment it was gathered"
        );

        // Everything above is bookkeeping; this is the claim.
        let (front_door, _front) = front_door_for(state).await;
        let into = tmp.path().join("cloned");
        git(
            &[
                "clone",
                &format!("http://{front_door}/anything.git"),
                into.to_str().unwrap(),
            ],
            Some(tmp.path()),
        )
        .await;

        assert_eq!(
            git(&["rev-parse", "HEAD"], Some(&into)).await.trim(),
            head.trim(),
            "the clone landed on a different commit"
        );
        let log = git(&["log", "--oneline"], Some(&into)).await;
        assert_eq!(log.lines().count(), PUSHES, "history is short:\n{log}");
        for n in 0..PUSHES {
            assert_eq!(
                std::fs::read_to_string(into.join(format!("file-{n}.txt"))).unwrap(),
                format!("contents {n}\n"),
                "a blob came back wrong after its image moved"
            );
        }
        let status = git(&["status", "--porcelain"], Some(&into)).await;
        assert!(status.trim().is_empty(), "working tree is dirty:\n{status}");
        drop(servers);
    }

    /// A gather retires the segments it copied, and the retirement is what
    /// the sweep respects — until the window passes and it takes them.
    #[tokio::test]
    async fn a_gathered_segment_is_kept_for_a_window_and_then_reclaimed() {
        let state = make_isolated_state().await;
        let name = format!("repo-{}", uuid::Uuid::new_v4());
        let repo = state.rows.create(None).await.unwrap();
        let (door, token, _landed, servers) =
            spawn_server(state.clone(), &[(&name, repo.id)]).await;

        let tmp = tempfile::tempdir().unwrap();
        let local = tmp.path().join("work");
        std::fs::create_dir_all(&local).unwrap();
        git(&["init", "-b", "main"], Some(&local)).await;
        git(
            &[
                "remote",
                "add",
                "origin",
                &format!("http://{ALICE}:{token}@{door}/{name}.git"),
            ],
            Some(&local),
        )
        .await;
        for n in 0..PUSHES {
            std::fs::write(
                local.join(format!("file-{n}.txt")),
                format!("contents {n}\n"),
            )
            .unwrap();
            git(&["add", "."], Some(&local)).await;
            git(&["commit", "-m", &format!("commit {n}")], Some(&local)).await;
            git(&["push", "origin", "main"], Some(&local)).await;
        }

        let stored = state.rows.repo(repo.id).lookup().await.unwrap().unwrap();
        let before = state.store.list_segments(&stored).await.unwrap().len();

        // A whole pass, gather and sweep together. The sources are seconds
        // old and retired seconds ago, so both windows hold them.
        let pass = run(&state, Maintenance::default()).await.unwrap();
        assert!(
            pass.coalesced.segments >= 2,
            "nothing was gathered: {pass:?}"
        );
        assert_eq!(
            state.store.list_segments(&stored).await.unwrap().len(),
            before + 1,
            "the pass that gathered also deleted what it gathered"
        );

        // A second pass inside the window must gather nothing. The retired
        // sources keep their rows for the whole of it, and taking them again
        // would copy bytes whose images have already moved — into an object
        // the pass after that would copy again, and so on.
        let again = run(&state, Maintenance::default()).await.unwrap();
        assert_eq!(
            again.coalesced.segments, 0,
            "a retired source was gathered a second time: {again:?}"
        );
        assert_eq!(
            state.store.list_segments(&stored).await.unwrap().len(),
            before + 1,
            "the second pass wrote another copy"
        );

        // And a pass that waits for nothing takes them, so retirement is a
        // delay rather than a second way to leak.
        run(
            &state,
            Maintenance {
                grace_secs: 0,
                deleted_grace_secs: 0,
                dry_run: false,
            },
        )
        .await
        .unwrap();
        let left = state.store.list_segments(&stored).await.unwrap().len();
        assert!(left < before, "nothing was ever reclaimed: {left} left");

        let (front_door, _front) = front_door_for(state).await;
        let into = tmp.path().join("cloned");
        git(
            &[
                "clone",
                &format!("http://{front_door}/anything.git"),
                into.to_str().unwrap(),
            ],
            Some(tmp.path()),
        )
        .await;
        let log = git(&["log", "--oneline"], Some(&into)).await;
        assert_eq!(log.lines().count(), PUSHES, "history is short:\n{log}");
        drop(servers);
    }
}
