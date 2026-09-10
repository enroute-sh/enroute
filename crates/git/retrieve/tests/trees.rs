//! What the composed tree walks answer once a push has landed.
//!
//! Every case seeds through the push path's writer and walks back through
//! [`enroute_git_retrieve::tree`] and [`enroute_git_retrieve::diff_trees`],
//! so both ends of the storage each walk composes are the real ones.

#[cfg(test)]
mod trees {
    use gix_hash::ObjectId;
    use gix_object::Kind;

    use enroute_git_graph::{DiffOptions, ObjectRefs, object_refs};
    use enroute_git_retrieve::{Storage, TreeError, TreeWalk, diff_trees, tree};
    use enroute_git_test_support::{
        create_repo, make_state, seed_commit_pair_with_inherited_subtree,
    };

    use enroute_git_metadata::RepoMetadata;

    /// A generous budget, for the cases not about running out of one.
    const PLENTY: usize = 50_000;

    fn parse(sha: &str) -> ObjectId {
        ObjectId::from_hex(sha.as_bytes()).unwrap()
    }

    fn paths(walk: &TreeWalk) -> Vec<String> {
        walk.levels
            .iter()
            .flatten()
            .map(|item| String::from_utf8_lossy(&item.path).into_owned())
            .collect()
    }

    /// The root tree of a stored commit, read back out of its bytes.
    async fn root_of(state: &Storage, repo: &RepoMetadata, commit: ObjectId) -> ObjectId {
        let (kind, bytes) = enroute_git_retrieve::object(state, repo, commit)
            .await
            .unwrap();
        let Ok(ObjectRefs::Commit { root_tree, .. }) = object_refs(kind, &bytes) else {
            panic!("seeded commit {commit} did not parse");
        };
        root_tree
    }

    #[tokio::test]
    async fn a_commit_lists_every_path_level_by_level() {
        let state = make_state();
        let repo = create_repo(&state).await;
        let (_, c1, _, _) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;

        let walk = tree(&state, &repo, parse(&c1), PLENTY).await.unwrap();

        assert_eq!(paths(&walk), ["dir", "top.txt", "dir/file.txt"]);
        assert_eq!(walk.levels.len(), 2);
        assert!(!walk.truncated);
    }

    #[tokio::test]
    async fn entries_carry_their_kind_and_oid() {
        let state = make_state();
        let repo = create_repo(&state).await;
        let (c0, _, subtree, blob) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;

        let walk = tree(&state, &repo, parse(&c0), PLENTY).await.unwrap();

        let flat: Vec<_> = walk.levels.iter().flatten().collect();
        assert_eq!(flat.len(), 2);
        assert!(flat[0].is_tree);
        assert_eq!(flat[0].oid, subtree);
        assert!(!flat[1].is_tree);
        assert_eq!(flat[1].oid, blob);
    }

    #[tokio::test]
    async fn a_walk_can_start_at_a_tree() {
        let state = make_state();
        let repo = create_repo(&state).await;
        let (_, _, subtree, _) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;

        let walk = tree(&state, &repo, subtree, PLENTY).await.unwrap();

        assert_eq!(paths(&walk), ["file.txt"]);
    }

    #[tokio::test]
    async fn a_blob_is_no_place_to_start() {
        let state = make_state();
        let repo = create_repo(&state).await;
        let (_, _, _, blob) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;

        let refused = tree(&state, &repo, blob, PLENTY).await;

        match refused {
            Err(TreeError::NotTreeish { oid, kind }) => {
                assert_eq!(oid, blob);
                assert_eq!(kind, Kind::Blob);
            }
            other => panic!("wanted NotTreeish, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_unknown_id_is_missing() {
        let state = make_state();
        let repo = create_repo(&state).await;
        seed_commit_pair_with_inherited_subtree(&state, &repo).await;

        let nowhere = ObjectId::from_bytes_or_panic(&[0x42; 20]);
        let refused = tree(&state, &repo, nowhere, PLENTY).await;

        match refused {
            Err(TreeError::Missing(oid)) => assert_eq!(oid, nowhere),
            other => panic!("wanted Missing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_entry_budget_cuts_the_walk_and_says_so() {
        let state = make_state();
        let repo = create_repo(&state).await;
        let (_, c1, _, _) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;

        let walk = tree(&state, &repo, parse(&c1), 1).await.unwrap();

        assert!(walk.truncated);
        assert_eq!(paths(&walk), ["dir"]);
    }

    #[tokio::test]
    async fn a_diff_prunes_a_shared_subtree() {
        let state = make_state();
        let repo = create_repo(&state).await;
        let (c0, c1, _, _) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;
        let old_root = root_of(&state, &repo, parse(&c0)).await;
        let new_root = root_of(&state, &repo, parse(&c1)).await;

        let options = DiffOptions { removals: true };
        let diff = diff_trees(&state, &repo, Some(old_root), new_root, options, PLENTY)
            .await
            .unwrap();

        assert!(diff.is_complete());
        let changed: Vec<&[u8]> = diff.changes.iter().map(|c| c.path.as_slice()).collect();
        // `dir/` is the same tree on both sides, so nothing under it counts.
        assert_eq!(changed, [b"top.txt".as_slice()]);
        assert!(diff.removals.is_empty());
    }

    #[tokio::test]
    async fn a_root_commit_diffs_as_all_adds() {
        let state = make_state();
        let repo = create_repo(&state).await;
        let (c0, _, _, _) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;
        let new_root = root_of(&state, &repo, parse(&c0)).await;

        let options = DiffOptions { removals: true };
        let diff = diff_trees(&state, &repo, None, new_root, options, PLENTY)
            .await
            .unwrap();

        assert!(diff.is_complete());
        assert!(diff.changes.iter().all(|change| change.old.is_none()));
        assert!(
            diff.changes
                .iter()
                .any(|change| change.path == b"dir/file.txt")
        );
    }

    #[tokio::test]
    async fn the_read_budget_stops_a_diff_short() {
        let state = make_state();
        let repo = create_repo(&state).await;
        let (c0, _, _, _) = seed_commit_pair_with_inherited_subtree(&state, &repo).await;
        let new_root = root_of(&state, &repo, parse(&c0)).await;

        let options = DiffOptions { removals: true };
        let diff = diff_trees(&state, &repo, None, new_root, options, 0)
            .await
            .unwrap();

        // A budget of no reads answers at once, and says it stopped short.
        assert!(!diff.is_complete());
    }
}
