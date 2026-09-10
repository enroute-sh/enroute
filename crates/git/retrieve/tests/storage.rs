//! What the layers answer once something has been written into them.
//!
//! Here rather than beside any one store: every case writes with the push
//! path and reads back through the composed reads, so it needs to see both
//! ends. `enroute-git-repo` held them while it held the reads.

#[cfg(test)]
mod storage {

    use enroute_git_core::{
        COMMIT_PACK_HEADER_SIZE, CommitPackLocation, NewCommit, NewObject, ObjectHashMap,
        ObjectMeta, PackImageLocation, RepoId, SegmentLocation, oid,
    };
    use gix_hash::ObjectId;
    use gix_object::Kind;

    use enroute_git_ingest::KnownIdentities;
    use enroute_git_metadata::{Identity, RepoMetadata};
    use enroute_git_retrieve::Storage;

    /// Record a push the way the push path does.
    async fn append_to(
        store: &Storage,
        repo_id: RepoId,
        new_commits: &ObjectHashMap<NewCommit>,
        new_objects: &[NewObject],
        known: &KnownIdentities,
    ) -> anyhow::Result<()> {
        enroute_git_ingest::append(
            enroute_git_ingest::Engine {
                ids: store.rows.repo(repo_id),
                graph: store.graph.repo(repo_id),
                objects: store.objects.repo(repo_id),
                ledger: store.ledger.as_ref(),
            },
            new_commits,
            new_objects,
            known,
        )
        .await
    }

    /// A fixed tree oid every `nc()` commit uses as its root tree.
    ///
    /// [`seed_repo`] inserts it once per repo so `root_tree_seq` resolves.
    fn placeholder_tree_oid() -> ObjectId {
        oid(0xAB)
    }

    /// The shared segment every `nc()` commit claims — one fixed id is fine
    /// here (commits sharing a segment is the normal multi-image case).
    fn test_segment() -> SegmentLocation {
        SegmentLocation {
            id: enroute_git_core::Ulid::from_parts(1, 1),
            base_offset: 0,
            image_len: 0,
        }
    }

    fn nc(parents: Vec<ObjectId>) -> NewCommit {
        NewCommit {
            committer_date: 0,
            root_tree: placeholder_tree_oid(),
            parents,
            entry_len: 0,
            blob_offset: 0,
            segment: test_segment(),
        }
    }

    /// A non-commit object with a single location in `pack`'s pack.
    fn obj(oid: ObjectId, kind: Kind, pack: ObjectId) -> NewObject {
        NewObject {
            oid,
            kind,
            locations: vec![PackImageLocation {
                pack_sha: pack,
                offset: 100,
                entry_len: 40,
                base: None,
            }],
            children: vec![],
        }
    }

    /// `let store = test_store!();` — a fresh store with nothing installed.
    ///
    /// What two of these do to one another is `concurrency.rs`, against a
    /// real database, since one map under one lock cannot stand in for that.
    macro_rules! test_store {
        () => {
            enroute_git_test_support::make_state()
        };
    }

    /// Register [`placeholder_tree_oid`] as a real, location-less object so
    /// every `nc()` commit's `root_tree_seq` resolves.
    async fn seed_placeholder_tree(store: &Storage, repo_id: RepoId) {
        append_to(
            store,
            repo_id,
            &ObjectHashMap::default(),
            &[NewObject {
                oid: placeholder_tree_oid(),
                kind: Kind::Tree,
                locations: vec![],
                children: vec![],
            }],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
    }

    /// Create a fresh repo via `create_repository` itself, so every test
    /// needing a `repo_id` exercises the real creation path too.
    async fn seed_repo(store: &Storage) -> RepoMetadata {
        let repo = store.rows.create(None).await.unwrap();
        seed_placeholder_tree(store, repo.id).await;
        repo
    }

    /// Append one commit, so a test reads as the graph it is building.
    async fn append_commit(
        store: &Storage,
        repo_id: RepoId,
        oid: ObjectId,
        parents: Vec<ObjectId>,
    ) {
        append_to(
            store,
            repo_id,
            &ObjectHashMap::from_iter([(oid, nc(parents))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
    }

    /// Test-only: a commit's parent seqs as the index recorded them, in the
    /// order git gave them — the encoding itself, not the oids around it.
    async fn parent_seqs(store: &Storage, repo_id: RepoId, oid: ObjectId) -> Vec<i64> {
        let seq = commit_seq(store, repo_id, oid).await;
        store.graph.repo(repo_id).parents_of(seq).await.unwrap()
    }

    /// Test-only: a commit's own seq, to compare against [`parent_seqs`].
    async fn commit_seq(store: &Storage, repo_id: RepoId, oid: ObjectId) -> i64 {
        store
            .graph
            .repo(repo_id)
            .seqs_of(&[oid])
            .await
            .unwrap()
            .get(&oid)
            .copied()
            .unwrap()
    }

    /// Test-only: asserts `commit`'s recorded parent seqs match `parents`,
    /// regardless of encoding order (`parent1`/`parent2`/`extra_parents`).
    async fn assert_parent_seqs(
        store: &Storage,
        repo_id: RepoId,
        commit: ObjectId,
        parents: &[ObjectId],
    ) {
        let mut actual = parent_seqs(store, repo_id, commit).await;
        actual.sort_unstable();
        let mut expected = Vec::with_capacity(parents.len());
        for &parent in parents {
            expected.push(commit_seq(store, repo_id, parent).await);
        }
        expected.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn append_linear_chain_records_each_commit() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (c0, c1, c2) = (oid(1), oid(2), oid(3));

        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c0, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c1, nc(vec![c0]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c2, nc(vec![c1]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        assert!(store.graph.repo(repo_id).contains(c0).await.unwrap());
        assert!(store.graph.repo(repo_id).contains(c1).await.unwrap());
        assert!(store.graph.repo(repo_id).contains(c2).await.unwrap());
        assert_parent_seqs(&store, repo_id, c1, &[c0]).await;
        assert_parent_seqs(&store, repo_id, c2, &[c1]).await;
    }

    // A push that numbered its commits and stopped before recording them is
    // what the split makes possible, so it has to read as absent rather than
    // as a commit whose bytes cannot be produced.
    #[tokio::test]
    async fn a_number_with_no_entry_reads_as_absent() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let stopped = oid(42);

        let seq = store
            .rows
            .repo(repo.id)
            .allocate(Kind::Commit, 1)
            .await
            .unwrap();
        store
            .rows
            .repo(repo.id)
            .record(&[(
                stopped,
                Identity {
                    seq,
                    kind: Kind::Commit,
                },
            )])
            .await
            .unwrap();

        assert!(
            !store.graph.repo(repo.id).contains(stopped).await.unwrap(),
            "a number is not an entry"
        );
        let read = enroute_git_retrieve::object(&store, &repo, stopped).await;
        assert!(
            matches!(read, Err(enroute_git_core::Error::Missing(missing)) if missing == stopped),
            "a numbered commit with no entry must read as absent"
        );
    }

    // The number is permanent, so the push that finishes the work has to take
    // it rather than strand it — otherwise nothing would ever record the
    // commit, every later push finding it numbered and skipping it.
    #[tokio::test]
    async fn a_later_push_records_under_the_number_already_taken() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let stopped = oid(43);

        let reserved = store
            .rows
            .repo(repo.id)
            .allocate(Kind::Commit, 1)
            .await
            .unwrap();
        store
            .rows
            .repo(repo.id)
            .record(&[(
                stopped,
                Identity {
                    seq: reserved,
                    kind: Kind::Commit,
                },
            )])
            .await
            .unwrap();

        append_commit(&store, repo.id, stopped, vec![]).await;

        assert!(
            store.graph.repo(repo.id).contains(stopped).await.unwrap(),
            "the push that followed had to record it"
        );
        assert_eq!(
            store
                .rows
                .repo(repo.id)
                .identify(&[stopped])
                .await
                .unwrap()
                .get(&stopped)
                .unwrap()
                .seq,
            reserved,
            "the number it was already given had to be the one recorded"
        );
    }

    #[tokio::test]
    async fn append_diamond_merge_records_both_parents() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (root, left, right, merge) = (oid(1), oid(2), oid(3), oid(4));

        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(root, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(left, nc(vec![root])), (right, nc(vec![root]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(merge, nc(vec![left, right]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        assert_parent_seqs(&store, repo_id, merge, &[left, right]).await;
    }

    /// Regression test for `extra_parents`: only an octopus merge (3+
    /// parents) exercises `copy_pending_rows`' array-literal encoding.
    #[tokio::test]
    async fn append_octopus_merge_records_all_parents() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (root, p1, p2, p3, p4, merge) = (oid(1), oid(2), oid(3), oid(4), oid(5), oid(6));

        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(root, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([
                (p1, nc(vec![root])),
                (p2, nc(vec![root])),
                (p3, nc(vec![root])),
                (p4, nc(vec![root])),
            ]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(merge, nc(vec![p1, p2, p3, p4]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        assert_parent_seqs(&store, repo_id, merge, &[p1, p2, p3, p4]).await;
    }

    #[tokio::test]
    async fn append_records_multiple_disjoint_roots_in_one_push() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (a, b) = (oid(1), oid(2));
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(a, nc(vec![])), (b, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        assert!(store.graph.repo(repo_id).contains(a).await.unwrap());
        assert!(store.graph.repo(repo_id).contains(b).await.unwrap());
    }

    #[tokio::test]
    async fn append_is_idempotent_for_already_known_commits() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let c0 = oid(1);
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c0, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c0, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        assert!(store.graph.repo(repo_id).contains(c0).await.unwrap());
    }

    #[tokio::test]
    async fn append_of_only_duplicate_commits_registers_no_segment() {
        // The losing side of an append race: its segment carries nothing new,
        // so it gets no row and its S3 object is left to the janitor.
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let c0 = oid(1);

        let first = nc(vec![]);
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c0, first)]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        let mut loser = nc(vec![]);
        loser.segment.id = enroute_git_core::Ulid::from_parts(9, 9);
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c0, loser)]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        let asked = [test_segment().id, enroute_git_core::Ulid::from_parts(9, 9)];
        let registered = store
            .rows
            .repo(repo_id)
            .referenced_segments(&asked)
            .await
            .unwrap();
        assert_eq!(
            registered,
            std::collections::HashSet::from([test_segment().id]),
            "only the winning push's segment is registered"
        );
        assert_eq!(
            enroute_git_retrieve::meta(&store, repo_id, c0)
                .await
                .unwrap()
                .unwrap()
                .location,
            Some(CommitPackLocation {
                image: PackImageLocation {
                    pack_sha: c0,
                    offset: COMMIT_PACK_HEADER_SIZE,
                    entry_len: 0,
                    base: None,
                },
                segment: test_segment(),
            }),
            "first location wins — never repointed at the loser's segment"
        );
    }

    #[tokio::test]
    async fn append_records_objects_and_lookup_resolves_them() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (commit, tree, blob) = (oid(1), oid(2), oid(3));

        let mut c = nc(vec![]);
        c.entry_len = 25;
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(commit, c)]),
            &[obj(tree, Kind::Tree, commit), obj(blob, Kind::Blob, commit)],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        let cm = enroute_git_retrieve::meta(&store, repo_id, commit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cm.kind, Kind::Commit);
        assert_eq!(
            cm.location,
            Some(CommitPackLocation {
                image: PackImageLocation {
                    pack_sha: commit,
                    offset: COMMIT_PACK_HEADER_SIZE,
                    entry_len: 25,
                    base: None,
                },
                segment: test_segment(),
            })
        );
        assert_eq!(
            cm.object_seq, None,
            "commits live in the commit-seq keyspace, not the object one"
        );

        let bm = enroute_git_retrieve::meta(&store, repo_id, blob)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bm.kind, Kind::Blob);
        assert!(bm.object_seq.is_some());
        assert_eq!(bm.location.unwrap().image.pack_sha, commit);
        assert_eq!(
            store.objects.repo(repo_id).packs_of(blob).await.unwrap(),
            vec![commit]
        );

        let map = enroute_git_retrieve::metas(&store, repo_id, &[commit, tree, blob, oid(9)])
            .await
            .unwrap();
        assert_eq!(map.len(), 3);
        assert_eq!(map[&tree].kind, Kind::Tree);
        assert_eq!(map[&commit].kind, Kind::Commit);
    }

    #[tokio::test]
    async fn append_tag_has_no_locations() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let commit = oid(1);
        let tag = oid(2);
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(commit, nc(vec![]))]),
            &[NewObject {
                oid: tag,
                kind: Kind::Tag,
                locations: vec![],
                children: vec![],
            }],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        let tm = enroute_git_retrieve::meta(&store, repo_id, tag)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tm.kind, Kind::Tag);
        assert!(tm.location.is_none());
        assert!(
            store
                .objects
                .repo(repo_id)
                .packs_of(tag)
                .await
                .unwrap()
                .is_empty(),
            "tags are never packed"
        );
    }

    #[tokio::test]
    async fn append_objects_is_idempotent_and_accumulates_locations() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (c1, c2, blob) = (oid(1), oid(2), oid(3));

        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c1, nc(vec![]))]),
            &[obj(blob, Kind::Blob, c1)],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c1, nc(vec![]))]),
            &[obj(blob, Kind::Blob, c1)],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(c2, nc(vec![c1]))]),
            &[obj(blob, Kind::Blob, c2)],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        let bm = enroute_git_retrieve::meta(&store, repo_id, blob)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            bm.location.unwrap().image.pack_sha,
            c1,
            "the introducing pack must stay the first push's"
        );
        assert_eq!(
            store.objects.repo(repo_id).packs_of(blob).await.unwrap(),
            vec![c1, c2],
            "the re-inclusion must land in the extra packs exactly once"
        );
    }

    /// A commit another push recorded between the prefetch and the append
    /// must not strand this one.
    ///
    /// `plan_commits` believes what the prefetch told it; the retry is what
    /// makes believing it safe, by reading again rather than re-trusting.
    #[tokio::test]
    async fn append_survives_a_stale_absence_for_a_commit() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let commit = oid(1);

        // The concurrent push records the very commit ours is about to.
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(commit, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        // Ours read before that landed, and was told the commit was absent.
        let stale = KnownIdentities::new(ObjectHashMap::from_iter([(commit, None)]));
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(commit, nc(vec![]))]),
            &[],
            &stale,
        )
        .await
        .expect("a stale absence must not strand a commit");

        let found = enroute_git_retrieve::meta(&store, repo_id, commit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.kind, Kind::Commit);
    }

    /// A stale absence is only safe for objects this push will actually
    /// insert.
    ///
    /// An object already recorded by a concurrent push is filtered out of
    /// `relevant`, so a "known absent" seq for it would strand any tree naming it.
    #[tokio::test]
    async fn append_reads_a_missing_seq_it_cannot_plan_as_new() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (pack_x, pack_y, tree, blob) = (oid(1), oid(2), oid(3), oid(4));

        // The concurrent push: records commit X and the blob its pack carries.
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(pack_x, nc(vec![]))]),
            &[obj(blob, Kind::Blob, pack_x)],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        // Our push: commit Y introduces `tree`, which names the blob X
        // already carried. X is now recorded, so X's pack is not fresh and
        // the blob drops out of `relevant`.
        let mut y = nc(vec![pack_x]);
        y.root_tree = tree;
        let mut tree_obj = obj(tree, Kind::Tree, pack_y);
        tree_obj.children = vec![blob];

        // The prefetch ran before the concurrent push committed, so it
        // covers the blob and believes it absent.
        let stale = KnownIdentities::new(ObjectHashMap::from_iter([(tree, None), (blob, None)]));

        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(pack_y, y)]),
            &[tree_obj, obj(blob, Kind::Blob, pack_x)],
            &stale,
        )
        .await
        .expect("stale absence must not strand the blob's seq");
    }

    /// A `KnownIdentities` whose "absent" is stale — inserted after the
    /// caller's read — must still resolve to the row's real seq.
    ///
    /// Planning it as new is the point of trusting the miss; `ON CONFLICT` makes it safe.
    #[tokio::test]
    async fn append_recovers_from_a_stale_absent_in_known_seqs() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (pack_a, pack_b, tree, blob) = (oid(1), oid(2), oid(3), oid(4));

        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(pack_a, nc(vec![]))]),
            &[obj(blob, Kind::Blob, pack_a)],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        let real_blob_seq = store
            .rows
            .repo(repo_id)
            .identify(&[blob])
            .await
            .unwrap()
            .remove(&blob)
            .expect("blob recorded by the first append");

        // What a push that read `blob` *before* the append above would hold:
        // asked about it, found nothing.
        let stale = KnownIdentities::new(ObjectHashMap::from_iter([(blob, None), (tree, None)]));

        let mut tree_obj = obj(tree, Kind::Tree, pack_b);
        tree_obj.children = vec![blob];
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(pack_b, nc(vec![]))]),
            &[tree_obj, obj(blob, Kind::Blob, pack_b)],
            &stale,
        )
        .await
        .unwrap();

        let seqs = store
            .rows
            .repo(repo_id)
            .identify(&[blob, tree])
            .await
            .unwrap();
        assert_eq!(
            seqs.get(&blob),
            Some(&real_blob_seq),
            "blob keeps the seq its winning insert gave it"
        );

        let Some(tree_seq) = seqs
            .get(&tree)
            .copied()
            .filter(|held| held.kind == Kind::Tree)
            .map(|held| u64::try_from(held.seq).unwrap())
        else {
            panic!("tree recorded, in the tree space");
        };
        let children = store
            .objects
            .repo(repo_id)
            .children_of(&[tree_seq])
            .await
            .unwrap();
        assert_eq!(
            children
                .get(&tree_seq)
                .map(|c| c.blobs.iter().collect::<Vec<_>>()),
            Some(vec![u64::try_from(real_blob_seq.seq).unwrap()]),
            "tree children must name the blob's real seq, not the one this push allocated"
        );
    }

    #[tokio::test]
    async fn append_unresolved_parent_errors() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (missing, child) = (oid(9), oid(2));
        let err = append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(child, nc(vec![missing]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("unresolved parent"));
    }

    /// The paint walk must subtract the haves' whole reachable closure, not
    /// just the haves — else common history below the merge base gets re-sent.
    #[tokio::test]
    async fn walk_commits_needed_excludes_ancestors_of_haves() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let (base, main_tip, feature_tip) = (oid(1), oid(2), oid(3));
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(base, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(main_tip, nc(vec![base]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(feature_tip, nc(vec![base]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        let needed = enroute_git_proto::needed(
            store.graph.repo(repo_id),
            store.objects.repo(repo_id),
            &[main_tip],
            &[feature_tip],
            &[],
            None,
        )
        .await
        .unwrap()
        .needed;
        let oids: Vec<ObjectId> = needed.commits.into_iter().map(|c| c.oid).collect();
        assert_eq!(
            oids,
            vec![main_tip],
            "base is an ancestor of the have and must not be re-sent"
        );
    }

    /// Regression test: `seq` is only unique within a `repo_id`, so every CTE
    /// join must also filter on it, or two repos' `seq = 0` roots join together.
    #[tokio::test]
    async fn walk_commits_needed_does_not_cross_repos_with_overlapping_seq() {
        let store = test_store!();
        let (repo_a, repo_b) = (seed_repo(&store).await.id, seed_repo(&store).await.id);
        let (root_a, root_b) = (oid(1), oid(2));

        append_to(
            &store,
            repo_a,
            &ObjectHashMap::from_iter([(root_a, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_b,
            &ObjectHashMap::from_iter([(root_b, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        let needed = enroute_git_proto::needed(
            store.graph.repo(repo_a),
            store.objects.repo(repo_a),
            &[root_a],
            &[],
            &[],
            None,
        )
        .await
        .unwrap()
        .needed;
        let oids: Vec<ObjectId> = needed.commits.into_iter().map(|c| c.oid).collect();
        assert_eq!(oids, vec![root_a], "must not include repo_b's root commit");
    }

    // ── ancestor reachability ────────────────────────────────────────────

    #[tokio::test]
    async fn is_ancestor_true_along_a_chain() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let (base, mid, tip) = (oid(1), oid(2), oid(3));
        append_to(
            &store,
            repo.id,
            &ObjectHashMap::from_iter([(base, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo.id,
            &ObjectHashMap::from_iter([(mid, nc(vec![base]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo.id,
            &ObjectHashMap::from_iter([(tip, nc(vec![mid]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        assert!(
            store
                .graph
                .repo(repo.id)
                .is_ancestor(base, tip)
                .await
                .unwrap()
        );
        assert!(
            !store
                .graph
                .repo(repo.id)
                .is_ancestor(tip, base)
                .await
                .unwrap()
        );
        assert!(
            store
                .graph
                .repo(repo.id)
                .is_ancestor(tip, tip)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn is_ancestor_false_across_a_fork() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let (base, main_tip, feature_tip) = (oid(1), oid(2), oid(3));
        append_to(
            &store,
            repo.id,
            &ObjectHashMap::from_iter([(base, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo.id,
            &ObjectHashMap::from_iter([(main_tip, nc(vec![base]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo.id,
            &ObjectHashMap::from_iter([(feature_tip, nc(vec![base]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        assert!(
            !store
                .graph
                .repo(repo.id)
                .is_ancestor(main_tip, feature_tip)
                .await
                .unwrap(),
            "siblings off the same base are not each other's ancestor"
        );
        assert!(
            store
                .graph
                .repo(repo.id)
                .is_ancestor(base, main_tip)
                .await
                .unwrap()
        );
        assert!(
            store
                .graph
                .repo(repo.id)
                .is_ancestor(base, feature_tip)
                .await
                .unwrap()
        );
    }

    /// The batch answer must agree with the pairwise one on a fork: reporting
    /// the other side as an ancestor would let a push store an unreachable delta base.
    #[tokio::test]
    async fn ancestors_among_separates_the_two_sides_of_a_fork() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let (base, main_tip, feature_tip) = (oid(1), oid(2), oid(3));
        for (commit, parents) in [
            (base, vec![]),
            (main_tip, vec![base]),
            (feature_tip, vec![base]),
        ] {
            append_to(
                &store,
                repo.id,
                &ObjectHashMap::from_iter([(commit, nc(parents))]),
                &[],
                &KnownIdentities::default(),
            )
            .await
            .unwrap();
        }

        let found = store
            .graph
            .repo(repo.id)
            .ancestors_among(main_tip, &[base, main_tip, feature_tip], 1_000)
            .await
            .unwrap();
        assert!(found.contains(&base), "the fork point is an ancestor");
        assert!(found.contains(&main_tip), "the seed is its own ancestor");
        assert!(
            !found.contains(&feature_tip),
            "the other side of the fork is not"
        );
    }

    /// The cap is what keeps the walk off a repo's whole history, and it may
    /// only ever lose an answer — never invent one.
    #[tokio::test]
    async fn ancestors_among_gives_up_rather_than_guessing_past_its_cap() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let mut parent: Vec<ObjectId> = vec![];
        for i in 1..=6u8 {
            append_to(
                &store,
                repo.id,
                &ObjectHashMap::from_iter([(oid(i), nc(parent))]),
                &[],
                &KnownIdentities::default(),
            )
            .await
            .unwrap();
            parent = vec![oid(i)];
        }

        let reachable = store
            .graph
            .repo(repo.id)
            .ancestors_among(oid(6), &[oid(1)], 1_000)
            .await
            .unwrap();
        assert!(reachable.contains(&oid(1)));

        let capped = store
            .graph
            .repo(repo.id)
            .ancestors_among(oid(6), &[oid(1)], 2)
            .await
            .unwrap();
        assert!(
            !capped.contains(&oid(1)),
            "a walk that stops short must report unproven, not proven"
        );
    }

    #[tokio::test]
    async fn is_ancestor_false_for_an_unknown_oid() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let known = oid(1);
        append_to(
            &store,
            repo.id,
            &ObjectHashMap::from_iter([(known, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        let unknown = oid(0xEE);

        assert!(
            !store
                .graph
                .repo(repo.id)
                .is_ancestor(unknown, known)
                .await
                .unwrap()
        );
        assert!(
            !store
                .graph
                .repo(repo.id)
                .is_ancestor(known, unknown)
                .await
                .unwrap()
        );
    }

    /// Same concern as the `walk_commits_needed` cross-repo test: every
    /// recursive step must filter on `repo_id`, or overlapping seqs get walked together.
    #[tokio::test]
    async fn is_ancestor_does_not_cross_repos_with_overlapping_seq() {
        let store = test_store!();
        let (repo_a, repo_b) = (seed_repo(&store).await.id, seed_repo(&store).await.id);
        let (root_a, root_b) = (oid(1), oid(2));
        append_to(
            &store,
            repo_a,
            &ObjectHashMap::from_iter([(root_a, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        append_to(
            &store,
            repo_b,
            &ObjectHashMap::from_iter([(root_b, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        assert!(
            !store
                .graph
                .repo(repo_a)
                .is_ancestor(root_b, root_a)
                .await
                .unwrap(),
            "root_b belongs to a different repo and must not resolve here"
        );
    }

    // ── shallow (deepen) walks ───────────────────────────────────────────

    /// Append a linear chain `c0 ← c1 ← ... ← c{n-1}` and return the oids.
    async fn seed_chain(store: &Storage, repo_id: RepoId, n: u8) -> Vec<ObjectId> {
        let mut oids = Vec::with_capacity(usize::from(n));
        let mut parent: Option<ObjectId> = None;
        for i in 1..=n {
            let c = oid(i);
            append_commit(store, repo_id, c, parent.into_iter().collect()).await;
            parent = Some(c);
            oids.push(c);
        }
        oids
    }

    fn sorted_oids(needed: &enroute_git_graph_store::NeededCommits) -> Vec<ObjectId> {
        let mut oids: Vec<ObjectId> = needed.commits.iter().map(|c| c.oid).collect();
        oids.sort();
        oids
    }

    fn sorted_backfill_oids(backfill: &[(ObjectId, ObjectMeta)]) -> Vec<ObjectId> {
        let mut oids: Vec<ObjectId> = backfill.iter().map(|(oid, _)| *oid).collect();
        oids.sort();
        oids
    }

    #[tokio::test]
    async fn walk_shallow_depth_one_cuts_at_tip() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let chain = seed_chain(&store, repo_id, 4).await;
        let tip = chain[3];

        let walked = enroute_git_proto::needed(
            store.graph.repo(repo_id),
            store.objects.repo(repo_id),
            &[tip],
            &[],
            &[],
            Some(1),
        )
        .await
        .unwrap();
        assert_eq!(sorted_oids(&walked.needed), vec![tip]);
        assert_eq!(walked.shallow, vec![tip], "the tip itself is boundary");
        assert!(walked.unshallow.is_empty());
        // Backfill: the tip's own root tree — every `nc()` commit shares one
        // placeholder tree that no commit "introduces," so it's always
        // outside `needed` and shows up as the backfill entry.
        assert_eq!(
            sorted_backfill_oids(&walked.backfill),
            vec![placeholder_tree_oid()]
        );
    }

    #[tokio::test]
    async fn walk_shallow_deeper_than_history_is_complete() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        let chain = seed_chain(&store, repo_id, 3).await;
        let tip = chain[2];

        let walked = enroute_git_proto::needed(
            store.graph.repo(repo_id),
            store.objects.repo(repo_id),
            &[tip],
            &[],
            &[],
            Some(10),
        )
        .await
        .unwrap();
        assert_eq!(sorted_oids(&walked.needed), chain);
        assert!(walked.shallow.is_empty(), "roots are never boundary");
        assert!(walked.unshallow.is_empty());
        assert!(
            walked.backfill.is_empty(),
            "no boundary means nothing to backfill"
        );
    }

    /// A shallow client's `shallow` lines graft both ways: the want side must
    /// not descend below them, nor the have side prove history below via them.
    #[tokio::test]
    async fn walk_needed_client_shallow_grafts_both_paints() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let repo_id = repo.id;
        // c0 ← c1 ← c2 (the client's shallow lineage: shallow at c1, has
        // c1..c2), plus a side commit d whose parent is c0 directly.
        let chain = seed_chain(&store, repo_id, 3).await;
        let (c0, c1, c2) = (chain[0], chain[1], chain[2]);
        let d = oid(10);
        append_to(
            &store,
            repo_id,
            &ObjectHashMap::from_iter([(d, nc(vec![c0]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();

        // Want side: fetching the shallow lineage's own tip never descends
        // below the graft — nothing but the tip is needed.
        let needed = enroute_git_proto::needed(
            store.graph.repo(repo_id),
            store.objects.repo(repo_id),
            &[c2],
            &[c1],
            &[c1],
            None,
        )
        .await
        .unwrap();
        assert_eq!(sorted_oids(&needed.needed), vec![c2]);

        // Have side: d's history reaches c0 without passing the graft, and
        // the client's haves (grafted at c1) must not subtract it. Without
        // the graft the have at c2 would paint c1 → c0 common and wrongly
        // drop c0.
        let needed = enroute_git_proto::needed(
            store.graph.repo(repo_id),
            store.objects.repo(repo_id),
            &[d],
            &[c2],
            &[],
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            sorted_oids(&needed.needed),
            vec![d],
            "sanity: ungrafted have subtracts c0"
        );
        let needed = enroute_git_proto::needed(
            store.graph.repo(repo_id),
            store.objects.repo(repo_id),
            &[d],
            &[c2],
            &[c1],
            None,
        )
        .await
        .unwrap();
        assert_eq!(sorted_oids(&needed.needed), vec![c0, d]);
    }

    #[tokio::test]
    async fn repo_isolation() {
        let store = test_store!();
        let (repo_a, repo_b) = (seed_repo(&store).await.id, seed_repo(&store).await.id);
        let c0 = oid(1);
        append_to(
            &store,
            repo_a,
            &ObjectHashMap::from_iter([(c0, nc(vec![]))]),
            &[],
            &KnownIdentities::default(),
        )
        .await
        .unwrap();
        assert!(!store.graph.repo(repo_b).contains(c0).await.unwrap());
    }

    // ── merge bases ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn merge_base_of_a_fork_is_where_it_forked() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let (base, main_tip, feature_tip) = (oid(1), oid(2), oid(3));
        append_commit(&store, repo.id, base, vec![]).await;
        append_commit(&store, repo.id, main_tip, vec![base]).await;
        append_commit(&store, repo.id, feature_tip, vec![base]).await;

        let found = store
            .graph
            .repo(repo.id)
            .merge_bases(main_tip, feature_tip, 10_000)
            .await
            .unwrap();
        assert_eq!(found.bases, vec![base]);
        assert!(!found.exhausted);
    }

    /// The case a first-parent walk cannot answer: the branch is already in
    /// trunk through a merge, so trunk's first-parent line is not all of it.
    #[tokio::test]
    async fn merge_base_sees_through_a_merge_commit() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let (base, branch, merge, after) = (oid(1), oid(2), oid(3), oid(4));
        append_commit(&store, repo.id, base, vec![]).await;
        append_commit(&store, repo.id, branch, vec![base]).await;
        append_commit(&store, repo.id, merge, vec![base, branch]).await;
        append_commit(&store, repo.id, after, vec![merge]).await;

        let found = store
            .graph
            .repo(repo.id)
            .merge_bases(after, branch, 10_000)
            .await
            .unwrap();
        assert_eq!(found.bases, vec![branch], "the branch is already in trunk");
    }

    #[tokio::test]
    async fn merge_bases_are_empty_for_an_unknown_oid() {
        let store = test_store!();
        let repo = seed_repo(&store).await;
        let known = oid(1);
        append_commit(&store, repo.id, known, vec![]).await;
        let unknown = oid(0xEE);

        assert!(
            store
                .graph
                .repo(repo.id)
                .merge_bases(known, unknown, 10_000)
                .await
                .unwrap()
                .bases
                .is_empty()
        );
    }

    /// Same concern as the `is_ancestor` cross-repo test: a walk that did not
    /// filter on `repo_id` would meet another repository's overlapping seqs.
    #[tokio::test]
    async fn merge_bases_do_not_cross_repos_with_overlapping_seq() {
        let store = test_store!();
        let (repo_a, repo_b) = (seed_repo(&store).await.id, seed_repo(&store).await.id);
        let (root_a, tip_a) = (oid(1), oid(2));
        append_commit(&store, repo_a, root_a, vec![]).await;
        append_commit(&store, repo_a, tip_a, vec![root_a]).await;
        // Same seqs in the other repository, and nothing to do with these.
        append_commit(&store, repo_b, oid(3), vec![]).await;
        append_commit(&store, repo_b, oid(4), vec![oid(3)]).await;

        let found = store
            .graph
            .repo(repo_a)
            .merge_bases(tip_a, root_a, 10_000)
            .await
            .unwrap();
        assert_eq!(found.bases, vec![root_a]);
    }

    /// A gather stops a segment being the live copy, and the row is what
    /// keeps the sweep off the object until its window has passed.
    ///
    /// The sweep's other clock is the ULID's, which says when the object was
    /// *written* — long past for anything a gather takes.
    #[tokio::test]
    async fn a_retired_segment_stays_referenced_until_its_window_passes() {
        let store = test_store!();
        let repo = store.rows.create(None).await.expect("a repository");
        let rows = store.rows.repo(repo.id);
        let id = enroute_git_core::Ulid::generate();

        // Through the ledger, which is the only thing that registers or
        // retires an image — a test writing the rows itself would be
        // asserting about its own writes.
        let mut registering = enroute_git_journal::Journal::new();
        registering.register([id]);
        store
            .ledger
            .commit(repo.id, &registering)
            .await
            .expect("registering");
        assert!(
            rows.referenced_segments(&[id])
                .await
                .expect("a lookup")
                .contains(&id),
            "a registered segment is referenced"
        );

        let mut retiring = enroute_git_journal::Journal::new();
        retiring.retire([id]);
        store
            .ledger
            .commit(repo.id, &retiring)
            .await
            .expect("retiring");
        assert!(
            rows.referenced_segments(&[id])
                .await
                .expect("a lookup")
                .contains(&id),
            "a retired segment is still one the sweep must leave alone"
        );

        assert_eq!(
            rows.drop_retired_segments(3600).await.expect("a pass"),
            0,
            "dropped inside the window a reader is still in"
        );
        assert!(
            rows.referenced_segments(&[id])
                .await
                .expect("a lookup")
                .contains(&id),
            "and so it is still referenced"
        );

        assert_eq!(
            rows.drop_retired_segments(0).await.expect("a pass"),
            1,
            "the window passed and the row stayed"
        );
        assert!(
            !rows
                .referenced_segments(&[id])
                .await
                .expect("a lookup")
                .contains(&id),
            "the sweep may take the object now"
        );
    }
}
