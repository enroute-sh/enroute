use super::*;

fn open(dir: &Path) -> StateStore {
    StateStore::open(dir, 16 << 20).unwrap()
}

/// Give `log` an empty state.
fn create(s: &StateStore, log: LogId) {
    s.install(
        log,
        Root {
            repo: format!("r{log}"),
            ..Default::default()
        },
        [],
    )
    .unwrap();
}

const A: &[u8] = &[1; 20];
const B: &[u8] = &[2; 20];

/// A round sees its own writes; committed, so does the store (and a
/// reopened one).
#[test]
fn rounds_write_atomically_and_read_their_own_writes() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = open(dir.path());
        create(&s, 7);
        let mut r = s.round();
        r.set_ref(7, b"refs/heads/main", Some(A));
        assert_eq!(
            r.get_ref(7, b"refs/heads/main").unwrap().as_deref(),
            Some(A)
        );
        r.set_ref(7, b"refs/heads/main", Some(B));
        r.set_head(7, b"refs/heads/main");
        r.put_layer(7, 0, b"layer-a");
        r.pin_inline(7, 3, b"layer-a");
        r.set_counters(
            7,
            Counters {
                version: 5,
                layers: 1,
                ..Default::default()
            },
        );
        let root = r.root_mut(7).unwrap();
        (root.applied_index, root.applied_term) = (3, 1);
        assert_eq!(
            s.root(7).unwrap().applied_index,
            0,
            "nothing visible before commit"
        );
        r.commit().unwrap();
        let root = s.root(7).unwrap();
        assert_eq!(
            (root.applied_index, root.applied_term, root.version),
            (3, 1, 5)
        );
        assert_eq!(s.oldest_pin(7).unwrap(), Some(3));
    }
    let s = open(dir.path());
    assert_eq!(s.logs(), vec![7]);
    assert_eq!(s.root(7).unwrap().repo, "r7");
    let mut r = s.round();
    assert_eq!(
        r.get_ref(7, b"refs/heads/main").unwrap().as_deref(),
        Some(B)
    );
    assert_eq!(r.head(7).unwrap().as_deref(), Some(&b"refs/heads/main"[..]));
    assert_eq!(r.layer(7, 0).unwrap().as_deref(), Some(&b"layer-a"[..]));
    assert_eq!(r.counters(7).unwrap().version, 5);
}

/// The first ref under a prefix, with the round's own sets and deletes.
#[test]
fn first_ref_sees_the_rounds_writes() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(dir.path());
    create(&s, 1);
    let mut r = s.round();
    for n in ["refs/heads/b", "refs/heads/c", "refs/tags/a"] {
        r.set_ref(1, n.as_bytes(), Some(A));
    }
    r.commit().unwrap();
    let mut r = s.round();
    assert_eq!(
        r.first_ref(1, b"refs/heads/").unwrap().as_deref(),
        Some(&b"refs/heads/b"[..])
    );
    r.set_ref(1, b"refs/heads/b", None);
    assert_eq!(
        r.first_ref(1, b"refs/heads/").unwrap().as_deref(),
        Some(&b"refs/heads/c"[..]),
        "deleted in the round"
    );
    r.set_ref(1, b"refs/heads/a", Some(B));
    assert_eq!(
        r.first_ref(1, b"refs/heads/").unwrap().as_deref(),
        Some(&b"refs/heads/a"[..]),
        "set in the round"
    );
    r.get_ref(1, b"refs/heads/0").unwrap();
    assert_eq!(
        r.first_ref(1, b"refs/heads/").unwrap().as_deref(),
        Some(&b"refs/heads/a"[..]),
        "a read is no ref"
    );
    assert_eq!(r.first_ref(1, b"refs/notes/").unwrap(), None);
}

/// The next layer of the manifest from each first id: a round's scans see
/// its own writes (a deletion too) over the store's.
fn next_layers(r: &Round<'_>) -> Vec<Option<Vec<u8>>> {
    [0, 11, 21, 26, 31, 41]
        .into_iter()
        .map(|id| r.next_layer(1, id).unwrap())
        .collect()
}

/// A round's scans (the manifest) see its own writes over the store's.
#[test]
fn rounds_scan_their_own_writes() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(dir.path());
    create(&s, 1);
    let mut r = s.round();
    for id in [10u32, 20, 30] {
        r.put_layer(1, id, &id.to_be_bytes());
    }
    r.commit().unwrap();
    let mut r = s.round();
    r.delete_layer(1, 20);
    r.put_layer(1, 25, b"new");
    r.put_layer(1, 30, b"changed");
    r.put_layer(1, 40, b"last");
    let want = vec![
        Some(10u32.to_be_bytes().to_vec()),
        Some(b"new".to_vec()),
        Some(b"new".to_vec()),
        Some(b"changed".to_vec()),
        Some(b"last".to_vec()),
        None,
    ];
    assert_eq!(next_layers(&r), want);
    r.commit().unwrap();
    assert_eq!(next_layers(&s.round()), want);
}

/// Pins hold the log back to the oldest entry with inline bytes not yet
/// flushed; flushes release them, in any order.
#[test]
fn pins_bound_truncation_until_their_flush() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(dir.path());
    create(&s, 1);
    create(&s, 2);
    let mut r = s.round();
    // Above 255, so an index sorts by number, not by its low byte.
    for (index, id) in [(300u64, &b"a"[..]), (301, b"b"), (1000, b"c")] {
        r.pin_inline(1, index, id);
    }
    r.pin_inline(2, 5, b"x");
    r.commit().unwrap();
    assert_eq!(s.oldest_pin(1).unwrap(), Some(300));
    assert_eq!(s.oldest_pin(2).unwrap(), Some(5));
    for ((index, id), next) in [
        ((301, &b"b"[..]), Some(300)),
        ((300, b"a"), Some(1000)),
        ((1000, b"c"), None),
    ] {
        let mut r = s.round();
        r.drop_inline(1, index, id);
        r.commit().unwrap();
        assert_eq!(s.oldest_pin(1).unwrap(), next);
    }
    assert_eq!(
        s.oldest_pin(2).unwrap(),
        Some(5),
        "another repository's pins are its own"
    );
}

/// A state exported from one repository and installed for another
/// replaces that one's; pins are not exported (their bytes come instead),
/// and deleting a state leaves nothing.
#[test]
fn snapshots_export_and_install() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(dir.path());
    create(&s, 1);
    create(&s, 2);
    let mut r = s.round();
    for i in 0..1000u32 {
        r.set_ref(1, format!("refs/pull/{i}/head").as_bytes(), Some(A));
    }
    r.set_ref(2, b"refs/heads/stale", Some(B));
    r.put_layer(1, 7, b"l");
    r.pin_inline(1, 40, b"l");
    r.set_counters(
        1,
        Counters {
            version: 9,
            ..Default::default()
        },
    );
    let root = r.root_mut(1).unwrap();
    (root.applied_index, root.applied_term) = (42, 3);
    r.commit().unwrap();

    let mut entries = Vec::new();
    let root = s
        .export(1, |e| {
            entries.push(e);
            Ok(())
        })
        .unwrap()
        .unwrap();
    // The exporter sends the pinned bytes from its log (here: made up).
    entries.push(Entry::Inline {
        id: b"l".to_vec(),
        value: vec![1; 100 << 10],
    });
    s.install(2, root.clone(), entries.clone()).unwrap();
    let installed = s.root(2).unwrap();
    assert_eq!(
        (
            installed.applied_index,
            installed.version,
            installed.repo.clone()
        ),
        (42, 9, "r1".to_string())
    );
    let mut r = s.round();
    assert_eq!(
        r.get_ref(2, b"refs/heads/stale").unwrap(),
        None,
        "the old state is gone"
    );
    assert_eq!(
        r.get_ref(2, b"refs/pull/999/head").unwrap().as_deref(),
        Some(A)
    );
    assert_eq!(
        s.oldest_pin(2).unwrap(),
        None,
        "the bytes are here, not in a log"
    );
    let mut again = Vec::new();
    s.export(2, |e| {
        again.push(e);
        Ok(())
    })
    .unwrap();
    assert_eq!(again, entries);
    // Its flush drops them.
    let mut r = s.round();
    r.drop_inline(2, 40, b"l");
    r.commit().unwrap();
    let inline = |e: Entry| match e {
        Entry::Inline { .. } => anyhow::bail!("kept bytes after their flush"),
        _ => Ok(()),
    };
    s.export(2, inline).unwrap();

    s.delete(1).unwrap();
    assert!(s.root(1).is_none());
    assert!(s.export(1, |_| Ok(())).unwrap().is_none());
    assert_eq!(
        s.round().next_layer(1, 0).unwrap(),
        None,
        "its keys are gone too"
    );
}

/// Clearing a state drops what the store and the round hold for it; a write
/// after the clear, in the same round, stays.
#[test]
fn clear_state_drops_stored_and_round_writes() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(dir.path());
    let inline = Entry::Inline {
        id: b"kept".to_vec(),
        value: vec![1; 100 << 10],
    };
    s.install(1, Root::default(), [inline]).unwrap();
    let mut r = s.round();
    r.put_layer(1, 1, b"stored");
    r.set_ref(1, b"refs/heads/main", Some(A));
    r.commit().unwrap();
    let mut r = s.round();
    r.put_layer(1, 2, b"this round");
    r.set_head(1, b"refs/heads/main");
    r.clear_state(1).unwrap();
    r.put_layer(1, 3, b"after");
    r.commit().unwrap();
    let mut r = s.round();
    assert_eq!(r.next_layer(1, 0).unwrap().as_deref(), Some(&b"after"[..]));
    assert_eq!(r.get_ref(1, b"refs/heads/main").unwrap(), None);
    assert_eq!(r.head(1).unwrap().as_deref(), Some(&b"refs/heads/main"[..]));
    let inline = |e: Entry| match e {
        Entry::Inline { .. } => anyhow::bail!("kept inline bytes after a clear"),
        _ => Ok(()),
    };
    s.export(1, inline).unwrap();
}

/// The oldest pin follows the rounds that pin and drop inline layers, and
/// a reopened store still knows it (and no other log's).
#[test]
fn the_oldest_pin_is_kept_across_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = open(dir.path());
        create(&s, 7);
        create(&s, 8);
        let mut r = s.round();
        r.pin_inline(7, 9, b"b");
        r.pin_inline(7, 5, b"a");
        r.commit().unwrap();
        assert_eq!(s.oldest_pin(7).unwrap(), Some(5));
        assert_eq!(s.oldest_pin(8).unwrap(), None);
        s.persist().unwrap();
    }
    let s = open(dir.path());
    assert_eq!(s.oldest_pin(7).unwrap(), Some(5));
    let mut r = s.round();
    r.drop_inline(7, 5, b"a");
    r.commit().unwrap();
    assert_eq!(s.oldest_pin(7).unwrap(), Some(9));
    s.delete(7).unwrap();
    assert_eq!(s.oldest_pin(7).unwrap(), None);
}
