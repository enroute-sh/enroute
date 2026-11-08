//! The engine and its two stores: what a restart finds when they disagree,
//! and what keeps the log from being truncated too far.

use std::path::Path;

use enroute_shared::oid::Oid;
use enroute_shared::proto::update;
use enroute_shared::state::{Commit, LayerMeta, RefUpdate, layer_id};

use crate::proto::root_entry;
use crate::state::store::Root;

use super::*;

/// A standalone node on `dir`, compacting every `every` entries.
fn start(dir: &Path, every: u64) -> Result<(Arc<Engine>, u64)> {
    let db = crate::logdb::open(&dir.join("log"))?;
    let id = crate::logdb::store_id(&db)?;
    let state = Arc::new(StateStore::open(&dir.join("state"), 16 << 20)?);
    Ok((Engine::start(db, state, every, id, None, true)?, id))
}

/// Stop a node, and wait for it to close its stores.
async fn stop(engine: Arc<Engine>) {
    engine.shutdown();
    tokio::time::timeout(Duration::from_secs(5), engine.stopped())
        .await
        .expect("the engine did not stop");
}

/// Wait (up to 5 s) until `f` holds. A restarted solo node sequences its
/// logs again only once appointed for a new term, a second or so later.
async fn wait_until(mut f: impl FnMut() -> bool) -> bool {
    for _ in 0..250 {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    f()
}

fn push(name: &str, n: u8) -> Command {
    Command::commit(
        "r",
        Commit {
            updates: vec![RefUpdate {
                name: name.into(),
                old: None,
                new: Some(Oid::sha1([n; 20])),
            }],
            ..Default::default()
        },
    )
}

/// Apply a root entry.
async fn root(e: &Engine, change: root_entry::Change) {
    e.propose_root(change.into()).await.unwrap();
}

/// Create repository `r` through the root log: it is hosted at once.
async fn create(e: &Engine) {
    let c = e.create("r", None, Default::default());
    root(e, root_entry::Change::Create(c)).await;
    assert_eq!(e.locate("r").await.unwrap(), Located::Here);
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to.join(e.file_name()));
        } else {
            std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

async fn propose(engine: &Engine, cmd: Command) -> Outcome {
    let out = engine.propose(cmd).await.unwrap();
    assert!(
        out.error.is_none() && out.ref_results.iter().all(|r| r.error.is_none()),
        "{out:?}"
    );
    out
}

/// A restarted node lost its sequencer's unsynced entries, so it does not
/// go on with its term: the root appoints it again, for the next one.
#[tokio::test]
async fn a_restart_starts_a_new_term() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = start(dir.path(), 1000).unwrap();
    create(&e).await;
    propose(&e, push("refs/heads/a", 1)).await;
    assert_eq!(e.hosted("r").unwrap().term, 1);
    stop(e).await;

    let (e, _) = start(dir.path(), 1000).unwrap();
    // Proposals wait for the new term.
    propose(&e, push("refs/heads/b", 2)).await;
    let h = e.hosted("r").unwrap();
    assert!(h.leading && h.term == 2, "{h:?}");
    assert_eq!(e.repo_meta("r").unwrap().refs.len(), 2);
    stop(e).await;
}

/// A state store that lost writes it had not synced (an old copy of it)
/// catches up from the log at restart.
#[tokio::test]
async fn restart_replays_the_log_after_the_state() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = start(dir.path(), 1000).unwrap();
    create(&e).await;
    for i in 0..3 {
        propose(&e, push(&format!("refs/heads/a{i}"), i)).await;
    }
    stop(e).await;
    copy_dir(&dir.path().join("state"), &dir.path().join("old"));

    let (e, _) = start(dir.path(), 1000).unwrap();
    for i in 0..3 {
        propose(&e, push(&format!("refs/heads/b{i}"), i)).await;
    }
    let want = e.repo_meta("r").unwrap();
    assert_eq!(want.refs.len(), 6);
    stop(e).await;

    std::fs::remove_dir_all(dir.path().join("state")).unwrap();
    std::fs::rename(dir.path().join("old"), dir.path().join("state")).unwrap();
    let (e, _) = start(dir.path(), 1000).unwrap();
    assert_eq!(e.repo_meta("r").unwrap(), want);
    assert_eq!(e.hosted("r").unwrap().version, want.version);
    propose(&e, push("refs/heads/c", 9)).await;
    stop(e).await;
}

/// A snapshot installed in the state store whose log was not reset to it
/// (a crash in between): the restart resets the log, and the log goes on
/// from the snapshot.
#[tokio::test]
async fn restart_resets_a_log_behind_its_state() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = start(dir.path(), 1000).unwrap();
    create(&e).await;
    propose(&e, push("refs/heads/main", 1)).await;
    let log = e.hosted("r").unwrap().log;
    let mut meta = e.repo_meta("r").unwrap();
    stop(e).await;

    let (ahead, version) = {
        let state = StateStore::open(&dir.path().join("state"), 16 << 20).unwrap();
        let root = state.root(log).unwrap();
        meta.refs
            .insert("refs/heads/installed".into(), Oid::sha1([7; 20]));
        meta.version += 10;
        let ahead = Root {
            applied_index: root.applied_index + 10,
            ..root
        };
        state
            .install(log, ahead.clone(), stored::entries(&meta).unwrap())
            .unwrap();
        (ahead.applied_index, meta.version)
    };
    for _ in 0..2 {
        let (e, _) = start(dir.path(), 1000).unwrap();
        let h = e.hosted("r").unwrap();
        assert!(h.applied >= ahead, "{} < {ahead}", h.applied);
        assert!(
            e.repo_meta("r")
                .unwrap()
                .refs
                .contains_key("refs/heads/installed")
        );
        let out = propose(&e, push(&format!("refs/heads/after-{}", h.applied), 2)).await;
        assert!(out.repo_version > version);
        stop(e).await;
    }
}

/// A log truncated past what its state store kept cannot be replayed: the
/// node refuses to start rather than lose those entries silently.
#[tokio::test]
async fn a_state_behind_the_log_start_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = start(dir.path(), 2).unwrap();
    create(&e).await;
    propose(&e, push("refs/heads/main", 1)).await;
    let h = e.hosted("r").unwrap();
    let (log, old) = (h.log, h.applied);
    stop(e).await;
    copy_dir(&dir.path().join("state"), &dir.path().join("old"));

    let (e, _) = start(dir.path(), 2).unwrap();
    for i in 0..6 {
        propose(&e, push(&format!("refs/heads/b{i}"), i)).await;
    }
    // Truncated past the old state (once the new one is durable).
    assert!(
        wait_until(|| e
            .db
            .upgrade()
            .unwrap()
            .first_index(log)
            .is_none_or(|f| f > old + 1))
        .await
    );
    stop(e).await;
    std::fs::remove_dir_all(dir.path().join("state")).unwrap();
    std::fs::rename(dir.path().join("old"), dir.path().join("state")).unwrap();
    let err = start(dir.path(), 2).expect_err("refused");
    assert!(
        format!("{err:#}").contains("before the log's start"),
        "{err:#}"
    );
}

/// The log keeps an inline layer's entry until the layer is flushed, and
/// its bytes are read from there meanwhile.
#[tokio::test]
async fn inline_layers_hold_the_log_until_flushed() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = start(dir.path(), 1).unwrap();
    create(&e).await;
    propose(&e, push("refs/heads/main", 1)).await;
    let inline = LayerMeta {
        id: "inline-1".into(),
        objects: 3,
        bytes: 3,
        data: Some(b"abc".to_vec().into()),
        ..Default::default()
    };
    propose(
        &e,
        Command::commit(
            "r",
            Commit {
                add_layers: vec![inline],
                ..Default::default()
            },
        ),
    )
    .await;
    let h = e.hosted("r").unwrap();
    let (log, pinned) = (h.log, h.applied);
    for i in 0..5 {
        propose(&e, push(&format!("refs/heads/b{i}"), i)).await;
    }
    stop(e).await;
    // The first entry the log keeps (none: every one was compacted).
    let first = |dir: &Path| {
        crate::logdb::open(&dir.join("log"))
            .unwrap()
            .first_index(log)
    };
    assert!(first(dir.path()).is_some());
    assert!(
        first(dir.path()).unwrap() <= pinned,
        "the inline layer's entry is kept"
    );

    // After a restart its bytes still come from the log.
    let (e, _) = start(dir.path(), 1).unwrap();
    let l = e.repo_meta("r").unwrap().layers[0].clone();
    assert_eq!(l.data.as_deref(), Some(&b"abc"[..]));
    let flushed = LayerMeta {
        id: layer_id(7, 0),
        objects: 3,
        bytes: 3,
        first_id: l.first_id,
        id_span: l.id_span,
        ..Default::default()
    };
    propose(
        &e,
        Command::commit(
            "r",
            Commit {
                add_layers: vec![flushed],
                remove_layers: vec![(&l).into()],
                ..Default::default()
            },
        ),
    )
    .await;
    propose(&e, push("refs/heads/after", 1)).await;
    assert!(
        e.repo_meta("r")
            .unwrap()
            .layers
            .iter()
            .all(LayerMeta::is_stored)
    );
    // Truncated once the flush is durable (the apply thread syncs and
    // reports every SYNC_EVERY).
    let moved_on = || {
        e.db.upgrade()
            .unwrap()
            .first_index(log)
            .is_none_or(|f| f > pinned)
    };
    assert!(wait_until(moved_on).await);
    stop(e).await;
    assert!(
        first(dir.path()).is_none_or(|f| f > pinned),
        "flushed, the log moves on"
    );
}

/// A log that goes quiet compacts to what is durably applied, long before
/// `snapshot_every` entries.
#[tokio::test]
async fn a_quiet_log_compacts() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = start(dir.path(), 1000).unwrap();
    create(&e).await;
    for i in 0..3 {
        propose(&e, push(&format!("refs/heads/b{i}"), i)).await;
    }
    let h = e.hosted("r").unwrap();
    let (log, applied) = (h.log, h.applied);
    let compacted = || {
        e.db.upgrade()
            .unwrap()
            .first_index(log)
            .is_none_or(|f| f > applied)
    };
    assert!(wait_until(compacted).await);
    stop(e).await;
}

/// Read `sub` until its state (as it builds it) holds `refs` refs.
async fn read_until(sub: &mut Subscription, refs: usize) -> (RepoMeta, usize) {
    let mut latest: Option<RepoMeta> = None;
    let mut states = 0;
    loop {
        let update = sub.recv().await.expect("the subscription ended");
        match (update.kind, latest.as_mut()) {
            (Some(update::Kind::State(state)), _) => {
                states += 1;
                latest = Some(state);
            }
            (Some(update::Kind::Change(change)), Some(state)) => state.apply_change(&change),
            _ => {}
        }
        if let Some(state) = latest.as_ref().filter(|m| m.refs.len() == refs) {
            return (state.clone(), states);
        }
    }
}

/// A subscriber that falls `FOLLOW_QUEUE` updates behind is not dropped:
/// once it reads again, it gets what it missed as one change (not a whole
/// state), and follows from there.
#[tokio::test]
async fn a_subscriber_behind_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _) = start(dir.path(), 1000).unwrap();
    create(&engine).await;
    let mut sub = engine.follow("r").await.unwrap();
    let refs = FOLLOW_QUEUE + 50;
    for i in 0..refs {
        let byte = u8::try_from(i % 250).unwrap() + 1;
        propose(&engine, push(&format!("refs/heads/b{i}"), byte)).await;
    }
    let (state, states) = tokio::time::timeout(Duration::from_secs(10), read_until(&mut sub, refs))
        .await
        .expect("caught up with every ref");
    assert_eq!(state, engine.repo_meta("r").unwrap());
    assert_eq!(states, 1, "only the first state");
    // Still followed: a later change arrives as a change.
    propose(&engine, push("refs/heads/after", 1)).await;
    let next = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(next.kind, Some(update::Kind::Change(_))),
        "{next:?}"
    );
    drop(sub);
    stop(engine).await;
}

/// A seal whose appointment never comes (its controller lost the root's
/// leadership, and the seal landed late) deposes the sequencer while the
/// root still names it: the root's leader gives the log a new term.
#[tokio::test]
async fn a_seal_without_an_appointment_gets_a_new_term() {
    let dir = tempfile::tempdir().unwrap();
    let (e, _) = start(dir.path(), 1000).unwrap();
    create(&e).await;
    propose(&e, push("refs/heads/a", 1)).await;
    assert_eq!(e.hosted("r").unwrap().term, 1);
    // From a node that led the root once.
    let seal = crate::proto::Seal { term: 2 };
    e.receive(7, vec![framed(log_id("r", 1), frame::Kind::Seal(seal))]);
    propose(&e, push("refs/heads/b", 2)).await;
    let h = e.hosted("r").unwrap();
    assert!(h.leading && h.term == 2, "{h:?}");
    stop(e).await;
}
