//! The layer sweep: deleting layer files in the object store that nothing
//! names and nothing ever will (layers a flush or compaction replaced,
//! outputs of lost races, files of crashed or abandoned publishes), found by
//! listing the repository's layers. It runs on the compute that maintains
//! the repository, every `sweep_interval`.
//!
//! A file is deleted only if all of these hold:
//!
//! - **No manifest names it, or ever will.** These are facts about the log,
//!   not about any clock or how far behind this node's view of it is. The
//!   live manifest, read through a read barrier after the listing, does not
//!   name it; and its upload generation (the end of its id) is out of the
//!   window of that state (`gen + UPLOAD_GEN_WINDOW < upload_gen`), so the
//!   state machine refuses any commit adding it. A layer that left the
//!   manifest cannot come back under its name either: ids are never reused.
//! - **It has gone unnamed for `sweep_retention`, by the object store's
//!   clock**, so readers of an older state (a compute serving an older
//!   snapshot, a long query reading the object store) can finish, and a
//!   ledger restored from volume snapshots no older than that finds every
//!   file its states name. Getting this wrong fails a read (or such a
//!   restore); it never loses a file the live state names.
//!
//! The sweep advances the upload generation (a log entry) only when a file
//! nothing names is still in the window, so a repository with nothing to
//! delete costs no log entries.
//!
//! **The sweep record**, `data/repos/<repo>/sweep` (JSON), keeps the files seen
//! unnamed and since when. No node's clock is involved: a file's time is
//! the object store's last-modified time of the record write that first
//! listed it, which is only known after that write, so a new entry has
//! none and the next sweep fills it in from the record's last-modified time
//! as it reads it. That same time is the next sweep's "now". Both err late
//! (a file was seen before the write that lists it, and "now" is the last
//! write, not the present), which only delays deletions. The record is
//! replaced by compare-and-swap, so overlapping sweeps (a repository changing
//! maintainers) cannot undo each other's progress; losing it only delays
//! deletions too.
//!
//! **Deleted incarnations** are reclaimed from their trash markers
//! ([`Trash`]), which no compute follows. The ledger writes one only once
//! the incarnation's tombstone is committed, so a file whose generation is
//! at most the marker's is named by nothing and never will be (a later
//! incarnation starts its generations past its window): it goes after the
//! retention, timed in the marker as in a sweep record. The marker goes
//! once no such file is left.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use enroute_shared::repo_state::{TRASH_PREFIX, Trash, Unnamed, data_path, parse_trash_path};

use enroute_shared::state::{Command, UPLOAD_GEN_WINDOW, layer_gen};
use enroute_shared::store::ObjectVersion;

use crate::meta::RepoState;

use super::Maintainer;

/// Files deleted per request, and the pause between requests: S3 throttles
/// a burst of deletes against one prefix (each key of a bulk delete counts).
const DELETE_BATCH: usize = 200;
const DELETE_PAUSE: Duration = Duration::from_millis(200);

fn record_path(repo: &str) -> String {
    format!("{}/sweep", data_path(repo))
}

/// One sweep's decision, from what it saw: `record` as read, written last
/// at `last_write` (None: there was no record); the layer files `listed`;
/// the files `named` by the live manifest (read after listing); and the
/// live state's `upload_gen`. Updates `record` and
/// returns the files to delete.
fn decide(
    record: &mut Unnamed,
    last_write: Option<u64>,
    listed: &[String],
    named: &HashSet<String>,
    upload_gen: u64,
    retention: Duration,
) -> Vec<String> {
    // Entries without a time were added by the last write.
    if let Some(t) = last_write {
        record
            .values_mut()
            .filter(|v| v.is_none())
            .for_each(|v| *v = Some(t));
    }
    let listed_set: HashSet<&str> = listed.iter().map(String::as_str).collect();
    record.retain(|id, _| listed_set.contains(id.as_str()) && !named.contains(id));
    // Files without a generation are not ours to delete (no rule says
    // nothing will name them).
    for id in listed
        .iter()
        .filter(|id| !named.contains(*id) && layer_gen(id).is_some())
    {
        record.entry(id.clone()).or_insert(None);
    }
    let Some(now) = last_write else {
        return Vec::new();
    };
    let retention = crate::util::millis(retention);
    let expired = |id: &str| layer_gen(id).is_some_and(|g| g + UPLOAD_GEN_WINDOW < upload_gen);
    let delete: Vec<String> = record
        .iter()
        .filter(|(id, seen)| {
            seen.is_some_and(|t| now.saturating_sub(t) >= retention) && expired(id)
        })
        .map(|(id, _)| id.clone())
        .collect();
    for id in &delete {
        record.remove(id);
    }
    delete
}

/// Delete `ids` of `repo` in batches, pausing between them.
async fn delete_batched(node: &Maintainer, repo: &str, ids: &[String]) -> Result<()> {
    for (i, batch) in ids.chunks(DELETE_BATCH).enumerate() {
        if i > 0 {
            tokio::time::sleep(DELETE_PAUSE).await;
        }
        node.storage.delete_layers(repo, batch).await?;
    }
    Ok(())
}

/// Advance the upload generation if a listed file that nothing names is
/// still in the window. Returns false if the log refused the advance.
async fn advance_if_waiting(
    node: &Maintainer,
    repo: &str,
    m: &RepoState,
    listed: &[String],
) -> Result<bool> {
    let named: HashSet<&str> = m.layers.iter().map(|l| l.id.as_str()).collect();
    let waiting = |id: &String| {
        !named.contains(id.as_str())
            && layer_gen(id).is_some_and(|g| g + UPLOAD_GEN_WINDOW >= m.upload_gen)
    };
    if !listed.iter().any(waiting) {
        return Ok(true);
    }
    let out = node
        .propose(Command::advance_upload_gen(repo).at(m.epoch))
        .await?;
    if let Some(e) = out.error {
        tracing::debug!(repo, "not sweeping: {e}");
        return Ok(false);
    }
    Ok(true)
}

/// Sweep `repo` (which this node follows and maintains). Returns how many
/// files it deleted.
async fn sweep(node: &Maintainer, repo: &str) -> Result<usize> {
    let path = record_path(repo);
    let (mut record, version, last_write) = match node.objects.get_timed(&path).await? {
        Some((data, version, at)) => {
            let record = serde_json::from_slice(&data).unwrap_or_else(|e| {
                tracing::warn!(repo, "unreadable sweep record, starting again: {e:#}");
                Unnamed::new()
            });
            (record, Some(version), Some(crate::util::unix_ms(at)))
        }
        None => (Unnamed::new(), None, None),
    };
    let before = record.clone();
    let listed = node.storage.list_layers(repo).await?;

    // A file nothing names that is still in the window can only be deleted
    // once the generation moves on.
    if let Some(m) = node.states.get(repo)
        && !advance_if_waiting(node, repo, &m, &listed).await?
    {
        return Ok(0);
    }

    // The live state, at least as new as the listing and the advance.
    let Some(live) = node.latest(repo).await? else {
        return Ok(0);
    };
    let snap = live.snapshot();
    let named: HashSet<String> = snap.meta.layers.iter().map(|l| l.id.clone()).collect();
    let delete = decide(
        &mut record,
        last_write,
        &listed,
        &named,
        snap.meta.upload_gen,
        node.config.sweep_retention,
    );

    delete_batched(node, repo, &delete).await?;
    if !delete.is_empty() {
        tracing::info!(repo, deleted = delete.len(), "swept unnamed layer files");
    }

    // Written whenever it lists anything: its write time is the next
    // sweep's clock.
    write_record(node, repo, &path, version.as_ref(), &record, &before).await?;
    Ok(delete.len())
}

/// Write the sweep record `record` (read as `before`) if it changed or lists
/// anything, by compare-and-swap on `version`.
async fn write_record(
    node: &Maintainer,
    repo: &str,
    path: &str,
    version: Option<&ObjectVersion>,
    record: &Unnamed,
    before: &Unnamed,
) -> Result<()> {
    if record.is_empty() && record == before {
        return Ok(());
    }
    let data = serde_json::to_vec(record)?;
    if node
        .objects
        .put_if(path, data.into(), version)
        .await?
        .is_none()
    {
        tracing::debug!(repo, "another sweep changed the sweep record first");
    }
    Ok(())
}

/// Reclaim the layer files of every deleted incarnation (see the module
/// documentation) whose repository this node maintains.
/// Returns how many files it deleted.
async fn reclaim(node: &Maintainer) -> Result<usize> {
    let mut deleted = 0;
    for path in node.objects.list_paths(TRASH_PREFIX).await? {
        let Some((repo, epoch)) = parse_trash_path(&path) else {
            continue;
        };
        if !node.owners.owns(repo) {
            continue;
        }
        match reclaim_one(node, repo, epoch, &path).await {
            Ok(n) => deleted += n,
            Err(e) => tracing::warn!(repo, epoch, "reclaiming a deleted repository: {e:#}"),
        }
    }
    Ok(deleted)
}

/// Reclaim the files of incarnation `epoch` of `repo`, as its trash
/// marker at `path` says.
async fn reclaim_one(node: &Maintainer, repo: &str, epoch: u64, path: &str) -> Result<usize> {
    let Some((data, version, at)) = node.objects.get_timed(path).await? else {
        return Ok(0);
    };
    let mut trash: Trash = serde_json::from_slice(&data)?;
    let listed: Vec<String> = node
        .storage
        .list_layers(repo)
        .await?
        .into_iter()
        .filter(|id| layer_gen(id).is_some_and(|g| g <= trash.upload_gen))
        .collect();
    if listed.is_empty() {
        node.objects
            .delete_objects([object_store::path::Path::from(path)])
            .await?;
        tracing::info!(repo, epoch, "reclaimed a deleted repository");
        return Ok(0);
    }
    // As the next incarnation's window starts: every listed file is out.
    let past = trash.upload_gen + UPLOAD_GEN_WINDOW + 1;
    let delete = decide(
        &mut trash.unnamed,
        Some(crate::util::unix_ms(at)),
        &listed,
        &HashSet::new(),
        past,
        node.config.sweep_retention,
    );
    delete_batched(node, repo, &delete).await?;
    let data = serde_json::to_vec(&trash)?;
    node.objects
        .put_if(path, data.into(), Some(&version))
        .await?;
    Ok(delete.len())
}

/// Sweep every repository this node maintains, and reclaim deleted ones,
/// every `sweep_interval`.
pub(super) fn spawn(node: &Arc<Maintainer>) {
    super::every(
        node,
        |n| tokio::time::sleep(n.config.sweep_interval),
        |n| async move { sweep_all(&n).await },
    );
}

/// Sweep the repositories this node maintains, then reclaim deleted ones.
async fn sweep_all(node: &Maintainer) {
    for repo in node
        .states
        .names()
        .into_iter()
        .filter(|r| node.owners.owns(r))
    {
        if let Err(e) = sweep(node, &repo).await {
            tracing::warn!(repo, "layer sweep failed: {e:#}");
        }
    }
    if let Err(e) = reclaim(node).await {
        tracing::warn!("reclaiming deleted repositories: {e:#}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enroute_shared::state::layer_id;

    const R: Duration = Duration::from_millis(1000);

    fn id(n: u128, generation: u64) -> String {
        layer_id(n, generation)
    }

    fn set(ids: &[&String]) -> HashSet<String> {
        ids.iter().map(|s| (*s).clone()).collect()
    }

    /// A file nothing names is deleted once its generation is out of the
    /// window and it has been unnamed for the retention, both measured
    /// from record writes.
    #[test]
    fn unnamed_files_go_after_the_window_and_the_retention() {
        let (old, kept) = (id(1, 0), id(2, 0));
        let listed = vec![old.clone(), kept.clone()];
        let named = set(&[&kept]);
        let mut r = Unnamed::new();
        // First sweep: no record yet, so no clock; the file is noted.
        assert!(decide(&mut r, None, &listed, &named, 5, R).is_empty());
        assert_eq!(r.get(&old), Some(&None));
        // Next: its time is the last write's.
        assert!(decide(&mut r, Some(10_000), &listed, &named, 5, R).is_empty());
        assert_eq!(r.get(&old), Some(&Some(10_000)));
        // Not yet past the retention.
        assert!(decide(&mut r, Some(10_999), &listed, &named, 5, R).is_empty());
        assert_eq!(
            decide(&mut r, Some(11_000), &listed, &named, 5, R),
            vec![old.clone()]
        );
        assert!(
            r.is_empty(),
            "deleted files leave the record; named ones never enter it"
        );
    }

    /// Within the window a file may still be committed (a publish between
    /// its upload and its commit), so it stays however long it has been
    /// unnamed; once the generation moves on, it can go.
    #[test]
    fn files_in_the_window_stay() {
        let f = id(1, 3);
        let listed = vec![f.clone()];
        let mut r = Unnamed::new();
        decide(&mut r, Some(0), &listed, &HashSet::new(), 3, R);
        decide(&mut r, Some(10), &listed, &HashSet::new(), 3, R);
        assert_eq!(r.get(&f), Some(&Some(10)));
        for upload_gen in [3, 4, 5] {
            assert!(
                decide(
                    &mut r,
                    Some(1_000_000),
                    &listed,
                    &HashSet::new(),
                    upload_gen,
                    R
                )
                .is_empty(),
                "generation {upload_gen}"
            );
        }
        assert_eq!(
            decide(&mut r, Some(1_000_000), &listed, &HashSet::new(), 6, R),
            vec![f]
        );
    }

    /// A file named again (committed after it was first seen unnamed)
    /// leaves the record, and starts over if it is unnamed later.
    #[test]
    fn named_files_leave_the_record() {
        let f = id(1, 0);
        let listed = vec![f.clone()];
        let mut r = Unnamed::new();
        decide(&mut r, Some(0), &listed, &HashSet::new(), 9, R);
        decide(&mut r, Some(100), &listed, &HashSet::new(), 9, R);
        assert_eq!(r.get(&f), Some(&Some(100)));
        assert!(decide(&mut r, Some(5_000), &listed, &set(&[&f]), 9, R).is_empty());
        assert!(r.is_empty());
        decide(&mut r, Some(6_000), &listed, &HashSet::new(), 9, R);
        assert!(
            decide(&mut r, Some(6_500), &listed, &HashSet::new(), 9, R).is_empty(),
            "its time started again"
        );
        assert_eq!(r.get(&f), Some(&Some(6_500)));
    }

    /// Files no longer listed leave the record; files without a generation
    /// in their id are never ours to delete.
    #[test]
    fn gone_and_foreign_files() {
        let (f, foreign) = (id(1, 0), "0123456789abcdef0123456789abcdef".to_string());
        let mut r = Unnamed::new();
        decide(
            &mut r,
            Some(0),
            &[f.clone(), foreign.clone()],
            &HashSet::new(),
            9,
            R,
        );
        assert_eq!(r.keys().collect::<Vec<_>>(), vec![&f]);
        assert!(
            decide(
                &mut r,
                Some(1_000_000),
                std::slice::from_ref(&foreign),
                &HashSet::new(),
                9,
                R
            )
            .is_empty()
        );
        assert!(r.is_empty());
    }
}
