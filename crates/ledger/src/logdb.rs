//! The ledger node's local log store: one [raft-engine] instance shared by
//! every replicated log on the node (one raft-engine group each, under its log id),
//! and the node's identity (its own region, key-value pairs only).
//!
//! raft-engine is `TiKV`'s log store for many raft groups: one append-only
//! sequence of files with group commit, an in-memory index of entry
//! positions per group, crash recovery, and garbage collection of old files
//! (purging, which may ask for groups holding them back to be compacted).
//! Values are protobuf messages (see `consensus::codec`): raft-rs's for the
//! root log, the ledger's own for the repository logs; the identity is
//! plain bytes.
//!
//! The consensus engine owns the store: everything else holds it weakly, so
//! it closes (releasing its directory lock) once consensus has stopped.
//!
//! **Reads go through one thread per store** ([`LogDb::read`]).
//! raft-engine caches the last block it decoded in a thread-local keyed by
//! file position only, so two stores in one process (several nodes in a
//! test) reading on the same thread can be handed each other's bytes. Every
//! read that can reach a log file (fetching entries, recovery, purging,
//! which rewrites entries) runs on the store's own reader thread.
//!
//! [raft-engine]: https://github.com/tikv/raft-engine

use std::hash::{BuildHasher, Hasher};
use std::path::Path;
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use raft_engine::{Config, Engine, LogBatch, ReadableSize};

/// The node's log store (a cheap handle). Writes and lookups that only
/// touch raft-engine's in-memory index go straight to the [`Engine`];
/// anything that may read a log file goes through [`LogDb::read`].
#[derive(Clone)]
pub(crate) struct LogDb(Arc<Db>);

pub(crate) struct Db {
    engine: Engine,
    reads: Mutex<std::sync::mpsc::Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send>;

impl std::ops::Deref for LogDb {
    type Target = Engine;

    fn deref(&self) -> &Engine {
        &self.0.engine
    }
}

impl LogDb {
    fn new(engine: Engine) -> Result<LogDb> {
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("logdb-reads".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    job();
                }
            })?;
        Ok(LogDb(Arc::new(Db {
            engine,
            reads: Mutex::new(tx),
        })))
    }

    /// Run `f` on this store's reader thread and wait for it.
    #[expect(
        clippy::expect_used,
        reason = "a stopped reader means the store is gone; callers cannot recover"
    )]
    pub(crate) fn read<R: Send + 'static>(
        &self,
        f: impl FnOnce(&Engine) -> R + Send + 'static,
    ) -> R {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let db = self.clone();
        let job: Job = Box::new(move || {
            // The caller panics on a stopped reader, so a lost result is moot.
            drop(tx.send(f(&db)));
        });
        self.0
            .reads
            .lock()
            .send(job)
            .expect("log store reader stopped");
        rx.recv().expect("log store reader stopped")
    }

    pub(crate) fn downgrade(&self) -> WeakLogDb {
        WeakLogDb(Arc::downgrade(&self.0))
    }
}

/// A handle that does not keep the store open.
pub(crate) struct WeakLogDb(Weak<Db>);

impl WeakLogDb {
    pub(crate) fn upgrade(&self) -> Option<LogDb> {
        self.0.upgrade().map(LogDb)
    }
}

/// Whether a region of the store is a log: log ids are odd (see
/// [`log_id`](crate::consensus::log_id)); other regions, like the store's
/// identity, are even.
pub(crate) fn is_log(region: u64) -> bool {
    region % 2 == 1
}

/// The region holding the store's identity (even).
const IDENTITY: u64 = 2;
const STORE_ID_KEY: &[u8] = b"store_id";

/// This store's id, created with the store: the node's id in the ledger. A node
/// whose store is lost comes back as a new node, so it can never vote in
/// logs it no longer remembers.
pub(crate) fn store_id(db: &LogDb) -> Result<u64> {
    if let Some(v) = db.get(IDENTITY, STORE_ID_KEY) {
        let bytes: [u8; 8] = v.as_slice().try_into().context("malformed store id")?;
        return Ok(u64::from_le_bytes(bytes));
    }
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    );
    h.write_u32(std::process::id());
    // Below 2^53, so it survives JSON readers that use doubles; never 0.
    let id = (h.finish() >> 11).max(1);
    let mut batch = LogBatch::default();
    batch.put(IDENTITY, STORE_ID_KEY.to_vec(), id.to_le_bytes().to_vec())?;
    db.write(&mut batch, true)?;
    Ok(id)
}

/// Open (and recover) the store in `dir`.
pub(crate) fn open(dir: &Path) -> Result<LogDb> {
    std::fs::create_dir_all(dir)?;
    let cfg = Config {
        dir: dir.display().to_string(),
        target_file_size: ReadableSize::mb(64),
        // Purge (rewrite live data out of old files) once the store is this
        // big; the default (10 GiB) is sized for TiKV.
        purge_threshold: ReadableSize::mb(512),
        ..Default::default()
    };
    // A node stopping in this process (tests restart nodes in place) may
    // still hold the directory lock for a moment.
    let mut tries = 0;
    loop {
        match Engine::open(cfg.clone()) {
            Ok(e) => return LogDb::new(e),
            Err(e) if tries < 100 && e.to_string().contains("Failed to lock file") => {
                tries += 1;
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            Err(e) => return Err(e).context("open the log store"),
        }
    }
}
