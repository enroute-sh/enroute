//! Small files written whole and durably: after a crash the path holds the
//! whole file or nothing, never a torn or empty one (as with a write to a
//! temporary file, an fsync, and a rename, the way SQLite and etcd do it).
//! Also directories that must survive a crash once made: a new file or
//! directory is durable only once the directory that names it is synced.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;

/// Writes `data` to `path`, readable as `mode`, durably.
///
/// # Errors
///
/// Returns an error if a write, sync or rename fails.
pub fn write(path: &Path, data: &[u8], mode: u32) -> io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    let tmp = path.with_extension("tmp");
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode)
        .open(&tmp)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    sync_dir(dir)
}

/// Makes the directory `path` durable: its entries (files made, renamed or
/// removed in it) survive a crash once this returns.
///
/// # Errors
///
/// Returns an error if `path` cannot be opened or synced.
pub fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Creates `path` and any missing parents, as [`fs::create_dir_all`] does,
/// and syncs the directory that names each new one, so none of them is
/// lost in a crash.
///
/// # Errors
///
/// Returns an error if a directory cannot be created or synced.
pub fn create_dir_all(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() || path.is_dir() {
        return Ok(());
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    create_dir_all(parent)?;
    match fs::create_dir(path) {
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {}
        r => r?,
    }
    sync_dir(parent)
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_written_file_reads_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("id");
        super::write(&path, b"one", 0o600).unwrap();
        super::write(&path, b"two", 0o600).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        assert!(!dir.path().join("id.tmp").exists());
    }

    #[test]
    fn directories_are_made_with_their_parents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/b/c");
        super::create_dir_all(&path).unwrap();
        assert!(path.is_dir());
        super::create_dir_all(&path).unwrap();
    }
}
