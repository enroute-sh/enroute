//! Small files written whole and durably: after a crash the path holds the
//! whole file or nothing, never a torn or empty one (as with a write to a
//! temporary file, an fsync, and a rename, the way SQLite and etcd do it).

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
    File::open(dir)?.sync_all()
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
}
