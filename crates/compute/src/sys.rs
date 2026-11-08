//! The only module allowed to use `unsafe` (the crate denies it elsewhere):
//! the OS calls that rustix does not wrap.

#![warn(clippy::undocumented_unsafe_blocks)]

/// Free the disk space of bytes `off..off + len` of `file`: they read as
/// zeros afterwards and the file keeps its length. Filesystems free whole
/// blocks only, so the range should be block-aligned.
pub(crate) fn punch_hole(file: &std::fs::File, off: u64, len: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::FallocateFlags;
        let mode = FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE;
        Ok(rustix::fs::fallocate(file, mode, off, len)?)
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        let args = libc::fpunchhole_t {
            fp_flags: 0,
            reserved: 0,
            fp_offset: off_t(off)?,
            fp_length: off_t(len)?,
        };
        // SAFETY: the descriptor is open for the call's duration and `args`
        // is a valid `fpunchhole_t` that outlives it.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &args) };
        if rc == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (file, off, len);
        Ok(())
    }
}

/// Convert a file offset or length to the OS type.
#[cfg(target_os = "macos")]
fn off_t(v: u64) -> std::io::Result<libc::off_t> {
    libc::off_t::try_from(v).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
}

/// The soft limit on open file descriptors; `u64::MAX` when unlimited.
pub(crate) fn open_file_limit() -> u64 {
    rustix::process::getrlimit(rustix::process::Resource::Nofile)
        .current
        .unwrap_or(u64::MAX)
}

/// Fill `bufs`, one after another, from offset `off` of `file`, with as few
/// `preadv` calls as the OS allows.
pub(crate) fn read_exact_vectored_at(
    file: &std::fs::File,
    bufs: &mut [&mut [u8]],
    off: u64,
) -> std::io::Result<()> {
    use std::io::IoSliceMut;
    let mut slices: Vec<IoSliceMut<'_>> = bufs.iter_mut().map(|b| IoSliceMut::new(b)).collect();
    let mut rest = &mut slices[..];
    let mut off = off;
    while !rest.is_empty() {
        let n = rustix::io::preadv(file, rest, off)?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        off += u64::try_from(n).unwrap_or(u64::MAX);
        IoSliceMut::advance_slices(&mut rest, n);
    }
    Ok(())
}
