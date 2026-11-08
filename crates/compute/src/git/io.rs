//! Byte sources for the codecs: positional reads, independent of where the
//! bytes live (memory, a scratch file, a remote object). Storage implements
//! [`ReadAt`] for its handles; the codecs never see files or pages.

use anyhow::{Context, Result, ensure};

/// Positional reads over an immutable byte sequence.
pub(crate) trait ReadAt: Send + Sync {
    fn len(&self) -> u64;

    /// Fill `buf` from `off` (the range lies within `len`).
    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()>;

    /// The bytes, when they are already in memory (lets readers borrow
    /// instead of copying).
    fn as_slice(&self) -> Option<&[u8]> {
        None
    }
}

/// Bytes in memory.
pub(crate) struct Mem<'a>(pub &'a [u8]);

impl ReadAt for Mem<'_> {
    fn len(&self) -> u64 {
        // Saturates; `usize` is never wider than `u64` on supported targets.
        u64::try_from(self.0.len()).unwrap_or(u64::MAX)
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
        #[expect(
            clippy::expect_used,
            reason = "the trait contract puts the range within `len`; a breach is a caller bug"
        )]
        let src = usize::try_from(off)
            .ok()
            .and_then(|o| self.0.get(o..o.checked_add(buf.len())?))
            .expect("read_at range within len");
        buf.copy_from_slice(src);
        Ok(())
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(self.0)
    }
}

/// Bytes in memory, owned.
pub(crate) struct Owned(pub Vec<u8>);

impl ReadAt for Owned {
    fn len(&self) -> u64 {
        Mem(&self.0).len()
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
        Mem(&self.0).read_at(off, buf)
    }

    fn as_slice(&self) -> Option<&[u8]> {
        Some(&self.0)
    }
}

/// The bytes of `inner` from `start` on, owning `inner`.
pub(crate) struct Suffix<T> {
    inner: T,
    start: u64,
}

impl<T: ReadAt> Suffix<T> {
    pub(crate) fn new(inner: T, start: u64) -> Result<Suffix<T>> {
        ensure!(start <= inner.len(), "suffix starts past the end");
        Ok(Suffix { inner, start })
    }
}

impl<T: ReadAt> ReadAt for Suffix<T> {
    fn len(&self) -> u64 {
        self.inner.len() - self.start
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
        self.inner.read_at(self.start + off, buf)
    }

    fn as_slice(&self) -> Option<&[u8]> {
        self.inner
            .as_slice()?
            .get(usize::try_from(self.start).ok()?..)
    }
}

/// `len` bytes at `off`: borrowed when in memory, else copied.
pub(crate) fn read(src: &dyn ReadAt, off: u64, len: usize) -> Result<std::borrow::Cow<'_, [u8]>> {
    let end = off.checked_add(u64::try_from(len)?);
    ensure!(end.is_some_and(|e| e <= src.len()), "read past end");
    if let Some(s) = src.as_slice() {
        let start = usize::try_from(off)?;
        let bytes = s.get(start..start + len).context("read past end")?;
        return Ok(bytes.into());
    }
    let mut buf = vec![0u8; len];
    src.read_at(off, &mut buf)?;
    Ok(buf.into())
}

/// Bytes [`Incoming::of`] reads at a time.
const CHUNK: u64 = 1 << 20;

/// Where [`Incoming`] gets its chunks: `None` once the bytes end.
type Chunks = Box<dyn FnMut() -> Result<Option<bytes::Bytes>> + Send>;

/// A forward-only reader for sequential passes over bytes that arrive in
/// chunks (a request body as it streams in, or a [`ReadAt`] read in
/// order). It keeps what is not consumed yet, so an item that spans chunks
/// is still seen whole.
pub(crate) struct Incoming {
    next: Chunks,
    buf: Vec<u8>,
    start: usize,
    end: bool,
}

impl Incoming {
    pub(crate) fn new(next: impl FnMut() -> Result<Option<bytes::Bytes>> + Send + 'static) -> Self {
        Incoming {
            next: Box::new(next),
            buf: Vec::new(),
            start: 0,
            end: false,
        }
    }

    /// The bytes of `src`, in order.
    pub(crate) fn of(src: std::sync::Arc<dyn ReadAt>) -> Self {
        let mut off = 0;
        Incoming::new(move || {
            let n = CHUNK.min(src.len() - off);
            if n == 0 {
                return Ok(None);
            }
            let mut chunk = vec![0; usize::try_from(n)?];
            src.read_at(off, &mut chunk)?;
            off += n;
            Ok(Some(chunk.into()))
        })
    }

    /// The bytes not consumed yet: at least `n` of them, unless the bytes
    /// end first.
    pub(crate) fn fill(&mut self, n: usize) -> Result<&[u8]> {
        while self.buf.len() - self.start < n && !self.end {
            // Drop the consumed prefix once it is most of the buffer.
            if self.start > self.buf.len() / 2 {
                self.buf.drain(..self.start);
                self.start = 0;
            }
            match (self.next)()? {
                Some(chunk) => self.buf.extend_from_slice(&chunk),
                None => self.end = true,
            }
        }
        Ok(self.buf.get(self.start..).unwrap_or_default())
    }

    /// Exactly `n` bytes, which must be there.
    pub(crate) fn take(&mut self, n: usize) -> Result<&[u8]> {
        self.fill(n)?;
        let s = self.start;
        self.start = s
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .context("truncated")?;
        Ok(self.buf.get(s..self.start).unwrap_or_default())
    }

    /// Inflate the zlib stream `at` bytes past the unconsumed start to
    /// `expected` bytes; the bytes the stream takes. Nothing is consumed.
    pub(crate) fn inflate(&mut self, at: usize, expected: usize) -> Result<(Vec<u8>, usize)> {
        let mut out = vec![0; expected];
        super::zlib::DECODER.with_borrow_mut(|d| {
            let r = self.inflate_with(d, at, &mut out);
            if r.is_err() {
                d.reset();
            }
            let used = r?;
            let n = d.total_out();
            ensure!(
                n == u64::try_from(expected)?,
                "zlib size mismatch: {n} != {expected}"
            );
            Ok((out, used))
        })
    }

    /// Feed `d` the stream at `at` until it ends.
    fn inflate_with(
        &mut self,
        d: &mut sirocco::inflate::Decoder,
        at: usize,
        out: &mut [u8],
    ) -> Result<usize> {
        // Most streams fit in the first window: zlib's worst case.
        let mut want = at
            .saturating_add(out.len())
            .saturating_add(out.len() / 8)
            .saturating_add(1024);
        let mut used = 0;
        loop {
            let w = self.fill(want)?;
            let end = w.len() < want;
            let z = w.get(at + used..).context("truncated")?;
            let p = d.decompress(z, out, end).context("inflate")?;
            used += p.consumed;
            if p.done {
                return Ok(used);
            }
            ensure!(!end, "zlib size mismatch: stream longer than {}", out.len());
            want = want.saturating_mul(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inflate_finds_the_end_of_streams_across_chunks() {
        let mut x = 1u32;
        let data: Vec<u8> = (0..3 << 20)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                x.to_le_bytes()[0]
            })
            .collect();
        let mut pack = Vec::new();
        super::super::zlib::compress(&data, &mut pack);
        let len = pack.len();
        pack.extend_from_slice(&[0x78, 0x9c, 1, 2, 3]);
        let chunks: Vec<bytes::Bytes> = pack
            .chunks(4096)
            .map(bytes::Bytes::copy_from_slice)
            .collect();
        let mut chunks = chunks.into_iter();
        let mut inc = Incoming::new(move || Ok(chunks.next()));
        let (out, used) = inc.inflate(0, data.len()).unwrap();
        assert_eq!((out == data, used), (true, len));
        inc.inflate(0, data.len() + 1).unwrap_err();
        inc.take(len).unwrap();
        assert_eq!(inc.fill(64).unwrap(), &[0x78, 0x9c, 1, 2, 3]);
    }
}
