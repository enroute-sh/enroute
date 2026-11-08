use std::cell::RefCell;

use anyhow::{Context, Result, anyhow, ensure};
use sirocco::{
    Format,
    deflate::{self, Encoder, Flush, Level},
    inflate::Decoder,
};

thread_local! {
    // The encoder's tables are about 340 KiB; reuse keeps that allocation
    // off the per-object path. An encoder over them costs nothing to make.
    static TABLES: RefCell<Box<[u16]>> = RefCell::new(vec![0; deflate::TABLES_LEN].into_boxed_slice());
    pub(super) static DECODER: RefCell<Decoder> = const { RefCell::new(Decoder::new(Format::Zlib)) };
}

/// Inputs below this size go in a stored block: deflate makes them larger,
/// and its setup costs more than the copy.
const STORED_BELOW: usize = 64;

/// Append the zlib stream of `data` to `out`.
pub(crate) fn compress(data: &[u8], out: &mut Vec<u8>) {
    let level = if data.len() < STORED_BELOW {
        Level::NONE
    } else {
        Level::DEFAULT
    };
    TABLES.with_borrow_mut(|tables| {
        let mut e = Encoder::with_tables(Format::Zlib, level, &mut tables[..]);
        let start = out.len();
        out.resize(start + deflate::compress_bound(Format::Zlib, data.len()), 0);
        let p = e.compress(
            data,
            out.get_mut(start..).unwrap_or_default(),
            Flush::Finish,
        );
        assert!(
            p.done,
            "the buffer is at the compress bound, so it cannot be too small"
        );
        let n = p.output.len();
        out.truncate(start + n);
    });
}

/// Inflate the zlib stream `data` to exactly `expected` bytes.
pub(crate) fn decompress(data: &[u8], expected: usize) -> Result<Vec<u8>> {
    DECODER.with_borrow_mut(|d| {
        let mut out = vec![0; expected];
        let n = match d.decompress(data, &mut out, true) {
            Ok(p) if p.done => Some(p.output.len()),
            // Not done with all of the input: the output is longer.
            Ok(_) | Err(sirocco::inflate::Error::BufferFull) => None,
            Err(e) => {
                d.reset();
                return Err(anyhow!("{e}")).context("inflate");
            }
        };
        if n != Some(expected) {
            d.reset();
        }
        ensure!(
            n == Some(expected),
            "zlib size mismatch: {n:?} != {expected}"
        );
        Ok(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zlib(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        compress(data, &mut out);
        out
    }

    #[test]
    fn a_failed_stream_does_not_affect_the_next() {
        let data: Vec<u8> = (0..100_000u32)
            .map(|i| u8::try_from(i * 7 % 251).unwrap())
            .collect();
        let z = zlib(&data);
        decompress(&z[..z.len() / 2], data.len()).unwrap_err();
        decompress(&z, data.len() - 1).unwrap_err();
        assert_eq!(decompress(&z, data.len()).unwrap(), data);
        assert_eq!(zlib(&data), z);
        assert_eq!(decompress(&zlib(b""), 0).unwrap(), b"");
        let mut out = b"head".to_vec();
        compress(&data, &mut out);
        assert_eq!(out[..4], *b"head");
        assert_eq!(out[4..], z);
    }

    #[test]
    fn tiny_inputs_go_stored() {
        // zlib header, block header, length and complement, adler32.
        assert_eq!(zlib(&[7; STORED_BELOW - 1]).len(), STORED_BELOW - 1 + 11);
    }

    #[test]
    fn stored_and_deflated_streams_read_as_zlib() {
        for n in [0, 1, STORED_BELOW - 1, STORED_BELOW, 5000] {
            let data: Vec<u8> = (0..n).map(|i| u8::try_from(i % 7).unwrap()).collect();
            let z = zlib(&data);
            assert_eq!(decompress(&z, n).unwrap(), data);
        }
    }
}
