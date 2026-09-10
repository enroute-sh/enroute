//! Unsigned LEB128: 7 bits per byte, least significant first, the high bit
//! marking a continuation.
//!
//! Git uses this for delta base/result sizes, and `enroute-git-store`'s commit-pack
//! format uses it for its inline entry headers. One codec for both, so a
//! disagreement between the two readers can't decode the same bytes two ways.

use enroute_git_core::Error;

/// Parse a varint from the front of `data`, returning the value and how many
/// bytes it consumed.
///
/// # Errors
/// Returns [`Error::Invalid`] if `data` ends before a terminating
/// (continuation-bit-clear) byte, or the value overruns a `u64`.
pub fn read_varint(data: &[u8]) -> Result<(u64, usize), Error> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (i, &byte) in data.iter().enumerate() {
        if shift >= 64 {
            return Err(Error::Invalid("varint overflow".into()));
        }
        value |= u64::from(byte & 0x7f) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }
    Err(Error::Invalid("varint truncated".into()))
}

/// Append a varint, the inverse of [`read_varint`].
pub fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        // Masked to the low 7 bits, so this always fits — `unwrap_or` never
        // actually falls back, just avoids an `as` truncation.
        let byte = u8::try_from(value & 0x7f).unwrap_or(0);
        value >>= 7;
        out.push(if value > 0 { byte | 0x80 } else { byte });
        if value == 0 {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{read_varint, write_varint};

    proptest::proptest! {
        #[test]
        fn varint_round_trips(v: u64) {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let (got, n) = read_varint(&buf).unwrap();
            proptest::prop_assert_eq!(got, v);
            proptest::prop_assert_eq!(n, buf.len());
        }
    }

    #[test]
    fn a_truncated_varint_is_rejected() {
        read_varint(&[0x80]).unwrap_err();
        read_varint(&[]).unwrap_err();
    }

    #[test]
    fn an_overlong_varint_is_rejected() {
        read_varint(&[0x80; 12]).unwrap_err();
    }
}
