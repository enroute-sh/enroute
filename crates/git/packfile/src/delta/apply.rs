use std::io::Write as _;

use enroute_git_core::Error;

use crate::format::MAX_OBJECT_BYTES;

use super::read_varint;

/// Apply git delta instructions to `base`, producing the reconstructed object.
///
/// `gix_pack::data::delta::apply` is `pub(crate)`, hence this reimplementation.
/// See the module docs for the stream layout.
///
/// # Errors
/// Returns [`Error::Invalid`] if the delta is truncated, declares a base size
/// that doesn't match `base`, copies outside the base, or produces a result of
/// the wrong length.
pub fn apply_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>, Error> {
    let (base_size, n) = read_varint(delta)?;
    let delta = delta
        .get(n..)
        .ok_or_else(|| Error::Invalid("delta truncated after base_size".into()))?;
    let expected_base =
        usize::try_from(base_size).map_err(|e| anyhow::anyhow!("base_size: {e}"))?;
    if expected_base != base.len() {
        return Err(Error::Invalid(format!(
            "delta base size mismatch: expected {base_size}, got {}",
            base.len()
        )));
    }

    let (result_size, n) = read_varint(delta)?;
    let delta = delta
        .get(n..)
        .ok_or_else(|| Error::Invalid("delta truncated after result_size".into()))?;
    if result_size > MAX_OBJECT_BYTES {
        return Err(Error::Invalid(format!(
            "delta result too large: {result_size}"
        )));
    }
    let result_len =
        usize::try_from(result_size).map_err(|e| anyhow::anyhow!("result_size: {e}"))?;

    let mut result = vec![0u8; result_len];
    let mut out = result.as_mut_slice();
    let mut i = 0;

    while let Some(&cmd) = delta.get(i) {
        i += 1;
        if cmd & 0x80 != 0 {
            let (mut ofs, mut sz) = (0u32, 0u32);
            macro_rules! read_byte {
                ($mask:expr, $shift:expr, $dst:ident) => {
                    if cmd & $mask != 0 {
                        $dst |= u32::from(*delta.get(i).ok_or_else(|| {
                            Error::Invalid("delta copy instruction truncated".into())
                        })?) << $shift;
                        i += 1;
                    }
                };
            }
            read_byte!(0x01, 0, ofs);
            read_byte!(0x02, 8, ofs);
            read_byte!(0x04, 16, ofs);
            read_byte!(0x08, 24, ofs);
            read_byte!(0x10, 0, sz);
            read_byte!(0x20, 8, sz);
            read_byte!(0x40, 16, sz);
            if sz == 0 {
                sz = 0x10000;
            }
            let ofs =
                usize::try_from(ofs).map_err(|e| anyhow::anyhow!("delta copy offset: {e}"))?;
            let sz = usize::try_from(sz).map_err(|e| anyhow::anyhow!("delta copy size: {e}"))?;
            let end = ofs
                .checked_add(sz)
                .ok_or_else(|| Error::Invalid("delta copy range overflow".into()))?;
            let src = base
                .get(ofs..end)
                .ok_or_else(|| Error::Invalid("delta copy range exceeds base".into()))?;
            out.write_all(src)
                .map_err(|e| Error::Invalid(format!("delta result buffer overflow: {e}")))?;
        } else if cmd == 0 {
            return Err(Error::Invalid("invalid delta command 0x00".into()));
        } else {
            let count = usize::from(cmd);
            let end = i
                .checked_add(count)
                .ok_or_else(|| Error::Invalid("delta insert range overflow".into()))?;
            let src = delta
                .get(i..end)
                .ok_or_else(|| Error::Invalid("delta insert data truncated".into()))?;
            out.write_all(src)
                .map_err(|e| Error::Invalid(format!("delta result buffer overflow: {e}")))?;
            i = end;
        }
    }

    if !out.is_empty() {
        return Err(Error::Invalid(
            "delta instructions produced fewer bytes than promised".into(),
        ));
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{super::write_varint, apply_delta};

    fn encode_delta_varint(v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint(&mut out, v);
        out
    }

    fn make_delta(base: &[u8], instructions: &[u8], result_size: usize) -> Vec<u8> {
        let mut delta = Vec::new();
        delta.extend(encode_delta_varint(u64::try_from(base.len()).unwrap()));
        delta.extend(encode_delta_varint(u64::try_from(result_size).unwrap()));
        delta.extend_from_slice(instructions);
        delta
    }

    #[test]
    fn delta_base_size_mismatch() {
        let base = b"hello";
        let delta = make_delta(b"wrong size base", &[], 0);
        apply_delta(base, &delta).unwrap_err();
    }

    #[test]
    fn delta_result_too_short() {
        let base = b"hello";
        let delta = make_delta(base, &[], 10);
        apply_delta(base, &delta).unwrap_err();
    }

    #[test]
    fn delta_copy_out_of_bounds() {
        let base = b"hi";
        let instructions = [0x91u8, 0x00, 0x0a]; // copy 10 bytes, base only has 2
        let delta = make_delta(base, &instructions, 10);
        apply_delta(base, &delta).unwrap_err();
    }

    #[test]
    fn delta_invalid_command_zero() {
        let base = b"hello";
        let instructions = [0x00u8];
        let delta = make_delta(base, &instructions, 0);
        apply_delta(base, &delta).unwrap_err();
    }

    #[test]
    fn delta_insert_data_truncated() {
        let base = b"hello";
        let instructions = [0x05u8, b'a', b'b', b'c'];
        let delta = make_delta(base, &instructions, 5);
        apply_delta(base, &delta).unwrap_err();
    }

    // Copies are generated both "full" (all offset/size bytes present) and
    // "minimal" (only nonzero bytes, like real packs): both must decode alike.

    #[derive(Clone, Debug)]
    enum Op {
        Copy {
            ofs: usize,
            size: usize,
            minimal: bool,
        },
        Insert(Vec<u8>),
    }

    fn op_strategy(base_len: usize) -> proptest::prelude::BoxedStrategy<Op> {
        use proptest::prelude::*;
        let insert = proptest::collection::vec(any::<u8>(), 1..=127)
            .prop_map(Op::Insert)
            .boxed();
        if base_len == 0 {
            insert
        } else {
            let copy =
                (0..base_len).prop_flat_map(move |ofs| {
                    (1..=(base_len - ofs), any::<bool>())
                        .prop_map(move |(size, minimal)| Op::Copy { ofs, size, minimal })
                });
            prop_oneof![3 => copy, 1 => insert].boxed()
        }
    }

    fn base_and_ops_strategy() -> impl proptest::strategy::Strategy<Value = (Vec<u8>, Vec<Op>)> {
        use proptest::prelude::*;
        proptest::collection::vec(any::<u8>(), 0..64).prop_flat_map(|base| {
            let base_len = base.len();
            (
                Just(base),
                proptest::collection::vec(op_strategy(base_len), 0..8),
            )
        })
    }

    fn encode_copy(ofs: usize, size: usize, minimal: bool, out: &mut Vec<u8>) {
        let ofs_bytes = u32::try_from(ofs).unwrap().to_le_bytes();
        let size_bytes = u32::try_from(size).unwrap().to_le_bytes();
        let mut cmd = 0x80u8;
        let mut payload = Vec::new();
        for (i, &b) in ofs_bytes.iter().enumerate() {
            if !minimal || b != 0 {
                cmd |= 1 << i;
                payload.push(b);
            }
        }
        for (i, &b) in size_bytes.iter().take(3).enumerate() {
            if !minimal || b != 0 {
                cmd |= 1 << (4 + i);
                payload.push(b);
            }
        }
        out.push(cmd);
        out.extend_from_slice(&payload);
    }

    proptest::proptest! {
        #[test]
        fn apply_delta_matches_replayed_ops((base, ops) in base_and_ops_strategy()) {
            let mut expected = Vec::new();
            let mut instructions = Vec::new();
            for op in &ops {
                match op {
                    Op::Copy { ofs, size, minimal } => {
                        expected.extend_from_slice(&base[*ofs..*ofs + *size]);
                        encode_copy(*ofs, *size, *minimal, &mut instructions);
                    }
                    Op::Insert(bytes) => {
                        instructions.push(u8::try_from(bytes.len()).unwrap());
                        instructions.extend_from_slice(bytes);
                        expected.extend_from_slice(bytes);
                    }
                }
            }
            let delta = make_delta(&base, &instructions, expected.len());
            proptest::prop_assert_eq!(apply_delta(&base, &delta).unwrap(), expected);
        }
    }
}
