//! Git's binary delta format: decoding and a simple block-hash encoder.

use std::ops::Range;

use anyhow::{Context, Result, bail, ensure};
use integer_encoding::VarInt;

/// A size in a delta's header (unsigned LEB128) at `*pos`.
fn read_size(data: &[u8], pos: &mut usize) -> Result<usize> {
    let rest = data.get(*pos..).context("truncated delta size")?;
    let (v, n) = u64::decode_var(rest).context("truncated or overlong delta size")?;
    *pos += n;
    usize::try_from(v).context("delta size too large")
}

/// Little-endian operand bytes selected by the low `count` bits of `mask`.
fn read_operand(delta: &[u8], pos: &mut usize, mask: u8, count: usize) -> Result<usize> {
    let mut value = 0usize;
    for i in 0..count {
        if mask & (1 << i) == 0 {
            continue;
        }
        let byte = *delta.get(*pos).context("truncated delta copy")?;
        value |= usize::from(byte) << (8 * i);
        *pos += 1;
    }
    Ok(value)
}

/// The size of the object that `delta` builds.
pub(crate) fn target_size(delta: &[u8]) -> Result<usize> {
    let mut pos = 0;
    read_size(delta, &mut pos)?;
    read_size(delta, &mut pos)
}

pub(crate) fn apply(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
    let mut pos = 0;
    let src_size = read_size(delta, &mut pos)?;
    ensure!(
        src_size == base.len(),
        "delta base size mismatch ({} != {})",
        src_size,
        base.len()
    );
    let dst_size = read_size(delta, &mut pos)?;
    let mut out = Vec::with_capacity(dst_size);
    while let Some(&op) = delta.get(pos) {
        pos += 1;
        if op & 0x80 != 0 {
            let off = read_operand(delta, &mut pos, op, 4)?;
            let mut size = read_operand(delta, &mut pos, op >> 4, 3)?;
            if size == 0 {
                size = 0x10000;
            }
            let src = off
                .checked_add(size)
                .and_then(|end| base.get(off..end))
                .context("delta copy out of range")?;
            // Copies can repeat the base without end: stop at the size
            // that the header gave, which the pack's limit bounds.
            ensure!(
                out.len() + src.len() <= dst_size,
                "delta result size mismatch"
            );
            out.extend_from_slice(src);
        } else if op != 0 {
            let end = pos + usize::from(op);
            let lit = delta.get(pos..end).context("truncated delta insert")?;
            out.extend_from_slice(lit);
            pos = end;
        } else {
            bail!("invalid delta opcode 0");
        }
    }
    ensure!(out.len() == dst_size, "delta result size mismatch");
    Ok(out)
}

const BLOCK: usize = 16;
/// Mixes the second word of a block into its hash.
const MIX: u64 = 0xC2B2_AE3D_27D4_EB4F;
/// Multiplier that spreads a block hash over the table's top bits.
const SPREAD: u64 = 0x9E37_79B9_7F4A_7C15;

/// Encode `target` as a delta against `base`. Returns `None` if the delta
/// would not be meaningfully smaller than `max_size`.
pub(crate) fn encode(base: &[u8], target: &[u8], max_size: usize) -> Option<Vec<u8>> {
    // Copies can only address the first 4GiB of the base.
    if u32::try_from(base.len()).is_err() {
        return None;
    }
    let cap = (target.len() / 4).min(max_size);
    let mut enc = Encoder::new(base.len(), target.len(), max_size, cap)?;

    // Most versions differ from their base in one region: copy the common
    // ends as they are, and search only between them.
    let prefix = worth_copying(common_prefix(base, target));
    let max_suffix = base.len().min(target.len()) - prefix;
    let suffix = worth_copying(common_suffix(base, target, max_suffix));
    enc.copy(0, prefix);
    enc.scan(
        base,
        prefix..base.len() - suffix,
        target,
        prefix..target.len() - suffix,
    )?;
    enc.copy(base.len() - suffix, suffix);
    enc.finish()
}

struct Encoder {
    out: Vec<u8>,
    pending: Vec<u8>,
    /// The size the delta must stay under.
    max_size: usize,
}

/// A delta from copies and literals that the caller already knows, such as
/// from a diff of two trees.
pub(crate) struct Writer {
    enc: Encoder,
    /// A copy not written yet, so that adjacent copies become one.
    run: Option<(usize, usize)>,
}

impl Writer {
    /// A delta that [`Writer::finish`] gives only if it is under
    /// `max_size`, as [`encode`] does.
    pub(crate) fn new(base_len: usize, target_len: usize, max_size: usize) -> Option<Writer> {
        Some(Writer {
            // Deltas from a diff are mostly tiny.
            enc: Encoder::new(base_len, target_len, max_size, 64)?,
            run: None,
        })
    }

    /// Copy `len` bytes of the base at `off`.
    pub(crate) fn copy(&mut self, off: usize, len: usize) {
        match &mut self.run {
            Some((o, l)) if *o + *l == off => *l += len,
            _ => {
                self.flush_copy();
                self.run = Some((off, len));
            }
        }
    }

    pub(crate) fn insert(&mut self, bytes: &[u8]) {
        self.flush_copy();
        self.enc.pending.extend_from_slice(bytes);
    }

    /// Whether the delta is already too large.
    pub(crate) fn full(&self) -> bool {
        self.enc.full()
    }

    pub(crate) fn finish(mut self) -> Option<Vec<u8>> {
        self.flush_copy();
        self.enc.finish()
    }

    fn flush_copy(&mut self) {
        if let Some((off, len)) = self.run.take() {
            self.enc.copy(off, len);
        }
    }
}

impl Encoder {
    /// An encoder with the delta's header written, and room for about
    /// `cap` more bytes.
    fn new(base_len: usize, target_len: usize, max_size: usize, cap: usize) -> Option<Encoder> {
        let mut out = Vec::with_capacity(cap + 16);
        out.extend_from_slice(&u64::try_from(base_len).ok()?.encode_var_vec());
        out.extend_from_slice(&u64::try_from(target_len).ok()?.encode_var_vec());
        Some(Encoder {
            out,
            pending: Vec::new(),
            max_size,
        })
    }

    fn full(&self) -> bool {
        self.out.len() + self.pending.len() >= self.max_size
    }

    /// The delta, if it stays under `max_size`.
    fn finish(mut self) -> Option<Vec<u8>> {
        self.flush_insert();
        (self.out.len() < self.max_size).then_some(self.out)
    }

    fn flush_insert(&mut self) {
        for chunk in self.pending.chunks(127) {
            // A chunk has at most 127 bytes, so the fallback never applies.
            self.out.push(u8::try_from(chunk.len()).unwrap_or(127));
            self.out.extend_from_slice(chunk);
        }
        self.pending.clear();
    }

    fn copy(&mut self, mut off: usize, mut len: usize) {
        self.flush_insert();
        while len > 0 {
            let n = len.min(0x10000);
            let sz = if n == 0x10000 { 0 } else { n };
            // The opcode byte is known only after the operands are chosen.
            let op_at = self.out.len();
            self.out.push(0);
            let mut op = 0x80u8;
            let fields = off
                .to_le_bytes()
                .into_iter()
                .take(4)
                .enumerate()
                .map(|(i, b)| (1u8 << i, b))
                .chain(
                    sz.to_le_bytes()
                        .into_iter()
                        .take(3)
                        .enumerate()
                        .map(|(i, b)| (0x10u8 << i, b)),
                );
            for (bit, b) in fields.filter(|&(_, b)| b != 0) {
                op |= bit;
                self.out.push(b);
            }
            if let Some(slot) = self.out.get_mut(op_at) {
                *slot = op;
            }
            off += n;
            len -= n;
        }
    }

    /// Encode `target[targets]` against the blocks of `base[bases]`, as
    /// copies from anywhere in `base` and literals. Returns `None` once the
    /// output reaches `max_size`.
    fn scan(
        &mut self,
        base: &[u8],
        bases: Range<usize>,
        target: &[u8],
        targets: Range<usize>,
    ) -> Option<()> {
        if bases.len() < BLOCK || targets.len() < BLOCK {
            self.pending.extend_from_slice(target.get(targets)?);
            return Some(());
        }
        // Copies may read anywhere in `base`, but must not cover target
        // bytes past `targets`.
        let target = target.get(..targets.end)?;
        let (table, shift) = index(base, bases);
        self.search(&table, shift, base, target, targets.start)
    }

    /// Scan `target` from `pos` for blocks indexed in `table`.
    fn search(
        &mut self,
        table: &[u32],
        shift: u32,
        base: &[u8],
        target: &[u8],
        mut pos: usize,
    ) -> Option<()> {
        while pos + BLOCK <= target.len() {
            let hit = block_hash(target, pos)
                .and_then(|h| candidate(table, h, shift))
                .and_then(|cand| {
                    Some((
                        cand,
                        extend_match(base, target, cand, pos, self.pending.len())?,
                    ))
                });
            if let Some((cand, (back, fwd))) = hit {
                self.pending.truncate(self.pending.len() - back);
                self.copy(cand - back, back + fwd);
                pos += fwd;
            } else {
                let &byte = target.get(pos)?;
                self.pending.push(byte);
                pos += 1;
            }
            if self.full() {
                return None;
            }
        }
        self.pending.extend_from_slice(target.get(pos..)?);
        Some(())
    }
}

/// The hash of the block at `b[i..]`. Hashing each position from its own
/// bytes, as LZ4 and zstd do, costs about one step of a rolling hash at
/// this block size, and indexing the base needs no per-byte chain.
fn block_hash(b: &[u8], i: usize) -> Option<u64> {
    Some(word(b, i)? ^ word(b, i + 8)?.rotate_left(31).wrapping_mul(MIX))
}

/// The slot of a block hash in a table indexed with `shift`.
fn slot_of(hash: u64, shift: u32) -> usize {
    usize::try_from(hash.wrapping_mul(SPREAD) >> shift).unwrap_or(0)
}

/// Index the non-overlapping blocks of `base[range]` into `table` (offset + 1;
/// 0 is empty). The first occurrence wins. Returns the table and the shift
/// for `slot_of`.
fn index(base: &[u8], range: Range<usize>) -> (Vec<u32>, u32) {
    let blocks = range.len() / BLOCK;
    let bits = (blocks * 2).next_power_of_two().max(16).trailing_zeros();
    let size = 1usize << bits;
    let mut table = vec![0u32; size];
    let shift = 64 - bits;
    let mut off = range.start;
    while off + BLOCK <= range.end {
        let (Some(h), Ok(stored)) = (block_hash(base, off), u32::try_from(off + 1)) else {
            break;
        };
        if let Some(slot) = table.get_mut(slot_of(h, shift))
            && *slot == 0
        {
            *slot = stored;
        }
        off += BLOCK;
    }
    (table, shift)
}

/// The base offset of the block indexed under `hash`, if any.
fn candidate(table: &[u32], hash: u64, shift: u32) -> Option<usize> {
    let off = table.get(slot_of(hash, shift))?.checked_sub(1)?;
    usize::try_from(off).ok()
}

/// For a hash hit at base offset `cand` and `target[pos..]`: the bytes
/// matched backwards (at most `max_back`) and forwards, or `None` on a hash
/// collision.
fn extend_match(
    base: &[u8],
    target: &[u8],
    cand: usize,
    pos: usize,
    max_back: usize,
) -> Option<(usize, usize)> {
    let (b, t) = (base.get(cand..)?, target.get(pos..)?);
    if b.get(..BLOCK)? != t.get(..BLOCK)? {
        return None;
    }
    let fwd = BLOCK + common_prefix(b.get(BLOCK..)?, t.get(BLOCK..)?);
    let back = common_suffix(base.get(..cand)?, target.get(..pos)?, max_back);
    Some((back, fwd))
}

/// A common end shorter than a block is cheaper as literals.
fn worth_copying(len: usize) -> usize {
    if len < BLOCK { 0 } else { len }
}

/// The 8 bytes of `b` at `i`, little-endian.
fn word(b: &[u8], i: usize) -> Option<u64> {
    Some(u64::from_le_bytes(b.get(i..i + 8)?.try_into().ok()?))
}

/// Equal bytes in a XOR of two words, given its count of zero bits.
fn bytes_of(zero_bits: u32) -> usize {
    usize::try_from(zero_bits / 8).unwrap_or(0)
}

/// The length of the common prefix of `a` and `b`.
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let mut i = 0;
    while let (Some(x), Some(y)) = (word(a, i), word(b, i)) {
        if x != y {
            return i + bytes_of((x ^ y).trailing_zeros());
        }
        i += 8;
    }
    let rest = a.get(i..).zip(b.get(i..));
    i + rest.map_or(0, |(a, b)| {
        a.iter().zip(b).take_while(|(x, y)| x == y).count()
    })
}

/// The length of the common suffix of `a` and `b`, at most `max`.
fn common_suffix(a: &[u8], b: &[u8], max: usize) -> usize {
    let limit = a.len().min(b.len()).min(max);
    let mut len = 0;
    while len + 8 <= limit
        && let (Some(x), Some(y)) = (word(a, a.len() - len - 8), word(b, b.len() - len - 8))
    {
        if x != y {
            return len + bytes_of((x ^ y).leading_zeros());
        }
        len += 8;
    }
    let rest = a.get(..a.len() - len).zip(b.get(..b.len() - len));
    len + rest.map_or(0, |(a, b)| {
        a.iter()
            .rev()
            .zip(b.iter().rev())
            .take(limit - len)
            .take_while(|(x, y)| x == y)
            .count()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(base: &[u8], target: &[u8]) {
        let d = encode(base, target, usize::MAX).unwrap();
        assert_eq!(apply(base, &d).unwrap(), target);
    }

    #[test]
    fn bad_headers_are_errors() {
        assert!(apply(b"", &[0x80]).is_err(), "truncated size");
        assert!(apply(b"", &[0xff; 11]).is_err(), "overlong size");
        // Base and result size 1, then two copies of the base.
        assert!(apply(b"x", &[1, 1, 0x90, 1, 0x90, 1]).is_err(), "too long");
    }

    #[test]
    fn roundtrips() {
        roundtrip(b"", b"");
        roundtrip(b"hello", b"world");
        let base: Vec<u8> = (0..200_000u32)
            .map(|i| u8::try_from(i * 7 % 251).unwrap())
            .collect();
        let mut target = base.clone();
        target.splice(1000..1000, b"inserted text here".iter().copied());
        target.drain(50_000..50_100);
        target.extend_from_slice(b"tail");
        roundtrip(&base, &target);
        let d = encode(&base, &target, usize::MAX).unwrap();
        assert!(d.len() < 200, "delta too large: {}", d.len());
        roundtrip(&target, &base);
        roundtrip(
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            b"bbbbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
    }

    /// At a position (per mille of the length), delete some bytes and insert
    /// others.
    type Edit = (usize, usize, Vec<u8>);

    /// A base from a small alphabet (so blocks repeat) and edits to it.
    fn any_edits() -> impl proptest::strategy::Strategy<Value = (Vec<u8>, Vec<Edit>)> {
        use proptest::prelude::*;
        let bytes =
            |n| proptest::collection::vec(prop_oneof![Just(b'a'), Just(b'b'), any::<u8>()], n);
        (
            bytes(0..4000),
            proptest::collection::vec((0..1000usize, 0..300usize, bytes(0..64)), 0..6),
        )
    }

    proptest::proptest! {
        #[test]
        fn edits_roundtrip((base, edits) in any_edits()) {
            let mut target = base.clone();
            for (at, del, ins) in edits {
                let at = at * target.len() / 1000;
                let end = (at + del).min(target.len());
                target.splice(at..end, ins);
            }
            let d = encode(&base, &target, usize::MAX).unwrap();
            proptest::prop_assert_eq!(apply(&base, &d).unwrap(), target.clone());
            if let Some(d) = encode(&base, &target, target.len() / 2) {
                proptest::prop_assert!(d.len() < target.len() / 2);
                proptest::prop_assert_eq!(apply(&base, &d).unwrap(), target);
            }
        }
    }
}
