use enroute_git_core::Error;

use crate::format::MAX_OBJECT_BYTES;

use super::{MAX_COPY, MAX_INSERT, write_varint};

/// Bytes per indexed base block, matching git's `diff-delta.c` — below this,
/// a match costs more in copy-instruction overhead than it saves.
const BLOCK: usize = 16;

/// Cap on how far a hash chain is walked per position — without it, input
/// whose blocks all collide degrades to quadratic.
const MAX_CANDIDATES: usize = 64;

/// Odd multiplier, which is what makes removing the outgoing byte in
/// [`roll`] exact under wrapping arithmetic.
const MULT: u32 = 0x0100_0193;

/// `MULT^(BLOCK-1)`, the weight of the byte leaving the rolling window.
const OUTGOING: u32 = {
    let mut pow = 1u32;
    let mut i = 1;
    while i < BLOCK {
        pow = pow.wrapping_mul(MULT);
        i += 1;
    }
    pow
};

/// Encode `target` as a git delta against `base`, the inverse of
/// [`apply_delta`](super::apply_delta).
///
/// `gix-pack` ships no encoder, so this follows the module docs' format —
/// always well-formed, even when larger than `target` itself.
///
/// # Errors
/// Returns [`Error::Invalid`] if either side exceeds [`MAX_OBJECT_BYTES`],
/// which is also the largest result `apply_delta` will reconstruct.
pub fn encode_delta(base: &[u8], target: &[u8]) -> Result<Vec<u8>, Error> {
    let base_len = u64::try_from(base.len()).unwrap_or(u64::MAX);
    let target_len = u64::try_from(target.len()).unwrap_or(u64::MAX);
    if base_len > MAX_OBJECT_BYTES || target_len > MAX_OBJECT_BYTES {
        return Err(Error::Invalid(format!(
            "delta inputs exceed {MAX_OBJECT_BYTES} bytes: base {base_len}, target {target_len}"
        )));
    }

    let mut out = Vec::new();
    write_varint(&mut out, base_len);
    write_varint(&mut out, target_len);

    let index = BlockIndex::build(base);
    // Start of the literal run not yet emitted; a match may extend backwards
    // into it, which is why it is tracked separately from `pos`.
    let mut pending = 0usize;
    let mut pos = 0usize;
    let mut hash = target.get(..BLOCK).map_or(0, block_hash);

    while pos < target.len() {
        let found = if pos + BLOCK <= target.len() {
            index.best_match(base, target, pos, pending, hash)
        } else {
            None
        };

        if let Some(m) = found {
            let literal = target
                .get(pending..m.start)
                .ok_or_else(|| Error::Invalid("delta literal range out of bounds".into()))?;
            push_literal(&mut out, literal)?;
            push_copy(&mut out, m.base_offset, m.len)?;
            pos = m.start + m.len;
            pending = pos;
            hash = target.get(pos..pos + BLOCK).map_or(0, block_hash);
            continue;
        }

        if let (Some(&outgoing), Some(&incoming)) = (target.get(pos), target.get(pos + BLOCK)) {
            hash = roll(hash, outgoing, incoming);
        }
        pos += 1;
    }

    let literal = target
        .get(pending..)
        .ok_or_else(|| Error::Invalid("delta literal tail out of bounds".into()))?;
    push_literal(&mut out, literal)?;
    Ok(out)
}

/// A run of `target` that reproduces bytes already present in `base`.
struct Match {
    start: usize,
    base_offset: usize,
    len: usize,
}

/// Chained hash table over `base`, keyed on the hash of each aligned
/// [`BLOCK`]-byte window.
struct BlockIndex {
    /// Bucket heads, as block numbers; [`NONE`] where the bucket is empty.
    head: Vec<u32>,
    /// Next block number in each bucket's chain.
    next: Vec<u32>,
    /// `head.len() - 1`, so bucket selection is a mask rather than a modulo.
    mask: usize,
}

/// Chain terminator; no real block number can collide with it, since a block
/// count is bounded by [`MAX_OBJECT_BYTES`] / [`BLOCK`].
const NONE: u32 = u32::MAX;

impl BlockIndex {
    fn build(base: &[u8]) -> Self {
        let blocks = base.len() / BLOCK;
        if blocks == 0 {
            return Self {
                head: Vec::new(),
                next: Vec::new(),
                mask: 0,
            };
        }
        let size = blocks.next_power_of_two();
        let mut head = vec![NONE; size];
        let mut next = vec![NONE; blocks];
        // Only aligned windows are indexed, as git does: a match found from any
        // of them is then extended in both directions, so nothing is lost.
        for (number, window) in base.as_chunks::<BLOCK>().0.iter().enumerate() {
            let Ok(block) = u32::try_from(number) else {
                break;
            };
            let slot = bucket(block_hash(window), size - 1);
            if let (Some(chain), Some(bucket_head)) = (next.get_mut(number), head.get_mut(slot)) {
                *chain = *bucket_head;
                *bucket_head = block;
            }
        }
        Self {
            head,
            next,
            mask: size - 1,
        }
    }

    /// Longest run at or just before `pos` that `base` can supply, if it
    /// reaches [`BLOCK`] — backward extension stops at `pending`, already emitted.
    fn best_match(
        &self,
        base: &[u8],
        target: &[u8],
        pos: usize,
        pending: usize,
        hash: u32,
    ) -> Option<Match> {
        let ahead = target.get(pos..)?;
        let behind = target.get(pending..pos)?;
        let mut candidate = *self.head.get(bucket(hash, self.mask))?;
        let mut best: Option<Match> = None;

        for _ in 0..MAX_CANDIDATES {
            let block = usize::try_from(candidate).ok()?;
            let offset = block.checked_mul(BLOCK)?;

            // Extending backwards lets a match absorb literals already scanned past.
            let forward = base
                .get(offset..)
                .map_or(0, |from| common_prefix(from, ahead));
            let backward = base
                .get(..offset)
                .map_or(0, |upto| common_suffix(upto, behind));
            let len = forward + backward;

            if forward > 0 && len >= BLOCK && best.as_ref().is_none_or(|b| len > b.len) {
                best = Some(Match {
                    start: pos - backward,
                    base_offset: offset - backward,
                    len,
                });
            }

            candidate = *self.next.get(block)?;
            if candidate == NONE {
                break;
            }
        }
        best
    }
}

/// Bucket for `hash`, mixing the high bits down first: the low bits of a
/// wrapping polynomial hash depend only on the last few bytes of the window.
fn bucket(hash: u32, mask: usize) -> usize {
    let mixed = hash ^ (hash >> 13);
    usize::try_from(mixed).unwrap_or(0) & mask
}

/// Polynomial hash of one window, matching what [`roll`] maintains.
fn block_hash(window: &[u8]) -> u32 {
    window.iter().fold(0u32, |acc, &byte| {
        acc.wrapping_mul(MULT).wrapping_add(u32::from(byte))
    })
}

/// Advance a window hash by one byte, dropping `outgoing` and taking `incoming`.
fn roll(hash: u32, outgoing: u8, incoming: u8) -> u32 {
    hash.wrapping_sub(u32::from(outgoing).wrapping_mul(OUTGOING))
        .wrapping_mul(MULT)
        .wrapping_add(u32::from(incoming))
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn common_suffix(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .rev()
        .zip(b.iter().rev())
        .take_while(|(x, y)| x == y)
        .count()
}

/// Emit literal bytes, split across as many insert instructions as the
/// [`MAX_INSERT`] length field requires.
fn push_literal(out: &mut Vec<u8>, data: &[u8]) -> Result<(), Error> {
    for chunk in data.chunks(MAX_INSERT) {
        let len = u8::try_from(chunk.len())
            .map_err(|e| Error::Invalid(format!("delta insert length: {e}")))?;
        out.push(len);
        out.extend_from_slice(chunk);
    }
    Ok(())
}

/// Emit a copy, split across as many instructions as [`MAX_COPY`] requires.
fn push_copy(out: &mut Vec<u8>, base_offset: usize, len: usize) -> Result<(), Error> {
    let mut done = 0usize;
    while done < len {
        let take = (len - done).min(MAX_COPY);
        let offset = u32::try_from(base_offset + done)
            .map_err(|e| Error::Invalid(format!("delta copy offset: {e}")))?;
        let size =
            u32::try_from(take).map_err(|e| Error::Invalid(format!("delta copy size: {e}")))?;
        push_copy_instruction(out, offset, size);
        done += take;
    }
    Ok(())
}

/// Emit one copy instruction, omitting zero bytes as real packs do — `size`
/// is never zero here, so never mistaken for the `0x10000` encoding.
fn push_copy_instruction(out: &mut Vec<u8>, offset: u32, size: u32) {
    let mut cmd = 0x80u8;
    let mut payload = [0u8; 7];
    let mut used = 0usize;
    for (i, byte) in offset.to_le_bytes().into_iter().enumerate() {
        if byte != 0 {
            cmd |= 1u8 << i;
            if let Some(slot) = payload.get_mut(used) {
                *slot = byte;
                used += 1;
            }
        }
    }
    for (i, byte) in size.to_le_bytes().into_iter().take(3).enumerate() {
        if byte != 0 {
            cmd |= 1u8 << (4 + i);
            if let Some(slot) = payload.get_mut(used) {
                *slot = byte;
                used += 1;
            }
        }
    }
    out.push(cmd);
    out.extend_from_slice(payload.get(..used).unwrap_or_default());
}

#[cfg(test)]
mod tests {
    use super::{BLOCK, encode_delta};
    use crate::delta::apply_delta;

    /// The decoder is the oracle: whatever the encoder emits must reconstruct
    /// the target exactly.
    fn round_trip(base: &[u8], target: &[u8]) -> Vec<u8> {
        let delta = encode_delta(base, target).unwrap();
        assert_eq!(apply_delta(base, &delta).unwrap(), target);
        delta
    }

    #[test]
    fn empty_inputs() {
        round_trip(b"", b"");
        round_trip(b"", b"hello");
        round_trip(b"hello", b"");
    }

    #[test]
    fn identical_content_costs_almost_nothing() {
        let content = b"the quick brown fox jumps over the lazy dog".repeat(64);
        let delta = round_trip(&content, &content);
        // One copy instruction plus the two-varint header, versus 2,752 bytes.
        assert!(
            delta.len() < 16,
            "expected a single copy, got {}",
            delta.len()
        );
    }

    #[test]
    fn shared_prefix_and_suffix_are_copied() {
        let base = b"aaaaaaaaaaaaaaaaaaaaaaaaMIDDLEzzzzzzzzzzzzzzzzzzzzzzzz";
        let target = b"aaaaaaaaaaaaaaaaaaaaaaaaCHANGEDzzzzzzzzzzzzzzzzzzzzzzzz";
        let delta = round_trip(base, target);
        assert!(delta.len() < target.len(), "delta {} bytes", delta.len());
    }

    #[test]
    fn literal_run_exceeds_one_insert_instruction() {
        // No shared content, so the whole target must go out as inserts, which
        // have to be split at 127 bytes each.
        let base = b"x".repeat(BLOCK);
        let target: Vec<u8> = (0..1000u32)
            .map(|i| u8::try_from(i.wrapping_mul(37) % 251).unwrap_or(0))
            .collect();
        round_trip(&base, &target);
    }

    #[test]
    fn copy_needs_multiple_size_bytes() {
        // Longer than 0xffff, so the third size byte is exercised, and longer
        // than 0x10000 so it cannot be confused with the zero-size encoding.
        let base: Vec<u8> = (0..300_000u32)
            .map(|i| u8::try_from(i.wrapping_mul(2_654_435_761) >> 24).unwrap_or(0))
            .collect();
        let mut target = base.clone();
        target.extend_from_slice(b"tail");
        let delta = round_trip(&base, &target);
        assert!(
            delta.len() < 64,
            "expected a long copy, got {}",
            delta.len()
        );
    }

    #[test]
    fn repeated_blocks_do_not_blow_up() {
        // Every block hashes identically; MAX_CANDIDATES is what keeps this
        // from going quadratic.
        let base = b"z".repeat(200_000);
        let target = b"z".repeat(200_000);
        round_trip(&base, &target);
    }

    proptest::proptest! {
        #[test]
        fn round_trips_for_arbitrary_pairs(
            base in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512),
            target in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512),
        ) {
            let delta = encode_delta(&base, &target).unwrap();
            proptest::prop_assert_eq!(apply_delta(&base, &delta).unwrap(), target);
        }
    }

    proptest::proptest! {
        #[test]
        fn round_trips_when_target_derives_from_base(
            base in proptest::collection::vec(proptest::prelude::any::<u8>(), 32..512),
            cut in 0usize..32,
            insert in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
        ) {
            // The interesting case: a target that really is an edit of the base,
            // so copies and inserts interleave.
            let split = cut.min(base.len());
            let mut target = base[..split].to_vec();
            target.extend_from_slice(&insert);
            target.extend_from_slice(&base[split..]);
            let delta = encode_delta(&base, &target).unwrap();
            proptest::prop_assert_eq!(apply_delta(&base, &delta).unwrap(), target);
        }
    }
}
