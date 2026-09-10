//! Collision-detecting SHA-1, accelerated with the CPU's SHA-1 instructions.
//!
//! `sha1c`/`sha1m`/`sha1p` fuse four rounds and expose no intermediate round
//! state, which detection needs — but [`ubc_check`](ubc_check::ubc_check)
//! reads only the message schedule, and the round states are needed only to
//! recompress a candidate once a disturbance vector fires (a few percent of
//! blocks). So `ARMv8` computes the schedule and digest in hardware, and
//! falls back to scalar compression only on those blocks. Without the
//! instructions, scalar compression runs on every block, as upstream does.
#![allow(
    unsafe_code,
    reason = "the SHA-1 instructions are reachable only as core::arch intrinsics"
)]

mod hw;
mod scalar;
mod ubc_check;

use digest::block_buffer::{BlockBuffer, Eager};
use digest::typenum::U64;

const BLOCK_SIZE: usize = 64;
const INITIAL_H: [u32; 5] = [
    0x6745_2301,
    0xEFCD_AB89,
    0x98BA_DCFE,
    0x1032_5476,
    0xC3D2_E1F0,
];

/// A SHA-1 collision attempt: the input is crafted to collide with another.
///
/// Carries the digest git would have produced, matching `safe_hash(false)`:
/// detect in order to refuse, not to compute an alternate id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollisionAttack {
    /// The colliding digest, for diagnostics only — never store it as an id.
    pub digest: [u8; 20],
}

impl core::fmt::Display for CollisionAttack {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "detected SHA-1 collision attack with digest ")?;
        for byte in self.digest {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl core::error::Error for CollisionAttack {}

/// Streaming collision-detecting SHA-1.
///
/// Feed with [`update`](Self::update), finish with
/// [`try_finalize`](Self::try_finalize).
#[derive(Clone)]
pub struct Hasher {
    h: [u32; 5],
    buffer: BlockBuffer<U64, Eager>,
    blocks: u64,
    found_collision: bool,
    /// Whether to take the digest from the CPU's SHA-1 instructions.
    ///
    /// Probed once — the check is not free enough for a per-block loop.
    hardware: bool,
    /// The in-flight block's message schedule and round states, held here
    /// rather than the stack since they are reused every block.
    m1: [u32; 80],
    m2: [u32; 80],
    ihv1: [u32; 5],
    ihv2: [u32; 5],
    state_58: [u32; 5],
    state_65: [u32; 5],
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    /// Create a hasher, using the CPU's SHA-1 instructions where they exist.
    #[must_use]
    pub fn new() -> Self {
        Self {
            h: INITIAL_H,
            buffer: BlockBuffer::default(),
            blocks: 0,
            found_collision: false,
            hardware: hw::available(),
            m1: [0; 80],
            m2: [0; 80],
            ihv1: [0; 5],
            ihv2: [0; 5],
            state_58: [0; 5],
            state_65: [0; 5],
        }
    }

    /// Feed more input.
    #[expect(
        clippy::as_conversions,
        reason = "usize -> u64 is widening on every target this builds for"
    )]
    pub fn update(&mut self, bytes: &[u8]) {
        let Self {
            h,
            buffer,
            blocks,
            found_collision,
            hardware,
            m1,
            m2,
            ihv1,
            ihv2,
            state_58,
            state_65,
        } = self;
        buffer.digest_blocks(bytes, |chunk| {
            *blocks += chunk.len() as u64;
            for block in chunk {
                let block: &[u8; BLOCK_SIZE] = block.as_ref();
                compress_block(
                    *hardware,
                    h,
                    block,
                    m1,
                    m2,
                    ihv1,
                    ihv2,
                    state_58,
                    state_65,
                    found_collision,
                );
            }
        });
    }

    /// Finish, or report that the input is a collision attempt.
    ///
    /// # Errors
    /// Returns [`CollisionAttack`] if a disturbance vector was confirmed.
    #[expect(
        clippy::as_conversions,
        reason = "usize -> u64 is widening on every target this builds for"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "`pos` is a position within one block, so every index below is \
                  bounded by construction: `pos` < 64, `end` <= 128, `used` <= 2"
    )]
    pub fn try_finalize(mut self) -> Result<[u8; 20], CollisionAttack> {
        let pos = self.buffer.get_pos();
        let bit_len = 8 * (pos as u64 + BLOCK_SIZE as u64 * self.blocks);

        // Padded here, not via the buffer's `len64_padding_be`, so the final
        // blocks go through the same collision-checked path as every other
        // block — a crafted message can end anywhere, including mid-block.
        let mut tail = [[0u8; BLOCK_SIZE]; 2];
        let flat = tail.as_flattened_mut();
        flat[..pos].copy_from_slice(&self.buffer.get_data()[..pos]);
        flat[pos] = 0x80;
        let used = if pos + 8 >= BLOCK_SIZE { 2 } else { 1 };
        let end = used * BLOCK_SIZE;
        flat[end - 8..end].copy_from_slice(&bit_len.to_be_bytes());

        for block in &tail[..used] {
            compress_block(
                self.hardware,
                &mut self.h,
                block,
                &mut self.m1,
                &mut self.m2,
                &mut self.ihv1,
                &mut self.ihv2,
                &mut self.state_58,
                &mut self.state_65,
                &mut self.found_collision,
            );
        }

        let mut digest = [0u8; 20];
        for (out, word) in digest.as_chunks_mut::<4>().0.iter_mut().zip(self.h) {
            *out = word.to_be_bytes();
        }

        if self.found_collision {
            return Err(CollisionAttack { digest });
        }
        Ok(digest)
    }
}

impl core::fmt::Debug for Hasher {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Hasher { .. }")
    }
}

/// One block: the digest and message schedule, then the collision check.
#[expect(
    clippy::too_many_arguments,
    reason = "split-borrowed fields of Hasher, over fixed-size algorithm state"
)]
fn compress_block(
    hardware: bool,
    h: &mut [u32; 5],
    block: &[u8; BLOCK_SIZE],
    m1: &mut [u32; 80],
    m2: &mut [u32; 80],
    ihv1: &mut [u32; 5],
    ihv2: &mut [u32; 5],
    state_58: &mut [u32; 5],
    state_65: &mut [u32; 5],
    found_collision: &mut bool,
) {
    *ihv1 = *h;

    let mut block_u32 = [0u32; BLOCK_SIZE / 4];
    for (word, chunk) in block_u32.iter_mut().zip(block.as_chunks::<4>().0) {
        *word = u32::from_be_bytes(*chunk);
    }

    // The whole point: the hardware gives the digest and the schedule, but not
    // the round states, so it leaves `state_58`/`state_65` stale. Without the
    // instructions, the scalar compression produces all of it in one pass.
    let states_are_current = if hardware {
        hw::compress_spill(h, block, m1);
        false
    } else {
        scalar::compression_states(h, &block_u32, m1, state_58, state_65);
        true
    };

    let mask = ubc_check::ubc_check(m1);
    if mask == 0 {
        return;
    }

    if !states_are_current {
        // A vector fired, so the states are needed after all. Paying a scalar
        // compression on the few percent of blocks that reach here, instead of
        // on every block, is what the fast path buys.
        let mut replayed = *ihv1;
        scalar::compression_states(&mut replayed, &block_u32, m1, state_58, state_65);
        debug_assert_eq!(&replayed, h, "hardware and scalar digests diverged");
    }

    let mut ihvtmp = [0u32; 5];
    for dv in &ubc_check::SHA1_DVS {
        if mask & (1 << dv.maskb) == 0 {
            continue;
        }
        for ((out, word), dm) in m2.iter_mut().zip(m1.iter()).zip(dv.dm.iter()) {
            *out = word ^ dm;
        }
        scalar::recompression_step(
            dv.testt,
            ihv2,
            &mut ihvtmp,
            m2,
            match dv.testt {
                ubc_check::Testt::T58 => state_58,
                ubc_check::Testt::T65 => state_65,
            },
        );
        if scalar::xor(&ihvtmp, h) == 0 {
            *found_collision = true;
            break;
        }
    }
}

#[cfg(test)]
mod tests;
