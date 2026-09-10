//! Hardware SHA-1 compression that also yields the message schedule.
//!
//! The collision check reads only the message schedule `w[80]`, never the
//! round states — those are needed just to recompress a candidate once a
//! disturbance vector actually fires. The CPU's SHA-1 instructions cannot
//! expose round states, which is why the scalar compression exists at all, but
//! they *do* compute the schedule: on `ARMv8` `sha1su0`/`sha1su1` produce exactly
//! the four words the standard expansion defines. Storing each result gives the
//! checker its input for the price of one vector store per four words, so the
//! digest itself can come from the hardware.

#[cfg(target_arch = "aarch64")]
mod neon {
    use core::arch::aarch64::{
        vaddq_u32, vdupq_n_u32, vgetq_lane_u32, vld1q_u8, vld1q_u32, vreinterpretq_u32_u8,
        vrev32q_u8, vsha1cq_u32, vsha1h_u32, vsha1mq_u32, vsha1pq_u32, vsha1su0q_u32,
        vsha1su1q_u32, vst1q_u32,
    };

    /// Cached because `is_aarch64_feature_detected!` is not free enough to sit
    /// in a per-block loop, and the answer cannot change while we run.
    pub(crate) fn available() -> bool {
        use core::sync::atomic::{AtomicU8, Ordering};

        static CACHE: AtomicU8 = AtomicU8::new(u8::MAX);
        match CACHE.load(Ordering::Relaxed) {
            u8::MAX => {
                let ok = std::arch::is_aarch64_feature_detected!("sha2");
                CACHE.store(u8::from(ok), Ordering::Relaxed);
                ok
            }
            v => v != 0,
        }
    }

    /// Compress `block` into `state`, writing the message schedule to `w`.
    ///
    /// # Panics
    /// Callers must have checked [`available`] first.
    pub(crate) fn compress_spill(state: &mut [u32; 5], block: &[u8; 64], w: &mut [u32; 80]) {
        debug_assert!(available());
        // SAFETY: `available()` reported the `sha2` target feature present.
        unsafe { compress_spill_inner(state, block, w) }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one unrolled intrinsic sequence; splitting it would break the round pairing"
    )]
    #[target_feature(enable = "sha2")]
    #[expect(
        unsafe_op_in_unsafe_fn,
        reason = "the whole body is one intrinsic sequence; per-op blocks would bury it"
    )]
    unsafe fn compress_spill_inner(state: &mut [u32; 5], block: &[u8; 64], w: &mut [u32; 80]) {
        const K: [u32; 4] = [0x5A82_7999, 0x6ED9_EBA1, 0x8F1B_BCDC, 0xCA62_C1D6];

        let mut abcd = vld1q_u32(state.as_ptr());
        let mut e0 = state[4];
        let [k0, k1, k2, k3] = K.map(|k| vdupq_n_u32(k));
        let (mut e1, mut tmp0, mut tmp1);

        let abcd_cpy = abcd;
        let e0_cpy = e0;
        let wp = w.as_mut_ptr();

        let [mut msg0, mut msg1, mut msg2, mut msg3] = [0, 1, 2, 3].map(|i| {
            let p = block.as_ptr().add(16 * i);
            vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(p)))
        });
        vst1q_u32(wp, msg0);
        vst1q_u32(wp.add(4), msg1);
        vst1q_u32(wp.add(8), msg2);
        vst1q_u32(wp.add(12), msg3);

        tmp0 = vaddq_u32(msg0, k0);
        tmp1 = vaddq_u32(msg1, k0);

        // Rounds 0-3
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1cq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg2, k0);
        msg0 = vsha1su0q_u32(msg0, msg1, msg2);

        // Rounds 4-7
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1cq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg3, k0);
        msg0 = vsha1su1q_u32(msg0, msg3);
        vst1q_u32(wp.add(16), msg0);
        msg1 = vsha1su0q_u32(msg1, msg2, msg3);

        // Rounds 8-11
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1cq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg0, k0);
        msg1 = vsha1su1q_u32(msg1, msg0);
        vst1q_u32(wp.add(20), msg1);
        msg2 = vsha1su0q_u32(msg2, msg3, msg0);

        // Rounds 12-15
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1cq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg1, k1);
        msg2 = vsha1su1q_u32(msg2, msg1);
        vst1q_u32(wp.add(24), msg2);
        msg3 = vsha1su0q_u32(msg3, msg0, msg1);

        // Rounds 16-19
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1cq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg2, k1);
        msg3 = vsha1su1q_u32(msg3, msg2);
        vst1q_u32(wp.add(28), msg3);
        msg0 = vsha1su0q_u32(msg0, msg1, msg2);

        // Rounds 20-23
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg3, k1);
        msg0 = vsha1su1q_u32(msg0, msg3);
        vst1q_u32(wp.add(32), msg0);
        msg1 = vsha1su0q_u32(msg1, msg2, msg3);

        // Rounds 24-27
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg0, k1);
        msg1 = vsha1su1q_u32(msg1, msg0);
        vst1q_u32(wp.add(36), msg1);
        msg2 = vsha1su0q_u32(msg2, msg3, msg0);

        // Rounds 28-31
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg1, k1);
        msg2 = vsha1su1q_u32(msg2, msg1);
        vst1q_u32(wp.add(40), msg2);
        msg3 = vsha1su0q_u32(msg3, msg0, msg1);

        // Rounds 32-35
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg2, k2);
        msg3 = vsha1su1q_u32(msg3, msg2);
        vst1q_u32(wp.add(44), msg3);
        msg0 = vsha1su0q_u32(msg0, msg1, msg2);

        // Rounds 36-39
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg3, k2);
        msg0 = vsha1su1q_u32(msg0, msg3);
        vst1q_u32(wp.add(48), msg0);
        msg1 = vsha1su0q_u32(msg1, msg2, msg3);

        // Rounds 40-43
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1mq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg0, k2);
        msg1 = vsha1su1q_u32(msg1, msg0);
        vst1q_u32(wp.add(52), msg1);
        msg2 = vsha1su0q_u32(msg2, msg3, msg0);

        // Rounds 44-47
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1mq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg1, k2);
        msg2 = vsha1su1q_u32(msg2, msg1);
        vst1q_u32(wp.add(56), msg2);
        msg3 = vsha1su0q_u32(msg3, msg0, msg1);

        // Rounds 48-51
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1mq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg2, k2);
        msg3 = vsha1su1q_u32(msg3, msg2);
        vst1q_u32(wp.add(60), msg3);
        msg0 = vsha1su0q_u32(msg0, msg1, msg2);

        // Rounds 52-55
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1mq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg3, k3);
        msg0 = vsha1su1q_u32(msg0, msg3);
        vst1q_u32(wp.add(64), msg0);
        msg1 = vsha1su0q_u32(msg1, msg2, msg3);

        // Rounds 56-59
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1mq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg0, k3);
        msg1 = vsha1su1q_u32(msg1, msg0);
        vst1q_u32(wp.add(68), msg1);
        msg2 = vsha1su0q_u32(msg2, msg3, msg0);

        // Rounds 60-63
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg1, k3);
        msg2 = vsha1su1q_u32(msg2, msg1);
        vst1q_u32(wp.add(72), msg2);
        msg3 = vsha1su0q_u32(msg3, msg0, msg1);

        // Rounds 64-67
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e0, tmp0);
        tmp0 = vaddq_u32(msg2, k3);
        msg3 = vsha1su1q_u32(msg3, msg2);
        vst1q_u32(wp.add(76), msg3);

        // Rounds 68-71
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e1, tmp1);
        tmp1 = vaddq_u32(msg3, k3);

        // Rounds 72-75
        e1 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e0, tmp0);

        // Rounds 76-79
        e0 = vsha1h_u32(vgetq_lane_u32(abcd, 0));
        abcd = vsha1pq_u32(abcd, e1, tmp1);

        abcd = vaddq_u32(abcd_cpy, abcd);
        e0 = e0.wrapping_add(e0_cpy);

        vst1q_u32(state.as_mut_ptr(), abcd);
        state[4] = e0;
    }
}

#[cfg(not(target_arch = "aarch64"))]
mod unsupported {
    pub(crate) fn available() -> bool {
        false
    }

    #[expect(
        clippy::unreachable,
        reason = "`available` is `false` on this target, so nothing can reach this"
    )]
    pub(crate) fn compress_spill(_: &mut [u32; 5], _: &[u8; 64], _: &mut [u32; 80]) {
        unreachable!("callers gate on available()")
    }
}

#[cfg(target_arch = "aarch64")]
pub(crate) use neon::{available, compress_spill};
#[cfg(not(target_arch = "aarch64"))]
pub(crate) use unsupported::{available, compress_spill};
