//! The oracle is plain `sha1`, not `sha1-checked`.
//!
//! `sha1-checked` shares code with `ubc_check`/`scalar`, so it can't referee
//! them independently. Plain `sha1` checks the buffering, padding, `hw`, and
//! the branch between paths, since under `safe_hash(false)` the digest is
//! always standard. Detection itself is pinned both ways: published
//! collisions must be caught, random input must never be flagged.

#![expect(
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::format_push_string,
    reason = "test-only conveniences over a fixed-size corpus"
)]

use std::path::Path;

use sha1::Digest as _;

use super::*;

fn xorshift(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, b| {
        out.push_str(&format!("{b:02x}"));
        out
    })
}

/// The standard digest, from an implementation sharing no code with this crate.
fn plain(data: &[u8]) -> [u8; 20] {
    sha1::Sha1::digest(data).into()
}

/// Digest either way: `Ok` means no attack was detected, `Err` carries the
/// digest anyway, since detection refuses rather than altering the hash.
fn ours(data: &[u8]) -> Result<[u8; 20], [u8; 20]> {
    let mut h = Hasher::new();
    h.update(data);
    h.try_finalize().map_err(|e| e.digest)
}

/// Forces the path taken where the SHA-1 instructions are missing.
///
/// Reaches in at the field so the public API needs no test-only knob.
fn scalar_only(data: &[u8]) -> Result<[u8; 20], [u8; 20]> {
    let mut h = Hasher::new();
    h.hardware = false;
    h.update(data);
    h.try_finalize().map_err(|e| e.digest)
}

fn digest_of(result: Result<[u8; 20], [u8; 20]>) -> [u8; 20] {
    result.unwrap_or_else(|digest| digest)
}

/// Without the instructions, the other aarch64 tests would test the scalar
/// path twice and say nothing about the accelerated one.
#[cfg(target_arch = "aarch64")]
#[test]
fn hardware_path_is_the_one_under_test() {
    assert!(
        hw::available(),
        "no SHA-1 instructions on this host; the accelerated path went untested"
    );
}

#[test]
fn known_vectors() {
    assert_eq!(
        hex(&ours(b"").unwrap()),
        "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    );
    assert_eq!(
        hex(&ours(b"hello world").unwrap()),
        "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"
    );
    // `printf 'hello\n' | git hash-object --stdin`, with the loose header, so
    // the anchor is git's own output and not just another Rust hasher.
    let mut h = Hasher::new();
    h.update(b"blob 6\0");
    h.update(b"hello\n");
    assert_eq!(
        hex(&h.try_finalize().unwrap()),
        "ce013625030ba8dba906f756967f9e9ca394464a"
    );
}

#[test]
fn matches_plain_sha1_over_random_inputs() {
    let mut seed = 0x9E37_79B9_7F4A_7C15;
    for case in 0..20_000 {
        // Lengths cluster on the padding edge (55/56/64), where a tail bug
        // hides, and otherwise range over realistic object sizes.
        let len = match case % 4 {
            0 => (xorshift(&mut seed) % 200) as usize,
            1 => (xorshift(&mut seed) % 9000) as usize,
            2 => 55 + (case % 12),
            _ => 64 * (1 + (case % 5)),
        };
        let data: Vec<u8> = (0..len)
            .map(|_| (xorshift(&mut seed) >> 24) as u8)
            .collect();

        let got = ours(&data);
        assert_eq!(
            got.map(|d| hex(&d)).map_err(|d| hex(&d)),
            Ok(hex(&plain(&data))),
            "diverged at len {len} (case {case})"
        );
    }
}

/// Without this, a host with the instructions would never exercise the path its
/// x86 counterpart runs on every block.
#[test]
fn scalar_path_matches_plain_sha1_and_the_hardware_path() {
    let mut seed = 0x5DEE_CE66_D3A7_1B2C;
    for case in 0..20_000 {
        let len = match case % 3 {
            0 => (xorshift(&mut seed) % 200) as usize,
            1 => (xorshift(&mut seed) % 9000) as usize,
            _ => 55 + (case % 12),
        };
        let data: Vec<u8> = (0..len)
            .map(|_| (xorshift(&mut seed) >> 24) as u8)
            .collect();

        assert_eq!(
            scalar_only(&data).map(|d| hex(&d)).map_err(|d| hex(&d)),
            Ok(hex(&plain(&data))),
            "scalar path diverged at len {len} (case {case})"
        );
        // Agreement between the two is what makes the choice of path a pure
        // optimisation rather than a behavioural fork.
        assert_eq!(
            scalar_only(&data),
            ours(&data),
            "paths disagree at len {len} (case {case})"
        );
    }
}

#[test]
fn matches_plain_sha1_across_split_updates() {
    // Buffering is this crate's own, so feeding the same bytes in awkward
    // pieces has to land on the same digest.
    let mut seed = 0x2545_F491_4F6C_DD1D;
    for case in 0..2000 {
        let len = (xorshift(&mut seed) % 4000) as usize;
        let data: Vec<u8> = (0..len)
            .map(|_| (xorshift(&mut seed) >> 24) as u8)
            .collect();

        let mut h = Hasher::new();
        let mut at = 0;
        while at < data.len() {
            let take = ((xorshift(&mut seed) % 100) as usize + 1).min(data.len() - at);
            h.update(&data[at..at + take]);
            at += take;
        }
        assert_eq!(
            h.try_finalize()
                .map(|d| hex(&d))
                .map_err(|e| hex(&e.digest)),
            Ok(hex(&plain(&data))),
            "split updates diverged at len {len} (case {case})"
        );
    }
}

#[test]
fn empty_and_block_boundary_lengths() {
    for len in [0usize, 1, 55, 56, 63, 64, 65, 119, 120, 127, 128, 129] {
        let data = vec![0xAB; len];
        assert_eq!(
            digest_of(ours(&data)),
            plain(&data),
            "diverged at len {len}"
        );
        assert_eq!(
            digest_of(scalar_only(&data)),
            plain(&data),
            "scalar path diverged at len {len}"
        );
    }
}

/// Detection must stay silent on ordinary data — the disturbance-vector mask
/// fires on a few percent of random blocks, so those firings must be dismissed.
#[test]
fn no_false_positives_on_random_input() {
    let mut seed = 0x0DDB_A11C_0FFE_E5EE;
    for case in 0..20_000 {
        let len = (xorshift(&mut seed) % 4000) as usize;
        let data: Vec<u8> = (0..len)
            .map(|_| (xorshift(&mut seed) >> 24) as u8)
            .collect();
        assert!(
            ours(&data).is_ok(),
            "reported a collision on benign input, len {len} (case {case})"
        );
        assert!(
            scalar_only(&data).is_ok(),
            "scalar path reported a collision on benign input, len {len} (case {case})"
        );
    }
}

/// The published collision pairs, on both paths: detection must fire, and
/// `safe_hash(false)` must refuse rather than alter the digest.
#[test]
fn detects_published_collisions() {
    // `env!` bakes in the compile-time directory, which a sandboxed build
    // (Bazel) makes stale; fall back to a workspace-relative path for that case.
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dir = if Path::new(manifest).is_dir() {
        format!("{manifest}/tests/data")
    } else {
        "crates/git/hash/tests/data".to_owned()
    };
    for (a, b) in [
        // The SHAttered PDFs' first 320 bytes: identical after byte 320, so
        // still a collision pair, and still contains the blocks that matter.
        ("shattered-1-prefix.bin", "shattered-2-prefix.bin"),
        ("sha-mbles-1.bin", "sha-mbles-2.bin"),
    ] {
        let da = std::fs::read(format!("{dir}/{a}")).expect("test vector present");
        let db = std::fs::read(format!("{dir}/{b}")).expect("test vector present");
        assert_eq!(plain(&da), plain(&db), "{a}/{b} are not a SHA-1 collision");

        for (name, data) in [(a, &da), (b, &db)] {
            for (path, got) in [("hardware", ours(data)), ("scalar", scalar_only(data))] {
                assert!(
                    got.is_err(),
                    "{name}: collision NOT detected on {path} path"
                );
                assert_eq!(
                    digest_of(got),
                    plain(data),
                    "{name}: {path} path altered the digest"
                );
            }
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[test]
fn hardware_and_scalar_agree_per_block() {
    if !hw::available() {
        return;
    }
    // The fast path rests on the hardware schedule being the standard one,
    // since that is what the collision check is handed.
    let mut seed = 0x0BAD_C0DE_DEAD_BEEF;
    for _ in 0..20_000 {
        let block: [u8; 64] = core::array::from_fn(|_| (xorshift(&mut seed) >> 24) as u8);
        let ihv: [u32; 5] = core::array::from_fn(|_| xorshift(&mut seed) as u32);

        let mut block_u32 = [0u32; 16];
        for (word, chunk) in block_u32.iter_mut().zip(block.as_chunks::<4>().0) {
            *word = u32::from_be_bytes(*chunk);
        }

        let (mut hw_state, mut hw_w) = (ihv, [0u32; 80]);
        hw::compress_spill(&mut hw_state, &block, &mut hw_w);

        let mut sc_state = ihv;
        let (mut sc_w, mut s58, mut s65) = ([0u32; 80], [0u32; 5], [0u32; 5]);
        scalar::compression_states(&mut sc_state, &block_u32, &mut sc_w, &mut s58, &mut s65);

        assert_eq!(hw_state, sc_state, "digest diverged");
        assert_eq!(hw_w, sc_w, "schedule diverged");

        // Spelled out independently of the round macros, so a shared bug in
        // them cannot hide.
        let mut want = [0u32; 80];
        want[..16].copy_from_slice(&block_u32);
        for t in 16..80 {
            want[t] = (want[t - 3] ^ want[t - 8] ^ want[t - 14] ^ want[t - 16]).rotate_left(1);
        }
        assert_eq!(hw_w, want, "schedule is not the standard expansion");
    }
}

#[cfg(target_arch = "aarch64")]
#[test]
fn replay_path_is_actually_exercised() {
    if !hw::available() {
        return;
    }
    // The scalar replay only runs when a vector fires. If that were rare, the
    // tests above would be leaving it dark.
    let mut seed = 0x1234_5678_9ABC_DEF0;
    let (mut fired, total) = (0u32, 20_000);
    for _ in 0..total {
        let block: [u8; 64] = core::array::from_fn(|_| (xorshift(&mut seed) >> 24) as u8);
        let mut state = INITIAL_H;
        let mut w = [0u32; 80];
        hw::compress_spill(&mut state, &block, &mut w);
        if ubc_check::ubc_check(&w) != 0 {
            fired += 1;
        }
    }
    assert!(
        fired > total / 100,
        "only {fired}/{total} blocks fired a vector; the replay path is barely covered"
    );
}
