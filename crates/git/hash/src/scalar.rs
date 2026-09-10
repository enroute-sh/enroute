// Third-party code. Copied from the `sha1-checked` crate, itself a translation
// of Marc Stevens and Dan Shumow's `sha1collisiondetection`. `sha1-checked` is
// offered as "MIT OR Apache-2.0" and is taken here under MIT; the upstream C is
// MIT. Their permission notices are word for word the same, so both copyright
// notices are carried against the single copy below, condensed to their year
// and holders; upstream's unabridged notice is at
// https://github.com/cr-marcstevens/sha1collisiondetection/blob/master/LICENSE.txt
//
// SPDX-License-Identifier: MIT
//
// Copyright (c) 2017 Marc Stevens (CWI) and Dan Shumow (Microsoft Research)
// Copyright (c) 2024 The RustCrypto Project Developers
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

//! The scalar compression, and the recompression that judges a candidate.
//!
//! Copied from `sha1-checked` 0.10.0, which translates
//! [sha1.c](https://github.com/cr-marcstevens/sha1collisiondetection/blob/master/lib/sha1.c);
//! see the licence preamble above.
//!
//! Copied rather than rewritten on purpose: this is the code that decides
//! whether a collision attempt is real, and a tidier reimplementation would be
//! new unreviewed cryptography in exactly the place that can least afford it.
//!
//! Trimmed to what this crate needs — the round states for a candidate block,
//! and the recompression that checks it — but otherwise unedited, beyond what
//! edition 2024 and the workspace lints required.

#![allow(
    clippy::indexing_slicing,
    clippy::as_conversions,
    clippy::cast_possible_truncation,
    clippy::many_single_char_names,
    clippy::too_many_arguments,
    clippy::unreadable_literal,
    clippy::excessive_nesting,
    clippy::doc_markdown,
    clippy::wildcard_imports,
    clippy::similar_names,
    clippy::module_name_repetitions,
    clippy::cognitive_complexity,
    clippy::inline_always,
    clippy::too_many_lines,
    missing_docs,
    reason = "verbatim third-party code: held to its upstream shape so the copy stays diffable, not to our house style"
)]

use crate::ubc_check::Testt;

const K: [u32; 4] = [0x5A827999, 0x6ED9EBA1, 0x8F1BBCDC, 0xCA62C1D6];

#[inline(always)]
fn mix(w: &mut [u32; 80], t: usize) -> u32 {
    (w[t - 3] ^ w[t - 8] ^ w[t - 14] ^ w[t - 16]).rotate_left(1)
}

#[inline(always)]
fn f1(b: u32, c: u32, d: u32) -> u32 {
    d ^ b & (c ^ d)
}

#[inline(always)]
fn f2(b: u32, c: u32, d: u32) -> u32 {
    b ^ c ^ d
}

#[inline(always)]
fn f3(b: u32, c: u32, d: u32) -> u32 {
    (b & c).wrapping_add(d & (b ^ c))
}

#[inline(always)]
fn f4(b: u32, c: u32, d: u32) -> u32 {
    b ^ c ^ d
}

#[inline(always)]
fn round3_step(a: u32, b: &mut u32, c: u32, d: u32, e: &mut u32, mt: u32) {
    *e = e.wrapping_add(
        a.rotate_left(5)
            .wrapping_add(f3(*b, c, d))
            .wrapping_add(K[2])
            .wrapping_add(mt),
    );
    *b = b.rotate_left(30);
}

#[inline(always)]
fn round4_step(a: u32, b: &mut u32, c: u32, d: u32, e: &mut u32, mt: u32) {
    *e = e.wrapping_add(
        a.rotate_left(5)
            .wrapping_add(f4(*b, c, d))
            .wrapping_add(K[3])
            .wrapping_add(mt),
    );
    *b = b.rotate_left(30);
}

#[inline(always)]
fn round1_step_bw(a: u32, b: &mut u32, c: u32, d: u32, e: &mut u32, mt: u32) {
    *b = b.rotate_right(30);
    *e = e.wrapping_sub(
        a.rotate_left(5)
            .wrapping_add(f1(*b, c, d))
            .wrapping_add(K[0])
            .wrapping_add(mt),
    );
}

#[inline(always)]
fn round2_step_bw(a: u32, b: &mut u32, c: u32, d: u32, e: &mut u32, mt: u32) {
    *b = b.rotate_right(30);
    *e = e.wrapping_sub(
        a.rotate_left(5)
            .wrapping_add(f2(*b, c, d))
            .wrapping_add(K[1])
            .wrapping_add(mt),
    );
}

#[inline(always)]
fn round3_step_bw(a: u32, b: &mut u32, c: u32, d: u32, e: &mut u32, mt: u32) {
    *b = b.rotate_right(30);
    *e = e.wrapping_sub(
        a.rotate_left(5)
            .wrapping_add(f3(*b, c, d))
            .wrapping_add(K[2])
            .wrapping_add(mt),
    );
}

#[inline(always)]
fn round4_step_bw(a: u32, b: &mut u32, c: u32, d: u32, e: &mut u32, mt: u32) {
    *b = b.rotate_right(30);
    *e = e.wrapping_sub(
        a.rotate_left(5)
            .wrapping_add(f4(*b, c, d))
            .wrapping_add(K[3])
            .wrapping_add(mt),
    );
}

#[inline(always)]
fn full_round3_step(a: u32, b: &mut u32, c: u32, d: u32, e: &mut u32, w: &mut [u32; 80], t: usize) {
    w[t] = mix(w, t);
    *e = e.wrapping_add(
        w[t].wrapping_add(a.rotate_left(5))
            .wrapping_add(f3(*b, c, d))
            .wrapping_add(K[2]),
    );
    *b = b.rotate_left(30);
}

#[inline(always)]
fn full_round4_step(a: u32, b: &mut u32, c: u32, d: u32, e: &mut u32, w: &mut [u32; 80], t: usize) {
    w[t] = mix(w, t);
    *e = e.wrapping_add(
        w[t].wrapping_add(a.rotate_left(5))
            .wrapping_add(f4(*b, c, d))
            .wrapping_add(K[3]),
    );
    *b = b.rotate_left(30);
}

#[inline]
fn round2_step4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    w: &[u32; 80],
    t: usize,
) {
    // 1
    *e = e.wrapping_add(
        w[t].wrapping_add(a.rotate_left(5))
            .wrapping_add(f2(*b, *c, *d))
            .wrapping_add(K[1]),
    );
    *b = b.rotate_left(30);

    // 2
    *d = d.wrapping_add(
        w[t + 1]
            .wrapping_add(e.rotate_left(5))
            .wrapping_add(f2(*a, *b, *c))
            .wrapping_add(K[1]),
    );
    *a = a.rotate_left(30);

    // 3
    *c = c.wrapping_add(
        w[t + 2]
            .wrapping_add(d.rotate_left(5))
            .wrapping_add(f2(*e, *a, *b))
            .wrapping_add(K[1]),
    );
    *e = e.rotate_left(30);

    // 4
    *b = b.wrapping_add(
        w[t + 3]
            .wrapping_add(c.rotate_left(5))
            .wrapping_add(f2(*d, *e, *a))
            .wrapping_add(K[1]),
    );
    *d = d.rotate_left(30);
}

#[inline]
fn round3_step4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    w: &[u32; 80],
    t: usize,
) {
    // 1
    *e = e.wrapping_add(
        w[t].wrapping_add(a.rotate_left(5))
            .wrapping_add(f3(*b, *c, *d))
            .wrapping_add(K[2]),
    );
    *b = b.rotate_left(30);

    // 2
    *d = d.wrapping_add(
        w[t + 1]
            .wrapping_add(e.rotate_left(5))
            .wrapping_add(f3(*a, *b, *c))
            .wrapping_add(K[2]),
    );
    *a = a.rotate_left(30);

    // 3
    *c = c.wrapping_add(
        w[t + 2]
            .wrapping_add(d.rotate_left(5))
            .wrapping_add(f3(*e, *a, *b))
            .wrapping_add(K[2]),
    );
    *e = e.rotate_left(30);

    // 4
    *b = b.wrapping_add(
        w[t + 3]
            .wrapping_add(c.rotate_left(5))
            .wrapping_add(f3(*d, *e, *a))
            .wrapping_add(K[2]),
    );
    *d = d.rotate_left(30);
}

#[inline]
fn round4_step4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    w: &[u32; 80],
    t: usize,
) {
    // 1
    *e = e.wrapping_add(
        w[t].wrapping_add(a.rotate_left(5))
            .wrapping_add(f4(*b, *c, *d))
            .wrapping_add(K[3]),
    );
    *b = b.rotate_left(30);

    // 2
    *d = d.wrapping_add(
        w[t + 1]
            .wrapping_add(e.rotate_left(5))
            .wrapping_add(f4(*a, *b, *c))
            .wrapping_add(K[3]),
    );
    *a = a.rotate_left(30);

    // 3
    *c = c.wrapping_add(
        w[t + 2]
            .wrapping_add(d.rotate_left(5))
            .wrapping_add(f4(*e, *a, *b))
            .wrapping_add(K[3]),
    );
    *e = e.rotate_left(30);

    // 4
    *b = b.wrapping_add(
        w[t + 3]
            .wrapping_add(c.rotate_left(5))
            .wrapping_add(f4(*d, *e, *a))
            .wrapping_add(K[3]),
    );
    *d = d.rotate_left(30);
}

#[inline]
fn full_round1_step_load4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    m: &[u32; 16],
    w: &mut [u32; 80],
    t: usize,
) {
    // load
    w[t..t + 4].copy_from_slice(&m[t..t + 4]);
    round1_step4(a, b, c, d, e, w, t);
}

#[inline(always)]
fn round1_step4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    w: &[u32; 80],
    t: usize,
) {
    // 1
    *e = e.wrapping_add(
        w[t].wrapping_add(a.rotate_left(5))
            .wrapping_add(f1(*b, *c, *d))
            .wrapping_add(K[0]),
    );
    *b = b.rotate_left(30);

    // 2
    *d = d.wrapping_add(
        w[t + 1]
            .wrapping_add(e.rotate_left(5))
            .wrapping_add(f1(*a, *b, *c))
            .wrapping_add(K[0]),
    );
    *a = a.rotate_left(30);

    // 3
    *c = c.wrapping_add(
        w[t + 2]
            .wrapping_add(d.rotate_left(5))
            .wrapping_add(f1(*e, *a, *b))
            .wrapping_add(K[0]),
    );
    *e = e.rotate_left(30);

    // 4
    *b = b.wrapping_add(
        w[t + 3]
            .wrapping_add(c.rotate_left(5))
            .wrapping_add(f1(*d, *e, *a))
            .wrapping_add(K[0]),
    );
    *d = d.rotate_left(30);
}

#[inline]
fn full_round1_step_expand4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    w: &mut [u32; 80],
    t: usize,
) {
    w[t] = mix(w, t);
    w[t + 1] = mix(w, t + 1);
    w[t + 2] = mix(w, t + 2);
    w[t + 3] = mix(w, t + 3);
    round1_step4(a, b, c, d, e, w, t);
}

#[inline]
fn full_round2_step4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    w: &mut [u32; 80],
    t: usize,
) {
    w[t] = mix(w, t);
    w[t + 1] = mix(w, t + 1);
    w[t + 2] = mix(w, t + 2);
    w[t + 3] = mix(w, t + 3);
    round2_step4(a, b, c, d, e, w, t);
}

#[inline]
fn full_round3_step4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    w: &mut [u32; 80],
    t: usize,
) {
    w[t] = mix(w, t);
    w[t + 1] = mix(w, t + 1);
    w[t + 2] = mix(w, t + 2);
    w[t + 3] = mix(w, t + 3);
    round3_step4(a, b, c, d, e, w, t);
}

#[inline]
fn full_round4_step4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    w: &mut [u32; 80],
    t: usize,
) {
    w[t] = mix(w, t);
    w[t + 1] = mix(w, t + 1);
    w[t + 2] = mix(w, t + 2);
    w[t + 3] = mix(w, t + 3);
    round4_step4(a, b, c, d, e, w, t);
}

#[inline]
fn round1_step_bw4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    m: &[u32; 80],
    t: usize,
) {
    round1_step_bw(*a, b, *c, *d, e, m[t]);
    round1_step_bw(*b, c, *d, *e, a, m[t - 1]);
    round1_step_bw(*c, d, *e, *a, b, m[t - 2]);
    round1_step_bw(*d, e, *a, *b, c, m[t - 3]);
}

#[inline]
fn round2_step_bw4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    m: &[u32; 80],
    t: usize,
) {
    round2_step_bw(*a, b, *c, *d, e, m[t]);
    round2_step_bw(*b, c, *d, *e, a, m[t - 1]);
    round2_step_bw(*c, d, *e, *a, b, m[t - 2]);
    round2_step_bw(*d, e, *a, *b, c, m[t - 3]);
}

#[inline]
fn round3_step_bw4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    m: &[u32; 80],
    t: usize,
) {
    round3_step_bw(*a, b, *c, *d, e, m[t]);
    round3_step_bw(*b, c, *d, *e, a, m[t - 1]);
    round3_step_bw(*c, d, *e, *a, b, m[t - 2]);
    round3_step_bw(*d, e, *a, *b, c, m[t - 3]);
}

#[inline]
fn round4_step_bw4(
    a: &mut u32,
    b: &mut u32,
    c: &mut u32,
    d: &mut u32,
    e: &mut u32,
    m: &[u32; 80],
    t: usize,
) {
    round4_step_bw(*a, b, *c, *d, e, m[t]);
    round4_step_bw(*b, c, *d, *e, a, m[t - 1]);
    round4_step_bw(*c, d, *e, *a, b, m[t - 2]);
    round4_step_bw(*d, e, *a, *b, c, m[t - 3]);
}

fn add_assign(left: &mut [u32; 5], right: [u32; 5]) {
    left[0] = left[0].wrapping_add(right[0]);
    left[1] = left[1].wrapping_add(right[1]);
    left[2] = left[2].wrapping_add(right[2]);
    left[3] = left[3].wrapping_add(right[3]);
    left[4] = left[4].wrapping_add(right[4]);
}

pub(crate) fn compression_states(
    ihv: &mut [u32; 5],
    m: &[u32; 16],
    w: &mut [u32; 80],
    state_58: &mut [u32; 5],
    state_65: &mut [u32; 5],
) {
    let [mut a, mut b, mut c, mut d, mut e] = *ihv;

    full_round1_step_load4(&mut a, &mut b, &mut c, &mut d, &mut e, m, w, 0);
    full_round1_step_load4(&mut b, &mut c, &mut d, &mut e, &mut a, m, w, 4);
    full_round1_step_load4(&mut c, &mut d, &mut e, &mut a, &mut b, m, w, 8);
    full_round1_step_load4(&mut d, &mut e, &mut a, &mut b, &mut c, m, w, 12);

    full_round1_step_expand4(&mut e, &mut a, &mut b, &mut c, &mut d, w, 16);

    full_round2_step4(&mut a, &mut b, &mut c, &mut d, &mut e, w, 20);
    full_round2_step4(&mut b, &mut c, &mut d, &mut e, &mut a, w, 24);
    full_round2_step4(&mut c, &mut d, &mut e, &mut a, &mut b, w, 28);
    full_round2_step4(&mut d, &mut e, &mut a, &mut b, &mut c, w, 32);
    full_round2_step4(&mut e, &mut a, &mut b, &mut c, &mut d, w, 36);

    full_round3_step4(&mut a, &mut b, &mut c, &mut d, &mut e, w, 40);
    full_round3_step4(&mut b, &mut c, &mut d, &mut e, &mut a, w, 44);
    full_round3_step4(&mut c, &mut d, &mut e, &mut a, &mut b, w, 48);
    full_round3_step4(&mut d, &mut e, &mut a, &mut b, &mut c, w, 52);

    full_round3_step(e, &mut a, b, c, &mut d, w, 56);
    full_round3_step(d, &mut e, a, b, &mut c, w, 57);

    // Store state58
    *state_58 = [a, b, c, d, e];

    full_round3_step(c, &mut d, e, a, &mut b, w, 58);
    full_round3_step(b, &mut c, d, e, &mut a, w, 59);

    full_round4_step4(&mut a, &mut b, &mut c, &mut d, &mut e, w, 60);
    full_round4_step(b, &mut c, d, e, &mut a, w, 64);

    // Store state65
    *state_65 = [a, b, c, d, e];

    full_round4_step(a, &mut b, c, d, &mut e, w, 65);
    full_round4_step(e, &mut a, b, c, &mut d, w, 66);
    full_round4_step(d, &mut e, a, b, &mut c, w, 67);

    full_round4_step4(&mut c, &mut d, &mut e, &mut a, &mut b, w, 68);
    full_round4_step4(&mut d, &mut e, &mut a, &mut b, &mut c, w, 72);
    full_round4_step4(&mut e, &mut a, &mut b, &mut c, &mut d, w, 76);

    add_assign(ihv, [a, b, c, d, e]);
}

pub(crate) fn recompress_fast_58(
    ihvin: &mut [u32; 5],
    ihvout: &mut [u32; 5],
    me2: &[u32; 80],
    state: &[u32; 5],
) {
    let [mut a, mut b, mut c, mut d, mut e] = *state;

    round3_step_bw(d, &mut e, a, b, &mut c, me2[57]);
    round3_step_bw(e, &mut a, b, c, &mut d, me2[56]);

    round3_step_bw4(&mut a, &mut b, &mut c, &mut d, &mut e, me2, 55);
    round3_step_bw4(&mut e, &mut a, &mut b, &mut c, &mut d, me2, 51);
    round3_step_bw4(&mut d, &mut e, &mut a, &mut b, &mut c, me2, 47);
    round3_step_bw4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 43);

    round2_step_bw4(&mut b, &mut c, &mut d, &mut e, &mut a, me2, 39);
    round2_step_bw4(&mut a, &mut b, &mut c, &mut d, &mut e, me2, 35);
    round2_step_bw4(&mut e, &mut a, &mut b, &mut c, &mut d, me2, 31);
    round2_step_bw4(&mut d, &mut e, &mut a, &mut b, &mut c, me2, 27);
    round2_step_bw4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 23);

    round1_step_bw4(&mut b, &mut c, &mut d, &mut e, &mut a, me2, 19);
    round1_step_bw4(&mut a, &mut b, &mut c, &mut d, &mut e, me2, 15);
    round1_step_bw4(&mut e, &mut a, &mut b, &mut c, &mut d, me2, 11);
    round1_step_bw4(&mut d, &mut e, &mut a, &mut b, &mut c, me2, 7);
    round1_step_bw4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 3);

    *ihvin = [a, b, c, d, e];
    [a, b, c, d, e] = *state;

    round3_step(c, &mut d, e, a, &mut b, me2[58]);
    round3_step(b, &mut c, d, e, &mut a, me2[59]);

    round4_step4(&mut a, &mut b, &mut c, &mut d, &mut e, me2, 60);
    round4_step4(&mut b, &mut c, &mut d, &mut e, &mut a, me2, 64);
    round4_step4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 68);
    round4_step4(&mut d, &mut e, &mut a, &mut b, &mut c, me2, 72);
    round4_step4(&mut e, &mut a, &mut b, &mut c, &mut d, me2, 76);

    ihvout[0] = ihvin[0].wrapping_add(a);
    ihvout[1] = ihvin[1].wrapping_add(b);
    ihvout[2] = ihvin[2].wrapping_add(c);
    ihvout[3] = ihvin[3].wrapping_add(d);
    ihvout[4] = ihvin[4].wrapping_add(e);
}

pub(crate) fn recompress_fast_65(
    ihvin: &mut [u32; 5],
    ihvout: &mut [u32; 5],
    me2: &[u32; 80],
    state: &[u32; 5],
) {
    let [mut a, mut b, mut c, mut d, mut e] = *state;

    round4_step_bw(b, &mut c, d, e, &mut a, me2[64]);
    round4_step_bw4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 63);

    round3_step_bw4(&mut b, &mut c, &mut d, &mut e, &mut a, me2, 59);
    round3_step_bw4(&mut a, &mut b, &mut c, &mut d, &mut e, me2, 55);
    round3_step_bw4(&mut e, &mut a, &mut b, &mut c, &mut d, me2, 51);
    round3_step_bw4(&mut d, &mut e, &mut a, &mut b, &mut c, me2, 47);
    round3_step_bw4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 43);

    round2_step_bw4(&mut b, &mut c, &mut d, &mut e, &mut a, me2, 39);
    round2_step_bw4(&mut a, &mut b, &mut c, &mut d, &mut e, me2, 35);
    round2_step_bw4(&mut e, &mut a, &mut b, &mut c, &mut d, me2, 31);
    round2_step_bw4(&mut d, &mut e, &mut a, &mut b, &mut c, me2, 27);
    round2_step_bw4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 23);

    round1_step_bw4(&mut b, &mut c, &mut d, &mut e, &mut a, me2, 19);
    round1_step_bw4(&mut a, &mut b, &mut c, &mut d, &mut e, me2, 15);
    round1_step_bw4(&mut e, &mut a, &mut b, &mut c, &mut d, me2, 11);
    round1_step_bw4(&mut d, &mut e, &mut a, &mut b, &mut c, me2, 7);
    round1_step_bw4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 3);

    *ihvin = [a, b, c, d, e];
    [a, b, c, d, e] = *state;

    round4_step(a, &mut b, c, d, &mut e, me2[65]);
    round4_step(e, &mut a, b, c, &mut d, me2[66]);
    round4_step(d, &mut e, a, b, &mut c, me2[67]);

    round4_step4(&mut c, &mut d, &mut e, &mut a, &mut b, me2, 68);
    round4_step4(&mut d, &mut e, &mut a, &mut b, &mut c, me2, 72);
    round4_step4(&mut e, &mut a, &mut b, &mut c, &mut d, me2, 76);

    ihvout[0] = ihvin[0].wrapping_add(a);
    ihvout[1] = ihvin[1].wrapping_add(b);
    ihvout[2] = ihvin[2].wrapping_add(c);
    ihvout[3] = ihvin[3].wrapping_add(d);
    ihvout[4] = ihvin[4].wrapping_add(e);
}

pub(crate) fn recompression_step(
    step: Testt,
    ihvin: &mut [u32; 5],
    ihvout: &mut [u32; 5],
    me2: &[u32; 80],
    state: &[u32; 5],
) {
    match step {
        Testt::T58 => {
            recompress_fast_58(ihvin, ihvout, me2, state);
        }
        Testt::T65 => {
            recompress_fast_65(ihvin, ihvout, me2, state);
        }
    }
}

#[inline(always)]
pub(crate) fn xor(a: &[u32; 5], b: &[u32; 5]) -> u32 {
    a[0] ^ b[0] | a[1] ^ b[1] | a[2] ^ b[2] | a[3] ^ b[3] | a[4] ^ b[4]
}
