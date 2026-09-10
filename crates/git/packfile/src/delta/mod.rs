//! Git delta encoding and decoding.
//!
//! Stream layout: `[base_size: varint][result_size: varint][instruction...]`.
//! Each instruction is one of
//! - **copy** (`0x80` set) — flag bits `0x01..0x08` select which of four
//!   little-endian offset bytes follow, `0x10..0x40` which of three size bytes;
//!   an absent byte is zero, and a size of zero means `0x10000`, not `0`.
//! - **insert** (`0x80` clear, non-zero) — that many literal bytes follow.
//! - `0x00` — reserved, rejected; the field widths and zero-size rule live
//!   here, not in either inverse, since a disagreement would corrupt objects
//!   silently.

mod apply;
mod encode;

pub use apply::apply_delta;
pub use encode::encode_delta;

use crate::varint::{read_varint, write_varint};

/// Longest run one copy instruction can encode — the size field is three
/// bytes, and `0` is reserved for `0x10000`.
const MAX_COPY: usize = 0x00ff_ffff;

/// Longest run one insert instruction can encode, clear of the `0x80` copy
/// bit it shares the command byte with.
const MAX_INSERT: usize = 0x7f;
