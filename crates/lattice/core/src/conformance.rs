//! Checking a segment type against everything the store assumes of it.
//!
//! [`laws`](crate::laws) covers the join alone, and a stored value has two
//! more obligations: it must survive its own encoding, and a ranged decode
//! must be a narrowing of the whole rather than a different answer. A type
//! that breaks either loses keys through compaction without failing, so
//! every segment type states the whole contract by running this.

use crate::join::compose;
use crate::key::{Key, KeyRange};
use crate::laws::{self, Law};
use crate::segment::Segment;

/// A property a stored segment type must have, and did not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Broken {
    /// What [`Segment::encode`] wrote did not decode at all.
    Unreadable,
    /// It decoded, and gave back a different value.
    RoundTrip,
    /// Ranged decodes covering the whole key space did not compose to it.
    RangedDecode,
    /// The join is not a lattice.
    Law(Law),
}

/// Checks `a`, `b` and `c`, cutting every ranged decode at `split`.
///
/// # Errors
/// The first property broken, so a proptest reports which rather than that
/// one was.
pub fn check<T: Segment + Clone + PartialEq>(
    a: &T,
    b: &T,
    c: &T,
    split: Key,
) -> Result<(), Broken> {
    laws::check(a, b, c).map_err(Broken::Law)?;
    for value in [a, b, c] {
        round_trip(value)?;
        narrowing(value, split)?;
    }
    Ok(())
}

/// Decoding what `encode` wrote must give the value back.
fn round_trip<T: Segment + PartialEq>(value: &T) -> Result<(), Broken> {
    let decoded = T::decode(&value.encoded()).ok().ok_or(Broken::Unreadable)?;
    if &decoded == value {
        Ok(())
    } else {
        Err(Broken::RoundTrip)
    }
}

/// Two ranged decodes that between them cover every key must compose to the
/// whole.
///
/// A narrowing may keep more than it was asked for, so this is the strongest
/// claim that holds for every layout: it invents nothing, and drops nothing.
fn narrowing<T: Segment + PartialEq>(value: &T, split: Key) -> Result<(), Broken> {
    let bytes = value.encoded();
    let below = KeyRange::new(Key::ZERO, split).unwrap_or(KeyRange::EVERYTHING);
    let above = KeyRange::new(split, KeyRange::EVERYTHING.last()).unwrap_or(KeyRange::EVERYTHING);

    let decode = |range| {
        T::decode_range(&bytes, range)
            .ok()
            .ok_or(Broken::Unreadable)
    };
    let whole = decode(KeyRange::EVERYTHING)?;
    let (low, high) = (decode(below)?, decode(above)?);

    if compose([low, high]) == Some(whole) {
        Ok(())
    } else {
        Err(Broken::RangedDecode)
    }
}
