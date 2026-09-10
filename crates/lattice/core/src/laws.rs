//! Checking a join against the three laws the rest of the design assumes.
//!
//! A segment type states these by implementing [`Join`], and nothing makes
//! it true. Every guarantee above — compaction under any grouping, a retried
//! pass, a segment arriving late inside a merged range — is one of the three
//! read back in operational terms, so a type that skips this check has no
//! claim to any of them.

use crate::join::Join;

/// One of the three properties a join must have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Law {
    /// `(a ⊔ b) ⊔ c` must equal `a ⊔ (b ⊔ c)`, so grouping cannot matter.
    Associative,
    /// `a ⊔ b` must equal `b ⊔ a`, so arrival order cannot matter.
    Commutative,
    /// `a ⊔ a` must equal `a`, so a repeat cannot matter.
    Idempotent,
}

/// Checks all three laws against `a`, `b` and `c`.
///
/// # Errors
/// The first law broken, so a proptest reports which rather than that one was.
pub fn check<T: Join + Clone + PartialEq>(a: &T, b: &T, c: &T) -> Result<(), Law> {
    for value in [a, b, c] {
        if &joined(value.clone(), value.clone()) != value {
            return Err(Law::Idempotent);
        }
    }
    if joined(a.clone(), b.clone()) != joined(b.clone(), a.clone()) {
        return Err(Law::Commutative);
    }
    let left = joined(joined(a.clone(), b.clone()), c.clone());
    let right = joined(a.clone(), joined(b.clone(), c.clone()));
    if left != right {
        return Err(Law::Associative);
    }
    Ok(())
}

/// The join of two values, as a value rather than in place.
fn joined<T: Join>(mut left: T, right: T) -> T {
    left.join(right);
    left
}
