//! Composition, and the one property everything above it rests on.

/// How two values of one kind become one.
///
/// # Laws
/// Associative, commutative and idempotent. [`laws`](crate::laws) checks
/// them, and nothing above this is correct without them.
pub trait Join {
    /// Absorbs `other`, leaving the join of the two.
    fn join(&mut self, other: Self);
}

/// Joins many values into one, or `None` when there were none.
///
/// The order is irrelevant and a repeat is harmless, which is what lets a
/// reader compose whatever the catalog handed it without sorting first.
pub fn compose<T: Join>(values: impl IntoIterator<Item = T>) -> Option<T> {
    let mut values = values.into_iter();
    let mut composed = values.next()?;
    for value in values {
        composed.join(value);
    }
    Some(composed)
}
