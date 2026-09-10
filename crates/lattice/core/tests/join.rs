//! The three laws and the segment contract, and what they buy: order,
//! repetition and overlap.

use proptest::prelude::*;

use enroute_lattice_core::conformance::Broken;
use enroute_lattice_core::laws::{self, Law};
use enroute_lattice_core::{EmptyRange, Join, Key, KeyRange, Segment, compose, conformance};
use enroute_lattice_store::{Counters, Ragged};

fn fixture() -> impl Strategy<Value = Counters> {
    prop::collection::vec((0_u64..64, 0_u32..8), 0..12).prop_map(|entries| Counters::of(&entries))
}

proptest! {
    #[test]
    fn the_join_obeys_its_laws(a in fixture(), b in fixture(), c in fixture()) {
        prop_assert_eq!(laws::check(&a, &b, &c), Ok(()));
    }

    // The property a reader depends on: whatever the catalog hands back, in
    // whatever order, with whatever repeats, composes to one answer.
    #[test]
    fn composition_ignores_order_and_repetition(
        segments in prop::collection::vec(fixture(), 1..6),
        repeats in prop::collection::vec(0_usize..6, 0..6),
    ) {
        let straight = compose(segments.clone()).unwrap();

        let mut shuffled = segments.clone();
        shuffled.reverse();
        for index in repeats {
            if let Some(extra) = segments.get(index % segments.len()) {
                shuffled.push(extra.clone());
            }
        }

        prop_assert_eq!(compose(shuffled).unwrap(), straight);
    }

    #[test]
    fn the_reference_type_meets_the_segment_contract(
        a in fixture(),
        b in fixture(),
        c in fixture(),
        split in 0_u64..64,
    ) {
        prop_assert_eq!(conformance::check(&a, &b, &c, Key::new(split)), Ok(()));
    }
}

#[test]
fn composing_nothing_is_nothing() {
    assert_eq!(compose(Vec::<Counters>::new()), None);
}

// The laws are what a new segment type states by implementing the trait, so
// the checker has to actually catch a type that does not have them.
#[test]
fn a_join_that_breaks_a_law_is_caught() {
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct LastWriteWins(u32);

    impl Join for LastWriteWins {
        fn join(&mut self, other: Self) {
            self.0 = other.0;
        }
    }

    assert_eq!(
        laws::check(&LastWriteWins(1), &LastWriteWins(2), &LastWriteWins(3)),
        Err(Law::Commutative)
    );
}

// A narrowing that drops what its range covers is how compaction loses a key
// without failing, so the checker has to catch one.
#[test]
fn a_ranged_decode_that_drops_keys_is_caught() {
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct Forgetful(Counters);

    impl Join for Forgetful {
        fn join(&mut self, other: Self) {
            self.0.join(other.0);
        }
    }

    impl Segment for Forgetful {
        type Error = Ragged;

        fn range(&self) -> Option<KeyRange> {
            self.0.range()
        }

        fn encode(&self, out: &mut Vec<u8>) {
            self.0.encode(out);
        }

        /// Everything a whole read asks for, and nothing a narrowed one does.
        fn decode_range(bytes: &[u8], range: KeyRange) -> Result<Self, Self::Error> {
            if range == KeyRange::EVERYTHING {
                return Counters::decode_range(bytes, range).map(Self);
            }
            Ok(Self::default())
        }
    }

    let value = Forgetful(Counters::of(&[(1, 1), (9, 9)]));
    assert_eq!(
        conformance::check(&value, &value, &value, Key::new(4)),
        Err(Broken::RangedDecode)
    );
}

#[test]
fn a_value_reports_the_range_it_holds() {
    let value = Counters::of(&[(4, 1), (9, 1)]);
    let range = value.range().unwrap();
    assert_eq!(range.first(), Key::new(4));
    assert_eq!(range.last(), Key::new(9));
    assert_eq!(range.span(), 6, "span counts keys, holes included");
    assert_eq!(Counters::default().range(), None);
}

#[test]
fn a_range_may_not_end_below_where_it_starts() {
    assert!(matches!(
        KeyRange::new(Key::new(7), Key::new(6)),
        Err(EmptyRange { first: 7, last: 6 })
    ));
    let single = KeyRange::new(Key::new(7), Key::new(7)).expect("one key is a range");
    assert_eq!(single.span(), 1);
}

#[test]
fn ranges_overlap_and_hull() {
    let low = KeyRange::new(Key::new(0), Key::new(10)).unwrap();
    let high = KeyRange::new(Key::new(5), Key::new(20)).unwrap();
    let far = KeyRange::new(Key::new(30), Key::new(40)).unwrap();

    assert!(low.overlaps(high));
    assert!(!low.overlaps(far));
    assert_eq!(low.hull(high).last(), Key::new(20));
    assert_eq!(
        low.hull(far),
        KeyRange::new(Key::new(0), Key::new(40)).unwrap()
    );
    assert!(low.contains(Key::new(10)) && !low.contains(Key::new(11)));
}
