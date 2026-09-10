//! A reference segment type, for testing whatever holds one.

use std::collections::BTreeMap;

use thiserror::Error;

use enroute_lattice_core::{Join, Key, KeyRange, Segment};

/// Bytes one entry takes: an eight-byte key and a four-byte count.
const ENTRY: usize = 12;

/// A grow-only map from key to count, joined by taking the larger count.
///
/// The shape the commit index has, and a lattice whichever way two segments
/// disagree — which is what lets a test generate them freely.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counters {
    /// What each key counts.
    pub entries: BTreeMap<u64, u32>,
}

impl Counters {
    /// The counters in `entries`.
    #[must_use]
    pub fn of(entries: &[(u64, u32)]) -> Self {
        Self {
            entries: entries.iter().copied().collect(),
        }
    }

    /// `count` keys from `first`, each counting its own key.
    #[must_use]
    pub fn run(first: u64, count: u64) -> Self {
        Self {
            entries: (first..first.saturating_add(count))
                .map(|key| (key, u32::try_from(key).unwrap_or(u32::MAX)))
                .collect(),
        }
    }
}

impl Join for Counters {
    fn join(&mut self, other: Self) {
        for (key, count) in other.entries {
            let slot = self.entries.entry(key).or_insert(count);
            *slot = (*slot).max(count);
        }
    }
}

/// Bytes that are not a whole number of entries.
#[derive(Debug, Error)]
#[error("a counters segment is {ENTRY}-byte entries, and {0} bytes is not")]
pub struct Ragged(usize);

impl Segment for Counters {
    type Error = Ragged;

    fn range(&self) -> Option<KeyRange> {
        let first = *self.entries.keys().next()?;
        let last = *self.entries.keys().next_back()?;
        KeyRange::new(Key::new(first), Key::new(last)).ok()
    }

    fn encode(&self, out: &mut Vec<u8>) {
        for (key, count) in &self.entries {
            out.extend_from_slice(&key.to_le_bytes());
            out.extend_from_slice(&count.to_le_bytes());
        }
    }

    /// Whole, whatever the range: a run of counters has nothing to seek by.
    fn decode_range(bytes: &[u8], _range: KeyRange) -> Result<Self, Self::Error> {
        if !bytes.len().is_multiple_of(ENTRY) {
            return Err(Ragged(bytes.len()));
        }
        let mut entries = BTreeMap::new();
        for entry in bytes.as_chunks::<ENTRY>().0 {
            let (Some(key), Some(count)) = (entry.first_chunk::<8>(), entry.last_chunk::<4>())
            else {
                return Err(Ragged(bytes.len()));
            };
            entries.insert(u64::from_le_bytes(*key), u32::from_le_bytes(*count));
        }
        Ok(Self { entries })
    }
}
