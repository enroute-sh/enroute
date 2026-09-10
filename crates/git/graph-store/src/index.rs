//! The commit graph as one joinable value, indexed by seq.

use std::collections::BTreeMap;

use thiserror::Error;

use enroute_lattice_core::frame::{HEADER, Header, read_header, write_header};
use enroute_lattice_core::{Join, Key, KeyRange, Segment};

use crate::format::{GRAPH, Malformed, NONE, RECORD, Record, first_seq};
use crate::generation::corrected_date;
use crate::strided::{Strided, offset};

/// A commit as the index is told about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit<'a> {
    /// Its parents' seqs, in the order git records them.
    pub parents: &'a [i64],
    /// Its root tree's `object_seq`.
    pub root_tree: u64,
    /// Its generation number, which must be above every parent's.
    ///
    /// Stored and compared, never interpreted, so the writer picks the
    /// scheme — git's topological depth, or its corrected commit date.
    pub generation: u32,
}

/// A commit whose generation the builder works out for itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatedCommit<'a> {
    /// Its parents' seqs, in the order git records them.
    pub parents: &'a [i64],
    /// Its root tree's `object_seq`.
    pub root_tree: u64,
    /// Its committer date, in seconds since the epoch.
    pub committer_date: i64,
}

/// Why a commit could not be recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum Unwritable {
    /// A seq the slots cannot hold.
    #[error(transparent)]
    TooLarge(#[from] TooLarge),

    /// A parent whose generation the builder has no way to know.
    ///
    /// Loud rather than treated as zero: a generation below a parent's
    /// prunes a walk that should have continued, and loses commits.
    #[error("commit {child} names parent {parent}, whose generation is not known here")]
    UnknownParent {
        /// The commit being recorded.
        child: i64,
        /// The parent it names.
        parent: i64,
    },
}

/// A seq the index's thirty-two-bit slots cannot hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TooLarge {
    /// A commit seq at or past `u32::MAX`, or below zero.
    #[error("a commit seq of {0} does not fit the index's 32-bit slots")]
    Commit(i64),
    /// An object seq at or past `u32::MAX`.
    #[error("an object seq of {0} does not fit the index's 32-bit slots")]
    Object(u64),
}

/// One commit as the index holds it, before it is laid out.
///
/// Field order is the tie-break the join uses, and nothing more: any total
/// order will do, and this one needs no allocation to compare.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Node {
    generation: u32,
    root_tree: u32,
    parent1: u32,
    parent2: u32,
    extra: Vec<u32>,
}

/// The parent graph over a range of commit seqs.
///
/// A fixed-stride array indexed by `seq - first`, so a lookup is arithmetic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitIndex {
    first: i64,
    records: Vec<Record>,
    tail: Vec<u32>,
}

/// One commit's parents: at most two inline, the rest out of line.
#[derive(Debug, Clone, Copy)]
pub struct Parents<'a> {
    inline: [u32; 2],
    extra: &'a [u32],
}

impl Parents<'_> {
    /// Every parent seq, in the order git records them.
    pub fn iter(&self) -> impl Iterator<Item = i64> + '_ {
        self.inline
            .iter()
            .copied()
            .take_while(|parent| *parent != NONE)
            .chain(self.extra.iter().copied())
            .map(i64::from)
    }

    /// How many parents there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.iter().count()
    }

    /// Whether this is a root commit.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inline.first().copied() == Some(NONE)
    }
}

/// What the index knows about one commit.
#[derive(Debug, Clone, Copy)]
pub struct Entry<'a> {
    record: Record,
    tail: &'a [u32],
}

impl<'a> Entry<'a> {
    /// Its root tree's `object_seq`.
    #[must_use]
    pub fn root_tree(&self) -> u64 {
        u64::from(self.record.root_tree)
    }

    /// Its generation number.
    #[must_use]
    pub const fn generation(&self) -> u32 {
        self.record.generation
    }

    /// Its parents.
    #[must_use]
    pub fn parents(&self) -> Parents<'a> {
        Parents {
            inline: [self.record.parent1, self.record.parent2],
            extra: extra_of(self.record, self.tail),
        }
    }
}

/// Builds an index one commit at a time.
///
/// A map rather than an array, because a push learns its commits in no
/// particular order and the layout is decided once at [`Builder::build`].
#[derive(Debug, Default)]
pub struct Builder {
    commits: BTreeMap<i64, Node>,
    known: BTreeMap<i64, u32>,
}

impl Builder {
    /// An index with nothing in it yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `commit` at `seq`, replacing whatever was there.
    ///
    /// # Errors
    /// [`TooLarge`] for a seq the thirty-two-bit slots cannot hold.
    pub fn insert(&mut self, seq: i64, commit: Commit<'_>) -> Result<(), TooLarge> {
        let mut parents = commit.parents.iter().copied();
        let node = Node {
            generation: commit.generation,
            root_tree: object_slot(commit.root_tree)?,
            parent1: optional_slot(parents.next())?,
            parent2: optional_slot(parents.next())?,
            extra: parents.map(commit_slot).collect::<Result<_, _>>()?,
        };
        self.commits.insert(commit_seq(seq)?, node);
        Ok(())
    }

    /// Tells the builder a generation it will need but cannot work out.
    ///
    /// A push's parents outside it, which the writer looks up once at the
    /// edge rather than walking back to a root for.
    pub fn know(&mut self, seq: i64, generation: u32) {
        self.known.insert(seq, generation);
    }

    /// Records `commit` at `seq`, working out its generation.
    ///
    /// Every parent must already be inserted or [known](Builder::know),
    /// which inserting a push in seq order satisfies.
    ///
    /// # Errors
    /// [`Unwritable`] for a seq that does not fit or a parent it cannot rank.
    pub fn insert_dated(&mut self, seq: i64, commit: DatedCommit<'_>) -> Result<u32, Unwritable> {
        let mut parents = Vec::with_capacity(commit.parents.len());
        for parent in commit.parents {
            let generation = self
                .generation_of(*parent)
                .ok_or(Unwritable::UnknownParent {
                    child: seq,
                    parent: *parent,
                })?;
            parents.push(generation);
        }
        let generation = corrected_date(commit.committer_date, parents);
        self.insert(
            seq,
            Commit {
                parents: commit.parents,
                root_tree: commit.root_tree,
                generation,
            },
        )?;
        Ok(generation)
    }

    /// What the builder can say about `seq`'s generation.
    fn generation_of(&self, seq: i64) -> Option<u32> {
        self.commits
            .get(&seq)
            .map(|node| node.generation)
            .or_else(|| self.known.get(&seq).copied())
    }

    /// Lays the commits out as an array over the range they span.
    #[must_use]
    pub fn build(self) -> CommitIndex {
        assemble(&Strided::laid_out(self.commits))
    }
}

impl CommitIndex {
    /// The seqs this index covers, or `None` when it holds no commits.
    ///
    /// The range is trimmed to what is present, so it never claims a hole at
    /// either end.
    #[must_use]
    pub fn range(&self) -> Option<KeyRange> {
        let last = self.last()?;
        let first = u64::try_from(self.first).ok()?;
        KeyRange::new(Key::new(first), Key::new(u64::try_from(last).ok()?)).ok()
    }

    /// What the index knows about `seq`, or `None` for a seq it does not hold.
    #[must_use]
    pub fn get(&self, seq: i64) -> Option<Entry<'_>> {
        if seq < self.first {
            return None;
        }
        let record = *self.records.get(offset(self.first, seq))?;
        record.present().then_some(Entry {
            record,
            tail: &self.tail,
        })
    }

    /// Whether the index holds no commits at all.
    pub(crate) fn is_empty(&self) -> bool {
        self.last().is_none()
    }

    /// The highest seq present, or `None` when there is none.
    fn last(&self) -> Option<i64> {
        let at = self.records.iter().rposition(|record| record.present())?;
        i64::try_from(at).ok().map(|at| self.first + at)
    }

    /// This index as a slot per seq of its range, for a rebuild.
    fn nodes(&self) -> Strided<Node> {
        let slots = self
            .records
            .iter()
            .map(|record| {
                record.present().then(|| Node {
                    generation: record.generation,
                    root_tree: record.root_tree,
                    parent1: record.parent1,
                    parent2: record.parent2,
                    extra: extra_of(*record, &self.tail).to_vec(),
                })
            })
            .collect();
        Strided::trimmed(self.first, slots)
    }
}

impl Join for CommitIndex {
    /// Union, and on a seq both hold, the greater node.
    ///
    /// They never disagree in practice: a seq's parents and root tree are
    /// written once. A total order is what makes the laws hold anyway.
    fn join(&mut self, other: Self) {
        // Short of a rebuild where one side has nothing to say: laying the
        // records back out is what costs, not the join itself.
        if other.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = other;
            return;
        }
        *self = assemble(&self.nodes().joined(other.nodes()));
    }
}

impl Segment for CommitIndex {
    type Error = Malformed;

    fn range(&self) -> Option<KeyRange> {
        Self::range(self)
    }

    fn encode(&self, out: &mut Vec<u8>) {
        write_header(
            &Header {
                first: Key::new(u64::try_from(self.first).unwrap_or(0)),
                records: self.records.len(),
                tail: self.tail.len(),
            },
            GRAPH,
            out,
        );
        for record in &self.records {
            record.write(out);
        }
        for value in &self.tail {
            out.extend_from_slice(&value.to_le_bytes());
        }
    }

    fn decode_range(bytes: &[u8], range: KeyRange) -> Result<Self, Malformed> {
        let header = read_header(bytes, GRAPH)?;
        let body = bytes.get(HEADER..).unwrap_or_default();
        let first = first_seq(header.first)?;

        // A stride array is indexed by seq, so the wanted records are a slice
        // rather than a scan — which is the whole reason the layout is one.
        let Some((from, upto)) = header.slice_of(range) else {
            return Ok(Self::default());
        };

        let mut records = Vec::with_capacity(upto - from);
        for at in from..upto {
            let at = at.saturating_mul(RECORD);
            let Some(chunk) = body
                .get(at..at + RECORD)
                .and_then(<[u8]>::first_chunk::<RECORD>)
            else {
                return Err(enroute_lattice_core::frame::Frame::Truncated {
                    expected: HEADER + header.records * RECORD,
                    actual: bytes.len(),
                }
                .into());
            };
            records.push(Record::read(chunk));
        }

        // Whole, whatever the slice: a record's `extra` is an offset into it,
        // and the tail is four bytes a parent past the second.
        let tail_at = header.records.saturating_mul(RECORD);
        let mut tail = Vec::with_capacity(header.tail);
        for at in 0..header.tail {
            let at = tail_at.saturating_add(at.saturating_mul(4));
            let value = body
                .get(at..at + 4)
                .and_then(|slice| slice.first_chunk::<4>())
                .map_or(0, |chunk| u32::from_le_bytes(*chunk));
            tail.push(value);
        }

        // Rebuilt rather than taken as read, so the value is canonical
        // whatever laid the bytes out, and a stray offset is caught here
        // rather than silently read as no parents at all.
        let read = Self {
            first: first.saturating_add(i64::try_from(from).unwrap_or(0)),
            records,
            tail,
        };
        for record in &read.records {
            if record.extra != NONE && extra_span(*record, &read.tail).is_none() {
                return Err(Malformed::StrayExtra(record.extra));
            }
        }
        Ok(assemble(&read.nodes()))
    }
}

/// Lays a stride array out as records plus an out-of-line tail.
fn assemble(nodes: &Strided<Node>) -> CommitIndex {
    let mut records = Vec::with_capacity(nodes.slots().len());
    let mut out_of_line = Vec::new();
    for slot in nodes.slots() {
        let Some(node) = slot else {
            records.push(Record::ABSENT);
            continue;
        };
        let extra = if node.extra.is_empty() {
            NONE
        } else {
            let at = u32::try_from(out_of_line.len()).unwrap_or(NONE);
            out_of_line.push(u32::try_from(node.extra.len()).unwrap_or(0));
            out_of_line.extend_from_slice(&node.extra);
            at
        };
        records.push(Record {
            parent1: node.parent1,
            parent2: node.parent2,
            root_tree: node.root_tree,
            generation: node.generation,
            extra,
        });
    }

    CommitIndex {
        first: nodes.first(),
        records,
        tail: out_of_line,
    }
}

/// The out-of-line parents of `record`, or nothing when it has none.
fn extra_of(record: Record, tail: &[u32]) -> &[u32] {
    extra_span(record, tail).unwrap_or_default()
}

/// The same, distinguishing "no extra parents" from "a stray offset".
fn extra_span(record: Record, tail: &[u32]) -> Option<&[u32]> {
    if record.extra == NONE {
        return Some(&[]);
    }
    let at = usize::try_from(record.extra).ok()?;
    let count = usize::try_from(*tail.get(at)?).ok()?;
    tail.get(at.checked_add(1)?..at.checked_add(1)?.checked_add(count)?)
}

fn commit_seq(seq: i64) -> Result<i64, TooLarge> {
    commit_slot(seq).map(|_| seq)
}

fn commit_slot(seq: i64) -> Result<u32, TooLarge> {
    match u32::try_from(seq) {
        Ok(slot) if slot != NONE => Ok(slot),
        _ => Err(TooLarge::Commit(seq)),
    }
}

fn optional_slot(seq: Option<i64>) -> Result<u32, TooLarge> {
    seq.map_or(Ok(NONE), commit_slot)
}

fn object_slot(seq: u64) -> Result<u32, TooLarge> {
    match u32::try_from(seq) {
        Ok(slot) if slot != NONE => Ok(slot),
        _ => Err(TooLarge::Object(seq)),
    }
}
