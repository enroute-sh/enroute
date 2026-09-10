//! Turning an incoming packfile into a stored push, and keeping it there.
//!
//! The scan streams the pack once, keeping a verbatim copy in memory and
//! noting where each entry sits, without materializing anything. Every later
//! pass works from that copy, where entries are addressable in any order —
//! the precondition for resolving them concurrently. The trailer is checked
//! before a single object is resolved: a corrupt pack costs one inflate pass
//! instead of a full ingest. That copy stays the push's backing store for as
//! long as the push runs — every entry is read back at least once, and the
//! pack is bounded by what the front door accepts, so nothing is staged.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use gix_hash::ObjectId;
use gix_object::Kind;
use tokio::io::AsyncBufRead;

use enroute_git_core::{Error, ObjectHashMap};
use enroute_git_packfile::{EntryHeader, MAX_OBJECT_BYTES, MAX_PACK_OBJECTS, PackReader};

/// Where one entry sits in the pack.
///
/// Offsets are true pack offsets from the parser, not from how much has
/// been banked — a buffered reader between the socket and parser runs ahead.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RawSpan {
    /// Where the entry starts in the pack.
    pub(crate) offset: u64,
    /// How many bytes the entry occupies, known from the scan and so
    /// available before it has been read.
    pub(crate) len: usize,
}

/// One entry's bytes, as a view into `pack` rather than a copy.
///
/// # Errors
/// Returns an error if the span falls outside the pack.
pub(crate) fn read_span(pack: &Bytes, span: RawSpan) -> Result<Bytes, Error> {
    let start = usize::try_from(span.offset).map_err(|e| anyhow::anyhow!("entry offset: {e}"))?;
    let end = start
        .checked_add(span.len)
        .filter(|end| *end <= pack.len())
        .ok_or_else(|| anyhow::anyhow!("entry span past end of pack"))?;
    Ok(pack.slice(start..end))
}

/// One entry of one of a push's packs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WireRef {
    /// Which of the session's packs.
    pub(crate) pack: usize,
    /// The entry's index within that pack.
    pub(crate) at: usize,
}

/// A received pack: its bytes, where each entry sits in them, and what each
/// entry turned out to be.
#[derive(Debug)]
pub(crate) struct WirePack {
    pack: Bytes,
    entries: Vec<ScannedEntry>,
    /// Each entry's object id, learned by the indexing pass, parallel to
    /// `entries`.
    oids: Vec<ObjectId>,
}

impl WirePack {
    /// # Errors
    /// Returns an error if `oids` doesn't line up with `entries` — a bug in
    /// the indexing pass rather than anything a client can provoke.
    pub(crate) fn new(
        pack: Bytes,
        entries: Vec<ScannedEntry>,
        oids: Vec<ObjectId>,
    ) -> Result<Self, Error> {
        if entries.len() != oids.len() {
            return Err(anyhow::anyhow!(
                "pack has {} entries but {} were identified",
                entries.len(),
                oids.len()
            )
            .into());
        }
        Ok(Self {
            pack,
            entries,
            oids,
        })
    }

    /// # Errors
    /// Returns an error if `at` is not an entry of this pack.
    pub(crate) fn entry(&self, at: usize) -> Result<&ScannedEntry, Error> {
        self.entries
            .get(at)
            .ok_or_else(|| anyhow::anyhow!("entry {at} is not in this pack").into())
    }

    /// The entry's compressed body — the object's own deflate stream, or a
    /// delta's, with the pack's framing left behind.
    ///
    /// # Errors
    /// Returns an error if `at` is not an entry of this pack, or its recorded
    /// header runs past its own bytes.
    pub(crate) fn body(&self, at: usize) -> Result<Bytes, Error> {
        let entry = self.entry(at)?;
        let raw = read_span(&self.pack, entry.entry)?;
        if entry.header_len > raw.len() {
            return Err(anyhow::anyhow!("entry {at} is shorter than its header").into());
        }
        Ok(raw.slice(entry.header_len..))
    }

    /// What entry `at` reconstructs.
    fn oid(&self, at: usize) -> Result<ObjectId, Error> {
        self.oids
            .get(at)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("entry {at} is not in this pack").into())
    }
}

/// Every pack one push arrived in, and where each object it received sits.
///
/// The push's backing store: every later pass reads bytes back out of this,
/// verbatim or rebuilt, rather than keeping objects of its own.
#[derive(Debug, Default)]
pub(crate) struct WirePacks {
    /// Shared so promotion's spawned tasks can hold a pack without copying.
    packs: Vec<Arc<WirePack>>,
    /// First occurrence wins: a repeated object is the same bytes either way.
    entry_of: ObjectHashMap<WireRef>,
}

impl WirePacks {
    /// Take a scanned, identified pack as this push's, indexing where each of
    /// its objects sits.
    pub(crate) fn adopt(&mut self, pack: WirePack) {
        let at = self.packs.len();
        for (index, &oid) in pack.oids.iter().enumerate() {
            self.entry_of.entry(oid).or_insert(WireRef {
                pack: at,
                at: index,
            });
        }
        self.packs.push(Arc::new(pack));
    }

    /// Where `oid` arrived, if this push received it at all.
    pub(crate) fn entry_of(&self, oid: ObjectId) -> Option<WireRef> {
        self.entry_of.get(&oid).copied()
    }

    /// # Errors
    /// Returns an error if `at` names no entry of this push.
    pub(crate) fn entry(&self, at: WireRef) -> Result<&ScannedEntry, Error> {
        self.pack(at.pack)?.entry(at.at)
    }

    /// What entry `at` reconstructs.
    ///
    /// # Errors
    /// Returns an error if `at` names no entry of this push.
    pub(crate) fn oid(&self, at: WireRef) -> Result<ObjectId, Error> {
        self.pack(at.pack)?.oid(at.at)
    }

    /// Entry `at`'s compressed body.
    ///
    /// # Errors
    /// Returns an error if `at` names no entry of this push, or its recorded
    /// header runs past its own bytes.
    pub(crate) fn body(&self, at: WireRef) -> Result<Bytes, Error> {
        self.pack(at.pack)?.body(at.at)
    }

    /// How much rebuilding `oid` would put in memory, as far as the pack it
    /// arrived in can say — zero for anything this push didn't receive.
    pub(crate) fn size_of(&self, oid: ObjectId) -> u64 {
        self.entry_of(oid)
            .and_then(|at| self.entry(at).ok())
            .map_or(0, |entry| entry.decompressed_size)
    }

    /// The packs themselves, for promotion's per-pack tasks.
    pub(crate) fn packs(&self) -> &[Arc<WirePack>] {
        &self.packs
    }

    fn pack(&self, at: usize) -> Result<&WirePack, Error> {
        self.packs
            .get(at)
            .map(Arc::as_ref)
            .ok_or_else(|| anyhow::anyhow!("pack {at} is not part of this push").into())
    }
}

/// What an entry needs before it can be materialized.
///
/// Resolved by the scan rather than passed along in wire form: `OFS_DELTA`
/// carries a distance meaningful only next to the offset it was read at.
#[derive(Clone, Copy, Debug)]
pub(crate) enum EntryBase {
    /// Complete on its own; carries the kind its deltas inherit.
    Whole(Kind),
    /// Deltified against another, earlier entry of this pack, by index.
    InPack(usize),
    /// Deltified against an object id: a thin-pack base the repo already has,
    /// or another entry of this pack whose id isn't known until it's hashed.
    Ref(ObjectId),
}

/// One entry the scan located, still compressed in the pack.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ScannedEntry {
    pub(crate) base: EntryBase,
    /// Decompressed size of this entry's payload — for a delta, of its
    /// instructions rather than of the object they produce.
    pub(crate) decompressed_size: u64,
    /// The whole entry, header included, so the staged pack stays a real pack
    /// rather than a bag of bodies.
    pub(crate) entry: RawSpan,
    /// How much of `entry` is header, and so comes back off before the body
    /// can be inflated.
    pub(crate) header_len: usize,
}

/// The reader the scan drives, named apart from [`crate::PackReader`] — the
/// boxed body a worker is handed, which is what this parses.
type ScanReader<R> = PackReader<R>;

/// Ceiling on what a caller's length hint may reserve up front.
///
/// A backstop, not a check: a wrong hint should cost a reallocation, never
/// a multi-gigabyte allocation before a byte has been parsed.
const MAX_HINT_BYTES: u64 = 1024 * 1024 * 1024;

async fn locate_entries<R: AsyncBufRead + Unpin>(
    reader: &mut ScanReader<R>,
) -> Result<Vec<ScannedEntry>, Error> {
    let object_count =
        usize::try_from(reader.read_pack_header().await?).map_err(|e| anyhow::anyhow!("{e}"))?;
    if object_count > MAX_PACK_OBJECTS {
        return Err(Error::Invalid(format!(
            "pack claims {object_count} objects (max {MAX_PACK_OBJECTS})"
        )));
    }

    // object_count is client-supplied and untrusted, so it isn't used to
    // pre-size these — a bogus count could force a multi-GB allocation before
    // a single entry is parsed.
    let mut entries: Vec<ScannedEntry> = Vec::new();
    let mut index_by_offset: HashMap<u64, usize> = HashMap::new();

    for _ in 0..object_count {
        let entry_start = reader.offset();
        let (header, decompressed_size) = reader.read_entry_header().await?;
        let header_len = usize::try_from(reader.offset().saturating_sub(entry_start))
            .map_err(|e| anyhow::anyhow!("entry header length: {e}"))?;

        if decompressed_size > MAX_OBJECT_BYTES {
            return Err(Error::Invalid(format!(
                "object too large: {decompressed_size} bytes (max {MAX_OBJECT_BYTES})"
            )));
        }
        let base = match header {
            EntryHeader::Full(kind) => EntryBase::Whole(kind),
            EntryHeader::OfsDelta { base_distance } => {
                // Distances are relative, so any consistent origin works;
                // these are offsets from the start of the pack.
                let base_offset = entry_start
                    .checked_sub(base_distance)
                    .ok_or_else(|| Error::Invalid("OFS_DELTA base offset underflow".into()))?;
                let &at = index_by_offset.get(&base_offset).ok_or_else(|| {
                    Error::Invalid("OFS_DELTA base is not an entry in this pack".into())
                })?;
                EntryBase::InPack(at)
            }
            EntryHeader::RefDelta { base_id } => EntryBase::Ref(base_id),
        };

        reader.skip_entry_body(decompressed_size).await?;

        let entry_len = usize::try_from(reader.offset().saturating_sub(entry_start))
            .map_err(|e| anyhow::anyhow!("entry length: {e}"))?;
        index_by_offset.insert(entry_start, entries.len());
        entries.push(ScannedEntry {
            base,
            decompressed_size,
            entry: RawSpan {
                offset: entry_start,
                len: entry_len,
            },
            header_len,
        });
    }

    reader.verify_trailer().await?;

    Ok(entries)
}

/// Read a pack off `body`, locating every entry and taking the verbatim copy
/// the parser kept while doing so.
///
/// `len_hint` is what the caller knows the pack to run to, if anything — see
/// [`crate::IncomingPack`].
///
/// # Errors
/// Returns an error if the pack is malformed or fails its checksum.
// Reports no progress: the client is still sending, and its own meter owns
// the terminal line until it stops.
#[tracing::instrument(name = "enroute_git_ingest::pack::scan", skip_all)]
pub(crate) async fn scan<R: AsyncBufRead + Unpin>(
    body: R,
    len_hint: Option<u64>,
) -> Result<(Vec<ScannedEntry>, Bytes), Error> {
    let reserve = usize::try_from(len_hint.unwrap_or(0).min(MAX_HINT_BYTES)).unwrap_or(0);
    let mut reader = PackReader::retaining(body, reserve);
    let entries = locate_entries(&mut reader).await?;
    Ok((entries, reader.take_pack()))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;
    use enroute_git_test_support::{
        TestCommit, annotated_tag, linear_commit, make_pack, make_state,
    };
    use gix_object::Kind;

    use super::{RawSpan, read_span};
    use crate::progress::IngestProgress;
    use crate::session::IngestSession;
    use crate::test_helpers::staging_store;

    #[test]
    fn reads_back_every_entry_of_a_pack() {
        let entries: Vec<Vec<u8>> = (0..64u8).map(|i| vec![i; usize::from(i) + 1]).collect();
        let pack = Bytes::from(entries.concat());

        let mut at = 0u64;
        for entry in &entries {
            let span = RawSpan {
                offset: at,
                len: entry.len(),
            };
            at += u64::try_from(entry.len()).unwrap();
            assert_eq!(&read_span(&pack, span).unwrap()[..], &entry[..]);
        }
    }

    /// Entries are read in dependency order, nothing like the order they
    /// arrived in, so reads must not depend on where the last one landed.
    #[test]
    fn reads_are_independent_of_their_order() {
        let head = vec![3u8; 4096];
        let tail = b"tail-marker".to_vec();
        let pack = Bytes::from([head.clone(), tail.clone()].concat());

        let tail_span = RawSpan {
            offset: u64::try_from(head.len()).unwrap(),
            len: tail.len(),
        };
        let head_span = RawSpan { offset: 0, len: 16 };

        // Back, then front, then back again.
        for (span, want) in [
            (tail_span, &tail[..]),
            (head_span, &head[..16]),
            (tail_span, &tail[..]),
        ] {
            assert_eq!(&read_span(&pack, span).unwrap()[..], want);
        }
    }

    #[test]
    fn rejects_a_span_past_the_end() {
        let pack = Bytes::from_static(b"short");
        read_span(
            &pack,
            RawSpan {
                offset: 0,
                len: 999,
            },
        )
        .unwrap_err();
        read_span(
            &pack,
            RawSpan {
                offset: u64::MAX,
                len: 1,
            },
        )
        .unwrap_err();
    }

    /// Regression: two passes both reported as `ResolvingObjects`, so a
    /// client's meter rewound and came back with a mismatched count.
    ///
    /// Each pass is now its own stage, and within a stage the count only
    /// climbs.
    #[tokio::test]
    async fn a_stage_never_rewinds_its_own_count() {
        let state = make_state();
        let repo = enroute_git_test_support::create_repo(&state).await;
        let mut session = IngestSession::new(state.clone(), staging_store(), repo.clone());

        // Several commits, so planning takes more than the one round a single
        // root commit would settle in.
        let mut commits: Vec<TestCommit> = Vec::new();
        for i in 0..4u64 {
            let parent = commits.last().map(|c| c.commit_sha.clone());
            commits.push(linear_commit(
                format!("hello {i}\n").as_bytes(),
                parent.as_deref(),
                i,
                "c",
            ));
        }
        // Entries the client sends whole are kept verbatim, so a pack of only
        // those leaves compression with nothing to count. Annotated tags are
        // always re-encoded — promotion reads them back as loose objects.
        let tags: Vec<Vec<u8>> = commits
            .iter()
            .map(|c| annotated_tag(c.commit_oid, "v1").1)
            .collect();

        let mut entries: Vec<_> = commits.iter().flat_map(|c| c.pack_entries()).collect();
        entries.extend(tags.iter().map(|t| (Kind::Tag, t.as_slice())));

        let reports = std::sync::Mutex::new(Vec::new());
        let progress = |stage: IngestProgress| reports.lock().unwrap().push(stage);
        session
            .ingest_pack(Cursor::new(make_pack(&entries)), None, &progress)
            .await
            .unwrap();

        let seen: Vec<Report> = reports.into_inner().unwrap().iter().map(named).collect();
        assert_counts_only_climb(&seen);
        assert_every_stage_counted(&seen);

        let mut order: Vec<&str> = seen.iter().map(|&(name, ..)| name).collect();
        order.dedup();
        assert_eq!(order, STAGES);
    }

    /// The stages `ingest_pack` reports, in the order it reaches them.
    const STAGES: [&str; 3] = ["resolving", "preparing", "compressing"];

    type Report = (&'static str, u64, u64);

    fn assert_counts_only_climb(seen: &[Report]) {
        for pair in seen.windows(2) {
            let ((prev_stage, prev_done, _), (stage, done, total)) = (pair[0], pair[1]);
            // A stage announces itself with a zero total before its
            // denominator exists; that report is the one that isn't a count.
            let comparable = prev_stage == stage && total != 0;
            assert!(
                !comparable || done >= prev_done,
                "{stage} went from {prev_done} back to {done} of {total}"
            );
        }
    }

    /// Without this, a stage that stopped counting altogether would still
    /// pass the order check.
    fn assert_every_stage_counted(seen: &[Report]) {
        for stage in STAGES {
            let reached_total =
                |&(name, done, total): &Report| name == stage && total > 1 && done == total;
            assert!(
                seen.iter().any(reached_total),
                "{stage} never counted to its total: {seen:?}"
            );
        }
    }

    fn named(stage: &IngestProgress) -> Report {
        let name = crate::test_helpers::stage_name(stage);
        match *stage {
            IngestProgress::ResolvingObjects { done, total }
            | IngestProgress::PreparingPacks { done, total }
            | IngestProgress::CompressingObjects { done, total } => (name, done, total),
            other => panic!("{other:?} does not belong to the pack path"),
        }
    }
}
