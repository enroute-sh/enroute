//! One contract, so every [`Catalog`] is held to the same thing.
//!
//! A catalog is a seam, and a seam with two implementations is two chances to
//! disagree about it. Running this against both is what makes the in-memory
//! one evidence about the real one rather than a separate thing that passes.

use bytes::Bytes;
use thiserror::Error;

use enroute_lattice_core::{Key, KeyRange, Residence, Tier};

use crate::{Body, Catalog, Entry, Scope, SegmentId, Written};

/// Something a catalog did that its contract does not allow.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct Failed(String);

/// Checks `catalog` against the contract, using `scope` as a scratch scope.
///
/// Both `scope` and the one numbered after it must start empty, since the
/// purge is only a purge if it leaves its neighbour alone.
///
/// # Errors
/// [`Failed`] on the first thing that does not hold.
pub async fn check<C: Catalog>(catalog: &C, scope: &Scope) -> Result<(), Failed> {
    starts_empty(catalog, scope).await?;
    let inline = inline_round_trips(catalog, scope).await?;
    let bucket = bucket_round_trips(catalog, scope).await?;
    bodies_return_what_was_asked(catalog, scope, inline, bucket).await?;
    let merged = replace_swaps_both_ways(catalog, scope, &[inline, bucket]).await?;
    overlapping_ranges_coexist(catalog, scope, merged).await?;
    purge_empties_one_scope(catalog, scope).await
}

async fn starts_empty<C: Catalog>(catalog: &C, scope: &Scope) -> Result<(), Failed> {
    let entries = entries(catalog, scope).await?;
    require(entries.is_empty(), "a fresh scope must list no segments")?;

    let covering = ask(
        catalog.covering(scope, KeyRange::EVERYTHING),
        "covering an empty scope",
    )
    .await?;
    require(covering.is_empty(), "a fresh scope must cover nothing")?;

    let bodies = ask(
        catalog.bodies(scope, &[SegmentId::fresh()]),
        "bodies of a segment that does not exist",
    )
    .await?;
    require(bodies.is_empty(), "an unknown id must return no body")
}

/// Inserts an inlined segment and reads it back, bytes and all.
async fn inline_round_trips<C: Catalog>(catalog: &C, scope: &Scope) -> Result<SegmentId, Failed> {
    let payload = Bytes::from_static(b"inlined");
    let segment = written(10, 19, 0, Body::Inline(payload.clone()), 7)?;
    let id = segment.id;
    ask(
        catalog.insert(scope, segment),
        "inserting an inlined segment",
    )
    .await?;

    let entry = only(entries(catalog, scope).await?, "after one insert")?;
    require(
        entry.id == id,
        "the listed segment must be the inserted one",
    )?;
    require(
        entry.range.first() == Key::new(10),
        "first key must survive",
    )?;
    require(entry.range.last() == Key::new(19), "last key must survive")?;
    require(entry.tier == Tier::ZERO, "tier must survive")?;
    require(entry.bytes == 7, "the encoded size must survive")?;
    require(
        entry.residence == Residence::Inline,
        "an inlined segment must report Inline",
    )?;

    // The whole reason to inline: the bytes arrive with the list.
    let listed = only(
        ask(
            catalog.covering(scope, KeyRange::EVERYTHING),
            "covering one inlined segment",
        )
        .await?,
        "covering one inlined segment",
    )?;
    require(
        listed.body == Body::Inline(payload),
        "an inlined segment's bytes must come back with the listing",
    )?;

    for (first, last, want) in [
        (0, 9, false),
        (9, 10, true),
        (15, 15, true),
        (20, 99, false),
    ] {
        let hits = ask(
            catalog.covering(scope, range(first, last)?),
            "covering a sub-range",
        )
        .await?;
        require(
            hits.is_empty() != want,
            &format!("[{first},{last}] must {} the segment [10,19]", touch(want)),
        )?;
    }
    Ok(id)
}

/// Inserts a bucket segment and reads back the key rather than the bytes.
async fn bucket_round_trips<C: Catalog>(catalog: &C, scope: &Scope) -> Result<SegmentId, Failed> {
    let id = SegmentId::fresh();
    let key = scope.key(id);
    let mut segment = written(30, 39, 1, Body::Bucket(key.clone()), 4096)?;
    segment.id = id;
    ask(catalog.insert(scope, segment), "inserting a bucket segment").await?;

    let listed = ask(
        catalog.covering(scope, range(30, 39)?),
        "covering the bucket segment",
    )
    .await?;
    let listed = only(listed, "covering the bucket segment")?;
    require(listed.entry.id == id, "the bucket segment must be listed")?;
    require(
        listed.entry.residence == Residence::Bucket,
        "a bucket segment must report Bucket",
    )?;
    require(
        listed.body == Body::Bucket(key),
        "a bucket segment must report the key it was put at",
    )?;
    require(listed.entry.tier == Tier::new(1), "tier must survive")?;
    Ok(id)
}

async fn bodies_return_what_was_asked<C: Catalog>(
    catalog: &C,
    scope: &Scope,
    inline: SegmentId,
    bucket: SegmentId,
) -> Result<(), Failed> {
    let one = ask(catalog.bodies(scope, &[inline]), "bodies of one").await?;
    require(one.len() == 1, "asking for one body must return one")?;
    require(
        one.first().is_some_and(|listed| listed.entry.id == inline),
        "bodies must return the segment asked for",
    )?;

    let both = ask(catalog.bodies(scope, &[inline, bucket]), "bodies of two").await?;
    require(both.len() == 2, "asking for two bodies must return two")?;

    let none = ask(catalog.bodies(scope, &[]), "bodies of none").await?;
    require(none.is_empty(), "asking for no bodies must return none")
}

/// Both halves of a compaction land, or neither does.
async fn replace_swaps_both_ways<C: Catalog>(
    catalog: &C,
    scope: &Scope,
    inputs: &[SegmentId],
) -> Result<SegmentId, Failed> {
    let output = written(10, 39, 2, Body::Inline(Bytes::from_static(b"merged")), 6)?;
    let id = output.id;
    ask(
        catalog.replace(scope, inputs, output),
        "replacing two with one",
    )
    .await?;

    let entry = only(entries(catalog, scope).await?, "after a replace")?;
    require(entry.id == id, "the replacement must be what is left")?;
    require(
        entry.tier == Tier::new(2),
        "the replacement's tier must survive",
    )?;
    Ok(id)
}

/// Ranges are not disjoint, and a catalog must not assume they are.
///
/// A late write landing inside a range already merged is the case, and a
/// catalog keyed by first key alone is where it would be rejected.
async fn overlapping_ranges_coexist<C: Catalog>(
    catalog: &C,
    scope: &Scope,
    merged: SegmentId,
) -> Result<(), Failed> {
    let late = written(10, 12, 0, Body::Inline(Bytes::from_static(b"late")), 4)?;
    let late_id = late.id;
    ask(
        catalog.insert(scope, late),
        "inserting a segment inside an existing range, at the same first key",
    )
    .await?;

    let listed = ask(
        catalog.covering(scope, range(10, 12)?),
        "covering the overlap",
    )
    .await?;
    require(
        listed.len() == 2,
        "two segments covering one key must both be listed",
    )?;
    let ids: Vec<SegmentId> = listed.iter().map(|one| one.entry.id).collect();
    require(
        ids.contains(&merged) && ids.contains(&late_id),
        "an overlapping insert must not displace what it overlaps",
    )
}

/// Last, since it is the one step that leaves the scope with nothing in it.
async fn purge_empties_one_scope<C: Catalog>(catalog: &C, scope: &Scope) -> Result<(), Failed> {
    let neighbour = Scope {
        id: scope.id.wrapping_add(1),
        prefix: scope.prefix.clone(),
    };
    let kept = written(0, 1, 0, Body::Inline(Bytes::from_static(b"kept")), 4)?;
    ask(
        catalog.insert(&neighbour, kept),
        "listing a neighbour's segment",
    )
    .await?;
    require(
        !entries(catalog, scope).await?.is_empty(),
        "the scope this purges must have something in it first",
    )?;

    ask(catalog.purge(scope.id), "purging a scope").await?;

    require(
        entries(catalog, scope).await?.is_empty(),
        "a purged scope must list no segments",
    )?;
    let covering = ask(
        catalog.covering(scope, KeyRange::EVERYTHING),
        "reading a purged scope",
    )
    .await?;
    require(
        covering.is_empty(),
        "a purged scope must have nothing covering any key",
    )?;
    require(
        entries(catalog, &neighbour).await?.len() == 1,
        "a purge must leave every scope but its own alone",
    )
}

/// Every entry, with the catalog's error turned into a failure.
async fn entries<C: Catalog>(catalog: &C, scope: &Scope) -> Result<Vec<Entry>, Failed> {
    ask(catalog.entries(scope), "listing entries").await
}

/// Awaits a catalog call, naming what was being done if it fails.
async fn ask<T, E: core::fmt::Display>(
    call: impl Future<Output = Result<T, E>>,
    what: &str,
) -> Result<T, Failed> {
    call.await.map_err(|err| Failed(format!("{what}: {err}")))
}

fn require(held: bool, what: &str) -> Result<(), Failed> {
    if held {
        Ok(())
    } else {
        Err(Failed(what.to_owned()))
    }
}

/// The one element of `found`, or a failure naming how many there were.
fn only<T>(found: Vec<T>, what: &str) -> Result<T, Failed> {
    let count = found.len();
    found
        .into_iter()
        .next()
        .filter(|_| count == 1)
        .ok_or_else(|| Failed(format!("{what}: expected one segment, found {count}")))
}

fn written(first: u64, last: u64, tier: u8, body: Body, bytes: u64) -> Result<Written, Failed> {
    Ok(Written {
        id: SegmentId::fresh(),
        range: range(first, last)?,
        tier: Tier::new(tier),
        body,
        bytes,
    })
}

fn range(first: u64, last: u64) -> Result<KeyRange, Failed> {
    KeyRange::new(Key::new(first), Key::new(last))
        .map_err(|err| Failed(format!("building a range for the suite: {err}")))
}

fn touch(want: bool) -> &'static str {
    if want { "touch" } else { "miss" }
}
