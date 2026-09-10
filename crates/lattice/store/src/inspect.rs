//! What a test asks of a catalog beyond what a store needs from one.
//!
//! Over the trait rather than over one implementation, so a question asked of
//! the map is the same question asked of a real database.

#![allow(
    clippy::expect_used,
    reason = "a fixture: a catalog that cannot answer fails the test asking"
)]

use bytes::Bytes;

use crate::{Body, Catalog, Scope, Written};

/// How many segments the scope holds, and how many of those are inlined.
///
/// # Panics
/// When the catalog cannot be read, which is a broken fixture.
pub async fn counts(catalog: &dyn Catalog, scope: &Scope) -> (usize, usize) {
    let entries = entries(catalog, scope).await;
    let inlined = entries
        .iter()
        .filter(|entry| entry.residence == enroute_lattice_core::Residence::Inline)
        .count();
    (entries.len(), inlined)
}

/// The tier of every segment in the scope, lowest first.
///
/// # Panics
/// When the catalog cannot be read, which is a broken fixture.
pub async fn tiers(catalog: &dyn Catalog, scope: &Scope) -> Vec<u8> {
    let mut tiers: Vec<u8> = entries(catalog, scope)
        .await
        .iter()
        .map(|entry| entry.tier.get())
        .collect();
    tiers.sort_unstable();
    tiers
}

/// Replaces one segment's bytes with something that will not decode.
///
/// Through the catalog's own write path, so what a reader meets is a segment
/// the list genuinely names rather than a state only a map could reach.
///
/// # Panics
/// When the catalog cannot be written, which is a broken fixture.
pub async fn corrupt(catalog: &dyn Catalog, scope: &Scope) {
    let Some(entry) = entries(catalog, scope).await.into_iter().next() else {
        return;
    };
    catalog
        .replace(
            scope,
            &[entry.id],
            Written {
                id: entry.id,
                range: entry.range,
                tier: entry.tier,
                body: Body::Inline(Bytes::from_static(b"nonsense")),
                bytes: entry.bytes,
            },
        )
        .await
        .expect("replacing a segment with nonsense");
}

async fn entries(catalog: &dyn Catalog, scope: &Scope) -> Vec<crate::Entry> {
    catalog.entries(scope).await.expect("listing a scope")
}
