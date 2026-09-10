//! Every case against the in-memory store, which needs nothing installed.
//!
//! What `cargo test` runs. The same cases against a database as well live in
//! `enroute-postgres`, which is where that store is.

use enroute_git_metadata::{Rows, conformance};

#[tokio::test]
async fn the_in_memory_store_meets_the_contract() {
    conformance::check(&Rows::in_memory()).await;
}
