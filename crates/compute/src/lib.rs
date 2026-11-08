//! The compute: the role that does the git work. Materialized repository
//! views, git formats (packs, deltas, objects), layer storage (local copies,
//! pages, the file formats), git traffic (smart HTTP, upload-pack,
//! receive-pack), server-side writes and maintenance (flushing inline
//! layers, compaction).

#![deny(unsafe_code)]
// rustc checks this lint only at the crate root, and zerocopy's derives emit a
// non-ASCII identifier.
#![expect(non_ascii_idents, reason = "zerocopy derive output")]

mod access;
mod api;
mod compute;
mod contract;
mod git;
mod hooks;
mod http;
mod maintenance;
mod membership;
mod meta;
mod protocol;
mod push;
mod query;
mod remote;
mod remote_ledger;
mod repo;
mod states;
mod storage;
mod subscriptions;
#[cfg_attr(
    target_os = "macos",
    expect(unsafe_code, reason = "calls with no safe wrapper, all in sys")
)]
mod sys;
mod util;

// The node, and the parts a node assembles it from.
pub use compute::{Compute, ComputeConfig, RepoInfo, RepoPage};
pub use maintenance::compute_id;
pub use membership::{join, join_at, members};
pub use meta::RepoState;
pub use remote_ledger::{LedgerUrls, RemoteLedger};
pub use storage::Storage;
pub use util::init_cpu_pool;

// The routers of the git port and the API port.
pub use api::router as api_router;
pub use http::git_router;

// What the host supplies: access decisions, push hooks, and the reach of
// pushes to other git servers.
pub use access::{Access, Authorize, Challenge, Denial, GitRequest, Grant, RefVisibility};
pub use hooks::{
    Hook, HookClient, HookCommand, HostHooks, Judgement, PreReceiveMode, PushContext, PushHooks,
    RefJudgement,
};
pub use remote::{Client as RemoteClient, Reach};
pub use repo::view::Snapshot;

/// Internals that the workspace's own tests reach into. Not an API.
#[doc(hidden)]
pub mod testing {
    pub use crate::maintenance::settled;
    pub use crate::protocol::{AGENT, pktline};
    pub use crate::query::{Error, PushPack, pack_for_push};
    pub use crate::remote_ledger::client;
    pub use crate::storage::{Sealed, new_layer_id};
    pub use crate::subscriptions::Mux;
}
