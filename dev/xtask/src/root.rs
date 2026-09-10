//! Where the workspace is, answered at run time rather than at compile time.
//!
//! `env!("CARGO_MANIFEST_DIR")` bakes in the directory the compile happened
//! in, which is the right answer under cargo and the wrong one under any
//! build system that compiles somewhere else. `BUILD_WORKSPACE_DIRECTORY` is
//! what such a build system sets, and it wins where it is set.

use std::env;
use std::path::{Path, PathBuf};

/// The workspace root, from whichever build system started this.
pub(crate) fn workspace() -> PathBuf {
    if let Some(dir) = env::var_os("BUILD_WORKSPACE_DIRECTORY") {
        return PathBuf::from(dir);
    }
    let fallback = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    fallback.canonicalize().unwrap_or(fallback)
}
