//! One capped read of one object, for the two things a process reads.
//!
//! The deployment file and the tenants arrive the same way: a store, a key,
//! and a cap that refuses a URI pointing at the wrong object before its
//! bytes are held. One reader, so the two cannot cap at different sizes.

use anyhow::{Context as _, Result, bail};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore};

/// The largest object either reader takes.
///
/// A URI pointing at the wrong object should not be read into a process's
/// memory to find that out.
pub const MAX_BYTES: u64 = 1 << 20;

/// What one capped read found.
#[derive(Debug)]
pub struct Capped {
    /// The object's bytes, as text.
    pub text: String,
    /// The store's tag for what was read, to ask about next time.
    pub etag: Option<String>,
}

/// Read `path` as text, refusing an object larger than a configuration is.
///
/// `Ok(None)` is a conditional read the store answered as unchanged. The
/// store is the caller's, so polling one does not rebuild it per read.
///
/// # Errors
/// Returns an error if the object cannot be read, is over the cap, or is not
/// UTF-8; `what` and `from` name it in each message.
pub async fn read_capped(
    store: &dyn ObjectStore,
    path: &Path,
    options: GetOptions,
    what: &str,
    from: &str,
) -> Result<Option<Capped>> {
    let read = match store.get_opts(path, options).await {
        Ok(read) => read,
        Err(object_store::Error::NotModified { .. }) => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("reading {what} from {from}")),
    };

    // Off the metadata rather than after reading: the point is not to hold it.
    if read.meta.size > MAX_BYTES {
        bail!(
            "{what} at {from}: {} bytes, over the {MAX_BYTES} this reads",
            read.meta.size
        );
    }
    let etag = read.meta.e_tag.clone();
    let bytes = read
        .bytes()
        .await
        .with_context(|| format!("reading {what} from {from}"))?;
    let text =
        String::from_utf8(bytes.into()).with_context(|| format!("{what} at {from}: not UTF-8"))?;
    Ok(Some(Capped { text, etag }))
}
