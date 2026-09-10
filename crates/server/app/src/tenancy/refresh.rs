//! Where a fixed directory is read from, and how it stays current.
//!
//! One object in a store, re-read on a timer, and replaced only when the store
//! says it changed. The conditional read is what makes a poll cheap enough to
//! be the only mechanism: `object_store` checks the precondition itself for the
//! backends that have none of their own, so a bind-mounted file, a `memory://`
//! store in a test and an S3 key all run this same loop. There is nothing to
//! signal and no filesystem watch, which is what a deployment on a read-only
//! mount or behind a projected `ConfigMap` needs anyway. The cost is that
//! revocation is no longer immediate: a tenant taken out of the file keeps
//! serving until each process next reads it, so the refresh interval is the
//! revocation window, and it is the one thing this trades for the rest.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore};
use tokio::sync::watch;

use enroute_config::read_capped;

use crate::ObjectUri;

use super::Directory;

/// Where the tenants are kept.
#[derive(Debug)]
pub struct Source {
    store: Arc<dyn ObjectStore>,
    path: Path,
    /// What the URI said, for saying where something was read from.
    uri: String,
}

/// What one read of a [`Source`] found.
#[derive(Debug)]
pub enum Read {
    /// The store says it holds what was read last.
    Unchanged,
    /// The tenants, and the tag to ask about next time.
    ///
    /// Boxed: a directory is far larger than the other variant, and one
    /// allocation per refresh is not a cost anything here can feel.
    Tenants(Box<Directory>, Option<String>),
}

impl Source {
    /// The tenants named by `uri`.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be built.
    pub fn new(uri: &ObjectUri) -> Result<Self> {
        let (store, path) = uri.build()?;
        Ok(Self {
            store,
            path,
            uri: uri.as_str().to_string(),
        })
    }

    /// Where this reads from, for a log line that has to say.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Read the tenants for the first time, with nothing to compare against.
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be read, is too large, or does
    /// not load as a tenants file.
    pub async fn load(&self) -> Result<(Directory, Option<String>)> {
        match self.read(None).await? {
            Read::Tenants(fixed, tag) => Ok((*fixed, tag)),
            // A conditional read with no condition cannot come back
            // unchanged, but saying so costs less than a type that proves it.
            Read::Unchanged => bail!("{} answered an unconditional read as unchanged", self.uri),
        }
    }

    /// Read the tenants, unless `etag` is what the store still holds.
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be read, is too large, or does
    /// not load as a tenants file.
    pub async fn read(&self, etag: Option<&str>) -> Result<Read> {
        let options = GetOptions {
            if_none_match: etag.map(str::to_string),
            ..GetOptions::default()
        };
        let store = self.store.as_ref();
        let Some(read) = read_capped(store, &self.path, options, "the tenants", &self.uri).await?
        else {
            return Ok(Read::Unchanged);
        };
        let directory = Directory::from_toml(&read.text)
            .with_context(|| format!("the tenants at {}", self.uri))?;

        Ok(Read::Tenants(Box::new(directory), read.etag))
    }
}

/// Re-reads a [`Source`] on a timer, replacing the tenants when they change.
///
/// Holds the only sender, so dropping it is what stops a deployment's tenants
/// from ever changing again.
#[derive(Debug)]
pub struct Refresh {
    source: Source,
    tenants: watch::Sender<Arc<Directory>>,
    tag: Option<String>,
    generation: u64,
}

impl Refresh {
    /// A refresher over `source`, publishing into `tenants`.
    pub(super) fn new(
        source: Source,
        tenants: watch::Sender<Arc<Directory>>,
        tag: Option<String>,
    ) -> Self {
        Self {
            source,
            tenants,
            tag,
            generation: 1,
        }
    }

    /// Read the source every `every`, until the process ends.
    ///
    /// A read that fails changes nothing, because a store that is briefly
    /// unreachable is not a deployment that has no customers.
    pub async fn run(mut self, every: Duration) {
        let mut ticks = tokio::time::interval(every);
        // A read that outruns the interval must not be followed by every tick
        // it missed, all at once, against a store already struggling.
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick is immediate, and the tenants were just read.
        ticks.tick().await;
        if self.tag.is_none() {
            // Without one there is nothing to ask "has this changed" with, so
            // every tick below re-reads and re-parses. Every store this can
            // reach does tag, which is why this says so rather than coping.
            tracing::warn!(
                uri = self.source.uri(),
                "the tenants carry no entity tag, so every refresh re-reads them"
            );
        }
        loop {
            ticks.tick().await;
            if let Err(error) = self.once().await {
                // Loud, because this is the only place a deployment learns
                // that what it published is not what is being served.
                tracing::error!(
                    uri = self.source.uri(),
                    generation = self.generation,
                    "{error:#}, so the tenants already loaded keep serving"
                );
            }
        }
    }

    /// One read, and the swap it may imply.
    async fn once(&mut self) -> Result<()> {
        let (directory, tag) = match self.source.read(self.tag.as_deref()).await? {
            Read::Unchanged => return Ok(()),
            Read::Tenants(directory, tag) => (directory, tag),
        };

        self.tag = tag;
        self.generation += 1;
        let generation = self.generation;
        let tenants = directory.len();
        // `send_replace` rather than `send`: nothing subscribes for changes,
        // every reader borrows the current value, and a send with no live
        // receiver is not a failure here.
        drop(self.tenants.send_replace(Arc::from(directory)));
        tracing::info!(
            uri = self.source.uri(),
            generation,
            tenants,
            "the tenants changed"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt as _;
    use object_store::memory::InMemory;

    use enroute_config::MAX_BYTES;

    use super::*;

    fn one() -> String {
        "[tenants.acme]\n\
         hook_endpoint_url = \"https://acme.example/hooks\"\n\
         domains = [\"*\"]\n"
            .to_string()
    }

    fn two() -> String {
        format!(
            "{}[tenants.other]\n\
             hook_endpoint_url = \"https://other.example/hooks\"\n",
            one()
        )
    }

    /// A source over an in-memory store, which checks the precondition itself
    /// exactly as a local file and a bucket do.
    fn source(store: &Arc<dyn ObjectStore>) -> Source {
        Source {
            store: Arc::clone(store),
            path: Path::from("tenants.toml"),
            uri: "memory:///tenants.toml".to_string(),
        }
    }

    async fn write(store: &Arc<dyn ObjectStore>, toml: &str) {
        store
            .put(&Path::from("tenants.toml"), toml.as_bytes().to_vec().into())
            .await
            .unwrap();
    }

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// The whole point of the conditional read: a poll that found nothing new
    /// costs the store a comparison and this process nothing.
    #[tokio::test]
    async fn an_unchanged_object_is_not_read_again() {
        let store = store();
        write(&store, &one()).await;
        let source = source(&store);

        let Read::Tenants(fixed, tag) = source.read(None).await.unwrap() else {
            panic!("the first read has nothing to compare against");
        };
        assert_eq!(fixed.len(), 1);
        assert!(tag.is_some(), "a store that cannot tag would poll forever");
        assert!(matches!(
            source.read(tag.as_deref()).await.unwrap(),
            Read::Unchanged
        ));
    }

    #[tokio::test]
    async fn a_changed_object_is_read_again() {
        let store = store();
        write(&store, &one()).await;
        let source = source(&store);
        let Read::Tenants(_, tag) = source.read(None).await.unwrap() else {
            panic!("a first read");
        };

        write(&store, &two()).await;
        let Read::Tenants(fixed, next) = source.read(tag.as_deref()).await.unwrap() else {
            panic!("the object changed under the tag");
        };
        assert_eq!(fixed.len(), 2);
        assert_ne!(next, tag);
    }

    /// A file that will not load is an error, not an empty directory.
    ///
    /// The caller keeps what it has, so a deployment cannot lose its tenants
    /// to a bad edit.
    #[tokio::test]
    async fn an_object_that_does_not_load_is_an_error_and_not_an_empty_directory() {
        let store = store();
        write(&store, "[tenants.acme]\ntoken = \"oops\"\n").await;
        let error = format!("{:#}", source(&store).read(None).await.unwrap_err());
        assert!(error.contains("unknown field `token`"), "{error}");
    }

    #[tokio::test]
    async fn an_object_that_is_not_there_is_an_error() {
        source(&store())
            .read(None)
            .await
            .expect_err("a tenants object nobody wrote");
    }

    /// Read off the metadata, so a URI pointing at something enormous is
    /// refused rather than pulled into the front door.
    #[tokio::test]
    async fn an_object_too_large_to_be_tenants_is_refused() {
        let store = store();
        let huge = vec![b'#'; usize::try_from(MAX_BYTES).unwrap() + 1];
        store
            .put(&Path::from("tenants.toml"), huge.into())
            .await
            .unwrap();

        let error = format!("{:#}", source(&store).read(None).await.unwrap_err());
        assert!(error.contains("over the"), "{error}");
    }

    /// A changed list replaces what is serving, and an unchanged one is not
    /// a change.
    #[tokio::test]
    async fn a_refresh_replaces_the_tenants_only_when_they_changed() {
        let store = store();
        write(&store, &one()).await;
        let source = source(&store);
        let (first, tag) = source.load().await.unwrap();
        let (tx, rx) = watch::channel(Arc::new(first));
        let mut refresh = Refresh::new(source, tx, tag);

        // A poll that found nothing new leaves the generation alone, which is
        // what keeps a healthy deployment silent.
        refresh.once().await.unwrap();
        assert_eq!(refresh.generation, 1);
        assert_eq!(rx.borrow().len(), 1);

        write(&store, &two()).await;
        refresh.once().await.unwrap();
        assert_eq!(refresh.generation, 2);
        assert_eq!(rx.borrow().len(), 2);
    }

    /// A list that will not load leaves the one that was serving in place.
    #[tokio::test]
    async fn a_refresh_that_fails_changes_nothing() {
        let store = store();
        write(&store, &one()).await;
        let source = source(&store);
        let (first, tag) = source.load().await.unwrap();
        let (tx, rx) = watch::channel(Arc::new(first));
        let mut refresh = Refresh::new(source, tx, tag);

        write(&store, "[tenants.acme]\n").await;
        refresh.once().await.expect_err("a list with no endpoint");
        assert_eq!(refresh.generation, 1);
        assert_eq!(rx.borrow().len(), 1);
    }
}
