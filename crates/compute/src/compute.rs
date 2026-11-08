//! The compute: materialized repository views, local layer storage,
//! pushes, fetches and maintenance (flushing inline layers, compaction).
//! It follows the replicated logs and proposes commands to them; making a
//! repository active is the ledger's job ([`Activate`]).

use std::sync::Arc;

use anyhow::Result;
use tracing::Instrument as _;

use enroute_shared::proto::RepoSummary;
use enroute_shared::roles::{self, LedgerRef};
use enroute_shared::state::{Command, Commit, DEFAULT_HEAD, LayerMeta, Outcome, Refusal};
use enroute_shared::store::{IoClass, Objects};

use crate::git::oid::ObjectFormat;
use crate::git::refname;
use crate::hooks::HostHooks;
use crate::maintenance::{self, Maintainer};
use crate::meta::RepoState;
use crate::repo::{Repo, Repos};
use crate::states::{Consistency, States};
use crate::storage::{Sealed, Storage};

/// Tuning for a [`Compute`] node.
#[derive(Clone, Debug)]
pub struct ComputeConfig {
    /// Compaction merges this many stored layers of about one size at a
    /// time, so a repository has fewer than this many per size class (under
    /// 2: never compact).
    pub merge_factor: usize,
    /// Layers up to this size go inline: their file travels in the log and
    /// the push skips the object store (0: never).
    pub inline_layer_bytes: u64,
    /// Cap on a repository's inline layers awaiting a flush; beyond it new
    /// layers go to the object store first (backpressure when the object
    /// store is slow or down).
    pub max_inline_bytes: u64,
    /// Merge-before-upload: flush a run of inline layers once its oldest
    /// layer is this old, or once it holds `flush_max_layers` (or
    /// the maintenance module's `FLUSH_BYTES`). Each flush is a manifest
    /// swap in the log, so waiting longer lets one swap carry the layers
    /// of several pushes: with pushes spread over 100k repositories, 30 s
    /// carried ~4 layers a flush and held ~9.8k pushes/s, against ~2.2k/s
    /// at 1 s.
    pub flush_interval: std::time::Duration,
    /// Flush a run of inline layers once it holds this many.
    pub flush_max_layers: usize,
    /// Drop a repository's view (and let its local copies go) once it is
    /// unused this long (and its inline layers are flushed), and stop
    /// following it.
    pub evict_after: std::time::Duration,
    /// How often the layer sweep runs on each repository this node
    /// maintains (see [`crate::maintenance`]): it advances the upload
    /// generation when that lets it delete something, so a publish has
    /// about `UPLOAD_GEN_WINDOW` of these between its upload and its
    /// commit.
    pub sweep_interval: std::time::Duration,
    /// Layer files nothing names are kept this long (by the object
    /// store's clock) before the sweep deletes them: what readers of an
    /// older state (lagging compute nodes, long queries reading the object store)
    /// may take to finish.
    pub sweep_retention: std::time::Duration,
    /// A git push or an API commit to a missing repository creates it.
    /// Without, it is not found, and only [`Compute::create_repo`] creates.
    pub create_on_push: bool,
    /// How a repository is made to name its objects when its creator does
    /// not say: a create on push, or [`Compute::create_repo`].
    pub default_object_format: ObjectFormat,
    /// A push or an API commit with an object larger than this fails, as
    /// does a delta that builds one. The check comes before the object is
    /// inflated, so a size that the pack only declares allocates nothing.
    pub max_object_bytes: u64,
}

impl Default for ComputeConfig {
    fn default() -> ComputeConfig {
        ComputeConfig {
            merge_factor: maintenance::MERGE_FACTOR,
            inline_layer_bytes: 1 << 20,
            max_inline_bytes: 64 << 20,
            flush_interval: std::time::Duration::from_secs(30),
            flush_max_layers: 256,
            evict_after: std::time::Duration::from_mins(15),
            sweep_interval: std::time::Duration::from_secs(3600),
            sweep_retention: std::time::Duration::from_hours(6),
            create_on_push: true,
            default_object_format: ObjectFormat::Sha1,
            max_object_bytes: 100 << 20,
        }
    }
}

/// The error of a request for a repository that does not exist (or was
/// deleted meanwhile).
#[derive(Debug)]
pub(crate) struct RepoNotFound(pub String);

impl std::fmt::Display for RepoNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "repository {} not found", self.0)
    }
}

impl std::error::Error for RepoNotFound {}

/// Whether `e` says a repository does not exist.
pub(crate) fn is_not_found(e: &anyhow::Error) -> bool {
    e.chain()
        .any(<dyn std::error::Error + 'static>::is::<RepoNotFound>)
}

/// A repository, as [`Compute::get_repo`] describes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoInfo {
    /// The repository's name.
    pub name: String,
    /// The branch HEAD names (maybe unborn).
    pub head: String,
    /// When a git push last moved a ref (None: never).
    pub last_push: Option<std::time::SystemTime>,
    /// How it names its objects.
    pub format: ObjectFormat,
}

impl RepoInfo {
    fn of(s: RepoSummary) -> RepoInfo {
        let format = s.object_format().into();
        RepoInfo {
            name: s.name,
            head: s.head,
            last_push: (s.last_push_ms > 0)
                .then(|| std::time::UNIX_EPOCH + std::time::Duration::from_millis(s.last_push_ms)),
            format,
        }
    }
}

/// A page of [`Compute::list_repos`].
#[derive(Clone, Debug, Default)]
pub struct RepoPage {
    /// The repositories on this page, in name order.
    pub repos: Vec<RepoInfo>,
    /// Where the next page starts (pass it as `after`); None: this is the
    /// last page. A page may be short (deleted repositories take places).
    pub next: Option<String>,
}

/// A commit's outcome (see [`Compute::commit`]).
#[derive(Debug)]
pub struct Committed {
    /// The ledger's answer.
    pub outcome: Outcome,
    /// When the layer was published and the command proposed, for timing.
    pub published: std::time::Instant,
}

/// A compute node: it serves the repositories it follows, and runs their
/// maintenance.
pub struct Compute {
    pub(crate) config: ComputeConfig,
    /// The host's hooks.
    pub(crate) hooks: HostHooks,
    /// The repositories this node follows, and their states.
    pub(crate) states: Arc<States>,
    pub(crate) storage: Arc<Storage>,
    repos: Arc<Repos>,
    /// The ledger: makes repositories active, orders their commands.
    ledger: LedgerRef,
    /// Held only to keep maintenance running while this node lives.
    _maintenance: Arc<Maintainer>,
}

impl std::fmt::Debug for Compute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compute")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Compute {
    /// Start the compute: maintenance and eviction of idle views.
    /// `cache_bytes` caps the memory of the repositories' views. The node
    /// is compute node `id`, which shares maintenance with the others the
    /// ledger hears from.
    pub fn start(
        config: ComputeConfig,
        hooks: HostHooks,
        objects: Arc<Objects>,
        storage: Arc<Storage>,
        cache_bytes: usize,
        ledger: LedgerRef,
        id: u64,
    ) -> Arc<Compute> {
        let repos = Repos::new(storage.clone(), cache_bytes);
        let states = States::new(repos.clone(), ledger.clone());
        let maintenance = maintenance::start(
            &config,
            states.clone(),
            repos.clone(),
            storage.clone(),
            objects,
            ledger.clone(),
            maintenance::owners::Owners::new(id),
        );
        Arc::new(Compute {
            config,
            hooks,
            states,
            repos,
            storage,
            ledger,
            _maintenance: maintenance,
        })
    }

    /// The configuration this node runs with.
    #[must_use]
    pub fn config(&self) -> &ComputeConfig {
        &self.config
    }

    /// The latest state of `repo` this node knows (None: it does not
    /// follow it).
    #[must_use]
    pub fn state(&self, repo: &str) -> Option<Arc<RepoState>> {
        self.states.get(repo)
    }

    /// The layer storage this node writes to.
    #[must_use]
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// The repo as of (at least) the latest state this replica knows.
    /// With `linearizable`, first waits for everything committed so far.
    ///
    /// # Errors
    ///
    /// Returns an error if the replica cannot catch up with the log.
    pub async fn repo_for_read(&self, name: &str, linearizable: bool) -> Result<Option<Arc<Repo>>> {
        let consistency = if linearizable {
            Consistency::Latest
        } else {
            Consistency::Known
        };
        self.states.open(name, consistency).await
    }

    /// The repository, created first if it does not exist and the
    /// configuration says pushes create (else [`RepoNotFound`]).
    pub(crate) async fn ensure_repo(&self, name: &str) -> Result<Arc<Repo>> {
        self.states
            .ensure(
                name,
                self.config
                    .create_on_push
                    .then_some(self.config.default_object_format),
            )
            .await?
            .ok_or_else(|| RepoNotFound(name.to_string()).into())
    }

    /// Create `name` with HEAD naming `default_branch` (a branch name or
    /// `refs/heads/...`; empty: `refs/heads/main`), unless it exists. Answers
    /// with what the repository has (an existing one is left as it is). A
    /// new repository is in [`ComputeConfig::default_object_format`]; see
    /// [`Compute::create_repo_in`].
    ///
    /// # Errors
    ///
    /// Returns an error if the name or default branch is invalid, or if the
    /// ledger refuses the creation.
    pub async fn create_repo(&self, name: &str, default_branch: &str) -> Result<RepoInfo> {
        self.create_repo_in(name, default_branch, self.config.default_object_format)
            .await
    }

    /// [`Compute::create_repo`], with a new repository's objects named in
    /// `format`.
    ///
    /// # Errors
    ///
    /// As `create_repo`, and the ledger refuses SHA-256 unless its
    /// `ledger.allow_sha256` is on.
    pub async fn create_repo_in(
        &self,
        name: &str,
        default_branch: &str,
        format: ObjectFormat,
    ) -> Result<RepoInfo> {
        anyhow::ensure!(
            crate::contract::repo_key::RepoKey::is_valid(name),
            "invalid repository name {name:?}"
        );
        let head = match default_branch {
            "" => DEFAULT_HEAD.to_string(),
            b if b.starts_with("refs/") => b.to_string(),
            b => format!("refs/heads/{b}"),
        };
        anyhow::ensure!(
            refname::valid_branch(&head),
            "invalid default branch {default_branch:?}"
        );
        roles::create(&*self.ledger, name, &head, format).await?;
        self.get_repo(name)
            .await?
            .ok_or_else(|| RepoNotFound(name.to_string()).into())
    }

    /// `name` as of everything committed before the call (None: it does
    /// not exist).
    ///
    /// # Errors
    ///
    /// Returns an error if the ledger cannot answer.
    pub async fn get_repo(&self, name: &str) -> Result<Option<RepoInfo>> {
        Ok(roles::describe(&*self.ledger, name)
            .await?
            .map(RepoInfo::of))
    }

    /// Repositories named with `prefix`, after `after` (empty: from the
    /// first), in byte order of their names: at most `limit` (1 to 1000).
    /// Includes every repository created before the call, and none deleted
    /// before it.
    ///
    /// # Errors
    ///
    /// Returns an error if the ledger cannot answer.
    pub async fn list_repos(&self, prefix: &str, after: &str, limit: u32) -> Result<RepoPage> {
        let list = roles::list(&*self.ledger, prefix, after, limit.clamp(1, 1000)).await?;
        let next = list
            .more
            .then(|| list.repos.last().map(|r| r.name.clone()))
            .flatten();
        let repos = list
            .repos
            .into_iter()
            .filter(|r| !r.deleted)
            .map(RepoInfo::of)
            .collect();
        Ok(RepoPage { repos, next })
    }

    /// Delete `name` (a missing or deleted one too). Its name is free at
    /// once; its layer files are reclaimed later (see
    /// [`crate::maintenance`]).
    ///
    /// # Errors
    ///
    /// Returns an error if the ledger refuses the deletion.
    pub async fn delete_repo(&self, name: &str) -> Result<()> {
        roles::delete(&*self.ledger, name).await?;
        // Other compute nodes drop theirs as the deletion reaches them.
        self.states.unfollow(name);
        Ok(())
    }

    /// Propose `cmd` to its repository's log (which must be live): it
    /// neither waits for the outcome nor retries.
    ///
    /// # Errors
    ///
    /// Returns an error if the ledger cannot take the proposal.
    pub async fn propose(&self, cmd: Command) -> Result<Outcome> {
        roles::propose(&*self.ledger, cmd).await
    }

    /// The manifest entry for a freshly built layer of `repo`: inline (its
    /// file in the log) if it is small and the repository's inline backlog
    /// allows, else uploaded to the object store first. Without ids: the
    /// commit adding it assigns them.
    ///
    /// # Errors
    ///
    /// Returns an error if the layer cannot be uploaded.
    pub async fn publish_layer(
        &self,
        repo: &str,
        sealed: Sealed,
        objects: u32,
    ) -> Result<LayerMeta> {
        let state = self.states.get(repo);
        let generation = state.as_ref().map_or(0, |m| m.upload_gen);
        let (mut meta, bytes) = (sealed.meta(objects, generation), sealed.len());
        let backlog: u64 = state.map_or(0, |m| {
            m.layers
                .iter()
                .filter(|l| !l.is_stored())
                .map(|l| l.bytes)
                .sum()
        });
        if bytes <= self.config.inline_layer_bytes
            && backlog + bytes <= self.config.max_inline_bytes
        {
            meta.data = Some(sealed.into_bytes().await?);
            return Ok(meta);
        }
        self.storage.publish_layer(repo, &meta.id, sealed).await?;
        Ok(meta)
    }

    /// Commit `commit` to incarnation `epoch` of `repo`, with `layer` (a
    /// freshly built layer) published and appended to its `add_layers` if
    /// given. Returns once the ledger answers: a following read sees the
    /// outcome through any node, because reads take a read barrier.
    /// [`RepoNotFound`] if the repository was deleted. An unstamped commit
    /// gets this node's clock as its refs' update time. If a stored layer was
    /// published in an upload generation that has since expired (a slow
    /// publish), copy it within the object store under the current
    /// generation and propose again, once; the old copy is left to the layer
    /// sweep.
    ///
    /// # Errors
    ///
    /// Returns an error if the repository was deleted, or if a layer cannot be
    /// published or copied.
    pub async fn commit(
        &self,
        repo: &str,
        epoch: u64,
        layer: Option<(Sealed, u32)>,
        mut commit: Commit,
    ) -> Result<Committed> {
        if commit.updated_at_ms == 0 && !commit.updates.is_empty() {
            commit.updated_at_ms = crate::util::now_ms();
        }
        if let Some((data, objects)) = layer {
            let publish = self.publish_layer(repo, data, objects);
            let span = tracing::info_span!("enroute::commit::publish_layer", objects);
            commit
                .add_layers
                .push(IoClass::Commit.scope(publish).instrument(span).await?);
        }
        let published = std::time::Instant::now();
        let propose = |commit: Commit| async move {
            let out = self
                .propose(Command::commit(repo, commit).at(epoch))
                .instrument(tracing::info_span!("enroute::commit::propose"))
                .await?;
            match out.refused() {
                Some(Refusal::Deleted | Refusal::OtherIncarnation) => {
                    Err(RepoNotFound(repo.to_string()).into())
                }
                Some(Refusal::Invalid | Refusal::StaleUpload | Refusal::Unspecified) | None => {
                    anyhow::Ok(out)
                }
            }
        };
        let mut outcome = propose(commit.clone()).await?;
        match outcome.refused() {
            Some(Refusal::StaleUpload) => {
                // This node's state has the generation that refused the commit.
                self.states.follow_active(repo).await?;
                self.repos
                    .handle(repo)
                    .wait_version(outcome.repo_version)
                    .await?;
                let generation = self.states.get(repo).map_or(0, |m| m.upload_gen);
                for l in commit.add_layers.iter_mut().filter(|l| l.is_stored()) {
                    let id = crate::storage::new_layer_id(generation);
                    let copy = self.storage.copy_layer(repo, &l.id, &id);
                    IoClass::Commit.scope(copy).await?;
                    tracing::info!(
                        repo,
                        from = l.id,
                        to = id,
                        "republished a layer of an expired upload generation"
                    );
                    l.id = id;
                }
                outcome = propose(commit).await?;
            }
            Some(
                Refusal::Invalid
                | Refusal::Deleted
                | Refusal::OtherIncarnation
                | Refusal::Unspecified,
            )
            | None => {}
        }
        Ok(Committed { outcome, published })
    }

    /// A JSON summary of the node: its repositories, their views and storage.
    #[must_use]
    pub fn status(&self) -> serde_json::Value {
        let names = self.repos.names();
        // Where each repository's view stands (a few: for diagnosis).
        let detail: serde_json::Map<String, serde_json::Value> = names
            .iter()
            .take(32)
            .filter_map(|n| {
                // Not `handle`: a status read must not make a view.
                let mut d = self.repos.get(n)?.status();
                #[expect(
                    clippy::indexing_slicing,
                    reason = "status() builds an object; IndexMut on it cannot fail"
                )]
                {
                    d["followed"] = serde_json::json!(self.states.get(n).map(|m| m.version));
                }
                Some((n.clone(), d))
            })
            .collect();
        serde_json::json!({
            "repos": names.len(),
            "views": detail,
            "storage": self.storage.status(),
        })
    }
}
