//! What a deployment is, as one file.
//!
//! Every value a process needs before it can serve. Every table refuses what
//! it does not know, since a typo that is ignored is a setting that silently
//! did nothing. Read once, at startup, because what is here decides what gets
//! *built* — a store, a listener, a node — and rebuilding those under live
//! requests is what a restart is for. A credential is *named* here and need
//! not be held here — a [`Secret`] is usually a `${VAR}` — so the file stays
//! something to commit, diff and roll back, and nothing that holds one will
//! print it.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use bytesize::ByteSize;
use object_store::memory::InMemory;
use serde::Deserialize;

use enroute_shared::oid::ObjectFormat;
use enroute_shared::store::IoConfig;

use crate::config::{Bucket, Hook, ObjectUri, PreReceiveMode, Secret, expand};
use crate::node::{self, Role, Store};

/// A deployment, as its file names it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where a repository's permanent objects live.
    pub bucket: Bucket,
    /// The node this process runs.
    pub node: Node,
    /// How layers are written, merged and deleted.
    #[serde(default)]
    pub layers: Layers,
    /// The replicated logs.
    #[serde(default)]
    pub ledger: Ledger,
    /// The node's concurrent requests to `bucket`, shared by kinds of work.
    ///
    /// A kind may always use its `reserve`, never more than its `limit`, and
    /// shares the rest by `weight`.
    #[serde(default)]
    pub store_io: IoConfig,
    /// Where each listener binds.
    #[serde(default)]
    pub listen: Listen,
    /// Which hooks Enroute calls, which is none by default.
    #[serde(default)]
    pub hooks: Hooks,
    /// How pushes behave, to Enroute and from it.
    #[serde(default)]
    pub git: Git,
    /// Where spans go, if anywhere.
    #[serde(default)]
    pub telemetry: Option<Telemetry>,
}

/// The node this process runs.
///
/// Its object store is `bucket`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Node {
    /// What the node runs.
    #[serde(default)]
    pub role: Role,
    /// Local disk for the node's logs, its state and its layer copies.
    ///
    /// The node's identity is its log, so a node that loses this directory
    /// comes back as a new node.
    #[serde(deserialize_with = "expand::parsed::deserialize")]
    pub data_dir: PathBuf,
    /// Where other nodes and operators reach this one.
    ///
    /// It serves `GET /status`, and on a replicated ledger node the
    /// gRPC between nodes.
    #[serde(
        default = "Node::internal_listen",
        deserialize_with = "expand::parsed::deserialize"
    )]
    pub internal_listen: SocketAddr,
    /// The URL of `internal_listen` for other nodes.
    ///
    /// Set it to replicate: nodes that share `bucket` find each other
    /// through it.
    #[serde(default, deserialize_with = "optional_url")]
    pub advertise_url: Option<url::Url>,
    /// The failure domain this node is in, such as an availability zone.
    ///
    /// A ledger node's repository logs keep their replicas in distinct
    /// zones where the members allow it. Unset: no zone.
    #[serde(default, deserialize_with = "expand::optional::deserialize")]
    pub zone: Option<String>,
    /// Workers for CPU-bound work, shared by all requests (0: one per core).
    #[serde(default = "Node::cpu_threads")]
    pub cpu_threads: usize,
    /// The decoded object and entry caches.
    #[serde(default = "Node::cache")]
    pub cache: ByteSize,
    /// The buffer pool that layer files are read through.
    #[serde(default = "Node::page_cache")]
    pub page_cache: ByteSize,
    /// Local disk for layers read from `bucket` (0: no limit).
    #[serde(default = "Node::disk_cache")]
    pub disk_cache: ByteSize,
    /// Files that layer storage may keep open (unset: the system's limit
    /// less a reserve of the larger of 256 and a quarter of it).
    #[serde(default)]
    pub max_open_files: Option<usize>,
    /// Drop a repository's view once it is unused this long.
    #[serde(default = "Node::evict_after", with = "humantime_serde")]
    pub evict_after: Duration,
    /// Console lines that may wait to be written before new ones are dropped.
    #[serde(default = "Node::console_buffer_lines")]
    pub console_buffer_lines: NonZeroUsize,
}

/// The engine's own defaults, which are the only statement of them.
fn defaults() -> node::Config {
    node::Config::local(PathBuf::new(), Store::new(Arc::new(InMemory::new())))
}

impl Node {
    /// The port that the node itself uses.
    fn internal_listen() -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], 7400))
    }

    fn cpu_threads() -> usize {
        defaults().cpu_threads
    }

    fn cache() -> ByteSize {
        size(defaults().cache_bytes)
    }

    fn page_cache() -> ByteSize {
        size(defaults().page_cache_bytes)
    }

    fn disk_cache() -> ByteSize {
        ByteSize(defaults().disk_cache_bytes)
    }

    /// tracing-appender's own default: at a few hundred bytes a line, tens
    /// of MB that a stalled stdout can hold.
    fn console_buffer_lines() -> NonZeroUsize {
        NonZeroUsize::new(tracing_appender::non_blocking::DEFAULT_BUFFERED_LINES_LIMIT)
            .unwrap_or(NonZeroUsize::MIN)
    }

    fn evict_after() -> Duration {
        defaults().compute.evict_after
    }
}

/// How layers are written, merged and deleted.
///
/// A small push travels in the log and is flushed to `bucket` later. The
/// sweep deletes the layer files that nothing names.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Layers {
    /// Merge this many stored layers of about one size at a time (0: never).
    ///
    /// Higher rewrites each byte less often, and leaves more layers to check.
    pub merge_factor: usize,
    /// A push whose layer is at most this large travels in the log (0:
    /// every push uploads first).
    pub inline_layer: ByteSize,
    /// The size of a repository's inline layers that wait for a flush; past
    /// this, a push uploads first.
    pub max_inline: ByteSize,
    /// Flush the inline layers once the oldest is this old.
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
    /// Flush the inline layers once there are this many.
    ///
    /// More per flush means fewer view rebuilds, and a longer inline tail.
    pub flush_max_layers: usize,
    /// How often the sweep runs on each repository.
    #[serde(with = "humantime_serde")]
    pub sweep_interval: Duration,
    /// How long a layer file that nothing names is kept before the sweep
    /// deletes it.
    ///
    /// The time a reader of an older state may need to finish.
    #[serde(with = "humantime_serde")]
    pub sweep_retention: Duration,
}

impl Default for Layers {
    fn default() -> Self {
        let compute = defaults().compute;
        Self {
            merge_factor: compute.merge_factor,
            inline_layer: ByteSize(compute.inline_layer_bytes),
            max_inline: ByteSize(compute.max_inline_bytes),
            flush_interval: compute.flush_interval,
            flush_max_layers: compute.flush_max_layers,
            sweep_interval: compute.sweep_interval,
            sweep_retention: compute.sweep_retention,
        }
    }
}

/// The replicated logs, on a node that runs the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Ledger {
    /// The state store's block cache and memtables.
    pub state_memory: ByteSize,
    /// The largest inline layer the ledger accepts.
    ///
    /// The ledger refuses a push that carries a larger layer. With 0, no
    /// repository content passes through it.
    pub max_inline_layer: ByteSize,
    /// Create repositories whose objects are named in SHA-256.
    ///
    /// Off by default: turn it on once every node runs a release that reads
    /// SHA-256 repositories.
    pub allow_sha256: bool,
}

impl Default for Ledger {
    fn default() -> Self {
        let defaults = defaults();
        Self {
            state_memory: size(defaults.ledger.state_memory_bytes),
            max_inline_layer: ByteSize(defaults.ledger.max_inline_layer_bytes),
            allow_sha256: defaults.ledger.allow_sha256,
        }
    }
}

fn size(bytes: usize) -> ByteSize {
    ByteSize(u64::try_from(bytes).unwrap_or(u64::MAX))
}

/// `size` as the engine counts it.
///
/// # Errors
///
/// Returns an error naming `key` if `size` does not fit.
fn bytes(size: ByteSize, key: &str) -> Result<usize> {
    usize::try_from(size.as_u64()).with_context(|| format!("{key} is too large"))
}

/// A node's internal URL as nodes name it: without a trailing `/`.
fn internal_url(url: &url::Url) -> String {
    url.as_str().trim_end_matches('/').to_string()
}

/// An optional URL, expanded and parsed when it is there.
///
/// # Errors
///
/// Returns an error if the value does not parse as a URL.
fn optional_url<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<url::Url>, D::Error> {
    expand::optional::deserialize(deserializer)?
        .map(|raw| raw.parse().map_err(serde::de::Error::custom))
        .transpose()
}

/// Where spans are exported, over OTLP.
///
/// Absent means the console alone, which is what a deployment with nowhere to
/// send them should get rather than an exporter pointed at nothing.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Telemetry {
    /// The collector's trace endpoint, in full.
    ///
    /// The whole path, as `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is: nothing
    /// appends `/v1/traces`, so what is written is what is dialed.
    #[serde(deserialize_with = "expand::string::deserialize")]
    pub endpoint: String,
    /// What each export carries, which is how a collector knows the caller.
    ///
    /// A [`Secret`], because this is where a token goes: `authorization =
    /// "Bearer ${OTEL_TOKEN}"`. Nothing here is specific to one vendor.
    #[serde(default, deserialize_with = "header_names")]
    pub headers: BTreeMap<String, Secret>,
    /// What fraction of traces to keep, from 0.0 to 1.0.
    ///
    /// Head-based, and parent-based above that: traffic is too low yet to
    /// justify tail-based sampling's buffering cost.
    #[serde(default = "Telemetry::sample_ratio", deserialize_with = "ratio")]
    pub sample_ratio: f64,
}

/// Header names, refused here rather than by the export that stops happening.
///
/// # Errors
///
/// Returns an error if a key is not a name the HTTP layer would accept.
fn header_names<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, Secret>, D::Error> {
    let named = BTreeMap::<String, Secret>::deserialize(deserializer)?;
    for name in named.keys() {
        drop(
            name.parse::<http::HeaderName>()
                .map_err(serde::de::Error::custom)?,
        );
    }
    Ok(named)
}

/// A fraction, refused where it is written rather than silently clamped.
///
/// # Errors
///
/// Returns an error if the value is not between 0.0 and 1.0.
fn ratio<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    let ratio = f64::deserialize(deserializer)?;
    if (0.0..=1.0).contains(&ratio) {
        return Ok(ratio);
    }
    Err(serde::de::Error::custom(format!(
        "{ratio} is not a fraction of traces to keep: 0.0 to 1.0"
    )))
}

/// Where each listener binds, since two callers reach two ports.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Listen {
    /// Where the contract is served, for an application to call.
    #[serde(deserialize_with = "expand::parsed::deserialize")]
    pub api: SocketAddr,
    /// Where git is served, for a git client to clone and push against.
    #[serde(deserialize_with = "expand::parsed::deserialize")]
    pub git: SocketAddr,
}

/// Which hooks Enroute calls, where, and what each call is signed with.
///
/// Every hook is opt-in. A hook not in `enabled` takes its default: see
/// docs/reference/hooks.md.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hooks {
    /// The hooks to call.
    #[serde(default)]
    pub enabled: BTreeSet<Hook>,
    /// Where the application answers, required when a hook is enabled.
    #[serde(default, deserialize_with = "optional_url")]
    pub endpoint_url: Option<url::Url>,
    /// The Ed25519 private key, in PEM, that every call is signed with.
    ///
    /// Required when a hook is enabled. Enroute holds this half and an
    /// application holds only the public one — see docs/operate/security.md.
    #[serde(default)]
    pub signing_key: Option<Secret>,
    /// How long an application has to answer before its git request fails.
    ///
    /// Generous by default for a cold serverless start.
    #[serde(default = "Hooks::timeout", with = "humantime_serde")]
    pub timeout: Duration,
    /// What `pre_receive` can see: `commands` or `objects`.
    ///
    /// `objects` costs a second log entry per push. Ignored, without a
    /// warning, unless `pre_receive` is enabled.
    #[serde(default)]
    pub pre_receive_mode: PreReceiveMode,
}

/// How pushes to Enroute, and from it to a remote, behave.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Git {
    /// A push to a missing repository creates it.
    pub create_on_push: bool,
    /// The object format of a repository whose creator names none: one a
    /// push creates, or `CreateRepository` without `object_format`.
    pub default_object_format: ObjectFormat,
    /// Let `SyncService.PushToRemote` and `FetchFromRemote` dial `http://`, and
    /// hosts that answer on an address off the public internet.
    ///
    /// For a development stack, and for a git server inside your own network.
    pub allow_private_remotes: bool,
    /// A push with an object larger than this fails, as does an API commit.
    pub max_object_size: ByteSize,
}

impl Default for Git {
    fn default() -> Self {
        Self {
            create_on_push: true,
            default_object_format: ObjectFormat::Sha1,
            allow_private_remotes: false,
            max_object_size: ByteSize(defaults().compute.max_object_bytes),
        }
    }
}

impl Config {
    /// The deployment `uri` names.
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be read, is too large, or does
    /// not load as a configuration.
    pub async fn read(uri: &ObjectUri) -> Result<Self> {
        crate::config::read::read_capped(uri)
            .await?
            .parse()
            .with_context(|| format!("the configuration at {uri}"))
    }

    /// What no field can say on its own.
    fn check(&self) -> Result<()> {
        anyhow::ensure!(
            self.node.role != Role::Ledger || self.node.advertise_url.is_some(),
            "node.role \"ledger\" needs node.advertise_url: \
             a ledger node serves no other node without it"
        );
        self.store_io.check().context("in store_io")?;
        anyhow::ensure!(
            self.layers.flush_max_layers > 0,
            "layers.flush_max_layers must be at least 1"
        );
        anyhow::ensure!(
            self.layers.merge_factor != 1,
            "layers.merge_factor must be 0 (never merge) or at least 2"
        );
        anyhow::ensure!(
            self.node.role != Role::Both
                || self.layers.inline_layer <= self.ledger.max_inline_layer,
            "layers.inline_layer must be at most ledger.max_inline_layer: \
             the ledger refuses every push that carries a larger layer"
        );
        anyhow::ensure!(
            self.node.role != Role::Both
                || self.ledger.allow_sha256
                || self.git.default_object_format != ObjectFormat::Sha256,
            "git.default_object_format = \"sha256\" needs ledger.allow_sha256 = true: \
             the ledger refuses every create in that format"
        );
        anyhow::ensure!(
            !self.hooks.timeout.is_zero(),
            "hooks.timeout must be longer than zero"
        );
        self.hooks.called().map(drop)
    }

    /// The node these tables describe, with its object store built.
    ///
    /// # Errors
    ///
    /// Returns an error if a size is too large, or if the store cannot be
    /// built.
    pub fn node(&self) -> Result<node::Config> {
        let mut config = self.node.engine(&self.bucket, &self.layers, &self.ledger)?;
        config.io = self.store_io;
        config.compute.create_on_push = self.git.create_on_push;
        config.compute.default_object_format = self.git.default_object_format;
        config.compute.max_object_bytes = self.git.max_object_size.as_u64();
        Ok(config)
    }

    /// The hook endpoint, or `None` when no hook is enabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the signing key is not an Ed25519 PKCS#8 PEM.
    pub fn endpoint(&self) -> Result<Option<crate::Endpoint>> {
        let hooks = &self.hooks;
        let Some((enabled, url, key)) = hooks.called()? else {
            return Ok(None);
        };
        let signing_key = enroute_signature::SigningKey::from_pem(key.expose())
            .map_err(|error| anyhow::anyhow!("hooks.signing_key: {error}"))?;
        Ok(Some(crate::Endpoint {
            url: url.clone(),
            signing_key,
            timeout: hooks.timeout,
            enabled,
            pre_receive_mode: hooks.pre_receive_mode,
        }))
    }
}

impl std::str::FromStr for Config {
    type Err = anyhow::Error;

    /// The deployment a TOML string names.
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not parse, or if a value it names is
    /// not one of the thing it names.
    fn from_str(toml: &str) -> Result<Self> {
        // No context of its own: the caller knows where the bytes came from,
        // and "the configuration" twice in one chain says nothing twice.
        let config: Self = toml::from_str(toml)?;
        config.check()?;
        Ok(config)
    }
}

impl Hooks {
    /// The hooks to call, where, and the key to sign with, or `None` when no
    /// hook is enabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the keys do not agree.
    fn called(&self) -> Result<Option<(BTreeSet<Hook>, &url::Url, &Secret)>> {
        if self.enabled.is_empty() {
            return Ok(None);
        }
        let url = self
            .endpoint_url
            .as_ref()
            .context("hooks.endpoint_url is required when a hook is enabled")?;
        let key = self
            .signing_key
            .as_ref()
            .context("hooks.signing_key is required when a hook is enabled")?;
        Ok(Some((self.enabled.clone(), url, key)))
    }
}

impl Node {
    /// The node this table describes, with its objects in `bucket`.
    ///
    /// # Errors
    ///
    /// Returns an error if a size is too large, or if the store cannot be
    /// built.
    pub fn engine(
        &self,
        bucket: &Bucket,
        layers: &Layers,
        ledger: &Ledger,
    ) -> Result<node::Config> {
        let table = self;
        let mut config = node::Config::local(table.data_dir.clone(), bucket.store()?);
        config.role = table.role;
        config.cpu_threads = table.cpu_threads;
        config.cache_bytes = bytes(table.cache, "node.cache")?;
        config.page_cache_bytes = bytes(table.page_cache, "node.page_cache")?;
        config.disk_cache_bytes = table.disk_cache.as_u64();
        config.max_open_files = table.max_open_files;
        config.ledger.advertise = table.advertise_url.as_ref().map(internal_url);
        config.ledger.zone = table.zone.clone().unwrap_or_default();
        config.ledger.state_memory_bytes = bytes(ledger.state_memory, "ledger.state_memory")?;
        config.ledger.max_inline_layer_bytes = ledger.max_inline_layer.as_u64();
        config.ledger.allow_sha256 = ledger.allow_sha256;
        let compute = &mut config.compute;
        compute.evict_after = table.evict_after;
        compute.merge_factor = layers.merge_factor;
        compute.inline_layer_bytes = layers.inline_layer.as_u64();
        compute.max_inline_bytes = layers.max_inline.as_u64();
        compute.flush_interval = layers.flush_interval;
        compute.flush_max_layers = layers.flush_max_layers;
        compute.sweep_interval = layers.sweep_interval;
        compute.sweep_retention = layers.sweep_retention;
        Ok(config)
    }
}

impl Default for Listen {
    /// Every interface, which is what a container needs — loopback inside one
    /// is reachable from nothing.
    fn default() -> Self {
        Self {
            api: SocketAddr::from(([0, 0, 0, 0], 50051)),
            git: SocketAddr::from(([0, 0, 0, 0], 8080)),
        }
    }
}

impl Default for Hooks {
    fn default() -> Self {
        Self {
            enabled: BTreeSet::new(),
            endpoint_url: None,
            signing_key: None,
            timeout: Hooks::timeout(),
            pre_receive_mode: PreReceiveMode::Commands,
        }
    }
}

impl Hooks {
    /// Ten seconds, which a cold serverless start fits inside.
    fn timeout() -> Duration {
        Duration::from_secs(10)
    }
}

impl Telemetry {
    /// Every trace, which is the right default until traffic says otherwise.
    fn sample_ratio() -> f64 {
        1.0
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use enroute_shared::store::Share;

    use super::*;

    /// The least a file can say and still name a deployment.
    fn minimal() -> String {
        format!("bucket.uri = \"s3://objects\"\n{REQUIRED}")
    }

    /// The two tables every fixture needs, since both are required and
    /// neither is what the test is about.
    const REQUIRED: &str = "[node]\ndata_dir = \"/var/lib/enroute/node\"\n\
                            [hooks]\nenabled = [\"authorize\"]\nsigning_key = \"pem\"\n\
                            endpoint_url = \"https://app.example/hooks\"\n";

    /// The whole chain: the outer context alone says only "the
    /// configuration", which is not what any of these assert on.
    fn refused(toml: &str) -> String {
        format!("{:#}", Config::from_str(&whole(toml)).unwrap_err())
    }

    /// A fixture with the two required tables on the end, so each test writes
    /// only the part it is about.
    fn whole(toml: &str) -> String {
        format!("{toml}{REQUIRED}")
    }

    #[test]
    fn the_tables_a_file_leaves_out_are_the_documented_defaults() {
        let config = Config::from_str(&minimal()).unwrap();
        assert_eq!(config.listen.api.port(), 50051);
        assert_eq!(config.listen.git.port(), 8080);
        assert_eq!(config.hooks.timeout, Duration::from_secs(10));
        assert_eq!(config.hooks.pre_receive_mode, PreReceiveMode::Commands);
        assert!(config.telemetry.is_none());
        assert!(!config.git.allow_private_remotes);

        let node = &config.node;
        assert_eq!(node.role, Role::Both);
        assert_eq!(node.data_dir, PathBuf::from("/var/lib/enroute/node"));
        assert_eq!(node.internal_listen.port(), 7400);
        assert!(node.advertise_url.is_none());
        assert_eq!(
            (node.cache, node.page_cache),
            (ByteSize::mib(512), ByteSize::mib(256))
        );
        assert_eq!(node.disk_cache, ByteSize(0));
        assert_eq!(node.max_open_files, None);
        assert_eq!(node.console_buffer_lines.get(), 128_000);
        assert_eq!(
            (node.cpu_threads, node.evict_after),
            (0, Duration::from_mins(15))
        );

        let layers = &config.layers;
        assert_eq!(layers.merge_factor, 8);
        assert_eq!(
            (layers.inline_layer, layers.max_inline),
            (ByteSize::mib(1), ByteSize::mib(64))
        );
        assert_eq!(
            (layers.flush_interval, layers.flush_max_layers),
            (Duration::from_secs(30), 256)
        );
        assert_eq!(
            (layers.sweep_interval, layers.sweep_retention),
            (Duration::from_hours(1), Duration::from_hours(6))
        );

        let ledger = &config.ledger;
        assert_eq!(ledger.state_memory, ByteSize::mib(512));
        assert_eq!(ledger.max_inline_layer, ByteSize::mib(1));

        let io = config.store_io;
        assert_eq!(io.slots, 256);
        assert_eq!((io.commit.reserve, io.background.limit), (16, 128));
    }

    /// A key that a file leaves out takes the engine's own value, not a copy.
    #[test]
    fn a_file_that_names_no_engine_key_builds_the_engines_defaults() {
        let toml = minimal().replace("s3://objects", "memory:///objects");
        let built = Config::from_str(&toml).unwrap().node().unwrap();
        let engine = defaults();

        assert_eq!(built.role, engine.role);
        assert_eq!(built.cpu_threads, engine.cpu_threads);
        assert_eq!(built.cache_bytes, engine.cache_bytes);
        assert_eq!(built.page_cache_bytes, engine.page_cache_bytes);
        assert_eq!(built.disk_cache_bytes, engine.disk_cache_bytes);
        assert_eq!(built.max_open_files, engine.max_open_files);
        assert_eq!(built.io, engine.io);
        assert_eq!(built.ledger.advertise, None);
        assert_eq!(built.ledger.zone, "");
        assert_eq!(
            built.ledger.state_memory_bytes,
            engine.ledger.state_memory_bytes
        );
        let (compute, defaults) = (&built.compute, &engine.compute);
        assert_eq!(compute.merge_factor, defaults.merge_factor);
        assert_eq!(compute.inline_layer_bytes, defaults.inline_layer_bytes);
        assert_eq!(compute.max_inline_bytes, defaults.max_inline_bytes);
        assert_eq!(compute.flush_interval, defaults.flush_interval);
        assert_eq!(compute.flush_max_layers, defaults.flush_max_layers);
        assert_eq!(compute.evict_after, defaults.evict_after);
        assert_eq!(compute.sweep_interval, defaults.sweep_interval);
        assert_eq!(compute.sweep_retention, defaults.sweep_retention);
    }

    #[test]
    fn an_engine_key_a_file_names_reaches_the_engine() {
        let toml = format!(
            "bucket.uri = \"memory:///objects\"\n\
             [layers]\nflush_interval = \"300ms\"\ninline_layer = 0\n\
             [ledger]\nstate_memory = \"64 MiB\"\nmax_inline_layer = 0\n\
             allow_sha256 = true\n\
             [store_io]\nslots = 64\nbackground = {{ reserve = 4, limit = 8, weight = 1 }}\n\
             [node]\ndata_dir = \"/data\"\ncache = \"64 MiB\"\n\
             advertise_url = \"http://10.0.0.1:7400/\"\nzone = \"eu-central-1a\"\n{}",
            REQUIRED.replace("[node]\ndata_dir = \"/var/lib/enroute/node\"\n", "")
        );
        let built = Config::from_str(&toml).unwrap().node().unwrap();
        assert_eq!(built.ledger.zone, "eu-central-1a");

        assert_eq!(built.compute.flush_interval, Duration::from_millis(300));
        assert_eq!(built.compute.inline_layer_bytes, 0);
        assert_eq!(built.ledger.state_memory_bytes, 64 << 20);
        assert_eq!(built.ledger.max_inline_layer_bytes, 0);
        assert!(built.ledger.allow_sha256);
        assert_eq!(built.cache_bytes, 64 << 20);
        assert_eq!(built.io.slots, 64);
        assert_eq!(
            built.io.background,
            Share {
                limit: 8,
                ..defaults().io.background
            }
        );
        assert_eq!(
            built.ledger.advertise.as_deref(),
            Some("http://10.0.0.1:7400")
        );
    }

    #[test]
    fn a_flush_of_no_layers_is_refused() {
        let error = refused("bucket.uri = \"s3://objects\"\n[layers]\nflush_max_layers = 0\n");
        assert!(error.contains("flush_max_layers"), "{error}");
    }

    #[test]
    fn store_io_shares_that_do_not_fit_are_refused() {
        let error = refused("bucket.uri = \"s3://objects\"\n[store_io]\nslots = 16\n");
        assert!(error.contains("reservations"), "{error}");
        let error = refused(
            "bucket.uri = \"s3://objects\"\n[store_io.commit]\nreserve = 0\nlimit = 1\nweight = 0\n",
        );
        assert!(error.contains("commit.weight"), "{error}");
    }

    #[test]
    fn a_node_must_not_write_inline_layers_its_ledger_refuses() {
        let error =
            refused("bucket.uri = \"s3://objects\"\n[ledger]\nmax_inline_layer = \"512 KiB\"\n");
        assert!(error.contains("layers.inline_layer"), "{error}");
        Config::from_str(&format!(
            "{}[layers]\ninline_layer = \"4 MiB\"\n[ledger]\nmax_inline_layer = \"4 MiB\"\n",
            minimal()
        ))
        .expect("a limit as large as the layers is fine");
        for role in ["ledger", "compute"] {
            let node =
                format!("role = \"{role}\"\nadvertise_url = \"http://10.0.0.1:7400\"\ndata_dir");
            let toml = minimal().replace("data_dir", &node);
            Config::from_str(&format!("{toml}[ledger]\nmax_inline_layer = 0\n"))
                .expect("a node of one role cannot check the other's setting");
        }
    }

    #[test]
    fn a_merge_of_one_layer_is_refused() {
        let error = refused("bucket.uri = \"s3://objects\"\n[layers]\nmerge_factor = 1\n");
        assert!(error.contains("merge_factor"), "{error}");
    }

    #[test]
    fn a_key_the_layers_table_does_not_know_is_refused() {
        refused("bucket.uri = \"s3://objects\"\n[layers]\nlayer_target = \"64 MiB\"\n");
    }

    #[test]
    fn a_node_needs_a_data_dir() {
        let toml = minimal().replace("data_dir = \"/var/lib/enroute/node\"\n", "");
        let error = format!("{:#}", Config::from_str(&toml).unwrap_err());
        assert!(error.contains("data_dir"), "{error}");
    }

    #[test]
    fn a_key_no_table_knows_is_refused_rather_than_ignored() {
        let toml = format!("bucekt = \"s3://typo\"\n{}", minimal());
        Config::from_str(&toml).expect_err("a misspelled key");
    }

    /// The commented example loads, and says what its comments say it says.
    ///
    /// `include_str!`, so moving the file breaks the build rather than
    /// leaving a documented example nothing checks.
    #[test]
    fn the_example_is_a_deployment_that_would_serve() {
        let example = include_str!("../../dev/enroute.example.toml");

        // These tests must not write to the environment they read, so the
        // variable the example names is stood in for. That it still names it
        // is half of what this asserts.
        let filled = example.replace("${ENROUTE_HOOK_SIGNING_KEY}", "pem");
        assert_ne!(filled, example, "the example stopped naming the key");

        let config = Config::from_str(&filled).expect("dev/enroute.example.toml");

        assert_eq!(config.node.role, Role::Both);
        assert_eq!(config.hooks.timeout, Duration::from_secs(10));
    }

    /// The defaults that the example shows in comments are the engine's.
    #[test]
    fn the_examples_commented_defaults_are_the_engines() {
        let example = include_str!("../../dev/enroute.example.toml")
            .replace("${ENROUTE_HOOK_SIGNING_KEY}", "pem");
        let uncommented = example
            .lines()
            .map(|line| match line.strip_prefix("# ") {
                // Indented lines are prose examples, not defaults.
                Some(rest)
                    if rest.starts_with('[')
                        || (rest.contains(" = ")
                            && !rest.starts_with([' ', '\t'])
                            && !rest.starts_with("advertise_url")
                            && !rest.starts_with("zone")) =>
                {
                    rest
                }
                _ => line,
            })
            .collect::<Vec<_>>()
            .join("\n");
        let shown = Config::from_str(&uncommented).expect("the example, uncommented");
        let defaults = Config::from_str(&example).expect("dev/enroute.example.toml");

        assert_eq!(shown.layers, Layers::default());
        assert_eq!(shown.ledger, Ledger::default());
        let (node, engine) = (&shown.node, &defaults.node);
        assert_eq!(node.internal_listen, engine.internal_listen);
        assert_eq!(node.cpu_threads, engine.cpu_threads);
        assert_eq!(
            (node.cache, node.page_cache, node.disk_cache),
            (engine.cache, engine.page_cache, engine.disk_cache)
        );
        assert_eq!(node.evict_after, engine.evict_after);
        assert_eq!(shown.store_io, defaults.store_io);
    }

    /// One `tracing::debug!(?config)` must not be the end of every care taken
    /// over where these values come from.
    #[test]
    fn a_configs_own_debug_prints_no_secret_it_holds() {
        let toml = "bucket.uri = \"s3://objects\"\n\
             [bucket.credentials]\nsecret_access_key = \"hunter2\"\n\
             [node]\ndata_dir = \"/data\"\n\
             [hooks]\nenabled = [\"authorize\"]\nsigning_key = \"hunter2-as-a-pem\"\n\
             endpoint_url = \"https://app.example/hooks\"\n";
        let config = Config::from_str(toml).expect("a config holding secrets");

        for printed in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!printed.contains("hunter2"), "{printed}");
        }
        // Still reachable where it is needed, and only there.
        assert_eq!(
            config.hooks.signing_key.as_ref().unwrap().expose(),
            "hunter2-as-a-pem"
        );
    }

    /// Where spans go is a collector and its headers, with no vendor in the
    /// shape — a URL and two headers, whichever vendor is at the far end.
    #[test]
    fn telemetry_is_an_endpoint_and_whatever_headers_it_takes() {
        let toml = format!(
            "{}[telemetry]\n\
             endpoint = \"https://otlp.example.com/v1/traces\"\n\
             sample_ratio = 0.25\n\
             [telemetry.headers]\n\
             authorization = \"Bearer hunter2\"\n\
             \"x-dataset\" = \"enroute\"\n",
            minimal()
        );
        let config = Config::from_str(&toml).expect("a telemetry table");
        let telemetry = config.telemetry.as_ref().expect("configured");

        assert_eq!(telemetry.endpoint, "https://otlp.example.com/v1/traces");
        assert!((telemetry.sample_ratio - 0.25).abs() < f64::EPSILON);
        assert_eq!(
            telemetry.headers["authorization"].expose(),
            "Bearer hunter2"
        );
        // A header value is where a token goes, so it is a `Secret` and the
        // whole configuration keeps its redaction.
        assert!(!format!("{config:?}").contains("hunter2"));
    }

    /// A ratio outside 0.0..=1.0 is refused rather than clamped: silently
    /// keeping every trace when 5.0 was meant as a percentage is a surprise.
    #[test]
    fn a_sample_ratio_that_is_not_a_fraction_is_refused() {
        for bad in ["-0.5", "1.5", "100"] {
            let toml = format!(
                "{}[telemetry]\nendpoint = \"https://c/v1/traces\"\nsample_ratio = {bad}\n",
                minimal()
            );
            Config::from_str(&toml).expect_err(bad);
        }
    }

    /// A plain string field expands.
    ///
    /// `PATH` because these must not write to the environment they read, and
    /// it is the one name every environment running them already holds.
    #[test]
    fn a_string_field_expands() {
        let path = std::env::var("PATH").expect("PATH is set wherever tests run");
        let config = Config::from_str(&whole(
            "bucket.uri = \"s3://objects\"\n\
             [telemetry]\nendpoint = \"https://c/${PATH}\"\n",
        ))
        .expect("a config naming PATH");
        assert_eq!(
            config.telemetry.expect("configured").endpoint,
            format!("https://c/{path}")
        );
    }

    /// So does a value the schema turns into something else.
    ///
    /// `$$` rather than a name, so this asserts on the wiring without reading
    /// an environment it cannot set.
    #[test]
    fn every_string_shaped_value_goes_through_expansion() {
        let config = Config::from_str(&whole(
            "bucket.uri = \"memory:///objects$$one\"\n\
             [listen]\napi = \"127.0.0.1:1$$\"\n",
        ));

        // The address is the one that cannot survive it: `1$$` expands to
        // `1$`, which is not a port — so reaching that error proves the
        // expansion ran before the parse.
        let error = format!("{:#}", config.unwrap_err());
        assert!(error.contains("api"), "{error}");

        // Written whole rather than through `whole`, since `[node]` and
        // `[hooks]` hold values under test.
        let config = Config::from_str(
            "bucket.uri = \"memory:///objects$$one\"\n\
             [node]\ndata_dir = \"/data/node$$one\"\n\
             [hooks]\nenabled = [\"authorize\"]\nsigning_key = \"pem\"\n\
             endpoint_url = \"https://app.example/hooks$$one\"\n",
        )
        .expect("a config whose strings hold a literal dollar");

        assert!(format!("{:?}", config.bucket.uri).contains("objects$one"));
        assert_eq!(config.node.data_dir, PathBuf::from("/data/node$one"));
        assert!(
            config
                .hooks
                .endpoint_url
                .unwrap()
                .as_str()
                .ends_with("hooks$one")
        );
    }

    /// A name nothing holds stops the load, rather than building a store out
    /// of the empty string.
    #[test]
    fn a_value_naming_nothing_the_environment_holds_is_refused() {
        let error = refused("bucket.uri = \"s3://${ENROUTE_NO_SUCH_VARIABLE_ANYWHERE}\"\n");
        assert!(
            error.contains("ENROUTE_NO_SUCH_VARIABLE_ANYWHERE"),
            "{error}"
        );
    }

    /// Proved once, at startup, so the hook path carries no error for it.
    #[test]
    fn an_endpoint_that_is_not_a_url_is_refused_here() {
        let toml = minimal().replace(
            "endpoint_url = \"https://app.example/hooks\"",
            "endpoint_url = \"not a url\"",
        );
        Config::from_str(&toml).expect_err("an endpoint that is not a URL");
    }

    #[test]
    fn a_ledger_node_needs_an_advertised_url() {
        let toml = minimal().replace("data_dir", "role = \"ledger\"\ndata_dir");
        let error = format!("{:#}", Config::from_str(&toml).unwrap_err());
        assert!(error.contains("advertise_url"), "{error}");

        let toml = minimal().replace(
            "data_dir",
            "role = \"ledger\"\nadvertise_url = \"http://10.0.0.1:7400\"\ndata_dir",
        );
        Config::from_str(&toml).expect("a ledger node with an advertised URL");
    }

    const NODE: &str = "bucket.uri = \"memory:///o\"\n[node]\ndata_dir = \"/data\"\n";

    #[test]
    fn no_hooks_table_means_no_hooks_and_pushes_create() {
        let config = Config::from_str(NODE).expect("a file with no hooks");
        assert!(config.endpoint().unwrap().is_none());
        assert!(config.git.create_on_push);
        assert!(config.node().unwrap().compute.create_on_push);
    }

    #[test]
    fn default_object_format_reaches_the_engine_and_needs_the_ledger_setting() {
        let toml = format!("{NODE}[git]\ndefault_object_format = \"sha256\"\n");
        let err = toml.parse::<Config>().expect_err("needs allow_sha256");
        assert!(format!("{err:#}").contains("allow_sha256"), "{err:#}");
        let toml = format!(
            "{NODE}[git]\ndefault_object_format = \"sha256\"\n[ledger]\nallow_sha256 = true\n"
        );
        let config: Config = toml.parse().expect("config");
        assert_eq!(
            config.node().unwrap().compute.default_object_format,
            ObjectFormat::Sha256
        );
        format!("{NODE}[git]\ndefault_object_format = \"sha3\"\n")
            .parse::<Config>()
            .expect_err("an unknown format");
    }

    #[test]
    fn create_on_push_reaches_the_engine() {
        let toml = format!("{NODE}[git]\ncreate_on_push = false\n");
        let config = Config::from_str(&toml).unwrap();
        assert!(!config.node().unwrap().compute.create_on_push);
    }

    #[test]
    fn max_object_size_reaches_the_engine() {
        let config = Config::from_str(NODE).unwrap();
        assert_eq!(config.node().unwrap().compute.max_object_bytes, 100 << 20);
        let toml = format!("{NODE}[git]\nmax_object_size = \"1 GiB\"\n");
        let config = Config::from_str(&toml).unwrap();
        assert_eq!(config.node().unwrap().compute.max_object_bytes, 1 << 30);
    }

    #[test]
    fn an_enabled_hook_needs_an_endpoint_and_a_key() {
        let toml = format!("{NODE}[hooks]\nenabled = [\"pre_receive\"]\n");
        let error = format!("{:#}", Config::from_str(&toml).unwrap_err());
        assert!(error.contains("endpoint_url"), "{error}");

        let toml = format!("{NODE}[hooks]\nenabled = []\n");
        let config = Config::from_str(&toml).expect("no hook enabled");
        assert!(config.endpoint().unwrap().is_none());
    }

    #[test]
    fn an_unknown_hook_name_is_refused() {
        let toml = format!(
            "{NODE}[hooks]\nenabled = [\"authorise\"]\n\
             endpoint_url = \"https://a/h\"\nsigning_key = \"pem\"\n"
        );
        let error = format!("{:#}", Config::from_str(&toml).unwrap_err());
        assert!(error.contains("authorise"), "{error}");
    }

    #[test]
    fn pre_receive_mode_takes_two_names_and_refuses_the_rest() {
        let with = |mode: &str| {
            let toml = format!(
                "{NODE}[hooks]\nenabled = [\"pre_receive\"]\npre_receive_mode = \"{mode}\"\n\
                 endpoint_url = \"https://a/h\"\nsigning_key = \"pem\"\n"
            );
            Config::from_str(&toml)
        };
        let objects = with("objects").unwrap();
        assert_eq!(objects.hooks.pre_receive_mode, PreReceiveMode::Objects);
        assert_eq!(
            with("commands").unwrap().hooks.pre_receive_mode,
            PreReceiveMode::Commands
        );
        let error = format!("{:#}", with("object").unwrap_err());
        assert!(error.contains("commands"), "{error}");
        "off".parse::<PreReceiveMode>().unwrap_err();
    }

    #[test]
    fn hook_names_parse_as_a_configuration_spells_them() {
        for hook in Hook::ALL {
            assert_eq!(hook.name().parse::<Hook>().unwrap(), hook);
        }
        "authorise".parse::<Hook>().unwrap_err();
    }
}
