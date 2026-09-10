//! What a deployment is, as one file.
//!
//! Every value a process needs before it can serve, shaped so a
//! half-configured one does not parse: a choice is a union whose variant is
//! the table's own name, so the values that choice needs are its fields and a
//! value belonging to the other is a key nothing has. Every table refuses what
//! it does not know, since a typo that is ignored is a setting that silently
//! did nothing. Read once, at startup, because what is here decides what gets
//! *built* — a store, a listener, a worker — and rebuilding those under live
//! requests is what a restart is for. The one thing that does change while the
//! process runs is named here and read from elsewhere: [`Tenants::uri`]. A
//! credential is *named* here and need not be held here — a [`Secret`] is
//! usually a `${VAR}` — so the file stays something to commit, diff and roll
//! back, and nothing that holds one will print it.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};

use anyhow::{Context as _, Result};
use serde::Deserialize;

use crate::expand;
use crate::{Bucket, ObjectUri, ScratchUri, Secret, read_capped};

/// A deployment, as its file names it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where a repository's permanent objects live.
    pub bucket: Bucket,
    /// Who this serves, and how a call names one of them.
    pub tenants: Tenants,
    /// Where a push's work runs.
    ///
    /// No default: both answers need values of their own, so there is no
    /// deployment this could guess for.
    pub ingest: Ingest,
    /// Where each listener binds.
    #[serde(default)]
    pub listen: Listen,
    /// Postgres, and what connects to it.
    pub database: Database,
    /// What every call to a hook endpoint is signed with.
    pub hooks: Hooks,
    /// Where the maintenance pass runs.
    #[serde(default)]
    pub maintenance: Maintenance,
    /// What a push to a remote may dial.
    #[serde(default)]
    pub sync: Sync,
    /// Where spans go, if anywhere.
    #[serde(default)]
    pub telemetry: Option<Telemetry>,
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
    /// appends `/v1/traces`, so what is written is what is dialled.
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

/// Who this serves, and how a contract call names one of them.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tenants {
    /// Where the tenants are named, as a URL down to the object.
    ///
    /// Named here and read from there, because this file is read once and
    /// that list is re-read: a tenant is added without a restart.
    #[serde(deserialize_with = "expand::parsed::deserialize")]
    pub uri: ObjectUri,
    /// How often that list is read again.
    ///
    /// A tenant taken out of it keeps serving for up to this long, so this is
    /// the revocation window.
    #[serde(default = "Tenants::refresh")]
    pub refresh_secs: NonZeroU64,
    /// The header naming which tenant a contract call is for.
    ///
    /// Whatever can set it is every tenant, so the contract listener must not
    /// be reachable except through whatever does — see docs/operate/security.md.
    #[serde(
        default = "Tenants::header",
        deserialize_with = "expand::parsed::deserialize"
    )]
    pub header: http::HeaderName,
}

/// Postgres, and what connects to it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Database {
    /// Postgres, which nothing has to be applied to first.
    ///
    /// This server builds what it reads, and will not serve a database it
    /// could not.
    pub url: Secret,
    /// Pool size, which is a cap on concurrent pushes rather than on the
    /// server, since `append` holds a connection for its whole transaction.
    #[serde(default = "Database::max_connections")]
    pub max_connections: NonZeroU32,
    /// Whether this server applies its own schema before serving.
    #[serde(default)]
    pub migrate: Migrate,
}

/// Whether the server applies outstanding schema steps before it serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Migrate {
    /// Apply whatever is outstanding, then serve.
    ///
    /// A transaction to a step under one advisory lock, because replicas
    /// start together and the whole design is that compute is stateless.
    #[default]
    Auto,
    /// Apply nothing, and refuse to serve a database that is not up to date.
    ///
    /// For a deployment whose DBA applies DDL, with `enroute-schema`.
    Off,
}

/// What a call to a hook endpoint is proved with.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hooks {
    /// The Ed25519 private key, in PEM, that every call is signed with.
    ///
    /// Enroute holds this half and an application holds only the public one,
    /// so it can check a call and cannot make one — see docs/operate/security.md.
    pub signing_key: Secret,
    /// How long an application has to answer before its git request fails.
    ///
    /// The customer's budget rather than Enroute's: generous by default for a
    /// cold serverless start, and theirs to shorten or extend.
    #[serde(default = "Hooks::timeout")]
    pub timeout_secs: NonZeroU64,
}

/// Where a push's work runs, as `[ingest.local]` or `[ingest.lambda]`.
///
/// The variant names the table so a key belonging to the choice not taken is
/// refused: an internally tagged enum drops what it cannot place.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Ingest {
    /// In this process, against this process's stores.
    Local(Local),
    /// In the ingest Lambda, which holds no signing key and resolves no
    /// tenant.
    Lambda(Lambda),
}

/// A push ingested here.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Local {
    /// Where a push is staged while this process ingests it.
    ///
    /// `file://` or `memory://`: this process sweeps the whole of it at
    /// startup, so it is not somewhere a second Enroute may also point.
    #[serde(deserialize_with = "expand::parsed::deserialize")]
    pub scratch: ScratchUri,
}

/// A push ingested in a function.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lambda {
    /// The function's name or ARN.
    #[serde(deserialize_with = "expand::string::deserialize")]
    pub function: String,
    /// The region the function is in.
    #[serde(deserialize_with = "expand::string::deserialize")]
    pub region: String,
    /// Where the front door leaves a pack for the function to read.
    ///
    /// Its own credentials, not the objects bucket's: the two are routinely
    /// different providers.
    pub handoff: Bucket,
    /// The largest pack this will hand the function.
    pub max_pack_bytes: NonZeroU64,
    /// A published version to pin the code a deploy rolls forward to.
    ///
    /// Unset means the function's own latest.
    #[serde(default, deserialize_with = "expand::optional::deserialize")]
    pub qualifier: Option<String>,
}

/// The maintenance pass: what one does, and where it runs.
///
/// Two axes, not one. The windows below hold for a pass wherever it runs, so
/// the two ends cannot erase on different schedules; only `run` is a choice.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Maintenance {
    /// Minimum age of an unreferenced object before it counts as an orphan.
    ///
    /// Generous on purpose: under-waiting deletes a running push's bytes.
    pub grace_secs: u64,
    /// Minimum age of a repository's deletion before it is erased.
    ///
    /// Guards nothing in flight — it is the window in which a mistaken delete
    /// can still be undone by clearing `repositories.deleted_at`.
    pub deleted_grace_secs: u64,
    /// Where the pass runs.
    pub run: Run,
}

/// Where the maintenance pass runs, named the way [`Ingest`] is.
///
/// Both are tables though one holds nothing: a bare `run = "off"` is a key the
/// next table would capture, which is TOML working as designed.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Run {
    /// On a timer in this process, which is one fewer thing to deploy.
    InProcess(InProcess),
    /// Nowhere here: something else invokes `enroute-maintenance`.
    Off(Off),
}

/// A maintenance pass on a timer in this process.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct InProcess {
    /// How often that pass runs.
    ///
    /// A pass lists every repository's prefixes, so this trades that cost
    /// against how long an orphan sits around.
    pub interval_secs: NonZeroU64,
}

/// A maintenance pass that runs somewhere else, which needs nothing said
/// about it here.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Off {}

/// What `SyncService.PushToRemote` may dial.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sync {
    /// Let a push dial `http://`, and hosts that answer on an address off the
    /// public internet.
    ///
    /// For a development stack, and for a git server inside your own network.
    #[serde(default)]
    pub allow_private_remotes: bool,
}

/// One object's text, read through the store `uri` names.
///
/// A store per read, unlike `tenancy::Source`, which polls one built at
/// startup: this runs once, before anything a store could be shared with.
async fn read_to_string(uri: &ObjectUri) -> Result<String> {
    let (store, path) = uri.build()?;
    let options = object_store::GetOptions::default();
    let read = read_capped(
        store.as_ref(),
        &path,
        options,
        "the configuration",
        &uri.to_string(),
    )
    .await?
    // A read with no precondition cannot come back unchanged, but saying
    // so costs less than a type that proves it.
    .ok_or_else(|| anyhow::anyhow!("{uri} answered an unconditional read as unchanged"))?;
    Ok(read.text)
}

impl Config {
    /// The deployment `uri` names.
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be read, is too large, or does
    /// not load as a configuration.
    pub async fn read(uri: &ObjectUri) -> Result<Self> {
        Self::from_toml(&read_to_string(uri).await?)
            .with_context(|| format!("the configuration at {uri}"))
    }

    /// The deployment `toml` names.
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not parse, or if a value it names is
    /// not one of the thing it names.
    pub fn from_toml(toml: &str) -> Result<Self> {
        // No context of its own: the caller knows where the bytes came from,
        // and "the configuration" twice in one chain says nothing twice.
        Ok(toml::from_str(toml)?)
    }
}

/// Just the `[database]` table, out of a file that may hold anything else.
///
/// For the schema tool, which runs before a deployment is complete: the whole
/// of [`Config`] would make it wait on a signing key it has no use for.
#[derive(Debug, Clone, Deserialize)]
pub struct JustDatabase {
    /// Postgres, and what connects to it.
    pub database: Database,
}

/// What a maintenance pass reads, out of a file that holds more.
///
/// Not [`Config`]: that requires `[hooks].signing_key`, so a pass would need
/// the hook private key in its environment — to sign nothing.
#[derive(Debug, Clone, Deserialize)]
pub struct JustMaintenance {
    /// Where a repository's permanent objects live.
    pub bucket: Bucket,
    /// Postgres, and what connects to it.
    pub database: Database,
    /// The windows a pass erases on, wherever it runs.
    #[serde(default)]
    pub maintenance: Maintenance,
}

impl JustMaintenance {
    /// What the deployment at `uri` says a pass should do.
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be read, is too large, or holds
    /// no `[bucket]` or `[database]`.
    pub async fn read(uri: &ObjectUri) -> Result<Self> {
        Ok(toml::from_str(&read_to_string(uri).await?)?)
    }
}

impl JustDatabase {
    /// The database the deployment at `uri` names, and nothing else from it.
    ///
    /// # Errors
    ///
    /// Returns an error if the object cannot be read, is too large, or holds
    /// no `[database]` table.
    pub async fn read(uri: &ObjectUri) -> Result<Self> {
        // No `deny_unknown_fields`: every other table is a key this ignores on
        // purpose, which is the whole of why it is a second type.
        Ok(toml::from_str(&read_to_string(uri).await?)?)
    }
}

// `NonZeroU64::MIN.saturating_add(n)` rather than a literal, because
// `unwrap_used` is denied and `new(n).unwrap()` is how a literal becomes one.
// The idiom `lattice/store/src/segments.rs` already uses.

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

impl Tenants {
    /// Thirty seconds, the revocation window a deployment gets unless it asks
    /// for another.
    fn refresh() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(29)
    }

    fn header() -> http::HeaderName {
        http::HeaderName::from_static("x-enroute-tenant")
    }
}

impl Hooks {
    /// Ten seconds, which a cold serverless start fits inside.
    fn timeout() -> NonZeroU64 {
        NonZeroU64::MIN.saturating_add(9)
    }
}

impl Telemetry {
    /// Every trace, which is the right default until traffic says otherwise.
    fn sample_ratio() -> f64 {
        1.0
    }
}

impl Database {
    /// Ten connections, which is ten concurrent pushes.
    fn max_connections() -> NonZeroU32 {
        NonZeroU32::MIN.saturating_add(9)
    }
}

impl Default for InProcess {
    /// Fifteen minutes, trading a pass over every repository's prefixes
    /// against how long an orphan sits around.
    fn default() -> Self {
        Self {
            interval_secs: NonZeroU64::MIN.saturating_add(899),
        }
    }
}

impl Default for Maintenance {
    /// Six hours and a day, generous enough that neither deletes something a
    /// deployment still wanted.
    fn default() -> Self {
        Self {
            grace_secs: 6 * 60 * 60,
            deleted_grace_secs: 24 * 60 * 60,
            run: Run::default(),
        }
    }
}

impl Default for Run {
    fn default() -> Self {
        Self::InProcess(InProcess::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The least a file can say and still name a deployment.
    fn minimal() -> String {
        format!(
            "bucket.uri = \"s3://objects\"\n\
             [tenants]\n\
             uri = \"file:///etc/enroute/tenants.toml\"\n\
             [ingest.local]\n\
             scratch = \"file:///data/scratch\"\n\
             {REQUIRED}"
        )
    }

    /// The two tables every fixture needs, since both are required and
    /// neither is what the test is about.
    const REQUIRED: &str = "[database]\nurl = \"postgres://enroute@localhost/enroute\"\n\
                            [hooks]\nsigning_key = \"pem\"\n";

    /// The whole chain: the outer context alone says only "the
    /// configuration", which is not what any of these assert on.
    fn refused(toml: &str) -> String {
        format!("{:#}", Config::from_toml(&whole(toml)).unwrap_err())
    }

    /// A fixture with the two required tables on the end, so each test writes
    /// only the part it is about.
    fn whole(toml: &str) -> String {
        format!("{toml}{REQUIRED}")
    }

    #[test]
    fn the_tables_a_file_leaves_out_are_the_documented_defaults() {
        let config = Config::from_toml(&minimal()).unwrap();

        assert_eq!(config.listen.api.port(), 50051);
        assert_eq!(config.listen.git.port(), 8080);
        assert_eq!(config.database.max_connections.get(), 10);
        assert_eq!(config.hooks.timeout_secs.get(), 10);
        assert!(config.telemetry.is_none());
        assert_eq!(config.tenants.refresh_secs.get(), 30);
        assert_eq!(config.tenants.header.as_str(), "x-enroute-tenant");
        assert!(!config.sync.allow_private_remotes);
        assert_eq!(config.maintenance.grace_secs, 6 * 60 * 60);
        assert_eq!(config.maintenance.deleted_grace_secs, 24 * 60 * 60);
        assert!(matches!(
            config.maintenance.run,
            Run::InProcess(InProcess { interval_secs }) if interval_secs.get() == 900
        ));
    }

    /// The point of the union: the values a choice needs are that variant's
    /// fields, so a half-configured ingest does not parse.
    #[test]
    fn a_lambda_ingest_missing_a_value_it_needs_is_refused() {
        let error = refused(
            "bucket.uri = \"s3://objects\"\n\
             [tenants]\nuri = \"file:///tenants.toml\"\n\
             [ingest.lambda]\nfunction = \"ingest\"\n\
             handoff.uri = \"s3://handoff\"\nmax_pack_bytes = 1024\n",
        );
        assert!(error.contains("region"), "{error}");
    }

    /// The other half of it, and why the variant names the table.
    ///
    /// A value belonging to the choice not taken is refused rather than read
    /// and dropped.
    #[test]
    fn a_value_belonging_to_the_other_ingest_is_refused() {
        let error = refused(
            "bucket.uri = \"s3://objects\"\n\
             [tenants]\nuri = \"file:///tenants.toml\"\n\
             [ingest.local]\nscratch = \"file:///scratch\"\n\
             region = \"eu-central-1\"\n",
        );
        assert!(error.contains("region"), "{error}");
    }

    #[test]
    fn an_interval_under_a_maintenance_that_runs_nowhere_is_refused() {
        // Not through `refused`: this one builds on `minimal`, which is whole
        // already, and a second set of the required tables is its own error.
        let toml = format!("{}[maintenance.run.off]\ninterval_secs = 900\n", minimal());
        let error = format!("{:#}", Config::from_toml(&toml).unwrap_err());
        assert!(error.contains("interval_secs"), "{error}");
    }

    /// The reason for the reshape: a window holds for a pass wherever it runs,
    /// so it sits beside the choice rather than inside one arm of it.
    #[test]
    fn the_grace_windows_are_set_whichever_end_runs_the_pass() {
        let toml = format!(
            "{}[maintenance]\ngrace_secs = 60\ndeleted_grace_secs = 604800\n\
             [maintenance.run.off]\n",
            minimal()
        );
        let config = Config::from_toml(&toml).expect("windows beside the choice");

        assert_eq!(config.maintenance.grace_secs, 60);
        assert_eq!(config.maintenance.deleted_grace_secs, 604_800);
        assert!(matches!(config.maintenance.run, Run::Off(_)));
    }

    /// A pass running elsewhere still names itself as a table, so no choice
    /// here is a bare key that the next table would swallow.
    #[test]
    fn a_maintenance_that_runs_nowhere_is_an_empty_table() {
        let toml = format!("{}[maintenance.run.off]\n", minimal());
        let config = Config::from_toml(&toml).unwrap();
        assert!(matches!(config.maintenance.run, Run::Off(_)));
    }

    /// What `value_parser!(u64).range(1..)` did on the command line, done by
    /// the type instead: an interval of zero is a busy loop.
    #[test]
    fn an_interval_of_zero_is_refused() {
        let toml = format!(
            "{}[maintenance.run.in-process]\ninterval_secs = 0\n",
            minimal()
        );
        Config::from_toml(&toml).expect_err("a zero interval");
    }

    #[test]
    fn a_key_no_table_knows_is_refused_rather_than_ignored() {
        let toml = format!("bucekt = \"s3://typo\"\n{}", minimal());
        Config::from_toml(&toml).expect_err("a misspelled key");
    }

    /// The URI types keep their own checks when a file is what names them,
    /// since the file and the grammar are the same parser.
    #[test]
    fn a_scratch_that_a_second_process_could_reach_is_refused() {
        let error = refused(
            "bucket.uri = \"s3://objects\"\n\
             [tenants]\nuri = \"file:///tenants.toml\"\n\
             [ingest.local]\nscratch = \"s3://shared\"\n",
        );
        assert!(error.contains("one process must own it alone"), "{error}");
    }

    #[test]
    fn a_tenants_uri_naming_no_object_is_refused() {
        let error = refused(
            "bucket.uri = \"s3://objects\"\n\
             [tenants]\nuri = \"s3://config\"\n\
             [ingest.local]\nscratch = \"file:///scratch\"\n",
        );
        assert!(error.contains("no object in it"), "{error}");
    }

    /// The commented example loads, and says what its comments say it says.
    ///
    /// `include_str!`, so moving the file breaks the build rather than
    /// leaving a documented example nothing checks.
    #[test]
    fn the_example_is_a_deployment_that_would_serve() {
        let example = include_str!("../../../../dev/enroute.example.toml");

        // These tests must not write to the environment they read, so the two
        // variables the example names are stood in for. That it still names
        // them is half of what this asserts.
        let filled = example
            .replace("${DATABASE_URL}", "postgres://enroute@localhost/enroute")
            .replace("${ENROUTE_HOOK_SIGNING_KEY}", "pem");
        assert_ne!(filled, example, "the example stopped naming the two");

        let config = Config::from_toml(&filled).expect("dev/enroute.example.toml");

        assert!(matches!(config.ingest, Ingest::Local(_)));
        assert_eq!(config.tenants.refresh_secs.get(), 30);
    }

    /// The one the local stack runs, checked the same way — a stack that will
    /// not start is a worse way to find out.
    #[test]
    fn the_local_stack_config_is_one_that_would_serve() {
        let local = include_str!("../../../../dev/config/enroute.toml");
        let config = Config::from_toml(local).expect("dev/config/enroute.toml");

        // The two the file overrides, and the defaults it leaves alone.
        assert_eq!(config.tenants.refresh_secs.get(), 2);
        assert!(matches!(config.ingest, Ingest::Local(_)));
        assert_eq!(config.listen.git.port(), 8080);
        assert!(!config.sync.allow_private_remotes);
    }

    /// One `tracing::debug!(?config)` must not be the end of every care taken
    /// over where these values come from.
    #[test]
    fn a_configs_own_debug_prints_no_secret_it_holds() {
        let toml = "bucket.uri = \"s3://objects\"\n\
             [bucket.credentials]\nsecret_access_key = \"hunter2\"\n\
             [tenants]\nuri = \"file:///tenants.toml\"\n\
             [ingest.local]\nscratch = \"file:///scratch\"\n\
             [database]\nurl = \"postgres://enroute:hunter2@host/enroute\"\n\
             [hooks]\nsigning_key = \"hunter2-as-a-pem\"\n";
        let config = Config::from_toml(toml).expect("a config holding secrets");

        for printed in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(!printed.contains("hunter2"), "{printed}");
        }
        // Still reachable where it is needed, and only there.
        assert_eq!(config.hooks.signing_key.expose(), "hunter2-as-a-pem");
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
        let config = Config::from_toml(&toml).expect("a telemetry table");
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

    #[test]
    fn no_telemetry_table_is_the_console_alone() {
        let config = Config::from_toml(&minimal()).expect("no telemetry table");
        assert!(config.telemetry.is_none());
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
            Config::from_toml(&toml).expect_err(bad);
        }
    }

    /// A plain string field expands.
    ///
    /// `PATH` because these must not write to the environment they read, and
    /// it is the one name every environment running them already holds.
    #[test]
    fn a_string_field_expands() {
        let path = std::env::var("PATH").expect("PATH is set wherever tests run");
        let config = Config::from_toml(&whole(
            "bucket.uri = \"memory:///\"\n\
             [tenants]\nuri = \"file:///tenants.toml\"\n\
             [ingest.lambda]\nfunction = \"ingest-${PATH}\"\n\
             region = \"eu-central-1\"\nhandoff.uri = \"s3://handoff\"\n\
             max_pack_bytes = 1024\n",
        ))
        .expect("a config naming PATH");

        let Ingest::Lambda(lambda) = config.ingest else {
            panic!("a lambda ingest");
        };
        assert_eq!(lambda.function, format!("ingest-{path}"));
    }

    /// So does a value the schema turns into something else.
    ///
    /// `$$` rather than a name, so this asserts on the wiring without reading
    /// an environment it cannot set.
    #[test]
    fn every_string_shaped_value_goes_through_expansion() {
        let config = Config::from_toml(&whole(
            "bucket.uri = \"memory:///objects$$one\"\n\
             [tenants]\nuri = \"file:///tenants$$one.toml\"\nheader = \"x-a$$b\"\n\
             [listen]\napi = \"127.0.0.1:1$$\"\n\
             [ingest.local]\nscratch = \"file:///scratch$$one\"\n",
        ));

        // The address is the one that cannot survive it: `1$$` expands to
        // `1$`, which is not a port — so reaching that error proves the
        // expansion ran before the parse.
        let error = format!("{:#}", config.unwrap_err());
        assert!(error.contains("api"), "{error}");

        let config = Config::from_toml(&whole(
            "bucket.uri = \"memory:///objects$$one\"\n\
             [tenants]\nuri = \"file:///tenants$$one.toml\"\nheader = \"x-a$$b\"\n\
             [ingest.local]\nscratch = \"file:///scratch$$one\"\n",
        ))
        .expect("a config whose strings hold a literal dollar");

        assert!(config.bucket.uri.to_uri().ends_with("objects$one"));
        assert!(config.tenants.uri.as_str().ends_with("tenants$one.toml"));
        assert_eq!(config.tenants.header.as_str(), "x-a$b");
    }

    /// A name nothing holds stops the load, rather than building a store out
    /// of the empty string.
    #[test]
    fn a_value_naming_nothing_the_environment_holds_is_refused() {
        let error = refused(
            "bucket.uri = \"s3://${ENROUTE_NO_SUCH_VARIABLE_ANYWHERE}\"\n\
             [tenants]\nuri = \"file:///tenants.toml\"\n\
             [ingest.local]\nscratch = \"file:///scratch\"\n",
        );
        assert!(
            error.contains("ENROUTE_NO_SUCH_VARIABLE_ANYWHERE"),
            "{error}"
        );
    }

    #[test]
    fn a_header_the_http_layer_would_refuse_is_refused_here() {
        let toml = minimal().replace(
            "uri = \"file:///etc/enroute/tenants.toml\"",
            "uri = \"file:///etc/enroute/tenants.toml\"\nheader = \"not a header\"",
        );
        Config::from_toml(&toml).expect_err("a header name with a space in it");
    }
}
