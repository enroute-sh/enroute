//! `enroute dev`: one local node with no configuration file.
//!
//! It writes a configuration in memory and serves it through the same path as
//! `serve`, so what works here works in a deployment.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result};

use enroute::config::{Config, Hook, PreReceiveMode};

/// The file that marks a directory as one `--clean` may delete.
const MARKER: &str = ".enroute-dev";
/// The dev signing key, private half.
const KEY_FILE: &str = "hooks-signing-key.pem";
/// The dev signing key, public half, for the application.
const PUBLIC_KEY_FILE: &str = "hooks-signing-key.pub.pem";

/// The `enroute dev` arguments.
#[derive(clap::Args, Debug)]
pub(crate) struct Args {
    /// Listen on N (Git), N+1 (API) and N+2 (internal) instead of 8080, 50051
    /// and 7400.
    #[arg(long, env = "ENROUTE_DEV_PORT_BASE", value_name = "N")]
    port_base: Option<u16>,
    /// Bind Git and the API to this address; internal stays on loopback.
    ///
    /// A container needs `0.0.0.0`: the host cannot reach its loopback.
    #[arg(
        long,
        env = "ENROUTE_HOST",
        value_name = "ADDR",
        default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST)
    )]
    host: IpAddr,
    /// Where the node keeps its data and objects.
    #[arg(
        long,
        env = "ENROUTE_DEV_DIR",
        value_name = "PATH",
        default_value = ".enroute/dev"
    )]
    dir: PathBuf,
    /// Delete the dev directory first.
    #[arg(long)]
    clean: bool,
    /// Call the hooks at this URL, signed with a generated dev key.
    #[arg(long, env = "ENROUTE_HOOKS_URL", value_name = "URL")]
    hooks: Option<url::Url>,
    /// Call only these hooks (default: all of them).
    ///
    /// One of `authorize`, `visible_refs`, `pre_receive`, `post_receive`. Repeat the
    /// flag or separate with commas.
    #[arg(
        long,
        env = "ENROUTE_HOOKS_ENABLE",
        value_name = "HOOK",
        value_delimiter = ',',
        requires = "hooks"
    )]
    enable: Vec<Hook>,
    /// What `pre_receive` sees: `commands` or `objects`.
    ///
    /// `objects` makes the pushed objects readable through the API, but a
    /// push then commits in two log entries.
    #[arg(
        long,
        env = "ENROUTE_PRE_RECEIVE_MODE",
        value_name = "MODE",
        default_value = "commands",
        requires = "hooks"
    )]
    pre_receive_mode: PreReceiveMode,
}

/// The address and three ports a dev node listens on.
#[derive(Debug, Clone, Copy)]
struct Ports {
    host: IpAddr,
    git: u16,
    api: u16,
    internal: u16,
}

impl Ports {
    fn from_base(base: Option<u16>) -> Result<Self> {
        let host = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let Some(base) = base else {
            return Ok(Self {
                host,
                git: 8080,
                api: 50051,
                internal: 7400,
            });
        };
        let next = |n: u16| base.checked_add(n).context("--port-base is too high");
        Ok(Self {
            host,
            git: base,
            api: next(1)?,
            internal: next(2)?,
        })
    }

    fn on(self, host: IpAddr) -> Self {
        Self { host, ..self }
    }

    /// The address a client on this host uses to reach `port`.
    fn reach(self, port: u16) -> SocketAddr {
        // A client cannot connect to a wildcard address.
        let ip = if self.host.is_unspecified() {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            self.host
        };
        SocketAddr::new(ip, port)
    }
}

/// The hooks a dev node calls, and the key it signs them with.
struct DevHooks {
    url: url::Url,
    enabled: Vec<Hook>,
    pre_receive_mode: PreReceiveMode,
    private_pem: String,
    public_pem: String,
    keyid: String,
    key_path: PathBuf,
    public_path: PathBuf,
}

/// Run `enroute dev`.
///
/// # Errors
///
/// Returns an error if the dev directory cannot be prepared, or the node
/// does not serve.
pub(crate) async fn run(args: Args) -> Result<()> {
    let ports = Ports::from_base(args.port_base)?.on(args.host);
    let dir = prepare(&args.dir, args.clean)?;
    let hooks = match args.hooks {
        Some(url) => {
            let enabled = if args.enable.is_empty() {
                Hook::ALL.to_vec()
            } else {
                args.enable
            };
            Some(dev_hooks(&dir, url, enabled, args.pre_receive_mode)?)
        }
        None => None,
    };
    let toml = config_toml(&dir, ports, hooks.as_ref());
    let config = toml
        .parse::<Config>()
        .context("the generated dev configuration")?;
    let banner = banner(&dir, ports, hooks.as_ref());
    // Quiet, so the banner is what a developer sees.
    crate::run(config, None, "warn", move || {
        if let Err(error) = super::print(&banner) {
            tracing::warn!("printing the banner: {error}");
        }
    })
    .await
}

/// Create `dir`, emptied first if `clean`, and return its absolute path.
fn prepare(dir: &Path, clean: bool) -> Result<PathBuf> {
    if clean && dir.exists() {
        let ours = dir.join(MARKER).exists();
        let empty = std::fs::read_dir(dir)?.next().is_none();
        anyhow::ensure!(
            ours || empty,
            "{} is not a dev directory (no {MARKER} file): refusing to delete it",
            dir.display()
        );
        std::fs::remove_dir_all(dir).with_context(|| format!("deleting {}", dir.display()))?;
    }
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    std::fs::write(
        dir.join(MARKER),
        "This directory is enroute dev state. `enroute dev --clean` deletes it.\n",
    )?;
    ignore_in_git(dir)?;
    Ok(std::fs::canonicalize(dir)?)
}

/// Keep dev state out of git in any project, with no entry in its own
/// `.gitignore`.
fn ignore_in_git(dir: &Path) -> Result<()> {
    // The default `.enroute/dev` is ignored from `.enroute`, so a sibling
    // made later is too.
    let root = match dir.parent() {
        Some(parent) if parent.file_name() == Some(std::ffi::OsStr::new(".enroute")) => parent,
        _ => dir,
    };
    let path = root.join(".gitignore");
    if !path.exists() {
        std::fs::write(&path, "*\n").with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(())
}

/// The dev hooks, with the key in `dir` made on first use.
fn dev_hooks(
    dir: &Path,
    url: url::Url,
    enabled: Vec<Hook>,
    pre_receive_mode: PreReceiveMode,
) -> Result<DevHooks> {
    let key_path = dir.join(KEY_FILE);
    let public_path = dir.join(PUBLIC_KEY_FILE);
    let key = if key_path.exists() {
        let pem = std::fs::read_to_string(&key_path)?;
        enroute_signature::SigningKey::from_pem(&pem)
            .map_err(|error| anyhow::anyhow!("{}: {error}", key_path.display()))?
    } else {
        let key = enroute_signature::SigningKey::from_seed(&rand::random());
        write_private(&key_path, &key.to_pem()?)?;
        key
    };
    let public_pem = key.verifying_key().to_pem()?;
    std::fs::write(&public_path, &public_pem)?;
    Ok(DevHooks {
        url,
        enabled,
        pre_receive_mode,
        private_pem: key.to_pem()?,
        public_pem,
        keyid: key.keyid().to_string(),
        key_path,
        public_path,
    })
}

/// Write a private key that only this user can read.
fn write_private(path: &Path, pem: &str) -> Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(path)
        .with_context(|| format!("creating {}", path.display()))?;
    std::io::Write::write_all(&mut file, pem.as_bytes())?;
    Ok(())
}

/// A TOML string that holds `value` as written.
fn quoted(value: &str) -> String {
    // `$` starts an expansion in the configuration, and `$$` is a literal one.
    toml::Value::String(value.replace('$', "$$")).to_string()
}

/// The configuration of a dev node in `dir`.
fn config_toml(dir: &Path, ports: Ports, hooks: Option<&DevHooks>) -> String {
    let path = |name: &str| quoted(&dir.join(name).display().to_string());
    let objects = quoted(&format!("file://{}", dir.join("objects").display()));
    let listen = |port: u16| quoted(&SocketAddr::new(ports.host, port).to_string());
    let mut toml = format!(
        "[bucket]\nuri = {objects}\n\
         [node]\nrole = \"both\"\ndata_dir = {data}\ninternal_listen = \"127.0.0.1:{internal}\"\n\
         [listen]\napi = {api}\ngit = {git}\n\
         [git]\nallow_private_remotes = true\n",
        data = path("node"),
        internal = ports.internal,
        api = listen(ports.api),
        git = listen(ports.git),
    );
    if let Some(hooks) = hooks {
        let names: Vec<String> = hooks.enabled.iter().map(|h| quoted(h.name())).collect();
        let table = format!(
            "[hooks]\nenabled = [{}]\nendpoint_url = {}\nsigning_key = {}\n\
             pre_receive_mode = {}\n",
            names.join(", "),
            quoted(hooks.url.as_str()),
            quoted(&hooks.private_pem),
            quoted(hooks.pre_receive_mode.name()),
        );
        toml.push_str(&table);
    }
    toml
}

/// What `enroute dev` prints once the node is up.
fn banner(dir: &Path, ports: Ports, hooks: Option<&DevHooks>) -> String {
    let git = format!("http://{}", ports.reach(ports.git));
    let api = format!("http://{}", ports.reach(ports.api));
    // With hooks on, Git waits for the endpoint, so only the API is a try.
    let push = match hooks {
        None => format!("  git push {git}/hello.git main\n"),
        Some(_) => String::new(),
    };
    let hooks = match hooks {
        None => "off (every request may read and push; pass --hooks URL to opt in)".to_string(),
        Some(h) => {
            let names: Vec<&str> = h.enabled.iter().map(|h| h.name()).collect();
            format!(
                "{} -> {}\n           signing key id {}\n           \
                 private key {}\n           public key {}:\n\n{}",
                names.join(", "),
                h.url,
                h.keyid,
                h.key_path.display(),
                h.public_path.display(),
                h.public_pem.trim_end(),
            )
        }
    };
    let api_flag = if ports.reach(ports.api) == SocketAddr::from((Ipv4Addr::LOCALHOST, 50051)) {
        String::new()
    } else {
        format!(" --api {api}")
    };
    format!(
        "\nenroute dev is running. Press Ctrl-C to stop.\n\n  \
         git      {git}\n  api      {api}\n  dir      {}\n  hooks    {hooks}\n\n\
         Try:\n{push}  enroute repo list{api_flag}\n",
        dir.display(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_generated_configuration_loads() {
        let dir = Path::new("/tmp/a $dir");
        let config = config_toml(dir, Ports::from_base(None).unwrap(), None)
            .parse::<Config>()
            .expect("a dev configuration");
        assert_eq!(config.listen.git.to_string(), "127.0.0.1:8080");
        assert_eq!(config.listen.api.to_string(), "127.0.0.1:50051");
        assert!(config.node.internal_listen.ip().is_loopback());
        assert!(config.node.advertise_url.is_none());
        assert_eq!(config.node.data_dir, dir.join("node"));
        assert!(config.endpoint().unwrap().is_none());
    }

    #[test]
    fn host_binds_git_and_the_api_but_not_internal() {
        let dir = Path::new("/tmp/dev");
        let ports = Ports::from_base(None).unwrap();
        let any = ports.on("0.0.0.0".parse().unwrap());
        let config = config_toml(dir, any, None).parse::<Config>().unwrap();
        assert_eq!(config.listen.git.to_string(), "0.0.0.0:8080");
        assert_eq!(config.listen.api.to_string(), "0.0.0.0:50051");
        assert!(config.node.internal_listen.ip().is_loopback());
        let text = banner(dir, any, None);
        assert!(text.contains("http://127.0.0.1:8080"), "{text}");

        let v6 = config_toml(dir, ports.on("::".parse().unwrap()), None)
            .parse::<Config>()
            .unwrap();
        assert_eq!(v6.listen.git.to_string(), "[::]:8080");
    }

    #[test]
    fn hooks_and_a_port_base_reach_the_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let url: url::Url = "http://localhost:3000/hooks".parse().unwrap();
        let hooks = dev_hooks(
            dir.path(),
            url,
            vec![Hook::Authorize],
            PreReceiveMode::Objects,
        )
        .unwrap();
        let ports = Ports::from_base(Some(9000)).unwrap();
        let config = config_toml(dir.path(), ports, Some(&hooks))
            .parse::<Config>()
            .unwrap();
        assert_eq!(config.listen.api.port(), 9001);
        assert_eq!(config.node.internal_listen.port(), 9002);
        let endpoint = config.endpoint().unwrap().expect("hooks");
        assert_eq!(endpoint.pre_receive_mode, PreReceiveMode::Objects);
        assert_eq!(
            endpoint.enabled.into_iter().collect::<Vec<_>>(),
            [Hook::Authorize]
        );
        assert_eq!(endpoint.signing_key.keyid(), hooks.keyid);
        // Git waits for the endpoint, so the banner suggests no push.
        let text = banner(dir.path(), ports, Some(&hooks));
        assert!(!text.contains("git push"), "{text}");
        assert!(banner(dir.path(), ports, None).contains("git push"));

        // The key persists.
        let again = dev_hooks(
            dir.path(),
            hooks.url.clone(),
            vec![],
            PreReceiveMode::Commands,
        )
        .unwrap();
        assert_eq!(again.keyid, hooks.keyid);
        Ports::from_base(Some(u16::MAX)).unwrap_err();
    }

    #[test]
    fn clean_deletes_only_a_dev_directory() {
        let dir = tempfile::tempdir().unwrap();
        let dev = dir.path().join("dev");
        prepare(&dev, false).unwrap();
        std::fs::write(dev.join("x"), "x").unwrap();
        prepare(&dev, true).unwrap();
        assert!(!dev.join("x").exists());

        let other = dir.path().join("other");
        std::fs::create_dir(&other).unwrap();
        std::fs::write(other.join("keep"), "x").unwrap();
        prepare(&other, true).unwrap_err();
        assert!(other.join("keep").exists());
    }

    #[test]
    fn git_ignores_the_dev_state() {
        let dir = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(dir.path())
                .output()
                .unwrap()
        };
        assert!(git(&["init", "-q"]).status.success());

        prepare(&dir.path().join(".enroute/dev"), false).unwrap();
        prepare(&dir.path().join("elsewhere"), false).unwrap();
        std::fs::write(dir.path().join(".enroute/dev/state"), "x").unwrap();

        let status = git(&["status", "--porcelain", "--untracked-files=all"]);
        assert_eq!(String::from_utf8(status.stdout).unwrap(), "");
    }
}
