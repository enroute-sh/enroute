//! The data commands: `repo`, `refs` and `commit`, as calls to the contract.

use std::fmt::Write as _;
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use serde_json::{Value, json};
use tonic::transport::Channel;

use enroute_api::api::v1alpha1 as api;
use enroute_api::common::v1alpha1::{ObjectId, RepoKey};

/// Where the contract is, and how to print.
#[derive(clap::Args, Debug)]
pub(crate) struct Api {
    /// The API address.
    #[arg(
        long,
        global = true,
        env = "ENROUTE_API",
        value_name = "URL",
        default_value = "http://127.0.0.1:50051"
    )]
    api: String,
    /// Print JSON.
    #[arg(long, global = true)]
    json: bool,
}

impl Api {
    async fn channel(&self) -> Result<Channel> {
        let url = if self.api.contains("://") {
            self.api.clone()
        } else {
            format!("http://{}", self.api)
        };
        Channel::from_shared(url.clone())
            .with_context(|| format!("{url} is not an API address"))?
            .connect()
            .await
            .with_context(|| format!("connecting to the API at {url}"))
    }
}

/// A repository's object format, as git names it.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub(crate) enum ObjectFormat {
    Sha1,
    Sha256,
}

/// `enroute repo`.
#[derive(clap::Subcommand, Debug)]
pub(crate) enum RepoCommand {
    /// Create a repository, or do nothing if it exists.
    Create {
        /// The repository key, for example acme/widgets.
        key: String,
        /// The branch HEAD names.
        #[arg(long, value_name = "REF", default_value = "refs/heads/main")]
        default_branch: String,
        /// How the repository names its objects, for good.
        #[arg(long, value_enum, default_value_t = ObjectFormat::Sha1)]
        object_format: ObjectFormat,
    },
    /// Show a repository.
    Get {
        /// The repository key.
        key: String,
    },
    /// List every repository.
    List {
        /// Only keys that start with these bytes.
        #[arg(long, value_name = "P")]
        prefix: Option<String>,
    },
    /// Delete a repository.
    Delete {
        /// The repository key.
        key: String,
    },
}

/// `enroute refs`.
#[derive(clap::Args, Debug)]
pub(crate) struct RefsArgs {
    /// The repository key.
    key: String,
    /// Only refs that start with this, for example refs/heads/.
    #[arg(long, value_name = "P")]
    prefix: Vec<String>,
}

/// `enroute commit`.
#[derive(clap::Args, Debug)]
pub(crate) struct CommitArgs {
    /// The repository key.
    key: String,
    /// The branch to commit on, for example main or refs/heads/main.
    #[arg(long, value_name = "REF")]
    branch: String,
    /// The commit message.
    #[arg(short = 'm', long = "message", value_name = "MSG")]
    message: String,
    /// The author and committer, as "Name <email>".
    #[arg(long, value_name = "NAME <EMAIL>")]
    author: String,
    /// Fail unless the branch is at this commit.
    #[arg(long, value_name = "OID", conflicts_with = "expect_absent")]
    expect: Option<String>,
    /// Fail unless the branch does not exist.
    #[arg(long)]
    expect_absent: bool,
    /// Write a file: path=@localfile for a file's bytes, path=text for text.
    #[arg(long, value_name = "PATH=@FILE|PATH=TEXT")]
    put: Vec<String>,
    /// Delete a file.
    #[arg(long, value_name = "PATH")]
    delete: Vec<String>,
    /// Make a file that --put writes executable.
    #[arg(long, value_name = "PATH")]
    exec: Vec<String>,
    /// Write a symbolic link at path to target.
    #[arg(long, value_name = "PATH=TARGET")]
    symlink: Vec<String>,
}

fn key(key: &str) -> RepoKey {
    RepoKey {
        key: key.to_string(),
    }
}

/// A status as an error, with its code, so a script can tell them apart.
fn failed(status: &tonic::Status) -> anyhow::Error {
    anyhow::anyhow!("{:?}: {}", status.code(), status.message())
}

/// Run `enroute repo`.
///
/// # Errors
///
/// Returns an error if the API cannot be reached or refuses the call.
pub(crate) async fn repo(api_args: &Api, command: RepoCommand) -> Result<()> {
    let mut client =
        api::repository_service_client::RepositoryServiceClient::new(api_args.channel().await?);
    match command {
        RepoCommand::Create {
            key: k,
            default_branch,
            object_format,
        } => {
            let object_format = match object_format {
                ObjectFormat::Sha1 => api::ObjectFormat::Sha1,
                ObjectFormat::Sha256 => api::ObjectFormat::Sha256,
            };
            let made = client
                .create_repository(api::CreateRepositoryRequest {
                    repo: Some(key(&k)),
                    default_branch: branch_ref(default_branch),
                    object_format: object_format.into(),
                })
                .await
                .map_err(|status| failed(&status))?
                .into_inner();
            show_one(api_args, made.repository.as_ref())
        }
        RepoCommand::Get { key: k } => {
            let got = client
                .get_repository(api::GetRepositoryRequest {
                    repo: Some(key(&k)),
                })
                .await
                .map_err(|status| failed(&status))?
                .into_inner();
            show_one(api_args, got.repository.as_ref())
        }
        RepoCommand::List { prefix } => {
            let mut all = Vec::new();
            let mut page_token = String::new();
            loop {
                let page = client
                    .list_repositories(api::ListRepositoriesRequest {
                        limit: 0,
                        page_token,
                        prefix: prefix.clone().unwrap_or_default(),
                    })
                    .await
                    .map_err(|status| failed(&status))?
                    .into_inner();
                all.extend(page.repositories);
                if page.next_page_token.is_empty() {
                    break;
                }
                page_token = page.next_page_token;
            }
            if api_args.json {
                return super::print_json(&Value::Array(all.iter().map(repo_json).collect()));
            }
            let mut text = String::new();
            for r in &all {
                writeln!(text, "{}", repo_line(r))?;
            }
            if all.is_empty() {
                text.push_str("no repositories\n");
            }
            super::print(&text)
        }
        RepoCommand::Delete { key: k } => {
            client
                .delete_repository(api::DeleteRepositoryRequest {
                    repo: Some(key(&k)),
                })
                .await
                .map_err(|status| failed(&status))?;
            if api_args.json {
                return super::print_json(&json!({ "deleted": k }));
            }
            super::print(&format!("deleted {k}"))
        }
    }
}

fn show_one(api_args: &Api, repo: Option<&api::Repository>) -> Result<()> {
    let repo = repo.context("the API answered with no repository")?;
    if api_args.json {
        return super::print_json(&repo_json(repo));
    }
    super::print(&repo_line(repo))
}

fn repo_key(repo: &api::Repository) -> &str {
    repo.repo.as_ref().map_or("", |k| k.key.as_str())
}

fn repo_line(repo: &api::Repository) -> String {
    let pushed = repo.last_push.as_ref().map_or_else(
        || "never pushed".to_string(),
        |t| format!("pushed {}", utc(t.seconds)),
    );
    format!("{}\t{}\t{pushed}", repo_key(repo), repo.default_branch)
}

fn repo_json(repo: &api::Repository) -> Value {
    json!({
        "key": repo_key(repo),
        "default_branch": repo.default_branch,
        "last_push": repo.last_push.as_ref().map(|t| utc(t.seconds)),
        "object_format": match repo.object_format() {
            api::ObjectFormat::Sha256 => "sha256",
            api::ObjectFormat::Sha1 | api::ObjectFormat::Unspecified => "sha1",
        },
    })
}

/// Seconds since the epoch as an RFC 3339 UTC time, or as the bare number
/// outside the years 1970 to 9999, which humantime does not format.
fn utc(seconds: i64) -> String {
    let time = u64::try_from(seconds)
        .ok()
        .and_then(|s| UNIX_EPOCH.checked_add(Duration::from_secs(s)));
    let mut out = String::new();
    match time {
        Some(time) if write!(out, "{}", humantime::format_rfc3339_seconds(time)).is_ok() => out,
        _ => seconds.to_string(),
    }
}

/// Run `enroute refs`.
///
/// # Errors
///
/// Returns an error if the API cannot be reached or refuses the call.
pub(crate) async fn refs(api_args: &Api, args: RefsArgs) -> Result<()> {
    let RefsArgs { key: k, prefix } = args;
    let mut client = api::ref_service_client::RefServiceClient::new(api_args.channel().await?);
    let listed = client
        .list_refs(api::ListRefsRequest {
            repo: Some(key(&k)),
            prefixes: prefix,
        })
        .await
        .map_err(|status| failed(&status))?
        .into_inner();
    let oid = |r: &api::Ref| {
        r.object_id
            .as_ref()
            .map_or("", |o| o.hex.as_str())
            .to_string()
    };
    if api_args.json {
        let refs: Vec<Value> = listed
            .refs
            .iter()
            .map(|r| json!({ "name": r.name, "object_id": oid(r) }))
            .collect();
        return super::print_json(&json!({
            "default_branch": listed.default_branch,
            "refs": refs,
        }));
    }
    let mut text = String::new();
    for r in &listed.refs {
        writeln!(text, "{}\t{}", oid(r), r.name)?;
    }
    if listed.refs.is_empty() {
        text.push_str("no refs\n");
    }
    super::print(&text)
}

/// Run `enroute commit`.
///
/// # Errors
///
/// Returns an error if an argument is malformed, a local file cannot be
/// read, or the API refuses the commit.
pub(crate) async fn commit(api_args: &Api, args: CommitArgs) -> Result<()> {
    let request = commit_request(args)?;
    let mut client =
        api::commit_service_client::CommitServiceClient::new(api_args.channel().await?);
    let made = client
        .create_commit(request)
        .await
        .map_err(|status| failed(&status))?
        .into_inner();
    let hex = |o: Option<&ObjectId>| o.map(|o| o.hex.clone());
    let (commit, tree, parent) = (
        hex(made.commit_id.as_ref()),
        hex(made.tree_id.as_ref()),
        hex(made.parent_commit_id.as_ref()),
    );
    if api_args.json {
        return super::print_json(&json!({
            "commit_id": commit,
            "tree_id": tree,
            "parent_commit_id": parent,
        }));
    }
    super::print(&format!(
        "commit {}\ntree   {}\nparent {}",
        commit.unwrap_or_default(),
        tree.unwrap_or_default(),
        parent.as_deref().unwrap_or("none"),
    ))
}

/// The request that `args` describe.
fn commit_request(args: CommitArgs) -> Result<api::CreateCommitRequest> {
    let author = identity(&args.author)?;
    let branch = branch_ref(args.branch);
    let expectation = match (args.expect, args.expect_absent) {
        (Some(hex), _) => Some(api::expected_old_commit::Expectation::CommitId(ObjectId {
            hex,
        })),
        (None, true) => Some(api::expected_old_commit::Expectation::Absent(
            api::expected_old_commit::Absent {},
        )),
        (None, false) => None,
    };
    let mut changes = Vec::new();
    for put in &args.put {
        let (path, value) = put
            .split_once('=')
            .with_context(|| format!("--put {put}: write path=@file or path=text"))?;
        let content = match value.strip_prefix('@') {
            Some(file) => std::fs::read(file).with_context(|| format!("reading {file}"))?,
            None => value.as_bytes().to_vec(),
        };
        let mode = if args.exec.iter().any(|e| e == path) {
            api::FileMode::Executable
        } else {
            api::FileMode::Regular
        };
        changes.push(put_change(path, content, mode));
    }
    for exec in &args.exec {
        anyhow::ensure!(
            changes.iter().any(|c| &c.path == exec),
            "--exec {exec}: no --put writes that path"
        );
    }
    for link in &args.symlink {
        let (path, target) = link
            .split_once('=')
            .with_context(|| format!("--symlink {link}: write path=target"))?;
        changes.push(put_change(
            path,
            target.as_bytes().to_vec(),
            api::FileMode::Symlink,
        ));
    }
    for path in &args.delete {
        changes.push(api::PathChange {
            path: path.clone(),
            change: Some(api::path_change::Change::Delete(api::DeleteFile {})),
        });
    }
    anyhow::ensure!(
        !changes.is_empty(),
        "nothing to commit: give --put, --delete or --symlink"
    );
    Ok(api::CreateCommitRequest {
        repo: Some(key(&args.key)),
        branch,
        expected_old_commit: expectation.map(|e| api::ExpectedOldCommit {
            expectation: Some(e),
        }),
        message: args.message,
        author: Some(author),
        committer: None,
        changes,
    })
}

/// `main` as `refs/heads/main`; a full refname as it is.
fn branch_ref(branch: String) -> String {
    if branch.starts_with("refs/") {
        branch
    } else {
        format!("refs/heads/{branch}")
    }
}

fn put_change(path: &str, content: Vec<u8>, mode: api::FileMode) -> api::PathChange {
    api::PathChange {
        path: path.to_string(),
        change: Some(api::path_change::Change::Put(api::PutFile {
            content: content.into(),
            mode: mode.into(),
        })),
    }
}

/// "Name <email>" as an identity, at the time of the call.
fn identity(spelled: &str) -> Result<api::Identity> {
    let parsed = spelled
        .trim()
        .strip_suffix('>')
        .and_then(|rest| rest.rsplit_once('<'))
        .map(|(name, email)| (name.trim(), email.trim()))
        .filter(|(name, email)| !name.is_empty() && !email.is_empty());
    let (name, email) =
        parsed.with_context(|| format!("--author {spelled:?}: write \"Name <email>\""))?;
    Ok(api::Identity {
        name: name.to_string(),
        email: email.to_string(),
        timestamp: None,
        utc_offset_seconds: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_author_is_a_name_and_an_address() {
        let who = identity("Ada Lovelace <ada@example.com>").unwrap();
        assert_eq!(
            (who.name.as_str(), who.email.as_str()),
            ("Ada Lovelace", "ada@example.com")
        );
        for bad in ["Ada", "<ada@example.com>", "Ada <>", "Ada ada@example.com>"] {
            identity(bad).unwrap_err();
        }
    }

    #[test]
    fn utc_is_rfc_3339() {
        assert_eq!(utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(utc(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(utc(-1), "-1");
        assert_eq!(utc(i64::MAX), i64::MAX.to_string());
    }

    fn args(extra: &[&str]) -> CommitArgs {
        use clap::{Args as _, FromArgMatches as _};
        let base = [
            "c", "k", "--branch", "main", "-m", "msg", "--author", "A <a@b>",
        ];
        let command = CommitArgs::augment_args(clap::Command::new("c"));
        let matches = command.get_matches_from(base.iter().chain(extra));
        CommitArgs::from_arg_matches(&matches).unwrap()
    }

    #[test]
    fn a_commit_request_says_what_the_flags_say() {
        let request = commit_request(args(&[
            "--put",
            "run.sh=echo",
            "--exec",
            "run.sh",
            "--symlink",
            "l=run.sh",
            "--delete",
            "old",
            "--expect-absent",
        ]))
        .unwrap();
        assert_eq!(request.branch, "refs/heads/main");
        let modes: Vec<(String, i32)> = request
            .changes
            .iter()
            .map(|c| match &c.change {
                Some(api::path_change::Change::Put(p)) => (c.path.clone(), p.mode),
                _ => (c.path.clone(), -1),
            })
            .collect();
        assert_eq!(
            modes,
            [
                ("run.sh".to_string(), api::FileMode::Executable.into()),
                ("l".to_string(), api::FileMode::Symlink.into()),
                ("old".to_string(), -1),
            ]
        );
        assert!(request.expected_old_commit.is_some());

        commit_request(args(&["--exec", "x"])).unwrap_err();
        commit_request(args(&[])).unwrap_err();
    }
}
