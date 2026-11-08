//! Access hooks: the host's authorizer and ref visibility, through git.

#![expect(
    clippy::unwrap_used,
    reason = "a test fails by panicking, on values it chose itself"
)]

mod common;

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

use axum::http::header;
use bytes::Bytes;
use common::{Git, Server};
use enroute_compute::{
    Access, Authorize, Challenge, Denial, GitRequest, Grant, HookCommand, HostHooks, Judgement,
    PreReceiveMode, PushContext, PushHooks, RefJudgement, RefVisibility,
};

const ALICE: &str = "Basic YWxpY2U6cHc="; // alice:pw
const READER: &str = "Basic cmVhZGVyOnB3"; // reader:pw

/// A host where `alice` may read and write, and `reader` may only read.
///
/// A path names the repository `store/<path>`; paths under `missing/` name none.
#[derive(Debug, Default)]
struct Host {
    /// Refnames that visibility hides.
    hide: Mutex<HashSet<String>>,
    /// What visibility was asked: the actor, the context and the access.
    asked: Mutex<Vec<(String, Bytes, Access)>>,
    pre: Mutex<Vec<PushContext>>,
    post: Mutex<Vec<PushContext>>,
    /// The paths the authorizer was asked about, as spelled.
    spelled: Mutex<Vec<String>>,
}

/// The context that `actor` is granted, not text on purpose.
fn context(actor: &str) -> Bytes {
    Bytes::from([b"\x00\xffctx-".as_slice(), actor.as_bytes()].concat())
}

#[async_trait::async_trait]
impl Authorize for Host {
    async fn authorize(&self, request: &GitRequest<'_>) -> Result<Grant, Denial> {
        self.spelled
            .lock()
            .unwrap()
            .push(request.spelled.to_string());
        let credential = request
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        let actor = match credential {
            Some(ALICE) => "alice",
            Some(READER) if request.access == Access::Read => "reader",
            Some(READER) => return Err(Denial::Forbidden),
            _ => {
                return Err(Denial::Unauthorized(Challenge::basic(
                    "enroute-test",
                    "use a token\n",
                )));
            }
        };
        if request.path.starts_with("missing/") {
            return Err(Denial::NotFound);
        }
        // Paths that are not repository names, which the host maps to names.
        let repo = if request.path.starts_with("alias/") {
            "store/r".to_string()
        } else if request.path.starts_with("bad/") {
            "store/bad name".to_string()
        } else {
            format!("store/{}", request.path)
        };
        Ok(Grant {
            repo,
            ctx: PushContext {
                actor: actor.into(),
                context: context(actor),
            },
        })
    }
}

#[async_trait::async_trait]
impl RefVisibility for Host {
    async fn visible_refs(
        &self,
        ctx: &PushContext,
        _repo: &str,
        access: Access,
        refs: &[&str],
    ) -> anyhow::Result<Vec<String>> {
        self.asked
            .lock()
            .unwrap()
            .push((ctx.actor.clone(), ctx.context.clone(), access));
        let hide = self.hide.lock().unwrap();
        Ok(refs
            .iter()
            .filter(|r| !hide.contains(**r))
            .map(|r| (*r).to_string())
            .collect())
    }
}

#[async_trait::async_trait]
impl PushHooks for Host {
    fn pre_receive_mode(&self) -> Option<PreReceiveMode> {
        Some(PreReceiveMode::Commands)
    }

    async fn pre_receive(
        &self,
        ctx: &PushContext,
        _repo: &str,
        commands: &[HookCommand],
        _options: &[String],
    ) -> anyhow::Result<Vec<RefJudgement>> {
        self.pre.lock().unwrap().push(ctx.clone());
        Ok(commands
            .iter()
            .map(|c| RefJudgement {
                refname: c.refname.clone(),
                judgement: Judgement::Allow,
            })
            .collect())
    }

    async fn post_receive(
        &self,
        ctx: &PushContext,
        _repo: &str,
        _commands: &[HookCommand],
        _options: &[String],
    ) -> anyhow::Result<Vec<String>> {
        self.post.lock().unwrap().push(ctx.clone());
        Ok(Vec::new())
    }
}

async fn start() -> (Server, Arc<Host>) {
    let host = Arc::new(Host::default());
    let hooks = HostHooks {
        authorize: Some(host.clone()),
        visibility: Some(host.clone()),
        push: Some(host.clone()),
    };
    let server = Server::start_hooked(hooks, |_| {}).await;
    (server, host)
}

/// The git URL of `path`, as `user` (with password `pw`), if any.
fn url(server: &Server, user: Option<&str>, path: &str) -> String {
    let base = match user {
        Some(u) => server
            .url
            .replacen("http://", &format!("http://{u}:pw@"), 1),
        None => server.url.clone(),
    };
    format!("{base}/{path}.git")
}

/// A raw `info/refs` request for `service`, with `credential` if any.
async fn info_refs(
    server: &Server,
    path: &str,
    service: &str,
    credential: Option<&str>,
) -> reqwest::Response {
    let mut req = reqwest::Client::new().get(format!(
        "{}/{path}.git/info/refs?service={service}",
        server.url
    ));
    if let Some(c) = credential {
        req = req.header("authorization", c);
    }
    req.send().await.unwrap()
}

/// The refnames that `ls-remote` lists with protocol `version`.
async fn ls_remote(git: &Git, cwd: &Path, url: &str, version: u8) -> Vec<String> {
    let v = format!("protocol.version={version}");
    let out = git.run(cwd, &["-c", &v, "ls-remote", url]).await;
    out.lines()
        .filter_map(|l| l.split('\t').nth(1))
        .map(str::to_string)
        .collect()
}

/// A source repository with `main`, an annotated tag on it, and a branch
/// `secret` one commit ahead.
async fn source(git: &Git) -> (std::path::PathBuf, String, String) {
    let src = git.init("src").await;
    let main = git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["tag", "-a", "-m", "v1", "v1"]).await;
    git.run(&src, &["checkout", "-q", "-b", "secret"]).await;
    let secret = git.commit_file(&src, "f", "2\n", "2").await;
    git.run(&src, &["checkout", "-q", "main"]).await;
    (src, main, secret)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grant_names_the_repository_and_its_ctx_reaches_the_hooks() {
    let (server, host) = start().await;
    let git = Git::new();
    let alice = url(&server, Some("alice"), "team/widgets");
    let src = git.init("src").await;
    let c1 = git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["push", &alice, "main"]).await;

    let compute = server.node.compute().unwrap();
    let stored = compute.repo_for_read("store/team/widgets", true).await;
    assert!(stored.unwrap().is_some());
    assert!(
        compute
            .repo_for_read("team/widgets", true)
            .await
            .unwrap()
            .is_none()
    );

    let ctx = |c: &PushContext| (c.actor.clone(), c.context.clone());
    let want = vec![("alice".to_string(), context("alice"))];
    assert_eq!(
        host.pre.lock().unwrap().iter().map(ctx).collect::<Vec<_>>(),
        want
    );
    assert_eq!(
        host.post
            .lock()
            .unwrap()
            .iter()
            .map(ctx)
            .collect::<Vec<_>>(),
        want
    );

    let dst = git.dir("dst");
    git.run(
        git.home.path(),
        &["clone", "-q", &alice, dst.to_str().unwrap()],
    )
    .await;
    assert_eq!(git.rev(&dst, "HEAD").await, c1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn denials_map_to_http() {
    let (server, _host) = start().await;
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["push", &url(&server, Some("alice"), "r"), "main"])
        .await;

    // No credential: a 401 with the challenge, and git asks for one.
    let res = info_refs(&server, "r", "git-upload-pack", None).await;
    assert_eq!(res.status(), 401);
    let challenge = res.headers().get("www-authenticate").unwrap();
    assert_eq!(challenge, "Basic realm=\"enroute-test\"");
    assert_eq!(res.text().await.unwrap(), "use a token\n");
    let anon = url(&server, None, "r");
    let (ok, _, err) = git
        .try_run(git.home.path(), &["clone", "-q", &anon, "anon"])
        .await;
    assert!(!ok);
    assert!(err.contains("Username"), "{err}");

    // A read-only grant can clone, not push.
    let reader = url(&server, Some("reader"), "r");
    git.run(git.home.path(), &["clone", "-q", &reader, "rd"])
        .await;
    let res = info_refs(&server, "r", "git-receive-pack", Some(READER)).await;
    assert_eq!(res.status(), 403);
    let rd = git.dir("rd");
    git.commit_file(&rd, "f", "2\n", "2").await;
    let (ok, _, err) = git.try_run(&rd, &["push", &reader, "main"]).await;
    assert!(!ok);
    assert!(err.contains("403"), "{err}");

    // No such repository.
    let res = info_refs(&server, "missing/r", "git-upload-pack", Some(ALICE)).await;
    assert_eq!(res.status(), 404);
    let missing = url(&server, Some("alice"), "missing/r");
    let (ok, _, err) = git
        .try_run(git.home.path(), &["clone", "-q", &missing, "m"])
        .await;
    assert!(!ok);
    assert!(err.contains("not found"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn advertisements_hide_refs_and_head_follows_its_branch() {
    let (server, host) = start().await;
    let git = Git::new();
    let alice = url(&server, Some("alice"), "r");
    let (src, _, _) = source(&git).await;
    git.run(&src, &["push", &alice, "main", "secret", "v1"])
        .await;

    host.hide.lock().unwrap().insert("refs/heads/secret".into());
    host.asked.lock().unwrap().clear();
    for v in [2, 0] {
        let refs = ls_remote(&git, &src, &alice, v).await;
        assert_eq!(
            refs,
            ["HEAD", "refs/heads/main", "refs/tags/v1", "refs/tags/v1^{}"],
            "v{v}"
        );
    }
    let push_ad = info_refs(&server, "r", "git-receive-pack", Some(ALICE)).await;
    let body = push_ad.text().await.unwrap();
    assert!(body.contains("refs/heads/main"), "{body}");
    assert!(!body.contains("refs/heads/secret"), "{body}");
    let asked = host.asked.lock().unwrap().clone();
    let read = ("alice".to_string(), context("alice"), Access::Read);
    let write = ("alice".to_string(), context("alice"), Access::Write);
    assert_eq!(asked, [read.clone(), read, write]);

    // HEAD goes with the branch it points at.
    host.hide.lock().unwrap().insert("refs/heads/main".into());
    for v in [2, 0] {
        let refs = ls_remote(&git, &src, &alice, v).await;
        assert_eq!(refs, ["refs/tags/v1", "refs/tags/v1^{}"], "v{v}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn include_tag_skips_hidden_tags() {
    let (server, host) = start().await;
    let git = Git::new();
    let alice = url(&server, Some("alice"), "r");
    let (src, _, _) = source(&git).await;
    git.run(&src, &["tag", "-a", "-m", "hidden", "hidden"])
        .await;
    git.run(&src, &["push", &alice, "main", "v1", "hidden"])
        .await;
    let v1 = git.rev(&src, "refs/tags/v1").await;
    let hidden = git.rev(&src, "refs/tags/hidden").await;
    host.hide.lock().unwrap().insert("refs/tags/hidden".into());

    for v in [2, 0] {
        let dst = git.init(&format!("dst{v}")).await;
        // A fetch of a configured remote follows tags, with `include-tag`.
        git.run(&dst, &["remote", "add", "-t", "main", "origin", &alice])
            .await;
        let proto = format!("protocol.version={v}");
        git.run(&dst, &["-c", &proto, "fetch", "-q", "origin"])
            .await;
        let (has_v1, _, _) = git.try_run(&dst, &["cat-file", "-e", &v1]).await;
        assert!(has_v1, "v{v}: v1 was not included");
        let (has_hidden, _, _) = git.try_run(&dst, &["cat-file", "-e", &hidden]).await;
        assert!(!has_hidden, "v{v}: a hidden tag was included");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hidden_commit_stays_fetchable_by_oid() {
    let (server, host) = start().await;
    let git = Git::new();
    let alice = url(&server, Some("alice"), "r");
    let (src, _, secret) = source(&git).await;
    git.run(&src, &["push", &alice, "main", "secret"]).await;
    host.hide.lock().unwrap().insert("refs/heads/secret".into());

    for v in [2, 0] {
        let dst = git.init(&format!("dst{v}")).await;
        git.run(&dst, &["remote", "add", "origin", &alice]).await;
        let proto = format!("protocol.version={v}");
        git.run(&dst, &["-c", &proto, "fetch", "-q", "origin", "main"])
            .await;
        let (ok, _, _) = git.try_run(&dst, &["cat-file", "-e", &secret]).await;
        assert!(!ok, "v{v}: the hidden commit came without a want");
        git.run(&dst, &["-c", &proto, "fetch", "-q", "origin", &secret])
            .await;
        git.run(&dst, &["cat-file", "-e", &secret]).await;
    }
}

/// Without `create_on_push`, the authorizer answers before the lookup.
///
/// A push to a granted repository that is missing is not found.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn granted_missing_repository_is_not_found_without_create_on_push() {
    let host = Arc::new(Host::default());
    let hooks = HostHooks {
        authorize: Some(host.clone()),
        ..HostHooks::default()
    };
    let server = Server::start_hooked(hooks, |c| c.compute.create_on_push = false).await;
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "f", "1\n", "1").await;

    let res = info_refs(&server, "r", "git-receive-pack", None).await;
    assert_eq!(res.status(), 401);
    let res = info_refs(&server, "missing/r", "git-receive-pack", Some(ALICE)).await;
    assert_eq!(res.status(), 404);
    let res = info_refs(&server, "r", "git-receive-pack", Some(ALICE)).await;
    assert_eq!(res.status(), 404);
    let alice = url(&server, Some("alice"), "r");
    let (ok, _, err) = git.try_run(&src, &["push", &alice, "main"]).await;
    assert!(!ok);
    assert!(err.contains("not found"), "{err}");
    let compute = server.node.compute().unwrap();
    assert!(compute.get_repo("store/r").await.unwrap().is_none());

    compute.create_repo("store/r", "").await.unwrap();
    git.run(&src, &["push", &alice, "main"]).await;
}

/// With an authorizer, the host judges every path that names a repository.
///
/// Only paths that name none are refused before it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_host_judges_paths_that_are_not_repository_names() {
    let (server, host) = start().await;
    let git = Git::new();
    let src = git.init("src").await;
    git.commit_file(&src, "f", "1\n", "1").await;
    git.run(&src, &["push", &url(&server, Some("alice"), "r"), "main"])
        .await;
    host.spelled.lock().unwrap().clear();

    for (path, spelled) in [
        ("alias/Hello%20World~1", "alias/Hello World~1.git"),
        ("alias/.hidden/x", "alias/.hidden/x.git"),
        ("alias/%C3%A9t%C3%A9", "alias/\u{e9}t\u{e9}.git"),
    ] {
        let res = info_refs(&server, path, "git-upload-pack", Some(ALICE)).await;
        assert_eq!(res.status(), 200, "{path}");
        let asked = host.spelled.lock().unwrap().pop();
        assert_eq!(asked.as_deref(), Some(spelled));
    }

    // An invalid grant is the host's fault.
    let res = info_refs(&server, "bad/x", "git-upload-pack", Some(ALICE)).await;
    assert_eq!(res.status(), 500);
    host.spelled.lock().unwrap().clear();

    // The URL parser drops `.` and `..` segments, so `http.rs` tests those.
    let long = format!("alias/{}", "x".repeat(1024));
    for path in [
        "alias/a%2Fb",
        "alias//x",
        "alias/%0Ax",
        "alias/%FF",
        long.as_str(),
    ] {
        let res = info_refs(&server, path, "git-upload-pack", Some(ALICE)).await;
        assert_eq!(res.status(), 404, "{path}");
    }
    assert!(host.spelled.lock().unwrap().is_empty());
}

/// `lines` as pkt-lines: `""` is a flush and `"\x01"` a delimiter.
///
/// It is not the server's encoder, so that the tests below check it too.
fn pkt(lines: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for line in lines {
        match *line {
            "" => out.extend_from_slice(b"0000"),
            "\x01" => out.extend_from_slice(b"0001"),
            line => {
                out.extend_from_slice(format!("{:04x}", line.len() + 4).as_bytes());
                out.extend_from_slice(line.as_bytes());
            }
        }
    }
    out
}

/// The answer to a protocol v2 `ls-refs` with `args`, for `path`.
async fn ls_refs(server: &Server, path: &str, args: &[&str]) -> Vec<u8> {
    let mut lines = vec!["command=ls-refs\n", "\x01"];
    lines.extend(args);
    lines.push("");
    let res = reqwest::Client::new()
        .post(format!("{}/{path}.git/git-upload-pack", server.url))
        .header("authorization", ALICE)
        .header("git-protocol", "version=2")
        .header("content-type", "application/x-git-upload-pack-request")
        .body(pkt(&lines))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.bytes().await.unwrap().to_vec()
}

/// The answer to an `info/refs` for `service`, for `path`.
async fn advertised(server: &Server, path: &str, service: &str) -> Vec<u8> {
    let res = info_refs(server, path, service, Some(ALICE)).await;
    assert_eq!(res.status(), 200);
    res.bytes().await.unwrap().to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn advertisements_keep_their_bytes() {
    let (server, host) = start().await;
    let git = Git::new();
    let alice = url(&server, Some("alice"), "r");
    let (src, main, _) = source(&git).await;
    git.run(&src, &["push", &alice, "main", "secret", "v1"])
        .await;
    let tag = git.rev(&src, "v1").await;
    host.hide.lock().unwrap().insert("refs/heads/secret".into());
    let agent = enroute_compute::testing::AGENT;
    let fetch_caps = "multi_ack_detailed side-band-64k thin-pack ofs-delta shallow \
        deepen-relative no-progress include-tag allow-tip-sha1-in-want \
        allow-reachable-sha1-in-want filter object-format=sha1";
    let push_caps = "report-status delete-refs side-band-64k quiet atomic ofs-delta push-options \
        object-format=sha1";

    let head = format!("{main} HEAD\0{fetch_caps} agent={agent} symref=HEAD:refs/heads/main\n");
    let main_line = format!("{main} refs/heads/main\n");
    let tag_line = format!("{tag} refs/tags/v1\n");
    let peeled_line = format!("{main} refs/tags/v1^{{}}\n");
    assert_eq!(
        advertised(&server, "r", "git-upload-pack").await,
        pkt(&[
            "# service=git-upload-pack\n",
            "",
            &head,
            &main_line,
            &tag_line,
            &peeled_line,
            ""
        ])
    );
    let first = format!("{main} refs/heads/main\0{push_caps} agent={agent}\n");
    assert_eq!(
        advertised(&server, "r", "git-receive-pack").await,
        pkt(&["# service=git-receive-pack\n", "", &first, &tag_line, ""])
    );
    let head = format!("{main} HEAD symref-target:refs/heads/main\n");
    let peeled_tag = format!("{tag} refs/tags/v1 peeled:{main}\n");
    let all = ["symrefs\n", "peel\n", "unborn\n"];
    assert_eq!(
        ls_refs(&server, "r", &all).await,
        pkt(&[&head, &main_line, &peeled_tag, ""])
    );
    let plain_head = format!("{main} HEAD\n");
    assert_eq!(
        ls_refs(&server, "r", &[]).await,
        pkt(&[&plain_head, &main_line, &tag_line, ""])
    );
    let tags = ["peel\n", "ref-prefix refs/tags/\n"];
    assert_eq!(ls_refs(&server, "r", &tags).await, pkt(&[&peeled_tag, ""]));
    let heads = ["symrefs\n", "ref-prefix HEAD\n", "ref-prefix refs/heads/\n"];
    assert_eq!(
        ls_refs(&server, "r", &heads).await,
        pkt(&[&head, &main_line, ""])
    );

    // HEAD goes with the branch it points at.
    host.hide.lock().unwrap().insert("refs/heads/main".into());
    let first = format!("{tag} refs/tags/v1\0{fetch_caps} agent={agent}\n");
    assert_eq!(
        advertised(&server, "r", "git-upload-pack").await,
        pkt(&["# service=git-upload-pack\n", "", &first, &peeled_line, ""])
    );
    assert_eq!(ls_refs(&server, "r", &all).await, pkt(&[&peeled_tag, ""]));

    // An empty repository: no refs, and an unborn HEAD.
    let compute = server.node.compute().unwrap();
    compute.create_repo("store/empty", "").await.unwrap();
    let zero = "0".repeat(40);
    let first = format!("{zero} capabilities^{{}}\0{fetch_caps} agent={agent}\n");
    assert_eq!(
        advertised(&server, "empty", "git-upload-pack").await,
        pkt(&["# service=git-upload-pack\n", "", &first, ""])
    );
    let first = format!("{zero} capabilities^{{}}\0{push_caps} agent={agent}\n");
    assert_eq!(
        advertised(&server, "empty", "git-receive-pack").await,
        pkt(&["# service=git-receive-pack\n", "", &first, ""])
    );
    assert_eq!(
        ls_refs(&server, "empty", &all).await,
        pkt(&["unborn HEAD symref-target:refs/heads/main\n", ""])
    );
    assert_eq!(ls_refs(&server, "empty", &[]).await, pkt(&[""]));
}
