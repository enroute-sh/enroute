//! Oracle `git daemon` lifecycle management.
//!
//! The daemon runs in a process group of its own, which its forked
//! connection handlers join, and Drop kills the whole group so none leaks
//! past it. The daemon stays this process's child until Drop reaps it, so
//! its pid, the group's id, names no other process meanwhile.
#![allow(
    clippy::unwrap_used,
    reason = "e2e support module, never compiled into the production build"
)]

use std::os::unix::process::CommandExt as _;
use std::process::{Child, Stdio};

pub(crate) struct OracleDaemon {
    child: Child,
    pub(crate) url: String,
}

impl OracleDaemon {
    /// Spawn a `git daemon` serving a fresh bare `format` repo rooted at `tmp` and
    /// block until it is ready to serve the git protocol.
    pub(crate) fn spawn(tmp: &std::path::Path, format: crate::model::Format) -> Self {
        let daemon_base = tmp.join("daemon-base");
        let oracle_repo = daemon_base.join("repo.git");
        std::fs::create_dir_all(&oracle_repo).unwrap();
        crate::support::isolated_git()
            .args(["init", "--quiet", "--bare", "-b", "main"])
            .arg(format!("--object-format={}", format.name()))
            .arg(&oracle_repo)
            .status()
            .unwrap();
        // allowFilter: off by default; without it git silently ignores `--filter`
        // and serves a full pack — masking enroute advertising `filter` incorrectly too.
        // allowAnySHA1InWant: lazy backfill `want`s a blob oid directly, which
        // upload-pack otherwise refuses to serve.
        for (key, value) in [
            ("uploadpack.allowFilter", "true"),
            ("uploadpack.allowAnySHA1InWant", "true"),
        ] {
            crate::support::isolated_git()
                .args(["-C", oracle_repo.to_str().unwrap(), "config", key, value])
                .status()
                .unwrap();
        }

        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };

        let daemon = Self {
            child: crate::support::isolated_git()
                .args([
                    "daemon",
                    "--reuseaddr",
                    "--export-all",
                    "--enable=receive-pack",
                    "--listen=127.0.0.1",
                    &format!("--port={port}"),
                    &format!("--base-path={}", daemon_base.display()),
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .process_group(0)
                .spawn()
                .unwrap(),
            url: format!("git://127.0.0.1:{port}/repo.git"),
        };

        // Poll via `ls-remote`, not a raw TCP connect — the kernel accepts the
        // socket before git daemon's protocol handler is ready.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let ready = crate::support::isolated_git()
                .args(["ls-remote", &daemon.url])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            if ready {
                return daemon;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "git daemon did not become ready on port {port} in time"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
}

impl Drop for OracleDaemon {
    fn drop(&mut self) {
        // The whole group: the listener and its handler children. Unreaped,
        // the daemon still holds the group's id, so this kills nothing else.
        drop(
            std::process::Command::new("kill")
                // `--`: procps `kill` reads `-<pgid>` as an option without it.
                .args(["-s", "KILL", "--", &format!("-{}", self.child.id())])
                .status(),
        );
        drop(self.child.wait());
    }
}
