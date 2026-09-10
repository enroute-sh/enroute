//! Oracle `git daemon` lifecycle management.
#![allow(
    clippy::unwrap_used,
    reason = "e2e support module, never compiled into the production build"
)]
//!
//! `git daemon --detach` double-forks + `setsid()`s, so Drop kills the whole
//! group to keep forked connections from leaking past it.

use std::process::Stdio;

pub(crate) struct OracleDaemon {
    pid: u32,
    pub(crate) url: String,
}

impl OracleDaemon {
    /// Spawn a `git daemon` serving a fresh bare repo rooted at `tmp` and
    /// block until it is ready to serve the git protocol.
    pub(crate) fn spawn(tmp: &std::path::Path) -> Self {
        let daemon_base = tmp.join("daemon-base");
        let oracle_repo = daemon_base.join("repo.git");
        std::fs::create_dir_all(&oracle_repo).unwrap();
        std::process::Command::new("git")
            .args(["init", "--quiet", "--bare", "-b", "main"])
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
            std::process::Command::new("git")
                .args(["-C", oracle_repo.to_str().unwrap(), "config", key, value])
                .status()
                .unwrap();
        }

        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };

        let pid_file = tmp.join("daemon.pid");

        let mut child = std::process::Command::new("git")
            .args([
                "daemon",
                "--detach",
                "--reuseaddr",
                "--export-all",
                "--enable=receive-pack",
                "--listen=127.0.0.1",
                &format!("--port={port}"),
                &format!("--pid-file={}", pid_file.display()),
                &format!("--base-path={}", daemon_base.display()),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        // The launcher process exits as soon as the daemon is up; reap it.
        child.wait().unwrap();

        let url = format!("git://127.0.0.1:{port}/repo.git");

        // Poll via `ls-remote`, not a raw TCP connect — the kernel accepts the
        // socket before git daemon's protocol handler is ready. By success, the pid file exists.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let ready = std::process::Command::new("git")
                .args(["ls-remote", &url])
                .env("GIT_TERMINAL_PROMPT", "0")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            if ready {
                let pid: u32 = std::fs::read_to_string(&pid_file)
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                return Self { pid, url };
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
        // Kill the whole process group; takes down the listener and its handler children.
        drop(
            std::process::Command::new("kill")
                .args(["-9", &format!("-{}", self.pid)])
                .status(),
        );
    }
}
