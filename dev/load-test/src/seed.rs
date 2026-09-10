//! Seeds the bench server's repo, either synthetically or from an existing
//! local checkout.
//!
//! [`seed_via_push`] generates a repo with a realistic tree shape — many
//! files across several directories, changing incrementally commit to
//! commit — so pack building and delta compression exercise the way a real
//! repository's history does. [`seed_via_existing_repo`] instead pushes an
//! already-checked-out local repo as-is, for benchmarking against a real
//! repository's shape rather than this synthetic approximation.

use std::path::Path;

use anyhow::Result;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;

/// Number of subdirectories files are spread across.
const DIRS: usize = 6;
/// Number of files touched per commit.
const FILES_PER_COMMIT: usize = 8;
/// Range of file sizes, in bytes.
const FILE_SIZE_RANGE: std::ops::Range<usize> = 200..4_000;
/// Fixed seed so repeated benchmark runs seed byte-identical repositories.
const RNG_SEED: u64 = 0x000C_A121_5EED;

async fn git(args: &[&str], dir: Option<&Path>) -> Result<()> {
    let mut cmd = tokio::process::Command::new("git");
    cmd.env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_AUTHOR_NAME", "Bench")
        .env("GIT_AUTHOR_EMAIL", "bench@example.com")
        .env("GIT_AUTHOR_DATE", "@1000000 +0000")
        .env("GIT_COMMITTER_NAME", "Bench")
        .env("GIT_COMMITTER_EMAIL", "bench@example.com")
        .env("GIT_COMMITTER_DATE", "@1000000 +0000")
        // Always pushes into this harness's own bench server, which reads
        // the namespace from `Host` rather than the URL path (see
        // `CloneBench.host_override`) — override it the same way.
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "http.extraHeader")
        .env("GIT_CONFIG_VALUE_0", format!("Host: {}", crate::OWNER));
    if let Some(d) = dir {
        cmd.current_dir(d);
    }
    cmd.args(args);
    let out = cmd.output().await?;
    if !out.status.success() {
        anyhow::bail!(
            "git {args:?} failed:\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    Ok(())
}

/// Pseudo-source-code text: whitespace-separated tokens from a small
/// vocabulary, repeated to the target length.
///
/// Plain repeated words compress under zlib roughly like real source text,
/// unlike uniform random bytes, which would make every delta unrealistically large.
fn random_text(rng: &mut StdRng, len: usize) -> String {
    const WORDS: &[&str] = &[
        "fn", "let", "mut", "struct", "impl", "return", "self", "match", "Ok", "Err", "Some",
        "None", "pub", "async", "await", "if", "else", "for", "in", "while", "use", "mod",
    ];
    let mut text = String::with_capacity(len);
    while text.len() < len {
        #[expect(
            clippy::indexing_slicing,
            reason = "index is generated in-range by random_range(0..WORDS.len())"
        )]
        let word = WORDS[rng.random_range(0..WORDS.len())];
        text.push_str(word);
        text.push(if rng.random_bool(0.15) { '\n' } else { ' ' });
    }
    text
}

/// Create a local git repo with `commits` commits, each touching
/// [`FILES_PER_COMMIT`] files across [`DIRS`] subdirectories, and push it to `url`.
pub(crate) async fn seed_via_push(url: &str, commits: usize) -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    let mut rng = StdRng::seed_from_u64(RNG_SEED);

    git(&["init", "-b", "main"], Some(dir)).await?;
    git(
        &["-c", "protocol.version=2", "remote", "add", "origin", url],
        Some(dir),
    )
    .await?;

    for d in 0..DIRS {
        tokio::fs::create_dir_all(dir.join(format!("dir{d}"))).await?;
    }

    for c in 0..commits {
        for f in 0..FILES_PER_COMMIT {
            let d = f % DIRS;
            let filename = format!("dir{d}/file{c}-{f}.txt");
            let size = rng.random_range(FILE_SIZE_RANGE);
            let content = random_text(&mut rng, size);
            tokio::fs::write(dir.join(&filename), content).await?;
        }
        git(&["add", "-A"], Some(dir)).await?;
        git(&["commit", "-m", &format!("commit {c}")], Some(dir)).await?;
    }

    git(
        &["-c", "protocol.version=2", "push", "origin", "main"],
        Some(dir),
    )
    .await?;

    Ok(())
}

/// Push an existing local repo's checked-out commit into `url`, for
/// load-testing against a real repository's shape.
///
/// Only pushes — nothing in the working tree or `.git/config` is touched —
/// and always to `refs/heads/main`, matching [`super::client::GitHttpClient::ls_refs_tip`].
pub(crate) async fn seed_via_existing_repo(url: &str, repo: &Path) -> Result<()> {
    git(
        &[
            "-c",
            "protocol.version=2",
            "push",
            url,
            "HEAD:refs/heads/main",
        ],
        Some(repo),
    )
    .await?;

    Ok(())
}
