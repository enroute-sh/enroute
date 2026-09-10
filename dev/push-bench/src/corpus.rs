//! The packfile under test, and enough of a record to know two runs measured
//! the same bytes.
//!
//! Packs are cached outside the repo, so a worktree per experiment doesn't
//! mean a rebuild per experiment. The cache key includes the tip commit — the
//! only thing that changes what `pack-objects` produces from a given checkout
//! — so a stale pack can't be reused and there's no freshness check to get
//! wrong.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context as _, Result, anyhow, bail};

/// Object count in the pack header, after `PACK` and the version word.
const OBJECT_COUNT_RANGE: std::ops::Range<usize> = 8..12;

/// A pack's trailing SHA-1 checksum, which git writes over the whole pack.
const TRAILER_LEN: usize = 20;

/// What the harness ingested, carried in every report so two reports that
/// disagree can be checked for whether they measured the same workload at all.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct Corpus {
    pub(crate) tip: String,
    /// The pack's trailing SHA-1 — git's identity for these exact bytes.
    pub(crate) pack_id: String,
    pub(crate) objects: u32,
    pub(crate) bytes: u64,
}

/// Outside the repo, so worktrees share them.
pub(crate) fn cache_dir() -> Result<PathBuf> {
    let base = if let Some(dir) = std::env::var_os("XDG_CACHE_HOME") {
        PathBuf::from(dir)
    } else {
        let home = std::env::var_os("HOME")
            .ok_or_else(|| anyhow!("neither XDG_CACHE_HOME nor HOME is set"))?;
        PathBuf::from(home).join(".cache")
    };
    Ok(base.join("enroute-push-bench"))
}

fn describe(pack: &[u8], bytes: u64, tip: &str) -> Result<Corpus> {
    let count = pack
        .get(OBJECT_COUNT_RANGE)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .ok_or_else(|| anyhow!("pack is too short to hold a header"))?;
    let trailer = pack
        .len()
        .checked_sub(TRAILER_LEN)
        .and_then(|start| pack.get(start..))
        .ok_or_else(|| anyhow!("pack is too short to hold a trailer"))?;
    let pack_id = gix_hash::ObjectId::try_from(trailer)
        .map_err(|e| anyhow!("pack trailer is not a SHA-1: {e}"))?;
    Ok(Corpus {
        tip: tip.to_owned(),
        pack_id: pack_id.to_string(),
        objects: u32::from_be_bytes(count),
        bytes,
    })
}

fn head_of(repo: &Path) -> Result<String> {
    let out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo)
        .output()
        .with_context(|| format!("running git in {}", repo.display()))?;
    if !out.status.success() {
        bail!(
            "git rev-parse HEAD failed in {}: {}",
            repo.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_owned())
}

/// Build the pack a first push of `tip` would send: every object reachable
/// from it, with offset deltas, and nothing assumed already present.
fn generate(repo: &Path, tip: &str, into: &Path) -> Result<()> {
    let partial = into.with_extension("pack.partial");
    let file =
        fs::File::create(&partial).with_context(|| format!("creating {}", partial.display()))?;
    let mut child = Command::new("git")
        .args(["pack-objects", "--revs", "--stdout", "--delta-base-offset"])
        .current_dir(repo)
        .stdin(Stdio::piped())
        .stdout(Stdio::from(file))
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("running git pack-objects in {}", repo.display()))?;
    {
        use std::io::Write as _;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("git pack-objects gave no stdin"))?;
        writeln!(stdin, "{tip}")?;
    }
    if !child.wait()?.success() {
        bail!("git pack-objects failed in {}", repo.display());
    }
    // Rename last: a killed generation leaves a `.partial`, never a truncated
    // pack the next run would happily measure.
    fs::rename(&partial, into)?;
    Ok(())
}

/// The pack for `repo`'s current `HEAD`, generating and caching it on the
/// first request for that tip.
///
/// `report` is called only when work is done, so a cache hit stays silent.
pub(crate) fn for_repo(
    repo: &Path,
    cache: &Path,
    report: impl Fn(&str),
) -> Result<(PathBuf, Corpus)> {
    let tip = head_of(repo)?;
    let name = repo
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("{} has no usable directory name", repo.display()))?;
    fs::create_dir_all(cache).with_context(|| format!("creating {}", cache.display()))?;
    let pack = cache.join(format!("{name}-{tip}.pack"));
    let sidecar = pack.with_extension("json");

    if !pack.exists() || !sidecar.exists() {
        report(&format!(
            "building pack for {name} @ {tip} (first run for this tip; a few minutes)"
        ));
        generate(repo, &tip, &pack)?;
    }
    let corpus = load_or_describe(&pack, &sidecar, &tip)?;
    Ok((pack, corpus))
}

/// An explicitly-named pack, with `tip` supplied by the caller when there's no
/// sidecar to read it from.
pub(crate) fn for_pack(pack: &Path, tip: Option<&str>) -> Result<(PathBuf, Corpus)> {
    let sidecar = pack.with_extension("json");
    let corpus = match (tip, sidecar.exists()) {
        (_, true) => serde_json::from_slice(&fs::read(&sidecar)?)
            .with_context(|| format!("reading {}", sidecar.display()))?,
        (Some(tip), false) => load_or_describe(pack, &sidecar, tip)?,
        (None, false) => bail!(
            "no {} beside the pack, so --tip is required",
            sidecar.display()
        ),
    };
    Ok((pack.to_owned(), corpus))
}

/// Cached in `sidecar` so the description is computed once, not per run.
fn load_or_describe(pack: &Path, sidecar: &Path, tip: &str) -> Result<Corpus> {
    if sidecar.exists() {
        return serde_json::from_slice(&fs::read(sidecar)?)
            .with_context(|| format!("reading {}", sidecar.display()));
    }
    let bytes = fs::read(pack).with_context(|| format!("reading {}", pack.display()))?;
    let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let corpus = describe(&bytes, len, tip)?;
    fs::write(sidecar, serde_json::to_vec_pretty(&corpus)?)
        .with_context(|| format!("writing {}", sidecar.display()))?;
    Ok(corpus)
}
