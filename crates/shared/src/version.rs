//! Release versions and the cluster version, as `CockroachDB` has them.
//!
//! The cluster's version is the release whose behaviour every node runs.
//! What is new in a release (a field, a command or entry kind, a format)
//! stays off until every node runs it and the cluster moves to it; from
//! then on no binary of an older release may run, since it would skip
//! what it cannot read. 0.4 is the first release with a cluster version,
//! so it only refuses a cluster past it; moving the cluster comes with 0.5.

use std::fmt;

use crate::proto;

/// A release, `major.minor`. Patch releases change no format or protocol.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct Version {
    /// The major release.
    pub major: u32,
    /// The minor release.
    pub minor: u32,
}

#[expect(
    clippy::panic,
    reason = "evaluated at compile time: a bad package version fails the build"
)]
const fn number(s: &str) -> u32 {
    match u32::from_str_radix(s, 10) {
        Ok(n) => n,
        Err(_) => panic!("the package version is not a number"),
    }
}

/// The release this binary is.
pub const BINARY: Version = Version {
    major: number(env!("CARGO_PKG_VERSION_MAJOR")),
    minor: number(env!("CARGO_PKG_VERSION_MINOR")),
};

/// Why this binary cannot run a cluster at `cluster`, if it cannot.
///
/// # Errors
///
/// Returns the reason, for the node's operator.
pub fn check(cluster: Version) -> anyhow::Result<()> {
    if BINARY < cluster {
        anyhow::bail!(
            "the cluster is at version {cluster}, past this node's release {BINARY}: run release {cluster} or later"
        );
    }
    Ok(())
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

impl From<Version> for String {
    fn from(v: Version) -> String {
        v.to_string()
    }
}

impl TryFrom<String> for Version {
    type Error = String;

    fn try_from(s: String) -> Result<Version, String> {
        let (major, minor) = s.split_once('.').unwrap_or_default();
        match (major.parse(), minor.parse()) {
            (Ok(major), Ok(minor)) => Ok(Version { major, minor }),
            _ => Err(format!("not a release, `major.minor`: {s:?}")),
        }
    }
}

impl From<proto::Version> for Version {
    fn from(v: proto::Version) -> Version {
        Version {
            major: v.major,
            minor: v.minor,
        }
    }
}

impl From<Version> for proto::Version {
    fn from(v: Version) -> proto::Version {
        proto::Version {
            major: v.major,
            minor: v.minor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A binary runs a cluster at its release or before, never past it.
    #[test]
    fn a_binary_runs_up_to_its_release() {
        check(BINARY).unwrap();
        let next = Version {
            minor: BINARY.minor + 1,
            ..BINARY
        };
        let past = check(next).unwrap_err().to_string();
        assert!(past.contains("past this node's release"), "{past}");
    }

    /// Versions order by major, then minor.
    #[test]
    fn versions_order() {
        let v = |major, minor| Version { major, minor };
        assert!(v(0, 9) < v(0, 10));
        assert!(v(0, 10) < v(1, 0));
        assert_eq!(v(0, 4).to_string(), "0.4");
        assert_eq!(Version::try_from("0.10".to_string()), Ok(v(0, 10)));
        Version::try_from("0.4.1".to_string()).unwrap_err();
    }
}
