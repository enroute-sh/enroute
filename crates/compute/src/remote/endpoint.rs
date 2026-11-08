//! Which remote may be dialed, and the URLs a push or a fetch reaches it at.
//!
//! A caller names the URL and Enroute dials it from inside the deployment's
//! own network, where a loopback or a link-local address is somebody else's
//! database or the cloud's metadata service. So a remote is checked before
//! it is dialed, and a redirect away from it is refused rather than checked.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use url::Url;

use super::Error;

/// Which remotes a deployment lets Enroute dial.
#[derive(Debug, Clone, Copy, Default)]
pub struct Reach {
    /// Allow `http://`, and hosts that answer on an address which is not on
    /// the public internet.
    ///
    /// Off by default: the caller chooses the URL, and Enroute dials it from
    /// where it reaches what nobody outside can.
    pub private: bool,
}

/// A remote's smart-HTTP endpoints.
#[derive(Debug, Clone)]
pub(super) struct Endpoint {
    base: Url,
}

impl Endpoint {
    /// The endpoint at `url`, refused if this deployment may not dial it.
    ///
    /// A name's addresses are checked as the connection resolves them, by
    /// [`PublicOnly`].
    pub(super) fn parse(url: &str, reach: Reach) -> Result<Self, Error> {
        let url = Url::parse(url).map_err(|error| Error::Remote(format!("not a URL: {error}")))?;
        Self::at(url, reach)
    }

    fn at(base: Url, reach: Reach) -> Result<Self, Error> {
        check(&base, reach)?;
        Ok(Self { base })
    }

    /// Where the remote advertises what it holds.
    pub(super) fn advertisement(&self) -> Result<Url, Error> {
        self.extend("/info/refs?service=git-receive-pack")
    }

    /// The URL this endpoint hangs off, safe to record: [`check`] refused
    /// any credentials inside it before one of these existed.
    pub(super) fn url(&self) -> &Url {
        &self.base
    }

    /// Where a push is sent.
    pub(super) fn receive_pack(&self) -> Result<Url, Error> {
        self.extend("/git-receive-pack")
    }

    /// Where the remote says what a fetch can ask it for.
    pub(super) fn upload_pack_capabilities(&self) -> Result<Url, Error> {
        self.extend("/info/refs?service=git-upload-pack")
    }

    /// Where a fetch's commands are sent.
    pub(super) fn upload_pack(&self) -> Result<Url, Error> {
        self.extend("/git-upload-pack")
    }

    /// `base` with `suffix` on the end.
    ///
    /// Concatenated rather than joined: `Url::join` replaces a path's last
    /// segment, which is the repository's own name.
    fn extend(&self, suffix: &str) -> Result<Url, Error> {
        let base = self.base.as_str().trim_end_matches('/');
        Url::parse(&format!("{base}{suffix}"))
            .map_err(|error| Error::Remote(format!("not a URL: {error}")))
    }
}

/// Refuse a URL this deployment may not dial.
fn check(url: &Url, reach: Reach) -> Result<(), Error> {
    match url.scheme() {
        "https" => {}
        "http" if reach.private => {}
        "http" => {
            return Err(Error::Remote(
                "http would send the credentials in the clear; use https".into(),
            ));
        }
        scheme => {
            return Err(Error::Remote(format!(
                "{scheme} is not a transport Enroute pushes over; use https"
            )));
        }
    }
    // Credentials in the URL end up in every log line that reports which
    // remote a push went to. The call carries them in a field of their own.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::Remote(
            "put the credentials in `credentials`, not in the URL".into(),
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(Error::Remote("a git remote URL carries no query".into()));
    }
    let host = url
        .host()
        .ok_or_else(|| Error::Remote("the URL names no host".into()))?;
    if reach.private {
        return Ok(());
    }
    // A name is checked where it is resolved, by [`PublicOnly`]; an address
    // reaches no resolver, so it is checked here.
    let address = match host {
        url::Host::Ipv4(ip) => IpAddr::V4(ip),
        url::Host::Ipv6(ip) => IpAddr::V6(ip),
        url::Host::Domain(_) => return Ok(()),
    };
    if !is_public(address) {
        return Err(Error::Remote(format!(
            "{host} is {address}, which is not on the public internet"
        )));
    }
    Ok(())
}

/// The client's resolver where only public remotes may be dialed: it
/// refuses an answer that names an address off the public internet, so the
/// addresses checked are the ones the connection dials, whatever a name
/// answered when [`check`] looked it up.
#[derive(Debug)]
pub(super) struct PublicOnly;

impl reqwest::dns::Resolve for PublicOnly {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let name = name.as_str().to_string();
        Box::pin(async move {
            // The port is the connection's to set; this answers for the name.
            let addresses: Vec<SocketAddr> =
                tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            if let Some(address) = addresses.iter().find(|a| !is_public(a.ip())) {
                return Err(format!(
                    "{name} answers on {}, which is not on the public internet",
                    address.ip()
                )
                .into());
            }
            let addresses: reqwest::dns::Addrs = Box::new(addresses.into_iter());
            Ok(addresses)
        })
    }
}

/// Whether an address is one the public internet routes to.
fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [first, second, third, _] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        // "This network", IETF protocol assignments, carrier-grade NAT,
        // benchmarking, and the reserved top of the space — none of which
        // `std` has a predicate for, and `is_unspecified` covers only the
        // one address rather than the block around it.
        || first == 0
        || (first == 192 && second == 0 && third == 0)
        || (first == 100 && (64..128).contains(&second))
        || (first == 198 && (18..20).contains(&second))
        || first >= 240)
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    // Both ways of writing a v4 address in v6, since either would otherwise
    // carry a private address past the v4 rule above.
    if let Some(mapped) = ip.to_ipv4_mapped().or_else(|| compatible_v4(ip)) {
        return is_public_v4(mapped);
    }
    let [first, second, third, fourth, ..] = ip.segments();
    !(ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        // Unique local, link local, and documentation.
        || (first & 0xfe00) == 0xfc00
        || (first & 0xffc0) == 0xfe80
        || (first == 0x2001 && second == 0x0db8)
        // NAT64, which on an IPv6-only network is how a v4 address is
        // reached at all — including the metadata service's.
        || (first == 0x0064 && second == 0xff9b && third == 0 && fourth == 0))
}

/// The address in a deprecated IPv4-compatible `::a.b.c.d`, if it is one.
///
/// `::` and `::1` are not: they are the unspecified and loopback addresses,
/// which the caller already refuses by name.
fn compatible_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    match ip.octets() {
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, a, b, c, d]
            if u32::from_be_bytes([a, b, c, d]) > 1 =>
        {
            Some(Ipv4Addr::new(a, b, c, d))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PUBLIC: Reach = Reach { private: false };
    const ANY: Reach = Reach { private: true };

    fn refusal(url: &str, reach: Reach) -> String {
        Endpoint::parse(url, reach)
            .expect_err("refused")
            .to_string()
    }

    #[test]
    fn refuses_http_unless_the_deployment_allows_it() {
        assert!(refusal("http://example.com/a.git", PUBLIC).contains("https"));
        Endpoint::parse("http://127.0.0.1:9000/a.git", ANY).expect("allowed");
    }

    #[test]
    fn refuses_a_scheme_that_is_not_http() {
        assert!(refusal("ssh://example.com/a.git", PUBLIC).contains("ssh"));
    }

    #[test]
    fn refuses_credentials_in_the_url() {
        assert!(refusal("https://user:token@example.com/a.git", PUBLIC).contains("credentials"));
    }

    #[test]
    fn refuses_an_address_off_the_public_internet() {
        assert!(refusal("https://127.0.0.1/a.git", PUBLIC).contains("public"));
        assert!(refusal("https://[::1]/a.git", PUBLIC).contains("public"));
        assert!(refusal("https://169.254.169.254/a.git", PUBLIC).contains("public"));
        assert!(refusal("https://10.0.0.1/a.git", PUBLIC).contains("public"));
    }

    /// A name that answered with a public address when it was checked can
    /// answer the connection with another.
    #[tokio::test]
    async fn the_resolver_refuses_a_name_off_the_public_internet() {
        use reqwest::dns::Resolve as _;

        let name = "localhost".parse().expect("a name");
        let refused = PublicOnly
            .resolve(name)
            .await
            .err()
            .expect("refused")
            .to_string();
        assert!(refused.contains("public"), "{refused}");
    }

    /// A name passes [`check`], so the remote client's resolver is what
    /// refuses it.
    #[tokio::test]
    async fn the_client_refuses_a_name_off_the_public_internet() {
        let client = super::super::Client::new(PUBLIC).expect("a client");
        let error = client
            .http
            .get("https://localhost/a.git")
            .send()
            .await
            .expect_err("refused");
        let chain = format!("{:#}", anyhow::Error::from(error));
        assert!(chain.contains("public"), "{chain}");
    }

    #[test]
    fn builds_both_endpoints_under_the_repository_path() {
        let endpoint =
            Endpoint::parse("https://github.com/acme/widgets.git", ANY).expect("allowed");
        assert_eq!(
            endpoint.advertisement().expect("a URL").as_str(),
            "https://github.com/acme/widgets.git/info/refs?service=git-receive-pack"
        );
        assert_eq!(
            endpoint.receive_pack().expect("a URL").as_str(),
            "https://github.com/acme/widgets.git/git-receive-pack"
        );
    }

    #[test]
    fn classifies_addresses() {
        assert!(is_public("140.82.121.4".parse().expect("an address")));
        assert!(is_public(
            "2606:50c0:8000::153".parse().expect("an address")
        ));
        assert!(!is_public("100.64.0.1".parse().expect("an address")));
        assert!(!is_public("::ffff:127.0.0.1".parse().expect("an address")));
        assert!(!is_public("fd00::1".parse().expect("an address")));
        assert!(!is_public("fe80::1".parse().expect("an address")));
    }

    /// Every way the metadata service can be named, since reaching it is
    /// most of what this rule is for.
    #[test]
    fn no_spelling_of_a_private_address_reads_as_public() {
        for address in [
            "169.254.169.254",
            "0.0.0.0",
            "0.1.2.3",
            "192.0.0.1",
            "::ffff:169.254.169.254",
            // The deprecated IPv4-compatible form, and NAT64: the two ways
            // an IPv6-only network still reaches a v4 address.
            "::169.254.169.254",
            "64:ff9b::169.254.169.254",
        ] {
            assert!(
                !is_public(address.parse().expect("an address")),
                "{address} reads as public"
            );
        }
    }
}
