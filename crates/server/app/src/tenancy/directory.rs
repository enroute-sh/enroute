//! The tenants a deployment serves, indexed by every way a request names one.
//!
//! Read from a URI and re-read from it, so this is what a request resolves
//! against and never what anything writes. Every check a database constraint
//! would have made happens once at load — an id is unique, and a hostname
//! reaches one tenant — so a list that would not have loaded is refused where
//! it is published rather than at the request that trips over it.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, anyhow, bail};
use serde::Deserialize;

use super::{Tenant, TenantId};

/// The tenants a deployment named.
///
/// Built once and read from every thread, so each lookup answers with a clone
/// rather than a borrow and nothing here is behind a lock.
#[derive(Debug, Clone, Default)]
pub struct Directory {
    by_id: HashMap<TenantId, Tenant>,
    /// Hostnames claimed outright.
    by_domain: HashMap<String, Tenant>,
    /// Suffix claims, most specific first — see [`Directory::by_host`].
    wildcards: Vec<Wildcard>,
}

/// A `*.example.com` claim, or the `*` that claims whatever is left.
#[derive(Debug, Clone)]
struct Wildcard {
    /// What a hostname must end with: `.example.com`, or empty for `*`.
    ///
    /// How many labels it names is how specific it is, counted where they are
    /// sorted rather than carried beside this and able to disagree with it.
    suffix: String,
    tenant: Tenant,
}

/// What a tenants file holds.
///
/// A map rather than a list, so a tenant's id is the table naming it and two
/// tenants cannot share one — TOML refuses the duplicate key itself.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default)]
    tenants: BTreeMap<String, Entered>,
}

/// One `[[tenants]]` table.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entered {
    /// Where this tenant's application answers.
    hook_endpoint_url: String,
    /// The hostnames that reach them, `*.example.com` and `*` included.
    #[serde(default)]
    domains: Vec<String>,
}

impl Directory {
    /// The tenants `toml` names.
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not parse, or if any check on what it
    /// named fails.
    pub fn from_toml(toml: &str) -> Result<Self> {
        let file: File = toml::from_str(toml)?;
        if file.tenants.is_empty() {
            bail!("names no tenants, so nothing it served could be reached");
        }
        let mut directory = Self::default();
        for (id, one) in file.tenants {
            directory.add(&id, one)?;
        }
        // Most specific first, so `by_host` takes the first that matches and
        // `*` is simply the one claiming no labels at all.
        directory
            .wildcards
            .sort_by_key(|wildcard| std::cmp::Reverse(wildcard.suffix.matches('.').count()));
        Ok(directory)
    }

    fn add(&mut self, id: &str, entered: Entered) -> Result<()> {
        let Entered {
            hook_endpoint_url,
            domains,
        } = entered;
        let id = check_id(id)?;
        // Parsed here and carried, so the hook path has neither the work nor
        // an error for something this already proved.
        let hook_endpoint_url = hook_endpoint_url
            .parse()
            .map_err(|error| anyhow!("tenant {id}: the hook endpoint URL: {error}"))?;
        let tenant = Tenant {
            id: id.clone(),
            hook_endpoint_url,
        };
        // No duplicate check: the id is a TOML key, so a second one is a
        // duplicate table and never reaches here.
        drop(self.by_id.insert(id.clone(), tenant.clone()));

        for domain in &domains {
            self.claim(domain, &tenant)?;
        }
        Ok(())
    }

    /// Point one `domains` entry at `tenant`, whichever shape it has.
    fn claim(&mut self, domain: &str, tenant: &Tenant) -> Result<()> {
        let domain = domain.trim().to_lowercase();
        let id = &tenant.id;
        // `*` and `*.example.com` are the same mechanism: a suffix a hostname
        // must end with, and `*` is the suffix every hostname ends with.
        let suffix = match domain.as_str() {
            "*" => Some(String::new()),
            other => other.strip_prefix('*').map(str::to_string),
        };
        let Some(suffix) = suffix else {
            if domain.is_empty() || domain.contains('*') {
                bail!("tenant {id}: {domain} is not a hostname or a *.suffix");
            }
            return match self.by_domain.entry(domain.clone()) {
                Entry::Occupied(taken) => {
                    bail!("{domain} reaches both {} and {id}", taken.get().id)
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(tenant.clone());
                    Ok(())
                }
            };
        };

        // Only a leading `*.`: `foo.*.com` and `*foo.com` have no reading that
        // is worth the ambiguity of guessing at one.
        if !suffix.is_empty() && (!suffix.starts_with('.') || suffix.contains('*')) {
            bail!("tenant {id}: {domain} is not a hostname or a *.suffix");
        }
        if let Some(held) = self.wildcards.iter().find(|w| w.suffix == suffix) {
            bail!("{domain} reaches both {} and {id}", held.tenant.id);
        }
        self.wildcards.push(Wildcard {
            suffix,
            tenant: tenant.clone(),
        });
        Ok(())
    }

    /// Every tenant this named, by id, for an operator reading it back.
    #[must_use]
    pub fn listing(&self) -> Vec<Listed<'_>> {
        let mut listing: Vec<Listed<'_>> = self
            .by_id
            .values()
            .map(|tenant| Listed {
                tenant,
                domains: self
                    .by_domain
                    .iter()
                    .filter(|(_, held)| held.id == tenant.id)
                    .map(|(domain, _)| domain.clone())
                    .chain(
                        self.wildcards
                            .iter()
                            .filter(|w| w.tenant.id == tenant.id)
                            .map(|w| format!("*{}", w.suffix)),
                    )
                    .collect(),
            })
            .collect();
        listing.sort_by(|one, two| one.tenant.id.cmp(&two.tenant.id));
        for one in &mut listing {
            one.domains.sort();
        }
        listing
    }

    /// How many tenants this names.
    #[must_use]
    #[expect(
        clippy::len_without_is_empty,
        reason = "loading refuses an empty directory, so nothing asks"
    )]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// The tenant with this id.
    #[must_use]
    pub(super) fn by_id(&self, id: &str) -> Option<Tenant> {
        self.by_id.get(id).cloned()
    }

    /// The tenant a request on `host` belongs to, `host` already lowercased
    /// and stripped of its port.
    ///
    /// Most specific first: a hostname claimed outright, then suffix claims by
    /// how many labels each of them pins.
    #[must_use]
    pub(super) fn by_host(&self, host: &str) -> Option<Tenant> {
        if let Some(tenant) = self.by_domain.get(host) {
            return Some(tenant.clone());
        }
        self.wildcards
            .iter()
            .find(|w| host.ends_with(&w.suffix))
            .map(|w| w.tenant.clone())
    }
}

/// One tenant a list named, and every way a request reaches them.
#[derive(Debug, Clone)]
pub struct Listed<'a> {
    /// Who this is.
    pub tenant: &'a Tenant,
    /// The hostnames pointed at them, wildcards written as they were.
    pub domains: Vec<String>,
}

/// An id, which the ledger keys by and so may never change.
///
/// No dots, because the id is the TOML table naming a tenant and a dot there is
/// what starts a sub-table: `[tenants.a.b]` is `a`'s `b`, not a tenant `a.b`.
fn check_id(id: &str) -> Result<TenantId> {
    let id = id.trim();
    let usable = !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'));
    if !usable {
        bail!("{id:?} is not an id: 1 to 64 of a-z, 0-9, dash or underscore");
    }
    Ok(TenantId::checked(id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One `[tenants.<id>]` table, since every test here needs the same
    /// two fields and cares about one of them.
    fn entry(id: &str, domains: &str) -> String {
        format!(
            "[tenants.{id}]\n\
             hook_endpoint_url = \"https://{id}.example/hooks\"\n{domains}\n"
        )
    }

    fn two() -> Directory {
        Directory::from_toml(&format!(
            "{}{}",
            entry("acme", "domains = [\"git.acme.com\", \"acme.enroute.sh\"]"),
            entry("other", "domains = [\"*\"]"),
        ))
        .unwrap()
    }

    /// The order a hostname is resolved in, with `*` as the last wildcard
    /// rather than a mechanism of its own.
    #[test]
    fn a_hostname_matches_the_most_specific_claim() {
        let directory = two();
        assert_eq!(
            directory.by_host("git.acme.com").unwrap().id.as_str(),
            "acme"
        );
        assert_eq!(
            directory.by_host("acme.enroute.sh").unwrap().id.as_str(),
            "acme"
        );
        assert_eq!(
            directory.by_host("nobody.example").unwrap().id.as_str(),
            "other"
        );
        // A name claimed outright is that name and nothing under it.
        assert_eq!(
            directory
                .by_host("git.acme.enroute.sh")
                .unwrap()
                .id
                .as_str(),
            "other"
        );
    }

    /// The whole point of unifying them: a suffix claim beats a broader one,
    /// and `*` is simply the broadest.
    #[test]
    fn a_longer_suffix_wins_over_a_shorter_one() {
        let directory = Directory::from_toml(&format!(
            "{}{}{}",
            entry("deep", "domains = [\"*.eu.acme.com\"]"),
            entry("shallow", "domains = [\"*.acme.com\"]"),
            entry("rest", "domains = [\"*\"]"),
        ))
        .unwrap();

        assert_eq!(
            directory.by_host("x.eu.acme.com").unwrap().id.as_str(),
            "deep"
        );
        assert_eq!(
            directory.by_host("x.acme.com").unwrap().id.as_str(),
            "shallow"
        );
        assert_eq!(
            directory.by_host("somewhere.else").unwrap().id.as_str(),
            "rest"
        );
        // A suffix claims what is *under* it, never the name itself.
        assert_eq!(directory.by_host("acme.com").unwrap().id.as_str(), "rest");
    }

    /// A hostname claimed outright beats any suffix that would also match it,
    /// which is what lets one tenant sit inside another's subdomain space.
    #[test]
    fn an_exact_claim_beats_a_suffix() {
        let directory = Directory::from_toml(&format!(
            "{}{}",
            entry(
                "exact",
                "domains = [\"git.acme.com\", \"named.enroute.sh\"]"
            ),
            entry("suffix", "domains = [\"*.acme.com\", \"*.enroute.sh\"]"),
        ))
        .unwrap();

        assert_eq!(
            directory.by_host("git.acme.com").unwrap().id.as_str(),
            "exact"
        );
        assert_eq!(
            directory.by_host("other.acme.com").unwrap().id.as_str(),
            "suffix"
        );
        assert_eq!(
            directory.by_host("named.enroute.sh").unwrap().id.as_str(),
            "exact"
        );
        assert_eq!(
            directory.by_host("anyone.enroute.sh").unwrap().id.as_str(),
            "suffix"
        );
    }

    /// A tenant reached only over the contract claims no hostname, which is
    /// what makes `domains` optional rather than a field to invent a value for.
    #[test]
    fn a_tenant_needs_no_hostname() {
        let directory = Directory::from_toml(&entry("acme", "")).unwrap();

        assert_eq!(directory.by_id("acme").unwrap().id.as_str(), "acme");
        assert!(directory.by_host("anything").is_none());
    }

    /// A tenant may be moved to another hostname without any repository
    /// moving, because the ledger keys by the id alone.
    #[test]
    fn a_hostname_may_change_while_the_id_does_not() {
        let renamed =
            Directory::from_toml(&entry("acme", "domains = [\"acme-corp.enroute.sh\"]")).unwrap();

        assert_eq!(
            renamed.by_host("acme-corp.enroute.sh").unwrap().id.as_str(),
            "acme"
        );
        assert!(renamed.by_host("acme.enroute.sh").is_none());
        assert_eq!(renamed.by_id("acme").unwrap().id.as_str(), "acme");
    }

    /// The id is the table naming a tenant, so a second one is a duplicate
    /// key and TOML refuses it before any of this runs.
    #[test]
    fn two_tenants_cannot_share_an_id() {
        let toml = format!("{}{}", entry("acme", ""), entry("acme", ""));
        let error = format!("{:#}", Directory::from_toml(&toml).unwrap_err());
        assert!(error.contains("duplicate key"), "{error}");
    }

    #[test]
    fn a_collision_is_refused_rather_than_resolved() {
        for (bad, why) in [
            (
                format!(
                    "{}{}",
                    entry("one", "domains = [\"git.example.com\"]"),
                    entry("two", "domains = [\"git.example.com\"]")
                ),
                "two tenants with one domain",
            ),
            (
                format!(
                    "{}{}",
                    entry("one", "domains = [\"*\"]"),
                    entry("two", "domains = [\"*\"]")
                ),
                "two catch-alls",
            ),
        ] {
            Directory::from_toml(&bad).expect_err(why);
        }
    }

    /// A wildcard anywhere but the front has no reading worth guessing at.
    #[test]
    fn only_a_leading_wildcard_is_a_wildcard() {
        for bad in ["foo.*.com", "*foo.com", "fo*o.com", ""] {
            let toml = entry("acme", &format!("domains = [\"{bad}\"]"));
            Directory::from_toml(&toml).expect_err(bad);
        }
    }

    /// A list that named nothing would serve nothing, and is far more likely
    /// to be the wrong URI than a deliberate choice.
    #[test]
    fn a_list_naming_no_tenants_is_refused() {
        Directory::from_toml("").expect_err("a list with no tenants");
    }

    #[test]
    fn an_id_a_ledger_could_not_key_by_is_refused() {
        // A dot most of all: it is what starts a sub-table, so an id
        // carrying one could not be told from `[tenants.a.b]`.
        for bad in ["", "a b", "a/b", "acme.eu"] {
            check_id(bad).expect_err(bad);
        }
        assert_eq!(check_id(" acme_eu-1 ").unwrap().as_str(), "acme_eu-1");
    }

    #[test]
    fn an_endpoint_that_is_not_a_url_is_refused_at_startup() {
        let toml = "[tenants.acme]\nhook_endpoint_url = \"not a url\"\n";
        Directory::from_toml(toml).expect_err("an endpoint that is not a URL");
    }

    /// Every way a request reaches a tenant, which is what a reader of a
    /// loaded list has to be able to see.
    #[test]
    fn a_listing_names_every_way_a_tenant_is_reached() {
        let directory = two();
        let listing = directory.listing();
        let [acme, other] = listing.as_slice() else {
            panic!("two tenants, sorted by id");
        };

        assert_eq!(acme.tenant.id.as_str(), "acme");
        assert_eq!(acme.domains, ["acme.enroute.sh", "git.acme.com"]);

        assert_eq!(other.tenant.id.as_str(), "other");
        assert_eq!(other.domains, ["*"]);
    }
}
