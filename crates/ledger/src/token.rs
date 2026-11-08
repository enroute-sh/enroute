//! Tokens for the internal port: who may call this ledger node.
//!
//! A node earns a token by joining (`membership`): it shows a ledger node
//! that it can read the object store and holds an Ed25519 key, and gets a JWT
//! (RFC 7519, `EdDSA` per RFC 8037) naming its key. Each ledger node signs with
//! its own key ([`Issuer`]), whose public half is in its registry entry, so
//! every ledger node checks every token without a shared secret. A token is
//! checked once and then cached until it expires ([`Verifier`]): a call costs
//! a lookup.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::pkcs8::EncodePrivateKey as _;
use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use enroute_shared::auth::Token;

/// How long a token lasts: removing a node's access to the object store
/// takes it out within this long.
pub(crate) const TOKEN_TTL: Duration = Duration::from_secs(3600);

/// Clock difference between nodes that checks tolerate, in seconds.
const SKEW: u64 = 30;

/// Tokens a ledger node keeps checked: about one per node that calls it.
const CACHED: usize = 100_000;

/// Seconds since the Unix epoch.
#[must_use]
pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// A public key from its 32 bytes.
///
/// # Errors
///
/// Returns an error if the bytes are not an Ed25519 public key.
pub(crate) fn public_key(bytes: &[u8]) -> Result<VerifyingKey> {
    let bytes: [u8; 32] = bytes.try_into().context("a public key is 32 bytes")?;
    Ok(VerifyingKey::from_bytes(&bytes)?)
}

/// Whether `signature` is `public`'s signature of `message`.
#[must_use]
pub(crate) fn signed(public: &VerifyingKey, message: &[u8], signature: &[u8]) -> bool {
    Signature::from_slice(signature).is_ok_and(|s| public.verify_strict(message, &s).is_ok())
}

/// A public key as its registry entry holds it: base64url, unpadded (a
/// JWK's `x`, RFC 8037).
#[must_use]
pub(crate) fn encode_key(public: &VerifyingKey) -> String {
    URL_SAFE_NO_PAD.encode(public.as_bytes())
}

/// The key that checks tokens, from [`encode_key`]'s form.
#[must_use]
pub(crate) fn decoding_key(s: &str) -> Option<DecodingKey> {
    DecodingKey::from_ed_components(s).ok()
}

/// A ledger node's token signer: its node id and its key.
pub(crate) struct Issuer {
    id: u64,
    key: EncodingKey,
    public: VerifyingKey,
    decoding: DecodingKey,
}

impl std::fmt::Debug for Issuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Issuer")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl Issuer {
    /// Node `id`'s signer, with the key at `path` (created there if
    /// missing, readable by the owner only).
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be read or written.
    pub(crate) fn load_or_create(path: &Path, id: u64) -> Result<Issuer> {
        let seed = match std::fs::read(path) {
            Ok(seed) => seed,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let seed: [u8; 32] = rand::random();
                enroute_shared::durable::write(path, &seed, 0o600)
                    .with_context(|| format!("creating {}", path.display()))?;
                seed.to_vec()
            }
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        Issuer::new(id, &signing_key(path, seed)?)
    }

    /// The signer whose key is at `path`, if it is one of `nodes` (by its
    /// public key): what a command run on a ledger node signs with.
    ///
    /// # Errors
    ///
    /// Returns an error if the key cannot be read, or is none of theirs.
    pub(crate) fn load_of(path: &Path, nodes: &[crate::proto::Member]) -> Result<Issuer> {
        let seed = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let key = signing_key(path, seed)?;
        let public = encode_key(&key.verifying_key());
        let me = (nodes.iter().find(|n| n.key == public)).with_context(|| {
            format!(
                "{} is no member's key: run this on a ledger node",
                path.display()
            )
        })?;
        Issuer::new(me.id, &key)
    }

    fn new(id: u64, key: &SigningKey) -> Result<Issuer> {
        Ok(Issuer {
            id,
            key: EncodingKey::from_ed_der(key.to_pkcs8_der()?.as_bytes()),
            public: key.verifying_key(),
            decoding: DecodingKey::from_ed_der(key.verifying_key().as_bytes()),
        })
    }

    /// The node that signs.
    #[must_use]
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// The key that checks its tokens.
    #[must_use]
    pub(crate) fn public(&self) -> VerifyingKey {
        self.public
    }

    /// The key that checks its tokens, as [`verify`] takes it.
    #[must_use]
    pub(crate) fn decoding_key(&self) -> &DecodingKey {
        &self.decoding
    }

    /// A token for `sub`, valid for `ttl`.
    #[must_use]
    pub(crate) fn issue(&self, sub: &str, ttl: Duration) -> String {
        let header = Header {
            kid: Some(kid(self.id)),
            ..Header::new(Algorithm::EdDSA)
        };
        let iat = now();
        let claims = Claims {
            iss: kid(self.id),
            sub: sub.to_string(),
            iat,
            exp: iat + ttl.as_secs(),
        };
        // Signing an Ed25519 key's JSON claims does not fail.
        jsonwebtoken::encode(&header, &claims, &self.key).unwrap_or_default()
    }
}

/// A node id as a token names it.
fn kid(id: u64) -> String {
    format!("{id:016x}")
}

/// What a token says.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Claims {
    /// The ledger node that signed it.
    pub iss: String,
    /// The node it names: its key's thumbprint (see
    /// [`enroute_shared::auth::thumbprint`]).
    pub sub: String,
    /// When it was signed (seconds since the epoch).
    pub iat: u64,
    /// When it expires (seconds since the epoch).
    pub exp: u64,
}

impl Claims {
    /// Whether a ledger node signed this token for itself (see
    /// [`self_issued`]): only ledger nodes' own calls carry one.
    #[must_use]
    pub fn is_ledger(&self) -> bool {
        self.sub.strip_prefix("ledger-") == Some(self.iss.as_str())
    }
}

/// The file in a ledger node's data directory that holds its signing key.
pub(crate) const KEY_FILE: &str = "token-key";

/// Why a token was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Refused(&'static str);

/// A token signed by a ledger node whose key is not known (yet).
pub(crate) const UNKNOWN_ISSUER: Refused = Refused("a token from an unknown ledger node");

/// A token that is not a JWT naming its issuer.
pub(crate) const MALFORMED: Refused = Refused("a malformed token");

/// An expired token.
const EXPIRED: Refused = Refused("an expired token");

/// The ledger node that says it issued `token` (not checked).
#[must_use]
pub(crate) fn issuer(token: &str) -> Option<u64> {
    let kid = jsonwebtoken::decode_header(token).ok()?.kid?;
    u64::from_str_radix(&kid, 16).ok()
}

/// Check `token`, signed by ledger node `issuer` (as [`issuer`] read it)
/// with the key `key` checks.
///
/// # Errors
///
/// Returns why the token is refused: malformed, not `EdDSA`, a bad
/// signature, or expired.
pub(crate) fn verify(token: &str, issuer: u64, key: &DecodingKey) -> Result<Claims, Refused> {
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.leeway = SKEW;
    validation.set_issuer(&[kid(issuer)]);
    validation.set_required_spec_claims(&["exp", "iss", "sub"]);
    match jsonwebtoken::decode::<Claims>(token, key, &validation) {
        Ok(t) => Ok(t.claims),
        Err(e) => Err(match e.kind() {
            ErrorKind::ExpiredSignature => EXPIRED,
            ErrorKind::InvalidSignature => Refused("a token with a bad signature"),
            ErrorKind::InvalidAlgorithm => Refused("a token not signed with EdDSA"),
            _ => MALFORMED,
        }),
    }
}

/// Tokens checked already, each until it expires: a call costs a lookup.
pub(crate) struct Verifier {
    checked: quick_cache::sync::Cache<String, Arc<Claims>>,
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier").finish_non_exhaustive()
    }
}

impl Default for Verifier {
    fn default() -> Self {
        Verifier {
            checked: quick_cache::sync::Cache::new(CACHED),
        }
    }
}

impl Verifier {
    /// `token`'s claims if it was checked already: refused if it has
    /// expired by `now` (seconds since the epoch), None if not checked.
    pub(crate) fn cached(&self, token: &str, now: u64) -> Option<Result<Arc<Claims>, Refused>> {
        let c = self.checked.get(token)?;
        if c.exp + SKEW > now {
            return Some(Ok(c));
        }
        self.checked.remove(token);
        Some(Err(EXPIRED))
    }

    /// Remember that `token` checked out as `claims`.
    pub(crate) fn insert(&self, token: &str, claims: Arc<Claims>) {
        self.checked.insert(token.to_string(), claims);
    }
}

fn signing_key(path: &Path, seed: Vec<u8>) -> Result<SigningKey> {
    let seed: [u8; 32] = seed
        .try_into()
        .map_err(|_short| anyhow::anyhow!("{} is not a 32-byte key", path.display()))?;
    Ok(SigningKey::from_bytes(&seed))
}

/// A token `issuer` signs for itself, lasting `ttl`: what
/// [`Claims::is_ledger`] recognizes.
#[must_use]
pub(crate) fn own(issuer: &Issuer, ttl: Duration) -> String {
    issuer.issue(&format!("ledger-{}", kid(issuer.id)), ttl)
}

/// Tokens `issuer` signs for itself lasting `ttl`, renewed at a third of it
/// until nobody holds the receiver: a ledger node's own calls to its peers.
#[must_use]
pub(crate) fn self_issued(issuer: Arc<Issuer>, ttl: Duration) -> Token {
    let (tx, rx) = watch::channel(Some(Arc::from(own(&issuer, ttl))));
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = tokio::time::sleep(ttl / 3) => {}
                () = tx.closed() => return,
            }
            tx.send_replace(Some(Arc::from(own(&issuer, ttl))));
        }
    });
    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issuer(id: u64) -> Issuer {
        Issuer::new(id, &SigningKey::from_bytes(&rand::random())).unwrap()
    }

    /// Check `t` against `i`'s key.
    fn check(t: &str, i: &Issuer) -> Result<Claims, Refused> {
        verify(t, super::issuer(t).ok_or(MALFORMED)?, i.decoding_key())
    }

    /// A token checks against its issuer's key, and no other.
    #[test]
    fn tokens_check_against_their_issuer() {
        let (a, b) = (issuer(1), issuer(2));
        let t = a.issue("node", TOKEN_TTL);
        assert_eq!(super::issuer(&t), Some(1));
        let c = check(&t, &a).unwrap();
        assert_eq!(
            (c.sub.as_str(), c.iss.as_str()),
            ("node", "0000000000000001")
        );
        assert_eq!(
            check(&t, &b).unwrap_err(),
            Refused("a token with a bad signature")
        );
        // Signed by node 1, but claiming node 2 issued it.
        assert_eq!(verify(&t, 2, a.decoding_key()).unwrap_err(), MALFORMED);
    }

    /// Expired, altered and unsigned tokens are refused.
    #[test]
    fn bad_tokens_are_refused() {
        let a = issuer(1);
        let expired = a.issue("node", Duration::ZERO);
        let claims = expired.split('.').nth(1).unwrap();
        let mut c: Claims =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(claims).unwrap()).unwrap();
        c.exp -= SKEW + 1;
        let header = Header {
            kid: Some(kid(1)),
            ..Header::new(Algorithm::EdDSA)
        };
        let old = jsonwebtoken::encode(&header, &c, &a.key).unwrap();
        assert_eq!(check(&old, &a).unwrap_err(), EXPIRED);
        let t = a.issue("node", TOKEN_TTL);
        let mut parts: Vec<&str> = t.split('.').collect();
        let other = a.issue("someone-else", TOKEN_TTL);
        parts[1] = other.split('.').nth(1).unwrap();
        assert_eq!(
            check(&parts.join("."), &a).unwrap_err(),
            Refused("a token with a bad signature")
        );
        let none = format!(
            "{}.{}.",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT","kid":"1"}"#),
            parts[1]
        );
        check(&none, &a).unwrap_err();
        let hmac = format!(
            "{}.{}.{}",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT","kid":"1"}"#),
            parts[1],
            parts[2]
        );
        check(&hmac, &a).unwrap_err();
        check("x.y", &a).unwrap_err();
    }

    /// The verifier answers from its cache, and still expires tokens.
    #[test]
    fn the_verifier_caches_until_expiry() {
        let a = issuer(1);
        let v = Verifier::default();
        let t = a.issue("node", Duration::from_secs(60));
        assert!(v.cached(&t, now()).is_none());
        v.insert(&t, Arc::new(check(&t, &a).unwrap()));
        v.cached(&t, now()).unwrap().unwrap();
        v.cached(&t, now() + 60 + SKEW).unwrap().unwrap_err();
    }

    /// A key persists across loads.
    #[test]
    fn issuer_keys_persist() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        let a = Issuer::load_or_create(&path, 3).unwrap();
        let b = Issuer::load_or_create(&path, 3).unwrap();
        assert_eq!(a.public(), b.public());
    }
}
