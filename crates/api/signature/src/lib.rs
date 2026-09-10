//! Proving a call to the hook endpoint came from Enroute, with [RFC 9421 HTTP
//! Message Signatures][rfc9421] over Ed25519.
//!
//! Asymmetric, so Enroute holds a private key and an application holds only the
//! public half — an application can check a call and cannot make one, which
//! matters most for a hosted Enroute, where one leaked tenant must not open a
//! way into any other. A standard rather than something of ours, since the far
//! end is meant to be any language: RFC 9421 already has verifying libraries in
//! most of them. Both ends live in this one crate, as with the ingest
//! protocol's wire types, because a scheme that disagrees by one byte fails at
//! runtime, on the call that mattered.
//!
//! [rfc9421]: https://www.rfc-editor.org/rfc/rfc9421.html

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ed25519_dalek::pkcs8::{DecodePrivateKey as _, DecodePublicKey as _};
use ed25519_dalek::{Signer as _, Verifier as _};
use http::HeaderMap;
use sha2::{Digest as _, Sha256};

/// The label these signatures carry — any label is legal, [`verify`] reads
/// whichever arrives, and this is only what [`sign`] writes.
const LABEL: &str = "sig1";

/// What every call from Enroute covers, in order.
///
/// Checked, not merely read: an accepted caller-sent list would let one
/// drop `content-digest` and sign a set that says nothing about the body.
const COVERED: &str = r#"("@method" "@authority" "@path" "content-digest")"#;

/// How far a call's `created` may be from now, kept generous since a false
/// rejection here refuses a git push.
pub const MAX_SKEW_SECS: u64 = 300;

/// The [RFC 9530] body digest.
///
/// [RFC 9530]: https://www.rfc-editor.org/rfc/rfc9530.html
pub const CONTENT_DIGEST_HEADER: &str = "content-digest";
/// What the signature covers, and the parameters it was made with.
pub const SIGNATURE_INPUT_HEADER: &str = "signature-input";
/// The signature itself.
pub const SIGNATURE_HEADER: &str = "signature";

/// The parts of a request the signature covers.
///
/// Named here, not taken from a request type, since Enroute signs a `reqwest`
/// builder and an application an axum request — the bytes must match.
#[derive(Debug, Clone, Copy)]
pub struct Covered<'a> {
    /// Upper-case, e.g. `POST`.
    pub method: &'a str,
    /// Host and port as the request addresses them, e.g. `forge.example.com`.
    pub authority: &'a str,
    /// Path only, no query, e.g. `/enroute/hooks`.
    pub path: &'a str,
    /// The body, which `content-digest` is taken over.
    pub body: &'a [u8],
}

/// Enroute's private key.
pub struct SigningKey {
    inner: ed25519_dalek::SigningKey,
    keyid: String,
}

/// Enroute's public key, as an application holds it.
#[derive(Debug, Clone)]
pub struct VerifyingKey {
    inner: ed25519_dalek::VerifyingKey,
    keyid: String,
}

impl std::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The private half must not reach a log by sitting inside a struct
        // somebody derived `Debug` on. The id is public by construction.
        write!(f, "SigningKey({})", self.keyid)
    }
}

impl SigningKey {
    /// Read a PKCS#8 PEM private key — what `openssl genpkey -algorithm
    /// ed25519` writes.
    ///
    /// # Errors
    ///
    /// Returns an error if `pem` is not a PKCS#8 Ed25519 private key.
    pub fn from_pem(pem: &str) -> Result<Self, KeyError> {
        let inner = ed25519_dalek::SigningKey::from_pkcs8_pem(pem).map_err(|_bad| KeyError)?;
        let keyid = thumbprint(&inner.verifying_key());
        Ok(Self { inner, keyid })
    }

    /// The public half, to hand to whoever verifies.
    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        VerifyingKey {
            inner: self.inner.verifying_key(),
            keyid: self.keyid.clone(),
        }
    }

    /// This key's id: its [RFC 7638] JWK thumbprint.
    ///
    /// [RFC 7638]: https://www.rfc-editor.org/rfc/rfc7638.html
    #[must_use]
    pub fn keyid(&self) -> &str {
        &self.keyid
    }
}

impl VerifyingKey {
    /// Read a SPKI PEM public key — what `openssl pkey -pubout` writes.
    ///
    /// # Errors
    ///
    /// Returns an error if `pem` is not a SPKI Ed25519 public key.
    pub fn from_pem(pem: &str) -> Result<Self, KeyError> {
        let inner =
            ed25519_dalek::VerifyingKey::from_public_key_pem(pem).map_err(|_bad| KeyError)?;
        let keyid = thumbprint(&inner);
        Ok(Self { inner, keyid })
    }

    /// This key's id: its [RFC 7638] JWK thumbprint.
    ///
    /// Derived rather than configured, so rotating a key is adding one and
    /// nothing has to agree on a name for it first.
    ///
    /// [RFC 7638]: https://www.rfc-editor.org/rfc/rfc7638.html
    #[must_use]
    pub fn keyid(&self) -> &str {
        &self.keyid
    }
}

/// A key that could not be read.
///
/// Deliberately opaque: saying more than "no" about a private key invites
/// printing parts of one.
#[derive(Debug, thiserror::Error)]
#[error("not a usable ed25519 key")]
pub struct KeyError;

/// Why a call was not accepted, one variant per thing that can be wrong.
///
/// For the log only — a caller who cannot sign has no use for the detail.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A field the signature is made of was absent, or held bytes that are
    /// not text.
    #[error("missing or unreadable `{0}`")]
    MissingHeader(&'static str),
    /// `Signature-Input` was present but not shaped like RFC 9421 says.
    #[error("malformed `{SIGNATURE_INPUT_HEADER}`: {0}")]
    Malformed(&'static str),
    /// The signature covers a different set of components than Enroute signs.
    #[error("signature covers {0}, not {COVERED}")]
    Coverage(String),
    /// Signed too long ago, or too far in the future, by this many seconds.
    #[error("signed {0}s away from now, past the {MAX_SKEW_SECS}s limit")]
    Skewed(u64),
    /// No configured key has the `keyid` the call named.
    #[error("no key with id `{0}`")]
    UnknownKey(String),
    /// The body is not the one that was signed.
    #[error("`{CONTENT_DIGEST_HEADER}` does not match the body")]
    Digest,
    /// The signature is well-formed and wrong.
    #[error("`{SIGNATURE_HEADER}` does not verify")]
    Mismatch,
}

/// The headers to send alongside `covered`: `Content-Digest`,
/// `Signature-Input`, and `Signature`.
///
/// `created` is passed rather than read, so a caller that already knows the
/// time does not read the clock twice, and so this stays testable without one.
#[must_use]
pub fn sign(key: &SigningKey, covered: &Covered<'_>, created: u64) -> [(&'static str, String); 3] {
    let digest = content_digest(covered.body);
    let params = format!(r#"{COVERED};created={created};keyid="{}""#, key.keyid);
    let base = signature_base(covered, &digest, &params);
    let signature = STANDARD.encode(key.inner.sign(base.as_bytes()).to_bytes());

    [
        (CONTENT_DIGEST_HEADER, digest),
        (SIGNATURE_INPUT_HEADER, format!("{LABEL}={params}")),
        (SIGNATURE_HEADER, format!("{LABEL}=:{signature}:")),
    ]
}

/// Check that `headers` prove `covered` was signed recently by one of `keys`.
///
/// # Errors
///
/// Returns an [`Error`] if a header, the coverage, `created`, the key, the
/// digest, or the signature itself does not check out.
pub fn verify(
    keys: &[VerifyingKey],
    covered: &Covered<'_>,
    headers: &HeaderMap,
    now_unix_secs: u64,
) -> Result<(), Error> {
    let input = header(headers, SIGNATURE_INPUT_HEADER)?;
    let (label, params) = input.split_once('=').ok_or(Error::Malformed("no label"))?;

    let (components, rest) = params
        .split_once(')')
        .ok_or(Error::Malformed("no component list"))?;
    let components = format!("{components})");
    if components != COVERED {
        return Err(Error::Coverage(components));
    }

    let created: u64 = param(rest, "created")
        .ok_or(Error::Malformed("no created"))?
        .parse()
        .map_err(|_not_a_number| Error::Malformed("created is not unix seconds"))?;
    let skew = now_unix_secs.abs_diff(created);
    if skew > MAX_SKEW_SECS {
        return Err(Error::Skewed(skew));
    }

    // The algorithm comes from the key, never from the message. An `alg` a
    // caller chose is how a signature scheme gets talked down to one the caller
    // can application, so nothing here reads it.
    let keyid = param(rest, "keyid")
        .ok_or(Error::Malformed("no keyid"))?
        .trim_matches('"')
        .to_string();
    let key = keys
        .iter()
        .find(|key| key.keyid == keyid)
        .ok_or(Error::UnknownKey(keyid))?;

    // Recomputed rather than read: the digest is what ties the signature to
    // this body, so a signature over somebody else's digest must not pass.
    let digest = header(headers, CONTENT_DIGEST_HEADER)?;
    if digest != content_digest(covered.body) {
        return Err(Error::Digest);
    }

    let presented = header(headers, SIGNATURE_HEADER)?
        .strip_prefix(&format!("{label}=:"))
        .and_then(|rest| rest.strip_suffix(':'))
        .ok_or(Error::Malformed("signature is not a byte sequence"))?;
    let presented = STANDARD.decode(presented).map_err(|_bad| Error::Mismatch)?;
    let presented: [u8; 64] = presented
        .try_into()
        .map_err(|_wrong_size| Error::Mismatch)?;

    let base = signature_base(covered, digest, &format!("{components}{rest}"));
    key.inner
        .verify(
            base.as_bytes(),
            &ed25519_dalek::Signature::from_bytes(&presented),
        )
        .map_err(|_mismatch| Error::Mismatch)
}

/// Read the clock the way [`sign`] and [`verify`] want it — saturates rather
/// than failing before the epoch.
#[must_use]
pub fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// The [RFC 9530] digest of a body, as the header value.
///
/// [RFC 9530]: https://www.rfc-editor.org/rfc/rfc9530.html
fn content_digest(body: &[u8]) -> String {
    format!("sha-256=:{}:", STANDARD.encode(Sha256::digest(body)))
}

/// The RFC 9421 signature base for what Enroute covers.
fn signature_base(covered: &Covered<'_>, digest: &str, params: &str) -> String {
    lines_to_base(
        &[
            ("@method", covered.method),
            ("@authority", covered.authority),
            ("@path", covered.path),
            ("content-digest", digest),
        ],
        params,
    )
}

/// The shape of every signature base, whatever it covers: one line per
/// component, then the parameters, with no trailing newline.
///
/// Separate from [`signature_base`] so the RFC's own test vector — which
/// covers a different set — can be built with the same code that builds ours.
fn lines_to_base(lines: &[(&str, &str)], params: &str) -> String {
    let mut base = String::new();
    for (name, value) in lines {
        base.push('"');
        base.push_str(name);
        base.push_str("\": ");
        base.push_str(value);
        base.push('\n');
    }
    base.push_str("\"@signature-params\": ");
    base.push_str(params);
    base
}

/// One `;name=value` parameter out of an RFC 9421 parameter string.
fn param<'a>(params: &'a str, name: &str) -> Option<&'a str> {
    params.split(';').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key.trim() == name).then_some(value)
    })
}

fn header<'a>(headers: &'a HeaderMap, name: &'static str) -> Result<&'a str, Error> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .ok_or(Error::MissingHeader(name))
}

/// An Ed25519 key's [RFC 7638] JWK thumbprint: the SHA-256 of its JWK with
/// only the required members, ordered, no whitespace.
///
/// [RFC 7638]: https://www.rfc-editor.org/rfc/rfc7638.html
fn thumbprint(key: &ed25519_dalek::VerifyingKey) -> String {
    let x = URL_SAFE_NO_PAD.encode(key.to_bytes());
    let jwk = format!(r#"{{"crv":"Ed25519","kty":"OKP","x":"{x}"}}"#);
    URL_SAFE_NO_PAD.encode(Sha256::digest(jwk.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_700_000_000;

    /// RFC 9421's `test-key-ed25519` (appendix B.1.4) — a published test key
    /// is exactly the thing these tests want, one that protects nothing.
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\n\
        MC4CAQAwBQYDK2VwBCIEIJ+DYvh6SEqVTm50DFtMDoQikTmiCqirVv9mWG9qfSnF\n\
        -----END PRIVATE KEY-----\n";
    const TEST_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\n\
        MCowBQYDK2VwAyEAJrQLj5P/89iXES9+vFgrIy29clF9CC/oPPsw3c5D0bs=\n\
        -----END PUBLIC KEY-----\n";

    fn key() -> SigningKey {
        SigningKey::from_pem(TEST_KEY).expect("a pkcs8 ed25519 key")
    }

    fn covered(body: &[u8]) -> Covered<'_> {
        Covered {
            method: "POST",
            authority: "forge.example.com",
            path: "/enroute/hooks",
            body,
        }
    }

    fn signed(key: &SigningKey, covered: &Covered<'_>, at: u64) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in sign(key, covered, at) {
            headers.insert(name, value.parse().expect("a header value"));
        }
        headers
    }

    /// A call this crate actually signed, checked as fixed bytes rather than
    /// round-tripped through itself.
    ///
    /// Frozen, so a refactor cannot quietly change what Enroute signs: these
    /// bytes pin what the RFC leaves to us, the components and the keyid.
    #[test]
    fn cross_language_fixture_verifies() {
        const BODY: &[u8] = b"a cross-language fixture";
        const DIGEST: &str = "sha-256=:xU5GyHrmAMTCLsWQXqFxTDup2ko9wy+ZQ6Wc6E+7hdA=:";
        const INPUT: &str = concat!(
            r#"sig1=("@method" "@authority" "@path" "content-digest")"#,
            ";created=1700000000",
            r#";keyid="poqkLGiymh_W0uP6PZFw-dvez3QJT5SolqXBCW38r0U""#,
        );
        const SIGNATURE: &str = concat!(
            "sig1=:v5Qd7JlSJpzS0fhYZkGEpnmN97cGIAiiVW9kRW9osqwgeqBJS0VaqZpA/AEc",
            "LdBXKU6aOW4m2bK8Hite0RCKAg==:",
        );

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_DIGEST_HEADER, DIGEST.parse().expect("a header"));
        headers.insert(SIGNATURE_INPUT_HEADER, INPUT.parse().expect("a header"));
        headers.insert(SIGNATURE_HEADER, SIGNATURE.parse().expect("a header"));

        let covered = Covered {
            method: "POST",
            authority: "forge.example.com",
            path: "/enroute/hooks",
            body: BODY,
        };
        let key = VerifyingKey::from_pem(TEST_PUBLIC_KEY).expect("the test public key");
        assert_eq!(key.keyid(), "poqkLGiymh_W0uP6PZFw-dvez3QJT5SolqXBCW38r0U");
        verify(&[key], &covered, &headers, 1_700_000_000)
            .expect("the frozen cross-language fixture");
    }
    #[test]
    fn a_signed_call_verifies() {
        let key = key();
        let headers = signed(&key, &covered(b"body"), NOW);
        verify(&[key.verifying_key()], &covered(b"body"), &headers, NOW)
            .expect("a freshly signed call verifies");
    }

    /// The point of doing this asymmetrically: holding the public half is
    /// enough to check a call and not enough to make one.
    #[test]
    fn a_verifier_cannot_mint_a_call() {
        let public = VerifyingKey::from_pem(TEST_PUBLIC_KEY).expect("a spki ed25519 key");
        assert_eq!(public.keyid(), key().keyid());
        // There is no method here that signs, and no way to reach one: the
        // private half never leaves Enroute. This test is the assertion.
    }

    #[test]
    fn another_key_does_not_verify() {
        let other = SigningKey::from_pem(
            "-----BEGIN PRIVATE KEY-----\n\
             MC4CAQAwBQYDK2VwBCIEIHwvKu4FGxUeNZDvBnLgAWyzTHDBjKGVSbLRRQKHOZgn\n\
             -----END PRIVATE KEY-----\n",
        )
        .expect("a pkcs8 ed25519 key");
        let headers = signed(&other, &covered(b"body"), NOW);

        let err = verify(&[key().verifying_key()], &covered(b"body"), &headers, NOW).unwrap_err();
        // A different key means a different thumbprint, so this is refused
        // before any signature check — the cheaper and clearer failure.
        assert!(matches!(err, Error::UnknownKey(_)), "{err:?}");
    }

    /// The signature covers the body through `content-digest`, so one captured
    /// set of headers must not authenticate a different body.
    #[test]
    fn a_changed_body_does_not_verify() {
        let key = key();
        let headers = signed(&key, &covered(b"body"), NOW);
        let err = verify(
            &[key.verifying_key()],
            &covered(b"other body"),
            &headers,
            NOW,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Digest), "{err:?}");
    }

    /// And it covers where the call was sent, so one captured by an application
    /// cannot be replayed at another.
    #[test]
    fn a_call_replayed_at_another_host_does_not_verify() {
        let key = key();
        let headers = signed(&key, &covered(b"body"), NOW);

        let elsewhere = Covered {
            authority: "someone-else.example.com",
            ..covered(b"body")
        };
        let err = verify(&[key.verifying_key()], &elsewhere, &headers, NOW).unwrap_err();
        assert!(matches!(err, Error::Mismatch), "{err:?}");
    }

    #[test]
    fn an_old_call_is_refused() {
        let key = key();
        let headers = signed(&key, &covered(b"body"), NOW);
        let err = verify(
            &[key.verifying_key()],
            &covered(b"body"),
            &headers,
            NOW + MAX_SKEW_SECS + 1,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Skewed(_)), "{err:?}");
    }

    /// Clocks run both ways, and an application whose clock is ahead of
    /// Enroute's must not refuse every call.
    #[test]
    fn skew_is_allowed_in_both_directions() {
        let key = key();
        let headers = signed(&key, &covered(b"body"), NOW + MAX_SKEW_SECS);
        verify(&[key.verifying_key()], &covered(b"body"), &headers, NOW)
            .expect("a clock ahead is still in the window");
    }

    #[test]
    fn an_unsigned_call_is_refused() {
        let err = verify(
            &[key().verifying_key()],
            &covered(b"body"),
            &HeaderMap::new(),
            NOW,
        )
        .unwrap_err();
        assert!(matches!(err, Error::MissingHeader(_)), "{err:?}");
    }

    /// A caller must not get to say what its own signature covers — dropping
    /// `content-digest` would leave a valid signature that says nothing about the body.
    #[test]
    fn a_call_that_covers_less_is_refused() {
        let key = key();
        let mut headers = signed(&key, &covered(b"body"), NOW);
        headers.insert(
            SIGNATURE_INPUT_HEADER,
            format!(r#"sig1=("@method");created={NOW};keyid="{}""#, key.keyid())
                .parse()
                .expect("a header value"),
        );
        let err = verify(&[key.verifying_key()], &covered(b"body"), &headers, NOW).unwrap_err();
        assert!(matches!(err, Error::Coverage(_)), "{err:?}");
    }

    #[test]
    fn a_signing_key_does_not_print_itself() {
        assert!(!format!("{:?}", key()).contains("MC4CAQ"));
    }

    /// RFC 9421 appendix B.2.6, verbatim: the signature base its
    /// `test-key-ed25519` example covers, and the signature over it.
    ///
    /// Pins the base's shape and the verification against the RFC's own
    /// component list, not Enroute's — what any implementation must agree with.
    #[test]
    fn matches_rfc_9421_test_vector() {
        const RFC_PARAMS: &str = concat!(
            "(\"date\" \"@method\" \"@path\" \"@authority\" ",
            "\"content-type\" \"content-length\");created=1618884473",
            ";keyid=\"test-key-ed25519\"",
        );
        const RFC_BASE: &str = concat!(
            "\"date\": Tue, 20 Apr 2021 02:07:55 GMT\n",
            "\"@method\": POST\n",
            "\"@path\": /foo\n",
            "\"@authority\": example.com\n",
            "\"content-type\": application/json\n",
            "\"content-length\": 18\n",
            "\"@signature-params\": (\"date\" \"@method\" \"@path\" \"@authority\" ",
            "\"content-type\" \"content-length\");created=1618884473",
            ";keyid=\"test-key-ed25519\"",
        );
        const RFC_SIGNATURE: &str = concat!(
            "wqcAqbmYJ2ji2glfAMaRy4gruYYnx2nEFN2HN6jrnDnQCK1",
            "u02Gb04v9EDgwUPiu4A0w6vuQv5lIp5WPpBKRCw==",
        );

        let built = lines_to_base(
            &[
                ("date", "Tue, 20 Apr 2021 02:07:55 GMT"),
                ("@method", "POST"),
                ("@path", "/foo"),
                ("@authority", "example.com"),
                ("content-type", "application/json"),
                ("content-length", "18"),
            ],
            RFC_PARAMS,
        );
        assert_eq!(built, RFC_BASE, "signature base differs from the RFC's");

        let key = VerifyingKey::from_pem(TEST_PUBLIC_KEY).expect("the RFC's public key");
        let signature: [u8; 64] = STANDARD
            .decode(RFC_SIGNATURE)
            .expect("base64")
            .try_into()
            .expect("64 bytes");
        key.inner
            .verify(
                RFC_BASE.as_bytes(),
                &ed25519_dalek::Signature::from_bytes(&signature),
            )
            .expect("the RFC's own signature verifies against its own base");
    }

    /// The RFC's two halves are each other's — the vector above alone would
    /// still pass a PKCS#8 misread that was self-consistent.
    #[test]
    fn the_rfc_key_pair_agrees() {
        let private = SigningKey::from_pem(TEST_KEY).expect("the RFC's private key");
        let public = VerifyingKey::from_pem(TEST_PUBLIC_KEY).expect("the RFC's public key");
        assert_eq!(private.keyid(), public.keyid());
    }
}
