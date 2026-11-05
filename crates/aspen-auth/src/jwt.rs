//! JSON Web Token verification for OIDC identity tokens (GitHub Actions,
//! GitLab CI, cloud workload identity, an organisation's IdP).
//!
//! Only signed tokens with an asymmetric algorithm are accepted: RS256,
//! RS384, RS512, ES256 and ES384. `none` and the HMAC algorithms are
//! refused whatever the key set says, so a public key can never be used as
//! an HMAC secret. A key must have the type the token's algorithm needs,
//! and if the key names an `alg`, it must be the token's.
//!
//! Claims checked, in this order, after the signature: `iss` equals the
//! configured issuer, `aud` (a string or an array) contains the configured
//! audience, `exp` is a number and in the future, `nbf` (if present) is a
//! number and not in the future, and `sub` is a non-empty string. Clock
//! leeway applies to `exp` and `nbf`. A header with `crit` is refused,
//! because no extension is understood.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ring::signature::{self, RsaPublicKeyComponents, UnparsedPublicKey};
use serde::Deserialize;
use serde_json::Value;

/// Largest token accepted, in bytes. Identity tokens are a few KiB.
pub const MAX_JWT_LEN: usize = 16 * 1024;

/// Why a token was refused. The text is for logs, never for clients, which
/// only see "authentication required".
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwtError {
    /// Not three base64url segments of JSON, or too long.
    #[error("malformed token: {0}")]
    Malformed(&'static str),
    /// The algorithm is not one of the accepted ones.
    #[error("algorithm `{0}` not accepted")]
    Algorithm(String),
    /// The header has `crit`.
    #[error("critical header extensions are not supported")]
    Critical,
    /// No key in the set fits the token's `kid` and algorithm.
    #[error("no matching key")]
    NoKey,
    /// The signature does not verify.
    #[error("bad signature")]
    Signature,
    /// A claim failed its check.
    #[error("claim `{0}` rejected")]
    Claim(&'static str),
}

/// What [`JwtVerifier::verify`] needs to know about time.
#[derive(Debug, Clone, Copy)]
pub struct Clock {
    /// Seconds since the Unix epoch.
    pub now: i64,
    /// Allowed skew, in seconds, for `exp` and `nbf`.
    pub leeway: i64,
}

/// Verified claims.
#[derive(Debug, Clone, PartialEq)]
pub struct Claims {
    /// `sub`.
    pub subject: String,
    /// Every claim, for claim-matching rules.
    pub all: BTreeMap<String, Value>,
}

impl Claims {
    /// A claim as a string: strings as they are, booleans and numbers in
    /// their JSON form; `None` for anything else or a missing claim.
    pub fn string(&self, name: &str) -> Option<String> {
        match self.all.get(name)? {
            Value::String(s) => Some(s.clone()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Alg {
    Rs256,
    Rs384,
    Rs512,
    Es256,
    Es384,
}

impl Alg {
    fn parse(s: &str) -> Result<Self, JwtError> {
        match s {
            "RS256" => Ok(Self::Rs256),
            "RS384" => Ok(Self::Rs384),
            "RS512" => Ok(Self::Rs512),
            "ES256" => Ok(Self::Es256),
            "ES384" => Ok(Self::Es384),
            other => Err(JwtError::Algorithm(other.chars().take(32).collect())),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Rs256 => "RS256",
            Self::Rs384 => "RS384",
            Self::Rs512 => "RS512",
            Self::Es256 => "ES256",
            Self::Es384 => "ES384",
        }
    }
}

#[derive(Debug, Clone)]
enum KeyMaterial {
    Rsa {
        n: Vec<u8>,
        e: Vec<u8>,
    },
    /// Uncompressed SEC1 point (`0x04 || x || y`) and its curve.
    Ec {
        point: Vec<u8>,
        p384: bool,
    },
}

#[derive(Debug, Clone)]
struct Key {
    kid: Option<String>,
    alg: Option<String>,
    material: KeyMaterial,
}

impl Key {
    fn fits(&self, alg: Alg) -> bool {
        let type_fits = matches!(
            (&self.material, alg),
            (
                KeyMaterial::Rsa { .. },
                Alg::Rs256 | Alg::Rs384 | Alg::Rs512
            ) | (KeyMaterial::Ec { p384: false, .. }, Alg::Es256)
                | (KeyMaterial::Ec { p384: true, .. }, Alg::Es384)
        );
        type_fits && self.alg.as_deref().is_none_or(|a| a == alg.name())
    }

    fn verify(&self, alg: Alg, message: &[u8], sig: &[u8]) -> bool {
        match &self.material {
            KeyMaterial::Rsa { n, e } => {
                let params = match alg {
                    Alg::Rs256 => &signature::RSA_PKCS1_2048_8192_SHA256,
                    Alg::Rs384 => &signature::RSA_PKCS1_2048_8192_SHA384,
                    Alg::Rs512 => &signature::RSA_PKCS1_2048_8192_SHA512,
                    _ => return false,
                };
                RsaPublicKeyComponents { n, e }
                    .verify(params, message, sig)
                    .is_ok()
            }
            KeyMaterial::Ec { point, .. } => {
                let params = match alg {
                    Alg::Es256 => &signature::ECDSA_P256_SHA256_FIXED,
                    Alg::Es384 => &signature::ECDSA_P384_SHA384_FIXED,
                    _ => return false,
                };
                UnparsedPublicKey::new(params, point)
                    .verify(message, sig)
                    .is_ok()
            }
        }
    }
}

/// A JSON Web Key Set (RFC 7517), as an issuer publishes it at its
/// `jwks_uri`. Keys of unsupported types or with `use` other than `sig` are
/// skipped, not errors, since issuers publish several kinds.
#[derive(Debug, Clone, Default)]
pub struct KeySet {
    keys: Vec<Key>,
}

/// A key set that does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("key set: {0}")]
pub struct KeySetError(String);

#[derive(Deserialize)]
struct RawSet {
    keys: Vec<RawKey>,
}

#[derive(Deserialize)]
struct RawKey {
    kty: String,
    kid: Option<String>,
    alg: Option<String>,
    #[serde(rename = "use")]
    use_: Option<String>,
    n: Option<String>,
    e: Option<String>,
    crv: Option<String>,
    x: Option<String>,
    y: Option<String>,
}

impl KeySet {
    /// Parses a JWKS document.
    pub fn from_json(json: &str) -> Result<Self, KeySetError> {
        let raw: RawSet = serde_json::from_str(json).map_err(|e| KeySetError(e.to_string()))?;
        let mut keys = Vec::new();
        for k in raw.keys {
            if k.use_.as_deref().is_some_and(|u| u != "sig") {
                continue;
            }
            let b64 = |field: Option<String>, name: &str| -> Result<Vec<u8>, KeySetError> {
                let v =
                    field.ok_or_else(|| KeySetError(format!("{} key without `{name}`", k.kty)))?;
                URL_SAFE_NO_PAD
                    .decode(v.trim_end_matches('='))
                    .map_err(|e| KeySetError(format!("`{name}`: {e}")))
            };
            let material = match (k.kty.as_str(), k.crv.as_deref()) {
                ("RSA", _) => KeyMaterial::Rsa {
                    n: b64(k.n.clone(), "n")?,
                    e: b64(k.e.clone(), "e")?,
                },
                ("EC", Some(crv @ ("P-256" | "P-384"))) => {
                    let len = if crv == "P-256" { 32 } else { 48 };
                    let x = b64(k.x.clone(), "x")?;
                    let y = b64(k.y.clone(), "y")?;
                    if x.len() != len || y.len() != len {
                        return Err(KeySetError(format!(
                            "{crv} coordinates of the wrong length"
                        )));
                    }
                    let mut point = Vec::with_capacity(1 + 2 * len);
                    point.push(0x04);
                    point.extend_from_slice(&x);
                    point.extend_from_slice(&y);
                    KeyMaterial::Ec {
                        point,
                        p384: crv == "P-384",
                    }
                }
                _ => continue,
            };
            keys.push(Key {
                kid: k.kid,
                alg: k.alg,
                material,
            });
        }
        Ok(Self { keys })
    }

    /// How many usable keys the set has.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the set has no usable key.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// Verifies tokens from one issuer for one audience.
#[derive(Debug, Clone)]
pub struct JwtVerifier {
    issuer: String,
    audience: String,
    keys: KeySet,
}

#[derive(Deserialize)]
struct Header {
    alg: String,
    kid: Option<String>,
    crit: Option<Value>,
}

impl JwtVerifier {
    /// A verifier for tokens whose `iss` is `issuer` and whose `aud`
    /// contains `audience`, signed by a key in `keys`.
    pub fn new(issuer: impl Into<String>, audience: impl Into<String>, keys: KeySet) -> Self {
        Self {
            issuer: issuer.into(),
            audience: audience.into(),
            keys,
        }
    }

    /// The issuer this verifier accepts.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Replaces the key set (after the issuer rotated its keys).
    pub fn set_keys(&mut self, keys: KeySet) {
        self.keys = keys;
    }

    /// Verifies a compact-serialised token and returns its claims.
    pub fn verify(&self, token: &str, clock: Clock) -> Result<Claims, JwtError> {
        if token.len() > MAX_JWT_LEN {
            return Err(JwtError::Malformed("too long"));
        }
        let mut parts = token.split('.');
        let (Some(h), Some(p), Some(s), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(JwtError::Malformed("expected three segments"));
        };
        let header: Header = serde_json::from_slice(&decode(h)?)
            .map_err(|_| JwtError::Malformed("header is not a JWS header"))?;
        if header.crit.is_some() {
            return Err(JwtError::Critical);
        }
        let alg = Alg::parse(&header.alg)?;
        let sig = decode(s)?;
        let message = &token.as_bytes()[..h.len() + 1 + p.len()];
        let mut candidates = self
            .keys
            .keys
            .iter()
            .filter(|k| k.fits(alg))
            .filter(|k| match (&header.kid, &k.kid) {
                (Some(want), Some(have)) => want == have,
                // A token without `kid` may be checked against every key
                // that fits; a token with one only against that key.
                (None, _) => true,
                (Some(_), None) => false,
            })
            .peekable();
        if candidates.peek().is_none() {
            return Err(JwtError::NoKey);
        }
        if !candidates.any(|k| k.verify(alg, message, &sig)) {
            return Err(JwtError::Signature);
        }
        let claims: BTreeMap<String, Value> = serde_json::from_slice(&decode(p)?)
            .map_err(|_| JwtError::Malformed("payload is not a JSON object"))?;
        self.check_claims(claims, clock)
    }

    fn check_claims(&self, all: BTreeMap<String, Value>, clock: Clock) -> Result<Claims, JwtError> {
        if all.get("iss").and_then(Value::as_str) != Some(self.issuer.as_str()) {
            return Err(JwtError::Claim("iss"));
        }
        let audience_ok = match all.get("aud") {
            Some(Value::String(a)) => *a == self.audience,
            Some(Value::Array(list)) => list.iter().any(|a| a.as_str() == Some(&self.audience)),
            _ => false,
        };
        if !audience_ok {
            return Err(JwtError::Claim("aud"));
        }
        // Numbers only: a string or other type in a time claim is refused,
        // never read as "absent".
        let time = |name| -> Result<Option<i64>, JwtError> {
            match all.get(name) {
                None => Ok(None),
                Some(Value::Number(n)) => n
                    .as_i64()
                    .or_else(|| n.as_f64().filter(|f| f.is_finite()).map(|f| f as i64))
                    .map(Some)
                    .ok_or(JwtError::Claim(name)),
                Some(_) => Err(JwtError::Claim(name)),
            }
        };
        match time("exp")? {
            Some(exp) if clock.now < exp.saturating_add(clock.leeway) => {}
            _ => return Err(JwtError::Claim("exp")),
        }
        if let Some(nbf) = time("nbf")?
            && clock.now.saturating_add(clock.leeway) < nbf
        {
            return Err(JwtError::Claim("nbf"));
        }
        let subject = match all.get("sub") {
            Some(Value::String(s)) if !s.is_empty() && !s.chars().any(char::is_control) => {
                s.clone()
            }
            _ => return Err(JwtError::Claim("sub")),
        };
        Ok(Claims { subject, all })
    }
}

fn decode(segment: &str) -> Result<Vec<u8>, JwtError> {
    URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|_| JwtError::Malformed("segment is not base64url"))
}

/// The `iss` claim of a compact JWS, read **without** verifying anything:
/// only for choosing which issuer's keys to verify it with, so a forged
/// token costs one issuer's signature checks rather than every issuer's.
/// `None` for a token that is too long or whose payload is not a JSON
/// object with a string `iss`.
pub fn unverified_issuer(token: &str) -> Option<String> {
    if token.len() > MAX_JWT_LEN {
        return None;
    }
    #[derive(Deserialize)]
    struct Iss {
        iss: String,
    }
    let payload = token.split('.').nth(1)?;
    let claims: Iss = serde_json::from_slice(&decode(payload).ok()?).ok()?;
    Some(claims.iss)
}

/// Whether `token` has the shape of a compact JWS (three dot-separated
/// segments). It may still be invalid.
pub fn looks_like_jwt(token: &str) -> bool {
    token.split('.').count() == 3
}
