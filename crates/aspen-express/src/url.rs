//! Signed express URLs.
//!
//! An express URL is
//!
//! ```text
//! <base>/<key id>/<expires>/<signature>/repos/<repo id>/packs/<checksum>.pack
//! ```
//!
//! - `<key id>` names the signing key, so keys rotate without breaking URLs
//!   already handed out: a verifier accepts every key it holds, a signer
//!   signs with one.
//! - `<expires>` is the Unix second after which the URL is refused. The
//!   signer rounds it **up** to a multiple of [`SigningPolicy::window`], so
//!   every fetch of a pack within one window gets the same URL, and an edge
//!   cache keyed on the URL serves them all from one entry.
//! - `<signature>` is HMAC-SHA256, in lowercase hex, over a domain tag, the
//!   key ID, the expiry and the blob key. It binds the repository: a URL for
//!   one repository's pack cannot be turned into a URL for another's, even
//!   when both store the same bytes under the same checksum.
//!
//! The signature and expiry are **path segments, not query parameters**.
//! CDNs often drop the query string from the cache key by default
//! (CloudFront's `Managed-CachingOptimized` does); with the signature in
//! the query, an edge would serve a cached pack to any URL with the right
//! path, signed or not. In the path, a forged or expired URL is always a
//! different cache key, so it reaches the origin and is refused there.
//!
//! The URL is the credential (packfile-URI downloads carry no
//! `Authorization`, S3): Aspen hands one out only after B3's guard let the
//! caller fetch the repository, and it stays valid until it expires, at
//! most [`SigningPolicy::max_lifetime`].

use std::fmt;
use std::time::Duration;

use aspen_core::blob::BlobKey;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::key::PackKey;

/// Domain separation for the signed message, so the secret cannot be
/// reused to forge some other HMAC-signed value of the same shape
/// (including the local store's `/-/blobs` URLs, which use their own tag).
const DOMAIN: &[u8] = b"aspen-express-url-v1\n";

/// Shortest accepted signing secret, in bytes.
pub const MIN_SECRET_LEN: usize = 32;

/// Longest key ID, in bytes.
pub const MAX_KEY_ID_LEN: usize = 16;

/// Longest URL lifetime (`ttl + window`) a [`SigningPolicy`] accepts: the
/// blob store's recommended presign cap (A4 decisions row), one hour. It
/// bounds how long a leaked URL, or a revoked caller's URL, stays useful.
/// It is [`aspen_core::pack::MAX_PACK_URL_LIFETIME`], which garbage
/// collection's grace period is checked against (issue #203).
pub const MAX_URL_LIFETIME: Duration = aspen_core::pack::MAX_PACK_URL_LIFETIME;

/// Why a signing key or policy was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UrlConfigError {
    /// A key ID must be 1 to 16 lowercase ASCII letters, digits or `_`.
    #[error("invalid key ID {0:?}: use 1 to 16 of a-z, 0-9 and _")]
    KeyId(String),
    /// The secret is too short.
    #[error("signing secret for key {0:?} is shorter than {MIN_SECRET_LEN} bytes")]
    ShortSecret(String),
    /// Two keys share an ID.
    #[error("key ID {0:?} is used twice")]
    DuplicateKeyId(String),
    /// A verifier needs at least one key.
    #[error("no signing keys")]
    NoKeys,
    /// The policy's lifetime is out of range.
    #[error("URL lifetime: {0}")]
    Lifetime(&'static str),
    /// A base URL is not an absolute `http(s)` URL without query or
    /// fragment, or ends in `/`.
    #[error("invalid base URL {0:?}: use http(s)://host[:port][/path] without a trailing /")]
    Base(String),
    /// A key list could not be parsed.
    #[error("invalid key list: {0}")]
    KeyList(String),
}

/// A signing key: its public ID and its secret. The secret never appears
/// in `Debug` output and the type has no `PartialEq` (N1: compare secrets
/// only in constant time).
#[derive(Clone)]
pub struct UrlKey {
    id: String,
    secret: Vec<u8>,
}

impl fmt::Debug for UrlKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UrlKey")
            .field("id", &self.id)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl UrlKey {
    /// A key with this ID and secret (at least [`MIN_SECRET_LEN`] bytes).
    pub fn new(id: &str, secret: &[u8]) -> Result<Self, UrlConfigError> {
        let id_ok = !id.is_empty()
            && id.len() <= MAX_KEY_ID_LEN
            && id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !id_ok {
            return Err(UrlConfigError::KeyId(id.to_owned()));
        }
        if secret.len() < MIN_SECRET_LEN {
            return Err(UrlConfigError::ShortSecret(id.to_owned()));
        }
        Ok(Self {
            id: id.to_owned(),
            secret: secret.to_vec(),
        })
    }

    /// The key's public ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    fn mac(&self, expires: u64, key: &BlobKey) -> Hmac<Sha256> {
        let mut mac =
            <Hmac<Sha256> as KeyInit>::new_from_slice(&self.secret).expect("HMAC takes any key");
        mac.update(DOMAIN);
        mac.update(self.id.as_bytes());
        mac.update(b"\n");
        mac.update(expires.to_string().as_bytes());
        mac.update(b"\n");
        mac.update(key.as_str().as_bytes());
        mac
    }
}

/// The keys an express node accepts, and which one signs. Build it from
/// the `ASPEN_EXPRESS_KEYS` setting with [`UrlKeys::parse`].
#[derive(Debug, Clone)]
pub struct UrlKeys {
    /// The first key signs; every key verifies.
    keys: Vec<UrlKey>,
}

impl UrlKeys {
    /// `active` signs; `active` and every key in `also_accept` verify. To
    /// rotate, add the new key to every express node's `also_accept`, then
    /// make it active on the servers, then drop the old key once the
    /// longest URL lifetime has passed.
    pub fn new(active: UrlKey, also_accept: Vec<UrlKey>) -> Result<Self, UrlConfigError> {
        let mut keys = vec![active];
        for k in also_accept {
            if keys.iter().any(|have| have.id == k.id) {
                return Err(UrlConfigError::DuplicateKeyId(k.id));
            }
            keys.push(k);
        }
        Ok(Self { keys })
    }

    /// Parses `id:hexsecret[,id:hexsecret...]`, the `ASPEN_EXPRESS_KEYS`
    /// format; the first key is the active one. Error messages never
    /// contain secret material.
    pub fn parse(s: &str) -> Result<Self, UrlConfigError> {
        let mut keys = Vec::new();
        for (i, item) in s.split(',').map(str::trim).enumerate() {
            let (id, hex) = item.split_once(':').ok_or_else(|| {
                UrlConfigError::KeyList(format!("entry {} is not id:hexsecret", i + 1))
            })?;
            let secret = unhex(hex).ok_or_else(|| {
                UrlConfigError::KeyList(format!(
                    "the secret of key {id:?} is not an even number of hex digits"
                ))
            })?;
            keys.push(UrlKey::new(id, &secret)?);
        }
        let mut keys = keys.into_iter();
        let active = keys.next().ok_or(UrlConfigError::NoKeys)?;
        Self::new(active, keys.collect())
    }

    /// The signing key.
    pub fn active(&self) -> &UrlKey {
        &self.keys[0]
    }

    fn get(&self, id: &str) -> Option<&UrlKey> {
        self.keys.iter().find(|k| k.id == id)
    }
}

/// How long signed URLs live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SigningPolicy {
    ttl: Duration,
    window: Duration,
}

impl SigningPolicy {
    /// Every URL is valid for at least `ttl` and less than `ttl + window`
    /// after it is signed: the expiry is `now + ttl` rounded up to a
    /// multiple of `window` (whole seconds). Both must be at least a
    /// second, and `ttl + window` at most [`MAX_URL_LIFETIME`].
    pub fn new(ttl: Duration, window: Duration) -> Result<Self, UrlConfigError> {
        if ttl.as_secs() == 0 || window.as_secs() == 0 {
            return Err(UrlConfigError::Lifetime(
                "ttl and window must be at least 1 s",
            ));
        }
        if ttl + window > MAX_URL_LIFETIME {
            return Err(UrlConfigError::Lifetime("ttl + window must be at most 1 h"));
        }
        Ok(Self { ttl, window })
    }

    /// The minimum lifetime of a URL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// The window expiries are rounded up to.
    pub fn window(&self) -> Duration {
        self.window
    }

    /// The longest lifetime of a URL (`ttl + window`, exclusive).
    pub fn max_lifetime(&self) -> Duration {
        self.ttl + self.window
    }

    /// The expiry of a URL signed at `now` (Unix seconds).
    pub fn expiry(&self, now: u64) -> u64 {
        let w = self.window.as_secs();
        now.saturating_add(self.ttl.as_secs())
            .div_ceil(w)
            .saturating_mul(w)
    }
}

impl Default for SigningPolicy {
    /// URLs live 10 to 15 minutes, and every fetch in the same 5-minute
    /// window shares one URL per pack.
    fn default() -> Self {
        Self::new(Duration::from_secs(10 * 60), Duration::from_secs(5 * 60)).expect("valid default")
    }
}

/// Checks that `base` is an absolute `http(s)` URL without a query,
/// fragment or trailing `/`.
pub(crate) fn check_base(base: &str) -> Result<(), UrlConfigError> {
    let bad = || UrlConfigError::Base(base.to_owned());
    let rest = base
        .strip_prefix("https://")
        .or_else(|| base.strip_prefix("http://"))
        .ok_or_else(bad)?;
    let host = rest.split('/').next().unwrap_or("");
    if host.is_empty()
        || base.ends_with('/')
        || base.contains(['?', '#', ' '])
        || base.bytes().any(|b| b.is_ascii_control())
    {
        return Err(bad());
    }
    Ok(())
}

/// The URL for `key` under `base`, valid until `expires`, signed with
/// `keys`' active key.
pub fn sign(keys: &UrlKeys, base: &str, key: &PackKey, expires: u64) -> String {
    let k = keys.active();
    let tag = k.mac(expires, key.blob_key()).finalize().into_bytes();
    format!(
        "{base}/{}/{expires}/{}/{}",
        k.id,
        hex(&tag),
        key.blob_key().as_str()
    )
}

/// What a verified URL grants: one pack, until a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Granted {
    /// The pack.
    pub key: PackKey,
    /// Unix second the URL expires at.
    pub expires: u64,
}

/// Why a URL was refused. The express answers `403` to all of them and
/// tells the client nothing more; the reason only feeds the metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// Not `<key id>/<expires>/<signature>/<key>`, or a non-canonical part.
    #[error("malformed URL")]
    Malformed,
    /// The path names a blob that is not a canonical pack key.
    #[error("not a pack")]
    NotAPack,
    /// The key ID is not one this node accepts.
    #[error("unknown key")]
    UnknownKey,
    /// The signature does not match.
    #[error("bad signature")]
    BadSignature,
    /// The URL has expired.
    #[error("expired")]
    Expired,
}

impl Refusal {
    /// Every reason, for metrics.
    pub const ALL: [Refusal; 5] = [
        Refusal::Malformed,
        Refusal::NotAPack,
        Refusal::UnknownKey,
        Refusal::BadSignature,
        Refusal::Expired,
    ];

    /// A label for metrics.
    pub fn label(self) -> &'static str {
        match self {
            Refusal::Malformed => "malformed",
            Refusal::NotAPack => "not_a_pack",
            Refusal::UnknownKey => "unknown_key",
            Refusal::BadSignature => "bad_signature",
            Refusal::Expired => "expired",
        }
    }
}

/// Checks the path of a request under an express base (the part after
/// `<base>/`, without a leading `/`) at `now` (Unix seconds). The
/// signature is compared in constant time, and is checked before the
/// expiry so that a forged URL is never told apart from an expired one by
/// timing.
pub fn verify(keys: &UrlKeys, path: &str, now: u64) -> Result<Granted, Refusal> {
    let mut parts = path.splitn(4, '/');
    let (Some(id), Some(expires), Some(sig), Some(key)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Refusal::Malformed);
    };
    let expires_n: u64 = expires.parse().map_err(|_| Refusal::Malformed)?;
    // One spelling per expiry: no leading zeros, no sign.
    if expires_n.to_string() != expires {
        return Err(Refusal::Malformed);
    }
    let sig = unhex(sig)
        .filter(|s| s.len() == 32 && hex(s) == sig)
        .ok_or(Refusal::Malformed)?;
    let key = PackKey::parse(key).ok_or(Refusal::NotAPack)?;
    let k = keys.get(id).ok_or(Refusal::UnknownKey)?;
    k.mac(expires_n, key.blob_key())
        .verify_slice(&sig)
        .map_err(|_| Refusal::BadSignature)?;
    if now >= expires_n {
        return Err(Refusal::Expired);
    }
    Ok(Granted {
        key,
        expires: expires_n,
    })
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[usize::from(b >> 4)] as char);
        s.push(DIGITS[usize::from(b & 15)] as char);
    }
    s
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    s.as_bytes()
        .chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aspen_core::id::{PackChecksum, RepoId};

    const HEX: &str = "0123456789abcdef0123456789abcdef01234567";

    fn keys() -> UrlKeys {
        UrlKeys::new(
            UrlKey::new("k2", &[2; 32]).unwrap(),
            vec![UrlKey::new("k1", &[1; 32]).unwrap()],
        )
        .unwrap()
    }

    fn pack(repo: i64) -> PackKey {
        PackKey::new(
            RepoId::new(repo).unwrap(),
            PackChecksum::parse_hex(HEX).unwrap(),
            aspen_core::pack::PackGeneration::FIRST,
        )
    }

    fn path_of(url: &str) -> &str {
        url.strip_prefix("https://cdn.example/x/").unwrap()
    }

    /// Invariant: a URL verifies, for exactly its pack and expiry, until it
    /// expires; the same inputs always give the same URL.
    #[test]
    fn signed_urls_verify_until_they_expire() {
        let k = keys();
        let url = sign(&k, "https://cdn.example/x", &pack(7), 1_000);
        assert_eq!(url, sign(&k, "https://cdn.example/x", &pack(7), 1_000));
        assert!(url.starts_with("https://cdn.example/x/k2/1000/"));
        assert!(url.ends_with(&format!("/repos/7/packs/{HEX}.pack")));
        let got = verify(&k, path_of(&url), 999).unwrap();
        assert_eq!(
            got,
            Granted {
                key: pack(7),
                expires: 1_000
            }
        );
        assert_eq!(verify(&k, path_of(&url), 1_000), Err(Refusal::Expired));
    }

    /// Invariant: changing any part of a URL (key ID, expiry, signature,
    /// repository, checksum, file kind) makes it refused; in particular a
    /// URL for repository 7's pack does not open repository 8's pack with
    /// the same checksum.
    #[test]
    fn every_tampered_url_is_refused() {
        let k = keys();
        let url = sign(&k, "https://cdn.example/x", &pack(7), 1_000);
        let path = path_of(&url).to_owned();
        let sig = path.split('/').nth(2).unwrap().to_owned();
        let flipped = format!(
            "{}{}",
            if sig.starts_with('0') { "1" } else { "0" },
            &sig[1..]
        );
        let cases = [
            (
                path.replace("/repos/7/", "/repos/8/"),
                Refusal::BadSignature,
            ),
            (path.replacen("/1000/", "/2000/", 1), Refusal::BadSignature),
            (path.replacen(&sig, &flipped, 1), Refusal::BadSignature),
            (path.replacen("k2/", "k1/", 1), Refusal::BadSignature),
            (path.replacen("k2/", "k9/", 1), Refusal::UnknownKey),
            (path.replacen("/1000/", "/01000/", 1), Refusal::Malformed),
            (
                path.replacen(&sig, &sig.to_uppercase(), 1),
                Refusal::Malformed,
            ),
            (path.replacen(&sig, &sig[2..], 1), Refusal::Malformed),
            (path.replace(".pack", ".idx"), Refusal::NotAPack),
            (path.replace("repos/7", "repos/07"), Refusal::NotAPack),
            ("k2/1000".to_owned(), Refusal::Malformed),
            (String::new(), Refusal::Malformed),
        ];
        for (p, want) in cases {
            assert_eq!(verify(&k, &p, 0), Err(want), "{p}");
        }
        // An old key still verifies its own URLs after rotation.
        let old = UrlKeys::new(UrlKey::new("k1", &[1; 32]).unwrap(), vec![]).unwrap();
        let by_old = sign(&old, "https://cdn.example/x", &pack(7), 1_000);
        assert!(verify(&k, path_of(&by_old), 0).is_ok());
        // ...and a node without it refuses them.
        let only_new = UrlKeys::new(UrlKey::new("k2", &[2; 32]).unwrap(), vec![]).unwrap();
        assert_eq!(
            verify(&only_new, path_of(&by_old), 0),
            Err(Refusal::UnknownKey)
        );
    }

    /// Invariant: expiries are rounded up to the window, so every URL lives
    /// at least `ttl` and less than `ttl + window`, and signings within one
    /// window agree.
    #[test]
    fn expiries_round_up_to_the_window() {
        let p = SigningPolicy::new(Duration::from_secs(600), Duration::from_secs(300)).unwrap();
        for now in [0u64, 1, 299, 300, 301, 1_700_000_123] {
            let e = p.expiry(now);
            assert_eq!(e % 300, 0);
            assert!(e >= now + 600 && e < now + 900, "{now} -> {e}");
        }
        assert_eq!(p.expiry(1), p.expiry(300));
        assert_ne!(p.expiry(300), p.expiry(301));
        assert!(SigningPolicy::new(Duration::from_secs(3000), Duration::from_secs(601)).is_err());
        assert!(SigningPolicy::new(Duration::ZERO, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn keys_and_bases_are_checked() {
        assert!(UrlKey::new("K", &[0; 32]).is_err());
        assert!(UrlKey::new("-", &[0; 32]).is_err());
        assert!(UrlKey::new("abcdefghijklmnopq", &[0; 32]).is_err());
        assert!(UrlKey::new("k", &[0; 31]).is_err());
        let dbg = format!("{:?}", UrlKey::new("k", &[0xab; 32]).unwrap());
        assert!(!dbg.contains("ab, ") && !dbg.contains("171"), "{dbg}");
        let parsed =
            UrlKeys::parse(&format!("new:{},old:{}", "11".repeat(32), "22".repeat(32))).unwrap();
        assert_eq!(parsed.active().id(), "new");
        let err = UrlKeys::parse("new:zz").unwrap_err().to_string();
        assert!(!err.contains("zz"), "{err}");
        assert!(UrlKeys::parse(&format!("a:{0},a:{0}", "11".repeat(32))).is_err());
        assert!(UrlKeys::parse("").is_err());
        for ok in ["http://h", "https://h:1/a/b"] {
            assert!(check_base(ok).is_ok(), "{ok}");
        }
        for bad in ["ftp://h", "http://", "http://h/", "http://h?x", "h"] {
            assert!(check_base(bad).is_err(), "{bad}");
        }
    }
}
