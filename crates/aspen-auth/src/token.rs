//! API tokens.
//!
//! A token is `aspen_<id>_<secret>`: a 16-byte ID and a 32-byte secret,
//! both lowercase hex. The ID is not secret; it names the token in logs and
//! in the store. Only the SHA-256 digest of the secret is stored, and a
//! presented secret is checked by comparing digests in constant time
//! ([`TokenDigest::matches`]). The plaintext exists only in the
//! [`IssuedToken`] returned once, when the token is created.
//!
//! The secret is 256 bits from the operating system's generator, so a fast
//! hash is enough: there is nothing to brute-force that a slow password hash
//! would protect (decisions log, B3).
//!
//! A token carries its own [`Grants`]. It never inherits what its subject
//! may do through other routes (OIDC rules, other tokens), so a token made
//! for one repository reaches only that repository.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Mutex;

use aspen_core::auth::Secret;
use aspen_core::id::UnixMicros;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::grant::Grants;

/// The text every token starts with. Distinctive, so secret scanners can
/// find leaked tokens.
pub const TOKEN_PREFIX: &str = "aspen_";

const ID_LEN: usize = 16;
const SECRET_LEN: usize = 32;
/// Length of a token's text form.
pub const TOKEN_LEN: usize = TOKEN_PREFIX.len() + 2 * ID_LEN + 1 + 2 * SECRET_LEN;

/// A token's public ID.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TokenId([u8; ID_LEN]);

impl TokenId {
    /// The ID's bytes.
    pub fn as_bytes(&self) -> &[u8; ID_LEN] {
        &self.0
    }

    /// An ID from its bytes (as stored).
    pub fn from_bytes(bytes: [u8; ID_LEN]) -> Self {
        Self(bytes)
    }

    /// Parses the 32-digit lowercase hex form.
    pub fn parse_hex(s: &str) -> Option<Self> {
        decode_hex::<ID_LEN>(s).map(Self)
    }
}

impl fmt::Display for TokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&encode_hex(&self.0))
    }
}

impl fmt::Debug for TokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TokenId({self})")
    }
}

/// The SHA-256 digest of a token's secret: the only form in which a secret
/// is stored.
///
/// It has no `PartialEq`, so it cannot be compared with a short-circuiting
/// `==`; [`TokenDigest::matches`] is the only comparison, and it runs in
/// constant time:
///
/// ```compile_fail
/// fn leaks_timing(a: &aspen_auth::TokenDigest, b: &aspen_auth::TokenDigest) -> bool {
///     a == b
/// }
/// ```
#[derive(Clone)]
pub struct TokenDigest([u8; 32]);

impl TokenDigest {
    /// The digest of a secret.
    fn of(secret: &[u8; SECRET_LEN]) -> Self {
        Self(Sha256::digest(secret).into())
    }

    /// The digest's bytes, for storage.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// A digest from its bytes (as stored).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Whether `other` is the same digest, compared in constant time.
    pub fn matches(&self, other: &TokenDigest) -> bool {
        self.0.ct_eq(&other.0).into()
    }
}

impl fmt::Debug for TokenDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenDigest(<sha256>)")
    }
}

/// A stored token: everything about it except the secret.
#[derive(Debug, Clone)]
pub struct TokenRecord {
    /// Public ID.
    pub id: TokenId,
    /// Digest of the secret.
    pub digest: TokenDigest,
    /// Who the token acts as, for logs and rate limits (`user:alice`,
    /// `agent:ci-7`). Grants never come from the subject.
    pub subject: String,
    /// What the token may do.
    pub grants: Grants,
    /// A note for operators.
    pub description: String,
    /// When it was created.
    pub created_at: UnixMicros,
    /// When it stops working, if ever.
    pub expires_at: Option<UnixMicros>,
    /// When it was revoked, if it was.
    pub revoked_at: Option<UnixMicros>,
}

impl TokenRecord {
    /// Whether the token may be used at `now`: not revoked, not expired.
    pub fn is_live(&self, now: UnixMicros) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|end| now < end)
    }
}

/// A newly created token: its record, and the plaintext to hand to the
/// client once. The plaintext is not stored anywhere.
#[derive(Debug)]
pub struct IssuedToken {
    /// What to store.
    pub record: TokenRecord,
    /// The token text, `aspen_<id>_<secret>`.
    pub token: Secret,
}

/// What a new token should allow.
#[derive(Debug, Clone)]
pub struct NewToken {
    /// Who the token acts as.
    pub subject: String,
    /// What it may do.
    pub grants: Grants,
    /// A note for operators.
    pub description: String,
    /// When it stops working, if ever.
    pub expires_at: Option<UnixMicros>,
}

/// The prefix of OIDC principal IDs (`oidc:<issuer name>:<sub>`). Token
/// subjects may not start with it, so a token can never act as, or share a
/// rate-limit bucket with, an OIDC principal.
pub const OIDC_SUBJECT_PREFIX: &str = "oidc:";

/// Whether `subject` is one a token may act as: 1 to 255 characters, none
/// of them control characters, not starting with [`OIDC_SUBJECT_PREFIX`].
pub fn is_token_subject(subject: &str) -> bool {
    !subject.is_empty()
        && subject.len() <= 255
        && !subject.chars().any(|c| c.is_control())
        && !subject.starts_with(OIDC_SUBJECT_PREFIX)
}

/// Why a token could not be created.
#[derive(Debug, thiserror::Error)]
pub enum IssueError {
    /// The operating system's random generator failed.
    #[error("random generator failed: {0}")]
    Random(String),
    /// The subject is empty, too long, not printable, or reserved for OIDC
    /// principals.
    #[error("token subject must be 1 to 255 printable characters and not start with \"oidc:\"")]
    Subject,
}

/// Creates a token: a fresh random ID and secret.
pub fn issue(new: NewToken, now: UnixMicros) -> Result<IssuedToken, IssueError> {
    if !is_token_subject(&new.subject) {
        return Err(IssueError::Subject);
    }
    let mut id = [0u8; ID_LEN];
    let mut secret = [0u8; SECRET_LEN];
    getrandom::fill(&mut id).map_err(|e| IssueError::Random(e.to_string()))?;
    getrandom::fill(&mut secret).map_err(|e| IssueError::Random(e.to_string()))?;
    let token = format!("{TOKEN_PREFIX}{}_{}", encode_hex(&id), encode_hex(&secret));
    let record = TokenRecord {
        id: TokenId(id),
        digest: TokenDigest::of(&secret),
        subject: new.subject,
        grants: new.grants,
        description: new.description,
        created_at: now,
        expires_at: new.expires_at,
        revoked_at: None,
    };
    Ok(IssuedToken {
        record,
        token: Secret::new(token),
    })
}

/// A presented token, split into its ID and the digest of its secret. The
/// secret itself is not kept.
#[derive(Debug)]
pub struct PresentedToken {
    /// The ID part.
    pub id: TokenId,
    /// Digest of the secret part.
    pub digest: TokenDigest,
}

impl PresentedToken {
    /// Parses `aspen_<id>_<secret>`; `None` for anything else.
    pub fn parse(token: &str) -> Option<Self> {
        if token.len() != TOKEN_LEN {
            return None;
        }
        let rest = token.strip_prefix(TOKEN_PREFIX)?;
        let (id, secret) = rest.split_once('_')?;
        let id = TokenId::parse_hex(id)?;
        let secret = decode_hex::<SECRET_LEN>(secret)?;
        Some(Self {
            id,
            digest: TokenDigest::of(&secret),
        })
    }
}

/// Whether `token` looks like an Aspen token (it may still be invalid).
pub fn looks_like_token(token: &str) -> bool {
    token.starts_with(TOKEN_PREFIX)
}

/// The store failed; the request should be retried.
#[derive(Debug, Clone, thiserror::Error)]
#[error("token store unavailable: {0}")]
pub struct TokenStoreError(pub String);

/// Where token records live. Implemented in memory here and on the
/// metadata database by the server.
pub trait TokenStore: Send + Sync {
    /// The record with this ID, revoked and expired ones included.
    fn token(
        &self,
        id: &TokenId,
    ) -> impl Future<Output = Result<Option<TokenRecord>, TokenStoreError>> + Send;
}

/// A [`TokenStore`] in memory, for tests and single-process setups whose
/// tokens come from configuration.
#[derive(Debug, Default)]
pub struct MemoryTokenStore {
    tokens: Mutex<HashMap<TokenId, TokenRecord>>,
}

impl MemoryTokenStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a record.
    pub fn insert(&self, record: TokenRecord) {
        self.lock().insert(record.id, record);
    }

    /// Marks a token revoked; `false` if there is no such token.
    pub fn revoke(&self, id: &TokenId, now: UnixMicros) -> bool {
        match self.lock().get_mut(id) {
            Some(r) => {
                r.revoked_at.get_or_insert(now);
                true
            }
            None => false,
        }
    }

    /// Every record, for inspection.
    pub fn records(&self) -> Vec<TokenRecord> {
        self.lock().values().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<TokenId, TokenRecord>> {
        // A panic while holding this lock cannot leave a half-written map
        // entry, so a poisoned lock is still consistent.
        self.tokens.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl TokenStore for MemoryTokenStore {
    async fn token(&self, id: &TokenId) -> Result<Option<TokenRecord>, TokenStoreError> {
        Ok(self.lock().get(id).cloned())
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[usize::from(b >> 4)] as char);
        s.push(DIGITS[usize::from(b & 0xf)] as char);
    }
    s
}

fn decode_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    fn digit(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    }
    let s = s.as_bytes();
    if s.len() != 2 * N {
        return None;
    }
    let mut out = [0u8; N];
    for (i, pair) in s.as_chunks::<2>().0.iter().enumerate() {
        out[i] = (digit(pair[0])? << 4) | digit(pair[1])?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_token() -> NewToken {
        NewToken {
            subject: "agent:ci-7".into(),
            grants: Grants::new(["write:acme/mono".parse().unwrap()]),
            description: "test".into(),
            expires_at: None,
        }
    }

    /// Invariant: a token's subject can never be an OIDC principal ID, so a
    /// token cannot act as an OIDC user or drain its rate-limit bucket.
    #[test]
    fn token_subjects_cannot_take_oidc_principal_ids() {
        for subject in [
            "",
            "oidc:gha:repo:acme/mono",
            "oidc:",
            "a\nb",
            &"x".repeat(256),
        ] {
            let new = NewToken {
                subject: subject.to_owned(),
                ..new_token()
            };
            assert!(
                matches!(issue(new, UnixMicros(1)), Err(IssueError::Subject)),
                "{subject:?}"
            );
        }
        for subject in ["agent:ci-7", "user:oidc:alice", "OIDC-bot"] {
            let new = NewToken {
                subject: subject.to_owned(),
                ..new_token()
            };
            assert!(issue(new, UnixMicros(1)).is_ok(), "{subject:?}");
        }
    }

    #[test]
    fn an_issued_token_parses_back_to_its_record() {
        let issued = issue(new_token(), UnixMicros(1)).unwrap();
        let text = issued.token.expose();
        assert_eq!(text.len(), TOKEN_LEN);
        assert!(text.starts_with(TOKEN_PREFIX));
        let presented = PresentedToken::parse(text).unwrap();
        assert_eq!(presented.id, issued.record.id);
        assert!(presented.digest.matches(&issued.record.digest));
    }

    /// Invariant: a stored record holds no part of the secret; its debug
    /// form (what reaches logs) neither.
    #[test]
    fn the_record_never_holds_the_secret() {
        let issued = issue(new_token(), UnixMicros(1)).unwrap();
        let text = issued.token.expose().to_owned();
        let secret_hex = text.rsplit('_').next().unwrap();
        let record = format!("{:?}", issued.record);
        let raw_digest = encode_hex(issued.record.digest.as_bytes());
        assert!(!record.contains(secret_hex));
        assert!(!record.contains(&raw_digest));
        assert!(!format!("{issued:?}").contains(secret_hex));
        // Every 8-character window of the secret is absent from the stored
        // bytes, so not even a fragment is kept.
        let stored = format!(
            "{}{}{}",
            issued.record.id, raw_digest, issued.record.subject
        );
        for w in secret_hex.as_bytes().windows(8) {
            assert!(!stored.contains(std::str::from_utf8(w).unwrap()));
        }
    }

    #[test]
    fn a_changed_secret_does_not_match() {
        let issued = issue(new_token(), UnixMicros(1)).unwrap();
        let mut text = issued.token.expose().to_owned();
        let last = text.pop().unwrap();
        text.push(if last == '0' { '1' } else { '0' });
        let presented = PresentedToken::parse(&text).unwrap();
        assert_eq!(presented.id, issued.record.id);
        assert!(!presented.digest.matches(&issued.record.digest));
    }

    #[test]
    fn malformed_tokens_do_not_parse() {
        let good = issue(new_token(), UnixMicros(1)).unwrap();
        let good = good.token.expose();
        let upper = good.to_uppercase().replace("ASPEN_", "aspen_");
        let cases = [
            "",
            "aspen_",
            &good[..good.len() - 1],
            &format!("{good}0"),
            &good.replacen("aspen_", "aspem_", 1),
            &upper,
            &good.replacen('_', "-", 2),
        ];
        for c in cases {
            assert!(PresentedToken::parse(c).is_none(), "{c} parsed");
        }
    }

    #[test]
    fn liveness_follows_revocation_and_expiry() {
        let mut r = issue(new_token(), UnixMicros(1)).unwrap().record;
        assert!(r.is_live(UnixMicros(i64::MAX)));
        r.expires_at = Some(UnixMicros(100));
        assert!(r.is_live(UnixMicros(99)));
        assert!(!r.is_live(UnixMicros(100)));
        r.expires_at = None;
        r.revoked_at = Some(UnixMicros(5));
        assert!(!r.is_live(UnixMicros(1)));
    }

    #[test]
    fn subjects_are_printable() {
        for bad in ["", "a\nb", &"x".repeat(256)] {
            let mut n = new_token();
            n.subject = bad.into();
            assert!(issue(n, UnixMicros(1)).is_err());
        }
    }
}
