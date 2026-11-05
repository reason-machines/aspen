//! SSH public keys: what a key is, how it is registered, and where
//! registered keys live (node C3).
//!
//! A registered key is to SSH what an API token is to HTTP: it carries its
//! own [`Grants`] and acts as a subject (`user:alice`, `agent:ci-7`) for logs
//! and rate limits. It never inherits what its subject may do through other
//! routes, so a key registered for one repository reaches only that
//! repository (plus what anonymous callers may do).
//!
//! The SSH transport (`aspen-server`'s `ssh` module) proves that the client
//! holds the private key, then presents the key to
//! [`crate::AuthService::identify`] as a
//! [`Credential::SshKey`](aspen_core::auth::Credential::SshKey) carrying its
//! [`SshFingerprint`]. A key is found by that fingerprint alone, the SHA-256
//! of the key's wire encoding, which is also what `ssh-keygen -l` prints.
//!
//! # Accepted key types
//!
//! [`SshKeyAlgorithm`] lists them: Ed25519 and ECDSA on the NIST P-256,
//! P-384 and P-521 curves. RSA keys (`ssh-rsa`) are refused at registration
//! and at authentication: verifying them needs the `rsa` crate, which
//! carries RUSTSEC-2023-0071 with no fix, and Aspen keeps that crate out of
//! its dependency tree (decisions log, C3). Security-key types
//! (`sk-ssh-ed25519@openssh.com`, `sk-ecdsa-sha2-nistp256@openssh.com`) are
//! not accepted in v1 either: nothing in Aspen's test suite exercises them.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Mutex;

use aspen_core::id::UnixMicros;
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use sha2::{Digest, Sha256};

use crate::grant::Grants;
use crate::token::is_token_subject;

/// The text an OpenSSH SHA-256 fingerprint starts with.
pub const FINGERPRINT_PREFIX: &str = "SHA256:";

/// Longest key blob accepted, in bytes. The largest accepted key
/// (ECDSA P-521) encodes to 172 bytes; anything far longer is refused
/// before it is hashed or stored.
pub const MAX_KEY_BLOB_LEN: usize = 1024;

/// An SSH key's fingerprint: the SHA-256 digest of its wire encoding (the
/// base64 part of an `authorized_keys` line, decoded). Its text form is
/// OpenSSH's, `SHA256:` and the unpadded base64 of the digest, as
/// `ssh-keygen -l` prints it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SshFingerprint([u8; 32]);

impl SshFingerprint {
    /// The fingerprint of a key's wire encoding.
    pub fn of_blob(blob: &[u8]) -> Self {
        Self(Sha256::digest(blob).into())
    }

    /// The digest's bytes, for storage.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// A fingerprint from its bytes (as stored).
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parses the text form, `SHA256:<43 base64 characters>`; `None` for
    /// anything else.
    pub fn parse(s: &str) -> Option<Self> {
        let b64 = s.strip_prefix(FINGERPRINT_PREFIX)?;
        // 32 bytes are 43 unpadded base64 characters. The decoder refuses
        // trailing bits that are not zero, so each digest has one spelling.
        if b64.len() != 43 {
            return None;
        }
        let bytes = STANDARD_NO_PAD.decode(b64).ok()?;
        Some(Self(bytes.try_into().ok()?))
    }
}

impl fmt::Display for SshFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{FINGERPRINT_PREFIX}{}", STANDARD_NO_PAD.encode(self.0))
    }
}

impl fmt::Debug for SshFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SshFingerprint({self})")
    }
}

/// A key type Aspen accepts (module docs, "Accepted key types").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SshKeyAlgorithm {
    /// `ssh-ed25519`.
    Ed25519,
    /// `ecdsa-sha2-nistp256`.
    EcdsaP256,
    /// `ecdsa-sha2-nistp384`.
    EcdsaP384,
    /// `ecdsa-sha2-nistp521`.
    EcdsaP521,
}

impl SshKeyAlgorithm {
    /// Every accepted type.
    pub const ALL: [Self; 4] = [
        Self::Ed25519,
        Self::EcdsaP256,
        Self::EcdsaP384,
        Self::EcdsaP521,
    ];

    /// The type's name, as it appears in a key and an `authorized_keys`
    /// line.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Ed25519 => "ssh-ed25519",
            Self::EcdsaP256 => "ecdsa-sha2-nistp256",
            Self::EcdsaP384 => "ecdsa-sha2-nistp384",
            Self::EcdsaP521 => "ecdsa-sha2-nistp521",
        }
    }

    /// The accepted type with this name.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|a| a.name() == name)
    }

    /// The fields after the type name in the key's wire encoding: the
    /// length of each SSH string, or `None` for "any length".
    fn fields(self) -> &'static [Field] {
        match self {
            // The 32-byte public key.
            Self::Ed25519 => &[Field::Exact(32)],
            // The curve name, then the SEC1 uncompressed point
            // (0x04 || x || y).
            Self::EcdsaP256 => &[Field::Text("nistp256"), Field::Exact(65)],
            Self::EcdsaP384 => &[Field::Text("nistp384"), Field::Exact(97)],
            Self::EcdsaP521 => &[Field::Text("nistp521"), Field::Exact(133)],
        }
    }
}

impl fmt::Display for SshKeyAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, Copy)]
enum Field {
    /// A string of exactly this many bytes.
    Exact(usize),
    /// A string equal to this text.
    Text(&'static str),
}

/// Why a public key was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SshKeyError {
    /// Not an `authorized_keys` line or a key encoding Aspen can read.
    #[error("malformed SSH public key: {0}")]
    Malformed(&'static str),
    /// A well-formed key of a type Aspen does not accept.
    #[error(
        "SSH key type `{0}` is not accepted; use an ssh-ed25519 or ecdsa-sha2-nistp256/384/521 key"
    )]
    Unsupported(String),
}

/// A public key of an accepted type, as its wire encoding.
#[derive(Clone, PartialEq, Eq)]
pub struct SshPublicKey {
    algorithm: SshKeyAlgorithm,
    blob: Vec<u8>,
}

impl SshPublicKey {
    /// Reads a key from its wire encoding. The encoding must be exactly the
    /// type name and the fields of that type, with nothing after them.
    pub fn from_blob(blob: &[u8]) -> Result<Self, SshKeyError> {
        if blob.len() > MAX_KEY_BLOB_LEN {
            return Err(SshKeyError::Malformed("key is too long"));
        }
        let mut rest = blob;
        let name = ssh_string(&mut rest).ok_or(SshKeyError::Malformed("no key type"))?;
        let name = std::str::from_utf8(name).map_err(|_| SshKeyError::Malformed("key type"))?;
        let algorithm = SshKeyAlgorithm::from_name(name)
            .ok_or_else(|| SshKeyError::Unsupported(printable(name)))?;
        for field in algorithm.fields() {
            let value = ssh_string(&mut rest).ok_or(SshKeyError::Malformed("truncated key"))?;
            let ok = match field {
                Field::Exact(n) => value.len() == *n,
                Field::Text(t) => value == t.as_bytes(),
            };
            if !ok {
                return Err(SshKeyError::Malformed("key fields do not match its type"));
            }
        }
        if !rest.is_empty() {
            return Err(SshKeyError::Malformed("data after the key"));
        }
        Ok(Self {
            algorithm,
            blob: blob.to_vec(),
        })
    }

    /// Reads a key from an `authorized_keys`-style line: `<type> <base64>`
    /// and an optional comment, which is dropped. The type in front must
    /// match the type inside the encoding. Options in front of the type
    /// (`no-pty ssh-ed25519 ...`) are refused rather than ignored.
    pub fn parse_openssh(line: &str) -> Result<Self, SshKeyError> {
        let mut words = line.split_ascii_whitespace();
        let name = words.next().ok_or(SshKeyError::Malformed("empty line"))?;
        let b64 = words
            .next()
            .ok_or(SshKeyError::Malformed("no key after the type"))?;
        if b64.len() > MAX_KEY_BLOB_LEN * 2 {
            return Err(SshKeyError::Malformed("key is too long"));
        }
        let blob = STANDARD
            .decode(b64)
            .map_err(|_| SshKeyError::Malformed("key is not base64"))?;
        let key = Self::from_blob(&blob)?;
        if key.algorithm.name() != name {
            return Err(SshKeyError::Malformed(
                "the type in front does not match the key",
            ));
        }
        Ok(key)
    }

    /// The key's type.
    pub fn algorithm(&self) -> SshKeyAlgorithm {
        self.algorithm
    }

    /// The key's wire encoding.
    pub fn blob(&self) -> &[u8] {
        &self.blob
    }

    /// The key's fingerprint.
    pub fn fingerprint(&self) -> SshFingerprint {
        SshFingerprint::of_blob(&self.blob)
    }

    /// The `authorized_keys` form without a comment: `<type> <base64>`.
    pub fn to_openssh(&self) -> String {
        format!("{} {}", self.algorithm.name(), STANDARD.encode(&self.blob))
    }
}

impl fmt::Debug for SshPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SshPublicKey({} {})", self.algorithm, self.fingerprint())
    }
}

/// Reads one SSH `string` (a 32-bit big-endian length, then the bytes).
fn ssh_string<'a>(buf: &mut &'a [u8]) -> Option<&'a [u8]> {
    let (len, rest) = buf.split_first_chunk::<4>()?;
    let len = usize::try_from(u32::from_be_bytes(*len)).ok()?;
    if rest.len() < len {
        return None;
    }
    let (value, rest) = rest.split_at(len);
    *buf = rest;
    Some(value)
}

/// A key type name made safe for an error message.
fn printable(name: &str) -> String {
    name.chars()
        .take(64)
        .map(|c| if c.is_ascii_graphic() { c } else { '?' })
        .collect()
}

/// A registered key: everything Aspen stores about it.
#[derive(Debug, Clone)]
pub struct SshKeyRecord {
    /// The key.
    pub key: SshPublicKey,
    /// Who the key acts as, for logs and rate limits (`user:alice`).
    /// Grants never come from the subject.
    pub subject: String,
    /// What the key may do.
    pub grants: Grants,
    /// A note for operators.
    pub description: String,
    /// When it was registered.
    pub created_at: UnixMicros,
    /// When it stops working, if ever.
    pub expires_at: Option<UnixMicros>,
    /// When it was revoked, if it was.
    pub revoked_at: Option<UnixMicros>,
}

impl SshKeyRecord {
    /// The key's fingerprint, under which it is stored.
    pub fn fingerprint(&self) -> SshFingerprint {
        self.key.fingerprint()
    }

    /// Whether the key may be used at `now`: not revoked, not expired.
    pub fn is_live(&self, now: UnixMicros) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|end| now < end)
    }
}

/// What a new key registration should allow.
#[derive(Debug, Clone)]
pub struct NewSshKey {
    /// The key.
    pub key: SshPublicKey,
    /// Who the key acts as.
    pub subject: String,
    /// What it may do.
    pub grants: Grants,
    /// A note for operators.
    pub description: String,
    /// When it stops working, if ever.
    pub expires_at: Option<UnixMicros>,
}

/// Why a key could not be registered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegisterError {
    /// The subject is empty, too long, not printable, or reserved for OIDC
    /// principals (the same rule as for token subjects).
    #[error("key subject must be 1 to 255 printable characters and not start with \"oidc:\"")]
    Subject,
    /// The expiry is not after the registration time.
    #[error("a key's expiry must be after its registration time")]
    Expiry,
}

/// The record for a new key, registered at `now`. The subject follows the
/// token rule ([`crate::token::is_token_subject`]), so a key never acts as
/// an OIDC principal, and it may share a subject (and so a rate-limit
/// bucket) with that subject's tokens.
pub fn register(new: NewSshKey, now: UnixMicros) -> Result<SshKeyRecord, RegisterError> {
    if !is_token_subject(&new.subject) {
        return Err(RegisterError::Subject);
    }
    if new.expires_at.is_some_and(|end| end <= now) {
        return Err(RegisterError::Expiry);
    }
    Ok(SshKeyRecord {
        key: new.key,
        subject: new.subject,
        grants: new.grants,
        description: new.description,
        created_at: now,
        expires_at: new.expires_at,
        revoked_at: None,
    })
}

/// The key store failed; the request should be retried.
#[derive(Debug, Clone, thiserror::Error)]
#[error("SSH key store unavailable: {0}")]
pub struct SshKeyStoreError(pub String);

/// Where registered keys live. Implemented in memory here and on the
/// metadata database by the server.
pub trait SshKeyStore: Send + Sync {
    /// The key with this fingerprint, revoked and expired ones included.
    fn ssh_key(
        &self,
        fingerprint: &SshFingerprint,
    ) -> impl Future<Output = Result<Option<SshKeyRecord>, SshKeyStoreError>> + Send;
}

/// No keys at all: what an [`crate::AuthService`] uses until it is given a
/// key store, so every SSH key is refused.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSshKeys;

impl SshKeyStore for NoSshKeys {
    async fn ssh_key(&self, _: &SshFingerprint) -> Result<Option<SshKeyRecord>, SshKeyStoreError> {
        Ok(None)
    }
}

/// An [`SshKeyStore`] in memory, for tests and single-process setups whose
/// keys come from configuration.
#[derive(Debug, Default)]
pub struct MemorySshKeyStore {
    keys: Mutex<HashMap<SshFingerprint, SshKeyRecord>>,
}

impl MemorySshKeyStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a record.
    pub fn insert(&self, record: SshKeyRecord) {
        self.lock().insert(record.fingerprint(), record);
    }

    /// Marks a key revoked; `false` if there is no such key.
    pub fn revoke(&self, fingerprint: &SshFingerprint, now: UnixMicros) -> bool {
        match self.lock().get_mut(fingerprint) {
            Some(r) => {
                r.revoked_at.get_or_insert(now);
                true
            }
            None => false,
        }
    }

    /// Every record, for inspection.
    pub fn records(&self) -> Vec<SshKeyRecord> {
        self.lock().values().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<SshFingerprint, SshKeyRecord>> {
        // A panic while holding this lock cannot leave a half-written map
        // entry, so a poisoned lock is still consistent.
        self.keys.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl SshKeyStore for MemorySshKeyStore {
    async fn ssh_key(
        &self,
        fingerprint: &SshFingerprint,
    ) -> Result<Option<SshKeyRecord>, SshKeyStoreError> {
        Ok(self.lock().get(fingerprint).cloned())
    }
}

impl<S: SshKeyStore> SshKeyStore for std::sync::Arc<S> {
    fn ssh_key(
        &self,
        fingerprint: &SshFingerprint,
    ) -> impl Future<Output = Result<Option<SshKeyRecord>, SshKeyStoreError>> + Send {
        (**self).ssh_key(fingerprint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ssh-keygen -t ed25519` output (a throwaway test key) and the
    /// fingerprint `ssh-keygen -l` prints for it.
    const ED25519: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINfpeAnPZKwL5SOdrJPD2fsT9+r+8GZFy5xfTsLiuMxp test@aspen";
    const ED25519_FINGERPRINT: &str = "SHA256:8EauBcbIoQLr4ozBJ1AH5fQVRcF2B4FYhZuTJgbqMrs";

    fn blob(fields: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for f in fields {
            out.extend_from_slice(&u32::try_from(f.len()).unwrap().to_be_bytes());
            out.extend_from_slice(f);
        }
        out
    }

    #[test]
    fn an_openssh_line_round_trips_and_drops_its_comment() {
        let key = SshPublicKey::parse_openssh(ED25519).unwrap();
        assert_eq!(key.algorithm(), SshKeyAlgorithm::Ed25519);
        assert_eq!(
            key.to_openssh(),
            ED25519.strip_suffix(" test@aspen").unwrap()
        );
        let again = SshPublicKey::parse_openssh(&key.to_openssh()).unwrap();
        assert_eq!(again, key);
    }

    #[test]
    fn fingerprints_round_trip_through_their_text_form() {
        let key = SshPublicKey::parse_openssh(ED25519).unwrap();
        let fp = key.fingerprint();
        let text = fp.to_string();
        assert_eq!(text, ED25519_FINGERPRINT, "what ssh-keygen -l prints");
        assert_eq!(SshFingerprint::parse(&text), Some(fp));
        for bad in [
            "",
            "SHA256:",
            &text[..text.len() - 1],
            &format!("{text}A"),
            &text.replace("SHA256:", "sha256:"),
            &text.replace("SHA256:", "MD5:"),
        ] {
            assert_eq!(SshFingerprint::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn every_accepted_type_parses_from_its_fields() {
        let cases: [(SshKeyAlgorithm, Vec<u8>); 4] = [
            (SshKeyAlgorithm::Ed25519, blob(&[b"ssh-ed25519", &[7; 32]])),
            (
                SshKeyAlgorithm::EcdsaP256,
                blob(&[b"ecdsa-sha2-nistp256", b"nistp256", &[4; 65]]),
            ),
            (
                SshKeyAlgorithm::EcdsaP384,
                blob(&[b"ecdsa-sha2-nistp384", b"nistp384", &[4; 97]]),
            ),
            (
                SshKeyAlgorithm::EcdsaP521,
                blob(&[b"ecdsa-sha2-nistp521", b"nistp521", &[4; 133]]),
            ),
        ];
        for (alg, b) in cases {
            let key = SshPublicKey::from_blob(&b).unwrap();
            assert_eq!(key.algorithm(), alg);
            assert_eq!(key.fingerprint(), SshFingerprint::of_blob(&b));
        }
    }

    /// Invariant: only the accepted types are keys; RSA and security-key
    /// types are refused as unsupported, with their name in the message.
    #[test]
    fn rsa_and_other_types_are_refused() {
        for name in [
            "ssh-rsa",
            "ssh-dss",
            "sk-ssh-ed25519@openssh.com",
            "sk-ecdsa-sha2-nistp256@openssh.com",
            "ssh-ed25519-cert-v01@openssh.com",
        ] {
            let b = blob(&[name.as_bytes(), &[1; 32]]);
            let e = SshPublicKey::from_blob(&b).unwrap_err();
            assert_eq!(e, SshKeyError::Unsupported(name.to_owned()));
            assert!(e.to_string().contains(name));
        }
    }

    /// Invariant: a key's encoding is exactly its type's fields, so one key
    /// has one encoding and so one fingerprint.
    #[test]
    fn malformed_encodings_are_refused() {
        let cases: Vec<Vec<u8>> = vec![
            Vec::new(),
            vec![0, 0, 0],
            blob(&[b"ssh-ed25519"]),
            blob(&[b"ssh-ed25519", &[7; 31]]),
            blob(&[b"ssh-ed25519", &[7; 33]]),
            [blob(&[b"ssh-ed25519", &[7; 32]]), vec![0]].concat(),
            blob(&[b"ecdsa-sha2-nistp256", b"nistp384", &[4; 65]]),
            blob(&[b"ecdsa-sha2-nistp256", b"nistp256", &[4; 64]]),
            blob(&[b"ssh-ed25519", &[7; MAX_KEY_BLOB_LEN]]),
            // A length that runs past the end.
            [&[0, 0, 0, 200][..], b"ssh-ed25519"].concat(),
        ];
        for b in cases {
            assert!(
                matches!(SshPublicKey::from_blob(&b), Err(SshKeyError::Malformed(_))),
                "{b:?}"
            );
        }
        let key = SshPublicKey::parse_openssh(ED25519).unwrap();
        let b64 = key.to_openssh().split(' ').nth(1).unwrap().to_owned();
        for line in [
            String::new(),
            "ssh-ed25519".into(),
            format!("ssh-ed25519 {b64}!"),
            format!("ecdsa-sha2-nistp256 {b64}"),
            format!("no-pty ssh-ed25519 {b64}"),
        ] {
            assert!(SshPublicKey::parse_openssh(&line).is_err(), "{line}");
        }
    }

    fn new_key() -> NewSshKey {
        NewSshKey {
            key: SshPublicKey::parse_openssh(ED25519).unwrap(),
            subject: "user:alice".into(),
            grants: Grants::new(["write:acme/mono".parse().unwrap()]),
            description: "laptop".into(),
            expires_at: None,
        }
    }

    /// Invariant: a key's subject follows the token rule, so a key can
    /// never act as an OIDC principal; an expiry must lie ahead.
    #[test]
    fn registration_checks_subject_and_expiry() {
        let r = register(new_key(), UnixMicros(10)).unwrap();
        assert_eq!(r.created_at, UnixMicros(10));
        assert!(r.is_live(UnixMicros(i64::MAX)));
        for subject in ["", "oidc:gha:x", "a\nb"] {
            let mut n = new_key();
            n.subject = subject.into();
            assert_eq!(
                register(n, UnixMicros(10)).unwrap_err(),
                RegisterError::Subject
            );
        }
        let mut n = new_key();
        n.expires_at = Some(UnixMicros(10));
        assert_eq!(
            register(n, UnixMicros(10)).unwrap_err(),
            RegisterError::Expiry
        );
    }

    #[test]
    fn liveness_follows_revocation_and_expiry() {
        let mut r = register(new_key(), UnixMicros(1)).unwrap();
        r.expires_at = Some(UnixMicros(100));
        assert!(r.is_live(UnixMicros(99)));
        assert!(!r.is_live(UnixMicros(100)));
        r.expires_at = None;
        r.revoked_at = Some(UnixMicros(5));
        assert!(!r.is_live(UnixMicros(1)));
        let store = MemorySshKeyStore::new();
        let r = register(new_key(), UnixMicros(1)).unwrap();
        let fp = r.fingerprint();
        store.insert(r);
        assert!(store.revoke(&fp, UnixMicros(3)));
        assert!(store.revoke(&fp, UnixMicros(9)));
        assert_eq!(store.records()[0].revoked_at, Some(UnixMicros(3)));
    }
}
