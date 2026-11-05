//! Who may do what: turning a credential into an [`Identity`], and the
//! access decision on it.
//!
//! # Where grants come from
//!
//! - **Everyone**, signed in or not, holds the policy's `anonymous` grants
//!   (for example `read:demo/*` for public repositories).
//! - **An API token** holds exactly the grants stored with it.
//! - **An SSH key** holds exactly the grants registered with it
//!   ([`crate::ssh`]).
//! - **An OIDC identity token** holds the grants of every rule of its
//!   issuer whose claims all match, plus the grants listed for its subject
//!   (`oidc:<issuer name>:<sub>`) under `subjects`.
//!
//! Grants never flow between those routes: a token or key does not pick up
//! its subject's OIDC grants, so a token or key made for one repository
//! reaches only that repository.
//!
//! # The decision
//!
//! [`Identity::authorize`] answers, for one repository and one [`Need`]:
//!
//! | Highest grant on the repository | Anonymous caller | Signed-in caller |
//! |---|---|---|
//! | enough for the need | allowed | allowed |
//! | read, but the need is more | `Unauthenticated` (401) | `Forbidden` (403) |
//! | none | `Unauthenticated` (401) | `NotFound` (404) |
//!
//! Anonymous callers get 401 rather than 404, so a Git client asks for
//! credentials and retries; that answer is the same whether the repository
//! exists or not, so it reveals nothing. A signed-in caller with no grant
//! gets 404, never 403, for the same reason.
//!
//! The decision depends only on the repository's name, never on whether it
//! exists: the server decides before it looks the repository up.

use std::collections::BTreeMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use aspen_core::auth::{Credential, Principal};
use aspen_core::error::AuthError;
use aspen_core::id::UnixMicros;
use aspen_core::name::{RefName, RepoName};
use serde::Deserialize;

use crate::grant::{Grant, Grants, Permission};
use crate::jwt::{Clock, JwtVerifier, KeySet, looks_like_jwt, unverified_issuer};
use crate::ssh::{NoSshKeys, SshFingerprint, SshKeyStore};
use crate::token::{
    OIDC_SUBJECT_PREFIX, PresentedToken, TokenStore, is_token_subject, looks_like_token,
};

/// What a request needs from the repository it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// Clone, fetch, `ls-refs`.
    Read,
    /// A push: the receive-pack advertisement and the push itself. Each ref
    /// the push updates is checked again with [`Identity::authorize_ref`].
    Push,
    /// Repository settings.
    Admin,
}

/// How a caller was identified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Via {
    /// No credential.
    Anonymous,
    /// An API token, by its public ID.
    Token(crate::token::TokenId),
    /// An OIDC identity token from the issuer with this name.
    Oidc(String),
    /// A registered SSH key, by its fingerprint.
    SshKey(SshFingerprint),
}

/// A caller and everything it may do, resolved once per request.
#[derive(Debug, Clone)]
pub struct Identity {
    principal: Principal,
    via: Via,
    grants: Grants,
}

impl Identity {
    /// An identity with these grants (for tests and custom front ends).
    pub fn new(principal: Principal, via: Via, grants: Grants) -> Self {
        Self {
            principal,
            via,
            grants,
        }
    }

    /// Who the caller is.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// How the caller was identified.
    pub fn via(&self) -> &Via {
        &self.via
    }

    /// Everything the caller may do.
    pub fn grants(&self) -> &Grants {
        &self.grants
    }

    /// Whether the caller presented no credential.
    pub fn is_anonymous(&self) -> bool {
        self.principal == Principal::Anonymous
    }

    /// The access decision for `need` on `repo` (module docs, "The
    /// decision").
    pub fn authorize(&self, repo: &RepoName, need: Need) -> Result<(), AuthError> {
        let highest = self.grants.highest(repo);
        let required = match need {
            Need::Read => Permission::Read,
            Need::Push => Permission::Write,
            Need::Admin => Permission::Admin,
        };
        match highest {
            Some(p) if p >= required => Ok(()),
            _ if self.is_anonymous() => Err(AuthError::Unauthenticated),
            Some(_) => Err(AuthError::Forbidden),
            None => Err(AuthError::NotFound),
        }
    }

    /// Whether the caller may update `name` in `repo`, for each command of a
    /// push that [`Identity::authorize`] admitted with [`Need::Push`]. A
    /// refusal is [`AuthError::Forbidden`], which the push reports as an
    /// `ng` line for that ref.
    pub fn authorize_ref(&self, repo: &RepoName, name: &RefName) -> Result<(), AuthError> {
        if self.grants.may_write_ref(repo, name) {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }
}

/// One OIDC issuer, as configured.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcIssuerConfig {
    /// Short name, used in principal IDs (`oidc:<name>:<sub>`). Letters,
    /// digits, `-` and `_`.
    pub name: String,
    /// The `iss` value tokens must carry, exactly.
    pub issuer: String,
    /// The audience tokens must be issued for (an `aud` entry).
    pub audience: String,
    /// The issuer's JWKS document.
    pub jwks: serde_json::Value,
    /// Claim rules: each rule whose claims all match adds its grants.
    #[serde(default)]
    pub rules: Vec<ClaimRule>,
}

/// Grants for OIDC tokens whose claims match.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRule {
    /// Claim name to exact value. Every one must match; an empty map is
    /// refused at load time, since it would match every token.
    pub claims: BTreeMap<String, String>,
    /// What a matching token may do.
    pub grants: Vec<Grant>,
}

/// The access policy, as configured.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    /// Grants everyone holds, signed in or not.
    #[serde(default)]
    pub anonymous: Vec<Grant>,
    /// Grants for OIDC principals, by principal ID (`oidc:<name>:<sub>`).
    #[serde(default)]
    pub subjects: BTreeMap<String, Vec<Grant>>,
    /// Accepted OIDC issuers.
    #[serde(default)]
    pub oidc: Vec<OidcIssuerConfig>,
    /// Clock leeway for identity tokens, in seconds (default 60).
    #[serde(default = "default_leeway")]
    pub leeway_seconds: u32,
}

fn default_leeway() -> u32 {
    60
}

/// A policy that does not load.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("auth policy: {0}")]
pub struct PolicyError(String);

#[derive(Debug)]
struct Issuer {
    name: String,
    verifier: RwLock<JwtVerifier>,
    rules: Vec<ClaimRule>,
}

/// The loaded policy plus the token store and the SSH key store: turns
/// credentials into identities.
///
/// `K` is the SSH key store; until one is given
/// ([`AuthService::with_ssh_keys`]) it is [`NoSshKeys`], which refuses
/// every key.
#[derive(Debug)]
pub struct AuthService<T, K = NoSshKeys> {
    anonymous: Grants,
    subjects: BTreeMap<String, Grants>,
    issuers: Vec<Issuer>,
    leeway: i64,
    tokens: T,
    ssh_keys: K,
    jwt_verifications: AtomicU64,
}

impl<T: TokenStore> AuthService<T> {
    /// Loads a policy over a token store, with no SSH keys.
    pub fn new(config: PolicyConfig, tokens: T) -> Result<Self, PolicyError> {
        let mut issuers = Vec::new();
        for i in config.oidc {
            if i.name.is_empty()
                || !i
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(PolicyError(format!(
                    "issuer name `{}` must be letters, digits, `-` and `_`",
                    i.name
                )));
            }
            if issuers.iter().any(|e: &Issuer| e.name == i.name) {
                return Err(PolicyError(format!("issuer name `{}` used twice", i.name)));
            }
            if i.rules.iter().any(|r| r.claims.is_empty()) {
                return Err(PolicyError(format!(
                    "issuer `{}`: a rule with no claims would match every token",
                    i.name
                )));
            }
            let keys = KeySet::from_json(&i.jwks.to_string())
                .map_err(|e| PolicyError(format!("issuer `{}`: {e}", i.name)))?;
            if keys.is_empty() {
                return Err(PolicyError(format!(
                    "issuer `{}`: no usable signing key",
                    i.name
                )));
            }
            issuers.push(Issuer {
                name: i.name,
                verifier: RwLock::new(JwtVerifier::new(i.issuer, i.audience, keys)),
                rules: i.rules,
            });
        }
        Ok(Self {
            anonymous: Grants::new(config.anonymous),
            subjects: config
                .subjects
                .into_iter()
                .map(|(k, v)| (k, Grants::new(v)))
                .collect(),
            issuers,
            leeway: i64::from(config.leeway_seconds),
            tokens,
            ssh_keys: NoSshKeys,
            jwt_verifications: AtomicU64::new(0),
        })
    }
}

impl<T: TokenStore, K: SshKeyStore> AuthService<T, K> {
    /// The same policy and tokens, with `keys` as the SSH key store.
    pub fn with_ssh_keys<K2: SshKeyStore>(self, keys: K2) -> AuthService<T, K2> {
        AuthService {
            anonymous: self.anonymous,
            subjects: self.subjects,
            issuers: self.issuers,
            leeway: self.leeway,
            tokens: self.tokens,
            ssh_keys: keys,
            jwt_verifications: self.jwt_verifications,
        }
    }

    /// How many times an identity token has been verified against an
    /// issuer's keys since this service was built: each costs up to one
    /// signature check per key of that issuer that fits the token.
    pub fn jwt_verifications(&self) -> u64 {
        self.jwt_verifications.load(Ordering::Relaxed)
    }

    /// The SSH key store.
    pub fn ssh_keys(&self) -> &K {
        &self.ssh_keys
    }

    /// The token store.
    pub fn tokens(&self) -> &T {
        &self.tokens
    }

    /// Replaces an issuer's key set, after it rotated its keys. `false` if
    /// there is no issuer of that name.
    pub fn set_issuer_keys(&self, name: &str, keys: KeySet) -> bool {
        match self.issuers.iter().find(|i| i.name == name) {
            Some(i) => {
                i.verifier
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .set_keys(keys);
                true
            }
            None => false,
        }
    }

    /// The identity behind a credential. No credential is the anonymous
    /// identity; a credential that is presented but invalid, expired or
    /// revoked is [`AuthError::Unauthenticated`], never anonymous.
    pub async fn identify(
        &self,
        credential: &Credential,
        now: UnixMicros,
    ) -> Result<Identity, AuthError> {
        let secret = match credential {
            Credential::Anonymous => {
                return Ok(Identity::new(
                    Principal::Anonymous,
                    Via::Anonymous,
                    self.anonymous.clone(),
                ));
            }
            // Git's credential helpers send the token as the password; the
            // user name is ignored (GitHub Actions sends `x-access-token`).
            Credential::Bearer(s) | Credential::Basic { password: s, .. } => s.expose(),
            Credential::SshKey { fingerprint } => {
                return self.identify_ssh_key(fingerprint, now).await;
            }
        };
        if looks_like_token(secret) {
            self.identify_token(secret, now).await
        } else if looks_like_jwt(secret) {
            self.identify_jwt(secret, now)
        } else {
            Err(AuthError::Unauthenticated)
        }
    }

    async fn identify_token(&self, secret: &str, now: UnixMicros) -> Result<Identity, AuthError> {
        let presented = PresentedToken::parse(secret).ok_or(AuthError::Unauthenticated)?;
        let record = self
            .tokens
            .token(&presented.id)
            .await
            .map_err(|_| AuthError::Unavailable)?
            .ok_or(AuthError::Unauthenticated)?;
        // Compare the digest first, in constant time, then liveness: the
        // answer is the same either way.
        // A row written around `issue` with an OIDC-shaped subject is
        // refused too, so a token never shares an OIDC principal's ID.
        if !presented.digest.matches(&record.digest)
            || !record.is_live(now)
            || !is_token_subject(&record.subject)
        {
            return Err(AuthError::Unauthenticated);
        }
        let mut grants = self.anonymous.clone();
        grants.extend(record.grants.as_slice().iter().cloned());
        Ok(Identity::new(
            Principal::Subject { id: record.subject },
            Via::Token(record.id),
            grants,
        ))
    }

    /// A key the SSH transport verified the client holds. Like a token: the
    /// key's own grants plus the anonymous ones, and a revoked, expired or
    /// unknown key is refused, never anonymous.
    async fn identify_ssh_key(
        &self,
        fingerprint: &str,
        now: UnixMicros,
    ) -> Result<Identity, AuthError> {
        let fingerprint = SshFingerprint::parse(fingerprint).ok_or(AuthError::Unauthenticated)?;
        let record = self
            .ssh_keys
            .ssh_key(&fingerprint)
            .await
            .map_err(|_| AuthError::Unavailable)?
            .ok_or(AuthError::Unauthenticated)?;
        // The store is keyed by fingerprint; a row whose key does not hash
        // to it is refused rather than trusted.
        if record.fingerprint() != fingerprint
            || !record.is_live(now)
            || !is_token_subject(&record.subject)
        {
            return Err(AuthError::Unauthenticated);
        }
        let mut grants = self.anonymous.clone();
        grants.extend(record.grants.as_slice().iter().cloned());
        Ok(Identity::new(
            Principal::Subject { id: record.subject },
            Via::SshKey(fingerprint),
            grants,
        ))
    }

    fn identify_jwt(&self, token: &str, now: UnixMicros) -> Result<Identity, AuthError> {
        let clock = Clock {
            now: now.0.div_euclid(1_000_000),
            leeway: self.leeway,
        };
        // Only issuers configured for the `iss` the token names are tried,
        // so a forged token costs one issuer's signature checks, not every
        // issuer's (decisions log, B3 review obligation 1). Reading `iss`
        // before the signature is safe: each verifier checks it again after
        // the signature, and a token verifies under at most the issuer
        // whose key signed it and whose `iss` it names.
        let Some(iss) = unverified_issuer(token) else {
            return Err(AuthError::Unauthenticated);
        };
        for issuer in &self.issuers {
            if issuer
                .verifier
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .issuer()
                != iss
            {
                continue;
            }
            self.jwt_verifications.fetch_add(1, Ordering::Relaxed);
            let verified = issuer
                .verifier
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .verify(token, clock);
            let Ok(claims) = verified else {
                continue;
            };
            let id = format!("{OIDC_SUBJECT_PREFIX}{}:{}", issuer.name, claims.subject);
            let mut grants = self.anonymous.clone();
            for rule in &issuer.rules {
                if rule
                    .claims
                    .iter()
                    .all(|(k, v)| claims.string(k).as_deref() == Some(v.as_str()))
                {
                    grants.extend(rule.grants.iter().cloned());
                }
            }
            if let Some(g) = self.subjects.get(&id) {
                grants.extend(g.as_slice().iter().cloned());
            }
            return Ok(Identity::new(
                Principal::Subject { id },
                Via::Oidc(issuer.name.clone()),
                grants,
            ));
        }
        Err(AuthError::Unauthenticated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(s: &str) -> RepoName {
        RepoName::new(s).unwrap()
    }

    fn signed_in(grants: &[&str]) -> Identity {
        Identity::new(
            Principal::Subject { id: "u".into() },
            Via::Oidc("x".into()),
            Grants::new(grants.iter().map(|g| g.parse().unwrap())),
        )
    }

    fn anonymous(grants: &[&str]) -> Identity {
        Identity::new(
            Principal::Anonymous,
            Via::Anonymous,
            Grants::new(grants.iter().map(|g| g.parse().unwrap())),
        )
    }

    /// The decision table in the module docs, cell by cell.
    #[test]
    fn the_decision_table() {
        let mono = repo("acme/mono");
        use AuthError::*;
        let cases: &[(Identity, Need, Result<(), AuthError>)] = &[
            (anonymous(&[]), Need::Read, Err(Unauthenticated)),
            (anonymous(&[]), Need::Push, Err(Unauthenticated)),
            (anonymous(&["read:acme/mono"]), Need::Read, Ok(())),
            (
                anonymous(&["read:acme/mono"]),
                Need::Push,
                Err(Unauthenticated),
            ),
            (anonymous(&["write:acme/mono"]), Need::Push, Ok(())),
            (signed_in(&[]), Need::Read, Err(NotFound)),
            (signed_in(&[]), Need::Push, Err(NotFound)),
            (signed_in(&["read:other/*"]), Need::Read, Err(NotFound)),
            (signed_in(&["read:acme/mono"]), Need::Read, Ok(())),
            (signed_in(&["read:acme/mono"]), Need::Push, Err(Forbidden)),
            (signed_in(&["write:acme/*"]), Need::Push, Ok(())),
            (signed_in(&["write:acme/*"]), Need::Admin, Err(Forbidden)),
            (signed_in(&["admin:acme/mono"]), Need::Admin, Ok(())),
        ];
        for (identity, need, want) in cases {
            assert_eq!(
                &identity.authorize(&mono, *need),
                want,
                "{:?} {need:?}",
                identity.grants()
            );
        }
    }

    #[test]
    fn ref_checks_follow_the_ref_patterns() {
        let id = signed_in(&["write:acme/mono:refs/heads/agent/*"]);
        let mono = repo("acme/mono");
        assert_eq!(id.authorize(&mono, Need::Push), Ok(()));
        let ok = RefName::new("refs/heads/agent/x").unwrap();
        let no = RefName::new("refs/heads/main").unwrap();
        assert_eq!(id.authorize_ref(&mono, &ok), Ok(()));
        assert_eq!(id.authorize_ref(&mono, &no), Err(AuthError::Forbidden));
    }
}
