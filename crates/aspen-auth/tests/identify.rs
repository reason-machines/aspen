//! `AuthService::identify` end to end: credentials in, identities and
//! access decisions out.

mod common;

use aspen_auth::token::{TokenId, TokenRecord, issue};
use aspen_auth::{
    AuthService, Grants, MemoryTokenStore, Need, NewToken, PolicyConfig, TokenStore,
    TokenStoreError, Via,
};
use aspen_core::auth::{Credential, Principal, Secret};
use aspen_core::error::AuthError;
use aspen_core::id::UnixMicros;
use aspen_core::name::{RefName, RepoName};
use common::{AUDIENCE, ISSUER, NOW, Signer, claims};
use serde_json::json;

const T: UnixMicros = UnixMicros(NOW * 1_000_000);

fn repo(s: &str) -> RepoName {
    RepoName::new(s).unwrap()
}

fn basic(token: &str) -> Credential {
    Credential::Basic {
        user: "x-access-token".into(),
        password: Secret::new(token),
    }
}

fn new_token(subject: &str, grants: &[&str]) -> NewToken {
    NewToken {
        subject: subject.into(),
        grants: Grants::new(grants.iter().map(|g| g.parse().unwrap())),
        description: String::new(),
        expires_at: None,
    }
}

fn service(policy: serde_json::Value) -> AuthService<MemoryTokenStore> {
    let policy: PolicyConfig = serde_json::from_value(policy).unwrap();
    AuthService::new(policy, MemoryTokenStore::new()).unwrap()
}

fn mint(svc: &AuthService<MemoryTokenStore>, subject: &str, grants: &[&str]) -> String {
    let issued = issue(new_token(subject, grants), T).unwrap();
    svc.tokens().insert(issued.record);
    issued.token.expose().to_owned()
}

/// Invariant: a token scoped to one repository can read and push that
/// repository and nothing else; to every other repository it is as if the
/// repository did not exist (404, not 403).
#[tokio::test]
async fn a_token_for_one_repository_cannot_reach_another() {
    let svc = service(json!({}));
    let token = mint(&svc, "agent:a", &["write:acme/a"]);
    let id = svc.identify(&basic(&token), T).await.unwrap();
    assert_eq!(
        id.principal(),
        &Principal::Subject {
            id: "agent:a".into()
        }
    );
    assert!(matches!(id.via(), Via::Token(_)));
    for need in [Need::Read, Need::Push] {
        assert_eq!(id.authorize(&repo("acme/a"), need), Ok(()));
        for other in ["acme/b", "acme/a2", "acme/a/sub", "acmea", "other/a"] {
            assert_eq!(
                id.authorize(&repo(other), need),
                Err(AuthError::NotFound),
                "{other}"
            );
        }
    }
    let main = RefName::new("refs/heads/main").unwrap();
    assert!(id.authorize_ref(&repo("acme/a"), &main).is_ok());
    assert_eq!(
        id.authorize_ref(&repo("acme/b"), &main),
        Err(AuthError::Forbidden)
    );
    assert_eq!(
        id.authorize(&repo("acme/a"), Need::Admin),
        Err(AuthError::Forbidden)
    );
}

/// Invariant: no credential gets exactly the anonymous grants; a presented
/// credential that is wrong, revoked or expired is refused with 401 and is
/// never downgraded to anonymous, even where anonymous could read.
#[tokio::test]
async fn bad_credentials_are_refused_not_downgraded() {
    let svc = service(json!({ "anonymous": ["read:demo/*"] }));
    let anon = svc.identify(&Credential::Anonymous, T).await.unwrap();
    assert!(anon.is_anonymous());
    assert_eq!(anon.authorize(&repo("demo/x"), Need::Read), Ok(()));
    assert_eq!(
        anon.authorize(&repo("demo/x"), Need::Push),
        Err(AuthError::Unauthenticated)
    );
    assert_eq!(
        anon.authorize(&repo("acme/a"), Need::Read),
        Err(AuthError::Unauthenticated)
    );

    let good = mint(&svc, "agent:a", &["write:acme/a"]);
    let mut wrong_secret = good.clone();
    let last = wrong_secret.pop().unwrap();
    wrong_secret.push(if last == 'f' { 'e' } else { 'f' });
    // A well-formed token whose ID was never issued.
    let unknown = issue(new_token("x", &[]), T)
        .unwrap()
        .token
        .expose()
        .to_owned();
    let revoked = mint(&svc, "agent:r", &["write:acme/a"]);
    let revoked_id = TokenId::parse_hex(&revoked[6..38]).unwrap();
    assert!(svc.tokens().revoke(&revoked_id, T));
    let mut expiring = issue(new_token("agent:e", &["write:acme/a"]), T).unwrap();
    expiring.record.expires_at = Some(T);
    svc.tokens().insert(expiring.record);
    let expired = expiring.token.expose().to_owned();

    for bad in [
        &wrong_secret,
        &unknown,
        &revoked,
        &expired,
        "aspen_x",
        "nope",
        "a.b.c",
    ] {
        for credential in [basic(bad), Credential::Bearer(Secret::new(bad))] {
            assert_eq!(
                svc.identify(&credential, T).await.unwrap_err(),
                AuthError::Unauthenticated,
                "{bad:.12}"
            );
        }
    }
    let ssh = Credential::SshKey {
        fingerprint: "SHA256:x".into(),
    };
    assert_eq!(
        svc.identify(&ssh, T).await.unwrap_err(),
        AuthError::Unauthenticated
    );

    // The good token still works, and also holds the anonymous grants.
    let id = svc.identify(&basic(&good), T).await.unwrap();
    assert_eq!(id.authorize(&repo("demo/x"), Need::Read), Ok(()));
}

/// Invariant: a token holds its own grants (plus the anonymous ones) and
/// never its subject's grants from other routes.
#[tokio::test]
async fn a_token_does_not_inherit_its_subjects_grants() {
    let subject = "user:alice";
    let svc = service(json!({ "subjects": { subject: ["admin:*"] } }));
    let token = mint(&svc, subject, &["read:acme/mono"]);
    let id = svc.identify(&basic(&token), T).await.unwrap();
    assert_eq!(id.authorize(&repo("acme/mono"), Need::Read), Ok(()));
    assert_eq!(
        id.authorize(&repo("acme/mono"), Need::Push),
        Err(AuthError::Forbidden)
    );
    assert_eq!(
        id.authorize(&repo("acme/other"), Need::Read),
        Err(AuthError::NotFound)
    );
}

/// Invariant: a token never acts as an OIDC principal. `issue` refuses an
/// `oidc:` subject, and a row stored with one around `issue` (a hand-edited
/// database) is refused at authentication rather than taking that
/// principal's grants or rate-limit bucket.
#[tokio::test]
async fn a_token_cannot_act_as_an_oidc_principal() {
    let oidc = "oidc:gh:repo:acme/mono:ref:refs/heads/main";
    let svc = service(json!({ "subjects": { oidc: ["admin:*"] } }));
    assert!(issue(new_token(oidc, &["read:acme/mono"]), T).is_err());
    let mut issued = issue(new_token("agent:a", &["read:acme/mono"]), T).unwrap();
    issued.record.subject = oidc.into();
    svc.tokens().insert(issued.record);
    assert_eq!(
        svc.identify(&basic(issued.token.expose()), T)
            .await
            .unwrap_err(),
        AuthError::Unauthenticated
    );
}

struct DownStore;

impl TokenStore for DownStore {
    async fn token(&self, _: &TokenId) -> Result<Option<TokenRecord>, TokenStoreError> {
        Err(TokenStoreError("connection refused".into()))
    }
}

#[tokio::test]
async fn a_store_outage_is_retryable_not_a_refusal() {
    let svc = AuthService::new(PolicyConfig::default(), DownStore).unwrap();
    let token = issue(new_token("a", &[]), T).unwrap();
    assert_eq!(
        svc.identify(&basic(token.token.expose()), T)
            .await
            .unwrap_err(),
        AuthError::Unavailable
    );
}

fn oidc_policy(signer: &Signer) -> serde_json::Value {
    json!({
        "anonymous": ["read:demo/*"],
        "subjects": { "oidc:gh:repo:acme/tools:ref:refs/heads/main": ["read:acme/tools"] },
        "oidc": [{
            "name": "gh",
            "issuer": ISSUER,
            "audience": AUDIENCE,
            "jwks": { "keys": [signer.jwk(Some("k1"))] },
            "rules": [
                {
                    "claims": { "repository": "acme/mono", "ref": "refs/heads/main" },
                    "grants": ["write:acme/mono"]
                },
                {
                    "claims": { "repository": "acme/mono" },
                    "grants": ["read:acme/mono"]
                }
            ]
        }]
    })
}

#[tokio::test]
async fn oidc_tokens_get_the_grants_of_matching_rules() {
    let signer = Signer::rsa();
    let svc = service(oidc_policy(&signer));
    let main = signer.simple(Some("k1"), &claims());
    let id = svc.identify(&basic(&main), T).await.unwrap();
    assert_eq!(
        id.principal(),
        &Principal::Subject {
            id: "oidc:gh:repo:acme/mono:ref:refs/heads/main".into()
        }
    );
    assert_eq!(id.authorize(&repo("acme/mono"), Need::Push), Ok(()));
    assert_eq!(id.authorize(&repo("demo/x"), Need::Read), Ok(()));
    assert_eq!(
        id.authorize(&repo("acme/other"), Need::Read),
        Err(AuthError::NotFound)
    );

    // Another branch matches only the read rule.
    let mut c = claims();
    c["ref"] = json!("refs/heads/feature");
    c["sub"] = json!("repo:acme/mono:ref:refs/heads/feature");
    let id = svc
        .identify(
            &Credential::Bearer(Secret::new(signer.simple(Some("k1"), &c))),
            T,
        )
        .await
        .unwrap();
    assert_eq!(id.authorize(&repo("acme/mono"), Need::Read), Ok(()));
    assert_eq!(
        id.authorize(&repo("acme/mono"), Need::Push),
        Err(AuthError::Forbidden)
    );

    // Another repository's workflow matches no rule, but its subject has
    // grants of its own.
    let mut c = claims();
    c["repository"] = json!("acme/tools");
    c["sub"] = json!("repo:acme/tools:ref:refs/heads/main");
    let id = svc
        .identify(&basic(&signer.simple(Some("k1"), &c)), T)
        .await
        .unwrap();
    assert_eq!(id.authorize(&repo("acme/tools"), Need::Read), Ok(()));
    assert_eq!(
        id.authorize(&repo("acme/mono"), Need::Read),
        Err(AuthError::NotFound)
    );

    // Expired, or signed by an unpublished key: refused, not anonymous.
    let mut c = claims();
    c["exp"] = json!(NOW - 3600);
    for bad in [
        signer.simple(Some("k1"), &c),
        Signer::ec().simple(None, &claims()),
    ] {
        assert_eq!(
            svc.identify(&basic(&bad), T).await.unwrap_err(),
            AuthError::Unauthenticated
        );
    }
}

/// Invariant: a token signed by issuer A's key but naming issuer B is
/// refused, so one tenant's identity provider cannot mint identities for
/// another's rules.
#[tokio::test]
async fn an_issuer_cannot_speak_for_another() {
    let a = Signer::rsa();
    let b = Signer::ec();
    let mut policy = oidc_policy(&a);
    policy["oidc"].as_array_mut().unwrap().push(json!({
        "name": "corp",
        "issuer": "https://idp.corp.example",
        "audience": AUDIENCE,
        "jwks": { "keys": [b.jwk(None)] },
        "rules": [{ "claims": { "repository": "acme/mono" }, "grants": ["admin:*"] }]
    }));
    let svc = service(policy);
    let mut c = claims();
    c["iss"] = json!("https://idp.corp.example");
    assert_eq!(
        svc.identify(&basic(&a.simple(Some("k1"), &c)), T)
            .await
            .unwrap_err(),
        AuthError::Unauthenticated
    );
    let id = svc.identify(&basic(&b.simple(None, &c)), T).await.unwrap();
    assert!(matches!(id.via(), Via::Oidc(n) if n == "corp"));
}

/// Invariant (B3 review obligation 1): a forged identity token is verified
/// against the keys of the issuers its `iss` names only, so with many
/// issuers configured it still costs one issuer's signature checks; a
/// token naming no configured issuer costs none.
#[tokio::test]
async fn a_forged_token_is_checked_by_the_issuer_it_names_only() {
    let signer = Signer::rsa();
    let mut policy = oidc_policy(&signer);
    for i in 0..8 {
        policy["oidc"].as_array_mut().unwrap().push(json!({
            "name": format!("other{i}"),
            "issuer": format!("https://idp{i}.example"),
            "audience": AUDIENCE,
            "jwks": { "keys": [Signer::ec().jwk(None)] },
        }));
    }
    let svc = service(policy);
    let forger = Signer::ec();
    let mut c = claims();
    c["iss"] = json!("https://idp3.example");
    let before = svc.jwt_verifications();
    assert_eq!(
        svc.identify(&basic(&forger.simple(None, &c)), T)
            .await
            .unwrap_err(),
        AuthError::Unauthenticated
    );
    assert_eq!(svc.jwt_verifications() - before, 1);

    c["iss"] = json!("https://nobody.example");
    let before = svc.jwt_verifications();
    assert_eq!(
        svc.identify(&basic(&forger.simple(None, &c)), T)
            .await
            .unwrap_err(),
        AuthError::Unauthenticated
    );
    assert_eq!(svc.jwt_verifications(), before);

    // A genuine token is still accepted by its own issuer.
    let id = svc
        .identify(&basic(&signer.simple(Some("k1"), &claims())), T)
        .await
        .unwrap();
    assert!(matches!(id.via(), Via::Oidc(n) if n == "gh"));
}

#[test]
fn bad_policies_do_not_load() {
    let signer = Signer::rsa();
    let mut empty_rule = oidc_policy(&signer);
    empty_rule["oidc"][0]["rules"] = json!([{ "claims": {}, "grants": ["admin:*"] }]);
    let mut no_keys = oidc_policy(&signer);
    no_keys["oidc"][0]["jwks"] = json!({ "keys": [] });
    let mut bad_name = oidc_policy(&signer);
    bad_name["oidc"][0]["name"] = json!("g:h");
    let mut twice = oidc_policy(&signer);
    let first = twice["oidc"][0].clone();
    twice["oidc"].as_array_mut().unwrap().push(first);
    for p in [empty_rule, no_keys, bad_name, twice] {
        let p: PolicyConfig = serde_json::from_value(p).unwrap();
        assert!(AuthService::new(p, MemoryTokenStore::new()).is_err());
    }
    // Unknown fields and bad grants are refused when the policy is read.
    assert!(serde_json::from_value::<PolicyConfig>(json!({ "anonymus": [] })).is_err());
    assert!(serde_json::from_value::<PolicyConfig>(json!({ "anonymous": ["own:*"] })).is_err());
}

mod ssh_keys {
    use super::*;
    use aspen_auth::ssh::{SshKeyStoreError, register};
    use aspen_auth::{
        MemorySshKeyStore, NewSshKey, SshFingerprint, SshKeyRecord, SshKeyStore, SshPublicKey,
    };

    /// Two throwaway keys from `ssh-keygen` (public halves only).
    const KEY_A: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINfpeAnPZKwL5SOdrJPD2fsT9+r+8GZFy5xfTsLiuMxp";
    const KEY_B: &str = "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBD82Rmi/l0Mx/EQe1Ch7TElO58zNZ7aO016Uc/7Bur52nFt4GI757L4vghxPCPiuHb/MKAo29rWWSMWmj3z+cq4=";

    fn key(line: &str) -> SshPublicKey {
        SshPublicKey::parse_openssh(line).unwrap()
    }

    fn record(line: &str, subject: &str, grants: &[&str]) -> SshKeyRecord {
        register(
            NewSshKey {
                key: key(line),
                subject: subject.into(),
                grants: Grants::new(grants.iter().map(|g| g.parse().unwrap())),
                description: String::new(),
                expires_at: None,
            },
            UnixMicros(1),
        )
        .unwrap()
    }

    fn ssh(fp: &SshFingerprint) -> Credential {
        Credential::SshKey {
            fingerprint: fp.to_string(),
        }
    }

    fn with_keys(
        policy: serde_json::Value,
    ) -> AuthService<MemoryTokenStore, std::sync::Arc<MemorySshKeyStore>> {
        service(policy).with_ssh_keys(std::sync::Arc::new(MemorySshKeyStore::new()))
    }

    /// Invariant: a key registered for one repository reaches that
    /// repository and, besides, only what anonymous callers may; to every
    /// other repository it is as if the repository did not exist.
    #[tokio::test]
    async fn a_key_for_one_repository_cannot_reach_another() {
        let svc = with_keys(json!({ "anonymous": ["read:demo"] }));
        let r = record(KEY_A, "user:alice", &["write:acme/a:refs/heads/alice/*"]);
        let fp = r.fingerprint();
        svc.ssh_keys().insert(r);
        let id = svc.identify(&ssh(&fp), T).await.unwrap();
        assert_eq!(
            id.principal(),
            &Principal::Subject {
                id: "user:alice".into()
            }
        );
        assert_eq!(id.via(), &Via::SshKey(fp));
        assert_eq!(id.authorize(&repo("acme/a"), Need::Push), Ok(()));
        assert_eq!(id.authorize(&repo("demo"), Need::Read), Ok(()));
        assert_eq!(
            id.authorize(&repo("demo"), Need::Push),
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            id.authorize(&repo("acme/b"), Need::Read),
            Err(AuthError::NotFound)
        );
        let mine = RefName::new("refs/heads/alice/x").unwrap();
        let main = RefName::new("refs/heads/main").unwrap();
        assert_eq!(id.authorize_ref(&repo("acme/a"), &mine), Ok(()));
        assert_eq!(
            id.authorize_ref(&repo("acme/a"), &main),
            Err(AuthError::Forbidden)
        );
    }

    /// Invariant: an SSH key that is unknown, revoked, expired or not a
    /// fingerprint is refused, never treated as anonymous, even when
    /// anonymous callers could read.
    #[tokio::test]
    async fn unusable_keys_are_refused_not_anonymous() {
        let svc = with_keys(json!({ "anonymous": ["read:*"] }));
        let a = record(KEY_A, "user:alice", &["read:*"]);
        let fp_a = a.fingerprint();
        svc.ssh_keys().insert(a);
        let mut b = record(KEY_B, "user:bob", &["read:*"]);
        let fp_b = b.fingerprint();
        b.expires_at = Some(T);
        svc.ssh_keys().insert(b);
        assert!(svc.identify(&ssh(&fp_a), T).await.is_ok());
        assert!(svc.ssh_keys().revoke(&fp_a, T));
        let unknown = SshFingerprint::of_blob(b"not a registered key");
        for c in [
            ssh(&fp_a),
            ssh(&fp_b),
            ssh(&unknown),
            Credential::SshKey {
                fingerprint: "SHA256:".into(),
            },
            Credential::SshKey {
                fingerprint: fp_b.to_string().to_lowercase(),
            },
        ] {
            assert_eq!(
                svc.identify(&c, T).await.unwrap_err(),
                AuthError::Unauthenticated,
                "{c:?}"
            );
        }
    }

    /// Invariant: until a key store is configured every key is refused.
    #[tokio::test]
    async fn a_service_without_a_key_store_refuses_every_key() {
        let svc = service(json!({ "anonymous": ["read:*"] }));
        let fp = key(KEY_A).fingerprint();
        assert_eq!(
            svc.identify(&ssh(&fp), T).await.unwrap_err(),
            AuthError::Unauthenticated
        );
    }

    /// A store that answers with a record for another key, or fails.
    struct Lying(Option<SshKeyRecord>);

    impl SshKeyStore for Lying {
        async fn ssh_key(
            &self,
            _: &SshFingerprint,
        ) -> Result<Option<SshKeyRecord>, SshKeyStoreError> {
            match &self.0 {
                Some(r) => Ok(Some(r.clone())),
                None => Err(SshKeyStoreError("down".into())),
            }
        }
    }

    /// Invariant: a stored record is trusted only for the key it holds,
    /// and a failing store is "retry", not "refused".
    #[tokio::test]
    async fn the_record_must_be_for_the_presented_key() {
        let other = record(KEY_B, "user:bob", &["admin:*"]);
        let svc = service(json!({})).with_ssh_keys(Lying(Some(other)));
        let fp = key(KEY_A).fingerprint();
        assert_eq!(
            svc.identify(&ssh(&fp), T).await.unwrap_err(),
            AuthError::Unauthenticated
        );
        let svc = service(json!({})).with_ssh_keys(Lying(None));
        assert_eq!(
            svc.identify(&ssh(&fp), T).await.unwrap_err(),
            AuthError::Unavailable
        );
    }
}
