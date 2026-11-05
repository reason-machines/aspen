//! Credentials for upstreams: personal or project access tokens, and
//! GitHub App installation tokens minted on demand.
//!
//! A mirror names its credential (`mirrors.credential`); the secret itself
//! never enters the database. The operator configures each credential by
//! name ([`crate::settings::credentials`]), from environment variables or
//! files that a secret store fills (`docs/MIRROR.md`).
//!
//! Git over HTTPS authenticates with `Basic` and the token as the password.
//! The user name depends on the host: GitHub takes `x-access-token` (for
//! both personal tokens and installation tokens), GitLab `oauth2`; any
//! other host gets the configured user name, `git` by default.
//!
//! A credential is only ever sent over HTTPS, or to a loopback address
//! (tests and sidecars); [`Credentials::authorization`] refuses anything
//! else.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ring::rand::SystemRandom;
use ring::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use tokio::sync::Mutex;

use crate::upstream::UpstreamUrl;

/// A secret string: never printed, and compared nowhere.
#[derive(Clone)]
pub struct Secret(Arc<str>);

impl Secret {
    /// Wraps a secret.
    pub fn new(s: impl Into<String>) -> Self {
        Self(Arc::from(s.into()))
    }

    /// The secret, for the one place that sends it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// The name of a configured credential: 1 to 64 characters of `a-z`,
/// `0-9`, `-` and `_`, as stored in `mirrors.credential`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CredentialName(String);

impl CredentialName {
    /// Checks a name.
    pub fn new(name: &str) -> Result<Self, CredentialError> {
        let ok = !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_');
        if ok {
            Ok(Self(name.to_owned()))
        } else {
            Err(CredentialError::BadName(name.to_owned()))
        }
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The infix of this credential's environment variables: upper case,
    /// `-` as `_`.
    pub fn env_infix(&self) -> String {
        self.0.to_ascii_uppercase().replace('-', "_")
    }
}

impl std::fmt::Display for CredentialName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a credential cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CredentialError {
    /// Not a valid credential name.
    #[error("bad credential name {0:?}: use 1 to 64 characters of a-z, 0-9, - and _")]
    BadName(String),
    /// The mirror names a credential the operator did not configure.
    #[error("credential {0:?} is not configured (docs/MIRROR.md, \"Credentials\")")]
    Unknown(String),
    /// The configuration of a credential is incomplete or wrong.
    #[error("credential {name:?}: {reason}")]
    Config {
        /// The credential.
        name: String,
        /// What is wrong.
        reason: String,
    },
    /// The credential would go to an upstream over plain HTTP.
    #[error("refusing to send credential {0:?} over plain HTTP to a host that is not loopback")]
    PlainHttp(String),
    /// GitHub did not issue an installation token.
    #[error("GitHub App credential {name:?}: {reason}")]
    GitHubApp {
        /// The credential.
        name: String,
        /// What went wrong.
        reason: String,
    },
}

/// A GitHub App installation: mints installation tokens (valid one hour)
/// by signing a JWT with the app's private key.
pub struct GitHubApp {
    app_id: String,
    installation_id: u64,
    key: RsaKeyPair,
    api_url: String,
}

impl std::fmt::Debug for GitHubApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubApp")
            .field("app_id", &self.app_id)
            .field("installation_id", &self.installation_id)
            .field("api_url", &self.api_url)
            .finish_non_exhaustive()
    }
}

/// The GitHub REST API of github.com.
pub const GITHUB_API_URL: &str = "https://api.github.com";

impl GitHubApp {
    /// An app from its ID, the installation to act as, its private key in
    /// PEM (PKCS#1 `RSA PRIVATE KEY`, as GitHub issues it, or PKCS#8
    /// `PRIVATE KEY`) and the REST API base URL (`https://api.github.com`,
    /// or `https://<host>/api/v3` for GitHub Enterprise Server).
    pub fn new(
        app_id: &str,
        installation_id: u64,
        pem: &str,
        api_url: &str,
    ) -> Result<Self, String> {
        if app_id.is_empty() || !app_id.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err("the app ID (or client ID) must be letters and digits".into());
        }
        let (label, der) = pem_decode(pem)?;
        let key = match label.as_str() {
            "RSA PRIVATE KEY" => RsaKeyPair::from_der(&der),
            "PRIVATE KEY" => RsaKeyPair::from_pkcs8(&der),
            other => return Err(format!("expected an RSA private key, found {other:?}")),
        }
        .map_err(|e| format!("the private key does not load: {e}"))?;
        let api_url = api_url.trim_end_matches('/').to_owned();
        if !(api_url.starts_with("https://") || is_loopback_url(&api_url)) {
            return Err("the API URL must be https (or loopback, for tests)".into());
        }
        Ok(Self {
            app_id: app_id.to_owned(),
            installation_id,
            key,
            api_url,
        })
    }

    /// The JWT that authenticates as the app, issued at `now` (seconds
    /// since the epoch): backdated 60 s for clock drift and valid for 9
    /// minutes, inside GitHub's 10-minute limit.
    pub fn jwt(&self, now: u64) -> Result<String, String> {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"RS256","typ":"JWT"}"#);
        let claims = serde_json::json!({
            "iat": now.saturating_sub(60),
            "exp": now + 540,
            "iss": self.app_id,
        });
        let claims = URL_SAFE_NO_PAD.encode(claims.to_string());
        let input = format!("{header}.{claims}");
        let mut sig = vec![0; self.key.public().modulus_len()];
        self.key
            .sign(
                &RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                input.as_bytes(),
                &mut sig,
            )
            .map_err(|_| "signing the JWT failed".to_owned())?;
        Ok(format!("{input}.{}", URL_SAFE_NO_PAD.encode(sig)))
    }

    /// The app's public key, DER `RSAPublicKey` (for tests that verify
    /// the JWT).
    pub fn public_key_der(&self) -> Vec<u8> {
        use ring::signature::KeyPair as _;
        self.key.public_key().as_ref().to_vec()
    }

    /// Asks GitHub for an installation token. Returns it with its expiry
    /// (seconds since the epoch).
    async fn mint(&self, http: &reqwest::Client) -> Result<(Secret, u64), String> {
        let jwt = self.jwt(unix_now())?;
        let url = format!(
            "{}/app/installations/{}/access_tokens",
            self.api_url, self.installation_id
        );
        let mut auth = reqwest::header::HeaderValue::from_str(&format!("Bearer {jwt}"))
            .map_err(|e| e.to_string())?;
        auth.set_sensitive(true);
        let resp = http
            .post(url)
            .header(reqwest::header::AUTHORIZATION, auth)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await
            .map_err(|e| format!("token request failed: {e}"))?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| format!("reading the token response: {e}"))?;
        if !status.is_success() {
            let message = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v.get("message").and_then(|m| m.as_str().map(str::to_owned)))
                .unwrap_or_default();
            return Err(format!("GitHub answered {status}: {message}"));
        }
        let v: serde_json::Value =
            serde_json::from_slice(&body).map_err(|e| format!("bad token response: {e}"))?;
        let token = v
            .get("token")
            .and_then(|t| t.as_str())
            .ok_or("the token response has no token")?;
        let expires = v
            .get("expires_at")
            .and_then(|t| t.as_str())
            .and_then(parse_rfc3339_utc)
            .ok_or("the token response has no valid expires_at")?;
        Ok((Secret::new(token), expires))
    }
}

/// How one configured credential authenticates.
#[derive(Debug)]
pub enum CredentialKind {
    /// A token used as the HTTP password. `username` overrides the host's
    /// default user name.
    Token {
        /// User name override.
        username: Option<String>,
        /// The token.
        token: Secret,
    },
    /// A GitHub App installation.
    GitHubApp(Box<GitHubApp>),
}

#[derive(Debug)]
struct Entry {
    kind: CredentialKind,
    /// A minted installation token and its expiry (GitHub App only).
    cached: Mutex<Option<(Secret, u64)>>,
}

/// Every configured credential, by name. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct Credentials {
    by_name: Arc<BTreeMap<CredentialName, Entry>>,
}

/// An installation token is renewed when it has less than this left.
const RENEW_BEFORE: Duration = Duration::from_secs(5 * 60);

impl Credentials {
    /// Credentials from a list of named kinds.
    pub fn new(list: impl IntoIterator<Item = (CredentialName, CredentialKind)>) -> Self {
        Self {
            by_name: Arc::new(
                list.into_iter()
                    .map(|(n, kind)| {
                        (
                            n,
                            Entry {
                                kind,
                                cached: Mutex::new(None),
                            },
                        )
                    })
                    .collect(),
            ),
        }
    }

    /// Whether `name` is configured.
    pub fn contains(&self, name: &CredentialName) -> bool {
        self.by_name.contains_key(name)
    }

    /// The configured names.
    pub fn names(&self) -> impl Iterator<Item = &CredentialName> {
        self.by_name.keys()
    }

    /// The `Authorization` header value for `url` with credential `name`
    /// (`None`: anonymous), minting a GitHub App installation token if the
    /// cached one is missing or about to expire.
    pub async fn authorization(
        &self,
        http: &reqwest::Client,
        name: Option<&CredentialName>,
        url: &UpstreamUrl,
    ) -> Result<Option<Secret>, CredentialError> {
        let Some(name) = name else { return Ok(None) };
        let entry = self
            .by_name
            .get(name)
            .ok_or_else(|| CredentialError::Unknown(name.to_string()))?;
        if !url.is_https() && !url.is_loopback() {
            return Err(CredentialError::PlainHttp(name.to_string()));
        }
        let (user, token) = match &entry.kind {
            CredentialKind::Token { username, token } => (
                username
                    .clone()
                    .unwrap_or_else(|| url.default_user().to_owned()),
                token.clone(),
            ),
            CredentialKind::GitHubApp(app) => {
                let mut cached = entry.cached.lock().await;
                let fresh = cached
                    .as_ref()
                    .filter(|(_, exp)| unix_now() + RENEW_BEFORE.as_secs() < *exp)
                    .map(|(t, _)| t.clone());
                let token = match fresh {
                    Some(t) => t,
                    None => {
                        let (t, exp) =
                            app.mint(http)
                                .await
                                .map_err(|reason| CredentialError::GitHubApp {
                                    name: name.to_string(),
                                    reason,
                                })?;
                        *cached = Some((t.clone(), exp));
                        t
                    }
                };
                ("x-access-token".to_owned(), token)
            }
        };
        let basic = STANDARD.encode(format!("{user}:{}", token.expose()));
        Ok(Some(Secret::new(format!("Basic {basic}"))))
    }
}

pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(crate) fn is_loopback_url(url: &str) -> bool {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host().map(|h| h.to_owned()))
        .is_some_and(|h| match h {
            url::Host::Domain(d) => d == "localhost",
            url::Host::Ipv4(a) => a.is_loopback(),
            url::Host::Ipv6(a) => a.is_loopback(),
        })
}

/// Decodes one PEM block: its label and DER bytes.
fn pem_decode(pem: &str) -> Result<(String, Vec<u8>), String> {
    let pem = pem.trim();
    let begin = pem
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("-----BEGIN "))
        .and_then(|l| l.strip_suffix("-----"))
        .ok_or("not a PEM block")?
        .to_owned();
    let body: String = pem
        .lines()
        .skip(1)
        .take_while(|l| !l.starts_with("-----END "))
        .collect();
    let end = format!("-----END {begin}-----");
    if !pem.lines().any(|l| l.trim() == end) {
        return Err("the PEM block has no matching END line".into());
    }
    let der = STANDARD
        .decode(body.trim())
        .map_err(|e| format!("the PEM body is not base64: {e}"))?;
    Ok((begin, der))
}

/// Parses `YYYY-MM-DDTHH:MM:SSZ` (what GitHub sends) as seconds since the
/// epoch; fractional seconds are allowed and dropped.
pub(crate) fn parse_rfc3339_utc(s: &str) -> Option<u64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.split('-').map(|p| p.parse::<i64>().ok());
    let (y, m, day) = (d.next()??, d.next()??, d.next()??);
    let time = time.split('.').next()?;
    let mut t = time.split(':').map(|p| p.parse::<i64>().ok());
    let (hh, mm, ss) = (t.next()??, t.next()??, t.next()??);
    if !(1..=12).contains(&m) || !(1..=31).contains(&day) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    // Days from civil (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    u64::try_from(days * 86_400 + hh * 3600 + mm * 60 + ss).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_checked() {
        assert!(CredentialName::new("github-acme_1").is_ok());
        for bad in ["", "GitHub", "a b", "x/y", &"a".repeat(65)] {
            assert!(CredentialName::new(bad).is_err(), "{bad:?}");
        }
        assert_eq!(
            CredentialName::new("gh-acme").unwrap().env_infix(),
            "GH_ACME"
        );
    }

    #[test]
    fn rfc3339_times_parse() {
        assert_eq!(parse_rfc3339_utc("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_utc("2016-07-11T22:14:10Z"),
            Some(1_468_275_250)
        );
        assert_eq!(
            parse_rfc3339_utc("2026-11-03T12:00:00.123Z"),
            Some(1_793_707_200)
        );
        assert_eq!(parse_rfc3339_utc("2026-13-03T12:00:00Z"), None);
        assert_eq!(parse_rfc3339_utc("2026-11-03 12:00:00"), None);
    }

    #[test]
    fn secrets_do_not_print() {
        let s = Secret::new("hunter2");
        assert!(!format!("{s:?}").contains("hunter2"));
        let k = CredentialKind::Token {
            username: None,
            token: s,
        };
        assert!(!format!("{k:?}").contains("hunter2"));
    }

    /// A 2048-bit RSA key from the system's `openssl`, in PKCS#1
    /// (`-traditional`, as GitHub issues app keys) or PKCS#8 PEM.
    fn rsa_pem(pkcs1: bool) -> String {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("k.pem");
        let mut cmd = std::process::Command::new("openssl");
        if pkcs1 {
            cmd.args(["genrsa", "-traditional", "-out"])
                .arg(&path)
                .arg("2048");
        } else {
            cmd.args([
                "genpkey",
                "-algorithm",
                "RSA",
                "-pkeyopt",
                "rsa_keygen_bits:2048",
                "-out",
            ])
            .arg(&path);
        }
        let out = cmd.output().expect("openssl is installed");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::fs::read_to_string(path).unwrap()
    }

    #[test]
    fn app_jwts_are_rs256_signed_by_the_app_key_with_githubs_time_window() {
        for pkcs1 in [true, false] {
            let pem = rsa_pem(pkcs1);
            assert!(pem.contains(if pkcs1 {
                "RSA PRIVATE KEY"
            } else {
                "BEGIN PRIVATE KEY"
            }));
            let app = GitHubApp::new("Iv23abc", 7, &pem, GITHUB_API_URL).unwrap();
            let jwt = app.jwt(1_800_000_000).unwrap();
            let parts: Vec<&str> = jwt.split('.').collect();
            assert_eq!(parts.len(), 3);
            let header: serde_json::Value =
                serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
            assert_eq!(header["alg"], "RS256");
            let claims: serde_json::Value =
                serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
            assert_eq!(claims["iss"], "Iv23abc");
            assert_eq!(claims["iat"], 1_800_000_000 - 60);
            assert_eq!(claims["exp"], 1_800_000_000 + 540);
            let signed = format!("{}.{}", parts[0], parts[1]);
            let sig = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
            let public = app.public_key_der();
            ring::signature::UnparsedPublicKey::new(
                &ring::signature::RSA_PKCS1_2048_8192_SHA256,
                &public,
            )
            .verify(signed.as_bytes(), &sig)
            .unwrap();
            // A different key does not verify it.
            let other = GitHubApp::new("Iv23abc", 7, &rsa_pem(true), GITHUB_API_URL).unwrap();
            assert!(
                ring::signature::UnparsedPublicKey::new(
                    &ring::signature::RSA_PKCS1_2048_8192_SHA256,
                    other.public_key_der(),
                )
                .verify(signed.as_bytes(), &sig)
                .is_err()
            );
        }
        assert!(GitHubApp::new("1", 7, "not a key", GITHUB_API_URL).is_err());
        assert!(GitHubApp::new("1", 7, &rsa_pem(true), "http://api.example.com").is_err());
        assert!(GitHubApp::new("1/2", 7, &rsa_pem(true), GITHUB_API_URL).is_err());
    }

    #[tokio::test]
    async fn tokens_use_the_hosts_user_name_and_never_plain_http() {
        let http = crate::upstream::http_client(Default::default()).unwrap();
        let name = CredentialName::new("gh").unwrap();
        let creds = Credentials::new([(
            name.clone(),
            CredentialKind::Token {
                username: None,
                token: Secret::new("tok"),
            },
        )]);
        let basic = |u: &str| format!("Basic {}", STANDARD.encode(u));
        let gh = UpstreamUrl::parse("github.com/acme/mono").unwrap();
        let got = creds.authorization(&http, Some(&name), &gh).await.unwrap();
        assert_eq!(got.unwrap().expose(), basic("x-access-token:tok"));
        let gl = UpstreamUrl::parse("https://gitlab.com/acme/mono.git").unwrap();
        let got = creds.authorization(&http, Some(&name), &gl).await.unwrap();
        assert_eq!(got.unwrap().expose(), basic("oauth2:tok"));
        let local = UpstreamUrl::parse("http://127.0.0.1:9/acme.git").unwrap();
        assert!(
            creds
                .authorization(&http, Some(&name), &local)
                .await
                .is_ok()
        );
        let plain = UpstreamUrl::parse("http://git.example.com/acme.git").unwrap();
        assert_eq!(
            creds.authorization(&http, Some(&name), &plain).await.err(),
            Some(CredentialError::PlainHttp("gh".into()))
        );
        let other = CredentialName::new("nope").unwrap();
        assert_eq!(
            creds.authorization(&http, Some(&other), &gh).await.err(),
            Some(CredentialError::Unknown("nope".into()))
        );
        assert!(
            creds
                .authorization(&http, None, &plain)
                .await
                .unwrap()
                .is_none()
        );
    }
}
