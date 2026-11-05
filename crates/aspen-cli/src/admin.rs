//! Operator commands against the metadata database: `aspen migrate`,
//! `aspen repo create`, `aspen token create|list|revoke` and
//! `aspen key add|list|revoke` (SSH keys, node C3).
//!
//! They read `DATABASE_URL` (plus `PGPASSWORD`) like `aspen serve`, and
//! apply migrations first, so they work on a fresh database before the
//! server ever ran. A token's text is printed once, on stdout, and never
//! stored: only the digest of its secret is.

use aspen_auth::ssh::{NewSshKey, SshFingerprint, SshPublicKey};
use aspen_auth::{Grant, Grants, NewToken, TokenId};
use aspen_core::id::UnixMicros;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::{ForcePolicy, RepoSettings};
use aspen_meta::SqlStore;
use aspen_server::access::tokens::{SqlTokens, from_row};
use aspen_server::ssh::keys::{self, SqlSshKeys};

use crate::config::check_database_url;
use crate::serve::{BoxError, create_repo_if_missing};

/// Most tokens `aspen token list` prints.
const LIST_LIMIT: u32 = 10_000;

fn runtime() -> Result<tokio::runtime::Runtime, BoxError> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

async fn connect(database_url: &str) -> Result<SqlStore, BoxError> {
    check_database_url(database_url)?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let meta = SqlStore::connect(database_url)
        .await
        .map_err(|e| format!("connecting to DATABASE_URL: {e}"))?;
    meta.migrate()
        .await
        .map_err(|e| format!("applying migrations: {e}"))?;
    Ok(meta)
}

/// `aspen migrate`.
pub(crate) fn migrate(database_url: &str) -> Result<(), BoxError> {
    runtime()?.block_on(async {
        connect(database_url).await?;
        eprintln!("aspen migrate: the database schema is up to date");
        Ok(())
    })
}

/// Parses a force policy name (`none`, `branches`, `all`).
pub(crate) fn parse_force(s: &str) -> Result<ForcePolicy, String> {
    ForcePolicy::ALL
        .into_iter()
        .find(|p| p.as_str() == s)
        .ok_or_else(|| format!("{s:?} is not none, branches or all"))
}

/// `aspen repo create`. Fails if the name is taken.
pub(crate) fn repo_create(
    database_url: &str,
    name: &str,
    trunk: &str,
    allow_force: ForcePolicy,
) -> Result<(), BoxError> {
    let name = RepoName::new(name).map_err(|e| format!("repository name {name:?}: {e}"))?;
    let trunk = RefName::new(trunk).map_err(|e| format!("trunk {trunk:?}: {e}"))?;
    let settings = RepoSettings::builder(
        trunk,
        RepoSettings::new(RefName::new("refs/heads/main")?)?
            .writable()
            .to_vec(),
    )
    .allow_force(allow_force)
    .build()?;
    runtime()?.block_on(async {
        let meta = connect(database_url).await?;
        if !create_repo_if_missing(&meta, &name, &settings).await? {
            return Err(format!("repository {name} already exists").into());
        }
        eprintln!(
            "aspen repo create: created {name} (trunk {}, force policy {})",
            settings.trunk(),
            allow_force.as_str()
        );
        Ok(())
    })
}

/// `aspen token create`: prints the token on stdout, once.
pub(crate) fn token_create(
    database_url: &str,
    subject: String,
    grants: Vec<Grant>,
    description: String,
    expires_in_days: Option<u32>,
) -> Result<(), BoxError> {
    if grants.is_empty() {
        return Err("give the token at least one --grant".into());
    }
    let now = UnixMicros::now();
    let expires_at = expires_in_days.map(|d| UnixMicros(now.0 + i64::from(d) * 86_400_000_000));
    runtime()?.block_on(async {
        let meta = connect(database_url).await?;
        let issued = SqlTokens(meta)
            .create(
                NewToken {
                    subject,
                    grants: Grants::new(grants),
                    description,
                    expires_at,
                },
                now,
            )
            .await?;
        eprintln!(
            "aspen token create: token {} for {} ({}). It is shown once; store it now.",
            issued.record.id,
            issued.record.subject,
            grant_list(&issued.record.grants),
        );
        println!("{}", issued.token.expose());
        Ok(())
    })
}

/// `aspen token list`: one line per token, never the secret.
pub(crate) fn token_list(database_url: &str) -> Result<(), BoxError> {
    runtime()?.block_on(async {
        let meta = connect(database_url).await?;
        let now = UnixMicros::now();
        println!("id\tsubject\tgrants\tstate\tdescription");
        for row in meta.tokens(LIST_LIMIT).await? {
            let record = from_row(row).map_err(|e| format!("stored token: {e}"))?;
            let state = if record.revoked_at.is_some() {
                "revoked"
            } else if record.is_live(now) {
                "live"
            } else {
                "expired"
            };
            println!(
                "{}\t{}\t{}\t{state}\t{}",
                record.id,
                record.subject,
                grant_list(&record.grants),
                record.description
            );
        }
        Ok(())
    })
}

/// `aspen token revoke`.
pub(crate) fn token_revoke(database_url: &str, id: &str) -> Result<(), BoxError> {
    let id = TokenId::parse_hex(id).ok_or("a token ID is 32 hexadecimal digits")?;
    runtime()?.block_on(async {
        let meta = connect(database_url).await?;
        if !SqlTokens(meta).revoke(&id, UnixMicros::now()).await? {
            return Err(format!("no token {id}").into());
        }
        eprintln!("aspen token revoke: revoked {id}");
        Ok(())
    })
}

/// `aspen key add`: `key` is an OpenSSH public key line, or the path of a
/// `.pub` file holding one. Prints the fingerprint.
pub(crate) fn key_add(
    database_url: &str,
    key: &str,
    subject: String,
    grants: Vec<Grant>,
    description: String,
    expires_in_days: Option<u32>,
) -> Result<(), BoxError> {
    let line = if std::path::Path::new(key).is_file() {
        std::fs::read_to_string(key).map_err(|e| format!("{key}: {e}"))?
    } else {
        key.to_owned()
    };
    let key = SshPublicKey::parse_openssh(line.trim())
        .map_err(|e| format!("not an SSH public key line or .pub file: {e}"))?;
    let now = UnixMicros::now();
    let expires_at = expires_in_days.map(|d| UnixMicros(now.0 + i64::from(d) * 86_400_000_000));
    runtime()?.block_on(async {
        let meta = connect(database_url).await?;
        let record = SqlSshKeys(meta)
            .register(
                NewSshKey {
                    key,
                    subject,
                    grants: Grants::new(grants),
                    description,
                    expires_at,
                },
                now,
            )
            .await?;
        eprintln!(
            "aspen key add: registered for {} ({})",
            record.subject,
            grant_list(&record.grants)
        );
        println!("{}", record.fingerprint());
        Ok(())
    })
}

/// `aspen key list`.
pub(crate) fn key_list(database_url: &str) -> Result<(), BoxError> {
    runtime()?.block_on(async {
        let meta = connect(database_url).await?;
        let now = UnixMicros::now();
        println!("fingerprint\tsubject\tgrants\tstate\tdescription");
        for row in meta.ssh_keys(LIST_LIMIT).await? {
            let record = keys::from_row(row).map_err(|e| format!("stored key: {e}"))?;
            let state = if record.revoked_at.is_some() {
                "revoked"
            } else if record.is_live(now) {
                "live"
            } else {
                "expired"
            };
            println!(
                "{}\t{}\t{}\t{state}\t{}",
                record.fingerprint(),
                record.subject,
                grant_list(&record.grants),
                record.description
            );
        }
        Ok(())
    })
}

/// `aspen key revoke`.
pub(crate) fn key_revoke(database_url: &str, fingerprint: &str) -> Result<(), BoxError> {
    let fp = SshFingerprint::parse(fingerprint)
        .ok_or("a key fingerprint looks like SHA256:<43 base64 characters>")?;
    runtime()?.block_on(async {
        let meta = connect(database_url).await?;
        if !SqlSshKeys(meta).revoke(&fp, UnixMicros::now()).await? {
            return Err(format!("no key {fp}").into());
        }
        eprintln!("aspen key revoke: revoked {fp}");
        Ok(())
    })
}

fn grant_list(grants: &Grants) -> String {
    grants
        .as_slice()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn force_policies_parse_by_their_stored_names() {
        for p in ForcePolicy::ALL {
            assert_eq!(parse_force(p.as_str()), Ok(p));
        }
        assert!(parse_force("yes").is_err());
    }
}
