//! Mirror settings from environment variables (`docs/MIRROR.md`).
//!
//! | Variable | Meaning |
//! |---|---|
//! | `ASPEN_MIRROR_CREDENTIALS` | Comma-separated credential names to load |
//! | `ASPEN_MIRROR_<NAME>_TOKEN` or `ASPEN_MIRROR_<NAME>_TOKEN_FILE` | A token (personal, project or fine-grained access token) |
//! | `ASPEN_MIRROR_<NAME>_USERNAME` | The HTTP user name the token goes with, if not the host's default |
//! | `ASPEN_MIRROR_<NAME>_GITHUB_APP_ID` | A GitHub App's ID (or client ID) |
//! | `ASPEN_MIRROR_<NAME>_GITHUB_INSTALLATION_ID` | The installation to act as |
//! | `ASPEN_MIRROR_<NAME>_GITHUB_PRIVATE_KEY_FILE` or `..._GITHUB_PRIVATE_KEY` | The app's private key (PEM) |
//! | `ASPEN_MIRROR_<NAME>_GITHUB_API_URL` | The REST API, default `https://api.github.com` |
//! | `ASPEN_MIRROR_WORK_DIR` | Spool for packs to and from upstreams |
//! | `ASPEN_MIRROR_SYNC_SECS` | Seconds between sync passes of each mirror (default 60) |
//! | `ASPEN_MIRROR_MAX_PACK_BYTES` | Largest pack fetched from an upstream (default 8 GiB) |
//!
//! `<NAME>` is the credential name in upper case with `-` as `_`. A
//! credential is either a token or a GitHub App, never both. Every name in
//! `ASPEN_MIRROR_CREDENTIALS` must be complete, so a typo fails at startup
//! rather than at the first sync.

use std::time::Duration;

use crate::credential::{
    CredentialError, CredentialKind, CredentialName, Credentials, GITHUB_API_URL, GitHubApp, Secret,
};
use crate::service::MirrorConfig;

/// Loads the credentials named in `ASPEN_MIRROR_CREDENTIALS`. `get` reads
/// a variable (`std::env::var(..).ok()` in production), `read_file` a file
/// a `..._FILE` variable names.
pub fn credentials(
    get: impl Fn(&str) -> Option<String>,
    read_file: impl Fn(&str) -> std::io::Result<String>,
) -> Result<Credentials, CredentialError> {
    let mut list = Vec::new();
    let names = get("ASPEN_MIRROR_CREDENTIALS").unwrap_or_default();
    for raw in names.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let name = CredentialName::new(raw)?;
        let infix = name.env_infix();
        let var = |suffix: &str| get(&format!("ASPEN_MIRROR_{infix}_{suffix}"));
        let config = |reason: String| CredentialError::Config {
            name: raw.to_owned(),
            reason,
        };
        let from_file = |suffix: &str| -> Result<Option<String>, CredentialError> {
            match var(suffix) {
                None => Ok(None),
                Some(path) => read_file(&path)
                    .map(|s| Some(s.trim().to_owned()))
                    .map_err(|e| config(format!("reading {suffix} {path}: {e}"))),
            }
        };
        let token = match (var("TOKEN"), from_file("TOKEN_FILE")?) {
            (Some(_), Some(_)) => {
                return Err(config("set TOKEN or TOKEN_FILE, not both".into()));
            }
            (t, f) => t.or(f).filter(|t| !t.is_empty()),
        };
        let app_id = var("GITHUB_APP_ID");
        let kind = match (token, app_id) {
            (Some(_), Some(_)) => {
                return Err(config("a token or a GitHub App, not both".into()));
            }
            (None, None) => {
                return Err(config(format!(
                    "set ASPEN_MIRROR_{infix}_TOKEN(_FILE) or ASPEN_MIRROR_{infix}_GITHUB_APP_ID"
                )));
            }
            (Some(token), None) => CredentialKind::Token {
                username: var("USERNAME").filter(|u| !u.is_empty()),
                token: Secret::new(token),
            },
            (None, Some(app_id)) => {
                let installation = var("GITHUB_INSTALLATION_ID")
                    .ok_or_else(|| config("GITHUB_INSTALLATION_ID is required".into()))?
                    .parse::<u64>()
                    .map_err(|_| config("GITHUB_INSTALLATION_ID must be a number".into()))?;
                let pem = match (
                    var("GITHUB_PRIVATE_KEY"),
                    from_file("GITHUB_PRIVATE_KEY_FILE")?,
                ) {
                    (Some(_), Some(_)) => {
                        return Err(config(
                            "set GITHUB_PRIVATE_KEY or GITHUB_PRIVATE_KEY_FILE, not both".into(),
                        ));
                    }
                    (Some(k), None) | (None, Some(k)) => k,
                    (None, None) => {
                        return Err(config("GITHUB_PRIVATE_KEY_FILE is required".into()));
                    }
                };
                let api = var("GITHUB_API_URL").unwrap_or_else(|| GITHUB_API_URL.to_owned());
                CredentialKind::GitHubApp(Box::new(
                    GitHubApp::new(&app_id, installation, &pem, &api).map_err(config)?,
                ))
            }
        };
        list.push((name, kind));
    }
    Ok(Credentials::new(list))
}

/// The service settings, from `ASPEN_MIRROR_*` with defaults; the work
/// directory defaults to `default_work_dir`.
pub fn config(
    get: impl Fn(&str) -> Option<String>,
    default_work_dir: impl Into<std::path::PathBuf>,
) -> Result<MirrorConfig, String> {
    let work = get("ASPEN_MIRROR_WORK_DIR").map_or_else(|| default_work_dir.into(), Into::into);
    let mut c = MirrorConfig::new(work);
    if let Some(s) = get("ASPEN_MIRROR_SYNC_SECS") {
        let secs: u64 = s
            .parse()
            .ok()
            .filter(|n| *n > 0)
            .ok_or("ASPEN_MIRROR_SYNC_SECS must be a positive number of seconds")?;
        c.sync_interval = Duration::from_secs(secs);
    }
    if let Some(s) = get("ASPEN_MIRROR_MAX_PACK_BYTES") {
        c.max_pack_bytes = s
            .parse()
            .ok()
            .filter(|n| *n > 0)
            .ok_or("ASPEN_MIRROR_MAX_PACK_BYTES must be a positive number")?;
    }
    Ok(c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |k| m.get(k).cloned()
    }

    fn no_files(_: &str) -> std::io::Result<String> {
        Err(std::io::Error::other("no files in this test"))
    }

    #[test]
    fn tokens_load_by_name() {
        let c = credentials(
            env(&[
                ("ASPEN_MIRROR_CREDENTIALS", "gh, gitlab-ci"),
                ("ASPEN_MIRROR_GH_TOKEN", "t1"),
                ("ASPEN_MIRROR_GITLAB_CI_TOKEN_FILE", "/run/secrets/gl"),
                ("ASPEN_MIRROR_GITLAB_CI_USERNAME", "bot"),
            ]),
            |p| {
                assert_eq!(p, "/run/secrets/gl");
                Ok("t2\n".into())
            },
        )
        .unwrap();
        let names: Vec<_> = c.names().map(|n| n.to_string()).collect();
        assert_eq!(names, ["gh", "gitlab-ci"]);
        assert!(
            credentials(env(&[]), no_files)
                .unwrap()
                .names()
                .next()
                .is_none()
        );
    }

    #[test]
    fn incomplete_or_ambiguous_credentials_fail_at_startup() {
        let cases: &[&[(&str, &str)]] = &[
            &[("ASPEN_MIRROR_CREDENTIALS", "gh")],
            &[("ASPEN_MIRROR_CREDENTIALS", "Bad Name")],
            &[
                ("ASPEN_MIRROR_CREDENTIALS", "gh"),
                ("ASPEN_MIRROR_GH_TOKEN", "t"),
                ("ASPEN_MIRROR_GH_GITHUB_APP_ID", "1"),
            ],
            &[
                ("ASPEN_MIRROR_CREDENTIALS", "gh"),
                ("ASPEN_MIRROR_GH_GITHUB_APP_ID", "1"),
            ],
            &[
                ("ASPEN_MIRROR_CREDENTIALS", "gh"),
                ("ASPEN_MIRROR_GH_GITHUB_APP_ID", "1"),
                ("ASPEN_MIRROR_GH_GITHUB_INSTALLATION_ID", "x"),
            ],
            &[
                ("ASPEN_MIRROR_CREDENTIALS", "gh"),
                ("ASPEN_MIRROR_GH_GITHUB_APP_ID", "1"),
                ("ASPEN_MIRROR_GH_GITHUB_INSTALLATION_ID", "2"),
                ("ASPEN_MIRROR_GH_GITHUB_PRIVATE_KEY", "not a key"),
            ],
            &[
                ("ASPEN_MIRROR_CREDENTIALS", "gh"),
                ("ASPEN_MIRROR_GH_TOKEN_FILE", "/missing"),
            ],
        ];
        for case in cases {
            assert!(credentials(env(case), no_files).is_err(), "{case:?}");
        }
    }

    #[test]
    fn service_settings() {
        let c = config(env(&[("ASPEN_MIRROR_SYNC_SECS", "5")]), "/var/aspen/mirror").unwrap();
        assert_eq!(c.sync_interval, Duration::from_secs(5));
        assert_eq!(c.work_dir, std::path::PathBuf::from("/var/aspen/mirror"));
        assert!(config(env(&[("ASPEN_MIRROR_SYNC_SECS", "0")]), "/x").is_err());
    }
}
