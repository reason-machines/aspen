//! Express settings from environment variables, the way every Aspen
//! process is configured (`deploy/README.md`, configuration contract).
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `ASPEN_EXPRESS_KEYS` | (none; secret) | `id:hexsecret[,id:hexsecret...]`, each secret at least 32 bytes; the first key signs, all verify. Required wherever express URLs are signed or served |
//! | `ASPEN_CDN_URL` | unset | Base URL of a CDN whose origin is the express nodes; first tier |
//! | `ASPEN_CDN_PAST_END` | `416` | What the CDN answers for a range at or past the end: `416` (most CDNs, including CloudFront; such URIs go only to git 2.56+) or `206` (an edge that passes the origin's empty `206` through) |
//! | `ASPEN_EXPRESS_URLS` | unset | Comma-separated base URLs of express nodes clients reach directly (an Aspen server's `https://<host>/-/express`, or standalone `aspen express` nodes); the next tier, safe for every client |
//! | `ASPEN_EXPRESS_TTL_SECS` | `600` | Shortest life of a signed URL |
//! | `ASPEN_EXPRESS_WINDOW_SECS` | `300` | Expiries round up to this, so fetches in one window share URLs; TTL + window at most 3600 |
//! | `ASPEN_EXPRESS_CACHE_DIR` | unset (no cache) | Disk cache directory of an express node |
//! | `ASPEN_EXPRESS_CACHE_BYTES` | `10737418240` (10 GiB) | Disk cache budget, packs being filled included |
//! | `ASPEN_EXPRESS_CACHE_FILLS` | `4` | Cache fills that may run at once (at least 1); a miss past it is served from the blob store without filling |
//!
//! An Aspen server signs express URIs only when `ASPEN_CDN_URL` or
//! `ASPEN_EXPRESS_URLS` is set ([`routes`]); an express node serves with
//! [`service`].

use std::path::PathBuf;
use std::time::Duration;

use aspen_core::blob::PastEndRange;

use crate::cache::CacheConfig;
use crate::routes::{ProbeConfig, Routes, Tier};
use crate::serve::ExpressConfig;
use crate::url::{SigningPolicy, UrlConfigError, UrlKeys};

/// Why the express settings were refused. Messages name the variable and
/// never contain secret material.
#[derive(Debug, thiserror::Error)]
pub enum SettingsError {
    /// A required variable is missing.
    #[error("{0} is required")]
    Missing(&'static str),
    /// A variable has a value that cannot be used.
    #[error("{var}: {reason}")]
    Invalid {
        /// The variable.
        var: &'static str,
        /// What is wrong.
        reason: String,
    },
}

fn invalid(var: &'static str, e: impl ToString) -> SettingsError {
    SettingsError::Invalid {
        var,
        reason: e.to_string(),
    }
}

fn keys(get: &impl Fn(&str) -> Option<String>) -> Result<UrlKeys, SettingsError> {
    let v = get("ASPEN_EXPRESS_KEYS").ok_or(SettingsError::Missing("ASPEN_EXPRESS_KEYS"))?;
    UrlKeys::parse(&v).map_err(|e: UrlConfigError| invalid("ASPEN_EXPRESS_KEYS", e))
}

fn secs(
    get: &impl Fn(&str) -> Option<String>,
    var: &'static str,
    default: u64,
) -> Result<u64, SettingsError> {
    get(var).map_or(Ok(default), |v| {
        v.trim().parse().map_err(|e| invalid(var, e))
    })
}

/// The routes an Aspen server signs URIs with, or `None` when neither
/// `ASPEN_CDN_URL` nor `ASPEN_EXPRESS_URLS` is set. `get` reads a
/// variable (`|k| std::env::var(k).ok()`).
pub fn routes(get: impl Fn(&str) -> Option<String>) -> Result<Option<Routes>, SettingsError> {
    let mut tiers = Vec::new();
    if let Some(cdn) = get("ASPEN_CDN_URL").filter(|v| !v.trim().is_empty()) {
        let past_end = match get("ASPEN_CDN_PAST_END").as_deref().map(str::trim) {
            None | Some("416") => PastEndRange::Status416,
            Some("206") => PastEndRange::Empty206,
            Some(other) => {
                return Err(invalid(
                    "ASPEN_CDN_PAST_END",
                    format!("{other:?} is not 416 or 206"),
                ));
            }
        };
        tiers.push(Tier::new(&[cdn.trim()], past_end).map_err(|e| invalid("ASPEN_CDN_URL", e))?);
    }
    if let Some(list) = get("ASPEN_EXPRESS_URLS").filter(|v| !v.trim().is_empty()) {
        let bases: Vec<&str> = list.split(',').map(str::trim).collect();
        tiers.push(
            Tier::new(&bases, PastEndRange::Empty206)
                .map_err(|e| invalid("ASPEN_EXPRESS_URLS", e))?,
        );
    }
    if tiers.is_empty() {
        return Ok(None);
    }
    let policy = SigningPolicy::new(
        Duration::from_secs(secs(&get, "ASPEN_EXPRESS_TTL_SECS", 600)?),
        Duration::from_secs(secs(&get, "ASPEN_EXPRESS_WINDOW_SECS", 300)?),
    )
    .map_err(|e| invalid("ASPEN_EXPRESS_TTL_SECS", e))?;
    Routes::new(tiers, keys(&get)?, policy, ProbeConfig::default())
        .map(Some)
        .map_err(|e| invalid("ASPEN_EXPRESS_URLS", e))
}

/// The keys and service settings of an express node.
pub fn service(
    get: impl Fn(&str) -> Option<String>,
) -> Result<(UrlKeys, ExpressConfig), SettingsError> {
    let keys = keys(&get)?;
    let cache = match get("ASPEN_EXPRESS_CACHE_DIR").filter(|v| !v.trim().is_empty()) {
        None => None,
        Some(dir) => Some(CacheConfig {
            dir: PathBuf::from(dir.trim()),
            max_bytes: secs(&get, "ASPEN_EXPRESS_CACHE_BYTES", 10 << 30)?,
            max_fills: match get("ASPEN_EXPRESS_CACHE_FILLS") {
                None => CacheConfig::DEFAULT_MAX_FILLS,
                Some(v) => match v.trim().parse::<usize>() {
                    Ok(n) if n >= 1 => n,
                    Ok(_) => {
                        return Err(invalid("ASPEN_EXPRESS_CACHE_FILLS", "must be at least 1"));
                    }
                    Err(e) => return Err(invalid("ASPEN_EXPRESS_CACHE_FILLS", e)),
                },
            },
        }),
    };
    Ok((
        keys,
        ExpressConfig {
            cache,
            ..ExpressConfig::default()
        },
    ))
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

    fn key_list() -> String {
        format!("a1:{}", "ab".repeat(32))
    }

    #[test]
    fn no_endpoints_means_no_routes() {
        assert!(
            routes(env(&[("ASPEN_EXPRESS_KEYS", &key_list())]))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cdn_then_express_tiers() {
        let r = routes(env(&[
            ("ASPEN_EXPRESS_KEYS", &key_list()),
            ("ASPEN_CDN_URL", "https://cdn.example.com"),
            (
                "ASPEN_EXPRESS_URLS",
                "https://a.example.com/-/express, https://b.example.com",
            ),
            ("ASPEN_EXPRESS_TTL_SECS", "60"),
            ("ASPEN_EXPRESS_WINDOW_SECS", "60"),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(r.tiers().len(), 2);
        assert_eq!(r.tiers()[0].past_end(), PastEndRange::Status416);
        assert_eq!(r.tiers()[1].past_end(), PastEndRange::Empty206);
        assert_eq!(r.tiers()[1].endpoints().len(), 2);
        assert_eq!(r.policy().max_lifetime(), Duration::from_secs(120));
    }

    #[test]
    fn bad_settings_name_the_variable_and_hide_secrets() {
        let cases: &[(&[(&str, &str)], &str)] = &[
            (
                &[("ASPEN_CDN_URL", "https://cdn")],
                "ASPEN_EXPRESS_KEYS is required",
            ),
            (
                &[
                    ("ASPEN_CDN_URL", "https://cdn"),
                    ("ASPEN_EXPRESS_KEYS", "a:00"),
                ],
                "ASPEN_EXPRESS_KEYS",
            ),
            (
                &[("ASPEN_CDN_URL", "ftp://cdn"), ("ASPEN_EXPRESS_KEYS", "x")],
                "ASPEN_CDN_URL",
            ),
            (
                &[
                    ("ASPEN_CDN_URL", "https://cdn"),
                    ("ASPEN_CDN_PAST_END", "200"),
                ],
                "ASPEN_CDN_PAST_END",
            ),
            (
                &[
                    ("ASPEN_EXPRESS_URLS", "https://a"),
                    ("ASPEN_EXPRESS_TTL_SECS", "3600"),
                ],
                "ASPEN_EXPRESS_TTL_SECS",
            ),
        ];
        for (vars, want) in cases {
            let mut vars = vars.to_vec();
            let k = key_list();
            if !vars.iter().any(|(k, _)| *k == "ASPEN_EXPRESS_KEYS")
                && *want != "ASPEN_EXPRESS_KEYS is required"
            {
                vars.push(("ASPEN_EXPRESS_KEYS", &k));
            }
            let err = routes(env(&vars)).unwrap_err().to_string();
            assert!(err.contains(want), "{err}");
            assert!(!err.contains("abab"), "{err}");
        }
        let (_, cfg) = service(env(&[
            ("ASPEN_EXPRESS_KEYS", &key_list()),
            ("ASPEN_EXPRESS_CACHE_DIR", "/var/cache/aspen"),
        ]))
        .unwrap();
        let cache = cfg.cache.unwrap();
        assert_eq!(
            (cache.max_bytes, cache.max_fills),
            (10 << 30, CacheConfig::DEFAULT_MAX_FILLS)
        );
        for (fills, ok) in [
            ("1", true),
            (" 16 ", true),
            ("0", false),
            ("-1", false),
            ("x", false),
        ] {
            let got = service(env(&[
                ("ASPEN_EXPRESS_KEYS", &key_list()),
                ("ASPEN_EXPRESS_CACHE_DIR", "/var/cache/aspen"),
                ("ASPEN_EXPRESS_CACHE_FILLS", fills),
            ]));
            match got {
                Ok((_, cfg)) => {
                    assert!(ok, "{fills:?} accepted");
                    assert_eq!(cfg.cache.unwrap().max_fills, fills.trim().parse().unwrap());
                }
                Err(e) => {
                    assert!(!ok, "{fills:?} refused: {e}");
                    assert!(e.to_string().contains("ASPEN_EXPRESS_CACHE_FILLS"), "{e}");
                }
            }
        }
    }
}
