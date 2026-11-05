//! `aspen serve` settings, read from the environment (the configuration
//! contract in `deploy/README.md`).
//!
//! Parsing is a pure function of a variable lookup ([`ServeConfig::from_env`]),
//! so every rule here is unit-tested without touching the process
//! environment. Messages name the variable and never echo a secret.

use std::net::SocketAddr;
use std::path::PathBuf;

use aspen_auth::{LimitsConfig, PolicyConfig};
use aspen_server::access::ClientAddr;
use aspen_server::admission::InstanceLimits;

/// Default address for Git over HTTP.
pub(crate) const DEFAULT_LISTEN: &str = "0.0.0.0:7070";

/// Connections in the metadata pool, by default. Every request waits its
/// turn for one in arrival order, so a busy pool slows requests down but
/// does not fail them (issue #150); more connections help only while the
/// database has CPU to spare.
pub(crate) const DEFAULT_DB_MAX_CONNECTIONS: u32 = 10;
/// How long a request waiting for a metadata connection may go without the
/// pool making progress (no connection returned) before it fails, by
/// default.
pub(crate) const DEFAULT_DB_ACQUIRE_TIMEOUT_SECS: u64 = 30;

/// Largest object Aspen inflates, by default: Git's own
/// `core.bigFileThreshold` default.
pub(crate) const DEFAULT_MAX_OBJECT_BYTES: u64 = 512 << 20;
/// Most one push may inflate to, by default.
pub(crate) const DEFAULT_MAX_INFLATED_BYTES: u64 = 8 << 30;
/// Largest pack one push may send, by default.
pub(crate) const DEFAULT_MAX_PUSH_BYTES: u64 = 8 << 30;
/// Most objects one push may carry, by default.
pub(crate) const DEFAULT_MAX_PUSH_OBJECTS: u64 = 10_000_000;
/// Memory all concurrent pushes may hold for prefetched delta bases.
pub(crate) const DEFAULT_PREFETCH_MEMORY_BYTES: u64 = 4 << 30;
/// Shortest secret accepted for local-store URL signing, in bytes.
pub(crate) const MIN_URL_SECRET_BYTES: usize = 32;

/// A setting that cannot be used. The message names the variable.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigError {
    /// A required variable is unset or empty.
    #[error("{0} is required")]
    Missing(&'static str),
    /// A variable's value is unusable.
    #[error("{var}: {reason}")]
    Invalid {
        /// The variable.
        var: &'static str,
        /// What is wrong (never the secret itself).
        reason: String,
    },
    /// A mirror-mode setting is unusable; the message names the variable.
    #[error("{0}")]
    Mirror(String),
}

fn invalid(var: &'static str, reason: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        var,
        reason: reason.into(),
    }
}

/// Three-way switch: `auto` decides from the rest of the configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Switch {
    Auto,
    On,
    Off,
}

/// The key that signs `/-/blobs` URLs. Its `Debug` shows nothing of it, so
/// a `ServeConfig` logged whole cannot leak a key that forges download
/// URLs.
#[derive(Clone)]
pub(crate) struct UrlSecret(pub(crate) Vec<u8>);

impl std::fmt::Debug for UrlSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UrlSecret(..)")
    }
}

/// Everything `aspen serve` needs, parsed and checked.
#[derive(Debug)]
pub(crate) struct ServeConfig {
    /// `ASPEN_LISTEN`.
    pub(crate) listen: SocketAddr,
    /// `ASPEN_PUBLIC_URL`, without a trailing `/`.
    pub(crate) public_url: Option<String>,
    /// `DATABASE_URL` (never carries a password).
    pub(crate) database_url: String,
    /// `ASPEN_DB_MAX_CONNECTIONS` and `ASPEN_DB_ACQUIRE_TIMEOUT_SECS`: the
    /// metadata pool's size and how long a request waiting for one of its
    /// connections may go without the pool making progress (issue #150).
    pub(crate) store: aspen_meta::StoreOptions,
    /// `ASPEN_BLOB_URL`.
    pub(crate) blob_url: String,
    /// `ASPEN_BLOB_URL_SECRET`: signs `/-/blobs` URLs for a `file://` store.
    pub(crate) blob_url_secret: Option<UrlSecret>,
    /// `ASPEN_POLICY` (inline JSON) or `ASPEN_POLICY_FILE`, parsed.
    /// Default: nobody may do anything without a token.
    pub(crate) policy: PolicyConfig,
    /// `ASPEN_LIMITS` (inline JSON) or `ASPEN_LIMITS_FILE`, parsed.
    /// Default: no request rates or quotas (only failed authentications are
    /// limited); `aspen serve` warns at startup about missing rate limits
    /// ([`unlimited_rates_warning`]), and every deploy target sets them.
    pub(crate) limits: LimitsConfig,
    /// `ASPEN_TRUSTED_PROXY_HOPS` and `ASPEN_TRUSTED_PROXIES`.
    pub(crate) client_addr: ClientAddr,
    /// `ASPEN_MAX_CONCURRENT_FETCHES`, `ASPEN_MAX_CONCURRENT_PUSHES`,
    /// `ASPEN_MAX_CONCURRENT_STITCHES` and `ASPEN_ADMISSION_QUEUE`: the work
    /// this instance runs at once (issue #118).
    pub(crate) instance: InstanceLimits,
    /// `ASPEN_SPOOL_DIR`.
    pub(crate) spool_dir: PathBuf,
    /// `ASPEN_MAX_OBJECT_BYTES`.
    pub(crate) max_object_bytes: u64,
    /// `ASPEN_MAX_INFLATED_BYTES`.
    pub(crate) max_inflated_bytes: u64,
    /// `ASPEN_MAX_PUSH_BYTES`.
    pub(crate) max_push_bytes: u64,
    /// `ASPEN_MAX_PUSH_OBJECTS`.
    pub(crate) max_push_objects: u32,
    /// `ASPEN_PREFETCH_MEMORY_BYTES`.
    pub(crate) prefetch_memory_bytes: u64,
    /// `ASPEN_PACKFILE_URIS`.
    pub(crate) packfile_uris: Switch,
    /// `ASPEN_EXPRESS_SERVE`: whether this server also serves express URLs
    /// at `/-/express`.
    pub(crate) express_serve: bool,
    /// `ASPEN_MIGRATE`: apply schema migrations at startup.
    pub(crate) migrate: bool,
    /// `ASPEN_SHALLOW_PACKS`: build and serve exact shallow packs (node
    /// C8) instead of the full closure for `--depth` fetches.
    pub(crate) shallow_packs: bool,
    /// `ASPEN_SHALLOW_MAX_REPO_BYTES`: the most bytes one repository's
    /// exact shallow packs may take; the least recently used are evicted
    /// to stay under it (issue #108).
    pub(crate) shallow_max_repo_bytes: u64,
    /// `ASPEN_SHALLOW_UNUSED_SECS`: how long an exact shallow pack no fetch
    /// uses is kept, current version or not (issue #108); at least
    /// [`aspen_compact::MIN_SHALLOW_UNUSED`].
    pub(crate) shallow_unused: std::time::Duration,
    /// `ASPEN_METRICS_LISTEN`: the private listener for `/-/metrics`
    /// (C5), or `None` (`off`).
    pub(crate) metrics_listen: Option<SocketAddr>,
    /// Also serve `/-/metrics` on the Git listener (`aspen dev` only; the
    /// Git listener is public everywhere else).
    pub(crate) metrics_on_git_listener: bool,
    /// Run the background worker (compaction, commit indexing, garbage
    /// collection). Always on for `aspen serve`; `aspen dev
    /// --no-background-worker` turns it off for benchmarks (bench/README.md).
    pub(crate) background_worker: bool,
    /// Git over SSH, when `ASPEN_SSH_LISTEN` is set.
    pub(crate) ssh: Option<SshSettings>,
    /// The credentials named in `ASPEN_MIRROR_CREDENTIALS` (node C4).
    pub(crate) mirror_credentials: aspen_mirror::Credentials,
    /// The other `ASPEN_MIRROR_*` settings; the work directory defaults to
    /// `<ASPEN_SPOOL_DIR>/mirror`, on the same real disk as the spool.
    pub(crate) mirror: aspen_mirror::MirrorConfig,
}

/// Default private listener for `/-/metrics`. Containers override it with
/// `0.0.0.0:9464` (the image sets it), and keep the port off any public
/// load balancer.
pub(crate) const DEFAULT_METRICS_LISTEN: &str = "127.0.0.1:9464";

/// `ASPEN_SSH_LISTEN` and its host key (node C3).
#[derive(Clone)]
pub(crate) struct SshSettings {
    /// Where SSH listens.
    pub(crate) listen: SocketAddr,
    /// The host key.
    pub(crate) host_key: HostKey,
}

/// Where the SSH host key comes from. It must be the same on every
/// instance and across restarts (clients pin it), so it is never generated.
#[derive(Clone)]
pub(crate) enum HostKey {
    /// `ASPEN_SSH_HOST_KEY_FILE`.
    File(PathBuf),
    /// `ASPEN_SSH_HOST_KEY`: the OpenSSH private key text, from a secret.
    Inline(String),
}

impl std::fmt::Debug for SshSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = match &self.host_key {
            HostKey::File(p) => format!("file {}", p.display()),
            HostKey::Inline(_) => "inline (redacted)".to_owned(),
        };
        f.debug_struct("SshSettings")
            .field("listen", &self.listen)
            .field("host_key", &key)
            .finish()
    }
}

/// The express path an Aspen server serves when `ASPEN_EXPRESS_SERVE` is on.
pub(crate) const EXPRESS_PATH: &str = "/-/express";

impl ServeConfig {
    /// Reads and checks the settings. `get` looks a variable up
    /// (`|k| std::env::var(k).ok()`); `read_file` reads a file the settings
    /// name (`ASPEN_POLICY_FILE`, `ASPEN_LIMITS_FILE`), and `read_secret` a
    /// secret file (the mirror credentials' `*_FILE` variables), which in
    /// production is [`crate::secret_file::read`].
    pub(crate) fn from_env(
        get: &impl Fn(&str) -> Option<String>,
        read_file: &impl Fn(&str) -> std::io::Result<String>,
        read_secret: &impl Fn(&str) -> std::io::Result<String>,
    ) -> Result<Self, ConfigError> {
        let var = |k: &str| {
            get(k)
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };

        let listen: SocketAddr = var("ASPEN_LISTEN")
            .unwrap_or_else(|| DEFAULT_LISTEN.to_owned())
            .parse()
            .map_err(|e: std::net::AddrParseError| invalid("ASPEN_LISTEN", e.to_string()))?;

        let public_url = match var("ASPEN_PUBLIC_URL") {
            None => None,
            Some(u) => Some(check_http_url("ASPEN_PUBLIC_URL", &u)?),
        };

        let database_url = var("DATABASE_URL").ok_or(ConfigError::Missing("DATABASE_URL"))?;
        check_database_url(&database_url)?;

        let blob_url = var("ASPEN_BLOB_URL").ok_or(ConfigError::Missing("ASPEN_BLOB_URL"))?;
        let blob_url_secret = match var("ASPEN_BLOB_URL_SECRET") {
            None => None,
            Some(hex) => {
                if !blob_url.starts_with("file:") {
                    return Err(invalid(
                        "ASPEN_BLOB_URL_SECRET",
                        "only a file:// blob store signs its own URLs; \
                         object stores presign with their own credentials",
                    ));
                }
                if public_url.is_none() {
                    return Err(invalid(
                        "ASPEN_BLOB_URL_SECRET",
                        "needs ASPEN_PUBLIC_URL, the base of the signed /-/blobs URLs",
                    ));
                }
                let bytes = decode_hex(&hex)
                    .ok_or_else(|| invalid("ASPEN_BLOB_URL_SECRET", "must be hexadecimal"))?;
                if bytes.len() < MIN_URL_SECRET_BYTES {
                    return Err(invalid(
                        "ASPEN_BLOB_URL_SECRET",
                        format!("must be at least {MIN_URL_SECRET_BYTES} bytes (64 hex digits)"),
                    ));
                }
                Some(UrlSecret(bytes))
            }
        };

        let policy =
            json_setting(&var, read_file, "ASPEN_POLICY", "ASPEN_POLICY_FILE")?.unwrap_or_default();
        let limits =
            json_setting(&var, read_file, "ASPEN_LIMITS", "ASPEN_LIMITS_FILE")?.unwrap_or_default();

        let hops = number(&var, "ASPEN_TRUSTED_PROXY_HOPS", 0)?;
        let hops =
            usize::try_from(hops).map_err(|_| invalid("ASPEN_TRUSTED_PROXY_HOPS", "too large"))?;
        let client_addr = ClientAddr::from_settings(hops, var("ASPEN_TRUSTED_PROXIES").as_deref())
            .map_err(|e| invalid("ASPEN_TRUSTED_PROXIES", e.to_string()))?;

        let store = aspen_meta::StoreOptions {
            max_connections: u32::try_from(positive(
                &var,
                "ASPEN_DB_MAX_CONNECTIONS",
                u64::from(DEFAULT_DB_MAX_CONNECTIONS),
            )?)
            .map_err(|_| invalid("ASPEN_DB_MAX_CONNECTIONS", "must be at most 4294967295"))?,
            acquire_timeout: {
                let secs = number(
                    &var,
                    "ASPEN_DB_ACQUIRE_TIMEOUT_SECS",
                    DEFAULT_DB_ACQUIRE_TIMEOUT_SECS,
                )?;
                let max = aspen_meta::MAX_ACQUIRE_TIMEOUT.as_secs();
                if secs > max {
                    return Err(invalid(
                        "ASPEN_DB_ACQUIRE_TIMEOUT_SECS",
                        format!("must be at most {max} (a year); 0 waits for ever"),
                    ));
                }
                std::time::Duration::from_secs(secs)
            },
            ..aspen_meta::StoreOptions::default()
        };

        let defaults = InstanceLimits::default();
        let slots = |name: &'static str, default: usize| -> Result<usize, ConfigError> {
            usize::try_from(positive(&var, name, default as u64)?)
                .map_err(|_| invalid(name, "too large"))
        };
        let instance = InstanceLimits {
            fetches: slots("ASPEN_MAX_CONCURRENT_FETCHES", defaults.fetches)?,
            pushes: slots("ASPEN_MAX_CONCURRENT_PUSHES", defaults.pushes)?,
            stitches: slots("ASPEN_MAX_CONCURRENT_STITCHES", defaults.stitches)?,
            queue: usize::try_from(number(
                &var,
                "ASPEN_ADMISSION_QUEUE",
                defaults.queue as u64,
            )?)
            .map_err(|_| invalid("ASPEN_ADMISSION_QUEUE", "too large"))?,
            ..defaults
        };

        let spool_dir = PathBuf::from(var("ASPEN_SPOOL_DIR").unwrap_or_else(|| "spool".into()));
        // Mirror mode (C4): every named credential must be complete, so a
        // typo fails here rather than at the first sync. Raw values: an
        // inline GitHub App key is PEM text.
        let mirror_credentials =
            aspen_mirror::settings::credentials(|k: &str| get(k), |p: &str| read_secret(p))
                .map_err(|e| ConfigError::Mirror(format!("ASPEN_MIRROR_CREDENTIALS: {e}")))?;
        let mirror = aspen_mirror::settings::config(var, spool_dir.join("mirror"))
            .map_err(ConfigError::Mirror)?;
        let max_object_bytes = positive(&var, "ASPEN_MAX_OBJECT_BYTES", DEFAULT_MAX_OBJECT_BYTES)?;
        let max_inflated_bytes =
            positive(&var, "ASPEN_MAX_INFLATED_BYTES", DEFAULT_MAX_INFLATED_BYTES)?;
        if max_inflated_bytes < max_object_bytes {
            return Err(invalid(
                "ASPEN_MAX_INFLATED_BYTES",
                "must be at least ASPEN_MAX_OBJECT_BYTES",
            ));
        }
        let max_push_bytes = positive(&var, "ASPEN_MAX_PUSH_BYTES", DEFAULT_MAX_PUSH_BYTES)?;
        let max_push_objects = u32::try_from(positive(
            &var,
            "ASPEN_MAX_PUSH_OBJECTS",
            DEFAULT_MAX_PUSH_OBJECTS,
        )?)
        .map_err(|_| invalid("ASPEN_MAX_PUSH_OBJECTS", "must be at most 4294967295"))?;
        let prefetch_memory_bytes = positive(
            &var,
            "ASPEN_PREFETCH_MEMORY_BYTES",
            DEFAULT_PREFETCH_MEMORY_BYTES,
        )?;

        let packfile_uris = switch(&var, "ASPEN_PACKFILE_URIS")?;
        let express_serve = match switch(&var, "ASPEN_EXPRESS_SERVE")? {
            Switch::On => true,
            Switch::Off => false,
            Switch::Auto => {
                names_self_express(public_url.as_deref(), var("ASPEN_EXPRESS_URLS").as_deref())
            }
        };
        let migrate = match switch(&var, "ASPEN_MIGRATE")? {
            Switch::Auto | Switch::On => true,
            Switch::Off => false,
        };
        let shallow_packs = match switch(&var, "ASPEN_SHALLOW_PACKS")? {
            Switch::Auto | Switch::On => true,
            Switch::Off => false,
        };
        let shallow_max_repo_bytes = positive(
            &var,
            "ASPEN_SHALLOW_MAX_REPO_BYTES",
            aspen_server::upload_pack::exact::DEFAULT_MAX_REPO_BYTES,
        )?;
        let shallow_unused = std::time::Duration::from_secs(number(
            &var,
            "ASPEN_SHALLOW_UNUSED_SECS",
            aspen_compact::DEFAULT_SHALLOW_UNUSED.as_secs(),
        )?);
        if shallow_unused < aspen_compact::MIN_SHALLOW_UNUSED {
            return Err(invalid(
                "ASPEN_SHALLOW_UNUSED_SECS",
                format!(
                    "must be at least {}",
                    aspen_compact::MIN_SHALLOW_UNUSED.as_secs()
                ),
            ));
        }

        let metrics_listen = match var("ASPEN_METRICS_LISTEN").as_deref() {
            Some("off") => None,
            other => Some(other.unwrap_or(DEFAULT_METRICS_LISTEN).parse().map_err(
                |e: std::net::AddrParseError| {
                    invalid("ASPEN_METRICS_LISTEN", format!("{e} (or off)"))
                },
            )?),
        };
        if metrics_listen.is_some_and(|m| m == listen && listen.port() != 0) {
            return Err(invalid(
                "ASPEN_METRICS_LISTEN",
                "must differ from ASPEN_LISTEN: the metrics stay off the Git listener",
            ));
        }

        let host_key = match (var("ASPEN_SSH_HOST_KEY"), var("ASPEN_SSH_HOST_KEY_FILE")) {
            (Some(_), Some(_)) => {
                return Err(invalid(
                    "ASPEN_SSH_HOST_KEY",
                    "set ASPEN_SSH_HOST_KEY or ASPEN_SSH_HOST_KEY_FILE, not both",
                ));
            }
            // Read raw: an inline key is multi-line and its trailing
            // newline matters to the parser.
            (Some(_), None) => get("ASPEN_SSH_HOST_KEY").map(HostKey::Inline),
            (None, Some(path)) => Some(HostKey::File(PathBuf::from(path))),
            (None, None) => None,
        };
        let ssh = match (var("ASPEN_SSH_LISTEN"), host_key) {
            (None, None) => None,
            (None, Some(_)) => {
                return Err(invalid(
                    "ASPEN_SSH_HOST_KEY",
                    "an SSH host key needs ASPEN_SSH_LISTEN",
                ));
            }
            (Some(_), None) => {
                return Err(invalid(
                    "ASPEN_SSH_LISTEN",
                    "needs a host key (ASPEN_SSH_HOST_KEY_FILE or ASPEN_SSH_HOST_KEY): \
                     clients pin it, so it must be the same on every instance and \
                     across restarts; it is never generated",
                ));
            }
            (Some(addr), Some(host_key)) => Some(SshSettings {
                listen: addr.parse().map_err(|e: std::net::AddrParseError| {
                    invalid("ASPEN_SSH_LISTEN", e.to_string())
                })?,
                host_key,
            }),
        };

        Ok(Self {
            listen,
            public_url,
            database_url,
            store,
            blob_url,
            blob_url_secret,
            policy,
            limits,
            client_addr,
            instance,
            spool_dir,
            max_object_bytes,
            max_inflated_bytes,
            max_push_bytes,
            max_push_objects,
            prefetch_memory_bytes,
            packfile_uris,
            express_serve,
            migrate,
            shallow_packs,
            shallow_max_repo_bytes,
            shallow_unused,
            metrics_listen,
            metrics_on_git_listener: false,
            background_worker: true,
            ssh,
            mirror_credentials,
            mirror,
        })
    }
}

/// The warning `aspen serve` logs at startup when its limits leave read or
/// push request rates unbounded (issue #125), or `None` when both are set.
pub(crate) fn unlimited_rates_warning(limits: &LimitsConfig) -> Option<String> {
    let missing = match (limits.read.is_none(), limits.push.is_none()) {
        (false, false) => return None,
        (true, true) => "no read or push rate limit",
        (true, false) => "no read rate limit",
        (false, true) => "no push rate limit",
    };
    Some(format!(
        "{missing} is set, so one caller can send requests as fast as this server \
         answers them; set ASPEN_LIMITS or ASPEN_LIMITS_FILE (deploy/README.md)"
    ))
}

/// A JSON setting given inline (`inline`) or as a file (`file`), never both.
/// Platforms that cannot mount a file (Cloud Run, Container Apps) use the
/// inline form.
fn json_setting<T: serde::de::DeserializeOwned>(
    var: &impl Fn(&str) -> Option<String>,
    read_file: &impl Fn(&str) -> std::io::Result<String>,
    inline: &'static str,
    file: &'static str,
) -> Result<Option<T>, ConfigError> {
    match (var(inline), var(file)) {
        (Some(_), Some(_)) => Err(invalid(inline, format!("set {inline} or {file}, not both"))),
        (Some(text), None) => serde_json::from_str(&text)
            .map(Some)
            .map_err(|e| invalid(inline, e.to_string())),
        (None, Some(path)) => {
            let text = read_file(&path).map_err(|e| invalid(file, format!("{path}: {e}")))?;
            serde_json::from_str(&text)
                .map(Some)
                .map_err(|e| invalid(file, format!("{path}: {e}")))
        }
        (None, None) => Ok(None),
    }
}

/// Whether `ASPEN_EXPRESS_URLS` lists this server's own `/-/express`
/// (C2 obligation: mount the route when the server is one of its express
/// endpoints).
pub(crate) fn names_self_express(public_url: Option<&str>, express_urls: Option<&str>) -> bool {
    let (Some(public), Some(urls)) = (public_url, express_urls) else {
        return false;
    };
    let own = format!("{}{EXPRESS_PATH}", public.trim_end_matches('/'));
    urls.split(',')
        .map(|u| u.trim().trim_end_matches('/'))
        .any(|u| u == own)
}

/// Refuses a database URL that carries a password: the password comes from
/// `PGPASSWORD`, so it can be stored and rotated on its own and never lands
/// in a log line or a process listing (A8 decisions row).
pub(crate) fn check_database_url(url: &str) -> Result<(), ConfigError> {
    let scheme = url.split(':').next().unwrap_or_default();
    match scheme {
        "postgres" | "postgresql" => {
            let parsed = url::Url::parse(url)
                .map_err(|e| invalid("DATABASE_URL", format!("not a URL: {e}")))?;
            if parsed.password().is_some() {
                return Err(invalid(
                    "DATABASE_URL",
                    "must not contain a password; set PGPASSWORD instead",
                ));
            }
            if parsed.query_pairs().any(|(k, _)| k == "password") {
                return Err(invalid(
                    "DATABASE_URL",
                    "must not contain a password; set PGPASSWORD instead",
                ));
            }
            Ok(())
        }
        "sqlite" => Ok(()),
        _ => Err(invalid(
            "DATABASE_URL",
            "must be a postgres:// or sqlite:// URL",
        )),
    }
}

fn check_http_url(var: &'static str, url: &str) -> Result<String, ConfigError> {
    let parsed = url::Url::parse(url).map_err(|e| invalid(var, format!("not a URL: {e}")))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(invalid(var, "must be an http:// or https:// URL"));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(invalid(var, "must not have a query or fragment"));
    }
    Ok(url.trim_end_matches('/').to_owned())
}

fn number(
    var: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: u64,
) -> Result<u64, ConfigError> {
    var(name).map_or(Ok(default), |v| {
        v.parse()
            .map_err(|e: std::num::ParseIntError| invalid(name, e.to_string()))
    })
}

fn positive(
    var: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: u64,
) -> Result<u64, ConfigError> {
    let n = number(var, name, default)?;
    if n == 0 {
        return Err(invalid(name, "must be greater than zero"));
    }
    Ok(n)
}

fn switch(
    var: &impl Fn(&str) -> Option<String>,
    name: &'static str,
) -> Result<Switch, ConfigError> {
    match var(name).as_deref() {
        None | Some("auto") => Ok(Switch::Auto),
        Some("true" | "1" | "on") => Ok(Switch::On),
        Some("false" | "0" | "off") => Ok(Switch::Off),
        Some(other) => Err(invalid(
            name,
            format!("{other:?} is not auto, true or false"),
        )),
    }
}

/// Lowercase or uppercase hex to bytes.
pub(crate) fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const SECRET_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    fn parse(pairs: &[(&str, &str)]) -> Result<ServeConfig, ConfigError> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let files: HashMap<&str, &str> = HashMap::from([
            ("policy.json", r#"{"anonymous": ["read:acme/*"]}"#),
            (
                "limits.json",
                r#"{"read": {"burst": 10, "per_second": 2.5}}"#,
            ),
            ("bad.json", r#"{"anonymous": ["fly:*"]}"#),
        ]);
        let read = |p: &str| {
            files
                .get(p)
                .map(|s| (*s).to_owned())
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
        };
        ServeConfig::from_env(&|k| m.get(k).cloned(), &read, &read)
    }

    const BASE: &[(&str, &str)] = &[
        (
            "DATABASE_URL",
            "postgres://aspen@db:5432/aspen?sslmode=require",
        ),
        ("ASPEN_BLOB_URL", "s3://aspen-packs"),
    ];

    fn with(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
        let mut v = BASE.to_vec();
        v.extend_from_slice(extra);
        v
    }

    fn err(pairs: &[(&str, &str)]) -> String {
        parse(pairs).expect_err("should be refused").to_string()
    }

    #[test]
    fn instance_limits_default_and_refuse_zero_slots() {
        assert_eq!(parse(BASE).unwrap().instance, InstanceLimits::default());
        let c = parse(&with(&[
            ("ASPEN_MAX_CONCURRENT_FETCHES", "3"),
            ("ASPEN_MAX_CONCURRENT_PUSHES", "2"),
            ("ASPEN_MAX_CONCURRENT_STITCHES", "1"),
            ("ASPEN_ADMISSION_QUEUE", "0"),
        ]))
        .unwrap();
        assert_eq!(
            (c.instance.fetches, c.instance.pushes, c.instance.stitches),
            (3, 2, 1)
        );
        assert_eq!(c.instance.queue, 0);
        assert_eq!(
            err(&with(&[("ASPEN_MAX_CONCURRENT_PUSHES", "0")])),
            "ASPEN_MAX_CONCURRENT_PUSHES: must be greater than zero"
        );
    }

    #[test]
    fn defaults_follow_the_configuration_contract() {
        let c = parse(BASE).unwrap();
        assert_eq!(c.listen, DEFAULT_LISTEN.parse().unwrap());
        assert!(matches!(c.client_addr, ClientAddr::Peer));
        assert!(
            c.policy.anonymous.is_empty(),
            "no anonymous access by default"
        );
        assert_eq!(c.packfile_uris, Switch::Auto);
        assert!(!c.express_serve);
        assert!(c.migrate);
        assert_eq!(c.spool_dir, PathBuf::from("spool"));
        assert_eq!(c.max_object_bytes, DEFAULT_MAX_OBJECT_BYTES);
    }

    #[test]
    fn required_variables_are_named() {
        assert_eq!(
            err(&[("ASPEN_BLOB_URL", "s3://x")]),
            "DATABASE_URL is required"
        );
        assert_eq!(
            err(&[("DATABASE_URL", "sqlite://x.db")]),
            "ASPEN_BLOB_URL is required"
        );
    }

    #[test]
    fn a_password_in_the_database_url_is_refused_without_echoing_it() {
        for url in [
            "postgres://aspen:hunter2-secret@db/aspen",
            "postgresql://aspen:hunter2-secret@db/aspen?sslmode=require",
            "postgres://aspen@db/aspen?password=hunter2-secret",
        ] {
            let e = err(&[("DATABASE_URL", url), ("ASPEN_BLOB_URL", "s3://x")]);
            assert!(e.contains("PGPASSWORD"), "{e}");
            assert!(!e.contains("hunter2"), "the secret leaked: {e}");
        }
        assert!(
            parse(&[
                ("DATABASE_URL", "sqlite:///tmp/a.db?mode=rwc"),
                ("ASPEN_BLOB_URL", "s3://x")
            ])
            .is_ok()
        );
        assert!(
            err(&[
                ("DATABASE_URL", "mysql://db/aspen"),
                ("ASPEN_BLOB_URL", "s3://x")
            ])
            .contains("postgres://")
        );
    }

    #[test]
    fn the_local_url_secret_needs_a_file_store_a_public_url_and_32_bytes() {
        let ok = [
            ("DATABASE_URL", "sqlite://a.db"),
            ("ASPEN_BLOB_URL", "file:///var/lib/aspen/blobs"),
            ("ASPEN_PUBLIC_URL", "http://git.example.com/"),
            ("ASPEN_BLOB_URL_SECRET", SECRET_HEX),
        ];
        let c = parse(&ok).unwrap();
        assert_eq!(c.blob_url_secret.as_ref().map(|s| s.0.len()), Some(32));
        assert!(
            !format!("{c:?}").contains(&format!("{:?}", c.blob_url_secret.as_ref().unwrap().0)),
            "Debug shows the URL signing key"
        );
        assert_eq!(c.public_url.as_deref(), Some("http://git.example.com"));

        let mut s3 = ok;
        s3[1] = ("ASPEN_BLOB_URL", "s3://bucket");
        assert!(err(&s3).contains("only a file://"));
        assert!(
            err(&ok[..2].iter().chain(&ok[3..]).copied().collect::<Vec<_>>())
                .contains("ASPEN_PUBLIC_URL")
        );
        let mut short = ok;
        short[3] = ("ASPEN_BLOB_URL_SECRET", "00ff");
        assert!(err(&short).contains("at least 32 bytes"));
        let mut not_hex = ok;
        not_hex[3] = ("ASPEN_BLOB_URL_SECRET", "zz");
        let e = err(&not_hex);
        assert!(e.contains("hexadecimal") && !e.contains("zz"), "{e}");
    }

    #[test]
    fn express_is_served_when_its_urls_name_this_server() {
        let c = parse(&with(&[
            ("ASPEN_PUBLIC_URL", "https://git.example.com"),
            (
                "ASPEN_EXPRESS_URLS",
                "https://eu.example.com, https://git.example.com/-/express/",
            ),
        ]))
        .unwrap();
        assert!(c.express_serve);
        let c = parse(&with(&[
            ("ASPEN_PUBLIC_URL", "https://git.example.com"),
            ("ASPEN_EXPRESS_URLS", "https://eu.example.com/-/express"),
        ]))
        .unwrap();
        assert!(!c.express_serve, "another server's express is not ours");
        let c = parse(&with(&[("ASPEN_EXPRESS_SERVE", "true")])).unwrap();
        assert!(c.express_serve, "explicit on (a CDN's origin)");
        let c = parse(&with(&[
            ("ASPEN_PUBLIC_URL", "https://git.example.com"),
            ("ASPEN_EXPRESS_URLS", "https://git.example.com/-/express"),
            ("ASPEN_EXPRESS_SERVE", "false"),
        ]))
        .unwrap();
        assert!(!c.express_serve, "explicit off wins");
    }

    #[test]
    fn policy_and_limits_come_from_json_files() {
        let c = parse(&with(&[
            ("ASPEN_POLICY_FILE", "policy.json"),
            ("ASPEN_LIMITS_FILE", "limits.json"),
        ]))
        .unwrap();
        assert_eq!(c.policy.anonymous.len(), 1);
        assert_eq!(c.limits.read.map(|r| r.burst), Some(10));
        assert!(
            err(&with(&[("ASPEN_POLICY_FILE", "bad.json")]))
                .starts_with("ASPEN_POLICY_FILE: bad.json")
        );
        assert!(
            err(&with(&[("ASPEN_LIMITS_FILE", "missing.json")]))
                .starts_with("ASPEN_LIMITS_FILE: missing.json")
        );
    }

    #[test]
    fn policy_and_limits_can_be_inline_but_not_both_ways() {
        let c = parse(&with(&[
            ("ASPEN_POLICY", r#"{"anonymous": ["read:*"]}"#),
            ("ASPEN_LIMITS", r#"{"push": {"burst": 5, "per_second": 1}}"#),
        ]))
        .unwrap();
        assert_eq!(c.policy.anonymous.len(), 1);
        assert_eq!(c.limits.push.map(|r| r.burst), Some(5));
        assert!(err(&with(&[("ASPEN_POLICY", "{oops")])).starts_with("ASPEN_POLICY:"));
        assert_eq!(
            err(&with(&[
                ("ASPEN_POLICY", "{}"),
                ("ASPEN_POLICY_FILE", "policy.json")
            ])),
            "ASPEN_POLICY: set ASPEN_POLICY or ASPEN_POLICY_FILE, not both"
        );
    }

    #[test]
    fn metrics_get_their_own_private_listener() {
        let c = parse(BASE).unwrap();
        assert_eq!(
            c.metrics_listen,
            Some(DEFAULT_METRICS_LISTEN.parse().unwrap())
        );
        assert!(
            DEFAULT_METRICS_LISTEN.starts_with("127.0.0.1:"),
            "private by default"
        );
        assert!(!c.metrics_on_git_listener);
        assert_eq!(
            parse(&with(&[("ASPEN_METRICS_LISTEN", "off")]))
                .unwrap()
                .metrics_listen,
            None
        );
        assert!(
            err(&with(&[
                ("ASPEN_LISTEN", "0.0.0.0:7070"),
                ("ASPEN_METRICS_LISTEN", "0.0.0.0:7070")
            ]))
            .contains("must differ from ASPEN_LISTEN")
        );
    }

    #[test]
    fn mirror_credentials_are_checked_at_startup_and_never_echoed() {
        let c = parse(BASE).unwrap();
        assert_eq!(c.mirror_credentials.names().count(), 0, "none by default");
        assert_eq!(c.mirror.work_dir, PathBuf::from("spool").join("mirror"));
        let e = err(&with(&[("ASPEN_MIRROR_CREDENTIALS", "github")]));
        assert!(e.contains("ASPEN_MIRROR_GITHUB_TOKEN"), "{e}");
        let c = parse(&with(&[
            ("ASPEN_MIRROR_CREDENTIALS", "github"),
            ("ASPEN_MIRROR_GITHUB_TOKEN", "secret-token-text"),
            ("ASPEN_MIRROR_SYNC_SECS", "5"),
        ]))
        .unwrap();
        assert_eq!(c.mirror_credentials.names().count(), 1);
        assert_eq!(c.mirror.sync_interval, std::time::Duration::from_secs(5));
        assert!(
            !format!("{c:?}").contains("secret-token-text"),
            "Debug redacts the token"
        );
        let e = err(&with(&[("ASPEN_MIRROR_SYNC_SECS", "0")]));
        assert!(e.contains("ASPEN_MIRROR_SYNC_SECS"), "{e}");
    }

    /// Issue #125: mirror credential files are secrets, so they go through
    /// the secret reader (which checks their mode), never the plain one.
    #[test]
    fn mirror_credential_files_are_read_as_secrets() {
        let m: HashMap<&str, &str> = HashMap::from([
            ("DATABASE_URL", "sqlite://meta.db"),
            ("ASPEN_BLOB_URL", "file:///blobs"),
            ("ASPEN_MIRROR_CREDENTIALS", "gh"),
            ("ASPEN_MIRROR_GH_TOKEN_FILE", "/run/secrets/gh"),
        ]);
        let plain = |_: &str| -> std::io::Result<String> { panic!("read as a plain file") };
        let refuse = |_: &str| -> std::io::Result<String> {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "accessible to every user (mode 0644)",
            ))
        };
        let get = |k: &str| m.get(k).map(|v| (*v).to_owned());
        let e = ServeConfig::from_env(&get, &plain, &refuse)
            .expect_err("a refused secret file stops startup")
            .to_string();
        assert!(
            e.starts_with("ASPEN_MIRROR_CREDENTIALS: ")
                && e.contains("TOKEN_FILE /run/secrets/gh")
                && e.contains("mode 0644"),
            "{e}"
        );
        let accept = |_: &str| Ok("token-text\n".to_owned());
        let c = ServeConfig::from_env(&get, &plain, &accept).unwrap();
        assert_eq!(c.mirror_credentials.names().count(), 1);
    }

    /// Issue #125: `aspen serve` names what is unlimited at startup.
    #[test]
    fn missing_request_rates_are_warned_about() {
        let limits = |json: &str| serde_json::from_str::<LimitsConfig>(json).unwrap();
        let w = unlimited_rates_warning(&LimitsConfig::default()).unwrap();
        assert!(
            w.starts_with("no read or push rate limit") && w.contains("ASPEN_LIMITS"),
            "{w}"
        );
        let read = r#"{"read": {"burst": 10, "per_second": 1}}"#;
        let push = r#"{"push": {"burst": 10, "per_second": 1}}"#;
        assert!(
            unlimited_rates_warning(&limits(read))
                .unwrap()
                .starts_with("no push rate limit")
        );
        assert!(
            unlimited_rates_warning(&limits(push))
                .unwrap()
                .starts_with("no read rate limit")
        );
        let both =
            r#"{"read": {"burst": 10, "per_second": 1}, "push": {"burst": 2, "per_second": 1}}"#;
        assert_eq!(unlimited_rates_warning(&limits(both)), None);
    }

    #[test]
    fn ssh_needs_a_stable_host_key_and_never_echoes_it() {
        assert!(parse(BASE).unwrap().ssh.is_none(), "SSH is off by default");
        let e = err(&with(&[("ASPEN_SSH_LISTEN", "0.0.0.0:2222")]));
        assert!(e.contains("needs a host key"), "{e}");
        let c = parse(&with(&[
            ("ASPEN_SSH_LISTEN", "0.0.0.0:2222"),
            (
                "ASPEN_SSH_HOST_KEY",
                "placeholder host key text: secret-bytes",
            ),
        ]))
        .unwrap();
        let ssh = c.ssh.unwrap();
        assert_eq!(ssh.listen, "0.0.0.0:2222".parse().unwrap());
        assert!(
            !format!("{ssh:?}").contains("secret-bytes"),
            "Debug redacts the key"
        );
        assert!(
            err(&with(&[("ASPEN_SSH_HOST_KEY_FILE", "/etc/aspen/host_key")]))
                .contains("needs ASPEN_SSH_LISTEN")
        );
        let both = err(&with(&[
            ("ASPEN_SSH_LISTEN", "0.0.0.0:2222"),
            ("ASPEN_SSH_HOST_KEY", "secret-bytes"),
            ("ASPEN_SSH_HOST_KEY_FILE", "/k"),
        ]));
        assert!(
            both.contains("not both") && !both.contains("secret-bytes"),
            "{both}"
        );
    }

    #[test]
    fn numbers_and_switches_are_checked() {
        let c = parse(&with(&[("ASPEN_TRUSTED_PROXY_HOPS", "1")])).unwrap();
        assert_eq!(c.client_addr.hops(), 1);
        assert_eq!(
            c.client_addr.trusted_proxies(),
            Some(&aspen_server::access::TrustedProxies::private())
        );
        let c = parse(&with(&[
            ("ASPEN_TRUSTED_PROXY_HOPS", "2"),
            ("ASPEN_TRUSTED_PROXIES", "10.0.0.0/16, 10.1.0.0/16"),
        ]))
        .unwrap();
        assert_eq!(c.client_addr.hops(), 2);
        assert_eq!(
            c.client_addr.trusted_proxies().map(ToString::to_string),
            Some("10.0.0.0/16,10.1.0.0/16".into())
        );
        assert!(
            err(&with(&[("ASPEN_TRUSTED_PROXIES", "10.0.0.0/16")]))
                .starts_with("ASPEN_TRUSTED_PROXIES")
        );
        // An empty value, as a Helm or Terraform template renders an unset
        // field, is unset: no refusal with hops 0, the default list with
        // hops above 0. It never trusts no proxy or every address.
        for empty in ["", "  "] {
            let c = parse(&with(&[("ASPEN_TRUSTED_PROXIES", empty)])).unwrap();
            assert_eq!(c.client_addr.hops(), 0, "{empty:?}");
            assert_eq!(c.client_addr.trusted_proxies(), None, "{empty:?}");
            let c = parse(&with(&[
                ("ASPEN_TRUSTED_PROXY_HOPS", "1"),
                ("ASPEN_TRUSTED_PROXIES", empty),
            ]))
            .unwrap();
            assert_eq!(
                c.client_addr.trusted_proxies(),
                Some(&aspen_server::access::TrustedProxies::private()),
                "{empty:?}"
            );
        }
        assert!(
            err(&with(&[
                ("ASPEN_TRUSTED_PROXY_HOPS", "1"),
                ("ASPEN_TRUSTED_PROXIES", "10.0.0.1/16")
            ]))
            .contains("past its prefix")
        );
        assert!(err(&with(&[("ASPEN_MAX_OBJECT_BYTES", "0")])).contains("greater than zero"));
        assert!(
            err(&with(&[("ASPEN_MAX_PUSH_BYTES", "1GiB")])).starts_with("ASPEN_MAX_PUSH_BYTES")
        );
        assert!(
            err(&with(&[("ASPEN_MAX_INFLATED_BYTES", "1024")])).contains("ASPEN_MAX_OBJECT_BYTES")
        );
        assert!(err(&with(&[("ASPEN_PACKFILE_URIS", "yes")])).starts_with("ASPEN_PACKFILE_URIS"));
        assert!(!parse(&with(&[("ASPEN_MIGRATE", "false")])).unwrap().migrate);
        assert!(parse(BASE).unwrap().shallow_packs);
        assert!(
            !parse(&with(&[("ASPEN_SHALLOW_PACKS", "off")]))
                .unwrap()
                .shallow_packs
        );
        assert!(err(&with(&[("ASPEN_SHALLOW_PACKS", "maybe")])).starts_with("ASPEN_SHALLOW_PACKS"));
        let c = parse(BASE).unwrap();
        assert_eq!(c.shallow_max_repo_bytes, 8 << 30);
        assert_eq!(c.shallow_unused, std::time::Duration::from_secs(86_400));
        let c = parse(&with(&[
            ("ASPEN_SHALLOW_MAX_REPO_BYTES", "1048576"),
            ("ASPEN_SHALLOW_UNUSED_SECS", "600"),
        ]))
        .unwrap();
        assert_eq!(c.shallow_max_repo_bytes, 1 << 20);
        assert_eq!(c.shallow_unused, std::time::Duration::from_secs(600));
        assert!(err(&with(&[("ASPEN_SHALLOW_MAX_REPO_BYTES", "0")])).contains("greater than zero"));
        assert!(
            err(&with(&[("ASPEN_SHALLOW_UNUSED_SECS", "599")]))
                .starts_with("ASPEN_SHALLOW_UNUSED_SECS")
        );
        assert!(err(&with(&[("ASPEN_LISTEN", "7070")])).starts_with("ASPEN_LISTEN"));
        let store = parse(BASE).unwrap().store;
        assert_eq!(store.max_connections, DEFAULT_DB_MAX_CONNECTIONS);
        assert_eq!(
            store.acquire_timeout,
            std::time::Duration::from_secs(DEFAULT_DB_ACQUIRE_TIMEOUT_SECS)
        );
        let store = parse(&with(&[
            ("ASPEN_DB_MAX_CONNECTIONS", "64"),
            ("ASPEN_DB_ACQUIRE_TIMEOUT_SECS", "0"),
        ]))
        .unwrap()
        .store;
        assert_eq!(store.max_connections, 64);
        assert!(store.acquire_timeout.is_zero(), "0 waits for ever");
        assert!(err(&with(&[("ASPEN_DB_MAX_CONNECTIONS", "0")])).contains("greater than zero"));
        assert_eq!(
            parse(&with(&[("ASPEN_DB_ACQUIRE_TIMEOUT_SECS", "31536000")]))
                .unwrap()
                .store
                .acquire_timeout,
            aspen_meta::MAX_ACQUIRE_TIMEOUT
        );
        for too_long in ["31536001", "18446744073709551615"] {
            assert!(
                err(&with(&[("ASPEN_DB_ACQUIRE_TIMEOUT_SECS", too_long)])).contains("at most"),
                "{too_long}"
            );
        }
        assert!(
            err(&with(&[("ASPEN_DB_ACQUIRE_TIMEOUT_SECS", "30s")]))
                .starts_with("ASPEN_DB_ACQUIRE_TIMEOUT_SECS")
        );
        assert!(
            err(&with(&[("ASPEN_PUBLIC_URL", "git.example.com")])).starts_with("ASPEN_PUBLIC_URL")
        );
    }
}
