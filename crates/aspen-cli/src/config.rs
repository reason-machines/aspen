//! `aspen serve` settings, read from the environment (the configuration
//! contract in `deploy/README.md`).
//!
//! Parsing is a pure function of a variable lookup ([`ServeConfig::from_env`]),
//! so every rule here is unit-tested without touching the process
//! environment. Messages name the variable and never echo a secret.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use aspen_auth::{LimitsConfig, PolicyConfig};
use aspen_blob::UploadOptions;
use aspen_compact::WorkerOptions;
use aspen_pack::locate::LocateOptions;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::access::ClientAddr;
use aspen_server::admission::InstanceLimits;
use aspen_server::receive::ReceiveConfig;

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
/// Background jobs one instance runs at once. A job mostly waits on the
/// blob store, so several keep compaction up with a busy repository
/// (bench/throughput) without much CPU.
pub(crate) const DEFAULT_COMPACT_WORKERS: u64 = 8;
pub(crate) const MAX_COMPACT_WORKERS: u64 = 64;
/// Merge-queue entries one trunk update lands at most, by default and at
/// most.
pub(crate) const DEFAULT_MERGE_QUEUE_BATCH: u64 = 64;
pub(crate) const MAX_MERGE_QUEUE_BATCH: u64 = 1024;
/// Seconds a merge-queue outcome ref is kept.
pub(crate) const DEFAULT_MERGE_QUEUE_OUTCOME_TTL_SECS: u64 = 3600;
/// Shortest secret accepted for local-store URL signing, in bytes.
pub(crate) const MIN_URL_SECRET_BYTES: usize = 32;
/// Longest wait `ASPEN_ADMISSION_QUEUE_WAIT_SECS` and
/// `ASPEN_PREFETCH_WAIT_SECS` accept: Git clients and load balancers give
/// up long before an hour.
pub(crate) const MAX_WAIT_SECS: u64 = 3600;
/// Longest `ASPEN_DB_LOCK_TIMEOUT_SECS` and `ASPEN_DB_STATEMENT_TIMEOUT_SECS`
/// accept (a day; `0` is no limit). Postgres takes these in milliseconds
/// as a 32-bit integer, so about 24 days is its own ceiling.
pub(crate) const MAX_DB_TIMEOUT_SECS: u64 = 86_400;
/// Bounds on `ASPEN_PREFETCH_PARALLEL_READS` and `ASPEN_PUSH_THREADS`.
pub(crate) const MAX_PARALLEL: u64 = 1024;
/// Most commit rows one push may write in its own transaction
/// (`ASPEN_SYNC_COMMIT_ROWS`), so a transaction stays bounded.
pub(crate) const MAX_SYNC_COMMIT_ROWS: u64 = 1_000_000;
/// Bounds on `ASPEN_COMPACT_IDLE_POLL_MS`: polling faster than every 10 ms
/// only loads the database.
pub(crate) const MIN_COMPACT_IDLE_POLL_MS: u64 = 10;
pub(crate) const MAX_COMPACT_IDLE_POLL_MS: u64 = 3_600_000;
/// Most multipart parts one upload sends at once (`ASPEN_UPLOAD_CONCURRENCY`).
pub(crate) const MAX_UPLOAD_CONCURRENCY: u64 = 64;
/// Bounds on group commit (`ASPEN_GROUP_COMMIT_MAX_BATCH` pushes per
/// transaction, `ASPEN_GROUP_COMMIT_WINDOW_MS`): a batch stays bounded,
/// and a window past a second only delays every push.
pub(crate) const MAX_GROUP_COMMIT_BATCH: u64 = 1024;
pub(crate) const MAX_GROUP_COMMIT_WINDOW_MS: u64 = 1000;

/// Throughput tuning beyond the limits in [`ServeConfig`]: settings that
/// were fixed in code before (site/src/configuration.md, "Throughput
/// tuning"). Each `None` keeps the default of the library that owns the
/// setting, so the default lives in one place; the tests below pin those
/// defaults to the documented values.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Tuning {
    /// `ASPEN_PREFETCH_FLOOR_BYTES`: what each push reserves of
    /// `ASPEN_PREFETCH_MEMORY_BYTES` before its first read of stored packs
    /// (`ReceiveConfig::prefetch_floor_bytes`).
    pub(crate) prefetch_floor_bytes: Option<u64>,
    /// `ASPEN_PREFETCH_PER_PUSH_BYTES`: the most prefetch memory one push
    /// may grow to (`LocateOptions::max_prefetch_bytes`).
    pub(crate) prefetch_per_push_bytes: Option<u64>,
    /// `ASPEN_PREFETCH_WAIT_SECS`: how long a push waits for prefetch
    /// memory before a retryable refusal (`ReceiveConfig::prefetch_wait`).
    pub(crate) prefetch_wait: Option<Duration>,
    /// `ASPEN_PREFETCH_PARALLEL_READS`: blob reads in flight per prefetch
    /// round (`LocateOptions::max_parallel_reads`).
    pub(crate) prefetch_parallel_reads: Option<usize>,
    /// `ASPEN_IDX_CACHE_BYTES`: `.idx` bytes kept in memory for the push
    /// path's object lookups (`LocateOptions::idx_cache_bytes`).
    pub(crate) idx_cache_bytes: Option<u64>,
    /// `ASPEN_OBJECT_CACHE_BYTES`: resolved base objects kept in memory
    /// (`LocateOptions::object_cache_bytes`); `0` turns the cache off.
    pub(crate) object_cache_bytes: Option<u64>,
    /// `ASPEN_IDX_WHOLE_MAX_BYTES`: the largest `.idx` kept whole; a larger
    /// one is sampled (`LocateOptions::max_whole_idx_bytes`).
    pub(crate) idx_whole_max_bytes: Option<u64>,
    /// `ASPEN_TRUNK_TREE_CACHE_BYTES`: trunk's newest trees kept in memory
    /// for the walk of a push's changed paths
    /// (`ReceiveConfig::trunk_tree_cache_bytes`); `0` keeps none.
    pub(crate) trunk_tree_cache_bytes: Option<u64>,
    /// `ASPEN_PUSH_THREADS`: threads one push uses to complete and index
    /// its pack (`ReceiveConfig::thread_limit`); unset uses every core.
    pub(crate) push_threads: Option<usize>,
    /// `ASPEN_SYNC_COMMIT_ROWS`: commit index rows written in a push's own
    /// transaction (`ReceiveConfig::sync_commit_rows`).
    pub(crate) sync_commit_rows: Option<usize>,
    /// `ASPEN_COMPACT_IDLE_POLL_MS`: how often an idle background worker
    /// looks for jobs (`WorkerOptions::idle_poll`).
    pub(crate) compact_idle_poll: Option<Duration>,
    /// `ASPEN_UPLOAD_PART_BYTES`: single-`PUT` threshold and multipart part
    /// size of an S3 store (`UploadOptions::part_size`).
    pub(crate) upload_part_bytes: Option<usize>,
    /// `ASPEN_UPLOAD_CONCURRENCY`: multipart parts in flight per upload
    /// (`UploadOptions::concurrency`).
    pub(crate) upload_concurrency: Option<usize>,
    /// `ASPEN_GROUP_COMMIT_WINDOW_MS`: how long a group commit batch waits
    /// for more pushes (`GroupCommit::window`); acts only with
    /// `ASPEN_GROUP_COMMIT=on`.
    pub(crate) group_commit_window: Option<Duration>,
    /// `ASPEN_GROUP_COMMIT_MAX_BATCH`: most pushes one group commit
    /// transaction takes (`GroupCommit::max_batch`).
    pub(crate) group_commit_max_batch: Option<usize>,
}

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

/// `ASPEN_ROLE`: what one `aspen serve` process runs, so compaction can be
/// scaled apart from the instances that serve Git.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    /// `all` (the default): Git over HTTP and SSH, the merge queue, mirror
    /// mode, and the background worker (compaction, commit indexing,
    /// garbage collection).
    All,
    /// `serve`: everything but the background worker. Some other process
    /// must run `compact` (or `all`), or covers are never built, large
    /// pushes' commits are never indexed and garbage is never collected.
    Serve,
    /// `compact`: only the background worker, with `/healthz` and `/readyz`
    /// on `ASPEN_LISTEN` and `/-/metrics` on `ASPEN_METRICS_LISTEN`; no Git.
    /// Work that should run beside compaction and never on the Git servers
    /// (a single-instance, lease-guarded GC sweep, for example) belongs in
    /// the processes for which [`Role::runs_jobs`] holds: `serve.rs` starts
    /// all of it in one place, the `compact_task` branch.
    Compact,
}

impl Role {
    /// Whether this process serves Git (and runs the merge queue and
    /// mirror mode).
    pub(crate) fn serves_git(self) -> bool {
        matches!(self, Self::All | Self::Serve)
    }

    /// Whether this process runs the background worker.
    pub(crate) fn runs_jobs(self) -> bool {
        matches!(self, Self::All | Self::Compact)
    }
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
    /// connections may go without the pool making progress (issue #150);
    /// `ASPEN_DB_LOCK_TIMEOUT_SECS` and `ASPEN_DB_STATEMENT_TIMEOUT_SECS`.
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
    /// `ASPEN_MAX_CONCURRENT_STITCHES`, `ASPEN_ADMISSION_QUEUE` and
    /// `ASPEN_ADMISSION_QUEUE_WAIT_SECS`: the work this instance runs at
    /// once (issue #118).
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
    /// The other throughput settings ([`Tuning`]).
    pub(crate) tuning: Tuning,
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
    /// `ASPEN_ROLE`: what this process runs ([`Role`]).
    pub(crate) role: Role,
    /// Run the background worker (compaction, commit indexing, garbage
    /// collection) when the role has one. Always on for `aspen serve`;
    /// `aspen dev --no-background-worker` turns it off for benchmarks
    /// (bench/README.md).
    pub(crate) background_worker: bool,
    /// `ASPEN_COMPACT_WORKERS`: background jobs (compaction and commit
    /// indexing) this instance runs at once.
    pub(crate) compact_workers: usize,
    /// `ASPEN_MERGE_QUEUE`: run the merge queue (`aspen_server::queue`).
    pub(crate) merge_queue: bool,
    /// `ASPEN_MERGE_QUEUE_BATCH`: most queue entries one trunk update lands.
    pub(crate) merge_queue_batch: usize,
    /// `ASPEN_GROUP_COMMIT` (off by default): concurrent pushes share their
    /// metadata transactions (`aspen_server::receive::GroupCommit`; its
    /// window and batch size are in [`Tuning`]).
    pub(crate) group_commit: bool,
    /// `ASPEN_MERGE_QUEUE_OUTCOME_TTL_SECS`: how long outcome refs are
    /// kept; `None` (0) keeps them for ever.
    pub(crate) merge_queue_outcome_ttl: Option<std::time::Duration>,
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
            lock_timeout: db_timeout(
                &var,
                "ASPEN_DB_LOCK_TIMEOUT_SECS",
                aspen_meta::StoreOptions::default().lock_timeout,
            )?,
            statement_timeout: db_timeout(
                &var,
                "ASPEN_DB_STATEMENT_TIMEOUT_SECS",
                aspen_meta::StoreOptions::default().statement_timeout,
            )?,
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
            queue_wait: Duration::from_secs(at_most(
                &var,
                "ASPEN_ADMISSION_QUEUE_WAIT_SECS",
                defaults.queue_wait.as_secs(),
                MAX_WAIT_SECS,
            )?),
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
        let tuning = tuning(&var, max_object_bytes, prefetch_memory_bytes)?;
        let compact_workers = positive(&var, "ASPEN_COMPACT_WORKERS", DEFAULT_COMPACT_WORKERS)?;
        if compact_workers > MAX_COMPACT_WORKERS {
            return Err(invalid(
                "ASPEN_COMPACT_WORKERS",
                format!("must be at most {MAX_COMPACT_WORKERS}"),
            ));
        }
        let compact_workers = compact_workers as usize;
        let role = match var("ASPEN_ROLE").as_deref() {
            None | Some("all") => Role::All,
            Some("serve") => Role::Serve,
            Some("compact") => Role::Compact,
            Some(other) => {
                return Err(invalid(
                    "ASPEN_ROLE",
                    format!("{other:?} is not all, serve or compact"),
                ));
            }
        };
        let merge_queue = match switch(&var, "ASPEN_MERGE_QUEUE")? {
            Switch::On => true,
            Switch::Auto | Switch::Off => false,
        };
        // A compaction-only process serves no Git: settings that only mean
        // something to a Git server are a misconfiguration, not ignored.
        if role == Role::Compact {
            let git_only = [
                ("ASPEN_SSH_LISTEN", var("ASPEN_SSH_LISTEN").is_some()),
                ("ASPEN_MERGE_QUEUE", merge_queue),
            ];
            if let Some((name, _)) = git_only.into_iter().find(|(_, set)| *set) {
                return Err(invalid(
                    name,
                    "not used by ASPEN_ROLE=compact, which serves no Git; set it on the \
                     ASPEN_ROLE=serve instances",
                ));
            }
        }
        let merge_queue_batch =
            positive(&var, "ASPEN_MERGE_QUEUE_BATCH", DEFAULT_MERGE_QUEUE_BATCH)?;
        if merge_queue_batch > MAX_MERGE_QUEUE_BATCH {
            return Err(invalid(
                "ASPEN_MERGE_QUEUE_BATCH",
                format!("must be at most {MAX_MERGE_QUEUE_BATCH}"),
            ));
        }
        let merge_queue_batch = merge_queue_batch as usize;
        let group_commit = match switch(&var, "ASPEN_GROUP_COMMIT")? {
            Switch::On => true,
            Switch::Auto | Switch::Off => false,
        };
        let merge_queue_outcome_ttl = match number(
            &var,
            "ASPEN_MERGE_QUEUE_OUTCOME_TTL_SECS",
            DEFAULT_MERGE_QUEUE_OUTCOME_TTL_SECS,
        )? {
            0 => None,
            s => Some(std::time::Duration::from_secs(s)),
        };

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
            tuning,
            packfile_uris,
            express_serve,
            migrate,
            shallow_packs,
            shallow_max_repo_bytes,
            shallow_unused,
            metrics_listen,
            metrics_on_git_listener: false,
            role,
            background_worker: true,
            compact_workers,
            merge_queue,
            merge_queue_batch,
            group_commit,
            merge_queue_outcome_ttl,
            ssh,
            mirror_credentials,
            mirror,
        })
    }
}

impl ServeConfig {
    /// The push path's settings: the limits and every [`Tuning`] field set,
    /// over [`ReceiveConfig::new`]'s defaults.
    pub(crate) fn receive_config(
        &self,
        max_object: MaxObjectBytes,
        max_inflated: MaxInflatedBytes,
    ) -> ReceiveConfig {
        let mut r = ReceiveConfig::new(&self.spool_dir, max_object, max_inflated);
        r.push_limits = aspen_core::pack::PushLimits {
            max_objects: self.max_push_objects,
            max_pack_bytes: self.max_push_bytes,
        };
        r.prefetch_memory_bytes = self.prefetch_memory_bytes;
        r.merge_queue = self.merge_queue;
        let t = &self.tuning;
        if let Some(b) = t.prefetch_floor_bytes {
            r.prefetch_floor_bytes = b;
        }
        if let Some(w) = t.prefetch_wait {
            r.prefetch_wait = w;
        }
        if t.push_threads.is_some() {
            r.thread_limit = t.push_threads;
        }
        if let Some(n) = t.sync_commit_rows {
            r.sync_commit_rows = n;
        }
        if let Some(b) = t.prefetch_per_push_bytes {
            r.locate.max_prefetch_bytes = b;
        }
        if let Some(n) = t.prefetch_parallel_reads {
            r.locate.max_parallel_reads = n;
        }
        if let Some(b) = t.idx_cache_bytes {
            r.locate.idx_cache_bytes = b;
        }
        if let Some(b) = t.object_cache_bytes {
            r.locate.object_cache_bytes = b;
        }
        r.group_commit.enabled = self.group_commit;
        if let Some(w) = t.group_commit_window {
            r.group_commit.window = w;
        }
        if let Some(n) = t.group_commit_max_batch {
            r.group_commit.max_batch = n;
        }
        if let Some(b) = t.idx_whole_max_bytes {
            r.locate.max_whole_idx_bytes = b;
        }
        if let Some(b) = t.trunk_tree_cache_bytes {
            r.trunk_tree_cache_bytes = b;
        }
        r
    }

    /// One background worker's pacing, named `owner` in job leases.
    pub(crate) fn worker_options(&self, owner: String) -> WorkerOptions {
        let mut o = WorkerOptions::new(owner);
        if let Some(p) = self.tuning.compact_idle_poll {
            o.idle_poll = p;
        }
        o
    }

    /// How an S3 store uploads packs (a `file://` store ignores it).
    pub(crate) fn upload_options(&self) -> UploadOptions {
        let mut u = UploadOptions::default();
        if let Some(b) = self.tuning.upload_part_bytes {
            u.part_size = b;
        }
        if let Some(n) = self.tuning.upload_concurrency {
            u.concurrency = n;
        }
        u
    }
}

/// Reads the [`Tuning`] settings. `max_object_bytes` and
/// `prefetch_memory_bytes` are the parsed `ASPEN_MAX_OBJECT_BYTES` and
/// `ASPEN_PREFETCH_MEMORY_BYTES`, which bound the prefetch settings.
fn tuning(
    var: &impl Fn(&str) -> Option<String>,
    max_object_bytes: u64,
    prefetch_memory_bytes: u64,
) -> Result<Tuning, ConfigError> {
    let usize_of = |name: &'static str, n: Option<u64>| -> Result<Option<usize>, ConfigError> {
        n.map(|n| usize::try_from(n).map_err(|_| invalid(name, "too large")))
            .transpose()
    };

    let per_push = optional_in(var, "ASPEN_PREFETCH_PER_PUSH_BYTES", 1, u64::MAX)?;
    if let Some(p) = per_push {
        if p < max_object_bytes {
            return Err(invalid(
                "ASPEN_PREFETCH_PER_PUSH_BYTES",
                "must be at least ASPEN_MAX_OBJECT_BYTES: a push must be able to read \
                 the largest object it may hold",
            ));
        }
        if p > prefetch_memory_bytes {
            return Err(invalid(
                "ASPEN_PREFETCH_PER_PUSH_BYTES",
                "must be at most ASPEN_PREFETCH_MEMORY_BYTES, the memory all pushes share",
            ));
        }
    }
    let floor = optional_in(var, "ASPEN_PREFETCH_FLOOR_BYTES", 1, u64::MAX)?;
    if let Some(f) = floor {
        if f > prefetch_memory_bytes {
            return Err(invalid(
                "ASPEN_PREFETCH_FLOOR_BYTES",
                "must be at most ASPEN_PREFETCH_MEMORY_BYTES, the memory all pushes share",
            ));
        }
        let per_push = per_push.unwrap_or_else(|| {
            MaxObjectBytes::new(max_object_bytes)
                .map_or(u64::MAX, |m| LocateOptions::new(m).max_prefetch_bytes)
        });
        if f > per_push {
            return Err(invalid(
                "ASPEN_PREFETCH_FLOOR_BYTES",
                format!(
                    "must be at most the per-push prefetch budget ({per_push}; \
                     ASPEN_PREFETCH_PER_PUSH_BYTES)"
                ),
            ));
        }
    }

    Ok(Tuning {
        prefetch_floor_bytes: floor,
        prefetch_per_push_bytes: per_push,
        prefetch_wait: optional_in(var, "ASPEN_PREFETCH_WAIT_SECS", 0, MAX_WAIT_SECS)?
            .map(Duration::from_secs),
        prefetch_parallel_reads: usize_of(
            "ASPEN_PREFETCH_PARALLEL_READS",
            optional_in(var, "ASPEN_PREFETCH_PARALLEL_READS", 1, MAX_PARALLEL)?,
        )?,
        idx_cache_bytes: optional_in(var, "ASPEN_IDX_CACHE_BYTES", 1, u64::MAX)?,
        object_cache_bytes: optional_in(var, "ASPEN_OBJECT_CACHE_BYTES", 0, u64::MAX)?,
        idx_whole_max_bytes: optional_in(var, "ASPEN_IDX_WHOLE_MAX_BYTES", 0, u64::MAX)?,
        trunk_tree_cache_bytes: optional_in(var, "ASPEN_TRUNK_TREE_CACHE_BYTES", 0, u64::MAX)?,
        push_threads: usize_of(
            "ASPEN_PUSH_THREADS",
            optional_in(var, "ASPEN_PUSH_THREADS", 1, MAX_PARALLEL)?,
        )?,
        sync_commit_rows: usize_of(
            "ASPEN_SYNC_COMMIT_ROWS",
            optional_in(var, "ASPEN_SYNC_COMMIT_ROWS", 1, MAX_SYNC_COMMIT_ROWS)?,
        )?,
        compact_idle_poll: optional_in(
            var,
            "ASPEN_COMPACT_IDLE_POLL_MS",
            MIN_COMPACT_IDLE_POLL_MS,
            MAX_COMPACT_IDLE_POLL_MS,
        )?
        .map(Duration::from_millis),
        upload_part_bytes: usize_of(
            "ASPEN_UPLOAD_PART_BYTES",
            optional_in(
                var,
                "ASPEN_UPLOAD_PART_BYTES",
                aspen_blob::S3_MIN_PART_SIZE as u64,
                aspen_blob::S3_MAX_PART_SIZE,
            )?,
        )?,
        upload_concurrency: usize_of(
            "ASPEN_UPLOAD_CONCURRENCY",
            optional_in(var, "ASPEN_UPLOAD_CONCURRENCY", 1, MAX_UPLOAD_CONCURRENCY)?,
        )?,
        group_commit_window: optional_in(
            var,
            "ASPEN_GROUP_COMMIT_WINDOW_MS",
            0,
            MAX_GROUP_COMMIT_WINDOW_MS,
        )?
        .map(Duration::from_millis),
        group_commit_max_batch: usize_of(
            "ASPEN_GROUP_COMMIT_MAX_BATCH",
            optional_in(
                var,
                "ASPEN_GROUP_COMMIT_MAX_BATCH",
                1,
                MAX_GROUP_COMMIT_BATCH,
            )?,
        )?,
    })
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

/// A number that may be unset (`None`) and must lie in `min..=max`.
fn optional_in(
    var: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    min: u64,
    max: u64,
) -> Result<Option<u64>, ConfigError> {
    let Some(v) = var(name) else {
        return Ok(None);
    };
    let n: u64 = v
        .parse()
        .map_err(|e: std::num::ParseIntError| invalid(name, e.to_string()))?;
    if n < min || n > max {
        let reason = match (min, max) {
            (1, u64::MAX) => "must be greater than zero".to_owned(),
            (min, u64::MAX) => format!("must be at least {min}"),
            (0, max) => format!("must be at most {max}"),
            (min, max) => format!("must be between {min} and {max}"),
        };
        return Err(invalid(name, reason));
    }
    Ok(Some(n))
}

/// A number, `default` when unset, of at most `max`.
fn at_most(
    var: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: u64,
    max: u64,
) -> Result<u64, ConfigError> {
    Ok(optional_in(var, name, 0, max)?.unwrap_or(default))
}

/// A database timeout in seconds: `0` is no limit, at most
/// [`MAX_DB_TIMEOUT_SECS`].
fn db_timeout(
    var: &impl Fn(&str) -> Option<String>,
    name: &'static str,
    default: Duration,
) -> Result<Duration, ConfigError> {
    let secs = number(var, name, default.as_secs())?;
    if secs > MAX_DB_TIMEOUT_SECS {
        return Err(invalid(
            name,
            format!("must be at most {MAX_DB_TIMEOUT_SECS} (a day); 0 is no limit"),
        ));
    }
    Ok(Duration::from_secs(secs))
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
    fn merge_queue_is_off_by_default_and_batches_are_bounded() {
        let c = parse(BASE).unwrap();
        assert!(!c.merge_queue);
        assert_eq!(c.merge_queue_batch, 64);
        let c = parse(&with(&[
            ("ASPEN_MERGE_QUEUE", "true"),
            ("ASPEN_MERGE_QUEUE_BATCH", "256"),
        ]))
        .unwrap();
        assert!(c.merge_queue);
        assert_eq!(c.merge_queue_batch, 256);
        assert_eq!(
            parse(BASE).unwrap().merge_queue_outcome_ttl,
            Some(std::time::Duration::from_secs(3600))
        );
        let ttl = |v: &'static str| {
            parse(&with(&[("ASPEN_MERGE_QUEUE_OUTCOME_TTL_SECS", v)]))
                .unwrap()
                .merge_queue_outcome_ttl
        };
        assert_eq!(ttl("60"), Some(std::time::Duration::from_secs(60)));
        assert_eq!(ttl("0"), None, "0 keeps outcomes for ever");
        assert!(err(&with(&[("ASPEN_MERGE_QUEUE", "maybe")])).starts_with("ASPEN_MERGE_QUEUE"));
        assert_eq!(
            err(&with(&[("ASPEN_MERGE_QUEUE_BATCH", "2000")])),
            "ASPEN_MERGE_QUEUE_BATCH: must be at most 1024"
        );
    }

    #[test]
    fn group_commit_is_off_by_default_and_reaches_the_push_path() {
        use aspen_server::receive::GroupCommit;
        let receive = |c: &ServeConfig| {
            c.receive_config(
                MaxObjectBytes::new(c.max_object_bytes).unwrap(),
                MaxInflatedBytes::new(c.max_inflated_bytes).unwrap(),
            )
            .group_commit
        };
        assert_eq!(receive(&parse(BASE).unwrap()), GroupCommit::default());
        assert!(!GroupCommit::default().enabled);
        assert_eq!(
            receive(&parse(&with(&[("ASPEN_GROUP_COMMIT", "on")])).unwrap()),
            GroupCommit::on()
        );
        let g = receive(
            &parse(&with(&[
                ("ASPEN_GROUP_COMMIT", "true"),
                ("ASPEN_GROUP_COMMIT_WINDOW_MS", "0"),
                ("ASPEN_GROUP_COMMIT_MAX_BATCH", "8"),
            ]))
            .unwrap(),
        );
        assert!(g.enabled);
        assert_eq!((g.window, g.max_batch), (Duration::ZERO, 8));
        assert_eq!(
            err(&with(&[("ASPEN_GROUP_COMMIT_WINDOW_MS", "5000")])),
            "ASPEN_GROUP_COMMIT_WINDOW_MS: must be at most 1000"
        );
    }

    #[test]
    fn compact_workers_default_and_bounds() {
        assert_eq!(
            parse(BASE).unwrap().compact_workers,
            DEFAULT_COMPACT_WORKERS as usize
        );
        let c = parse(&with(&[("ASPEN_COMPACT_WORKERS", "16")])).unwrap();
        assert_eq!(c.compact_workers, 16);
        assert_eq!(
            err(&with(&[("ASPEN_COMPACT_WORKERS", "0")])),
            "ASPEN_COMPACT_WORKERS: must be greater than zero"
        );
        assert_eq!(
            err(&with(&[("ASPEN_COMPACT_WORKERS", "65")])),
            "ASPEN_COMPACT_WORKERS: must be at most 64"
        );
    }

    /// Invariant: one process runs Git and the background worker unless
    /// `ASPEN_ROLE` splits them, and a compaction-only process refuses the
    /// settings only a Git server uses instead of silently ignoring them.
    #[test]
    fn role_defaults_to_all_and_compact_refuses_git_settings() {
        assert_eq!(parse(BASE).unwrap().role, Role::All);
        for (value, role) in [
            ("all", Role::All),
            ("serve", Role::Serve),
            ("compact", Role::Compact),
        ] {
            assert_eq!(parse(&with(&[("ASPEN_ROLE", value)])).unwrap().role, role);
        }
        assert!(Role::All.serves_git() && Role::All.runs_jobs());
        assert!(Role::Serve.serves_git() && !Role::Serve.runs_jobs());
        assert!(!Role::Compact.serves_git() && Role::Compact.runs_jobs());
        assert_eq!(
            err(&with(&[("ASPEN_ROLE", "worker")])),
            "ASPEN_ROLE: \"worker\" is not all, serve or compact"
        );
        assert!(
            err(&with(&[
                ("ASPEN_ROLE", "compact"),
                ("ASPEN_SSH_LISTEN", "0.0.0.0:2222"),
                ("ASPEN_SSH_HOST_KEY", "x"),
            ]))
            .starts_with("ASPEN_SSH_LISTEN: not used by ASPEN_ROLE=compact")
        );
        assert!(
            err(&with(&[
                ("ASPEN_ROLE", "compact"),
                ("ASPEN_MERGE_QUEUE", "on")
            ]))
            .starts_with("ASPEN_MERGE_QUEUE: not used by ASPEN_ROLE=compact")
        );
        let c = parse(&with(&[
            ("ASPEN_ROLE", "compact"),
            ("ASPEN_MERGE_QUEUE", "off"),
        ]))
        .unwrap();
        assert_eq!(c.role, Role::Compact);
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

    /// Every throughput setting `aspen serve` reads: its name, its default
    /// as `site/src/configuration.md` documents it (`None` when the default
    /// is not a number), a valid value other than the default, and values
    /// it must refuse.
    #[rustfmt::skip]
    const THROUGHPUT: &[(&str, Option<&str>, &str, &[&str])] = &[
        ("ASPEN_MAX_CONCURRENT_FETCHES", Some("32"), "8", &["0", "x"]),
        ("ASPEN_MAX_CONCURRENT_PUSHES", Some("16"), "64", &["0", "-1"]),
        ("ASPEN_MAX_CONCURRENT_STITCHES", Some("32"), "8", &["0"]),
        ("ASPEN_ADMISSION_QUEUE", Some("64"), "256", &["x"]),
        ("ASPEN_ADMISSION_QUEUE_WAIT_SECS", Some("10"), "2", &["3601", "2s"]),
        ("ASPEN_PREFETCH_MEMORY_BYTES", Some("4294967296"), "8589934592", &["0"]),
        ("ASPEN_PREFETCH_FLOOR_BYTES", Some("67108864"), "16777216", &["0", "4294967297"]),
        ("ASPEN_PREFETCH_PER_PUSH_BYTES", Some("2147483648"), "1073741824", &["1", "4294967297"]),
        ("ASPEN_PREFETCH_WAIT_SECS", Some("60"), "5", &["3601"]),
        ("ASPEN_PREFETCH_PARALLEL_READS", Some("64"), "128", &["0", "1025"]),
        ("ASPEN_IDX_CACHE_BYTES", Some("268435456"), "1073741824", &["0", "1GiB"]),
        ("ASPEN_OBJECT_CACHE_BYTES", Some("67108864"), "0", &["x", "-1"]),
        ("ASPEN_IDX_WHOLE_MAX_BYTES", Some("67108864"), "0", &["x", "-1"]),
        ("ASPEN_TRUNK_TREE_CACHE_BYTES", Some("67108864"), "0", &["64MiB", "-1"]),
        ("ASPEN_PUSH_THREADS", None, "2", &["0", "1025"]),
        ("ASPEN_SYNC_COMMIT_ROWS", Some("10000"), "1000", &["0", "1000001"]),
        ("ASPEN_UPLOAD_PART_BYTES", Some("16777216"), "8388608", &["5242879", "5368709121"]),
        ("ASPEN_UPLOAD_CONCURRENCY", Some("4"), "8", &["0", "65"]),
        ("ASPEN_COMPACT_WORKERS", Some("8"), "16", &["0", "65"]),
        ("ASPEN_COMPACT_IDLE_POLL_MS", Some("5000"), "100", &["9", "3600001"]),
        ("ASPEN_ROLE", None, "compact", &["worker", "Compact"]),
        ("ASPEN_MERGE_QUEUE_BATCH", Some("64"), "128", &["0", "1025"]),
        ("ASPEN_GROUP_COMMIT", None, "on", &["maybe"]),
        ("ASPEN_GROUP_COMMIT_WINDOW_MS", Some("2"), "20", &["1001", "x"]),
        ("ASPEN_GROUP_COMMIT_MAX_BATCH", Some("64"), "128", &["0", "1025"]),
        ("ASPEN_DB_MAX_CONNECTIONS", Some("10"), "32", &["0"]),
        ("ASPEN_DB_ACQUIRE_TIMEOUT_SECS", Some("30"), "5", &["31536001"]),
        ("ASPEN_DB_LOCK_TIMEOUT_SECS", Some("10"), "2", &["86401", "1.5"]),
        ("ASPEN_DB_STATEMENT_TIMEOUT_SECS", Some("60"), "0", &["86401"]),
    ];

    /// What the server is built from: the structs `aspen serve` hands to
    /// the push path, the background workers, the blob store, admission
    /// and the metadata pool. Two configurations that print the same here
    /// run the same server, as far as throughput goes.
    fn effective(c: &ServeConfig) -> String {
        let max_object = MaxObjectBytes::new(c.max_object_bytes).unwrap();
        let max_inflated = MaxInflatedBytes::new(c.max_inflated_bytes).unwrap();
        format!(
            "{:?}\n{:?}\n{:?}\n{:?}\n{:?}\nworkers {}\nbatch {}\nrole {:?}",
            c.receive_config(max_object, max_inflated),
            c.worker_options("w".to_owned()),
            c.upload_options(),
            c.instance,
            c.store,
            c.compact_workers,
            c.merge_queue_batch,
            c.role,
        )
    }

    #[test]
    fn every_throughput_setting_changes_the_server_and_its_documented_default_does_not() {
        let base = effective(&parse(BASE).unwrap());
        let mut problems = Vec::new();
        for (name, default, value, _) in THROUGHPUT {
            match parse(&with(&[(name, value)])) {
                Ok(c) if effective(&c) == base => {
                    problems.push(format!("{name}={value} changes nothing (ignored?)"));
                }
                Ok(_) => {}
                Err(e) => problems.push(format!("{name}={value} refused: {e}")),
            }
            if let Some(default) = default {
                match parse(&with(&[(name, default)])) {
                    Ok(c) if effective(&c) != base => problems.push(format!(
                        "{name}={default}, its documented default, changes the server"
                    )),
                    Ok(_) => {}
                    Err(e) => problems.push(format!("{name}={default} refused: {e}")),
                }
            }
        }
        assert!(problems.is_empty(), "{}", problems.join("\n"));
    }

    #[test]
    fn invalid_throughput_settings_are_refused_naming_the_variable() {
        let mut accepted = Vec::new();
        for (name, _, _, bad) in THROUGHPUT {
            for value in *bad {
                match parse(&with(&[(name, value)])) {
                    Ok(_) => accepted.push(format!("{name}={value}")),
                    Err(e) => {
                        let e = e.to_string();
                        assert!(e.starts_with(&format!("{name}: ")), "{name}={value}: {e}");
                    }
                }
            }
        }
        assert!(
            accepted.is_empty(),
            "accepted {} invalid values:\n{}",
            accepted.len(),
            accepted.join("\n")
        );
    }

    #[test]
    fn every_throughput_setting_is_documented_with_its_default() {
        let page = include_str!("../../../site/src/configuration.md");
        let section = page
            .split("\n## ")
            .find(|s| s.starts_with("Throughput tuning"))
            .expect("site/src/configuration.md has a \"Throughput tuning\" section");
        for (name, default, _, _) in THROUGHPUT {
            let row = section
                .lines()
                .find(|l| l.starts_with(&format!("| `{name}` |")))
                .unwrap_or_else(|| panic!("{name} has no row in \"Throughput tuning\""));
            if let Some(default) = default {
                assert!(
                    row.starts_with(&format!("| `{name}` | `{default}`")),
                    "{name}: the documented default is not `{default}`: {row}"
                );
            }
            assert_eq!(
                row.matches(" | ").count(),
                3,
                "{name}: a row is name | default | meaning | tuning: {row}"
            );
        }
    }

    #[test]
    fn throughput_settings_reach_the_structs_the_server_runs() {
        let c = parse(&with(&[
            ("ASPEN_ADMISSION_QUEUE_WAIT_SECS", "0"),
            ("ASPEN_PREFETCH_MEMORY_BYTES", "8589934592"),
            ("ASPEN_PREFETCH_FLOOR_BYTES", "16777216"),
            ("ASPEN_PREFETCH_PER_PUSH_BYTES", "4294967296"),
            ("ASPEN_PREFETCH_WAIT_SECS", "5"),
            ("ASPEN_PREFETCH_PARALLEL_READS", "128"),
            ("ASPEN_IDX_CACHE_BYTES", "1073741824"),
            ("ASPEN_OBJECT_CACHE_BYTES", "0"),
            ("ASPEN_IDX_WHOLE_MAX_BYTES", "1048576"),
            ("ASPEN_TRUNK_TREE_CACHE_BYTES", "0"),
            ("ASPEN_PUSH_THREADS", "2"),
            ("ASPEN_SYNC_COMMIT_ROWS", "1000"),
            ("ASPEN_UPLOAD_PART_BYTES", "8388608"),
            ("ASPEN_UPLOAD_CONCURRENCY", "8"),
            ("ASPEN_COMPACT_IDLE_POLL_MS", "100"),
            ("ASPEN_DB_LOCK_TIMEOUT_SECS", "2"),
            ("ASPEN_DB_STATEMENT_TIMEOUT_SECS", "0"),
        ]))
        .unwrap();
        assert_eq!(c.instance.queue_wait, Duration::ZERO);
        assert_eq!(c.store.lock_timeout, Duration::from_secs(2));
        assert!(c.store.statement_timeout.is_zero(), "0 is no limit");
        let r = c.receive_config(
            MaxObjectBytes::new(c.max_object_bytes).unwrap(),
            MaxInflatedBytes::new(c.max_inflated_bytes).unwrap(),
        );
        assert_eq!(r.prefetch_memory_bytes, 8 << 30);
        assert_eq!(r.prefetch_floor_bytes, 16 << 20);
        assert_eq!(r.locate.max_prefetch_bytes, 4 << 30);
        assert_eq!(r.prefetch_wait, Duration::from_secs(5));
        assert_eq!(r.locate.max_parallel_reads, 128);
        assert_eq!(r.locate.idx_cache_bytes, 1 << 30);
        assert_eq!(r.locate.object_cache_bytes, 0);
        assert_eq!(r.locate.max_whole_idx_bytes, 1 << 20);
        assert_eq!(r.trunk_tree_cache_bytes, 0);
        assert_eq!(r.thread_limit, Some(2));
        assert_eq!(r.sync_commit_rows, 1000);
        let u = c.upload_options();
        assert_eq!((u.part_size, u.concurrency), (8 << 20, 8));
        assert_eq!(
            c.worker_options("w".into()).idle_poll,
            Duration::from_millis(100)
        );

        // Unset, each is its library's default, which the page documents.
        let c = parse(BASE).unwrap();
        let r = c.receive_config(
            MaxObjectBytes::new(c.max_object_bytes).unwrap(),
            MaxInflatedBytes::new(c.max_inflated_bytes).unwrap(),
        );
        assert_eq!(c.instance.queue_wait, Duration::from_secs(10));
        assert_eq!(c.store.lock_timeout, Duration::from_secs(10));
        assert_eq!(c.store.statement_timeout, Duration::from_secs(60));
        assert_eq!(r.prefetch_floor_bytes, 64 << 20);
        assert_eq!(r.locate.max_prefetch_bytes, 2 << 30, "4 x 512 MiB");
        assert_eq!(r.prefetch_wait, Duration::from_secs(60));
        assert_eq!(r.locate.max_parallel_reads, 64);
        assert_eq!(r.locate.idx_cache_bytes, 256 << 20);
        assert_eq!(r.locate.object_cache_bytes, 64 << 20);
        assert_eq!(r.locate.max_whole_idx_bytes, 64 << 20);
        assert_eq!(r.trunk_tree_cache_bytes, 64 << 20);
        assert_eq!(r.thread_limit, None, "every core");
        assert_eq!(r.sync_commit_rows, 10_000);
        assert_eq!(c.upload_options(), UploadOptions::default());
        assert_eq!(c.upload_options().part_size, 16 << 20);
        assert_eq!(
            c.worker_options("w".into()).idle_poll,
            Duration::from_secs(5)
        );
    }

    #[test]
    fn prefetch_settings_must_fit_together() {
        assert_eq!(
            err(&with(&[("ASPEN_PREFETCH_PER_PUSH_BYTES", "1048576")])),
            "ASPEN_PREFETCH_PER_PUSH_BYTES: must be at least ASPEN_MAX_OBJECT_BYTES: \
             a push must be able to read the largest object it may hold"
        );
        // A smaller object limit allows a smaller per-push budget.
        let c = parse(&with(&[
            ("ASPEN_MAX_OBJECT_BYTES", "1048576"),
            ("ASPEN_PREFETCH_PER_PUSH_BYTES", "1048576"),
        ]))
        .unwrap();
        assert_eq!(c.tuning.prefetch_per_push_bytes, Some(1 << 20));
        assert_eq!(
            err(&with(&[
                ("ASPEN_PREFETCH_MEMORY_BYTES", "1073741824"),
                ("ASPEN_PREFETCH_PER_PUSH_BYTES", "2147483648"),
            ])),
            "ASPEN_PREFETCH_PER_PUSH_BYTES: must be at most ASPEN_PREFETCH_MEMORY_BYTES, \
             the memory all pushes share"
        );
        assert_eq!(
            err(&with(&[
                ("ASPEN_PREFETCH_MEMORY_BYTES", "33554432"),
                ("ASPEN_PREFETCH_FLOOR_BYTES", "67108864"),
            ])),
            "ASPEN_PREFETCH_FLOOR_BYTES: must be at most ASPEN_PREFETCH_MEMORY_BYTES, \
             the memory all pushes share"
        );
        assert_eq!(
            err(&with(&[
                ("ASPEN_MAX_OBJECT_BYTES", "1048576"),
                ("ASPEN_PREFETCH_PER_PUSH_BYTES", "1048576"),
                ("ASPEN_PREFETCH_FLOOR_BYTES", "2097152"),
            ])),
            "ASPEN_PREFETCH_FLOOR_BYTES: must be at most the per-push prefetch budget \
             (1048576; ASPEN_PREFETCH_PER_PUSH_BYTES)"
        );
        // Unset, the floor follows the gate's own rule (it is capped at the
        // memory there), so a small memory setting on its own still starts.
        assert!(parse(&with(&[("ASPEN_PREFETCH_MEMORY_BYTES", "33554432")])).is_ok());
    }

    #[test]
    fn database_timeouts_allow_no_limit_and_at_most_a_day() {
        assert_eq!(
            err(&with(&[("ASPEN_DB_LOCK_TIMEOUT_SECS", "86401")])),
            "ASPEN_DB_LOCK_TIMEOUT_SECS: must be at most 86400 (a day); 0 is no limit"
        );
        let c = parse(&with(&[
            ("ASPEN_DB_LOCK_TIMEOUT_SECS", "86400"),
            ("ASPEN_DB_STATEMENT_TIMEOUT_SECS", "0"),
        ]))
        .unwrap();
        assert_eq!(c.store.lock_timeout, Duration::from_secs(86_400));
        assert!(c.store.statement_timeout.is_zero());
        assert_eq!(
            err(&with(&[("ASPEN_UPLOAD_PART_BYTES", "1048576")])),
            "ASPEN_UPLOAD_PART_BYTES: must be between 5242880 and 5368709120"
        );
    }
}
