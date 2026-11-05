//! `aspen serve` and `aspen dev`: build the stores, the server and its
//! access control from a [`ServeConfig`], and serve until SIGTERM or Ctrl-C.
//!
//! Routes, in the order a request meets them:
//!
//! | Path | Served by |
//! |---|---|
//! | `GET /healthz` | liveness: `ok` while the process serves HTTP |
//! | `GET /readyz` | readiness: `ok` once the metadata database answers, `503` otherwise |
//! | `/-/blobs/...`, `/-/express/...` | `aspen_server::Server::router`, outside the access guard |
//! | everything else | the Git endpoints, behind `aspen_server::access::guard` |
//!
//! `/-/metrics` is served on its own private listener
//! (`ASPEN_METRICS_LISTEN`, node C5), never on the Git listener except in
//! `aspen dev`. Git over SSH (node C3) runs on `ASPEN_SSH_LISTEN` with the
//! same server and the same access decision as HTTP.

use std::sync::Arc;
use std::time::Duration;

use aspen_auth::{AuthService, Limits};
use aspen_blob::{AnyBlobStore, LocalBlobStore, LocalUrlSigner};
use aspen_compact::{CompactConfig, Compactor, Worker, WorkerOptions};
use aspen_core::error::StoreError;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::PushLimits;
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::access::Access;
use aspen_server::access::tokens::SqlTokens;
use aspen_server::mirror::SqlMirrors;
use aspen_server::receive::ReceiveConfig;
use aspen_server::ssh::keys::SqlSshKeys;
use aspen_server::ssh::{PrivateKey, SshConfig, SshServer};
use aspen_server::upload_pack::exact::ShallowCacheConfig;
use aspen_server::{Server, ServerConfig};
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;

use crate::config::{HostKey, ServeConfig, Switch};

/// What differs between `aspen serve` and `aspen dev`.
#[derive(Debug)]
pub(crate) struct Startup {
    /// Name in log lines (`serve`, `dev`).
    pub(crate) label: &'static str,
    /// Repositories created at startup if missing (`aspen dev --repo`).
    pub(crate) create_repos: Vec<RepoName>,
    /// When `ASPEN_PUBLIC_URL` is unset, use `http://<bound address>`
    /// (`aspen dev`, which listens on loopback only).
    pub(crate) public_url_from_listener: bool,
    /// Generate a fresh signing secret for `/-/blobs` URLs when the store is
    /// local and none is configured (`aspen dev`). Presigned URLs live
    /// minutes, so a secret that changes on restart costs nothing.
    pub(crate) ephemeral_url_secret: bool,
    /// Warn at startup when no read or push rate limit is set (`aspen
    /// serve`; `aspen dev` listens on loopback only).
    pub(crate) warn_unlimited_rates: bool,
    /// Reads the `ASPEN_EXPRESS_*` / `ASPEN_CDN_*` variables (`aspen dev`
    /// ignores them).
    pub(crate) env: fn(&str) -> Option<String>,
}

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Serves until a shutdown signal; returns once in-flight requests ended.
pub(crate) fn run(config: ServeConfig, startup: Startup) -> Result<(), BoxError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(serve(config, startup))
}

async fn serve(config: ServeConfig, startup: Startup) -> Result<(), BoxError> {
    // The S3 client and sqlx's TLS need a rustls provider (A4 obligation).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let label = startup.label;

    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|e| format!("binding {}: {e}", config.listen))?;
    let addr = listener.local_addr()?;
    let public_url = config.public_url.clone().or_else(|| {
        startup
            .public_url_from_listener
            .then(|| format!("http://{addr}"))
    });

    let meta = SqlStore::connect_with(&config.database_url, &config.store)
        .await
        .map_err(|e| format!("connecting to DATABASE_URL: {e}"))?;
    if config.migrate {
        meta.migrate()
            .await
            .map_err(|e| format!("applying migrations: {e}"))?;
    }
    for name in &startup.create_repos {
        create_repo_if_missing(&meta, name, &default_settings()?).await?;
    }

    let secret = match (&config.blob_url_secret, startup.ephemeral_url_secret) {
        (Some(s), _) => Some(s.0.clone()),
        (None, true) => Some(random_secret()?),
        (None, false) => None,
    };
    let blobs = open_blobs(&config.blob_url, secret.as_deref(), public_url.as_deref())?;
    let presigns = match &blobs {
        AnyBlobStore::Object(_) => true,
        AnyBlobStore::Local(local) => local.url_signer().is_some(),
    };

    let env = startup.env;
    let routes = aspen_express::settings::routes(env)?;
    let packfile_uris = match config.packfile_uris {
        Switch::On => true,
        Switch::Off => false,
        Switch::Auto => presigns || routes.is_some(),
    };

    let max_object = MaxObjectBytes::new(config.max_object_bytes)
        .ok_or("ASPEN_MAX_OBJECT_BYTES must be greater than zero")?;
    let max_inflated = MaxInflatedBytes::new(config.max_inflated_bytes)
        .ok_or("ASPEN_MAX_INFLATED_BYTES must be greater than zero")?;
    let mut server_config = ServerConfig::new(max_object);
    server_config.packfile_uris = packfile_uris;
    server_config.metrics_on_git_listener = config.metrics_on_git_listener;
    if config.shallow_packs {
        // Exact cached shallow packs (C8): builds download a closure and
        // write the new pack here, on the spool's real disk.
        let mut shallow = ShallowCacheConfig::new(config.spool_dir.join("shallow"), max_inflated);
        shallow.max_repo_bytes = config.shallow_max_repo_bytes;
        server_config.shallow_cache = Some(shallow);
    }
    let mut receive = ReceiveConfig::new(&config.spool_dir, max_object, max_inflated);
    receive.push_limits = PushLimits {
        max_objects: config.max_push_objects,
        max_pack_bytes: config.max_push_bytes,
    };
    receive.prefetch_memory_bytes = config.prefetch_memory_bytes;
    // Compaction and commit indexing (C1): the worker started below runs
    // the `compact` and `index_commits` jobs pushes enqueue, and garbage
    // collection. It must lay covers out the way the push path schedules
    // them.
    let compact_dir = config.spool_dir.join("compact");
    std::fs::create_dir_all(&compact_dir)
        .map_err(|e| format!("compaction spool {}: {e}", compact_dir.display()))?;
    let mut compact = CompactConfig::new(compact_dir, max_object, max_inflated);
    compact.layout = receive.cover_layout;
    compact.shallow_unused = config.shallow_unused;
    compact
        .validate()
        .map_err(|e| format!("compaction settings: {e}"))?;
    let mut compactor = Worker::new(
        Compactor::new(meta.clone(), blobs.clone(), compact),
        WorkerOptions::new(worker_id(label)?),
    );

    let mut server = Server::new(meta.clone(), blobs.clone(), server_config)
        .with_receive_pack(receive)
        .map_err(|e| format!("spool directory {}: {e}", config.spool_dir.display()))?
        // Job, queue-lag and wider-trunk gauges, read at most every 30 s (C5).
        .with_store_gauges(STORE_GAUGES_REFRESH);
    if let AnyBlobStore::Local(local) = &blobs
        && local.url_signer().is_some()
    {
        server = server.with_local_blob_route(local.clone());
    }
    let prober = match routes {
        Some(routes) => {
            let routes = Arc::new(routes);
            let prober = routes.spawn_prober();
            server = server.with_express_routes(routes);
            Some(prober)
        }
        None => None,
    };
    if config.express_serve {
        let (keys, express_config) = aspen_express::settings::service(env)?;
        let service = aspen_express::Express::new(blobs.clone(), keys, express_config)?;
        server = server.with_express_route(service);
    }

    // Mirror mode (C4 obligation): the push path forwards proxy-mode pushes
    // and enqueues async ones, and the service started below runs the
    // `mirror_push` jobs and the sync passes. It holds the server weakly,
    // so this is the last builder call, before the server is shared.
    let sync_secs = config.mirror.sync_interval.as_secs();
    let (server, mirror) = server
        .with_mirror(
            SqlMirrors(meta.clone()),
            config.mirror_credentials,
            config.mirror,
        )
        .map_err(|e| format!("mirror mode: {e}"))?;

    // One access decision for HTTP and SSH, so a subject's requests over
    // both draw on the same rate-limit buckets (C3 obligation).
    let auth = AuthService::new(config.policy, SqlTokens(meta.clone()))?
        .with_ssh_keys(SqlSshKeys(meta.clone()));
    let access = Arc::new(
        Access::new(
            auth,
            Limits::new(config.limits),
            meta.clone(),
            config.client_addr,
        )
        .with_instance_limits(config.instance),
    );

    // Bind every listener before announcing any, so a port in use fails
    // the start instead of leaving a half-served process.
    let metrics_listener = match config.metrics_listen {
        Some(at) => Some(
            tokio::net::TcpListener::bind(at)
                .await
                .map_err(|e| format!("binding ASPEN_METRICS_LISTEN {at}: {e}"))?,
        ),
        None => None,
    };
    let ssh = match &config.ssh {
        Some(settings) => {
            let key = host_key(&settings.host_key)?;
            let ssh = SshServer::new(
                server.clone(),
                Arc::clone(&access),
                SshConfig::new(vec![key]),
            )?;
            let listener = tokio::net::TcpListener::bind(settings.listen)
                .await
                .map_err(|e| format!("binding ASPEN_SSH_LISTEN {}: {e}", settings.listen))?;
            Some((ssh, listener))
        }
        None => None,
    };

    let app = router(meta, server.router(Arc::clone(&access)));
    let (stop, stopped) = tokio::sync::watch::channel(false);
    let until_stopped = |mut rx: tokio::sync::watch::Receiver<bool>| async move {
        let _ = rx.wait_for(|s| *s).await;
    };

    tracing::info!("aspen {label}: listening on http://{addr}");
    if startup.warn_unlimited_rates
        && let Some(warning) = crate::config::unlimited_rates_warning(&config.limits)
    {
        tracing::warn!("aspen {label}: {warning}");
    }
    if let Some(url) = &public_url {
        tracing::info!("aspen {label}: clone URLs start with {url}/");
    }
    tracing::info!(
        "aspen {label}: packfile URIs {}, express route {}",
        if packfile_uris { "on" } else { "off" },
        if config.express_serve {
            "mounted at /-/express"
        } else {
            "off"
        },
    );
    let mut tasks = Vec::new();
    if let Some(listener) = metrics_listener {
        tracing::info!(
            "aspen {label}: metrics on http://{}/-/metrics",
            listener.local_addr()?
        );
        let metrics = server.metrics_router();
        let done = until_stopped(stopped.clone());
        tasks.push(tokio::spawn(async move {
            if let Err(e) = aspen_server::listen::serve(
                listener,
                metrics,
                aspen_server::listen::HttpLimits::default(),
                done,
            )
            .await
            {
                tracing::error!("aspen {label}: metrics listener: {e}");
            }
        }));
    }
    let ssh_task = match ssh {
        Some((ssh, listener)) => {
            tracing::info!("aspen {label}: Git over SSH on {}", listener.local_addr()?);
            Some(tokio::spawn(async move {
                if let Err(e) = ssh.serve(listener).await {
                    tracing::error!("aspen {label}: SSH listener: {e}");
                }
            }))
        }
        None => None,
    };
    tracing::info!("aspen {label}: mirror sync passes every {sync_secs} s");
    let mirror_task = tokio::spawn(mirror.run(worker_id(label)?));
    // Every instance runs a worker: jobs are leased, and garbage collection
    // is safe on several workers at once (pack generations, issue #48).
    let compact_task = if config.background_worker {
        tracing::info!("aspen {label}: compaction and indexing worker running");
        tokio::spawn(async move {
            compactor
                .run(std::future::pending(), |e| {
                    tracing::warn!("aspen {label}: background worker: {e}");
                })
                .await;
        })
    } else {
        tracing::warn!(
            "aspen {label}: background worker off: no compaction, commit indexing or \
             garbage collection"
        );
        tokio::spawn(async {})
    };
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = stop.send(true);
    });
    // Not `axum::serve`: it gives hyper no timer, so request headers
    // would never time out (decisions log, D4).
    aspen_server::listen::serve(
        listener,
        app,
        aspen_server::listen::HttpLimits::default(),
        until_stopped(stopped),
    )
    .await?;
    // SSH sessions have no graceful drain: a stopped server closes them.
    if let Some(t) = ssh_task {
        t.abort();
    }
    for t in tasks {
        let _ = t.await;
    }
    // Every mirror step is a compare-and-swap on both sides, so a pass cut
    // short here leaves nothing half-written; the next start redoes it.
    mirror_task.abort();
    // Jobs are leased and idempotent, so a job cut short here is reclaimed
    // after its lease by any worker and redone.
    compact_task.abort();
    if let Some(p) = prober {
        p.abort();
    }
    tracing::info!("aspen {label}: stopped");
    Ok(())
}

/// How often the database gauges (`aspen_jobs`, queue lag, wider trunks)
/// may be read for `/-/metrics` (C5: 30 seconds).
const STORE_GAUGES_REFRESH: Duration = Duration::from_secs(30);

/// Reads the SSH host key. An inline key comes from a secret store, so
/// errors never include it.
fn host_key(source: &HostKey) -> Result<PrivateKey, BoxError> {
    match source {
        // Read once through the secret-file check (issue #125), then
        // decoded like the inline key; errors never include the key.
        HostKey::File(path) => {
            let shown = path.display();
            let text = crate::secret_file::read(path)
                .map_err(|e| format!("ASPEN_SSH_HOST_KEY_FILE: {shown}: {e}"))?;
            PrivateKey::from_openssh(text.as_bytes()).map_err(|e| {
                format!("ASPEN_SSH_HOST_KEY_FILE: {shown}: not an OpenSSH private key ({e})").into()
            })
        }
        HostKey::Inline(text) => PrivateKey::from_openssh(text.as_bytes())
            .map_err(|e| format!("ASPEN_SSH_HOST_KEY: not an OpenSSH private key ({e})").into()),
    }
}

/// Health routes in front of the server's own router.
fn router(meta: SqlStore, server: Router) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route("/readyz", get(readyz))
        .with_state(Arc::new(Readiness::new(meta, READY_CACHE)))
        .fallback_service(server)
}

/// How long one `/readyz` database check answers for.
const READY_CACHE: Duration = Duration::from_secs(1);

/// The readiness check, answered from the last database check while it is
/// fresh: `/readyz` is on the public listener and needs no credential, so
/// without this a flood of probes would take one pooled database
/// connection each, away from Git requests (decisions log, D4).
/// Concurrent probes share one check.
struct Readiness {
    meta: SqlStore,
    fresh_for: Duration,
    last: tokio::sync::Mutex<Option<(std::time::Instant, bool)>>,
}

impl Readiness {
    fn new(meta: SqlStore, fresh_for: Duration) -> Self {
        Self {
            meta,
            fresh_for,
            last: tokio::sync::Mutex::new(None),
        }
    }

    async fn ready(&self) -> bool {
        let mut last = self.last.lock().await;
        if let Some((at, ready)) = *last
            && at.elapsed() < self.fresh_for
        {
            return ready;
        }
        let ready = sqlx::query("SELECT 1")
            .execute(self.meta.pool())
            .await
            .is_ok();
        *last = Some((std::time::Instant::now(), ready));
        ready
    }
}

/// `200 ok` once the metadata database answers a query, `503` otherwise.
/// Load balancers and Kubernetes route traffic only to ready instances;
/// liveness (`/healthz`) does not depend on the database, so a database
/// outage does not restart every instance at once.
async fn readyz(State(readiness): State<Arc<Readiness>>) -> (StatusCode, &'static str) {
    if readiness.ready().await {
        (StatusCode::OK, "ok\n")
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "metadata database unavailable\n",
        )
    }
}

/// Opens `ASPEN_BLOB_URL`. A `file://` store with a secret and a public URL
/// presigns `<public>/-/blobs/...` URLs that this server serves.
pub(crate) fn open_blobs(
    url: &str,
    secret: Option<&[u8]>,
    public_url: Option<&str>,
) -> Result<AnyBlobStore, BoxError> {
    let store = aspen_blob::open(url).map_err(|e| format!("ASPEN_BLOB_URL: {e}"))?;
    Ok(match (store, secret, public_url) {
        (AnyBlobStore::Local(local), Some(secret), Some(public)) => {
            let signer = LocalUrlSigner::new(&format!("{public}/-/blobs"), secret)
                .map_err(|e| format!("ASPEN_BLOB_URL_SECRET: {e}"))?;
            AnyBlobStore::Local(LocalBlobStore::open(local.root())?.with_url_signer(signer))
        }
        (store, _, _) => store,
    })
}

/// A job-queue worker name unique to this process: containers all run as
/// PID 1, so the host name and a random suffix tell replicas apart.
fn worker_id(label: &str) -> Result<String, BoxError> {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".into());
    let nonce: String = random_secret()?[..4]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok(format!(
        "aspen-{label}:{host}:{}:{nonce}",
        std::process::id()
    ))
}

fn random_secret() -> Result<Vec<u8>, BoxError> {
    let mut secret = vec![0u8; 32];
    getrandom::fill(&mut secret).map_err(|e| format!("random secret: {e}"))?;
    Ok(secret)
}

/// Default settings for a new repository: trunk `refs/heads/main`.
pub(crate) fn default_settings() -> Result<RepoSettings, BoxError> {
    Ok(RepoSettings::new(RefName::new("refs/heads/main")?)?)
}

/// Creates `name` unless it exists. Returns whether it was created.
pub(crate) async fn create_repo_if_missing(
    meta: &SqlStore,
    name: &RepoName,
    settings: &RepoSettings,
) -> Result<bool, BoxError> {
    match meta.create_repo(name, settings).await {
        Ok(_) => Ok(true),
        Err(StoreError::Conflict { .. }) => Ok(false),
        Err(e) => Err(format!("creating repository {name}: {e}").into()),
    }
}

/// Resolves on SIGTERM (what Docker, ECS and Kubernetes send) or Ctrl-C.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
    }
    tracing::info!("aspen: shutting down; finishing requests in flight");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readyz_follows_the_metadata_database() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("m.db").display());
        runtime.block_on(async {
            let meta = SqlStore::connect(&url).await.unwrap();
            let fresh_for = Duration::from_millis(300);
            let readiness = Arc::new(Readiness::new(meta.clone(), fresh_for));
            assert_eq!(readyz(State(readiness.clone())).await.0, StatusCode::OK);
            meta.pool().close().await;
            // Within the cache window the last answer stands: probes do
            // not reach the database (D4).
            assert_eq!(readyz(State(readiness.clone())).await.0, StatusCode::OK);
            tokio::time::sleep(fresh_for).await;
            assert_eq!(
                readyz(State(readiness)).await.0,
                StatusCode::SERVICE_UNAVAILABLE,
                "a database that does not answer makes the instance unready"
            );
        });
    }
}
