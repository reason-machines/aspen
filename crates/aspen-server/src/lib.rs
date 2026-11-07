//! Aspen's server: Git over smart HTTP (axum) and SSH (russh), and the
//! wiring of the storage backends.
//!
//! This crate is one of the two composition roots (`docs/PLAN.md`, section
//! 2): it picks the concrete metadata store (`aspen-meta`) and blob store
//! (`aspen-blob`) and wires them, through the `aspen-core` traits, into the
//! protocol (`aspen-proto`), the planner (`aspen-planner`), the stitcher
//! (`aspen-pack`) and access control (`aspen-auth`).
//!
//! | Module | What it serves |
//! |---|---|
//! | [`access`] | Authentication, authorization, repository routing, rate limits and push quotas, as one middleware ([`access::guard`]) in front of every Git endpoint (node B3) |
//! | [`admission`] | How many fetches, pushes and stitches run at once, per caller and per instance (issue #118) |
//! | [`receive`] | `git-receive-pack` (node B1): the push advertisement (trunk, the scoped ref, `.have` tips) and the push path (ingest, one pack per ref write, the commit check, compare-and-swap) |
//! | [`upload_pack`] | `git-upload-pack`: protocol v2 (`ls-refs`, `fetch` with planning, `shallow-info`, packfile URIs, the stitched pack) and v0/v1 for clients without v2 fetch |
//! | [`blobs`] | `/-/blobs/<key>`: downloads of presigned local-store URLs, with `Range` and the empty-`206` rule |
//! | [`express`] | Packfile URIs on express and CDN endpoints, and the in-process express service at `/-/express` (node C2) |
//! | [`metrics`] | Prometheus metrics at `/-/metrics`: fetches (including the planner's `PlanStats.visited`), pushes, and the job queue and trunk closures read from the database (node C5) |
//! | [`ssh`] | Git over SSH (node C3): public-key authentication against registered keys, one `git-upload-pack` or `git-receive-pack` per session, the same access decision ([`access::Access::admit`]) and protocol code as HTTP |
//! | [`mirror`] | Mirror mode (node C4): `aspen-mirror` over this server's stores, push path and planner |
//!
//! [`Server::router`] serves the Git endpoints behind [`access::guard`]:
//! a Git handler runs only with the [`access::Authorized`] extension the
//! guard adds, and takes the repository, the endpoint and the branch scope
//! from it, never from the path again. `/-/blobs/...` and
//! `/-/express/...` are not Git endpoints and are mounted outside the
//! guard (the blob and express routes check their own URL signatures).
//! Pushes are served once the server is given a receive-pack
//! configuration ([`Server::with_receive_pack`]); a server without one
//! answers a push with `501 Not Implemented`.
//!
//! `/-/metrics` is served by [`Server::metrics_router`], meant for a
//! separate listener that only the monitoring network reaches. It joins
//! the Git router only when [`ServerConfig::metrics_on_git_listener`] is
//! set (local development), because the Git listener is the public one.
//!
//! The server logs and traces through `tracing`: every request to the Git
//! router, those the guard refuses included, runs in a `git_request` span
//! and ends with one `info` event (an access log);
//! pushes, fetch plans and paging conditions log their own events. The
//! binary installs the subscriber and the OpenTelemetry exporter
//! (`aspen-cli`; `docs/OBSERVABILITY.md`).

#![deny(missing_docs)]

pub mod access;
pub mod admission;
pub mod blobs;
mod body;
mod conn;
pub mod express;
#[cfg(feature = "fuzzing")]
pub mod fuzzing;
pub mod listen;
pub mod metrics;
pub mod mirror;
pub mod queue;
pub mod receive;
pub mod ssh;
#[cfg(feature = "testing")]
pub mod testing;
pub mod upload_pack;

use std::sync::Arc;
use std::time::Duration;

use access::{Access, Authorized, Endpoint};
use aspen_auth::{SshKeyStore, TokenStore};
use aspen_blob::{LocalBlobStore, RECOMMENDED_MAX_PRESIGN_TTL};
use aspen_core::blob::{BlobCaller, BlobStore};
use aspen_core::meta::MetaStore;
use aspen_express::{Express, Routes};
use aspen_pack::MaxObjectBytes;
use aspen_pack::locate::{LocateOptions, StoredPacks};
use aspen_proto::caps::Agent;
use aspen_proto::v2::V2Limits;
use axum::Router;
use axum::body::Body;
use axum::extract::{Extension, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tracing::Instrument as _;

pub use metrics::Metrics;

/// Server settings. There is no `Default`: the object limit must come from
/// configuration, as for ingest (`aspen_pack::MaxObjectBytes`).
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The `agent=` value advertised.
    pub agent: Agent,
    /// Advertise `packfile-uris` (S3). Turn it on only when the blob store
    /// presigns URLs: an S3 store, or a local store with a URL signer served
    /// by [`Server::with_local_blob_route`]. A pack whose URL cannot be
    /// presigned is stitched into the inline pack instead, so a wrong
    /// setting costs bytes, never a failed fetch.
    pub packfile_uris: bool,
    /// Lifetime of each presigned pack URL. At most
    /// [`RECOMMENDED_MAX_PRESIGN_TTL`] (A4 decisions row).
    pub presign_ttl: Duration,
    /// Soft cap on packfile URIs per response; planned packs beyond it are
    /// stitched into the inline pack (S3).
    pub max_uris_per_response: usize,
    /// Limits on one protocol v2 request.
    pub v2_limits: V2Limits,
    /// Largest fetch request body accepted, compressed or not, in bytes.
    /// Bodies are pkt-lines only (wants, haves, prefixes), so this is
    /// [`V2Limits::max_request_bytes`] (8 MiB) plus framing; a larger
    /// limit only lets a request buffer bytes the parser would refuse.
    pub max_request_body: usize,
    /// Largest object read from a stored pack outside the stitcher (a
    /// commit not yet in `commit_index`, an annotated tag to peel).
    pub max_object_bytes: MaxObjectBytes,
    /// Bytes of each pack blob read while stitching; a stitch keeps 16
    /// such reads in flight.
    pub stitch_read_bytes: u64,
    /// Most ref versions one fetch may load to plan. A full clone loads
    /// every version of the repository (the want side of the planner's walk
    /// is linear in versions), at roughly 200 bytes each; a fetch over the limit
    /// gets an `ERR`, not a corruption alert.
    pub max_plan_versions: usize,
    /// Most commits one shallow request may walk to compute `shallow-info`
    /// (about 100 bytes each); a request over it gets an `ERR`.
    pub max_shallow_commits: usize,
    /// Most `refs/tags/` refs scanned to resolve wants that have no
    /// `commit_index` row (annotated tags). Schema v1 cannot look a ref up
    /// by its tip, so each such request costs one scan; a want still
    /// unresolved past the limit gets an `ERR`.
    pub max_tag_scan: usize,
    /// Also serve `/-/metrics` on the Git router ([`Server::router`]).
    /// Off by default: the Git listener is the public endpoint, and the
    /// metrics show load and repository counts to whoever can reach them.
    /// Serve [`Server::metrics_router`] on its own listener instead, and
    /// turn this on only where the Git listener is private (`aspen dev`).
    pub metrics_on_git_listener: bool,
    /// Longest a read of the database gauges ([`Server::with_store_gauges`])
    /// may take. One that takes longer is abandoned: the scrape gets the
    /// last values with `aspen_meta_stats_up` 0, so a hung database cannot
    /// stall every scrape.
    pub store_gauges_timeout: Duration,
    /// Exact cached shallow packs (node C8,
    /// [`upload_pack::exact`]): `None` serves every shallow fetch the full
    /// closure of its wants, as before C8.
    pub shallow_cache: Option<upload_pack::exact::ShallowCacheConfig>,
    /// A Git request body (a push's pack, a fetch's wants and haves)
    /// fails once the client has sent nothing for this long while the
    /// server was waiting for it, so a stalled or deliberately slow client
    /// cannot hold a request, and for a push a blocking thread, forever.
    /// Time the server spends not reading does not count.
    pub body_idle_timeout: Duration,
    /// A Git request body fails once it has arrived slower than this many
    /// bytes per second on average, over the time the server waited for
    /// it, after the first [`ServerConfig::body_rate_grace`] of waiting
    /// (issue #118), so a client that sends one byte just before every
    /// idle timeout cannot hold a request either. `0` turns the rule off.
    pub body_min_bytes_per_sec: u64,
    /// How long the server waits for a body before
    /// [`ServerConfig::body_min_bytes_per_sec`] applies.
    pub body_rate_grace: Duration,
    /// Most refs `ls-refs` keeps in memory across repositories
    /// (`upload_pack::ref_snapshot`), so a listing asks the store only what
    /// changed since the last one instead of scanning every ref; `0` lists
    /// every ref from the store on every request. Never stale: each
    /// request is answered from a store read that began after it arrived.
    pub ls_refs_cache_refs: usize,
}

impl ServerConfig {
    /// Defaults with this object limit: packfile URIs off, 15-minute
    /// presigned URLs, 64 URIs per response, A1's request limits, 9 MiB
    /// fetch request bodies, 256 KiB stitching reads, and a million ref versions
    /// per plan, a million commits per shallow walk and 100,000 tags per
    /// tag-want scan; `/-/metrics` only on [`Server::metrics_router`],
    /// 5 seconds for a read of the database gauges, and request bodies
    /// idle for at most 60 seconds and arriving at 1 KiB/s or faster on
    /// average after 10 seconds.
    pub fn new(max_object_bytes: MaxObjectBytes) -> Self {
        Self {
            agent: Agent::aspen(),
            packfile_uris: false,
            presign_ttl: Duration::from_secs(15 * 60).min(RECOMMENDED_MAX_PRESIGN_TTL),
            max_uris_per_response: 64,
            v2_limits: V2Limits::default(),
            max_request_body: 9 << 20,
            max_object_bytes,
            stitch_read_bytes: 256 << 10,
            max_plan_versions: 1_000_000,
            max_shallow_commits: 1_000_000,
            max_tag_scan: 100_000,
            metrics_on_git_listener: false,
            store_gauges_timeout: Duration::from_secs(5),
            shallow_cache: None,
            body_idle_timeout: Duration::from_secs(60),
            body_min_bytes_per_sec: 1024,
            body_rate_grace: Duration::from_secs(10),
            ls_refs_cache_refs: DEFAULT_LS_REFS_CACHE_REFS,
        }
    }
}

/// Default of [`ServerConfig::ls_refs_cache_refs`]: about 40 MB of ref
/// names and tips at most.
pub const DEFAULT_LS_REFS_CACHE_REFS: usize = 250_000;

/// The server: shared state behind every request. Cheap to clone.
///
/// `M` and `B` are the concrete stores the composition root picked
/// (`aspen_meta::SqlStore`, `aspen_blob::AnyBlobStore`); they must be cheap
/// to clone (both share their pools).
#[derive(Debug)]
pub struct Server<M, B> {
    inner: Arc<Inner<M, B>>,
}

impl<M, B> Clone for Server<M, B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug)]
struct Inner<M, B> {
    meta: M,
    blobs: B,
    config: ServerConfig,
    metrics: Metrics,
    /// Reads single objects (unindexed commits, tags) from stored packs.
    objects: StoredPacks<M, B>,
    /// Serves `/-/blobs/` for a local store's presigned URLs.
    local_blobs: Option<LocalBlobStore>,
    /// Serves pushes, once configured.
    receive: Option<Arc<receive::ReceivePack<M, B>>>,
    /// Signs packfile URIs on express and CDN endpoints (C2).
    express_routes: Option<Arc<Routes>>,
    /// Serves express URLs at `/-/express` (C2).
    express: Option<Express<B>>,
    /// Job-queue and trunk-closure gauges read from the database (C5).
    store_gauges: Option<metrics::StoreGauges>,
    /// Mirror mode's counters, once set up ([`Server::with_mirror`]).
    mirror_metrics: Option<Arc<aspen_mirror::Metrics>>,
    /// Garbage collection's counters ([`Server::with_gc_metrics`]).
    gc_metrics: Option<Arc<aspen_compact::GcMetrics>>,
    /// The client-address source of the first [`Server::router`]'s
    /// access layer, whose fallback counters join `/-/metrics`.
    client_addr: std::sync::OnceLock<access::ClientAddr>,
    /// The router's admission, for its memory-pressure metrics.
    admission: std::sync::OnceLock<Arc<admission::Admission>>,
    shallow_cache: Option<upload_pack::exact::ShallowCache<M, B>>,
    /// Fetches' compaction heat, written to the job queue in batches.
    heat: upload_pack::heat::HeatNotes,
    /// `ls-refs` listings held in memory; `None` when
    /// [`ServerConfig::ls_refs_cache_refs`] is 0.
    ref_snapshots: Option<upload_pack::ref_snapshot::RefSnapshots>,
}

impl<M, B> Server<M, B>
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    /// Records one served fetch's compaction heat and, when a write is due
    /// ([`upload_pack::heat::HEAT_FLUSH_EVERY`]), writes what this process
    /// summed from a spawned task, off the fetch's path.
    pub(crate) fn note_compaction_heat(&self, heat: Vec<aspen_core::meta::CompactionHeat>) {
        let versions: u64 = heat.iter().map(|h| h.heat).sum();
        self.inner.metrics.uncompacted_versions(versions);
        let Some(due) = self.inner.heat.add(heat) else {
            return;
        };
        let server = self.clone();
        tokio::spawn(async move {
            let inner = &server.inner;
            if let Err(e) = upload_pack::heat::write(&inner.meta, &inner.heat, due).await {
                inner.metrics.heat_write_failure();
                tracing::warn!(error = %e, "recording compaction heat failed");
            }
        });
    }

    /// A server over these stores.
    pub fn new(meta: M, blobs: B, config: ServerConfig) -> Self {
        let objects = StoredPacks::new(
            meta.clone(),
            blobs.clone(),
            LocateOptions::new(config.max_object_bytes),
        );
        let shallow_cache = config.shallow_cache.clone().map(|c| {
            upload_pack::exact::ShallowCache::new(
                meta.clone(),
                blobs.clone(),
                c,
                config.max_object_bytes,
            )
        });
        let ref_snapshots = (config.ls_refs_cache_refs > 0)
            .then(|| upload_pack::ref_snapshot::RefSnapshots::new(config.ls_refs_cache_refs));
        Self {
            inner: Arc::new(Inner {
                meta,
                blobs,
                config,
                metrics: Metrics::default(),
                objects,
                local_blobs: None,
                receive: None,
                express_routes: None,
                express: None,
                store_gauges: None,
                mirror_metrics: None,
                gc_metrics: None,
                client_addr: std::sync::OnceLock::new(),
                admission: std::sync::OnceLock::new(),
                shallow_cache,
                heat: upload_pack::heat::HeatNotes::default(),
                ref_snapshots,
            }),
        }
    }

    /// Also exports the counters of this process's garbage collection
    /// (`aspen_gc_*`; the background worker records them). Call before
    /// [`Server::router`] and before cloning.
    pub fn with_gc_metrics(mut self, metrics: Arc<aspen_compact::GcMetrics>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("with_gc_metrics is called before the server is shared")
            .gc_metrics = Some(metrics);
        self
    }

    /// Also serves downloads of `store`'s presigned URLs at `/-/blobs/`
    /// (A4 obligation for B2). `store` must have a URL signer whose base is
    /// this server's `/-/blobs` URL, and be the same store as `B`. Call
    /// before [`Server::router`] and before cloning.
    pub fn with_local_blob_route(mut self, store: LocalBlobStore) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("with_local_blob_route is called before the server is shared")
            .local_blobs = Some(store);
        self
    }

    /// Also serves pushes (`git-receive-pack`, node B1) with `config`, over
    /// this server's stores. Creates the spool directory. Call before
    /// [`Server::router`] and before cloning.
    pub fn with_receive_pack(mut self, config: receive::ReceiveConfig) -> std::io::Result<Self> {
        let inner = Arc::get_mut(&mut self.inner)
            .expect("with_receive_pack is called before the server is shared");
        inner.receive = Some(receive::ReceivePack::new(
            inner.meta.clone(),
            inner.blobs.clone(),
            config,
        )?);
        Ok(self)
    }

    /// Signs packfile URIs on `routes`' express and CDN endpoints before
    /// falling back to the blob store's URLs and the stitched pack
    /// ([`express`]). Needs [`ServerConfig::packfile_uris`]. Run
    /// [`Routes::spawn_prober`] beside the server so an endpoint outage
    /// moves fetches to the next tier or the stitched pack. Call before
    /// [`Server::router`] and before cloning.
    pub fn with_express_routes(mut self, routes: Arc<Routes>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("with_express_routes is called before the server is shared")
            .express_routes = Some(routes);
        self
    }

    /// Also serves express URLs at [`express::EXPRESS_PATH`] with
    /// `service`, which should read from this server's blob store. Its
    /// counters join `/-/metrics`. Call before [`Server::router`] and
    /// before cloning.
    pub fn with_express_route(mut self, service: Express<B>) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("with_express_route is called before the server is shared")
            .express = Some(service);
        self
    }

    /// The axum router serving every Git and blob URL. Git endpoints pass
    /// [`access::guard`] over `access` (whose metadata store should be this
    /// server's); `/-/blobs/...` does not. `/-/metrics` is here only with
    /// [`ServerConfig::metrics_on_git_listener`]; otherwise it is a
    /// non-Git path and the guard answers `404`. Serve it with
    /// `into_make_service_with_connect_info::<SocketAddr>()` so anonymous
    /// callers are rate-limited per address. The first router's access
    /// layer reports its client-address fallbacks at `/-/metrics`.
    pub fn router<T, K>(&self, access: Arc<Access<M, T, K>>) -> Router
    where
        T: TokenStore + 'static,
        K: SshKeyStore + 'static,
    {
        let _ = self.inner.client_addr.set(access.client_addr().clone());
        let _ = self.inner.admission.set(Arc::clone(access.admission()));
        let git = Router::new()
            .fallback(serve_git::<M, B>)
            .with_state(self.clone())
            .layer(axum::middleware::from_fn_with_state(
                access,
                access::guard::<M, T, K>,
            ))
            .layer(axum::middleware::from_fn_with_state(
                body::BodyPace {
                    idle: self.inner.config.body_idle_timeout,
                    min_bytes_per_sec: self.inner.config.body_min_bytes_per_sec,
                    grace: self.inner.config.body_rate_grace,
                },
                idle_bodies,
            ))
            // Outside the guard, so requests it refuses are logged too.
            .layer(axum::middleware::from_fn(access_log));
        let mut router = Router::new().route("/-/blobs/{*key}", get(serve_blob::<M, B>));
        if self.inner.config.metrics_on_git_listener {
            router = router.route(METRICS_PATH, get(serve_metrics::<M, B>));
        }
        if let Some(service) = &self.inner.express {
            router = router.nest_service(express::EXPRESS_PATH, service.router());
        }
        router.with_state(self.clone()).fallback_service(git)
    }

    /// A router serving only `GET /-/metrics`, for a listener of its own
    /// that only the monitoring network reaches (every other path is
    /// `404`).
    pub fn metrics_router(&self) -> Router {
        Router::new()
            .route(METRICS_PATH, get(serve_metrics::<M, B>))
            .with_state(self.clone())
    }

    /// The Prometheus exposition `/-/metrics` serves: build info, fetch,
    /// push, database and express families. Reads the database gauges
    /// first when they are due ([`Server::with_store_gauges`]).
    pub async fn render_metrics(&self) -> String {
        let mut text = String::new();
        metrics::render_build_info(&mut text);
        metrics::render_blob_requests(&mut text, aspen_blob::RequestCounts::global());
        text.push_str(&self.metrics().render());
        if let Some(receive) = &self.inner.receive {
            receive.render_metrics(&mut text);
        }
        if let Some(gauges) = &self.inner.store_gauges {
            gauges.render(&mut text).await;
        }
        if let Some(routes) = &self.inner.express_routes {
            routes.render(&mut text);
        }
        if let Some(service) = &self.inner.express {
            service.metrics().render(&mut text);
        }
        if let Some(mirror) = &self.inner.mirror_metrics {
            mirror.render(&mut text);
        }
        if let Some(gc) = &self.inner.gc_metrics {
            gc.render(&mut text);
        }
        self.inner
            .client_addr
            .get()
            .unwrap_or(&access::ClientAddr::Peer)
            .render_metrics(&mut text);
        if let Some(admission) = self.inner.admission.get() {
            admission.render_metrics(&mut text);
        }
        text
    }
}

/// Where the metrics are served.
pub const METRICS_PATH: &str = "/-/metrics";

impl<B> Server<aspen_meta::SqlStore, B> {
    /// Also exports gauges read from the metadata database: background
    /// jobs per kind and state (dead jobs page), queue lag, and how many
    /// repositories' current trunk versions are wider
    /// ([`metrics::StoreGauges`]). They are read when `/-/metrics` is
    /// scraped, at most once per `refresh` (30 seconds is a good value;
    /// zero reads on every scrape), and each read is bounded by
    /// [`ServerConfig::store_gauges_timeout`]. Call before
    /// [`Server::router`] and before cloning.
    pub fn with_store_gauges(mut self, refresh: Duration) -> Self {
        let inner = Arc::get_mut(&mut self.inner)
            .expect("with_store_gauges is called before the server is shared");
        inner.store_gauges = Some(metrics::StoreGauges::new(
            inner.meta.clone(),
            refresh,
            inner.config.store_gauges_timeout,
        ));
        self
    }
}

impl<M, B> Server<M, B> {
    /// Counters for this server's fetches.
    pub fn metrics(&self) -> &Metrics {
        &self.inner.metrics
    }

    /// The receive-pack service, when configured
    /// ([`Server::with_receive_pack`]).
    pub fn receive_pack(&self) -> Option<&Arc<receive::ReceivePack<M, B>>> {
        self.inner.receive.as_ref()
    }

    pub(crate) fn meta(&self) -> &M {
        &self.inner.meta
    }

    pub(crate) fn blobs(&self) -> &B {
        &self.inner.blobs
    }

    pub(crate) fn config(&self) -> &ServerConfig {
        &self.inner.config
    }

    pub(crate) fn ref_snapshots(&self) -> Option<&upload_pack::ref_snapshot::RefSnapshots> {
        self.inner.ref_snapshots.as_ref()
    }

    pub(crate) fn objects(&self) -> &StoredPacks<M, B> {
        &self.inner.objects
    }

    pub(crate) fn shallow_cache(&self) -> Option<&upload_pack::exact::ShallowCache<M, B>> {
        self.inner.shallow_cache.as_ref()
    }

    pub(crate) fn express_routes(&self) -> Option<&Routes> {
        self.inner.express_routes.as_deref()
    }
}

/// `GET /-/metrics`. Unauthenticated, like most Prometheus endpoints, so
/// it is served on its own listener ([`Server::metrics_router`]) unless
/// [`ServerConfig::metrics_on_git_listener`] says otherwise.
async fn serve_metrics<M, B>(State(server): State<Server<M, B>>) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let text = server.render_metrics().await;
    ([("content-type", metrics::CONTENT_TYPE)], text).into_response()
}

/// `GET /-/blobs/<key>?expires=..&signature=..`: the URL's signature is the
/// credential ([`blobs`]).
async fn serve_blob<M, B>(State(server): State<Server<M, B>>, request: Request) -> Response {
    match &server.inner.local_blobs {
        Some(store) => blobs::serve(store, request).await,
        None => plain(StatusCode::NOT_FOUND, "not found"),
    }
}

/// Every Git endpoint, behind [`access::guard`]: the repository, the
/// endpoint and the scope come from [`Authorized`], which only the guard
/// adds (without it the extractor fails and nothing is served).
async fn serve_git<M, B>(
    State(server): State<Server<M, B>>,
    Extension(authorized): Extension<Authorized>,
    request: Request,
) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    // The guard let the request in: name what it resolved in the
    // `git_request` span `access_log` opened.
    let span = tracing::Span::current();
    span.record("repo", tracing::field::display(&authorized.repo.name));
    span.record("endpoint", endpoint_name(authorized.route.endpoint));
    if let Some(scope) = &authorized.route.scope {
        span.record("scope", scope.as_str());
    }
    serve_endpoint(&server, &authorized, request).await
}

/// Wraps every Git request body in [`body::IdleBody`]
/// ([`ServerConfig::body_idle_timeout`],
/// [`ServerConfig::body_min_bytes_per_sec`]).
async fn idle_bodies(
    axum::extract::State(pace): axum::extract::State<body::BodyPace>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    next.run(request.map(|b| axum::body::Body::new(body::IdleBody::new(b, pace))))
        .await
}

/// The access log of the Git router: one `info` event per request, those
/// the guard refuses (`401`, `403`, `404`, `429`) included, inside a
/// `git_request` span that the Git handler fills with the repository,
/// endpoint and branch scope once the guard admits the request. Only the
/// method and the path are taken from the request: no headers (so no
/// credentials) and no query string. Streamed bodies (fetch packs)
/// continue after the event; their failures log on their own.
async fn access_log(request: Request, next: axum::middleware::Next) -> Response {
    let started = std::time::Instant::now();
    let span = tracing::info_span!(
        "git_request",
        method = %request.method(),
        path = request.uri().path(),
        repo = tracing::field::Empty,
        endpoint = tracing::field::Empty,
        scope = tracing::field::Empty,
    );
    let response = next.run(request).instrument(span.clone()).await;
    tracing::info!(
        parent: &span,
        status = response.status().as_u16(),
        ms = started.elapsed().as_millis() as u64,
        "git request"
    );
    response
}

fn endpoint_name(e: Endpoint) -> &'static str {
    match e {
        Endpoint::UploadPackAdvertisement => "upload_pack_advertisement",
        Endpoint::UploadPack => "upload_pack",
        Endpoint::ReceivePackAdvertisement => "receive_pack_advertisement",
        Endpoint::ReceivePack => "receive_pack",
    }
}

async fn serve_endpoint<M, B>(
    server: &Server<M, B>,
    authorized: &Authorized,
    request: Request,
) -> Response
where
    M: MetaStore + Clone + 'static,
    B: BlobStore + Clone + 'static,
{
    let (parts, body) = request.into_parts();
    let Authorized { repo, route, .. } = authorized;
    match route.endpoint {
        Endpoint::UploadPackAdvertisement => {
            BlobCaller::Fetch
                .scope(upload_pack::advertise(server, repo, route, &parts))
                .await
        }
        Endpoint::UploadPack => {
            BlobCaller::Fetch
                .scope(upload_pack::serve(server, repo, authorized, &parts, body))
                .await
        }
        Endpoint::ReceivePackAdvertisement | Endpoint::ReceivePack => match server.receive_pack() {
            Some(svc) => receive::http::serve(svc, authorized, &parts, body).await,
            None => plain(
                StatusCode::NOT_IMPLEMENTED,
                "push (git-receive-pack) is not configured on this server",
            ),
        },
    }
}

pub(crate) fn plain(status: StatusCode, text: &str) -> Response {
    (
        status,
        [("content-type", "text/plain; charset=utf-8")],
        Body::from(format!("{text}\n")),
    )
        .into_response()
}
