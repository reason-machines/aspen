//! The back end of `place` (plan node C7): a shared pixel canvas stored in
//! a Git repository served by Aspen, where every brush stroke is a push.
//!
//! `aspen-place` is an application built on Aspen, and a composition root
//! like `aspen-server` and `aspen-cli` (`cargo xtask check-deps`): it
//! picks the stores and wires Aspen's server with one extra piece, a push
//! hook ([`hook::PlaceHook`]) that runs the tile rules of
//! `demo/place/docs/TILES.md` inside receive-pack and puts every accepted
//! push on the canvas.
//!
//! | Module | What |
//! |---|---|
//! | [`rules`] | The tile format and push rules (the Rust twin of `src/shared/tiles.ts`) |
//! | [`image`] | The canvas as an SVG for READMEs, rendered once per `main` commit |
//! | [`hook`] | The pre-receive check over each update's exact commit range, and the post-receive step |
//! | [`place`] | Writes: web and agent strokes, the merger bot, branch expiry, the swarm and the kill switch |
//! | [`sprites`] | The pixel sprites ambient agents paint |
//! | [`personas`] | Pretend painters who drop in on shifts and paint pictures tile by tile |
//! | [`state`] | The canvas, branch overlays, the merge queue and the event log the page reads |
//! | [`api`] | `demo/place/api/openapi.yaml` over HTTP, server-sent events included |
//! | [`git`] | The Git objects the server writes, and in-process push requests |
//! | [`limits`] | Rolling-window rate limits |
//! | [`defaults`] | The limits the `aspen-place` binary runs Aspen with |
//!
//! [`build`] assembles all of it over any stores; the `aspen-place` binary
//! reads its settings from the environment (`demo/place/server/README.md`).

#![deny(missing_docs)]

pub mod api;
pub mod defaults;
pub mod git;
pub mod hook;
pub mod image;
pub mod limits;
pub mod personas;
pub mod place;
pub mod rules;
pub mod sprites;
pub mod state;

use std::path::PathBuf;
use std::sync::Arc;

use aspen_auth::{AuthService, LimitsConfig, PolicyConfig, TokenStore};
use aspen_compact::{CompactConfig, Compactor, Worker, WorkerOptions};
use aspen_core::blob::BlobStore;
use aspen_core::meta::MetaStore;
use aspen_core::name::RepoName;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::access::{Access, ClientAddr};
use aspen_server::admission::InstanceLimits;
use aspen_server::receive::ReceiveConfig;
use aspen_server::upload_pack::exact::ShallowCacheConfig;
use aspen_server::{Server, ServerConfig};

pub use place::{Place, PlaceConfig, PlaceError};

/// Server-side settings besides the canvas's.
#[derive(Debug, Clone)]
pub struct ServerSettings {
    /// Where pushes are spooled (a real disk).
    pub spool_dir: PathBuf,
    /// The largest object any step reads.
    pub max_object_bytes: MaxObjectBytes,
    /// The most one push may inflate to.
    pub max_inflated_bytes: MaxInflatedBytes,
    /// Who may read and push, and the operators (an `admin` grant on the
    /// canvas repository).
    pub policy: PolicyConfig,
    /// Request rates and push sizes per caller.
    pub limits: LimitsConfig,
    /// Where client addresses come from.
    pub client_addr: ClientAddr,
    /// Git requests running at once on this instance, and the resident
    /// memory over which new ones are refused with `503`
    /// ([`defaults::instance_limits`]). The server's own pushes (the
    /// merger, web strokes, agents) do not pass through it.
    pub instance: InstanceLimits,
}

/// A running `place`: the service, the router serving Git
/// (`/<repo>.git/...`) and the API (`/api/v1/...`), and Aspen's background
/// worker.
#[derive(Debug)]
pub struct Built<M, B> {
    /// The service; run [`Place::run`] beside the router.
    pub place: Arc<Place<M, B>>,
    /// Serve with `into_make_service_with_connect_info::<SocketAddr>()`.
    pub router: axum::Router,
    /// Runs the `compact` and `index_commits` jobs every push enqueues, and
    /// garbage collection; run [`Worker::run`] beside the router. Without
    /// it every stroke stays its own pack and a clone stitches one pack per
    /// push the canvas ever took, one blob read after another (a full clone
    /// of the public demo took 253 s with 4,941 uncompacted packs).
    pub worker: Worker<M, B>,
}

/// The URL paths the router of [`build`] keeps for itself, so a built page
/// served beside it must have no file at or under any of them: the API
/// ([`api::API_BASE`]), Aspen's own routes under `/-` (`/-/blobs`,
/// `/-/metrics`, `/-/express`), and the canvas repository's Git URLs,
/// `/<repo>` and `/<repo>.git` with everything below them (scoped URLs
/// included). A page file at an API path would make axum panic when the two
/// routers are merged; one under the other prefixes would shadow a route
/// the router answers, Git's fallback included.
pub fn reserved_paths(config: &PlaceConfig) -> Vec<String> {
    vec![
        api::API_BASE.to_owned(),
        "/-".to_owned(),
        format!("/{}", config.repo),
        format!("/{}.git", config.repo),
    ]
}

/// Assembles `place` over these stores: the canvas repository (created if
/// missing), Aspen's server with the tile-rules hook, the access policy
/// and the API.
pub async fn build<M, B, T>(
    meta: M,
    blobs: B,
    tokens: T,
    config: PlaceConfig,
    settings: ServerSettings,
) -> Result<Built<M, B>, PlaceError>
where
    M: MetaStore + Clone + std::fmt::Debug + 'static,
    B: BlobStore + Clone + 'static,
    T: TokenStore + std::fmt::Debug + 'static,
{
    config.validate().map_err(PlaceError::Corrupt)?;
    let repo = place::ensure_repo(&meta, &config.repo).await?;
    let shared = place::shared(
        &repo,
        &config,
        Arc::new(hook::IndexedMain {
            meta: meta.clone(),
            repo: repo.id,
        }),
    );
    let mut receive = ReceiveConfig::new(
        &settings.spool_dir,
        settings.max_object_bytes,
        settings.max_inflated_bytes,
    );
    receive.hook = Some(Arc::new(hook::PlaceHook(shared.clone())));
    // Compaction lays covers out the way the push path schedules them, as
    // in `aspen serve`.
    let mut compact = CompactConfig::new(
        settings.spool_dir.join("compact"),
        settings.max_object_bytes,
        settings.max_inflated_bytes,
    );
    compact.layout = receive.cover_layout;
    compact
        .validate()
        .map_err(|e| PlaceError::Corrupt(format!("compaction settings: {e}")))?;
    std::fs::create_dir_all(&compact.spool_dir)
        .map_err(|e| PlaceError::Corrupt(format!("compaction spool: {e}")))?;
    let worker = Worker::new(
        Compactor::new(meta.clone(), blobs.clone(), compact),
        WorkerOptions::new(worker_owner()),
    );
    let mut server_config = ServerConfig::new(settings.max_object_bytes);
    // `git clone --depth=1` gets a cached exact shallow pack instead of the
    // full closure (C8).
    server_config.shallow_cache = Some(ShallowCacheConfig::new(
        settings.spool_dir.join("shallow"),
        settings.max_inflated_bytes,
    ));
    let server = Server::new(meta.clone(), blobs, server_config)
        .with_receive_pack(receive)
        .map_err(|e| PlaceError::Corrupt(format!("spool directory: {e}")))?;
    let svc = server.receive_pack().expect("configured").clone();
    let auth = AuthService::new(settings.policy, tokens)
        .map_err(|e| PlaceError::Corrupt(e.to_string()))?;
    let access = Arc::new(
        Access::new(
            auth,
            aspen_auth::Limits::new(settings.limits),
            meta.clone(),
            settings.client_addr,
        )
        .with_instance_limits(settings.instance),
    );
    let operators = Arc::new(place::AdminGrant {
        access: access.clone(),
        repo: RepoName::new(&config.repo).map_err(|e| PlaceError::Corrupt(e.to_string()))?,
    });
    let place = Place::open(config, shared, meta, svc, repo, operators).await?;
    let router = api::router(place.clone()).merge(server.router(access));
    Ok(Built {
        place,
        router,
        worker,
    })
}

/// The worker's name in job leases: unique per process, like `aspen serve`'s.
fn worker_owner() -> String {
    let mut nonce = [0u8; 4];
    let _ = getrandom::fill(&mut nonce);
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "host".into());
    let nonce: String = nonce.iter().map(|b| format!("{b:02x}")).collect();
    format!("aspen-place:{host}:{}:{nonce}", std::process::id())
}
