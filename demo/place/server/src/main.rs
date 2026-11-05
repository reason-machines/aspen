//! `aspen-place`: the `place` demo's back end in one process (Git over
//! HTTP, the API, the merger bot and the agents). Settings come from the
//! environment; see `demo/place/server/README.md`.
//!
//! ```text
//! aspen-place serve                     run it
//! aspen-place operator-token <subject>  issue a kill-switch operator token
//! ```

mod drain;
mod shutdown;
mod web;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use aspen_auth::{Grants, LimitsConfig, NewToken, PolicyConfig};
use aspen_blob::AnyBlobStore;
use aspen_core::id::UnixMicros;
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_place::{PlaceConfig, ServerSettings};
use aspen_server::access::ClientAddr;
use aspen_server::access::tokens::SqlTokens;

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn parse<T: std::str::FromStr>(name: &str, default: T) -> Result<T, String> {
    match var(name) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| format!("{name}={v:?} is not valid")),
    }
}

fn data_dir() -> PathBuf {
    PathBuf::from(var("PLACE_DATA_DIR").unwrap_or_else(|| ".place".to_owned()))
}

async fn open_meta() -> Result<SqlStore, Box<dyn std::error::Error>> {
    let url = match var("PLACE_DATABASE_URL") {
        Some(u) => u,
        None => {
            std::fs::create_dir_all(data_dir())?;
            format!("sqlite://{}?mode=rwc", data_dir().join("meta.db").display())
        }
    };
    let meta = SqlStore::connect(&url).await?;
    meta.migrate().await?;
    Ok(meta)
}

fn config() -> Result<PlaceConfig, String> {
    let domain = var("PLACE_DOMAIN").unwrap_or_else(|| "example.com".to_owned());
    let mut c = PlaceConfig::new(
        var("PLACE_CLONE_URL").unwrap_or_else(|| format!("https://place.{domain}/canvas.git")),
    );
    c.dashboard_url = match var("PLACE_DASHBOARD_URL").as_deref() {
        Some("none") => None,
        Some(u) => Some(u.to_owned()),
        None => Some(format!("https://dashboard.{domain}/")),
    };
    c.rules.width = parse("PLACE_WIDTH", c.rules.width)?;
    c.rules.height = parse("PLACE_HEIGHT", c.rules.height)?;
    c.rules.tiles_per_push = parse("PLACE_TILES_PER_PUSH", c.rules.tiles_per_push)?;
    c.pushes_per_minute = parse("PLACE_PUSHES_PER_MINUTE", c.pushes_per_minute)?;
    c.branch_ttl = Duration::from_secs(parse("PLACE_BRANCH_TTL_SECONDS", c.branch_ttl.as_secs())?);
    c.merge_interval = Duration::from_millis(parse(
        "PLACE_MERGE_INTERVAL_MS",
        c.merge_interval.as_millis() as u64,
    )?);
    c.ambient_agents = parse("PLACE_AGENTS", c.ambient_agents)?;
    c.ambient_interval = Duration::from_millis(parse(
        "PLACE_AMBIENT_INTERVAL_MS",
        c.ambient_interval.as_millis() as u64,
    )?);
    c.ambient_merge_interval = Duration::from_secs(parse(
        "PLACE_AMBIENT_MERGE_INTERVAL_SECONDS",
        c.ambient_merge_interval.as_secs(),
    )?);
    c.swarm.max_agents = parse("PLACE_SWARM_MAX_AGENTS", c.swarm.max_agents)?;
    c.swarm.pushes_per_second_cap = parse(
        "PLACE_SWARM_PUSHES_PER_SECOND",
        c.swarm.pushes_per_second_cap,
    )?;
    c.swarm.run_seconds = parse("PLACE_SWARM_RUN_SECONDS", c.swarm.run_seconds)?;
    c.swarm.cooldown_seconds = parse("PLACE_SWARM_COOLDOWN_SECONDS", c.swarm.cooldown_seconds)?;
    c.swarm_starts_per_hour = parse("PLACE_SWARM_STARTS_PER_HOUR", c.swarm_starts_per_hour)?;
    c.agent_concurrency = parse("PLACE_AGENT_CONCURRENCY", c.agent_concurrency)?;
    c.client_addr = ClientAddr::from_settings(
        parse("PLACE_TRUSTED_PROXY_HOPS", 0)?,
        var("PLACE_TRUSTED_PROXIES").as_deref(),
    )
    .map_err(|e| format!("PLACE_TRUSTED_PROXIES: {e}"))?;
    c.max_event_streams = parse("PLACE_EVENT_STREAMS", c.max_event_streams)?;
    c.max_event_streams_per_address = parse(
        "PLACE_EVENT_STREAMS_PER_ADDRESS",
        c.max_event_streams_per_address,
    )?;
    c.event_stream_max_age = Duration::from_secs(parse(
        "PLACE_EVENT_STREAM_MAX_SECONDS",
        c.event_stream_max_age.as_secs(),
    )?);
    c.kill_switch = var("PLACE_KILL_SWITCH").as_deref() == Some("1");
    c.validate()?;
    Ok(c)
}

fn policy(repo: &str) -> Result<PolicyConfig, Box<dyn std::error::Error>> {
    Ok(match var("PLACE_POLICY_FILE") {
        Some(path) => serde_json::from_slice(&std::fs::read(path)?)?,
        // Anyone may clone and push branches; main and the server's own
        // namespaces are protected by the tile rules, and operators are
        // token holders with an admin grant.
        None => serde_json::from_value(serde_json::json!({
            "anonymous": [format!("read:{repo}"), format!("write:{repo}:refs/heads/*")],
        }))?,
    })
}

fn limits() -> Result<LimitsConfig, Box<dyn std::error::Error>> {
    Ok(match var("PLACE_LIMITS_FILE") {
        Some(path) => serde_json::from_slice(&std::fs::read(path)?)?,
        None => serde_json::from_value(serde_json::json!({
            "read": { "burst": 30, "per_second": 5.0 },
            "push": { "burst": 10, "per_second": 1.0 },
            "quota": { "max_pack_bytes": 8 << 20, "max_objects": 20_000, "bytes_per_hour": 256 << 20 },
        }))?,
    })
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    // The S3 client needs a crypto provider (A4 obligation).
    let _ = rustls::crypto::ring::default_provider().install_default();
    let listen: SocketAddr = parse("PLACE_LISTEN", "127.0.0.1:7070".parse().expect("valid"))?;
    let config = config()?;
    let meta = open_meta().await?;
    let blobs: AnyBlobStore = match var("PLACE_BLOB_URL") {
        Some(u) => aspen_blob::open(&u)?,
        None => AnyBlobStore::Local(aspen_blob::LocalBlobStore::open(data_dir().join("blobs"))?),
    };
    let settings = ServerSettings {
        spool_dir: var("PLACE_SPOOL_DIR").map_or_else(|| data_dir().join("spool"), PathBuf::from),
        max_object_bytes: MaxObjectBytes::new(16 << 20).expect("valid"),
        max_inflated_bytes: MaxInflatedBytes::new(256 << 20).expect("valid"),
        policy: policy(&config.repo)?,
        limits: limits()?,
        client_addr: config.client_addr.clone(),
    };
    // Read before anything starts, so a wrong directory, or a file at a
    // path the API and Git router keeps, refuses the start.
    let web = var("PLACE_WEB_DIR")
        .map(|dir| {
            web::WebFiles::load(
                std::path::Path::new(&dir),
                &aspen_place::reserved_paths(&config),
            )
        })
        .transpose()?;
    let drain = Duration::from_secs(parse("PLACE_SHUTDOWN_DRAIN_SECONDS", 20)?);
    let built = aspen_place::build(meta.clone(), blobs, SqlTokens(meta), config, settings).await?;
    // Compaction, commit indexing and garbage collection. Jobs are leased
    // and idempotent, so one cut short by exit is redone after its lease.
    let mut background = built.worker;
    tokio::spawn(async move {
        background
            .run(std::future::pending(), |e| {
                eprintln!("aspen-place: background worker: {e}");
            })
            .await;
    });
    let router = match web {
        Some(files) => files.router().merge(built.router),
        None => built.router,
    };
    let (cutoff, cutoff_watch) = drain::Cutoff::new();
    let router = router.layer(axum::middleware::from_fn_with_state(
        cutoff_watch,
        drain::cut_off,
    ));
    // Watch for SIGTERM before the port opens: a platform may stop the task
    // as soon as it sees it listening, and a signal nobody watches yet
    // kills the process before its last merge.
    let shutdown = shutdown_signal()?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    eprintln!("aspen-place: listening on http://{listen}");
    let worker = shutdown::Worker::spawn({
        let place = built.place.clone();
        |stopped| place.run(stopped)
    });

    // SIGTERM or Ctrl-C: `serve_then_merge` drains, then runs the last merge.
    let (signal_tx, signal_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown.await;
        let _ = signal_tx.send(true);
    });
    let mut graceful = signal_rx.clone();
    // Not `axum::serve`: it gives hyper no timer, so request headers
    // would never time out (decisions log, D4). Like `axum::serve`, it
    // waits for every request in flight after `shutdown`, so the drain
    // in `serve_then_merge` bounds that wait.
    let server = aspen_server::listen::serve(
        listener,
        router,
        aspen_server::listen::HttpLimits::default(),
        async move {
            let _ = graceful.wait_for(|stop| *stop).await;
            eprintln!("aspen-place: shutting down after a last merge");
        },
    );
    shutdown::serve_then_merge(server, signal_rx, drain, cutoff, worker).await
}

/// How long exit waits for blocking work still running (a push being
/// ingested, say) once the last merge is done. Dropping the runtime would
/// wait for it without bound, up to the platform's kill.
const EXIT_WAIT: Duration = Duration::from_secs(10);

/// A future that resolves on SIGTERM or SIGINT (Ctrl-C). The handlers are
/// installed when this is called, not when the future is first polled, so
/// a signal that arrives in between is not lost to the default action. A
/// process that cannot watch for SIGTERM refuses to start rather than lose
/// its last merge on every stop.
fn shutdown_signal() -> std::io::Result<impl std::future::Future<Output = ()> + Send + 'static> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        Ok(async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = interrupt.recv() => {}
            }
        })
    }
    #[cfg(not(unix))]
    {
        // Registered when first polled; the caller polls it right away.
        Ok(async {
            let _ = tokio::signal::ctrl_c().await;
        })
    }
}

async fn operator_token(subject: &str) -> Result<(), Box<dyn std::error::Error>> {
    let repo = var("PLACE_REPO").unwrap_or_else(|| "canvas".to_owned());
    let meta = open_meta().await?;
    let grant: aspen_auth::Grant =
        serde_json::from_value(serde_json::json!(format!("admin:{repo}")))?;
    let grants = Grants::new([grant]);
    let issued = SqlTokens(meta)
        .create(
            NewToken {
                subject: subject.to_owned(),
                grants,
                description: "place kill-switch operator".to_owned(),
                expires_at: None,
            },
            UnixMicros::now(),
        )
        .await?;
    // Shown once to the operator who asked for it; only its digest is stored.
    println!("{}", issued.token.expose());
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("aspen-place: {e}");
            return ExitCode::FAILURE;
        }
    };
    let result = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["serve"] | [] => runtime.block_on(serve()),
        ["operator-token", subject] => runtime.block_on(operator_token(subject)),
        _ => {
            eprintln!("usage: aspen-place [serve] | aspen-place operator-token <subject>");
            return ExitCode::FAILURE;
        }
    };
    // Drops every task still running, which closes any connection still
    // open, and waits for blocking work at most `EXIT_WAIT`.
    runtime.shutdown_timeout(EXIT_WAIT);
    exit_code(result)
}

/// The process's exit status: failure, with the error on stderr, for any
/// error, a failed server included.
fn exit_code(result: Result<(), Box<dyn std::error::Error>>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("aspen-place: {e}");
            ExitCode::FAILURE
        }
    }
}
