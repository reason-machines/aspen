//! How many metadata database statements the access layer costs one Git
//! request: its token and its repository (decisions log, "throughput 6a").
//!
//! Statements are counted as sqlx logs them (one `sqlx::query` event per
//! statement executed), by a global `tracing` subscriber. That is global to
//! the test binary, so this file holds this one test and nothing else that
//! talks to a database.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_auth::{AuthService, Grants, Limits, NewToken, PolicyConfig};
use aspen_core::id::UnixMicros;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_server::access::tokens::{CachedTokens, SqlTokens};
use aspen_server::access::{Access, AccessCaches, Authorized, ClientAddr, guard};
use axum::Router;
use axum::body::Body;
use axum::extract::Extension;
use axum::http::{Request, StatusCode, header};
use common::basic;
use tower::ServiceExt;

static STATEMENTS: AtomicU64 = AtomicU64::new(0);

/// Counts sqlx's per-statement events and ignores everything else.
struct CountStatements;

impl tracing::Subscriber for CountStatements {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == "sqlx::query"
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() == "sqlx::query" {
            STATEMENTS.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Opens every connection `meta`'s pool may hold and waits until all of
/// them are back in it, idle.
///
/// A pool at its maximum size never opens a connection, so a measurement
/// taken after this counts the requests' own statements and not the
/// statements SQLite runs on each new connection (sqlx's and the store's
/// `PRAGMA`s, 4 in all). Without it the count depends on timing: sqlx hands
/// a dropped connection back to the pool on a spawned task, and an acquire
/// that comes before that task has run opens a new connection (issue #331).
async fn fill_pool(meta: &SqlStore) {
    let pool = meta.pool();
    let max = pool.options().get_max_connections();
    let mut held = Vec::new();
    for _ in 0..max {
        held.push(pool.acquire().await.unwrap());
    }
    assert_eq!(pool.size(), max);
    drop(held);
    tokio::time::timeout(Duration::from_secs(10), async {
        while pool.num_idle() < max as usize {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the pool's connections come back");
}

/// Statements run while `f` runs.
async fn statements<F: Future<Output = ()>>(f: F) -> u64 {
    let before = STATEMENTS.load(Ordering::SeqCst);
    f.await;
    STATEMENTS.load(Ordering::SeqCst) - before
}

/// A file-backed SQLite store with repository `acme/a` and a token that may
/// push to it; returns the token's text.
async fn store(dir: &std::path::Path) -> (SqlStore, String) {
    let url = format!("sqlite://{}?mode=rwc", dir.join("aspen.db").display());
    let meta = SqlStore::connect(&url).await.unwrap();
    meta.migrate().await.unwrap();
    let settings = RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap();
    meta.create_repo(&RepoName::new("acme/a").unwrap(), &settings)
        .await
        .unwrap();
    let issued = SqlTokens(meta.clone())
        .create(
            NewToken {
                subject: "agent:count".into(),
                grants: Grants::new(["write:acme/a".parse().unwrap()]),
                description: String::new(),
                expires_at: None,
            },
            UnixMicros::now(),
        )
        .await
        .unwrap();
    (meta, issued.token.expose().to_owned())
}

/// The access middleware, wired as `aspen serve` wires it, in front of a
/// handler that touches no database.
fn app(meta: SqlStore, caches: AccessCaches) -> Router {
    let tokens = CachedTokens::new(SqlTokens(meta.clone()), caches.token_ttl);
    let auth = AuthService::new(PolicyConfig::default(), tokens).unwrap();
    let access = Arc::new(
        Access::new(auth, Limits::unlimited(), meta, ClientAddr::Peer)
            .with_repo_cache_ttl(caches.repo_ttl),
    );
    Router::new()
        .fallback(|Extension(_): Extension<Authorized>| async { "ok" })
        .layer(axum::middleware::from_fn_with_state(
            access,
            guard::<SqlStore, CachedTokens<SqlTokens>, _>,
        ))
}

/// The two HTTP requests of a `git push`: the advertisement and the pack.
async fn push(app: &Router, token: &str) {
    for (method, endpoint) in [
        ("GET", "info/refs?service=git-receive-pack"),
        ("POST", "git-receive-pack"),
    ] {
        let r = Request::builder()
            .method(method)
            .uri(format!("/acme/a.git/{endpoint}"))
            .header(header::AUTHORIZATION, basic("x", token))
            .body(Body::empty())
            .unwrap();
        let status = app.clone().oneshot(r).await.unwrap().status();
        assert_eq!(status, StatusCode::OK, "{method} {endpoint}");
    }
}

/// Invariant (throughput 6a): once a caller's token and repository are
/// cached, its requests cost the metadata database no statement at all
/// for them; without the caches each request costs two (the token and the
/// repository). The numbers are printed for the record.
#[tokio::test]
async fn warm_requests_read_neither_token_nor_repository() {
    tracing::subscriber::set_global_default(CountStatements).unwrap();
    const PUSHES: u64 = 20;

    // The caches off: what every request cost before them.
    let dir = tempfile::tempdir().unwrap();
    let (meta, token) = store(dir.path()).await;
    let off = AccessCaches {
        repo_ttl: Duration::ZERO,
        token_ttl: Duration::ZERO,
    };
    fill_pool(&meta).await;
    let app_off = app(meta, off);
    let uncached = statements(async {
        for _ in 0..PUSHES {
            push(&app_off, &token).await;
        }
    })
    .await;

    // The defaults `aspen serve` runs with.
    let dir = tempfile::tempdir().unwrap();
    let (meta, token) = store(dir.path()).await;
    fill_pool(&meta).await;
    let app_on = app(meta.clone(), AccessCaches::default());
    let cold = statements(push(&app_on, &token)).await;
    fill_pool(&meta).await;
    let warm = statements(async {
        for _ in 0..PUSHES {
            push(&app_on, &token).await;
        }
    })
    .await;

    println!(
        "access-layer statements per HTTP request: uncached {}, cold {}, warm {}",
        uncached as f64 / (2 * PUSHES) as f64,
        cold as f64 / 2.0,
        warm as f64 / (2 * PUSHES) as f64,
    );
    assert_eq!(
        uncached,
        2 * 2 * PUSHES,
        "uncached: a token and a repository lookup per request"
    );
    assert_eq!(cold, 2, "the first request reads each row once");
    assert_eq!(warm, 0, "warm requests read nothing");
}
