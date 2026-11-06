//! The access log of the Git router (node C5), as a log reader sees it.
//!
//! The log is captured by a global `tracing` subscriber, so this file holds
//! this one test and nothing else. A thread-local subscriber
//! (`tracing::subscriber::set_default`) is not enough in a binary whose
//! other tests also serve Git requests: while it is the only registered
//! dispatcher, `tracing-core` computes the interest of a callsite first hit
//! on another thread from that thread's default (no subscriber) and caches
//! it as never, so the `git request` event was dropped on every thread for
//! the rest of the process (issue #97). A global subscriber is every
//! thread's default, so no thread can register a callsite against nothing.

use std::sync::Arc;

use aspen_auth::{AuthService, Limits, LimitsConfig, MemoryTokenStore, PolicyConfig};
use aspen_blob::LocalBlobStore;
use aspen_core::meta::MetaStore;
use aspen_meta::SqlStore;
use aspen_pack::MaxObjectBytes;
use aspen_server::access::{Access, ClientAddr};
use aspen_server::{Server, ServerConfig};
use axum::body::Body;
use axum::http::Request;
use tower::ServiceExt as _;

/// A SQLite store in `dir`, migrated, with a pool of at most
/// `connections`.
async fn sqlite_store(dir: &std::path::Path, connections: u32) -> SqlStore {
    let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
    let opts = aspen_meta::StoreOptions {
        max_connections: connections,
        ..aspen_meta::StoreOptions::default()
    };
    let meta = SqlStore::connect_with(&url, &opts).await.unwrap();
    meta.migrate().await.unwrap();
    meta
}

fn server_config() -> ServerConfig {
    ServerConfig::new(MaxObjectBytes::new(1 << 20).unwrap())
}

/// A `MakeWriter` collecting formatted log lines.
#[derive(Clone, Default)]
struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn lines(&self) -> Vec<String> {
        String::from_utf8(self.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

/// The access log covers requests the guard refuses as well as those it
/// admits: one `git request` event each, with the status, in a
/// `git_request` span holding the method and the path. An admitted request
/// also names the repository and endpoint the guard resolved. Neither the
/// query string nor the credential is ever logged.
#[tokio::test]
async fn the_access_log_covers_refused_requests_without_credentials_or_queries() {
    let dir = tempfile::tempdir().unwrap();
    let meta = sqlite_store(dir.path(), 10).await;
    meta.create_repo(
        &aspen_core::name::RepoName::new("acme/obs").unwrap(),
        &aspen_core::repo::RepoSettings::new(
            aspen_core::name::RefName::new("refs/heads/main").unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let policy = PolicyConfig {
        anonymous: vec!["read:*".parse().unwrap()],
        ..PolicyConfig::default()
    };
    let access = Arc::new(Access::new(
        AuthService::new(policy, MemoryTokenStore::new()).unwrap(),
        Limits::new(LimitsConfig::default()),
        meta.clone(),
        ClientAddr::Peer,
    ));
    let router = Server::new(meta, blobs, server_config()).router(access);

    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    // Global, not `set_default`: see the module comment.
    tracing::subscriber::set_global_default(subscriber).unwrap();

    // "x:not-a-token": a credential that is presented but invalid.
    const CREDENTIAL: &str = "eDpub3QtYS10b2tlbg==";
    let requests = [
        // Admitted: anonymous may read.
        ("/acme/obs.git/info/refs?service=git-upload-pack", None, 200),
        // An invalid credential is refused, never downgraded.
        (
            "/acme/obs.git/info/refs?service=git-upload-pack",
            Some(CREDENTIAL),
            401,
        ),
        // Pushing needs write, even for the advertisement.
        (
            "/acme/obs.git/info/refs?service=git-receive-pack",
            None,
            401,
        ),
        // Not a Git path.
        ("/favicon.ico", None, 404),
    ];
    for (uri, credential, status) in requests {
        let mut request = Request::get(uri);
        if let Some(c) = credential {
            request = request.header("authorization", format!("Basic {c}"));
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status, "{uri}");
    }

    let lines: Vec<String> = captured
        .lines()
        .into_iter()
        .filter(|l| l.contains("git request"))
        .collect();
    assert_eq!(lines.len(), requests.len(), "{lines:#?}");
    for ((uri, _, status), line) in requests.iter().zip(&lines) {
        let path = uri.split('?').next().unwrap();
        assert!(line.contains(&format!("status={status}")), "{line}");
        assert!(line.contains(&format!("path=\"{path}\"")), "{line}");
        assert!(line.contains("method=GET"), "{line}");
        assert!(!line.contains("service="), "query logged: {line}");
        assert!(!line.contains(CREDENTIAL), "credential logged: {line}");
        assert!(!line.contains("not-a-token"), "credential logged: {line}");
    }
    assert!(lines[0].contains("repo=acme/obs"), "{}", lines[0]);
    assert!(
        lines[0].contains("endpoint=\"upload_pack_advertisement\""),
        "{}",
        lines[0]
    );
    for line in &lines[1..] {
        assert!(!line.contains("repo="), "refused before routing: {line}");
    }
}
