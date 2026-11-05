//! The whole `place` app (Git, API, hook) on a throwaway SQLite database
//! and local blob store, served on 127.0.0.1, with stock `git` working
//! copies and a minimal HTTP client.

#![allow(dead_code, unreachable_pub)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aspen_auth::{Grant, Grants, MemoryTokenStore, NewToken, TokenStore};
use aspen_blob::LocalBlobStore;
use aspen_core::id::UnixMicros;
use aspen_core::meta::MetaStore;
use aspen_core::name::RefName;
use aspen_core::refs::RefVersion;
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_place::{Place, PlaceConfig, ServerSettings};
use aspen_server::access::ClientAddr;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The token store, shared so tests can add operator tokens; it counts
/// its lookups.
#[derive(Debug, Clone, Default)]
pub struct Tokens(
    pub Arc<MemoryTokenStore>,
    pub Arc<std::sync::atomic::AtomicUsize>,
);

impl Tokens {
    /// How many tokens were looked up.
    pub fn lookups(&self) -> usize {
        self.1.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl TokenStore for Tokens {
    async fn token(
        &self,
        id: &aspen_auth::TokenId,
    ) -> Result<Option<aspen_auth::TokenRecord>, aspen_auth::TokenStoreError> {
        self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.0.token(id).await
    }
}

pub struct App {
    pub dir: PathBuf,
    _keep: Option<tempfile::TempDir>,
    pub base: String,
    pub place: Arc<Place<SqlStore, LocalBlobStore>>,
    /// Aspen's background worker, not running: tests drive it.
    pub worker: tokio::sync::Mutex<aspen_compact::Worker<SqlStore, LocalBlobStore>>,
    pub meta: SqlStore,
    pub tokens: Tokens,
    pub home: PathBuf,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for App {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// A test configuration: a small canvas, no ambient agents, a slow merger
/// (tests call `merge_once` themselves).
pub fn config() -> PlaceConfig {
    let mut c = PlaceConfig::new("http://localhost/canvas.git");
    c.rules.width = 16;
    c.rules.height = 16;
    c.ambient_agents = 0;
    c.merge_interval = Duration::from_secs(3600);
    c
}

impl App {
    pub async fn start(tune: impl FnOnce(&mut PlaceConfig)) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();
        Self::start_in(path, Some(dir), tune).await
    }

    /// Starts with `limits` on Git and operator requests.
    pub async fn start_with_limits(
        limits: aspen_auth::LimitsConfig,
        tune: impl FnOnce(&mut PlaceConfig),
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();
        Self::start_full(path, Some(dir), limits, tune).await
    }

    /// Starts on an existing data directory (for restarts).
    pub async fn start_in(
        dir: PathBuf,
        keep: Option<tempfile::TempDir>,
        tune: impl FnOnce(&mut PlaceConfig),
    ) -> Self {
        Self::start_full(dir, keep, aspen_auth::LimitsConfig::default(), tune).await
    }

    async fn start_full(
        dir: PathBuf,
        keep: Option<tempfile::TempDir>,
        limits: aspen_auth::LimitsConfig,
        tune: impl FnOnce(&mut PlaceConfig),
    ) -> Self {
        let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = LocalBlobStore::open(dir.join("blobs")).unwrap();
        let mut config = config();
        tune(&mut config);
        let tokens = Tokens::default();
        let policy = serde_json::from_value(serde_json::json!({
            "anonymous": ["read:canvas", "write:canvas:refs/heads/*"],
        }))
        .unwrap();
        let settings = ServerSettings {
            spool_dir: dir.join("spool"),
            max_object_bytes: MaxObjectBytes::new(16 << 20).unwrap(),
            max_inflated_bytes: MaxInflatedBytes::new(256 << 20).unwrap(),
            policy,
            limits,
            client_addr: ClientAddr::Peer,
        };
        let built = aspen_place::build(meta.clone(), blobs, tokens.clone(), config, settings)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = built.router;
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let home = dir.join("home");
        std::fs::create_dir_all(&home).unwrap();
        Self {
            dir,
            _keep: keep,
            base: format!("http://{addr}"),
            place: built.place,
            worker: tokio::sync::Mutex::new(built.worker),
            meta,
            tokens,
            home,
            server,
        }
    }

    pub fn url(&self) -> String {
        format!("{}/canvas.git", self.base)
    }

    /// A token for `subject` with `grants` (`admin:canvas`, ...).
    pub fn token(&self, subject: &str, grants: &[&str]) -> String {
        let grants = Grants::new(
            grants
                .iter()
                .map(|g| serde_json::from_value::<Grant>(Value::from(*g)).unwrap()),
        );
        let issued = aspen_auth::token::issue(
            NewToken {
                subject: subject.into(),
                grants,
                description: String::new(),
                expires_at: None,
            },
            UnixMicros::now(),
        )
        .unwrap();
        self.tokens.0.insert(issued.record);
        issued.token.expose().to_owned()
    }

    pub async fn current(&self, name: &str) -> Option<RefVersion> {
        self.meta
            .current_version(self.place.repo().id, &RefName::new(name).unwrap())
            .await
            .unwrap()
    }

    pub async fn tip(&self, name: &str) -> Option<String> {
        self.current(name)
            .await
            .and_then(|v| v.value.tip())
            .map(|t| t.to_string())
    }

    /// A clone of the canvas.
    pub async fn clone_work(&self, name: &str) -> Work {
        let path = self.dir.join(name);
        let w = Work {
            path: path.clone(),
            home: self.home.clone(),
        };
        let url = self.url();
        let home = self.home.clone();
        let dir = self.dir.clone();
        let name = name.to_owned();
        blocking(move || {
            git_ok(&home, &dir, &["clone", "-q", &url, &name]);
        })
        .await;
        w.git(&["config", "user.name", "Ada"]);
        w.git(&["config", "user.email", "ada@example.com"]);
        w
    }

    // --- HTTP ---

    pub async fn http(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<&Value>,
    ) -> Response {
        http(&self.base, method, path, headers, body).await
    }

    pub async fn get(&self, path: &str) -> Value {
        let r = self.http("GET", path, &[], None).await;
        assert_eq!(r.status, 200, "{path}: {}", r.text());
        r.json()
    }

    /// Opens `GET /api/v1/events` with `query` and extra headers.
    pub async fn events(&self, query: &str, headers: &[(&str, &str)]) -> Sse {
        Sse::open(&self.base, &format!("/api/v1/events{query}"), headers).await
    }
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.text()))
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

fn request_bytes(
    base: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> Vec<u8> {
    let host = base.trim_start_matches("http://");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    let body = body.unwrap_or_default();
    if !body.is_empty() || method == "POST" {
        req.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    req.push_str("\r\n");
    let mut out = req.into_bytes();
    out.extend_from_slice(body);
    out
}

/// Status, headers, and where the body starts.
type Head = (u16, Vec<(String, String)>, usize);

fn split_head(raw: &[u8]) -> Option<Head> {
    let end = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&raw[..end]);
    let mut lines = head.split("\r\n");
    let status = lines.next()?.split(' ').nth(1)?.parse().ok()?;
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        .collect();
    Some((status, headers, end + 4))
}

/// Decodes as much of a chunked body as is complete; returns the bytes and
/// how many raw bytes were used.
fn dechunk(raw: &[u8]) -> (Vec<u8>, usize, bool) {
    let mut out = Vec::new();
    let mut at = 0;
    loop {
        let Some(nl) = raw[at..].windows(2).position(|w| w == b"\r\n") else {
            return (out, at, false);
        };
        let size = usize::from_str_radix(String::from_utf8_lossy(&raw[at..at + nl]).trim(), 16)
            .unwrap_or(0);
        if size == 0 {
            return (out, at, true);
        }
        let start = at + nl + 2;
        if raw.len() < start + size + 2 {
            return (out, at, false);
        }
        out.extend_from_slice(&raw[start..start + size]);
        at = start + size + 2;
    }
}

pub async fn http(
    base: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&Value>,
) -> Response {
    let host = base.trim_start_matches("http://");
    let mut s = tokio::net::TcpStream::connect(host).await.unwrap();
    let body = body.map(|b| b.to_string().into_bytes());
    s.write_all(&request_bytes(base, method, path, headers, body.as_deref()))
        .await
        .unwrap();
    read_response(s).await
}

/// Reads a whole response from `s`.
async fn read_response(mut s: tokio::net::TcpStream) -> Response {
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).await.unwrap();
    let (status, headers, start) = split_head(&raw).expect("an HTTP response");
    let chunked = headers
        .iter()
        .any(|(k, v)| k.eq_ignore_ascii_case("transfer-encoding") && v.contains("chunked"));
    let body = if chunked {
        dechunk(&raw[start..]).0
    } else {
        raw[start..].to_vec()
    };
    Response {
        status,
        headers,
        body,
    }
}

/// Sends `n` copies of a request on `n` connections so they reach the
/// server together: each connection first gets all of its request but the
/// last byte, then every last byte is sent at once. Returns the responses.
pub async fn burst(base: &str, method: &str, path: &str, n: usize) -> Vec<Response> {
    let host = base.trim_start_matches("http://");
    let req = request_bytes(base, method, path, &[], None);
    let (head, last) = req.split_at(req.len() - 1);
    let mut conns = Vec::with_capacity(n);
    for _ in 0..n {
        let mut s = tokio::net::TcpStream::connect(host).await.unwrap();
        s.set_nodelay(true).unwrap();
        s.write_all(head).await.unwrap();
        conns.push(s);
    }
    let go = Arc::new(tokio::sync::Barrier::new(n));
    let tasks: Vec<_> = conns
        .into_iter()
        .map(|mut s| {
            let go = go.clone();
            let last = last.to_vec();
            tokio::spawn(async move {
                go.wait().await;
                s.write_all(&last).await.unwrap();
                read_response(s).await
            })
        })
        .collect();
    let mut out = Vec::with_capacity(n);
    for t in tasks {
        out.push(t.await.unwrap());
    }
    out
}

/// A server-sent events stream.
pub struct Sse {
    stream: tokio::net::TcpStream,
    raw: Vec<u8>,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    text: String,
    ended: bool,
}

/// One parsed event.
#[derive(Debug, Clone)]
pub struct Ev {
    pub id: Option<u64>,
    pub kind: String,
    pub data: Value,
}

impl Sse {
    pub async fn open(base: &str, path: &str, headers: &[(&str, &str)]) -> Self {
        let host = base.trim_start_matches("http://");
        let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
        stream
            .write_all(&request_bytes(base, "GET", path, headers, None))
            .await
            .unwrap();
        let mut raw = Vec::new();
        let (status, headers, start) = loop {
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0, "stream closed before headers");
            raw.extend_from_slice(&buf[..n]);
            if let Some(h) = split_head(&raw) {
                break h;
            }
        };
        raw.drain(..start);
        // Body bytes that came with the headers.
        let (bytes, used, ended) = dechunk(&raw);
        raw.drain(..used);
        Self {
            stream,
            raw,
            status,
            headers,
            text: String::from_utf8_lossy(&bytes).into_owned(),
            ended,
        }
    }

    async fn fill(&mut self, timeout: Duration) -> bool {
        let mut buf = [0u8; 8192];
        match tokio::time::timeout(timeout, self.stream.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => {
                self.raw.extend_from_slice(&buf[..n]);
                let (bytes, used, ended) = dechunk(&self.raw);
                self.text.push_str(&String::from_utf8_lossy(&bytes));
                self.raw.drain(..used);
                self.ended |= ended;
                true
            }
            _ => false,
        }
    }

    /// The raw text received so far.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Whether the server ended the response body (its last chunk) within
    /// `timeout`, reading and keeping whatever comes before it.
    pub async fn ended(&mut self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while !self.ended {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() || !self.fill(left).await {
                break;
            }
        }
        self.ended
    }

    /// The next event (comments and `retry:` skipped), or `None` if none
    /// arrives within `timeout`.
    pub async fn next(&mut self, timeout: Duration) -> Option<Ev> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            while let Some(end) = self.text.find("\n\n") {
                let block: String = self.text.drain(..end + 2).collect();
                let mut ev = Ev {
                    id: None,
                    kind: String::new(),
                    data: Value::Null,
                };
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("id: ") {
                        ev.id = v.parse().ok();
                    } else if let Some(v) = line.strip_prefix("event: ") {
                        ev.kind = v.to_owned();
                    } else if let Some(v) = line.strip_prefix("data: ") {
                        ev.data = serde_json::from_str(v).unwrap();
                    }
                }
                if !ev.kind.is_empty() {
                    return Some(ev);
                }
            }
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() || !self.fill(left).await {
                return None;
            }
        }
    }

    /// Events until one matches `stop` (included), within `timeout`.
    pub async fn until(&mut self, timeout: Duration, stop: impl Fn(&Ev) -> bool) -> Vec<Ev> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut out = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Some(e) = self.next(left).await else {
                panic!("no matching event within {timeout:?}; got {out:?}");
            };
            let done = stop(&e);
            out.push(e);
            if done {
                return out;
            }
        }
    }
}

// --- Git ---

pub async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

pub fn git_ok(home: &Path, dir: &Path, args: &[&str]) -> String {
    let mut cmd = aspen_testkit::git::isolated_git(home);
    cmd.current_dir(dir).args(args);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[derive(Debug, Clone)]
pub struct Work {
    pub path: PathBuf,
    pub home: PathBuf,
}

impl Work {
    pub fn git(&self, args: &[&str]) -> String {
        git_ok(&self.home, &self.path, args)
    }

    pub fn write(&self, rel: &str, content: &[u8]) {
        let p = self.path.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, content).unwrap();
    }

    /// Writes tiles and commits them.
    pub fn paint(&self, tiles: &[(u32, u32, &str)], message: &str) -> String {
        for (x, y, c) in tiles {
            self.write(&format!("tiles/{x}_{y}.txt"), format!("{c}\n").as_bytes());
        }
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", message]);
        self.git(&["rev-parse", "HEAD"]).trim().to_owned()
    }

    /// `git push <args>`: success and combined output.
    pub async fn push(&self, args: &[&str]) -> (bool, String) {
        self.push_with(&[], args).await
    }

    /// `git <git_args> push <args>`.
    pub async fn push_with(&self, git_args: &[&str], args: &[&str]) -> (bool, String) {
        let mut cmd = aspen_testkit::git::isolated_git(&self.home);
        cmd.current_dir(&self.path)
            .args(git_args)
            .arg("push")
            .args(args);
        blocking(move || {
            let o = cmd.output().unwrap();
            let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
            t.push_str(&String::from_utf8_lossy(&o.stderr));
            (o.status.success(), t)
        })
        .await
    }

    pub async fn run(&self, args: &[&str]) -> String {
        let w = self.clone();
        let args: Vec<String> = args.iter().map(|s| (*s).to_owned()).collect();
        blocking(move || {
            let a: Vec<&str> = args.iter().map(String::as_str).collect();
            w.git(&a)
        })
        .await
    }
}
