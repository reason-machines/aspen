//! Stock `git` (protocol v2, smart HTTP) fetching through express and CDN
//! mode (node C2).
//!
//! Each test seeds a repository with `aspen_server::testing`, serves it
//! with express routes, and clones and fetches with `fetch.uriprotocols`,
//! checking the result with `git fsck --strict`. CDN mode runs against
//! [`Edge`], a small caching reverse proxy standing in for a CDN: it keys
//! its cache on the request path and `Range`, stores only responses that
//! `Cache-Control` marks `public` with a `max-age`, keeps each one for that
//! long, and forwards everything else to its origin (the server's
//! `/-/express`). No real CDN or cloud resource is involved.

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_blob::LocalBlobStore;
use aspen_core::blob::PastEndRange;
use aspen_express::{
    CacheConfig, Express, ExpressConfig, ProbeConfig, Refusal, Routes, SigningPolicy, Tier, UrlKey,
    UrlKeys,
};
use aspen_meta::SqlStore;
use aspen_server::testing::{Push, Seeder, TestServer};
use aspen_testkit::NO_AUTO_MAINTENANCE;

// ---------------------------------------------------------------------------
// Git helpers (the same isolated setup as tests/fetch.rs)
// ---------------------------------------------------------------------------

struct Env {
    tmp: tempfile::TempDir,
    clock: std::cell::Cell<i64>,
}

impl Env {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join("home")).unwrap();
        Self {
            tmp,
            clock: std::cell::Cell::new(1_700_000_000),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }

    fn git(&self, dir: &Path) -> Command {
        let mut c = Command::new("git");
        c.current_dir(dir)
            .env("HOME", self.path("home"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_AUTHOR_NAME", "Aspen Test")
            .env("GIT_AUTHOR_EMAIL", "test@aspen.invalid")
            .env("GIT_COMMITTER_NAME", "Aspen Test")
            .env("GIT_COMMITTER_EMAIL", "test@aspen.invalid")
            .env_remove("GIT_DIR")
            .env_remove("GIT_PROTOCOL")
            .env_remove("GIT_TRACE_PACKET")
            .args(["-c", "protocol.version=2", "-c", "init.defaultBranch=main"])
            // No automatic maintenance: after a fetch that brought several
            // packfile-URI packs it repacks in the background while `fsck`
            // reads the pack directory (#72; see `NO_AUTO_MAINTENANCE`).
            .args(NO_AUTO_MAINTENANCE);
        c
    }

    fn source(&self) -> PathBuf {
        let src = self.path("src");
        fs::create_dir_all(&src).unwrap();
        run(self.git(&src).args(["init", "--quiet", "-b", "main"]));
        src
    }

    fn commit(&self, dir: &Path, file: &str, contents: &str) -> String {
        fs::write(dir.join(file), contents).unwrap();
        run(self.git(dir).args(["add", file]));
        let t = self.clock.get() + 1000;
        self.clock.set(t);
        let date = format!("@{t} +0000");
        run(self
            .git(dir)
            .env("GIT_AUTHOR_DATE", &date)
            .env("GIT_COMMITTER_DATE", &date)
            .args(["commit", "--quiet", "-m", &format!("{file}: {t}")]));
        rev(self, dir, "HEAD")
    }

    /// `git clone` with `fetch.uriprotocols=http` and packet tracing;
    /// returns the trace.
    fn uri_clone(&self, url: &str, dest: &Path, extra: &[&str]) -> String {
        let trace = dest.with_extension("trace");
        run(self
            .git(self.tmp.path())
            .env("GIT_TRACE_PACKET", &trace)
            .args(["-c", "fetch.uriprotocols=http"])
            .args(extra)
            .args(["clone", "--quiet", url])
            .arg(dest));
        fs::read_to_string(trace).unwrap()
    }

    fn uri_fetch(&self, dir: &Path) -> String {
        let trace = dir.with_extension("fetch-trace");
        run(self.git(dir).env("GIT_TRACE_PACKET", &trace).args([
            "-c",
            "fetch.uriprotocols=http",
            "fetch",
            "--quiet",
            "origin",
        ]));
        fs::read_to_string(trace).unwrap()
    }
}

fn run(cmd: &mut Command) -> String {
    let out = cmd.output().expect("spawn git");
    assert!(
        out.status.success(),
        "{cmd:?} failed with {}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

fn rev(env: &Env, dir: &Path, r: &str) -> String {
    run(env.git(dir).args(["rev-parse", r])).trim().to_owned()
}

fn fsck(env: &Env, dir: &Path) {
    run(env.git(dir).args(["fsck", "--strict", "--no-progress"]));
}

fn lines(n: usize, tag: &str) -> String {
    (0..n)
        .map(|i| format!("line {i} of the file {tag}\n"))
        .collect()
}

/// The packfile URIs the server sent, from a packet trace.
fn uris(trace: &str) -> Vec<String> {
    trace
        .lines()
        .filter_map(|l| {
            let at = l.find("http://127.0.0.1")?;
            let uri: String = l[at..].chars().take_while(|c| !c.is_whitespace()).collect();
            uri.contains("/repos/").then_some(uri)
        })
        .collect()
}

/// One raw HTTP/1.1 GET; returns the status code, headers and body.
fn get(url: &str, headers: &[(&str, &str)]) -> (u16, String, Vec<u8>) {
    let rest = url.strip_prefix("http://").unwrap();
    let (addr, path) = rest.split_at(rest.find('/').unwrap());
    let mut s = TcpStream::connect(addr).unwrap();
    let mut req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).unwrap();
    split(&out)
}

fn split(raw: &[u8]) -> (u16, String, Vec<u8>) {
    let end = raw.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let head = String::from_utf8_lossy(&raw[..end]).into_owned();
    let status = head[9..12].parse().unwrap();
    (status, head, raw[end + 4..].to_vec())
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.split("\r\n").find_map(|l| {
        let (k, v) = l.split_once(": ")?;
        k.eq_ignore_ascii_case(name).then_some(v)
    })
}

// ---------------------------------------------------------------------------
// Express wiring
// ---------------------------------------------------------------------------

fn keys() -> UrlKeys {
    UrlKeys::new(UrlKey::new("t1", &[0x42; 32]).unwrap(), vec![]).unwrap()
}

fn probe() -> ProbeConfig {
    ProbeConfig {
        failures_to_down: 1,
        timeout: Duration::from_secs(2),
        ..ProbeConfig::default()
    }
}

/// What a test server was started with.
struct Wired {
    server: TestServer,
    routes: Arc<Routes>,
    express: Express<LocalBlobStore>,
    /// `<server>/-/express`.
    origin: String,
}

/// What `wired_at`'s hook hands back: routes, the express service and its
/// origin URL.
type Parts = (Arc<Routes>, Express<LocalBlobStore>, String);

/// An edge's cache: `path|range` to the expiry and the raw response.
type EdgeCache = Mutex<HashMap<String, (Instant, Vec<u8>)>>;

/// A server with packfile URIs on, serving express URLs itself at
/// `/-/express` (with a disk cache), whose routes are `tiers(origin)`.
fn wired(
    env: &Env,
    policy: aspen_auth::PolicyConfig,
    tiers: impl FnOnce(&str) -> Vec<Tier>,
) -> Wired {
    wired_at(env, policy, tiers, None)
}

/// [`wired`], signing at a fixed time if `frozen` is given.
fn wired_at(
    env: &Env,
    policy: aspen_auth::PolicyConfig,
    tiers: impl FnOnce(&str) -> Vec<Tier>,
    frozen: Option<u64>,
) -> Wired {
    let slot: Arc<Mutex<Option<Parts>>> = Arc::default();
    let out = Arc::clone(&slot);
    let cache_dir = env.path("express-cache");
    let server = TestServer::start_extended(
        policy,
        |c| c.packfile_uris = true,
        move |server, base, blobs| {
            let origin = format!("{base}{}", aspen_server::express::EXPRESS_PATH);
            let mut routes =
                Routes::new(tiers(&origin), keys(), SigningPolicy::default(), probe()).unwrap();
            if let Some(t) = frozen {
                routes = routes.with_clock(move || t);
            }
            let routes = Arc::new(routes);
            let express = Express::new(
                blobs.clone(),
                keys(),
                ExpressConfig {
                    cache: Some(CacheConfig {
                        dir: cache_dir,
                        max_bytes: 64 << 20,
                        max_fills: CacheConfig::DEFAULT_MAX_FILLS,
                    }),
                    ..ExpressConfig::default()
                },
            )
            .unwrap();
            *out.lock().unwrap() = Some((Arc::clone(&routes), express.clone(), origin));
            server
                .with_express_routes(routes)
                .with_express_route(express)
        },
    );
    let (routes, express, origin) = slot.lock().unwrap().take().unwrap();
    Wired {
        server,
        routes,
        express,
        origin,
    }
}

fn anyone_reads() -> aspen_auth::PolicyConfig {
    aspen_auth::PolicyConfig {
        anonymous: vec!["read:*".parse().unwrap()],
        ..aspen_auth::PolicyConfig::default()
    }
}

fn seed<'a>(
    s: &'a TestServer,
    name: &str,
    src: &Path,
    tips: &[String],
) -> Seeder<'a, SqlStore, LocalBlobStore> {
    let seeder = s.seeder(name, "refs/heads/main", src);
    for t in tips {
        s.push(&seeder, &Push::new("refs/heads/main", t));
    }
    seeder
}

/// The signature segment of an express URL (after `/t1/<expires>/`).
fn sig_of(url: &str) -> String {
    let rest = &url[url.find("/t1/").unwrap() + 4..];
    rest.split('/').nth(1).unwrap().to_owned()
}

fn flip_first(sig: &str) -> String {
    format!(
        "{}{}",
        if sig.starts_with('0') { '1' } else { '0' },
        &sig[1..]
    )
}

fn metrics(s: &TestServer) -> String {
    let (status, _, body) = get(&format!("{}/-/metrics", s.base_url()), &[]);
    assert_eq!(status, 200);
    String::from_utf8(body).unwrap()
}

// ---------------------------------------------------------------------------
// Edge: a local stand-in for a CDN
// ---------------------------------------------------------------------------

/// A caching reverse proxy in front of `origin`. It caches a `200` or
/// `206` only when `Cache-Control` says `public` and gives a `max-age`,
/// for that many seconds, keyed by path and `Range`; it never caches
/// anything else. When `down`, it answers `503` to everything, like an
/// edge that lost its origin or an outage.
struct Edge {
    base: String,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    down: Arc<AtomicBool>,
    entries: Arc<EdgeCache>,
}

impl Edge {
    fn start(origin: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let edge = Self {
            base,
            hits: Arc::default(),
            misses: Arc::default(),
            down: Arc::default(),
            entries: Arc::default(),
        };
        let origin = origin.strip_prefix("http://").unwrap().to_owned();
        let (hits, misses, down, entries) = (
            Arc::clone(&edge.hits),
            Arc::clone(&edge.misses),
            Arc::clone(&edge.down),
            Arc::clone(&edge.entries),
        );
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(conn) = conn else { continue };
                let (origin, hits, misses, down, entries) = (
                    origin.clone(),
                    Arc::clone(&hits),
                    Arc::clone(&misses),
                    Arc::clone(&down),
                    Arc::clone(&entries),
                );
                std::thread::spawn(move || {
                    let _ = Self::handle(conn, &origin, &hits, &misses, &down, &entries);
                });
            }
        });
        edge
    }

    fn handle(
        mut conn: TcpStream,
        origin: &str,
        hits: &AtomicU64,
        misses: &AtomicU64,
        down: &AtomicBool,
        entries: &EdgeCache,
    ) -> std::io::Result<()> {
        let mut reader = BufReader::new(conn.try_clone()?);
        let mut request_line = String::new();
        reader.read_line(&mut request_line)?;
        let path = request_line.split(' ').nth(1).unwrap_or("/").to_owned();
        let mut range = None;
        loop {
            let mut l = String::new();
            reader.read_line(&mut l)?;
            if l == "\r\n" || l.is_empty() {
                break;
            }
            if let Some((k, v)) = l.trim_end().split_once(": ")
                && k.eq_ignore_ascii_case("range")
            {
                range = Some(v.to_owned());
            }
        }
        if down.load(Ordering::Relaxed) {
            return conn.write_all(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
        let cache_key = format!("{path}|{}", range.as_deref().unwrap_or(""));
        if let Some((deadline, raw)) = entries.lock().unwrap().get(&cache_key)
            && Instant::now() < *deadline
        {
            hits.fetch_add(1, Ordering::Relaxed);
            return conn.write_all(raw);
        }
        misses.fetch_add(1, Ordering::Relaxed);
        let (host, prefix) = origin.split_at(origin.find('/').unwrap_or(origin.len()));
        let mut up = TcpStream::connect(host)?;
        let mut req =
            format!("GET {prefix}{path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
        if let Some(r) = &range {
            req.push_str(&format!("Range: {r}\r\n"));
        }
        req.push_str("\r\n");
        up.write_all(req.as_bytes())?;
        let mut raw = Vec::new();
        up.read_to_end(&mut raw)?;
        let (status, head, _) = split(&raw);
        let cc = header(&head, "cache-control").unwrap_or("");
        let directives: Vec<&str> = cc.split(',').map(str::trim).collect();
        let max_age = directives
            .iter()
            .find_map(|d| d.strip_prefix("max-age="))
            .and_then(|v| v.parse::<u64>().ok());
        if matches!(status, 200 | 206)
            && directives.contains(&"public")
            && !directives.contains(&"no-store")
            && let Some(age) = max_age.filter(|a| *a > 0)
        {
            entries.lock().unwrap().insert(
                cache_key,
                (Instant::now() + Duration::from_secs(age), raw.clone()),
            );
        }
        conn.write_all(&raw)
    }

    fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    fn cached(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Invariant (express mode): a stock v2 client that opted in gets every
/// planned pack as a URI on the express route, ahead of the blob store's
/// own `/-/blobs` URL, downloads it from there, and its clone and
/// incremental fetch pass `fsck --strict`; the express fills its cache from
/// the store once per pack. A client that did not opt in still gets the
/// stitched pack from the same server.
#[test]
fn express_mode_clone_and_incremental_fetch() {
    let env = Env::new();
    let src = env.source();
    let tips: Vec<String> = (0..3)
        .map(|i| env.commit(&src, "file.txt", &lines(100 + i * 10, &format!("v{i}"))))
        .collect();
    let w = wired(&env, anyone_reads(), |origin| {
        vec![Tier::new(&[origin], PastEndRange::Empty206).unwrap()]
    });
    let s = &w.server;
    let seeder = seed(s, "demo", &src, &tips);

    let client = env.path("client");
    let trace = env.uri_clone(&s.url("demo"), &client, &[]);
    let sent = uris(&trace);
    assert_eq!(sent.len(), 3, "{trace}");
    for u in &sent {
        assert!(u.starts_with(&format!("{}/t1/", w.origin)), "{u}");
    }
    assert!(
        !trace.contains("/-/blobs/"),
        "express comes before /-/blobs"
    );
    assert_eq!(rev(&env, &client, "HEAD"), tips[2]);
    fsck(&env, &client);
    assert_eq!(
        (s.metrics().packs_by_uri(), s.metrics().packs_stitched()),
        (3, 0)
    );
    let m = w.express.metrics();
    assert_eq!((m.fills(), m.misses()), (3, 3));

    let t3 = env.commit(&src, "file.txt", &lines(140, "v3"));
    s.push(&seeder, &Push::new("refs/heads/main", &t3));
    let trace = env.uri_fetch(&client);
    assert_eq!(uris(&trace).len(), 1, "only the new version's pack");
    assert_eq!(rev(&env, &client, "origin/main"), t3);
    fsck(&env, &client);
    assert_eq!(
        (s.metrics().packs_by_uri(), s.metrics().packs_stitched()),
        (4, 0)
    );

    // A second client is served from the express cache.
    let hits = m.hits();
    let again = env.path("again");
    env.uri_clone(&s.url("demo"), &again, &[]);
    fsck(&env, &again);
    assert_eq!(m.hits(), hits + 4);
    assert_eq!(m.fills(), 4);

    let plain = env.path("plain");
    run(env
        .git(env.tmp.path())
        .args(["clone", "--quiet", &s.url("demo")])
        .arg(&plain));
    fsck(&env, &plain);
    assert_eq!(s.metrics().packs_stitched(), 4);

    let text = metrics(s);
    for name in [
        "aspen_express_uris_total 8",
        "aspen_express_cache_fills_total 4",
        "aspen_express_endpoint_up{tier=\"0\"",
    ] {
        assert!(text.contains(name), "{name}\n{text}");
    }
}

/// Invariant (CDN mode): URIs point at the edge, and clients in the same
/// signing window get the same URL per pack, so a second clone is served
/// entirely from the edge's cache (no new origin requests) and every
/// response the edge kept was one the origin marked `public` with a
/// `max-age` no longer than the URL's life. Clones and an incremental
/// fetch through the edge pass `fsck --strict`. A forged URL through the
/// edge is refused by the origin every time and never cached.
#[test]
fn cdn_mode_edge_serves_repeat_clones_from_cache() {
    let env = Env::new();
    let src = env.source();
    let tips: Vec<String> = (0..3)
        .map(|i| env.commit(&src, "file.txt", &lines(150 + i * 10, &format!("c{i}"))))
        .collect();
    let edge_slot: Arc<Mutex<Option<Edge>>> = Arc::default();
    let es = Arc::clone(&edge_slot);
    // Sign at a fixed time, so both clones fall in one signing window even
    // if the test runs across a window boundary.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let w = wired_at(
        &env,
        anyone_reads(),
        move |origin| {
            let edge = Edge::start(origin);
            let tier = Tier::new(&[edge.base.as_str()], PastEndRange::Empty206).unwrap();
            *es.lock().unwrap() = Some(edge);
            vec![tier]
        },
        Some(now),
    );
    let edge = edge_slot.lock().unwrap().take().unwrap();
    let s = &w.server;
    let seeder = seed(s, "demo", &src, &tips);

    let a = env.path("a");
    let first = uris(&env.uri_clone(&s.url("demo"), &a, &[]));
    assert_eq!(first.len(), 3);
    for u in &first {
        assert!(u.starts_with(&format!("{}/t1/", edge.base)), "{u}");
    }
    fsck(&env, &a);
    let (origin_requests, edge_misses) = (
        w.express.metrics().misses() + w.express.metrics().hits(),
        edge.misses(),
    );
    assert!(edge_misses >= 3);
    assert_eq!(edge.cached(), 3);

    let b = env.path("b");
    let second = uris(&env.uri_clone(&s.url("demo"), &b, &[]));
    assert_eq!(first, second, "same window, same URLs");
    fsck(&env, &b);
    assert_eq!(
        edge.misses(),
        edge_misses,
        "the second clone never reached the origin"
    );
    assert!(edge.hits() >= 3);
    assert_eq!(
        w.express.metrics().misses() + w.express.metrics().hits(),
        origin_requests
    );

    let t3 = env.commit(&src, "file.txt", &lines(190, "c3"));
    s.push(&seeder, &Push::new("refs/heads/main", &t3));
    let fetched = uris(&env.uri_fetch(&a));
    assert_eq!(fetched.len(), 1);
    assert!(fetched[0].starts_with(&edge.base));
    assert_eq!(rev(&env, &a, "origin/main"), t3);
    fsck(&env, &a);

    // A forged URL reaches the origin each time and is never stored.
    let sig = sig_of(&first[0]);
    let forged = first[0].replacen(&sig, &flip_first(&sig), 1);
    let cached = edge.cached();
    for _ in 0..2 {
        let (status, head, _) = get(&forged, &[]);
        assert_eq!(status, 403);
        assert_eq!(header(&head, "cache-control"), Some("no-store"));
    }
    assert_eq!(edge.cached(), cached);
    assert_eq!(w.express.metrics().refusals(Refusal::BadSignature), 2);

    // Range requests through the edge: exact 206, and an empty 206 past
    // the end (the tier is declared Empty206 because this edge passes the
    // origin's answer through).
    let (status, head, body) = get(&first[0], &[("Range", "bytes=0-3")]);
    assert_eq!((status, body.as_slice()), (206, &b"PACK"[..]));
    let size: u64 = header(&head, "content-range")
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let (status, head, body) = get(&first[0], &[("Range", &format!("bytes={size}-"))]);
    assert_eq!((status, body.len()), (206, 0));
    assert_eq!(
        header(&head, "content-range"),
        Some(format!("bytes */{size}").as_str())
    );
}

/// Invariant: express URLs are only signed for a fetch B3's guard allowed,
/// so a caller without a grant on a hidden repository gets no URL; the
/// express route is outside the guard, but forged URLs and URLs signed for
/// another repository's pack are refused. A caller with a grant clones
/// through express URIs that carry no credential (git does not send the
/// URL-scoped `Authorization` to them).
#[test]
fn hidden_repository_packs_need_a_grant() {
    let env = Env::new();
    let src = env.source();
    let tips = vec![env.commit(&src, "f.txt", &lines(80, "secret"))];
    let policy = aspen_auth::PolicyConfig {
        anonymous: vec!["read:open".parse().unwrap()],
        ..aspen_auth::PolicyConfig::default()
    };
    let w = wired(&env, policy, |origin| {
        vec![Tier::new(&[origin], PastEndRange::Empty206).unwrap()]
    });
    let s = &w.server;
    seed(s, "secret", &src, &tips);
    seed(s, "open", &src, &tips);

    // Anonymous: the guard refuses the hidden repository before any plan,
    // so nothing is signed.
    let out = env
        .git(env.tmp.path())
        .args([
            "-c",
            "fetch.uriprotocols=http",
            "clone",
            "--quiet",
            &s.url("secret"),
        ])
        .arg(env.path("anon"))
        .output()
        .unwrap();
    assert!(!out.status.success());
    let signed_before = metrics(s);
    assert!(
        signed_before.contains("aspen_express_uris_total 0"),
        "{signed_before}"
    );

    // Anonymous on the open repository: URIs on the express route.
    let open_uris = uris(&env.uri_clone(&s.url("open"), &env.path("open"), &[]));
    assert_eq!(open_uris.len(), 1);
    fsck(&env, &env.path("open"));

    // With a grant, scoped to the Git URL only.
    let token = mint(s, &["read:secret"]);
    let scoped = format!(
        "http.{}.extraHeader=Authorization: {}",
        s.url("secret"),
        basic_token(&token)
    );
    let mine = env.path("mine");
    let secret_uris = uris(&env.uri_clone(&s.url("secret"), &mine, &["-c", &scoped]));
    assert_eq!(secret_uris.len(), 1);
    fsck(&env, &mine);
    assert_eq!(rev(&env, &mine, "HEAD"), tips[0]);

    // The two repositories hold the same pack bytes under different keys.
    let repo_of = |u: &str| {
        u.split("/repos/")
            .nth(1)
            .unwrap()
            .split('/')
            .next()
            .unwrap()
            .to_owned()
    };
    let (open_id, secret_id) = (repo_of(&open_uris[0]), repo_of(&secret_uris[0]));
    assert_ne!(open_id, secret_id);
    assert_eq!(
        open_uris[0].rsplit('/').next(),
        secret_uris[0].rsplit('/').next(),
        "same checksum"
    );
    // An open URL rewritten to the hidden repository's copy is refused.
    let crossed = open_uris[0].replace(
        &format!("/repos/{open_id}/"),
        &format!("/repos/{secret_id}/"),
    );
    assert_eq!(get(&crossed, &[]).0, 403);
    // So are a forged signature and a guessed, unsigned path.
    let sig = sig_of(&secret_uris[0]);
    assert_eq!(
        get(&secret_uris[0].replacen(&sig, &flip_first(&sig), 1), &[]).0,
        403
    );
    assert_eq!(
        get(&secret_uris[0].replacen(&sig, &"0".repeat(64), 1), &[]).0,
        403
    );
    assert_eq!(
        get(&format!("{}/repos/{secret_id}/packs/x.pack", w.origin), &[]).0,
        403
    );
    // The genuine URL works without any credential.
    let (status, _, body) = get(&secret_uris[0], &[]);
    assert_eq!(status, 200);
    assert!(body.starts_with(b"PACK"));
}

/// Invariant: when the CDN path is unavailable, fetches still succeed: the
/// prober marks the edge down and URIs move to the next tier (express
/// nodes); with every express endpoint down, the pack falls back to the
/// blob store's own URL or, here with that off for the test, the stitched
/// inline pack, and the fallback is counted. When the edge recovers, URIs
/// return to it.
#[test]
fn stitched_fallback_when_the_cdn_path_is_down() {
    let env = Env::new();
    let src = env.source();
    let tips: Vec<String> = (0..2)
        .map(|i| env.commit(&src, "file.txt", &lines(60 + i, &format!("d{i}"))))
        .collect();
    let edge_slot: Arc<Mutex<Option<Edge>>> = Arc::default();
    let es = Arc::clone(&edge_slot);
    let w = wired(&env, anyone_reads(), move |origin| {
        let edge = Edge::start(origin);
        let tiers = vec![
            Tier::new(&[edge.base.as_str()], PastEndRange::Empty206).unwrap(),
            Tier::new(&[origin], PastEndRange::Empty206).unwrap(),
        ];
        *es.lock().unwrap() = Some(edge);
        tiers
    });
    let edge = edge_slot.lock().unwrap().take().unwrap();
    let s = &w.server;
    seed(s, "demo", &src, &tips);
    let http = aspen_express::routes::probe_client(Duration::from_secs(2));
    let probe = || s.block_on(w.routes.probe(&http));
    let endpoint_up = |t: usize| w.routes.tiers()[t].endpoints()[0].is_up();

    probe();
    assert!(endpoint_up(0) && endpoint_up(1));
    let c1 = env.path("c1");
    let sent = uris(&env.uri_clone(&s.url("demo"), &c1, &[]));
    assert!(sent.iter().all(|u| u.starts_with(&edge.base)), "{sent:?}");
    fsck(&env, &c1);

    // The edge goes down: the probe notices, URIs go to the express tier.
    edge.down.store(true, Ordering::Relaxed);
    probe();
    assert!(!endpoint_up(0) && endpoint_up(1));
    let c2 = env.path("c2");
    let sent = uris(&env.uri_clone(&s.url("demo"), &c2, &[]));
    assert_eq!(sent.len(), 2);
    assert!(
        sent.iter()
            .all(|u| u.starts_with(&format!("{}/", w.origin))),
        "{sent:?}"
    );
    fsck(&env, &c2);

    // Every express endpoint is down: no express URI. The blob store's own
    // URL (B2's `/-/blobs`) is next; the stitched pack comes after that
    // (next test).
    w.routes.tiers()[1].endpoints()[0].set_up(false);
    let c3 = env.path("c3");
    let sent = uris(&env.uri_clone(&s.url("demo"), &c3, &[]));
    assert!(sent.iter().all(|u| u.contains("/-/blobs/")), "{sent:?}");
    fsck(&env, &c3);
    let text = metrics(s);
    assert!(text.contains("aspen_express_unavailable_total 2"), "{text}");
    assert!(
        text.contains("aspen_express_endpoint_up{tier=\"0\""),
        "{text}"
    );

    // Recovery: the edge comes back and the next probe restores it.
    edge.down.store(false, Ordering::Relaxed);
    probe();
    assert!(endpoint_up(0));
    let c4 = env.path("c4");
    let sent = uris(&env.uri_clone(&s.url("demo"), &c4, &[]));
    assert!(sent.iter().all(|u| u.starts_with(&edge.base)), "{sent:?}");
    fsck(&env, &c4);
}

/// Invariant: when the only express tier the client could use is down and
/// the store's own URL does not fit the client either, every pack is
/// stitched into the inline pack, the clone passes `fsck --strict`, and the
/// fallback is counted.
#[test]
fn stitched_pack_when_no_uri_source_is_available() {
    let env = Env::new();
    let src = env.source();
    let tips: Vec<String> = (0..2)
        .map(|i| env.commit(&src, "file.txt", &lines(70 + i, &format!("s{i}"))))
        .collect();
    let dead = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        format!("https://{}", l.local_addr().unwrap())
    };
    let w = wired(&env, anyone_reads(), |_| {
        vec![Tier::new(&[dead.as_str()], PastEndRange::Empty206).unwrap()]
    });
    let s = &w.server;
    seed(s, "demo", &src, &tips);
    let http = aspen_express::routes::probe_client(Duration::from_secs(2));
    s.block_on(w.routes.probe(&http));
    assert!(!w.routes.tiers()[0].endpoints()[0].is_up());
    // `fetch.uriprotocols=https`: the express tier fits the client but is
    // down, and the local store's `/-/blobs` URLs are http.
    let c = env.path("c");
    run(env
        .git(env.tmp.path())
        .args([
            "-c",
            "fetch.uriprotocols=https",
            "clone",
            "--quiet",
            &s.url("demo"),
        ])
        .arg(&c));
    fsck(&env, &c);
    assert_eq!(rev(&env, &c, "HEAD"), tips[1]);
    assert_eq!(
        (s.metrics().packs_by_uri(), s.metrics().packs_stitched()),
        (0, 2)
    );
    assert!(metrics(s).contains("aspen_express_unavailable_total 2"));
}

/// Stores a token for subject `agent:test` with `grants`; returns its text.
fn mint(s: &TestServer, grants: &[&str]) -> String {
    use aspen_auth::token::issue;
    use aspen_auth::{Grants, NewToken};
    let issued = issue(
        NewToken {
            subject: "agent:test".into(),
            grants: Grants::new(grants.iter().map(|g| g.parse().unwrap())),
            description: String::new(),
            expires_at: None,
        },
        aspen_core::id::UnixMicros::now(),
    )
    .unwrap();
    s.access.auth().tokens().insert(issued.record);
    issued.token.expose().to_owned()
}

/// `Basic` credentials for a token (Git's credential form).
fn basic_token(token: &str) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input = format!("x-access-token:{token}");
    let mut out = String::new();
    for chunk in input.as_bytes().chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            out.push(if i <= chunk.len() {
                T[((n >> (18 - 6 * i)) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    format!("Basic {out}")
}
