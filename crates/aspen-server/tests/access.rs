//! The access middleware against a recording Git handler: who reaches a
//! repository, which one, and what is refused before the handler runs.

mod common;

use std::sync::{Arc, Mutex};

use aspen_auth::{LimitsConfig, PushQuota, RateLimit};
use aspen_server::access::{Authorized, ClientAddr, PushBodyError};
use axum::Router;
use axum::body::Body;
use axum::extract::Extension;
use axum::http::{Request, StatusCode, header};
use common::{Log, Seen, access, access_with, app, basic, meta_with, mint};
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;

/// A Git handler that records what reaches it. Like receive-pack, it
/// stores a body only once it has read all of it.
fn recorder(log: Log) -> Router {
    Router::new().fallback(move |Extension(a): Extension<Authorized>, body: Body| {
        let log = log.clone();
        async move {
            log.lock().unwrap().calls.push((
                format!("{}#{:?}", a.repo.name, a.repo.id),
                format!("{:?}", a.route.endpoint),
            ));
            match body.collect().await {
                Ok(b) => log.lock().unwrap().stored.push(b.to_bytes().to_vec()),
                Err(e) => {
                    let quota = std::error::Error::source(&e)
                        .and_then(|s| s.downcast_ref::<PushBodyError>())
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| e.to_string());
                    log.lock().unwrap().failed.push(quota);
                }
            }
            "ok"
        }
    })
}

struct Harness {
    app: Router,
    log: Log,
    access: Arc<common::TestAccess>,
}

async fn harness(policy: serde_json::Value, limits: LimitsConfig) -> Harness {
    let meta = meta_with(&["acme/a", "acme/b", "demo/pub"]).await;
    let access = access(policy, limits, meta);
    let log: Log = Arc::new(Mutex::new(Seen::default()));
    Harness {
        app: app(access.clone(), recorder(log.clone())),
        log,
        access,
    }
}

const ENDPOINTS: [(&str, &str); 4] = [
    ("GET", "info/refs?service=git-upload-pack"),
    ("POST", "git-upload-pack"),
    ("GET", "info/refs?service=git-receive-pack"),
    ("POST", "git-receive-pack"),
];

fn req(method: &str, url: &str, auth: Option<&str>, body: Vec<u8>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(url);
    if let Some(a) = auth {
        b = b.header(header::AUTHORIZATION, a);
    }
    if method == "POST" {
        b = b.header(header::CONTENT_LENGTH, body.len());
    }
    b.body(Body::from(body)).unwrap()
}

async fn send(app: &Router, r: Request<Body>) -> (StatusCode, axum::http::HeaderMap, String) {
    let resp = app.clone().oneshot(r).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, headers, String::from_utf8_lossy(&body).into_owned())
}

/// Invariant: an unauthenticated request can neither read nor write a
/// private repository. Every endpoint answers 401 with a `Basic` challenge,
/// the handler never runs, and the answer is byte-identical whether the
/// repository exists or not.
#[tokio::test]
async fn unauthenticated_requests_reach_no_private_repository() {
    let h = harness(
        json!({ "anonymous": ["read:demo/*"] }),
        LimitsConfig::default(),
    )
    .await;
    for (method, endpoint) in ENDPOINTS {
        let mut answers = Vec::new();
        for repo in ["acme/a", "acme/missing"] {
            let url = format!("/{repo}.git/{endpoint}");
            let (status, headers, body) =
                send(&h.app, req(method, &url, None, b"0000".to_vec())).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {url}");
            assert!(
                headers[header::WWW_AUTHENTICATE]
                    .to_str()
                    .unwrap()
                    .starts_with("Basic realm="),
                "{url}"
            );
            answers.push((status, body));
        }
        assert_eq!(answers[0], answers[1], "{endpoint}: existence leaked");
    }
    // Anonymous may read the public repository, and still not push to it.
    for (method, endpoint) in ENDPOINTS {
        let url = format!("/demo/pub/{endpoint}");
        let (status, ..) = send(&h.app, req(method, &url, None, Vec::new())).await;
        let want = if endpoint.contains("receive") {
            StatusCode::UNAUTHORIZED
        } else {
            StatusCode::OK
        };
        assert_eq!(status, want, "{method} {url}");
    }
    let log = h.log.lock().unwrap();
    assert_eq!(log.calls.len(), 2, "{:?}", log.calls);
    assert!(
        log.calls
            .iter()
            .all(|(repo, _)| repo.starts_with("demo/pub#"))
    );
}

/// Invariant: the guard lets nothing but a routed Git endpoint through. A
/// path that is not a Git endpoint (or a dumb-HTTP file) answers 404 and
/// the handler never runs, so it can neither serve data nor fail with a
/// missing `Authorized` extension; a request carrying two `Authorization`
/// headers is refused (400) instead of having one of them picked.
#[tokio::test]
async fn only_git_endpoints_reach_the_handler() {
    let h = harness(
        json!({ "anonymous": ["read:demo/*"] }),
        LimitsConfig::default(),
    )
    .await;
    for url in [
        "/",
        "/healthz",
        "/demo/pub.git",
        "/demo/pub.git/HEAD",
        "/demo/pub.git/objects/info/packs",
    ] {
        for method in ["GET", "POST"] {
            let (status, ..) = send(&h.app, req(method, url, None, Vec::new())).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{method} {url}");
        }
    }
    let mut two = req(
        "GET",
        "/demo/pub.git/info/refs?service=git-upload-pack",
        None,
        Vec::new(),
    );
    for auth in ["Bearer a", "Bearer b"] {
        two.headers_mut()
            .append(header::AUTHORIZATION, auth.parse().unwrap());
    }
    let (status, ..) = send(&h.app, two).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.log.lock().unwrap().calls.is_empty());
}

/// Invariant: one repository's token cannot reach another repository. A
/// token for `acme/a` reads and pushes `acme/a`; on `acme/b` every endpoint
/// answers 404 exactly as for a repository that does not exist, and the
/// handler never runs. A read token cannot push (403). Requests reach the
/// handler with the repository they named (multi-repository routing).
#[tokio::test]
async fn a_token_reaches_only_its_repository() {
    let h = harness(json!({}), LimitsConfig::default()).await;
    let writer = basic(
        "x-access-token",
        &mint(&h.access, "agent:a", &["write:acme/a"]),
    );
    let reader = basic(
        "x-access-token",
        &mint(&h.access, "agent:r", &["read:acme/b"]),
    );

    for (method, endpoint) in ENDPOINTS {
        let ok = send(
            &h.app,
            req(
                method,
                &format!("/acme/a.git/{endpoint}"),
                Some(&writer),
                vec![],
            ),
        )
        .await;
        assert_eq!(ok.0, StatusCode::OK, "{method} {endpoint}");
        let other = send(
            &h.app,
            req(
                method,
                &format!("/acme/b.git/{endpoint}"),
                Some(&writer),
                vec![],
            ),
        )
        .await;
        let missing = send(
            &h.app,
            req(
                method,
                &format!("/acme/zz.git/{endpoint}"),
                Some(&writer),
                vec![],
            ),
        )
        .await;
        assert_eq!(other.0, StatusCode::NOT_FOUND, "{method} {endpoint}");
        assert_eq!((other.0, &other.2), (missing.0, &missing.2));
        // The scoped URL of the other repository is no way in either.
        let scoped = format!("/acme/b.git/-/refs/heads/main/{endpoint}");
        assert_eq!(
            send(&h.app, req(method, &scoped, Some(&writer), vec![]))
                .await
                .0,
            StatusCode::NOT_FOUND
        );
    }
    for (method, endpoint) in ENDPOINTS {
        let (status, ..) = send(
            &h.app,
            req(
                method,
                &format!("/acme/b/{endpoint}"),
                Some(&reader),
                vec![],
            ),
        )
        .await;
        let want = if endpoint.contains("receive") {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::OK
        };
        assert_eq!(status, want, "{method} {endpoint}");
    }
    // A signed-in caller asking for a repository that its grant covers but
    // that does not exist gets the same 404.
    let (status, ..) = send(
        &h.app,
        req(
            "GET",
            "/acme/a2.git/info/refs?service=git-upload-pack",
            Some(&writer),
            vec![],
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let log = h.log.lock().unwrap();
    let a: Vec<_> = log
        .calls
        .iter()
        .filter(|(r, _)| r.starts_with("acme/a#"))
        .collect();
    let b: Vec<_> = log
        .calls
        .iter()
        .filter(|(r, _)| r.starts_with("acme/b#"))
        .collect();
    assert_eq!(a.len(), 4);
    assert_eq!(
        b.len(),
        2,
        "the read token reached only acme/b's fetch endpoints"
    );
    assert!(b.iter().all(|(_, e)| e.contains("Upload")));
    assert_eq!(log.calls.len(), 6);
    // Different repositories, different IDs.
    assert_ne!(a[0].0, b[0].0);
}

/// Invariant: a credential that is presented and wrong is refused, never
/// treated as anonymous, even on a public repository.
#[tokio::test]
async fn a_bad_credential_is_not_anonymous() {
    let h = harness(
        json!({ "anonymous": ["read:demo/*"] }),
        LimitsConfig::default(),
    )
    .await;
    let good = mint(&h.access, "agent:a", &["write:acme/a"]);
    let mut bad = good.clone();
    let last = bad.pop().unwrap();
    bad.push(if last == '0' { '1' } else { '0' });
    for auth in [
        basic("x", &bad),
        format!("Bearer {bad}"),
        "Digest x".into(),
        basic("x", ""),
    ] {
        let (status, headers, _) = send(
            &h.app,
            req(
                "GET",
                "/demo/pub/info/refs?service=git-upload-pack",
                Some(&auth),
                vec![],
            ),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{auth:.20}");
        assert!(headers.contains_key(header::WWW_AUTHENTICATE));
    }
    assert!(h.log.lock().unwrap().calls.is_empty());
}

/// Invariant (B3 review obligation 1): every presented credential that
/// fails is charged to the client address; once its bucket is empty a
/// request with a credential is refused with 429 before the credential is
/// checked, a valid one included, while anonymous requests and other
/// addresses are unaffected. A successful authentication costs nothing.
#[tokio::test]
async fn failed_authentications_are_limited_per_address() {
    let limits = LimitsConfig {
        auth_failures: Some(RateLimit {
            burst: 3,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let h = harness(json!({ "anonymous": ["read:demo/*"] }), limits).await;
    let good = basic("x", &mint(&h.access, "agent:a", &["read:acme/*"]));
    let bad = basic(
        "x",
        "aspen_00000000000000000000000000000000_0000000000000000000000000000000000000000000000000000000000000000",
    );
    let from = |addr: &str, auth: Option<&str>, repo: &str| {
        let mut r = req(
            "GET",
            &format!("/{repo}/info/refs?service=git-upload-pack"),
            auth,
            vec![],
        );
        r.extensions_mut().insert(axum::extract::ConnectInfo(
            addr.parse::<std::net::SocketAddr>().unwrap(),
        ));
        r
    };
    for _ in 0..10 {
        assert_eq!(
            send(&h.app, from("192.0.2.1:1", Some(&good), "acme/a"))
                .await
                .0,
            StatusCode::OK
        );
    }
    for _ in 0..3 {
        assert_eq!(
            send(&h.app, from("192.0.2.1:2", Some(&bad), "acme/a"))
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
    }
    let (status, headers, body) = send(&h.app, from("192.0.2.1:3", Some(&bad), "acme/a")).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(headers.contains_key(header::RETRY_AFTER));
    assert!(body.contains("failed authentications"), "{body}");
    assert_eq!(
        send(&h.app, from("192.0.2.1:4", Some(&good), "acme/a"))
            .await
            .0,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(
        send(&h.app, from("192.0.2.1:5", None, "demo/pub")).await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(&h.app, from("198.51.100.7:1", Some(&good), "acme/a"))
            .await
            .0,
        StatusCode::OK
    );
}

/// Invariant (D4 review): requests whose client address is unknown (here:
/// no `ConnectInfo`) do not share one failed-authentication bucket, so
/// a stream of bad credentials from some of them cannot lock every
/// credential out.
#[tokio::test]
async fn an_unknown_address_does_not_share_a_failure_bucket() {
    let limits = LimitsConfig {
        auth_failures: Some(RateLimit {
            burst: 3,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let h = harness(json!({}), limits).await;
    let good = basic("x", &mint(&h.access, "agent:a", &["read:acme/*"]));
    let bad = basic(
        "x",
        "aspen_00000000000000000000000000000000_0000000000000000000000000000000000000000000000000000000000000000",
    );
    let url = "/acme/a/info/refs?service=git-upload-pack";
    for _ in 0..10 {
        assert_eq!(
            send(&h.app, req("GET", url, Some(&bad), vec![])).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        send(&h.app, req("GET", url, Some(&good), vec![])).await.0,
        StatusCode::OK
    );
}

/// A token store whose lookups are counted and wait until `open` is true,
/// so a test can hold many credential checks in flight at once. Answers
/// from an inner [`aspen_auth::MemoryTokenStore`].
#[derive(Clone)]
struct GatedTokens(Arc<Gate>);

struct Gate {
    inner: aspen_auth::MemoryTokenStore,
    lookups: std::sync::atomic::AtomicUsize,
    open: tokio::sync::watch::Sender<bool>,
    /// Every lookup fails as a store outage.
    down: std::sync::atomic::AtomicBool,
}

impl GatedTokens {
    fn lookups(&self) -> usize {
        self.0.lookups.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl aspen_auth::TokenStore for GatedTokens {
    async fn token(
        &self,
        id: &aspen_auth::TokenId,
    ) -> Result<Option<aspen_auth::TokenRecord>, aspen_auth::TokenStoreError> {
        self.0
            .lookups
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut open = self.0.open.subscribe();
        // The sender lives in `self`, so the wait cannot fail.
        let _ = open.wait_for(|o| *o).await;
        if self.0.down.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(aspen_auth::TokenStoreError("connection refused".into()));
        }
        self.0.inner.token(id).await
    }
}

/// Invariant (issue #198): credentials that arrive together from one
/// address cannot all pass the failed-authentication check on the same
/// token. With a bucket of `burst` and `burst + K` bad credentials in
/// flight at once, at most `burst` are looked up and the other `K` are
/// refused with 429 before any lookup; successes, and checks a store
/// outage stopped, still cost nothing, even while several are in flight
/// together.
#[tokio::test]
async fn concurrent_bad_credentials_cannot_overshoot_the_failure_bucket() {
    const BURST: u32 = 3;
    const EXTRA: usize = 7;
    let limits = LimitsConfig {
        auth_failures: Some(RateLimit {
            burst: BURST,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let tokens = GatedTokens(Arc::new(Gate {
        inner: aspen_auth::MemoryTokenStore::new(),
        lookups: 0.into(),
        open: tokio::sync::watch::channel(false).0,
        down: false.into(),
    }));
    let policy: aspen_auth::PolicyConfig = serde_json::from_value(json!({})).unwrap();
    let auth = aspen_auth::AuthService::new(policy, tokens.clone()).unwrap();
    let access = Arc::new(aspen_server::access::Access::new(
        auth,
        aspen_auth::Limits::new(limits),
        meta_with(&["acme/a"]).await,
        ClientAddr::Peer,
    ));
    let good = {
        let issued = aspen_auth::token::issue(
            aspen_auth::NewToken {
                subject: "agent:a".into(),
                grants: aspen_auth::Grants::new(["read:acme/*".parse().unwrap()]),
                description: String::new(),
                expires_at: None,
            },
            aspen_core::id::UnixMicros::now(),
        )
        .unwrap();
        tokens.0.inner.insert(issued.record);
        basic("x", issued.token.expose())
    };
    let bad = basic(
        "x",
        "aspen_00000000000000000000000000000000_0000000000000000000000000000000000000000000000000000000000000000",
    );
    let app = recorder(Arc::new(Mutex::new(Seen::default()))).layer(
        axum::middleware::from_fn_with_state(
            access,
            aspen_server::access::guard::<aspen_meta::SqlStore, GatedTokens, aspen_auth::NoSshKeys>,
        ),
    );
    // Sends `n` requests with `auth` from one address at once, waits until
    // each has either been answered or reached the token store, then opens
    // the store and returns every answer and the lookups made.
    let burst = |auth: String, n: usize| {
        let app = app.clone();
        let tokens = tokens.clone();
        async move {
            let before = tokens.lookups();
            tokens.0.open.send_replace(false);
            let answered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let tasks: Vec<_> = (0..n)
                .map(|i| {
                    let mut r = req(
                        "GET",
                        "/acme/a/info/refs?service=git-upload-pack",
                        Some(&auth),
                        vec![],
                    );
                    r.extensions_mut().insert(axum::extract::ConnectInfo(
                        std::net::SocketAddr::from(([192, 0, 2, 1], 1000 + i as u16)),
                    ));
                    let (app, answered) = (app.clone(), answered.clone());
                    tokio::spawn(async move {
                        let status = app.oneshot(r).await.unwrap().status();
                        answered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        status
                    })
                })
                .collect();
            let settled = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while answered.load(std::sync::atomic::Ordering::SeqCst) + tokens.lookups() - before
                    < n
                {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await;
            assert!(settled.is_ok(), "requests neither answered nor looked up");
            let lookups = tokens.lookups() - before;
            tokens.0.open.send_replace(true);
            let mut statuses = Vec::new();
            for t in tasks {
                statuses.push(t.await.unwrap());
            }
            (statuses, lookups)
        }
    };
    let count =
        |statuses: &[StatusCode], want: StatusCode| statuses.iter().filter(|s| **s == want).count();

    // A store outage is not the client's failure: each check's token is
    // given back, so many in a row leave the bucket full.
    tokens
        .0
        .down
        .store(true, std::sync::atomic::Ordering::SeqCst);
    for _ in 0..3 * BURST {
        let (statuses, _) = burst(bad.clone(), 1).await;
        assert_eq!(statuses, [StatusCode::SERVICE_UNAVAILABLE]);
    }
    tokens
        .0
        .down
        .store(false, std::sync::atomic::Ordering::SeqCst);

    // As many good credentials at once as the bucket holds: each takes a
    // token while it is checked and gives it back when it succeeds.
    let (statuses, lookups) = burst(good.clone(), BURST as usize).await;
    assert_eq!(
        count(&statuses, StatusCode::OK),
        BURST as usize,
        "{statuses:?}"
    );
    assert_eq!(lookups, BURST as usize);

    // `burst + K` bad credentials at once: only `burst` are checked.
    let (statuses, lookups) = burst(bad.clone(), BURST as usize + EXTRA).await;
    assert!(
        lookups <= BURST as usize,
        "{lookups} bad credentials were looked up with a bucket of {BURST}"
    );
    assert_eq!(
        count(&statuses, StatusCode::UNAUTHORIZED),
        BURST as usize,
        "{statuses:?}"
    );
    assert_eq!(
        count(&statuses, StatusCode::TOO_MANY_REQUESTS),
        EXTRA,
        "{statuses:?}"
    );

    // The bucket is spent: a good credential is refused unchecked too.
    let (statuses, lookups) = burst(good, 1).await;
    assert_eq!(statuses, [StatusCode::TOO_MANY_REQUESTS]);
    assert_eq!(lookups, 0);
}

/// How the tests below take client addresses: behind `hops` proxies whose
/// own addresses are private (the default trusted proxies).
fn forwarded(hops: usize) -> ClientAddr {
    ClientAddr::from_settings(hops, None).unwrap()
}

/// Invariant (issue #121): behind a trusted proxy, whatever a client
/// writes into `X-Forwarded-For`, its failed authentications are charged
/// to one limited bucket: the address the proxy recorded, or the TCP peer
/// when no recorded address can be read (a client that reaches the server
/// directly, an entry that is not an address, a header too short). A
/// client can neither pick a fresh bucket per request nor fall into one
/// that is never limited; a proxy's entry with a port, or after a client
/// entry that is not ASCII, is still that client's address.
#[tokio::test]
async fn a_client_cannot_choose_its_failure_bucket_through_x_forwarded_for() {
    let limits = LimitsConfig {
        auth_failures: Some(RateLimit {
            burst: 3,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let meta = meta_with(&["acme/a"]).await;
    let access = access_with(json!({}), limits, meta, forwarded(1));
    let app = app(
        access.clone(),
        recorder(Arc::new(Mutex::new(Seen::default()))),
    );
    let good = basic("x", &mint(&access, "agent:a", &["read:acme/*"]));
    let bad = basic(
        "x",
        "aspen_00000000000000000000000000000000_0000000000000000000000000000000000000000000000000000000000000000",
    );
    // A request from TCP peer `peer` carrying these `X-Forwarded-For`
    // lines, byte for byte.
    let from = |peer: &str, lines: &[&[u8]], auth: &str| {
        let mut r = req(
            "GET",
            "/acme/a/info/refs?service=git-upload-pack",
            Some(auth),
            vec![],
        );
        r.extensions_mut().insert(axum::extract::ConnectInfo(
            peer.parse::<std::net::SocketAddr>().unwrap(),
        ));
        for line in lines {
            r.headers_mut().append(
                "x-forwarded-for",
                axum::http::HeaderValue::from_bytes(line).unwrap(),
            );
        }
        r
    };
    // Three failures spend a bucket; `then` must find it spent.
    let spend = |first: [Request<Body>; 3], then: Request<Body>| {
        let app = app.clone();
        async move {
            for r in first {
                assert_eq!(send(&app, r).await.0, StatusCode::UNAUTHORIZED);
            }
            send(&app, then).await.0
        }
    };

    // A client that reaches the server directly is its TCP address,
    // whatever it claims.
    let direct = "198.51.100.7:1";
    let status = spend(
        [
            from(direct, &[b"203.0.113.1"], &bad),
            from(direct, &[b"203.0.113.2"], &bad),
            from(direct, &[b"10.0.0.9, 203.0.113.3"], &bad),
        ],
        from(direct, &[b"203.0.113.4"], &good),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "direct client");
    assert_eq!(
        send(&app, from(direct, &[], &good)).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );

    // Behind the proxy at 10.0.0.2, a client entry that is not ASCII does
    // not hide the address the proxy appended to the same line.
    let proxy = "10.0.0.2:1";
    let status = spend(
        [
            from(proxy, &[b"\xff, 203.0.113.9"], &bad),
            from(proxy, &[b"\xff", b"203.0.113.9"], &bad),
            from(proxy, &[b"x\x80y,203.0.113.9"], &bad),
        ],
        from(proxy, &[b"203.0.113.9"], &good),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "non-ASCII entry");
    // ... and that bucket is the client's, not the proxy's.
    assert_eq!(
        send(&app, from(proxy, &[b"203.0.113.10"], &good)).await.0,
        StatusCode::OK
    );

    // A proxy that records ports (`ip:port`, `[v6]:port`) names the same
    // client as one that does not.
    let status = spend(
        [
            from(proxy, &[b"203.0.113.20:4711"], &bad),
            from(proxy, &[b"203.0.113.20:4712"], &bad),
            from(proxy, &[b"6.6.6.6, 203.0.113.20"], &bad),
        ],
        from(proxy, &[b"203.0.113.20:1"], &good),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "IPv4 with a port");
    let status = spend(
        [
            from(proxy, &[b"[2001:db8::20]:443"], &bad),
            from(proxy, &[b"[2001:db8::21]"], &bad),
            from(proxy, &[b"2001:db8::22"], &bad),
        ],
        from(proxy, &[b"[2001:db8::23]:80"], &good),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "IPv6 with a port");

    // No address at the read position (not an address, or a header too
    // short): the proxy's own bucket, which is limited like any other.
    for (proxy, lines) in [
        ("10.0.0.3:1", &[&b"unknown"[..]][..]),
        ("10.0.0.4:1", &[&b""[..]][..]),
        ("10.0.0.5:1", &[][..]),
    ] {
        let status = spend(
            [
                from(proxy, lines, &bad),
                from(proxy, lines, &bad),
                from(proxy, lines, &bad),
            ],
            from(proxy, lines, &good),
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{proxy} {lines:?}");
    }
    assert_eq!(
        send(&app, from("10.0.0.6:1", &[b"203.0.113.30"], &good))
            .await
            .0,
        StatusCode::OK
    );
}

/// Invariant: an SSH key's fingerprint is public, so it is never an HTTP
/// credential. With a live key registered for a private repository, no
/// `Authorization` header built from the key (its fingerprint as a token,
/// a password or a user name, under any scheme, or the key itself) gets
/// past the guard; only the SSH transport, after checking the client's
/// signature, identifies a key.
#[tokio::test]
async fn an_ssh_key_fingerprint_is_not_an_http_credential() {
    use aspen_auth::ssh::register;
    use aspen_auth::{NewSshKey, SshPublicKey};
    use aspen_core::auth::Credential;
    use aspen_core::id::UnixMicros;

    let h = harness(json!({}), LimitsConfig::default()).await;
    let mut blob = Vec::new();
    for field in [&b"ssh-ed25519"[..], &[7; 32]] {
        blob.extend_from_slice(&(field.len() as u32).to_be_bytes());
        blob.extend_from_slice(field);
    }
    let key = SshPublicKey::from_blob(&blob).unwrap();
    let fp = key.fingerprint().to_string();
    let line = key.to_openssh();
    let record = register(
        NewSshKey {
            key,
            subject: "user:alice".into(),
            grants: aspen_auth::Grants::new(["write:acme/a".parse().unwrap()]),
            description: String::new(),
            expires_at: None,
        },
        UnixMicros::now(),
    )
    .unwrap();
    h.access.auth().ssh_keys().insert(record);
    // The key is live: the SSH transport's credential identifies it.
    let ssh = Credential::SshKey {
        fingerprint: fp.clone(),
    };
    h.access
        .auth()
        .identify(&ssh, UnixMicros::now())
        .await
        .expect("the registered key identifies over SSH");

    for auth in [
        format!("Bearer {fp}"),
        basic("git", &fp),
        basic(&fp, &fp),
        format!("SshKey {fp}"),
        format!("ssh {fp}"),
        format!("PublicKey {fp}"),
        line.clone(),
        format!("Bearer {line}"),
        basic("git", &line),
    ] {
        for (method, endpoint) in ENDPOINTS {
            let (status, _, _) = send(
                &h.app,
                req(method, &format!("/acme/a/{endpoint}"), Some(&auth), vec![]),
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{auth:.30} {endpoint}");
        }
    }
    assert!(h.log.lock().unwrap().calls.is_empty());
}

/// Invariant: a rate-limit refusal happens before the handler: nothing is
/// served past the burst, and the refusal says when to retry. Callers have
/// separate buckets.
#[tokio::test]
async fn rate_limits_refuse_before_anything_is_served() {
    let limits = LimitsConfig {
        read: Some(RateLimit {
            burst: 2,
            per_second: 0.001,
        }),
        push: Some(RateLimit {
            burst: 1,
            per_second: 0.001,
        }),
        ..LimitsConfig::default()
    };
    let h = harness(json!({}), limits).await;
    let a = basic("x", &mint(&h.access, "agent:a", &["write:acme/*"]));
    let b = basic("x", &mint(&h.access, "agent:b", &["write:acme/*"]));
    let fetch = |auth: &str| {
        req(
            "POST",
            "/acme/a/git-upload-pack",
            Some(auth),
            b"0000".to_vec(),
        )
    };
    assert_eq!(send(&h.app, fetch(&a)).await.0, StatusCode::OK);
    assert_eq!(send(&h.app, fetch(&a)).await.0, StatusCode::OK);
    let (status, headers, body) = send(&h.app, fetch(&a)).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(
        headers[header::RETRY_AFTER]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= 1
    );
    assert!(body.contains("rate limit"));
    assert_eq!(send(&h.app, fetch(&b)).await.0, StatusCode::OK);
    let push = |auth: &str| {
        req(
            "POST",
            "/acme/a/git-receive-pack",
            Some(auth),
            b"PACK".to_vec(),
        )
    };
    assert_eq!(send(&h.app, push(&a)).await.0, StatusCode::OK);
    assert_eq!(
        send(&h.app, push(&a)).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );

    let log = h.log.lock().unwrap();
    assert_eq!(log.calls.len(), 4, "{:?}", log.calls);
    assert_eq!(log.stored.len(), 4);
}

/// Invariant: quota refusals happen before any pack bytes are stored. A
/// declared length over the per-push cap is 413 and the handler never runs;
/// a streamed body that crosses the cap, or the caller's byte budget,
/// fails while the handler reads it, so the handler (which stores only
/// complete bodies) stores nothing; a spent budget refuses the next push
/// before its body is read.
#[tokio::test]
async fn quota_refusals_come_before_any_pack_is_stored() {
    let limits = LimitsConfig {
        quota: PushQuota {
            max_pack_bytes: Some(1000),
            max_objects: Some(50),
            bytes_per_hour: Some(1500),
        },
        ..LimitsConfig::default()
    };
    let h = harness(json!({}), limits).await;
    let a = basic("x", &mint(&h.access, "agent:a", &["write:acme/a"]));
    let url = "/acme/a/git-receive-pack";

    // Declared too large: refused before the handler.
    let (status, _, body) = send(&h.app, req("POST", url, Some(&a), vec![7; 1001])).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert!(h.log.lock().unwrap().calls.is_empty());

    // Streamed (no length) past the cap: the read fails mid-body.
    let streamed = chunked(url, &a, 11, 1);
    assert_eq!(send(&h.app, streamed).await.0, StatusCode::OK);
    {
        let log = h.log.lock().unwrap();
        assert_eq!(log.calls.len(), 1);
        assert!(log.stored.is_empty(), "an over-cap pack was stored");
        assert_eq!(log.failed.len(), 1);
        assert!(log.failed[0].contains("push too large"), "{:?}", log.failed);
    }

    // The failed push spent agent a's budget up to the cap (1,000 of
    // 1,500 bytes). A fresh caller has its own: 900 bytes fit and are
    // stored, leaving 600.
    let fresh = basic("x", &mint(&h.access, "agent:b", &["write:acme/a"]));
    let (status, ..) = send(&h.app, req("POST", url, Some(&fresh), vec![2; 900])).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.log.lock().unwrap().stored, vec![vec![2u8; 900]]);
    // The next declared 900 bytes do not fit the 600 left: refused up front.
    let (status, headers, body) = send(&h.app, req("POST", url, Some(&fresh), vec![3; 900])).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert!(body.contains("push quota"));
    assert!(headers.contains_key(header::RETRY_AFTER));
    // A streamed body that runs out of budget part-way (after 600 of its
    // 700 bytes) fails while it is read.
    let streamed = chunked(url, &fresh, 7, 4);
    send(&h.app, streamed).await;
    let log = h.log.lock().unwrap();
    assert_eq!(log.stored.len(), 1, "only the in-quota pack was stored");
    assert!(
        log.failed.last().unwrap().contains("push quota"),
        "{:?}",
        log.failed
    );
}

/// A body of these chunks with no declared length, as Git sends a large
/// push (chunked transfer encoding).
fn chunked(url: &str, auth: &str, chunks: usize, byte: u8) -> Request<Body> {
    let frames: std::collections::VecDeque<_> = (0..chunks)
        .map(|_| bytes::Bytes::from(vec![byte; 100]))
        .collect();
    Request::builder()
        .method("POST")
        .uri(url)
        .header(header::AUTHORIZATION, auth)
        .body(Body::new(Chunks(frames)))
        .unwrap()
}

struct Chunks(std::collections::VecDeque<bytes::Bytes>);

impl http_body::Body for Chunks {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<bytes::Bytes>, Self::Error>>> {
        std::task::Poll::Ready(self.0.pop_front().map(|b| Ok(http_body::Frame::data(b))))
    }
}

/// Tokens created in the metadata database authenticate through the
/// middleware, survive a reconnect (they are durable), and stop working
/// when revoked. The database holds the digest, never the token text.
#[tokio::test]
async fn database_tokens_authenticate_and_revoke() {
    use aspen_auth::{AuthService, Limits, NewToken, PolicyConfig};
    use aspen_core::id::UnixMicros;
    use aspen_server::access::tokens::SqlTokens;
    use aspen_server::access::{Access, ClientAddr, guard};

    let dir = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("aspen.db").display()
    );
    let meta = aspen_meta::SqlStore::connect(&url).await.unwrap();
    meta.migrate().await.unwrap();
    let settings = aspen_core::repo::RepoSettings::new(
        aspen_core::name::RefName::new("refs/heads/main").unwrap(),
    )
    .unwrap();
    aspen_core::meta::MetaStore::create_repo(
        &meta,
        &aspen_core::name::RepoName::new("acme/a").unwrap(),
        &settings,
    )
    .await
    .unwrap();
    let issued = SqlTokens(meta.clone())
        .create(
            NewToken {
                subject: "agent:db".into(),
                grants: aspen_auth::Grants::new(["write:acme/a".parse().unwrap()]),
                description: "test".into(),
                expires_at: None,
            },
            UnixMicros::now(),
        )
        .await
        .unwrap();
    let text = issued.token.expose().to_owned();
    let rows = meta.tokens(10).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        !format!("{rows:?}").contains(&text[39..]),
        "secret in the row"
    );
    assert!(!rows[0].grants.contains(&text[39..]));

    // A fresh connection, as after a restart.
    let meta = aspen_meta::SqlStore::connect(&url).await.unwrap();
    let tokens = SqlTokens(meta.clone());
    let auth = AuthService::new(PolicyConfig::default(), tokens.clone()).unwrap();
    let access = Arc::new(Access::new(
        auth,
        Limits::unlimited(),
        meta,
        ClientAddr::Peer,
    ));
    let log: Log = Arc::default();
    let app = recorder(log.clone()).layer(axum::middleware::from_fn_with_state(
        access,
        guard::<aspen_meta::SqlStore, SqlTokens, _>,
    ));
    let fetch = || {
        req(
            "GET",
            "/acme/a/info/refs?service=git-upload-pack",
            Some(&basic("x", &text)),
            vec![],
        )
    };
    assert_eq!(send(&app, fetch()).await.0, StatusCode::OK);
    assert!(
        tokens
            .revoke(&issued.record.id, UnixMicros::now())
            .await
            .unwrap()
    );
    assert_eq!(send(&app, fetch()).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(log.lock().unwrap().calls.len(), 1);
}
