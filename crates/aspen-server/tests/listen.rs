//! The HTTP listener (`aspen_server::listen`, node D4): requests are served
//! with the peer address, over HTTP/1.1 and cleartext HTTP/2, and a client
//! that does not finish its request headers in time is disconnected,
//! which `axum::serve` never did.

use std::net::SocketAddr;
use std::time::Duration;

use aspen_server::listen::{HttpLimits, serve};
use axum::Router;
use axum::extract::ConnectInfo;
use axum::routing::get;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn start(limits: HttpLimits) -> SocketAddr {
    let app = Router::new().route(
        "/peer",
        get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move { peer.ip().to_string() }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, app, limits, std::future::pending()));
    addr
}

/// Reads until the server closes the connection or `within` passes;
/// `None` if it stayed open.
async fn read_to_close(s: &mut tokio::net::TcpStream, within: Duration) -> Option<Vec<u8>> {
    let mut all = Vec::new();
    let mut buf = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + within;
    loop {
        match tokio::time::timeout_at(deadline, s.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => return Some(all),
            Ok(Ok(n)) => all.extend_from_slice(&buf[..n]),
            Err(_) => return None,
        }
    }
}

/// Invariant: a client that starts a request and never finishes its
/// headers is disconnected after `header_read_timeout`, however slowly it
/// keeps sending; a complete request is answered, with the peer address.
#[tokio::test]
async fn slow_headers_time_out_and_requests_see_their_peer() {
    let addr = start(HttpLimits {
        header_read_timeout: Duration::from_millis(500),
        ..HttpLimits::default()
    })
    .await;

    let mut ok = tokio::net::TcpStream::connect(addr).await.unwrap();
    ok.write_all(b"GET /peer HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let reply = read_to_close(&mut ok, Duration::from_secs(5))
        .await
        .unwrap();
    let reply = String::from_utf8_lossy(&reply);
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    assert!(reply.ends_with("127.0.0.1"), "{reply}");

    let mut slow = tokio::net::TcpStream::connect(addr).await.unwrap();
    slow.write_all(b"GET /peer HTTP/1.1\r\n").await.unwrap();
    // One header byte every 200 ms: never idle for the timeout, but the
    // headers are never done.
    let dribble = tokio::spawn(async move {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            if slow.write_all(b"x").await.is_err() {
                return true;
            }
        }
        // Still open after 4 s: not cut off.
        read_to_close(&mut slow, Duration::from_millis(1))
            .await
            .is_some()
    });
    assert!(dribble.await.unwrap(), "slow headers were not cut off");
}

/// Invariant: cleartext HTTP/2 with prior knowledge is served on the same
/// port (Cloud Run sends pushes over h2c, decisions log C6).
#[tokio::test]
async fn cleartext_http2_is_served() {
    let addr = start(HttpLimits::default()).await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    // An empty SETTINGS frame.
    s.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await.unwrap();
    let mut head = [0u8; 9];
    tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut head))
        .await
        .unwrap()
        .unwrap();
    // The server's first frame is its SETTINGS frame (type 4), and it caps
    // concurrent streams (SETTINGS_MAX_CONCURRENT_STREAMS, id 3) at the
    // default limit.
    assert_eq!(head[3], 4, "{head:?}");
    let len = usize::from(head[0]) << 16 | usize::from(head[1]) << 8 | usize::from(head[2]);
    let mut payload = vec![0u8; len];
    tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    let settings: Vec<(u16, u32)> = payload
        .as_chunks::<6>()
        .0
        .iter()
        .map(|&[a, b, c, d, e, f]| (u16::from_be_bytes([a, b]), u32::from_be_bytes([c, d, e, f])))
        .collect();
    let max_streams = settings.iter().find(|(id, _)| *id == 3).map(|e| e.1);
    assert_eq!(max_streams, Some(100), "{settings:?}");
    assert_eq!(HttpLimits::default().h2_max_concurrent_streams, 100);
}

/// Invariant: shutdown is graceful. A request in flight when shutdown
/// starts is answered in full, no new connection is accepted, and `serve`
/// returns once the request is done.
#[tokio::test]
async fn shutdown_lets_requests_in_flight_finish_then_returns() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let started_tx = std::sync::Arc::new(std::sync::Mutex::new(Some(started_tx)));
    let app = Router::new().route(
        "/slow",
        get(move || {
            let started_tx = started_tx.clone();
            async move {
                if let Some(tx) = started_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                "done"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, app, HttpLimits::default(), async move {
        let _ = stop_rx.await;
    }));
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    c.write_all(b"GET /slow HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    started_rx.await.unwrap();
    stop_tx.send(()).unwrap();
    // The handler sleeps 500 ms: `serve` waits for it rather than return
    // (and let the process exit) while the request is in flight.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !server.is_finished(),
        "serve returned with a request in flight"
    );
    let reply = read_to_close(&mut c, Duration::from_secs(5))
        .await
        .expect("the connection stayed open after shutdown");
    let reply = String::from_utf8_lossy(&reply);
    assert!(reply.starts_with("HTTP/1.1 200"), "{reply}");
    assert!(reply.ends_with("done"), "{reply}");
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("serve did not return after shutdown")
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

/// Invariant: a Git request body that stops arriving fails after the
/// server's `body_idle_timeout` and the request is answered, instead of
/// holding the request (and for a push a blocking thread) for as long as
/// the client keeps the connection open.
#[test]
fn a_stalled_git_request_body_is_cut_off() {
    use std::io::{Read, Write};
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.git");
    let init = std::process::Command::new("git")
        .args(["init", "-q", "--bare"])
        .arg(&src)
        .output()
        .unwrap();
    assert!(init.status.success());
    let s = aspen_server::testing::TestServer::start(|c| {
        c.body_idle_timeout = Duration::from_millis(500);
    });
    let _seed = s.seeder("demo", "refs/heads/main", &src);
    let addr = s.base_url().trim_start_matches("http://").to_owned();
    let mut c = std::net::TcpStream::connect(&addr).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    c.write_all(
        b"POST /demo.git/git-upload-pack HTTP/1.1\r\nhost: x\r\n\
          content-type: application/x-git-upload-pack-request\r\n\
          git-protocol: version=2\r\ncontent-length: 1000\r\n\r\n0014command=ls-refs\n",
    )
    .unwrap();
    let started = std::time::Instant::now();
    let mut reply = Vec::new();
    let mut buf = [0u8; 4096];
    // The answer is a Git error (an `ERR` line, which Git clients show as
    // `remote error`), then the response ends.
    while !String::from_utf8_lossy(&reply).contains("idle") {
        let n = c
            .read(&mut buf)
            .expect("no answer within 10 s: the body never timed out");
        assert!(
            n > 0,
            "closed without an answer: {}",
            String::from_utf8_lossy(&reply)
        );
        reply.extend_from_slice(&buf[..n]);
    }
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(400) && waited < Duration::from_secs(8),
        "{waited:?}"
    );
}

/// Invariant (D4 review): the same holds for a push. A pack that stops
/// arriving part-way through fails after `body_idle_timeout`, the push is
/// answered rather than holding its spool thread, and no ref moves.
#[test]
fn a_stalled_push_body_is_cut_off() {
    use std::io::{Read, Write};
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.git");
    let init = std::process::Command::new("git")
        .args(["init", "-q", "--bare"])
        .arg(&src)
        .output()
        .unwrap();
    assert!(init.status.success());
    let policy = aspen_auth::PolicyConfig {
        anonymous: vec!["write:*".parse().unwrap()],
        ..aspen_auth::PolicyConfig::default()
    };
    let s = aspen_server::testing::TestServer::start_with_policy(policy, |c| {
        c.body_idle_timeout = Duration::from_millis(500);
    });
    let _seed = s.seeder("demo", "refs/heads/main", &src);
    let addr = s.base_url().trim_start_matches("http://").to_owned();
    let mut c = std::net::TcpStream::connect(&addr).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let zero = "0".repeat(40);
    let new = "1".repeat(40);
    let command = format!("{zero} {new} refs/heads/stalled\0report-status\n");
    let mut body = format!("{:04x}{command}0000", command.len() + 4).into_bytes();
    // A pack header for one object, and then nothing more.
    body.extend_from_slice(b"PACK\0\0\0\x02\0\0\0\x01");
    c.write_all(
        format!(
            "POST /demo.git/git-receive-pack HTTP/1.1\r\nhost: x\r\n\
             content-type: application/x-git-receive-pack-request\r\n\
             content-length: {}\r\n\r\n",
            body.len() + 1000
        )
        .as_bytes(),
    )
    .unwrap();
    c.write_all(&body).unwrap();
    let started = std::time::Instant::now();
    let mut reply = Vec::new();
    let mut buf = [0u8; 4096];
    while !String::from_utf8_lossy(&reply).contains("idle") {
        let n = c
            .read(&mut buf)
            .expect("no answer within 10 s: the push body never timed out");
        assert!(
            n > 0,
            "closed without an answer: {}",
            String::from_utf8_lossy(&reply)
        );
        reply.extend_from_slice(&buf[..n]);
    }
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(400) && waited < Duration::from_secs(8),
        "{waited:?}"
    );
    let refs = std::process::Command::new("git")
        .args(["ls-remote", &format!("{}/demo.git", s.base_url())])
        .output()
        .unwrap();
    assert!(refs.status.success());
    let refs = String::from_utf8_lossy(&refs.stdout);
    assert!(!refs.contains("refs/heads/stalled"), "{refs}");
}
