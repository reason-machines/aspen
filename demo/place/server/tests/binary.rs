//! The `aspen-place` binary as a platform runs it: configured from the
//! environment and stopped with SIGTERM (what ECS, Kubernetes and `docker
//! stop` send). Each test states the invariant it pins.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const RED: &str = "#d6262b";

/// A free loopback port (bound and released at once).
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn start(data: &Path, port: u16, extra: &[(&str, &str)]) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_aspen-place"));
    cmd.arg("serve")
        .env("PLACE_LISTEN", format!("127.0.0.1:{port}"))
        .env("PLACE_DATA_DIR", data)
        .env("PLACE_AGENTS", "0")
        // The merger never runs on its own during a test: only the last
        // merge at shutdown can merge anything.
        .env("PLACE_MERGE_INTERVAL_MS", "600000")
        .env("PLACE_SHUTDOWN_DRAIN_SECONDS", "2")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for (k, v) in extra {
        cmd.env(k, v);
    }
    cmd.spawn().expect("aspen-place starts")
}

/// One HTTP/1.1 GET on a fresh connection: status line and body.
fn get(port: u16, path: &str) -> std::io::Result<(String, Vec<u8>)> {
    let mut s = TcpStream::connect(("127.0.0.1", port))?;
    s.set_read_timeout(Some(Duration::from_secs(10)))?;
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: place.test\r\nConnection: close\r\n\r\n"
    )?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw)?;
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a response head");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    Ok((head, raw[split + 4..].to_vec()))
}

fn wait_ready(child: &mut Child, port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok((head, _)) = get(port, "/api/v1/config") {
            assert!(head.starts_with("HTTP/1.1 200"), "{head}");
            return;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("aspen-place exited before it was ready: {status}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("aspen-place did not answer within 60 s");
}

/// Sends SIGTERM and waits for the exit, at most `limit`.
fn terminate(mut child: Child, limit: Duration) -> (Output, Duration) {
    let began = Instant::now();
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    while child.try_wait().unwrap().is_none() {
        if began.elapsed() > limit {
            let _ = child.kill();
            panic!("aspen-place did not exit within {limit:?} of SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let elapsed = began.elapsed();
    (child.wait_with_output().unwrap(), elapsed)
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(["-c", "user.name=Test", "-c", "user.email=test@example.test"])
        .args(["-c", "protocol.version=2", "-c", "init.defaultBranch=main"])
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn sigterm_merges_accepted_pushes_and_exits_while_an_event_stream_is_open() {
    let data = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}/canvas.git");
    let mut child = start(data.path(), port, &[]);
    wait_ready(&mut child, port);

    // A page's event stream: it lasts up to its 10-minute age limit, far
    // past the drain.
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        stream,
        "GET /api/v1/events HTTP/1.1\r\nHost: place.test\r\nAccept: text/event-stream\r\n\r\n"
    )
    .unwrap();
    let mut first = [0u8; 12];
    stream.read_exact(&mut first).unwrap();
    assert_eq!(&first, b"HTTP/1.1 200");
    // Everything the stream carries until the server closes it.
    let events = std::thread::spawn(move || {
        let mut seen = Vec::new();
        // A reset after the last bytes ends the read as well as a close.
        let _ = stream.read_to_end(&mut seen);
        String::from_utf8_lossy(&seen).into_owned()
    });

    // A push is accepted onto its branch; the merger has not run.
    git(work.path(), &["clone", "-q", &url, "canvas"]);
    let canvas = work.path().join("canvas");
    std::fs::create_dir_all(canvas.join("tiles")).unwrap();
    std::fs::write(canvas.join("tiles/3_4.txt"), format!("{RED}\n")).unwrap();
    git(&canvas, &["add", "tiles"]);
    git(&canvas, &["commit", "-qm", "paint"]);
    git(
        &canvas,
        &["push", "-q", "origin", "HEAD:refs/heads/paint/term"],
    );

    // SIGTERM: the open stream holds the drain for its 2 s, then the server
    // stops, runs its last merge and exits 0, well inside ECS's 120 s.
    let (out, elapsed) = terminate(child, Duration::from_secs(60));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "exit {:?}: {stderr}", out.status);
    assert!(stderr.contains("closing them"), "{stderr}");
    assert!(stderr.contains("aspen-place: stopped"), "{stderr}");
    assert!(elapsed < Duration::from_secs(30), "took {elapsed:?}");
    // The drain closed the stream (#140): it saw the push, but the server
    // had closed it before the last merge, so it never saw that merge.
    // A connection left open would be served through the merge.
    let events = events.join().unwrap();
    assert!(events.contains("event: push"), "{events}");
    assert!(!events.contains("event: merge"), "{events}");

    // After a restart, which never merges pending pushes by itself, the
    // tile is on main: the last merge at SIGTERM put it there.
    let mut child = start(data.path(), port, &[]);
    wait_ready(&mut child, port);
    git(work.path(), &["clone", "-q", &url, "after"]);
    let tile = std::fs::read_to_string(work.path().join("after/tiles/3_4.txt"))
        .expect("the pushed tile is on main");
    assert_eq!(tile, format!("{RED}\n"));
    let (out, _) = terminate(child, Duration::from_secs(60));
    assert!(out.status.success());
}

#[test]
fn sigterm_with_no_connections_open_merges_and_exits_without_waiting_out_the_drain() {
    let data = tempfile::tempdir().unwrap();
    let work = tempfile::tempdir().unwrap();
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}/canvas.git");
    // A drain far longer than the exit below may take.
    let drain = Duration::from_secs(60);
    let drain_env = drain.as_secs().to_string();
    let mut child = start(
        data.path(),
        port,
        &[("PLACE_SHUTDOWN_DRAIN_SECONDS", &drain_env)],
    );
    wait_ready(&mut child, port);

    // A push is accepted onto its branch; the merger has not run. `git`
    // has exited, so no connection is open.
    git(work.path(), &["clone", "-q", &url, "canvas"]);
    let canvas = work.path().join("canvas");
    std::fs::create_dir_all(canvas.join("tiles")).unwrap();
    std::fs::write(canvas.join("tiles/5_6.txt"), format!("{RED}\n")).unwrap();
    git(&canvas, &["add", "tiles"]);
    git(&canvas, &["commit", "-qm", "paint"]);
    git(
        &canvas,
        &["push", "-q", "origin", "HEAD:refs/heads/paint/fast"],
    );

    // SIGTERM: with nothing in flight the server stops at once, runs its
    // last merge and exits 0, long before the drain would end.
    let (out, elapsed) = terminate(child, drain + Duration::from_secs(30));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "exit {:?}: {stderr}", out.status);
    assert!(stderr.contains("aspen-place: stopped"), "{stderr}");
    assert!(!stderr.contains("closing them"), "{stderr}");
    assert!(elapsed < Duration::from_secs(20), "took {elapsed:?}");

    // After a restart the tile is on main: the last merge at SIGTERM put
    // it there. A restart reloads branches with no pending tiles, so its
    // merger's immediate first tick has nothing to merge; with the last
    // merge removed from `Place::run`, this test fails here.
    let mut child = start(data.path(), port, &[]);
    wait_ready(&mut child, port);
    git(work.path(), &["clone", "-q", &url, "after"]);
    let tile = std::fs::read_to_string(work.path().join("after/tiles/5_6.txt"))
        .expect("the pushed tile is on main");
    assert_eq!(tile, format!("{RED}\n"));
    let (out, _) = terminate(child, Duration::from_secs(60));
    assert!(out.status.success());
}

#[test]
fn the_built_page_is_served_beside_the_api_and_git() {
    let data = tempfile::tempdir().unwrap();
    let web = tempfile::tempdir().unwrap();
    std::fs::write(
        web.path().join("index.html"),
        "<!doctype html><title>place</title>",
    )
    .unwrap();
    std::fs::create_dir(web.path().join("assets")).unwrap();
    std::fs::write(web.path().join("assets/index-0a1b2c.js"), "export {}").unwrap();
    let port = free_port();
    let dir = web.path().to_str().unwrap();
    let mut child = start(data.path(), port, &[("PLACE_WEB_DIR", dir)]);
    wait_ready(&mut child, port);

    let (head, body) = get(port, "/").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(
        head.to_ascii_lowercase()
            .contains("content-type: text/html"),
        "{head}"
    );
    assert_eq!(body, b"<!doctype html><title>place</title>");
    let (head, _) = get(port, "/assets/index-0a1b2c.js").unwrap();
    assert!(head.contains("immutable"), "{head}");
    // Git and the API still answer on the same listener.
    let (head, _) = get(port, "/canvas.git/info/refs?service=git-upload-pack").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    let (head, _) = get(port, "/nothing-here").unwrap();
    assert!(head.starts_with("HTTP/1.1 404"), "{head}");

    let (out, _) = terminate(child, Duration::from_secs(60));
    assert!(out.status.success());
}

#[test]
fn a_web_directory_without_a_built_page_refuses_the_start() {
    let data = tempfile::tempdir().unwrap();
    let web = tempfile::tempdir().unwrap();
    let port = free_port();
    let dir = web.path().to_str().unwrap();
    let child = start(data.path(), port, &[("PLACE_WEB_DIR", dir)]);
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("no index.html"));
}

/// A page file at a path the API answers would make axum panic when the
/// page's routes are merged with the API's, with a message that names no
/// file (#146). The start is refused instead, with an error naming it.
#[test]
fn a_page_file_at_an_api_path_refuses_the_start_and_names_the_file() {
    let data = tempfile::tempdir().unwrap();
    let web = tempfile::tempdir().unwrap();
    std::fs::write(web.path().join("index.html"), "<!doctype html>").unwrap();
    std::fs::create_dir_all(web.path().join("api/v1")).unwrap();
    std::fs::write(web.path().join("api/v1/config"), "{}").unwrap();
    let port = free_port();
    let dir = web.path().to_str().unwrap();
    let mut child = start(data.path(), port, &[("PLACE_WEB_DIR", dir)]);
    let deadline = Instant::now() + Duration::from_secs(60);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            child.kill().unwrap();
            panic!("aspen-place started with a page file at /api/v1/config");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
    let file = web.path().join("api/v1/config");
    assert!(
        stderr.contains(&file.display().to_string()) && stderr.contains("/api/v1"),
        "{stderr}"
    );
}

/// An empty `PLACE_TRUSTED_PROXIES`, as a template renders an unset field,
/// is unset: with hops 0 the server starts. The same variable naming a
/// network with hops 0 refuses the start, so the empty value is read and
/// not ignored.
#[test]
fn an_empty_trusted_proxy_list_is_unset() {
    let data = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut child = start(
        data.path(),
        port,
        &[
            ("PLACE_TRUSTED_PROXY_HOPS", "0"),
            ("PLACE_TRUSTED_PROXIES", ""),
        ],
    );
    wait_ready(&mut child, port);
    let (out, _) = terminate(child, Duration::from_secs(60));
    assert!(out.status.success());

    let data = tempfile::tempdir().unwrap();
    let child = start(
        data.path(),
        free_port(),
        &[
            ("PLACE_TRUSTED_PROXY_HOPS", "0"),
            ("PLACE_TRUSTED_PROXIES", "10.0.0.0/8"),
        ],
    );
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("PLACE_TRUSTED_PROXIES"));
}
