//! Black-box tests of the `aspen` binary: `aspen serve` and `aspen dev`
//! driven by stock `git` over HTTP, the operator commands, the health
//! check and shutdown.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use aspen_testkit::NO_AUTO_MAINTENANCE;

fn aspen() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_aspen"));
    // Nothing from the developer's environment leaks into a test server.
    for (k, _) in std::env::vars() {
        if k.starts_with("ASPEN_") || k == "DATABASE_URL" || k == "PGPASSWORD" {
            c.env_remove(k);
        }
    }
    c
}

fn git(dir: &Path, args: &[&str]) -> Output {
    Command::new("git")
        // No background repack while `fsck` reads the pack directory (#207).
        .args(NO_AUTO_MAINTENANCE)
        .args(args)
        .current_dir(dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_AUTHOR_NAME", "Test")
        .env("GIT_AUTHOR_EMAIL", "test@example.com")
        .env("GIT_COMMITTER_NAME", "Test")
        .env("GIT_COMMITTER_EMAIL", "test@example.com")
        .output()
        .expect("git runs")
}

#[track_caller]
fn git_ok(dir: &Path, args: &[&str]) -> String {
    let out = git(dir, args);
    assert!(
        out.status.success(),
        "git {args:?} failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

/// A local repository with `n` commits on `main`.
fn work_repo(dir: &Path, n: usize) -> PathBuf {
    let repo = dir.join("work");
    std::fs::create_dir_all(&repo).unwrap();
    git_ok(&repo, &["init", "-q", "-b", "main"]);
    for i in 0..n {
        std::fs::write(repo.join(format!("file{i}.txt")), format!("content {i}\n")).unwrap();
        git_ok(&repo, &["add", "."]);
        git_ok(&repo, &["commit", "-q", "-m", &format!("commit {i}")]);
    }
    repo
}

/// A running `aspen` server process; killed on drop.
struct Running {
    child: Child,
    addr: String,
    log: mpsc::Receiver<String>,
}

impl Running {
    /// Starts `cmd` and waits for its "listening on http://..." line.
    fn start(mut cmd: Command) -> Self {
        let mut child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("aspen starts");
        let stderr = child.stderr.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                eprintln!("[aspen] {line}");
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(left) {
                Ok(line) => {
                    if let Some(rest) = line.split("listening on http://").nth(1) {
                        return Self {
                            child,
                            addr: rest.trim().to_owned(),
                            log: rx,
                        };
                    }
                }
                Err(_) => {
                    let _ = child.kill();
                    panic!("aspen did not start listening: {:?}", child.wait());
                }
            }
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// Sends SIGTERM and waits for the process to exit.
    fn terminate(mut self) -> std::process::ExitStatus {
        let pid = self.child.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(Instant::now() < deadline, "aspen did not exit on SIGTERM");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// What follows `marker` on the first log line within 10 s that has it,
    /// up to the next space or `/`.
    fn logged_after(&self, marker: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        while let Ok(line) = self
            .log
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            if let Some(rest) = line.split(marker).nth(1) {
                return rest
                    .split(|c: char| c == '/' || c.is_whitespace())
                    .next()
                    .unwrap()
                    .to_owned();
            }
        }
        panic!("no log line with {marker:?}");
    }

    /// Whether the server logs a line containing `text` within 10 s.
    fn logs(&self, text: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(10);
        while let Ok(line) = self
            .log
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        {
            if line.contains(text) {
                return true;
            }
        }
        false
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Runs `cmd` to completion, but kills it after 30 s: a command expected to
/// refuse its configuration must not hang the suite by serving instead.
fn output_within(cmd: &mut Command) -> Output {
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("aspen starts");
    let deadline = Instant::now() + Duration::from_secs(30);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("aspen kept running instead of refusing to start");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    child.wait_with_output().unwrap()
}

/// `GET path` on `addr`; returns the status code and body.
fn http_get(addr: &str, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut text = String::new();
    s.read_to_string(&mut text).unwrap();
    let status = text.split(' ').nth(1).unwrap().parse().unwrap();
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

fn metric(addr: &str, name: &str) -> u64 {
    let (status, body) = http_get(addr, "/-/metrics");
    assert_eq!(status, 200);
    body.lines()
        .find_map(|l| l.strip_prefix(name).and_then(|r| r.trim().parse().ok()))
        .unwrap_or_else(|| panic!("no metric {name} in:\n{body}"))
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Environment for `aspen serve` over SQLite and a local blob store in `dir`.
fn serve_env(cmd: &mut Command, dir: &Path) {
    cmd.env(
        "DATABASE_URL",
        format!("sqlite://{}?mode=rwc", dir.join("meta.db").display()),
    )
    .env(
        "ASPEN_BLOB_URL",
        format!("file://{}", dir.join("blobs").display()),
    )
    .env("ASPEN_SPOOL_DIR", dir.join("spool"))
    .env("ASPEN_LISTEN", "127.0.0.1:0")
    .env("ASPEN_METRICS_LISTEN", "127.0.0.1:0");
}

fn admin(dir: &Path, args: &[&str]) -> Output {
    let mut c = aspen();
    serve_env(&mut c, dir);
    c.args(args).output().unwrap()
}

#[track_caller]
fn admin_ok(dir: &Path, args: &[&str]) -> String {
    let out = admin(dir, args);
    assert!(
        out.status.success(),
        "aspen {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

/// `http://<user>:<token>@<addr>/<repo>.git`.
fn authed(server: &Running, token: &str, repo: &str) -> String {
    format!("http://agent:{token}@{}/{repo}.git", server.addr)
}

#[test]
fn version_reports_workspace_version() {
    let out = aspen().arg("--version").output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert_eq!(
        stdout.trim(),
        format!("aspen {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn dev_rejects_an_invalid_listen_address() {
    let out = aspen()
        .args(["dev", "--listen", "not-an-address"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "clap usage errors exit 2");
}

#[test]
fn dev_refuses_to_listen_beyond_loopback() {
    let dir = tempfile::tempdir().unwrap();
    let out = output_within(
        aspen()
            .args(["dev", "--listen", "0.0.0.0:0", "--data-dir"])
            .arg(dir.path()),
    );
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("not a loopback address"), "{stderr}");
}

/// `aspen dev`: anyone on loopback pushes and clones; packs go out as
/// signed `/-/blobs` URIs to a client that asks; the data survives a
/// restart, under a new signing key.
#[test]
fn dev_serves_push_and_clone_and_keeps_data_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let start = || {
        let mut c = aspen();
        c.args([
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--repo",
            "demo",
            "--data-dir",
        ])
        .arg(&data);
        Running::start(c)
    };
    let work = work_repo(dir.path(), 3);
    let tip = git_ok(&work, &["rev-parse", "HEAD"]);

    let server = start();
    git_ok(&work, &["push", "-q", &server.url("/demo.git"), "main"]);
    let clone = dir.path().join("clone");
    git_ok(
        dir.path(),
        &[
            "-c",
            "fetch.uriprotocols=http",
            "clone",
            "-q",
            &server.url("/demo.git"),
            clone.to_str().unwrap(),
        ],
    );
    assert_eq!(git_ok(&clone, &["rev-parse", "HEAD"]), tip);
    git_ok(&clone, &["fsck", "--strict"]);
    assert!(
        metric(&server.addr, "aspen_fetch_packs_uri_total") >= 1,
        "the clone should get its pack as a signed /-/blobs URI"
    );
    // Cleartext HTTP/2 (prior knowledge), as Cloud Run's h2c port sends it.
    let h2 = Command::new("curl")
        .args([
            "-sS",
            "--http2-prior-knowledge",
            "-o",
            "/dev/null",
            "-w",
            "%{http_version} %{http_code}",
        ])
        .arg(server.url("/demo.git/info/refs?service=git-upload-pack"))
        .output()
        .expect("curl runs");
    assert_eq!(
        String::from_utf8_lossy(&h2.stdout),
        "2 200",
        "h2c is served"
    );
    assert_eq!(server.terminate().code(), Some(0));

    let server = start();
    let again = dir.path().join("again");
    git_ok(
        dir.path(),
        &[
            "-c",
            "fetch.uriprotocols=http",
            "clone",
            "-q",
            &server.url("/demo.git"),
            again.to_str().unwrap(),
        ],
    );
    assert_eq!(git_ok(&again, &["rev-parse", "HEAD"]), tip);
}

/// `aspen dev` (and so `aspen serve`) runs the background worker (C1): 16
/// single-commit pushes to `main` end a level-1 cover at the default
/// fanout, the push enqueues a `compact` job, and the worker drains the
/// queue and stores the cover's pack (a 17th pack) without any command; a
/// clone afterwards is complete.
#[test]
fn dev_runs_the_compaction_worker() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let mut c = aspen();
    c.args([
        "dev",
        "--listen",
        "127.0.0.1:0",
        "--repo",
        "demo",
        "--data-dir",
    ])
    .arg(&data);
    let server = Running::start(c);
    let work = work_repo(dir.path(), 0);
    for i in 0..16 {
        std::fs::write(work.join("n.txt"), format!("{i}\n")).unwrap();
        git_ok(&work, &["add", "n.txt"]);
        git_ok(&work, &["commit", "-q", "-m", &format!("c{i}")]);
        git_ok(&work, &["push", "-q", &server.url("/demo.git"), "main"]);
    }
    let tip = git_ok(&work, &["rev-parse", "HEAD"]);

    let packs = || {
        std::fs::read_dir(data.join("blobs/repos/1/packs"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".pack"))
            .count()
    };
    let jobs = || {
        let out = aspen()
            .env(
                "DATABASE_URL",
                format!("sqlite://{}?mode=rwc", data.join("meta.db").display()),
            )
            .args(["doctor", "jobs"])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while packs() < 17 || !jobs().contains("No jobs are queued, running or dead") {
        assert!(
            Instant::now() < deadline,
            "the worker did not compact: {} packs, jobs:\n{}",
            packs(),
            jobs()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(packs(), 17, "16 push packs and one cover pack");

    let clone = dir.path().join("clone");
    git_ok(
        dir.path(),
        &[
            "clone",
            "-q",
            &server.url("/demo.git"),
            clone.to_str().unwrap(),
        ],
    );
    assert_eq!(git_ok(&clone, &["rev-parse", "HEAD"]), tip);
    git_ok(&clone, &["fsck", "--strict"]);
    assert_eq!(server.terminate().code(), Some(0));
}

/// `aspen dev --no-background-worker` (for benchmarks, D2): the same 16
/// pushes enqueue the `compact` job, but nothing runs it, so no cover pack
/// is stored and the job stays queued; fetches still work.
#[test]
fn dev_without_the_background_worker_leaves_jobs_queued() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let mut c = aspen();
    c.args([
        "dev",
        "--listen",
        "127.0.0.1:0",
        "--repo",
        "demo",
        "--no-background-worker",
        "--data-dir",
    ])
    .arg(&data);
    let server = Running::start(c);
    assert!(server.logs("background worker off"));
    let work = work_repo(dir.path(), 0);
    for i in 0..16 {
        std::fs::write(work.join("n.txt"), format!("{i}\n")).unwrap();
        git_ok(&work, &["add", "n.txt"]);
        git_ok(&work, &["commit", "-q", "-m", &format!("c{i}")]);
        git_ok(&work, &["push", "-q", &server.url("/demo.git"), "main"]);
    }
    let tip = git_ok(&work, &["rev-parse", "HEAD"]);
    // The worker, when on, compacts these 16 pushes in well under a second
    // (dev_runs_the_compaction_worker); give it ample time to show it is off.
    std::thread::sleep(Duration::from_secs(3));

    let packs = std::fs::read_dir(data.join("blobs/repos/1/packs"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().ends_with(".pack"))
        .count();
    assert_eq!(packs, 16, "one pack per push and no cover pack");
    let out = aspen()
        .env(
            "DATABASE_URL",
            format!("sqlite://{}?mode=rwc", data.join("meta.db").display()),
        )
        .args(["doctor", "jobs"])
        .output()
        .unwrap();
    let jobs = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        jobs.contains("compact") && !jobs.contains("No jobs are queued"),
        "the compact job should stay queued:\n{jobs}"
    );

    let clone = dir.path().join("clone");
    git_ok(
        dir.path(),
        &[
            "clone",
            "-q",
            &server.url("/demo.git"),
            clone.to_str().unwrap(),
        ],
    );
    assert_eq!(git_ok(&clone, &["rev-parse", "HEAD"]), tip);
    assert_eq!(server.terminate().code(), Some(0));
}

/// `aspen serve`: repositories and tokens come from the operator commands,
/// pushes need a write token, a revoked token stops working, the health
/// endpoints answer, and SIGTERM stops the server cleanly.
#[test]
fn serve_enforces_tokens_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    admin_ok(d, &["repo", "create", "acme/app"]);
    let dup = admin(d, &["repo", "create", "acme/app"]);
    assert!(!dup.status.success(), "a taken name is refused");
    let writer = admin_ok(
        d,
        &[
            "token",
            "create",
            "--subject",
            "agent:writer",
            "--grant",
            "write:acme/app",
        ],
    );
    let reader = admin_ok(
        d,
        &[
            "token",
            "create",
            "--subject",
            "agent:reader",
            "--grant",
            "read:acme/*",
        ],
    );
    assert!(writer.starts_with("aspen_") && reader.starts_with("aspen_"));
    let list = admin_ok(d, &["token", "list"]);
    assert!(
        list.contains("agent:writer") && list.contains("write:acme/app"),
        "{list}"
    );
    assert!(
        !list.contains(writer.rsplit('_').next().unwrap()),
        "token list must never show a secret"
    );

    let mut cmd = aspen();
    serve_env(&mut cmd, d);
    cmd.arg("serve");
    let server = Running::start(cmd);
    let check = |path: &str| {
        aspen()
            .args(["healthcheck", "--url", &server.url(path)])
            .status()
            .unwrap()
            .success()
    };
    assert!(check("/healthz") && check("/readyz"));
    assert_eq!(http_get(&server.addr, "/healthz"), (200, "ok\n".into()));
    // Metrics are on their own private listener, never the Git one (C5).
    let metrics_addr = server.logged_after("metrics on http://");
    assert_ne!(metrics_addr, server.addr);
    assert_eq!(
        http_get(&server.addr, "/-/metrics").0,
        404,
        "not on the Git listener"
    );
    assert!(
        http_get(&metrics_addr, "/-/metrics")
            .1
            .contains("aspen_jobs"),
        "store gauges are exported"
    );

    let work = work_repo(d, 2);
    let tip = git_ok(&work, &["rev-parse", "HEAD"]);
    let anon = git(&work, &["push", &server.url("/acme/app.git"), "main"]);
    assert!(!anon.status.success(), "anonymous push refused");
    let ro = git(
        &work,
        &["push", &authed(&server, &reader, "acme/app"), "main"],
    );
    assert!(!ro.status.success(), "read token cannot push");
    assert!(
        String::from_utf8_lossy(&ro.stderr).contains("403"),
        "{}",
        String::from_utf8_lossy(&ro.stderr)
    );
    git_ok(
        &work,
        &["push", "-q", &authed(&server, &writer, "acme/app"), "main"],
    );

    let anon_clone = git(d, &["clone", "-q", &server.url("/acme/app.git"), "anon"]);
    assert!(
        !anon_clone.status.success(),
        "no anonymous reads by default"
    );
    git_ok(
        d,
        &[
            "clone",
            "-q",
            &authed(&server, &reader, "acme/app"),
            "clone",
        ],
    );
    assert_eq!(git_ok(&d.join("clone"), &["rev-parse", "HEAD"]), tip);

    let id = writer.split('_').nth(1).unwrap();
    admin_ok(d, &["token", "revoke", id]);
    std::fs::write(work.join("late.txt"), "late\n").unwrap();
    git_ok(&work, &["add", "."]);
    git_ok(&work, &["commit", "-q", "-m", "late"]);
    let revoked = git(
        &work,
        &["push", &authed(&server, &writer, "acme/app"), "main"],
    );
    assert!(!revoked.status.success(), "a revoked token stops working");

    let addr = server.addr.clone();
    assert_eq!(
        server.terminate().code(),
        Some(0),
        "SIGTERM is a clean exit"
    );
    let after = aspen()
        .args([
            "healthcheck",
            "--timeout-secs",
            "2",
            "--url",
            &format!("http://{addr}/healthz"),
        ])
        .output()
        .unwrap();
    assert_eq!(
        after.status.code(),
        Some(1),
        "healthcheck fails once the server is gone"
    );
}

/// The server refuses to start rather than run with a misconfiguration,
/// and never echoes the secret it refused.
#[test]
fn serve_refuses_a_password_in_the_database_url() {
    let dir = tempfile::tempdir().unwrap();
    let out = output_within(
        aspen()
            .arg("serve")
            .env(
                "DATABASE_URL",
                "postgres://aspen:s3cret-value@127.0.0.1:1/aspen",
            )
            .env("ASPEN_BLOB_URL", format!("file://{}", dir.path().display())),
    );
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("PGPASSWORD"), "{stderr}");
    assert!(!stderr.contains("s3cret"), "secret echoed: {stderr}");

    let missing = aspen().arg("serve").output().unwrap();
    assert!(
        String::from_utf8(missing.stderr)
            .unwrap()
            .contains("DATABASE_URL is required")
    );
}

/// Issue #125: a secret file other users can read stops `aspen serve`
/// before it serves (naming the variable and the mode, never the secret),
/// and a server without request rate limits says so at startup.
#[cfg(unix)]
#[test]
fn serve_refuses_world_readable_secret_files_and_warns_without_rate_limits() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let out = Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-C", "host", "-f"])
        .arg(d.join("host_key"))
        .output()
        .expect("ssh-keygen runs");
    assert!(out.status.success());
    let key_text = std::fs::read_to_string(d.join("host_key")).unwrap();
    let body = key_text.lines().nth(1).unwrap().to_owned();
    std::fs::write(d.join("token"), "mirror-s3cret\n").unwrap();
    let mode = |name: &str, m: u32| {
        std::fs::set_permissions(d.join(name), std::fs::Permissions::from_mode(m)).unwrap()
    };
    let ssh_serve = || {
        let mut cmd = aspen();
        serve_env(&mut cmd, d);
        cmd.arg("serve")
            .env("ASPEN_SSH_LISTEN", format!("127.0.0.1:{}", free_port()))
            .env("ASPEN_SSH_HOST_KEY_FILE", d.join("host_key"));
        cmd
    };

    mode("host_key", 0o644);
    let out = output_within(&mut ssh_serve());
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("ASPEN_SSH_HOST_KEY_FILE")
            && stderr.contains("accessible to every user (mode 0644)"),
        "{stderr}"
    );
    assert!(!stderr.contains(&body), "host key echoed: {stderr}");

    mode("token", 0o604);
    let mut cmd = aspen();
    serve_env(&mut cmd, d);
    cmd.arg("serve")
        .env("ASPEN_MIRROR_CREDENTIALS", "gh")
        .env("ASPEN_MIRROR_GH_TOKEN_FILE", d.join("token"));
    let out = output_within(&mut cmd);
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert_eq!(out.status.code(), Some(1), "{stderr}");
    assert!(
        stderr.contains("ASPEN_MIRROR_CREDENTIALS") && stderr.contains("mode 0604"),
        "{stderr}"
    );
    assert!(!stderr.contains("mirror-s3cret"), "token echoed: {stderr}");

    // Owner-only (or group-readable for aspen's own group) is accepted.
    mode("host_key", 0o600);
    let server = Running::start(ssh_serve());
    assert!(
        server.logs("no read or push rate limit is set"),
        "startup warns that request rates are unlimited"
    );
}

/// With `ASPEN_EXPRESS_URLS` naming the server's own `/-/express`, the
/// server signs express URIs and serves them itself (C2 obligation), and
/// stock git clones through them.
#[test]
fn serve_mounts_its_own_express_route() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    admin_ok(d, &["repo", "create", "acme/app"]);
    let token = admin_ok(
        d,
        &[
            "token",
            "create",
            "--subject",
            "agent:w",
            "--grant",
            "write:acme/*",
        ],
    );
    let port = free_port();
    let public = format!("http://127.0.0.1:{port}");
    std::fs::write(d.join("policy.json"), r#"{"anonymous": ["read:acme/*"]}"#).unwrap();
    let mut cmd = aspen();
    serve_env(&mut cmd, d);
    cmd.arg("serve")
        .env("ASPEN_LISTEN", format!("127.0.0.1:{port}"))
        .env("ASPEN_PUBLIC_URL", &public)
        .env("ASPEN_POLICY_FILE", d.join("policy.json"))
        .env("ASPEN_EXPRESS_URLS", format!("{public}/-/express"))
        .env(
            "ASPEN_EXPRESS_KEYS",
            "k1:00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
        );
    let server = Running::start(cmd);
    assert!(
        server.logs("express route mounted at /-/express"),
        "startup names the express route"
    );
    let metrics_addr = server.logged_after("metrics on http://");

    let work = work_repo(d, 2);
    let tip = git_ok(&work, &["rev-parse", "HEAD"]);
    git_ok(
        &work,
        &["push", "-q", &authed(&server, &token, "acme/app"), "main"],
    );
    git_ok(
        d,
        &[
            "-c",
            "fetch.uriprotocols=http",
            "clone",
            "-q",
            &server.url("/acme/app.git"),
            "clone",
        ],
    );
    assert_eq!(git_ok(&d.join("clone"), &["rev-parse", "HEAD"]), tip);
    assert!(metric(&metrics_addr, "aspen_express_uris_total") >= 1);
    assert!(metric(&metrics_addr, "aspen_express_bytes_served_total") >= 1);
}

/// Git over SSH (C3 obligation): `aspen key add` registers a key, stock
/// `ssh` pushes and clones with it through `aspen serve`, and a revoked key
/// is refused. The host key comes from configuration, never generated.
#[test]
fn serve_runs_git_over_ssh_with_registered_keys() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let keygen = |name: &str| {
        let out = Command::new("ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-C", name, "-f"])
            .arg(d.join(name))
            .output()
            .expect("ssh-keygen runs");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    keygen("host_key");
    keygen("user_key");
    admin_ok(d, &["repo", "create", "acme/app"]);
    let user_pub = d.join("user_key.pub");
    let fingerprint = admin_ok(
        d,
        &[
            "key",
            "add",
            user_pub.to_str().unwrap(),
            "--subject",
            "user:ci",
            "--grant",
            "write:acme/app",
        ],
    );
    assert!(fingerprint.starts_with("SHA256:"), "{fingerprint}");
    assert!(admin_ok(d, &["key", "list"]).contains(&fingerprint));

    let port = free_port();
    let mut cmd = aspen();
    serve_env(&mut cmd, d);
    cmd.arg("serve")
        .env("ASPEN_SSH_LISTEN", format!("127.0.0.1:{port}"))
        .env("ASPEN_SSH_HOST_KEY_FILE", d.join("host_key"));
    let server = Running::start(cmd);
    assert!(
        server.logs("Git over SSH on 127.0.0.1:"),
        "startup names the SSH listener"
    );

    let ssh = format!(
        "ssh -i {} -o IdentitiesOnly=yes -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile={}",
        d.join("user_key").display(),
        d.join("known_hosts").display()
    );
    let url = format!("ssh://git@127.0.0.1:{port}/acme/app.git");
    let git_ssh = |dir: &Path, args: &[&str]| {
        Command::new("git")
            .args(NO_AUTO_MAINTENANCE)
            .args(args)
            .current_dir(dir)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_SSH_COMMAND", &ssh)
            .env("GIT_AUTHOR_NAME", "Test")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
            .output()
            .unwrap()
    };
    let work = work_repo(d, 2);
    let tip = git_ok(&work, &["rev-parse", "HEAD"]);
    let push = git_ssh(&work, &["push", "-q", &url, "main"]);
    assert!(
        push.status.success(),
        "{}",
        String::from_utf8_lossy(&push.stderr)
    );
    let clone = git_ssh(d, &["clone", "-q", &url, "clone"]);
    assert!(
        clone.status.success(),
        "{}",
        String::from_utf8_lossy(&clone.stderr)
    );
    assert_eq!(git_ok(&d.join("clone"), &["rev-parse", "HEAD"]), tip);

    admin_ok(d, &["key", "revoke", &fingerprint]);
    let refused = git_ssh(d, &["ls-remote", &url]);
    assert!(!refused.status.success(), "a revoked key is refused");
}

/// Issue #125: `aspen import` (and `aspen mirror`, through the same setup)
/// refuses a mirror credential file other users may read before it reaches
/// any upstream, and never echoes the token.
#[cfg(unix)]
#[test]
fn import_refuses_a_world_readable_credential_file() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let token = d.join("token");
    std::fs::write(&token, "import-s3cret\n").unwrap();
    std::fs::set_permissions(&token, std::fs::Permissions::from_mode(0o644)).unwrap();
    let out = output_within(
        aspen()
            .env(
                "DATABASE_URL",
                format!("sqlite://{}?mode=rwc", d.join("m.db").display()),
            )
            .env(
                "ASPEN_BLOB_URL",
                format!("file://{}", d.join("blobs").display()),
            )
            .env("ASPEN_WORK_DIR", d.join("spool"))
            .env("ASPEN_MIRROR_CREDENTIALS", "up")
            .env("ASPEN_MIRROR_UP_TOKEN_FILE", &token)
            // Unreachable on purpose: the refusal must come first.
            .args([
                "import",
                "http://127.0.0.1:9/acme.git",
                "--name",
                "acme",
                "--credential",
                "up",
            ]),
    );
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(!out.status.success(), "{stderr}");
    assert!(
        stderr.contains("TOKEN_FILE") && stderr.contains("accessible to every user (mode 0644)"),
        "{stderr}"
    );
    assert!(!stderr.contains("import-s3cret"), "token echoed: {stderr}");
}

/// `aspen import` and `aspen mirror` against a stock `git http-backend`
/// upstream that requires a token, with SQLite and a local blob store.
#[test]
fn import_and_mirror_commands_work_against_a_stock_upstream() {
    use aspen_testkit::git::{init_bare, isolated_git, run};
    use aspen_testkit::http_backend::GitHttpBackend;

    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let up = dir.path().join("up");
    std::fs::create_dir_all(&up).unwrap();
    let bare = init_bare(&up, "acme.git").unwrap();
    let work = dir.path().join("work");
    let git = |args: &[&str]| run(isolated_git(&home).current_dir(&work).args(args)).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    git(&["init", "-q", "--initial-branch=main"]);
    std::fs::write(work.join("a.txt"), "one\n").unwrap();
    git(&["add", "a.txt"]);
    git(&["commit", "-q", "-m", "one"]);
    git(&["tag", "-a", "v1", "-m", "v1"]);
    git(&["push", "-q", bare.to_str().unwrap(), "main", "v1"]);
    // "Basic " + base64("git:tok-123").
    let server = GitHttpBackend::start_with_auth(&up, "Basic Z2l0OnRvay0xMjM=").unwrap();
    let url = format!("{}/acme.git", server.base_url());

    let aspen_with = |token: &str, args: &[&str]| {
        let out = aspen()
            .env(
                "DATABASE_URL",
                format!("sqlite://{}?mode=rwc", dir.path().join("m.db").display()),
            )
            .env(
                "ASPEN_BLOB_URL",
                format!("file://{}", dir.path().join("blobs").display()),
            )
            .env("ASPEN_WORK_DIR", dir.path().join("spool"))
            .env("ASPEN_MIRROR_CREDENTIALS", "up")
            .env("ASPEN_MIRROR_UP_TOKEN", token)
            .args(args)
            .output()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out.status.success(), text)
    };
    let aspen_ok = |args: &[&str]| {
        let (ok, text) = aspen_with("tok-123", args);
        assert!(ok, "aspen {args:?}:\n{text}");
        text
    };

    let (ok, text) = aspen_with(
        "wrong",
        &["import", &url, "--name", "acme", "--credential", "up"],
    );
    assert!(!ok && text.contains("refused the credential"), "{text}");

    let text = aspen_ok(&["import", &url, "--name", "acme", "--credential", "up"]);
    assert!(text.contains("trunk refs/heads/main"), "{text}");
    assert!(text.contains("0 in sync, 2 imported"), "{text}");
    assert!(text.contains("mirror mode: proxy push-through"), "{text}");
    let text = aspen_ok(&["mirror", "status"]);
    assert!(text.contains("acme\tproxy\t"), "{text}");
    let text = aspen_ok(&["mirror", "status", "acme"]);
    assert!(text.contains("credential up"), "{text}");
    assert!(
        !text.contains("tok-123"),
        "the token is never printed: {text}"
    );

    std::fs::write(work.join("a.txt"), "two\n").unwrap();
    git(&["commit", "-q", "-am", "two"]);
    git(&["push", "-q", bare.to_str().unwrap(), "main"]);
    let text = aspen_ok(&["mirror", "sync", "acme"]);
    assert!(text.contains("1 in sync, 1 imported"), "{text}");

    let text = aspen_ok(&[
        "import",
        &url,
        "--name",
        "copy",
        "--mode",
        "copy",
        "--credential",
        "up",
    ]);
    assert!(text.contains("copied: copy is no longer linked"), "{text}");
    let (ok, text) = aspen_with("tok-123", &["mirror", "status", "copy"]);
    assert!(!ok && text.contains("copy is not a mirror"), "{text}");
    aspen_ok(&["mirror", "remove", "acme"]);
    let (ok, _) = aspen_with("tok-123", &["mirror", "sync", "acme"]);
    assert!(!ok, "a removed mirror no longer syncs");
}

/// Mirror mode in `aspen serve` (C4 obligation): the server's own sync
/// passes import what lands at a stock `git http-backend` upstream, and a
/// client push to the mirror is forwarded to the upstream before Aspen
/// commits it (proxy push-through).
#[test]
fn serve_runs_mirror_sync_passes_and_proxy_push_through() {
    use aspen_testkit::git::init_bare;
    use aspen_testkit::http_backend::GitHttpBackend;

    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let up = d.join("up");
    std::fs::create_dir_all(&up).unwrap();
    let bare = init_bare(&up, "acme.git").unwrap();
    let bare_path = bare.to_str().unwrap();
    let work = work_repo(d, 1);
    git_ok(&work, &["push", "-q", bare_path, "main"]);
    // "Basic " + base64("git:tok-123").
    let upstream = GitHttpBackend::start_with_auth(&up, "Basic Z2l0OnRvay0xMjM=").unwrap();
    let upstream_url = format!("{}/acme.git", upstream.base_url());
    let mirror_env = |cmd: &mut Command| {
        serve_env(cmd, d);
        cmd.env("ASPEN_WORK_DIR", d.join("work-dir"))
            .env("ASPEN_MIRROR_CREDENTIALS", "up")
            .env("ASPEN_MIRROR_UP_TOKEN", "tok-123")
            .env("ASPEN_MIRROR_SYNC_SECS", "1");
    };
    let mut import = aspen();
    mirror_env(&mut import);
    let out = import
        .args([
            "import",
            &upstream_url,
            "--name",
            "acme",
            "--credential",
            "up",
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let token = admin_ok(
        d,
        &[
            "token",
            "create",
            "--subject",
            "agent:w",
            "--grant",
            "write:acme",
        ],
    );

    let mut cmd = aspen();
    mirror_env(&mut cmd);
    cmd.arg("serve");
    let server = Running::start(cmd);
    assert!(
        server.logs("mirror sync passes every 1 s"),
        "startup names the sync interval"
    );
    let url = authed(&server, &token, "acme");
    let aspen_main = || {
        git_ok(d, &["ls-remote", &url, "refs/heads/main"])
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_owned()
    };

    // A commit lands at the upstream; the server's sync pass imports it.
    std::fs::write(work.join("upstream.txt"), "from upstream\n").unwrap();
    git_ok(&work, &["add", "."]);
    git_ok(&work, &["commit", "-q", "-m", "upstream"]);
    git_ok(&work, &["push", "-q", bare_path, "main"]);
    let upstream_tip = git_ok(&work, &["rev-parse", "HEAD"]);
    let deadline = Instant::now() + Duration::from_secs(30);
    while aspen_main() != upstream_tip {
        assert!(
            Instant::now() < deadline,
            "no sync pass imported {upstream_tip} within 30 s"
        );
        std::thread::sleep(Duration::from_millis(250));
    }

    // A client push to the mirror reaches the upstream (proxy mode).
    std::fs::write(work.join("client.txt"), "from a client\n").unwrap();
    git_ok(&work, &["add", "."]);
    git_ok(&work, &["commit", "-q", "-m", "client"]);
    git_ok(&work, &["push", "-q", &url, "main"]);
    let client_tip = git_ok(&work, &["rev-parse", "HEAD"]);
    assert_eq!(
        git_ok(&bare, &["rev-parse", "refs/heads/main"]),
        client_tip,
        "the push was forwarded to the upstream"
    );
    assert_eq!(aspen_main(), client_tip);
    assert_eq!(server.terminate().code(), Some(0));
}

/// The subcommands a help page lists under `Commands:`, except `help`.
fn listed_subcommands(help: &str) -> Vec<String> {
    help.lines()
        .skip_while(|l| l.trim() != "Commands:")
        .skip(1)
        .take_while(|l| !l.trim().is_empty())
        .filter_map(|l| l.split_whitespace().next())
        .filter(|name| *name != "help")
        .map(str::to_owned)
        .collect()
}

/// Issue #113: help showed the values of env-backed options, which put
/// `DATABASE_URL` (and any credential in it) on the terminal. Every help
/// page of every subcommand, found by walking `Commands:`, is rendered with
/// sentinel values in each variable a CLI option reads, and none may appear.
#[test]
fn help_never_shows_environment_values() {
    const SENTINEL: &str = "SENTINEL_SECRET_113";
    let vars = [
        "DATABASE_URL",
        "PGPASSWORD",
        "ASPEN_BLOB_URL",
        "ASPEN_WORK_DIR",
        "ASPEN_EXPRESS_LISTEN",
        "ASPEN_MIRROR_TOKEN",
    ];
    let mut pending = vec![Vec::<String>::new()];
    let mut pages = 0;
    while let Some(path) = pending.pop() {
        let mut cmd = aspen();
        cmd.args(&path).arg("--help");
        for var in vars {
            cmd.env(var, format!("{SENTINEL}_{var}"));
        }
        let out = cmd.output().expect("aspen runs");
        let help = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success(),
            "aspen {path:?} --help failed: {out:?}"
        );
        for text in [help.as_ref(), String::from_utf8_lossy(&out.stderr).as_ref()] {
            assert!(
                !text.contains(SENTINEL),
                "aspen {} --help shows an environment value:\n{text}",
                path.join(" ")
            );
        }
        pages += 1;
        for sub in listed_subcommands(&help) {
            let mut next = path.clone();
            next.push(sub);
            pending.push(next);
        }
    }
    // The walk reached the nested commands (`mirror`, `token`, `doctor
    // jobs`, ...), not just the top-level page.
    assert!(pages > 15, "only {pages} help pages found");
}

/// Invariant (D4): every command that reads `DATABASE_URL` refuses one
/// carrying a password, without echoing it, and `--help` never shows the
/// value of `DATABASE_URL` from the environment.
#[test]
fn operator_commands_refuse_and_hide_a_password_in_the_database_url() {
    let url = "postgres://aspen:hunter2-not-a-secret@127.0.0.1:9/aspen";
    for args in [
        &["doctor", "jobs"][..],
        &["mirror", "--blob-url", "file:///nonexistent", "status"][..],
    ] {
        let out = aspen()
            .args(args)
            .env("DATABASE_URL", url)
            .output()
            .unwrap();
        let shown = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!out.status.success(), "{args:?}: {shown}");
        assert!(shown.contains("PGPASSWORD"), "{args:?}: {shown}");
        assert!(
            !shown.contains("hunter2"),
            "{args:?} echoed the password: {shown}"
        );
    }
    for args in [&["doctor", "jobs", "--help"][..], &["mirror", "--help"][..]] {
        let out = aspen()
            .args(args)
            .env("DATABASE_URL", url)
            .output()
            .unwrap();
        let shown = String::from_utf8_lossy(&out.stdout);
        assert!(out.status.success(), "{args:?}: {shown}");
        assert!(
            !shown.contains("hunter2"),
            "{args:?} --help shows the password: {shown}"
        );
    }
}

// Issue #118: concurrent fetches, pushes and stitches are bounded per
// instance and per caller, over the binary's own configuration.

/// `user:token` as an HTTP Basic credential.
fn basic(user: &str, token: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input = format!("{user}:{token}");
    let mut out = String::new();
    for chunk in input.as_bytes().chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    format!("Basic {out}")
}

/// `git -c http.extraHeader=...`: sends `token` with the first request.
/// Git sends the credentials in a URL only after a `401`, so where
/// anonymous callers may read, a URL's token is never used.
fn as_token(token: &str) -> String {
    format!("http.extraHeader=Authorization: {}", basic("agent", token))
}

/// Commits one more file in `work`.
fn new_commit(work: &Path) {
    let n = std::fs::read_dir(work).unwrap().count();
    std::fs::write(work.join(format!("next{n}.txt")), format!("{n}\n")).unwrap();
    git_ok(work, &["add", "."]);
    git_ok(work, &["commit", "-q", "-m", &format!("next {n}")]);
}

/// A pkt-line holding `text`.
fn pkt(text: &str) -> String {
    format!("{:04x}{text}", text.len() + 4)
}

/// The headers of a smart-HTTP POST to `path` with a body of `len` bytes.
fn post_head(addr: &str, path: &str, auth: Option<&str>, len: usize) -> String {
    let service = path.rsplit('/').next().unwrap();
    let auth = auth.map_or(String::new(), |a| format!("Authorization: {a}\r\n"));
    format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n{auth}\
         Git-Protocol: version=2\r\nContent-Type: application/x-{service}-request\r\n\
         Content-Length: {len}\r\n\r\n"
    )
}

/// Sends a POST's headers and none of its body: the request waits for a
/// body that never comes (until the connection is dropped).
///
/// Returns once the server has had the request for a while without
/// answering it, so it holds its slot before the caller probes: on separate
/// connections the server may take a later request first, and the held
/// one would then be the refused one (seen on Linux CI).
fn hold_post(addr: &str, path: &str, auth: Option<&str>) -> TcpStream {
    eventually("the held request is not answered", || {
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(post_head(addr, path, auth, 1000).as_bytes())
            .unwrap();
        std::thread::sleep(Duration::from_millis(500));
        s.set_nonblocking(true).unwrap();
        let waiting = matches!(
            s.peek(&mut [0; 1]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
        );
        s.set_nonblocking(false).unwrap();
        waiting.then_some(s)
    })
}

/// One POST with `body`; returns the status, the headers and the body.
fn post(addr: &str, path: &str, auth: Option<&str>, body: &[u8]) -> (u16, String, Vec<u8>) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    s.write_all(post_head(addr, path, auth, body.len()).as_bytes())
        .unwrap();
    // The server may answer before it reads the whole body.
    let _ = s.write_all(body);
    let mut raw = Vec::new();
    let _ = s.read_to_end(&mut raw);
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("no response: {}", String::from_utf8_lossy(&raw)));
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, head, raw[split + 4..].to_vec())
}

/// A protocol v2 `ls-refs` request body.
fn ls_refs_body() -> Vec<u8> {
    (pkt("command=ls-refs\n") + "0001" + "0000").into_bytes()
}

/// Repeats `attempt` for up to 20 s until it returns `Some`.
#[track_caller]
fn eventually<T>(what: &str, mut attempt: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(t) = attempt() {
            return t;
        }
        assert!(Instant::now() < deadline, "never: {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `aspen serve` over `dir` with anonymous `grant` and these extra
/// settings.
fn serve_with(dir: &Path, grant: &str, extra: &[(&str, &str)]) -> Running {
    let mut cmd = aspen();
    serve_env(&mut cmd, dir);
    cmd.arg("serve")
        .env("ASPEN_POLICY", format!(r#"{{"anonymous": ["{grant}"]}}"#));
    for (k, v) in extra {
        cmd.env(k, v);
    }
    Running::start(cmd)
}

/// Per instance (issue #118): with one fetch and one push allowed to run
/// and no queue, a request beyond them is refused with `503` and
/// `Retry-After` while the first holds its slot, before its body is read,
/// and served once the slot is free. Stock git sees the refusal.
#[test]
fn serve_bounds_running_fetches_and_pushes_per_instance() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    admin_ok(d, &["repo", "create", "acme/app"]);
    let server = serve_with(
        d,
        "write:*",
        &[
            ("ASPEN_MAX_CONCURRENT_FETCHES", "1"),
            ("ASPEN_MAX_CONCURRENT_PUSHES", "1"),
            ("ASPEN_ADMISSION_QUEUE", "0"),
        ],
    );
    let url = server.url("/acme/app.git");
    let work = work_repo(d, 2);
    git_ok(&work, &["push", "-q", &url, "main"]);
    let fetch = "/acme/app.git/git-upload-pack";
    let push = "/acme/app.git/git-receive-pack";

    let held = hold_post(&server.addr, fetch, None);
    let (head, body) = eventually("a second fetch is refused", || {
        let (status, head, body) = post(&server.addr, fetch, None, &ls_refs_body());
        (status == 503).then_some((head, body))
    });
    assert!(head.to_ascii_lowercase().contains("retry-after:"), "{head}");
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("busy") && text.contains("fetch"), "{text}");
    let refused = git(d, &["ls-remote", &url]);
    assert!(!refused.status.success(), "ls-remote while busy");
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("503"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    drop(held);
    eventually("the freed fetch slot serves", || {
        (post(&server.addr, fetch, None, &ls_refs_body()).0 == 200).then_some(())
    });
    // A request that fails gives its slot back too.
    let (status, _, body) = post(&server.addr, fetch, None, b"0009junk\n0000");
    assert_eq!(status, 200);
    assert!(String::from_utf8_lossy(&body).contains("ERR"), "{body:?}");
    assert_eq!(post(&server.addr, fetch, None, &ls_refs_body()).0, 200);

    let held = hold_post(&server.addr, push, None);
    eventually("a second push is refused", || {
        new_commit(&work);
        let out = git(&work, &["push", "-q", &url, "main"]);
        (!out.status.success() && String::from_utf8_lossy(&out.stderr).contains("503"))
            .then_some(())
    });
    // Fetches still run beside a held push.
    assert_eq!(post(&server.addr, fetch, None, &ls_refs_body()).0, 200);
    drop(held);
    eventually("the freed push slot serves", || {
        git(&work, &["push", "-q", &url, "main"])
            .status
            .success()
            .then_some(())
    });
}

/// Per caller (issue #118): `ASPEN_LIMITS` caps the fetches and pushes one
/// caller (a token's subject, or an anonymous address) runs at once,
/// shared by HTTP and SSH; over the cap is `429` with `Retry-After`, while
/// other callers are served.
#[test]
fn serve_bounds_concurrent_fetches_and_pushes_per_caller() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    admin_ok(d, &["repo", "create", "acme/app"]);
    let writer = admin_ok(
        d,
        &[
            "token",
            "create",
            "--subject",
            "agent:writer",
            "--grant",
            "write:acme/app",
        ],
    );
    let other = admin_ok(
        d,
        &[
            "token",
            "create",
            "--subject",
            "agent:other",
            "--grant",
            "write:acme/app",
        ],
    );
    let server = serve_with(
        d,
        "read:*",
        &[(
            "ASPEN_LIMITS",
            r#"{"concurrent": {"fetches": 1, "pushes": 1}}"#,
        )],
    );
    let work = work_repo(d, 2);
    git_ok(
        &work,
        &["push", "-q", &authed(&server, &writer, "acme/app"), "main"],
    );
    let fetch = "/acme/app.git/git-upload-pack";
    let push = "/acme/app.git/git-receive-pack";

    // An anonymous caller (its address) holds its one fetch.
    let held = hold_post(&server.addr, fetch, None);
    let (head, body) = eventually("the caller's second fetch is refused", || {
        let (status, head, body) = post(&server.addr, fetch, None, &ls_refs_body());
        (status == 429).then_some((head, body))
    });
    assert!(head.to_ascii_lowercase().contains("retry-after:"), "{head}");
    assert!(
        String::from_utf8_lossy(&body).contains("concurrent fetches"),
        "{}",
        String::from_utf8_lossy(&body)
    );
    // Another caller is not affected.
    git_ok(
        d,
        &[
            "-c",
            &as_token(&other),
            "ls-remote",
            &server.url("/acme/app.git"),
        ],
    );
    drop(held);
    eventually("the caller's slot is free again", || {
        (post(&server.addr, fetch, None, &ls_refs_body()).0 == 200).then_some(())
    });

    // The writer holds its one push; its second is refused, another
    // caller's push is served.
    let held = hold_post(&server.addr, push, Some(&basic("agent", &writer)));
    eventually("the writer's second push is refused", || {
        new_commit(&work);
        let out = git(
            &work,
            &["push", "-q", &authed(&server, &writer, "acme/app"), "main"],
        );
        (!out.status.success() && String::from_utf8_lossy(&out.stderr).contains("429"))
            .then_some(())
    });
    git_ok(
        &work,
        &["push", "-q", &authed(&server, &other, "acme/app"), "main"],
    );
    drop(held);
}

/// Issue #118: a fetch request body is at most 9 MiB (the v2 parser's
/// 8 MiB of pkt-lines plus framing), and a body that trickles in at less
/// than 1 KiB/s on average is refused after its 10 s grace, however
/// steadily it trickles.
#[test]
fn serve_bounds_fetch_body_size_and_rate() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    admin_ok(d, &["repo", "create", "acme/app"]);
    let server = serve_with(d, "read:*", &[]);
    let fetch = "/acme/app.git/git-upload-pack";

    let mut big = ls_refs_body();
    big.resize((9 << 20) + 1, b'0');
    let (status, _, body) = post(&server.addr, fetch, None, &big);
    assert_eq!(status, 200);
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("larger than 9437184 bytes"), "{text}");

    // 128 bytes at 10 bytes a second: never idle for long, but far below
    // 1 KiB/s; the whole body would take about 13 s.
    let mut request = pkt("command=ls-refs\n") + "0001";
    while request.len() < 124 {
        request += &pkt("ref-prefix refs/heads/x\n");
    }
    request += "0000";
    let mut s = TcpStream::connect(&server.addr).unwrap();
    s.write_all(post_head(&server.addr, fetch, None, request.len()).as_bytes())
        .unwrap();
    let started = Instant::now();
    let writer = {
        let mut s = s.try_clone().unwrap();
        let request = request.clone().into_bytes();
        std::thread::spawn(move || {
            for b in request {
                if s.write_all(&[b]).is_err() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let mut raw = Vec::new();
    let _ = s.read_to_end(&mut raw);
    let answered = started.elapsed();
    let _ = s.shutdown(std::net::Shutdown::Both);
    writer.join().unwrap();
    let text = String::from_utf8_lossy(&raw);
    assert!(text.contains("too slow"), "{text}");
    assert!(
        answered < Duration::from_secs(12),
        "refused after {answered:?}, before the body could end"
    );
}

/// A deterministic stream of `n` bytes that do not compress.
fn noise(n: usize) -> Vec<u8> {
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

/// A v2 fetch of `tip` whose response is never read: the server stitches
/// a pack too large for the socket buffers and holds its stitch slot.
fn hold_stitch(addr: &str, tip: &str, auth: Option<&str>) -> TcpStream {
    let body = pkt("command=fetch\n")
        + "0001"
        + &pkt("no-progress\n")
        + &pkt(&format!("want {tip}\n"))
        + &pkt("done\n")
        + "0000";
    let mut s = TcpStream::connect(addr).unwrap();
    s.write_all(post_head(addr, "/acme/app.git/git-upload-pack", auth, body.len()).as_bytes())
        .unwrap();
    s.write_all(body.as_bytes()).unwrap();
    // The response starts only once the stitch holds its slot.
    s.set_read_timeout(Some(Duration::from_secs(20))).unwrap();
    let n = s.peek(&mut [0; 1]).expect("the held stitch starts");
    assert_eq!(n, 1, "the held stitch was answered with nothing");
    s
}

/// Issue #118: stitching (building the inline pack of a fetch) is bounded
/// per caller and per instance, apart from fetches: a fetch beyond either
/// cap gets a retryable `ERR`, and other callers (per caller) or the same
/// caller once a slot frees (per instance) are served.
#[test]
fn serve_bounds_concurrent_stitches_per_caller_and_per_instance() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    admin_ok(d, &["repo", "create", "acme/app"]);
    let reader = admin_ok(
        d,
        &[
            "token",
            "create",
            "--subject",
            "agent:reader",
            "--grant",
            "read:acme/app",
        ],
    );
    let work = work_repo(d, 1);
    // Far more than the socket buffers and the server's frame channel hold.
    std::fs::write(work.join("noise.bin"), noise(48 << 20)).unwrap();
    git_ok(&work, &["add", "."]);
    git_ok(&work, &["commit", "-q", "-m", "noise"]);
    let tip = git_ok(&work, &["rev-parse", "HEAD"]);
    let clone = |server: &Running, name: &str, token: Option<&str>| {
        let url = server.url("/acme/app.git");
        let _ = std::fs::remove_dir_all(d.join(name));
        match token {
            Some(t) => git(d, &["-c", &as_token(t), "clone", "-q", &url, name]),
            None => git(d, &["clone", "-q", &url, name]),
        }
    };

    // Per caller: one stitch per caller, any number per instance.
    let server = serve_with(
        d,
        "write:*",
        &[("ASPEN_LIMITS", r#"{"concurrent": {"stitches": 1}}"#)],
    );
    git_ok(&work, &["push", "-q", &server.url("/acme/app.git"), "main"]);
    let held = hold_stitch(&server.addr, &tip, None);
    let stderr = eventually("the caller's second stitch is refused", || {
        let out = clone(&server, "anon", None);
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        (!out.status.success()).then_some(stderr)
    });
    assert!(stderr.contains("concurrent stitches"), "{stderr}");
    let out = clone(&server, "other", Some(&reader));
    assert!(
        out.status.success(),
        "another caller is served: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    drop(held);
    drop(server);

    // Per instance: one stitch at a time, no queue.
    let server = serve_with(
        d,
        "read:*",
        &[
            ("ASPEN_MAX_CONCURRENT_STITCHES", "1"),
            ("ASPEN_ADMISSION_QUEUE", "0"),
        ],
    );
    let held = hold_stitch(&server.addr, &tip, None);
    let stderr = eventually("a second stitch on the instance is refused", || {
        let out = clone(&server, "busy", Some(&reader));
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        (!out.status.success()).then_some(stderr)
    });
    assert!(stderr.contains("busy"), "{stderr}");
    drop(held);
    eventually("the freed stitch slot serves", || {
        clone(&server, "after", Some(&reader))
            .status
            .success()
            .then_some(())
    });
}
