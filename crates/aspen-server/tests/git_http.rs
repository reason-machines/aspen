//! Stock `git` over HTTP against the access middleware.
//!
//! Until receive-pack (B1) and upload-pack (B2) exist, the Git handler
//! behind the middleware is stock `git http-backend` over CGI, serving the
//! bare repository named by the [`Authorized`] extension. That makes this a
//! test of the middleware with a real client: the credential challenge,
//! Git's credential helper flow, and refusals as Git reports them.

mod common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};

use aspen_auth::LimitsConfig;
use aspen_core::auth::Principal;
use aspen_server::access::{Authorized, Endpoint};
use aspen_testkit::{init_bare, isolated_git};
use axum::Router;
use axum::body::Body;
use axum::extract::{Extension, Request};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{Next, from_fn};
use axum::response::Response;
use common::{access, app, meta_with, mint};
use http_body_util::BodyExt;
use serde_json::json;
use tokio::io::AsyncWriteExt;

/// Runs `git http-backend` for an authorized request.
async fn cgi(root: PathBuf, a: Authorized, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let Ok(body) = body.collect().await else {
        return status(StatusCode::BAD_REQUEST);
    };
    let body = body.to_bytes();
    let suffix = match a.route.endpoint {
        Endpoint::UploadPackAdvertisement | Endpoint::ReceivePackAdvertisement => "info/refs",
        Endpoint::UploadPack => "git-upload-pack",
        Endpoint::ReceivePack => "git-receive-pack",
    };
    let user = match a.identity.principal() {
        Principal::Subject { id } => id.clone(),
        Principal::Anonymous => String::new(),
    };
    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned()
    };
    let mut child = tokio::process::Command::new("git")
        .arg("http-backend")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_PROJECT_ROOT", &root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("GATEWAY_INTERFACE", "CGI/1.1")
        .env("REQUEST_METHOD", parts.method.as_str())
        .env("PATH_INFO", format!("/{}.git/{suffix}", a.repo.name))
        .env("QUERY_STRING", parts.uri.query().unwrap_or(""))
        .env("REMOTE_USER", user)
        .env("REMOTE_ADDR", "127.0.0.1")
        .env("CONTENT_LENGTH", body.len().to_string())
        .env("CONTENT_TYPE", header("content-type"))
        .env("HTTP_CONTENT_ENCODING", header("content-encoding"))
        .env("GIT_PROTOCOL", header("git-protocol"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let writer = tokio::spawn(async move {
        let _ = stdin.write_all(&body).await;
    });
    let out = child.wait_with_output().await.unwrap();
    writer.await.unwrap();
    let out = out.stdout;
    let Some(split) = out.windows(4).position(|w| w == b"\r\n\r\n") else {
        return status(StatusCode::BAD_GATEWAY);
    };
    let mut response = Response::new(Body::from(out[split + 4..].to_vec()));
    for line in String::from_utf8_lossy(&out[..split]).split("\r\n") {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        if name.eq_ignore_ascii_case("status") {
            let code = value.split(' ').next().unwrap().parse().unwrap();
            *response.status_mut() = StatusCode::from_u16(code).unwrap();
        } else {
            response.headers_mut().insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
    }
    response
}

fn status(s: StatusCode) -> Response {
    let mut r = Response::new(Body::empty());
    *r.status_mut() = s;
    r
}

type Statuses = Arc<Mutex<Vec<(String, u16)>>>;

struct Server {
    url: String,
    statuses: Statuses,
    access: Arc<common::TestAccess>,
    _root: tempfile::TempDir,
}

async fn start() -> Server {
    let root = tempfile::tempdir().unwrap();
    for repo in ["acme/a.git", "acme/b.git"] {
        init_bare(root.path(), repo).unwrap();
    }
    let meta = meta_with(&["acme/a", "acme/b"]).await;
    let access = access(json!({}), LimitsConfig::default(), meta);
    let project = root.path().to_path_buf();
    let inner = Router::new().fallback(move |Extension(a): Extension<Authorized>, r: Request| {
        cgi(project.clone(), a, r)
    });
    let statuses: Statuses = Arc::default();
    let seen = statuses.clone();
    // Records every answer the client got, including the middleware's.
    let app = app(access.clone(), inner).layer(from_fn(move |r: Request, next: Next| {
        let seen = seen.clone();
        async move {
            let path = format!("{} {}", r.method(), r.uri());
            let resp = next.run(r).await;
            seen.lock().unwrap().push((path, resp.status().as_u16()));
            resp
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server {
        url: format!("http://{addr}"),
        statuses,
        access,
        _root: root,
    }
}

/// Runs git in `dir` with `token` (if any) from a credential helper, as an
/// agent would configure it. Returns (success, stderr).
fn git(home: &Path, dir: &Path, token: Option<&str>, args: &[&str]) -> (bool, String) {
    let mut cmd = isolated_git(home);
    cmd.current_dir(dir)
        .args(["-c", "protocol.version=2", "-c", "credential.helper="]);
    if let Some(token) = token {
        cmd.env("ASPEN_TEST_TOKEN", token).args([
            "-c",
            "credential.helper=!f() { echo username=x-access-token; echo \"password=$ASPEN_TEST_TOKEN\"; }; f",
        ]);
    }
    let out = cmd.args(args).output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stock_git_gets_a_challenge_and_then_succeeds_with_a_valid_token() {
    let server = start().await;
    let home = tempfile::tempdir().unwrap();
    let h = home.path();
    let a_url = format!("{}/acme/a.git", server.url);
    let writer = mint(&server.access, "agent:a", &["write:acme/a"]);
    let reader = mint(&server.access, "agent:r", &["read:acme/a"]);
    let other = mint(&server.access, "agent:b", &["write:acme/b"]);

    // No credential: Git gets the 401 challenge and, with terminal prompts
    // off and no helper, cannot answer it.
    let (ok, err) = git(h, h, None, &["ls-remote", &a_url]);
    assert!(!ok);
    assert!(err.contains("could not read Username"), "{err}");
    assert_eq!(server.statuses.lock().unwrap().last().unwrap().1, 401);

    // A valid token through the credential helper: challenged, then
    // accepted. Push a commit, then clone it back.
    let work = h.join("work");
    std::fs::create_dir(&work).unwrap();
    git(h, &work, None, &["init", "-q", "-b", "main"]);
    std::fs::write(work.join("README"), "hello\n").unwrap();
    git(h, &work, None, &["add", "README"]);
    assert!(git(h, &work, None, &["commit", "-q", "-m", "first"]).0);
    server.statuses.lock().unwrap().clear();
    let (ok, err) = git(h, &work, Some(&writer), &["push", &a_url, "main"]);
    assert!(ok, "{err}");
    {
        let statuses = server.statuses.lock().unwrap();
        let first = &statuses[0];
        assert!(
            first.0.contains("service=git-receive-pack") && first.1 == 401,
            "{statuses:?}"
        );
        assert!(
            statuses.iter().any(|(p, s)| p.starts_with("POST")
                && p.ends_with("git-receive-pack")
                && *s == 200),
            "{statuses:?}"
        );
    }
    let clone = h.join("clone");
    let (ok, err) = git(
        h,
        h,
        Some(&reader),
        &["clone", "-q", &a_url, clone.to_str().unwrap()],
    );
    assert!(ok, "{err}");
    assert_eq!(
        std::fs::read_to_string(clone.join("README")).unwrap(),
        "hello\n"
    );
    assert!(git(h, &clone, None, &["fsck", "--strict"]).0);

    // A read token cannot push: 403.
    std::fs::write(work.join("README"), "two\n").unwrap();
    assert!(git(h, &work, None, &["commit", "-q", "-am", "second"]).0);
    let (ok, err) = git(h, &work, Some(&reader), &["push", &a_url, "main"]);
    assert!(!ok);
    assert!(err.contains("403"), "{err}");

    // Another repository's token: Git is told the repository is not found.
    let (ok, err) = git(h, h, Some(&other), &["ls-remote", &a_url]);
    assert!(!ok);
    assert!(err.contains("not found"), "{err}");
    let (ok, err) = git(h, &work, Some(&other), &["push", &a_url, "main"]);
    assert!(!ok);
    assert!(err.contains("not found"), "{err}");

    // A wrong token: authentication fails.
    let mut wrong = writer.clone();
    let last = wrong.pop().unwrap();
    wrong.push(if last == '0' { '1' } else { '0' });
    let (ok, err) = git(h, h, Some(&wrong), &["ls-remote", &a_url]);
    assert!(!ok);
    assert!(err.contains("Authentication failed"), "{err}");

    // Nothing above changed the repository except the writer's push: the
    // reader's and the other token's pushes never landed.
    let (ok, err) = git(h, &clone, Some(&reader), &["fetch", "-q", "origin"]);
    assert!(ok, "{err}");
    let log = isolated_git(h)
        .current_dir(&clone)
        .args(["log", "--format=%s", "origin/main"])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&log.stdout), "first\n");
}
