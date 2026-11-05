//! [`GitHttpBackend`]: a tiny HTTP server that hands every request to stock
//! `git http-backend` over CGI.
//!
//! It is the reference server for the interop tests and the differential
//! oracle. The bridge uses only the standard library, handles one request per
//! connection (`Connection: close`) and is meant for tests on localhost only.
//!
//! With [`RefVisibility::BranchScoped`] it imitates Aspen's ref visibility
//! (spike S2 in `docs/PLAN.md`): the plain repository URL advertises only the
//! trunk branch, and `<repo>/-/refs/heads/<branch>` advertises the trunk plus
//! that one branch. It does this with stock `transfer.hideRefs`, so it is a
//! cheap stand-in for testing tools against Aspen's URL layout.

use std::io::{self, BufRead, BufReader, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

/// Largest request body the bridge accepts, as a guard against runaway tests.
const MAX_BODY: usize = 256 * 1024 * 1024;

/// Which refs a [`GitHttpBackend`] shows to clients.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RefVisibility {
    /// Every ref, as plain `git http-backend` does.
    #[default]
    All,
    /// Aspen's layout: `/<repo>` advertises only `refs/heads/<trunk>`, and
    /// `/<repo>/-/refs/heads/<branch>` advertises the trunk and that branch.
    /// Other branches are hidden with `transfer.hideRefs`, so stock
    /// `receive-pack` also refuses to update them through a URL that hides
    /// them (Aspen would accept such a push; tools should not rely on either).
    BranchScoped {
        /// The trunk branch's short name, such as `main`.
        trunk: String,
    },
}

/// The path segment that starts a branch scope: `<repo>/-/refs/heads/<branch>`.
pub const SCOPE_SEGMENT: &str = "/-/";

/// An HTTP server on `127.0.0.1` that serves the bare repositories under
/// `project_root` through `git http-backend`. Stopped when dropped.
#[derive(Debug)]
pub struct GitHttpBackend {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
}

impl GitHttpBackend {
    /// Starts serving `project_root` on an ephemeral port of `127.0.0.1`.
    pub fn start(project_root: &Path) -> io::Result<Self> {
        Self::bind("127.0.0.1:0", project_root)
    }

    /// Like [`GitHttpBackend::start`], showing refs according to `visibility`.
    pub fn start_with(project_root: &Path, visibility: RefVisibility) -> io::Result<Self> {
        Self::bind_with("127.0.0.1:0", project_root, visibility)
    }

    /// Starts serving `project_root` on `addr`, for example `127.0.0.1:7070`.
    /// The bridge has no authentication: bind it to loopback only.
    pub fn bind(addr: impl ToSocketAddrs, project_root: &Path) -> io::Result<Self> {
        Self::bind_with(addr, project_root, RefVisibility::All)
    }

    /// Like [`GitHttpBackend::start`], but every request must carry exactly
    /// `authorization` as its `Authorization` header; any other request
    /// gets `401` with a `Basic` challenge, as a forge answers. Authorized
    /// requests run with `REMOTE_USER` set, so `receive-pack` is enabled
    /// as on a server behind authentication.
    pub fn start_with_auth(project_root: &Path, authorization: &str) -> io::Result<Self> {
        Self::bind_full(
            "127.0.0.1:0",
            project_root,
            RefVisibility::All,
            Some(authorization.to_owned()),
        )
    }

    /// Like [`GitHttpBackend::bind`], showing refs according to `visibility`.
    pub fn bind_with(
        addr: impl ToSocketAddrs,
        project_root: &Path,
        visibility: RefVisibility,
    ) -> io::Result<Self> {
        Self::bind_full(addr, project_root, visibility, None)
    }

    fn bind_full(
        addr: impl ToSocketAddrs,
        project_root: &Path,
        visibility: RefVisibility,
        authorization: Option<String>,
    ) -> io::Result<Self> {
        let visibility = Arc::new(visibility);
        let authorization = Arc::new(authorization);
        let listener = TcpListener::bind(addr)?;
        let addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let root = project_root.to_path_buf();
        let stop_flag = Arc::clone(&stop);
        let accept_thread = thread::spawn(move || {
            for stream in listener.incoming() {
                if stop_flag.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let root = root.clone();
                let visibility = Arc::clone(&visibility);
                let authorization = Arc::clone(&authorization);
                thread::spawn(move || {
                    if let Err(err) = serve(stream, &root, &visibility, authorization.as_deref()) {
                        eprintln!("git-http-backend bridge: {err}");
                    }
                });
            }
        });
        Ok(Self {
            addr,
            stop,
            accept_thread: Some(accept_thread),
        })
    }

    /// The address the server listens on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The base URL; append `/<repo>.git` to get a clone URL.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for GitHttpBackend {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the accept loop so it sees the flag.
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    query: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

fn serve(
    stream: TcpStream,
    root: &Path,
    visibility: &RefVisibility,
    authorization: Option<&str>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let Some(request) = read_request(&mut reader)? else {
        return Ok(());
    };
    let response = match authorization {
        Some(want) if request.header("Authorization") != Some(want) => {
            b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"git\"\r\n\
              Content-Length: 0\r\nConnection: close\r\n\r\n"
                .to_vec()
        }
        Some(_) => run_cgi(&request, root, visibility, Some("git"))?,
        None => run_cgi(&request, root, visibility, None)?,
    };
    let mut stream = stream;
    stream.write_all(&response)?;
    stream.flush()?;
    stream.shutdown(Shutdown::Both)
}

fn read_request(reader: &mut impl BufRead) -> io::Result<Option<Request>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return Err(invalid(format!("bad request line {line:?}")));
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut request = Request {
        method: method.to_owned(),
        path: path.to_owned(),
        query: query.to_owned(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| invalid(format!("bad header {line:?}")))?;
        request
            .headers
            .push((name.trim().to_owned(), value.trim().to_owned()));
    }
    if request
        .header("Transfer-Encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        request.body = read_chunked(reader)?;
    } else if let Some(len) = request.header("Content-Length") {
        let len: usize = len
            .parse()
            .map_err(|_| invalid(format!("bad Content-Length {len:?}")))?;
        if len > MAX_BODY {
            return Err(invalid(format!("body of {len} bytes is too large")));
        }
        request.body = vec![0; len];
        reader.read_exact(&mut request.body)?;
    }
    Ok(Some(request))
}

fn read_chunked(reader: &mut impl BufRead) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut size_line = String::new();
        reader.read_line(&mut size_line)?;
        let size_hex = size_line.trim().split(';').next().unwrap_or("");
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| invalid(format!("bad chunk size {size_line:?}")))?;
        if size == 0 {
            // Skip optional trailers up to the blank line.
            loop {
                let mut trailer = String::new();
                if reader.read_line(&mut trailer)? == 0 || trailer.trim().is_empty() {
                    return Ok(body);
                }
            }
        }
        if body.len() + size > MAX_BODY {
            return Err(invalid("chunked body is too large".to_owned()));
        }
        let start = body.len();
        body.resize(start + size, 0);
        reader.read_exact(&mut body[start..])?;
        let mut crlf = [0; 2];
        reader.read_exact(&mut crlf)?;
    }
}

/// The `transfer.hideRefs` values for a request path, and the path with any
/// branch scope removed.
fn scope(path: &str, visibility: &RefVisibility) -> (String, Vec<String>) {
    let RefVisibility::BranchScoped { trunk } = visibility else {
        return (path.to_owned(), Vec::new());
    };
    let mut hide = vec!["refs/heads/".to_owned(), format!("!refs/heads/{trunk}")];
    let Some((repo, rest)) = path.split_once(SCOPE_SEGMENT) else {
        return (path.to_owned(), hide);
    };
    // `rest` is `<ref>/<service path>`, such as `refs/heads/b/info/refs`.
    for service in [
        "/info/refs",
        "/git-upload-pack",
        "/git-receive-pack",
        "/HEAD",
    ] {
        if let Some(refname) = rest.strip_suffix(service) {
            hide.push(format!("!{refname}"));
            return (format!("{repo}{service}"), hide);
        }
    }
    (path.to_owned(), hide)
}

fn run_cgi(
    request: &Request,
    root: &Path,
    visibility: &RefVisibility,
    remote_user: Option<&str>,
) -> io::Result<Vec<u8>> {
    let (path, hide) = scope(&request.path, visibility);
    let mut cmd = Command::new("git");
    cmd.arg("http-backend")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_PROJECT_ROOT", root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("GATEWAY_INTERFACE", "CGI/1.1")
        .env("SERVER_PROTOCOL", "HTTP/1.1")
        .env("REQUEST_METHOD", &request.method)
        .env("PATH_INFO", &path)
        .env("QUERY_STRING", &request.query)
        .env("REMOTE_ADDR", "127.0.0.1")
        .env("CONTENT_LENGTH", request.body.len().to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if let Some(v) = request.header("Content-Type") {
        cmd.env("CONTENT_TYPE", v);
    }
    if let Some(user) = remote_user {
        cmd.env("REMOTE_USER", user);
    }
    // `git http-backend` inflates gzip request bodies itself.
    if let Some(v) = request.header("Content-Encoding") {
        cmd.env("HTTP_CONTENT_ENCODING", v);
    }
    // Protocol v2 is negotiated through this header.
    if let Some(v) = request.header("Git-Protocol") {
        cmd.env("GIT_PROTOCOL", v);
    }
    // Per-request configuration (git 2.31 or later), inherited by the
    // `upload-pack` or `receive-pack` that `http-backend` runs.
    if !hide.is_empty() {
        cmd.env("GIT_CONFIG_COUNT", hide.len().to_string());
        for (i, value) in hide.iter().enumerate() {
            cmd.env(format!("GIT_CONFIG_KEY_{i}"), "transfer.hideRefs")
                .env(format!("GIT_CONFIG_VALUE_{i}"), value);
        }
    }
    let mut child = cmd.spawn()?;
    let mut stdin = child.stdin.take().expect("stdin is piped");
    let body = request.body.clone();
    let writer = thread::spawn(move || stdin.write_all(&body));
    let output = child.wait_with_output()?;
    writer
        .join()
        .map_err(|_| invalid("stdin writer panicked".to_owned()))??;
    cgi_to_http(&output.stdout)
}

/// Turns CGI output (headers, blank line, body) into an HTTP/1.1 response.
fn cgi_to_http(cgi: &[u8]) -> io::Result<Vec<u8>> {
    let (head, body) =
        split_head(cgi).ok_or_else(|| invalid("CGI output has no header block".to_owned()))?;
    let head = String::from_utf8_lossy(head);
    let mut status = "200 OK".to_owned();
    let mut headers = String::new();
    for line in head.lines() {
        match line.split_once(':') {
            Some((name, value)) if name.eq_ignore_ascii_case("Status") => {
                status = value.trim().to_owned();
            }
            Some(_) => {
                headers.push_str(line);
                headers.push_str("\r\n");
            }
            None => {}
        }
    }
    let mut out = format!(
        "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    Ok(out)
}

fn split_head(cgi: &[u8]) -> Option<(&[u8], &[u8])> {
    for (sep, len) in [(&b"\r\n\r\n"[..], 4), (&b"\n\n"[..], 2)] {
        if let Some(i) = cgi.windows(len).position(|w| w == sep) {
            return Some((&cgi[..i], &cgi[i + len..]));
        }
    }
    None
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunked_body_is_reassembled() {
        let raw = b"POST /r.git/git-upload-pack HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nabcd\r\n3;ext=1\r\nefg\r\n0\r\n\r\n";
        let req = read_request(&mut &raw[..]).unwrap().unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/r.git/git-upload-pack");
        assert_eq!(req.body, b"abcdefg");
    }

    #[test]
    fn query_string_is_split_from_path() {
        let raw = b"GET /r.git/info/refs?service=git-upload-pack HTTP/1.1\r\nGit-Protocol: version=2\r\n\r\n";
        let req = read_request(&mut &raw[..]).unwrap().unwrap();
        assert_eq!(req.path, "/r.git/info/refs");
        assert_eq!(req.query, "service=git-upload-pack");
        assert_eq!(req.header("git-protocol"), Some("version=2"));
    }

    #[test]
    fn branch_scope_is_stripped_and_unhides_one_branch() {
        let scoped = RefVisibility::BranchScoped {
            trunk: "main".to_owned(),
        };
        let (path, hide) = scope("/r.git/-/refs/heads/topic-1/info/refs", &scoped);
        assert_eq!(path, "/r.git/info/refs");
        assert_eq!(
            hide,
            ["refs/heads/", "!refs/heads/main", "!refs/heads/topic-1"]
        );
        let (path, hide) = scope("/r.git/-/refs/heads/a/b/git-receive-pack", &scoped);
        assert_eq!(path, "/r.git/git-receive-pack");
        assert_eq!(hide[2], "!refs/heads/a/b");
        let (path, hide) = scope("/r.git/git-upload-pack", &scoped);
        assert_eq!(path, "/r.git/git-upload-pack");
        assert_eq!(hide, ["refs/heads/", "!refs/heads/main"]);
        let (path, hide) = scope("/r.git/-/refs/heads/x/info/refs", &RefVisibility::All);
        assert_eq!(path, "/r.git/-/refs/heads/x/info/refs");
        assert!(hide.is_empty());
    }

    #[test]
    fn cgi_status_header_becomes_the_status_line() {
        let http =
            cgi_to_http(b"Status: 404 Not Found\r\nContent-Type: text/plain\r\n\r\nnope").unwrap();
        let http = String::from_utf8(http).unwrap();
        assert!(http.starts_with("HTTP/1.1 404 Not Found\r\n"), "{http}");
        assert!(http.contains("Content-Type: text/plain\r\n"));
        assert!(http.contains("Content-Length: 4\r\n"));
        assert!(http.ends_with("\r\n\r\nnope"));
        assert!(!http.contains("Status:"));
    }
}
