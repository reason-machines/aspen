//! A fault-injecting HTTP/1.1 proxy for an S3-compatible object store.
//!
//! Aspen reaches the bucket through it (`AWS_ENDPOINT_URL` points here), and
//! so do clients that follow a presigned pack URL. It forwards each request
//! unchanged, Host header included, so SigV4 signatures still verify at the
//! store. For a configurable share of requests it injects instead:
//!
//! - an error answer: `503 SlowDown` or `500 InternalError`, without
//!   forwarding;
//! - a close **before** forwarding: the connection closes and the store never
//!   sees the request;
//! - a close **after** forwarding: the store handles the request (an upload
//!   lands) but the client never hears back, the case that tests whether
//!   writes are idempotent;
//! - a truncated answer: the head and part of the body, then a close;
//! - latency before forwarding.
//!
//! It can also stall every request until healed, which is what a partition
//! between Aspen and the store looks like.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use aspen_testkit::rng::Rng;

/// The largest request or response body the proxy buffers.
const MAX_BODY: usize = 512 * 1024 * 1024;

/// What share of requests gets each fault, as probabilities from 0 to 1.
/// They are tried in field order; at most one applies per request.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct S3Faults {
    /// Answer `503 SlowDown` without forwarding.
    pub slow_down: f64,
    /// Answer `500 InternalError` without forwarding.
    pub internal_error: f64,
    /// Close the connection without forwarding.
    pub close_before: f64,
    /// Forward, then close the connection instead of answering.
    pub close_after: f64,
    /// Forward, then send only part of the answer.
    pub truncate: f64,
    /// Share of requests held before forwarding, by up to `max_delay`.
    pub delayed: f64,
    /// The longest delay.
    pub max_delay: Duration,
}

impl S3Faults {
    /// No faults.
    pub const NONE: Self = Self {
        slow_down: 0.0,
        internal_error: 0.0,
        close_before: 0.0,
        close_after: 0.0,
        truncate: 0.0,
        delayed: 0.0,
        max_delay: Duration::ZERO,
    };
}

/// How many requests met each fault.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct S3Counts {
    pub requests: u64,
    pub slow_down: u64,
    pub internal_error: u64,
    pub close_before: u64,
    pub close_after: u64,
    pub truncated: u64,
    pub delayed: u64,
    pub stalled: u64,
}

#[derive(Debug)]
struct State {
    faults: S3Faults,
    stalled: bool,
    rng: Rng,
    counts: S3Counts,
}

#[derive(Debug)]
struct Shared {
    upstream: SocketAddr,
    state: Mutex<State>,
    changed: Condvar,
    stop: AtomicBool,
}

/// The proxy, on an ephemeral port of 127.0.0.1; stops when dropped.
#[derive(Debug)]
pub struct S3Proxy {
    addr: SocketAddr,
    shared: Arc<Shared>,
}

/// The fault chosen for one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Forward,
    SlowDown,
    InternalError,
    CloseBefore,
    CloseAfter,
    Truncate,
}

impl S3Proxy {
    /// Starts a proxy to the store at `upstream`; `seed` picks which
    /// requests get faults.
    pub fn start(upstream: SocketAddr, seed: u64) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared {
            upstream,
            state: Mutex::new(State {
                faults: S3Faults::NONE,
                stalled: false,
                rng: Rng::new(seed),
                counts: S3Counts::default(),
            }),
            changed: Condvar::new(),
            stop: AtomicBool::new(false),
        });
        let s = Arc::clone(&shared);
        thread::Builder::new()
            .name("s3-proxy".into())
            .spawn(move || {
                while !s.stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((client, _)) => {
                            let s = Arc::clone(&s);
                            let _ = thread::Builder::new().name("s3-proxy-conn".into()).spawn(
                                move || {
                                    let _ = serve(client, &s);
                                },
                            );
                        }
                        Err(_) => thread::sleep(Duration::from_millis(5)),
                    }
                }
            })?;
        Ok(Self { addr, shared })
    }

    /// The address to use as the S3 endpoint.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Applies `faults` to requests from now on.
    pub fn set_faults(&self, faults: S3Faults) {
        lock(&self.shared.state).faults = faults;
    }

    /// Holds every request until [`S3Proxy::heal`] (a partition).
    pub fn stall(&self, on: bool) {
        lock(&self.shared.state).stalled = on;
        self.shared.changed.notify_all();
    }

    /// No faults and no stall.
    pub fn heal(&self) {
        {
            let mut st = lock(&self.shared.state);
            st.faults = S3Faults::NONE;
            st.stalled = false;
        }
        self.shared.changed.notify_all();
    }

    /// What the proxy has done so far.
    pub fn counts(&self) -> S3Counts {
        lock(&self.shared.state).counts
    }
}

impl Drop for S3Proxy {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        self.heal();
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// One parsed HTTP message head plus its raw bytes.
#[derive(Debug)]
struct Head {
    raw: Vec<u8>,
    start_line: String,
    headers: Vec<(String, String)>,
}

impl Head {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn closes(&self) -> bool {
        self.header("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"))
    }

    fn chunked(&self) -> bool {
        self.header("transfer-encoding")
            .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"))
    }

    fn content_length(&self) -> io::Result<Option<usize>> {
        match self.header("content-length") {
            None => Ok(None),
            Some(v) => v
                .trim()
                .parse()
                .map(Some)
                .map_err(|_| io::Error::other("bad content-length")),
        }
    }
}

/// Reads a message head; `None` at a clean EOF before any byte.
fn read_head(r: &mut impl BufRead) -> io::Result<Option<Head>> {
    let mut raw = Vec::new();
    let mut lines = Vec::new();
    loop {
        let mut line = Vec::new();
        let n = r.read_until(b'\n', &mut line)?;
        if n == 0 {
            if raw.is_empty() {
                return Ok(None);
            }
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        raw.extend_from_slice(&line);
        if raw.len() > 64 * 1024 {
            return Err(io::Error::other("head too long"));
        }
        let text = String::from_utf8_lossy(&line).trim_end().to_owned();
        if text.is_empty() {
            break;
        }
        lines.push(text);
    }
    let mut it = lines.into_iter();
    let start_line = it.next().unwrap_or_default();
    let headers = it
        .filter_map(|l| {
            let (k, v) = l.split_once(':')?;
            Some((k.trim().to_owned(), v.trim().to_owned()))
        })
        .collect();
    Ok(Some(Head {
        raw,
        start_line,
        headers,
    }))
}

/// Reads a body framed by `head`, raw (chunk framing kept), so it can be
/// forwarded byte for byte. `until_eof` reads a response body that has
/// neither length nor chunking.
fn read_body(r: &mut impl BufRead, head: &Head, until_eof: bool) -> io::Result<Vec<u8>> {
    if head.chunked() {
        let mut raw = Vec::new();
        loop {
            let mut size_line = Vec::new();
            if r.read_until(b'\n', &mut size_line)? == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            raw.extend_from_slice(&size_line);
            let text = String::from_utf8_lossy(&size_line);
            let hex = text.trim().split(';').next().unwrap_or("");
            let size = usize::from_str_radix(hex, 16).map_err(|_| io::Error::other("bad chunk"))?;
            if size == 0 {
                // Trailers, up to the empty line.
                loop {
                    let mut t = Vec::new();
                    if r.read_until(b'\n', &mut t)? == 0 {
                        return Err(io::ErrorKind::UnexpectedEof.into());
                    }
                    raw.extend_from_slice(&t);
                    if t == b"\r\n" || t == b"\n" {
                        return Ok(raw);
                    }
                }
            }
            if raw.len() + size > MAX_BODY {
                return Err(io::Error::other("body too large"));
            }
            let mut chunk = vec![0u8; size + 2];
            r.read_exact(&mut chunk)?;
            raw.extend_from_slice(&chunk);
        }
    }
    if let Some(n) = head.content_length()? {
        if n > MAX_BODY {
            return Err(io::Error::other("body too large"));
        }
        let mut body = vec![0u8; n];
        r.read_exact(&mut body)?;
        return Ok(body);
    }
    let mut body = Vec::new();
    if until_eof {
        r.take(MAX_BODY as u64).read_to_end(&mut body)?;
    }
    Ok(body)
}

fn choose(shared: &Shared) -> (Action, Duration) {
    let mut st = lock(&shared.state);
    let f = st.faults;
    st.counts.requests += 1;
    let mut roll = |p: f64| p > 0.0 && (st.rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64 <= p;
    let action = if roll(f.slow_down) {
        Action::SlowDown
    } else if roll(f.internal_error) {
        Action::InternalError
    } else if roll(f.close_before) {
        Action::CloseBefore
    } else if roll(f.close_after) {
        Action::CloseAfter
    } else if roll(f.truncate) {
        Action::Truncate
    } else {
        Action::Forward
    };
    let delayed = roll(f.delayed);
    let delay = if delayed && !f.max_delay.is_zero() {
        let ms = f.max_delay.as_millis().max(1) as u64;
        Duration::from_millis(st.rng.next_u64() % ms)
    } else {
        Duration::ZERO
    };
    let c = &mut st.counts;
    match action {
        Action::Forward => {}
        Action::SlowDown => c.slow_down += 1,
        Action::InternalError => c.internal_error += 1,
        Action::CloseBefore => c.close_before += 1,
        Action::CloseAfter => c.close_after += 1,
        // Counted in `serve`, only when an answer had a body to cut.
        Action::Truncate => {}
    }
    if !delay.is_zero() {
        c.delayed += 1;
    }
    (action, delay)
}

/// Waits out a stall. Returns false if the proxy stopped.
fn wait_unstalled(shared: &Shared) -> bool {
    let mut st = lock(&shared.state);
    let mut counted = false;
    while st.stalled {
        if shared.stop.load(Ordering::SeqCst) {
            return false;
        }
        if !counted {
            st.counts.stalled += 1;
            counted = true;
        }
        st = shared
            .changed
            .wait_timeout(st, Duration::from_millis(200))
            .unwrap_or_else(|e| e.into_inner())
            .0;
    }
    true
}

fn error_answer(code: u16, reason: &str, s3_code: &str) -> Vec<u8> {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{s3_code}</Code>\
         <Message>injected by the Aspen chaos suite</Message></Error>"
    );
    format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\
         Retry-After: 0\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn serve(client: TcpStream, shared: &Shared) -> io::Result<()> {
    client.set_nonblocking(false)?;
    let mut to_client = client.try_clone()?;
    let mut from_client = BufReader::new(client);
    let mut upstream: Option<(TcpStream, BufReader<TcpStream>)> = None;
    let close = |c: &TcpStream| {
        let _ = c.shutdown(Shutdown::Both);
    };
    loop {
        let Some(mut req) = read_head(&mut from_client)? else {
            return Ok(());
        };
        // `Expect: 100-continue` would make the client wait for an interim
        // answer before sending the body; answer it here and drop the
        // header, which SigV4 does not sign.
        if req
            .header("expect")
            .is_some_and(|v| v.eq_ignore_ascii_case("100-continue"))
        {
            to_client.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
            req = strip_expect(req);
        }
        let body = read_body(&mut from_client, &req, false)?;
        if !wait_unstalled(shared) {
            close(&to_client);
            return Ok(());
        }
        let (action, delay) = choose(shared);
        if !delay.is_zero() {
            thread::sleep(delay);
        }
        match action {
            Action::SlowDown => {
                to_client.write_all(&error_answer(503, "Slow Down", "SlowDown"))?;
                continue;
            }
            Action::InternalError => {
                to_client.write_all(&error_answer(
                    500,
                    "Internal Server Error",
                    "InternalError",
                ))?;
                continue;
            }
            Action::CloseBefore => {
                close(&to_client);
                return Ok(());
            }
            _ => {}
        }
        let is_head = req.start_line.starts_with("HEAD ");
        // A reused keep-alive connection the store has closed in the
        // meantime fails before any answer; that is not an injected fault,
        // so reconnect once and resend.
        let reused = upstream.is_some();
        let answer = match exchange(&mut upstream, shared.upstream, &req, &body, is_head) {
            Ok(Some(a)) => Some(a),
            _ if reused => {
                upstream = None;
                exchange(&mut upstream, shared.upstream, &req, &body, is_head)?
            }
            Ok(None) => None,
            Err(e) => return Err(e),
        };
        let Some((resp, resp_body, bodyless)) = answer else {
            close(&to_client);
            return Ok(());
        };
        match action {
            Action::CloseAfter => {
                close(&to_client);
                return Ok(());
            }
            Action::Truncate if resp_body.len() >= 2 => {
                lock(&shared.state).counts.truncated += 1;
                to_client.write_all(&resp.raw)?;
                to_client.write_all(&resp_body[..resp_body.len() / 2])?;
                to_client.flush()?;
                close(&to_client);
                return Ok(());
            }
            _ => {}
        }
        to_client.write_all(&resp.raw)?;
        to_client.write_all(&resp_body)?;
        to_client.flush()?;
        let framed = bodyless || resp.chunked() || resp.content_length()?.is_some();
        if req.closes() || resp.closes() || !framed {
            close(&to_client);
            return Ok(());
        }
    }
}

/// Sends one request upstream (connecting if needed) and reads its answer:
/// the head, the raw body, and whether the answer has no body. `None` when
/// the store closed the connection before answering.
fn exchange(
    upstream: &mut Option<(TcpStream, BufReader<TcpStream>)>,
    addr: SocketAddr,
    req: &Head,
    body: &[u8],
    is_head: bool,
) -> io::Result<Option<(Head, Vec<u8>, bool)>> {
    if upstream.is_none() {
        let s = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
        let r = BufReader::new(s.try_clone()?);
        *upstream = Some((s, r));
    }
    let (up_w, up_r) = upstream.as_mut().expect("connected above");
    up_w.write_all(&req.raw)?;
    up_w.write_all(body)?;
    // Skip interim (1xx) answers from the store.
    let resp = loop {
        let Some(h) = read_head(up_r)? else {
            return Ok(None);
        };
        let code = status_code(&h);
        if (100..200).contains(&code) && code != 101 {
            continue;
        }
        break h;
    };
    let code = status_code(&resp);
    let bodyless = is_head || code == 204 || code == 304;
    let resp_body = if bodyless {
        Vec::new()
    } else {
        read_body(up_r, &resp, true)?
    };
    Ok(Some((resp, resp_body, bodyless)))
}

fn status_code(h: &Head) -> u16 {
    h.start_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

fn strip_expect(head: Head) -> Head {
    let mut raw = format!("{}\r\n", head.start_line);
    let headers: Vec<_> = head
        .headers
        .into_iter()
        .filter(|(k, _)| !k.eq_ignore_ascii_case("expect"))
        .collect();
    for (k, v) in &headers {
        raw.push_str(&format!("{k}: {v}\r\n"));
    }
    raw.push_str("\r\n");
    Head {
        raw: raw.into_bytes(),
        start_line: head.start_line,
        headers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tiny HTTP/1.1 store: `PUT` remembers the body, `GET` returns it
    /// (chunked when the path ends in `chunked`), `HEAD` its length.
    fn store() -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let data = Arc::new(Mutex::new(Vec::<u8>::new()));
        let s2 = Arc::clone(&seen);
        thread::spawn(move || {
            for c in l.incoming() {
                let Ok(c) = c else { continue };
                let (seen, data) = (Arc::clone(&s2), Arc::clone(&data));
                thread::spawn(move || {
                    let mut w = c.try_clone().unwrap();
                    let mut r = BufReader::new(c);
                    while let Ok(Some(h)) = read_head(&mut r) {
                        let body = read_body(&mut r, &h, false).unwrap();
                        lock(&seen).push(h.start_line.clone());
                        let answer = if h.start_line.starts_with("PUT ") {
                            *lock(&data) = body;
                            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
                                .as_bytes()
                                .to_vec()
                        } else if h.start_line.starts_with("HEAD ") {
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                                lock(&data).len()
                            )
                            .into_bytes()
                        } else if h.start_line.contains("chunked") {
                            let d = lock(&data).clone();
                            let mut a =
                                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n".to_vec();
                            a.extend(format!("{:x}\r\n", d.len()).as_bytes());
                            a.extend(&d);
                            a.extend(b"\r\n0\r\n\r\n");
                            a
                        } else {
                            let d = lock(&data).clone();
                            let mut a =
                                format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", d.len())
                                    .into_bytes();
                            a.extend(d);
                            a
                        };
                        w.write_all(&answer).unwrap();
                    }
                });
            }
        });
        (addr, seen)
    }

    /// Sends one request on a fresh connection; returns the status code
    /// and body, or an error if the connection closed early.
    fn request(addr: SocketAddr, req: &str) -> io::Result<(u16, Vec<u8>)> {
        let s = TcpStream::connect(addr)?;
        s.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut w = s.try_clone()?;
        w.write_all(req.as_bytes())?;
        let mut r = BufReader::new(s);
        let head = read_head(&mut r)?.ok_or(io::ErrorKind::UnexpectedEof)?;
        let body = if req.starts_with("HEAD ") {
            Vec::new()
        } else {
            read_body(&mut r, &head, true)?
        };
        if let Some(n) = head.content_length()?
            && !req.starts_with("HEAD ")
            && n != body.len()
        {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        Ok((status_code(&head), body))
    }

    const PUT: &str = "PUT /b/k HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello";

    #[test]
    fn forwards_keep_alive_requests_and_both_body_framings() {
        let (store, _) = store();
        let p = S3Proxy::start(store, 1).unwrap();
        let s = TcpStream::connect(p.addr()).unwrap();
        let mut w = s.try_clone().unwrap();
        let mut r = BufReader::new(s);
        w.write_all(PUT.as_bytes()).unwrap();
        let h = read_head(&mut r).unwrap().unwrap();
        assert_eq!(status_code(&h), 200);
        read_body(&mut r, &h, false).unwrap();
        w.write_all(b"GET /b/chunked HTTP/1.1\r\nHost: x\r\n\r\n")
            .unwrap();
        let h = read_head(&mut r).unwrap().unwrap();
        let raw = read_body(&mut r, &h, false).unwrap();
        assert_eq!(raw, b"5\r\nhello\r\n0\r\n\r\n");
        assert_eq!(
            request(p.addr(), "HEAD /b/k HTTP/1.1\r\nHost: x\r\n\r\n")
                .unwrap()
                .0,
            200
        );
    }

    #[test]
    fn injects_errors_without_forwarding() {
        let (store, seen) = store();
        let p = S3Proxy::start(store, 1).unwrap();
        p.set_faults(S3Faults {
            slow_down: 1.0,
            ..S3Faults::NONE
        });
        let (code, body) = request(p.addr(), PUT).unwrap();
        assert_eq!(code, 503);
        assert!(String::from_utf8_lossy(&body).contains("SlowDown"));
        assert!(lock(&seen).is_empty(), "the store never saw it");
        assert_eq!(p.counts().slow_down, 1);
    }

    #[test]
    fn close_after_lets_the_write_land_but_hides_the_answer() {
        let (store, seen) = store();
        let p = S3Proxy::start(store, 1).unwrap();
        p.set_faults(S3Faults {
            close_after: 1.0,
            ..S3Faults::NONE
        });
        assert!(request(p.addr(), PUT).is_err(), "no answer");
        assert_eq!(lock(&seen).as_slice(), ["PUT /b/k HTTP/1.1"]);
        p.heal();
        let (code, body) = request(p.addr(), "GET /b/k HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        assert_eq!(
            (code, body.as_slice()),
            (200, &b"hello"[..]),
            "the write landed"
        );
    }

    #[test]
    fn truncates_and_closes_before() {
        let (store, seen) = store();
        let p = S3Proxy::start(store, 1).unwrap();
        request(p.addr(), PUT).unwrap();
        p.set_faults(S3Faults {
            truncate: 1.0,
            ..S3Faults::NONE
        });
        assert!(request(p.addr(), "GET /b/k HTTP/1.1\r\nHost: x\r\n\r\n").is_err());
        assert_eq!(p.counts().truncated, 1);
        // An answer with no body cannot be cut: it goes through, uncounted.
        assert!(request(p.addr(), "HEAD /b/k HTTP/1.1\r\nHost: x\r\n\r\n").is_ok());
        assert_eq!(p.counts().truncated, 1, "nothing was truncated");
        p.set_faults(S3Faults {
            close_before: 1.0,
            ..S3Faults::NONE
        });
        let before = lock(&seen).len();
        assert!(request(p.addr(), PUT).is_err());
        assert_eq!(lock(&seen).len(), before, "never forwarded");
    }

    #[test]
    fn a_stall_holds_requests_until_healed() {
        let (store, _) = store();
        let p = S3Proxy::start(store, 1).unwrap();
        p.stall(true);
        let addr = p.addr();
        let t = thread::spawn(move || request(addr, PUT));
        thread::sleep(Duration::from_millis(300));
        assert!(!t.is_finished(), "held by the stall");
        p.heal();
        assert_eq!(t.join().unwrap().unwrap().0, 200);
        assert_eq!(p.counts().stalled, 1);
    }
}
