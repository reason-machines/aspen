//! v0 upload-pack (`aspen_proto::upload_v0`) against real clients.
//!
//! The server side here speaks only through `aspen-proto`; storage is a
//! stock bare repository, as in the rest of this harness (`git cat-file`
//! decides which haves are shared, `git pack-objects` builds the pack).
//! It runs two ways:
//!
//! - **stateful**, as the `ext::` command of the harness (stock `git` with
//!   `protocol.version=0`), like SSH or `git://`;
//! - **stateless**, behind a small std-only smart-HTTP server in this
//!   process ([`HttpServer`]), like `aspen-server` (B2): every `POST` is
//!   parsed and answered on its own.
//!
//! Clients: stock `git` (always), libgit2 through pygit2 and go-git, which
//! have no protocol v2. The last two run when they are available
//! (`ASPEN_PYGIT2_PYTHON`, a Python that imports pygit2, default
//! `python3`; `ASPEN_GO_GIT_CLIENT`, the binary built from
//! `tests/clients/go-git`), and are skipped with a note otherwise, unless
//! `ASPEN_PROTO_REQUIRE_CLIENTS=1`. Their byte-level transcripts are
//! committed under `tests/transcripts/` and replayed through the parser and
//! writer by [`v0_transcripts_replay`], which always runs. Set
//! `ASPEN_PROTO_RECORD=<dir>` to record fresh ones.
//!
//! The server's `ready` policy is the simplest one B2 could use: ready as
//! soon as any have is shared (flag `never-ready` turns it off, so the
//! client negotiates to the end and sends `done`).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use aspen_core::error::{FetchError, ToWire};
use aspen_core::fetch::{Deepen, PlanMode, ShallowInfo};
use aspen_core::id::{ObjectId, PackId};
use aspen_core::name::RefName;
use aspen_core::repo::ShallowPolicy;
use aspen_proto::caps::Agent;
use aspen_proto::pkt::{self, Packet, PktReader};
use aspen_proto::service::{self, ProtocolVersion, Service, UploadVersion, V0Version};
use aspen_proto::upload_v0::{
    AckMode, Answered, Header, Negotiating, RequestParser, Transport, V0Advertisement, V0Head,
    V0Limits, V0Ref, write_advertisement,
};
use aspen_proto::v2::FetchArgs;

use super::{Scenario, World, oid, send};

// ---------------------------------------------------------------- server

/// The v0 advertisement of the server repository: `HEAD` (pointing at the
/// branch `HEAD` names) and every ref, tags peeled.
fn advertisement(sc: &Scenario, version: V0Version) -> Vec<u8> {
    let head = sc
        .git_out(&["rev-parse", "--verify", "-q", "HEAD"])
        .zip(sc.git_out(&["symbolic-ref", "HEAD"]))
        .map(|(tip, target)| V0Head {
            tip: oid(&tip),
            target: RefName::new(target.trim()).unwrap(),
        });
    let list = sc
        .git_out(&[
            "for-each-ref",
            "--format=%(objectname) %(refname) %(*objectname)",
        ])
        .unwrap();
    let refs: Vec<V0Ref> = list
        .lines()
        .map(|line| {
            let mut f = line.split(' ');
            let (tip, name, peeled) =
                (f.next().unwrap(), f.next().unwrap(), f.next().unwrap_or(""));
            V0Ref {
                name: RefName::new(name).unwrap(),
                tip: oid(tip),
                peeled: (!peeled.is_empty()).then(|| oid(peeled)),
            }
        })
        .collect();
    let mut wire = Vec::new();
    write_advertisement(
        &mut wire,
        version,
        &V0Advertisement::new(Agent::aspen()),
        head.as_ref(),
        &refs,
    )
    .unwrap();
    wire
}

/// The commits among `haves` that the server repository has.
fn existing(sc: &Scenario, haves: &[ObjectId]) -> Vec<ObjectId> {
    if haves.is_empty() {
        return Vec::new();
    }
    let mut child = sc
        .git()
        .args(["cat-file", "--batch-check=%(objectname) %(objecttype)"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let input: String = haves.iter().map(|h| format!("{h}\n")).collect();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .filter_map(|l| l.strip_suffix(" commit").map(oid))
        .collect()
}

/// `shallow-info` for `deepen <n>` from the wants: the commits at depth `n`
/// that have parents, and every client shallow commit the new depth reaches
/// above that boundary, as unshallow.
fn shallow_info(sc: &Scenario, args: &FetchArgs, depth: u32) -> ShallowInfo {
    let r = &args.request;
    let mut cmd = sc.git();
    cmd.args(["rev-list", "--parents"]);
    cmd.args(r.wants.iter().map(ObjectId::to_hex));
    let out = cmd.output().unwrap();
    let parents: HashMap<ObjectId, Vec<ObjectId>> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| {
            let mut ids = l.split(' ').map(oid);
            (ids.next().unwrap(), ids.collect())
        })
        .collect();
    let mut at: HashMap<ObjectId, u32> = HashMap::new();
    let mut frontier: Vec<ObjectId> = r.wants.clone();
    let mut d = 1;
    while !frontier.is_empty() && d <= depth {
        let mut next = Vec::new();
        for c in frontier {
            if at.contains_key(&c) {
                continue;
            }
            at.insert(c, d);
            if d < depth {
                next.extend(parents.get(&c).into_iter().flatten().copied());
            }
        }
        frontier = next;
        d += 1;
    }
    let has_parents = |c: &ObjectId| parents.get(c).is_some_and(|p| !p.is_empty());
    let mut info = ShallowInfo::default();
    let mut boundary: Vec<ObjectId> = at
        .iter()
        .filter(|(c, d)| **d == depth && has_parents(c))
        .map(|(c, _)| *c)
        .collect();
    boundary.sort();
    info.shallow = boundary
        .into_iter()
        .filter(|c| !r.client_shallow.contains(c))
        .collect();
    info.unshallow = r
        .client_shallow
        .iter()
        .copied()
        .filter(|c| at.get(c).is_some_and(|d| *d < depth) && has_parents(c))
        .collect();
    info
}

fn pack_objects(sc: &Scenario, args: &FetchArgs, mode: PlanMode) -> Vec<u8> {
    let mut revs: Vec<String> = args.request.wants.iter().map(ObjectId::to_hex).collect();
    if mode == PlanMode::Incremental {
        revs.extend(args.request.haves.iter().map(|h| format!("^{h}")));
    }
    let mut cmd = sc.git();
    cmd.args(["pack-objects", "--revs", "--stdout", "-q"]);
    if args.request.ofs_delta {
        cmd.arg("--delta-base-offset");
    }
    if args.include_tag {
        cmd.arg("--include-tag");
    }
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all((revs.join("\n") + "\n").as_bytes())
        .unwrap();
    let pack = child.wait_with_output().unwrap();
    assert!(pack.status.success());
    pack.stdout
}

/// Where a response goes, counting what was sent before the pack.
struct Out<'a> {
    sink: &'a mut dyn FnMut(&[u8]),
    written: usize,
}

impl Out<'_> {
    fn send(&mut self, buf: &mut Vec<u8>) {
        self.written += buf.len();
        (self.sink)(buf);
        buf.clear();
    }
}

/// How a request ended, for the transcript manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// A flush with no wants.
    End,
    /// A stateless request that was only the header (a deepen fetch's
    /// first request), answered with the shallow section alone.
    HeaderOnly,
    /// A stateless round answered without the pack.
    Round,
    /// The pack was sent; the bytes before it were negotiation.
    Pack { negotiation: usize },
    /// The request was refused with `ERR`.
    Err,
}

impl Ending {
    fn name(self) -> &'static str {
        match self {
            Self::End => "end",
            Self::HeaderOnly => "header",
            Self::Round => "round",
            Self::Pack { .. } => "pack",
            Self::Err => "err",
        }
    }
}

/// Serves one request (stateless) or one whole exchange (stateful) after
/// the advertisement.
fn serve_request<R: Read>(
    sc: &Scenario,
    reader: &mut PktReader<R>,
    t: Transport,
    sink: &mut dyn FnMut(&[u8]),
) -> Ending {
    let mut out = Out { sink, written: 0 };
    let mut buf = Vec::new();
    let mut parser = RequestParser::new(t, V0Limits::default());
    let req = loop {
        let p = reader.read_packet().unwrap().expect("request ended early");
        match parser.packet(p) {
            Ok(None) => {}
            Ok(Some(Header::End)) => return Ending::End,
            Ok(Some(Header::Fetch(r))) => break r,
            Err(e) => {
                pkt::write_err(&mut buf, &e.to_string());
                out.send(&mut buf);
                return Ending::Err;
            }
        }
    };
    let policy = if sc.has("shallow-refuse") {
        ShallowPolicy::Refuse
    } else {
        ShallowPolicy::Serve
    };
    let mode = match req.args().request.plan_mode(policy) {
        Ok(m) => m,
        Err(e) => {
            req.error(&mut buf, &e.to_wire()).unwrap();
            out.send(&mut buf);
            return Ending::Err;
        }
    };
    let info = match &req.args().request.deepen {
        None => None,
        Some(Deepen::Depth(d)) if !req.args().request.deepen_relative => {
            Some(shallow_info(sc, req.args(), d.get()))
        }
        Some(_) => {
            let e = FetchError::Unsupported {
                feature: aspen_core::error::Unsupported::Shallow,
                message: "this test server serves `deepen <n>` only".into(),
            };
            req.error(&mut buf, &e.to_wire()).unwrap();
            out.send(&mut buf);
            return Ending::Err;
        }
    };
    let mut neg = req.start(&mut buf, info.as_ref()).unwrap();
    if t == Transport::Stateful {
        // A stateful client reads the shallow section before it sends haves.
        out.send(&mut buf);
    }
    let (args, mut resp) = loop {
        let Some(p) = reader.read_packet().unwrap() else {
            return match neg.end_of_input() {
                Ok(()) => {
                    out.send(&mut buf);
                    Ending::HeaderOnly
                }
                Err(e) => {
                    pkt::write_err(&mut buf, &e.to_string());
                    out.send(&mut buf);
                    Ending::Err
                }
            };
        };
        neg = match neg.packet(p) {
            Ok(Negotiating::More(n)) => n,
            Ok(Negotiating::Round(r)) => {
                let common = existing(sc, r.haves());
                let ready =
                    !sc.has("never-ready") && (!common.is_empty() || !r.commons().is_empty());
                match r.answer(&mut buf, &common, ready).unwrap() {
                    Answered::More(n) => {
                        out.send(&mut buf);
                        n
                    }
                    Answered::ResponseEnd => {
                        out.send(&mut buf);
                        return Ending::Round;
                    }
                    Answered::Pack { args, response } => break (args, response),
                }
            }
            Err(e) => {
                pkt::write_err(&mut buf, &e.to_string());
                out.send(&mut buf);
                return Ending::Err;
            }
        };
    };
    out.send(&mut buf);
    let negotiation = out.written;
    let pack = pack_objects(sc, &args, mode);
    resp.progress(&mut buf, "aspen-proto test server: sending pack")
        .unwrap();
    if sc.has("fatal-mid-pack") {
        resp.pack_data(&mut buf, &pack[..12]).unwrap();
        let e = FetchError::StoredPackNotBaseFirst {
            pack: PackId::new(7).unwrap(),
            offset: 12,
        }
        .to_wire();
        assert!(resp.error(&mut buf, &e).unwrap());
    } else {
        resp.pack_data(&mut buf, &pack).unwrap();
        resp.finish(&mut buf).unwrap();
    }
    out.send(&mut buf);
    Ending::Pack { negotiation }
}

/// The `ext::` server for a stateful v0 fetch (flag `upload-v0`).
pub(super) fn serve_stateful(sc: &Scenario, out: &mut impl Write) {
    let version = if sc.has("v1") {
        V0Version::V1
    } else {
        V0Version::V0
    };
    send(out, &advertisement(sc, version));
    let mut reader = PktReader::new(io::stdin());
    serve_request(sc, &mut reader, Transport::Stateful, &mut |b| send(out, b));
}

// ---------------------------------------------------------------- HTTP

/// Writes `NN-*.pkt` files and a `manifest.txt` line per HTTP request.
struct Recorder {
    dir: PathBuf,
    seq: usize,
}

impl Recorder {
    fn new(dir: PathBuf) -> Self {
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self { dir, seq: 0 }
    }

    fn record(&mut self, kind: &str, files: &[(&str, &[u8])], ending: &str) {
        self.seq += 1;
        for (name, bytes) in files {
            fs::write(self.dir.join(format!("{:02}-{name}.pkt", self.seq)), bytes).unwrap();
        }
        let mut m = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("manifest.txt"))
            .unwrap();
        writeln!(m, "{:02} {kind} {ending}", self.seq).unwrap();
    }
}

/// A std-only smart-HTTP server for v0 upload-pack, one request per
/// connection (`Connection: close`), for localhost tests only.
struct HttpServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl HttpServer {
    /// Serves `w`'s repository with `flags`; records into
    /// `$ASPEN_PROTO_RECORD/<record>` when that is set.
    fn start(w: &World, flags: &str, record: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let sc = Scenario {
            repo: w.repo.clone(),
            home: w.home.clone(),
            flags: flags
                .split(',')
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect(),
        };
        let mut recorder = std::env::var_os("ASPEN_PROTO_RECORD")
            .map(|d| Recorder::new(Path::new(&d).join(record)));
        let stopping = stop.clone();
        let thread = thread::spawn(move || {
            for stream in listener.incoming() {
                if stopping.load(Ordering::SeqCst) {
                    return;
                }
                let stream = stream.unwrap();
                if let Err(e) = handle(&sc, stream, recorder.as_mut()) {
                    eprintln!("test HTTP server: {e}");
                }
            }
        });
        Self {
            addr,
            stop,
            thread: Some(thread),
        }
    }

    fn url(&self) -> String {
        format!("http://{}/repo.git", self.addr)
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

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

fn read_request(r: &mut impl BufRead) -> io::Result<Option<Request>> {
    let mut line = String::new();
    if r.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut req = Request {
        method: method.to_owned(),
        path: path.to_owned(),
        query: query.to_owned(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    loop {
        let mut h = String::new();
        r.read_line(&mut h)?;
        let h = h.trim_end_matches(['\r', '\n']);
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            req.headers.push((k.trim().to_owned(), v.trim().to_owned()));
        }
    }
    if req
        .header("Transfer-Encoding")
        .is_some_and(|v| v.eq_ignore_ascii_case("chunked"))
    {
        loop {
            let mut size = String::new();
            r.read_line(&mut size)?;
            let size = usize::from_str_radix(size.trim().split(';').next().unwrap_or(""), 16)
                .map_err(|_| io::Error::other("bad chunk size"))?;
            if size == 0 {
                let mut trailer = String::new();
                while r.read_line(&mut trailer)? > 0 && !trailer.trim().is_empty() {
                    trailer.clear();
                }
                break;
            }
            let at = req.body.len();
            req.body.resize(at + size, 0);
            r.read_exact(&mut req.body[at..])?;
            let mut crlf = [0; 2];
            r.read_exact(&mut crlf)?;
        }
    } else if let Some(len) = req.header("Content-Length") {
        let len: usize = len.parse().map_err(|_| io::Error::other("bad length"))?;
        req.body = vec![0; len];
        r.read_exact(&mut req.body)?;
    }
    if req
        .header("Content-Encoding")
        .is_some_and(|v| v.contains("gzip"))
    {
        // git compresses request bodies over 1 KiB.
        let mut gz = Command::new("gzip")
            .arg("-dc")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let mut stdin = gz.stdin.take().unwrap();
        let body = std::mem::take(&mut req.body);
        let writer = thread::spawn(move || stdin.write_all(&body));
        let out = gz.wait_with_output()?;
        writer.join().unwrap()?;
        req.body = out.stdout;
    }
    Ok(Some(req))
}

fn respond(stream: &mut TcpStream, status: &str, ctype: &str, body: &[u8]) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Cache-Control: no-cache\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    stream.shutdown(Shutdown::Both)
}

fn handle(sc: &Scenario, mut stream: TcpStream, rec: Option<&mut Recorder>) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let Some(req) = read_request(&mut reader)? else {
        return Ok(());
    };
    let requested = ProtocolVersion::requested(req.header("Git-Protocol"));
    let svc = Service::UploadPack;
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", p) if p.ends_with("/info/refs") && req.query == "service=git-upload-pack" => {
            let version = match UploadVersion::answering(requested) {
                UploadVersion::V0(v) => v,
                UploadVersion::V2 => {
                    return respond(&mut stream, "400 Bad Request", "text/plain", b"v0 only\n");
                }
            };
            let mut body = Vec::new();
            service::write_info_refs_header(&mut body, svc, version.into());
            body.extend_from_slice(&advertisement(sc, version));
            if let Some(r) = rec {
                r.record("get", &[("advertisement", &body)], "advertisement");
            }
            respond(
                &mut stream,
                "200 OK",
                svc.advertisement_content_type(),
                &body,
            )
        }
        ("POST", p) if p.ends_with("/git-upload-pack") => {
            let mut body = Vec::new();
            let mut pkts = PktReader::new(&req.body[..]);
            let ending = serve_request(sc, &mut pkts, Transport::Stateless, &mut |b| {
                body.extend_from_slice(b)
            });
            if let Some(r) = rec {
                let negotiation = match ending {
                    Ending::Pack { negotiation } => &body[..negotiation],
                    _ => &body[..],
                };
                r.record(
                    "post",
                    &[("request", &req.body), ("response", negotiation)],
                    ending.name(),
                );
            }
            respond(&mut stream, "200 OK", svc.result_content_type(), &body)
        }
        _ => respond(&mut stream, "404 Not Found", "text/plain", b"not found\n"),
    }
}

// ---------------------------------------------------------------- clients

/// A client the harness can drive: stock git, pygit2 or go-git.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Client {
    Git,
    Pygit2,
    GoGit,
}

impl Client {
    fn name(self) -> &'static str {
        match self {
            Self::Git => "git",
            Self::Pygit2 => "pygit2",
            Self::GoGit => "go-git",
        }
    }

    fn python() -> String {
        std::env::var("ASPEN_PYGIT2_PYTHON").unwrap_or_else(|_| "python3".into())
    }

    fn script() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/clients/pygit2_client.py")
    }

    /// Whether the client can run here; prints why not.
    fn available(self) -> bool {
        let (ok, why) = match self {
            Self::Git => (true, String::new()),
            Self::Pygit2 => {
                let ok = Command::new(Self::python())
                    .args(["-c", "import pygit2"])
                    .stderr(Stdio::null())
                    .status()
                    .is_ok_and(|s| s.success());
                (ok, format!("{} cannot import pygit2", Self::python()))
            }
            Self::GoGit => match std::env::var_os("ASPEN_GO_GIT_CLIENT") {
                Some(p) => (Path::new(&p).is_file(), format!("{p:?} is not a file")),
                None => (false, "ASPEN_GO_GIT_CLIENT is not set".into()),
            },
        };
        if !ok {
            assert!(
                std::env::var_os("ASPEN_PROTO_REQUIRE_CLIENTS").is_none(),
                "{} client required but unavailable: {why}",
                self.name()
            );
            println!("  skipped: {} client unavailable ({why})", self.name());
        }
        ok
    }

    /// Runs a clone or fetch; returns (success, stdout + stderr).
    fn run(self, w: &World, dir: &Path, args: &[&str]) -> (bool, String) {
        let out = match self {
            Self::Git => {
                let mut c = w.git(dir);
                c.args(["-c", "protocol.version=0"]).args(args);
                c.output().unwrap()
            }
            Self::Pygit2 => Command::new(Self::python())
                .arg(Self::script())
                .args(args)
                .current_dir(dir)
                .env("HOME", &w.home)
                .output()
                .unwrap(),
            Self::GoGit => Command::new(std::env::var_os("ASPEN_GO_GIT_CLIENT").unwrap())
                .args(args)
                .current_dir(dir)
                .env("HOME", &w.home)
                .output()
                .unwrap(),
        };
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        (out.status.success(), text)
    }

    fn clone(self, w: &World, url: &str, dest: &Path, depth: Option<u32>) -> (bool, String) {
        let d = depth.map(|d| d.to_string());
        let dest = dest.to_str().unwrap();
        match (self, d.as_deref()) {
            (Self::Git, None) => self.run(w, &w.root, &["clone", "-q", url, dest]),
            (Self::Git, Some(d)) => self.run(w, &w.root, &["clone", "-q", "--depth", d, url, dest]),
            (_, None) => self.run(w, &w.root, &["clone", url, dest]),
            (_, Some(d)) => self.run(w, &w.root, &["clone", url, dest, d]),
        }
    }

    fn fetch(self, w: &World, dir: &Path) -> (bool, String) {
        match self {
            Self::Git => self.run(w, dir, &["fetch", "-q", "origin"]),
            _ => self.run(w, &w.root, &["fetch", dir.to_str().unwrap()]),
        }
    }
}

/// Writes which client version made a recorded transcript.
fn note_client(record: &str, client: Client, output: &str) {
    let Some(root) = std::env::var_os("ASPEN_PROTO_RECORD") else {
        return;
    };
    let version = match client {
        Client::Git => {
            let out = Command::new("git").arg("--version").output().unwrap();
            String::from_utf8(out.stdout).unwrap()
        }
        Client::Pygit2 => output.lines().next().unwrap_or_default().to_owned(),
        Client::GoGit => {
            let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
            let gomod = fs::read_to_string(dir.join("tests/clients/go-git/go.mod")).unwrap();
            gomod
                .lines()
                .find(|l| l.contains("github.com/go-git/go-git/v5 "))
                .map(|l| format!("go-git {}", l.trim().rsplit(' ').next().unwrap()))
                .unwrap_or_default()
        }
    };
    fs::write(
        Path::new(&root).join(record).join("client.txt"),
        version.trim().to_owned() + "\n",
    )
    .unwrap();
}

fn assert_ok((ok, out): (bool, String), what: &str) -> String {
    assert!(ok, "{what} failed:\n{out}");
    out
}

/// Makes `n` commits on a local branch the server never sees, so a later
/// fetch sends haves the server does not share before reaching ones it
/// does: a real multi-round negotiation.
/// They are dated in the future, so every client's negotiator (newest
/// first) sends them before any commit the server has.
fn local_history(w: &World, dir: &Path, n: usize) {
    w.run(dir, &["checkout", "-q", "-b", "local"]);
    for i in 0..n {
        let file = format!("local{i}.txt");
        fs::write(dir.join(&file), &file).unwrap();
        w.run(dir, &["add", &file]);
        let date = format!("@{} +0000", 2_000_000_000 + i);
        let out = w
            .git(dir)
            .env("GIT_COMMITTER_DATE", &date)
            .env("GIT_AUTHOR_DATE", &date)
            .args(["commit", "-q", "-m", &file])
            .output()
            .unwrap();
        assert!(out.status.success());
    }
}

/// Clone, fetch after a negotiation, shallow clone and empty clone, all
/// over stateless HTTP, each checked with stock `git fsck --strict`.
fn http_scenarios(client: Client) {
    if !client.available() {
        return;
    }
    let name = client.name();
    let w = World::new(20, "");
    let server = HttpServer::start(&w, "", &format!("{name}-clone-fetch"));
    let clone = w.root.join("clone");
    let out = assert_ok(client.clone(&w, &server.url(), &clone, None), "clone");
    note_client(&format!("{name}-clone-fetch"), client, &out);
    let main = w.server_ref("refs/heads/main").unwrap();
    assert_eq!(w.run(&clone, &["rev-parse", "HEAD"]).trim(), main);
    w.fsck(&clone);
    // The client gets history the server never sees; the server moves on.
    local_history(&w, &clone, 20);
    let writer = w.work("writer");
    let tip = w.commit(&writer, "later1.txt");
    let tip2 = w.commit(&writer, "later2.txt");
    assert_ne!(tip, tip2);
    w.run(&writer, &["push", "-q", w.repo.to_str().unwrap(), "main"]);
    assert_ok(client.fetch(&w, &clone), "fetch");
    assert_eq!(
        w.run(&clone, &["rev-parse", "refs/remotes/origin/main"])
            .trim(),
        tip2
    );
    w.fsck(&clone);
    drop(server);

    let server = HttpServer::start(&w, "", &format!("{name}-shallow"));
    let shallow = w.root.join("shallow");
    let out = assert_ok(
        client.clone(&w, &server.url(), &shallow, Some(1)),
        "shallow clone",
    );
    note_client(&format!("{name}-shallow"), client, &out);
    assert_eq!(
        w.run(&shallow, &["rev-parse", "--is-shallow-repository"])
            .trim(),
        "true"
    );
    assert_eq!(
        w.run(&shallow, &["rev-list", "--count", "HEAD"]).trim(),
        "1"
    );
    w.fsck(&shallow);
    drop(server);

    let empty = World::new(0, "");
    let server = HttpServer::start(&empty, "", &format!("{name}-empty"));
    let dest = empty.root.join("clone");
    let (ok, out) = client.clone(&empty, &server.url(), &dest, None);
    match client {
        // go-git refuses an empty remote whatever the server, as it does
        // against stock `git http-backend`.
        Client::GoGit => assert!(!ok && out.contains("remote repository is empty"), "{out}"),
        _ => assert!(ok, "{out}"),
    }
}

/// Invariant: pygit2 (libgit2) clones, fetches after a multi-round
/// negotiation, clones shallow and clones an empty repository through v0
/// over stateless HTTP, with `fsck --strict` clean.
pub(super) fn v0_http_pygit2() {
    http_scenarios(Client::Pygit2);
}

/// Invariant: the same for go-git, which negotiates without `multi_ack`.
pub(super) fn v0_http_go_git() {
    http_scenarios(Client::GoGit);
}

fn trace(w: &World, dir: &Path, args: &[&str]) -> (bool, String, String) {
    let file = w.root.join("trace");
    let _ = fs::remove_file(&file);
    let out = w
        .git(dir)
        .env("GIT_TRACE_PACKET", &file)
        .args(args)
        .output()
        .unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        fs::read_to_string(&file).unwrap_or_default(),
    )
}

/// Stock git over v0, stateless HTTP or a stateful `ext::` connection:
/// clone, a fetch whose negotiation takes more than one round, ending
/// either with `ready` and `no-done` or (`never-ready`) with `done`.
fn git_negotiation(stateful: bool, flags: &str) {
    let ext_flags = if stateful {
        format!("upload-v0,{flags}")
    } else {
        String::new()
    };
    // 20 shared commits after 20 local ones: the second round (haves 17
    // to 32) holds both, and git still has haves left, so it ends that
    // round with a flush the server can answer `ready`.
    let w = World::new(20, &ext_flags);
    let record = if flags.is_empty() {
        "git-ready-no-done"
    } else {
        "git-never-ready"
    };
    let server = (!stateful).then(|| HttpServer::start(&w, flags, record));
    let url = server.as_ref().map_or_else(|| w.url(), HttpServer::url);
    let clone = w.root.join("clone");
    let (ok, err, t) = trace(
        &w,
        &w.root,
        &[
            "-c",
            "protocol.version=0",
            "clone",
            "-q",
            &url,
            clone.to_str().unwrap(),
        ],
    );
    assert!(ok, "{err}");
    assert!(!t.contains("version 2"), "{t}");
    w.fsck(&clone);
    local_history(&w, &clone, 20);
    let writer = w.work("writer");
    w.commit(&writer, "later.txt");
    let tip = w.commit(&writer, "later2.txt");
    w.run(&writer, &["push", "-q", w.repo.to_str().unwrap(), "main"]);
    let (ok, err, t) = trace(
        &w,
        &clone,
        &["-c", "protocol.version=0", "fetch", "-q", "origin"],
    );
    assert!(ok, "{err}");
    assert_eq!(
        w.run(&clone, &["rev-parse", "refs/remotes/origin/main"])
            .trim(),
        tip
    );
    w.fsck(&clone);
    let first_want = t.lines().find(|l| l.contains("> want ")).unwrap();
    assert!(
        first_want.contains("multi_ack_detailed") && first_want.contains("side-band-64k"),
        "{first_want}"
    );
    assert!(t.contains("< NAK"), "an unshared round is NAKed: {t}");
    assert!(t.contains(" common"), "shared haves are ACKed: {t}");
    let haves = t.lines().filter(|l| l.contains("> have ")).count();
    assert!(haves > 16, "more than one round of haves ({haves}): {t}");
    if flags.contains("never-ready") {
        assert!(!t.contains(" ready"), "{t}");
        assert!(t.contains("> done"), "the client ends with done: {t}");
    } else {
        assert!(t.contains(" ready"), "{t}");
        // On a connection git sends its next rounds (and `done`, once it
        // runs out of haves) before it reads the answers, so only the
        // stateless exchange shows that `no-done` saves the `done`.
        if !stateful {
            assert!(
                !t.contains("> done"),
                "no-done: the pack follows ready: {t}"
            );
        }
    }
}

/// Invariant: stock git negotiates over stateless HTTP v0 in several
/// rounds, the server's `ready` with `no-done` ends it without `done`, and
/// the fetch is complete.
pub(super) fn v0_http_git_negotiation_ready_no_done() {
    git_negotiation(false, "");
}

/// Invariant: without `ready`, stateless rounds go on until the client
/// sends `done`, answered by the final ACK.
pub(super) fn v0_http_git_negotiation_until_done() {
    git_negotiation(false, "never-ready");
}

/// Invariant: the same negotiation on one stateful connection.
pub(super) fn v0_stateful_git_negotiation() {
    git_negotiation(true, "");
    git_negotiation(true, "never-ready");
}

/// Invariant: shallow over v0, stateless and stateful: a depth-1 clone is
/// shallow with one commit, a deeper fetch moves the boundary (`shallow`
/// and `unshallow` lines), and `--unshallow` (`deepen 2147483647`) makes
/// the repository complete; `fsck --strict` passes after each.
pub(super) fn v0_git_shallow_deepen_unshallow() {
    for stateful in [false, true] {
        let w = World::new(4, if stateful { "upload-v0" } else { "" });
        let server = (!stateful).then(|| HttpServer::start(&w, "", "git-shallow"));
        let url = server.as_ref().map_or_else(|| w.url(), HttpServer::url);
        let clone = w.root.join("clone");
        let v0 = ["-c", "protocol.version=0"];
        let mut args = v0.to_vec();
        args.extend(["clone", "-q", "--depth", "1", "--no-local", &url]);
        args.push(clone.to_str().unwrap());
        let (ok, err, t) = trace(&w, &w.root, &args);
        assert!(ok, "{err}");
        assert!(t.contains("> deepen 1"), "{t}");
        assert!(t.contains("< shallow "), "{t}");
        let count = |w: &World| w.run(&clone, &["rev-list", "--count", "HEAD"]);
        assert_eq!(count(&w).trim(), "1");
        w.fsck(&clone);

        let mut args = v0.to_vec();
        args.extend(["fetch", "-q", "--depth", "3", "origin"]);
        let (ok, err, t) = trace(&w, &clone, &args);
        assert!(ok, "{err}");
        assert!(
            t.contains("< unshallow "),
            "the old boundary is lifted: {t}"
        );
        assert_eq!(count(&w).trim(), "3");
        w.fsck(&clone);

        let mut args = v0.to_vec();
        args.extend(["fetch", "-q", "--unshallow", "origin"]);
        let (ok, err, t) = trace(&w, &clone, &args);
        assert!(ok, "{err}");
        assert!(t.contains("> deepen 2147483647"), "{t}");
        assert_eq!(
            w.run(&clone, &["rev-parse", "--is-shallow-repository"])
                .trim(),
            "false"
        );
        assert_eq!(count(&w).trim(), "4");
        w.fsck(&clone);
        drop(server);
    }
}

/// Invariant: a v1 request gets `version 1` before the advertisement (and
/// the fetch then proceeds as v0), over HTTP and `ext::`; an empty
/// repository clones over v0.
pub(super) fn v0_git_v1_and_empty_repository() {
    let w = World::new(2, "");
    let server = HttpServer::start(&w, "", "git-v1");
    let clone = w.root.join("clone");
    let (ok, err, t) = trace(
        &w,
        &w.root,
        &[
            "-c",
            "protocol.version=1",
            "clone",
            "-q",
            &server.url(),
            clone.to_str().unwrap(),
        ],
    );
    assert!(ok, "{err}");
    assert!(t.contains("< version 1"), "{t}");
    w.fsck(&clone);

    let w = World::new(2, "upload-v0,v1");
    let clone = w.root.join("clone");
    let (ok, err, t) = trace(
        &w,
        &w.root,
        &[
            "-c",
            "protocol.version=1",
            "clone",
            "-q",
            &w.url(),
            clone.to_str().unwrap(),
        ],
    );
    assert!(ok, "{err}");
    assert!(t.contains("< version 1"), "{t}");

    let w = World::new(0, "");
    let server = HttpServer::start(&w, "", "git-empty");
    let clone = w.root.join("clone");
    let (ok, err) = Client::Git.clone(&w, &server.url(), &clone, None);
    assert!(ok, "{err}");
    assert!(err.contains("empty repository"), "{err}");
}

/// Invariant: errors are shown verbatim before the pack (`ERR`, printed as
/// `remote error: ...`) and inside it (band 3, printed as `remote: ...`,
/// and the fetch fails), over v0.
pub(super) fn v0_git_errors_are_shown() {
    let w = World::new(2, "");
    let server = HttpServer::start(&w, "shallow-refuse", "git-errors");
    let clone = w.root.join("clone");
    let (ok, err) = Client::Git.clone(&w, &server.url(), &clone, Some(1));
    assert!(!ok);
    assert!(
        err.contains(&format!(
            "remote error: {}",
            FetchError::shallow_refused().to_wire().message
        )),
        "{err}"
    );
    drop(server);
    let server = HttpServer::start(&w, "fatal-mid-pack", "git-errors-pack");
    let (ok, err) = Client::Git.clone(&w, &server.url(), &clone, None);
    assert!(!ok);
    assert!(
        err.contains("remote: stored pack 7 has a forward delta base at offset 12; retry"),
        "{err}"
    );
}

// ---------------------------------------------------------------- replay

fn packets(wire: &[u8]) -> Vec<Packet<'_>> {
    let mut out = Vec::new();
    let mut at = 0;
    while let Some((p, n)) = pkt::decode(&wire[at..]).unwrap() {
        out.push(p);
        at += n;
    }
    assert_eq!(at, wire.len(), "recorded bytes are whole pkt-lines");
    out
}

fn texts(wire: &[u8]) -> Vec<String> {
    packets(wire)
        .iter()
        .map(|p| match p.text() {
            Some(t) => String::from_utf8_lossy(t).into_owned(),
            None => "0000".into(),
        })
        .collect()
}

/// What each recorded client must have asked for, checked on every
/// request it made.
fn expected_caps(client: &str) -> (AckMode, bool) {
    match client {
        "git" => (AckMode::Detailed, true),
        "pygit2" => (AckMode::Detailed, false),
        "go-git" => (AckMode::Single, false),
        other => panic!("no expectations for client {other}"),
    }
}

/// Replays one recorded `GET info/refs` body: reads back the refs, `HEAD`'s
/// `symref` and the agent it shows, writes the advertisement for them with
/// the current code, and checks it is the same bytes. What it pins is the
/// encoding (service header, version line, capability list and order, the
/// NUL and `capabilities^{}` forms, peeled lines, the flush), since the
/// refs and agent come from the recording. The agent is taken as recorded
/// so a version bump does not invalidate the transcripts.
fn replay_advertisement(dir: &Path, seq: &str) {
    let recorded = fs::read(dir.join(format!("{seq}-advertisement.pkt"))).unwrap();
    let at = format!("{}/{seq}", dir.display());
    let lines = texts(&recorded);
    assert_eq!(
        lines[..2],
        ["# service=git-upload-pack", "0000"],
        "{at}: service header"
    );
    let mut body = &lines[2..];
    let version = match body.first().map(String::as_str) {
        Some("version 1") => {
            body = &body[1..];
            V0Version::V1
        }
        _ => V0Version::V0,
    };
    let first = body.first().unwrap_or_else(|| panic!("{at}: no ref lines"));
    let (_, caps) = first
        .trim_end_matches('\n')
        .split_once('\0')
        .unwrap_or_else(|| panic!("{at}: no capabilities on the first line"));
    let mut agent = None;
    let mut symref = None;
    for c in caps.split(' ') {
        if let Some(a) = c.strip_prefix("agent=") {
            agent = Some(a.to_owned());
        }
        if let Some(t) = c.strip_prefix("symref=HEAD:") {
            symref = Some(RefName::new(t).unwrap());
        }
    }
    let agent = agent.unwrap_or_else(|| panic!("{at}: no agent"));
    assert!(agent.starts_with("aspen/"), "{at}: recorded from Aspen");
    let mut head = None;
    let mut refs: Vec<V0Ref> = Vec::new();
    for l in body.iter().take_while(|l| *l != "0000") {
        let l = l.trim_end_matches('\n');
        let l = l.split_once('\0').map_or(l, |(l, _)| l);
        let (tip, name) = l.split_once(' ').unwrap();
        if name == "capabilities^{}" {
            continue;
        }
        if name == "HEAD" {
            head = Some(V0Head {
                tip: oid(tip),
                target: symref.clone().expect("HEAD shown with its symref"),
            });
        } else if let Some(base) = name.strip_suffix("^{}") {
            let last = refs.last_mut().unwrap();
            assert_eq!(
                last.name.as_str(),
                base,
                "{at}: peeled line follows its tag"
            );
            last.peeled = Some(oid(tip));
        } else {
            refs.push(V0Ref {
                name: RefName::new(name).unwrap(),
                tip: oid(tip),
                peeled: None,
            });
        }
    }
    let mut ours = Vec::new();
    service::write_info_refs_header(&mut ours, Service::UploadPack, version.into());
    write_advertisement(
        &mut ours,
        version,
        &V0Advertisement::new(Agent::new(agent).unwrap()),
        head.as_ref(),
        &refs,
    )
    .unwrap();
    assert_eq!(ours, recorded, "{at}: advertisement byte-exact");
}

/// Replays one recorded request: parses it, answers with the shares and
/// `ready` the recorded response shows, and checks the answer is the same
/// bytes and ends the same way.
fn replay(dir: &Path, seq: &str, ending: &str, client: &str) -> AckMode {
    let request = fs::read(dir.join(format!("{seq}-request.pkt"))).unwrap();
    let recorded = fs::read(dir.join(format!("{seq}-response.pkt"))).unwrap();
    let lines = texts(&recorded);
    let pks = packets(&request);
    let mut it = pks.into_iter();
    let mut parser = RequestParser::new(Transport::Stateless, V0Limits::default());
    let req = loop {
        match parser.packet(it.next().expect("header")).unwrap() {
            Some(Header::Fetch(r)) => break r,
            Some(Header::End) => panic!("{seq}: no wants"),
            None => {}
        }
    };
    let (ack, no_done) = expected_caps(client);
    assert_eq!(req.caps().ack, ack, "{}/{seq}", dir.display());
    assert_eq!(req.caps().no_done, no_done, "{}/{seq}", dir.display());
    assert!(
        req.caps().sideband.is_some(),
        "every client asks for side-band-64k"
    );
    let info = req.needs_shallow_info().then(|| {
        let mut info = ShallowInfo::default();
        for l in lines.iter().take_while(|l| *l != "0000") {
            match l.split_once(' ') {
                Some(("shallow", h)) => info.shallow.push(oid(h)),
                Some(("unshallow", h)) => info.unshallow.push(oid(h)),
                _ => panic!("{seq}: unexpected line {l:?} in the shallow section"),
            }
        }
        info
    });
    let acked: HashSet<ObjectId> = lines
        .iter()
        .filter_map(|l| l.strip_prefix("ACK "))
        .map(|rest| oid(rest.split(' ').next().unwrap()))
        .collect();
    let ready = lines.iter().any(|l| l.ends_with(" ready"));
    let mut out = Vec::new();
    let mut neg = req.start(&mut out, info.as_ref()).unwrap();
    let answered = loop {
        let Some(p) = it.next() else {
            neg.end_of_input().unwrap();
            assert_eq!(ending, "header", "{}/{seq}", dir.display());
            assert_eq!(out, recorded, "{}/{seq}: byte-exact", dir.display());
            return ack;
        };
        neg = match neg.packet(p).unwrap() {
            Negotiating::More(n) => n,
            Negotiating::Round(r) => {
                let common: Vec<ObjectId> = r
                    .haves()
                    .iter()
                    .copied()
                    .filter(|h| acked.contains(h))
                    .collect();
                break r.answer(&mut out, &common, ready).unwrap();
            }
        }
    };
    assert!(
        it.next().is_none(),
        "{seq}: a stateless request is one round"
    );
    let got = match answered {
        Answered::ResponseEnd => "round",
        Answered::Pack { .. } => "pack",
        Answered::More(_) => panic!("stateless"),
    };
    assert_eq!(got, ending, "{}/{seq}", dir.display());
    assert_eq!(
        texts(&out),
        lines,
        "{}/{seq}: the answer differs from the recorded one",
        dir.display()
    );
    assert_eq!(out, recorded, "{}/{seq}: byte-exact", dir.display());
    ack
}

/// Invariant: every recorded request from pygit2 (libgit2), go-git and
/// stock git over stateless HTTP parses, asks for the capabilities that
/// client is known to use, and gets the same answer, byte for byte, that
/// the live run sent and the client accepted.
pub(super) fn v0_transcripts_replay() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/transcripts");
    let mut dirs: Vec<PathBuf> = fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut adverts = 0;
    for dir in dirs {
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        let client = ["pygit2", "go-git", "git"]
            .into_iter()
            .find(|c| name.starts_with(&format!("{c}-")))
            .unwrap_or_else(|| panic!("transcript {name} names no known client"));
        let manifest = fs::read_to_string(dir.join("manifest.txt")).unwrap();
        for line in manifest.lines() {
            let mut f = line.split(' ');
            let (seq, kind, ending) = (f.next().unwrap(), f.next().unwrap(), f.next().unwrap());
            if kind == "get" {
                replay_advertisement(&dir, seq);
                adverts += 1;
            }
            if kind == "post" && ending != "err" {
                replay(&dir, seq, ending, client);
                *seen.entry(client.to_owned()).or_default() += 1;
            }
        }
    }
    for c in ["pygit2", "go-git", "git"] {
        assert!(
            seen.get(c).copied().unwrap_or(0) > 0,
            "no recorded requests from {c}"
        );
    }
    assert!(adverts > 0, "no recorded advertisements");
    println!("  replayed {adverts} advertisements and requests {seen:?}");
}
