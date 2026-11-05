//! A receive-pack server on a throwaway SQLite database and local blob
//! store, served over HTTP on 127.0.0.1 for stock `git` to push to, plus
//! checks that read the stored state back the way a fetch would need it.

#![allow(dead_code, unreachable_pub)]

use std::collections::{BTreeSet, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use aspen_blob::LocalBlobStore;
use aspen_core::id::{ObjectId, PackId, RefVersionId};
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::PackRecord;
use aspen_core::refs::{Closure, RefVersion};
use aspen_core::repo::{ForcePolicy, Repo, RepoSettings, RepoSettingsBuilder};
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::receive::{ReceiveConfig, ReceivePack};

use aspen_testkit::git::isolated_git;

pub const MIB: u64 = 1 << 20;

pub fn config(dir: &Path) -> ReceiveConfig {
    let mut c = ReceiveConfig::new(
        dir.join("spool"),
        MaxObjectBytes::new(64 * MIB).unwrap(),
        MaxInflatedBytes::new(1024 * MIB).unwrap(),
    );
    c.thread_limit = Some(2);
    c
}

pub struct Server {
    pub dir: tempfile::TempDir,
    pub meta: SqlStore,
    pub blobs: LocalBlobStore,
    pub svc: Arc<ReceivePack<SqlStore, LocalBlobStore>>,
    pub repo: Repo,
    pub base: String,
    /// A working clone's home for `git`.
    pub home: PathBuf,
}

impl Server {
    pub async fn start() -> Self {
        Self::start_with(|_| {}, ForcePolicy::Branches).await
    }

    pub async fn start_with(tune: impl FnOnce(&mut ReceiveConfig), force: ForcePolicy) -> Self {
        Self::start_full(
            tune,
            force,
            serde_json::json!({ "anonymous": ["write:demo"] }),
        )
        .await
    }

    /// A server whose access policy is `policy` (aspen-auth's
    /// `PolicyConfig` as JSON), with default limits.
    pub async fn start_full(
        tune: impl FnOnce(&mut ReceiveConfig),
        force: ForcePolicy,
        policy: serde_json::Value,
    ) -> Self {
        Self::start_repo(tune, force, policy, |b| b).await
    }

    /// [`Server::start_full`], with `settings` applied to the repository's
    /// settings builder (after the force policy).
    pub async fn start_repo(
        tune: impl FnOnce(&mut ReceiveConfig),
        force: ForcePolicy,
        policy: serde_json::Value,
        tune_settings: impl FnOnce(RepoSettingsBuilder) -> RepoSettingsBuilder,
    ) -> Self {
        Self::start_limited(
            tune,
            force,
            policy,
            tune_settings,
            aspen_auth::LimitsConfig::default(),
        )
        .await
    }

    /// [`Server::start_repo`], with the access layer's rate limits and push
    /// quota set to `limits`.
    pub async fn start_limited(
        tune: impl FnOnce(&mut ReceiveConfig),
        force: ForcePolicy,
        policy: serde_json::Value,
        tune_settings: impl FnOnce(RepoSettingsBuilder) -> RepoSettingsBuilder,
        limits: aspen_auth::LimitsConfig,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
        let mut cfg = config(dir.path());
        tune(&mut cfg);
        let server = aspen_server::Server::new(meta.clone(), blobs.clone(), {
            let mut c = aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap());
            // Tests read the planner's counters over HTTP.
            c.metrics_on_git_listener = true;
            c
        })
        .with_receive_pack(cfg)
        .unwrap();
        let svc = server.receive_pack().unwrap().clone();
        let settings = RepoSettings::builder(
            RefName::new("refs/heads/main").unwrap(),
            RepoSettings::new(RefName::new("refs/heads/main").unwrap())
                .unwrap()
                .writable()
                .to_vec(),
        )
        .allow_force(force);
        let settings = tune_settings(settings).build().unwrap();
        let repo = meta
            .create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let policy: aspen_auth::PolicyConfig = serde_json::from_value(policy).unwrap();
        let access = Arc::new(aspen_server::access::Access::new(
            aspen_auth::AuthService::new(policy, aspen_auth::MemoryTokenStore::new()).unwrap(),
            aspen_auth::Limits::new(limits),
            meta.clone(),
            aspen_server::access::ClientAddr::Peer,
        ));
        let app = server.router(access);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        Self {
            meta,
            blobs,
            svc,
            repo,
            base: format!("http://{addr}"),
            home,
            dir,
        }
    }

    pub fn url(&self) -> String {
        format!("{}/demo.git", self.base)
    }

    pub fn scoped_url(&self, branch: &str) -> String {
        format!("{}/demo.git/-/refs/heads/{branch}", self.base)
    }

    /// A fresh working repository with `main` checked out.
    pub fn clone_dir(&self, name: &str) -> Work {
        let path = self.dir.path().join(name);
        std::fs::create_dir_all(&path).unwrap();
        let w = Work {
            path,
            home: self.home.clone(),
        };
        w.git(&["init", "-q", "--initial-branch=main"]);
        w
    }

    pub async fn current(&self, name: &str) -> Option<RefVersion> {
        self.meta
            .current_version(self.repo.id, &RefName::new(name).unwrap())
            .await
            .unwrap()
    }

    pub async fn live(&self, name: &str) -> RefVersion {
        let v = self.current(name).await.expect("ref exists");
        assert!(v.value.tip().is_some(), "{name} is live");
        v
    }

    /// Every version in `v`'s closure (`v` and its dependencies,
    /// transitively).
    pub async fn closure_versions(&self, v: &RefVersion) -> Vec<RefVersion> {
        let mut out = Vec::new();
        let mut queue = vec![v.id];
        let mut seen = HashSet::new();
        while let Some(id) = queue.pop() {
            if !seen.insert(id) {
                continue;
            }
            let v = self.meta.ref_version(id).await.unwrap().unwrap();
            queue.extend(v.deps.iter());
            out.push(v);
        }
        out
    }

    pub async fn pack_rows(&self, ids: &[PackId]) -> Vec<PackRecord> {
        self.meta.packs(ids).await.unwrap()
    }

    /// The stored bytes of a pack.
    pub fn pack_path(&self, row: &PackRecord) -> PathBuf {
        self.blobs.path_of(&row.keys().pack)
    }

    /// The objects of one stored pack, from `git index-pack` on the pack
    /// alone (so it must be self-contained) and `git verify-pack`.
    pub fn pack_objects(&self, row: &PackRecord) -> BTreeSet<ObjectId> {
        let scratch = tempfile::tempdir().unwrap();
        let p = scratch.path().join("p.pack");
        std::fs::copy(self.pack_path(row), &p).unwrap();
        git_ok(
            &self.home,
            scratch.path(),
            &["index-pack", "-o", "p.idx", "p.pack"],
        );
        let out = git_ok(&self.home, scratch.path(), &["verify-pack", "-v", "p.idx"]);
        out.lines()
            .filter_map(|l| l.split_whitespace().next())
            .filter_map(|h| ObjectId::parse_hex(h).ok())
            .collect()
    }

    /// The stored-pack invariants on one pack: indexes alone, no repeat,
    /// base-first (`aspen_pack::verify::verify_pack`).
    pub fn verify_stored(&self, row: &PackRecord) {
        let scratch = tempfile::tempdir().unwrap();
        aspen_pack::verify::verify_pack(
            &self.pack_path(row),
            scratch.path(),
            MaxObjectBytes::new(64 * MIB).unwrap(),
            MaxInflatedBytes::new(1024 * MIB).unwrap(),
        )
        .unwrap_or_else(|e| panic!("stored pack {} breaks an invariant: {e}", row.id));
    }

    /// The union of the objects of every pack in `v`'s closure.
    pub async fn closure_objects(&self, v: &RefVersion) -> BTreeSet<ObjectId> {
        let mut ids: Vec<PackId> = self
            .closure_versions(v)
            .await
            .iter()
            .filter_map(|v| v.value.pack())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        let mut all = BTreeSet::new();
        for row in self.pack_rows(&ids).await {
            self.verify_stored(&row);
            all.extend(self.pack_objects(&row));
        }
        all
    }

    /// Checks what a fetch of `name` relies on: the closure holds every
    /// object the tip reaches in `work` (connected), every stored pack is
    /// self-contained and base-first, and an `Exact` version's closure is
    /// exactly what the tip reaches.
    pub async fn check_ref(&self, name: &str, work: &Work) -> RefVersion {
        let v = self.live(name).await;
        let tip = v.value.tip().unwrap();
        let reach = work.objects(&tip.to_hex());
        let closure = self.closure_objects(&v).await;
        let lacking: Vec<_> = reach.difference(&closure).collect();
        assert!(lacking.is_empty(), "{name}: closure lacks {lacking:?}");
        if v.value.closure() == Some(Closure::Exact) {
            let extra: Vec<_> = closure.difference(&reach).collect();
            assert!(
                extra.is_empty(),
                "{name} is Exact but its closure holds {} objects its tip does not reach",
                extra.len()
            );
        }
        v
    }

    pub async fn version(&self, id: RefVersionId) -> RefVersion {
        self.meta.ref_version(id).await.unwrap().unwrap()
    }
}

/// A working repository.
pub struct Work {
    pub path: PathBuf,
    pub home: PathBuf,
}

impl Work {
    pub fn command(&self) -> Command {
        let mut c = isolated_git(&self.home);
        c.current_dir(&self.path);
        c
    }

    pub fn git(&self, args: &[&str]) -> String {
        git_ok(&self.home, &self.path, args)
    }

    /// Runs git; returns (success, stdout + stderr).
    pub fn try_git(&self, args: &[&str]) -> (bool, String) {
        let out = self.command().args(args).output().unwrap();
        let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&out.stderr));
        (out.status.success(), text)
    }

    /// Writes `path` and commits it.
    pub fn commit(&self, path: &str, content: &str, msg: &str) -> String {
        let p = self.path.join(path);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(p, content).unwrap();
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", msg]);
        self.rev("HEAD")
    }

    pub fn rev(&self, r: &str) -> String {
        self.git(&["rev-parse", r]).trim().to_owned()
    }

    /// Every object `rev` reaches.
    pub fn objects(&self, rev: &str) -> BTreeSet<ObjectId> {
        self.git(&["rev-list", "--objects", rev])
            .lines()
            .filter_map(|l| l.split_whitespace().next())
            .map(|h| ObjectId::parse_hex(h).unwrap())
            .collect()
    }
}

pub fn git_ok(home: &Path, dir: &Path, args: &[&str]) -> String {
    let out = isolated_git(home)
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?} failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Runs a blocking closure (a `git` command) without blocking the server.
pub async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

pub fn oid(hex: &str) -> ObjectId {
    ObjectId::parse_hex(hex.trim()).unwrap()
}

// ---------------------------------------------------------------------------
// Hand-built packs and raw push requests, for the shapes stock `git push`
// never sends (libgit2's all-REF_DELTA packs, forward bases, repeats).

/// An object for a hand-built pack.
#[derive(Debug, Clone)]
pub struct Obj {
    pub kind: &'static str,
    pub data: Vec<u8>,
}

impl Obj {
    pub fn blob(data: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: "blob",
            data: data.into(),
        }
    }

    pub fn tree(entries: &[(&str, &str, ObjectId)]) -> Self {
        let mut data = Vec::new();
        let mut entries = entries.to_vec();
        entries.sort_by(|a, b| a.1.cmp(b.1));
        for (mode, name, id) in entries {
            data.extend_from_slice(format!("{mode} {name}\0").as_bytes());
            data.extend_from_slice(id.as_bytes());
        }
        Self { kind: "tree", data }
    }

    pub fn commit(tree: ObjectId, parents: &[ObjectId], msg: &str, time: i64) -> Self {
        let mut s = format!("tree {tree}\n");
        for p in parents {
            s.push_str(&format!("parent {p}\n"));
        }
        s.push_str(&format!(
            "author A <a@aspen.invalid> {time} +0000\ncommitter A <a@aspen.invalid> {time} +0000\n\n{msg}\n"
        ));
        Self {
            kind: "commit",
            data: s.into_bytes(),
        }
    }

    pub fn id(&self) -> ObjectId {
        let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
        h.update(format!("{} {}\0", self.kind, self.data.len()).as_bytes());
        h.update(&self.data);
        ObjectId::from_slice(h.try_finalize().unwrap().as_slice()).unwrap()
    }

    fn type_code(&self) -> u8 {
        match self.kind {
            "commit" => 1,
            "tree" => 2,
            "blob" => 3,
            _ => 4,
        }
    }
}

/// One entry of a hand-built pack.
#[derive(Debug, Clone)]
pub enum Entry {
    Full(Obj),
    /// A REF_DELTA of `target` against `base` (an object anywhere).
    RefDelta {
        base: Obj,
        target: Obj,
    },
}

fn varint_header(code: u8, size: usize, out: &mut Vec<u8>) {
    let mut c = (code << 4) | (size & 15) as u8;
    let mut size = size >> 4;
    while size != 0 {
        out.push(c | 0x80);
        c = (size & 0x7f) as u8;
        size >>= 7;
    }
    out.push(c);
}

fn delta_size(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(b);
            break;
        }
        out.push(b | 0x80);
    }
}

/// A delta that copies `base` whole, then inserts what `target` adds after
/// it (targets built as `base + suffix`), or inserts `target` whole.
fn delta(base: &[u8], target: &[u8]) -> Vec<u8> {
    let mut d = Vec::new();
    delta_size(base.len(), &mut d);
    delta_size(target.len(), &mut d);
    let rest: &[u8] = if target.starts_with(base) && !base.is_empty() && base.len() < 0x10000 {
        // copy offset 0, size base.len()
        let size = base.len();
        let mut op = 0x80u8;
        let mut args = Vec::new();
        for i in 0..3 {
            let byte = ((size >> (8 * i)) & 0xff) as u8;
            if byte != 0 {
                op |= 1 << (4 + i);
                args.push(byte);
            }
        }
        d.push(op);
        d.extend(args);
        &target[base.len()..]
    } else {
        target
    };
    for chunk in rest.chunks(127) {
        d.push(chunk.len() as u8);
        d.extend_from_slice(chunk);
    }
    d
}

fn deflate(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut z = gix_zlib::stream::deflate::Write::new(&mut out, gix_zlib::Compression::DEFAULT);
    z.write_all(data).unwrap();
    z.flush().unwrap();
    drop(z);
    out
}

pub fn build_pack(entries: &[Entry]) -> Vec<u8> {
    let mut pack = Vec::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2u32.to_be_bytes());
    pack.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for e in entries {
        match e {
            Entry::Full(o) => {
                varint_header(o.type_code(), o.data.len(), &mut pack);
                pack.extend(deflate(&o.data));
            }
            Entry::RefDelta { base, target } => {
                let d = delta(&base.data, &target.data);
                varint_header(7, d.len(), &mut pack);
                pack.extend_from_slice(base.id().as_bytes());
                pack.extend(deflate(&d));
            }
        }
    }
    let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
    h.update(&pack);
    pack.extend_from_slice(h.try_finalize().unwrap().as_slice());
    pack
}

fn pkt(line: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(format!("{:04x}", line.len() + 4).as_bytes());
    out.extend_from_slice(line);
}

/// A receive-pack request body: commands (with `report-status` and
/// `side-band-64k` off, so the report is plain pkt-lines), then `pack`.
pub fn request(commands: &[(ObjectId, ObjectId, &str)], options: &[&str], pack: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    for (i, (old, new, name)) in commands.iter().enumerate() {
        let mut line = format!("{old} {new} {name}").into_bytes();
        if i == 0 {
            line.push(0);
            let mut caps = "report-status ofs-delta agent=aspen-test".to_owned();
            if !options.is_empty() {
                caps.push_str(" push-options");
            }
            line.extend_from_slice(caps.as_bytes());
        }
        line.push(b'\n');
        pkt(&line, &mut body);
    }
    body.extend_from_slice(b"0000");
    if !options.is_empty() {
        for o in options {
            pkt(format!("{o}\n").as_bytes(), &mut body);
        }
        body.extend_from_slice(b"0000");
    }
    body.extend_from_slice(pack);
    body
}

/// POSTs a raw receive-pack request with curl; returns the report lines.
pub async fn post(url: String, body: Vec<u8>) -> Vec<String> {
    let (status, lines) = post_with(url, body, &[]).await;
    assert_eq!(status, 200, "{lines:?}");
    lines
}

/// [`post`] with extra request `headers` (`Name: value`): returns the HTTP
/// status and the report lines (the response text as one line when the
/// status is not 200).
pub async fn post_with(url: String, body: Vec<u8>, headers: &[&str]) -> (u16, Vec<String>) {
    let headers: Vec<String> = headers.iter().map(|h| (*h).to_owned()).collect();
    blocking(move || {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("body");
        std::fs::write(&f, body).unwrap();
        let mut cmd = Command::new("curl");
        cmd.args(["-sS", "-X", "POST", "-w", "\n%{http_code}"])
            .args(["-H", "Content-Type: application/x-git-receive-pack-request"]);
        for h in &headers {
            cmd.args(["-H", h]);
        }
        let out = cmd
            .arg("--data-binary")
            .arg(format!("@{}", f.display()))
            .arg(format!("{url}/git-receive-pack"))
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "curl: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let split = out.stdout.iter().rposition(|&c| c == b'\n').unwrap();
        let status: u16 = std::str::from_utf8(&out.stdout[split + 1..])
            .unwrap()
            .parse()
            .unwrap();
        let body = &out.stdout[..split];
        if status != 200 {
            return (
                status,
                vec![String::from_utf8_lossy(body).trim_end().to_owned()],
            );
        }
        let mut lines = Vec::new();
        let mut b = body;
        while b.len() >= 4 {
            let n = usize::from_str_radix(std::str::from_utf8(&b[..4]).unwrap(), 16).unwrap();
            if n == 0 {
                lines.push("0000".to_owned());
                b = &b[4..];
                continue;
            }
            lines.push(String::from_utf8_lossy(&b[4..n]).trim_end().to_owned());
            b = &b[n..];
        }
        (status, lines)
    })
    .await
}

impl Server {
    /// Stored pack files of the repository, in the blob store.
    pub fn stored_pack_files(&self) -> usize {
        let dir = self
            .blobs
            .root()
            .join(format!("repos/{}/packs", self.repo.id));
        std::fs::read_dir(dir)
            .map(|d| {
                d.flatten()
                    .filter(|e| e.path().extension().is_some_and(|x| x == "pack"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// A working repository holding the objects of `pack` (indexed with
    /// `--fix-thin` against `base`, if given), for reachability checks.
    pub fn work_from_packs(&self, name: &str, packs: &[&[u8]]) -> Work {
        let w = self.clone_dir(name);
        for p in packs {
            let mut child = w
                .command()
                .args(["index-pack", "--stdin", "--fix-thin"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap();
            child.stdin.take().unwrap().write_all(p).unwrap();
            assert!(child.wait().unwrap().success());
        }
        w
    }
}
