//! What a push's graph step costs must not grow with trunk's history or
//! with how far trunk has moved since a writer's branch was based (node
//! 12c; decisions log, "12c: generation numbers").
//!
//! The push shape: many writers push to their own branches, each based on a
//! trunk tip that has since moved on, while trunk advances many times a
//! second. A plain-URL branch push depends on trunk (issue #203), so its
//! closure asks whether trunk's current tip is an ancestor of the writer's
//! new tip. Without generation numbers the ancestry walk answered "no" only
//! after painting down to the branch's base: about one `commit_index`
//! lookup per trunk landing since, on every push. With them, trunk's tip
//! has the larger generation and the answer needs no walk.
//!
//! The tests run Aspen on SQLite against a blob store that counts reads
//! (an object store's GETs) and can add a delay to each, and report per
//! push: GETs, the graph step's wall time, commits the ancestry walk
//! expanded, and metadata statements.

mod receive_support;

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_blob::LocalBlobStore;
use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_pack::MaxObjectBytes;
use aspen_server::receive::{ReceivePack, Timings};

use receive_support::{MIB, Work, blocking, config};

/// A local blob store that counts reads and adds `delay_ms` to each.
#[derive(Debug, Clone)]
struct Counting {
    inner: LocalBlobStore,
    delay_ms: Arc<AtomicU64>,
    gets: Arc<AtomicU64>,
}

impl BlobStore for Counting {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        self.inner.upload_file(key, path).await
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let d = self.delay_ms.load(Ordering::Relaxed);
        if d > 0 {
            tokio::time::sleep(Duration::from_millis(d)).await;
        }
        self.inner.read_range(key, range).await
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        self.inner.size(key).await
    }

    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    async fn presign_get(&self, key: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        self.inner.presign_get(key, ttl).await
    }
}

struct Harness {
    _dir: tempfile::TempDir,
    blobs: Counting,
    svc: Arc<ReceivePack<SqlStore, Counting>>,
    url: String,
    work: Work,
    /// Committer time of the next commit.
    time: u64,
}

impl Harness {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = Counting {
            inner: LocalBlobStore::open(dir.path().join("blobs")).unwrap(),
            delay_ms: Arc::default(),
            gets: Arc::default(),
        };
        let settings = RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap();
        meta.create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap();
        let home: PathBuf = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let mut cfg = config(&dir.path().join("instance"));
        cfg.max_ancestry_commits = 1_000_000;
        let server = aspen_server::Server::new(
            meta.clone(),
            blobs.clone(),
            aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap()),
        )
        .with_receive_pack(cfg)
        .unwrap();
        let svc = server.receive_pack().unwrap().clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let policy: aspen_auth::PolicyConfig =
            serde_json::from_value(serde_json::json!({ "anonymous": ["write:demo"] })).unwrap();
        let access = Arc::new(aspen_server::access::Access::new(
            aspen_auth::AuthService::new(policy, aspen_auth::MemoryTokenStore::new()).unwrap(),
            aspen_auth::Limits::new(aspen_auth::LimitsConfig::default()),
            meta.clone(),
            aspen_server::access::ClientAddr::Peer,
        ));
        let app = server.router(access);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let path = dir.path().join("w");
        std::fs::create_dir_all(&path).unwrap();
        let work = Work { path, home };
        work.git(&["init", "-q", "--initial-branch=main"]);
        Self {
            _dir: dir,
            blobs,
            svc,
            url: format!("http://{addr}/demo.git"),
            work,
            time: 1_700_000_000,
        }
    }

    /// `n` commits on `branch`, each changing one of 50 files; the first
    /// starts from `from` (a ref), else from the branch's tip, else (a new
    /// branch) from nothing.
    fn import(&mut self, branch: &str, from: Option<&str>, n: usize) {
        let exists = self
            .work
            .try_git(&[
                "rev-parse",
                "--verify",
                "-q",
                &format!("refs/heads/{branch}"),
            ])
            .0;
        let from = from.or(exists.then_some(branch));
        let mut s = Vec::new();
        for i in 0..n {
            self.time += 1;
            write!(
                s,
                "commit refs/heads/{branch}\ncommitter A <a@a> {} +0000\ndata 2\nc\n",
                self.time
            )
            .unwrap();
            // Later commits of the stream continue the branch.
            if let (0, Some(f)) = (i, from) {
                writeln!(s, "from refs/heads/{f}^0").unwrap();
            }
            let body = format!("{branch} {}\n", self.time);
            write!(
                s,
                "M 100644 inline {branch}/f{}.txt\ndata {}\n{body}\n",
                i % 50,
                body.len()
            )
            .unwrap();
        }
        let mut child = self
            .work
            .command()
            .args(["fast-import", "--quiet", "--done"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(&s).unwrap();
        stdin.write_all(b"done\n").unwrap();
        drop(stdin);
        assert!(child.wait().unwrap().success(), "fast-import failed");
    }

    async fn push(&self, branch: &str) -> Sample {
        let before = self.svc.timings();
        let gets = self.blobs.gets.load(Ordering::SeqCst);
        let mut cmd = self.work.command();
        cmd.args(["push", "--porcelain", &self.url, branch]);
        let (ok, out) = blocking(move || {
            let o = cmd.output().unwrap();
            let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
            t.push_str(&String::from_utf8_lossy(&o.stderr));
            (o.status.success(), t)
        })
        .await;
        assert!(ok, "push {branch}: {out}");
        let after = self.svc.timings();
        Sample::of(
            &before,
            &after,
            self.blobs.gets.load(Ordering::SeqCst) - gets,
        )
    }
}

/// One push's costs.
#[derive(Debug, Clone, Copy, Default)]
struct Sample {
    gets: f64,
    graph_ms: f64,
    ancestry_commits: f64,
    statements: f64,
}

impl Sample {
    fn of(before: &Timings, after: &Timings, gets: u64) -> Self {
        assert_eq!(after.pushes - before.pushes, 1, "one push");
        Self {
            gets: gets as f64,
            graph_ms: (after.graph - before.graph).as_secs_f64() * 1e3,
            ancestry_commits: (after.ancestry_commits - before.ancestry_commits) as f64,
            statements: (after.db_queries - before.db_queries) as f64,
        }
    }

    fn mean(samples: &[Sample]) -> Self {
        let n = samples.len() as f64;
        let sum = |f: fn(&Sample) -> f64| samples.iter().map(f).sum::<f64>() / n;
        Self {
            gets: sum(|s| s.gets),
            graph_ms: sum(|s| s.graph_ms),
            ancestry_commits: sum(|s| s.ancestry_commits),
            statements: sum(|s| s.statements),
        }
    }
}

impl std::fmt::Display for Sample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:5.1} GETs {:6.1} ms graph {:5.1} commits {:5.1} stmts",
            self.gets, self.graph_ms, self.ancestry_commits, self.statements
        )
    }
}

/// Per-push costs of one scenario.
struct Report {
    history: usize,
    trunk: Sample,
    writer_early: Sample,
    writer_late: Sample,
}

/// Trunk gets `history` commits in one push. The writer's branch `w1` is
/// based on that tip and pushed once. Trunk then lands `moves` single
/// commits, and `rounds` rounds follow of one trunk landing and one writer
/// push of one commit, so by round `r` trunk has moved `moves + r` commits
/// past the writer's base. Every measured push reads the blob store with
/// `delay_ms` added.
async fn scenario(history: usize, moves: usize, rounds: usize, delay_ms: u64) -> Report {
    let mut h = Harness::start().await;
    h.import("main", None, history);
    h.push("main").await;
    h.import("w1", Some("main"), 1);
    h.push("w1").await;
    h.blobs.delay_ms.store(delay_ms, Ordering::Relaxed);
    let mut trunk = Vec::new();
    for _ in 0..moves {
        h.import("main", None, 1);
        trunk.push(h.push("main").await);
    }
    let mut writer = Vec::new();
    for _ in 0..rounds {
        h.import("main", None, 1);
        trunk.push(h.push("main").await);
        h.import("w1", None, 1);
        writer.push(h.push("w1").await);
    }
    let third = (rounds / 3).max(1);
    Report {
        history,
        trunk: Sample::mean(&trunk),
        writer_early: Sample::mean(&writer[..third]),
        writer_late: Sample::mean(&writer[rounds - third..]),
    }
}

fn table(reports: &[Report]) -> String {
    let mut out = String::new();
    for r in reports {
        writeln!(
            out,
            "H={:<5} trunk        {}\n        writer early {}\n        writer late  {}",
            r.history, r.trunk, r.writer_early, r.writer_late
        )
        .unwrap();
    }
    out
}

/// Invariant: a writer's push to its own branch, while trunk lands commit
/// after commit, expands no more ancestry commits and sends no more
/// statements after trunk has moved 25 commits past the branch's base than
/// after 5 (it used to walk down to the base: about two commits per
/// landing since).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_writer_push_costs_the_same_however_far_trunk_moved() {
    let r = scenario(50, 5, 21, 0).await;
    let report = table(std::slice::from_ref(&r));
    println!("{report}");
    assert!(
        r.writer_late.ancestry_commits <= 4.0,
        "a writer push walked {} commits on average once trunk had moved 19 to 26 \
         commits past its base:\n{report}",
        r.writer_late.ancestry_commits
    );
    assert!(
        r.writer_late.ancestry_commits <= r.writer_early.ancestry_commits + 1.0,
        "ancestry commits per writer push grew as trunk moved:\n{report}"
    );
    assert!(
        r.writer_late.statements <= r.writer_early.statements + 1.0,
        "statements per writer push grew as trunk moved:\n{report}"
    );
}

/// The measurement behind the 12c decision: several trunk history depths,
/// 30 trunk landings after the writer's base, then 30 rounds of a trunk
/// landing and a writer push, with 20 ms added to every blob read. Prints
/// per-push GETs, graph time, ancestry commits and statements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement, about a minute; run with --ignored --nocapture"]
async fn graph_cost_per_push_at_several_history_depths() {
    let mut reports = Vec::new();
    for history in [200, 2_000, 5_000] {
        reports.push(scenario(history, 30, 30, 20).await);
    }
    println!("{}", table(&reports));
    for r in &reports {
        assert!(r.writer_late.ancestry_commits <= 4.0, "{}", table(&reports));
    }
}
