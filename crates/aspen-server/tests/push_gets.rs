//! Blob store GETs per steady-state push, by who sends them (monorepo
//! benchmark, node 12a).
//!
//! On a 300,000-file, 500,000-commit repository on S3 (one c7g.2xlarge,
//! stock git, 30 acknowledged pushes a second), the bucket saw 17,900 GETs
//! a second against 87 PUTs: about 600 GETs per push. S3 bills per request
//! and each GET is a request the store must serve, so the question is
//! which part of Aspen sends them and whether that grows with the
//! repository.
//!
//! Every request now carries a caller label (`aspen_core::blob::BlobCaller`)
//! and is counted (`aspen_blob::RequestCounts`, exported at `/-/metrics` as
//! `aspen_blob_requests_total`). These tests push to a synthetic repository
//! through stock git, run compaction after every push as a worker that
//! keeps up does, and count the GETs of a window of steady-state pushes by
//! caller. Each run prints its breakdown per push. They run at two
//! repository sizes, the larger with 14 times the files and 3.5 times the
//! history, and check that GETs per push stay under a fixed bound at both.

mod receive_support;

use std::collections::BTreeMap;
use std::io::Write as _;
use std::process::Stdio;
use std::sync::Arc;

use aspen_blob::{Counted, LocalBlobStore, RequestCounts, RequestKind};
use aspen_compact::{CompactConfig, Compactor, JobOutcome, Worker, WorkerOptions};
use aspen_core::blob::{BlobCaller, BlobFile};
use aspen_core::meta::{CoverLayout, MetaStore as _};
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};

use receive_support::{MIB, Work, blocking, config};

type Blobs = Counted<LocalBlobStore>;

/// A synthetic repository: `fanout` subdirectories per directory for
/// `depth` levels, and `files` files in every directory at the bottom.
#[derive(Debug, Clone, Copy)]
struct Shape {
    fanout: usize,
    depth: u32,
    files: usize,
}

impl Shape {
    fn leaves(self) -> usize {
        self.fanout.pow(self.depth)
    }

    fn file_count(self) -> usize {
        self.leaves() * self.files
    }

    fn leaf_dir(self, mut n: usize) -> String {
        let mut parts = Vec::with_capacity(self.depth as usize);
        for level in 0..self.depth {
            parts.push(format!("d{level}x{}", n % self.fanout));
            n /= self.fanout;
        }
        parts.reverse();
        parts.join("/")
    }
}

/// 4,608 files, 8 levels deep.
const SMALL: Shape = Shape {
    fanout: 2,
    depth: 8,
    files: 18,
};

/// 65,610 files, 8 levels deep.
const LARGE: Shape = Shape {
    fanout: 3,
    depth: 8,
    files: 10,
};

/// About the monorepo benchmark's 300,000 files (nightly).
const HUGE: Shape = Shape {
    fanout: 3,
    depth: 8,
    files: 46,
};

/// How the server keeps `.idx` files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Idx {
    /// Whole (the default for a pack under about 2.4 million objects).
    Whole,
    /// Every `.idx` over this many bytes sampled, as the monorepo's
    /// 11.6-million-object import pack is by default.
    SampledOver(u64),
}

/// What the measured pushes do.
#[derive(Debug, Clone, Copy)]
enum Workload {
    /// One writer fast-forwards trunk.
    Trunk,
    /// The load test's cycle: trunk moves by one push; then one of
    /// `agents` clones fetches trunk and pushes a new branch with one
    /// commit on it through the plain URL. Counted per push (two a
    /// cycle), the fetch included.
    Agents { agents: usize },
}

/// Cycles measured: 16 trunk versions, one level-1 cover (fanout 16), so
/// the window holds exactly one level-1 `compact` job and its cost is
/// amortized as a steady push rate amortizes it.
const MEASURED: usize = 16;

struct Harness {
    dir: tempfile::TempDir,
    counts: Arc<RequestCounts>,
    url: String,
    home: std::path::PathBuf,
    worker: Worker<SqlStore, Blobs>,
}

impl Harness {
    async fn start(idx: Idx) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let counts = Arc::new(RequestCounts::default());
        let blobs = Counted::new(
            LocalBlobStore::open(dir.path().join("blobs")).unwrap(),
            counts.clone(),
        );
        let settings = RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap();
        meta.create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap();
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();

        // The server's defaults, but for the test's thread limit and, with
        // `Idx::SampledOver`, a small `.idx` limit standing in for a
        // monorepo's.
        let mut cfg = config(&dir.path().join("instance"));
        if let Idx::SampledOver(bytes) = idx {
            cfg.locate.idx_cache_bytes = 1 << 20;
            cfg.locate.max_whole_idx_bytes = bytes;
        }
        let server = aspen_server::Server::new(
            meta.clone(),
            blobs.clone(),
            aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap()),
        )
        .with_receive_pack(cfg)
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let policy: aspen_auth::PolicyConfig =
            serde_json::from_value(serde_json::json!({ "anonymous": ["write:demo"] })).unwrap();
        // Every agent comes from one address: no per-caller caps.
        let limits = aspen_auth::LimitsConfig {
            concurrent: aspen_auth::ConcurrentLimits {
                fetches: None,
                pushes: None,
                stitches: None,
            },
            ..aspen_auth::LimitsConfig::default()
        };
        let access = Arc::new(aspen_server::access::Access::new(
            aspen_auth::AuthService::new(policy, aspen_auth::MemoryTokenStore::new()).unwrap(),
            aspen_auth::Limits::new(limits),
            meta.clone(),
            aspen_server::access::ClientAddr::Peer,
        ));
        let app = server.router(access);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let mut compact = CompactConfig::new(
            dir.path().join("compact"),
            MaxObjectBytes::new(64 * MIB).unwrap(),
            MaxInflatedBytes::new(1024 * MIB).unwrap(),
        );
        compact.layout = CoverLayout::default();
        std::fs::create_dir_all(&compact.spool_dir).unwrap();
        let worker = Worker::new(
            Compactor::new(meta, blobs, compact),
            WorkerOptions::new("test-worker"),
        );
        Self {
            dir,
            counts,
            url: format!("http://{addr}/demo.git"),
            home,
            worker,
        }
    }

    fn work(&self, name: &str) -> Work {
        let path = self.dir.path().join(name);
        std::fs::create_dir_all(&path).unwrap();
        Work {
            path,
            home: self.home.clone(),
        }
    }

    /// Runs every due job, as a worker keeping up does.
    async fn compact(&mut self) {
        while let Some(o) = self.worker.run_one().await.unwrap() {
            if let JobOutcome::Failed { error, .. } = o {
                panic!("a job failed: {error}");
            }
        }
    }

    /// GETs and their bytes by caller and file so far.
    fn gets(&self) -> BTreeMap<(BlobCaller, BlobFile), (u64, u64)> {
        let mut out = BTreeMap::new();
        for r in self.counts.snapshot() {
            if r.kind == RequestKind::Get {
                out.insert((r.caller, r.file), (r.requests, r.bytes));
            }
        }
        out
    }
}

fn fast_import(w: &Work, stream: Vec<u8>) {
    let mut child = w
        .command()
        .args(["fast-import", "--quiet", "--done"])
        .stdin(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(&stream).unwrap();
    stdin.write_all(b"done\n").unwrap();
    drop(stdin);
    assert!(child.wait().unwrap().success(), "fast-import failed");
}

/// Every file of `shape` as the first commit on `main`.
fn import_initial(w: &Work, shape: Shape) {
    let mut s = Vec::with_capacity(shape.file_count() * 80);
    s.extend_from_slice(
        b"commit refs/heads/main\ncommitter A <a@a> 1700000000 +0000\ndata 4\ninit\n",
    );
    for leaf in 0..shape.leaves() {
        let dir = shape.leaf_dir(leaf);
        for f in 0..shape.files {
            let body = format!("{dir}/f{f}\n");
            write!(
                s,
                "M 100644 inline {dir}/f{f}.txt\ndata {}\n{body}",
                body.len()
            )
            .unwrap();
        }
    }
    s.push(b'\n');
    fast_import(w, s);
}

/// One commit on branch `name`, on top of `from`, that changes `f0.txt`
/// in leaf directory `leaf`.
fn import_change(w: &Work, shape: Shape, leaf: usize, round: usize, name: &str, from: &str) {
    let dir = shape.leaf_dir(leaf);
    // Long enough that git sends it as a delta against the old version.
    let body = format!("{dir}/f0\n{}round {round}\n", "unchanged line\n".repeat(20));
    let mut s = Vec::new();
    write!(
        s,
        "commit refs/heads/{name}\ncommitter A <a@a> {} +0000\ndata 6\nchange\nfrom {from}\n\
         M 100644 inline {dir}/f0.txt\ndata {}\n{body}\n",
        1_700_000_000 + round + 1,
        body.len()
    )
    .unwrap();
    fast_import(w, s);
}

async fn push(w: &Work, url: &str, name: &str) {
    let mut cmd = w.command();
    cmd.args(["push", "--porcelain", url, name]);
    let (ok, out) = blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await;
    assert!(ok, "push: {out}");
}

/// GETs (and bytes) per push, by caller and file (`caller.file`), over a
/// measured window.
#[derive(Debug, Clone)]
#[expect(dead_code, reason = "printed with Debug")]
struct PerPush {
    files: usize,
    trunk_versions: usize,
    idx: Idx,
    workload: Workload,
    gets: BTreeMap<String, f64>,
    bytes: BTreeMap<String, f64>,
    total: f64,
}

/// A repository of `shape` with `history` one-file trunk pushes after its
/// import, compacted as it goes; returns the GETs per push of
/// [`MEASURED`] more cycles of `workload` on the same warm server.
async fn measure(shape: Shape, history: usize, idx: Idx, workload: Workload) -> PerPush {
    let mut h = Harness::start(idx).await;
    let w = h.work("w");
    w.git(&["init", "-q", "--initial-branch=main"]);
    import_initial(&w, shape);
    push(&w, &h.url, "main").await;
    h.compact().await;
    let mut lcg: u64 = 0x2545_f491_4f6c_dd1d;
    let mut leaf = move || {
        lcg = lcg
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (lcg >> 33) as usize % shape.leaves()
    };
    let mut round = 0;
    let trunk = "refs/heads/main^0";
    for _ in 0..history {
        import_change(&w, shape, leaf(), round, "main", trunk);
        round += 1;
        push(&w, &h.url, "main").await;
        h.compact().await;
    }
    let agents = match workload {
        Workload::Trunk => 0,
        Workload::Agents { agents } => agents,
    };
    let clones: Vec<Work> = (0..agents)
        .map(|k| {
            let a = h.work(&format!("agent{k}"));
            a.git(&["clone", "-q", &h.url, "."]);
            a
        })
        .collect();

    let before = h.gets();
    let mut pushes = 0;
    for i in 0..MEASURED {
        import_change(&w, shape, leaf(), round, "main", trunk);
        round += 1;
        push(&w, &h.url, "main").await;
        pushes += 1;
        h.compact().await;
        let cycle: Vec<&Work> = match workload {
            Workload::Trunk => Vec::new(),
            Workload::Agents { agents } => vec![&clones[i % agents]],
        };
        let mut names = Vec::new();
        for a in &cycle {
            a.git(&["fetch", "-q", "origin"]);
            let name = format!("agent-{i}-{round}");
            import_change(a, shape, leaf(), round, &name, "refs/remotes/origin/main^0");
            round += 1;
            names.push(name);
        }
        futures_util::future::join_all(
            cycle
                .iter()
                .zip(&names)
                .map(|(a, name)| push(a, &h.url, name)),
        )
        .await;
        pushes += cycle.len();
        h.compact().await;
    }
    let after = h.gets();

    let n = pushes as f64;
    let tenth = |x: f64| (x * 10.0).round() / 10.0;
    let (mut gets, mut bytes) = (BTreeMap::new(), BTreeMap::new());
    for (&(c, f), &(g1, b1)) in &after {
        let (g0, b0) = before.get(&(c, f)).copied().unwrap_or((0, 0));
        if g1 > g0 {
            let key = format!("{}.{}", c.as_str(), f.as_str());
            gets.insert(key.clone(), tenth((g1 - g0) as f64 / n));
            bytes.insert(key, tenth((b1 - b0) as f64 / n));
        }
    }
    let r = PerPush {
        files: shape.file_count(),
        trunk_versions: 1 + history + MEASURED,
        idx,
        workload,
        total: tenth(gets.values().sum::<f64>()),
        gets,
        bytes,
    };
    eprintln!("GETs per push: {r:?}");
    r
}

/// Steady-state GETs per push, every caller included (the push's stages
/// and its share of compaction), may not exceed this at any repository
/// size, whether `.idx` files are kept whole or sampled (the default for a
/// pack over about 2.4 million objects, so the monorepo's import pack).
/// Measured with whole files: 4.2 at 4,608 files and 5.1 at 65,610
/// (decisions log, 12a). Sampled, the same since 12b; before it 28.4 and
/// 33.6, of which `push_graph` sent 22 (two ranged reads of a sampled
/// `.idx` per object a push looked up) and `push_prefetch` 3.4 to 8.6,
/// growing with the repository. A sample now keeps the sections a lookup
/// reads on local disk (`aspen_pack::sampled`; decisions log, 12b).
const GETS_PER_PUSH_BOUND: f64 = 8.0;

/// The agent cycle's fetch, counted per push (one fetch every two pushes):
/// it stitches the trunk pack it lacks, reading that pack, its `.idx` and
/// its entry table. Measured: 9.3 at 4,608 and at 65,610 files (12b). Not
/// what this file bounds (node 12b leaves the fetch path alone); here so
/// the agent cycle's bound is the push's plus this.
const FETCH_GETS_PER_PUSH: f64 = 10.0;

/// Asserts that `r` counted what a push and its compaction really send, so
/// that the bounds cannot pass because counting broke: some GETs, some
/// under `push_prefetch`, at least [`COMPACTION_FLOOR`] per trunk push for
/// each file compaction reads, and none outside a caller scope (`other`,
/// `push_other`).
fn assert_counted(r: &PerPush) {
    assert!(r.total > 0.0, "{r:?}: no GETs counted");
    let sum = |prefix: &str| -> f64 {
        r.gets
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(_, v)| v)
            .sum()
    };
    assert!(sum("push_prefetch.") > 0.0, "{r:?}: no push_prefetch GETs");
    // Only trunk pushes leave a pack compaction reads at once; an agent
    // cycle is one trunk push and one branch push.
    let floor = match r.workload {
        Workload::Trunk => COMPACTION_FLOOR,
        Workload::Agents { .. } => COMPACTION_FLOOR / 2.0,
    };
    for file in ["pack", "idx", "entries"] {
        let key = format!("compaction.{file}");
        let got = r.gets.get(&key).copied().unwrap_or(0.0);
        assert!(
            got >= floor,
            "{r:?}: {key} is {got} GETs per push, under {floor}"
        );
    }
    for unscoped in ["other.", "push_other."] {
        assert_eq!(sum(unscoped), 0.0, "{r:?}: GETs outside a caller scope");
    }
}

/// Fewest GETs per trunk push compaction sends for each of the pack,
/// `.idx` and entry table files: every trunk push leaves a level-0 pack
/// that the compaction after it reads (measured: exactly 1.0 each, at both
/// sizes, whole and sampled).
const COMPACTION_FLOOR: f64 = 1.0;

/// Asserts `bound` at both sizes, and that the larger repository costs at
/// most 2 more GETs per push than the smaller (flat).
fn assert_bounded(small: &PerPush, large: &PerPush, bound: f64) {
    for r in [small, large] {
        assert_counted(r);
        assert!(r.total <= bound, "{r:?}: more than {bound} GETs per push");
    }
    assert!(
        large.total <= small.total + 2.0,
        "GETs per push grew with the repository: {large:?} against {small:?}"
    );
}

/// Invariant (12a, 12b): a trunk writer's GETs per push stay under a fixed
/// bound that does not grow with the repository, with whole `.idx` files
/// and with sampled ones: 4,608 files and 33 trunk versions against 65,610
/// files and 113.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gets_per_push_do_not_grow_with_the_repository() {
    for idx in [Idx::Whole, Idx::SampledOver(64 << 10)] {
        let small = measure(SMALL, 16, idx, Workload::Trunk).await;
        let large = measure(LARGE, 96, idx, Workload::Trunk).await;
        assert_bounded(&small, &large, GETS_PER_PUSH_BOUND);
    }
}

/// Invariant (12b): the load test's agent cycle (fetch trunk, push a new
/// branch through the plain URL), with `.idx` files sampled, stays under
/// [`GETS_PER_PUSH_BOUND`] plus the fetch's share and does not grow with
/// the repository (nightly). Measured: 11.7 GETs per push at 4,608 files and
/// 12.7 at 65,610 (fetch 9.3, `push_prefetch` 0.9 to 1.9, compaction 1.5);
/// before 12b, 35.8 and 41.7 (`push_graph` 22, `push_prefetch` 3.0 to 8.9).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow: a few minutes; run by the nightly suite"]
async fn agent_gets_per_push_with_sampled_idx() {
    let idx = Idx::SampledOver(64 << 10);
    let workload = Workload::Agents { agents: 4 };
    let small = measure(SMALL, 16, idx, workload).await;
    let large = measure(LARGE, 96, idx, workload).await;
    assert_bounded(&small, &large, GETS_PER_PUSH_BOUND + FETCH_GETS_PER_PUSH);
}

/// Invariant (12b): the trunk writer at about the monorepo benchmark's
/// 300,000 files and 500 trunk versions, sampled `.idx`, stays under
/// [`GETS_PER_PUSH_BOUND`] and within 2 GETs per push of the small
/// repository (nightly). Measured: 4.2 against 4.6; before 12b, 28.4
/// against 33.8.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow: a few minutes; run by the nightly suite"]
async fn gets_per_push_at_300k_files() {
    let idx = Idx::SampledOver(64 << 10);
    let small = measure(SMALL, 16, idx, Workload::Trunk).await;
    let huge = measure(HUGE, 480, idx, Workload::Trunk).await;
    assert_bounded(&small, &huge, GETS_PER_PUSH_BOUND);
}
