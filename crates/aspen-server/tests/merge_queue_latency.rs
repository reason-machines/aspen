//! The merge queue's batch building against a blob store with a fixed delay
//! per read, as an object store has: a batch of many entries must not cost
//! one entry's reads times the number of entries. On S3 (bench/throughput)
//! entries were prepared one after another, each with several round trips
//! of its own, so a batch of 8 took about 1.4 s and the queue merged about
//! 5 changes/s. And entries whose bases lag behind trunk must not cost
//! a walk of trunk each (128 clients: 12 s per batch).

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aspen_blob::LocalBlobStore;
use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use aspen_core::meta::MetaStore;
use aspen_core::name::{RefName, RepoName};
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::queue::{MergeQueue, QueueConfig};
use aspen_server::receive::ReceiveConfig;
use aspen_testkit::git::isolated_git;

const MIB: u64 = 1 << 20;
/// Added to every blob read: about an object store's first-byte latency.
const DELAY: Duration = Duration::from_millis(25);

/// A local blob store whose reads each take [`DELAY`] longer.
#[derive(Debug, Clone)]
struct Slow {
    inner: LocalBlobStore,
    reads: Arc<AtomicU64>,
}

impl BlobStore for Slow {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        self.inner.upload_file(key, path).await
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(DELAY).await;
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

fn git(home: &Path, dir: &Path, args: &[&str]) -> String {
    let mut cmd = isolated_git(home);
    cmd.current_dir(dir).args(args);
    let out = cmd.output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

/// Clients that each enqueue one new file on top of the `main` they fetch.
async fn enqueue(dir: &Path, home: &Path, url: &str, names: &[String]) {
    let mut tasks = Vec::new();
    for name in names {
        let (dir, home, url, name) = (
            dir.to_owned(),
            home.to_owned(),
            url.to_owned(),
            name.clone(),
        );
        tasks.push(blocking(move || {
            let w = dir.join(&name);
            std::fs::create_dir_all(&w).unwrap();
            git(&home, &w, &["init", "-q", "-b", "main"]);
            git(&home, &w, &["fetch", "-q", &url, "main"]);
            git(&home, &w, &["reset", "-q", "--hard", "FETCH_HEAD"]);
            std::fs::create_dir_all(w.join("changes")).unwrap();
            std::fs::write(
                w.join("changes").join(format!("{name}.txt")),
                format!("{name}\n"),
            )
            .unwrap();
            git(&home, &w, &["add", "-A"]);
            git(&home, &w, &["commit", "-q", "-m", &name]);
            git(
                &home,
                &w,
                &["push", "-q", &url, &format!("HEAD:refs/queue/main/{name}")],
            );
        }));
    }
    for t in tasks {
        t.await;
    }
}

/// A server on the slow store with the merge queue on, a trunk a few
/// directories deep, and its queue worker.
struct Harness {
    dir: tempfile::TempDir,
    url: String,
    home: std::path::PathBuf,
    queue: MergeQueue<SqlStore, Slow>,
    svc: Arc<aspen_server::receive::ReceivePack<SqlStore, Slow>>,
    reads: Arc<AtomicU64>,
}

impl Harness {
    /// `sync_commit_rows`: commit-index rows a push writes at once; with 0
    /// only tips get rows and walks read the other commits from packs (the
    /// rows the index job writes later, which these tests never run).
    async fn start(sync_commit_rows: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = Slow {
            inner: LocalBlobStore::open(dir.path().join("blobs")).unwrap(),
            reads: Arc::default(),
        };
        let mut cfg = ReceiveConfig::new(
            dir.path().join("spool"),
            MaxObjectBytes::new(64 * MIB).unwrap(),
            MaxInflatedBytes::new(1024 * MIB).unwrap(),
        );
        cfg.thread_limit = Some(2);
        cfg.merge_queue = true;
        cfg.sync_commit_rows = sync_commit_rows;
        let server = aspen_server::Server::new(meta.clone(), blobs.clone(), {
            let mut c = aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap());
            c.metrics_on_git_listener = true;
            c
        })
        .with_receive_pack(cfg)
        .unwrap();
        let svc = server.receive_pack().unwrap().clone();
        let trunk = RefName::new("refs/heads/main").unwrap();
        let settings = RepoSettings::new(trunk).unwrap();
        meta.create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap();
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
        let url = format!("http://{addr}/demo.git");
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        // Trunk: a few directories deep, like a real repository.
        let seed = dir.path().join("seed");
        {
            let (seed, home, url) = (seed.clone(), home.clone(), url.clone());
            blocking(move || {
                std::fs::create_dir_all(seed.join("src/a/b")).unwrap();
                std::fs::create_dir_all(seed.join("changes")).unwrap();
                git(&home, &seed, &["init", "-q", "-b", "main"]);
                for i in 0..20 {
                    std::fs::write(seed.join(format!("src/a/b/f{i}.rs")), format!("{i}\n"))
                        .unwrap();
                }
                std::fs::write(seed.join("changes/README"), "changes\n").unwrap();
                git(&home, &seed, &["add", "-A"]);
                git(&home, &seed, &["commit", "-q", "-m", "seed"]);
                git(&home, &seed, &["push", "-q", &url, "main"]);
            })
            .await;
        }
        let queue = MergeQueue::new(svc.clone(), QueueConfig::default());
        Self {
            svc,
            dir,
            url,
            home,
            queue,
            reads: blobs.reads.clone(),
        }
    }

    fn clients(&self) -> std::path::PathBuf {
        self.dir.path().join("clients")
    }

    /// Lands every pending entry; returns how long it took and what landed.
    async fn land(&self) -> (Duration, aspen_server::queue::QueueReport) {
        let started = Instant::now();
        let reads = self.reads.load(Ordering::Relaxed);
        let report = self.queue.run_once().await.unwrap();
        eprintln!(
            "  landed {report:?} in {:?} with {} blob reads",
            started.elapsed(),
            self.reads.load(Ordering::Relaxed) - reads
        );
        (started.elapsed(), report)
    }

    /// Enqueues one entry per name and times the batch that lands them.
    async fn time_batch(&self, names: Vec<String>) -> Duration {
        enqueue(&self.clients(), &self.home, &self.url, &names).await;
        let (took, report) = self.land().await;
        assert_eq!(report.merged, names.len() as u64, "{report:?}");
        assert_eq!(report.batches, 1, "{report:?}");
        took
    }
}

/// Regression: 32 entries in one batch cost far less than 32 times what
/// one entry costs, because entries are prepared at once and each level
/// of their trees is read in one round trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_batch_does_not_cost_its_size_in_round_trips() {
    let h = Harness::start(10_000).await;
    let one = h.time_batch(vec!["one".into()]).await;
    let many = h
        .time_batch((0..32).map(|i| format!("c{i}")).collect())
        .await;
    eprintln!("one entry: {one:?}; 32 entries: {many:?}");
    assert!(
        many < one * 6,
        "a batch of 32 took {many:?}, one entry {one:?} (each read costs {DELAY:?})"
    );
}

/// Regression: entries whose bases are 100 trunk commits behind cost about
/// what entries on trunk's tip cost. Each base search used to walk trunk
/// from its tip back past the base, one commit lookup (here a read from
/// the packs) per trunk commit, so the further clients lagged, the slower
/// every batch got, and the more they lagged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entries_far_behind_trunk_cost_no_walk_of_trunk() {
    let h = Harness::start(0).await;
    // Sixteen clients fetch trunk now and make their changes...
    let lagging: Vec<String> = (0..16).map(|i| format!("late{i}")).collect();
    let behind = prepare_clients(&h.clients(), &h.home, &h.url, &lagging).await;
    // ...while 100 other changes land through the queue.
    let mut landed = 0;
    for round in 0..2 {
        let names: Vec<String> = (0..50).map(|i| format!("r{round}x{i}")).collect();
        enqueue(&h.clients(), &h.home, &h.url, &names).await;
        landed += h.land().await.1.merged;
    }
    assert_eq!(landed, 100);
    // Fresh clients, for comparison.
    let fresh = h
        .time_batch((0..16).map(|i| format!("fresh{i}")).collect())
        .await;
    // The lagging clients enqueue now, 100+ commits behind trunk.
    push_prepared(&h.home, &h.url, behind).await;
    let (late, report) = h.land().await;
    assert_eq!(report.merged, 16, "{report:?}");
    eprintln!("16 fresh entries: {fresh:?}; 16 entries 116 commits behind: {late:?}");
    // Walking trunk back to each base costs about 116 commits per entry;
    // found on trunk's recent chain it costs none.
    assert!(
        report.ancestry_commits <= 16,
        "base searches walked {} commits for 16 entries: {report:?}",
        report.ancestry_commits
    );
    assert!(
        late < fresh * 3,
        "16 lagging entries took {late:?}, 16 fresh ones {fresh:?} (each read costs {DELAY:?})"
    );
}

/// Clients that fetch `main` and commit one new file each, without pushing.
async fn prepare_clients(
    dir: &Path,
    home: &Path,
    url: &str,
    names: &[String],
) -> Vec<(std::path::PathBuf, String)> {
    let mut tasks = Vec::new();
    for name in names {
        let (dir, home, url, name) = (
            dir.to_owned(),
            home.to_owned(),
            url.to_owned(),
            name.clone(),
        );
        tasks.push(blocking(move || {
            let w = dir.join(&name);
            std::fs::create_dir_all(&w).unwrap();
            git(&home, &w, &["init", "-q", "-b", "main"]);
            git(&home, &w, &["fetch", "-q", &url, "main"]);
            git(&home, &w, &["reset", "-q", "--hard", "FETCH_HEAD"]);
            std::fs::create_dir_all(w.join("changes")).unwrap();
            std::fs::write(
                w.join("changes").join(format!("{name}.txt")),
                format!("{name}\n"),
            )
            .unwrap();
            git(&home, &w, &["add", "-A"]);
            git(&home, &w, &["commit", "-q", "-m", &name]);
            (w, name)
        }));
    }
    let mut out = Vec::new();
    for t in tasks {
        out.push(t.await);
    }
    out
}

async fn push_prepared(home: &Path, url: &str, clients: Vec<(std::path::PathBuf, String)>) {
    let mut tasks = Vec::new();
    for (w, name) in clients {
        let (home, url) = (home.to_owned(), url.to_owned());
        tasks.push(blocking(move || {
            git(
                &home,
                &w,
                &["push", "-q", &url, &format!("HEAD:refs/queue/main/{name}")],
            );
        }));
    }
    for t in tasks {
        t.await;
    }
}

/// Regression: a queued change's objects (commit, new trees, blobs) are
/// read with one read of its whole pack, not one read per object and tree
/// level; trunk's trees and the trees the worker wrote stay cached between
/// batches. Before, a batch read about three times per entry, each read a
/// blob-store round trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queued_change_costs_one_blob_read() {
    let h = Harness::start(10_000).await;
    h.time_batch(vec!["warm".into()]).await;
    let names: Vec<String> = (0..32).map(|i| format!("r{i}")).collect();
    enqueue(&h.clients(), &h.home, &h.url, &names).await;
    let reads = h.reads.load(Ordering::Relaxed);
    let (_, report) = h.land().await;
    assert_eq!(report.merged, 32, "{report:?}");
    let reads = h.reads.load(Ordering::Relaxed) - reads;
    assert!(
        reads <= 32 + 8,
        "landing 32 queued changes read the blob store {reads} times"
    );
}

/// How fast the worker drains a backlog of `PROFILE_N` queued changes
/// (default 1000) in batches of `PROFILE_BATCH` (default 64), over the
/// slow blob store, with stage times (exploration; run with `--ignored
/// --nocapture`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn profile_a_large_batch() {
    let env = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(d)
    };
    let (n, batch) = (env("PROFILE_N", 1000), env("PROFILE_BATCH", 64));
    let mut h = Harness::start(10_000).await;
    h.queue = MergeQueue::new(
        h.svc.clone(),
        QueueConfig {
            batch,
            ..QueueConfig::default()
        },
    );
    h.time_batch(vec!["warm".into()]).await;
    enqueue_many(&h, n).await;
    let before = metrics(&h.url);
    let (took, report) = h.land().await;
    let after = metrics(&h.url);
    eprintln!(
        "{n} changes in batches of {batch}: {took:?}, {:.0} merged/s, {report:?}",
        report.merged as f64 / took.as_secs_f64()
    );
    for (k, v) in &after {
        if k.starts_with("aspen_queue_stage") || k.starts_with("aspen_push_stage") {
            eprintln!(
                "{k} {:.1} ms",
                1e3 * (v - before.get(k).copied().unwrap_or(0.0))
            );
        }
    }
}

/// Enqueues `n` changes, each a new file on trunk's tip, with one push.
async fn enqueue_many(h: &Harness, n: usize) {
    let (dir, home, url) = (h.clients().join("many"), h.home.clone(), h.url.clone());
    blocking(move || {
        std::fs::create_dir_all(&dir).unwrap();
        git(&home, &dir, &["init", "-q", "--bare"]);
        git(&home, &dir, &["fetch", "-q", &url, "main:main"]);
        let mut stream = String::new();
        for i in 0..n {
            let data = format!("change {i}\n");
            stream.push_str(&format!(
                "commit refs/heads/e{i}\ncommitter B <b@x> 1800000000 +0000\ndata 2\nc\nfrom main\nM 100644 inline many/f{i}.txt\ndata {}\n{data}\n",
                data.len()
            ));
        }
        let mut cmd = isolated_git(&home);
        cmd.current_dir(&dir)
            .args(["fast-import", "--quiet"])
            .stdin(std::process::Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        use std::io::Write as _;
        child.stdin.take().unwrap().write_all(stream.as_bytes()).unwrap();
        assert!(child.wait().unwrap().success());
        git(&home, &dir, &["push", "-q", &url, "refs/heads/e*:refs/queue/main/e*"]);
    })
    .await;
}

fn metrics(url: &str) -> std::collections::BTreeMap<String, f64> {
    let base = url.trim_end_matches("/demo.git");
    let out = std::process::Command::new("curl")
        .args(["-s", &format!("{base}/-/metrics")])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.starts_with('#'))
        .filter_map(|l| {
            l.rsplit_once(' ')
                .and_then(|(k, v)| Some((k.to_owned(), v.parse().ok()?)))
        })
        .collect()
}
