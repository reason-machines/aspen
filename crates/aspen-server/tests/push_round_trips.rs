//! Round trips to the object store per push must not grow with delta-chain
//! depth or with the repository (monorepo benchmark, item 11).
//!
//! On a 300,000-file, 11.6-million-object repository on S3, a push spent
//! about 666 ms in prefetch and 489 ms in the object graph, nearly all of
//! it waiting on the store, one round trip after another:
//!
//! - **Delta chains were read one level per round trip.** A prefetch, and
//!   the walk of the changed paths reading old trees, learned each base's
//!   base only by reading it, so a chain of depth 50 (what `git gc` writes,
//!   and what compaction wrote for hot trees, chaining each version onto
//!   the one before) cost 51 round trips. Stored packs now come with chain
//!   maps (`aspen_pack::locate`, `aspen_pack::chains`), so a whole chain is
//!   read in one round, and compaction keeps trees within 10 levels.
//! - **Lookups in large packs went one pack at a time**, two round trips
//!   each (`.idx` IDs, then the offset), and a push looks objects up
//!   several times (bases, held objects, connectivity): more large packs,
//!   more round trips. A lookup now reads IDs and offsets together and
//!   probes every large pack at once.
//! - **Loading the previous push's `.idx`** cost every push a round trip;
//!   the pack's writer now hands it to the locator.
//!
//! The tests run Aspen against a blob store that adds a fixed delay to
//! every read, as an object store's first byte does, and count a push's
//! round trips as the store sees them: the times it went from no read in
//! flight to one, so reads made together count once and the machine's
//! speed does not matter. Each stage's wall time over the delay is printed
//! beside it.

mod receive_support;

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aspen_blob::LocalBlobStore;
use aspen_compact::{CompactConfig, Compactor, JobOutcome, Worker, WorkerOptions};
use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use aspen_core::meta::{CoverLayout, MetaStore};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::PackRecord;
use aspen_core::repo::RepoSettings;
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use aspen_server::receive::{ReceivePack, Timings};

use receive_support::{MIB, Work, blocking, config, git_ok};

/// Added to every blob read while measuring: about S3's first byte.
const DELAY: Duration = Duration::from_millis(50);

/// A local blob store whose reads each take `delay_ms` longer, counting
/// round trips: each time a read starts with none in flight.
#[derive(Debug, Clone)]
struct Slow {
    inner: LocalBlobStore,
    delay_ms: Arc<AtomicU64>,
    in_flight: Arc<AtomicU64>,
    rounds: Arc<AtomicU64>,
}

impl BlobStore for Slow {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        self.inner.upload_file(key, path).await
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        if self.in_flight.fetch_add(1, Ordering::SeqCst) == 0 {
            self.rounds.fetch_add(1, Ordering::SeqCst);
        }
        let d = self.delay_ms.load(Ordering::Relaxed);
        if d > 0 {
            tokio::time::sleep(Duration::from_millis(d)).await;
        }
        let read = self.inner.read_range(key, range).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        read
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

/// Required CI's large repository: 65,610 files, 8 levels deep.
const LARGE: Shape = Shape {
    fanout: 3,
    depth: 8,
    files: 10,
};

/// The nightly suite's: 301,806 files, 8 levels deep, about the monorepo
/// benchmark's 300,000.
const HUGE: Shape = Shape {
    fanout: 3,
    depth: 8,
    files: 46,
};

/// How the server keeps `.idx` files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Idx {
    /// Whole, with chain maps in memory (the defaults, for a pack under
    /// 2.4 million objects).
    Whole,
    /// Every `.idx` over this many bytes sampled, with chain maps on disk:
    /// the test repositories' large packs stand for a monorepo's.
    SampledOver(u64),
}

struct Harness {
    dir: tempfile::TempDir,
    meta: SqlStore,
    repo: aspen_core::id::RepoId,
    blobs: Slow,
    home: PathBuf,
    fanout: u64,
    idx: Idx,
    /// The serving instance: replaced by [`Harness::restart`].
    svc: Arc<ReceivePack<SqlStore, Slow>>,
    url: String,
    restarts: usize,
}

impl Harness {
    async fn start(fanout: u64, idx: Idx) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        let blobs = Slow {
            inner: LocalBlobStore::open(dir.path().join("blobs")).unwrap(),
            delay_ms: Arc::default(),
            in_flight: Arc::default(),
            rounds: Arc::default(),
        };
        let settings = RepoSettings::new(RefName::new("refs/heads/main").unwrap()).unwrap();
        let repo = meta
            .create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap()
            .id;
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        let (svc, url) = serve(&dir, &meta, &blobs, fanout, idx, 0).await;
        Self {
            dir,
            meta,
            repo,
            blobs,
            home,
            fanout,
            idx,
            svc,
            url,
            restarts: 0,
        }
    }

    /// A new server instance over the same stores, with nothing cached.
    async fn restart(&mut self) {
        self.restarts += 1;
        let (svc, url) = serve(
            &self.dir,
            &self.meta,
            &self.blobs,
            self.fanout,
            self.idx,
            self.restarts,
        )
        .await;
        self.svc = svc;
        self.url = url;
    }

    fn work(&self) -> Work {
        let path = self.dir.path().join("w");
        std::fs::create_dir_all(&path).unwrap();
        let w = Work {
            path,
            home: self.home.clone(),
        };
        w.git(&["init", "-q", "--initial-branch=main"]);
        w
    }

    /// Runs every due `compact` job with `tree_depth` as the trees' bound.
    async fn compact(&self, tree_depth: u32) {
        let mut config = CompactConfig::new(
            self.dir.path().join("compact"),
            MaxObjectBytes::new(64 * MIB).unwrap(),
            MaxInflatedBytes::new(1024 * MIB).unwrap(),
        );
        config.layout = CoverLayout::new(self.fanout).unwrap();
        config.max_tree_delta_depth = tree_depth;
        std::fs::create_dir_all(&config.spool_dir).unwrap();
        let c = Compactor::new(self.meta.clone(), self.blobs.clone(), config);
        let mut worker = Worker::new(c, WorkerOptions::new("test-worker"));
        while let Some(o) = worker.run_one().await.unwrap() {
            if let JobOutcome::Failed { error, .. } = o {
                panic!("a compact job failed: {error}");
            }
        }
    }

    /// Every compaction pack's row.
    async fn compaction_packs(&self) -> Vec<PackRecord> {
        let main = RefName::new("refs/heads/main").unwrap();
        let covers = self.meta.covers(self.repo, &main).await.unwrap();
        let ids: Vec<_> = covers.iter().filter_map(|c| c.pack).collect();
        assert!(!ids.is_empty(), "no cover was compacted");
        self.meta.packs(&ids).await.unwrap()
    }

    /// The deepest delta chain per object type in a stored pack, by
    /// `git verify-pack -v`.
    fn depths(&self, row: &PackRecord) -> BTreeMap<String, u32> {
        let scratch = tempfile::tempdir().unwrap();
        let p = scratch.path().join("p.pack");
        std::fs::copy(self.blobs.inner.path_of(&row.keys().pack), &p).unwrap();
        git_ok(
            &self.home,
            scratch.path(),
            &["index-pack", "-o", "p.idx", "p.pack"],
        );
        let out = git_ok(&self.home, scratch.path(), &["verify-pack", "-v", "p.idx"]);
        let mut depths = BTreeMap::new();
        for line in out.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() >= 5 && f[0].len() == 40 {
                let depth = f.get(5).map_or(0, |d| d.parse().unwrap());
                let e = depths.entry(f[1].to_owned()).or_insert(0);
                *e = (*e).max(depth);
            }
        }
        depths
    }
}

async fn serve(
    dir: &tempfile::TempDir,
    meta: &SqlStore,
    blobs: &Slow,
    fanout: u64,
    idx: Idx,
    instance: usize,
) -> (Arc<ReceivePack<SqlStore, Slow>>, String) {
    let mut cfg = config(&dir.path().join(format!("instance-{instance}")));
    cfg.cover_layout = CoverLayout::new(fanout).unwrap();
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
    (svc, format!("http://{addr}/demo.git"))
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

/// The files of `shape`'s leaves `leaves` as one commit on `main` (on top
/// of `main` unless `first`).
fn import_leaves(w: &Work, shape: Shape, leaves: std::ops::Range<usize>, first: bool) {
    let mut s = Vec::with_capacity(leaves.len() * shape.files * 80);
    s.extend_from_slice(
        b"commit refs/heads/main\ncommitter A <a@a> 1700000000 +0000\ndata 4\ninit\n",
    );
    if !first {
        s.extend_from_slice(b"from refs/heads/main^0\n");
    }
    for leaf in leaves {
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

/// The initial commit of `shape` on `main`.
fn import_initial(w: &Work, shape: Shape) {
    import_leaves(w, shape, 0..shape.leaves(), true);
}

/// One commit on `main` that changes `f0.txt` in leaf directory `leaf`.
fn import_change(w: &Work, shape: Shape, leaf: usize, round: usize) {
    let dir = shape.leaf_dir(leaf);
    // Long enough that git sends it as a delta against the old version.
    let body = format!("{dir}/f0\n{}round {round}\n", "unchanged line\n".repeat(20));
    let mut s = Vec::new();
    write!(
        s,
        "commit refs/heads/main\ncommitter A <a@a> {} +0000\ndata 6\nchange\nfrom refs/heads/main^0\n\
         M 100644 inline {dir}/f0.txt\ndata {}\n{body}\n",
        1_700_000_000 + round + 1,
        body.len()
    )
    .unwrap();
    fast_import(w, s);
}

async fn push(w: &Work, url: &str) {
    let mut cmd = w.command();
    cmd.args(["push", "--porcelain", url, "main"]);
    let (ok, out) = blocking(move || {
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t)
    })
    .await;
    assert!(ok, "push: {out}");
}

/// Round trips to the store per push, and each stage's wall time over
/// [`DELAY`] per push (to the nearest tenth), which counts CPU time too.
#[derive(Debug, Clone, Copy)]
struct Rounds {
    total: f64,
    /// Printed only.
    #[expect(dead_code, reason = "printed with Debug")]
    prefetch: f64,
    /// Printed only.
    #[expect(dead_code, reason = "printed with Debug")]
    graph: f64,
}

impl Rounds {
    fn of(rounds: u64, before: &Timings, after: &Timings) -> Self {
        let n = (after.pushes - before.pushes) as f64;
        let tenth = |x: f64| (x * 10.0).round() / 10.0;
        let r = |d: Duration| tenth(d.as_secs_f64() / DELAY.as_secs_f64() / n);
        Self {
            total: tenth(rounds as f64 / n),
            prefetch: r(after.prefetch - before.prefetch),
            graph: r(after.graph - before.graph),
        }
    }
}

/// Measures `pushes` pushes, each changing a leaf `leaf` picks, on a
/// freshly restarted server when `cold`.
async fn measure(
    h: &mut Harness,
    w: &Work,
    shape: Shape,
    round: &mut usize,
    pushes: usize,
    cold: bool,
    mut leaf: impl FnMut() -> usize,
) -> Rounds {
    if cold {
        h.restart().await;
    }
    h.blobs
        .delay_ms
        .store(DELAY.as_millis() as u64, Ordering::Relaxed);
    let before = h.svc.timings();
    let rounds_before = h.blobs.rounds.load(Ordering::SeqCst);
    let started = Instant::now();
    for _ in 0..pushes {
        import_change(w, shape, leaf(), *round);
        *round += 1;
        push(w, &h.url).await;
    }
    let rounds = Rounds::of(
        h.blobs.rounds.load(Ordering::SeqCst) - rounds_before,
        &before,
        &h.svc.timings(),
    );
    h.blobs.delay_ms.store(0, Ordering::Relaxed);
    eprintln!(
        "{} files, {:?}, {}: {pushes} pushes in {:?}, per push {rounds:?}",
        shape.file_count(),
        h.idx,
        if cold { "cold" } else { "warm" },
        started.elapsed()
    );
    rounds
}

/// A repository of `SMALL`'s shape whose `main` has 51 versions, the last
/// 50 each changing the same file, compacted into one level-1 cover of 51
/// (fanout 51) with trees bounded at `tree_depth`: the file's blob, and
/// with `tree_depth` 50 the trees above it, sit at the end of 50-deep
/// delta chains, as compaction leaves a hot path.
async fn deep_chains(idx: Idx, tree_depth: u32) -> (Harness, Work, usize) {
    let h = Harness::start(51, idx).await;
    let w = h.work();
    import_initial(&w, SMALL);
    push(&w, &h.url).await;
    let mut round = 0;
    for _ in 0..50 {
        import_change(&w, SMALL, 7, round);
        round += 1;
        push(&w, &h.url).await;
    }
    h.compact(tree_depth).await;
    (h, w, round)
}

/// Invariant (item 11): a push whose delta bases sit at the end of 50-deep
/// chains, on a server with nothing cached, reads each chain in one round
/// trip, whatever its depth: at most 12 round trips in all, 24 with
/// sampled `.idx` files. Expected, and printed per stage:
///
/// - prefetch: one round to load the cover's `.idx` (two for a sampled
///   one, which streams the sample and the chain map), one to look the
///   bases up in a sampled `.idx`, and one to read all 101 entries of the
///   two 50-deep chains: about 2 rounds, 4 sampled;
/// - the object graph walks the changed path from the parent commit down,
///   reading each old tree it does not have (the commit, the root tree and
///   8 directories: 10 reads in turn, as each names the next) in one round
///   each, two in a sampled `.idx` (lookup, then the chain): about 10
///   rounds, 20 sampled.
///
/// Before chain maps the prefetch read took a round per level of its
/// chains, 51 of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deep_delta_chains_cost_constant_round_trips() {
    for (idx, bound) in [(Idx::Whole, 12.0), (Idx::SampledOver(0), 24.0)] {
        let (mut h, w, mut round) = deep_chains(idx, 50).await;
        let packs = h.compaction_packs().await;
        assert_eq!(packs.len(), 1);
        let depths = h.depths(&packs[0]);
        assert_eq!(
            (depths["blob"], depths["tree"]),
            (50, 50),
            "the fixture's chains are 50 deep: {depths:?}"
        );
        let rounds = measure(&mut h, &w, SMALL, &mut round, 1, true, || 7).await;
        assert!(
            rounds.total <= bound,
            "{idx:?}: {rounds:?} round trips per push on 50-deep chains"
        );
    }
}

/// Invariant (item 11): compaction keeps trees within
/// `CompactConfig::max_tree_delta_depth` (10 by default) while blobs keep
/// `max_delta_depth` (50): the same hot path as above, compacted with the
/// default bound, holds no tree deeper than 10, and the stored version is
/// still served whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_keeps_hot_trees_within_the_tree_bound() {
    let default = CompactConfig::new(
        "unused",
        MaxObjectBytes::new(64 * MIB).unwrap(),
        MaxInflatedBytes::new(1024 * MIB).unwrap(),
    )
    .max_tree_delta_depth;
    assert_eq!(default, 10);
    let (h, w, _) = deep_chains(Idx::Whole, default).await;
    let packs = h.compaction_packs().await;
    let depths = h.depths(&packs[0]);
    assert_eq!(
        (depths["blob"], depths["tree"]),
        (50, 10),
        "blobs at the general bound, trees at the tree bound: {depths:?}"
    );
    // The cover's pack is a valid stored pack holding every object.
    let tip = w.rev("main");
    let reach = w.objects(&tip);
    let scratch = tempfile::tempdir().unwrap();
    let p = scratch.path().join("p.pack");
    std::fs::copy(h.blobs.inner.path_of(&packs[0].keys().pack), &p).unwrap();
    git_ok(
        &h.home,
        scratch.path(),
        &["index-pack", "--strict", "-o", "p.idx", "p.pack"],
    );
    let out = git_ok(&h.home, scratch.path(), &["verify-pack", "-v", "p.idx"]);
    let held: std::collections::BTreeSet<_> = out
        .lines()
        .filter_map(|l| l.split_whitespace().next())
        .filter_map(|x| aspen_core::id::ObjectId::parse_hex(x).ok())
        .collect();
    assert!(
        reach.is_subset(&held),
        "the cover holds everything main reaches"
    );
}

/// Pushes per repository after its import, before measurement.
const HISTORY: usize = 24;
/// Pushes measured per repository.
const MEASURED: usize = 6;

/// A repository of `shape` imported in `parts` pushes (so it has `parts`
/// large packs, as a repository grown over time has several), then
/// [`HISTORY`] pushes each changing a random leaf; returns the round trips
/// per push of [`MEASURED`] more, on a warm server. No compaction runs
/// (fanout 1,024): every version's pack is searched, as the newest are
/// before compaction reaches them.
async fn grown(shape: Shape, parts: usize, idx: Idx) -> Rounds {
    let mut h = Harness::start(1024, idx).await;
    let w = h.work();
    let per = shape.leaves().div_ceil(parts);
    for part in 0..parts {
        let leaves = part * per..((part + 1) * per).min(shape.leaves());
        import_leaves(&w, shape, leaves, part == 0);
        push(&w, &h.url).await;
    }
    let mut lcg: u64 = 0x2545_f491_4f6c_dd1d;
    let mut leaf = move || {
        lcg = lcg
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (lcg >> 33) as usize % shape.leaves()
    };
    let mut round = 0;
    for _ in 0..HISTORY {
        import_change(&w, shape, leaf(), round);
        round += 1;
        push(&w, &h.url).await;
    }
    measure(&mut h, &w, shape, &mut round, MEASURED, false, &mut leaf).await
}

/// Invariant (item 11): a push waits on the store for as many round trips
/// in a repository whose history is in 8 large packs as in the same
/// repository in one: within half a round trip. Every `.idx` over
/// 64 KiB is sampled (the imports', not the pushes'), as a monorepo's large
/// packs are. Before, a lookup probed the large packs one after another,
/// two round trips each, and a push looks objects up in several steps
/// (bases, held objects, connectivity, old trees), so both stages grew with
/// the number of large packs: as a repository grows, so does that number.
async fn compare(shape: Shape) {
    let one = grown(shape, 1, Idx::SampledOver(64 << 10)).await;
    let eight = grown(shape, 8, Idx::SampledOver(64 << 10)).await;
    assert!(
        eight.total <= one.total + 0.5,
        "round trips per push, {} files: {eight:?} in 8 large packs against {one:?} in one",
        shape.file_count(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_push_round_trips_do_not_grow_with_the_repository() {
    compare(LARGE).await;
}

/// The same at about the monorepo benchmark's 300,000 files (nightly).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow: a few minutes; run by the nightly suite"]
async fn per_push_round_trips_do_not_grow_with_a_300k_file_repository() {
    compare(HUGE).await;
}
