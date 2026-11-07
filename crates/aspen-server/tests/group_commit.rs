//! Group commit (`ReceiveConfig::group_commit`, `ASPEN_GROUP_COMMIT`),
//! end to end with stock `git push`: what it saves (metadata write
//! transactions per push; pack objects are unchanged), and what it must
//! keep: a push is acknowledged only after its pack is uploaded and its own
//! ref update committed, a lost compare-and-swap fails only the push that
//! lost it, and a batch whose transaction fails refuses every push in it.
//!
//! The numbers the measurement prints are in the PR that added group
//! commit; run them with
//! `cargo test -p aspen-server --test group_commit -- --nocapture`.

mod receive_support;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use aspen_blob::LocalBlobStore;
use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{CoverId, JobId, PackChecksum, PackId, RefVersionId, RepoId, Seq};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{
    CommitLookup, CommitOutcome, CommitRow, Cover, Job, JobEvent, JobTag, MetaStore, NewCover,
    NewJob, PackTombstone, PushCommit, RefListQuery, ShallowDepth, ShallowRecorded,
};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{CoverLevel, NewPack, PackRecord};
use aspen_core::refs::RefVersion;
use aspen_core::repo::{ForcePolicy, Repo, RepoSettings};
use aspen_meta::SqlStore;
use aspen_pack::MaxObjectBytes;
use aspen_server::receive::{GroupCommit, ReceivePack};
use aspen_testkit::RACE_LOSS_REASONS;

use receive_support::*;

/// `SqlStore`, plus two checks of what reaches it. Before every commit it
/// looks up each pack the writes name and records any of its blobs that is
/// not in the blob store yet (a push must never be committed, and so
/// acknowledged, before its pack is durable). While `fail_batches` is set,
/// every commit fails without running, as a batch whose `COMMIT` did not
/// land would.
#[derive(Debug, Clone)]
struct Checked {
    inner: SqlStore,
    blobs: LocalBlobStore,
    fail_batches: Arc<AtomicBool>,
    not_durable: Arc<Mutex<Vec<String>>>,
    batch_sizes: Arc<Mutex<Vec<usize>>>,
}

fn injected() -> StoreError {
    StoreError::Unavailable {
        reason: "injected: the batch's COMMIT failed".into(),
    }
}

impl Checked {
    async fn check_durable(&self, pushes: &[PushCommit]) -> Result<(), StoreError> {
        for push in pushes {
            for w in &push.writes {
                let Some(id) = w.value.pack() else { continue };
                let row = self.inner.packs(&[id]).await?.remove(0);
                let keys = row.keys();
                for key in [&keys.pack, &keys.idx, &keys.entries] {
                    if self.blobs.size(key).await?.is_none() {
                        self.not_durable
                            .lock()
                            .unwrap()
                            .push(format!("{}: {key:?}", w.name));
                    }
                }
            }
        }
        Ok(())
    }
}

impl MetaStore for Checked {
    async fn create_repo(
        &self,
        name: &RepoName,
        settings: &RepoSettings,
    ) -> Result<Repo, StoreError> {
        self.inner.create_repo(name, settings).await
    }
    async fn repo(&self, name: &RepoName) -> Result<Option<Repo>, StoreError> {
        self.inner.repo(name).await
    }
    async fn repo_by_id(&self, id: RepoId) -> Result<Option<Repo>, StoreError> {
        self.inner.repo_by_id(id).await
    }
    async fn list_repos(&self, after: Option<RepoId>, limit: u32) -> Result<Vec<Repo>, StoreError> {
        self.inner.list_repos(after, limit).await
    }
    async fn current_version(
        &self,
        repo: RepoId,
        name: &RefName,
    ) -> Result<Option<RefVersion>, StoreError> {
        self.inner.current_version(repo, name).await
    }
    async fn ref_version(&self, id: RefVersionId) -> Result<Option<RefVersion>, StoreError> {
        self.inner.ref_version(id).await
    }
    async fn versions_by_seq(
        &self,
        repo: RepoId,
        name: &RefName,
        seqs: &[Seq],
    ) -> Result<Vec<RefVersion>, StoreError> {
        self.inner.versions_by_seq(repo, name, seqs).await
    }
    async fn list_refs(
        &self,
        repo: RepoId,
        query: &RefListQuery,
    ) -> Result<Vec<RefVersion>, StoreError> {
        self.inner.list_refs(repo, query).await
    }
    async fn register_pack(&self, pack: &NewPack) -> Result<PackRecord, StoreError> {
        self.inner.register_pack(pack).await
    }
    async fn packs(&self, ids: &[PackId]) -> Result<Vec<PackRecord>, StoreError> {
        self.inner.packs(ids).await
    }
    async fn commit_push(&self, push: &PushCommit) -> Result<CommitOutcome, StoreError> {
        self.check_durable(std::slice::from_ref(push)).await?;
        if self.fail_batches.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.commit_push(push).await
    }
    async fn commit_pushes(
        &self,
        pushes: &[PushCommit],
    ) -> Result<Vec<Result<CommitOutcome, StoreError>>, StoreError> {
        self.check_durable(pushes).await?;
        self.batch_sizes.lock().unwrap().push(pushes.len());
        if self.fail_batches.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.commit_pushes(pushes).await
    }
    async fn register_packs(
        &self,
        packs: &[NewPack],
    ) -> Result<Vec<Result<PackRecord, StoreError>>, StoreError> {
        self.inner.register_packs(packs).await
    }
    async fn lookup_commits(
        &self,
        repo: RepoId,
        ids: &SortedIds,
    ) -> Result<Vec<CommitLookup>, StoreError> {
        self.inner.lookup_commits(repo, ids).await
    }
    async fn index_commits(&self, repo: RepoId, rows: &[CommitRow]) -> Result<(), StoreError> {
        self.inner.index_commits(repo, rows).await
    }
    async fn unexamined_commits(
        &self,
        repo: RepoId,
        after: Option<(aspen_core::id::RefVersionId, aspen_core::id::ObjectId)>,
        limit: usize,
    ) -> Result<Vec<CommitRow>, StoreError> {
        self.inner.unexamined_commits(repo, after, limit).await
    }
    async fn commits_waiting_on(
        &self,
        repo: RepoId,
        ids: &aspen_core::ingest::SortedIds,
        limit: usize,
    ) -> Result<Vec<CommitRow>, StoreError> {
        self.inner.commits_waiting_on(repo, ids, limit).await
    }
    async fn set_generation_waits_on(
        &self,
        repo: RepoId,
        waits: &[(aspen_core::id::ObjectId, aspen_core::id::ObjectId)],
    ) -> Result<(), StoreError> {
        self.inner.set_generation_waits_on(repo, waits).await
    }
    async fn set_generations(
        &self,
        repo: RepoId,
        generations: &[(aspen_core::id::ObjectId, u64)],
    ) -> Result<(), StoreError> {
        self.inner.set_generations(repo, generations).await
    }
    async fn covers(&self, repo: RepoId, name: &RefName) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers(repo, name).await
    }
    async fn covers_at(
        &self,
        repo: RepoId,
        name: &RefName,
        level: CoverLevel,
        firsts: &[Seq],
    ) -> Result<Vec<Cover>, StoreError> {
        self.inner.covers_at(repo, name, level, firsts).await
    }
    async fn insert_cover(&self, cover: &NewCover) -> Result<Cover, StoreError> {
        self.inner.insert_cover(cover).await
    }
    async fn set_cover_pack(&self, cover: CoverId, pack: PackId) -> Result<Cover, StoreError> {
        self.inner.set_cover_pack(cover, pack).await
    }
    async fn pack_by_checksum(
        &self,
        repo: RepoId,
        checksum: &PackChecksum,
    ) -> Result<Option<PackRecord>, StoreError> {
        self.inner.pack_by_checksum(repo, checksum).await
    }
    async fn acquire_lease(
        &self,
        name: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Option<aspen_core::meta::Lease>, StoreError> {
        self.inner.acquire_lease(name, owner, ttl).await
    }
    async fn sweep_orphan_packs(
        &self,
        lease: &aspen_core::meta::Lease,
        grace: Duration,
        limit: u32,
    ) -> Result<aspen_core::meta::SweepBatch, StoreError> {
        self.inner.sweep_orphan_packs(lease, grace, limit).await
    }
    async fn pending_pack_deletes(&self, limit: u32) -> Result<Vec<PackTombstone>, StoreError> {
        self.inner.pending_pack_deletes(limit).await
    }
    async fn finish_pack_deletes(&self, done: &[PackTombstone]) -> Result<(), StoreError> {
        self.inner.finish_pack_deletes(done).await
    }
    async fn shallow_pack(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
    ) -> Result<Option<PackRecord>, StoreError> {
        self.inner.shallow_pack(repo, version, depth).await
    }
    async fn set_shallow_pack(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
        pack: PackId,
        max_repo_bytes: u64,
    ) -> Result<ShallowRecorded, StoreError> {
        self.inner
            .set_shallow_pack(repo, version, depth, pack, max_repo_bytes)
            .await
    }
    async fn mark_shallow_pack_used(
        &self,
        repo: RepoId,
        version: RefVersionId,
        depth: ShallowDepth,
    ) -> Result<(), StoreError> {
        self.inner
            .mark_shallow_pack_used(repo, version, depth)
            .await
    }
    async fn evict_shallow_packs(
        &self,
        superseded_after: Duration,
        unused_after: Duration,
        limit: u32,
    ) -> Result<u64, StoreError> {
        self.inner
            .evict_shallow_packs(superseded_after, unused_after, limit)
            .await
    }
    async fn pack_references(
        &self,
        pack: aspen_core::id::PackId,
    ) -> Result<aspen_core::meta::PackReferences, StoreError> {
        self.inner.pack_references(pack).await
    }
    async fn replace_pack(
        &self,
        old: aspen_core::id::PackId,
        new: aspen_core::id::PackId,
    ) -> Result<aspen_core::meta::PackReferences, StoreError> {
        self.inner.replace_pack(old, new).await
    }
    async fn enqueue_jobs(&self, jobs: &[NewJob]) -> Result<Vec<JobId>, StoreError> {
        self.inner.enqueue_jobs(jobs).await
    }
    async fn claim_job(
        &self,
        tags: &[JobTag],
        owner: &str,
        lease: Duration,
    ) -> Result<Option<Job>, StoreError> {
        self.inner.claim_job(tags, owner, lease).await
    }
    async fn release_jobs(
        &self,
        tags: &[JobTag],
        owner: &str,
        reason: &str,
    ) -> Result<u64, StoreError> {
        self.inner.release_jobs(tags, owner, reason).await
    }
    async fn add_compaction_heat(
        &self,
        heat: &[aspen_core::meta::CompactionHeat],
    ) -> Result<u64, StoreError> {
        self.inner.add_compaction_heat(heat).await
    }
    async fn update_job(&self, id: JobId, event: JobEvent) -> Result<Job, StoreError> {
        self.inner.update_job(id, event).await
    }
}

struct Srv {
    dir: tempfile::TempDir,
    meta: Checked,
    svc: Arc<ReceivePack<Checked, LocalBlobStore>>,
    repo: Repo,
    addr: std::net::SocketAddr,
    url: String,
    home: PathBuf,
}

impl Srv {
    async fn start(group: Option<GroupCommit>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let inner = SqlStore::connect(&url).await.unwrap();
        inner.migrate().await.unwrap();
        let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
        let meta = Checked {
            inner,
            blobs: blobs.clone(),
            fail_batches: Arc::default(),
            not_durable: Arc::default(),
            batch_sizes: Arc::default(),
        };
        let mut cfg = config(dir.path());
        cfg.group_commit = group.unwrap_or_default();
        let server = aspen_server::Server::new(meta.clone(), blobs, {
            let mut c = aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap());
            c.metrics_on_git_listener = true;
            c
        })
        .with_receive_pack(cfg)
        .unwrap();
        let svc = server.receive_pack().unwrap().clone();
        let main = RefName::new("refs/heads/main").unwrap();
        let settings = RepoSettings::builder(
            main.clone(),
            RepoSettings::new(main).unwrap().writable().to_vec(),
        )
        .allow_force(ForcePolicy::Branches)
        .build()
        .unwrap();
        let repo = meta
            .create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let policy: aspen_auth::PolicyConfig =
            serde_json::from_value(serde_json::json!({ "anonymous": ["write:demo"] })).unwrap();
        let access = Arc::new(aspen_server::access::Access::new(
            aspen_auth::AuthService::new(policy, aspen_auth::MemoryTokenStore::new()).unwrap(),
            aspen_auth::Limits::new(aspen_auth::LimitsConfig {
                concurrent: aspen_auth::ConcurrentLimits::UNLIMITED,
                ..aspen_auth::LimitsConfig::default()
            }),
            meta.clone(),
            aspen_server::access::ClientAddr::Peer,
        ));
        let app = server.router(access);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        Self {
            addr,
            url: format!("http://{addr}/demo.git"),
            meta,
            svc,
            repo,
            home,
            dir,
        }
    }

    /// A working repository with `main` at one commit, pushed.
    async fn seeded(&self) -> Work {
        let w = Work {
            path: self.dir.path().join("w"),
            home: self.home.clone(),
        };
        std::fs::create_dir_all(&w.path).unwrap();
        w.git(&["init", "-q", "--initial-branch=main"]);
        w.commit("base.txt", "base\n", "base");
        let (ok, out, _) = push(&w, &self.url, "main").await;
        assert!(ok, "{out}");
        w
    }

    async fn tip(&self, name: &str) -> Option<String> {
        self.meta
            .current_version(self.repo.id, &RefName::new(name).unwrap())
            .await
            .unwrap()
            .and_then(|v: RefVersion| v.value.tip())
            .map(|t| t.to_string())
    }

    /// The value of an unlabelled sample on `/-/metrics`.
    async fn metric(&self, name: &str) -> Option<f64> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut conn = tokio::net::TcpStream::connect(self.addr).await.unwrap();
        conn.write_all(b"GET /-/metrics HTTP/1.0\r\nHost: aspen\r\n\r\n")
            .await
            .unwrap();
        let mut body = String::new();
        conn.read_to_string(&mut body).await.unwrap();
        body.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
    }

    fn blob_files(&self) -> usize {
        files(&self.dir.path().join("blobs"))
    }
}

fn files(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .map(|e| {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                files(&e.path())
            } else {
                1
            }
        })
        .sum()
}

/// `git push --porcelain <url> <refspec>`: success, output, wall time.
async fn push(w: &Work, url: &str, refspec: &str) -> (bool, String, Duration) {
    let mut cmd = w.command();
    cmd.args(["push", "--porcelain", url, refspec]);
    blocking(move || {
        let started = Instant::now();
        let o = cmd.output().unwrap();
        let mut t = String::from_utf8_lossy(&o.stdout).into_owned();
        t.push_str(&String::from_utf8_lossy(&o.stderr));
        (o.status.success(), t, started.elapsed())
    })
    .await
}

/// Branch `name` off `main` with one commit of its own (`content` decides
/// its ID); returns the commit.
fn branch(w: &Work, name: &str, content: &str) -> String {
    let head = w.git(&["rev-parse", "main"]);
    let blob = {
        let mut c = w.command();
        c.args(["hash-object", "-w", "--stdin"]);
        c.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        let mut child = c.spawn().unwrap();
        use std::io::Write as _;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(content.as_bytes())
            .unwrap();
        let out = child.wait_with_output().unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    };
    let base_tree = w.git(&["rev-parse", "main^{tree}"]);
    let tree = {
        let ls = w.git(&["ls-tree", base_tree.trim()]);
        let mut c = w.command();
        c.arg("mktree")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped());
        let mut child = c.spawn().unwrap();
        use std::io::Write as _;
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(ls.as_bytes()).unwrap();
        writeln!(stdin, "100644 blob {blob}\t{name}.txt").unwrap();
        drop(stdin);
        let out = child.wait_with_output().unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    };
    let commit = w
        .git(&["commit-tree", &tree, "-p", head.trim(), "-m", name])
        .trim()
        .to_owned();
    w.git(&["update-ref", &format!("refs/heads/{name}"), &commit]);
    commit
}

/// One run of `n` concurrent pushes, each of its own branch.
#[derive(Debug)]
struct Run {
    write_transactions: u64,
    blob_puts: usize,
    p50: Duration,
    max: Duration,
    wall: Duration,
    batches: Vec<usize>,
}

async fn concurrent_branch_pushes(group: Option<GroupCommit>, n: usize) -> Run {
    let s = Srv::start(group).await;
    let w = s.seeded().await;
    let mut tips = Vec::new();
    for i in 0..n {
        tips.push(branch(&w, &format!("b{i}"), &format!("branch {i}\n")));
    }
    let tx_before = s.meta.inner.write_transactions();
    let blobs_before = s.blob_files();
    s.meta.batch_sizes.lock().unwrap().clear();
    let started = Instant::now();
    let mut handles = Vec::new();
    for i in 0..n {
        let (w, url) = (
            Work {
                path: w.path.clone(),
                home: w.home.clone(),
            },
            s.url.clone(),
        );
        handles.push(tokio::spawn(async move {
            push(&w, &url, &format!("refs/heads/b{i}:refs/heads/b{i}")).await
        }));
    }
    let mut times = Vec::new();
    for (i, h) in handles.into_iter().enumerate() {
        let (ok, out, took) = h.await.unwrap();
        assert!(ok, "push {i}: {out}");
        times.push(took);
    }
    let wall = started.elapsed();
    for (i, tip) in tips.iter().enumerate() {
        assert_eq!(s.tip(&format!("refs/heads/b{i}")).await.as_ref(), Some(tip));
    }
    assert_eq!(
        *s.meta.not_durable.lock().unwrap(),
        Vec::<String>::new(),
        "a push was committed before its pack was uploaded"
    );
    times.sort();
    Run {
        write_transactions: s.meta.inner.write_transactions() - tx_before,
        blob_puts: s.blob_files() - blobs_before,
        p50: times[n / 2],
        max: times[n - 1],
        wall,
        batches: s.meta.batch_sizes.lock().unwrap().clone(),
    }
}

/// The measurement. Without group commit every push runs two metadata write
/// transactions (register its pack, commit its ref); with it, concurrent
/// pushes share them. Every push still stores its own pack, `.idx` and
/// entry table (three blob writes), uploaded before the transaction that
/// references them, and every push is acknowledged and lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_commit_shares_metadata_transactions_between_concurrent_pushes() {
    const N: usize = 32;
    let off = concurrent_branch_pushes(None, N).await;
    let on = concurrent_branch_pushes(
        Some(GroupCommit {
            window: Duration::from_millis(20),
            ..GroupCommit::on()
        }),
        N,
    )
    .await;
    let default = concurrent_branch_pushes(Some(GroupCommit::on()), N).await;
    eprintln!("{N} concurrent pushes, each to its own branch (SQLite, local blob store):");
    for (label, r) in [
        ("off", &off),
        ("on, window 20 ms", &on),
        ("on, window 2 ms (default)", &default),
    ] {
        eprintln!(
            "  {label:<27} write transactions {:>3} ({:.2}/push)  blob writes {:>3} ({:.1}/push)  \
             push p50 {:>4} ms  max {:>4} ms  all {:>5} ms  commit batches {:?}",
            r.write_transactions,
            r.write_transactions as f64 / N as f64,
            r.blob_puts,
            r.blob_puts as f64 / N as f64,
            r.p50.as_millis(),
            r.max.as_millis(),
            r.wall.as_millis(),
            r.batches,
        );
    }
    assert_eq!(off.write_transactions, 2 * N as u64, "two per push, off");
    assert!(
        off.batches.is_empty(),
        "no batches with group commit off: {:?}",
        off.batches
    );
    for r in [&off, &on, &default] {
        assert_eq!(r.blob_puts, 3 * N, "pack, .idx and entry table per push");
    }
    assert!(
        on.write_transactions <= N as u64,
        "group commit at least halves the write transactions: {} for {N} pushes",
        on.write_transactions
    );
    assert_eq!(on.batches.iter().sum::<usize>(), N);
    assert!(default.write_transactions < 2 * N as u64);
}

/// Pushes that race for one branch inside batches: exactly one wins, every
/// other one is refused as a lost race (its compare-and-swap failed alone
/// and its recomputed verdict is a non-fast-forward), and the pushes to
/// other branches committed in the same batches all land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_compare_and_swap_in_a_batch_fails_only_that_push() {
    const HOT: usize = 6;
    const OWN: usize = 6;
    let s = Srv::start(Some(GroupCommit {
        window: Duration::from_millis(300),
        ..GroupCommit::on()
    }))
    .await;
    let w = s.seeded().await;
    let mut hot = Vec::new();
    for i in 0..HOT {
        hot.push(branch(&w, &format!("hot{i}"), &format!("hot {i}\n")));
    }
    let mut own = Vec::new();
    for i in 0..OWN {
        own.push(branch(&w, &format!("own{i}"), &format!("own {i}\n")));
    }
    let mut handles = Vec::new();
    for (src, dst) in (0..HOT)
        .map(|i| (format!("hot{i}"), "hot".to_owned()))
        .chain((0..OWN).map(|i| (format!("own{i}"), format!("own{i}"))))
    {
        let w = Work {
            path: w.path.clone(),
            home: w.home.clone(),
        };
        let url = s.url.clone();
        handles.push(tokio::spawn(async move {
            push(&w, &url, &format!("refs/heads/{src}:refs/heads/{dst}")).await
        }));
    }
    let mut results = Vec::new();
    for h in handles {
        let (ok, out, _) = h.await.unwrap();
        results.push((ok, out));
    }
    let (hot_results, own_results) = results.split_at(HOT);
    let winners: Vec<usize> = (0..HOT).filter(|&i| hot_results[i].0).collect();
    assert_eq!(
        winners.len(),
        1,
        "exactly one push wins `hot`: {hot_results:#?}"
    );
    for (i, (ok, out)) in hot_results.iter().enumerate() {
        if !ok {
            assert!(
                RACE_LOSS_REASONS.iter().any(|r| out.contains(r)),
                "hot push {i} is refused as a lost race: {out}"
            );
        }
    }
    assert_eq!(s.tip("refs/heads/hot").await, Some(hot[winners[0]].clone()));
    for (i, (ok, out)) in own_results.iter().enumerate() {
        assert!(ok, "own push {i}: {out}");
        assert_eq!(
            s.tip(&format!("refs/heads/own{i}")).await,
            Some(own[i].clone())
        );
    }
    let stats = s.svc.group_commit_stats().unwrap();
    assert!(
        stats.commits > stats.commit_batches,
        "pushes shared commit transactions: {stats:?}"
    );
    assert_eq!(stats.failed_batches, 0);
    assert_eq!(*s.meta.not_durable.lock().unwrap(), Vec::<String>::new());
    // Everything fetches complete.
    git_ok(
        &s.home,
        s.dir.path(),
        &["clone", "-q", "--mirror", &s.url, "c"],
    );
    git_ok(
        &s.home,
        &s.dir.path().join("c"),
        &["fsck", "--strict", "--no-dangling"],
    );
}

/// A batch whose transaction fails refuses every push in it: none is
/// acknowledged and none of their refs moves. The same pushes land once
/// the store works again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_batch_refuses_every_push_in_it() {
    const N: usize = 4;
    let s = Srv::start(Some(GroupCommit {
        window: Duration::from_millis(300),
        ..GroupCommit::on()
    }))
    .await;
    let w = s.seeded().await;
    let mut tips = Vec::new();
    for i in 0..N {
        tips.push(branch(&w, &format!("b{i}"), &format!("b {i}\n")));
    }
    let run = || {
        let mut handles = Vec::new();
        for i in 0..N {
            let w = Work {
                path: w.path.clone(),
                home: w.home.clone(),
            };
            let url = s.url.clone();
            handles.push(tokio::spawn(async move {
                push(&w, &url, &format!("refs/heads/b{i}:refs/heads/b{i}")).await
            }));
        }
        handles
    };
    s.meta.fail_batches.store(true, Ordering::SeqCst);
    for (i, h) in run().into_iter().enumerate() {
        let (ok, out, _) = h.await.unwrap();
        assert!(!ok, "push {i} must not be acknowledged: {out}");
    }
    for i in 0..N {
        assert_eq!(s.tip(&format!("refs/heads/b{i}")).await, None, "b{i}");
    }
    let failed = s.svc.group_commit_stats().unwrap().failed_batches;
    assert!(failed >= 1, "{failed} failed batches");
    assert_eq!(
        s.metric("aspen_push_group_failed_batches_total").await,
        Some(failed as f64)
    );

    s.meta.fail_batches.store(false, Ordering::SeqCst);
    for (i, h) in run().into_iter().enumerate() {
        let (ok, out, _) = h.await.unwrap();
        assert!(ok, "push {i} after recovery: {out}");
        assert_eq!(
            s.tip(&format!("refs/heads/b{i}")).await.as_ref(),
            Some(&tips[i])
        );
    }
    assert_eq!(*s.meta.not_durable.lock().unwrap(), Vec::<String>::new());
    let stats = s.svc.group_commit_stats().unwrap();
    assert_eq!(
        s.metric("aspen_push_group_commit_pushes_total").await,
        Some(stats.commits as f64)
    );
}
