//! Compaction keeps up with a sustained push rate: the `compact` backlog
//! stays flat however long the ref's history grows.
//!
//! A long-running repository's trunk gains a version per push and a
//! `compact` job per `fanout` versions. If a job's work grows with the
//! ref's history, the worker's throughput falls as the ref grows, and at
//! any fixed push rate the backlog eventually grows without bound: fetches
//! then send one pack per version instead of one per cover, and the
//! metric `aspen_compaction_backlog` climbs. On main before this test,
//! every job read every cover of its ref (`MetaStore::covers`), so a job
//! on a trunk of N versions read about N / 15 cover rows.
//!
//! The harness runs in ticks of simulated time. Each tick commits
//! [`PUSHES_PER_TICK`] versions to `main` (one `compact` job per 16, as the
//! push path enqueues them) and then lets one worker run jobs until it has
//! read [`ROWS_PER_TICK`] metadata rows: a fixed budget of store work per
//! tick, standing in for a database that answers so many rows a second.
//! Counting rows instead of timing them keeps the result exact on any
//! machine. Every version stores its own one-blob pack, so jobs repack and
//! verify real packs as in production; the repack's blob and CPU work is
//! per cover and does not depend on the ref's length, so the budget counts
//! metadata only.
//!
//! Measured with `cargo test -p aspen-compact --test backlog -- --nocapture`
//! (80 ticks, 5,120 versions):
//!
//! - before (main at 0b41ba7: every cover of the ref read per job): rows
//!   per job grew from 33 to 254 as the ref grew; the worker fell behind
//!   at tick 19 and the backlog grew every tick after, to 111 jobs at tick
//!   80 (and still climbing);
//! - after (only the covers inside the range a job builds, keyed): 33
//!   rows per level-1 job at any length (335 for the one level-3 job, at
//!   4,096 versions), the last quarter of jobs read 1.10 times the first
//!   quarter's rows (3,066 against 2,795), and the backlog was 0 at the
//!   end of every tick.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_blob::LocalBlobStore;
use aspen_compact::{CompactConfig, Compactor, JobOutcome, Worker, WorkerOptions};
use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{
    CoverId, JobId, ObjectId, PackChecksum, PackId, RefVersionId, RepoId, Seq, UnixMicros,
};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{
    CommitLookup, CommitOutcome, CommitRow, Cover, Job, JobEvent, JobTag, MetaStore, NewCover,
    NewJob, PackTombstone, PushCommit, RefListQuery, RefWrite, ShallowDepth, ShallowRecorded,
    VersionJob, VersionJobKind,
};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{CoverLevel, NewPack, PackOrigin, PackRecord};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};
use aspen_core::repo::{Repo, RepoSettings};
use aspen_meta::{OpenJobState, SqlStore};
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};

/// Versions committed to `main` per tick: 4 `compact` jobs.
const PUSHES_PER_TICK: u64 = 64;
/// Ticks simulated: 5,120 versions in all.
const TICKS: u64 = 80;
/// Metadata rows the worker may read per tick. With work per job flat
/// (about 20 rows), 4 jobs take a fifth of it; with work growing with the
/// ref, it runs out once a job reads about 100 rows.
const ROWS_PER_TICK: u64 = 400;

/// `SqlStore` that counts the rows its reads return: the metadata work a
/// job does.
#[derive(Debug, Clone)]
struct Counting {
    inner: SqlStore,
    rows: Arc<AtomicU64>,
}

impl Counting {
    fn read(&self, n: usize) {
        self.rows.fetch_add(n as u64, Ordering::Relaxed);
    }
}

impl MetaStore for Counting {
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
        let v = self.inner.ref_version(id).await?;
        self.read(v.iter().count());
        Ok(v)
    }
    async fn versions_by_seq(
        &self,
        repo: RepoId,
        name: &RefName,
        seqs: &[Seq],
    ) -> Result<Vec<RefVersion>, StoreError> {
        let v = self.inner.versions_by_seq(repo, name, seqs).await?;
        self.read(v.len());
        Ok(v)
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
        let v = self.inner.packs(ids).await?;
        self.read(v.len());
        Ok(v)
    }
    async fn commit_push(&self, push: &PushCommit) -> Result<CommitOutcome, StoreError> {
        self.inner.commit_push(push).await
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
    async fn covers(&self, repo: RepoId, name: &RefName) -> Result<Vec<Cover>, StoreError> {
        let v = self.inner.covers(repo, name).await?;
        self.read(v.len());
        Ok(v)
    }
    async fn covers_at(
        &self,
        repo: RepoId,
        name: &RefName,
        level: CoverLevel,
        firsts: &[Seq],
    ) -> Result<Vec<Cover>, StoreError> {
        let v = self.inner.covers_at(repo, name, level, firsts).await?;
        self.read(v.len());
        Ok(v)
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
    async fn add_compaction_heat(
        &self,
        heat: &[aspen_core::meta::CompactionHeat],
    ) -> Result<u64, StoreError> {
        self.inner.add_compaction_heat(heat).await
    }
    async fn release_jobs(
        &self,
        tags: &[JobTag],
        owner: &str,
        reason: &str,
    ) -> Result<u64, StoreError> {
        self.inner.release_jobs(tags, owner, reason).await
    }
    async fn update_job(&self, id: JobId, event: JobEvent) -> Result<Job, StoreError> {
        self.inner.update_job(id, event).await
    }
}

/// Writes a pack holding one blob, `content`, with its `.idx` and entry
/// table, registers it and uploads it: what a push of one new file stores.
async fn store_blob_pack(
    store: &SqlStore,
    blobs: &LocalBlobStore,
    repo: RepoId,
    scratch: &std::path::Path,
    content: &[u8],
) -> PackId {
    let mut pack = b"PACK".to_vec();
    pack.extend(2u32.to_be_bytes());
    pack.extend(1u32.to_be_bytes());
    // Entry header: type 3 (blob) and the size, 4 bits then 7 per byte.
    let mut size = content.len();
    let mut byte = (3u8 << 4) | (size & 15) as u8;
    size >>= 4;
    while size > 0 {
        pack.push(byte | 0x80);
        byte = (size & 0x7f) as u8;
        size >>= 7;
    }
    pack.push(byte);
    // zlib with one stored (uncompressed) deflate block.
    let len = u16::try_from(content.len()).unwrap();
    pack.extend([0x78, 0x01, 0x01]);
    pack.extend(len.to_le_bytes());
    pack.extend((!len).to_le_bytes());
    pack.extend(content);
    let (mut a, mut b) = (1u32, 0u32);
    for &c in content {
        a = (a + u32::from(c)) % 65_521;
        b = (b + a) % 65_521;
    }
    pack.extend(((b << 16) | a).to_be_bytes());
    let mut h = gix_hash::hasher(gix_hash::Kind::Sha1);
    h.update(&pack);
    let trailer: [u8; 20] = h.try_finalize().unwrap().as_slice().try_into().unwrap();
    pack.extend(trailer);

    let work = tempfile::tempdir_in(scratch).unwrap();
    let pack_path = work.path().join("p.pack");
    std::fs::write(&pack_path, &pack).unwrap();
    let limit = 1 << 20;
    let verified = aspen_pack::verify::verify_pack(
        &pack_path,
        work.path(),
        MaxObjectBytes::new(limit).unwrap(),
        MaxInflatedBytes::new(limit).unwrap(),
    )
    .unwrap();
    let idx_path = work.path().join("p.idx");
    std::fs::write(&idx_path, &verified.idx).unwrap();
    let entries = verified.table.encode();
    let entries_path = work.path().join("p.entries");
    std::fs::write(&entries_path, &entries).unwrap();
    let record = store
        .register_pack(&NewPack {
            repo,
            checksum: PackChecksum::from_bytes(trailer),
            size: pack.len() as u64,
            idx_size: verified.idx.len() as u64,
            entries_size: entries.len() as u64,
            object_count: 1,
            ref_delta_count: 0,
            origin: PackOrigin::Push {
                received_bytes: pack.len() as u64,
            },
        })
        .await
        .unwrap();
    let keys = record.keys();
    blobs.upload_file(&keys.pack, &pack_path).await.unwrap();
    blobs.upload_file(&keys.idx, &idx_path).await.unwrap();
    blobs
        .upload_file(&keys.entries, &entries_path)
        .await
        .unwrap();
    record.id
}

/// What one run of the harness saw.
#[derive(Debug)]
struct Run {
    /// Queued and running `compact` jobs at the end of each tick.
    backlog: Vec<u64>,
    /// Rows read by each job, in the order jobs ran.
    rows_per_job: Vec<u64>,
}

async fn backlog_of(store: &SqlStore) -> u64 {
    store
        .job_counts()
        .await
        .unwrap()
        .iter()
        .filter(|c| {
            c.kind == JobTag::Compact
                && matches!(c.state, OpenJobState::Queued | OpenJobState::Running)
        })
        .map(|c| c.count)
        .sum()
}

async fn run() -> Run {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let store = SqlStore::connect(&url).await.unwrap();
    store.migrate().await.unwrap();
    let main = RefName::new("refs/heads/main").unwrap();
    let repo = store
        .create_repo(
            &RepoName::new("acme/mono").unwrap(),
            &RepoSettings::new(main.clone()).unwrap(),
        )
        .await
        .unwrap();
    let spool = dir.path().join("spool");
    std::fs::create_dir_all(&spool).unwrap();
    let rows = Arc::new(AtomicU64::new(0));
    let meta = Counting {
        inner: store.clone(),
        rows: Arc::clone(&rows),
    };
    let config = CompactConfig::new(
        spool,
        MaxObjectBytes::new(1 << 20).unwrap(),
        MaxInflatedBytes::new(1 << 20).unwrap(),
    );
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let scratch = dir.path().join("scratch");
    std::fs::create_dir_all(&scratch).unwrap();
    let mut worker = Worker::new(
        Compactor::new(meta, blobs.clone(), config),
        WorkerOptions::new("backlog-worker"),
    );

    let fanout = aspen_core::meta::CoverLayout::DEFAULT.fanout();
    let mut current: Option<RefVersionId> = None;
    let mut seq = 0u64;
    let mut out = Run {
        backlog: Vec::new(),
        rows_per_job: Vec::new(),
    };
    // Rows the worker read past a tick's budget are taken from the next.
    let mut debt = 0u64;
    for _tick in 0..TICKS {
        for _ in 0..PUSHES_PER_TICK {
            seq += 1;
            let mut tip = [0u8; 20];
            tip[..8].copy_from_slice(&seq.to_be_bytes());
            tip[19] = 1;
            let jobs = if seq.is_multiple_of(fanout) {
                vec![VersionJob {
                    kind: VersionJobKind::Compact,
                    run_after: UnixMicros(0),
                }]
            } else {
                Vec::new()
            };
            let pack = store_blob_pack(
                &store,
                &blobs,
                repo.id,
                &scratch,
                format!("version {seq}\n").as_bytes(),
            )
            .await;
            let CommitOutcome::Committed(v) = store
                .commit_push(&PushCommit {
                    repo: repo.id,
                    writes: vec![RefWrite {
                        name: main.clone(),
                        expected: current,
                        value: RefValue::Live {
                            tip: ObjectId::from_bytes(tip),
                            pack: Some(pack),
                            closure: Closure::Exact,
                        },
                        deps: Deps::default(),
                        commits: Vec::new(),
                        jobs,
                    }],
                })
                .await
                .unwrap()
            else {
                panic!("one writer always commits");
            };
            current = Some(v[0].id);
        }
        let mut spent = debt;
        while spent < ROWS_PER_TICK {
            let before = rows.load(Ordering::Relaxed);
            match worker.run_one().await.unwrap() {
                None => break,
                Some(JobOutcome::Done { .. }) => {}
                Some(JobOutcome::Failed { error, .. }) => panic!("a compact job failed: {error}"),
            }
            let used = rows.load(Ordering::Relaxed) - before;
            out.rows_per_job.push(used);
            spent += used;
        }
        debt = spent.saturating_sub(ROWS_PER_TICK);
        out.backlog.push(backlog_of(&store).await);
    }
    out
}

/// Invariant: under a sustained push rate to one ref, the `compact` backlog
/// stays bounded by what one tick enqueues, and a job's metadata work does
/// not grow with the ref's history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_compaction_backlog_stays_flat_under_a_sustained_push_rate() {
    let r = run().await;
    let jobs_per_tick = PUSHES_PER_TICK / 16;
    let quarter = r.backlog.len() / 4;
    eprintln!(
        "backlog per tick (first, last quarter): {:?} ... {:?}",
        &r.backlog[..quarter],
        &r.backlog[r.backlog.len() - quarter..]
    );
    let first = &r.rows_per_job[..10.min(r.rows_per_job.len())];
    let last = &r.rows_per_job[r.rows_per_job.len().saturating_sub(10)..];
    eprintln!(
        "rows per job: first {first:?}, last {last:?}, most {}",
        r.rows_per_job.iter().max().unwrap()
    );
    let peak = *r.backlog.iter().max().unwrap();
    assert!(
        peak <= jobs_per_tick,
        "the backlog peaked at {peak} jobs, above the {jobs_per_tick} one tick enqueues: \
         compaction fell behind ({:?})",
        r.backlog
    );
    assert_eq!(
        *r.backlog.last().unwrap(),
        0,
        "every job enqueued was run by the end"
    );
    // Work per job is amortized constant: a job that ends a level-k cover
    // reads the covers inside it (273 rows for the level-3 job at 4,096),
    // once per 16^k versions. Compare the metadata read over the last
    // quarter of jobs, which holds that level-3 job, with the first.
    let q = r.rows_per_job.len() / 4;
    let early: u64 = r.rows_per_job[..q].iter().sum();
    let late: u64 = r.rows_per_job[r.rows_per_job.len() - q..].iter().sum();
    eprintln!("rows read by the first {q} jobs: {early}, by the last {q}: {late}");
    assert!(
        late * 4 <= early * 5,
        "the last {q} jobs read {late} metadata rows against {early} for the first {q}: \
         per-job work grows with the ref's history"
    );
}
