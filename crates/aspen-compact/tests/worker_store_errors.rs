//! Issue #131: a transient metadata-store error while the worker records a
//! job's outcome, or while it claims a job, must not leave the job leased
//! to a live worker that is no longer running it (until the 10-minute lease
//! runs out).
//!
//! The store is SQLite behind [`Flaky`], which fails chosen job calls with
//! [`StoreError::Unavailable`] the way a database failover does: either
//! before the statement reaches the database, or after it committed, with
//! the answer lost.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use aspen_blob::LocalBlobStore;
use aspen_compact::{CompactConfig, Compactor, JobOutcome, Worker, WorkerOptions};
use aspen_core::error::StoreError;
use aspen_core::id::{
    CoverId, JobId, ObjectId, PackChecksum, PackId, RefVersionId, RepoId, Seq, UnixMicros,
};
use aspen_core::ingest::SortedIds;
use aspen_core::meta::{
    CommitLookup, CommitOutcome, CommitRow, Cover, Job, JobEvent, JobKind, JobState, JobTag,
    MetaStore, NewCover, NewJob, PackTombstone, PushCommit, RefListQuery, RefWrite, ShallowDepth,
    ShallowRecorded,
};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{CoverLevel, NewPack, PackOrigin, PackRecord, entry_table_len};
use aspen_core::refs::{Closure, Deps, RefValue, RefVersion};
use aspen_core::repo::{Repo, RepoSettings};
use aspen_meta::SqlStore;
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};

/// How [`Flaky`] fails the next job calls.
#[derive(Debug, Default)]
struct Faults {
    /// `Complete` and `Fail` updates refused before they reach the store.
    update_unsent: AtomicU32,
    /// `Complete` and `Fail` updates applied, then answered with an error.
    update_answer_lost: AtomicU32,
    /// Every `Complete` and `Fail` update refused, while set.
    update_down: AtomicBool,
    /// Claims applied, then answered with an error.
    claim_answer_lost: AtomicU32,
    /// Calls that reached the inner store's `update_job` with `Complete`
    /// or `Fail`.
    updates_sent: AtomicU32,
    /// `Complete` and `Fail` updates refused before reaching the store.
    updates_refused: AtomicU32,
}

fn take(n: &AtomicU32) -> bool {
    n.try_update(Ordering::SeqCst, Ordering::SeqCst, |v| v.checked_sub(1))
        .is_ok()
}

fn blip() -> StoreError {
    StoreError::Unavailable {
        reason: "error communicating with database: expected to read 5 bytes, got 0 bytes at EOF"
            .into(),
    }
}

/// `SqlStore` with injected job-call failures ([`Faults`]).
#[derive(Debug, Clone)]
struct Flaky {
    inner: SqlStore,
    faults: Arc<Faults>,
}

impl MetaStore for Flaky {
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
    async fn enqueue_jobs(&self, jobs: &[NewJob]) -> Result<Vec<JobId>, StoreError> {
        self.inner.enqueue_jobs(jobs).await
    }
    async fn claim_job(
        &self,
        tags: &[JobTag],
        owner: &str,
        lease: Duration,
    ) -> Result<Option<Job>, StoreError> {
        let claimed = self.inner.claim_job(tags, owner, lease).await?;
        if claimed.is_some() && take(&self.faults.claim_answer_lost) {
            return Err(blip());
        }
        Ok(claimed)
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
        let outcome = matches!(event, JobEvent::Complete { .. } | JobEvent::Fail { .. });
        if outcome {
            let f = &self.faults;
            if f.update_down.load(Ordering::SeqCst) || take(&f.update_unsent) {
                f.updates_refused.fetch_add(1, Ordering::SeqCst);
                return Err(blip());
            }
            f.updates_sent.fetch_add(1, Ordering::SeqCst);
        }
        let job = self.inner.update_job(id, event).await?;
        if outcome && take(&self.faults.update_answer_lost) {
            return Err(blip());
        }
        Ok(job)
    }
}

struct Env {
    _dir: tempfile::TempDir,
    store: SqlStore,
    faults: Arc<Faults>,
    compactor: Compactor<Flaky, LocalBlobStore>,
    job: JobId,
}

/// A repository with one version of `main` and a due `compact` job on it.
/// The version is the ref's first, which ends no cover, so the job finishes
/// at once without reading blobs.
async fn env() -> Env {
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
    let pack = store
        .register_pack(&NewPack {
            repo: repo.id,
            checksum: PackChecksum::from_bytes([1; 20]),
            size: 100,
            idx_size: 1072 + 28,
            entries_size: entry_table_len(1),
            object_count: 1,
            ref_delta_count: 0,
            origin: PackOrigin::Push { received_bytes: 80 },
        })
        .await
        .unwrap();
    let CommitOutcome::Committed(versions) = store
        .commit_push(&PushCommit {
            repo: repo.id,
            writes: vec![RefWrite {
                name: main,
                expected: None,
                value: RefValue::Live {
                    tip: ObjectId::from_bytes([1; 20]),
                    pack: Some(pack.id),
                    closure: Closure::Exact,
                },
                deps: Deps::default(),
                commits: Vec::new(),
                jobs: Vec::new(),
            }],
        })
        .await
        .unwrap()
    else {
        panic!("the first push commits");
    };
    let job = store
        .enqueue_jobs(&[NewJob {
            repo: repo.id,
            kind: JobKind::Compact {
                version: versions[0].id,
            },
            run_after: UnixMicros(0),
        }])
        .await
        .unwrap()[0];
    let faults = Arc::new(Faults::default());
    let blobs = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
    let spool = dir.path().join("spool");
    std::fs::create_dir_all(&spool).unwrap();
    let config = CompactConfig::new(
        spool,
        MaxObjectBytes::new(1 << 20).unwrap(),
        MaxInflatedBytes::new(1 << 20).unwrap(),
    );
    let meta = Flaky {
        inner: store.clone(),
        faults: Arc::clone(&faults),
    };
    Env {
        compactor: Compactor::new(meta, blobs, config),
        _dir: dir,
        store,
        faults,
        job,
    }
}

impl Env {
    fn worker(&self, lease: Duration) -> Worker<Flaky, LocalBlobStore> {
        let mut options = WorkerOptions::new("worker-1");
        options.lease = lease;
        Worker::new(self.compactor.clone(), options)
    }

    async fn state(&self) -> JobState {
        self.store.job(self.job).await.unwrap().unwrap().state
    }
}

/// Invariant: an outcome update that fails before it reaches the database
/// is sent again, so the job is recorded as done while the worker still
/// holds its lease, not left running under it.
#[tokio::test]
async fn an_outcome_the_store_never_received_is_sent_again() {
    let e = env().await;
    e.faults.update_unsent.store(2, Ordering::SeqCst);
    let outcome = e.worker(Duration::from_secs(600)).run_one().await;
    assert!(
        matches!(outcome, Ok(Some(JobOutcome::Done { job, .. })) if job == e.job),
        "{outcome:?}"
    );
    assert_eq!(e.state().await, JobState::Done);
    assert_eq!(e.faults.updates_sent.load(Ordering::SeqCst), 1);
}

/// Invariant: an outcome update that committed but whose answer was lost
/// is sent again; the store refuses the repeat (the job is no longer
/// running under this attempt), and the worker takes that as recorded.
#[tokio::test]
async fn an_outcome_whose_answer_was_lost_counts_as_recorded() {
    let e = env().await;
    e.faults.update_answer_lost.store(1, Ordering::SeqCst);
    let outcome = e.worker(Duration::from_secs(600)).run_one().await;
    assert!(
        matches!(outcome, Ok(Some(JobOutcome::Done { job, .. })) if job == e.job),
        "{outcome:?}"
    );
    assert_eq!(e.state().await, JobState::Done);
    assert_eq!(e.faults.updates_sent.load(Ordering::SeqCst), 2);
}

/// Invariant: a worker that cannot record an outcome keeps retrying for
/// most of its lease, stops once the next wait would pass the lease's end
/// (another worker may hold the job by then) and reports the store error;
/// it never blocks for longer than the lease.
#[tokio::test]
async fn retrying_an_outcome_stops_when_the_lease_runs_out() {
    let e = env().await;
    e.faults.update_down.store(true, Ordering::SeqCst);
    let lease = Duration::from_secs(2);
    let started = Instant::now();
    let outcome = e.worker(lease).run_one().await;
    assert!(
        matches!(outcome, Err(StoreError::Unavailable { .. })),
        "{outcome:?}"
    );
    let elapsed = started.elapsed();
    assert!(elapsed < lease + Duration::from_secs(5), "{elapsed:?}");
    // Waits of 50 ms doubling: 50 + 100 + 200 + 400 + 800 ms are slept,
    // and the next (1.6 s) would end past the 2 s lease.
    assert!(
        elapsed >= lease / 2,
        "it retried until near the lease end: {elapsed:?}"
    );
    let refused = e.faults.updates_refused.load(Ordering::SeqCst);
    assert!(refused >= 5, "it retried, {refused} attempts");
    assert_eq!(e.faults.updates_sent.load(Ordering::SeqCst), 0);
    assert!(
        matches!(e.state().await, JobState::Running { .. }),
        "the lease ends on its own"
    );
}

/// Invariant: when a claim committed but its answer was lost, the worker
/// gives the job back before its next claim, so the job runs now instead of
/// after the lease (it holds no job, so any lease under its name is one it
/// never received). The lost attempt still counts.
#[tokio::test]
async fn a_claim_whose_answer_was_lost_is_released_and_run() {
    let e = env().await;
    e.faults.claim_answer_lost.store(1, Ordering::SeqCst);
    let mut w = e.worker(Duration::from_secs(600));
    let first = w.run_one().await;
    assert!(
        matches!(first, Err(StoreError::Unavailable { .. })),
        "{first:?}"
    );
    assert!(matches!(e.state().await, JobState::Running { .. }));
    let second = w.run_one().await;
    assert!(
        matches!(second, Ok(Some(JobOutcome::Done { job, .. })) if job == e.job),
        "{second:?}"
    );
    let job = e.store.job(e.job).await.unwrap().unwrap();
    assert_eq!(job.state, JobState::Done);
    assert_eq!(job.attempts, 2, "the lost claim's attempt counts");
}
