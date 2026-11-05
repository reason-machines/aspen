//! Issue #188: a transient metadata-store error while mirror mode's worker
//! records a `mirror_push` job's outcome, or while it claims a job, must not
//! leave the job leased to a live worker that is no longer running it
//! (until the 15-minute lease runs out). The same gap in the compaction
//! worker was issue #131.
//!
//! The store is SQLite behind [`Flaky`], which fails chosen job calls with
//! [`StoreError::Unavailable`] the way a database failover does: either
//! before the statement reaches the database, or after it committed, with
//! the answer lost. The repository is not a mirror (or, with
//! `Mirrors::down` set, its mirror settings cannot be read), so a job
//! completes (or fails) at once without reaching an upstream.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
use aspen_mirror::{
    Applied, Credentials, JobWorker, LedgerEntry, LocalRepo, Metrics, Mirror, MirrorConfig,
    MirrorRecord, MirrorStore, RefUpdate,
};

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
    /// When the last of `updates_refused` was refused.
    last_refused_at: Mutex<Option<Instant>>,
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
    async fn sweep_orphan_packs(
        &self,
        grace: Duration,
        limit: u32,
    ) -> Result<Vec<PackRecord>, StoreError> {
        self.inner.sweep_orphan_packs(grace, limit).await
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
                *f.last_refused_at.lock().unwrap() = Some(Instant::now());
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

/// A mirror store with no mirrors, whose reads fail while `down` is set.
/// The job only reads a repository's mirror settings; nothing else is
/// called.
#[derive(Debug, Clone, Default)]
struct Mirrors {
    down: Arc<AtomicBool>,
}

fn unused() -> StoreError {
    StoreError::Corrupt {
        what: "not used by these tests".into(),
    }
}

impl MirrorStore for Mirrors {
    async fn create_mirror(
        &self,
        _: &RepoName,
        _: &RepoSettings,
        _: impl FnOnce(RepoId) -> MirrorRecord + Send,
    ) -> Result<Repo, StoreError> {
        Err(unused())
    }
    async fn mirror(&self, _: RepoId) -> Result<Option<MirrorRecord>, StoreError> {
        if self.down.load(Ordering::SeqCst) {
            return Err(blip());
        }
        Ok(None)
    }
    async fn mirrors(&self) -> Result<Vec<MirrorRecord>, StoreError> {
        Ok(Vec::new())
    }
    async fn delete_mirror(&self, _: RepoId) -> Result<bool, StoreError> {
        Err(unused())
    }
    async fn record_sync(
        &self,
        _: RepoId,
        _: UnixMicros,
        _: Option<&str>,
    ) -> Result<(), StoreError> {
        Err(unused())
    }
    async fn ledger(&self, _: RepoId) -> Result<Vec<(RefName, LedgerEntry)>, StoreError> {
        Err(unused())
    }
    async fn ledger_entry(&self, _: RepoId, _: &RefName) -> Result<LedgerEntry, StoreError> {
        Err(unused())
    }
    async fn set_ledger(
        &self,
        _: RepoId,
        _: &RefName,
        _: Option<ObjectId>,
        _: LedgerEntry,
        _: UnixMicros,
    ) -> Result<bool, StoreError> {
        Err(unused())
    }
    async fn conflict_count(&self) -> Result<u64, StoreError> {
        Ok(0)
    }
}

/// Local repository operations; no test reaches them.
#[derive(Debug)]
struct NoLocal;

impl LocalRepo for NoLocal {
    async fn apply(
        &self,
        _: &Repo,
        _: &RefUpdate,
        _: Option<&Path>,
    ) -> Result<Applied, StoreError> {
        Err(unused())
    }
    async fn write_pack(
        &self,
        _: &Repo,
        _: ObjectId,
        _: &[ObjectId],
        _: &Path,
    ) -> Result<(), StoreError> {
        Err(unused())
    }
}

type TestMirror = Mirror<Flaky, Mirrors, NoLocal>;

struct Env {
    _dir: tempfile::TempDir,
    store: SqlStore,
    faults: Arc<Faults>,
    mirrors: Mirrors,
    repo: RepoId,
    version: RefVersionId,
    job: JobId,
}

/// A repository with one version of `main` and a due `mirror_push` job on
/// it.
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
    let version = versions[0].id;
    let job = store
        .enqueue_jobs(&[NewJob {
            repo: repo.id,
            kind: JobKind::MirrorPush { version },
            run_after: UnixMicros(0),
        }])
        .await
        .unwrap()[0];
    Env {
        _dir: dir,
        store,
        faults: Arc::new(Faults::default()),
        mirrors: Mirrors::default(),
        repo: repo.id,
        version,
        job,
    }
}

impl Env {
    /// The mirror service over the flaky store, with job leases of `lease`.
    fn mirror(&self, lease: Duration) -> TestMirror {
        let mut config = MirrorConfig::new(self._dir.path().join("mirror"));
        config.job_lease = lease;
        let meta = Flaky {
            inner: self.store.clone(),
            faults: Arc::clone(&self.faults),
        };
        Mirror::new(
            meta,
            self.mirrors.clone(),
            NoLocal,
            Credentials::default(),
            config,
            Arc::new(Metrics::default()),
        )
        .unwrap()
    }

    async fn job(&self) -> Job {
        self.store.job(self.job).await.unwrap().unwrap()
    }
}

const LEASE: Duration = Duration::from_secs(600);

/// Invariant: a `Complete` that fails before it reaches the database is
/// sent again, so the job is recorded as done while the worker still holds
/// its lease, not left running under it.
#[tokio::test]
async fn an_outcome_the_store_never_received_is_sent_again() {
    let e = env().await;
    e.faults.update_unsent.store(2, Ordering::SeqCst);
    let ran = e
        .mirror(LEASE)
        .run_one_job(&mut JobWorker::new("worker-1"))
        .await;
    assert!(matches!(ran, Ok(true)), "{ran:?}");
    assert_eq!(e.job().await.state, JobState::Done);
    assert_eq!(e.faults.updates_sent.load(Ordering::SeqCst), 1);
}

/// Invariant: a `Fail` is retried the same way: the job is requeued with
/// backoff (attempt counted), not left running under the lease.
#[tokio::test]
async fn a_failure_the_store_never_received_is_sent_again() {
    let e = env().await;
    e.mirrors.down.store(true, Ordering::SeqCst);
    e.faults.update_unsent.store(2, Ordering::SeqCst);
    let ran = e
        .mirror(LEASE)
        .run_one_job(&mut JobWorker::new("worker-1"))
        .await;
    assert!(matches!(ran, Ok(true)), "{ran:?}");
    let job = e.job().await;
    assert!(matches!(job.state, JobState::Queued { .. }), "{job:?}");
    assert_eq!(job.attempts, 1);
    assert!(job.last_error.is_some(), "{job:?}");
    assert_eq!(e.faults.updates_sent.load(Ordering::SeqCst), 1);
}

/// Invariant: an outcome update that committed but whose answer was lost
/// is sent again; the store refuses the repeat (the job is no longer
/// running under this attempt), and the worker takes that as recorded.
#[tokio::test]
async fn an_outcome_whose_answer_was_lost_counts_as_recorded() {
    let e = env().await;
    e.faults.update_answer_lost.store(1, Ordering::SeqCst);
    let ran = e
        .mirror(LEASE)
        .run_one_job(&mut JobWorker::new("worker-1"))
        .await;
    assert!(matches!(ran, Ok(true)), "{ran:?}");
    assert_eq!(e.job().await.state, JobState::Done);
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
    let ran = e
        .mirror(lease)
        .run_one_job(&mut JobWorker::new("worker-1"))
        .await;
    assert!(
        matches!(ran, Err(StoreError::Unavailable { .. })),
        "{ran:?}"
    );
    let elapsed = started.elapsed();
    assert!(elapsed < lease + Duration::from_secs(5), "{elapsed:?}");
    // These bounds hold for any doubling backoff whose first wait is small
    // next to the lease, not only today's (50 ms doubling to 5 s, which
    // sleeps 50 + 100 + 200 + 400 + 800 ms and stops before the 1.6 s wait
    // that would end past the 2 s lease). A doubling wait, capped or not,
    // is at most the time already waited plus the first wait, and the
    // worker stops only when the next wait would pass the lease end, so its
    // last attempt comes at least (lease - first wait) / 2 after the claim:
    // a third of the lease for any first wait up to a third of it.
    let refused = e.faults.updates_refused.load(Ordering::SeqCst);
    assert!(refused >= 2, "it retried, {refused} attempts");
    let last = e
        .faults
        .last_refused_at
        .lock()
        .unwrap()
        .expect("an attempt was refused");
    let last = last.duration_since(started);
    assert!(
        last >= lease / 3,
        "its last attempt came near the lease end: {last:?} of {lease:?}"
    );
    assert_eq!(e.faults.updates_sent.load(Ordering::SeqCst), 0);
    assert!(
        matches!(e.job().await.state, JobState::Running { .. }),
        "the lease ends on its own"
    );
}

/// Invariant: when a claim committed but its answer was lost, the worker
/// gives the job back before its next claim, so the job runs now instead of
/// after the lease. The lost attempt still counts. Only `mirror_push` jobs
/// are given back: a compaction job leased under the same name (a
/// compaction worker may share the mirror worker's name) keeps its lease.
#[tokio::test]
async fn a_claim_whose_answer_was_lost_is_released_and_run() {
    let e = env().await;
    let compact = e
        .store
        .enqueue_jobs(&[NewJob {
            repo: e.repo,
            kind: JobKind::Compact { version: e.version },
            run_after: UnixMicros(0),
        }])
        .await
        .unwrap()[0];
    let claimed = e
        .store
        .claim_job(&[JobTag::Compact], "worker-1", LEASE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, compact);

    e.faults.claim_answer_lost.store(1, Ordering::SeqCst);
    let mirror = e.mirror(LEASE);
    let mut w = JobWorker::new("worker-1");
    let first = mirror.run_one_job(&mut w).await;
    assert!(
        matches!(first, Err(StoreError::Unavailable { .. })),
        "{first:?}"
    );
    assert!(matches!(e.job().await.state, JobState::Running { .. }));

    let second = mirror.run_one_job(&mut w).await;
    assert!(matches!(second, Ok(true)), "{second:?}");
    let job = e.job().await;
    assert_eq!(job.state, JobState::Done);
    assert_eq!(job.attempts, 2, "the lost claim's attempt counts");

    let compact = e.store.job(compact).await.unwrap().unwrap();
    assert!(
        matches!(&compact.state, JobState::Running { owner, .. } if owner == "worker-1"),
        "{compact:?}"
    );
    assert_eq!(compact.attempts, 1);
}
