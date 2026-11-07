//! The background worker: claims `compact`, `index_commits` and
//! `rewrite_pack` jobs and runs sweeps.

use std::future::Future;
use std::time::{Duration, Instant};

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::id::{JobId, UnixMicros};
use aspen_core::meta::{JobEvent, JobKind, JobTag, MetaStore};

use crate::{CompactError, CompactReport, Compactor, GcReport, IndexReport, RewriteReport};

/// How the worker paces itself.
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// The worker's name in job leases; unique per worker. After a claim
    /// whose answer it did not get, the worker gives back every job of the
    /// kinds it claims ([`Worker::TAGS`]) leased under this name, so no other worker
    /// may lease those kinds under it. `aspen serve` and `aspen dev` use
    /// the host name, process ID and a random suffix.
    pub owner: String,
    /// Lease length. The worker renews it every third of this while a job
    /// runs, so a crashed worker's job becomes claimable within one lease.
    /// It also bounds how long the worker keeps retrying a job's outcome
    /// during a store outage; garbage-collection sweeps wait meanwhile.
    pub lease: Duration,
    /// Wait between polls when no job is due.
    pub idle_poll: Duration,
    /// Time between garbage-collection sweeps.
    pub gc_every: Duration,
    /// Whether this worker runs the sweeps. An instance running several
    /// workers lets one sweep: a sweep is safe on any number of workers,
    /// but more of them in one process only repeat the work.
    pub collect_garbage: bool,
}

impl WorkerOptions {
    /// A 10-minute lease, a 5-second idle poll and a sweep every 5 minutes.
    pub fn new(owner: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            lease: Duration::from_secs(600),
            idle_poll: Duration::from_secs(5),
            gc_every: Duration::from_secs(300),
            collect_garbage: true,
        }
    }
}

/// What a finished job did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobReport {
    /// A `compact` job.
    Compact(CompactReport),
    /// An `index_commits` job.
    Index(IndexReport),
    /// A `rewrite_pack` job.
    Rewrite(RewriteReport),
}

/// The end of one job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobOutcome {
    /// Done; the job is marked done.
    Done {
        /// The job.
        job: JobId,
        /// What it did.
        report: JobReport,
    },
    /// Failed; the job is requeued with backoff (or parked as dead after
    /// its last attempt).
    Failed {
        /// The job.
        job: JobId,
        /// Why.
        error: String,
        /// Whether the failure means stored data or the pack writer is
        /// wrong (it pages; C5 exports the metric).
        corruption: bool,
    },
}

/// First wait before recording a job's outcome again after a transient
/// store error; it doubles up to [`OUTCOME_RETRY_MAX`].
const OUTCOME_RETRY_FIRST: Duration = Duration::from_millis(50);
/// Longest wait between attempts to record a job's outcome.
const OUTCOME_RETRY_MAX: Duration = Duration::from_secs(5);

/// Claims `compact`, `index_commits` and `rewrite_pack` jobs
/// ([`Worker::TAGS`]) one at a time and runs a sweep
/// every [`WorkerOptions::gc_every`].
///
/// A transient store error ([`StoreError::Unavailable`]: a failover, a
/// reset connection) must not leave a job leased to this worker while it
/// runs nothing (issue #131), so:
///
/// - recording a job's outcome is retried with backoff for as long as the
///   worker's lease lasts (so during an outage the worker, and its sweeps,
///   wait up to one lease before `run` reports the error). The update is
///   fenced on worker and attempt, so a repeat is safe. One the store
///   refuses after a transient error means the job is no longer running
///   under this attempt: the outcome was written with its answer lost, or
///   the lease ended and the job was reclaimed. Either way it is no longer
///   this worker's, and the job's work is idempotent, so the worker moves
///   on as if the outcome were recorded;
/// - after a claim that failed, the claim may still have committed, so the
///   worker gives back the jobs leased under its name
///   ([`MetaStore::release_jobs`]) before it claims again.
///
/// One worker runs one job at a time: [`Worker::run_one`] and
/// [`Worker::run`] take `&mut self`, and a worker cannot be cloned, so a
/// release never takes back a job another loop of the same worker is
/// running. Separate workers need separate [`WorkerOptions::owner`] names.
#[derive(Debug)]
pub struct Worker<M, B> {
    compactor: Compactor<M, B>,
    options: WorkerOptions,
    /// Set when a claim failed: jobs leased under this worker's name may
    /// exist that it never received. Cleared once they are given back.
    release_pending: bool,
}

impl<M, B> Worker<M, B> {
    /// The job kinds the worker claims. Every kind a production process
    /// enqueues has a worker that claims it: these, and `mirror_push`
    /// (`aspen-mirror`). A kind no worker claims stays queued for good
    /// (reason-machines/aspen#10).
    pub const TAGS: [JobTag; 3] = [JobTag::Compact, JobTag::IndexCommits, JobTag::RewritePack];
}

impl<M, B> Worker<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    /// A worker.
    pub fn new(compactor: Compactor<M, B>, options: WorkerOptions) -> Self {
        Self {
            compactor,
            options,
            release_pending: false,
        }
    }

    /// Claims and runs one due job of a kind in [`Worker::TAGS`]; `None` if
    /// none is due.
    pub async fn run_one(&mut self) -> Result<Option<JobOutcome>, StoreError> {
        // Blob requests are counted under `compaction` (`aspen_blob_*`).
        aspen_core::blob::BlobCaller::Compaction
            .scope(self.claim_and_run())
            .await
    }

    async fn claim_and_run(&mut self) -> Result<Option<JobOutcome>, StoreError> {
        let meta = &self.compactor.meta;
        let owner = &self.options.owner;
        if self.release_pending {
            meta.release_jobs(
                &Self::TAGS,
                owner,
                "given back after a claim whose answer was lost",
            )
            .await?;
            self.release_pending = false;
        }
        // The lease runs from the claim; read the time before it, so this
        // end is never later than the store's.
        let claimed_at = Instant::now();
        let claimed = meta.claim_job(&Self::TAGS, owner, self.options.lease).await;
        self.release_pending = claimed.is_err();
        let Some(job) = claimed? else {
            return Ok(None);
        };
        let mut lease_end = claimed_at + self.options.lease;
        let attempt = job.attempts;
        let work = async {
            match job.kind {
                JobKind::Compact { version } => self
                    .compactor
                    .compact_version(version)
                    .await
                    .map(JobReport::Compact),
                JobKind::IndexCommits { version } => self
                    .compactor
                    .index_version(version)
                    .await
                    .map(JobReport::Index),
                JobKind::RewritePack { pack } => self
                    .compactor
                    .rewrite_pack(pack)
                    .await
                    .map(JobReport::Rewrite),
                other => Err(CompactError::Store(StoreError::Corrupt {
                    what: format!("job {} was claimed as compaction work: {other:?}", job.id),
                })),
            }
        };
        // Run the job, renewing the lease while it runs.
        tokio::pin!(work);
        let mut renew = tokio::time::interval(self.options.lease / 3);
        renew.tick().await;
        let result = loop {
            tokio::select! {
                r = &mut work => break r,
                _ = renew.tick() => {
                    let renewed_at = Instant::now();
                    let event = JobEvent::Renew {
                        owner: owner.clone(),
                        attempt,
                        now: UnixMicros::now(),
                        lease: self.options.lease,
                    };
                    // A failed renewal means the lease is gone, or the
                    // store could not be reached; the job's work is
                    // idempotent, so carry on either way.
                    if meta.update_job(job.id, event).await.is_ok() {
                        lease_end = renewed_at + self.options.lease;
                    }
                }
            }
        };
        match result {
            Ok(report) => {
                self.record(job.id, lease_end, || JobEvent::Complete {
                    owner: owner.clone(),
                    attempt,
                })
                .await?;
                Ok(Some(JobOutcome::Done {
                    job: job.id,
                    report,
                }))
            }
            Err(e) => {
                let error = e.to_string();
                self.record(job.id, lease_end, || JobEvent::Fail {
                    owner: owner.clone(),
                    attempt,
                    now: UnixMicros::now(),
                    error: error.clone(),
                    backoff: backoff(attempt),
                })
                .await?;
                Ok(Some(JobOutcome::Failed {
                    job: job.id,
                    error,
                    corruption: e.is_corruption(),
                }))
            }
        }
    }

    /// Records a job's outcome (`event()`, built anew for each attempt),
    /// retrying a transient store error with backoff until `lease_end`.
    /// After an attempt that failed with [`StoreError::Unavailable`], the
    /// update may or may not have committed; a repeat the store then
    /// refuses ([`StoreError::Conflict`]: the job is no longer running under
    /// this attempt) means it did, or that the lease ended and the job was
    /// reclaimed. Either way the job is no longer this worker's, so this
    /// returns `Ok` without knowing which.
    async fn record(
        &self,
        job: JobId,
        lease_end: Instant,
        event: impl Fn() -> JobEvent,
    ) -> Result<(), StoreError> {
        let meta = &self.compactor.meta;
        let mut wait = OUTCOME_RETRY_FIRST;
        let mut sent_before = false;
        loop {
            match meta.update_job(job, event()).await {
                Ok(_) => return Ok(()),
                Err(StoreError::Conflict { .. }) if sent_before => return Ok(()),
                Err(e @ StoreError::Unavailable { .. }) => {
                    if Instant::now() + wait >= lease_end {
                        return Err(e);
                    }
                    sent_before = true;
                    tokio::time::sleep(wait).await;
                    wait = (wait * 2).min(OUTCOME_RETRY_MAX);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Runs one garbage-collection sweep.
    pub async fn collect_garbage(&self) -> Result<GcReport, crate::CompactError> {
        aspen_core::blob::BlobCaller::Gc
            .scope(self.compactor.collect_garbage())
            .await
    }

    /// Runs jobs and sweeps until `shutdown` completes. Store errors are
    /// reported to `on_error` and retried after the idle poll.
    ///
    /// Sweeps run between jobs, so during a store outage a sweep can slip by
    /// up to one lease ([`WorkerOptions::lease`]) plus the idle poll: that is
    /// how long [`Worker::run_one`] keeps retrying a job's outcome before it
    /// returns the error.
    pub async fn run(
        &mut self,
        shutdown: impl Future<Output = ()>,
        mut on_error: impl FnMut(&dyn std::error::Error),
    ) {
        tokio::pin!(shutdown);
        let mut last_gc: Option<Instant> = None;
        loop {
            if self.options.collect_garbage
                && last_gc.is_none_or(|t| t.elapsed() >= self.options.gc_every)
            {
                if let Err(e) = self.collect_garbage().await {
                    on_error(&e);
                }
                last_gc = Some(Instant::now());
            }
            let idle = match self.run_one().await {
                Ok(Some(_)) => false,
                Ok(None) => true,
                Err(e) => {
                    on_error(&e);
                    true
                }
            };
            if idle {
                tokio::select! {
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(self.options.idle_poll) => {}
                }
            } else if futures_ready(&mut shutdown).await {
                return;
            }
        }
    }
}

/// Whether `f` is already complete, without waiting.
async fn futures_ready<F: Future<Output = ()>>(f: &mut std::pin::Pin<&mut F>) -> bool {
    tokio::select! {
        biased;
        _ = f.as_mut() => true,
        _ = std::future::ready(()) => false,
    }
}

/// 2^attempt seconds, at most 10 minutes.
fn backoff(attempt: u32) -> Duration {
    Duration::from_secs(1u64 << attempt.min(10)).min(Duration::from_secs(600))
}
