//! The job queue.
//!
//! [`Job::apply`] in `aspen-core` is the queue's specification. The store
//! reads the job row under a lock (`FOR UPDATE` on Postgres, the
//! `BEGIN IMMEDIATE` write lock on SQLite), computes the next state with
//! `Job::apply`, and writes it back with an update that is also fenced on
//! the state, attempt and lease owner it read. So the SQL cannot drift from
//! the state machine, and a stale worker cannot finish a newer attempt.

use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::id::JobId;
use aspen_core::meta::{CompactionHeat, Job, JobEvent, JobState, JobTag, MAX_JOB_HEAT, NewJob};
use sqlx::AssertSqlSafe;

use crate::error::{conflict, store_error};
use crate::rows::{self, JOB_COLS, job_target};
use crate::sql::placeholders;
use crate::store::SqlStore;

/// Enqueues one job idempotently inside a transaction: if a job of the same
/// kind and target is not done (queued, running or dead), returns its ID
/// instead of adding another (partial unique indexes `jobs_open_version`
/// and `jobs_open_pack`).
pub(crate) async fn enqueue(
    conn: &mut crate::conn::Conn,
    job: &NewJob,
) -> Result<JobId, StoreError> {
    let (version, pack) = job_target(&job.kind);
    let tag = job.kind.tag().as_str();
    // The open job can finish between the insert and the select; then the
    // slot is free again and the next insert succeeds. Two rounds always
    // suffice unless another writer keeps finishing and re-adding the job.
    for _ in 0..4 {
        let inserted = sqlx::query(
            "INSERT INTO jobs (repo_id, kind, ref_version_id, pack_id, run_after_us) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING RETURNING id",
        )
        .bind(job.repo.get())
        .bind(tag)
        .bind(version)
        .bind(pack)
        .bind(job.run_after.0)
        .fetch_optional(&mut *conn)
        .await
        .map_err(store_error)?;
        let row = match inserted {
            Some(row) => Some(row),
            None => {
                let target = if version.is_some() {
                    "ref_version_id"
                } else {
                    "pack_id"
                };
                sqlx::query(AssertSqlSafe(format!(
                    "SELECT id FROM jobs WHERE kind = $1 AND {target} = $2 AND state <> 'done'"
                )))
                .bind(tag)
                .bind(version.or(pack))
                .fetch_optional(&mut *conn)
                .await
                .map_err(store_error)?
            }
        };
        if let Some(row) = row {
            let id: i64 = rows::get(&row, "id")?;
            return JobId::new(id).map_err(|e| crate::error::corrupt(format!("jobs.id: {e}")));
        }
    }
    Err(StoreError::Unavailable {
        reason: format!("enqueue {tag}: the open job kept changing; retry"),
    })
}

/// Writes `next` over `prev`, fenced on what was read: the row must still
/// be in `prev`'s state, attempt and lease owner.
async fn write_transition(
    conn: &mut crate::conn::Conn,
    prev: &Job,
    next: &Job,
) -> Result<(), StoreError> {
    let (owner, lease, run_after) = match &next.state {
        JobState::Queued { run_after } => (None, None, Some(run_after.0)),
        JobState::Running { owner, lease_until } => {
            (Some(owner.clone()), Some(lease_until.0), None)
        }
        JobState::Done | JobState::Dead => (None, None, None),
    };
    let prev_owner = match &prev.state {
        JobState::Running { owner, .. } => owner.clone(),
        _ => String::new(),
    };
    let updated = sqlx::query(
        "UPDATE jobs SET state = $1, attempts = $2, lease_owner = $3, lease_expires_us = $4, \
         last_error = $5, run_after_us = COALESCE($6, run_after_us) \
         WHERE id = $7 AND state = $8 AND attempts = $9 AND COALESCE(lease_owner, '') = $10",
    )
    .bind(next.state.as_str())
    .bind(i64::from(next.attempts))
    .bind(owner)
    .bind(lease)
    .bind(next.last_error.clone())
    .bind(run_after)
    .bind(prev.id.get())
    .bind(prev.state.as_str())
    .bind(i64::from(prev.attempts))
    .bind(prev_owner)
    .execute(&mut *conn)
    .await
    .map_err(store_error)?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        // The row is locked, so this means another writer bypassed the lock.
        Err(StoreError::Unavailable {
            reason: format!("job {} changed while locked; retry", prev.id),
        })
    }
}

/// [`aspen_core::meta::MetaStore::claim_job`]: the due job with the
/// highest priority (fetch heat, [`add_heat`]), then the earliest due time
/// (a queued job's `run_after`, or an expired lease's end), ties broken by
/// job ID. A reclaim that [`Job::apply`] parks as dead is written and
/// skipped.
pub(crate) async fn claim(
    store: &SqlStore,
    tags: &[JobTag],
    owner: &str,
    lease: Duration,
) -> Result<Option<Job>, StoreError> {
    if tags.is_empty() {
        return Ok(None);
    }
    let kinds = placeholders(2, tags.len());
    let lock = store.skip_locked();
    let queued = format!(
        "SELECT {JOB_COLS}, priority, run_after_us AS due FROM jobs \
         WHERE state = 'queued' AND run_after_us <= $1 AND kind IN ({kinds}) \
         ORDER BY priority DESC, run_after_us, id LIMIT 1{lock}"
    );
    let expired = format!(
        "SELECT {JOB_COLS}, priority, lease_expires_us AS due FROM jobs \
         WHERE state = 'running' AND lease_expires_us <= $1 AND kind IN ({kinds}) \
         ORDER BY priority DESC, lease_expires_us, id LIMIT 1{lock}"
    );
    store
        .write(async |tx| {
            // Read the clock only once the transaction has started: on
            // SQLite, `BEGIN IMMEDIATE` can wait for the write lock, and a
            // time read before that wait would end the lease early by the
            // length of the wait.
            let now = store.now();
            claim_in(tx, tags, owner, lease, now, [&queued, &expired]).await
        })
        .await
}

async fn claim_in(
    conn: &mut crate::conn::Conn,
    tags: &[JobTag],
    owner: &str,
    lease: Duration,
    now: aspen_core::id::UnixMicros,
    queries: [&String; 2],
) -> Result<Option<Job>, StoreError> {
    loop {
        // The best candidate of each query, then the better of the two.
        //
        // On Postgres each query locks the row it returns (`SKIP LOCKED`),
        // so the candidate that loses the comparison stays locked, unused,
        // until this transaction ends (milliseconds). Meanwhile another
        // worker's claim skips it and takes a later job, or finds none and
        // polls again; no job is lost or claimed twice. A single query over
        // both states could lock just one row, but it would have to sort
        // every due job by a computed due time instead of reading the first
        // entry of each state's index, on every claim.
        let mut best: Option<((std::cmp::Reverse<i64>, i64), Job)> = None;
        for sql in queries {
            let mut q = sqlx::query(AssertSqlSafe(sql.clone())).bind(now.0);
            for t in tags {
                q = q.bind(t.as_str());
            }
            if let Some(row) = q.fetch_optional(&mut *conn).await.map_err(store_error)? {
                let due: i64 = rows::get(&row, "due")?;
                let priority: i64 = rows::get(&row, "priority")?;
                let key = (std::cmp::Reverse(priority), due);
                let job = rows::job(&row)?;
                let better = match &best {
                    None => true,
                    Some((k, j)) => (key, job.id) < (*k, j.id),
                };
                if better {
                    best = Some((key, job));
                }
            }
        }
        let Some((_, job)) = best else {
            return Ok(None);
        };
        let next = job
            .apply(JobEvent::Claim {
                owner: owner.to_owned(),
                now,
                lease,
            })
            .map_err(|e| StoreError::Unavailable {
                reason: format!("job {} not claimable after selection: {e}", job.id),
            })?;
        write_transition(conn, &job, &next).await?;
        // A reclaim that parks the job as dead is kept; look for the next.
        if next.state != JobState::Dead {
            return Ok(Some(next));
        }
    }
}

/// [`aspen_core::meta::MetaStore::add_compaction_heat`]: one update per
/// entry, in one transaction. Each scans the repository's queued jobs (the
/// backlog, not the ref's history) and checks each `compact` job's version
/// by its primary key.
pub(crate) async fn add_heat(store: &SqlStore, heat: &[CompactionHeat]) -> Result<u64, StoreError> {
    if heat.is_empty() {
        return Ok(0);
    }
    let max = i64::try_from(MAX_JOB_HEAT).expect("MAX_JOB_HEAT fits i64");
    store
        .write(async |tx| {
            let mut raised = 0;
            for h in heat {
                let add = i64::try_from(h.heat.min(MAX_JOB_HEAT)).expect("at most MAX_JOB_HEAT");
                if add == 0 {
                    continue;
                }
                let done = sqlx::query(
                    "UPDATE jobs SET priority = \
                     CASE WHEN priority > $1 - $2 THEN $1 ELSE priority + $2 END \
                     WHERE state = 'queued' AND kind = 'compact' AND repo_id = $3 \
                     AND EXISTS (SELECT 1 FROM ref_versions v WHERE v.id = jobs.ref_version_id \
                     AND v.ref_name = $4 AND v.seq >= $5)",
                )
                .bind(max)
                .bind(add)
                .bind(h.repo.get())
                .bind(h.name.as_str().to_owned())
                .bind(h.from.to_db())
                .execute(&mut **tx)
                .await
                .map_err(store_error)?;
                raised += done.rows_affected();
            }
            Ok(raised)
        })
        .await
}

async fn locked_job(
    store: &SqlStore,
    conn: &mut crate::conn::Conn,
    id: JobId,
) -> Result<Job, StoreError> {
    let sql = format!(
        "SELECT {JOB_COLS} FROM jobs WHERE id = $1{}",
        store.for_update()
    );
    let row = sqlx::query(AssertSqlSafe(sql))
        .bind(id.get())
        .fetch_optional(&mut *conn)
        .await
        .map_err(store_error)?
        .ok_or_else(|| StoreError::NotFound {
            what: format!("job {id}"),
        })?;
    rows::job(&row)
}

/// [`aspen_core::meta::MetaStore::release_jobs`]: each running job of
/// `tags` leased to `owner`, read under lock, goes through [`Job::apply`]
/// with a holder's [`JobEvent::Fail`] and no backoff. The holder's `Fail`
/// does not depend on the lease, so an expired lease is released too; a row
/// `Job::apply` refuses anyway is skipped, never allowed to block the
/// release of the others.
pub(crate) async fn release(
    store: &SqlStore,
    tags: &[JobTag],
    owner: &str,
    reason: &str,
) -> Result<u64, StoreError> {
    if tags.is_empty() {
        return Ok(0);
    }
    let kinds = placeholders(2, tags.len());
    let sql = format!(
        "SELECT {JOB_COLS} FROM jobs \
         WHERE state = 'running' AND lease_owner = $1 AND kind IN ({kinds}) \
         ORDER BY id{}",
        store.for_update()
    );
    store
        .write(async |tx| {
            let now = store.now();
            let mut q = sqlx::query(AssertSqlSafe(sql.clone())).bind(owner);
            for t in tags {
                q = q.bind(t.as_str());
            }
            let held = q.fetch_all(&mut **tx).await.map_err(store_error)?;
            let mut released = 0;
            for row in &held {
                let job = rows::job(row)?;
                let Ok(next) = job.apply(JobEvent::Fail {
                    owner: owner.to_owned(),
                    attempt: job.attempts,
                    now,
                    error: reason.to_owned(),
                    backoff: Duration::ZERO,
                }) else {
                    continue;
                };
                write_transition(tx, &job, &next).await?;
                released += 1;
            }
            Ok(released)
        })
        .await
}

/// [`aspen_core::meta::MetaStore::update_job`].
pub(crate) async fn update(
    store: &SqlStore,
    id: JobId,
    event: JobEvent,
) -> Result<Job, StoreError> {
    if matches!(event, JobEvent::Claim { .. }) {
        return Err(conflict("claims go through claim_job"));
    }
    store
        .write(async |tx| {
            let job = locked_job(store, tx, id).await?;
            let next = job
                .apply(event)
                .map_err(|e| conflict(format!("job {id}: {e}")))?;
            write_transition(tx, &job, &next).await?;
            Ok(next)
        })
        .await
}

/// Job reads and the operator procedures for dead jobs (`crates/aspen-meta/SCHEMA.md`,
/// "jobs"). `aspen doctor jobs` (node C5) calls these.
impl SqlStore {
    /// One job by ID, in any state.
    pub async fn job(&self, id: JobId) -> Result<Option<Job>, StoreError> {
        let sql = format!("SELECT {JOB_COLS} FROM jobs WHERE id = $1");
        sqlx::query(AssertSqlSafe(sql))
            .bind(id.get())
            .fetch_optional(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .as_ref()
            .map(rows::job)
            .transpose()
    }

    /// Dead jobs, oldest first, at most `limit`.
    pub async fn dead_jobs(&self, limit: u32) -> Result<Vec<Job>, StoreError> {
        let sql = format!("SELECT {JOB_COLS} FROM jobs WHERE state = 'dead' ORDER BY id LIMIT $1");
        sqlx::query(AssertSqlSafe(sql))
            .bind(i64::from(limit))
            .fetch_all(&mut *self.admit().await?)
            .await
            .map_err(store_error)?
            .iter()
            .map(rows::job)
            .collect()
    }

    /// Marks a dead job done, for when its work was done some other way.
    /// [`StoreError::NotFound`] if there is no such job,
    /// [`StoreError::Conflict`] if it is not dead.
    pub async fn resolve_dead_job(&self, id: JobId) -> Result<(), StoreError> {
        self.write(async |tx| self.resolve_locked(tx, id).await.map(|_| ()))
            .await
    }

    /// Retries a dead job's work: in one transaction, marks the dead job
    /// done and enqueues a fresh job for the same kind and target, due now.
    /// Returns the new job, which has a new ID and starts at attempt 0.
    ///
    /// The dead row is never reset in place. Workers are fenced on
    /// `(job ID, worker, attempt)`; resetting `attempts` to 0 on the same row
    /// would hand out attempt numbers 1, 2, ... again, so a worker still
    /// running an old attempt of that row (its lease long expired) could
    /// match the fence of a new attempt and complete or fail it. A new row
    /// has a new ID that no old worker holds.
    pub async fn requeue_dead_job(&self, id: JobId) -> Result<Job, StoreError> {
        self.write(async |tx| {
            let dead = self.resolve_locked(tx, id).await?;
            let new_id = enqueue(
                tx,
                &NewJob {
                    repo: dead.repo,
                    kind: dead.kind,
                    run_after: self.now(),
                },
            )
            .await?;
            locked_job(self, tx, new_id).await
        })
        .await
    }

    async fn resolve_locked(
        &self,
        conn: &mut crate::conn::Conn,
        id: JobId,
    ) -> Result<Job, StoreError> {
        let job = locked_job(self, conn, id).await?;
        if job.state != JobState::Dead {
            return Err(conflict(format!(
                "job {id} is {}, not dead",
                job.state.as_str()
            )));
        }
        let done = Job {
            state: JobState::Done,
            ..job.clone()
        };
        write_transition(conn, &job, &done).await?;
        Ok(job)
    }
}
