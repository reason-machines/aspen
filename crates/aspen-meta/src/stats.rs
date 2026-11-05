//! Read-only summaries for metrics and `aspen doctor` (node C5).
//!
//! These are inherent [`SqlStore`] methods, not [`aspen_core::meta::MetaStore`]
//! methods: only the composition roots (the server's metrics endpoint and
//! the CLI) call them, and they may name the concrete store, like the
//! dead-job procedures in `jobs.rs`.
//!
//! Each query is bounded by something small: [`SqlStore::job_counts`] reads
//! only jobs that are not done (the rows the partial unique index
//! `jobs_open_version` holds), and [`SqlStore::trunk_closures`] reads one
//! `refs` row and one `ref_versions` row per repository.

use aspen_core::error::StoreError;
use aspen_core::id::UnixMicros;
use aspen_core::meta::JobTag;

use crate::error::{corrupt, store_error};
use crate::rows::get;
use crate::store::SqlStore;

/// The state of a job that is not done: the states an operator watches.
/// `done` jobs are never counted; the table keeps every one of them, so
/// counting them would scan the whole table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OpenJobState {
    /// Waiting to run (possibly not due yet).
    Queued,
    /// Claimed by a worker.
    Running,
    /// Failed too often; needs an operator (`aspen doctor jobs`).
    Dead,
}

impl OpenJobState {
    /// Every open state.
    pub const ALL: [OpenJobState; 3] = [Self::Queued, Self::Running, Self::Dead];

    /// The value stored in `jobs.state`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Dead => "dead",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|x| x.as_str() == s)
    }
}

/// How many jobs of one kind are in one open state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobCount {
    /// The job kind.
    pub kind: JobTag,
    /// The state.
    pub state: OpenJobState,
    /// Jobs of this kind in this state; at least 1.
    pub count: u64,
    /// For [`OpenJobState::Queued`], the earliest `run_after` among them:
    /// if it is in the past, the queue is behind by at least that much.
    /// `None` for the other states.
    pub earliest_run_after: Option<UnixMicros>,
}

/// Repositories by the closure of their current trunk version
/// (`ref_versions.closure_exact`).
///
/// A wider trunk version makes every later trunk version, and every branch
/// built on trunk, wider for good, which turns every incremental fetch of
/// the repository into a full clone (decisions log, N1 review). In
/// ordinary use trunk stays exact, so `wider > 0` means something went
/// wrong and an operator should look.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrunkClosures {
    /// Repositories whose current trunk version is exact.
    pub exact: u64,
    /// Repositories whose current trunk version is wider than its tip.
    pub wider: u64,
}

fn count(row: &sqlx::any::AnyRow, col: &str) -> Result<u64, StoreError> {
    let n: i64 = get(row, col)?;
    u64::try_from(n).map_err(|_| corrupt(format!("negative count in {col}")))
}

impl SqlStore {
    /// Jobs that are not done, counted per kind and state, ordered by kind
    /// then state. Kinds and states without jobs are absent.
    pub async fn job_counts(&self) -> Result<Vec<JobCount>, StoreError> {
        let rows = sqlx::query(
            "SELECT kind, state, COUNT(*) AS n, \
             MIN(CASE WHEN state = 'queued' THEN run_after_us END) AS earliest \
             FROM jobs WHERE state <> 'done' GROUP BY kind, state",
        )
        .fetch_all(&mut *self.admit().await?)
        .await
        .map_err(store_error)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let kind: String = get(row, "kind")?;
            let kind: JobTag = kind
                .parse()
                .map_err(|e| corrupt(format!("jobs.kind: {e}")))?;
            let state: String = get(row, "state")?;
            let state = OpenJobState::parse(&state)
                .ok_or_else(|| corrupt(format!("jobs.state: unknown value {state:?}")))?;
            let earliest: Option<i64> = get(row, "earliest")?;
            out.push(JobCount {
                kind,
                state,
                count: count(row, "n")?,
                earliest_run_after: earliest.map(UnixMicros),
            });
        }
        out.sort_by_key(|c| (JobTag::ALL.iter().position(|t| *t == c.kind), c.state));
        Ok(out)
    }

    /// Repositories with a live trunk, by whether the current trunk
    /// version's closure is exact. Repositories whose trunk has no version
    /// yet are not counted.
    pub async fn trunk_closures(&self) -> Result<TrunkClosures, StoreError> {
        let row = sqlx::query(
            "SELECT \
             COALESCE(SUM(CASE WHEN v.closure_exact = 1 THEN 1 ELSE 0 END), 0) AS exact, \
             COALESCE(SUM(CASE WHEN v.closure_exact = 0 THEN 1 ELSE 0 END), 0) AS wider \
             FROM repos r \
             JOIN refs f ON f.repo_id = r.id AND f.ref_name = r.trunk_ref \
             JOIN ref_versions v ON v.id = f.version_id \
             WHERE v.tip_oid IS NOT NULL",
        )
        .fetch_one(&mut *self.admit().await?)
        .await
        .map_err(store_error)?;
        Ok(TrunkClosures {
            exact: count(&row, "exact")?,
            wider: count(&row, "wider")?,
        })
    }
}
