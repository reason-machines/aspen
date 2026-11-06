//! Gauges read from the metadata database (node C5): the job queue and the
//! closure of each repository's current trunk version.
//!
//! These describe shared state, so every server instance reports the same
//! values; aggregate them with `max` across instances. They are read when
//! `/-/metrics` is scraped, at most once per refresh interval (concurrent
//! scrapes share one read), so the database sees one small query pair per
//! interval per instance whatever the number of scrapers. When a read fails,
//! or takes longer than its timeout (a hung database must not hold every
//! scrape on the lock), the last values are kept, `aspen_meta_stats_up`
//! drops to 0 and `aspen_meta_stats_failures_total` counts it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aspen_core::error::StoreError;
use aspen_core::meta::JobTag;
use aspen_meta::{JobCount, OpenJobState, SqlStore, TrunkClosures};
use tokio::sync::Mutex;

use super::expo::{self, Kind};

/// Reads and caches the database gauges.
#[derive(Debug)]
pub struct StoreGauges {
    store: SqlStore,
    refresh: Duration,
    timeout: Duration,
    state: Mutex<State>,
    failures: AtomicU64,
}

#[derive(Debug, Default)]
struct State {
    /// When the last read was attempted.
    tried: Option<Instant>,
    /// Whether it succeeded.
    up: bool,
    /// The last successful read.
    last: Option<Snapshot>,
}

#[derive(Debug, Clone)]
struct Snapshot {
    /// Wall-clock time of the read, in microseconds since the epoch.
    at_micros: i64,
    jobs: Vec<JobCount>,
    trunks: TrunkClosures,
}

fn now_micros() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

impl StoreGauges {
    /// Gauges over `store`, read again when the last read is older than
    /// `refresh` (zero reads on every scrape). A read that takes longer
    /// than `timeout` is abandoned and counts as a failure.
    pub fn new(store: SqlStore, refresh: Duration, timeout: Duration) -> Self {
        Self {
            store,
            refresh,
            timeout,
            state: Mutex::new(State::default()),
            failures: AtomicU64::new(0),
        }
    }

    async fn read(&self) -> Result<Snapshot, StoreError> {
        let jobs = self.store.job_counts().await?;
        let trunks = self.store.trunk_closures().await?;
        Ok(Snapshot {
            at_micros: now_micros(),
            jobs,
            trunks,
        })
    }

    /// Refreshes if due, then writes the families.
    pub async fn render(&self, out: &mut String) {
        let mut state = self.state.lock().await;
        if state.tried.is_none_or(|t| t.elapsed() >= self.refresh) {
            state.tried = Some(Instant::now());
            let read = tokio::time::timeout(self.timeout, self.read()).await;
            match read {
                Ok(Ok(s)) => {
                    state.up = true;
                    state.last = Some(s);
                }
                Ok(Err(e)) => {
                    state.up = false;
                    self.failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(error = %e, "reading metadata gauges failed");
                }
                Err(_) => {
                    state.up = false;
                    self.failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        timeout_ms = self.timeout.as_millis() as u64,
                        "reading metadata gauges failed: timed out"
                    );
                }
            }
        }
        write(out, &state, self.failures.load(Ordering::Relaxed));
    }
}

fn write(out: &mut String, state: &State, failures: u64) {
    expo::gauge(
        out,
        "aspen_meta_stats_up",
        "1 if the last read of the database gauges (aspen_jobs, aspen_compaction_backlog, aspen_trunk_repos) succeeded; they keep their last values while it is 0.",
        if state.up { 1.0 } else { 0.0 },
    );
    expo::counter(
        out,
        "aspen_meta_stats_failures_total",
        "Reads of the database gauges that failed or timed out.",
        failures,
    );
    let Some(snap) = &state.last else {
        // Never read: leave the families out rather than report zeros
        // nobody measured.
        return;
    };
    expo::gauge(
        out,
        "aspen_meta_stats_timestamp_seconds",
        "When the database gauges were last read (Unix time).",
        snap.at_micros as f64 / 1e6,
    );
    let count = |kind: JobTag, state: OpenJobState| {
        snap.jobs
            .iter()
            .find(|c| c.kind == kind && c.state == state)
            .map_or(0, |c| c.count)
    };
    // Every kind and state, zeros included, so an alert on `> 0` always
    // has a series to evaluate.
    expo::family(
        out,
        "aspen_jobs",
        "Background jobs that are not done, by kind and state. Any dead job pages: see `aspen doctor jobs`.",
        Kind::Gauge,
        JobTag::ALL.iter().flat_map(|k| {
            OpenJobState::ALL.iter().map(move |s| {
                (
                    vec![("kind", k.as_str()), ("state", s.as_str())],
                    count(*k, *s) as f64,
                )
            })
        }),
    );
    expo::family(
        out,
        "aspen_jobs_queued_lag_seconds",
        "How long the oldest due queued job of each kind has been waiting (0 when none is due).",
        Kind::Gauge,
        JobTag::ALL.iter().map(|k| {
            let earliest = snap
                .jobs
                .iter()
                .find(|c| c.kind == *k && c.state == OpenJobState::Queued)
                .and_then(|c| c.earliest_run_after);
            let lag = earliest.map_or(0, |t| snap.at_micros.saturating_sub(t.0).max(0));
            (vec![("kind", k.as_str())], lag as f64 / 1e6)
        }),
    );
    // The compaction backlog: compact jobs waiting or running. Each one is
    // a cover (or a few, at higher levels) not built yet, so fetches of
    // those versions send one pack per version instead of one per cover.
    // Under a steady push rate it stays flat when compaction keeps up and
    // grows when it does not (docs/OBSERVABILITY.md, "Compaction backlog").
    expo::gauge(
        out,
        "aspen_compaction_backlog",
        "Compact jobs not done: queued (due or backing off) plus running. Flat under steady pushes when compaction keeps up; growing means add compaction workers (ASPEN_COMPACT_WORKERS, or more ASPEN_ROLE=compact instances).",
        (count(JobTag::Compact, OpenJobState::Queued)
            + count(JobTag::Compact, OpenJobState::Running)) as f64,
    );
    expo::family(
        out,
        "aspen_trunk_repos",
        "Repositories by the closure of their current trunk version. Any wider one makes every incremental fetch of that repository a full clone: investigate.",
        Kind::Gauge,
        [
            (vec![("closure", "exact")], snap.trunks.exact as f64),
            (vec![("closure", "wider")], snap.trunks.wider as f64),
        ],
    );
}
