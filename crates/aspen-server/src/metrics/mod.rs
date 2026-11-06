//! Metrics, in the Prometheus text format at `/-/metrics` (node C5).
//!
//! Every family the server exports, with its labels, meaning and the alert
//! that watches it, is listed in `docs/OBSERVABILITY.md`; the dashboard and
//! alert rules in `deploy/observability/` use only those names (a test
//! renders the endpoint and checks every name they reference).
//!
//! | Source | Families |
//! |---|---|
//! | [`Metrics`] (this module) | fetches: `aspen_fetch*`, the planner's `PlanStats.visited` (`aspen_fetch_plan_visited_versions`), versions read to plan (`aspen_fetch_plan_loaded_versions`), chain skips, planning time, and the paging counters `aspen_alert_*` |
//! | [`PushMetrics`] | pushes: `aspen_push*`, including committed trunk versions by closure (`aspen_push_trunk_versions_total`) |
//! | [`StoreGauges`] | the metadata database: jobs per kind and state (`aspen_jobs`, which carries the dead-job alert), queue lag, and repositories whose current trunk version is wider (`aspen_trunk_repos`) |
//! | `aspen-express` | `aspen_express_*`, when the server mounts the express route or signs express URLs |
//!
//! Counters live in memory per process and start at zero; the database
//! gauges are the same on every instance (aggregate them with `max`, not
//! `sum`). The names of B2's fetch counters are kept unchanged.
//!
//! Metrics are dependency-free: each family is a few atomics and is
//! rendered by the `expo` module, so recording never allocates or locks on the
//! request path.

pub(crate) mod expo;
pub(crate) mod push;
mod store;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_planner::PlanStats;

pub use expo::CONTENT_TYPE;
pub use push::PushMetrics;
pub(crate) use push::{PushResult, RefOutcome};
pub use store::StoreGauges;

use expo::{DURATION_MICROS, Histogram, MICROS_PER_SECOND};

/// Upper bounds of the `visited` histogram buckets.
const VISITED_BUCKETS: [u64; 10] = [1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262_144];

/// Counters for one server's fetches. All are monotonic.
#[derive(Debug)]
pub struct Metrics {
    fetch_errors: AtomicU64,
    visited: Histogram,
    loaded: Histogram,
    chain_skips: AtomicU64,
    plan_time: Histogram,
    needed_sum: AtomicU64,
    packs_planned: AtomicU64,
    packs_by_uri: AtomicU64,
    packs_stitched: AtomicU64,
    stitched_bytes: AtomicU64,
    haves_dropped: AtomicU64,
    haves_used: AtomicU64,
    forward_base_alerts: AtomicU64,
    corruption_alerts: AtomicU64,
    job_enqueue_failures: AtomicU64,
    /// Versions fetches sent as their own push packs (compaction heat).
    uncompacted_versions: AtomicU64,
    /// Writes of compaction heat to the job queue that failed.
    heat_write_failures: AtomicU64,
    /// Shallow fetches served exact packs: already recorded, or after
    /// waiting for their build (node C8).
    shallow_cached: AtomicU64,
    shallow_built: AtomicU64,
    /// ... and shallow fetches whose full closure was already exact.
    shallow_closure: AtomicU64,
    /// Eligible shallow fetches served the full closure, by reason.
    shallow_fallbacks: [AtomicU64; ShallowFallback::ALL.len()],
    /// Exact shallow pack builds, by result.
    shallow_builds: [AtomicU64; ShallowBuildResult::ALL.len()],
    shallow_build_time: Histogram,
    /// Recordings evicted to keep a repository's exact shallow packs under
    /// its byte cap (issue #108).
    shallow_limit_evictions: AtomicU64,
    /// Recorded exact shallow packs built from the previous recorded pack
    /// and the packs pushed since, not the full closure.
    shallow_builds_incremental: AtomicU64,
}

/// Why a shallow fetch that could have used exact packs was served the
/// full closure (node C8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShallowFallback {
    /// A want is not the tip of the version it resolves to.
    NotATip,
    /// Every build slot was busy.
    NoBuildSlot,
    /// A build of the same version and depth recorded nothing recently.
    RecentlyFailed,
    /// The build did not finish within the wait.
    WaitTimedOut,
    /// The build this fetch waited for recorded nothing.
    BuildRecordedNothing,
}

impl ShallowFallback {
    const ALL: [Self; 5] = [
        Self::NotATip,
        Self::NoBuildSlot,
        Self::RecentlyFailed,
        Self::WaitTimedOut,
        Self::BuildRecordedNothing,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::NotATip => "not_a_tip",
            Self::NoBuildSlot => "no_build_slot",
            Self::RecentlyFailed => "recently_failed",
            Self::WaitTimedOut => "wait_timed_out",
            Self::BuildRecordedNothing => "build_recorded_nothing",
        }
    }
}

/// How a shallow fetch was served exactly (node C8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShallowServed {
    /// Every exact pack it needed was already recorded.
    Recorded,
    /// It waited for at least one build.
    Built,
    /// No build was waited for, and at least one want's full closure was
    /// already exact.
    Closure,
}

/// How an exact shallow pack build ended (node C8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShallowBuildResult {
    /// The pack is recorded.
    Recorded,
    /// The full closure is already exact; nothing stored.
    NotNeeded,
    /// The closure is over `max_source_bytes`.
    TooLarge,
    /// The want peels to a tree or blob.
    NotACommit,
    /// A store, disk or verification failure.
    Failed,
    /// Garbage collection deleted the pack before the build recorded it:
    /// the build took longer than `gc_grace` (issue #203).
    GraceExceeded,
    /// The pack alone is larger than the repository's byte cap
    /// (`max_repo_bytes`, issue #108): not stored.
    OverLimit,
}

impl ShallowBuildResult {
    const ALL: [Self; 7] = [
        Self::Recorded,
        Self::NotNeeded,
        Self::TooLarge,
        Self::NotACommit,
        Self::Failed,
        Self::GraceExceeded,
        Self::OverLimit,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::NotNeeded => "not_needed",
            Self::TooLarge => "too_large",
            Self::NotACommit => "not_a_commit",
            Self::Failed => "failed",
            Self::GraceExceeded => "grace_exceeded",
            Self::OverLimit => "over_limit",
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            fetch_errors: AtomicU64::new(0),
            visited: Histogram::new(&VISITED_BUCKETS, 1.0),
            loaded: Histogram::new(&VISITED_BUCKETS, 1.0),
            chain_skips: AtomicU64::new(0),
            plan_time: Histogram::new(&DURATION_MICROS, MICROS_PER_SECOND),
            needed_sum: AtomicU64::new(0),
            packs_planned: AtomicU64::new(0),
            packs_by_uri: AtomicU64::new(0),
            packs_stitched: AtomicU64::new(0),
            stitched_bytes: AtomicU64::new(0),
            haves_dropped: AtomicU64::new(0),
            haves_used: AtomicU64::new(0),
            forward_base_alerts: AtomicU64::new(0),
            corruption_alerts: AtomicU64::new(0),
            job_enqueue_failures: AtomicU64::new(0),
            uncompacted_versions: AtomicU64::new(0),
            heat_write_failures: AtomicU64::new(0),
            shallow_cached: AtomicU64::new(0),
            shallow_built: AtomicU64::new(0),
            shallow_closure: AtomicU64::new(0),
            shallow_fallbacks: Default::default(),
            shallow_builds: Default::default(),
            shallow_build_time: Histogram::new(&DURATION_MICROS, MICROS_PER_SECOND),
            shallow_limit_evictions: AtomicU64::new(0),
            shallow_builds_incremental: AtomicU64::new(0),
        }
    }
}

/// How one fetch was delivered.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Delivery {
    pub(crate) by_uri: u64,
    pub(crate) stitched: u64,
}

impl Metrics {
    pub(crate) fn plan(
        &self,
        stats: &PlanStats,
        loaded_versions: usize,
        haves_used: usize,
        haves_dropped: usize,
        took: Duration,
    ) {
        self.visited.observe(stats.visited as u64);
        self.loaded.observe(loaded_versions as u64);
        self.chain_skips
            .fetch_add(stats.chain_skips as u64, Ordering::Relaxed);
        self.plan_time.observe(expo::micros(took));
        self.needed_sum
            .fetch_add(stats.needed as u64, Ordering::Relaxed);
        self.packs_planned
            .fetch_add(stats.packs as u64, Ordering::Relaxed);
        self.haves_used
            .fetch_add(haves_used as u64, Ordering::Relaxed);
        self.haves_dropped
            .fetch_add(haves_dropped as u64, Ordering::Relaxed);
    }

    pub(crate) fn delivered(&self, d: Delivery) {
        self.packs_by_uri.fetch_add(d.by_uri, Ordering::Relaxed);
        self.packs_stitched.fetch_add(d.stitched, Ordering::Relaxed);
    }

    pub(crate) fn stitched_bytes(&self, n: u64) {
        self.stitched_bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn fetch_error(&self) {
        self.fetch_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// A stored pack broke the base-first invariant (pages, S1).
    pub(crate) fn forward_base_alert(&self) {
        self.forward_base_alerts.fetch_add(1, Ordering::Relaxed);
    }

    /// Stored data did not match its row (`StoreError::Corrupt` or
    /// `NotFound` on a blob a row names; pages).
    pub(crate) fn corruption_alert(&self) {
        self.corruption_alerts.fetch_add(1, Ordering::Relaxed);
    }

    /// A repair job (`rewrite_pack`) could not be enqueued.
    pub(crate) fn job_enqueue_failure(&self) {
        self.job_enqueue_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// A served fetch sent `n` versions as their own push packs.
    pub(crate) fn uncompacted_versions(&self, n: u64) {
        self.uncompacted_versions.fetch_add(n, Ordering::Relaxed);
    }

    /// Writing compaction heat to the job queue failed.
    pub(crate) fn heat_write_failure(&self) {
        self.heat_write_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// A shallow fetch was served exactly.
    pub(crate) fn shallow_served(&self, how: ShallowServed) {
        let c = match how {
            ShallowServed::Recorded => &self.shallow_cached,
            ShallowServed::Built => &self.shallow_built,
            ShallowServed::Closure => &self.shallow_closure,
        };
        c.fetch_add(1, Ordering::Relaxed);
    }

    /// Shallow fetches served exactly because a want's full closure was
    /// already exact.
    pub fn shallow_closure(&self) -> u64 {
        self.shallow_closure.load(Ordering::Relaxed)
    }

    /// An eligible shallow fetch was served the full closure.
    pub(crate) fn shallow_fallback(&self, why: ShallowFallback) {
        let i = ShallowFallback::ALL
            .iter()
            .position(|w| *w == why)
            .expect("listed");
        self.shallow_fallbacks[i].fetch_add(1, Ordering::Relaxed);
    }

    /// An exact shallow pack build ended.
    pub(crate) fn shallow_build(&self, result: ShallowBuildResult) {
        let i = ShallowBuildResult::ALL
            .iter()
            .position(|r| *r == result)
            .expect("listed");
        self.shallow_builds[i].fetch_add(1, Ordering::Relaxed);
    }

    /// A recording evicted others to keep its repository under its byte
    /// cap.
    pub(crate) fn shallow_limit_evictions(&self, evicted: u64) {
        self.shallow_limit_evictions
            .fetch_add(evicted, Ordering::Relaxed);
    }

    /// How long a build that recorded a pack took.
    pub(crate) fn shallow_build_time(&self, took: Duration) {
        self.shallow_build_time.observe(expo::micros(took));
    }

    /// Shallow fetches served exact packs that were already recorded.
    pub fn shallow_cached(&self) -> u64 {
        self.shallow_cached.load(Ordering::Relaxed)
    }

    /// Shallow fetches served exact packs after waiting for their build.
    pub fn shallow_built(&self) -> u64 {
        self.shallow_built.load(Ordering::Relaxed)
    }

    /// Eligible shallow fetches served the full closure, every reason.
    pub fn shallow_fallbacks(&self) -> u64 {
        self.shallow_fallbacks
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .sum()
    }

    /// Exact shallow pack builds that recorded a pack.
    pub fn shallow_builds_recorded(&self) -> u64 {
        self.shallow_builds[0].load(Ordering::Relaxed)
    }

    /// A recorded exact shallow pack was built incrementally.
    pub(crate) fn shallow_build_incremental(&self) {
        self.shallow_builds_incremental
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Recorded exact shallow packs built from the previous recorded pack
    /// and the packs pushed since (also counted as recorded builds).
    pub fn shallow_builds_incremental(&self) -> u64 {
        self.shallow_builds_incremental.load(Ordering::Relaxed)
    }

    /// Exact shallow pack builds that ended any other way.
    pub fn shallow_builds_not_recorded(&self) -> u64 {
        self.shallow_builds[1..]
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .sum()
    }

    /// Exact shallow pack builds whose pack alone was over the
    /// repository's byte cap.
    pub fn shallow_builds_over_limit(&self) -> u64 {
        self.shallow_builds[6].load(Ordering::Relaxed)
    }

    /// Recordings evicted to keep repositories under their byte cap.
    pub fn shallow_limit_evictions_total(&self) -> u64 {
        self.shallow_limit_evictions.load(Ordering::Relaxed)
    }

    /// Store-corruption alerts so far.
    pub fn corruption_alerts(&self) -> u64 {
        self.corruption_alerts.load(Ordering::Relaxed)
    }

    /// Fetches planned so far.
    pub fn fetches(&self) -> u64 {
        self.visited.count()
    }

    /// Sum of `PlanStats.visited` over every planned fetch.
    pub fn visited_sum(&self) -> u64 {
        self.visited.sum()
    }

    /// Packs sent as packfile URIs so far.
    pub fn packs_by_uri(&self) -> u64 {
        self.packs_by_uri.load(Ordering::Relaxed)
    }

    /// Packs stitched into inline packs so far.
    pub fn packs_stitched(&self) -> u64 {
        self.packs_stitched.load(Ordering::Relaxed)
    }

    /// Haves dropped because they resolved to no `HaveVersion`.
    pub fn haves_dropped(&self) -> u64 {
        self.haves_dropped.load(Ordering::Relaxed)
    }

    /// Haves the planner used (each a `HaveVersion`).
    pub fn haves_used(&self) -> u64 {
        self.haves_used.load(Ordering::Relaxed)
    }

    /// Fetches that failed on a forward base in a stored pack.
    pub fn forward_base_alerts(&self) -> u64 {
        self.forward_base_alerts.load(Ordering::Relaxed)
    }

    /// The fetch families, in the Prometheus text format.
    pub fn render(&self) -> String {
        let mut s = String::new();
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        expo::counter(
            &mut s,
            "aspen_fetches_total",
            "Fetches planned.",
            self.visited.count(),
        );
        expo::counter(
            &mut s,
            "aspen_fetch_errors_total",
            "Fetches that ended in an ERR or band-3 error.",
            get(&self.fetch_errors),
        );
        self.visited.render(
            &mut s,
            "aspen_fetch_plan_visited_versions",
            "Ref versions the planner's walk expanded per fetch (PlanStats.visited).",
        );
        self.loaded.render(
            &mut s,
            "aspen_fetch_plan_loaded_versions",
            "Ref versions read from the metadata store to plan one fetch, one read each.",
        );
        expo::counter(
            &mut s,
            "aspen_fetch_plan_chain_skips_total",
            "Cover chains the planner's have-side walk skipped through (PlanStats.chain_skips).",
            get(&self.chain_skips),
        );
        self.plan_time.render(
            &mut s,
            "aspen_fetch_plan_duration_seconds",
            "Time to load the version graph and plan one fetch.",
        );
        expo::counter(
            &mut s,
            "aspen_fetch_plan_needed_versions_total",
            "Ref versions reachable from a want and not from a have, summed over fetches.",
            get(&self.needed_sum),
        );
        expo::counter(
            &mut s,
            "aspen_fetch_packs_planned_total",
            "Packs selected by the planner.",
            get(&self.packs_planned),
        );
        expo::counter(
            &mut s,
            "aspen_fetch_packs_uri_total",
            "Packs sent as packfile URIs.",
            get(&self.packs_by_uri),
        );
        expo::counter(
            &mut s,
            "aspen_fetch_packs_stitched_total",
            "Packs stitched into inline packs.",
            get(&self.packs_stitched),
        );
        expo::counter(
            &mut s,
            "aspen_fetch_stitched_bytes_total",
            "Bytes of stitched packs sent.",
            get(&self.stitched_bytes),
        );
        expo::counter(
            &mut s,
            "aspen_fetch_haves_used_total",
            "Client haves that resolved to a HaveVersion.",
            get(&self.haves_used),
        );
        expo::counter(
            &mut s,
            "aspen_fetch_haves_dropped_total",
            "Client haves dropped (unknown, non-tip, or a wider closure).",
            get(&self.haves_dropped),
        );
        expo::counter(
            &mut s,
            "aspen_alert_stored_pack_forward_base_total",
            "Fetches that hit a stored pack breaking base-first. Pages: rewrite the pack.",
            get(&self.forward_base_alerts),
        );
        expo::counter(
            &mut s,
            "aspen_alert_store_corruption_total",
            "Fetches that found stored data not matching its row. Pages.",
            get(&self.corruption_alerts),
        );
        expo::counter(
            &mut s,
            "aspen_job_enqueue_failures_total",
            "Repair jobs (rewrite_pack) that could not be enqueued; the next fetch retries.",
            get(&self.job_enqueue_failures),
        );
        expo::counter(
            &mut s,
            "aspen_fetch_uncompacted_versions_total",
            "Versions served fetches sent as their own push packs: compaction heat, which raises the claim priority of those refs' compact jobs.",
            get(&self.uncompacted_versions),
        );
        expo::counter(
            &mut s,
            "aspen_compaction_heat_write_failures_total",
            "Writes of compaction heat to the job queue that failed (advisory: only job order is affected).",
            get(&self.heat_write_failures),
        );
        expo::family(
            &mut s,
            "aspen_shallow_exact_fetches_total",
            "Shallow fetches served exactly: from packs already recorded, after waiting for a build, or from a full closure that was already exact.",
            expo::Kind::Counter,
            [
                (vec![("pack", "recorded")], get(&self.shallow_cached) as f64),
                (vec![("pack", "built")], get(&self.shallow_built) as f64),
                (vec![("pack", "closure")], get(&self.shallow_closure) as f64),
            ],
        );
        expo::family(
            &mut s,
            "aspen_shallow_fallbacks_total",
            "Shallow fetches that could have used exact shallow packs and were served the full closure, by reason.",
            expo::Kind::Counter,
            ShallowFallback::ALL
                .iter()
                .zip(&self.shallow_fallbacks)
                .map(|(w, a)| (vec![("reason", w.label())], get(a) as f64)),
        );
        expo::family(
            &mut s,
            "aspen_shallow_builds_total",
            "Exact shallow pack builds, by result.",
            expo::Kind::Counter,
            ShallowBuildResult::ALL
                .iter()
                .zip(&self.shallow_builds)
                .map(|(r, a)| (vec![("result", r.label())], get(a) as f64)),
        );
        expo::counter(
            &mut s,
            "aspen_shallow_builds_incremental_total",
            "Recorded exact shallow packs built from the previous version's recorded pack and the packs pushed since, instead of the full closure (also counted in aspen_shallow_builds_total).",
            get(&self.shallow_builds_incremental),
        );
        self.shallow_build_time.render(
            &mut s,
            "aspen_shallow_build_duration_seconds",
            "Time to build, verify, upload and record one exact shallow pack.",
        );
        expo::counter(
            &mut s,
            "aspen_shallow_limit_evictions_total",
            "Exact shallow pack recordings evicted, least recently used first, to keep a repository's recorded packs under ASPEN_SHALLOW_MAX_REPO_BYTES.",
            get(&self.shallow_limit_evictions),
        );
        s
    }
}

/// `aspen_build_info`: always 1, labelled with the version, so dashboards
/// can show which build each instance runs.
pub(crate) fn render_build_info(out: &mut String) {
    expo::family(
        out,
        "aspen_build_info",
        "Always 1; the labels name the running build.",
        expo::Kind::Gauge,
        [(vec![("version", aspen_core::VERSION)], 1.0)],
    );
}
