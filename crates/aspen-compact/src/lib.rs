//! Aspen's compactor (node C1): the cover skip list over each ref's version
//! history, compaction packs, garbage collection of orphaned packs, and the
//! `index_commits` job (commit index rows for large pushes; see
//! [`Compactor::index_version`]).
//!
//! Every push stores level-0 packs ([`aspen_core::pack`]). A fetch of many
//! versions would send one pack per version, so the compactor lays
//! **covers** over each ref's versions: level `k` spans `fanout^k` versions,
//! aligned ([`CoverLayout`], fanout 16 by default), and a full cover is
//! repacked into one **compaction pack** at its level. The planner
//! (`aspen-planner`) sends a cover's pack in place of its versions' packs
//! when it needs every version in the range, trying the highest level
//! first, so a fetch of N versions of one ref sends about `15 × 4 +
//! N / 65,536` packs plus the newest versions' own packs.
//!
//! A cover also records whether its range is a **dependency chain**
//! ([`aspen_core::meta::CoverChain`]): every version after the first
//! depends on its parent. That lets the planner skip the range on the have
//! side, which makes the walk of a stale-branch fetch logarithmic instead
//! of linear in the versions between the wants and the haves.
//!
//! # What the compactor guarantees
//!
//! - A cover's pack holds **exactly** the objects of the packs of the
//!   versions in its range (the planner's contract, A5): it is repacked from
//!   those packs, or from the packs of the covers one level below that tile
//!   the range, which hold exactly theirs. The compactor checks the result
//!   against the union of its sources' `.idx` files before it uploads it.
//! - Every compaction pack is **self-contained** and **base-first**, holds
//!   **no object twice**, and has delta chains of at most
//!   [`CompactConfig::max_delta_depth`] (50 by default), and of at most
//!   [`CompactConfig::max_tree_delta_depth`] (10) for trees: it is written by
//!   [`aspen_pack::repack`] and then checked by
//!   [`aspen_pack::verify::verify_pack`], whose `.idx` and entry table must
//!   equal the writer's byte for byte.
//! - Work is **idempotent**: jobs are leased and may run twice. Covers are
//!   created idempotently
//!   ([`MetaStore::insert_cover`](aspen_core::meta::MetaStore::insert_cover)),
//!   repacking the same
//!   sources gives the same bytes, blob uploads of the same key and size are
//!   no-ops, `register_pack` returns the existing row, and
//!   `set_cover_pack` accepts the same pack again.
//! - Nothing a plan may select is deleted: compaction never deletes the
//!   level-0 packs it repacks. [`Compactor::collect_garbage`] deletes only
//!   packs that no version, cover or job names and that nobody registered
//!   within the grace period (issue #48), and a deletion that runs late
//!   never removes a live pack's blobs: a checksum registered again gets a
//!   new generation and new keys.
//!
//! # Exact shallow packs
//!
//! [`Compactor::build_shallow_pack`] (node C8) builds the exact pack of a
//! `git fetch --depth=N` of one ref version with the same repack writer
//! and the same checks, registers it as a level-1 compaction pack that no
//! cover names, and records it in `shallow_packs`. The fetch path calls it;
//! garbage collection evicts recordings of superseded versions after
//! [`CompactConfig::shallow_ttl`], and recordings no fetch has used for
//! [`CompactConfig::shallow_unused`], current or not (issue #108).
//!
//! # When it runs
//!
//! The push path enqueues a `compact` job for every new version whose `seq`
//! ends a level-1 cover, in the push's own transaction. [`Worker`] claims
//! those jobs and builds every cover that ends there, first making sure the
//! covers one level below exist (so a block whose last version was a
//! deletion, which enqueues no job, is built when the cover above it is).
//! [`Compactor::compact_through`] builds every cover of a ref up to a
//! version, for repositories that predate the compactor. The same worker
//! runs `index_commits` jobs, which a push enqueues for a write with more
//! commits than it indexes itself.
//!
//! # Cost
//!
//! A cover's repack downloads its sources (at most `fanout` packs from the
//! level below, or the versions' own packs at level 1) to the spool
//! directory, writes the new pack beside them and verifies it, which
//! inflates every object once. Memory is proportional to the objects of the
//! new pack (its `.idx` and entry table, about 100 bytes per object), never
//! to a pack's bytes.

#![deny(missing_docs)]

mod build;
mod gc;
mod index;
mod shallow;
mod worker;

use std::path::PathBuf;
use std::time::Duration;

use aspen_core::error::StoreError;
use aspen_core::meta::CoverLayout;
use aspen_pack::verify::VerifyError;
use aspen_pack::{LayoutError, MaxInflatedBytes, MaxObjectBytes};

pub use build::{CompactReport, Compactor};
pub use gc::{GcMetrics, GcReport};
pub use index::IndexReport;
pub use shallow::{ShallowBuild, ShallowSpec};
pub use worker::{JobOutcome, JobReport, Worker, WorkerOptions};

/// Default [`CompactConfig::gc_grace`]: 2 hours.
pub const DEFAULT_GC_GRACE: Duration = Duration::from_secs(2 * 3600);

/// Default [`CompactConfig::shallow_ttl`]: 10 minutes.
pub const DEFAULT_SHALLOW_TTL: Duration = Duration::from_secs(10 * 60);

/// Default [`CompactConfig::shallow_unused`]: 24 hours.
pub const DEFAULT_SHALLOW_UNUSED: Duration = Duration::from_secs(24 * 3600);

/// The shortest [`CompactConfig::shallow_unused`] accepted: 10 minutes.
/// The fetch path records a recording's use at most once a minute per
/// process, so a shorter window could evict a recording fetches are still
/// using.
pub const MIN_SHALLOW_UNUSED: Duration = Duration::from_secs(10 * 60);

/// The most batches one garbage-collection pass runs of each step
/// ([`CompactConfig::gc_batch`] each), so one pass stays bounded.
pub const GC_ROUNDS: u32 = 50;

/// Default [`CompactConfig::gc_pass_budget`]: 30 seconds.
pub const DEFAULT_GC_PASS_BUDGET: Duration = Duration::from_secs(30);

/// Default [`CompactConfig::gc_sweep_batch`]: 1,000 registrations per sweep
/// transaction.
pub const DEFAULT_GC_SWEEP_BATCH: u32 = 1000;

/// Default [`CompactConfig::gc_lease`]: 5 minutes, one pass interval
/// ([`WorkerOptions::gc_every`]), so the holder keeps the lease from one
/// pass to the next.
pub const DEFAULT_GC_LEASE: Duration = Duration::from_secs(300);

/// The shortest [`CompactConfig::gc_lease`] accepted: 10 seconds. Each
/// sweep transaction extends the lease, so it only has to outlast the
/// gaps between them.
pub const MIN_GC_LEASE: Duration = Duration::from_secs(10);

/// A lease owner unique to this process: its ID and the time it first
/// asked. Compactors in one process share it, so they share the
/// garbage-collection lease; another process's differs.
fn process_owner() -> String {
    static OWNER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    OWNER
        .get_or_init(|| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            format!("aspen-{}-{nanos:x}", std::process::id())
        })
        .clone()
}

// The defaults must pass `CompactConfig::validate`.
const _: () =
    assert!(DEFAULT_GC_GRACE.as_secs() >= 2 * aspen_core::pack::MAX_PACK_URL_LIFETIME.as_secs());

/// The compactor's configuration.
#[derive(Debug, Clone)]
pub struct CompactConfig {
    /// Where covers go. The push path must use the same layout
    /// (`aspen_server::receive::ReceiveConfig::cover_layout`).
    pub layout: CoverLayout,
    /// Local disk for sources and the new pack while a cover is repacked: a
    /// real disk, sized for the largest level-4 cover twice.
    pub spool_dir: PathBuf,
    /// The largest object or delta result a repack or its verification
    /// inflates.
    pub max_object_bytes: MaxObjectBytes,
    /// The most one compaction pack may inflate to in all, checked by its
    /// verification.
    pub max_inflated_bytes: MaxInflatedBytes,
    /// Longest delta chain a compaction pack may hold. Deeper entries are
    /// stored undeltified. Stitched fetches add the depths of their
    /// segments, so this keeps them far below Git's limit of 4,095 (A2).
    pub max_delta_depth: u32,
    /// Longest delta chain a tree (any object but a blob) may sit at in a
    /// compaction pack. Keep-first repacking hangs each version of a hot
    /// tree (the root, the directories above busy files) off the one
    /// before, so without its own bound hot trees reached
    /// [`CompactConfig::max_delta_depth`] in every compaction pack, and a
    /// push, which needs the trees on its paths as delta bases, read 50
    /// levels of them (monorepo benchmark, item 11). Default 10.
    pub max_tree_delta_depth: u32,
    /// How long an orphaned pack must go unregistered (or, for an evicted
    /// exact shallow pack, unnamed) before it is deleted. It bounds two
    /// things (blob deletions are safe however late they run):
    ///
    /// - every writer's time from registering a pack to committing what
    ///   names it: a push, a compaction job recording a cover's pack, an
    ///   exact shallow pack build. One slower than this is refused
    ///   retryably ([`aspen_core::error::StoreError::pack_swept`]), and
    ///   registers and uploads the pack again on its retry;
    /// - a stitched fetch still opening an evicted shallow pack this long
    ///   after its eviction fails, and is retried by the client.
    ///
    /// The default, 2 hours, is twice
    /// [`aspen_core::pack::MAX_PACK_URL_LIFETIME`] (1 hour, the longest any
    /// packfile URL Aspen hands out lives), so every URL handed out for a
    /// pack before it became an orphan has expired an hour before the pack
    /// is deleted. [`CompactConfig::validate`] refuses less than twice the
    /// cap (issue #203).
    pub gc_grace: Duration,
    /// Packs deleted per sweep transaction, and shallow pack recordings
    /// evicted per transaction; at least 1 ([`CompactConfig::validate`]).
    /// A pass repeats each step while it fills a batch, up to [`GC_ROUNDS`]
    /// times and within [`CompactConfig::gc_pass_budget`], so garbage
    /// collection keeps up with more than one batch per pass (issue #203).
    pub gc_batch: u32,
    /// How long one garbage-collection pass may keep repeating its steps.
    /// Each step (eviction, sweep, blob deletion) always runs one batch;
    /// a further batch starts only while the pass has run less than this,
    /// so a pass with a large backlog cannot keep the worker from its
    /// `compact` and `index_commits` jobs for long. What is left waits for
    /// the next pass.
    pub gc_pass_budget: Duration,
    /// How long an exact shallow pack recording (node C8) is kept once its
    /// version is no longer its ref's current version, counted from when
    /// it was recorded. Garbage collection then evicts it, and its pack
    /// becomes an orphan that the sweep deletes a full
    /// [`CompactConfig::gc_grace`] later. Recordings of current versions are
    /// never evicted for this (but see [`CompactConfig::shallow_unused`]).
    /// A fetch of an evicted version builds the pack again.
    pub shallow_ttl: Duration,
    /// How long an exact shallow pack recording is kept after a fetch last
    /// used it (or since it was recorded), whether its version is current
    /// or not (issue #108). Garbage collection then evicts it, and its pack
    /// is deleted a full [`CompactConfig::gc_grace`] later. At least
    /// [`MIN_SHALLOW_UNUSED`] ([`CompactConfig::validate`]).
    pub shallow_unused: Duration,
    /// Registrations one sweep transaction reads, after where the last one
    /// stopped (plus as many packs of its audit); at least 1. A pass runs
    /// sweep transactions until the sweep has caught up or
    /// [`CompactConfig::gc_pass_budget`] is spent, so this bounds the cost
    /// and lock time of one transaction, not how much a pass collects.
    pub gc_sweep_batch: u32,
    /// How long the garbage-collection lease lasts after it was acquired or
    /// extended ([`aspen_core::meta::MetaStore::acquire_lease`]). One
    /// process at a time collects garbage; each sweep transaction extends
    /// the lease, and a holder that dies loses it this long after its last
    /// extension. At least [`MIN_GC_LEASE`].
    pub gc_lease: Duration,
    /// The name this compactor holds the garbage-collection lease under.
    /// Compactors with the same owner share the lease; processes must use
    /// different owners. The default is unique per process.
    pub gc_owner: String,
}

impl CompactConfig {
    /// The defaults: fanout 16, depth 50 (10 for trees), 2 hours of grace, 100 packs per
    /// sweep, passes of at most 30 seconds, shallow pack recordings of
    /// superseded versions kept 10 minutes and unused ones 24 hours. With a pass every 5 minutes
    /// ([`WorkerOptions::gc_every`]) unreachable data is deleted within
    /// [`CompactConfig::reclaim_bound`], 2 h 20 min (issue #203).
    pub fn new(
        spool_dir: impl Into<PathBuf>,
        max_object_bytes: MaxObjectBytes,
        max_inflated_bytes: MaxInflatedBytes,
    ) -> Self {
        Self {
            layout: CoverLayout::DEFAULT,
            spool_dir: spool_dir.into(),
            max_object_bytes,
            max_inflated_bytes,
            max_delta_depth: 50,
            max_tree_delta_depth: 10,
            gc_grace: DEFAULT_GC_GRACE,
            gc_batch: 100,
            gc_pass_budget: DEFAULT_GC_PASS_BUDGET,
            shallow_ttl: DEFAULT_SHALLOW_TTL,
            shallow_unused: DEFAULT_SHALLOW_UNUSED,
            gc_sweep_batch: DEFAULT_GC_SWEEP_BATCH,
            gc_lease: DEFAULT_GC_LEASE,
            gc_owner: process_owner(),
        }
    }

    /// Refuses a configuration garbage collection cannot run safely or at
    /// all: a `gc_batch` of 0 (a pass would collect nothing), a
    /// `shallow_unused` under [`MIN_SHALLOW_UNUSED`], and a `gc_grace`
    /// shorter than twice
    /// [`aspen_core::pack::MAX_PACK_URL_LIFETIME`] (a pack could be deleted
    /// while a URL handed out for it still works). `aspen serve` checks
    /// this at startup.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.gc_batch == 0 || self.gc_sweep_batch == 0 {
            return Err(ConfigError::GcBatchZero);
        }
        if self.gc_lease < MIN_GC_LEASE {
            return Err(ConfigError::GcLeaseTooShort {
                lease: self.gc_lease,
                min: MIN_GC_LEASE,
            });
        }
        if self.shallow_unused < MIN_SHALLOW_UNUSED {
            return Err(ConfigError::ShallowUnusedTooShort {
                unused: self.shallow_unused,
                min: MIN_SHALLOW_UNUSED,
            });
        }
        let min = aspen_core::pack::MAX_PACK_URL_LIFETIME * 2;
        if self.gc_grace < min {
            return Err(ConfigError::GcGraceTooShort {
                grace: self.gc_grace,
                min,
            });
        }
        Ok(())
    }

    /// The longest unreachable data waits to be deleted when a pass runs
    /// every `gc_every`: `shallow_ttl + gc_grace + 2 × gc_every`, counted
    /// from when it stopped being needed (a shallow pack recording: from
    /// the later of its version being superseded and its recording
    /// `shallow_ttl` old; an orphan: from its last registration, which
    /// needs only `gc_grace + gc_every`). One `gc_every` is the wait for
    /// the pass that evicts the recording (the eviction restarts the
    /// pack's grace period), the other the wait for the pass that sweeps
    /// it once the grace period is over; the blobs go in that same pass.
    ///
    /// It holds while each pass finishes what is due: at most
    /// [`GC_ROUNDS`] × `gc_batch` items per step that are due in one pass,
    /// within `gc_pass_budget`. A larger backlog is worked off over later
    /// passes, one batch per step per pass at least. When the process
    /// holding the garbage-collection lease dies, add up to
    /// [`CompactConfig::gc_lease`] for another to take it over.
    pub fn reclaim_bound(&self, gc_every: Duration) -> Duration {
        self.shallow_ttl + self.gc_grace + gc_every * 2
    }
}

/// Why a [`CompactConfig`] was refused ([`CompactConfig::validate`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// `gc_batch` or `gc_sweep_batch` is 0.
    #[error("gc_batch and gc_sweep_batch must be at least 1")]
    GcBatchZero,
    /// `gc_lease` is shorter than [`MIN_GC_LEASE`].
    #[error("gc_lease {lease:?} is shorter than {min:?}")]
    GcLeaseTooShort {
        /// The configured lease.
        lease: Duration,
        /// The shortest accepted.
        min: Duration,
    },
    /// `shallow_unused` is shorter than [`MIN_SHALLOW_UNUSED`].
    #[error("shallow_unused {unused:?} is shorter than {min:?}")]
    ShallowUnusedTooShort {
        /// The configured window.
        unused: Duration,
        /// The shortest accepted.
        min: Duration,
    },
    /// `gc_grace` is shorter than twice the longest packfile URL lifetime.
    #[error("gc_grace {grace:?} is shorter than {min:?}, twice the longest packfile URL lifetime")]
    GcGraceTooShort {
        /// The configured grace period.
        grace: Duration,
        /// The shortest accepted.
        min: Duration,
    },
}

/// Why compaction failed. A job that fails is retried with backoff.
#[derive(Debug, thiserror::Error)]
pub enum CompactError {
    /// A store failed, or stored data is wrong.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Writing the compaction pack failed (a local disk error, or a source
    /// whose pack, `.idx` and entry table disagree).
    #[error("repack: {0}")]
    Layout(#[from] LayoutError),
    /// The compaction pack failed its verification: a bug in the writer or a
    /// corrupt source. Nothing was uploaded.
    #[error("compaction pack failed verification: {0}")]
    Verify(String),
}

impl From<VerifyError> for CompactError {
    fn from(e: VerifyError) -> Self {
        Self::Verify(e.to_string())
    }
}

impl CompactError {
    /// Whether the failure means stored data (or the writer) is wrong, which
    /// pages, rather than an outage to retry through.
    pub fn is_corruption(&self) -> bool {
        match self {
            Self::Store(e) => e.is_corruption(),
            Self::Layout(LayoutError::Io(_) | LayoutError::OverLimit(_)) => false,
            Self::Layout(_) | Self::Verify(_) => true,
        }
    }
}

fn local_io(what: &str, e: impl std::fmt::Display) -> StoreError {
    StoreError::LocalIo {
        reason: format!("{what}: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> CompactConfig {
        CompactConfig::new(
            "/nonexistent",
            MaxObjectBytes::new(1 << 20).unwrap(),
            MaxInflatedBytes::new(1 << 20).unwrap(),
        )
    }

    /// Invariant (issue #203): garbage collection is never configured to
    /// delete a pack while a URL handed out for it may still work, nor to
    /// collect nothing.
    #[test]
    fn validate_refuses_a_short_grace_and_an_empty_batch() {
        assert_eq!(config().validate(), Ok(()));
        let cap = aspen_core::pack::MAX_PACK_URL_LIFETIME;
        let mut c = config();
        c.gc_grace = cap * 2;
        assert_eq!(c.validate(), Ok(()), "exactly twice the cap");
        c.gc_grace = cap * 2 - Duration::from_secs(1);
        assert_eq!(
            c.validate(),
            Err(ConfigError::GcGraceTooShort {
                grace: c.gc_grace,
                min: cap * 2
            })
        );
        let mut c = config();
        c.gc_batch = 0;
        assert_eq!(c.validate(), Err(ConfigError::GcBatchZero));
        let mut c = config();
        c.gc_sweep_batch = 0;
        assert_eq!(c.validate(), Err(ConfigError::GcBatchZero));
    }

    /// Throughput item 7: the garbage-collection lease outlasts the gaps
    /// between sweep transactions, and every process holds it under its own
    /// name while compactors of one process share it.
    #[test]
    fn validate_refuses_a_short_lease_and_owners_are_per_process() {
        let mut c = config();
        assert_eq!(c.gc_lease, DEFAULT_GC_LEASE);
        c.gc_lease = MIN_GC_LEASE;
        assert_eq!(c.validate(), Ok(()), "exactly the minimum");
        c.gc_lease = MIN_GC_LEASE - Duration::from_millis(1);
        assert_eq!(
            c.validate(),
            Err(ConfigError::GcLeaseTooShort {
                lease: c.gc_lease,
                min: MIN_GC_LEASE,
            })
        );
        assert_eq!(config().gc_owner, config().gc_owner);
        assert!(
            config()
                .gc_owner
                .starts_with(&format!("aspen-{}-", std::process::id()))
        );
    }

    /// Invariant (issue #108): unused recordings are never evicted on a
    /// window the fetch path's once-a-minute use updates cannot keep up
    /// with.
    #[test]
    fn validate_refuses_a_short_unused_window() {
        assert_eq!(config().shallow_unused, Duration::from_secs(86_400));
        let mut c = config();
        c.shallow_unused = MIN_SHALLOW_UNUSED;
        assert_eq!(c.validate(), Ok(()), "exactly the minimum");
        c.shallow_unused = MIN_SHALLOW_UNUSED - Duration::from_secs(1);
        assert_eq!(
            c.validate(),
            Err(ConfigError::ShallowUnusedTooShort {
                unused: c.shallow_unused,
                min: MIN_SHALLOW_UNUSED,
            })
        );
    }

    #[test]
    fn the_default_reclaim_bound_is_2_h_20_min() {
        let every = WorkerOptions::new("t").gc_every;
        assert_eq!(
            config().reclaim_bound(every),
            Duration::from_secs(2 * 3600 + 20 * 60)
        );
    }
}
