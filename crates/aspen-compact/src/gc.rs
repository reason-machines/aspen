//! Garbage collection of orphaned packs (issue #48).
//!
//! A push registers a write's pack, uploads its blobs to the row's keys,
//! then names the row in that write's commit transaction. A write that then loses its compare-and-swap for
//! good, or whose recomputed verdict is a rejection, leaves a `packs` row
//! and three blobs that no version names; so does a compactor that crashed
//! between registering a pack and recording it on its cover. Nothing ever
//! selects such a pack, so it is only storage.
//!
//! Exact shallow packs (node C8) become orphans the same way once garbage
//! collection evicts their recording
//! ([`aspen_core::meta::MetaStore::evict_shallow_packs`]).
//!
//! The sweep is [`aspen_core::meta::MetaStore::sweep_orphan_packs`], which
//! deletes the rows of packs that no version, cover, job or shallow pack
//! recording names and that
//! nobody registered within the grace period, and leaves a pending
//! tombstone for each; the compactor then deletes the tombstones' blobs and
//! finishes them. A checksum registered again gets a new generation and new
//! keys, so a deletion that runs late never removes a live pack's blobs.
//! The protocol is in `crates/aspen-meta/SCHEMA.md` ("Garbage
//! collection").
//!
//! # One process at a time, in bounded batches (throughput item 7)
//!
//! A pass first takes the garbage-collection lease
//! ([`aspen_core::meta::GC_LEASE`], [`crate::CompactConfig::gc_lease`]);
//! while another process holds it, the pass does nothing. The sweep is
//! incremental: each transaction reads at most
//! [`crate::CompactConfig::gc_sweep_batch`] registrations after where the
//! last one stopped, so its cost does not grow with the number of packs,
//! and each extends the lease or is refused once it was lost. The pass runs
//! its statements on [`Compactor::with_gc_store`]'s store when one is set
//! (`aspen serve` gives it a pool of its own on Postgres, with its own
//! statement timeout), so it never waits in, or takes from, the queue the
//! push path draws on. [`GcMetrics`] counts passes and times every sweep
//! batch.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use aspen_core::blob::BlobStore;
use aspen_core::id::PackId;
use aspen_core::meta::{GC_LEASE, MetaStore};

use crate::CompactError;
use crate::Compactor;

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Another process held the garbage-collection lease, so this pass did
    /// nothing.
    pub lease_held_elsewhere: bool,
    /// The lease was lost partway (it expired, or another process took it
    /// over): the pass stopped sweeping. What it did before stands.
    pub lease_lost: bool,
    /// Exact shallow pack recordings evicted (node C8): recorded at least
    /// [`crate::CompactConfig::shallow_ttl`] ago for a version that is no
    /// longer current, or unused for
    /// [`crate::CompactConfig::shallow_unused`] (issue #108). Their packs
    /// are swept a grace period later.
    pub shallow_evicted: u64,
    /// Sweep transactions run.
    pub sweep_batches: u32,
    /// Registrations and packs those transactions read.
    pub sweep_examined: u64,
    /// The sweep caught up: it read every registration whose grace period
    /// is over.
    pub sweep_caught_up: bool,
    /// Pack rows the sweep deleted.
    pub packs_deleted: Vec<PackId>,
    /// Bytes (pack, `.idx` and entry table) of those rows whose blobs were
    /// deleted in this pass.
    pub bytes_deleted: u64,
    /// Pending deletions finished, this pass's own and any an earlier pass
    /// (or a crashed worker) left.
    pub deletes_finished: usize,
    /// Blobs whose deletion failed. Their tombstones stay pending and the
    /// next pass tries again.
    pub blobs_left: Vec<String>,
    /// A step stopped repeating because the pass ran past
    /// [`crate::CompactConfig::gc_pass_budget`]; the next pass continues.
    pub budget_spent: bool,
}

/// Upper bounds of the sweep batch duration histogram, in microseconds.
const DURATION_BUCKETS_US: [u64; 10] = [
    1_000, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 1_000_000, 5_000_000, 10_000_000,
];
/// Upper bounds of the rows-per-batch histogram.
const ROW_BUCKETS: [u64; 7] = [0, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

/// A Prometheus histogram of `u64` observations, lock-free.
#[derive(Debug)]
struct Histogram<const N: usize> {
    bounds: [u64; N],
    /// Observations at or under each bound (not cumulative), then above
    /// the last.
    counts: [AtomicU64; N],
    over: AtomicU64,
    sum: AtomicU64,
}

impl<const N: usize> Histogram<N> {
    fn new(bounds: [u64; N]) -> Self {
        Self {
            bounds,
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            over: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    fn observe(&self, v: u64) {
        match self.bounds.iter().position(|&b| v <= b) {
            Some(i) => self.counts[i].fetch_add(1, Ordering::Relaxed),
            None => self.over.fetch_add(1, Ordering::Relaxed),
        };
        self.sum.fetch_add(v, Ordering::Relaxed);
    }

    /// Writes the family; `scale` divides bounds and sum (1,000,000 for
    /// microseconds shown as seconds).
    fn render(&self, out: &mut String, name: &str, help: &str, scale: f64) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut total = 0;
        for (bound, count) in self.bounds.iter().zip(&self.counts) {
            total += count.load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "{name}_bucket{{le=\"{}\"}} {total}",
                *bound as f64 / scale
            );
        }
        total += self.over.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {total}");
        let sum = self.sum.load(Ordering::Relaxed) as f64 / scale;
        let _ = writeln!(out, "{name}_sum {sum}");
        let _ = writeln!(out, "{name}_count {total}");
    }
}

/// Garbage-collection metrics of one process (throughput item 7), shown at
/// `/-/metrics` by `aspen serve` (`docs/OBSERVABILITY.md`). Counters start
/// at zero with the process; only the process holding the lease sweeps, so
/// sum them across instances.
#[derive(Debug)]
pub struct GcMetrics {
    passes_ran: AtomicU64,
    passes_lease_held: AtomicU64,
    passes_lease_lost: AtomicU64,
    passes_failed: AtomicU64,
    holds_lease: AtomicU64,
    batches: AtomicU64,
    examined: AtomicU64,
    deleted: AtomicU64,
    batch_time: Histogram<10>,
    batch_rows: Histogram<7>,
}

impl Default for GcMetrics {
    fn default() -> Self {
        Self {
            passes_ran: AtomicU64::new(0),
            passes_lease_held: AtomicU64::new(0),
            passes_lease_lost: AtomicU64::new(0),
            passes_failed: AtomicU64::new(0),
            holds_lease: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            examined: AtomicU64::new(0),
            deleted: AtomicU64::new(0),
            batch_time: Histogram::new(DURATION_BUCKETS_US),
            batch_rows: Histogram::new(ROW_BUCKETS),
        }
    }
}

impl GcMetrics {
    fn batch(&self, took: Duration, examined: u64, deleted: usize) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.examined.fetch_add(examined, Ordering::Relaxed);
        self.deleted.fetch_add(deleted as u64, Ordering::Relaxed);
        self.batch_time
            .observe(u64::try_from(took.as_micros()).unwrap_or(u64::MAX));
        self.batch_rows.observe(examined);
    }

    fn pass(&self, result: &Result<GcReport, CompactError>) {
        let counter = match result {
            Ok(r) if r.lease_held_elsewhere => &self.passes_lease_held,
            Ok(r) if r.lease_lost => &self.passes_lease_lost,
            Ok(_) => &self.passes_ran,
            Err(_) => &self.passes_failed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let holds = matches!(result, Ok(r) if !r.lease_held_elsewhere && !r.lease_lost);
        self.holds_lease.store(u64::from(holds), Ordering::Relaxed);
    }

    /// Appends the `aspen_gc_*` families in the Prometheus text format.
    pub fn render(&self, out: &mut String) {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "# HELP aspen_gc_passes_total Garbage-collection passes by result: ran, \
             lease_held_elsewhere (another instance collects), lease_lost (stopped partway), \
             failed (a store error).\n# TYPE aspen_gc_passes_total counter"
        );
        for (result, c) in [
            ("ran", &self.passes_ran),
            ("lease_held_elsewhere", &self.passes_lease_held),
            ("lease_lost", &self.passes_lease_lost),
            ("failed", &self.passes_failed),
        ] {
            let _ = writeln!(
                out,
                "aspen_gc_passes_total{{result=\"{result}\"}} {}",
                get(c)
            );
        }
        let _ = writeln!(
            out,
            "# HELP aspen_gc_lease_held Whether this instance's last garbage-collection pass held \
             the lease (1) or not (0).\n# TYPE aspen_gc_lease_held gauge\n\
             aspen_gc_lease_held {}",
            get(&self.holds_lease)
        );
        for (name, help, c) in [
            (
                "aspen_gc_sweep_batches_total",
                "Sweep transactions run.",
                &self.batches,
            ),
            (
                "aspen_gc_sweep_rows_examined_total",
                "Registrations and packs sweep transactions read.",
                &self.examined,
            ),
            (
                "aspen_gc_sweep_packs_deleted_total",
                "Orphaned pack rows the sweep deleted.",
                &self.deleted,
            ),
        ] {
            let _ = writeln!(
                out,
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}",
                get(c)
            );
        }
        self.batch_time.render(
            out,
            "aspen_gc_sweep_batch_duration_seconds",
            "Time of one sweep transaction, lease check to commit.",
            1_000_000.0,
        );
        self.batch_rows.render(
            out,
            "aspen_gc_sweep_batch_rows",
            "Registrations and packs one sweep transaction read; at most twice gc_sweep_batch.",
            1.0,
        );
    }
}

impl<M, B> Compactor<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    /// Runs garbage collection's statements on `store` instead of the
    /// compactor's store (`aspen serve`: a pool of its own on Postgres), so
    /// a pass never queues for, or holds, the connections the push path
    /// uses. `store` must be the same database.
    pub fn with_gc_store(mut self, store: M) -> Self {
        self.gc_meta = Some(store);
        self
    }

    /// Records every pass and sweep batch in `metrics`.
    pub fn with_gc_metrics(mut self, metrics: Arc<GcMetrics>) -> Self {
        self.gc_metrics = Some(metrics);
        self
    }

    /// Runs one pass, if this process can take the garbage-collection
    /// lease ([`crate::CompactConfig::gc_lease`], held under
    /// [`crate::CompactConfig::gc_owner`]); otherwise reports
    /// [`GcReport::lease_held_elsewhere`] and does nothing. The pass evicts
    /// shallow pack recordings of superseded versions, sweeps, then deletes
    /// the blobs of pending tombstones, oldest first (so a crashed worker's
    /// are retried by the next one).
    ///
    /// Eviction and blob deletion run in transactions of
    /// [`crate::CompactConfig::gc_batch`] and repeat while a transaction
    /// fills its batch, up to [`crate::GC_ROUNDS`] times; the sweep runs
    /// transactions of [`crate::CompactConfig::gc_sweep_batch`] until it has
    /// caught up (issue #203: one batch per pass fell behind a busy
    /// repository). Every step runs its first batch; a further batch starts
    /// only while the pass has run less than
    /// [`crate::CompactConfig::gc_pass_budget`], so a large backlog cannot
    /// hold the worker's jobs up for long, and no step is starved by an
    /// earlier one. A pass whose lease is lost stops sweeping. Running it
    /// again is safe, and so is a lease that changes hands mid-pass: a
    /// tombstone's keys belong to a deleted generation that no row will
    /// ever name again.
    pub async fn collect_garbage(&self) -> Result<GcReport, CompactError> {
        let result = self.collect_garbage_once().await;
        if let Some(m) = &self.gc_metrics {
            m.pass(&result);
        }
        result
    }

    async fn collect_garbage_once(&self) -> Result<GcReport, CompactError> {
        let meta = self.gc_meta.as_ref().unwrap_or(&self.meta);
        let mut report = GcReport::default();
        let Some(lease) = meta
            .acquire_lease(GC_LEASE, &self.config.gc_owner, self.config.gc_lease)
            .await?
        else {
            report.lease_held_elsewhere = true;
            return Ok(report);
        };
        // `validate` refuses 0; a batch of 0 would never fill, so would
        // otherwise repeat every step `GC_ROUNDS` times for nothing.
        let batch = self.config.gc_batch.max(1);
        let sweep_batch = self.config.gc_sweep_batch.max(1);
        let started = Instant::now();
        let budget = self.config.gc_pass_budget;
        let more = |round: u32| round == 0 || started.elapsed() < budget;
        // Eviction refreshes each evicted pack's registration time, so this
        // pass's sweep does not take them: a fetch that read a recording
        // just before may still be streaming its pack.
        for round in 0..crate::GC_ROUNDS {
            if !more(round) {
                report.budget_spent = true;
                break;
            }
            let n = meta
                .evict_shallow_packs(self.config.shallow_ttl, self.config.shallow_unused, batch)
                .await?;
            report.shallow_evicted += n;
            if n < u64::from(batch) {
                break;
            }
        }
        let mut swept = Vec::new();
        let mut round = 0u32;
        loop {
            if !more(round) {
                report.budget_spent = true;
                break;
            }
            round += 1;
            let begun = Instant::now();
            let b = match meta
                .sweep_orphan_packs(&lease, self.config.gc_grace, sweep_batch)
                .await
            {
                Ok(b) => b,
                Err(e) if e.is_lease_lost() => {
                    report.lease_lost = true;
                    break;
                }
                Err(e) => return Err(e.into()),
            };
            if let Some(m) = &self.gc_metrics {
                m.batch(begun.elapsed(), b.examined, b.deleted.len());
            }
            report.sweep_batches += 1;
            report.sweep_examined += b.examined;
            swept.extend(b.deleted);
            if b.caught_up {
                report.sweep_caught_up = true;
                break;
            }
        }
        report.packs_deleted = swept.iter().map(|r| r.id).collect();
        for round in 0..crate::GC_ROUNDS {
            if !more(round) {
                report.budget_spent = true;
                break;
            }
            let pending = meta.pending_pack_deletes(batch).await?;
            let full = pending.len() >= batch as usize;
            let mut done = Vec::with_capacity(pending.len());
            for t in pending {
                let keys = t.keys();
                let row = swept.iter().find(|r| {
                    r.pack.repo == t.repo
                        && r.pack.checksum == t.checksum
                        && r.generation == t.generation
                });
                let mut ok = true;
                for (key, size) in [
                    (&keys.pack, row.map(|r| r.pack.size)),
                    (&keys.idx, row.map(|r| r.pack.idx_size)),
                    (&keys.entries, row.map(|r| r.pack.entries_size)),
                ] {
                    match self.blobs.delete(key).await {
                        Ok(()) => report.bytes_deleted += size.unwrap_or(0),
                        Err(e) => {
                            ok = false;
                            report.blobs_left.push(format!("{key}: {e}"));
                        }
                    }
                }
                if ok {
                    done.push(t);
                }
            }
            meta.finish_pack_deletes(&done).await?;
            report.deletes_finished += done.len();
            // A batch with a failed deletion stops the pass: the same
            // tombstones come first again (oldest first), so another round
            // would only retry them (issue #99 tracks the ordering).
            if !full || done.len() < batch as usize {
                break;
            }
        }
        Ok(report)
    }
}
