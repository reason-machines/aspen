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

use std::time::Instant;

use aspen_core::blob::BlobStore;
use aspen_core::id::PackId;
use aspen_core::meta::MetaStore;

use crate::CompactError;
use crate::Compactor;

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Exact shallow pack recordings evicted (node C8): recorded at least
    /// [`crate::CompactConfig::shallow_ttl`] ago for a version that is no
    /// longer current, or unused for
    /// [`crate::CompactConfig::shallow_unused`] (issue #108). Their packs
    /// are swept a grace period later.
    pub shallow_evicted: u64,
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

impl<M, B> Compactor<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    /// Runs one pass: eviction of shallow pack recordings of superseded
    /// versions, the sweep, then the blob deletions of pending tombstones,
    /// oldest first (so a crashed worker's are retried by the next one).
    /// Each step runs in transactions of [`crate::CompactConfig::gc_batch`]
    /// and repeats while a transaction fills its batch, up to
    /// [`crate::GC_ROUNDS`] times, so a pass collects more than one batch
    /// when more is due (issue #203: one batch per pass fell behind a busy
    /// repository's evictions). Every step runs its first batch; a further
    /// batch starts only while the pass has run less than
    /// [`crate::CompactConfig::gc_pass_budget`], so a large backlog cannot
    /// hold the worker's jobs up for long, and no step is starved by an
    /// earlier one. Running it again, or concurrently on another worker, is
    /// safe: a tombstone's keys belong to a deleted generation that no row
    /// will ever name again.
    pub async fn collect_garbage(&self) -> Result<GcReport, CompactError> {
        // `validate` refuses 0; a batch of 0 would never fill, so would
        // otherwise repeat every step `GC_ROUNDS` times for nothing.
        let batch = self.config.gc_batch.max(1);
        let started = Instant::now();
        let budget = self.config.gc_pass_budget;
        let more = |round: u32| round == 0 || started.elapsed() < budget;
        let mut report = GcReport::default();
        // Eviction refreshes each evicted pack's registration time, so this
        // pass's sweep does not take them: a fetch that read a recording
        // just before may still be streaming its pack.
        for round in 0..crate::GC_ROUNDS {
            if !more(round) {
                report.budget_spent = true;
                break;
            }
            let n = self
                .meta
                .evict_shallow_packs(self.config.shallow_ttl, self.config.shallow_unused, batch)
                .await?;
            report.shallow_evicted += n;
            if n < u64::from(batch) {
                break;
            }
        }
        let mut swept = Vec::new();
        for round in 0..crate::GC_ROUNDS {
            if !more(round) {
                report.budget_spent = true;
                break;
            }
            let rows = self
                .meta
                .sweep_orphan_packs(self.config.gc_grace, batch)
                .await?;
            let full = rows.len() >= batch as usize;
            swept.extend(rows);
            if !full {
                break;
            }
        }
        report.packs_deleted = swept.iter().map(|r| r.id).collect();
        for round in 0..crate::GC_ROUNDS {
            if !more(round) {
                report.budget_spent = true;
                break;
            }
            let pending = self.meta.pending_pack_deletes(batch).await?;
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
            self.meta.finish_pack_deletes(&done).await?;
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
