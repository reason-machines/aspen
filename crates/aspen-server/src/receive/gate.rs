//! The prefetch gate: one memory budget for delta bases and other objects
//! read from stored packs, shared by every concurrent push (PLAN section 3,
//! B1 row).
//!
//! `StoredPacks::prefetch_bases` bounds one call by
//! `LocateOptions::max_prefetch_bytes`, and its caller holds the result
//! until it is done with it. So the gate admits at most `slots` holders at
//! once, where `slots` is the shared budget divided by the per-call budget.
//!
//! The gate bounds **object reads**, the memory that grows with what is
//! read. Locating objects (`.idx` lookups through `ObjectLocator::locate`,
//! for the held flags and the connectivity check) reads only index pages
//! and takes no slot.
//!
//! Every object read from stored packs on the push path goes through
//! [`Slot::prefetch`], so it cannot run without a held slot: the thin-pack
//! bases (held until completion ends), the forced-trunk reads (held until
//! the extra objects are spilled to disk), the one-object reads of a tip's
//! type and a tag's target, and the commit reads of the ancestry walk. A
//! [`Slot`] owns its permit, so it can move into the blocking task that
//! uses the bases and is released only when that task ends, even if the
//! request that started it is dropped. No code path takes a second slot
//! while it holds one, so a one-slot gate cannot deadlock.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::ingest::{CandidateBases, InMemoryBases, Prefetched, SearchPacks};
use aspen_core::meta::MetaStore;
use aspen_pack::locate::{PrefetchError, PrefetchStats, StoredPacks};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Default)]
struct Counters {
    in_use: AtomicUsize,
    peak: AtomicUsize,
    acquired: AtomicU64,
    timeouts: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct PrefetchGate {
    sem: Arc<Semaphore>,
    slots: usize,
    wait: Duration,
    counters: Arc<Counters>,
}

/// A held slot; released on drop.
#[derive(Debug)]
pub(crate) struct Slot {
    _permit: OwnedSemaphorePermit,
    counters: Arc<Counters>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.counters.in_use.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Slot {
    /// Reads `candidates` from `packs` under this slot's share of the
    /// budget.
    pub(crate) async fn prefetch<M, B>(
        &self,
        stored: &StoredPacks<M, B>,
        packs: &SearchPacks,
        candidates: &CandidateBases,
    ) -> Result<(Prefetched<InMemoryBases>, PrefetchStats), PrefetchError>
    where
        M: MetaStore + Clone + 'static,
        B: BlobStore + Clone + 'static,
    {
        stored.prefetch_bases(packs, candidates).await
    }
}

impl PrefetchGate {
    /// A gate of `slots` (at least 1); a holder-to-be waits up to `wait`.
    pub(crate) fn new(slots: usize, wait: Duration) -> Self {
        let slots = slots.max(1);
        Self {
            sem: Arc::new(Semaphore::new(slots)),
            slots,
            wait,
            counters: Arc::default(),
        }
    }

    pub(crate) fn slots(&self) -> usize {
        self.slots
    }

    /// Most slots ever held at once.
    pub(crate) fn peak(&self) -> usize {
        self.counters.peak.load(Ordering::SeqCst)
    }

    /// Slots granted so far.
    pub(crate) fn acquired(&self) -> u64 {
        self.counters.acquired.load(Ordering::SeqCst)
    }

    /// Slots held now.
    pub(crate) fn in_use(&self) -> usize {
        self.counters.in_use.load(Ordering::SeqCst)
    }

    /// Waits that gave up without a slot so far.
    pub(crate) fn timeouts(&self) -> u64 {
        self.counters.timeouts.load(Ordering::SeqCst)
    }

    /// Waits up to the gate's wait for a slot; a retryable
    /// [`StoreError::Unavailable`] if none came free.
    pub(crate) async fn acquire(&self) -> Result<Slot, StoreError> {
        let busy = || {
            self.counters.timeouts.fetch_add(1, Ordering::SeqCst);
            StoreError::Unavailable {
                reason: "no prefetch slot came free".into(),
            }
        };
        let permit = tokio::time::timeout(self.wait, self.sem.clone().acquire_owned())
            .await
            .map_err(|_| busy())?
            .map_err(|_| busy())?;
        let c = &self.counters;
        let now = c.in_use.fetch_add(1, Ordering::SeqCst) + 1;
        c.peak.fetch_max(now, Ordering::SeqCst);
        c.acquired.fetch_add(1, Ordering::SeqCst);
        Ok(Slot {
            _permit: permit,
            counters: c.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Invariant: no more than `slots` holders at once, and a waiter gives
    /// up after its wait instead of queueing forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_gate_admits_at_most_its_slots() {
        let gate = Arc::new(PrefetchGate::new(2, Duration::from_secs(10)));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let gate = gate.clone();
            tasks.push(tokio::spawn(async move {
                let slot = gate.acquire().await.unwrap();
                tokio::time::sleep(Duration::from_millis(20)).await;
                drop(slot);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(gate.peak(), 2);
        let short = PrefetchGate::new(2, Duration::from_millis(20));
        let a = short.acquire().await.unwrap();
        let b = short.acquire().await.unwrap();
        assert!(short.acquire().await.is_err());
        drop((a, b));
        assert!(short.acquire().await.is_ok());
    }

    /// Invariant: a slot moved into a blocking task stays held until that
    /// task ends, even when the future that awaited the task is dropped
    /// first (a client that hangs up mid-completion).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_slot_moved_into_a_blocking_task_outlives_a_dropped_request() {
        let gate = Arc::new(PrefetchGate::new(1, Duration::from_millis(50)));
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let slot = gate.acquire().await.unwrap();
        let request = tokio::spawn(async move {
            tokio::task::spawn_blocking(move || {
                let _slot = slot;
                let _ = wait.recv();
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        request.abort();
        let _ = request.await;
        assert!(
            gate.acquire().await.is_err(),
            "the blocking task still holds the slot"
        );
        release.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(gate.acquire().await.is_ok());
    }
}
