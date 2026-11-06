//! The prefetch gate: one memory budget for delta bases and other objects
//! read from stored packs, shared by every concurrent push (PLAN section 3,
//! B1 row).
//!
//! The budget is a semaphore of mebibytes. A holder first reserves a small
//! **floor** ([`ReceiveConfig::prefetch_floor_bytes`], 64 MiB by default)
//! and reads under that budget. A read that needs more reports
//! [`PrefetchError::OverBudget`]; the slot then gives its floor back,
//! reserves the **full** per-call budget
//! (`LocateOptions::max_prefetch_bytes`, at least 1 GiB) and reads again.
//! Nearly every push needs a few MiB of bases, so the default 4 GiB admits
//! 64 pushes at once instead of the 2 that reserving the full budget for
//! every push allowed (bench/throughput: 2 slots held a c7g.2xlarge to
//! about 15 pushes/s with its CPU idle). Large pushes still get the full
//! budget, as many at once as fit.
//!
//! A slot never waits for more while it holds a reservation: it releases
//! the floor before it waits for the full budget, so slots cannot deadlock
//! by each holding part of the budget and waiting for the rest.
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
//! [`Slot`] owns its reservation, so it can move into the blocking task that
//! uses the bases and is released only when that task ends, even if the
//! request that started it is dropped. No code path takes a second slot
//! while it holds one, so the gate cannot deadlock.
//!
//! [`ReceiveConfig::prefetch_floor_bytes`]: super::ReceiveConfig::prefetch_floor_bytes

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aspen_core::blob::BlobStore;
use aspen_core::error::StoreError;
use aspen_core::ingest::{CandidateBases, InMemoryBases, Prefetched, SearchPacks};
use aspen_core::meta::MetaStore;
use aspen_pack::locate::{PrefetchError, PrefetchStats, StoredPacks};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The semaphore's unit.
const MIB: u64 = 1 << 20;

#[derive(Debug, Default)]
struct Counters {
    in_use: AtomicUsize,
    peak: AtomicUsize,
    acquired: AtomicU64,
    timeouts: AtomicU64,
    escalations: AtomicU64,
}

#[derive(Debug)]
struct Shared {
    sem: Arc<Semaphore>,
    /// Units of the floor and of the full budget, each at most the total.
    floor_units: u32,
    full_units: u32,
    floor_bytes: u64,
    full_bytes: u64,
    wait: Duration,
    counters: Counters,
}

#[derive(Debug)]
pub(crate) struct PrefetchGate {
    shared: Arc<Shared>,
    slots: usize,
}

/// A held slot; its reservation is released on drop.
#[derive(Debug)]
pub(crate) struct Slot {
    /// The reservation and the budget it pays for; `None` only while the
    /// slot waits to grow from the floor to the full budget.
    held: Mutex<Option<(OwnedSemaphorePermit, u64)>>,
    shared: Arc<Shared>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.shared.counters.in_use.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Slot {
    /// Reads `candidates` from `packs` under this slot's reservation,
    /// growing it once to the full per-call budget if the floor is not
    /// enough. [`PrefetchError::OverBudget`] means even the full budget is
    /// not enough, as before.
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
        let budget = self.budget();
        match stored
            .prefetch_bases_within(packs, candidates, budget)
            .await
        {
            Err(PrefetchError::OverBudget { .. }) if budget < self.shared.full_bytes => {
                self.grow().await.map_err(PrefetchError::Store)?;
                stored
                    .prefetch_bases_within(packs, candidates, self.shared.full_bytes)
                    .await
            }
            other => other,
        }
    }

    fn budget(&self) -> u64 {
        self.held
            .lock()
            .expect("not poisoned")
            .as_ref()
            .map_or(0, |(_, b)| *b)
    }

    /// Replaces the floor with the full budget: gives the floor back first,
    /// then waits (up to the gate's wait) for the full reservation.
    async fn grow(&self) -> Result<(), StoreError> {
        let floor = self.held.lock().expect("not poisoned").take();
        drop(floor);
        let s = &self.shared;
        s.counters.escalations.fetch_add(1, Ordering::SeqCst);
        let permit = reserve(s, s.full_units).await?;
        *self.held.lock().expect("not poisoned") = Some((permit, s.full_bytes));
        Ok(())
    }
}

async fn reserve(s: &Shared, units: u32) -> Result<OwnedSemaphorePermit, StoreError> {
    let busy = || {
        s.counters.timeouts.fetch_add(1, Ordering::SeqCst);
        StoreError::Unavailable {
            reason: "no prefetch slot came free".into(),
        }
    };
    tokio::time::timeout(s.wait, s.sem.clone().acquire_many_owned(units))
        .await
        .map_err(|_| busy())?
        .map_err(|_| busy())
}

impl PrefetchGate {
    /// A gate over `total_bytes` of memory where each holder reserves
    /// `floor_bytes` and grows to `full_bytes` (the per-call budget) when
    /// a read needs it; a holder-to-be waits up to `wait`. Each reservation
    /// is rounded up to whole MiB and capped at the total, so one holder
    /// can always get the full budget when nothing else is held.
    pub(crate) fn new(total_bytes: u64, floor_bytes: u64, full_bytes: u64, wait: Duration) -> Self {
        let units = |b: u64| b.div_ceil(MIB).max(1);
        let total = units(total_bytes).min(u64::from(u32::MAX >> 3));
        let full = units(full_bytes).min(total);
        let floor = units(floor_bytes.min(full_bytes)).min(full);
        let shared = Arc::new(Shared {
            sem: Arc::new(Semaphore::new(total as usize)),
            floor_units: floor as u32,
            full_units: full as u32,
            floor_bytes: floor_bytes.min(full_bytes),
            full_bytes,
            wait,
            counters: Counters::default(),
        });
        Self {
            shared,
            slots: (total / floor) as usize,
        }
    }

    /// Holders admitted at once while each holds only the floor.
    pub(crate) fn slots(&self) -> usize {
        self.slots
    }

    /// Most slots ever held at once.
    pub(crate) fn peak(&self) -> usize {
        self.shared.counters.peak.load(Ordering::SeqCst)
    }

    /// Slots granted so far.
    pub(crate) fn acquired(&self) -> u64 {
        self.shared.counters.acquired.load(Ordering::SeqCst)
    }

    /// Slots held now.
    pub(crate) fn in_use(&self) -> usize {
        self.shared.counters.in_use.load(Ordering::SeqCst)
    }

    /// Waits that gave up without a slot so far.
    pub(crate) fn timeouts(&self) -> u64 {
        self.shared.counters.timeouts.load(Ordering::SeqCst)
    }

    /// Reads that needed more than the floor, so their slot grew to the
    /// full budget.
    pub(crate) fn escalations(&self) -> u64 {
        self.shared.counters.escalations.load(Ordering::SeqCst)
    }

    /// Waits up to the gate's wait for a slot holding the floor; a
    /// retryable [`StoreError::Unavailable`] if none came free.
    pub(crate) async fn acquire(&self) -> Result<Slot, StoreError> {
        let s = &self.shared;
        let permit = reserve(s, s.floor_units).await?;
        let c = &s.counters;
        let now = c.in_use.fetch_add(1, Ordering::SeqCst) + 1;
        c.peak.fetch_max(now, Ordering::SeqCst);
        c.acquired.fetch_add(1, Ordering::SeqCst);
        Ok(Slot {
            held: Mutex::new(Some((permit, s.floor_bytes))),
            shared: s.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    /// A gate of `n` floor slots of 1 MiB each, with a full budget of all
    /// of them.
    fn slots(n: u64, wait: Duration) -> PrefetchGate {
        PrefetchGate::new(n * MIB, MIB, n * MIB, wait)
    }

    /// Regression (bench/throughput): every push reserved the full per-push
    /// budget (2 GiB with the default 512 MiB object limit), so the default
    /// 4 GiB admitted 2 pushes at once whatever their size. With a 64 MiB
    /// floor the defaults admit 64 small pushes.
    #[tokio::test]
    async fn the_defaults_admit_many_small_pushes() {
        // The server's default object limit, 512 MiB.
        let config = super::super::ReceiveConfig::new(
            "spool",
            aspen_pack::MaxObjectBytes::new(512 << 20).unwrap(),
            aspen_pack::MaxInflatedBytes::new(8 << 30).unwrap(),
        );
        let gate = PrefetchGate::new(
            config.prefetch_memory_bytes,
            config.prefetch_floor_bytes,
            config.locate.max_prefetch_bytes,
            Duration::from_millis(20),
        );
        assert!(config.locate.max_prefetch_bytes >= GIB);
        assert_eq!(gate.slots(), 64);
        let mut held = Vec::new();
        for _ in 0..64 {
            held.push(gate.acquire().await.unwrap());
        }
        assert!(gate.acquire().await.is_err(), "the 65th waits");
    }

    /// Invariant: a slot that grows gives its floor back before it waits
    /// for the full budget, so slots that all need to grow do not
    /// deadlock; each gets the full budget in turn, and while one holds
    /// it, nobody else holds anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slots_grow_to_the_full_budget_one_at_a_time() {
        let gate = Arc::new(PrefetchGate::new(
            4 * MIB,
            MIB,
            4 * MIB,
            Duration::from_secs(10),
        ));
        let mut slots = Vec::new();
        for _ in 0..4 {
            slots.push(gate.acquire().await.unwrap());
        }
        let busy = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for slot in slots {
            let busy = busy.clone();
            tasks.push(tokio::spawn(async move {
                slot.grow().await.unwrap();
                assert_eq!(slot.budget(), 4 * MIB);
                assert_eq!(busy.fetch_add(1, Ordering::SeqCst), 0, "one full holder");
                tokio::time::sleep(Duration::from_millis(10)).await;
                busy.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(gate.escalations(), 4);
        assert_eq!(gate.in_use(), 0);
    }

    /// A full budget larger than all the memory is capped at it: one
    /// holder can still grow when nothing else is held.
    #[tokio::test]
    async fn a_full_budget_over_the_total_is_capped() {
        let gate = PrefetchGate::new(4 * MIB, MIB, 2 * GIB, Duration::from_millis(20));
        assert_eq!(gate.slots(), 4);
        let slot = gate.acquire().await.unwrap();
        slot.grow().await.unwrap();
        assert_eq!(slot.budget(), 2 * GIB);
        assert!(
            gate.acquire().await.is_err(),
            "the grown slot holds everything"
        );
        drop(slot);
        assert!(gate.acquire().await.is_ok());
    }

    /// Invariant: no more than `slots` holders at once, and a waiter gives
    /// up after its wait instead of queueing forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_gate_admits_at_most_its_slots() {
        let gate = Arc::new(slots(2, Duration::from_secs(10)));
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
        let short = slots(2, Duration::from_millis(20));
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
        let gate = Arc::new(slots(1, Duration::from_millis(50)));
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
