//! Finding objects in stored packs: the `ObjectLocator` and
//! `BasePrefetcher` contracts of `aspen_core::ingest`, implemented over any
//! `MetaStore` (for pack rows) and `BlobStore` (for `.idx` files and pack
//! entries).
//!
//! - **Location** probes each search pack's `.idx` (fanout, then binary
//!   search) in probe order and stops at the first hit (S3, "Base-object
//!   lookup"). `.idx` files are immutable, so they are cached in memory up to
//!   a byte budget; a warm lookup reads no object storage.
//! - **Cold start** (monorepo benchmark, item 8): concurrent misses on one
//!   pack share one load (single flight), so 128 pushes that start together
//!   read each `.idx` once, not 128 times. Whether a pack's `.idx` is kept
//!   whole or sampled depends on the search set, not only on its own size:
//!   in probe order, an `.idx` is kept whole only while the set's whole
//!   `.idx` files still fit [`LocateOptions::idx_cache_bytes`], and the rest
//!   are sampled. A repository whose indexes outgrow the cache then keeps
//!   all of them (part whole, part sampled) instead of evicting and
//!   reloading them on every push.
//! - **Prefetch** locates every candidate base, then reads its entry and its
//!   delta chain from the blob store. A pack whose `.idx` is kept whole is
//!   kept with its **chain map**, each entry's base from the pack's entry
//!   table, so every entry of every chain is known before anything is read
//!   and they are all read in one round of parallel range reads: a chain of
//!   depth 50 (what `git gc` and compaction write) costs one round trip,
//!   not 51 (monorepo benchmark, item 11). In a sampled pack
//!   (`crate::sampled`) chains are still found by reading: each round's
//!   reads reveal the next bases, so a chain of depth d costs d + 1 rounds
//!   there. Chains are then resolved locally and the full objects handed to
//!   completion as [`aspen_core::ingest::InMemoryBases`].
//! - **Lookups** in sampled packs read a window's IDs and offsets together
//!   (one round trip), and probe every sampled search pack at once, so a
//!   lookup costs one round trip however many large packs the repository
//!   has; probe order still decides which pack answers. With a local
//!   directory ([`StoredPacks::with_local_dir`]) the sample keeps a copy of
//!   the sections lookups read on local disk, written while it is built
//!   (`crate::sampled`), and a lookup sends no blob store request at all
//!   (node 12b: a push looks up every object it brings, and each lookup in a
//!   sampled `.idx` was two GETs).
//! - **Local disk** ([`StoredPacks::with_local_dir`]): beside each sampled
//!   `.idx` in the cache, its chain map (16 bytes per object) and its
//!   lookup sections (24 bytes per object), in unnamed files deleted when
//!   the sample leaves the cache.
//! - **Memory** of a prefetch is bounded by
//!   [`LocateOptions::max_prefetch_bytes`]: every inflated entry, every
//!   intermediate chain result and every output object is charged before it
//!   is allocated, and going over refuses the prefetch with
//!   [`PrefetchError::OverBudget`]. An intermediate result is kept only
//!   while another chain or candidate still needs it, and each object is
//!   moved, not copied, into the output when nothing else needs it, so one
//!   chain of depth d over objects of size S peaks near 2 S, not (d + 2) S.
//!
//! Every read is of a range inside a size the `packs` row states, so it
//! uses [`BlobStore::read_exact_range`]: a short read is
//! [`StoreError::Corrupt`].

use std::collections::HashMap;

use hashlink::LinkedHashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use aspen_core::blob::{BlobKey, BlobStore, ByteRange};
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, PackId};
use aspen_core::ingest::{
    BasePrefetcher, CandidateBases, InMemoryBases, ObjectKind, ObjectLocation, ObjectLocator,
    Prefetched, SearchPacks, SortedIds,
};
use aspen_core::meta::MetaStore;
use aspen_core::pack::{PACK_HEADER_LEN, PACK_TRAILER_LEN, PackRecord};

use crate::MaxObjectBytes;
use crate::chains::{ChainsBuilder, ChainsError, DiskChains};
use crate::header::{self, RawBase};
use crate::idx::IdxV2;
use crate::sampled::{SampleBuilder, SampledIdx};

/// IDs (or CRCs, offsets) per read while a sample is built: 1.25 MiB of
/// IDs per read, eight reads in flight.
const SAMPLE_READ_ENTRIES: u64 = 1 << 16;

/// The first read of an entry in a sampled pack, whose end is not known
/// without the whole `.idx`: most entries fit, and a longer one is read
/// again with room for its compressed size.
const FIRST_ENTRY_READ: u64 = 16 << 10;

/// The one `MetaStore` method this module needs, so it can be tested
/// without a database. Every `MetaStore` provides it.
pub trait PackRows: Send + Sync {
    /// Pack rows by ID, in the order asked ([`MetaStore::packs`]).
    fn pack_rows(
        &self,
        ids: &[PackId],
    ) -> impl Future<Output = Result<Vec<PackRecord>, StoreError>> + Send;
}

impl<M: MetaStore> PackRows for M {
    fn pack_rows(
        &self,
        ids: &[PackId],
    ) -> impl Future<Output = Result<Vec<PackRecord>, StoreError>> + Send {
        self.packs(ids)
    }
}

/// Tuning for [`StoredPacks`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocateOptions {
    /// Bytes of `.idx` kept in memory (about 6% of the packs they index,
    /// S4), plus 8 bytes per object of a whole one's sorted offsets. Least
    /// recently used entries are evicted first. A lookup whose search packs'
    /// whole `.idx` files do not fit keeps the ones probed first whole and
    /// samples the rest, so one repository never evicts its own indexes.
    pub idx_cache_bytes: u64,
    /// Largest `.idx` kept whole (default 64 MiB, a pack of about 2.4
    /// million objects). A larger one is kept as a sample, one ID of every
    /// 256 (`crate::sampled`), and each lookup in it reads the 256 IDs that
    /// can hold the object, and their offsets: from the sample's copy on
    /// local disk when the locator has a directory for one
    /// ([`StoredPacks::with_local_dir`]), else from the blob store (two
    /// reads of about 5 KiB and 1 KiB), however large the pack. Without
    /// it, a repository whose `.idx` files outgrow
    /// [`Self::idx_cache_bytes`] (about 7 million objects at the default
    /// 256 MiB) reads them whole on every push. Keep it at most a quarter
    /// of `idx_cache_bytes`, so several whole ones fit.
    pub max_whole_idx_bytes: u64,
    /// Bytes of resolved base objects kept in memory, least recently used
    /// evicted first; objects over [`OBJECT_CACHE_MAX_ENTRY`] are never
    /// kept. Objects are immutable, so a hit is exact. Consecutive pushes
    /// to one repository mostly need the same few bases (trunk's root tree
    /// and the directories above the files they change), and resolving one
    /// from a stored pack costs a blob-store read per delta in its chain.
    /// 0 turns the cache off.
    pub object_cache_bytes: u64,
    /// Most blob reads in flight at once: entries in a prefetch round, and
    /// lookups in sampled `.idx` files (each a few kilobytes). A round with
    /// more waits for a second wave, a whole round trip: a push reads every
    /// chain of its bases in one round, up to about 10 bases at the depth
    /// cap of 50 or 20 at compaction's tree cap of 10, and probes each
    /// sampled pack for each ID it looks up (item 11).
    pub max_parallel_reads: usize,
    /// Largest inflated object or delta a prefetch accepts; use ingest's
    /// limit. Stored entries were validated at ingest; this bounds memory
    /// if a row is wrong.
    pub max_object_bytes: MaxObjectBytes,
    /// Most bytes one prefetch holds at once: inflated entries, chain
    /// results and the bases it returns (compressed reads in flight too).
    /// Over it, the prefetch fails with [`PrefetchError::OverBudget`].
    pub max_prefetch_bytes: u64,
}

impl LocateOptions {
    /// Defaults for everything but the object limit, which must come from
    /// the server's configuration: a 1 GiB `.idx` cache (an `.idx` over
    /// 64 MiB is sampled), a 64 MiB base object cache, 256 reads in flight,
    /// and a prefetch budget of four times the object limit (at least
    /// 1 GiB).
    ///
    /// 1 GiB holds every index of a repository of about 30 million objects
    /// whole (the monorepo benchmark's 11.6 million take 400 MiB); a larger
    /// one is part sampled, never reloaded. It is an upper bound, not a
    /// reservation: a server with small repositories uses what they need.
    pub fn new(max_object_bytes: MaxObjectBytes) -> Self {
        Self {
            idx_cache_bytes: DEFAULT_IDX_CACHE_BYTES,
            max_whole_idx_bytes: 64 << 20,
            object_cache_bytes: 64 << 20,
            max_parallel_reads: 256,
            max_object_bytes,
            max_prefetch_bytes: max_object_bytes.get().saturating_mul(4).max(1 << 30),
        }
    }
}

/// [`LocateOptions::idx_cache_bytes`] by default: 1 GiB.
pub const DEFAULT_IDX_CACHE_BYTES: u64 = 1 << 30;

/// The largest object the base-object cache keeps
/// ([`LocateOptions::object_cache_bytes`]).
pub const OBJECT_CACHE_MAX_ENTRY: u64 = 1 << 20;

/// Why [`StoredPacks::prefetch_bases`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PrefetchError {
    /// Reading or decoding stored data failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The bases and their chains need more memory than
    /// [`LocateOptions::max_prefetch_bytes`]. Nothing is wrong with stored
    /// data and a retry of the same push fails the same way: refuse the
    /// push (it needs too many large bases at once) without paging.
    #[error("prefetch needs more than its budget of {budget} bytes")]
    OverBudget {
        /// The budget.
        budget: u64,
    },
}

/// Memory figures of one prefetch, for metrics and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PrefetchStats {
    /// Most bytes held at once.
    pub peak_bytes: u64,
    /// Bytes of the returned bases.
    pub output_bytes: u64,
}

/// Bytes charged against a prefetch's budget.
/// Futures of one prefetch share it, and the prefetch future must be
/// `Send`, so it is a mutex (never contended: one task polls them all).
#[derive(Debug)]
struct Budget {
    limit: u64,
    /// (held, peak).
    state: Mutex<(u64, u64)>,
}

impl Budget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            state: Mutex::new((0, 0)),
        }
    }

    fn charge(&self, bytes: u64) -> Result<(), PrefetchError> {
        let mut s = self.state.lock().expect("not poisoned");
        let held = s.0.saturating_add(bytes);
        if held > self.limit {
            return Err(PrefetchError::OverBudget { budget: self.limit });
        }
        *s = (held, s.1.max(held));
        Ok(())
    }

    fn release(&self, bytes: u64) {
        let mut s = self.state.lock().expect("not poisoned");
        s.0 = s.0.saturating_sub(bytes);
    }

    fn peak(&self) -> u64 {
        self.state.lock().expect("not poisoned").1
    }
}

/// A loaded, validated `.idx` with its pack row.
#[derive(Debug)]
struct LoadedIdx {
    record: PackRecord,
    bytes: Vec<u8>,
    /// Entry offsets in pack order, to find where an entry ends.
    sorted_offsets: Vec<u64>,
    /// The chain map: per entry in pack order, the position (in
    /// `sorted_offsets`) of its delta base, or [`NO_BASE`] for a full
    /// object. From the pack's entry table.
    bases: Vec<u32>,
}

/// A full object in [`LoadedIdx::bases`].
const NO_BASE: u32 = u32::MAX;

impl LoadedIdx {
    /// Bytes this entry holds in the cache: the `.idx`, the offsets and the
    /// chain map.
    fn cost(&self) -> u64 {
        (self.bytes.len()
            + self.sorted_offsets.len() * std::mem::size_of::<u64>()
            + self.bases.len() * std::mem::size_of::<u32>()) as u64
    }

    /// The entry at `offset` and every entry below it on its delta chain,
    /// from the chain map, appended to `out`. Stops early (the prefetch's
    /// rounds find the rest) at an offset that is not an entry or a base
    /// that is not earlier, which a stored pack never has.
    fn chain(&self, pack: usize, offset: u64, out: &mut Vec<At>) {
        let Ok(mut i) = self.sorted_offsets.binary_search(&offset) else {
            out.push((pack, offset));
            return;
        };
        loop {
            out.push((pack, self.sorted_offsets[i]));
            match self.bases[i] {
                NO_BASE => return,
                b if (b as usize) < i => i = b as usize,
                _ => return,
            }
        }
    }

    fn idx(&self) -> IdxV2<'_> {
        IdxV2::parse_trusted(&self.bytes)
    }

    fn entry_range(&self, offset: u64) -> Result<ByteRange, StoreError> {
        let i = self
            .sorted_offsets
            .binary_search(&offset)
            .map_err(|_| corrupt(&self.record, format!("no entry starts at offset {offset}")))?;
        let end = self
            .sorted_offsets
            .get(i + 1)
            .copied()
            .unwrap_or(self.record.pack.size - PACK_TRAILER_LEN);
        ByteRange::new(offset, end)
            .ok_or_else(|| corrupt(&self.record, format!("empty entry at {offset}")))
    }
}

/// What a pack's `.idx` costs in the cache kept whole: the file plus its
/// sorted offsets and chain map ([`LoadedIdx::cost`]).
fn whole_cost(row: &PackRecord) -> u64 {
    // Offsets (8 bytes) and the chain map (4 bytes) per object.
    row.pack
        .idx_size
        .saturating_add(u64::from(row.pack.object_count) * 12)
}

/// About what a pack's `.idx` costs in the cache kept as a sample
/// ([`SampledIdx::cost`]).
fn sampled_cost(row: &PackRecord) -> u64 {
    let samples = u64::from(row.pack.object_count).div_ceil(crate::sampled::SAMPLE_EVERY);
    samples * 20 + 256 * 4 + 64
}

/// A search pack's `.idx` as the locator holds it.
#[derive(Debug, Clone)]
enum Idx {
    /// The whole file.
    Whole(Arc<LoadedIdx>),
    /// A sample of a file over [`LocateOptions::max_whole_idx_bytes`]
    /// (`crate::sampled`), with its chain map on local disk
    /// (`crate::chains`) when the locator has a directory for one.
    Sampled(Arc<SampledIdx>, Option<Arc<DiskChains>>),
}

impl Idx {
    fn record(&self) -> &PackRecord {
        match self {
            Self::Whole(l) => &l.record,
            Self::Sampled(s, _) => &s.record,
        }
    }

    fn cost(&self) -> u64 {
        match self {
            Self::Whole(l) => l.cost(),
            Self::Sampled(s, c) => s.cost() + c.as_ref().map_or(0, |c| c.cost()),
        }
    }
}

/// What a [`StoredPacks`] has read and looked up since it was made: for
/// metrics, and for the tests that bound what one push costs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LocateStats {
    /// `.idx` files read whole and kept.
    pub whole_idx_loads: u64,
    /// `.idx` files read once to build a sample.
    pub sampled_idx_loads: u64,
    /// Bytes of `.idx` read from the blob store: whole loads, samples, and
    /// the IDs and offsets lookups in samples read.
    pub idx_bytes_read: u64,
    /// Bytes of entry tables read from the blob store for the chain maps of
    /// whole `.idx` files.
    pub table_bytes_read: u64,
    /// Object IDs looked up (each once per call, however many packs it was
    /// probed in).
    pub ids_looked_up: u64,
    /// Lookups in sampled `.idx` files answered from the sample's copy on
    /// local disk, with no blob store read (one per sampled pack probed).
    pub local_lookups: u64,
    /// Pack entries read from the blob store by prefetches.
    pub entries_read: u64,
    /// Misses that waited for another call's load of the same `.idx`
    /// instead of reading it again.
    pub idx_load_waits: u64,
}

#[derive(Debug, Default)]
struct Counters {
    whole_idx_loads: AtomicU64,
    sampled_idx_loads: AtomicU64,
    idx_bytes_read: AtomicU64,
    table_bytes_read: AtomicU64,
    ids_looked_up: AtomicU64,
    local_lookups: AtomicU64,
    entries_read: AtomicU64,
    idx_load_waits: AtomicU64,
}

/// A least-recently-used cache bounded in bytes: the `.idx` cache and the
/// base-object cache.
///
/// Every operation is O(1): a hit moves its entry to the back of a linked
/// hash map, and eviction pops from the front. The cache is behind one
/// mutex on every push and fetch, and it holds one entry per recently used
/// pack, tens of thousands on a busy server; a linear scan per hit (as an
/// earlier version did) made every push slower as packs accumulated and
/// serialized pushes on the lock.
#[derive(Debug)]
struct Cache<K, V> {
    /// Value and its cost in bytes; least recently used at the front.
    entries: LinkedHashMap<K, (V, u64)>,
    bytes: u64,
    budget: u64,
}

impl<K: std::hash::Hash + Eq, V: Clone> Cache<K, V> {
    fn new(budget: u64) -> Self {
        Self {
            entries: LinkedHashMap::new(),
            bytes: 0,
            budget,
        }
    }

    /// The entry for `id`, now the most recently used.
    fn get(&mut self, id: &K) -> Option<V> {
        self.entries.to_back(id).map(|(v, _)| v.clone())
    }

    /// Adds `value` as the most recently used entry unless `id` is cached,
    /// then evicts least recently used entries until the cache is within
    /// its budget. An entry larger than the whole budget is evicted at once
    /// (the caller still has its copy).
    fn insert(&mut self, id: K, value: V, cost: u64) {
        if self.entries.contains_key(&id) {
            return;
        }
        self.bytes += cost;
        self.entries.insert(id, (value, cost));
        while self.bytes > self.budget {
            let Some((_, (_, cost))) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= cost;
        }
    }
}

/// Stored packs as an object locator and base prefetcher.
#[derive(Debug)]
pub struct StoredPacks<M, B> {
    meta: M,
    blobs: B,
    options: LocateOptions,
    cache: Mutex<Cache<PackId, Idx>>,
    /// `.idx` loads in progress, so concurrent misses on a pack share one.
    /// Lock order: `flights`, then `cache`.
    flights: Mutex<HashMap<PackId, Arc<Flight>>>,
    objects: Mutex<Cache<ObjectId, CachedObject>>,
    /// Prefetched objects answered by the base-object cache, and read from
    /// the blob store.
    object_hits: std::sync::atomic::AtomicU64,
    object_misses: std::sync::atomic::AtomicU64,
    counters: Counters,
    /// Where sampled packs' chain maps (`crate::chains`) and lookup
    /// sections (`crate::sampled`) are kept; `None` keeps neither.
    local_dir: Option<std::path::PathBuf>,
}

/// A resolved base object in the base-object cache.
type CachedObject = Arc<(ObjectKind, Vec<u8>)>;

impl<M: PackRows, B: BlobStore> StoredPacks<M, B> {
    /// Wraps the stores.
    pub fn new(meta: M, blobs: B, options: LocateOptions) -> Self {
        Self {
            meta,
            blobs,
            options,
            cache: Mutex::new(Cache::new(options.idx_cache_bytes)),
            flights: Mutex::default(),
            objects: Mutex::new(Cache::new(options.object_cache_bytes)),
            object_hits: Default::default(),
            object_misses: Default::default(),
            counters: Counters::default(),
            local_dir: None,
        }
    }

    /// Keeps, for every sampled pack in the `.idx` cache, its chain map
    /// (`crate::chains`, 16 bytes per object) and the sections of its
    /// `.idx` a lookup reads (`crate::sampled`, 24 bytes per object) in
    /// files in `dir`, a real local disk: about 40 bytes per object in all,
    /// deleted when the sample leaves the cache. Without it a chain in a
    /// sampled pack costs a round trip per level, and each lookup in one
    /// reads the blob store twice.
    pub fn with_local_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.local_dir = Some(dir.into());
        self
    }

    /// What this locator has read and looked up so far.
    pub fn stats(&self) -> LocateStats {
        let c = &self.counters;
        LocateStats {
            whole_idx_loads: c.whole_idx_loads.load(Relaxed),
            sampled_idx_loads: c.sampled_idx_loads.load(Relaxed),
            idx_bytes_read: c.idx_bytes_read.load(Relaxed),
            table_bytes_read: c.table_bytes_read.load(Relaxed),
            ids_looked_up: c.ids_looked_up.load(Relaxed),
            local_lookups: c.local_lookups.load(Relaxed),
            entries_read: c.entries_read.load(Relaxed),
            idx_load_waits: c.idx_load_waits.load(Relaxed),
        }
    }

    /// The `.idx` of each pack, from the cache or the blob store.
    ///
    /// Misses are loaded in parallel, each through [`Self::load_shared`],
    /// so a pack another call is already loading is waited for, not read
    /// again. Each miss is kept whole or sampled by [`Self::plan_misses`].
    async fn load(&self, packs: &[PackId]) -> Result<Vec<Idx>, StoreError> {
        let mut out: Vec<Option<Idx>> = {
            let mut cache = self.cache.lock().expect("not poisoned");
            packs.iter().map(|p| cache.get(p)).collect()
        };
        let missing: Vec<PackId> = packs
            .iter()
            .zip(&out)
            .filter(|(_, o)| o.is_none())
            .map(|(p, _)| *p)
            .collect();
        if !missing.is_empty() {
            let rows = self.meta.pack_rows(&missing).await?;
            if rows.len() != missing.len() || rows.iter().zip(&missing).any(|(r, p)| r.id != *p) {
                return Err(StoreError::Corrupt {
                    what: "the metadata store returned pack rows out of order".into(),
                });
            }
            let cached: u64 = out.iter().flatten().map(Idx::cost).sum();
            let whole = self.plan_misses(&rows, cached);
            let loads = join_bounded(
                rows.into_iter()
                    .zip(whole)
                    .map(|(r, whole)| self.load_shared(r, whole)),
                self.options.max_parallel_reads,
            )
            .await;
            let mut fresh = loads.into_iter();
            for slot in out.iter_mut().filter(|o| o.is_none()) {
                *slot = Some(fresh.next().expect("one load per miss")?);
            }
        }
        Ok(out.into_iter().map(|o| o.expect("filled")).collect())
    }

    /// Whether each missed pack (in probe order) is kept whole: while the
    /// search set's whole `.idx` files, after the `cached` bytes it already
    /// holds, fit [`LocateOptions::idx_cache_bytes`], and only an `.idx` of
    /// at most [`LocateOptions::max_whole_idx_bytes`]. The rest are sampled.
    ///
    /// Without this, a set over the budget was loaded whole, evicted itself
    /// as it was inserted, and was read whole again by the next push: the
    /// monorepo benchmark's 400 MiB of indexes against a 256 MiB cache read
    /// every `.idx` on every push.
    fn plan_misses(&self, rows: &[PackRecord], cached: u64) -> Vec<bool> {
        let mut left = self.options.idx_cache_bytes.saturating_sub(cached);
        rows.iter()
            .map(|r| {
                let cost = whole_cost(r);
                let whole = r.pack.idx_size <= self.options.max_whole_idx_bytes && cost <= left;
                left = left.saturating_sub(if whole { cost } else { sampled_cost(r) });
                whole
            })
            .collect()
    }

    /// One pack's `.idx`, loaded once however many calls miss it at the
    /// same time: the first becomes the leader and loads it (and caches
    /// it), the others wait for its result. A leader that is dropped
    /// before it finishes (its request ended) hands the load to a waiter.
    async fn load_shared(&self, row: PackRecord, whole: bool) -> Result<Idx, StoreError> {
        loop {
            let (flight, leader) = {
                let mut flights = self.flights.lock().expect("not poisoned");
                // Under the flights lock: a leader caches its result before
                // it leaves the map, so a pack is always in one or the other.
                if let Some(hit) = self.cache.lock().expect("not poisoned").get(&row.id) {
                    return Ok(hit);
                }
                match flights.get(&row.id) {
                    Some(f) => (Arc::clone(f), false),
                    None => {
                        let f = Arc::new(Flight::default());
                        flights.insert(row.id, Arc::clone(&f));
                        (f, true)
                    }
                }
            };
            if !leader {
                self.counters.idx_load_waits.fetch_add(1, Relaxed);
                match (WaitFlight { flight: &flight }).await {
                    Some(result) => return result,
                    // The leader was dropped: try again, maybe as leader.
                    None => continue,
                }
            }
            let mut lead = Lead {
                flights: &self.flights,
                id: row.id,
                flight,
                finished: false,
            };
            let result = self.load_one(row, whole).await;
            if let Ok(idx) = &result {
                let mut cache = self.cache.lock().expect("not poisoned");
                cache.insert(idx.record().id, idx.clone(), idx.cost());
            }
            lead.finish(result.clone());
            return result;
        }
    }

    /// Reads one pack's `.idx`: whole with its chain map (the `.idx` and
    /// the entry table read at once), or as a sample with its chain map and
    /// lookup sections on disk when the locator has a directory for them.
    async fn load_one(&self, row: PackRecord, whole: bool) -> Result<Idx, StoreError> {
        if !whole {
            let dir = self.local_dir.as_deref();
            let chains = async {
                match dir {
                    Some(dir) => self.load_chains(&row, dir).await,
                    None => Ok(None),
                }
            };
            let (chains, sampled) = futures_join(chains, self.load_sampled(row.clone(), dir)).await;
            return Ok(Idx::Sampled(Arc::new(sampled?), chains?.map(Arc::new)));
        }
        let range = ByteRange::new(0, row.pack.idx_size)
            .ok_or_else(|| corrupt(&row, "empty .idx".into()))?;
        // The entry table streams in sections, so a large one is never
        // held whole: its first section is read with the `.idx`, the rest
        // two at a time once the `.idx` is checked.
        let count = u64::from(row.pack.object_count);
        let sections: Vec<(u64, u64)> = (0..count)
            .step_by(SAMPLE_READ_ENTRIES as usize)
            .map(|from| ChainsBuilder::records_range(from, (from + SAMPLE_READ_ENTRIES).min(count)))
            .collect();
        let keys = row.keys();
        let read_table = |(from, to): (u64, u64)| {
            let key = &keys.entries;
            async move {
                let range = ByteRange::new(from, to).expect("non-empty");
                let bytes = self.blobs.read_exact_range(key, range).await?;
                self.counters
                    .table_bytes_read
                    .fetch_add(bytes.len() as u64, Relaxed);
                Ok::<_, StoreError>(bytes)
            }
        };
        let header_and_first = (
            0,
            sections
                .first()
                .map_or(ChainsBuilder::header_range().1, |s| s.1),
        );
        let (bytes, first) = futures_join(
            self.read_idx(&keys.idx, range),
            read_table(header_and_first),
        )
        .await;
        let (bytes, first) = (bytes?, first?);
        self.counters.whole_idx_loads.fetch_add(1, Relaxed);
        let mut loaded = validate_idx(row, bytes)?;
        let header_len = aspen_core::pack::ENTRY_TABLE_HEADER_LEN;
        if first.len() < header_len {
            return Err(corrupt(&loaded.record, "entry table: short".into()));
        }
        check_table_header(&loaded, &first[..header_len])?;
        let mut bases = Vec::with_capacity(loaded.sorted_offsets.len());
        feed_chain_map(&loaded, &first[header_len..], &mut bases)?;
        drop(first);
        for group in sections.get(1..).unwrap_or_default().chunks(2) {
            let reads = join_bounded(group.iter().map(|&r| read_table(r)), 2).await;
            for bytes in reads {
                feed_chain_map(&loaded, &bytes?, &mut bases)?;
            }
        }
        if bases.len() != loaded.sorted_offsets.len() {
            return Err(corrupt(
                &loaded.record,
                "entry table: too few records".into(),
            ));
        }
        loaded.bases = bases;
        Ok(Idx::Whole(Arc::new(loaded)))
    }

    /// Streams a large `.idx` through [`SampleBuilder`], which checks all
    /// of it and keeps one ID of every 256, and copies the sections lookups
    /// read to `dir` when given.
    async fn load_sampled(
        &self,
        row: PackRecord,
        dir: Option<&std::path::Path>,
    ) -> Result<SampledIdx, StoreError> {
        let key = row.keys().idx;
        let head = self.read_idx(&key, SampledIdx::head_range()).await?;
        let mut builder = SampleBuilder::new(row.clone(), &head).map_err(|e| corrupt(&row, e.0))?;
        if let Some(dir) = dir {
            builder = builder.copy_to(dir);
        }
        let sections = builder.sections(SAMPLE_READ_ENTRIES);
        for group in sections.chunks(8) {
            let reads = join_bounded(group.iter().map(|(_, r)| self.read_idx(&key, *r)), 8).await;
            for ((section, _), bytes) in group.iter().zip(reads) {
                builder
                    .feed(*section, &bytes?)
                    .map_err(|e| corrupt(&row, e.0))?;
            }
        }
        let sampled = builder.finish().map_err(|e| corrupt(&row, e.0))?;
        self.counters.sampled_idx_loads.fetch_add(1, Relaxed);
        Ok(sampled)
    }

    /// A sampled pack's chain map, built in a file in `dir` from its entry
    /// table, streamed in sections. `None` (the prefetch then finds chains
    /// by reading) if the local file cannot be written; an entry table that
    /// does not describe the pack is corruption.
    async fn load_chains(
        &self,
        row: &PackRecord,
        dir: &std::path::Path,
    ) -> Result<Option<DiskChains>, StoreError> {
        let key = row.keys().entries;
        let key = &key;
        let read = |(from, to): (u64, u64)| {
            let range = ByteRange::new(from, to).expect("non-empty");
            async move {
                let bytes = self.blobs.read_exact_range(key, range).await?;
                self.counters
                    .table_bytes_read
                    .fetch_add(bytes.len() as u64, Relaxed);
                Ok::<_, StoreError>(bytes)
            }
        };
        let fail = |e: ChainsError| match e {
            ChainsError::Corrupt(what) => Err(corrupt(row, what)),
            ChainsError::Io(_) => Ok(None),
        };
        let header = read(ChainsBuilder::header_range()).await?;
        let mut builder = match ChainsBuilder::new(dir, row, &header) {
            Ok(b) => b,
            Err(e) => return fail(e),
        };
        let count = builder.count();
        let sections: Vec<(u64, u64)> = (0..count)
            .step_by(SAMPLE_READ_ENTRIES as usize)
            .map(|from| ChainsBuilder::records_range(from, (from + SAMPLE_READ_ENTRIES).min(count)))
            .collect();
        for group in sections.chunks(8) {
            let reads = join_bounded(group.iter().map(|&r| read(r)), 8).await;
            for bytes in reads {
                if let Err(e) = builder.feed(&bytes?) {
                    return fail(e);
                }
            }
        }
        match builder.finish() {
            Ok(c) => Ok(Some(c)),
            Err(e) => fail(e),
        }
    }

    /// One range of an `.idx`, counted.
    async fn read_idx(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        let bytes = self.blobs.read_exact_range(key, range).await?;
        self.counters
            .idx_bytes_read
            .fetch_add(bytes.len() as u64, Relaxed);
        Ok(bytes)
    }

    /// Where each of `ids` is, in the first of `packs` (probe order) that
    /// holds it: the index into `packs` and the entry's offset.
    ///
    /// Whole `.idx` files answer from memory. Every sampled pack that comes
    /// before an ID's first whole-pack hit is probed for it at once, so the
    /// lookup costs one round trip however many sampled packs there are
    /// (one probing them in turn cost a round trip per pack, on every
    /// lookup of a push: monorepo benchmark, item 11); the earliest hit in
    /// probe order wins, as before. A sampled pack after a hit is not read.
    async fn find_all(
        &self,
        packs: &[Idx],
        ids: &[ObjectId],
    ) -> Result<Vec<Option<At>>, StoreError> {
        self.counters
            .ids_looked_up
            .fetch_add(ids.len() as u64, Relaxed);
        let mut out: Vec<Option<At>> = vec![None; ids.len()];
        for (p, pack) in packs.iter().enumerate() {
            if let Idx::Whole(l) = pack {
                let idx = l.idx();
                for (o, id) in out.iter_mut().zip(ids) {
                    if o.is_none()
                        && let Some(i) = idx.lookup(id)
                    {
                        *o = Some((p, idx.offset(i)));
                    }
                }
            }
        }
        let mut todo: Vec<(usize, usize, &SampledIdx)> = Vec::new();
        for (p, pack) in packs.iter().enumerate() {
            if let Idx::Sampled(s, _) = pack {
                for (i, id) in ids.iter().enumerate() {
                    if out[i].is_none_or(|(q, _)| q > p) && s.window(id).is_some() {
                        todo.push((p, i, s));
                    }
                }
            }
        }
        let found = join_bounded(
            todo.iter()
                .map(|&(_, i, s)| self.sampled_lookup(s, &ids[i])),
            self.options.max_parallel_reads,
        )
        .await;
        for (&(p, i, _), f) in todo.iter().zip(found) {
            if let Some(offset) = f?
                && out[i].is_none_or(|(q, _)| q > p)
            {
                out[i] = Some((p, offset));
            }
        }
        Ok(out)
    }

    /// The offset of `id` in a sampled pack: from the sample's local copy
    /// when it has one (no blob store read), else one round trip, reading
    /// the IDs that can hold it and their offsets together (a second for a
    /// large offset).
    async fn sampled_lookup(
        &self,
        s: &SampledIdx,
        id: &ObjectId,
    ) -> Result<Option<u64>, StoreError> {
        let Some((from, to, range)) = s.window(id) else {
            return Ok(None);
        };
        if let Some(found) = s.lookup_local(id) {
            self.counters.local_lookups.fetch_add(1, Relaxed);
            return Ok(found);
        }
        let key = s.record.keys().idx;
        let (ids, offsets) = futures_join(
            self.read_idx(&key, range),
            self.read_idx(&key, s.offsets_range(from, to)),
        )
        .await;
        let (ids, offsets) = (ids?, offsets?);
        let Some(i) = SampledIdx::find(from, &ids, id) else {
            return Ok(None);
        };
        let at = 4 * (i - from) as usize;
        Ok(Some(match s.small_offset(&offsets[at..at + 4]) {
            Ok(offset) => offset,
            Err(large) => SampledIdx::large_offset(&self.read_idx(&key, large).await?),
        }))
    }

    /// Reads and inflates one entry, charging the compressed bytes while
    /// they are held and the inflated bytes before they are allocated.
    async fn read_entry(
        &self,
        pack: &Idx,
        offset: u64,
        budget: &Budget,
    ) -> Result<(header::RawHeader, Vec<u8>), PrefetchError> {
        self.counters.entries_read.fetch_add(1, Relaxed);
        let pack = match pack {
            Idx::Whole(l) => l,
            Idx::Sampled(s, _) => {
                return self.read_entry_open_ended(&s.record, offset, budget).await;
            }
        };
        let range = pack.entry_range(offset)?;
        let key = pack.record.keys().pack;
        budget.charge(range.len())?;
        let read = self.blobs.read_exact_range(&key, range).await;
        let result = read.map_err(PrefetchError::from).and_then(|bytes| {
            decode_entry(
                &pack.record,
                offset,
                range,
                &bytes,
                self.options.max_object_bytes.get(),
                budget,
            )
        });
        budget.release(range.len());
        result
    }
}

impl<M: PackRows, B: BlobStore> StoredPacks<M, B> {
    /// [`Self::read_entry`] in a sampled pack, where the entry's end is not
    /// known: reads [`FIRST_ENTRY_READ`] bytes, and if the entry's
    /// compressed data goes on past them, reads again with room for it (a
    /// valid zlib stream can be longer than any bound, so this repeats,
    /// doubling, until the stream ends or the pack does).
    async fn read_entry_open_ended(
        &self,
        record: &PackRecord,
        offset: u64,
        budget: &Budget,
    ) -> Result<(header::RawHeader, Vec<u8>), PrefetchError> {
        let end = record.pack.size.saturating_sub(PACK_TRAILER_LEN);
        if offset < PACK_HEADER_LEN || offset >= end {
            return Err(corrupt(record, format!("no entry starts at offset {offset}")).into());
        }
        let key = record.keys().pack;
        let mut want = (end - offset).min(FIRST_ENTRY_READ);
        loop {
            let range = ByteRange::new(offset, offset + want).expect("non-empty");
            budget.charge(want)?;
            let read = self.blobs.read_exact_range(&key, range).await;
            let result = read.map_err(PrefetchError::from).and_then(|bytes| {
                decode_prefix(
                    record,
                    offset,
                    &bytes,
                    self.options.max_object_bytes.get(),
                    budget,
                )
            });
            budget.release(want);
            match result? {
                Prefix::Done(h, data) => return Ok((h, data)),
                Prefix::Short { need } if want < end - offset => {
                    want = need.max(want.saturating_mul(2)).min(end - offset);
                }
                Prefix::Short { .. } => {
                    return Err(corrupt(
                        record,
                        format!("entry at {offset} runs past the end of the pack"),
                    )
                    .into());
                }
            }
        }
    }
}

/// One `.idx` load in progress ([`StoredPacks::load_shared`]).
#[derive(Debug, Default)]
struct Flight {
    state: Mutex<FlightState>,
}

#[derive(Debug)]
enum FlightState {
    /// The leader is loading; these waiters are woken when it ends.
    Running(Vec<Waker>),
    /// The leader's result.
    Done(Result<Idx, StoreError>),
    /// The leader was dropped before it finished.
    Abandoned,
}

impl Default for FlightState {
    fn default() -> Self {
        Self::Running(Vec::new())
    }
}

impl Flight {
    /// Ends the flight and wakes every waiter.
    fn end(&self, to: FlightState) {
        let state = std::mem::replace(&mut *self.state.lock().expect("not poisoned"), to);
        if let FlightState::Running(wakers) = state {
            wakers.into_iter().for_each(Waker::wake);
        }
    }
}

/// The leader's hold on a [`Flight`]: removes it from the map when the load
/// ends, and marks it abandoned if the leader is dropped first, so waiters
/// never wait for a load nobody is running.
struct Lead<'a> {
    flights: &'a Mutex<HashMap<PackId, Arc<Flight>>>,
    id: PackId,
    flight: Arc<Flight>,
    finished: bool,
}

impl Lead<'_> {
    fn finish(&mut self, result: Result<Idx, StoreError>) {
        self.finished = true;
        self.leave();
        self.flight.end(FlightState::Done(result));
    }

    fn leave(&self) {
        let mut flights = self.flights.lock().expect("not poisoned");
        if flights
            .get(&self.id)
            .is_some_and(|f| Arc::ptr_eq(f, &self.flight))
        {
            flights.remove(&self.id);
        }
    }
}

impl Drop for Lead<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.leave();
            self.flight.end(FlightState::Abandoned);
        }
    }
}

/// Waits for a [`Flight`]: its result, or `None` if it was abandoned.
struct WaitFlight<'a> {
    flight: &'a Flight,
}

impl Future for WaitFlight<'_> {
    type Output = Option<Result<Idx, StoreError>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.flight.state.lock().expect("not poisoned");
        match &mut *state {
            FlightState::Running(wakers) => {
                if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
                    wakers.push(cx.waker().clone());
                }
                Poll::Pending
            }
            FlightState::Done(result) => Poll::Ready(Some(result.clone())),
            FlightState::Abandoned => Poll::Ready(None),
        }
    }
}

/// An entry decoded from bytes that start where it does and may end before
/// or after it.
enum Prefix {
    Done(header::RawHeader, Vec<u8>),
    /// The bytes end inside the entry; about `need` bytes hold it.
    Short {
        need: u64,
    },
}

/// The most bytes zlib needs for `n` bytes of data (`deflateBound`, plus
/// room for a header and checksum).
fn zlib_bound(n: u64) -> u64 {
    n.saturating_add(n >> 12)
        .saturating_add(n >> 14)
        .saturating_add(n >> 25)
        .saturating_add(64)
}

/// [`decode_entry`] for bytes that may stop inside the entry or go past its
/// end: what the stream needs is used, what follows it is ignored.
fn decode_prefix(
    record: &PackRecord,
    offset: u64,
    bytes: &[u8],
    max: u64,
    budget: &Budget,
) -> Result<Prefix, PrefetchError> {
    let h = match header::decode(bytes) {
        Ok(h) => h,
        Err(header::HeaderError::Truncated) => {
            return Ok(Prefix::Short {
                need: (bytes.len() as u64).saturating_mul(2).max(64),
            });
        }
        Err(e) => return Err(corrupt(record, format!("entry at {offset}: {e}")).into()),
    };
    if h.size > max {
        return Err(corrupt(
            record,
            format!("entry at {offset} inflates to {} bytes", h.size),
        )
        .into());
    }
    let need = u64::from(h.len).saturating_add(zlib_bound(h.size));
    budget.charge(h.size)?;
    let mut out = vec![0u8; h.size as usize + 1];
    let mut inflate = gix_zlib::Inflate::default();
    let input = &bytes[usize::from(h.len)..];
    match inflate.once(input, &mut out) {
        Ok((gix_zlib::Status::StreamEnd, _, w)) if w as u64 == h.size => {
            out.truncate(h.size as usize);
            Ok(Prefix::Done(h, out))
        }
        Ok((status, consumed, w))
            if status != gix_zlib::Status::StreamEnd
                && consumed == input.len()
                && w as u64 <= h.size =>
        {
            budget.release(h.size);
            Ok(Prefix::Short { need })
        }
        other => {
            budget.release(h.size);
            Err(corrupt(
                record,
                match other {
                    Err(e) => format!("entry at {offset}: {e}"),
                    Ok(_) => format!("entry at {offset} does not inflate to its size"),
                },
            )
            .into())
        }
    }
}

/// Checks a `.idx` blob against its row: exact size, valid structure,
/// object count, and the pack checksum it names.
fn validate_idx(record: PackRecord, bytes: Vec<u8>) -> Result<LoadedIdx, StoreError> {
    if bytes.len() as u64 != record.pack.idx_size {
        return Err(corrupt(
            &record,
            format!(
                "read {} bytes of a {}-byte .idx",
                bytes.len(),
                record.pack.idx_size
            ),
        ));
    }
    let idx = IdxV2::parse(&bytes, Some(record.pack.size)).map_err(|e| corrupt(&record, e.0))?;
    if idx.len() != record.pack.object_count as usize || idx.pack_checksum() != record.pack.checksum
    {
        return Err(corrupt(&record, ".idx does not describe this pack".into()));
    }
    let mut sorted_offsets: Vec<u64> = (0..idx.len()).map(|i| idx.offset(i)).collect();
    sorted_offsets.sort_unstable();
    Ok(LoadedIdx {
        record,
        bytes,
        sorted_offsets,
        bases: Vec::new(),
    })
}

/// The chain map of a validated `.idx` from its pack's whole entry table
/// ([`check_table_header`], then [`feed_chain_map`]).
fn chain_map(l: &LoadedIdx, table: &[u8]) -> Result<Vec<u32>, StoreError> {
    use aspen_core::pack::{ENTRY_RECORD_LEN, ENTRY_TABLE_HEADER_LEN};
    let n = l.sorted_offsets.len();
    if table.len() != ENTRY_TABLE_HEADER_LEN + ENTRY_RECORD_LEN * n {
        return Err(corrupt(&l.record, "entry table: wrong length".into()));
    }
    check_table_header(l, &table[..ENTRY_TABLE_HEADER_LEN])?;
    let mut bases = Vec::with_capacity(n);
    feed_chain_map(l, &table[ENTRY_TABLE_HEADER_LEN..], &mut bases)?;
    Ok(bases)
}

/// Checks that an entry table's header (`aspen_core::pack::EntryTable`'s
/// encoding) describes the pack of a validated `.idx`.
fn check_table_header(l: &LoadedIdx, header: &[u8]) -> Result<(), StoreError> {
    use aspen_core::pack::{ENTRY_TABLE_HEADER_LEN, ENTRY_TABLE_MAGIC, ENTRY_TABLE_VERSION};
    let record = &l.record;
    let n = l.sorted_offsets.len();
    if n >= NO_BASE as usize
        || header.len() != ENTRY_TABLE_HEADER_LEN
        || header[..8] != ENTRY_TABLE_MAGIC
        || header[8..12] != ENTRY_TABLE_VERSION.to_be_bytes()
        || header[12..16] != (n as u32).to_be_bytes()
        || header[16..24] != record.pack.size.to_be_bytes()
        || header[24..44] != *record.pack.checksum.as_bytes()
    {
        return Err(corrupt(
            record,
            "entry table: does not describe this pack".into(),
        ));
    }
    Ok(())
}

/// Appends to `bases` the chain map of the entry table's next records, read
/// without building the table: each must be the `.idx`'s next entry in
/// pack order, and every delta's base one of its entries. Any mismatch is
/// [`StoreError::Corrupt`].
fn feed_chain_map(l: &LoadedIdx, records: &[u8], bases: &mut Vec<u32>) -> Result<(), StoreError> {
    use aspen_core::pack::ENTRY_RECORD_LEN;
    let record = &l.record;
    let bad = |what: &str| corrupt(record, format!("entry table: {what}"));
    if !records.len().is_multiple_of(ENTRY_RECORD_LEN) {
        return Err(bad("a read ends inside a record"));
    }
    let idx = l.idx();
    let position = |offset: u64| {
        l.sorted_offsets
            .binary_search(&offset)
            .map(|i| i as u32)
            .map_err(|_| bad(&format!("a base at {offset} is not an entry")))
    };
    for rec in records.as_chunks::<ENTRY_RECORD_LEN>().0 {
        let Some(&offset) = l.sorted_offsets.get(bases.len()) else {
            return Err(bad("more records than entries"));
        };
        if rec[..8] != offset.to_be_bytes() {
            return Err(bad("lists other entries than the .idx"));
        }
        bases.push(match rec[8] {
            1..=4 => NO_BASE,
            6 => position(u64::from_be_bytes(rec[32..40].try_into().expect("8 bytes")))?,
            7 => {
                let base = ObjectId::from_slice(&rec[32..52]).expect("20 bytes");
                let i = idx
                    .lookup(&base)
                    .ok_or_else(|| bad(&format!("a REF_DELTA base {base} is not in the pack")))?;
                position(idx.offset(i))?
            }
            t => return Err(bad(&format!("unknown entry type {t}"))),
        });
    }
    Ok(())
}

/// Runs two futures concurrently (no runtime needed) and returns both
/// outputs.
async fn futures_join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    let (mut ra, mut rb) = (None, None);
    std::future::poll_fn(|cx| {
        if ra.is_none()
            && let Poll::Ready(v) = a.as_mut().poll(cx)
        {
            ra = Some(v);
        }
        if rb.is_none()
            && let Poll::Ready(v) = b.as_mut().poll(cx)
        {
            rb = Some(v);
        }
        if ra.is_some() && rb.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    (ra.expect("ready"), rb.expect("ready"))
}

/// Decodes one entry read from the blob store: header, then the inflated
/// data (a full object or a delta).
fn decode_entry(
    record: &PackRecord,
    offset: u64,
    range: ByteRange,
    bytes: &[u8],
    max: u64,
    budget: &Budget,
) -> Result<(header::RawHeader, Vec<u8>), PrefetchError> {
    if bytes.len() as u64 != range.len() {
        return Err(corrupt(
            record,
            format!(
                "short read: {} of {} bytes at offset {offset}",
                bytes.len(),
                range.len()
            ),
        )
        .into());
    }
    let h = header::decode(bytes)
        .map_err(|e| PrefetchError::from(corrupt(record, format!("entry at {offset}: {e}"))))?;
    if h.size > max {
        return Err(corrupt(
            record,
            format!("entry at {offset} inflates to {} bytes", h.size),
        )
        .into());
    }
    budget.charge(h.size)?;
    let mut out = vec![0u8; h.size as usize + 1];
    let mut inflate = gix_zlib::Inflate::default();
    let inflated = inflate.once(&bytes[usize::from(h.len)..], &mut out);
    let ok = matches!(&inflated, Ok((gix_zlib::Status::StreamEnd, _, w)) if *w as u64 == h.size);
    if !ok {
        budget.release(h.size);
        return Err(corrupt(
            record,
            match inflated {
                Err(e) => format!("entry at {offset}: {e}"),
                Ok(_) => format!("entry at {offset} does not inflate to its size"),
            },
        )
        .into());
    }
    out.truncate(h.size as usize);
    Ok((h, out))
}

fn ref_delta_outside(record: &PackRecord, off: u64, id: &ObjectId) -> StoreError {
    corrupt(
        record,
        format!("REF_DELTA at {off} is based on {id}, which the pack lacks"),
    )
}

fn corrupt(record: &PackRecord, what: String) -> StoreError {
    StoreError::Corrupt {
        what: format!("pack {} ({}): {what}", record.id, record.pack.checksum),
    }
}

impl<M: PackRows, B: BlobStore> ObjectLocator for StoredPacks<M, B> {
    async fn locate(
        &self,
        packs: &SearchPacks,
        ids: &SortedIds,
    ) -> Result<Vec<Option<ObjectLocation>>, StoreError> {
        let loaded = self.load(packs.as_slice()).await?;
        let found = self.find_all(&loaded, ids.as_slice()).await?;
        Ok(found
            .into_iter()
            .map(|f| {
                f.map(|(p, offset)| ObjectLocation {
                    pack: loaded[p].record().id,
                    offset,
                })
            })
            .collect())
    }
}

/// An entry's position: (index into the search packs, offset).
type At = (usize, u64);

/// One entry read during a prefetch.
#[derive(Debug)]
enum Read {
    Full(ObjectKind, Vec<u8>),
    Delta { base: At, delta: Vec<u8> },
}

impl<M: PackRows, B: BlobStore> StoredPacks<M, B> {
    /// Prefetched objects the base-object cache answered, and those read
    /// from the blob store, since the start.
    pub fn object_cache_counts(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (
            self.object_hits.load(Relaxed),
            self.object_misses.load(Relaxed),
        )
    }

    /// Puts an object into the base-object cache, as if a prefetch had
    /// resolved it (objects over [`OBJECT_CACHE_MAX_ENTRY`] are ignored).
    /// For a writer that has just stored the object: the receive path
    /// seeds the trees a push stored, because the next push to the same
    /// ref sends deltas against exactly those trees.
    pub fn remember(&self, id: ObjectId, kind: ObjectKind, data: Vec<u8>) {
        let len = data.len() as u64;
        if len > OBJECT_CACHE_MAX_ENTRY || len > self.options.object_cache_bytes {
            return;
        }
        self.objects
            .lock()
            .expect("not poisoned")
            .insert(id, Arc::new((kind, data)), len);
    }

    /// Puts a pack this process has just stored into the `.idx` cache, from
    /// the `.idx` and entry table it uploaded, as a later load would read
    /// them: the next push to the same ref searches this pack, and loading
    /// it cost every push a round trip (item 11). Returns whether it was
    /// kept: a pack whose `.idx` is over
    /// [`LocateOptions::max_whole_idx_bytes`] is not (it would be sampled),
    /// nor one whose files do not validate against its row.
    pub fn remember_pack(&self, record: PackRecord, idx: Vec<u8>, table: &[u8]) -> bool {
        if record.pack.idx_size > self.options.max_whole_idx_bytes {
            return false;
        }
        let Ok(mut loaded) = validate_idx(record, idx) else {
            return false;
        };
        let Ok(bases) = chain_map(&loaded, table) else {
            return false;
        };
        loaded.bases = bases;
        let id = loaded.record.id;
        let entry = Idx::Whole(Arc::new(loaded));
        let cost = entry.cost();
        self.cache
            .lock()
            .expect("not poisoned")
            .insert(id, entry, cost);
        true
    }

    /// Whether [`Self::remember_pack`] keeps a pack with an `.idx` of
    /// `idx_size` bytes, so a caller can skip reading its files.
    pub fn keeps_whole(&self, idx_size: u64) -> bool {
        idx_size <= self.options.max_whole_idx_bytes
    }

    /// [`BasePrefetcher::prefetch`] with its typed error and memory
    /// figures. The trait method reports [`PrefetchError::OverBudget`] as
    /// [`StoreError::Unavailable`]; a caller that can refuse the push with
    /// a clear message (B1) should call this instead.
    pub async fn prefetch_bases(
        &self,
        packs: &SearchPacks,
        candidates: &CandidateBases,
    ) -> Result<(Prefetched<InMemoryBases>, PrefetchStats), PrefetchError> {
        self.prefetch_bases_within(packs, candidates, self.options.max_prefetch_bytes)
            .await
    }

    /// [`Self::prefetch_bases`] under a smaller budget than
    /// [`LocateOptions::max_prefetch_bytes`] (it is capped at that), for a
    /// caller that reserves memory for the common small prefetch and asks
    /// again with the full budget only if this one reports
    /// [`PrefetchError::OverBudget`].
    pub async fn prefetch_bases_within(
        &self,
        packs: &SearchPacks,
        candidates: &CandidateBases,
        budget: u64,
    ) -> Result<(Prefetched<InMemoryBases>, PrefetchStats), PrefetchError> {
        let loaded = self.load(packs.as_slice()).await?;
        let budget = Budget::new(budget.min(self.options.max_prefetch_bytes));
        let mut wanted: Vec<(ObjectId, At)> = Vec::new();
        let mut not_in_deps = Vec::new();
        let mut bases = InMemoryBases::default();
        let mut output_bytes = 0;
        let hits = self.find_all(&loaded, candidates.0.as_slice()).await?;
        for (id, hit) in candidates.0.as_slice().iter().zip(hits) {
            match hit {
                Some(at) => {
                    // Only an object the search packs hold may come from
                    // the cache, so a cached object never answers for a
                    // dependency that lacks it.
                    let cached = self.objects.lock().expect("not poisoned").get(id);
                    match cached {
                        Some(obj) => {
                            self.object_hits
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let (kind, data) = &*obj;
                            budget.charge(data.len() as u64)?;
                            output_bytes += data.len() as u64;
                            bases.insert(*id, *kind, data.clone());
                        }
                        None => {
                            self.object_misses
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            wanted.push((*id, at));
                        }
                    }
                }
                None => not_in_deps.push(*id),
            }
        }
        // Read entries in rounds; each round's reads reveal the next bases.
        // A whole pack's chain map gives every entry of a chain up front,
        // so its chains are read in the first round, whatever their depth.
        let mut reads: HashMap<At, Read> = HashMap::new();
        let mut frontier: Vec<At> = Vec::with_capacity(wanted.len());
        for &(_, (p, off)) in &wanted {
            match &loaded[p] {
                Idx::Whole(l) => l.chain(p, off, &mut frontier),
                Idx::Sampled(_, Some(c)) => c.chain(p, off, &mut frontier),
                Idx::Sampled(_, None) => frontier.push((p, off)),
            }
        }
        frontier.sort_unstable();
        frontier.dedup();
        while !frontier.is_empty() {
            let results = join_bounded(
                frontier
                    .iter()
                    .map(|&(p, off)| self.read_entry(&loaded[p], off, &budget)),
                self.options.max_parallel_reads,
            )
            .await;
            let mut next = Vec::new();
            // REF_DELTAs in sampled packs, whose bases are looked up after
            // the round.
            let mut by_id: Vec<(At, ObjectId, Vec<u8>)> = Vec::new();
            for (&(p, off), result) in frontier.iter().zip(results) {
                let (h, data) = result?;
                let l = loaded[p].record();
                let read = match h.base {
                    RawBase::None => Read::Full(kind_of(h.type_code), data),
                    RawBase::Distance(d) => {
                        let base = off
                            .checked_sub(d)
                            .filter(|&b| b >= PACK_HEADER_LEN)
                            .ok_or_else(|| {
                                corrupt(l, format!("OFS_DELTA at {off} points before the pack"))
                            })?;
                        Read::Delta {
                            base: (p, base),
                            delta: data,
                        }
                    }
                    RawBase::Id(id) => {
                        // Stored packs are self-contained: the base is here.
                        let Idx::Whole(w) = &loaded[p] else {
                            by_id.push(((p, off), id, data));
                            continue;
                        };
                        let idx = w.idx();
                        let i = idx
                            .lookup(&id)
                            .ok_or_else(|| ref_delta_outside(l, off, &id))?;
                        Read::Delta {
                            base: (p, idx.offset(i)),
                            delta: data,
                        }
                    }
                };
                if let Read::Delta { base, .. } = &read
                    && !reads.contains_key(base)
                {
                    next.push(*base);
                }
                reads.insert((p, off), read);
            }
            let bases = join_bounded(
                by_id.iter().map(|((p, _), id, _)| match &loaded[*p] {
                    Idx::Sampled(s, _) => self.sampled_lookup(s, id),
                    Idx::Whole(_) => unreachable!("whole packs answer at once"),
                }),
                self.options.max_parallel_reads,
            )
            .await;
            for (((p, off), id, delta), base) in by_id.into_iter().zip(bases) {
                let base = base?.ok_or_else(|| ref_delta_outside(loaded[p].record(), off, &id))?;
                let read = Read::Delta {
                    base: (p, base),
                    delta,
                };
                if !reads.contains_key(&(p, base)) {
                    next.push((p, base));
                }
                reads.insert((p, off), read);
            }
            next.sort_unstable();
            next.dedup();
            next.retain(|k| !reads.contains_key(k));
            frontier = next;
        }
        let mut chains = Chains::new(reads, &wanted, &loaded, &budget);
        // Copies for the cache, at most what it can hold.
        let mut fresh = Vec::new();
        let mut fresh_bytes = 0;
        for (id, at) in wanted {
            let (kind, data) = chains.take_resolved(at, self.options.max_object_bytes.get())?;
            output_bytes += data.len() as u64;
            let len = data.len() as u64;
            if len <= OBJECT_CACHE_MAX_ENTRY && fresh_bytes + len <= self.options.object_cache_bytes
            {
                fresh_bytes += len;
                fresh.push((id, Arc::new((kind, data.clone())), len));
            }
            bases.insert(id, kind, data);
        }
        if !fresh.is_empty() {
            let mut objects = self.objects.lock().expect("not poisoned");
            for (id, obj, len) in fresh {
                objects.insert(id, obj, len);
            }
        }
        Ok((
            Prefetched {
                bases,
                not_in_deps: SortedIds::from_unsorted(not_in_deps),
            },
            PrefetchStats {
                peak_bytes: budget.peak(),
                output_bytes,
            },
        ))
    }
}

impl<M: PackRows, B: BlobStore> BasePrefetcher for StoredPacks<M, B> {
    type Bases = InMemoryBases;

    async fn prefetch(
        &self,
        packs: &SearchPacks,
        candidates: &CandidateBases,
    ) -> Result<Prefetched<InMemoryBases>, StoreError> {
        match self.prefetch_bases(packs, candidates).await {
            Ok((p, _)) => Ok(p),
            Err(PrefetchError::Store(e)) => Err(e),
            Err(e @ PrefetchError::OverBudget { .. }) => Err(StoreError::Unavailable {
                reason: e.to_string(),
            }),
        }
    }
}

fn kind_of(type_code: u8) -> ObjectKind {
    match type_code {
        1 => ObjectKind::Commit,
        2 => ObjectKind::Tree,
        3 => ObjectKind::Blob,
        _ => ObjectKind::Tag,
    }
}

/// Resolution of the entries a prefetch read. Every entry read lies on the
/// chain of some wanted entry, so each is resolved exactly once and used a
/// known number of times: once per entry based on it, plus once if it is
/// wanted. A result is kept only while uses remain; its last use moves it.
struct Chains<'a> {
    reads: HashMap<At, Read>,
    /// Results with uses remaining.
    resolved: HashMap<At, (ObjectKind, Vec<u8>)>,
    uses: HashMap<At, u32>,
    loaded: &'a [Idx],
    budget: &'a Budget,
}

impl<'a> Chains<'a> {
    fn new(
        reads: HashMap<At, Read>,
        wanted: &[(ObjectId, At)],
        loaded: &'a [Idx],
        budget: &'a Budget,
    ) -> Self {
        let mut uses: HashMap<At, u32> = HashMap::with_capacity(reads.len());
        for r in reads.values() {
            if let Read::Delta { base, .. } = r {
                *uses.entry(*base).or_default() += 1;
            }
        }
        for (_, at) in wanted {
            *uses.entry(*at).or_default() += 1;
        }
        Self {
            reads,
            resolved: HashMap::new(),
            uses,
            loaded,
            budget,
        }
    }

    /// Spends one use of `at`, which is resolved: its bytes are dropped
    /// and released after the last use.
    fn spend(&mut self, at: At) {
        let left = self.uses.get_mut(&at).expect("every read entry has uses");
        *left -= 1;
        if *left == 0
            && let Some((_, data)) = self.resolved.remove(&at)
        {
            self.budget.release(data.len() as u64);
        }
    }

    /// The object at `at` (wanted), resolving its chain first. Moved out
    /// on its last use, copied (and charged) otherwise.
    fn take_resolved(&mut self, at: At, max: u64) -> Result<(ObjectKind, Vec<u8>), PrefetchError> {
        self.resolve(at, max)?;
        let left = self.uses.get_mut(&at).expect("wanted entries have a use");
        *left -= 1;
        if *left == 0 {
            // Its bytes stay charged: they are the output now.
            Ok(self.resolved.remove(&at).expect("just resolved"))
        } else {
            let (kind, data) = &self.resolved[&at];
            self.budget.charge(data.len() as u64)?;
            Ok((*kind, data.clone()))
        }
    }

    /// Makes sure `at` is in `resolved`: walks down to a resolved entry or
    /// a full object, then applies the deltas back up. Iterative, so deep
    /// chains cannot overflow the stack.
    fn resolve(&mut self, at: At, max: u64) -> Result<(), PrefetchError> {
        let mut chain = Vec::new();
        let mut cur = at;
        while !self.resolved.contains_key(&cur) {
            match self.reads.remove(&cur) {
                Some(Read::Full(kind, data)) => {
                    // Already charged when it was read.
                    self.resolved.insert(cur, (kind, data));
                }
                Some(Read::Delta { base, delta }) => {
                    if chain.len() > self.uses.len() {
                        return Err(corrupt(
                            self.loaded[at.0].record(),
                            "delta chain loops".into(),
                        )
                        .into());
                    }
                    chain.push((cur, delta));
                    cur = base;
                }
                None => {
                    // Every read entry is resolved before it is spent, and
                    // it is only removed from `reads` when resolved, so a
                    // missing one is on the chain being built: a loop.
                    return Err(
                        corrupt(self.loaded[at.0].record(), "delta chain loops".into()).into(),
                    );
                }
            }
        }
        let mut base = cur;
        while let Some((k, delta)) = chain.pop() {
            let record = self.loaded[k.0].record();
            let out_len = delta_result_len(&delta)
                .map_err(|e| corrupt(record, format!("delta at {}: {e}", k.1)))?;
            if out_len > max {
                return Err(corrupt(
                    record,
                    format!(
                        "delta at {} makes {out_len} bytes, over the object limit",
                        k.1
                    ),
                )
                .into());
            }
            self.budget.charge(out_len)?;
            let (kind, data) = &self.resolved[&base];
            let applied = apply_delta(data, &delta, out_len);
            let kind = *kind;
            self.budget.release(delta.len() as u64);
            drop(delta);
            let out = applied.map_err(|e| {
                self.budget.release(out_len);
                corrupt(record, format!("delta at {}: {e}", k.1))
            })?;
            self.spend(base);
            self.resolved.insert(k, (kind, out));
            base = k;
        }
        Ok(())
    }
}

/// The result size a Git delta declares (its second header varint).
fn delta_result_len(delta: &[u8]) -> Result<u64, String> {
    let mut pos = 0;
    delta_varint(delta, &mut pos)?;
    delta_varint(delta, &mut pos)
}

fn delta_varint(d: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = *d.get(*pos).ok_or("truncated delta header")?;
        *pos += 1;
        if shift > 63 {
            return Err("delta header varint overflows".into());
        }
        v |= u64::from(b & 0x7f) << shift;
        shift += 7;
        if b & 0x80 == 0 {
            return Ok(v);
        }
    }
}

/// Applies a Git delta to `base`. Pure; every bound is checked.
pub fn apply_delta(base: &[u8], delta: &[u8], max: u64) -> Result<Vec<u8>, String> {
    let varint = delta_varint;
    let mut pos = 0;
    let base_len = varint(delta, &mut pos)?;
    if base_len != base.len() as u64 {
        return Err(format!(
            "delta expects a {base_len}-byte base, got {}",
            base.len()
        ));
    }
    let out_len = varint(delta, &mut pos)?;
    if out_len > max {
        return Err(format!("delta result of {out_len} bytes is over the limit"));
    }
    let mut out = Vec::with_capacity(out_len as usize);
    while pos < delta.len() {
        let op = delta[pos];
        pos += 1;
        if op & 0x80 != 0 {
            let mut field = |bits: u8, at: u32| -> Result<u64, String> {
                let mut v = 0u64;
                for i in 0..bits {
                    if op & (1 << (at + u32::from(i))) != 0 {
                        let b = *delta.get(pos).ok_or("truncated copy")?;
                        pos += 1;
                        v |= u64::from(b) << (8 * i);
                    }
                }
                Ok(v)
            };
            let offset = field(4, 0)?;
            let mut size = field(3, 4)?;
            if size == 0 {
                size = 0x10000;
            }
            let end = offset
                .checked_add(size)
                .filter(|&e| e <= base.len() as u64)
                .ok_or("copy outside the base")?;
            out.extend_from_slice(&base[offset as usize..end as usize]);
        } else if op != 0 {
            let end = pos + usize::from(op);
            out.extend_from_slice(delta.get(pos..end).ok_or("truncated insert")?);
            pos = end;
        } else {
            return Err("delta opcode 0 is reserved".into());
        }
        if out.len() as u64 > out_len {
            return Err("delta writes past its result size".into());
        }
    }
    if out.len() as u64 != out_len {
        return Err("delta result is shorter than its header says".into());
    }
    Ok(out)
}

/// Runs futures concurrently, at most `limit` at a time, and returns their
/// outputs in order. Needs no runtime: it polls the futures it holds.
async fn join_bounded<F: Future>(futures: impl Iterator<Item = F>, limit: usize) -> Vec<F::Output> {
    let mut pending = futures.enumerate();
    let mut out: Vec<Option<F::Output>> = Vec::new();
    let mut running: Vec<(usize, Pin<Box<F>>)> = Vec::new();
    let limit = limit.max(1);
    loop {
        while running.len() < limit {
            match pending.next() {
                Some((i, f)) => {
                    if out.len() <= i {
                        out.resize_with(i + 1, || None);
                    }
                    running.push((i, Box::pin(f)));
                }
                None => break,
            }
        }
        if running.is_empty() {
            break;
        }
        let done = PollSome(&mut running).await;
        for (i, v) in done {
            out[i] = Some(v);
        }
    }
    out.into_iter()
        .map(|o| o.expect("every future ran"))
        .collect()
}

/// Polls every running future once per wake; resolves with the outputs of
/// those that finished (at least one).
struct PollSome<'a, F: Future>(&'a mut Vec<(usize, Pin<Box<F>>)>);

impl<F: Future> Future for PollSome<'_, F> {
    type Output = Vec<(usize, F::Output)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut done = Vec::new();
        let running = &mut *self.0;
        let mut i = 0;
        while i < running.len() {
            if let Poll::Ready(v) = running[i].1.as_mut().poll(cx) {
                let (k, _) = running.swap_remove(i);
                done.push((k, v));
            } else {
                i += 1;
            }
        }
        if done.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(done)
        }
    }
}

#[cfg(test)]
mod cache_tests {
    use std::time::{Duration, Instant};

    use super::*;

    fn id(n: u64) -> PackId {
        PackId::new(n as i64 + 1).expect("positive")
    }

    /// Invariant: the cache evicts the least recently used entries first
    /// and stays within its byte budget; a hit makes an entry the most
    /// recently used.
    #[test]
    fn evicts_least_recently_used_within_the_budget() {
        let mut c = Cache::new(30);
        c.insert(id(1), 1, 10);
        c.insert(id(2), 2, 10);
        c.insert(id(3), 3, 10);
        assert_eq!(c.get(&id(1)), Some(1), "1 is now the most recent");
        c.insert(id(4), 4, 10);
        assert_eq!(c.get(&id(2)), None, "2 was the least recent");
        assert_eq!(c.get(&id(3)), Some(3));
        assert_eq!(c.get(&id(1)), Some(1));
        assert_eq!(c.get(&id(4)), Some(4));
        assert_eq!(c.bytes, 30);
        c.insert(id(1), 99, 10);
        assert_eq!(c.get(&id(1)), Some(1), "an existing entry is kept");
        c.insert(id(5), 5, 100);
        assert_eq!(c.get(&id(5)), None, "an entry over the budget is not kept");
        assert_eq!(c.bytes, 0);
    }

    /// Regression (throughput benchmark, bench/throughput): a hit used to
    /// scan the whole LRU order, so with N cached packs every hit cost O(N)
    /// under the cache's mutex and pushes slowed down as packs accumulated
    /// (200,000 hits on 200,000 entries took minutes). Hits must cost the
    /// same however many packs are cached.
    #[test]
    fn hits_do_not_slow_down_as_the_cache_grows() {
        const N: u64 = 200_000;
        let mut c = Cache::new(u64::MAX);
        for n in 0..N {
            c.insert(id(n), n, 1);
        }
        let started = Instant::now();
        for n in 0..N {
            // Oldest first: the worst case for a scan from the front.
            assert_eq!(c.get(&id(n)), Some(n));
        }
        let took = started.elapsed();
        assert!(
            took < Duration::from_secs(5),
            "{N} hits on {N} cached packs took {took:?}"
        );
    }
}
