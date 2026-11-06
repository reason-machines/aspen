//! Finding objects in stored packs: the `ObjectLocator` and
//! `BasePrefetcher` contracts of `aspen_core::ingest`, implemented over any
//! `MetaStore` (for pack rows) and `BlobStore` (for `.idx` files and pack
//! entries).
//!
//! - **Location** probes each search pack's `.idx` (fanout, then binary
//!   search) in probe order and stops at the first hit (S3, "Base-object
//!   lookup"). `.idx` files are immutable, so they are cached in memory up to
//!   a byte budget; a warm lookup reads no object storage.
//! - **Prefetch** locates every candidate base, then reads its entry and its
//!   delta chain from the blob store in rounds: all entries known in a round
//!   are read in parallel (bounded), each read reveals the next base, and a
//!   chain of depth d costs d + 1 rounds, never one round trip per base
//!   (S4: p99 chains of 3 to 7 entries). Chains are then resolved locally
//!   and the full objects handed to completion as
//!   [`aspen_core::ingest::InMemoryBases`].
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
use std::task::{Context, Poll};

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
    /// S4). Least recently loaded entries are evicted first.
    pub idx_cache_bytes: u64,
    /// Largest `.idx` kept whole (default 64 MiB, a pack of about 2.4
    /// million objects). A larger one is kept as a sample, one ID of every
    /// 256 (`crate::sampled`), and each lookup in it reads the 256 IDs that
    /// can hold the object, and its offset, from the blob store: one read
    /// of about 5 KiB plus one of 4 bytes, however large the pack. Without
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
    /// Most blob reads in flight at once during a prefetch round.
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
    /// the server's configuration: a 256 MiB `.idx` cache (an `.idx` over
    /// 64 MiB is sampled), a 64 MiB base
    /// object cache, 64 reads in
    /// flight, and a prefetch budget of four times the object limit (at
    /// least 1 GiB).
    pub fn new(max_object_bytes: MaxObjectBytes) -> Self {
        Self {
            idx_cache_bytes: 256 << 20,
            max_whole_idx_bytes: 64 << 20,
            object_cache_bytes: 64 << 20,
            max_parallel_reads: 64,
            max_object_bytes,
            max_prefetch_bytes: max_object_bytes.get().saturating_mul(4).max(1 << 30),
        }
    }
}

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
}

impl LoadedIdx {
    /// Bytes this entry holds in the cache: the `.idx` plus the offsets.
    fn cost(&self) -> u64 {
        (self.bytes.len() + self.sorted_offsets.len() * std::mem::size_of::<u64>()) as u64
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

/// A search pack's `.idx` as the locator holds it.
#[derive(Debug, Clone)]
enum Idx {
    /// The whole file.
    Whole(Arc<LoadedIdx>),
    /// A sample of a file over [`LocateOptions::max_whole_idx_bytes`]
    /// (`crate::sampled`).
    Sampled(Arc<SampledIdx>),
}

impl Idx {
    fn record(&self) -> &PackRecord {
        match self {
            Self::Whole(l) => &l.record,
            Self::Sampled(s) => &s.record,
        }
    }

    fn cost(&self) -> u64 {
        match self {
            Self::Whole(l) => l.cost(),
            Self::Sampled(s) => s.cost(),
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
    /// Object IDs looked up (each once per call, however many packs it was
    /// probed in).
    pub ids_looked_up: u64,
    /// Pack entries read from the blob store by prefetches.
    pub entries_read: u64,
}

#[derive(Debug, Default)]
struct Counters {
    whole_idx_loads: AtomicU64,
    sampled_idx_loads: AtomicU64,
    idx_bytes_read: AtomicU64,
    ids_looked_up: AtomicU64,
    entries_read: AtomicU64,
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
    objects: Mutex<Cache<ObjectId, CachedObject>>,
    /// Prefetched objects answered by the base-object cache, and read from
    /// the blob store.
    object_hits: std::sync::atomic::AtomicU64,
    object_misses: std::sync::atomic::AtomicU64,
    counters: Counters,
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
            objects: Mutex::new(Cache::new(options.object_cache_bytes)),
            object_hits: Default::default(),
            object_misses: Default::default(),
            counters: Counters::default(),
        }
    }

    /// What this locator has read and looked up so far.
    pub fn stats(&self) -> LocateStats {
        let c = &self.counters;
        LocateStats {
            whole_idx_loads: c.whole_idx_loads.load(Relaxed),
            sampled_idx_loads: c.sampled_idx_loads.load(Relaxed),
            idx_bytes_read: c.idx_bytes_read.load(Relaxed),
            ids_looked_up: c.ids_looked_up.load(Relaxed),
            entries_read: c.entries_read.load(Relaxed),
        }
    }

    /// Caches the `.idx` of a pack this process just stored (`bytes`, the
    /// file it uploaded), so the next read of the pack does not fetch it
    /// back from the blob store. Every push's pack is searched by later
    /// pushes and fetches of its ref: reading its `.idx` costs one
    /// blob-store round trip (about 25 ms on S3), which the merge queue
    /// paid on every batch. An `.idx` that does not validate against the
    /// row, or one too large to cache whole, is left to be read as before.
    pub fn remember_idx(&self, record: PackRecord, bytes: Vec<u8>) {
        if record.pack.idx_size > self.options.max_whole_idx_bytes {
            return;
        }
        if let Ok(l) = validate_idx(record, bytes) {
            let l = Arc::new(l);
            let cost = l.cost();
            self.cache
                .lock()
                .expect("not poisoned")
                .insert(l.record.id, Idx::Whole(l), cost);
        }
    }

    /// The `.idx` of each pack, from the cache or the blob store.
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
            let loads = join_bounded(
                rows.into_iter().map(|r| self.load_one(r)),
                self.options.max_parallel_reads,
            )
            .await;
            let mut loaded = Vec::with_capacity(loads.len());
            for l in loads {
                loaded.push(l?);
            }
            let mut cache = self.cache.lock().expect("not poisoned");
            let mut fresh = loaded.into_iter();
            for slot in out.iter_mut().filter(|o| o.is_none()) {
                let l = fresh.next().expect("one load per miss");
                cache.insert(l.record().id, l.clone(), l.cost());
                *slot = Some(l);
            }
        }
        Ok(out.into_iter().map(|o| o.expect("filled")).collect())
    }

    /// Reads one pack's `.idx`: whole, or as a sample when it is over
    /// [`LocateOptions::max_whole_idx_bytes`].
    async fn load_one(&self, row: PackRecord) -> Result<Idx, StoreError> {
        if row.pack.idx_size > self.options.max_whole_idx_bytes {
            return Ok(Idx::Sampled(Arc::new(self.load_sampled(row).await?)));
        }
        let range = ByteRange::new(0, row.pack.idx_size)
            .ok_or_else(|| corrupt(&row, "empty .idx".into()))?;
        let bytes = self.read_idx(&row.keys().idx, range).await?;
        self.counters.whole_idx_loads.fetch_add(1, Relaxed);
        Ok(Idx::Whole(Arc::new(validate_idx(row, bytes)?)))
    }

    /// Streams a large `.idx` through [`SampleBuilder`], which checks all
    /// of it and keeps one ID of every 256.
    async fn load_sampled(&self, row: PackRecord) -> Result<SampledIdx, StoreError> {
        let key = row.keys().idx;
        let head = self.read_idx(&key, SampledIdx::head_range()).await?;
        let mut builder = SampleBuilder::new(row.clone(), &head).map_err(|e| corrupt(&row, e.0))?;
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
            match pack {
                Idx::Whole(l) => {
                    let idx = l.idx();
                    for (o, id) in out.iter_mut().zip(ids) {
                        if o.is_none()
                            && let Some(i) = idx.lookup(id)
                        {
                            *o = Some((p, idx.offset(i)));
                        }
                    }
                }
                Idx::Sampled(s) => {
                    let todo: Vec<usize> = (0..ids.len())
                        .filter(|&i| out[i].is_none() && s.window(&ids[i]).is_some())
                        .collect();
                    let found = join_bounded(
                        todo.iter().map(|&i| self.sampled_lookup(s, &ids[i])),
                        self.options.max_parallel_reads,
                    )
                    .await;
                    for (&i, f) in todo.iter().zip(found) {
                        if let Some(offset) = f? {
                            out[i] = Some((p, offset));
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// The offset of `id` in a sampled pack: one read of the IDs that can
    /// hold it, one of its offset (two for a large offset).
    async fn sampled_lookup(
        &self,
        s: &SampledIdx,
        id: &ObjectId,
    ) -> Result<Option<u64>, StoreError> {
        let Some((from, _, range)) = s.window(id) else {
            return Ok(None);
        };
        let key = s.record.keys().idx;
        let ids = self.read_idx(&key, range).await?;
        let Some(i) = SampledIdx::find(from, &ids, id) else {
            return Ok(None);
        };
        let raw = self.read_idx(&key, s.offset_range(i)).await?;
        Ok(Some(match s.small_offset(&raw) {
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
            Idx::Sampled(s) => return self.read_entry_open_ended(&s.record, offset, budget).await,
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
    })
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
        let mut reads: HashMap<At, Read> = HashMap::new();
        let mut frontier: Vec<At> = wanted.iter().map(|w| w.1).collect();
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
                    Idx::Sampled(s) => self.sampled_lookup(s, id),
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
