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

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
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
    /// the server's configuration: a 256 MiB `.idx` cache, 64 reads in
    /// flight, and a prefetch budget of four times the object limit (at
    /// least 1 GiB).
    pub fn new(max_object_bytes: MaxObjectBytes) -> Self {
        Self {
            idx_cache_bytes: 256 << 20,
            max_parallel_reads: 64,
            max_object_bytes,
            max_prefetch_bytes: max_object_bytes.get().saturating_mul(4).max(1 << 30),
        }
    }
}

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

/// The `.idx` cache: least recently used first out.
#[derive(Debug, Default)]
struct Cache {
    entries: HashMap<PackId, Arc<LoadedIdx>>,
    /// Least recently used at the front.
    order: VecDeque<PackId>,
    bytes: u64,
}

impl Cache {
    fn touch(&mut self, id: PackId) {
        if let Some(at) = self.order.iter().position(|p| *p == id) {
            self.order.remove(at);
            self.order.push_back(id);
        }
    }
}

/// Stored packs as an object locator and base prefetcher.
#[derive(Debug)]
pub struct StoredPacks<M, B> {
    meta: M,
    blobs: B,
    options: LocateOptions,
    cache: Mutex<Cache>,
}

impl<M: PackRows, B: BlobStore> StoredPacks<M, B> {
    /// Wraps the stores.
    pub fn new(meta: M, blobs: B, options: LocateOptions) -> Self {
        Self {
            meta,
            blobs,
            options,
            cache: Mutex::new(Cache::default()),
        }
    }

    /// The `.idx` of each pack, from the cache or the blob store.
    async fn load(&self, packs: &[PackId]) -> Result<Vec<Arc<LoadedIdx>>, StoreError> {
        let mut out: Vec<Option<Arc<LoadedIdx>>> = {
            let mut cache = self.cache.lock().expect("not poisoned");
            packs
                .iter()
                .map(|p| {
                    let hit = cache.entries.get(p).cloned();
                    if hit.is_some() {
                        cache.touch(*p);
                    }
                    hit
                })
                .collect()
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
            let keys: Vec<(BlobKey, ByteRange)> = rows
                .iter()
                .map(|r| {
                    let range = ByteRange::new(0, r.pack.idx_size)
                        .ok_or_else(|| corrupt(r, "empty .idx".into()))?;
                    Ok((r.keys().idx, range))
                })
                .collect::<Result<_, StoreError>>()?;
            let reads = join_bounded(
                keys.iter().map(|(k, r)| self.blobs.read_exact_range(k, *r)),
                self.options.max_parallel_reads,
            )
            .await;
            let mut loaded = Vec::with_capacity(rows.len());
            for (row, read) in rows.into_iter().zip(reads) {
                let bytes = read?;
                loaded.push(Arc::new(validate_idx(row, bytes)?));
            }
            let mut cache = self.cache.lock().expect("not poisoned");
            let mut fresh = loaded.into_iter();
            for slot in out.iter_mut().filter(|o| o.is_none()) {
                let l = fresh.next().expect("one load per miss");
                if !cache.entries.contains_key(&l.record.id) {
                    cache.bytes += l.cost();
                    cache.order.push_back(l.record.id);
                    cache.entries.insert(l.record.id, l.clone());
                }
                *slot = Some(l);
            }
            while cache.bytes > self.options.idx_cache_bytes {
                let Some(old) = cache.order.pop_front() else {
                    break;
                };
                if let Some(e) = cache.entries.remove(&old) {
                    cache.bytes -= e.cost();
                }
            }
        }
        Ok(out.into_iter().map(|o| o.expect("filled")).collect())
    }

    /// Reads and inflates one entry, charging the compressed bytes while
    /// they are held and the inflated bytes before they are allocated.
    async fn read_entry(
        &self,
        pack: &LoadedIdx,
        offset: u64,
        budget: &Budget,
    ) -> Result<(header::RawHeader, Vec<u8>), PrefetchError> {
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
        Ok(ids
            .as_slice()
            .iter()
            .map(|id| {
                loaded.iter().find_map(|l| {
                    let idx = l.idx();
                    idx.lookup(id).map(|i| ObjectLocation {
                        pack: l.record.id,
                        offset: idx.offset(i),
                    })
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
    /// [`BasePrefetcher::prefetch`] with its typed error and memory
    /// figures. The trait method reports [`PrefetchError::OverBudget`] as
    /// [`StoreError::Unavailable`]; a caller that can refuse the push with
    /// a clear message (B1) should call this instead.
    pub async fn prefetch_bases(
        &self,
        packs: &SearchPacks,
        candidates: &CandidateBases,
    ) -> Result<(Prefetched<InMemoryBases>, PrefetchStats), PrefetchError> {
        let loaded = self.load(packs.as_slice()).await?;
        let budget = Budget::new(self.options.max_prefetch_bytes);
        let mut wanted: Vec<(ObjectId, At)> = Vec::new();
        let mut not_in_deps = Vec::new();
        for id in candidates.0.as_slice() {
            let hit = loaded.iter().enumerate().find_map(|(p, l)| {
                let idx = l.idx();
                idx.lookup(id).map(|i| (p, idx.offset(i)))
            });
            match hit {
                Some(at) => wanted.push((*id, at)),
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
            for (&(p, off), result) in frontier.iter().zip(results) {
                let (h, data) = result?;
                let l = &loaded[p];
                let read = match h.base {
                    RawBase::None => Read::Full(kind_of(h.type_code), data),
                    RawBase::Distance(d) => {
                        let base = off
                            .checked_sub(d)
                            .filter(|&b| b >= PACK_HEADER_LEN)
                            .ok_or_else(|| {
                                corrupt(
                                    &l.record,
                                    format!("OFS_DELTA at {off} points before the pack"),
                                )
                            })?;
                        Read::Delta {
                            base: (p, base),
                            delta: data,
                        }
                    }
                    RawBase::Id(id) => {
                        // Stored packs are self-contained: the base is here.
                        let idx = l.idx();
                        let i = idx.lookup(&id).ok_or_else(|| {
                            corrupt(
                                &l.record,
                                format!(
                                    "REF_DELTA at {off} is based on {id}, which the pack lacks"
                                ),
                            )
                        })?;
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
            next.sort_unstable();
            next.dedup();
            next.retain(|k| !reads.contains_key(k));
            frontier = next;
        }
        let mut chains = Chains::new(reads, &wanted, &loaded, &budget);
        let mut bases = InMemoryBases::default();
        let mut output_bytes = 0;
        for (id, at) in wanted {
            let (kind, data) = chains.take_resolved(at, self.options.max_object_bytes.get())?;
            output_bytes += data.len() as u64;
            bases.insert(id, kind, data);
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
    loaded: &'a [Arc<LoadedIdx>],
    budget: &'a Budget,
}

impl<'a> Chains<'a> {
    fn new(
        reads: HashMap<At, Read>,
        wanted: &[(ObjectId, At)],
        loaded: &'a [Arc<LoadedIdx>],
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
                        return Err(
                            corrupt(&self.loaded[at.0].record, "delta chain loops".into()).into(),
                        );
                    }
                    chain.push((cur, delta));
                    cur = base;
                }
                None => {
                    // Every read entry is resolved before it is spent, and
                    // it is only removed from `reads` when resolved, so a
                    // missing one is on the chain being built: a loop.
                    return Err(
                        corrupt(&self.loaded[at.0].record, "delta chain loops".into()).into(),
                    );
                }
            }
        }
        let mut base = cur;
        while let Some((k, delta)) = chain.pop() {
            let record = &self.loaded[k.0].record;
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
