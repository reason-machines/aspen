//! Cold start of the `.idx` cache under many concurrent pushes (monorepo
//! benchmark, item 8).
//!
//! The monorepo benchmark (300,000 files, 500,000 commits, 11.6 million
//! objects in about 300 MB of `.idx`) started 128 pushes at once against a
//! fresh server. Every push looks up objects in the same search packs, and
//! every one of them missed the cache and read each `.idx` itself: 128
//! copies of the same 300 MB. The repository's `.idx` files were also
//! larger than the cache (256 MiB), so the cache thrashed and the copies
//! never stopped. The server reached 15.6 GB of memory and was killed.
//!
//! This test rebuilds that shape without Git: seven compacted packs whose
//! `.idx` files are each under the size kept whole
//! ([`LocateOptions::max_whole_idx_bytes`]) and together over the cache.
//! The `.idx` files are synthetic but valid (sorted IDs, offsets inside the
//! pack, checksums), as are the entry tables a whole `.idx` is kept with
//! (its chain map, item 11); the packs themselves are never read, because
//! locating reads only `.idx` files and entry tables. 128 lookups start before any read completes
//! (the blob store yields once on every read), twice over. The process's
//! peak resident memory (`VmHWM`, reset after the fixture is built)
//! measures what the locator adds, and past a hard cap every blob read
//! fails, so a run that would exhaust the machine fails the test instead.
//!
//! It asserts:
//! - **one load per pack**: concurrent misses on a pack share one read,
//!   and a second wave reads nothing (the cache does not thrash);
//! - **bounded memory**: the memory the locator adds stays within its `.idx`
//!   budget plus a fixed allowance, however many lookups run;
//! - **every lookup is answered exactly**.
//!
//! Linux only (it reads `/proc/self/status`). Required CI runs the shape at 1/100 scale with the budgets scaled with
//! it; the nightly suite runs the ignored full-size one.

#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PastEndRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, PackChecksum, PackId, RepoId, UnixMicros};
use aspen_core::ingest::{ObjectLocator, SearchPacks, SortedIds};
use aspen_core::pack::{
    EntryKind, EntryTable, PackEntry, PackGeneration, PackOrigin, PackRecord, entry_table_len,
};
use aspen_pack::MaxObjectBytes;
use aspen_pack::idx::{IdxEntry, write};
use aspen_pack::locate::{LocateOptions, PackRows, StoredPacks};

// ------------------------------------------------------------------ memory

/// This process's resident memory now, in bytes (`VmRSS`).
fn rss() -> u64 {
    status_kib("VmRSS:") * 1024
}

/// Most resident memory since [`reset_peak`], in bytes (`VmHWM`).
fn peak_rss() -> u64 {
    status_kib("VmHWM:") * 1024
}

/// Resets the peak to the current resident memory (Linux 4.0 and later).
fn reset_peak() {
    std::fs::write("/proc/self/clear_refs", "5").expect("reset VmHWM");
}

fn status_kib(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("procfs");
    let line = status
        .lines()
        .find(|l| l.starts_with(field))
        .expect("field present");
    line[field.len()..]
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .expect("a number")
}

/// Resident memory past which every blob read fails: a run that would
/// otherwise exhaust the machine fails the test instead. Reads are where
/// the locator's memory comes from.
static CAP: AtomicU64 = AtomicU64::new(u64::MAX);

/// Tests in this binary measure one process's memory: one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

// ------------------------------------------------------------------ stores

const REPO: RepoId = match RepoId::new(1) {
    Ok(r) => r,
    Err(_) => panic!(),
};

struct Rows(HashMap<PackId, PackRecord>);

impl PackRows for &Rows {
    async fn pack_rows(&self, ids: &[PackId]) -> Result<Vec<PackRecord>, StoreError> {
        Ok(ids.iter().map(|id| self.0[id].clone()).collect())
    }
}

/// `.idx` and entry-table blobs in memory. Every read yields before it starts and again
/// once its bytes are allocated, as a network read does, so concurrent
/// lookups all start, and all hold their bytes, before any finishes.
struct Blobs(HashMap<BlobKey, Arc<Vec<u8>>>);

struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl BlobStore for &Blobs {
    async fn upload_file(&self, _: &BlobKey, _: &Path) -> Result<UploadReceipt, StoreError> {
        unreachable!("the fixture is built in memory")
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        YieldOnce(false).await;
        if rss() > CAP.load(SeqCst) {
            return Err(StoreError::Unavailable {
                reason: format!("test memory cap of {} MiB reached", CAP.load(SeqCst) >> 20),
            });
        }
        let blob = self.0.get(key).ok_or(StoreError::NotFound {
            what: key.to_string(),
        })?;
        let end = (range.end() as usize).min(blob.len());
        let start = (range.start() as usize).min(end);
        let bytes = blob[start..end].to_vec();
        // Holds the bytes across a second yield, as a body arriving over the
        // network is held: concurrent reads are all in memory at once.
        YieldOnce(false).await;
        Ok(bytes)
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        Ok(self.0.get(key).map(|b| b.len() as u64))
    }

    async fn delete(&self, _: &BlobKey) -> Result<(), StoreError> {
        unreachable!()
    }

    async fn presign_get(&self, _: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        Ok(PresignedUrl {
            url: "memory://".into(),
            ttl,
            past_end: PastEndRange::Empty206,
        })
    }
}

// ----------------------------------------------------------------- fixture

/// A repository's compacted packs: `packs` packs of `objects` objects each.
#[derive(Debug, Clone, Copy)]
struct Shape {
    packs: u32,
    objects: u32,
}

/// The monorepo benchmark's 11.6 million objects, in seven top-level
/// compaction packs of about 46 MB of `.idx` each (325 MB in all).
const MONOREPO: Shape = Shape {
    packs: 7,
    objects: 1_657_143,
};

/// The same shape at 1/100 scale, for required CI.
const SMALL: Shape = Shape {
    packs: 7,
    objects: 16_571,
};

/// Object `i` of pack `p`: IDs are spread evenly over the ID space (each
/// pack's interleaved with the others'), so every pack's fanout is full and
/// a pack's IDs are sorted by `i`.
fn object(shape: Shape, p: u32, i: u32) -> ObjectId {
    let n = u64::from(shape.objects) * u64::from(shape.packs);
    let k = u64::from(i) * u64::from(shape.packs) + u64::from(p);
    let hi = ((u128::from(k) << 64) / u128::from(n)) as u64;
    let mut b = [0u8; 20];
    b[..8].copy_from_slice(&hi.to_be_bytes());
    b[8..16].copy_from_slice(&k.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes());
    b[16..].copy_from_slice(&(p.wrapping_mul(0x85eb_ca6b)).to_be_bytes());
    ObjectId::from_bytes(b)
}

/// Entry `i`'s offset in its (never read) pack: 32 bytes per entry.
fn offset(i: u32) -> u64 {
    12 + 32 * u64::from(i)
}

struct Fixture {
    shape: Shape,
    rows: Rows,
    blobs: Blobs,
    ids: Vec<PackId>,
    /// Bytes of every `.idx`, and what one costs kept whole (the file plus
    /// 12 bytes per object of sorted offsets and chain map).
    idx_bytes: u64,
    whole_cost: u64,
}

fn fixture(shape: Shape) -> Fixture {
    let mut rows = HashMap::new();
    let mut blobs = HashMap::new();
    let mut ids = Vec::new();
    let mut idx_bytes = 0;
    let mut whole_cost = 0;
    for p in 0..shape.packs {
        let checksum = PackChecksum::from_slice(&[p as u8 + 1; 20]).expect("20 bytes");
        let entries = (0..shape.objects)
            .map(|i| IdxEntry {
                id: object(shape, p, i),
                offset: offset(i),
                crc32: i,
            })
            .collect();
        let idx = write(entries, &checksum);
        let record = PackRecord {
            id: PackId::new(i64::from(p) + 1).expect("positive"),
            pack: aspen_core::pack::NewPack {
                repo: REPO,
                checksum,
                size: offset(shape.objects) + 20,
                idx_size: idx.len() as u64,
                entries_size: entry_table_len(shape.objects),
                object_count: shape.objects,
                ref_delta_count: 0,
                origin: PackOrigin::Compaction {
                    level: aspen_core::pack::CoverLevel::new(4).expect("a level"),
                },
            },
            generation: PackGeneration::FIRST,
            created_at: UnixMicros(0),
        };
        idx_bytes += idx.len() as u64;
        whole_cost += idx.len() as u64 + 12 * u64::from(shape.objects);
        let table = EntryTable::new(
            checksum,
            record.pack.size,
            (0..shape.objects)
                .map(|i| PackEntry {
                    offset: offset(i),
                    header_len: 1,
                    id: object(shape, p, i),
                    kind: EntryKind::Blob,
                })
                .collect(),
        )
        .expect("a valid entry table");
        blobs.insert(record.keys().entries, Arc::new(table.encode()));
        blobs.insert(record.keys().idx, Arc::new(idx));
        ids.push(record.id);
        rows.insert(record.id, record);
    }
    Fixture {
        shape,
        rows: Rows(rows),
        blobs: Blobs(blobs),
        ids,
        idx_bytes,
        whole_cost,
    }
}

// ---------------------------------------------------------------- executor

/// Polls every future until all are done, one thread, no runtime: each
/// round polls each unfinished future once.
fn run_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut out: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    let mut cx = Context::from_waker(Waker::noop());
    while out.iter().any(Option::is_none) {
        for (f, o) in futures.iter_mut().zip(out.iter_mut()) {
            if o.is_none()
                && let Poll::Ready(v) = f.as_mut().poll(&mut cx)
            {
                *o = Some(v);
            }
        }
    }
    out.into_iter().map(|o| o.expect("done")).collect()
}

// -------------------------------------------------------------------- test

struct Outcome {
    loads: u64,
    sampled: u64,
    peak_over_baseline: u64,
    took: Duration,
}

/// 128 concurrent lookups, twice, of a few objects of every pack plus one
/// no pack holds, through a fresh locator with `options`.
fn storm(fx: &Fixture, options: LocateOptions, cap_over_baseline: u64) -> Outcome {
    const CONCURRENT: usize = 128;
    let search = SearchPacks::new(fx.ids.iter().copied());
    let shape = fx.shape;
    // Each push looks up different objects: four of every pack, and one
    // that no pack holds.
    let wanted = |n: usize| -> (SortedIds, Vec<Option<(PackId, u64)>>) {
        let mut asks = Vec::new();
        for p in 0..shape.packs {
            for j in 0..4u32 {
                let i = (n as u32 * 7919 + j * 104_729 + p) % shape.objects;
                asks.push((object(shape, p, i), Some((fx.ids[p as usize], offset(i)))));
            }
        }
        asks.push((ObjectId::from_bytes([0xff; 20]), None));
        asks.sort_by_key(|a| a.0);
        let ids = SortedIds::from_unsorted(asks.iter().map(|a| a.0).collect());
        (ids, asks.into_iter().map(|a| a.1).collect())
    };
    let asks: Vec<_> = (0..2 * CONCURRENT).map(wanted).collect();

    reset_peak();
    let baseline = rss();
    CAP.store(baseline.saturating_add(cap_over_baseline), SeqCst);
    let started = Instant::now();
    let stored = StoredPacks::new(&fx.rows, &fx.blobs, options);
    for wave in asks.chunks(CONCURRENT) {
        let results = run_all(
            wave.iter()
                .map(|(ids, _)| stored.locate(&search, ids))
                .collect(),
        );
        for ((_, expect), got) in wave.iter().zip(results) {
            let got: Vec<_> = got
                .expect("lookup")
                .into_iter()
                .map(|l| l.map(|l| (l.pack, l.offset)))
                .collect();
            assert_eq!(&got, expect, "every lookup is answered exactly");
        }
    }
    let took = started.elapsed();
    let stats = stored.stats();
    drop(stored);
    CAP.store(u64::MAX, SeqCst);
    Outcome {
        loads: stats.whole_idx_loads + stats.sampled_idx_loads,
        sampled: stats.sampled_idx_loads,
        peak_over_baseline: peak_rss().saturating_sub(baseline),
        took,
    }
}

const MIB: u64 = 1 << 20;

/// The cache budget the benchmark ran (256 MiB, an `.idx` over 64 MiB
/// sampled), scaled down by `scale`.
fn benchmark_options(scale: u64) -> LocateOptions {
    LocateOptions {
        idx_cache_bytes: (256 * MIB) / scale,
        max_whole_idx_bytes: (64 * MIB) / scale,
        ..defaults()
    }
}

fn defaults() -> LocateOptions {
    LocateOptions::new(MaxObjectBytes::new(512 * MIB).expect("non-zero"))
}

/// Runs the storm against `shape` with `options` and checks one load per
/// pack and memory within the `.idx` budget plus `allowance`; returns how
/// many packs were sampled.
fn cold_start(shape: Shape, options: LocateOptions, allowance: u64) -> u64 {
    let _one = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let fx = fixture(shape);
    assert!(
        fx.idx_bytes / u64::from(shape.packs) <= options.max_whole_idx_bytes,
        "every .idx is small enough to keep whole"
    );
    let bound = options.idx_cache_bytes.min(fx.whole_cost) + allowance;
    // Twice the bound: a run past it fails the assertion below with its
    // number; the cap only stops a run that would exhaust the machine.
    let o = storm(&fx, options, 2 * bound + 64 * MIB);
    eprintln!(
        "{} packs, {} objects, {} MiB of .idx ({} MiB kept whole); cache {} MiB: \
         {} .idx loads ({} sampled), peak +{} MiB, {:?}",
        shape.packs,
        u64::from(shape.packs) * u64::from(shape.objects),
        fx.idx_bytes / MIB,
        fx.whole_cost / MIB,
        options.idx_cache_bytes / MIB,
        o.loads,
        o.sampled,
        o.peak_over_baseline / MIB,
        o.took,
    );
    assert_eq!(
        o.loads,
        u64::from(shape.packs),
        "256 lookups load each pack's .idx once"
    );
    assert!(
        o.peak_over_baseline <= bound,
        "peak +{} MiB over the bound of {} MiB",
        o.peak_over_baseline / MIB,
        bound / MIB,
    );
    o.sampled
}

#[test]
fn cold_start_storm_loads_each_idx_once_within_the_budget() {
    // At 1/100 scale the allowance is dominated by what is in flight while
    // samples are built (eight 1.25 MiB reads per pack).
    let fx_over = {
        let fx = fixture(SMALL);
        fx.whole_cost > benchmark_options(100).idx_cache_bytes
    };
    assert!(
        fx_over,
        "the indexes are over the cache, as in the benchmark"
    );
    let sampled = cold_start(SMALL, benchmark_options(100), 32 * MIB);
    assert!(sampled > 0, "the indexes that do not fit are sampled");
}

#[test]
#[ignore = "monorepo scale: 11.6 million objects, about 2 GiB of memory; run nightly"]
fn monorepo_cold_start_storm_loads_each_idx_once_within_the_budget() {
    // The benchmark's 256 MiB cache: part whole, part sampled.
    let sampled = cold_start(MONOREPO, benchmark_options(1), 128 * MIB);
    assert!(sampled > 0);
    // The default cache (1 GiB) keeps all of them whole.
    assert_eq!(cold_start(MONOREPO, defaults(), 128 * MIB), 0);
}
