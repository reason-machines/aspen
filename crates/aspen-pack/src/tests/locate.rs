//! Locating objects in stored packs and prefetching thin bases, against
//! in-memory row and blob stores holding real ingested packs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PastEndRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, PackId, RepoId, UnixMicros};
use aspen_core::ingest::{BaseLookup, BasePrefetcher, ObjectLocator, SearchPacks, SortedIds};
use aspen_core::pack::{PackRecord, PushLimits};

use crate::ingest::{CompleteOptions, IngestedPack, complete, object_id_of, receive};
use crate::locate::{LocateOptions, PackRows, PrefetchError, StoredPacks, apply_delta};
use crate::testutil::{MAX_OBJECT, Obj, Spec, build_pack, git, git_raw, scratch};

use super::ingest::history;
use super::scan::ready;

#[derive(Default)]
struct Rows(Mutex<HashMap<PackId, PackRecord>>);

impl PackRows for &Rows {
    async fn pack_rows(&self, ids: &[PackId]) -> Result<Vec<PackRecord>, StoreError> {
        let rows = self.0.lock().unwrap();
        ids.iter()
            .map(|id| {
                rows.get(id).cloned().ok_or(StoreError::NotFound {
                    what: format!("pack {id}"),
                })
            })
            .collect()
    }
}

#[derive(Default)]
struct Blobs {
    data: Mutex<HashMap<BlobKey, Vec<u8>>>,
    reads: AtomicUsize,
    /// Drop this many bytes from the end of every read (a short read).
    short_by: AtomicUsize,
    /// While set, every read stays pending (a slow object store).
    hold: std::sync::atomic::AtomicBool,
}

impl BlobStore for &Blobs {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        let bytes = std::fs::read(path).unwrap();
        let size = bytes.len() as u64;
        self.data.lock().unwrap().insert(key.clone(), bytes);
        Ok(UploadReceipt { size })
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        std::future::poll_fn(|_| {
            if self.hold.load(Ordering::Relaxed) {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(())
            }
        })
        .await;
        self.reads.fetch_add(1, Ordering::Relaxed);
        let data = self.data.lock().unwrap();
        let blob = data.get(key).ok_or(StoreError::NotFound {
            what: key.to_string(),
        })?;
        let end = (range.end() as usize).min(blob.len());
        let start = (range.start() as usize).min(end);
        let mut out = blob[start..end].to_vec();
        let short = self.short_by.load(Ordering::Relaxed).min(out.len());
        out.truncate(out.len() - short);
        Ok(out)
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        Ok(self.data.lock().unwrap().get(key).map(|b| b.len() as u64))
    }

    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        self.data.lock().unwrap().remove(key);
        Ok(())
    }

    async fn presign_get(&self, _: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        Ok(PresignedUrl {
            url: "memory://".into(),
            ttl,
            past_end: PastEndRange::Empty206,
        })
    }
}

const REPO: RepoId = match RepoId::new(1) {
    Ok(r) => r,
    Err(_) => panic!(),
};

/// Stores an ingested pack: uploads its three blobs and adds its row.
fn store(p: &IngestedPack, id: i64, rows: &Rows, blobs: &Blobs) -> PackId {
    let record = PackRecord {
        id: PackId::new(id).unwrap(),
        pack: p.new_pack(REPO),
        generation: aspen_core::pack::PackGeneration::FIRST,
        created_at: UnixMicros(0),
    };
    let keys = record.keys();
    ready(blobs.upload_file(&keys.pack, &p.pack_path)).unwrap();
    ready(blobs.upload_file(&keys.idx, &p.idx_path)).unwrap();
    ready(blobs.upload_file(&keys.entries, &p.entries_path)).unwrap();
    rows.0.lock().unwrap().insert(record.id, record.clone());
    record.id
}

fn pack_objects(repo: &Path, revs: &str, thin: bool) -> Vec<u8> {
    let mut args = vec![
        "pack-objects",
        "-q",
        "--revs",
        "--stdout",
        "--delta-base-offset",
    ];
    if thin {
        args.push("--thin");
    }
    let out = git_raw(repo, &args, Some(revs.as_bytes()));
    assert!(out.status.success());
    out.stdout
}

const LIMITS: PushLimits = PushLimits {
    max_objects: 100_000,
    max_pack_bytes: 1 << 30,
};

/// Ingests `pack` with bases prefetched from `deps` (the stored packs),
/// checking that every prefetched base hashes to its ID.
fn push(
    pack: &[u8],
    deps: &[PackId],
    stored: &StoredPacks<&Rows, &Blobs>,
    dir: &Path,
    n: usize,
) -> (IngestedPack, usize) {
    let spool = dir.join(format!("spool-{n}"));
    let received = receive(&mut &pack[..], &spool, &LIMITS).unwrap();
    let search = SearchPacks::new(deps.iter().copied());
    let pre = ready(stored.prefetch(&search, &received.candidates)).unwrap();
    let mut buf = Vec::new();
    let mut found = 0;
    for id in received.candidates.0.as_slice() {
        if let Some(kind) = pre.bases.find_base(id, &mut buf).unwrap() {
            assert_eq!(
                object_id_of(kind, &buf),
                *id,
                "prefetched base is the object"
            );
            found += 1;
        } else {
            assert!(pre.not_in_deps.contains(id));
        }
    }
    let out_dir = dir.join(format!("out-{n}"));
    std::fs::create_dir_all(&out_dir).unwrap();
    let out = complete(
        &received,
        &pre.bases,
        &out_dir,
        &CompleteOptions::new(crate::testutil::MAX_OBJECT, crate::testutil::MAX_INFLATED),
    )
    .unwrap();
    (out, found)
}

#[test]
fn three_pushes_complete_from_prefetched_stored_bases() {
    // Push 1 stores C1..C4 whole; push 2 (C5..C8) and push 3 (C9..C12) are
    // thin, and their bases are prefetched from the stored packs of the
    // earlier pushes, newest first, through the blob store only.
    let dir = scratch();
    let src = history(dir.path(), 12);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let stored = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions::new(crate::testutil::MAX_OBJECT),
    );
    let (p1, _) = push(
        &pack_objects(&src, "HEAD~8\n", false),
        &[],
        &stored,
        dir.path(),
        1,
    );
    let id1 = store(&p1, 1, &rows, &blobs);
    let (p2, found2) = push(
        &pack_objects(&src, "HEAD~4\n^HEAD~8\n", true),
        &[id1],
        &stored,
        dir.path(),
        2,
    );
    assert!(found2 > 0, "push 2 is thin");
    let id2 = store(&p2, 2, &rows, &blobs);
    let (p3, found3) = push(
        &pack_objects(&src, "HEAD\n^HEAD~4\n", true),
        &[id2, id1],
        &stored,
        dir.path(),
        3,
    );
    assert!(found3 > 0, "push 3 is thin");
    assert!(!p3.completion_bases.is_empty());
    // The three stored packs hold the whole history between them.
    let total: u32 = [&p1, &p2, &p3].iter().map(|p| p.object_count).sum();
    let want = git_raw(&src, &["rev-list", "--objects", "--count", "HEAD"], None).stdout;
    let want: u32 = String::from_utf8(want).unwrap().trim().parse().unwrap();
    assert!(total >= want);
}

#[test]
fn locate_probes_in_order_and_caches_idx_files() {
    let dir = scratch();
    let src = history(dir.path(), 6);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let stored = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions::new(crate::testutil::MAX_OBJECT),
    );
    let (full, _) = push(
        &pack_objects(&src, "HEAD\n", false),
        &[],
        &stored,
        dir.path(),
        1,
    );
    let (half, _) = push(
        &pack_objects(&src, "HEAD~3\n", false),
        &[],
        &stored,
        dir.path(),
        2,
    );
    let a = store(&full, 1, &rows, &blobs);
    let b = store(&half, 2, &rows, &blobs);
    let head = ObjectId::parse_hex(
        String::from_utf8(git_raw(&src, &["rev-parse", "HEAD"], None).stdout)
            .unwrap()
            .trim(),
    )
    .unwrap();
    let old = ObjectId::parse_hex(
        String::from_utf8(git_raw(&src, &["rev-parse", "HEAD~3"], None).stdout)
            .unwrap()
            .trim(),
    )
    .unwrap();
    let unknown = ObjectId::from_bytes([0xee; 20]);
    let ids = SortedIds::from_unsorted(vec![head, old, unknown]);
    let got = ready(stored.locate(&SearchPacks::new([b, a]), &ids)).unwrap();
    let by_id: HashMap<ObjectId, Option<PackId>> = ids
        .as_slice()
        .iter()
        .zip(&got)
        .map(|(id, l)| (*id, l.map(|l| l.pack)))
        .collect();
    assert_eq!(by_id[&head], Some(a), "only the full pack has HEAD");
    assert_eq!(by_id[&old], Some(b), "the first pack in probe order wins");
    assert_eq!(by_id[&unknown], None);
    let reads = blobs.reads.load(Ordering::Relaxed);
    ready(stored.locate(&SearchPacks::new([b, a]), &ids)).unwrap();
    assert_eq!(
        blobs.reads.load(Ordering::Relaxed),
        reads,
        "warm lookups read nothing"
    );
}

#[test]
fn a_remembered_idx_is_not_read_back() {
    // Invariant: the `.idx` a writer remembers right after storing a pack
    // serves the next lookup without a blob read; one that does not match
    // its row is ignored, and the lookup reads the stored files.
    let dir = scratch();
    let src = history(dir.path(), 3);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let setup = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let (p, _) = push(
        &pack_objects(&src, "HEAD\n", false),
        &[],
        &setup,
        dir.path(),
        1,
    );
    let id = store(&p, 1, &rows, &blobs);
    let record = rows.0.lock().unwrap()[&id].clone();
    let idx = std::fs::read(&p.idx_path).unwrap();
    let table = std::fs::read(&p.entries_path).unwrap();
    let none = SortedIds::from_unsorted(vec![ObjectId::from_bytes([0xee; 20])]);
    let fresh = || StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let reads = |s: &StoredPacks<&Rows, &Blobs>| {
        let before = blobs.reads.load(Ordering::Relaxed);
        ready(s.locate(&SearchPacks::new([id]), &none)).unwrap();
        blobs.reads.load(Ordering::Relaxed) - before
    };
    let seeded = fresh();
    assert!(seeded.remember_pack(record.clone(), idx.clone(), &table));
    assert_eq!(reads(&seeded), 0, "the remembered .idx answers");
    let wrong = fresh();
    assert!(!wrong.remember_pack(record, idx[..idx.len() - 1].to_vec(), &table));
    // A cold load reads the `.idx` and the entry table (its chain map).
    assert_eq!(reads(&wrong), 2, "a short .idx is not cached");
}

#[test]
fn short_reads_and_wrong_rows_are_corruption() {
    let dir = scratch();
    let src = history(dir.path(), 4);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let (p, _) = {
        let stored = StoredPacks::new(
            &rows,
            &blobs,
            LocateOptions::new(crate::testutil::MAX_OBJECT),
        );
        push(
            &pack_objects(&src, "HEAD\n", false),
            &[],
            &stored,
            dir.path(),
            1,
        )
    };
    let id = store(&p, 1, &rows, &blobs);
    let ids = SortedIds::from_unsorted(vec![ObjectId::from_bytes([1; 20])]);
    // A short .idx read.
    blobs.short_by.store(1, Ordering::Relaxed);
    let cold = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions::new(crate::testutil::MAX_OBJECT),
    );
    let err = ready(cold.locate(&SearchPacks::new([id]), &ids)).unwrap_err();
    assert!(err.is_corruption(), "{err}");
    // A short entry read during prefetch (the .idx is cached first).
    blobs.short_by.store(0, Ordering::Relaxed);
    let warm = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions::new(crate::testutil::MAX_OBJECT),
    );
    ready(warm.locate(&SearchPacks::new([id]), &ids)).unwrap();
    blobs.short_by.store(1, Ordering::Relaxed);
    let candidate = {
        let t = crate::verify::verify_pack(
            &p.pack_path,
            dir.path(),
            crate::testutil::MAX_OBJECT,
            crate::testutil::MAX_INFLATED,
        )
        .unwrap()
        .table;
        t.entries()[0].id
    };
    let err = ready(warm.prefetch(
        &SearchPacks::new([id]),
        &aspen_core::ingest::CandidateBases(SortedIds::from_unsorted(vec![candidate])),
    ))
    .unwrap_err();
    // A4's read_exact_range reports it.
    assert!(
        err.is_corruption() && err.to_string().contains("returned"),
        "{err}"
    );
    // A row whose .idx size is wrong.
    blobs.short_by.store(0, Ordering::Relaxed);
    rows.0.lock().unwrap().get_mut(&id).unwrap().pack.idx_size += 8;
    let fresh = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions::new(crate::testutil::MAX_OBJECT),
    );
    assert!(
        ready(fresh.locate(&SearchPacks::new([id]), &ids))
            .unwrap_err()
            .is_corruption()
    );
}

#[test]
fn deltas_apply_and_bad_deltas_are_errors() {
    let base = b"hello, world, this is the base object".to_vec();
    let target = b"hello, world, this is the target object, longer".to_vec();
    let d = crate::testutil::delta(&base, &target);
    assert_eq!(apply_delta(&base, &d, 1 << 20).unwrap(), target);
    assert!(
        apply_delta(&base[1..], &d, 1 << 20).is_err(),
        "wrong base size"
    );
    assert!(
        apply_delta(&base, &d[..d.len() - 1], 1 << 20).is_err(),
        "truncated"
    );
    assert!(apply_delta(&base, &d, 4).is_err(), "over the limit");
    // Copy past the end of the base: base 2 bytes, result 4, copy 4 at 0.
    assert!(apply_delta(b"ab", &[2, 4, 0x90, 4], 100).is_err());
    assert!(apply_delta(b"ab", &[2, 1, 0], 100).is_err(), "opcode 0");
    // Copy size 0 means 0x10000.
    let big = vec![7u8; 0x10000];
    let d = [0x80, 0x80, 0x04, 0x80, 0x80, 0x04, 0x80];
    assert_eq!(apply_delta(&big, &d, 1 << 20).unwrap(), big);
}

/// `n + 1` blobs of `size` bytes sharing all but their last 8 bytes, each
/// stored as a delta of the one before: a chain of depth `n` whose deltas
/// are tiny and whose objects compress well.
fn big_chain(n: usize, size: usize) -> Vec<Obj> {
    let line = b"the same line, over and over, so it compresses\n";
    let prefix: Vec<u8> = line.iter().copied().cycle().take(size - 8).collect();
    (0..=n)
        .map(|i| {
            let mut d = prefix.clone();
            d.extend_from_slice(format!("{i:08}").as_bytes());
            Obj::blob(d)
        })
        .collect()
}

fn chain_specs(objs: &[Obj]) -> Vec<Spec> {
    objs.iter()
        .enumerate()
        .map(|(i, o)| match i {
            0 => Spec::Full(o.clone()),
            _ => Spec::Ofs(i - 1, o.clone()),
        })
        .collect()
}

fn candidates(ids: &[ObjectId]) -> aspen_core::ingest::CandidateBases {
    aspen_core::ingest::CandidateBases(SortedIds::from_unsorted(ids.to_vec()))
}

#[test]
fn a_repeated_base_comes_from_the_object_cache() {
    // Consecutive pushes mostly need the same bases (trunk's root tree):
    // the second prefetch of a resolved base reads no blobs, returns the
    // same bytes, and still counts against the prefetch budget. A cached
    // object is used only when the search packs hold it.
    let dir = scratch();
    let objs = big_chain(6, 4 << 10);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let stored = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let (p, _) = push(
        &build_pack(&chain_specs(&objs)),
        &[],
        &stored,
        dir.path(),
        1,
    );
    let id = store(&p, 1, &rows, &blobs);
    let search = SearchPacks::new([id]);
    let top = objs[6].id();
    let (first, _) = ready(stored.prefetch_bases(&search, &candidates(&[top]))).unwrap();
    let reads = blobs.reads.load(Ordering::Relaxed);
    let (again, stats) = ready(stored.prefetch_bases(&search, &candidates(&[top]))).unwrap();
    assert_eq!(blobs.reads.load(Ordering::Relaxed), reads, "no blob reads");
    let (mut a, mut b) = (Vec::new(), Vec::new());
    first.bases.find_base(&top, &mut a).unwrap().unwrap();
    again.bases.find_base(&top, &mut b).unwrap().unwrap();
    assert_eq!(
        (a.as_slice(), b.as_slice()),
        (objs[6].data.as_slice(), objs[6].data.as_slice())
    );
    assert_eq!(stats.output_bytes, objs[6].data.len() as u64);
    assert!(
        stats.peak_bytes >= objs[6].data.len() as u64,
        "a hit is charged"
    );
    // Search packs that lack the object: not found, cache or not.
    let (missing, _) =
        ready(stored.prefetch_bases(&SearchPacks::new([]), &candidates(&[top]))).unwrap();
    assert!(missing.bases.is_empty());
    assert_eq!(missing.not_in_deps.as_slice(), &[top]);
    // With the cache off, every prefetch reads again.
    let off = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions {
            object_cache_bytes: 0,
            ..LocateOptions::new(MAX_OBJECT)
        },
    );
    ready(off.prefetch_bases(&search, &candidates(&[top]))).unwrap();
    let reads = blobs.reads.load(Ordering::Relaxed);
    ready(off.prefetch_bases(&search, &candidates(&[top]))).unwrap();
    assert!(blobs.reads.load(Ordering::Relaxed) > reads);
}

#[test]
fn a_deep_chain_prefetch_peaks_near_two_objects() {
    // Invariant: prefetch memory does not grow with chain depth. Only
    // intermediates another chain still needs are kept, and the last use
    // of an object moves it, so a depth-6 chain of 256 KiB objects peaks
    // near 2 objects (the base being applied and its result), not the
    // 8 objects keeping every level and a copy would hold.
    const S: usize = 256 << 10;
    let dir = scratch();
    let objs = big_chain(6, S);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let stored = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let (p, _) = push(
        &build_pack(&chain_specs(&objs)),
        &[],
        &stored,
        dir.path(),
        1,
    );
    assert_eq!(p.max_delta_depth, 6);
    let id = store(&p, 1, &rows, &blobs);
    let search = SearchPacks::new([id]);
    let top = objs[6].id();
    let (pre, stats) = ready(stored.prefetch_bases(&search, &candidates(&[top]))).unwrap();
    let mut buf = Vec::new();
    pre.bases.find_base(&top, &mut buf).unwrap().unwrap();
    assert_eq!(buf, objs[6].data);
    assert_eq!(stats.output_bytes, S as u64);
    assert!(
        stats.peak_bytes < 2 * S as u64 + (64 << 10),
        "peak {} for objects of {S}",
        stats.peak_bytes
    );
    // Every object of the chain at once: shared levels are kept while
    // needed and each comes back intact.
    let all: Vec<ObjectId> = objs.iter().map(Obj::id).collect();
    let (pre, stats) = ready(stored.prefetch_bases(&search, &candidates(&all))).unwrap();
    for o in &objs {
        pre.bases.find_base(&o.id(), &mut buf).unwrap().unwrap();
        assert_eq!(buf, o.data);
    }
    assert_eq!(stats.output_bytes, 7 * S as u64);
    assert!(stats.peak_bytes < 8 * S as u64 + (64 << 10));
}

#[test]
fn a_prefetch_over_its_budget_is_refused_before_allocating() {
    // Invariant: one prefetch never holds more than max_prefetch_bytes. A
    // stored "zlib bomb" (8 MiB of one byte, far less on disk) used as a
    // base by a later thin push is refused with a typed error that is not
    // corruption (nothing stored is wrong, so it must not page).
    let dir = scratch();
    let bomb = Obj::blob(vec![b'z'; 8 << 20]);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let roomy = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let (p, _) = push(
        &build_pack(&[Spec::Full(bomb.clone())]),
        &[],
        &roomy,
        dir.path(),
        1,
    );
    assert!(p.size < 1 << 20, "on disk it fits the budget it overruns");
    let id = store(&p, 1, &rows, &blobs);
    let search = SearchPacks::new([id]);
    let tight = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions {
            max_prefetch_bytes: 1 << 20,
            ..LocateOptions::new(MAX_OBJECT)
        },
    );
    let err = ready(tight.prefetch_bases(&search, &candidates(&[bomb.id()]))).unwrap_err();
    assert_eq!(err, PrefetchError::OverBudget { budget: 1 << 20 });
    let err = ready(tight.prefetch(&search, &candidates(&[bomb.id()]))).unwrap_err();
    assert!(!err.is_corruption(), "{err}");
    // Under a budget that fits, the same prefetch succeeds.
    let (pre, _) = ready(roomy.prefetch_bases(&search, &candidates(&[bomb.id()]))).unwrap();
    assert_eq!(pre.bases.len(), 1);
}

#[test]
fn the_idx_cache_evicts_the_least_recently_used_pack() {
    // Invariant: the cache stays within idx_cache_bytes (counting the
    // offsets and chain map it keeps per object) and evicts the pack used
    // longest ago, so a pack probed on every push stays warm. A load reads
    // the `.idx` and the entry table.
    let dir = scratch();
    let src = history(dir.path(), 9);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let setup = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let mut ids = Vec::new();
    let mut costs = Vec::new();
    for (n, revs) in ["HEAD~6\n", "HEAD~3\n^HEAD~6\n", "HEAD\n^HEAD~3\n"]
        .iter()
        .enumerate()
    {
        let (p, _) = push(
            &pack_objects(&src, revs, false),
            &[],
            &setup,
            dir.path(),
            n + 1,
        );
        costs.push(p.idx_size + 12 * u64::from(p.object_count));
        ids.push(store(&p, n as i64 + 1, &rows, &blobs));
    }
    let [a, b, c] = [ids[0], ids[1], ids[2]];
    let budget = costs[0] + costs[1].max(costs[2]);
    let cache = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions {
            idx_cache_bytes: budget,
            ..LocateOptions::new(MAX_OBJECT)
        },
    );
    let none = SortedIds::from_unsorted(vec![ObjectId::from_bytes([0xee; 20])]);
    let locate = |p: PackId| {
        let before = blobs.reads.load(Ordering::Relaxed);
        ready(cache.locate(&SearchPacks::new([p]), &none)).unwrap();
        blobs.reads.load(Ordering::Relaxed) - before
    };
    assert_eq!(locate(a), 2);
    assert_eq!(locate(b), 2);
    assert_eq!(locate(a), 0, "a is cached and now the most recently used");
    assert_eq!(locate(c), 2, "loading c evicts b, the least recently used");
    assert_eq!(locate(a), 0, "a survived the eviction");
    assert_eq!(locate(b), 2, "b was evicted");
}

/// Polls `f` once with a waker that does nothing.
fn poll_once<F: std::future::Future + ?Sized>(
    f: std::pin::Pin<&mut F>,
) -> std::task::Poll<F::Output> {
    f.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

#[test]
fn concurrent_misses_on_a_pack_share_one_load() {
    // Invariant (monorepo benchmark, item 8): calls that miss the same
    // `.idx` while it is being read wait for that read instead of reading
    // it again; and a leader dropped mid-read (its request ended) hands
    // the load to a waiter instead of leaving it waiting for ever.
    let dir = scratch();
    let src = history(dir.path(), 3);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let setup = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let (p, _) = push(
        &pack_objects(&src, "HEAD\n", false),
        &[],
        &setup,
        dir.path(),
        1,
    );
    let pack = store(&p, 1, &rows, &blobs);
    let search = SearchPacks::new([pack]);
    let none = SortedIds::from_unsorted(vec![ObjectId::from_bytes([0xee; 20])]);

    // Two callers miss together; one read serves both.
    let stored = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    blobs.hold.store(true, Ordering::Relaxed);
    let mut a = Box::pin(stored.locate(&search, &none));
    let mut b = Box::pin(stored.locate(&search, &none));
    assert!(poll_once(a.as_mut()).is_pending(), "a leads the load");
    assert!(poll_once(b.as_mut()).is_pending(), "b waits for it");
    blobs.hold.store(false, Ordering::Relaxed);
    assert!(poll_once(a.as_mut()).is_ready());
    assert!(poll_once(b.as_mut()).is_ready());
    let stats = stored.stats();
    assert_eq!(stats.whole_idx_loads, 1, "one read for two misses");
    assert_eq!(stats.idx_load_waits, 1);

    // The leader is dropped mid-read: the waiter loads it itself.
    let stored = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    blobs.hold.store(true, Ordering::Relaxed);
    let mut a = Box::pin(stored.locate(&search, &none));
    let mut b = Box::pin(stored.locate(&search, &none));
    assert!(poll_once(a.as_mut()).is_pending());
    assert!(poll_once(b.as_mut()).is_pending());
    drop(a);
    blobs.hold.store(false, Ordering::Relaxed);
    match poll_once(b.as_mut()) {
        std::task::Poll::Ready(found) => assert_eq!(found.unwrap(), vec![None]),
        std::task::Poll::Pending => panic!("the waiter took over the abandoned load"),
    }
    assert_eq!(stored.stats().whole_idx_loads, 1);
    // And it is cached: the next call reads nothing.
    let before = blobs.reads.load(Ordering::Relaxed);
    ready(stored.locate(&search, &none)).unwrap();
    assert_eq!(blobs.reads.load(Ordering::Relaxed), before);
}

#[test]
fn a_search_set_over_the_budget_is_part_sampled_and_never_reloaded() {
    // Invariant (monorepo benchmark, item 8): when a search set's whole
    // `.idx` files do not fit the cache, the packs probed first are kept
    // whole while they fit and the rest are sampled, so the set stays
    // cached: repeated lookups read no `.idx` again. Before, every pack was
    // loaded whole, the set evicted itself, and every lookup reloaded it.
    let dir = scratch();
    let src = history(dir.path(), 60);
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let setup = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let mut ids = Vec::new();
    let mut costs = Vec::new();
    for (n, revs) in ["HEAD~40\n", "HEAD~20\n^HEAD~40\n", "HEAD\n^HEAD~20\n"]
        .iter()
        .enumerate()
    {
        let (p, _) = push(
            &pack_objects(&src, revs, false),
            &[],
            &setup,
            dir.path(),
            n + 1,
        );
        costs.push(p.idx_size + 8 * u64::from(p.object_count));
        ids.push(store(&p, n as i64 + 1, &rows, &blobs));
    }
    // Room for the first two whole, and for the third only sampled (a
    // sample is about 1 KiB plus 20 bytes per 256 objects).
    assert!(
        costs[2] > 2048,
        "the third pack's whole .idx outweighs a sample"
    );
    let stored = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions {
            idx_cache_bytes: costs[0] + costs[1] + costs[2] - 1,
            ..LocateOptions::new(MAX_OBJECT)
        },
    );
    let search = SearchPacks::new(ids.iter().copied());
    let wanted = SortedIds::from_unsorted(
        (0..60)
            .map(|n| {
                git(&src, &["rev-parse", &format!("HEAD~{n}")])
                    .trim()
                    .parse::<ObjectId>()
                    .unwrap()
            })
            .collect(),
    );
    let first = ready(stored.locate(&search, &wanted)).unwrap();
    assert!(first.iter().all(Option::is_some), "every commit is found");
    let stats = stored.stats();
    assert_eq!((stats.whole_idx_loads, stats.sampled_idx_loads), (2, 1));
    for _ in 0..3 {
        assert_eq!(ready(stored.locate(&search, &wanted)).unwrap(), first);
    }
    let again = stored.stats();
    assert_eq!(
        (again.whole_idx_loads, again.sampled_idx_loads),
        (2, 1),
        "nothing is loaded again"
    );
}

#[test]
fn a_sampled_idx_answers_exactly_as_a_whole_one() {
    // Invariant: a locator that keeps every `.idx` as a sample
    // (`max_whole_idx_bytes: 0`) finds the same objects at the same places,
    // and prefetches the same bases, as one that keeps them whole: for
    // OFS_DELTA and REF_DELTA packs, for entries longer than its first read
    // of an entry, and for IDs no pack holds. Once its samples are built, a
    // lookup reads a few kilobytes of `.idx`, never a whole one.
    let dir = scratch();
    let src = history(dir.path(), 9);
    // An incompressible blob, longer than the first read of an entry.
    let mut noise = Vec::with_capacity(200_000);
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    while noise.len() < 200_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        noise.extend_from_slice(&x.to_le_bytes());
    }
    std::fs::write(src.join("noise.bin"), &noise).unwrap();
    git_raw(&src, &["add", "-A"], None);
    let commit = git_raw(&src, &["commit", "-q", "-m", "noise"], None);
    assert!(commit.status.success());
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let setup = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let ref_delta = {
        let args = ["pack-objects", "-q", "--revs", "--stdout"];
        let out = git_raw(&src, &args, Some(b"HEAD~5\n"));
        assert!(out.status.success());
        out.stdout
    };
    let (p1, _) = push(&ref_delta, &[], &setup, dir.path(), 1);
    assert!(p1.new_pack(REPO).ref_delta_count > 0, "a REF_DELTA pack");
    let a = store(&p1, 1, &rows, &blobs);
    let (p2, _) = push(
        &pack_objects(&src, "HEAD\n^HEAD~5\n", true),
        &[a],
        &setup,
        dir.path(),
        2,
    );
    let b = store(&p2, 2, &rows, &blobs);
    let search = SearchPacks::new([b, a]);
    let mut ids: Vec<ObjectId> =
        String::from_utf8(git_raw(&src, &["rev-list", "--objects", "HEAD"], None).stdout)
            .unwrap()
            .lines()
            .map(|l| ObjectId::parse_hex(&l[..40]).unwrap())
            .collect();
    for k in 0..64u8 {
        ids.push(ObjectId::from_bytes([k.wrapping_mul(4); 20]));
    }
    let ids = SortedIds::from_unsorted(ids);
    let whole = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let sampled = StoredPacks::new(
        &rows,
        &blobs,
        LocateOptions {
            max_whole_idx_bytes: 0,
            object_cache_bytes: 0,
            ..LocateOptions::new(MAX_OBJECT)
        },
    );
    let want = ready(whole.locate(&search, &ids)).unwrap();
    let got = ready(sampled.locate(&search, &ids)).unwrap();
    assert_eq!(got, want);
    assert!(want.iter().filter(|w| w.is_some()).count() > 20);
    assert_eq!(sampled.stats().sampled_idx_loads, 2);
    assert_eq!(sampled.stats().whole_idx_loads, 0);
    // Every object of the history as a candidate base: chains through
    // REF_DELTA and OFS_DELTA entries, and the long blob.
    let all = aspen_core::ingest::CandidateBases(ids.clone());
    let (w, _) = ready(whole.prefetch_bases(&search, &all)).unwrap();
    let (s, _) = ready(sampled.prefetch_bases(&search, &all)).unwrap();
    assert_eq!(s.not_in_deps, w.not_in_deps);
    assert_eq!(s.bases.len(), w.bases.len());
    let (mut x, mut y) = (Vec::new(), Vec::new());
    for id in ids.as_slice() {
        let kx = w.bases.find_base(id, &mut x).unwrap();
        let ky = s.bases.find_base(id, &mut y).unwrap();
        assert_eq!((kx, &x), (ky, &y), "{id}");
        if let Some(kind) = ky {
            assert_eq!(object_id_of(kind, &y), *id);
        }
    }
    // A warm lookup in a sample reads one stretch of IDs and an offset.
    let before = sampled.stats().idx_bytes_read;
    let head = SortedIds::from_unsorted(vec![ids.as_slice()[0]]);
    ready(sampled.locate(&search, &head)).unwrap();
    let read = sampled.stats().idx_bytes_read - before;
    assert!(
        read <= 2 * (256 * 20 + 8),
        "a sampled lookup read {read} bytes of .idx"
    );
}

/// A blob store that counts round trips: a read started in one round
/// completes only in the next, so [`Lockstep::run`] counts how many rounds
/// of reads a call waits for in sequence, the latency it would pay on an
/// object store, whatever the machine's speed.
struct Lockstep<'a> {
    inner: &'a Blobs,
    round: AtomicUsize,
}

/// Pending until the round after the one it was first polled in.
struct NextRound<'a> {
    round: &'a AtomicUsize,
    started: Option<usize>,
}

impl std::future::Future for NextRound<'_> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let now = self.round.load(Ordering::Relaxed);
        match self.started {
            None => {
                self.started = Some(now);
                std::task::Poll::Pending
            }
            Some(s) if now > s => std::task::Poll::Ready(()),
            Some(_) => std::task::Poll::Pending,
        }
    }
}

impl BlobStore for &Lockstep<'_> {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        self.inner.upload_file(key, path).await
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        NextRound {
            round: &self.round,
            started: None,
        }
        .await;
        self.inner.read_range(key, range).await
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        self.inner.size(key).await
    }

    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    async fn presign_get(&self, key: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        self.inner.presign_get(key, ttl).await
    }
}

impl Lockstep<'_> {
    /// Runs `f` to completion; returns its output and the rounds of reads
    /// it waited for.
    fn run<F: std::future::Future>(&self, f: F) -> (F::Output, usize) {
        let mut f = std::pin::pin!(f);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut rounds = 0;
        loop {
            if let std::task::Poll::Ready(v) = f.as_mut().poll(&mut cx) {
                return (v, rounds);
            }
            rounds += 1;
            assert!(rounds < 10_000, "no progress");
            self.round.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Stores a pack holding one chain of depth `depth` (each object a delta
/// of the one before) and returns its row ID and the chain's objects.
fn store_chain(
    depth: usize,
    id: i64,
    rows: &Rows,
    blobs: &Blobs,
    dir: &Path,
) -> (PackId, Vec<Obj>) {
    let objs = big_chain(depth, 1 << 10);
    let setup = StoredPacks::new(rows, blobs, LocateOptions::new(MAX_OBJECT));
    let (p, _) = push(&build_pack(&chain_specs(&objs)), &[], &setup, dir, depth);
    assert_eq!(p.max_delta_depth as usize, depth);
    (store(&p, id, rows, blobs), objs)
}

#[test]
fn prefetch_round_trips_do_not_grow_with_chain_depth() {
    // Invariant (monorepo benchmark, item 11): prefetching the top of a
    // delta chain costs the same round trips at depth 50, what `git gc` and
    // compaction write, as at depth 1, whether the pack's `.idx` is kept
    // whole (chain map in memory) or sampled (chain map on local disk).
    // Warm, a whole pack reads the chain in one round; a sampled one looks
    // the object up (one round) and then reads the chain (one more). Before
    // chain maps a chain cost one round per level: 51 at depth 50, about
    // 1 s on S3. A sampled pack without a chain directory still does.
    let dir = scratch();
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let lock = Lockstep {
        inner: &blobs,
        round: AtomicUsize::new(0),
    };
    let chains = dir.path().join("chains");
    let mut measured = Vec::new();
    for depth in [1, 10, 50] {
        let (id, objs) = store_chain(depth, depth as i64, &rows, &blobs, dir.path());
        let search = SearchPacks::new([id]);
        let top = objs[depth].id();
        for mode in ["whole", "sampled", "sampled without a chain map"] {
            let options = LocateOptions {
                object_cache_bytes: 0,
                max_whole_idx_bytes: if mode == "whole" { u64::MAX } else { 0 },
                ..LocateOptions::new(MAX_OBJECT)
            };
            let mut stored = StoredPacks::new(&rows, &lock, options);
            if mode == "sampled" {
                stored = stored.with_chain_dir(&chains);
            }
            let (cold, cold_rounds) = lock.run(stored.prefetch_bases(&search, &candidates(&[top])));
            let (warm, warm_rounds) = lock.run(stored.prefetch_bases(&search, &candidates(&[top])));
            for (pre, _) in [cold.unwrap(), warm.unwrap()] {
                let mut buf = Vec::new();
                pre.bases.find_base(&top, &mut buf).unwrap().unwrap();
                assert_eq!(buf, objs[depth].data);
            }
            eprintln!("depth {depth}, {mode}: {cold_rounds} rounds cold, {warm_rounds} warm");
            measured.push((depth, mode, cold_rounds, warm_rounds));
        }
    }
    for (depth, mode, cold, warm) in measured {
        match mode {
            "whole" => assert_eq!((cold, warm), (2, 1), "depth {depth}, {mode}"),
            "sampled" => assert_eq!(warm, 2, "depth {depth}, {mode}"),
            _ => assert_eq!(warm, depth + 2, "depth {depth}, {mode}"),
        }
    }
    // Nothing is left on disk once the locators are gone.
    assert_eq!(std::fs::read_dir(&chains).unwrap().count(), 0);
}

#[test]
fn a_lookup_probes_every_sampled_pack_in_one_round_trip() {
    // Invariant (item 11): a lookup in sampled packs costs one round trip
    // however many of them the search holds (it cost two per pack, in
    // turn), and the earliest pack in probe order still answers.
    let dir = scratch();
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let lock = Lockstep {
        inner: &blobs,
        round: AtomicUsize::new(0),
    };
    let mut ids = Vec::new();
    let mut tops = Vec::new();
    for depth in [2, 3, 4, 5] {
        let (id, objs) = store_chain(depth, depth as i64, &rows, &blobs, dir.path());
        ids.push(id);
        tops.push(objs[depth].id());
    }
    // The depth-2 pack again under another row, last in probe order: its
    // objects are in the first and the last pack.
    let (again_id, _) = store_chain(2, 99, &rows, &blobs, dir.path());
    ids.push(again_id);
    let sampled = StoredPacks::new(
        &rows,
        &lock,
        LocateOptions {
            max_whole_idx_bytes: 0,
            ..LocateOptions::new(MAX_OBJECT)
        },
    );
    let whole = StoredPacks::new(&rows, &blobs, LocateOptions::new(MAX_OBJECT));
    let search = SearchPacks::new(ids.iter().copied());
    let mut want = tops.clone();
    want.push(ObjectId::from_bytes([0x42; 20]));
    let want = SortedIds::from_unsorted(want);
    // Build the samples.
    let (first, _) = lock.run(sampled.locate(&search, &want));
    let (again, rounds) = lock.run(sampled.locate(&search, &want));
    assert_eq!(first.unwrap(), ready(whole.locate(&search, &want)).unwrap());
    let again = again.unwrap();
    assert_eq!(again, ready(whole.locate(&search, &want)).unwrap());
    assert_eq!(rounds, 1, "5 sampled packs, warm");
    let at = want
        .as_slice()
        .iter()
        .position(|id| *id == tops[0])
        .unwrap();
    assert_eq!(
        again[at].map(|l| l.pack),
        Some(ids[0]),
        "the first pack answers"
    );
    // In the other probe order, the other copy answers.
    let reversed = SearchPacks::new(ids.iter().rev().copied());
    let (got, _) = lock.run(sampled.locate(&reversed, &want));
    let got = got.unwrap();
    assert_eq!(got[at].map(|l| l.pack), Some(again_id));
    assert_eq!(got, ready(whole.locate(&reversed, &want)).unwrap());
}

#[test]
fn a_damaged_entry_table_never_gives_a_wrong_base() {
    // Invariant: a chain map only decides which entries are read up front;
    // each entry read is resolved by its own header. An entry table that
    // does not list the pack's entries (here, an entry's offset changed)
    // is corruption beside a whole `.idx`, which it is checked against;
    // beside a sampled one it can only cost reads, never a wrong object.
    let dir = scratch();
    let (rows, blobs) = (Rows::default(), Blobs::default());
    let (id, objs) = store_chain(3, 3, &rows, &blobs, dir.path());
    let key = rows.0.lock().unwrap()[&id].keys().entries;
    let record = aspen_core::pack::ENTRY_TABLE_HEADER_LEN + aspen_core::pack::ENTRY_RECORD_LEN;
    blobs.data.lock().unwrap().get_mut(&key).unwrap()[record + 7] ^= 1;
    let search = SearchPacks::new([id]);
    let top = objs[3].id();
    for max_whole_idx_bytes in [u64::MAX, 0] {
        let stored = StoredPacks::new(
            &rows,
            &blobs,
            LocateOptions {
                max_whole_idx_bytes,
                ..LocateOptions::new(MAX_OBJECT)
            },
        )
        .with_chain_dir(dir.path().join("chains"));
        match ready(stored.prefetch(&search, &candidates(&[top]))) {
            Err(e) => assert!(e.is_corruption(), "{e}"),
            Ok(pre) => {
                assert_eq!(max_whole_idx_bytes, 0, "a whole .idx checks its table");
                let mut buf = Vec::new();
                pre.bases.find_base(&top, &mut buf).unwrap().unwrap();
                assert_eq!(buf, objs[3].data);
            }
        }
    }
}
