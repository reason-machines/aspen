//! The regional pack cache: whole packs on local disk, filled from the blob
//! store, evicted least recently used to stay within a byte budget.
//!
//! **Only verified bytes are cached.** A fill streams the blob from the
//! store into a staging file while hashing it, and publishes the file only
//! if the SHA-1 of everything before the last 20 bytes equals those 20
//! bytes (Git's pack trailer) **and** the checksum in the pack's key. A
//! blob that does not match its key is never cached and counts as a
//! corruption alert, so the cache cannot make one bad read permanent, and
//! the bytes served for a key are the bytes the key names.
//!
//! Fills are single-flight: concurrent requests for a pack that is not
//! cached share one fill. The staging file is `fsync`ed before it is
//! renamed into place, so a crash leaves either a complete, verified pack
//! or a staging file, and staging files are deleted at startup.
//!
//! **Fills are bounded** (#123). At most [`CacheConfig::max_fills`] run at
//! once, and a fill reserves its pack's size against
//! [`CacheConfig::max_bytes`] before it copies a byte, evicting least
//! recently used packs to make room. The budget therefore covers staging
//! files as well as published packs, so the cache directory never holds
//! more than `max_bytes` however many packs are asked for at once. A miss
//! that finds the cap reached, or the budget taken by fills in progress,
//! starts no fill and is served from the blob store.
//!
//! One cache directory belongs to one process. Eviction unlinks files; a
//! response already reading an evicted pack keeps its open file.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use aspen_core::blob::{BlobStore, ByteRange};
use sha1::{Digest, Sha1};
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;

use crate::key::PackKey;
use crate::metrics::ExpressMetrics;

const DATA_DIR: &str = "data";
const STAGING_DIR: &str = "staging";
/// A pack's 12-byte header plus its 20-byte trailer.
const MIN_PACK: u64 = 32;

/// Where the cache lives and how large it may grow.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// The cache directory, on a local disk. Created if missing.
    pub dir: PathBuf,
    /// Most bytes of packs kept. A pack larger than this is never cached.
    pub max_bytes: u64,
    /// Most fills that may run at once.
    pub max_fills: usize,
}

impl CacheConfig {
    /// Fills that may run at once by default.
    pub const DEFAULT_MAX_FILLS: usize = 4;
}

#[derive(Debug)]
struct Entry {
    size: u64,
    last_used: u64,
}

#[derive(Debug, Default)]
struct State {
    entries: HashMap<PackKey, Entry>,
    used: u64,
    /// Bytes reserved by fills in progress (their staging files).
    reserved: u64,
    tick: u64,
    fills: HashMap<PackKey, watch::Receiver<Option<bool>>>,
}

/// The disk cache.
#[derive(Debug)]
pub(crate) struct PackCache {
    dir: PathBuf,
    max_bytes: u64,
    max_fills: usize,
    state: Mutex<State>,
    metrics: Arc<ExpressMetrics>,
}

impl PackCache {
    /// Opens the cache: deletes leftover staging files and indexes the
    /// packs already on disk (oldest use first, since their use times are
    /// unknown), evicting down to the budget.
    pub(crate) fn open(config: &CacheConfig, metrics: Arc<ExpressMetrics>) -> io::Result<Self> {
        let staging = config.dir.join(STAGING_DIR);
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        fs::create_dir_all(&staging)?;
        let data = config.dir.join(DATA_DIR);
        fs::create_dir_all(&data)?;
        let cache = Self {
            dir: config.dir.clone(),
            max_bytes: config.max_bytes,
            max_fills: config.max_fills,
            state: Mutex::new(State::default()),
            metrics,
        };
        let mut found = Vec::new();
        scan(&data, &data, &mut found)?;
        {
            let mut st = cache.lock();
            for (key, size) in found {
                st.used += size;
                st.entries.insert(key, Entry { size, last_used: 0 });
            }
            cache.evict_to(&mut st, 0);
        }
        Ok(cache)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn path(&self, key: &PackKey) -> PathBuf {
        let mut p = self.dir.join(DATA_DIR);
        p.extend(key.blob_key().as_str().split('/'));
        p
    }

    /// Most bytes the cache keeps.
    pub(crate) fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// The cached file and size of `key`, marking it used.
    pub(crate) fn get(&self, key: &PackKey) -> Option<(PathBuf, u64)> {
        let mut st = self.lock();
        st.tick += 1;
        let tick = st.tick;
        let e = st.entries.get_mut(key)?;
        e.last_used = tick;
        Some((self.path(key), e.size))
    }

    /// Starts a fill of `key` (`size` bytes) from `blobs`, or joins the one
    /// running. The receiver turns `Some(true)` once the pack is cached,
    /// `Some(false)` if the fill failed. It is `Some(false)` at once when
    /// no fill could start: [`CacheConfig::max_fills`] are running, or
    /// the fills in progress leave less than `size` of the budget.
    pub(crate) fn fill<B>(
        self: &Arc<Self>,
        key: &PackKey,
        size: u64,
        blobs: &B,
        read_bytes: u64,
    ) -> watch::Receiver<Option<bool>>
    where
        B: BlobStore + Clone + 'static,
    {
        let mut st = self.lock();
        if let Some(rx) = st.fills.get(key) {
            return rx.clone();
        }
        let (tx, rx) = watch::channel(None);
        if st.entries.contains_key(key) {
            let _ = tx.send(Some(true));
            return rx;
        }
        if st.fills.len() >= self.max_fills || !self.reserve(&mut st, size) {
            drop(st);
            self.metrics.fill_skipped();
            let _ = tx.send(Some(false));
            return rx;
        }
        st.fills.insert(key.clone(), rx.clone());
        drop(st);
        let blobs = blobs.clone();
        let mut done = FillDone {
            cache: Arc::clone(self),
            key: key.clone(),
            reserved: size,
            tx,
            ok: false,
        };
        tokio::spawn(async move {
            let result = done
                .cache
                .copy(
                    &done.key,
                    size,
                    &blobs,
                    read_bytes.max(1),
                    &mut done.reserved,
                )
                .await;
            let (cache, key) = (&done.cache, &done.key);
            let ok = match result {
                Ok(()) => true,
                Err(FillError::Corrupt) => {
                    cache.metrics.corrupt();
                    eprintln!(
                        "aspen: express: stored pack {} does not hash to its checksum; not cached",
                        key.blob_key().as_str()
                    );
                    false
                }
                Err(FillError::Other(e)) => {
                    eprintln!(
                        "aspen: express: cannot cache {}: {e}",
                        key.blob_key().as_str()
                    );
                    false
                }
            };
            done.finish(ok);
        });
        rx
    }

    /// Reserves `size` bytes of the budget for a fill, evicting least
    /// recently used packs to make room. Refuses, evicting nothing, when
    /// the fills already in progress leave less than `size`.
    fn reserve(&self, st: &mut State, size: u64) -> bool {
        if st.reserved.saturating_add(size) > self.max_bytes {
            return false;
        }
        self.evict_to(st, size);
        st.reserved += size;
        true
    }

    /// Copies `key` into staging, verifies it and publishes it. `reserved`
    /// is the fill's reservation; publishing moves it into `used` and sets
    /// it to 0, otherwise the fill's [`FillDone`] releases it.
    async fn copy<B: BlobStore>(
        &self,
        key: &PackKey,
        size: u64,
        blobs: &B,
        read_bytes: u64,
        reserved: &mut u64,
    ) -> Result<(), FillError> {
        if size < MIN_PACK {
            return Err(FillError::Corrupt);
        }
        let staged = tempfile::Builder::new()
            .prefix("fill-")
            .tempfile_in(self.dir.join(STAGING_DIR))?;
        let mut file = tokio::fs::File::from_std(staged.reopen()?);
        let hash_end = size - 20;
        let mut hasher = Sha1::new();
        let mut trailer = Vec::with_capacity(20);
        let mut at = 0;
        while at < size {
            let end = (at + read_bytes).min(size);
            let bytes = blobs
                .read_exact_range(key.blob_key(), ByteRange::new(at, end).expect("at < end"))
                .await
                .map_err(|e| FillError::Other(e.to_string()))?;
            self.metrics.origin(bytes.len() as u64);
            let split = usize::try_from(hash_end.saturating_sub(at))
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            hasher.update(&bytes[..split]);
            trailer.extend_from_slice(&bytes[split..]);
            file.write_all(&bytes).await?;
            at = end;
        }
        let digest = hasher.finalize();
        if digest.as_slice() != trailer.as_slice()
            || trailer.as_slice() != key.checksum().as_bytes()
        {
            return Err(FillError::Corrupt);
        }
        file.sync_all().await?;
        drop(file);

        let dest = self.path(key);
        let mut st = self.lock();
        fs::create_dir_all(dest.parent().expect("a key path has a parent"))?;
        staged.persist(&dest).map_err(|e| e.error)?;
        st.tick += 1;
        let tick = st.tick;
        st.reserved -= std::mem::take(reserved);
        st.used += size;
        st.entries.insert(
            key.clone(),
            Entry {
                size,
                last_used: tick,
            },
        );
        self.metrics.set_cached_bytes(st.used);
        Ok(())
    }

    /// Evicts least recently used packs until `incoming` more bytes fit
    /// beside the published packs and the fills in progress.
    fn evict_to(&self, st: &mut State, incoming: u64) {
        while st.used + st.reserved + incoming > self.max_bytes {
            let Some(victim) = st
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            let e = st.entries.remove(&victim).expect("just found");
            st.used -= e.size;
            match fs::remove_file(self.path(&victim)) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => eprintln!(
                    "aspen: express: cannot evict {}: {err}",
                    victim.blob_key().as_str()
                ),
            }
            self.metrics.evicted();
        }
        self.metrics.set_cached_bytes(st.used);
    }
}

/// Ends a fill however its task ends: counts it, releases what is left of
/// its reservation, removes its single-flight entry and wakes the waiters
/// with `Some(ok)`. Running this on drop means a fill whose task panics
/// (or is cancelled) fails like any other fill and gives its bytes back, and
/// the next request starts a new one; without it the entry would stay and
/// every later request would join a fill that never finishes, so the pack
/// could not be cached again until restart.
struct FillDone {
    cache: Arc<PackCache>,
    key: PackKey,
    /// Bytes of the budget the fill still holds.
    reserved: u64,
    tx: watch::Sender<Option<bool>>,
    ok: bool,
}

impl FillDone {
    /// Records how the fill ended; the drop that follows reports it.
    fn finish(&mut self, ok: bool) {
        self.ok = ok;
    }
}

impl Drop for FillDone {
    fn drop(&mut self) {
        self.cache.metrics.fill(self.ok);
        {
            let mut st = self.cache.lock();
            st.reserved -= self.reserved;
            st.fills.remove(&self.key);
        }
        let _ = self.tx.send(Some(self.ok));
    }
}

#[derive(Debug, thiserror::Error)]
enum FillError {
    #[error("the blob does not hash to the checksum in its key")]
    Corrupt,
    #[error("{0}")]
    Other(String),
}

impl From<io::Error> for FillError {
    fn from(e: io::Error) -> Self {
        Self::Other(e.to_string())
    }
}

/// Collects the packs under `data`; deletes any file that is not a
/// canonical pack key (nothing else is ever written there).
fn scan(data: &Path, dir: &Path, out: &mut Vec<(PackKey, u64)>) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let ty = entry.file_type()?;
        if ty.is_dir() {
            scan(data, &path, out)?;
            continue;
        }
        let key = path
            .strip_prefix(data)
            .ok()
            .and_then(Path::to_str)
            .and_then(PackKey::parse);
        match key {
            Some(key) if ty.is_file() => out.push((key, entry.metadata()?.len())),
            _ => fs::remove_file(&path)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use aspen_core::blob::{BlobKey, PresignedUrl, UploadReceipt};
    use aspen_core::error::StoreError;
    use aspen_core::id::{PackChecksum, RepoId};

    use super::*;

    /// One pack in memory; the first read panics, later reads succeed.
    #[derive(Clone)]
    struct PanicsOnce {
        bytes: Arc<Vec<u8>>,
        panicked: Arc<AtomicBool>,
    }

    impl BlobStore for PanicsOnce {
        async fn upload_file(&self, _: &BlobKey, _: &Path) -> Result<UploadReceipt, StoreError> {
            unreachable!("not used")
        }

        async fn read_range(&self, _: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
            assert!(
                self.panicked.swap(true, Ordering::SeqCst),
                "deliberate panic in the first read of a fill"
            );
            let len = self.bytes.len() as u64;
            let (start, end) = (range.start().min(len), range.end().min(len));
            Ok(self.bytes[start as usize..end as usize].to_vec())
        }

        async fn size(&self, _: &BlobKey) -> Result<Option<u64>, StoreError> {
            Ok(Some(self.bytes.len() as u64))
        }

        async fn delete(&self, _: &BlobKey) -> Result<(), StoreError> {
            unreachable!("not used")
        }

        async fn presign_get(&self, _: &BlobKey, _: Duration) -> Result<PresignedUrl, StoreError> {
            unreachable!("not used")
        }
    }

    /// Packs in memory. Every read of a pack's last chunk waits until the
    /// test opens the gate, so each fill that started holds all but its
    /// last chunk in staging, on disk, while the test looks.
    #[derive(Clone)]
    struct HoldsLastChunk {
        packs: Arc<HashMap<BlobKey, Vec<u8>>>,
        open: watch::Receiver<bool>,
        held: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl BlobStore for HoldsLastChunk {
        async fn upload_file(&self, _: &BlobKey, _: &Path) -> Result<UploadReceipt, StoreError> {
            unreachable!("not used")
        }

        async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
            let bytes = &self.packs[key];
            let len = bytes.len() as u64;
            if range.end() >= len {
                self.held.fetch_add(1, Ordering::SeqCst);
                let _ = self.open.clone().wait_for(|open| *open).await;
            }
            let (start, end) = (range.start().min(len), range.end().min(len));
            Ok(bytes[start as usize..end as usize].to_vec())
        }

        async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
            Ok(self.packs.get(key).map(|b| b.len() as u64))
        }

        async fn delete(&self, _: &BlobKey) -> Result<(), StoreError> {
            unreachable!("not used")
        }

        async fn presign_get(&self, _: &BlobKey, _: Duration) -> Result<PresignedUrl, StoreError> {
            unreachable!("not used")
        }
    }

    /// `n` distinct packs of `payload` bytes each (plus header and trailer).
    fn packs(n: u8, payload: usize) -> Vec<(PackKey, Vec<u8>)> {
        (0..n)
            .map(|seed| {
                let mut b = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
                b.extend((0..payload).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)));
                let sum: [u8; 20] = Sha1::digest(&b).into();
                b.extend_from_slice(&sum);
                let key = PackKey::new(
                    RepoId::new(1).unwrap(),
                    PackChecksum::from_bytes(sum),
                    aspen_core::pack::PackGeneration::FIRST,
                );
                (key, b)
            })
            .collect()
    }

    /// Bytes of every file under `dir`: published packs and staging files.
    fn disk_bytes(dir: &Path) -> u64 {
        fs::read_dir(dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    disk_bytes(&e.path())
                } else {
                    e.metadata().unwrap().len()
                }
            })
            .sum()
    }

    struct Burst {
        /// Fills still running when every started fill was held.
        running: usize,
        /// Most bytes the cache directory held at that moment.
        disk: u64,
        /// Cached packs once every fill ended, each checked byte for byte.
        cached: usize,
        metrics: String,
    }

    /// Caches the first `warm` packs, then asks for every other pack at
    /// once (the misses a reader can cause with one `packfile-uris`
    /// response), holds every fill that started just before its last
    /// chunk, measures, then lets them finish.
    async fn burst(
        max_bytes: u64,
        max_fills: usize,
        warm: usize,
        packs: Vec<(PackKey, Vec<u8>)>,
    ) -> Burst {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(ExpressMetrics::default());
        let config = CacheConfig {
            dir: dir.path().to_owned(),
            max_bytes,
            max_fills,
        };
        let cache = Arc::new(PackCache::open(&config, Arc::clone(&metrics)).unwrap());
        let (open_tx, open) = watch::channel(false);
        let store = HoldsLastChunk {
            packs: Arc::new(
                packs
                    .iter()
                    .map(|(k, b)| (k.blob_key().clone(), b.clone()))
                    .collect(),
            ),
            open,
            held: Arc::default(),
        };
        open_tx.send(true).unwrap();
        for (k, b) in &packs[..warm] {
            let mut rx = cache.fill(k, b.len() as u64, &store, 100);
            assert_eq!(*rx.wait_for(Option::is_some).await.unwrap(), Some(true));
        }
        open_tx.send(false).unwrap();
        store.held.store(0, Ordering::SeqCst);
        let mut rxs: Vec<_> = packs[warm..]
            .iter()
            .map(|(k, b)| cache.fill(k, b.len() as u64, &store, 100))
            .collect();
        let running = rxs.iter().filter(|rx| rx.borrow().is_none()).count();
        for _ in 0..1000 {
            if store.held.load(Ordering::SeqCst) == running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            store.held.load(Ordering::SeqCst),
            running,
            "every fill held"
        );
        let disk = disk_bytes(dir.path());
        open_tx.send(true).unwrap();
        for rx in &mut rxs {
            rx.wait_for(Option::is_some).await.unwrap();
        }
        let mut cached = 0;
        for (k, b) in &packs {
            if let Some((path, size)) = cache.get(k) {
                assert_eq!((fs::read(path).unwrap(), size), (b.clone(), b.len() as u64));
                cached += 1;
            }
        }
        assert!(disk_bytes(dir.path()) <= max_bytes, "within budget at rest");
        assert!(cache.lock().fills.is_empty(), "no fill left behind");
        let mut text = String::new();
        metrics.render(&mut text);
        Burst {
            running,
            disk,
            cached,
            metrics: text,
        }
    }

    /// Invariant (#123): the cache's byte budget covers fills in progress,
    /// not only published packs. A reader asking for many large packs at
    /// once cannot make the cache directory (published packs plus staging
    /// files) hold more than `max_bytes`: a fill reserves its pack's size
    /// before it copies, evicting published packs to make room, and a miss
    /// that cannot reserve starts no fill (it is served from the blob
    /// store) and says so at once. The cache starts with two published
    /// packs, so eviction has to make room for the reservations. Before
    /// the fix every miss started a fill that wrote up to its whole pack
    /// into staging (10 KB against the 2.6 KB budget without the warm
    /// packs), and eviction waited until a fill had finished.
    #[tokio::test(flavor = "multi_thread")]
    async fn fills_in_progress_count_against_the_byte_budget() {
        let b = burst(2600, 16, 2, packs(10, 1000)).await;
        assert!(
            b.disk <= 2600,
            "{} bytes on disk for a 2600-byte budget",
            b.disk
        );
        assert_eq!(b.running, 2, "two 1,032-byte packs fit in 2,600 bytes");
        assert_eq!(b.cached, 2, "the two warm packs made room for them");
        assert!(
            b.metrics.contains("aspen_express_cache_fills_total 4\n"),
            "{}",
            b.metrics
        );
        assert!(
            b.metrics
                .contains("aspen_express_cache_fills_skipped_total 6\n"),
            "{}",
            b.metrics
        );
        assert!(
            b.metrics
                .contains("aspen_express_cache_fill_failures_total 0\n"),
            "a skipped fill is not a failure: {}",
            b.metrics
        );
    }

    /// Invariant (#123): at most `max_fills` fills run at once, however
    /// much room the budget leaves; a miss past the cap starts no fill and
    /// is served from the blob store. Before the fix every miss started a
    /// fill of its own.
    #[tokio::test(flavor = "multi_thread")]
    async fn at_most_max_fills_run_at_once() {
        let b = burst(1 << 20, 3, 0, packs(10, 1000)).await;
        assert_eq!(b.running, 3);
        assert_eq!(b.cached, 3);
        assert!(
            b.metrics
                .contains("aspen_express_cache_fills_skipped_total 7\n"),
            "{}",
            b.metrics
        );
    }

    /// Invariant: a fill whose task panics fails like any other fill. Its
    /// waiters hear `Some(false)`, the single-flight entry is gone, and the
    /// next request starts a fresh fill that caches the pack (before the
    /// drop guard, the entry stayed and the pack could not be cached until
    /// restart).
    #[tokio::test]
    async fn a_panicked_fill_does_not_wedge_its_pack() {
        let dir = tempfile::tempdir().unwrap();
        let metrics = Arc::new(ExpressMetrics::default());
        let config = CacheConfig {
            dir: dir.path().to_owned(),
            max_bytes: 1 << 20,
            max_fills: CacheConfig::DEFAULT_MAX_FILLS,
        };
        let cache = Arc::new(PackCache::open(&config, Arc::clone(&metrics)).unwrap());
        let mut bytes = b"PACK\0\0\0\x02\0\0\0\0".to_vec();
        let sum: [u8; 20] = Sha1::digest(&bytes).into();
        bytes.extend_from_slice(&sum);
        let key = PackKey::new(
            RepoId::new(1).unwrap(),
            PackChecksum::from_bytes(sum),
            aspen_core::pack::PackGeneration::FIRST,
        );
        let store = PanicsOnce {
            bytes: Arc::new(bytes.clone()),
            panicked: Arc::new(AtomicBool::new(false)),
        };
        let size = bytes.len() as u64;

        let mut first = cache.fill(&key, size, &store, 1000);
        let first = *first.wait_for(Option::is_some).await.unwrap();
        assert_eq!(first, Some(false), "the panicked fill reports failure");
        assert!(cache.lock().fills.is_empty(), "its entry is removed");

        let mut second = cache.fill(&key, size, &store, 1000);
        let second = *second.wait_for(Option::is_some).await.unwrap();
        assert_eq!(second, Some(true), "a new fill runs and succeeds");
        let (path, cached) = cache.get(&key).expect("cached");
        assert_eq!((std::fs::read(path).unwrap(), cached), (bytes, size));
        let mut text = String::new();
        metrics.render(&mut text);
        assert!(text.contains("aspen_express_cache_fills_total 1"), "{text}");
        assert!(
            text.contains("aspen_express_cache_fill_failures_total 1"),
            "{text}"
        );
    }
}
