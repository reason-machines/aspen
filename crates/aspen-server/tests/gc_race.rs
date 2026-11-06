//! Garbage collection of orphaned packs against concurrent writers (issue
//! #48), end to end: raw receive-pack requests (so that two pushes send
//! byte-identical packs, and the same checksum), the compactor's sweep and
//! blob deletions, and stock `git clone` + `fsck --strict` of the result.
//!
//! The store runs on a fake clock, and a blob store wrapper can hold
//! uploads (after the bytes are written) and deletions (before they start),
//! so each race is made deterministic:
//!
//! - a blob deletion that runs long after its sweep, past the grace period,
//!   while the same checksum is pushed and committed again;
//! - a sweep that runs while a push is uploading;
//! - a push whose freshly registered pack is swept, and registered again by
//!   someone else at the next generation, before the push commits;
//! - a compaction that reuses a stored pack whose writer died before
//!   uploading, with a sweep while it uploads.

mod receive_support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use aspen_blob::LocalBlobStore;
use aspen_compact::{CompactConfig, Compactor, JobOutcome, Worker, WorkerOptions};
use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use aspen_core::id::{ObjectId, UnixMicros};
use aspen_core::meta::{CoverLayout, GC_LEASE, JobKind, MetaStore, NewJob};
use aspen_core::name::{RefName, RepoName};
use aspen_core::pack::{PackGeneration, PackKeys, PackRecord};
use aspen_core::repo::{ForcePolicy, Repo, RepoSettings};
use aspen_meta::{Clock, SqlStore};
use aspen_pack::{MaxInflatedBytes, MaxObjectBytes};
use tokio::sync::watch;

use receive_support::*;

/// The grace period the sweeps use.
const GRACE: Duration = Duration::from_secs(3600);
const GRACE_US: i64 = 3_600_000_000;

/// Holds every caller of [`Gate::pass`] while armed, until released.
struct Gate {
    armed: AtomicBool,
    reached: AtomicUsize,
    open: watch::Sender<bool>,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
            reached: AtomicUsize::new(0),
            open: watch::channel(false).0,
        })
    }

    fn arm(&self) {
        self.open.send_replace(false);
        self.reached.store(0, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn pass(&self) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.reached.fetch_add(1, Ordering::SeqCst);
        let mut rx = self.open.subscribe();
        let _ = rx.wait_for(|open| *open).await;
    }

    fn release(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.open.send_replace(true);
    }

    /// Whether a caller reached the gate within `within`.
    async fn reached(&self, within: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            if self.reached.load(Ordering::SeqCst) > 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }
}

/// A local blob store whose uploads can be held after their bytes are
/// durable, and whose deletions can be held before they start.
#[derive(Clone)]
struct Gated {
    inner: LocalBlobStore,
    after_upload: Arc<Gate>,
    before_delete: Arc<Gate>,
}

impl Gated {
    fn new(inner: LocalBlobStore) -> Self {
        Self {
            inner,
            after_upload: Gate::new(),
            before_delete: Gate::new(),
        }
    }
}

impl BlobStore for Gated {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        let r = self.inner.upload_file(key, path).await;
        self.after_upload.pass().await;
        r
    }
    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        self.inner.read_range(key, range).await
    }
    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        self.inner.size(key).await
    }
    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        self.before_delete.pass().await;
        self.inner.delete(key).await
    }
    async fn presign_get(&self, key: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        self.inner.presign_get(key, ttl).await
    }
}

/// A server on a fake clock, serving `demo` over HTTP.
struct Env {
    dir: tempfile::TempDir,
    home: std::path::PathBuf,
    meta: SqlStore,
    local: LocalBlobStore,
    /// The server's blob store.
    blobs: Gated,
    repo: Repo,
    url: String,
    /// Added to the system clock, in microseconds.
    offset: Arc<AtomicI64>,
}

impl Env {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let offset = Arc::new(AtomicI64::new(0));
        let clock = {
            let offset = offset.clone();
            Clock::from_fn(move || UnixMicros(UnixMicros::now().0 + offset.load(Ordering::SeqCst)))
        };
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
        let meta = SqlStore::connect(&url).await.unwrap().with_clock(clock);
        meta.migrate().await.unwrap();
        let local = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
        let blobs = Gated::new(local.clone());
        let mut cfg = config(dir.path());
        cfg.cover_layout = CoverLayout::new(4).unwrap();
        let server = aspen_server::Server::new(
            meta.clone(),
            blobs.clone(),
            aspen_server::ServerConfig::new(MaxObjectBytes::new(64 * MIB).unwrap()),
        )
        .with_receive_pack(cfg)
        .unwrap();
        let main = RefName::new("refs/heads/main").unwrap();
        let settings = RepoSettings::builder(
            main.clone(),
            RepoSettings::new(main).unwrap().writable().to_vec(),
        )
        .allow_force(ForcePolicy::Branches)
        .build()
        .unwrap();
        let repo = meta
            .create_repo(&RepoName::new("demo").unwrap(), &settings)
            .await
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let policy: aspen_auth::PolicyConfig =
            serde_json::from_value(serde_json::json!({ "anonymous": ["write:demo"] })).unwrap();
        let access = Arc::new(aspen_server::access::Access::new(
            aspen_auth::AuthService::new(policy, aspen_auth::MemoryTokenStore::new()).unwrap(),
            aspen_auth::Limits::new(aspen_auth::LimitsConfig::default()),
            meta.clone(),
            aspen_server::access::ClientAddr::Peer,
        ));
        let app = server.router(access);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let home = dir.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        Self {
            home,
            meta,
            local,
            blobs,
            repo,
            url: format!("http://{addr}/demo.git"),
            offset,
            dir,
        }
    }

    fn advance(&self, us: i64) {
        self.offset.fetch_add(us, Ordering::SeqCst);
    }

    /// A compactor on the same store and clock, over `blobs`.
    fn compactor<B: BlobStore + Clone>(&self, blobs: B) -> Compactor<SqlStore, B> {
        let mut config = CompactConfig::new(
            self.dir.path().join("compact"),
            MaxObjectBytes::new(64 * MIB).unwrap(),
            MaxInflatedBytes::new(1024 * MIB).unwrap(),
        );
        config.layout = CoverLayout::new(4).unwrap();
        config.gc_grace = GRACE;
        std::fs::create_dir_all(&config.spool_dir).unwrap();
        Compactor::new(self.meta.clone(), blobs, config)
    }

    /// Registers and uploads `pack` as another writer would (register,
    /// then upload), without referencing it.
    async fn store_unreferenced(&self, pack: &[u8]) -> PackRecord {
        use aspen_pack::ingest::{CompleteOptions, complete, receive};
        let work = tempfile::tempdir().unwrap();
        let limits = aspen_core::pack::PushLimits {
            max_objects: 1_000,
            max_pack_bytes: 64 * MIB,
        };
        let received = receive(&mut &pack[..], &work.path().join("spool"), &limits).unwrap();
        let done = complete(
            &received,
            &aspen_core::ingest::InMemoryBases::default(),
            work.path(),
            &CompleteOptions::new(
                MaxObjectBytes::new(64 * MIB).unwrap(),
                MaxInflatedBytes::new(1024 * MIB).unwrap(),
            ),
        )
        .unwrap();
        let record = self
            .meta
            .register_pack(&done.new_pack(self.repo.id))
            .await
            .unwrap();
        let keys = record.keys();
        self.local
            .upload_file(&keys.pack, &done.pack_path)
            .await
            .unwrap();
        self.local
            .upload_file(&keys.idx, &done.idx_path)
            .await
            .unwrap();
        self.local
            .upload_file(&keys.entries, &done.entries_path)
            .await
            .unwrap();
        record
    }

    fn exists(&self, keys: &PackKeys) -> [bool; 3] {
        [&keys.pack, &keys.idx, &keys.entries].map(|k| self.local.path_of(k).exists())
    }

    /// The pack row `main`'s current version names.
    async fn main_pack(&self) -> PackRecord {
        let v = self
            .meta
            .current_version(self.repo.id, &RefName::new("refs/heads/main").unwrap())
            .await
            .unwrap()
            .expect("main exists");
        let id = v.value.pack().expect("main has a pack");
        self.meta.packs(&[id]).await.unwrap().remove(0)
    }

    /// A stock `git clone` of the repository passes `fsck --strict` and
    /// checks out `tip`.
    async fn clone_is_complete(&self, name: &str, tip: ObjectId) {
        let (home, dir, url, name) = (
            self.home.clone(),
            self.dir.path().to_owned(),
            self.url.clone(),
            name.to_owned(),
        );
        blocking(move || {
            git_ok(&home, &dir, &["clone", "-q", &url, &name]);
            let c = dir.join(&name);
            git_ok(&home, &c, &["fsck", "--strict", "--no-dangling"]);
            let head = git_ok(&home, &c, &["rev-parse", "HEAD"]);
            assert_eq!(oid(&head), tip);
        })
        .await;
    }
}

/// Commit `n` on `main` (parent `parent`), as one non-thin pack of its
/// commit, tree and blob; the same `n` and parent give the same bytes.
fn commit_pack(n: u32, parent: Option<ObjectId>) -> (ObjectId, Vec<u8>) {
    let blob = Obj::blob(format!("version {n}\n"));
    let tree = Obj::tree(&[("100644", "v.txt", blob.id())]);
    let parents: Vec<ObjectId> = parent.into_iter().collect();
    let commit = Obj::commit(
        tree.id(),
        &parents,
        &format!("c{n}"),
        1_700_000_000 + i64::from(n),
    );
    let pack = build_pack(&[
        Entry::Full(commit.clone()),
        Entry::Full(tree),
        Entry::Full(blob),
    ]);
    (commit.id(), pack)
}

/// Pushes `pack` moving `main` from `old` to `new`; the report lines.
async fn push(env: &Env, old: Option<ObjectId>, new: ObjectId, pack: &[u8]) -> Vec<String> {
    let old = old.unwrap_or(ObjectId::from_bytes([0; 20]));
    post(
        env.url.clone(),
        request(&[(old, new, "refs/heads/main")], &[], pack),
    )
    .await
}

fn accepted(report: &[String]) -> bool {
    report.iter().any(|l| l == "ok refs/heads/main")
}

/// Invariant (issue #48): a blob deletion started by a sweep can run any
/// time later, past the grace period, without deleting the blobs of a pack
/// that was pushed again (same checksum) and committed meanwhile: the new
/// row is the next generation, under keys no deletion names. The push is
/// not refused, and the clone afterwards is complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_late_blob_delete_never_removes_a_pack_pushed_again() {
    let env = Env::start().await;
    let (c1, pack) = commit_pack(1, None);
    // A pack a failed push left behind: registered, uploaded, unreferenced.
    let orphan = env.store_unreferenced(&pack).await;
    assert_eq!(orphan.generation, PackGeneration::FIRST);
    env.advance(GRACE_US + 1_000_000);

    // The sweep deletes the orphan's row; its first blob deletion is held.
    let gated = Gated::new(env.local.clone());
    gated.before_delete.arm();
    let gc = {
        let c = env.compactor(gated.clone());
        tokio::spawn(async move { c.collect_garbage().await })
    };
    assert!(gated.before_delete.reached(Duration::from_secs(10)).await);
    assert!(
        env.meta.packs(&[orphan.id]).await.is_err(),
        "the row is gone"
    );
    // Long past the grace period, the same pack is pushed and committed.
    env.advance(3 * GRACE_US);
    let report = push(&env, None, c1, &pack).await;
    assert!(accepted(&report), "{report:?}");
    let stored = env.main_pack().await;
    assert_eq!(stored.pack.checksum, orphan.pack.checksum, "the same pack");
    assert_eq!(stored.generation, PackGeneration::new(1));
    assert_eq!(env.exists(&stored.keys()), [true; 3]);

    // Now the held deletion runs, and finishes the tombstone.
    gated.before_delete.release();
    let report = gc.await.unwrap().unwrap();
    assert_eq!(report.packs_deleted, vec![orphan.id], "{report:?}");
    assert_eq!(report.deletes_finished, 1, "{report:?}");
    assert!(report.blobs_left.is_empty(), "{report:?}");
    assert_eq!(env.exists(&orphan.keys()), [false; 3], "the orphan's blobs");
    assert_eq!(
        env.exists(&stored.keys()),
        [true; 3],
        "the pushed pack's blobs"
    );
    assert!(env.meta.pending_pack_deletes(10).await.unwrap().is_empty());
    env.clone_is_complete("after", c1).await;
}

/// Invariant (issue #48): a sweep that runs while a push is uploading never
/// deletes what the push uploads. The push registers before it uploads, so
/// an old orphan with the same bytes is registered again (its time
/// refreshed) and not swept; nothing is deleted, and the clone is complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sweep_during_a_push_upload_never_deletes_its_blobs() {
    let env = Env::start().await;
    let (c1, pack) = commit_pack(1, None);
    let orphan = env.store_unreferenced(&pack).await;
    env.advance(GRACE_US + 1_000_000);

    env.blobs.after_upload.arm();
    let pushing = {
        let (url, pack) = (env.url.clone(), pack.clone());
        tokio::spawn(async move {
            post(
                url,
                request(
                    &[(ObjectId::from_bytes([0; 20]), c1, "refs/heads/main")],
                    &[],
                    &pack,
                ),
            )
            .await
        })
    };
    assert!(
        env.blobs
            .after_upload
            .reached(Duration::from_secs(10))
            .await
    );
    // A whole garbage-collection pass while the push's upload is held.
    let report = env
        .compactor(env.local.clone())
        .collect_garbage()
        .await
        .unwrap();
    assert!(report.packs_deleted.is_empty(), "{report:?}");
    env.blobs.after_upload.release();
    let report = pushing.await.unwrap();
    assert!(accepted(&report), "{report:?}");
    let stored = env.main_pack().await;
    assert_eq!(stored.id, orphan.id, "the orphan's row, registered again");
    assert_eq!(env.exists(&stored.keys()), [true; 3]);
    env.clone_is_complete("after", c1).await;
}

/// Invariant (issue #48, coordinator condition 1): a push whose pack row is
/// swept between its registration and its commit, and registered again
/// by another writer at the next generation, fails retryably rather than
/// committing a reference to the old generation; the retried push commits
/// the new row, and the clone is complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_push_whose_pack_is_swept_before_its_commit_fails_retryably() {
    let env = Env::start().await;
    let (c1, pack) = commit_pack(1, None);

    env.blobs.after_upload.arm();
    let pushing = {
        let (url, pack) = (env.url.clone(), pack.clone());
        tokio::spawn(async move {
            post(
                url,
                request(
                    &[(ObjectId::from_bytes([0; 20]), c1, "refs/heads/main")],
                    &[],
                    &pack,
                ),
            )
            .await
        })
    };
    assert!(
        env.blobs
            .after_upload
            .reached(Duration::from_secs(10))
            .await
    );
    env.advance(GRACE_US + 1_000_000);
    let report = env
        .compactor(env.local.clone())
        .collect_garbage()
        .await
        .unwrap();
    assert_eq!(report.packs_deleted.len(), 1, "{report:?}");
    assert_eq!(report.deletes_finished, 1, "{report:?}");
    let again = env.store_unreferenced(&pack).await;
    assert_eq!(again.generation, PackGeneration::new(1));
    env.blobs.after_upload.release();

    let report = pushing.await.unwrap();
    assert!(!accepted(&report), "{report:?}");
    let text = report.join("\n");
    // Retryable: the client is told to retry, not that its push is bad.
    assert!(text.contains("retry"), "a retryable refusal: {text}");
    assert!(
        env.meta
            .current_version(env.repo.id, &RefName::new("refs/heads/main").unwrap())
            .await
            .unwrap()
            .is_none(),
        "nothing was committed"
    );

    let report = push(&env, None, c1, &pack).await;
    assert!(accepted(&report), "{report:?}");
    assert_eq!(env.main_pack().await.id, again.id);
    env.clone_is_complete("after", c1).await;
}

/// Invariant (issue #48, coordinator finding 1): a compaction whose cover
/// pack is already registered (a compactor that died before uploading left
/// the row and no blobs) goes through `register_pack`, so the row's time
/// is refreshed and a sweep while it uploads leaves it alone, and uploads
/// the blobs, so the cover it records is readable: the clone is complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_reusing_a_stored_pack_uploads_and_keeps_it() {
    let env = Env::start().await;
    let mut tip = None;
    for n in 1..=4 {
        let (c, pack) = commit_pack(n, tip);
        let report = push(&env, tip, c, &pack).await;
        assert!(accepted(&report), "{report:?}");
        tip = Some(c);
    }
    let tip = tip.unwrap();
    let worker = |blobs: Gated| Worker::new(env.compactor(blobs), WorkerOptions::new("test"));
    let mut w = worker(Gated::new(env.local.clone()));
    match w.run_one().await.unwrap() {
        Some(JobOutcome::Done { .. }) => {}
        other => panic!("the compact job: {other:?}"),
    }
    let main = RefName::new("refs/heads/main").unwrap();
    let cover = env.meta.covers(env.repo.id, &main).await.unwrap().remove(0);
    let cover_pack = env
        .meta
        .packs(&[cover.pack.unwrap()])
        .await
        .unwrap()
        .remove(0);

    // As if the compactor had registered the pack and died before it
    // uploaded and recorded it: the cover has no pack and the row no blobs.
    sqlx::query("UPDATE covers SET pack_id = NULL WHERE id = $1")
        .bind(cover.id.get())
        .execute(env.meta.pool())
        .await
        .unwrap();
    for key in [
        &cover_pack.keys().pack,
        &cover_pack.keys().idx,
        &cover_pack.keys().entries,
    ] {
        std::fs::remove_file(env.local.path_of(key)).unwrap();
    }
    env.advance(GRACE_US + 1_000_000);
    let version = env
        .meta
        .current_version(env.repo.id, &main)
        .await
        .unwrap()
        .unwrap();
    env.meta
        .enqueue_jobs(&[NewJob {
            repo: env.repo.id,
            kind: JobKind::Compact {
                version: version.id,
            },
            run_after: UnixMicros(0),
        }])
        .await
        .unwrap();

    let gated = Gated::new(env.local.clone());
    gated.after_upload.arm();
    let compacting = {
        let mut w = worker(gated.clone());
        tokio::spawn(async move { w.run_one().await })
    };
    // The reused row must be uploaded again: its upload is held here.
    assert!(
        gated.after_upload.reached(Duration::from_secs(10)).await,
        "the compactor uploads the reused row's blobs"
    );
    let report = env
        .compactor(env.local.clone())
        .collect_garbage()
        .await
        .unwrap();
    assert!(report.packs_deleted.is_empty(), "{report:?}");
    gated.after_upload.release();
    match compacting.await.unwrap().unwrap() {
        Some(JobOutcome::Done { .. }) => {}
        other => panic!("the second compact job: {other:?}"),
    }
    let cover = env.meta.covers(env.repo.id, &main).await.unwrap().remove(0);
    assert_eq!(cover.pack, Some(cover_pack.id), "the stored pack is reused");
    assert_eq!(env.exists(&cover_pack.keys()), [true; 3], "and uploaded");
    env.clone_is_complete("after", tip).await;
}

/// Invariant (issue #48, coordinator re-review 3): the compactor and the
/// commit indexer read a source pack under its row's generation. Here the
/// first version's pack is generation 1 (its checksum was swept and its
/// generation-0 blobs deleted), so a reader that derived keys from the
/// checksum alone would find nothing; both jobs succeed, and the clone of
/// the cover is complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_and_indexing_read_a_source_pack_by_its_generation() {
    let env = Env::start().await;
    let (_, pack) = commit_pack(1, None);
    let orphan = env.store_unreferenced(&pack).await;
    env.advance(GRACE_US + 1_000_000);
    let report = env
        .compactor(env.local.clone())
        .collect_garbage()
        .await
        .unwrap();
    assert_eq!(report.packs_deleted, vec![orphan.id], "{report:?}");
    assert_eq!(env.exists(&orphan.keys()), [false; 3]);

    let main = RefName::new("refs/heads/main").unwrap();
    let mut tip = None;
    let mut first = None;
    for n in 1..=4 {
        let (c, pack) = commit_pack(n, tip);
        let report = push(&env, tip, c, &pack).await;
        assert!(accepted(&report), "{report:?}");
        if n == 1 {
            let stored = env.main_pack().await;
            assert_eq!(stored.pack.checksum, orphan.pack.checksum);
            assert_eq!(stored.generation, PackGeneration::new(1));
            first = Some(
                env.meta
                    .current_version(env.repo.id, &main)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        tip = Some(c);
    }
    let first = first.unwrap();

    // The indexer reads version 1's generation-1 pack.
    let indexed = env
        .compactor(env.local.clone())
        .index_version(first.id)
        .await
        .unwrap();
    assert_eq!(indexed.rows, 1, "c1, read from the pack: {indexed:?}");

    // The compactor reads it as one of the cover's sources.
    let mut w = Worker::new(env.compactor(env.local.clone()), WorkerOptions::new("test"));
    match w.run_one().await.unwrap() {
        Some(JobOutcome::Done {
            report: aspen_compact::JobReport::Compact(r),
            ..
        }) => assert_eq!(r.packs_written, 1, "{r:?}"),
        other => panic!("the compact job: {other:?}"),
    }
    let cover = env.meta.covers(env.repo.id, &main).await.unwrap().remove(0);
    assert_eq!(
        cover.range.first(),
        first.seq,
        "the cover starts at version 1"
    );
    assert!(cover.pack.is_some(), "{cover:?}");
    env.clone_is_complete("after", tip.unwrap()).await;
}

/// Invariant (issue #48): a worker that dies between its sweep and its
/// blob deletions leaves pending tombstones, and the next pass of any
/// worker deletes those blobs and finishes them. Throughput item 7: the
/// dead worker held the garbage-collection lease, so no other process
/// collects until that lease expires, and then the next one takes it over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crashed_workers_deletions_are_finished_by_the_next_pass() {
    let env = Env::start().await;
    let (_, pack) = commit_pack(1, None);
    let orphan = env.store_unreferenced(&pack).await;
    env.advance(GRACE_US + 1_000_000);
    // Another process takes the lease and sweeps; the sweep commits, then
    // that process dies before deleting anything.
    let ttl = Duration::from_secs(60);
    let lease = env
        .meta
        .acquire_lease(GC_LEASE, "crashed-process", ttl)
        .await
        .unwrap()
        .unwrap();
    let swept = env
        .meta
        .sweep_orphan_packs(&lease, GRACE, 100)
        .await
        .unwrap()
        .deleted;
    assert_eq!(swept, vec![orphan.clone()]);
    assert_eq!(env.exists(&orphan.keys()), [true; 3]);

    // Its lease is still live: this process's pass does nothing.
    env.advance(59_000_000);
    let report = env
        .compactor(env.local.clone())
        .collect_garbage()
        .await
        .unwrap();
    assert!(report.lease_held_elsewhere, "{report:?}");
    assert_eq!(report.deletes_finished, 0, "{report:?}");
    assert_eq!(env.exists(&orphan.keys()), [true; 3]);

    // The lease expires a minute after the dead process last extended it.
    env.advance(1_000_000);
    let report = env
        .compactor(env.local.clone())
        .collect_garbage()
        .await
        .unwrap();
    assert!(!report.lease_held_elsewhere, "{report:?}");
    assert!(report.packs_deleted.is_empty(), "{report:?}");
    assert_eq!(report.deletes_finished, 1, "{report:?}");
    assert_eq!(env.exists(&orphan.keys()), [false; 3]);
    assert!(env.meta.pending_pack_deletes(10).await.unwrap().is_empty());
}
