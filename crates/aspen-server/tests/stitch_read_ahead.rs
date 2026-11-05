//! A stitched clone of many small packs keeps several blob reads in flight.
//!
//! Every blob read is a round trip (tens of milliseconds on S3). The public
//! `place` demo reached thousands of uncompacted one-stroke packs, and its
//! stitcher read each segment's `.idx`, entry table and pack one after
//! another: a full clone of a 12 MB repository took 253 s. Large covers
//! were read the same way, one range after another. These tests serve
//! clones from a blob store that records how many reads overlap, and
//! check that the stitcher reads ahead, across packs and within one.

use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aspen_auth::{AuthService, Limits, LimitsConfig, MemoryTokenStore, PolicyConfig};
use aspen_blob::LocalBlobStore;
use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use aspen_meta::SqlStore;
use aspen_pack::MaxObjectBytes;
use aspen_server::access::{Access, ClientAddr};
use aspen_server::testing::{Push, Seeder};
use aspen_server::{Server, ServerConfig};
use aspen_testkit::NO_AUTO_MAINTENANCE;

/// A local store whose reads take a few milliseconds and are counted.
#[derive(Debug, Clone)]
struct Slow {
    inner: LocalBlobStore,
    in_flight: Arc<AtomicUsize>,
    most: Arc<AtomicUsize>,
}

impl BlobStore for Slow {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        self.inner.upload_file(key, path).await
    }
    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(5)).await;
        let r = self.inner.read_range(key, range).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        r
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

fn git(home: &Path, dir: &Path) -> Command {
    let mut c = Command::new("git");
    c.current_dir(dir)
        .env("HOME", home)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_AUTHOR_NAME", "Aspen Test")
        .env("GIT_AUTHOR_EMAIL", "test@aspen.invalid")
        .env("GIT_COMMITTER_NAME", "Aspen Test")
        .env("GIT_COMMITTER_EMAIL", "test@aspen.invalid")
        .env_remove("GIT_DIR")
        .args(["-c", "protocol.version=2", "-c", "init.defaultBranch=main"])
        .args(NO_AUTO_MAINTENANCE);
    c
}

fn run(cmd: &mut Command) -> String {
    let out = cmd.output().expect("git runs");
    assert!(
        out.status.success(),
        "{cmd:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn a_clone_of_many_small_packs_reads_ahead() {
    let pushes = 48;
    let (most, stitched) = clone_reads(pushes, 1, None);
    assert_eq!(stitched, pushes as u64);
    assert!(
        most >= 8,
        "the stitcher read its {pushes} segments one at a time (at most {most} reads in flight)"
    );
}

#[test]
fn a_clone_of_one_large_pack_reads_ranges_ahead() {
    // About 320 KiB of incompressible blobs in one pack, read in 4 KiB
    // ranges.
    let (most, stitched) = clone_reads(1, 40, Some(4 << 10));
    assert_eq!(stitched, 1);
    assert!(
        most >= 8,
        "the stitcher read one pack's ranges one at a time (at most {most} reads in flight)"
    );
}

/// Seeds `pushes` single-commit pushes, each adding `files` files of 8 KiB
/// of incompressible bytes (or one small file when `files` is 1), serves
/// them with `stitch_read_bytes`, clones with stock git and checks the
/// clone; returns the most reads in flight during the clone and the packs
/// stitched.
fn clone_reads(pushes: usize, files: usize, stitch_read_bytes: Option<u64>) -> (usize, u64) {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join("home");
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&src).unwrap();
    run(git(&home, &src).args(["init", "--quiet", "-b", "main"]));
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut tips = Vec::new();
    for i in 0..pushes {
        if files == 1 {
            std::fs::write(src.join(format!("tile_{i}.txt")), format!("#{i:06x}\n")).unwrap();
        }
        for f in 0..files.max(2) - 1 {
            let bytes: Vec<u8> = (0..8 << 10)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect();
            std::fs::write(src.join(format!("blob_{i}_{f}.bin")), bytes).unwrap();
        }
        run(git(&home, &src).args(["add", "."]));
        run(git(&home, &src).args(["commit", "--quiet", "-m", &format!("stroke {i}")]));
        tips.push(
            run(git(&home, &src).args(["rev-parse", "HEAD"]))
                .trim()
                .to_owned(),
        );
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let url = format!("sqlite://{}?mode=rwc", tmp.path().join("meta.db").display());
    let meta = runtime.block_on(async {
        let meta = SqlStore::connect(&url).await.unwrap();
        meta.migrate().await.unwrap();
        meta
    });
    let blobs = Slow {
        inner: LocalBlobStore::open(tmp.path().join("blobs")).unwrap(),
        in_flight: Arc::default(),
        most: Arc::default(),
    };
    runtime.block_on(async {
        let seed = Seeder::create(&meta, &blobs, "demo", "refs/heads/main", &src)
            .await
            .unwrap();
        for tip in &tips {
            seed.push(&Push::new("refs/heads/main", tip)).await.unwrap();
        }
    });

    let mut config = ServerConfig::new(MaxObjectBytes::new(64 << 20).unwrap());
    if let Some(n) = stitch_read_bytes {
        config.stitch_read_bytes = n;
    }
    let server = Server::new(meta.clone(), blobs.clone(), config);
    let policy: PolicyConfig =
        serde_json::from_value(serde_json::json!({ "anonymous": ["read:*"] })).unwrap();
    let access = Arc::new(Access::new(
        AuthService::new(policy, MemoryTokenStore::new()).unwrap(),
        Limits::new(LimitsConfig::default()),
        meta.clone(),
        ClientAddr::Peer,
    ));
    let router = server.router(access);
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let addr = listener.local_addr().unwrap();
    runtime.spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    blobs.most.store(0, Ordering::SeqCst);
    let reader = tmp.path().join("reader");
    run(git(&home, tmp.path())
        .args(["clone", "--quiet", &format!("http://{addr}/demo.git")])
        .arg(&reader));
    assert_eq!(
        run(git(&home, &reader).args(["rev-parse", "HEAD"])).trim(),
        tips[pushes - 1]
    );
    run(git(&home, &reader).args(["fsck", "--strict", "--no-dangling"]));
    (
        blobs.most.load(Ordering::SeqCst),
        server.metrics().packs_stitched(),
    )
}
