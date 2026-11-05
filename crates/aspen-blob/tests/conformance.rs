//! One conformance suite for every blob store backend.
//!
//! Each check states the invariant it pins. The suite runs against:
//!
//! - [`LocalBlobStore`] in a temporary directory;
//! - [`ObjectBlobStore`] over `object_store`'s in-memory store, which
//!   exercises the wrapper's own logic (range mapping, idempotent uploads,
//!   multipart) without a network;
//! - [`ObjectBlobStore::s3`] against a real S3-compatible server, when
//!   `ASPEN_TEST_S3_ENDPOINT` is set (CI starts MinIO; see the crate
//!   README). Under CI (`CI` set) a missing endpoint fails the test rather
//!   than skipping it.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aspen_blob::object_store::aws::AmazonS3Builder;
use aspen_blob::object_store::memory::InMemory;
use aspen_blob::object_store::path::Path as ObjectPath;
use aspen_blob::object_store::signer::{Method, Signer, Url};
use aspen_blob::{
    AnyBlobStore, BlobStore, LocalBlobStore, LocalUrlSigner, ObjectBlobStore, STAGING_DIR,
    UploadOptions,
};
use aspen_core::blob::{BlobKey, ByteRange, PastEndRange};
use aspen_core::error::StoreError;

/// What a backend supports beyond the core contract, and how big a file
/// has to be to take the multipart path.
struct Backend<S> {
    store: S,
    /// A file this large is uploaded in several parts.
    multipart_len: usize,
    presign: Presign,
}

enum Presign {
    /// A [`StubSigner`]: check that the wrapper hands it the TTL and the
    /// prefixed path, and returns its URL.
    Stub,
    /// Fetch the URL over HTTP and check status codes and bodies.
    Http,
    /// Verify the URL with the local signer (no HTTP server in this crate).
    Local(LocalUrlSigner),
}

/// Deterministic bytes, distinct per `seed`, so a read of the wrong blob or
/// the wrong offset cannot pass by accident.
fn pattern(len: usize, seed: u8) -> Vec<u8> {
    let mut x = 0x9e37_79b9_u32 ^ u32::from(seed);
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

fn write(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, bytes).unwrap();
    path
}

fn range(start: u64, end: u64) -> ByteRange {
    ByteRange::new(start, end).unwrap()
}

fn is_corrupt(r: Result<Vec<u8>, StoreError>) -> bool {
    matches!(r, Err(StoreError::Corrupt { .. }))
}

/// A key namespace unique to this run, so runs can share a bucket.
fn namespace(backend: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("conformance/{backend}-{}-{nanos}", std::process::id())
}

async fn conformance<S: BlobStore>(b: &Backend<S>, ns: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let key = |name: &str| BlobKey::new(format!("{ns}/{name}")).unwrap();
    let s = &b.store;

    // Invariant: an uploaded blob reads back byte for byte, and its size and
    // receipt are the file's length.
    let small = pattern(1000, 1);
    let k = key("small.pack");
    let receipt = s
        .upload_file(&k, &write(tmp.path(), "small", &small))
        .await
        .unwrap();
    assert_eq!(receipt.size, 1000);
    assert_eq!(s.size(&k).await.unwrap(), Some(1000));
    assert_eq!(s.read_range(&k, range(0, 1000)).await.unwrap(), small);

    // Invariant: `read_range` returns exactly the asked bytes inside the
    // blob, clamps a range that crosses the end, returns nothing for a range
    // at or past the end, and never returns more than asked.
    assert_eq!(
        s.read_range(&k, range(10, 20)).await.unwrap(),
        small[10..20]
    );
    assert_eq!(
        s.read_range(&k, range(999, 1000)).await.unwrap(),
        small[999..]
    );
    assert_eq!(
        s.read_range(&k, range(990, 5000)).await.unwrap(),
        small[990..]
    );
    assert_eq!(
        s.read_range(&k, range(0, u64::from(u32::MAX)))
            .await
            .unwrap(),
        small
    );
    assert!(
        s.read_range(&k, range(1000, 1001))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        s.read_range(&k, range(5000, 6000))
            .await
            .unwrap()
            .is_empty()
    );

    // Invariant (N1 review carry-over): `read_exact_range` returns exactly
    // the asked bytes, and any short read, crossing the end or starting at
    // or past it, is `Corrupt` (which pages), never a short `Ok`.
    assert_eq!(s.read_exact_range(&k, range(0, 1000)).await.unwrap(), small);
    assert_eq!(
        s.read_exact_range(&k, range(500, 501)).await.unwrap(),
        small[500..501]
    );
    assert!(is_corrupt(s.read_exact_range(&k, range(999, 1001)).await));
    assert!(is_corrupt(s.read_exact_range(&k, range(1000, 1001)).await));
    assert!(is_corrupt(s.read_exact_range(&k, range(4000, 4100)).await));

    // Invariant: a missing blob has no size and reads as `NotFound` (which
    // pages: a `packs` row points at it), through both read methods.
    let missing = key("missing.pack");
    assert_eq!(s.size(&missing).await.unwrap(), None);
    let err = s.read_range(&missing, range(0, 1)).await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }), "{err:?}");
    let err = s.read_exact_range(&missing, range(5, 6)).await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }), "{err:?}");
    assert!(err.is_corruption());

    // Invariant: uploading the same bytes to the same key again succeeds
    // (retries are idempotent) and the blob is unchanged.
    let again = s
        .upload_file(&k, &write(tmp.path(), "again", &small))
        .await
        .unwrap();
    assert_eq!(again.size, 1000);
    assert_eq!(s.read_range(&k, range(0, 1000)).await.unwrap(), small);

    // Invariant: a key is written once. Uploading a different size to an
    // existing key is `Conflict`, and the stored blob is untouched.
    let longer = pattern(1001, 2);
    let err = s
        .upload_file(&k, &write(tmp.path(), "longer", &longer))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }), "{err:?}");
    assert_eq!(s.read_range(&k, range(0, 2000)).await.unwrap(), small);

    // Invariant: a failed upload (the source is missing) is a local error
    // and leaves no blob behind.
    let ghost = key("ghost.pack");
    let err = s
        .upload_file(&ghost, &tmp.path().join("does-not-exist"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::LocalIo { .. }), "{err:?}");
    assert_eq!(s.size(&ghost).await.unwrap(), None);

    // Invariant: an empty blob round-trips; every range of it is empty, so
    // every exact read is `Corrupt`.
    let empty = key("empty.entries");
    assert_eq!(
        s.upload_file(&empty, &write(tmp.path(), "empty", &[]))
            .await
            .unwrap()
            .size,
        0
    );
    assert_eq!(s.size(&empty).await.unwrap(), Some(0));
    assert!(s.read_range(&empty, range(0, 1)).await.unwrap().is_empty());
    assert!(is_corrupt(s.read_exact_range(&empty, range(0, 1)).await));

    // Invariant: a file larger than one part (the multipart path, with an
    // unaligned last part) reads back byte for byte, including ranges that
    // cross part boundaries.
    let big = pattern(b.multipart_len, 3);
    let len = big.len() as u64;
    let kb = key("big.pack");
    let receipt = s
        .upload_file(&kb, &write(tmp.path(), "big", &big))
        .await
        .unwrap();
    assert_eq!(receipt.size, len);
    assert_eq!(s.size(&kb).await.unwrap(), Some(len));
    for (a, z) in [
        (0, len),
        (len / 3 - 7, len / 3 + 7),
        (len - 1, len),
        (len / 2, len + 100),
    ] {
        let got = s.read_range(&kb, range(a, z)).await.unwrap();
        assert!(
            got == big[a as usize..z.min(len) as usize],
            "range {a}..{z}"
        );
    }
    assert!(is_corrupt(
        s.read_exact_range(&kb, range(len - 1, len + 1)).await
    ));

    // Invariant: concurrent uploads of the same bytes to one key all
    // succeed and leave exactly those bytes.
    let race = pattern(4096, 4);
    let kr = key("race.pack");
    let paths: Vec<_> = (0..4)
        .map(|i| write(tmp.path(), &format!("race{i}"), &race))
        .collect();
    let results = futures_join_all(paths.iter().map(|p| s.upload_file(&kr, p))).await;
    for r in results {
        assert_eq!(r.unwrap().size, 4096);
    }
    assert_eq!(s.read_range(&kr, range(0, 4096)).await.unwrap(), race);

    delete(s, &key, tmp.path()).await;
    presign(b, &k, &small).await;
}

/// `BlobStore::delete`, which garbage collection calls after it deleted the
/// blobs' `packs` row.
async fn delete<S: BlobStore>(s: &S, key: &impl Fn(&str) -> BlobKey, tmp: &Path) {
    let bytes = pattern(700, 5);
    let k = key("gone.pack");
    s.upload_file(&k, &write(tmp, "gone", &bytes))
        .await
        .unwrap();

    // Invariant: a deleted blob is gone: no size, and reads are `NotFound`.
    s.delete(&k).await.unwrap();
    assert_eq!(s.size(&k).await.unwrap(), None);
    let err = s.read_range(&k, range(0, 1)).await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }), "{err:?}");

    // Invariant: deleting is idempotent: a blob that is already gone, or
    // never existed, deletes successfully.
    s.delete(&k).await.unwrap();
    s.delete(&key("never-uploaded.pack")).await.unwrap();

    // Invariant: a key can be uploaded again after its delete, with any
    // size (nothing of the old blob is left to conflict with), and reads
    // back the new bytes.
    let other = pattern(900, 6);
    let receipt = s
        .upload_file(&k, &write(tmp, "gone-again", &other))
        .await
        .unwrap();
    assert_eq!(receipt.size, 900);
    assert_eq!(s.read_range(&k, range(0, 2000)).await.unwrap(), other);

    // Invariant: deleting a key that is a prefix of other blobs' keys (a
    // directory on a local disk) deletes nothing else.
    let inner = key("dir/inner.pack");
    s.upload_file(&inner, &write(tmp, "inner", &bytes))
        .await
        .unwrap();
    s.delete(&key("dir")).await.unwrap();
    assert_eq!(s.size(&inner).await.unwrap(), Some(700));
    assert_eq!(s.read_range(&inner, range(0, 700)).await.unwrap(), bytes);
    // Nor does deleting a key that is a prefix of another key's name.
    s.delete(&key("dir/inner")).await.unwrap();
    assert_eq!(s.size(&inner).await.unwrap(), Some(700));
    s.delete(&inner).await.unwrap();
    assert_eq!(s.size(&inner).await.unwrap(), None);
}

async fn presign<S: BlobStore>(b: &Backend<S>, key: &BlobKey, bytes: &[u8]) {
    let ttl = Duration::from_secs(300);
    // Invariant: every backend refuses a presign TTL outside (0, 7 days]
    // with `Unavailable`, so the fetch path falls back to stitching rather
    // than handing out a dead or overlong URL. Every backend here can
    // presign, so the refusal comes from the TTL rule (the reason says so),
    // not from a missing signer.
    for bad in [
        Duration::ZERO,
        aspen_blob::S3_MAX_PRESIGN_TTL + Duration::from_secs(1),
    ] {
        match b.store.presign_get(key, bad).await {
            Err(StoreError::Unavailable { reason }) => {
                assert!(reason.contains("presign TTL"), "{bad:?}: {reason}");
            }
            other => panic!("{bad:?}: {other:?}"),
        }
    }
    assert_eq!(
        aspen_blob::LOCAL_MAX_PRESIGN_TTL,
        aspen_blob::S3_MAX_PRESIGN_TTL
    );
    match &b.presign {
        Presign::Stub => {
            let max = b
                .store
                .presign_get(key, aspen_blob::S3_MAX_PRESIGN_TTL)
                .await
                .unwrap();
            assert_eq!(max.ttl, aspen_blob::S3_MAX_PRESIGN_TTL);
            let url = b.store.presign_get(key, ttl).await.unwrap();
            assert_eq!(url.ttl, ttl);
            assert_eq!(url.past_end, PastEndRange::Empty206);
            assert_eq!(
                url.url,
                format!("https://stub.invalid/tenant/a/{key}?method=GET&expires_in=300")
            );
        }
        Presign::Local(signer) => {
            // Invariant: the URL verifies to its key and promises the
            // empty-206 past-end rule that Aspen's server implements.
            let url = b.store.presign_get(key, ttl).await.unwrap();
            assert_eq!(url.ttl, ttl);
            assert_eq!(url.past_end, PastEndRange::Empty206);
            let rest = url
                .url
                .strip_prefix("http://127.0.0.1:7070/-/blobs/")
                .unwrap();
            let (path, query) = rest.split_once('?').unwrap();
            assert_eq!(
                signer.verify(path, query, SystemTime::now()),
                Ok(key.clone())
            );
            let later = SystemTime::now() + ttl + Duration::from_secs(2);
            assert!(signer.verify(path, query, later).is_err());
        }
        Presign::Http => {
            // Invariant: the URL is direct (no redirect), self-authorizing
            // (no credentials sent), honours `Range`, and answers a range
            // past the end as `past_end` says (S3 rules, fetch path step 3).
            let url = b.store.presign_get(key, ttl).await.unwrap();
            assert_eq!(url.past_end, PastEndRange::Status416);
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap();
            let full = client.get(&url.url).send().await.unwrap();
            assert_eq!(full.status(), 200);
            assert_eq!(full.bytes().await.unwrap(), bytes);
            let part = client
                .get(&url.url)
                .header("Range", "bytes=10-19")
                .send()
                .await
                .unwrap();
            assert_eq!(part.status(), 206);
            assert_eq!(part.bytes().await.unwrap(), bytes[10..20]);
            let past = client
                .get(&url.url)
                .header("Range", format!("bytes={}-", bytes.len()))
                .send()
                .await
                .unwrap();
            assert_eq!(past.status(), 416);
            // A URL for another key with the same signature is refused.
            let forged = url.url.replace("small.pack", "big.pack");
            let refused = client.get(&forged).send().await.unwrap();
            assert_eq!(refused.status(), 403);
        }
    }
}

/// Runs futures concurrently on this task (no extra dependency).
async fn futures_join_all<F: std::future::Future>(
    futures: impl Iterator<Item = F>,
) -> Vec<F::Output> {
    let handles: Vec<_> = futures.map(Box::pin).collect();
    let mut outputs: Vec<Option<F::Output>> = handles.iter().map(|_| None).collect();
    let mut handles: Vec<_> = handles.into_iter().map(Some).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (h, out) in handles.iter_mut().zip(outputs.iter_mut()) {
            if let Some(f) = h {
                match f.as_mut().poll(cx) {
                    std::task::Poll::Ready(v) => {
                        *out = Some(v);
                        *h = None;
                    }
                    std::task::Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;
    outputs.into_iter().map(Option::unwrap).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn local_store_conforms() {
    let dir = tempfile::tempdir().unwrap();
    let signer = LocalUrlSigner::new("http://127.0.0.1:7070/-/blobs", &[9; 32]).unwrap();
    let store = LocalBlobStore::open(dir.path().join("blobs"))
        .unwrap()
        .with_url_signer(signer.clone());
    let backend = Backend {
        store: AnyBlobStore::Local(store.clone()),
        multipart_len: 3 * 1024 * 1024 + 17,
        presign: Presign::Local(signer),
    };
    conformance(&backend, &namespace("local")).await;

    // Local-only invariants: uploads leave nothing in staging, blobs are
    // plain files at `<root>/<key>`, and a key that is a directory (a
    // prefix of other keys) is not a blob.
    let staging: Vec<_> = std::fs::read_dir(store.root().join(STAGING_DIR))
        .unwrap()
        .collect();
    assert!(staging.is_empty(), "{staging:?}");
    let prefix = BlobKey::new("conformance").unwrap();
    assert!(store.path_of(&prefix).is_dir());
    assert_eq!(store.size(&prefix).await.unwrap(), None);
    let err = store.read_range(&prefix, range(0, 1)).await.unwrap_err();
    assert!(matches!(err, StoreError::NotFound { .. }), "{err:?}");
    let tmp = tempfile::tempdir().unwrap();
    let err = store
        .upload_file(&prefix, &write(tmp.path(), "x", b"x"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict { .. }), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn local_store_without_signer_does_not_presign() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalBlobStore::open(dir.path()).unwrap();
    let key = BlobKey::new("k").unwrap();
    let err = store
        .presign_get(&key, Duration::from_secs(1))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Unavailable { .. }), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn object_store_wrapper_conforms_in_memory() {
    let upload = UploadOptions {
        part_size: 64 * 1024,
        concurrency: 3,
    };
    let store = ObjectBlobStore::new(Arc::new(InMemory::new()))
        .with_prefix("tenant/a")
        .unwrap()
        .with_upload_options(upload)
        .unwrap()
        .with_signer(
            Arc::new(StubSigner),
            PastEndRange::Empty206,
            aspen_blob::S3_MAX_PRESIGN_TTL,
        );
    let backend = Backend {
        store: AnyBlobStore::Object(store),
        multipart_len: 5 * 64 * 1024 + 123,
        presign: Presign::Stub,
    };
    conformance(&backend, &namespace("memory")).await;
}

/// A signer that encodes what it was asked to sign into the URL, so the
/// in-memory backend presigns and reaches the TTL rule.
#[derive(Debug)]
struct StubSigner;

#[async_trait::async_trait]
impl Signer for StubSigner {
    async fn signed_url(
        &self,
        method: Method,
        path: &ObjectPath,
        expires_in: Duration,
    ) -> aspen_blob::object_store::Result<Url> {
        Ok(Url::parse(&format!(
            "https://stub.invalid/{path}?method={method}&expires_in={}",
            expires_in.as_secs()
        ))
        .unwrap())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn object_store_without_signer_does_not_presign() {
    let store = ObjectBlobStore::new(Arc::new(InMemory::new()));
    let key = BlobKey::new("k").unwrap();
    match store.presign_get(&key, Duration::from_secs(1)).await {
        Err(StoreError::Unavailable { reason }) => {
            assert!(reason.contains("does not presign"), "{reason}");
        }
        other => panic!("{other:?}"),
    }
}

/// Connection settings for a test S3 server, from `ASPEN_TEST_S3_*`.
fn s3_builder() -> Option<AmazonS3Builder> {
    let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let Some(endpoint) = var("ASPEN_TEST_S3_ENDPOINT") else {
        assert!(
            std::env::var_os("CI").is_none(),
            "ASPEN_TEST_S3_ENDPOINT is not set; CI must run the S3 conformance suite"
        );
        eprintln!("skipping S3 conformance: set ASPEN_TEST_S3_ENDPOINT (crate README)");
        return None;
    };
    let need = |name: &str| var(name).unwrap_or_else(|| panic!("{name} is not set"));
    Some(
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_region(var("ASPEN_TEST_S3_REGION").unwrap_or_else(|| "us-east-1".into()))
            .with_bucket_name(need("ASPEN_TEST_S3_BUCKET"))
            .with_access_key_id(need("ASPEN_TEST_S3_ACCESS_KEY_ID"))
            .with_secret_access_key(need("ASPEN_TEST_S3_SECRET_ACCESS_KEY")),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_store_conforms() {
    let Some(builder) = s3_builder() else { return };
    // S3's smallest part, so the multipart path runs with three parts.
    let upload = UploadOptions {
        part_size: aspen_blob::S3_MIN_PART_SIZE,
        concurrency: 2,
    };
    let store = ObjectBlobStore::s3(builder, upload).unwrap();
    let backend = Backend {
        store: AnyBlobStore::Object(store),
        multipart_len: 2 * aspen_blob::S3_MIN_PART_SIZE + 4321,
        presign: Presign::Http,
    };
    conformance(&backend, &namespace("s3")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_store_honours_a_prefix() {
    let Some(builder) = s3_builder() else { return };
    let prefix = namespace("s3-prefix");
    let prefixed = ObjectBlobStore::s3(builder.clone(), UploadOptions::default())
        .unwrap()
        .with_prefix(&prefix)
        .unwrap();
    let plain = ObjectBlobStore::s3(builder, UploadOptions::default()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let key = BlobKey::new("repos/1/packs/p.pack").unwrap();
    prefixed
        .upload_file(&key, &write(tmp.path(), "p", b"prefixed"))
        .await
        .unwrap();
    // Invariant: a prefixed store's key `k` is the object `<prefix>/k`, and
    // nothing else.
    assert_eq!(prefixed.size(&key).await.unwrap(), Some(8));
    assert_eq!(plain.size(&key).await.unwrap(), None);
    let located = BlobKey::new(format!("{prefix}/{key}")).unwrap();
    assert_eq!(plain.size(&located).await.unwrap(), Some(8));
    assert_eq!(
        plain.read_range(&located, range(0, 8)).await.unwrap(),
        b"prefixed"
    );
}

/// Invariant: an S3 store's part size stays within S3's 5 MiB to 5 GiB,
/// whether set when the store is built or later with
/// `with_upload_options`; other stores take any positive size.
#[test]
fn s3_part_sizes_stay_within_s3_limits() {
    let builder = AmazonS3Builder::new()
        .with_bucket_name("b")
        .with_region("us-east-1");
    let part = |part_size| UploadOptions {
        part_size,
        concurrency: 1,
    };
    let min = aspen_blob::S3_MIN_PART_SIZE;
    assert!(ObjectBlobStore::s3(builder.clone(), part(min - 1)).is_err());
    let s3 = ObjectBlobStore::s3(builder, part(min)).unwrap();
    assert!(s3.clone().with_upload_options(part(min - 1)).is_err());
    assert!(s3.clone().with_upload_options(part(min)).is_ok());
    #[cfg(target_pointer_width = "64")]
    {
        let max = usize::try_from(aspen_blob::S3_MAX_PART_SIZE).unwrap();
        assert!(s3.clone().with_upload_options(part(max)).is_ok());
        assert!(s3.clone().with_upload_options(part(max + 1)).is_err());
    }
    let zero_concurrency = UploadOptions {
        part_size: min,
        concurrency: 0,
    };
    assert!(s3.with_upload_options(zero_concurrency).is_err());

    let memory = ObjectBlobStore::new(Arc::new(InMemory::new()));
    assert!(memory.clone().with_upload_options(part(1)).is_ok());
    assert!(memory.with_upload_options(part(0)).is_err());
}
