//! Failure handling in [`ObjectBlobStore`], checked by wrapping an
//! in-memory store in one that fails chosen operations.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use aspen_blob::object_store::memory::InMemory;
use aspen_blob::object_store::path::Path;
use aspen_blob::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use aspen_blob::{BlobStore, ObjectBlobStore, UploadOptions};
use aspen_core::blob::{BlobKey, ByteRange};
use aspen_core::error::StoreError;
use futures_util::stream::BoxStream;

/// Which operations fail, and how many multipart uploads were aborted.
#[derive(Debug, Default)]
struct Faults {
    /// Ranged GETs fail (HEADs still work), the way a network error or a
    /// `416` looks to the caller.
    ranged_get: AtomicBool,
    /// Every part upload fails.
    put_part: AtomicBool,
    /// Completing a multipart upload fails.
    complete: AtomicBool,
    aborted: AtomicUsize,
    /// Create-only PUTs are answered `NotImplemented`, as by a store
    /// without conditional writes.
    no_create: AtomicBool,
    /// Requests of each kind that reached the store.
    heads: AtomicUsize,
    puts: AtomicUsize,
}

#[derive(Debug)]
struct Faulty {
    inner: InMemory,
    faults: Arc<Faults>,
}

fn injected() -> aspen_blob::object_store::Error {
    aspen_blob::object_store::Error::Generic {
        store: "faulty",
        source: "injected failure".into(),
    }
}

impl fmt::Display for Faulty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Faulty({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Faulty {
    async fn put_opts(&self, p: &Path, payload: PutPayload, o: PutOptions) -> Result<PutResult> {
        if self.faults.no_create.load(Ordering::SeqCst)
            && matches!(o.mode, aspen_blob::object_store::PutMode::Create)
        {
            return Err(aspen_blob::object_store::Error::NotImplemented {
                operation: "create-only put".into(),
                implementer: "faulty".into(),
            });
        }
        self.faults.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put_opts(p, payload, o).await
    }

    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(FaultyUpload {
            inner: self.inner.put_multipart_opts(p, o).await?,
            faults: self.faults.clone(),
        }))
    }

    async fn get_opts(&self, p: &Path, o: GetOptions) -> Result<GetResult> {
        if o.range.is_some() && !o.head && self.faults.ranged_get.load(Ordering::SeqCst) {
            return Err(injected());
        }
        if o.head {
            self.faults.heads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(p, o).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, o: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, o).await
    }
}

#[derive(Debug)]
struct FaultyUpload {
    inner: Box<dyn MultipartUpload>,
    faults: Arc<Faults>,
}

#[async_trait::async_trait]
impl MultipartUpload for FaultyUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.faults.put_part.load(Ordering::SeqCst) {
            return Box::pin(async { Err(injected()) });
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.faults.complete.load(Ordering::SeqCst) {
            return Err(injected());
        }
        self.inner.complete().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.faults.aborted.fetch_add(1, Ordering::SeqCst);
        self.inner.abort().await
    }
}

const PART: usize = 64 * 1024;

fn store() -> (ObjectBlobStore, Arc<Faults>) {
    let faults = Arc::new(Faults::default());
    let store = ObjectBlobStore::new(Arc::new(Faulty {
        inner: InMemory::new(),
        faults: faults.clone(),
    }))
    .with_upload_options(UploadOptions {
        part_size: PART,
        concurrency: 2,
    })
    .unwrap();
    (store, faults)
}

fn range(start: u64, end: u64) -> ByteRange {
    ByteRange::new(start, end).unwrap()
}

/// Invariant: a ranged read that fails inside the blob is `Unavailable`
/// (retryable), never an empty or short `Ok` and never `NotFound`; the
/// same failure for a range at or past the end is the empty read the
/// contract promises (that is how stores spell a `416`).
#[tokio::test]
async fn a_failed_read_inside_the_blob_is_unavailable() {
    let (store, faults) = store();
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("small");
    std::fs::write(&src, vec![7u8; 100]).unwrap();
    let key = BlobKey::new("repos/1/packs/a.pack").unwrap();
    store.upload_file(&key, &src).await.unwrap();

    faults.ranged_get.store(true, Ordering::SeqCst);
    for r in [range(0, 100), range(50, 60), range(99, 200)] {
        let got = store.read_range(&key, r).await;
        assert!(
            matches!(got, Err(StoreError::Unavailable { .. })),
            "{r:?}: {got:?}"
        );
        let exact = store.read_exact_range(&key, r).await;
        assert!(
            matches!(exact, Err(StoreError::Unavailable { .. })),
            "{r:?}: {exact:?}"
        );
    }
    assert!(
        store
            .read_range(&key, range(100, 101))
            .await
            .unwrap()
            .is_empty()
    );
}

/// Invariant: a multipart upload that fails, whether a part or the
/// completion fails, is aborted (its parts stop being billed), reports a
/// retryable error, and leaves no blob.
#[tokio::test]
async fn a_failed_multipart_upload_is_aborted() {
    let tmp = tempfile::tempdir().unwrap();
    let src = tmp.path().join("big");
    std::fs::write(&src, vec![3u8; 3 * PART + 5]).unwrap();
    for fault in ["put_part", "complete"] {
        let (store, faults) = store();
        match fault {
            "put_part" => faults.put_part.store(true, Ordering::SeqCst),
            _ => faults.complete.store(true, Ordering::SeqCst),
        }
        let key = BlobKey::new("repos/1/packs/big.pack").unwrap();
        let err = store.upload_file(&key, &src).await.unwrap_err();
        assert!(
            matches!(err, StoreError::Unavailable { .. }),
            "{fault}: {err:?}"
        );
        assert_eq!(faults.aborted.load(Ordering::SeqCst), 1, "{fault}");
        assert_eq!(store.size(&key).await.unwrap(), None, "{fault}");
    }
}

/// A small upload is one create-only PUT, with no HEAD first (each push
/// uploads three blobs, so a HEAD per blob was a round trip on every push).
/// An existing blob is still checked: the same size is a success, another
/// size an error, and a store without conditional creates gets HEAD then
/// PUT as before.
#[tokio::test]
async fn a_small_upload_is_one_create_only_put() {
    let (store, faults) = store();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("blob");
    std::fs::write(&file, vec![7u8; 1000]).unwrap();
    let key = BlobKey::new("repos/1/packs/a.pack").unwrap();
    assert_eq!(store.upload_file(&key, &file).await.unwrap().size, 1000);
    assert_eq!(faults.puts.load(Ordering::SeqCst), 1);
    assert_eq!(
        faults.heads.load(Ordering::SeqCst),
        0,
        "no HEAD before the PUT"
    );
    // Again: refused by the store, checked with a HEAD, the same size.
    assert_eq!(store.upload_file(&key, &file).await.unwrap().size, 1000);
    assert_eq!(faults.heads.load(Ordering::SeqCst), 1);
    std::fs::write(&file, vec![7u8; 999]).unwrap();
    assert!(
        store.upload_file(&key, &file).await.is_err(),
        "another size is an error"
    );
    // Without conditional creates: HEAD, then a plain PUT.
    faults.no_create.store(true, Ordering::SeqCst);
    let other = BlobKey::new("repos/1/packs/b.pack").unwrap();
    let puts = faults.puts.load(Ordering::SeqCst);
    assert_eq!(store.upload_file(&other, &file).await.unwrap().size, 999);
    assert_eq!(faults.puts.load(Ordering::SeqCst), puts + 1);
    let mut read = Vec::new();
    read.extend(store.read_exact_range(&other, range(0, 999)).await.unwrap());
    assert_eq!(read, vec![7u8; 999]);
}
