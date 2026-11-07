//! Requests to the blob store, counted by who asked ([`BlobCaller`]) and
//! by what they read ([`BlobFile`]).
//!
//! Object storage bills per request, and a store's latency is per request
//! too, so "which part of Aspen sends the GETs" is the first question when
//! a deployment's request rate is out of line with its push rate. Every
//! request through [`crate::AnyBlobStore`] (what `aspen serve` uses) is
//! counted in the process-wide [`RequestCounts::global`], which the server
//! exports at `/-/metrics` as `aspen_blob_*` (`docs/OBSERVABILITY.md`).
//! [`Counted`] counts any other [`BlobStore`] into counts of its own, for
//! tests and tools.
//!
//! What is counted is the [`BlobStore`] call, which is what a backend sends
//! in the usual case:
//!
//! | Call | Counter | S3 requests |
//! |---|---|---|
//! | [`BlobStore::read_range`] | `get` (and `get_bytes`, the bytes returned) | one `GET` with a `Range`; one more `HEAD` only when the `GET` fails |
//! | [`BlobStore::size`] | `head` | one `HEAD` |
//! | [`BlobStore::upload_file`] | `put` | one `PUT`; a multipart upload sends a `HEAD` and one request per part |
//! | [`BlobStore::delete`] | `delete` | one `DELETE` |
//!
//! Presigning sends nothing and is not counted. Counting is a few relaxed
//! atomic adds per request: no lock, no allocation.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aspen_core::blob::{
    BlobCaller, BlobFile, BlobKey, BlobStore, ByteRange, PresignedUrl, UploadReceipt,
};
use aspen_core::error::StoreError;

/// A kind of request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequestKind {
    /// A ranged read ([`BlobStore::read_range`]).
    Get,
    /// A size check ([`BlobStore::size`]).
    Head,
    /// An upload ([`BlobStore::upload_file`]).
    Put,
    /// A deletion ([`BlobStore::delete`]).
    Delete,
}

impl RequestKind {
    /// Every kind, in label order.
    pub const ALL: [Self; 4] = [Self::Get, Self::Head, Self::Put, Self::Delete];

    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }
}

const CALLERS: usize = BlobCaller::ALL.len();
const FILES: usize = BlobFile::ALL.len();
const KINDS: usize = RequestKind::ALL.len();

/// Request counters by caller, file and kind, plus the bytes ranged reads
/// returned. All monotonic.
#[derive(Debug)]
pub struct RequestCounts {
    requests: [[[AtomicU64; KINDS]; FILES]; CALLERS],
    get_bytes: [[AtomicU64; FILES]; CALLERS],
}

impl Default for RequestCounts {
    fn default() -> Self {
        Self {
            requests: std::array::from_fn(|_| {
                std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0)))
            }),
            get_bytes: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }
}

/// One row of [`RequestCounts::snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestCount {
    /// Who asked.
    pub caller: BlobCaller,
    /// What was read or written.
    pub file: BlobFile,
    /// The request.
    pub kind: RequestKind,
    /// Requests.
    pub requests: u64,
    /// Bytes returned (ranged reads only; zero for the other kinds).
    pub bytes: u64,
}

static GLOBAL: std::sync::OnceLock<Arc<RequestCounts>> = std::sync::OnceLock::new();

impl RequestCounts {
    /// The counts every [`crate::AnyBlobStore`] in this process records.
    pub fn global() -> &'static Arc<RequestCounts> {
        GLOBAL.get_or_init(Arc::default)
    }

    /// Counts one request by the current caller ([`BlobCaller::current`]).
    pub fn record(&self, key: &BlobKey, kind: RequestKind) {
        let (c, f) = (BlobCaller::current().index(), BlobFile::of(key).index());
        self.requests[c][f][kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Requests of `kind` by `caller`, every file.
    pub fn requests(&self, caller: BlobCaller, kind: RequestKind) -> u64 {
        self.requests[caller.index()]
            .iter()
            .map(|by_kind| by_kind[kind as usize].load(Ordering::Relaxed))
            .sum()
    }

    /// Every non-zero counter.
    pub fn snapshot(&self) -> Vec<RequestCount> {
        let mut rows = Vec::new();
        for caller in BlobCaller::ALL {
            for file in BlobFile::ALL {
                for kind in RequestKind::ALL {
                    let requests = self.requests[caller.index()][file.index()][kind as usize]
                        .load(Ordering::Relaxed);
                    if requests == 0 {
                        continue;
                    }
                    let bytes = if kind == RequestKind::Get {
                        self.get_bytes[caller.index()][file.index()].load(Ordering::Relaxed)
                    } else {
                        0
                    };
                    rows.push(RequestCount {
                        caller,
                        file,
                        kind,
                        requests,
                        bytes,
                    });
                }
            }
        }
        rows
    }

    /// Counts a ranged read of `key` around `read`.
    pub(crate) async fn get<F>(&self, key: &BlobKey, read: F) -> Result<Vec<u8>, StoreError>
    where
        F: Future<Output = Result<Vec<u8>, StoreError>>,
    {
        // The caller is read before the first await: the read may finish on
        // another thread.
        let (c, f) = (BlobCaller::current().index(), BlobFile::of(key).index());
        self.requests[c][f][RequestKind::Get as usize].fetch_add(1, Ordering::Relaxed);
        let out = read.await;
        if let Ok(bytes) = &out {
            self.get_bytes[c][f].fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        out
    }
}

/// A [`BlobStore`] that counts every request to `inner` into its own
/// [`RequestCounts`].
///
/// Never wrap an [`AnyBlobStore`](crate::AnyBlobStore): it already counts
/// every request into [`RequestCounts::global`], so wrapping it counts each
/// request twice.
#[derive(Debug, Clone)]
pub struct Counted<S> {
    inner: S,
    counts: Arc<RequestCounts>,
}

impl<S> Counted<S> {
    /// Counts `inner`'s requests into `counts`.
    pub fn new(inner: S, counts: Arc<RequestCounts>) -> Self {
        Self { inner, counts }
    }

    /// The store underneath.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// The counts.
    pub fn counts(&self) -> &Arc<RequestCounts> {
        &self.counts
    }
}

impl<S: BlobStore> BlobStore for Counted<S> {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        self.counts.record(key, RequestKind::Put);
        self.inner.upload_file(key, path).await
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        self.counts
            .get(key, self.inner.read_range(key, range))
            .await
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        self.counts.record(key, RequestKind::Head);
        self.inner.size(key).await
    }

    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        self.counts.record(key, RequestKind::Delete);
        self.inner.delete(key).await
    }

    async fn presign_get(&self, key: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        self.inner.presign_get(key, ttl).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalBlobStore;

    /// Invariant: each request is counted once, under the caller whose
    /// scope polled it and the file its key names, with the bytes a read
    /// returned; outside a scope it is `other`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn requests_are_counted_by_caller_file_and_kind() {
        let dir = tempfile::tempdir().unwrap();
        let counts = Arc::new(RequestCounts::default());
        let store = Counted::new(
            LocalBlobStore::open(dir.path().join("b")).unwrap(),
            counts.clone(),
        );
        let src = dir.path().join("src");
        std::fs::write(&src, vec![7u8; 100]).unwrap();
        let pack = BlobKey::new("repos/1/packs/a.pack").unwrap();
        let idx = BlobKey::new("repos/1/packs/a.idx").unwrap();
        BlobCaller::PushUpload
            .scope(async {
                store.upload_file(&pack, &src).await.unwrap();
                store.upload_file(&idx, &src).await.unwrap();
            })
            .await;
        BlobCaller::PushPrefetch
            .scope(async {
                let r = ByteRange::new(10, 40).unwrap();
                store.read_range(&pack, r).await.unwrap();
                store.read_range(&pack, r).await.unwrap();
                store.read_range(&idx, r).await.unwrap();
            })
            .await;
        store.size(&pack).await.unwrap();

        assert_eq!(
            counts.requests(BlobCaller::PushPrefetch, RequestKind::Get),
            3
        );
        assert_eq!(counts.requests(BlobCaller::PushUpload, RequestKind::Put), 2);
        assert_eq!(counts.requests(BlobCaller::Other, RequestKind::Head), 1);
        assert_eq!(counts.requests(BlobCaller::Other, RequestKind::Get), 0);
        let row = counts
            .snapshot()
            .into_iter()
            .find(|r| {
                r.caller == BlobCaller::PushPrefetch
                    && r.file == BlobFile::Pack
                    && r.kind == RequestKind::Get
            })
            .unwrap();
        assert_eq!((row.requests, row.bytes), (2, 60));
        assert_eq!(counts.snapshot().len(), 5, "{:?}", counts.snapshot());
    }
}
