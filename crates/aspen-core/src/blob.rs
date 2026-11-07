//! The blob store contract: packs, `.idx` files and entry tables in object
//! storage (S3, GCS, Azure Blob, R2, MinIO or a local directory).
//!
//! The contract is deliberately small (S4): ingest writes a complete pack to
//! local disk first, so the store only needs "upload this local file" plus
//! ranged reads for base prefetch and stitching, and presigned URLs for
//! `packfile-uris` (S3). There is no streaming write: nothing is teed into
//! the store while a pack is still being received.

use std::cell::Cell;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::error::StoreError;

/// Longest blob key, in bytes.
pub const MAX_BLOB_KEY_LEN: usize = 1024;

/// Why a string is not a valid blob key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid blob key {key:?}: {rule}")]
pub struct BlobKeyError {
    /// The key.
    pub key: String,
    /// The rule it breaks.
    pub rule: &'static str,
}

/// A key in the blob store, relative to the configured bucket and prefix.
///
/// Keys are `/`-separated, non-empty segments of ASCII letters, digits, `.`,
/// `_` and `-`; no segment is `.` or `..`, and the key is at most
/// [`MAX_BLOB_KEY_LEN`] bytes. That subset is safe on every supported store
/// and as a local path. Pack keys are derived from checksums by
/// [`crate::pack::PackKeys`], never built by hand.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobKey(String);

impl BlobKey {
    /// Validates a key.
    pub fn new(key: impl Into<String>) -> Result<Self, BlobKeyError> {
        let key = key.into();
        let err = |rule| BlobKeyError {
            key: key.clone(),
            rule,
        };
        if key.is_empty() {
            return Err(err("empty"));
        }
        if key.len() > MAX_BLOB_KEY_LEN {
            return Err(err("too long"));
        }
        for segment in key.split('/') {
            if segment.is_empty() {
                return Err(err("empty segment"));
            }
            if segment == "." || segment == ".." {
                return Err(err("`.` or `..` segment"));
            }
            if !segment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
            {
                return Err(err("only ASCII letters, digits, `.`, `_`, `-` and `/`"));
            }
        }
        Ok(Self(key))
    }

    /// The key as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BlobKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A half-open byte range `[start, end)`, never empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    start: u64,
    end: u64,
}

impl ByteRange {
    /// `[start, end)`; `None` if `start >= end`.
    pub const fn new(start: u64, end: u64) -> Option<Self> {
        if start < end {
            Some(Self { start, end })
        } else {
            None
        }
    }

    /// First byte.
    pub const fn start(&self) -> u64 {
        self.start
    }

    /// One past the last byte.
    pub const fn end(&self) -> u64 {
        self.end
    }

    /// Number of bytes.
    pub const fn len(&self) -> u64 {
        self.end - self.start
    }

    /// Always false: a `ByteRange` is never empty. Present for clippy's
    /// `len_without_is_empty`.
    pub const fn is_empty(&self) -> bool {
        false
    }
}

/// What a URL from [`BlobStore::presign_get`] answers for a `Range` that
/// starts at or past the end of the blob (S3, finding 14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PastEndRange {
    /// `416 Range Not Satisfiable`: what S3, GCS and Azure do. git 2.55 and
    /// older get stuck for good resuming an interrupted download against it,
    /// so such URLs go only to clients whose agent is `git/2.56` or later.
    Status416,
    /// `206` with an empty body: what an Aspen-served endpoint
    /// (`aspen-express`, or a CDN configured for it) answers. Safe for every
    /// client.
    Empty206,
}

/// A presigned download URL: direct (no redirect), self-authorizing, and
/// honouring `Range`.
#[derive(Clone, PartialEq, Eq)]
pub struct PresignedUrl {
    /// The URL. It carries a credential, so it is redacted from `Debug`.
    pub url: String,
    /// How long it stays valid from issue.
    pub ttl: Duration,
    /// What it answers for a range past the end.
    pub past_end: PastEndRange,
}

impl std::fmt::Debug for PresignedUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PresignedUrl")
            .field("url", &"<redacted>")
            .field("ttl", &self.ttl)
            .field("past_end", &self.past_end)
            .finish()
    }
}

/// Result of an upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadReceipt {
    /// Bytes stored.
    pub size: u64,
}

/// Object storage for packs, `.idx` files and entry tables.
///
/// Blobs are immutable: a key is written once, with bytes determined by
/// the key (pack keys embed the pack checksum). Uploading the same bytes to
/// the same key again succeeds, so retries are idempotent.
///
/// Methods return `Send` futures so the server can spawn them; implement
/// them with `async fn`.
pub trait BlobStore: Send + Sync {
    /// Uploads a complete local file to `key`, using multipart upload when
    /// the file is large. Returns once the blob is durable. The file must not
    /// change during the upload.
    fn upload_file(
        &self,
        key: &BlobKey,
        path: &Path,
    ) -> impl Future<Output = Result<UploadReceipt, StoreError>> + Send;

    /// Reads `range` of the blob. Returns fewer bytes than asked when the
    /// range extends past the end, and no bytes when it starts at or past
    /// the end; never more than `range.len()` bytes. A missing blob is
    /// [`StoreError::NotFound`].
    ///
    /// A short read is normal only past the blob's end. Callers that know
    /// the blob's size from its `packs` row (the stitcher, the entry-table
    /// and `.idx` loaders) use [`BlobStore::read_exact_range`] instead, which
    /// turns a short read into [`StoreError::Corrupt`].
    fn read_range(
        &self,
        key: &BlobKey,
        range: ByteRange,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send;

    /// Reads exactly `range` of the blob, or fails.
    ///
    /// For callers that know the blob's size from its `packs` row and so
    /// ask only for ranges inside it. Any read shorter than `range.len()`
    /// (the blob ends early, or does not reach `range.start()` at all) means
    /// the stored blob does not match its row, so it is
    /// [`StoreError::Corrupt`], which pages, rather than a normal outcome to
    /// retry or a truncated pack to send. A missing blob stays
    /// [`StoreError::NotFound`], which pages too.
    ///
    /// Backends do not override this: the check must be the same for all of
    /// them.
    fn read_exact_range(
        &self,
        key: &BlobKey,
        range: ByteRange,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        async move {
            let bytes = self.read_range(key, range).await?;
            if bytes.len() as u64 != range.len() {
                return Err(StoreError::Corrupt {
                    what: format!(
                        "blob {key}: bytes {}..{} returned {} of {} bytes",
                        range.start(),
                        range.end(),
                        bytes.len(),
                        range.len()
                    ),
                });
            }
            Ok(bytes)
        }
    }

    /// The blob's size, or `None` if it does not exist.
    fn size(&self, key: &BlobKey) -> impl Future<Output = Result<Option<u64>, StoreError>> + Send;

    /// Deletes the blob. Idempotent: deleting a blob that does not exist
    /// succeeds. Only garbage collection calls it, for blobs whose `packs`
    /// row it has already deleted ([`crate::meta::MetaStore::sweep_orphan_packs`]).
    fn delete(&self, key: &BlobKey) -> impl Future<Output = Result<(), StoreError>> + Send;

    /// A presigned GET URL for the blob, valid for `ttl`.
    fn presign_get(
        &self,
        key: &BlobKey,
        ttl: Duration,
    ) -> impl Future<Output = Result<PresignedUrl, StoreError>> + Send;
}

/// Which part of Aspen a blob store request serves: the `caller` label of
/// the request counters the stores keep (`aspen_blob`, exported at
/// `/-/metrics` as `aspen_blob_*`).
///
/// The label travels with the work, not with the store: a request is
/// counted under the caller of the innermost [`BlobCaller::scope`] that
/// polls it, so one store (and one `.idx` cache in front of it) serves
/// every caller and still says who asked. Outside any scope it is
/// [`BlobCaller::Other`]. A scope covers what its future polls on the same
/// task; work handed to `tokio::spawn` starts outside it, so a spawner
/// wraps the spawned future in a scope of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlobCaller {
    /// The receive-pack advertisement before a push.
    PushAdvertise,
    /// A push computing its search packs (the closures of its
    /// dependencies), and widening them.
    PushSearch,
    /// A push reading its delta bases (push path, base prefetch).
    PushPrefetch,
    /// A push's object graph: the walk of the changed paths (old trees),
    /// the lookup of held objects, the connectivity check and the plan of
    /// each write.
    PushGraph,
    /// A push's commit check (ancestry walks that read commits from packs).
    PushCheck,
    /// A push uploading its packs.
    PushUpload,
    /// The rest of a push: its hook's reads, mirror forwarding.
    PushOther,
    /// A fetch: planning, `shallow-info`, the stitched pack and packfile
    /// URIs.
    Fetch,
    /// Building an exact shallow pack for a fetch (node C8).
    ShallowBuild,
    /// The compaction worker's jobs: `compact`, `index_commits` and
    /// `rewrite_pack`.
    Compaction,
    /// Garbage collection's sweep.
    Gc,
    /// The merge queue preparing batches. The pushes it lands count under
    /// the push stages, like any push.
    MergeQueue,
    /// Mirror mode's imports (outside the push stages they run).
    Mirror,
    /// Anything outside a scope.
    Other,
}

impl BlobCaller {
    /// Every caller, in label order.
    pub const ALL: [Self; 14] = [
        Self::PushAdvertise,
        Self::PushSearch,
        Self::PushPrefetch,
        Self::PushGraph,
        Self::PushCheck,
        Self::PushUpload,
        Self::PushOther,
        Self::Fetch,
        Self::ShallowBuild,
        Self::Compaction,
        Self::Gc,
        Self::MergeQueue,
        Self::Mirror,
        Self::Other,
    ];

    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PushAdvertise => "push_advertise",
            Self::PushSearch => "push_search",
            Self::PushPrefetch => "push_prefetch",
            Self::PushGraph => "push_graph",
            Self::PushCheck => "push_check",
            Self::PushUpload => "push_upload",
            Self::PushOther => "push_other",
            Self::Fetch => "fetch",
            Self::ShallowBuild => "shallow_build",
            Self::Compaction => "compaction",
            Self::Gc => "gc",
            Self::MergeQueue => "merge_queue",
            Self::Mirror => "mirror",
            Self::Other => "other",
        }
    }

    /// Position in [`BlobCaller::ALL`], for counter arrays.
    pub const fn index(self) -> usize {
        self as usize
    }

    /// The caller of the scope being polled on this thread, or
    /// [`BlobCaller::Other`]. Stores call it when a request starts, before
    /// handing the request to another thread.
    pub fn current() -> Self {
        CURRENT_CALLER.with(Cell::get)
    }

    /// `future`, with every blob store request it makes counted under
    /// `self` (unless a scope inside it says otherwise).
    pub fn scope<F: Future>(self, future: F) -> CallerScope<F> {
        CallerScope {
            caller: self,
            future: Box::pin(future),
        }
    }
}

thread_local! {
    static CURRENT_CALLER: Cell<BlobCaller> = const { Cell::new(BlobCaller::Other) };
}

/// A future whose blob store requests are counted under one
/// [`BlobCaller`] ([`BlobCaller::scope`]). Each poll sets the caller for
/// the duration of the poll and restores the outer one after it, so scopes
/// nest and interleaved tasks on one thread never see each other's.
#[must_use = "futures do nothing unless polled"]
pub struct CallerScope<F> {
    caller: BlobCaller,
    // Boxed so this type is `Unpin` without unsafe pin projection; a scope
    // wraps a whole stage, not a single read.
    future: Pin<Box<F>>,
}

impl<F> std::fmt::Debug for CallerScope<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallerScope")
            .field("caller", &self.caller)
            .finish_non_exhaustive()
    }
}

impl<F: Future> Future for CallerScope<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        /// Puts the outer caller back, on unwind too.
        struct Restore(BlobCaller);
        impl Drop for Restore {
            fn drop(&mut self) {
                CURRENT_CALLER.with(|c| c.set(self.0));
            }
        }
        let _restore = Restore(CURRENT_CALLER.with(|c| c.replace(self.caller)));
        self.future.as_mut().poll(cx)
    }
}

/// What a blob key names, from its file name: the `file` label of the
/// request counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlobFile {
    /// A pack (`.pack`).
    Pack,
    /// A pack index (`.idx`).
    Idx,
    /// An entry table (`.entries`).
    Entries,
    /// Anything else.
    Other,
}

impl BlobFile {
    /// Every kind, in label order.
    pub const ALL: [Self; 4] = [Self::Pack, Self::Idx, Self::Entries, Self::Other];

    /// The kind of file `key` names.
    pub fn of(key: &BlobKey) -> Self {
        let k = key.as_str();
        if k.ends_with(".pack") {
            Self::Pack
        } else if k.ends_with(".idx") {
            Self::Idx
        } else if k.ends_with(".entries") {
            Self::Entries
        } else {
            Self::Other
        }
    }

    /// The label value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pack => "pack",
            Self::Idx => "idx",
            Self::Entries => "entries",
            Self::Other => "other",
        }
    }

    /// Position in [`BlobFile::ALL`], for counter arrays.
    pub const fn index(self) -> usize {
        self as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_keys() {
        assert!(BlobKey::new("repos/1/packs/ab.pack").is_ok());
        for bad in ["", "/a", "a/", "a//b", "a/../b", "./a", "a b", "a\\b", "é"] {
            assert!(BlobKey::new(bad).is_err(), "{bad:?}");
        }
        assert!(BlobKey::new("a".repeat(MAX_BLOB_KEY_LEN + 1)).is_err());
    }

    #[test]
    fn byte_ranges_are_never_empty() {
        assert!(ByteRange::new(5, 5).is_none());
        assert!(ByteRange::new(6, 5).is_none());
        let r = ByteRange::new(5, 9).unwrap();
        assert_eq!((r.start(), r.end(), r.len()), (5, 9, 4));
    }

    /// A store over one in-memory blob whose `read_range` follows the
    /// contract: it clamps at the end of the blob.
    struct OneBlob {
        key: BlobKey,
        bytes: Vec<u8>,
    }

    impl BlobStore for OneBlob {
        async fn upload_file(&self, _: &BlobKey, _: &Path) -> Result<UploadReceipt, StoreError> {
            unreachable!("not used")
        }

        async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
            if *key != self.key {
                return Err(StoreError::NotFound {
                    what: key.to_string(),
                });
            }
            let len = self.bytes.len() as u64;
            let start = range.start().min(len) as usize;
            let end = range.end().min(len) as usize;
            Ok(self.bytes[start..end].to_vec())
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

    /// Polls a future that never waits (the store above is synchronous).
    fn ready<F: Future>(f: F) -> F::Output {
        let mut f = std::pin::pin!(f);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match f.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => panic!("store future is not ready"),
        }
    }

    /// Invariant: `read_exact_range` returns exactly the asked bytes, and
    /// every short read (ending early, or starting at or past the end) is
    /// `Corrupt`, never a short `Ok`. A missing blob stays `NotFound`.
    #[test]
    fn read_exact_range_turns_short_reads_into_corruption() {
        let key = BlobKey::new("repos/1/packs/a.pack").unwrap();
        let store = OneBlob {
            key: key.clone(),
            bytes: (0u8..10).collect(),
        };
        let r = |a, b| ByteRange::new(a, b).unwrap();

        assert_eq!(
            ready(store.read_exact_range(&key, r(0, 10))).unwrap(),
            store.bytes
        );
        assert_eq!(
            ready(store.read_exact_range(&key, r(3, 5))).unwrap(),
            vec![3, 4]
        );
        for short in [r(8, 11), r(10, 11), r(20, 30)] {
            let err = ready(store.read_exact_range(&key, short)).unwrap_err();
            assert!(
                matches!(err, StoreError::Corrupt { .. }),
                "{short:?}: {err:?}"
            );
            assert!(err.is_corruption());
        }
        // The same short ranges are a normal outcome for `read_range`.
        assert_eq!(ready(store.read_range(&key, r(8, 11))).unwrap(), vec![8, 9]);

        let other = BlobKey::new("repos/1/packs/b.pack").unwrap();
        let err = ready(store.read_exact_range(&other, r(0, 1))).unwrap_err();
        assert!(matches!(err, StoreError::NotFound { .. }), "{err:?}");
    }

    #[test]
    fn presigned_urls_do_not_leak_through_debug() {
        let u = PresignedUrl {
            url: "https://bucket/x?X-Amz-Signature=secret".into(),
            ttl: Duration::from_secs(60),
            past_end: PastEndRange::Status416,
        };
        assert!(!format!("{u:?}").contains("secret"));
    }

    /// Returns `Pending` once, then the caller it sees when polled again.
    struct YieldThenRead(bool);

    impl Future for YieldThenRead {
        type Output = BlobCaller;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<BlobCaller> {
            if self.0 {
                Poll::Ready(BlobCaller::current())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    /// Invariant: a scope labels exactly what it polls. Scopes nest (the
    /// innermost wins and the outer label comes back after it), the label
    /// is gone between polls, and two scoped futures polled in turn on one
    /// thread each see only their own.
    #[test]
    fn caller_scopes_nest_restore_and_do_not_leak_between_tasks() {
        assert_eq!(BlobCaller::current(), BlobCaller::Other);
        let seen = ready(BlobCaller::PushGraph.scope(async {
            let outer = BlobCaller::current();
            let inner = BlobCaller::PushPrefetch
                .scope(async { BlobCaller::current() })
                .await;
            (outer, inner, BlobCaller::current())
        }));
        assert_eq!(
            seen,
            (
                BlobCaller::PushGraph,
                BlobCaller::PushPrefetch,
                BlobCaller::PushGraph
            )
        );
        assert_eq!(BlobCaller::current(), BlobCaller::Other);

        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut a = BlobCaller::Fetch.scope(YieldThenRead(false));
        let mut b = BlobCaller::Compaction.scope(YieldThenRead(false));
        assert!(Pin::new(&mut a).poll(&mut cx).is_pending());
        assert_eq!(BlobCaller::current(), BlobCaller::Other, "nothing left set");
        assert!(Pin::new(&mut b).poll(&mut cx).is_pending());
        assert_eq!(
            Pin::new(&mut a).poll(&mut cx),
            Poll::Ready(BlobCaller::Fetch)
        );
        assert_eq!(
            Pin::new(&mut b).poll(&mut cx),
            Poll::Ready(BlobCaller::Compaction)
        );
        assert_eq!(BlobCaller::current(), BlobCaller::Other);
    }

    #[test]
    fn callers_and_files_index_their_label_arrays() {
        for (i, c) in BlobCaller::ALL.iter().enumerate() {
            assert_eq!(c.index(), i, "{c:?}");
        }
        for (i, f) in BlobFile::ALL.iter().enumerate() {
            assert_eq!(f.index(), i, "{f:?}");
        }
        let key = |k: &str| BlobKey::new(k).unwrap();
        assert_eq!(BlobFile::of(&key("repos/1/packs/ab.pack")), BlobFile::Pack);
        assert_eq!(BlobFile::of(&key("repos/1/packs/ab.g2.idx")), BlobFile::Idx);
        assert_eq!(
            BlobFile::of(&key("repos/1/packs/ab.entries")),
            BlobFile::Entries
        );
        assert_eq!(BlobFile::of(&key("repos/1/x.json")), BlobFile::Other);
    }
}
