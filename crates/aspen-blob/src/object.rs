//! A blob store over [`object_store`]: S3 and S3-compatible services (R2,
//! MinIO, Ceph, GCS's S3 API), and anything else `object_store` speaks.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use aspen_core::blob::{
    BlobKey, BlobKeyError, BlobStore, ByteRange, PastEndRange, PresignedUrl, UploadReceipt,
};
use aspen_core::error::StoreError;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use tokio::io::AsyncReadExt;

use crate::local::{changed_during_upload, local_io, not_found, same_size, too_large};

/// How [`ObjectBlobStore`] uploads files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadOptions {
    /// Files up to this size go in one `PUT`; larger ones in a multipart
    /// upload with parts of this size (the last part may be smaller).
    pub part_size: usize,
    /// Parts in flight at once. Memory per upload is about
    /// `part_size * (concurrency + 2)`: the parts in flight, the part being
    /// read from disk, and the writer's own buffer.
    pub concurrency: usize,
}

/// S3's smallest part size, except for the last part.
pub const S3_MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// S3's largest part size (5 GiB).
pub const S3_MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// S3's limit on parts per upload.
pub const S3_MAX_PARTS: u64 = 10_000;

/// SigV4's longest presigned URL lifetime.
pub const S3_MAX_PRESIGN_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

impl Default for UploadOptions {
    /// 16 MiB parts, four in flight: about 96 MiB of buffers, and room for
    /// packs up to 156 GiB within S3's 10,000-part limit.
    fn default() -> Self {
        Self {
            part_size: 16 * 1024 * 1024,
            concurrency: 4,
        }
    }
}

/// A [`BlobStore`] over an [`ObjectStore`].
///
/// Keys live under an optional prefix. Uploads check for an existing blob
/// first (`HEAD`): the same size is the same blob uploaded again (keys
/// determine their bytes) and is not sent twice; a different size is
/// [`StoreError::Conflict`]. Two concurrent uploads of one key both write;
/// object stores replace whole objects atomically and both carry the same
/// bytes, so readers see one or the other, never a mix.
#[derive(Clone)]
pub struct ObjectBlobStore {
    store: Arc<dyn ObjectStore>,
    signer: Option<Arc<dyn Signer>>,
    past_end: PastEndRange,
    max_presign_ttl: Duration,
    prefix: Option<String>,
    upload: UploadOptions,
    /// Inclusive bounds on [`UploadOptions::part_size`] for this store
    /// (S3's 5 MiB to 5 GiB for [`ObjectBlobStore::s3`]).
    part_size_bounds: (u64, u64),
}

impl std::fmt::Debug for ObjectBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectBlobStore")
            .field("store", &self.store.to_string())
            .field("presigns", &self.signer.is_some())
            .field("prefix", &self.prefix)
            .field("upload", &self.upload)
            .finish()
    }
}

/// Why an S3 store could not be configured.
#[derive(Debug, thiserror::Error)]
pub enum S3ConfigError {
    /// The prefix is not a valid key.
    #[error(transparent)]
    Prefix(#[from] BlobKeyError),
    /// Upload options outside S3's limits.
    #[error("invalid upload options: {0}")]
    Upload(&'static str),
    /// `object_store` refused the configuration.
    #[error("S3 configuration: {0}")]
    Backend(#[from] object_store::Error),
}

impl ObjectBlobStore {
    /// A store over `store`, with no presigning (see
    /// [`ObjectBlobStore::with_signer`]).
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            signer: None,
            past_end: PastEndRange::Status416,
            max_presign_ttl: Duration::MAX,
            prefix: None,
            upload: UploadOptions::default(),
            part_size_bounds: (1, u64::MAX),
        }
    }

    /// An S3 (or S3-compatible) store from `builder`, which must name the
    /// bucket. [`AmazonS3Builder::from_env`] reads the usual `AWS_*`
    /// variables: region, `AWS_ENDPOINT_URL` and `AWS_ALLOW_HTTP` for MinIO,
    /// static keys, or the ECS task role and web identity.
    ///
    /// Presigned URLs point straight at the bucket, which answers a range
    /// past the end with `416` ([`PastEndRange::Status416`]).
    ///
    /// Part sizes must stay within S3's 5 MiB to 5 GiB, here and in any
    /// later [`ObjectBlobStore::with_upload_options`].
    ///
    /// This installs rustls's `ring` provider as the process default if
    /// none is installed yet (the HTTP client needs one), and leaves an
    /// installed one alone. The server binary (the composition root) should
    /// install its provider explicitly at startup rather than rely on this.
    pub fn s3(builder: AmazonS3Builder, upload: UploadOptions) -> Result<Self, S3ConfigError> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let s3 = Arc::new(builder.build()?);
        let mut store =
            Self::new(s3.clone()).with_signer(s3, PastEndRange::Status416, S3_MAX_PRESIGN_TTL);
        store.part_size_bounds = (S3_MIN_PART_SIZE as u64, S3_MAX_PART_SIZE);
        store.with_upload_options(upload)
    }

    /// Presigns URLs with `signer`; they answer ranges past the end as
    /// `past_end` says, and live at most `max_ttl`.
    #[must_use]
    pub fn with_signer(
        mut self,
        signer: Arc<dyn Signer>,
        past_end: PastEndRange,
        max_ttl: Duration,
    ) -> Self {
        self.signer = Some(signer);
        self.past_end = past_end;
        self.max_presign_ttl = max_ttl;
        self
    }

    /// Puts every key under `prefix` (for example `aspen/prod`).
    pub fn with_prefix(mut self, prefix: &str) -> Result<Self, BlobKeyError> {
        self.prefix = Some(BlobKey::new(prefix)?.as_str().to_owned());
        Ok(self)
    }

    /// Sets how files are uploaded. The part size must be within the
    /// store's bounds (S3's 5 MiB to 5 GiB for an S3 store) and concurrency
    /// positive.
    pub fn with_upload_options(mut self, upload: UploadOptions) -> Result<Self, S3ConfigError> {
        let (min, max) = self.part_size_bounds;
        let part = upload.part_size as u64;
        if part == 0 || part < min {
            return Err(S3ConfigError::Upload(
                "part_size is below the store's minimum",
            ));
        }
        if part > max {
            return Err(S3ConfigError::Upload(
                "part_size is above the store's maximum",
            ));
        }
        if upload.concurrency == 0 {
            return Err(S3ConfigError::Upload("concurrency must be positive"));
        }
        self.upload = upload;
        Ok(self)
    }

    fn path(&self, key: &BlobKey) -> ObjectPath {
        let full = match &self.prefix {
            Some(prefix) => format!("{prefix}/{key}"),
            None => key.as_str().to_owned(),
        };
        // Blob keys are a subset of object_store paths: non-empty segments,
        // no `.` or `..`, no control characters.
        ObjectPath::parse(full).expect("blob keys are valid object paths")
    }

    async fn head_size(&self, key: &BlobKey, path: &ObjectPath) -> Result<Option<u64>, StoreError> {
        match self.store.head(path).await {
            Ok(meta) => Ok(Some(meta.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(backend(key, e)),
        }
    }

    async fn upload_multipart(
        &self,
        key: &BlobKey,
        path: &ObjectPath,
        source: &Path,
        len: u64,
    ) -> Result<(), StoreError> {
        let UploadOptions {
            part_size,
            concurrency,
        } = self.upload;
        let upload = self
            .store
            .put_multipart(path)
            .await
            .map_err(|e| backend(key, e))?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, part_size);
        let sent = async {
            let mut file = tokio::fs::File::open(source)
                .await
                .map_err(|e| local_io(&format!("opening {}", source.display()), &e))?;
            let mut total = 0u64;
            loop {
                // A fresh buffer per part, handed over without a copy.
                let mut buf = vec![0; part_size];
                let n = read_full(&mut file, &mut buf)
                    .await
                    .map_err(|e| local_io(&format!("reading {}", source.display()), &e))?;
                if n == 0 {
                    break;
                }
                writer
                    .wait_for_capacity(concurrency)
                    .await
                    .map_err(|e| backend(key, e))?;
                buf.truncate(n);
                writer.put(bytes::Bytes::from(buf));
                total += n as u64;
                if total > len {
                    break;
                }
            }
            if total != len {
                return Err(changed_during_upload(source, len, total));
            }
            Ok(())
        }
        .await;
        // Every failure aborts the upload, so its parts stop being billed:
        // `finish` aborts by itself when a part or the completion fails;
        // the other failures abort here. Aborting is best effort (the
        // process may die first), so the bucket also needs a lifecycle rule
        // that aborts incomplete uploads; the AWS Terraform module sets one
        // day (`deploy/terraform/aws/storage.tf`).
        match sent {
            Ok(()) => writer
                .finish()
                .await
                .map(|_| ())
                .map_err(|e| backend(key, e)),
            Err(e) => {
                let _ = writer.abort().await;
                Err(e)
            }
        }
    }
}

impl BlobStore for ObjectBlobStore {
    async fn upload_file(&self, key: &BlobKey, source: &Path) -> Result<UploadReceipt, StoreError> {
        let path = self.path(key);
        let len = tokio::fs::metadata(source)
            .await
            .map_err(|e| local_io(&format!("reading {}", source.display()), &e))?
            .len();
        if let Some(existing) = self.head_size(key, &path).await? {
            return same_size(key, existing, len);
        }
        let part_size = self.upload.part_size as u64;
        if len <= part_size {
            let bytes = tokio::fs::read(source)
                .await
                .map_err(|e| local_io(&format!("reading {}", source.display()), &e))?;
            if bytes.len() as u64 != len {
                return Err(changed_during_upload(source, len, bytes.len() as u64));
            }
            self.store
                .put(&path, PutPayload::from(bytes))
                .await
                .map_err(|e| backend(key, e))?;
        } else {
            if len.div_ceil(part_size) > S3_MAX_PARTS {
                return Err(StoreError::LocalIo {
                    reason: format!(
                        "{} is {len} bytes, more than {S3_MAX_PARTS} parts of {part_size}",
                        source.display()
                    ),
                });
            }
            self.upload_multipart(key, &path, source, len).await?;
        }
        Ok(UploadReceipt { size: len })
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        usize::try_from(range.len()).map_err(|_| too_large(range))?;
        let path = self.path(key);
        match self
            .store
            .get_range(&path, range.start()..range.end())
            .await
        {
            Ok(bytes) if bytes.len() as u64 <= range.len() => Ok(Vec::from(bytes)),
            Ok(bytes) => Err(StoreError::Unavailable {
                reason: format!(
                    "blob {key}: asked for {} bytes, the store sent {}",
                    range.len(),
                    bytes.len()
                ),
            }),
            Err(object_store::Error::NotFound { .. }) => Err(not_found(key)),
            Err(e) => {
                // Stores refuse a range that starts at or past the end
                // (S3 answers 416) rather than returning nothing. Tell that
                // apart from a real failure with one HEAD, on this error
                // path only.
                match self.head_size(key, &path).await? {
                    None => Err(not_found(key)),
                    Some(size) if size <= range.start() => Ok(Vec::new()),
                    Some(_) => Err(backend(key, e)),
                }
            }
        }
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        self.head_size(key, &self.path(key)).await
    }

    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        // S3 answers a DELETE of a missing key with success; other stores
        // may say not found, which is success here too.
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(backend(key, e)),
        }
    }

    async fn presign_get(&self, key: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| StoreError::Unavailable {
                reason: format!("{} does not presign URLs", self.store),
            })?;
        crate::check_presign_ttl(ttl, self.max_presign_ttl)?;
        let url = signer
            .signed_url(object_store::signer::Method::GET, &self.path(key), ttl)
            .await
            .map_err(|e| backend(key, e))?;
        Ok(PresignedUrl {
            url: url.into(),
            ttl,
            past_end: self.past_end,
        })
    }
}

/// Fills `buf` from `file`, short only at the end of the file.
async fn read_full(file: &mut tokio::fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]).await? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

/// Maps an `object_store` error. The text goes to logs only: clients see
/// "internal storage error; retry" ([`aspen_core::error`]).
fn backend(key: &BlobKey, e: object_store::Error) -> StoreError {
    match e {
        object_store::Error::NotFound { .. } => not_found(key),
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => {
            StoreError::Conflict {
                what: format!("blob {key}: {e}"),
            }
        }
        e => StoreError::Unavailable {
            reason: format!("blob {key}: {e}"),
        },
    }
}
