//! Blob store backends for Aspen: where packs, `.idx` files and entry
//! tables live.
//!
//! Two implementations of [`aspen_core::blob::BlobStore`]:
//!
//! - [`LocalBlobStore`]: a directory on local disk, for `aspen dev` and
//!   single-host installs. Write-once, `fsync`ed uploads; presigned URLs are
//!   HMAC-signed links that Aspen's own HTTP server serves
//!   ([`LocalUrlSigner`]).
//! - [`ObjectBlobStore`]: anything [`object_store`] speaks. [`open`] builds
//!   S3 and S3-compatible stores (MinIO, R2, Ceph) from an `s3://` URL;
//!   multipart uploads for large packs, SigV4 presigned URLs.
//!
//! [`open`] turns the `ASPEN_BLOB_URL` setting (`deploy/README.md`) into an
//! [`AnyBlobStore`]. Only the composition roots (`aspen-server`,
//! `aspen-cli`) depend on this crate; everything else uses the trait.
//!
//! Every backend passes the same conformance suite
//! (`tests/conformance.rs`): round trips, range reads clamped at the end,
//! [`BlobStore::read_exact_range`] turning short reads into corruption,
//! idempotent and write-once uploads, multipart uploads, and presigned
//! URLs. The S3 backend runs it against a real MinIO server.

mod dns;
mod local;
mod object;

use std::path::Path;
use std::time::Duration;

pub use aspen_core::blob::BlobStore;
use aspen_core::blob::{BlobKey, ByteRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
pub use local::{
    LOCAL_MAX_PRESIGN_TTL, LocalBlobStore, LocalUrlSigner, MIN_SECRET_LEN, STAGING_DIR,
    SignerConfigError, UrlRejected,
};
pub use object::{
    ObjectBlobStore, S3_MAX_PART_SIZE, S3_MAX_PARTS, S3_MAX_PRESIGN_TTL, S3_MIN_PART_SIZE,
    S3ConfigError, UploadOptions,
};

/// The longest presigned-URL lifetime callers (the fetch path, B2 and C2)
/// should ask for. Backends accept up to their own cap (7 days), but a
/// SigV4 URL signed with temporary credentials, such as an ECS task role,
/// stops working when those credentials expire, often within an hour; a
/// short TTL also limits how long a leaked URL is useful. It is
/// [`aspen_core::pack::MAX_PACK_URL_LIFETIME`], which garbage collection's
/// grace period is checked against; the fetch path clamps every presign to
/// it.
pub const RECOMMENDED_MAX_PRESIGN_TTL: Duration = aspen_core::pack::MAX_PACK_URL_LIFETIME;

/// Refuses a presign TTL outside `(0, max]`, the same rule on every
/// backend.
pub(crate) fn check_presign_ttl(ttl: Duration, max: Duration) -> Result<(), StoreError> {
    if ttl.is_zero() || ttl > max {
        return Err(StoreError::Unavailable {
            reason: format!("presign TTL {ttl:?} is outside (0, {max:?}]"),
        });
    }
    Ok(())
}
/// The `object_store` crate, for building stores ([`object_store::aws::AmazonS3Builder`]).
pub use object_store;

/// The backend chosen by configuration.
#[derive(Debug, Clone)]
pub enum AnyBlobStore {
    /// A local directory.
    Local(LocalBlobStore),
    /// Object storage.
    Object(ObjectBlobStore),
}

/// Why [`open`] refused a blob store URL.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// Not a URL, or a URL of the wrong shape for its scheme.
    #[error("invalid blob store URL {url:?}: {reason}")]
    InvalidUrl {
        /// The URL.
        url: String,
        /// What is wrong.
        reason: String,
    },
    /// A scheme this build does not support.
    #[error("unsupported blob store URL scheme {0:?}; use file:// or s3://")]
    UnsupportedScheme(String),
    /// The S3 configuration was refused.
    #[error(transparent)]
    S3(#[from] S3ConfigError),
    /// The local directory could not be opened.
    #[error(transparent)]
    Local(#[from] StoreError),
}

/// Opens the blob store named by `url`:
///
/// - `file:///var/lib/aspen/blobs`: a [`LocalBlobStore`] rooted there;
/// - `s3://<bucket>` or `s3://<bucket>/<prefix>`: an S3 store, configured
///   from the `AWS_*` environment ([`ObjectBlobStore::s3`]), with keys
///   under `<prefix>` if given.
///
/// The local store has no URL signer until the server adds one
/// ([`LocalBlobStore::with_url_signer`]).
pub fn open(url: &str) -> Result<AnyBlobStore, OpenError> {
    let invalid = |reason: &str| OpenError::InvalidUrl {
        url: url.to_owned(),
        reason: reason.to_owned(),
    };
    let parsed = url::Url::parse(url).map_err(|e| invalid(&e.to_string()))?;
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(invalid("no query or fragment allowed"));
    }
    match parsed.scheme() {
        "file" => {
            if parsed.host_str().is_some_and(|h| !h.is_empty()) {
                return Err(invalid("file URLs name a local path: file:///path"));
            }
            let root = parsed
                .to_file_path()
                .map_err(|()| invalid("not an absolute local path"))?;
            Ok(AnyBlobStore::Local(LocalBlobStore::open(root)?))
        }
        "s3" => {
            let bucket = parsed
                .host_str()
                .filter(|b| !b.is_empty())
                .ok_or_else(|| invalid("missing bucket: s3://<bucket>[/<prefix>]"))?;
            if !parsed.username().is_empty() || parsed.password().is_some() {
                return Err(invalid(
                    "credentials do not belong in the URL; use AWS_* variables",
                ));
            }
            let builder = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_client_options(client_options_from_env());
            let store = ObjectBlobStore::s3(builder, UploadOptions::default())?;
            let prefix = parsed.path().trim_matches('/');
            Ok(AnyBlobStore::Object(if prefix.is_empty() {
                store
            } else {
                store.with_prefix(prefix).map_err(S3ConfigError::from)?
            }))
        }
        other => Err(OpenError::UnsupportedScheme(other.to_owned())),
    }
}

/// The HTTP client options `AmazonS3Builder::from_env` would read from the
/// `AWS_*` environment (`AWS_ALLOW_HTTP`, timeouts, proxies, ...), plus the
/// in-process DNS cache ([`dns::CachingResolver`]). Setting client options on
/// the builder replaces the ones `from_env` read, so they are read again
/// here.
fn client_options_from_env() -> object_store::ClientOptions {
    let mut options = object_store::ClientOptions::new();
    for (key, value) in std::env::vars_os() {
        let (Some(key), Some(value)) = (key.to_str(), value.to_str()) else {
            continue;
        };
        let Some(name) = key.strip_prefix("AWS_") else {
            continue;
        };
        if let Ok(k) = name
            .to_ascii_lowercase()
            .parse::<object_store::ClientConfigKey>()
        {
            options = options.with_config(k, value);
        }
    }
    options.with_dns_resolver(std::sync::Arc::new(dns::CachingResolver::default()))
}

impl BlobStore for AnyBlobStore {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        match self {
            Self::Local(s) => s.upload_file(key, path).await,
            Self::Object(s) => s.upload_file(key, path).await,
        }
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        match self {
            Self::Local(s) => s.read_range(key, range).await,
            Self::Object(s) => s.read_range(key, range).await,
        }
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        match self {
            Self::Local(s) => s.size(key).await,
            Self::Object(s) => s.size(key).await,
        }
    }

    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        match self {
            Self::Local(s) => s.delete(key).await,
            Self::Object(s) => s.delete(key).await,
        }
    }

    async fn presign_get(&self, key: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        match self {
            Self::Local(s) => s.presign_get(key, ttl).await,
            Self::Object(s) => s.presign_get(key, ttl).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_rejects_malformed_urls() {
        for (url, want) in [
            ("not a url", "invalid"),
            ("gs://bucket", "unsupported"),
            ("file://host/x", "invalid"),
            ("s3://", "invalid"),
            ("s3://bucket?x=1", "invalid"),
            ("s3://key:secret@bucket", "invalid"),
            ("s3://bucket/a//b", "key"),
            ("s3://bucket/a b", "key"),
        ] {
            let err = open(url).unwrap_err().to_string();
            assert!(err.contains(want), "{url}: {err}");
        }
    }

    #[test]
    fn open_file_url_roots_a_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let url = url::Url::from_directory_path(dir.path().join("blobs")).unwrap();
        match open(url.as_str()).unwrap() {
            AnyBlobStore::Local(s) => {
                assert_eq!(s.root(), dir.path().join("blobs"));
                assert!(s.root().join(STAGING_DIR).is_dir());
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn open_s3_url_applies_the_prefix() {
        let store = match open("s3://aspen-packs/team/prod").unwrap() {
            AnyBlobStore::Object(s) => s,
            other => panic!("{other:?}"),
        };
        let debug = format!("{store:?}");
        assert!(debug.contains("team/prod"), "{debug}");
    }
}
