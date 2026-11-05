//! A blob store in a local directory, for `aspen dev` and single-host
//! installs without object storage.
//!
//! Each key maps to the file `<root>/<key>`. Uploads copy the source into a
//! staging file under the root, `fsync` it, and link it into place without
//! replacing anything already there, so a blob is written once and a
//! reader never sees a partial file. The parent directories are synced
//! afterwards, so the blob survives a crash once `upload_file` returns.
//!
//! An upload that finds the blob already there (a retry, or the loser of a
//! race) syncs the existing file and its directories before it returns
//! `Ok` too: the earlier writer may have crashed, or still be running,
//! between linking the blob into place and syncing its directory, and
//! `Ok` promises durability whichever upload made the file.

use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aspen_core::blob::{BlobKey, BlobStore, ByteRange, PastEndRange, PresignedUrl, UploadReceipt};
use aspen_core::error::StoreError;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// Staging directory for uploads in progress, under the root. `~` is not
/// allowed in a [`BlobKey`], so no key can name it or anything inside it.
///
/// A crash can leave files here. They are never read; delete them while no
/// Aspen process uses the directory.
pub const STAGING_DIR: &str = ".staging~";

/// A [`BlobStore`] in a local directory.
#[derive(Debug, Clone)]
pub struct LocalBlobStore {
    root: PathBuf,
    signer: Option<LocalUrlSigner>,
}

impl LocalBlobStore {
    /// Opens (creating if needed) a store rooted at `root`. The directory
    /// must be on a local POSIX-like filesystem: uploads rely on an atomic
    /// no-replace link or rename, and on `fsync`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let root = root.into();
        fs::create_dir_all(root.join(STAGING_DIR))
            .map_err(|e| local_io(&format!("creating {}", root.display()), &e))?;
        Ok(Self { root, signer: None })
    }

    /// Serves presigned URLs with `signer`. Without one,
    /// [`BlobStore::presign_get`] fails, and the fetch path falls back to
    /// stitched packs.
    #[must_use]
    pub fn with_url_signer(mut self, signer: LocalUrlSigner) -> Self {
        self.signer = Some(signer);
        self
    }

    /// The store's root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The URL signer, if any. The HTTP handler that serves the signed URLs
    /// verifies requests with it.
    pub fn url_signer(&self) -> Option<&LocalUrlSigner> {
        self.signer.as_ref()
    }

    /// The file that holds `key`. Every key is a relative path of plain
    /// segments (no `.`, `..`, empty or absolute parts), so this is always
    /// inside the root.
    pub fn path_of(&self, key: &BlobKey) -> PathBuf {
        let mut path = self.root.clone();
        path.extend(key.as_str().split('/'));
        path
    }

    fn upload_blocking(&self, key: &BlobKey, source: &Path) -> Result<UploadReceipt, StoreError> {
        let source_len = fs::metadata(source)
            .map_err(|e| local_io(&format!("reading {}", source.display()), &e))?
            .len();
        let dest = self.path_of(key);
        if let Some(existing) = blob_size(&dest, key)? {
            return self.adopt_existing(key, &dest, existing, source_len);
        }
        // Refuse a key below a blob before copying the source into staging.
        if let Some(blob) = self.blob_above(key) {
            return Err(below_blob(key, &blob));
        }

        let mut staged = tempfile::Builder::new()
            .prefix("upload-")
            .tempfile_in(self.root.join(STAGING_DIR))
            .map_err(|e| local_io("creating a staging file", &e))?;
        let mut src = fs::File::open(source)
            .map_err(|e| local_io(&format!("opening {}", source.display()), &e))?;
        let copied = io::copy(&mut src, staged.as_file_mut())
            .map_err(|e| local_io(&format!("copying {}", source.display()), &e))?;
        if copied != source_len {
            return Err(changed_during_upload(source, source_len, copied));
        }
        staged
            .as_file()
            .sync_all()
            .map_err(|e| local_io("syncing a staging file", &e))?;

        let parent = dest.parent().expect("a key path has a parent");
        if let Err(e) = fs::create_dir_all(parent) {
            // A concurrent upload may have made a blob above this key since
            // the check before staging.
            return Err(match self.blob_above(key) {
                Some(blob) => below_blob(key, &blob),
                None => local_io(&format!("creating {}", parent.display()), &e),
            });
        }
        #[cfg(test)]
        test_hooks::before_link(&dest);
        match staged.persist_noclobber(&dest) {
            Ok(_) => {}
            Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {
                // A concurrent upload of the same key won; the staging file
                // is removed when `e.file` drops.
                let existing = blob_size(&dest, key)?
                    .ok_or_else(|| local_io("linking a blob into place", &e.error))?;
                return self.adopt_existing(key, &dest, existing, copied);
            }
            Err(e) => return Err(local_io(&format!("linking {}", dest.display()), &e.error)),
        }
        self.sync_dirs(parent)?;
        Ok(UploadReceipt { size: copied })
    }

    /// The outcome of an upload that found `existing` bytes already at
    /// `dest`. On the same size, syncs the file and its directories before
    /// returning `Ok`: the upload that created it may not have synced them
    /// yet (it crashed, or is still running), and `Ok` means durable.
    fn adopt_existing(
        &self,
        key: &BlobKey,
        dest: &Path,
        existing: u64,
        new: u64,
    ) -> Result<UploadReceipt, StoreError> {
        let receipt = same_size(key, existing, new)?;
        sync_file(dest).map_err(|e| local_io(&format!("syncing {}", dest.display()), &e))?;
        self.sync_dirs(dest.parent().expect("a key path has a parent"))?;
        Ok(receipt)
    }

    /// The shortest proper prefix of `key` that is a blob (a file), if any.
    fn blob_above(&self, key: &BlobKey) -> Option<String> {
        let segments: Vec<&str> = key.as_str().split('/').collect();
        let mut path = self.root.clone();
        for (i, segment) in segments[..segments.len() - 1].iter().enumerate() {
            path.push(segment);
            if path.is_file() {
                return Some(segments[..=i].join("/"));
            }
        }
        None
    }

    /// Syncs `dir` and each of its ancestors up to the root, so the new
    /// directory entries (the blob's, and any directory just created) are
    /// durable.
    fn sync_dirs(&self, mut dir: &Path) -> Result<(), StoreError> {
        loop {
            sync_dir(dir).map_err(|e| local_io(&format!("syncing {}", dir.display()), &e))?;
            if dir == self.root {
                return Ok(());
            }
            match dir.parent() {
                Some(parent) if parent.starts_with(&self.root) => dir = parent,
                _ => return Ok(()),
            }
        }
    }

    fn read_range_blocking(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        let path = self.path_of(key);
        let mut file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if is_absent(&e) => return Err(not_found(key)),
            Err(e) => return Err(local_io(&format!("opening {}", path.display()), &e)),
        };
        let meta = file
            .metadata()
            .map_err(|e| local_io(&format!("reading {}", path.display()), &e))?;
        if !meta.is_file() {
            return Err(not_found(key));
        }
        let len = meta.len();
        if range.start() >= len {
            return Ok(Vec::new());
        }
        let want = range.end().min(len) - range.start();
        let mut out = Vec::with_capacity(usize::try_from(want).map_err(|_| too_large(range))?);
        file.seek(SeekFrom::Start(range.start()))
            .and_then(|_| file.take(want).read_to_end(&mut out))
            .map_err(|e| local_io(&format!("reading {}", path.display()), &e))?;
        Ok(out)
    }
}

impl BlobStore for LocalBlobStore {
    async fn upload_file(&self, key: &BlobKey, path: &Path) -> Result<UploadReceipt, StoreError> {
        let (this, key, path) = (self.clone(), key.clone(), path.to_owned());
        blocking(move || this.upload_blocking(&key, &path)).await
    }

    async fn read_range(&self, key: &BlobKey, range: ByteRange) -> Result<Vec<u8>, StoreError> {
        let (this, key) = (self.clone(), key.clone());
        blocking(move || this.read_range_blocking(&key, range)).await
    }

    async fn size(&self, key: &BlobKey) -> Result<Option<u64>, StoreError> {
        let (path, key) = (self.path_of(key), key.clone());
        blocking(move || match blob_size(&path, &key) {
            // A directory is a key prefix, not a blob.
            Err(StoreError::Conflict { .. }) => Ok(None),
            other => other,
        })
        .await
    }

    async fn delete(&self, key: &BlobKey) -> Result<(), StoreError> {
        let (path, key) = (self.path_of(key), key.clone());
        blocking(move || match blob_size(&path, &key) {
            // Absent, or a directory (a key prefix, not a blob): nothing to
            // delete.
            Ok(None) | Err(StoreError::Conflict { .. }) => Ok(()),
            Ok(Some(_)) => match fs::remove_file(&path) {
                Ok(()) => {
                    // Make the removal durable, as uploads make links
                    // durable.
                    if let Some(dir) = path.parent() {
                        sync_dir(dir)
                            .map_err(|e| local_io(&format!("syncing {}", dir.display()), &e))?;
                    }
                    Ok(())
                }
                Err(e) if is_absent(&e) => Ok(()),
                Err(e) => Err(local_io(&format!("deleting {}", path.display()), &e)),
            },
            Err(e) => Err(e),
        })
        .await
    }

    async fn presign_get(&self, key: &BlobKey, ttl: Duration) -> Result<PresignedUrl, StoreError> {
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| StoreError::Unavailable {
                reason: "the local blob store has no URL signer configured".to_owned(),
            })?;
        crate::check_presign_ttl(ttl, LOCAL_MAX_PRESIGN_TTL)?;
        let expires = SystemTime::now()
            .checked_add(ttl)
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .ok_or_else(|| StoreError::Unavailable {
                reason: format!("presign TTL {ttl:?} is out of range"),
            })?;
        // Round up, so the URL lives at least `ttl`.
        let expires = expires.as_secs() + u64::from(expires.subsec_nanos() > 0);
        Ok(PresignedUrl {
            url: signer.sign(key, expires),
            ttl,
            past_end: PastEndRange::Empty206,
        })
    }
}

/// Signs and verifies download URLs for a [`LocalBlobStore`].
///
/// A signed URL is `<base>/<key>?expires=<unix seconds>&signature=<hex>`,
/// where the signature is HMAC-SHA256 over the key and the expiry. Aspen's
/// own HTTP server serves these URLs (there is no bucket to send clients
/// to): it passes the path after `<base>/` and the query to
/// [`LocalUrlSigner::verify`], then answers from the store, honouring
/// `Range`, and answers a range at or past the end with `206` and an empty
/// body (S3's rule; [`PastEndRange::Empty206`]), never `416`.
#[derive(Clone)]
pub struct LocalUrlSigner {
    base: String,
    mac: Hmac<Sha256>,
}

/// Longest lifetime of a local presigned URL: the same cap as SigV4
/// ([`crate::S3_MAX_PRESIGN_TTL`]), so a TTL that works on one backend works
/// on the other. Callers should ask for much less (see
/// [`crate::RECOMMENDED_MAX_PRESIGN_TTL`]).
pub const LOCAL_MAX_PRESIGN_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Shortest accepted signing secret, in bytes.
pub const MIN_SECRET_LEN: usize = 32;

/// Domain separation for the signed message, so the secret cannot be
/// reused to forge some other HMAC-signed value of the same shape.
const SIGNING_CONTEXT: &[u8] = b"aspen-blob-url-v1\0";

impl std::fmt::Debug for LocalUrlSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalUrlSigner")
            .field("base", &self.base)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Why a [`LocalUrlSigner`] could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SignerConfigError {
    /// The base URL is not an absolute `http` or `https` URL without a query
    /// or fragment.
    #[error("URL signer base {0:?} must be an http(s) URL without query or fragment")]
    BadBase(String),
    /// The secret is shorter than [`MIN_SECRET_LEN`].
    #[error("URL signer secret must be at least {MIN_SECRET_LEN} bytes")]
    ShortSecret,
}

/// Why a request to a signed URL is refused. The server answers `403` for
/// all of them and does not tell the client which.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UrlRejected {
    /// The path is not a blob key, or the query lacks a well-formed
    /// `expires` or `signature`.
    #[error("malformed signed URL")]
    Malformed,
    /// The signature does not match the key and expiry.
    #[error("bad signature")]
    BadSignature,
    /// The URL has expired.
    #[error("signed URL expired")]
    Expired,
}

impl LocalUrlSigner {
    /// A signer for URLs under `base` (for example
    /// `https://git.example.com/-/blobs`), keyed with `secret`.
    pub fn new(base: &str, secret: &[u8]) -> Result<Self, SignerConfigError> {
        let parsed = url::Url::parse(base).map_err(|_| SignerConfigError::BadBase(base.into()))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.cannot_be_a_base()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(SignerConfigError::BadBase(base.into()));
        }
        if secret.len() < MIN_SECRET_LEN {
            return Err(SignerConfigError::ShortSecret);
        }
        let mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC takes any key length");
        Ok(Self {
            base: parsed.as_str().trim_end_matches('/').to_owned(),
            mac,
        })
    }

    /// The signed URL for `key`, valid until `expires` (Unix seconds).
    pub fn sign(&self, key: &BlobKey, expires: u64) -> String {
        let signature = hex(&self.tag(key.as_str(), expires));
        format!(
            "{}/{key}?expires={expires}&signature={signature}",
            self.base
        )
    }

    /// Checks a request for a signed URL: `path` is the part after
    /// `<base>/` (no leading `/`), `query` the raw query string. Returns the
    /// key to serve. The signature is compared in constant time.
    pub fn verify(&self, path: &str, query: &str, now: SystemTime) -> Result<BlobKey, UrlRejected> {
        let key = BlobKey::new(path).map_err(|_| UrlRejected::Malformed)?;
        let (mut expires, mut signature) = (None, None);
        for pair in query.split('&') {
            match pair.split_once('=') {
                Some(("expires", v)) if expires.is_none() => expires = Some(v),
                Some(("signature", v)) if signature.is_none() => signature = Some(v),
                _ => return Err(UrlRejected::Malformed),
            }
        }
        let expires = expires
            .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(UrlRejected::Malformed)?;
        let signature = signature.and_then(unhex).ok_or(UrlRejected::Malformed)?;
        let mut mac = self.mac.clone();
        mac.update(&message(key.as_str(), expires));
        mac.verify_slice(&signature)
            .map_err(|_| UrlRejected::BadSignature)?;
        let now = now.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
        if now >= expires {
            return Err(UrlRejected::Expired);
        }
        Ok(key)
    }

    fn tag(&self, key: &str, expires: u64) -> Vec<u8> {
        let mut mac = self.mac.clone();
        mac.update(&message(key, expires));
        mac.finalize().into_bytes().to_vec()
    }
}

fn message(key: &str, expires: u64) -> Vec<u8> {
    let mut m = SIGNING_CONTEXT.to_vec();
    m.extend_from_slice(key.as_bytes());
    m.push(0);
    m.extend_from_slice(expires.to_string().as_bytes());
    m
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Decodes lowercase hex, the only spelling [`hex`] produces.
fn unhex(s: &str) -> Option<Vec<u8>> {
    let digit = |b: u8| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    };
    let s = s.as_bytes();
    if s.is_empty() || !s.len().is_multiple_of(2) {
        return None;
    }
    s.chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect()
}

/// The size of the blob at `path`: `None` if absent, [`StoreError::Conflict`]
/// if the path is a directory (a key that is a prefix of another key).
fn blob_size(path: &Path, key: &BlobKey) -> Result<Option<u64>, StoreError> {
    match fs::metadata(path) {
        Ok(m) if m.is_file() => Ok(Some(m.len())),
        Ok(_) => Err(StoreError::Conflict {
            what: format!("blob {key}: the path is a directory"),
        }),
        Err(e) if is_absent(&e) => Ok(None),
        Err(e) => Err(local_io(&format!("reading {}", path.display()), &e)),
    }
}

/// The outcome of uploading `new` bytes to a key that already holds a blob
/// of `existing` bytes. Keys determine their bytes (pack keys embed the
/// checksum), so an equal size is the same blob uploaded again, and a
/// different size is a caller bug.
pub(crate) fn same_size(
    key: &BlobKey,
    existing: u64,
    new: u64,
) -> Result<UploadReceipt, StoreError> {
    if existing == new {
        Ok(UploadReceipt { size: existing })
    } else {
        Err(StoreError::Conflict {
            what: format!("blob {key} exists with {existing} bytes; the upload has {new}"),
        })
    }
}

pub(crate) fn changed_during_upload(path: &Path, expected: u64, actual: u64) -> StoreError {
    StoreError::LocalIo {
        reason: format!(
            "{} changed during upload: {expected} bytes at the start, {actual} read",
            path.display()
        ),
    }
}

pub(crate) fn not_found(key: &BlobKey) -> StoreError {
    StoreError::NotFound {
        what: format!("blob {key}"),
    }
}

pub(crate) fn too_large(range: ByteRange) -> StoreError {
    StoreError::LocalIo {
        reason: format!("range of {} bytes does not fit in memory", range.len()),
    }
}

pub(crate) fn local_io(what: &str, e: &io::Error) -> StoreError {
    StoreError::LocalIo {
        reason: format!("{what}: {e}"),
    }
}

/// `NotFound`, or `NotADirectory` (a key below a path that is a file).
fn is_absent(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
    )
}

fn below_blob(key: &BlobKey, blob: &str) -> StoreError {
    StoreError::Conflict {
        what: format!("blob {key}: {blob} is a blob, so no key can be below it"),
    }
}

fn sync_file(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    test_hooks::synced(path);
    fs::File::open(path)?.sync_all()
}

#[cfg(unix)]
fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(test)]
    test_hooks::synced(dir);
    fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> io::Result<()> {
    // Directories cannot be opened for syncing on Windows; NTFS journals
    // the rename itself.
    Ok(())
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StoreError::LocalIo {
            reason: format!("blocking task failed: {e}"),
        })?
}

/// Observation points for unit tests: which paths were synced, and a hook
/// that runs just before a staged file is linked into place. Both are
/// thread-local, and `upload_blocking` runs on the calling thread, so tests
/// that call it directly see only their own upload.
#[cfg(test)]
mod test_hooks {
    use std::cell::RefCell;
    use std::path::{Path, PathBuf};

    type Hook = Box<dyn FnOnce(&Path)>;

    thread_local! {
        static SYNCED: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
        static BEFORE_LINK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub(super) fn synced(path: &Path) {
        SYNCED.with(|s| s.borrow_mut().push(path.to_owned()));
    }

    pub(super) fn before_link(dest: &Path) {
        if let Some(hook) = BEFORE_LINK.with(|h| h.borrow_mut().take()) {
            hook(dest);
        }
    }

    /// Clears and returns the paths synced so far on this thread.
    pub(super) fn take_synced() -> Vec<PathBuf> {
        SYNCED.with(|s| std::mem::take(&mut *s.borrow_mut()))
    }

    pub(super) fn set_before_link(hook: impl FnOnce(&Path) + 'static) {
        BEFORE_LINK.with(|h| *h.borrow_mut() = Some(Box::new(hook)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, LocalBlobStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::open(dir.path().join("blobs")).unwrap();
        (dir, store)
    }

    fn source(dir: &Path, bytes: &[u8]) -> PathBuf {
        let path = dir.join("source");
        fs::write(&path, bytes).unwrap();
        path
    }

    /// The syncs an `Ok` upload of `key` must have made: the blob file
    /// (when `file` is set) and every directory from its parent up to the
    /// root.
    fn assert_durable(store: &LocalBlobStore, key: &BlobKey, synced: &[PathBuf], file: bool) {
        let dest = store.path_of(key);
        assert_eq!(synced.contains(&dest), file, "{synced:?}");
        let mut dir = dest.parent().unwrap();
        loop {
            assert!(
                synced.iter().any(|p| p == dir),
                "{dir:?} not synced: {synced:?}"
            );
            if dir == store.root() {
                break;
            }
            dir = dir.parent().unwrap();
        }
    }

    /// Invariant: `Ok` from an upload means the blob is durable, whoever
    /// wrote it. A new blob's directories are synced (its bytes were synced
    /// in staging); an upload that finds the blob already there (a retry
    /// after a crash between link and directory sync) syncs the file and
    /// its directories before returning `Ok`.
    #[test]
    fn every_ok_upload_syncs_the_blob_and_its_directories() {
        if !cfg!(unix) {
            return;
        }
        let (dir, store) = store();
        let key = BlobKey::new("repos/1/packs/ab.pack").unwrap();
        let src = source(dir.path(), b"pack bytes");

        test_hooks::take_synced();
        store.upload_blocking(&key, &src).unwrap();
        assert_durable(&store, &key, &test_hooks::take_synced(), false);

        store.upload_blocking(&key, &src).unwrap();
        assert_durable(&store, &key, &test_hooks::take_synced(), true);

        // A different size is refused before anything is synced or changed.
        let other = dir.path().join("other");
        fs::write(&other, b"other").unwrap();
        assert!(matches!(
            store.upload_blocking(&key, &other),
            Err(StoreError::Conflict { .. })
        ));
        assert!(test_hooks::take_synced().is_empty());
    }

    /// Invariant: the loser of a race (another upload links the blob after
    /// this one's existence check) also syncs the blob before `Ok`, and its
    /// staging file is removed.
    #[test]
    fn a_race_loser_syncs_the_winners_blob() {
        if !cfg!(unix) {
            return;
        }
        let (dir, store) = store();
        let key = BlobKey::new("repos/1/packs/cd.pack").unwrap();
        let src = source(dir.path(), b"same bytes");
        test_hooks::set_before_link(|dest| fs::write(dest, b"same bytes").unwrap());
        test_hooks::take_synced();
        let receipt = store.upload_blocking(&key, &src).unwrap();
        assert_eq!(receipt.size, 10);
        assert_durable(&store, &key, &test_hooks::take_synced(), true);
        assert_eq!(
            fs::read_dir(store.root().join(STAGING_DIR))
                .unwrap()
                .count(),
            0
        );

        // A racing writer with different bytes is a conflict, not `Ok`.
        let key = BlobKey::new("repos/1/packs/ef.pack").unwrap();
        test_hooks::set_before_link(|dest| fs::write(dest, b"longer bytes").unwrap());
        assert!(matches!(
            store.upload_blocking(&key, &src),
            Err(StoreError::Conflict { .. })
        ));
    }

    /// Invariant: a key below an existing blob (`a/b` when `a` is a blob)
    /// is a `Conflict` naming that blob, not a local I/O failure, at any
    /// depth, and is refused before the source is copied into staging.
    #[test]
    fn a_key_below_a_blob_conflicts() {
        let (dir, store) = store();
        let src = source(dir.path(), b"x");
        store
            .upload_blocking(&BlobKey::new("a").unwrap(), &src)
            .unwrap();
        for below in ["a/b", "a/b/c"] {
            match store.upload_blocking(&BlobKey::new(below).unwrap(), &src) {
                Err(StoreError::Conflict { what }) => {
                    assert!(what.contains("a is a blob"), "{what}")
                }
                other => panic!("{below}: {other:?}"),
            }
        }
        // The check runs before the source is read: a source that fails on
        // read (a directory) still gets `Conflict`, not a copy error.
        match store.upload_blocking(&BlobKey::new("a/b").unwrap(), dir.path()) {
            Err(StoreError::Conflict { what }) => assert!(what.contains("a is a blob"), "{what}"),
            other => panic!("{other:?}"),
        }
        let staging: Vec<_> = fs::read_dir(store.root().join(STAGING_DIR))
            .unwrap()
            .collect();
        assert!(staging.is_empty(), "{staging:?}");
    }

    fn signer() -> LocalUrlSigner {
        LocalUrlSigner::new("http://127.0.0.1:7070/-/blobs/", &[7; 32]).unwrap()
    }

    fn split(url: &str) -> (&str, &str) {
        let rest = url.strip_prefix("http://127.0.0.1:7070/-/blobs/").unwrap();
        rest.split_once('?').unwrap()
    }

    /// Invariant: a signed URL verifies to exactly its key until it expires,
    /// and any change to the key, expiry or signature is refused.
    #[test]
    fn signed_urls_verify_only_unchanged_and_unexpired() {
        let s = signer();
        let key = BlobKey::new("repos/1/packs/ab.pack").unwrap();
        let at = |secs| UNIX_EPOCH + Duration::from_secs(secs);
        let url = s.sign(&key, 1_000);
        let (path, query) = split(&url);
        assert_eq!(s.verify(path, query, at(999)), Ok(key.clone()));
        assert_eq!(s.verify(path, query, at(1_000)), Err(UrlRejected::Expired));

        let other = s.sign(&BlobKey::new("repos/1/packs/cd.pack").unwrap(), 1_000);
        let (_, other_query) = split(&other);
        assert_eq!(
            s.verify(path, other_query, at(0)),
            Err(UrlRejected::BadSignature)
        );
        let later = query.replace("expires=1000", "expires=9000");
        assert_eq!(
            s.verify(path, &later, at(0)),
            Err(UrlRejected::BadSignature)
        );
        let (head, sig) = query.split_once("signature=").unwrap();
        let upper = format!("{head}signature={}", sig.to_uppercase());
        assert_eq!(s.verify(path, &upper, at(0)), Err(UrlRejected::Malformed));
        for bad in [
            "",
            "expires=1000",
            "signature=00",
            "expires=+1000&signature=00",
        ] {
            assert_eq!(
                s.verify(path, bad, at(0)),
                Err(UrlRejected::Malformed),
                "{bad}"
            );
        }
        let twice = format!("{query}&expires=1");
        assert_eq!(s.verify(path, &twice, at(0)), Err(UrlRejected::Malformed));
        assert_eq!(s.verify("../x", query, at(0)), Err(UrlRejected::Malformed));

        let other_secret = LocalUrlSigner::new("http://127.0.0.1:7070/-/blobs", &[8; 32]).unwrap();
        assert_eq!(
            other_secret.verify(path, query, at(0)),
            Err(UrlRejected::BadSignature)
        );
    }

    #[test]
    fn signer_config_is_checked_and_redacted() {
        assert_eq!(
            LocalUrlSigner::new("http://x/", &[0; 31]).unwrap_err(),
            SignerConfigError::ShortSecret
        );
        for bad in [
            "ftp://x/",
            "http://x/?a=1",
            "http://x/#f",
            "mailto:a@b",
            "x",
        ] {
            assert!(LocalUrlSigner::new(bad, &[0; 32]).is_err(), "{bad}");
        }
        let s = LocalUrlSigner::new("https://x/blobs", &[0xab; 32]).unwrap();
        assert!(!format!("{s:?}").contains("ab"), "{s:?}");
        let key = BlobKey::new("k").unwrap();
        assert!(
            s.sign(&key, 5)
                .starts_with("https://x/blobs/k?expires=5&signature=")
        );
    }

    #[test]
    fn hex_round_trips() {
        let bytes: Vec<u8> = (0..=255).collect();
        assert_eq!(unhex(&hex(&bytes)), Some(bytes));
        assert_eq!(unhex("abc"), None);
        assert_eq!(unhex("zz"), None);
    }
}
