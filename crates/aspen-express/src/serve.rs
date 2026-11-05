//! The express HTTP service: `GET`/`HEAD` of signed pack URLs, and a
//! health check.
//!
//! | Path (under the express base) | Answer |
//! |---|---|
//! | `/-/health` | `200 ok`, never cached |
//! | `/<key id>/<expires>/<signature>/<pack key>` | the pack, if the URL verifies ([`crate::url`]) |
//! | anything else, or a URL that does not verify | `403 forbidden`, never cached, the same bytes whatever the reason |
//!
//! **Responses are immutable and safe to share.** The bytes behind a pack
//! key never change (the key embeds the pack's checksum, blobs are
//! write-once, and the cache keeps only bytes that hash to that checksum),
//! so a successful response carries:
//!
//! - `ETag: "<checksum>"` (strong), and `304` for a matching
//!   `If-None-Match`;
//! - `Cache-Control: public, max-age=<seconds until the URL expires>,
//!   immutable, must-revalidate, no-transform`. An edge cache may share the
//!   response with every client holding the same URL, but never past the
//!   URL's expiry, and `must-revalidate` forbids serving it stale (for
//!   example while the origin is down), since revalidating an expired URL
//!   is refused.
//!
//! Refusals, missing packs and errors carry `Cache-Control: no-store`.
//!
//! **Ranges.** A single `bytes=<a>-[<b>]` range gets `206` with an exact
//! `Content-Range`; a range starting at or past the end gets `206` with an
//! empty body and `Content-Range: bytes */<size>`, never `416` (S3: git
//! 2.55 and older cannot resume after a `416`). An `If-Range` that is not
//! this pack's ETag turns the request into a full `200`. Other range forms
//! (suffix ranges, lists) are ignored, as RFC 9110 allows.
//!
//! **Where bytes come from.** A cached pack is read from disk. On a miss,
//! the request starts (or joins) a cache fill; a pack of at most
//! [`ExpressConfig::wait_for_fill_bytes`] is served from disk once the fill
//! is done, a larger one streams straight from the blob store while the
//! fill runs, so the first client of a large pack does not wait for the
//! whole copy (and a CDN's origin timeout is not hit).

use std::io::SeekFrom;
use std::sync::Arc;

use aspen_core::blob::{BlobStore, ByteRange};
use aspen_core::error::StoreError;
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use futures_util::stream;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::cache::{CacheConfig, PackCache};
use crate::key::PackKey;
use crate::metrics::ExpressMetrics;
use crate::routes::unix_now;
use crate::url::{UrlKeys, verify};

/// Express service settings.
#[derive(Debug, Clone)]
pub struct ExpressConfig {
    /// The disk cache; `None` serves every request from the blob store.
    pub cache: Option<CacheConfig>,
    /// Largest pack a cache miss waits for (and then serves from disk);
    /// larger packs stream from the blob store while the cache fills.
    pub wait_for_fill_bytes: u64,
    /// Bytes read from the blob store or the disk per chunk.
    pub read_bytes: u64,
}

impl Default for ExpressConfig {
    /// No cache, 64 MiB wait threshold, 1 MiB reads.
    fn default() -> Self {
        Self {
            cache: None,
            wait_for_fill_bytes: 64 << 20,
            read_bytes: 1 << 20,
        }
    }
}

/// The express service over blob store `B`. Cheap to clone.
#[derive(Debug)]
pub struct Express<B> {
    inner: Arc<Inner<B>>,
}

impl<B> Clone for Express<B> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug)]
struct Inner<B> {
    blobs: B,
    keys: UrlKeys,
    cache: Option<Arc<PackCache>>,
    metrics: Arc<ExpressMetrics>,
    config: ExpressConfig,
}

impl<B> Express<B>
where
    B: BlobStore + Clone + 'static,
{
    /// A service reading packs from `blobs` and accepting URLs signed by
    /// any of `keys`. Opens (and cleans) the cache directory if one is
    /// configured.
    pub fn new(blobs: B, keys: UrlKeys, config: ExpressConfig) -> std::io::Result<Self> {
        let metrics = Arc::new(ExpressMetrics::default());
        let cache = match &config.cache {
            Some(c) => Some(Arc::new(PackCache::open(c, Arc::clone(&metrics))?)),
            None => None,
        };
        Ok(Self {
            inner: Arc::new(Inner {
                blobs,
                keys,
                cache,
                metrics,
                config,
            }),
        })
    }

    /// The router: serve it at the root of a standalone express process,
    /// or nest it (Aspen's server mounts it at `/-/express`).
    pub fn router(&self) -> Router {
        Router::new().fallback(handle::<B>).with_state(self.clone())
    }

    /// The service's counters.
    pub fn metrics(&self) -> &ExpressMetrics {
        &self.inner.metrics
    }
}

async fn handle<B>(State(x): State<Express<B>>, request: Request) -> Response
where
    B: BlobStore + Clone + 'static,
{
    let path = request.uri().path().strip_prefix('/').unwrap_or("");
    if path == "-/health" {
        return plain(StatusCode::OK, "ok");
    }
    let head = match *request.method() {
        Method::GET => false,
        Method::HEAD => true,
        _ => {
            let mut r = plain(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
            r.headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return r;
        }
    };
    let x = &x.inner;
    x.metrics.request();
    let now = unix_now();
    let granted = match verify(&x.keys, path, now) {
        Ok(g) => g,
        Err(r) => {
            x.metrics.refused(r);
            return plain(StatusCode::FORBIDDEN, "forbidden");
        }
    };
    let key = granted.key;

    let (source, size) = match locate(x, &key).await {
        Ok(found) => found,
        Err(LocateError::NotFound) => {
            x.metrics.not_found();
            return plain(StatusCode::NOT_FOUND, "not found");
        }
        Err(LocateError::Store(e)) => {
            x.metrics.store_error();
            let status = if matches!(e, StoreError::Unavailable { .. }) {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            return plain(status, "storage error");
        }
    };

    let etag = format!("\"{}\"", key.checksum().to_hex());
    // A cache counts `max-age` from when it receives the response, so read
    // the clock again now: `locate` may have waited seconds for the store
    // or a cache fill since the request was verified. A URL that expired
    // meanwhile gets `max-age=0`, which `must-revalidate` keeps from being
    // reused.
    let cache_control = format!(
        "public, max-age={}, immutable, must-revalidate, no-transform",
        granted.expires.saturating_sub(unix_now())
    );
    let headers = request.headers();
    let mut builder = Response::builder()
        .header(header::ETAG, &etag)
        .header(header::CACHE_CONTROL, &cache_control)
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    if if_none_match(headers, &etag) {
        x.metrics.not_modified();
        return builder
            .status(StatusCode::NOT_MODIFIED)
            .body(Body::empty())
            .expect("valid response");
    }
    builder = builder
        .header(header::CONTENT_TYPE, "application/x-git-packed-objects")
        .header(header::ACCEPT_RANGES, "bytes");
    let range = range_applies(headers, &etag)
        .then(|| {
            headers
                .get(header::RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_range)
        })
        .flatten();
    let (status, start, end) = match span(range, size) {
        Span::PastEnd => {
            return builder
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_RANGE, format!("bytes */{size}"))
                .header(header::CONTENT_LENGTH, 0)
                .body(Body::empty())
                .expect("valid response");
        }
        Span::Partial { start, end } => {
            builder = builder.header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{}/{size}", end - 1),
            );
            (StatusCode::PARTIAL_CONTENT, start, end)
        }
        Span::Whole => (StatusCode::OK, 0, size),
    };
    builder = builder
        .status(status)
        .header(header::CONTENT_LENGTH, HeaderValue::from(end - start));
    if head || start == end {
        return builder.body(Body::empty()).expect("valid response");
    }
    let body = match source {
        Source::File(path) => match file_body(x, &path, start, end).await {
            Some(body) => body,
            // Evicted between the lookup and the open.
            None => origin_body(x, &key, start, end),
        },
        Source::Origin => origin_body(x, &key, start, end),
    };
    builder.body(body).expect("valid response")
}

enum Source {
    File(std::path::PathBuf),
    Origin,
}

enum LocateError {
    NotFound,
    Store(StoreError),
}

/// Where to read `key` from, and its size.
async fn locate<B>(x: &Inner<B>, key: &PackKey) -> Result<(Source, u64), LocateError>
where
    B: BlobStore + Clone + 'static,
{
    let Some(cache) = &x.cache else {
        x.metrics.miss();
        return origin_size(x, key).await.map(|s| (Source::Origin, s));
    };
    if let Some((path, size)) = cache.get(key) {
        x.metrics.hit();
        return Ok((Source::File(path), size));
    }
    x.metrics.miss();
    let size = origin_size(x, key).await?;
    if size <= cache.max_bytes() {
        let mut rx = cache.fill(key, size, &x.blobs, x.config.read_bytes);
        if size <= x.config.wait_for_fill_bytes {
            let filled = rx
                .wait_for(Option::is_some)
                .await
                .is_ok_and(|v| *v == Some(true));
            if filled && let Some((path, size)) = cache.get(key) {
                return Ok((Source::File(path), size));
            }
        }
    }
    Ok((Source::Origin, size))
}

async fn origin_size<B: BlobStore>(x: &Inner<B>, key: &PackKey) -> Result<u64, LocateError> {
    match x.blobs.size(key.blob_key()).await {
        Ok(Some(size)) => Ok(size),
        Ok(None) => Err(LocateError::NotFound),
        Err(e) => Err(LocateError::Store(e)),
    }
}

type Chunk = Result<Bytes, std::io::Error>;

async fn file_body<B>(x: &Inner<B>, path: &std::path::Path, start: u64, end: u64) -> Option<Body> {
    let mut file = tokio::fs::File::open(path).await.ok()?;
    file.seek(SeekFrom::Start(start)).await.ok()?;
    let metrics = Arc::clone(&x.metrics);
    let chunk = x.config.read_bytes.max(1);
    let chunks = stream::unfold((file, start), move |(mut file, at)| {
        let metrics = Arc::clone(&metrics);
        async move {
            if at >= end {
                return None;
            }
            let n = usize::try_from(chunk.min(end - at)).unwrap_or(usize::MAX);
            let mut buf = vec![0; n];
            match file.read_exact(&mut buf).await {
                Ok(_) => {
                    metrics.served(n as u64);
                    Some((
                        Ok::<_, std::io::Error>(Bytes::from(buf)),
                        (file, at + n as u64),
                    ))
                }
                Err(e) => Some((Err(e), (file, end))),
            }
        }
    });
    Some(Body::from_stream(chunks))
}

fn origin_body<B>(x: &Inner<B>, key: &PackKey, start: u64, end: u64) -> Body
where
    B: BlobStore + Clone + 'static,
{
    let blobs = x.blobs.clone();
    let metrics = Arc::clone(&x.metrics);
    let key = key.blob_key().clone();
    let chunk = x.config.read_bytes.max(1);
    let chunks = stream::unfold(start, move |at| {
        let (blobs, key, metrics) = (blobs.clone(), key.clone(), Arc::clone(&metrics));
        async move {
            let range = ByteRange::new(at, (at + chunk).min(end))?;
            let item: Chunk = match blobs.read_exact_range(&key, range).await {
                Ok(bytes) => {
                    metrics.origin(bytes.len() as u64);
                    metrics.served(bytes.len() as u64);
                    Ok(Bytes::from(bytes))
                }
                // A failed read ends the body early; the client sees a short
                // download and resumes with a Range request.
                Err(e) => return Some((Err(std::io::Error::other(e.to_string())), end)),
            };
            Some((item, range.end()))
        }
    });
    Body::from_stream(chunks)
}

/// Whether `If-None-Match` matches (`*`, or any listed tag equal to
/// `etag` under the weak comparison RFC 9110 prescribes for it).
fn if_none_match(headers: &HeaderMap, etag: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .any(|t| t == "*" || t.strip_prefix("W/").unwrap_or(t) == etag)
}

/// Whether a `Range` header may be honoured: no `If-Range`, or one that is
/// exactly this pack's (strong) ETag. A date or another tag means the
/// client's partial copy may be of something else, so it gets the whole
/// pack.
fn range_applies(headers: &HeaderMap, etag: &str) -> bool {
    match headers.get(header::IF_RANGE) {
        None => true,
        Some(v) => v.to_str().is_ok_and(|v| v.trim() == etag),
    }
}

/// A requested byte range: `start` and, if given, the last byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeSpec {
    start: u64,
    last: Option<u64>,
}

/// Parses `bytes=<a>-` or `bytes=<a>-<b>`, the forms `git http-fetch` and
/// caches send.
fn parse_range(v: &str) -> Option<RangeSpec> {
    let (a, b) = v.trim().strip_prefix("bytes=")?.split_once('-')?;
    let start = a.parse().ok()?;
    let last = match b {
        "" => None,
        b => Some(b.parse().ok().filter(|l| *l >= start)?),
    };
    Some(RangeSpec { start, last })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Span {
    Whole,
    PastEnd,
    Partial { start: u64, end: u64 },
}

fn span(range: Option<RangeSpec>, size: u64) -> Span {
    match range {
        None => Span::Whole,
        Some(r) if r.start >= size => Span::PastEnd,
        Some(r) => Span::Partial {
            start: r.start,
            end: r.last.map_or(size, |l| l.saturating_add(1).min(size)),
        },
    }
}

fn plain(status: StatusCode, text: &str) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from(format!("{text}\n")))
        .expect("valid response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_and_spans() {
        assert_eq!(
            span(parse_range("bytes=5-18446744073709551615"), 100),
            Span::Partial { start: 5, end: 100 }
        );
        assert_eq!(span(parse_range("bytes=100-"), 100), Span::PastEnd);
        assert_eq!(
            span(parse_range("bytes=0-9"), 100),
            Span::Partial { start: 0, end: 10 }
        );
        for bad in ["bytes=-5", "bytes=5-1", "items=0-1", "bytes=0-1,4-5"] {
            assert_eq!(parse_range(bad), None, "{bad}");
        }
    }

    #[test]
    fn conditionals() {
        let etag = "\"abc\"";
        let mut h = HeaderMap::new();
        assert!(!if_none_match(&h, etag));
        assert!(range_applies(&h, etag));
        h.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("\"x\", W/\"abc\""),
        );
        assert!(if_none_match(&h, etag));
        h.insert(header::IF_RANGE, HeaderValue::from_static("\"abc\""));
        assert!(range_applies(&h, etag));
        h.insert(header::IF_RANGE, HeaderValue::from_static("W/\"abc\""));
        assert!(!range_applies(&h, etag));
        h.insert(
            header::IF_RANGE,
            HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
        );
        assert!(!range_applies(&h, etag));
    }
}
