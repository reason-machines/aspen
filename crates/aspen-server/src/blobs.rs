//! `/-/blobs/<key>?expires=..&signature=..`: downloads of a local blob
//! store's presigned URLs (A4 obligation for B2).
//!
//! The URL is checked with [`LocalUrlSigner::verify`](aspen_blob::LocalUrlSigner::verify),
//! and every refusal is the same `403`, so a client learns nothing about
//! which check failed. The response honours a single `Range: bytes=a-[b]`,
//! and answers a range that starts at or past the end with `206` and an
//! empty body (`Content-Range: bytes */<size>`), never `416`: git 2.55 and
//! older get stuck for good resuming an interrupted packfile-URI download
//! against a `416` (S3, finding 14). That makes these URLs safe for every
//! client, so the fetch path hands them out whatever the client's agent.

use std::time::SystemTime;

use aspen_blob::LocalBlobStore;
use aspen_core::blob::{BlobStore, ByteRange};
use aspen_core::error::StoreError;
use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::stream;

use crate::plain;

/// Bytes read from the store per chunk of a download.
const CHUNK: u64 = 1 << 20;

/// A requested byte range: `start` and, if given, the last byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeSpec {
    start: u64,
    last: Option<u64>,
}

/// Parses `bytes=<a>-` or `bytes=<a>-<b>`, the forms `git http-fetch`
/// sends. Suffix ranges and lists are not supported; such a header is
/// ignored, as RFC 9110 allows, and the whole blob is sent.
fn parse_range(v: &str) -> Option<RangeSpec> {
    let (a, b) = v.trim().strip_prefix("bytes=")?.split_once('-')?;
    let start = a.parse().ok()?;
    let last = match b {
        "" => None,
        b => Some(b.parse().ok().filter(|l| *l >= start)?),
    };
    Some(RangeSpec { start, last })
}

/// What a request for a blob of `size` bytes gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Span {
    /// No (usable) range: `200` with every byte.
    Whole,
    /// A range starting at or past the end: an empty `206`.
    PastEnd,
    /// `206` with bytes `start..end` (`end` exclusive, never past `size`).
    Partial { start: u64, end: u64 },
}

fn span(range: Option<RangeSpec>, size: u64) -> Span {
    match range {
        None => Span::Whole,
        Some(r) if r.start >= size => Span::PastEnd,
        Some(r) => Span::Partial {
            start: r.start,
            // `last` may be `u64::MAX`; the range ends at the blob's end.
            end: r.last.map_or(size, |l| l.saturating_add(1).min(size)),
        },
    }
}

pub(crate) async fn serve(store: &LocalBlobStore, request: Request) -> Response {
    let forbidden = || plain(StatusCode::FORBIDDEN, "forbidden");
    let Some(signer) = store.url_signer() else {
        return forbidden();
    };
    let path = request.uri().path().strip_prefix("/-/blobs/").unwrap_or("");
    let query = request.uri().query().unwrap_or("");
    let Ok(key) = signer.verify(path, query, SystemTime::now()) else {
        return forbidden();
    };
    let size = match store.size(&key).await {
        Ok(Some(size)) => size,
        Ok(None) => return plain(StatusCode::NOT_FOUND, "not found"),
        Err(e) => return store_error(&e),
    };
    let range = request
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);
    let mut builder = Response::builder()
        .header(header::CONTENT_TYPE, "application/x-git-packed-objects")
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "private, max-age=0, no-transform")
        // The bytes are a pack a pusher chose, served from the Git origin:
        // a browser must never sniff them into a page (D4).
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header(header::CONTENT_SECURITY_POLICY, "sandbox");
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
    if request.method() == Method::HEAD || start == end {
        return builder.body(Body::empty()).expect("valid response");
    }
    let store = store.clone();
    let chunks = stream::unfold(start, move |at| {
        let (store, key) = (store.clone(), key.clone());
        async move {
            let range = ByteRange::new(at, (at + CHUNK).min(end))?;
            match store.read_exact_range(&key, range).await {
                Ok(bytes) => Some((Ok::<_, StoreError>(bytes), range.end())),
                // A failed read ends the body early; the client sees a
                // short download and resumes with a Range request.
                Err(e) => Some((Err(e), end)),
            }
        }
    });
    builder
        .body(Body::from_stream(chunks))
        .expect("valid response")
}

fn store_error(e: &StoreError) -> Response {
    let status = if matches!(e, StoreError::Unavailable { .. }) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (status, "storage error\n").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(
            parse_range("bytes=10-"),
            Some(RangeSpec {
                start: 10,
                last: None
            })
        );
        assert_eq!(
            parse_range("bytes=0-99"),
            Some(RangeSpec {
                start: 0,
                last: Some(99)
            })
        );
        for bad in [
            "bytes=-5",
            "bytes=5-1",
            "items=0-1",
            "bytes=a-",
            "bytes=0-1,4-5",
        ] {
            assert_eq!(parse_range(bad), None, "{bad}");
        }
    }

    /// Invariant: every parsed range maps to bytes inside the blob without
    /// overflow, including a last byte of `u64::MAX` (which used to
    /// overflow `last + 1`).
    #[test]
    fn spans_never_overflow_or_pass_the_end() {
        let max = parse_range("bytes=5-18446744073709551615");
        assert_eq!(span(max, 100), Span::Partial { start: 5, end: 100 });
        assert_eq!(
            span(parse_range("bytes=0-9"), 100),
            Span::Partial { start: 0, end: 10 }
        );
        assert_eq!(
            span(parse_range("bytes=90-"), 100),
            Span::Partial {
                start: 90,
                end: 100
            }
        );
        assert_eq!(span(parse_range("bytes=100-"), 100), Span::PastEnd);
        assert_eq!(
            span(
                parse_range("bytes=18446744073709551615-18446744073709551615"),
                100
            ),
            Span::PastEnd
        );
        assert_eq!(span(None, 100), Span::Whole);
    }
}
